//! Emulator fidelity probe (multi-substrate plan 01, gate G0).
//!
//! Verifies that fake-gcs-server and Azurite faithfully implement the
//! object-store semantics Zeppelin's storage seam depends on, BEFORE any
//! transport code exists. Talks to the emulators through raw `object_store`
//! builders on purpose: the probe must not depend on Zeppelin code under
//! change, and it must fail (not vacuously pass) if an emulator ignores
//! preconditions.
//!
//! Env-gated so `cargo test` stays green without emulators:
//! - GCS probes run only when `FIDELITY_PROBE_GCS_ENDPOINT` is set
//!   (e.g. `http://127.0.0.1:4443`, a native fake-gcs-server).
//! - Azure probes run only when `FIDELITY_PROBE_AZURE` is set (any value;
//!   Azurite on the default `http://127.0.0.1:10000`, or override with
//!   `AZURITE_BLOB_STORAGE_URL`).
//!
//! Probe map (see tasks/multi-substrate/01-emulator-fidelity-gate.md):
//! P1 create-conflict, P2 CAS update, P3 stale-CAS rejection (the
//! vacuous-pass check), P4 GET version tokens, P5 LIST-vs-GET ETag equality,
//! P6 if-none-match revalidation, P7 user-metadata round-trip, P8
//! copy_if_not_exists, P9 delete-of-absent semantics, P10 1,100-key
//! delete_stream, P11 list-with-delimiter.

use std::sync::{Arc, OnceLock};

use base64::Engine as _;
use futures::stream::{self, StreamExt as _};
use object_store::{
    path::Path, Attribute, AttributeValue, Attributes, ClientOptions, Error, GetOptions,
    ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion,
};

const BUCKET: &str = "zeppelin-test";
const CONTAINER: &str = "zeppelin-test";
const AZURE_ACCOUNT: &str = "devstoreaccount1";
const AZURE_ACCOUNT_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

// ---------------------------------------------------------------------------
// Store construction
// ---------------------------------------------------------------------------

/// Service-account JSON kept alive for the whole process: `object_store`
/// 0.11.2 has no GCS endpoint builder knob, only `gcs_base_url` +
/// `disable_oauth` inside the service-account file.
static GCS_SA_FILE: OnceLock<tempfile::NamedTempFile> = OnceLock::new();

async fn gcs_store() -> Option<Arc<dyn ObjectStore>> {
    let endpoint = std::env::var("FIDELITY_PROBE_GCS_ENDPOINT").ok()?;

    // fake-gcs-server creates buckets via its JSON API; 409 = already there.
    let resp = reqwest::Client::new()
        .post(format!("{endpoint}/storage/v1/b?project=fidelity-probe"))
        .json(&serde_json::json!({ "name": BUCKET }))
        .send()
        .await
        .expect("fake-gcs-server unreachable — is it running natively?");
    assert!(
        resp.status().is_success() || resp.status().as_u16() == 409,
        "bucket create failed: {}",
        resp.status()
    );

    let sa = GCS_SA_FILE.get_or_init(|| {
        let file = tempfile::NamedTempFile::new().expect("temp service-account file");
        std::fs::write(
            file.path(),
            serde_json::json!({
                "gcs_base_url": endpoint,
                "disable_oauth": true,
                "client_email": "",
                "private_key": "",
                "private_key_id": "",
            })
            .to_string(),
        )
        .expect("write service-account json");
        file
    });

    let store = object_store::gcp::GoogleCloudStorageBuilder::new()
        .with_bucket_name(BUCKET)
        .with_service_account_path(sa.path().to_str().expect("utf-8 temp path"))
        .with_client_options(ClientOptions::new().with_allow_http(true))
        .build()
        .expect("gcs builder");
    Some(Arc::new(store))
}

async fn azure_store() -> Option<Arc<dyn ObjectStore>> {
    std::env::var("FIDELITY_PROBE_AZURE").ok()?;
    ensure_azurite_container().await;

    let store = object_store::azure::MicrosoftAzureBuilder::new()
        .with_use_emulator(true)
        .with_container_name(CONTAINER)
        .build()
        .expect("azure builder");
    Some(Arc::new(store))
}

/// RFC 2104 HMAC-SHA256. Hand-rolled from `sha2` (already a dependency)
/// because the probe may not add crates; used only to sign the one
/// container-create request Azurite requires before `object_store` can talk
/// to it.
fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut block = [0u8; 64];
    if key.len() > 64 {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let ipad: Vec<u8> = block.iter().map(|b| b ^ 0x36).collect();
    let opad: Vec<u8> = block.iter().map(|b| b ^ 0x5c).collect();
    let inner = Sha256::digest([ipad.as_slice(), msg].concat());
    let outer = Sha256::digest([opad.as_slice(), inner.as_slice()].concat());
    outer.into()
}

/// Creates the probe container with a SharedKey-signed request (Azurite has
/// no unauthenticated management surface). 201 = created, 409 = exists.
async fn ensure_azurite_container() {
    let base = std::env::var("AZURITE_BLOB_STORAGE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:10000".to_string());
    let date = chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();
    let version = "2021-08-06";
    // Full SharedKey string-to-sign: VERB, then 11 standard-header slots (all
    // empty here — Content-Length is empty when 0), canonicalized x-ms-*
    // headers, canonicalized resource including query params as name:value.
    let string_to_sign = format!(
        "PUT\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{date}\nx-ms-version:{version}\n/{AZURE_ACCOUNT}/{AZURE_ACCOUNT}/{CONTAINER}\nrestype:container"
    );
    let key = base64::engine::general_purpose::STANDARD
        .decode(AZURE_ACCOUNT_KEY)
        .expect("well-known emulator key decodes");
    let sig = base64::engine::general_purpose::STANDARD
        .encode(hmac_sha256(&key, string_to_sign.as_bytes()));
    let resp = reqwest::Client::new()
        .put(format!(
            "{base}/{AZURE_ACCOUNT}/{CONTAINER}?restype=container"
        ))
        .header("x-ms-date", &date)
        .header("x-ms-version", version)
        .header("Authorization", format!("SharedKey {AZURE_ACCOUNT}:{sig}"))
        .header("Content-Length", "0")
        .send()
        .await
        .expect("azurite unreachable — is it running natively?");
    let status = resp.status().as_u16();
    assert!(
        status == 201 || status == 409,
        "container create failed: {status} {:?}",
        resp.text().await
    );
}

// ---------------------------------------------------------------------------
// Shared probe bodies (identical semantics asserted on both emulators)
// ---------------------------------------------------------------------------

fn unique_prefix() -> String {
    format!("fidelity-probe/{}", uuid::Uuid::new_v4())
}

fn create_opts() -> PutOptions {
    PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    }
}

fn update_opts(version: UpdateVersion) -> PutOptions {
    PutOptions {
        mode: PutMode::Update(version),
        ..Default::default()
    }
}

/// P1: `PutMode::Create` twice — the second must be `AlreadyExists`.
async fn probe_create_conflict(store: &dyn ObjectStore) {
    let path = Path::from(format!("{}/p1", unique_prefix()));
    store
        .put_opts(&path, PutPayload::from("first"), create_opts())
        .await
        .expect("P1: initial create");
    let second = store
        .put_opts(&path, PutPayload::from("second"), create_opts())
        .await;
    match second {
        Err(Error::AlreadyExists { .. }) => {}
        other => panic!("P1: second create must be AlreadyExists, got {other:?}"),
    }
}

/// P2: `PutMode::Update` with the current token succeeds and returns a fresh
/// token. `require_version`: GCS must return a generation; Azure an ETag.
async fn probe_cas_update(store: &dyn ObjectStore, require_version: bool) {
    let path = Path::from(format!("{}/p2", unique_prefix()));
    let created = store
        .put_opts(&path, PutPayload::from("v1"), create_opts())
        .await
        .expect("P2: create");
    let token = UpdateVersion {
        e_tag: created.e_tag.clone(),
        version: created.version.clone(),
    };
    if require_version {
        assert!(
            created.version.is_some(),
            "P2: create must return a version token (generation)"
        );
    } else {
        assert!(created.e_tag.is_some(), "P2: create must return an ETag");
    }
    let updated = store
        .put_opts(&path, PutPayload::from("v2"), update_opts(token))
        .await
        .expect("P2: CAS update with current token");
    if require_version {
        assert!(
            updated.version.is_some(),
            "P2: update must return a fresh generation"
        );
        assert_ne!(
            updated.version, created.version,
            "P2: generation must advance on update"
        );
    } else {
        assert!(
            updated.e_tag.is_some(),
            "P2: update must return a fresh ETag"
        );
        assert_ne!(
            updated.e_tag, created.e_tag,
            "P2: ETag must change on update"
        );
    }
}

/// P3: `PutMode::Update` with a STALE token must fail with `Precondition`.
/// This is the vacuous-pass check — if the emulator ignores preconditions,
/// lost updates would be invisible to every downstream CAS test.
async fn probe_stale_cas_rejected(store: &dyn ObjectStore) {
    let path = Path::from(format!("{}/p3", unique_prefix()));
    let created = store
        .put_opts(&path, PutPayload::from("v1"), create_opts())
        .await
        .expect("P3: create");
    let stale = UpdateVersion {
        e_tag: created.e_tag.clone(),
        version: created.version.clone(),
    };
    store
        .put_opts(&path, PutPayload::from("v2"), update_opts(stale.clone()))
        .await
        .expect("P3: first CAS advances the object");
    let second = store
        .put_opts(&path, PutPayload::from("v3"), update_opts(stale))
        .await;
    match second {
        Err(Error::Precondition { .. }) => {}
        Ok(_) => panic!(
            "P3: STALE token CAS SUCCEEDED — emulator does not enforce \
             preconditions; every CAS test downstream would pass vacuously"
        ),
        other => panic!("P3: stale CAS must be Precondition, got {other:?}"),
    }
}

/// P4: GET returns the token(s) `StorageVersion::from_parts` needs.
async fn probe_get_version_tokens(store: &dyn ObjectStore, require_version: bool) -> String {
    let path = Path::from(format!("{}/p4", unique_prefix()));
    store
        .put_opts(&path, PutPayload::from("data"), create_opts())
        .await
        .expect("P4: create");
    let got = store.get(&path).await.expect("P4: get");
    assert!(got.meta.e_tag.is_some(), "P4: GET must return an ETag");
    if require_version {
        assert!(
            got.meta.version.is_some(),
            "P4: GET must return the version header (generation)"
        );
    }
    format!("e_tag={:?} version={:?}", got.meta.e_tag, got.meta.version)
}

/// How LIST-returned ETags relate to GET-returned ETags on a substrate.
#[derive(Clone, Copy)]
enum ListEtagShape {
    /// LIST ETag byte-equals GET ETag (S3/MinIO behavior).
    ByteEqual,
    /// LIST ETag is the GET ETag without surrounding quotes. This is REAL
    /// Azure behavior (List Blobs `<Etag>` is unquoted, the GET header is
    /// quoted), faithfully mirrored by Azurite — it forces a quote-stripping
    /// canonicalization in Zeppelin's LIST-vs-GET comparisons (plan 02).
    UnquotedInList,
}

fn strip_etag_quotes(etag: &str) -> &str {
    etag.strip_prefix('"')
        .and_then(|e| e.strip_suffix('"'))
        .unwrap_or(etag)
}

/// P5: LIST ETag identifies the same version as GET ETag, including after an
/// overwrite. GC's LIST-vs-GET checks fail closed without this — unbounded
/// storage growth. `shape` pins the exact raw relationship so a drifting
/// emulator still fails loudly.
async fn probe_list_get_etag_equality(store: &dyn ObjectStore, shape: ListEtagShape) {
    let prefix = unique_prefix();
    let path = Path::from(format!("{prefix}/p5"));
    for round in ["initial", "overwritten"] {
        let payload = format!("payload-{round}");
        store
            .put(&path, PutPayload::from(payload))
            .await
            .expect("P5: put");
        let got = store.get(&path).await.expect("P5: get");
        let listed: Vec<_> = store
            .list(Some(&Path::from(prefix.as_str())))
            .collect::<Vec<_>>()
            .await;
        let meta = listed
            .into_iter()
            .map(|r| r.expect("P5: list entry"))
            .find(|m| m.location == path)
            .expect("P5: listed object present");
        let list_etag = meta
            .e_tag
            .as_deref()
            .unwrap_or_else(|| panic!("P5: LIST must carry an ETag ({round})"));
        let get_etag = got
            .meta
            .e_tag
            .as_deref()
            .unwrap_or_else(|| panic!("P5: GET must carry an ETag ({round})"));
        match shape {
            ListEtagShape::ByteEqual => assert_eq!(
                list_etag, get_etag,
                "P5: LIST ETag must byte-equal GET ETag ({round})"
            ),
            ListEtagShape::UnquotedInList => {
                assert_eq!(
                    format!("\"{list_etag}\""),
                    get_etag,
                    "P5: LIST ETag must be exactly the unquoted GET ETag ({round})"
                );
            }
        }
        assert!(
            !strip_etag_quotes(list_etag).is_empty(),
            "P5: normalized ETag must be non-empty ({round})"
        );
    }
}

/// P6: `if_none_match` with a stale ETag must return the full body (hard
/// assert); the current-ETag NotModified path is recorded — the seam's one
/// sanctioned degradation is falling back to full-body revalidation.
async fn probe_if_none_match(store: &dyn ObjectStore) -> String {
    let path = Path::from(format!("{}/p6", unique_prefix()));
    let created = store
        .put_opts(&path, PutPayload::from("cached"), create_opts())
        .await
        .expect("P6: create");
    let current = created.e_tag.clone().expect("P6: create returns ETag");

    let on_current = store
        .get_opts(
            &path,
            GetOptions {
                if_none_match: Some(current.clone()),
                ..Default::default()
            },
        )
        .await;
    let current_behavior = match on_current {
        Err(Error::NotModified { .. }) => "NotModified".to_string(),
        Ok(_) => "full-body (degraded revalidation)".to_string(),
        Err(other) => {
            panic!("P6: if_none_match(current) must be NotModified or full body, got {other:?}")
        }
    };

    let stale = format!("\"stale-{}\"", uuid::Uuid::new_v4());
    let on_stale = store
        .get_opts(
            &path,
            GetOptions {
                if_none_match: Some(stale),
                ..Default::default()
            },
        )
        .await;
    match on_stale {
        Ok(got) => {
            let body = got.bytes().await.expect("P6: body");
            assert_eq!(
                &body[..],
                b"cached",
                "P6: stale if_none_match returns full body"
            );
        }
        Err(e) => panic!("P6: if_none_match(stale) must return the full body, got {e:?}"),
    }
    current_behavior
}

/// P7: user metadata round-trips through a conditional put. `name` varies by
/// substrate (Azure rejects hyphens; that variant is probed separately).
async fn probe_metadata_roundtrip(
    store: &dyn ObjectStore,
    name: &'static str,
) -> Result<(), Error> {
    let path = Path::from(format!("{}/p7", unique_prefix()));
    let mut attributes = Attributes::new();
    attributes.insert(
        Attribute::Metadata(name.into()),
        AttributeValue::from("incarnation-01"),
    );
    store
        .put_opts(
            &path,
            PutPayload::from("meta"),
            PutOptions {
                mode: PutMode::Create,
                attributes,
                ..Default::default()
            },
        )
        .await?;
    let got = store.get(&path).await.expect("P7: get");
    let value = got
        .attributes
        .get(&Attribute::Metadata(name.into()))
        .unwrap_or_else(|| panic!("P7: metadata {name:?} must round-trip"));
    assert_eq!(value.as_ref(), "incarnation-01", "P7: metadata value");
    Ok(())
}

/// P8: `copy_if_not_exists` — fresh target succeeds, existing target is
/// `AlreadyExists`.
async fn probe_copy_if_not_exists(store: &dyn ObjectStore) {
    let prefix = unique_prefix();
    let src = Path::from(format!("{prefix}/p8-src"));
    let dst = Path::from(format!("{prefix}/p8-dst"));
    store
        .put(&src, PutPayload::from("clone-me"))
        .await
        .expect("P8: put src");
    store
        .copy_if_not_exists(&src, &dst)
        .await
        .expect("P8: copy to fresh key");
    let got = store.get(&dst).await.expect("P8: get dst");
    assert_eq!(
        &got.bytes().await.expect("P8: dst body")[..],
        b"clone-me",
        "P8: copied content"
    );
    match store.copy_if_not_exists(&src, &dst).await {
        Err(Error::AlreadyExists { .. }) => {}
        other => panic!("P8: copy onto existing key must be AlreadyExists, got {other:?}"),
    }
}

/// P9: delete of an absent key — record Ok vs NotFound (feeds the plan-02
/// seam normalization); anything else fails.
async fn probe_delete_absent(store: &dyn ObjectStore) -> String {
    let path = Path::from(format!("{}/p9-never-written", unique_prefix()));
    match store.delete(&path).await {
        Ok(()) => "Ok".to_string(),
        Err(Error::NotFound { .. }) => "NotFound".to_string(),
        Err(other) => panic!("P9: delete-of-absent must be Ok or NotFound, got {other:?}"),
    }
}

/// P10: 1,100-key `delete_stream` completes with every key reported deleted
/// (exceeds `DELETE_MANY_MAX_KEYS`-style batch limits; GCS/Azure have no
/// native batch endpoint in object_store 0.11.2, so this exercises the
/// serial fallback at realistic GC-drain volume).
async fn probe_bulk_delete(store: &Arc<dyn ObjectStore>) {
    let prefix = unique_prefix();
    let paths: Vec<Path> = (0..1100)
        .map(|i| Path::from(format!("{prefix}/p10/{i:04}")))
        .collect();
    stream::iter(paths.clone())
        .map(|p| {
            let store = Arc::clone(store);
            async move {
                store
                    .put(&p, PutPayload::from("x"))
                    .await
                    .expect("P10: seed put");
            }
        })
        .buffer_unordered(32)
        .collect::<Vec<()>>()
        .await;

    let results: Vec<_> = store
        .delete_stream(stream::iter(paths.clone().into_iter().map(Ok)).boxed())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(results.len(), 1100, "P10: one result per key");
    for r in results {
        r.expect("P10: every delete succeeds");
    }

    let leftover: Vec<_> = store
        .list(Some(&Path::from(format!("{prefix}/p10"))))
        .collect::<Vec<_>>()
        .await;
    assert!(
        leftover.is_empty(),
        "P10: prefix must be empty after delete_stream, {} left",
        leftover.len()
    );
}

/// P11: list-with-delimiter returns correct common prefixes (boot namespace
/// discovery walks these).
async fn probe_list_with_delimiter(store: &dyn ObjectStore) {
    let prefix = unique_prefix();
    for key in ["a/1", "a/2", "b/1", "top"] {
        store
            .put(
                &Path::from(format!("{prefix}/{key}")),
                PutPayload::from("x"),
            )
            .await
            .expect("P11: seed put");
    }
    let listed = store
        .list_with_delimiter(Some(&Path::from(prefix.as_str())))
        .await
        .expect("P11: list_with_delimiter");
    let mut prefixes: Vec<String> = listed
        .common_prefixes
        .iter()
        .map(|p| p.to_string())
        .collect();
    prefixes.sort();
    assert_eq!(
        prefixes,
        vec![format!("{prefix}/a"), format!("{prefix}/b")],
        "P11: common prefixes"
    );
    let objects: Vec<String> = listed
        .objects
        .iter()
        .map(|o| o.location.to_string())
        .collect();
    assert_eq!(
        objects,
        vec![format!("{prefix}/top")],
        "P11: non-delimited objects"
    );
}

// ---------------------------------------------------------------------------
// GCS (fake-gcs-server) probes
// ---------------------------------------------------------------------------

macro_rules! gcs_probe {
    ($name:ident, $body:expr) => {
        #[tokio::test]
        async fn $name() {
            let Some(store) = gcs_store().await else {
                eprintln!("skipped: FIDELITY_PROBE_GCS_ENDPOINT unset");
                return;
            };
            #[allow(clippy::redundant_closure_call)]
            ($body)(store).await;
        }
    };
}

gcs_probe!(
    gcs_p01_create_conflict,
    |s: Arc<dyn ObjectStore>| async move {
        probe_create_conflict(s.as_ref()).await;
    }
);
gcs_probe!(gcs_p02_cas_update_returns_fresh_generation, |s: Arc<
    dyn ObjectStore,
>| async move {
    probe_cas_update(s.as_ref(), true).await;
});
gcs_probe!(
    gcs_p03_stale_cas_rejected,
    |s: Arc<dyn ObjectStore>| async move {
        probe_stale_cas_rejected(s.as_ref()).await;
    }
);
gcs_probe!(gcs_p04_get_returns_generation_and_etag, |s: Arc<
    dyn ObjectStore,
>| async move {
    let tokens = probe_get_version_tokens(s.as_ref(), true).await;
    eprintln!("P4[gcs]: {tokens}");
});
gcs_probe!(
    gcs_p05_list_get_etag_equality,
    |s: Arc<dyn ObjectStore>| async move {
        probe_list_get_etag_equality(s.as_ref(), ListEtagShape::ByteEqual).await;
    }
);
gcs_probe!(
    gcs_p06_if_none_match,
    |s: Arc<dyn ObjectStore>| async move {
        let behavior = probe_if_none_match(s.as_ref()).await;
        eprintln!("P6[gcs]: if_none_match(current) → {behavior}");
    }
);
gcs_probe!(gcs_p07_metadata_roundtrip_hyphenated, |s: Arc<
    dyn ObjectStore,
>| async move {
    // GCS x-goog-meta-* names may contain hyphens; Zeppelin's current
    // incarnation attribute name must round-trip unchanged.
    probe_metadata_roundtrip(s.as_ref(), "zeppelin-namespace-incarnation")
        .await
        .expect("P7[gcs]: hyphenated metadata name must be accepted");
});
gcs_probe!(
    gcs_p08_copy_if_not_exists,
    |s: Arc<dyn ObjectStore>| async move {
        probe_copy_if_not_exists(s.as_ref()).await;
    }
);
gcs_probe!(
    gcs_p09_delete_absent,
    |s: Arc<dyn ObjectStore>| async move {
        let behavior = probe_delete_absent(s.as_ref()).await;
        eprintln!("P9[gcs]: delete-of-absent → {behavior}");
    }
);
gcs_probe!(
    gcs_p10_bulk_delete_1100,
    |s: Arc<dyn ObjectStore>| async move {
        probe_bulk_delete(&s).await;
    }
);
gcs_probe!(
    gcs_p11_list_with_delimiter,
    |s: Arc<dyn ObjectStore>| async move {
        probe_list_with_delimiter(s.as_ref()).await;
    }
);

// ---------------------------------------------------------------------------
// Azure (Azurite) probes
// ---------------------------------------------------------------------------

macro_rules! azure_probe {
    ($name:ident, $body:expr) => {
        #[tokio::test]
        async fn $name() {
            let Some(store) = azure_store().await else {
                eprintln!("skipped: FIDELITY_PROBE_AZURE unset");
                return;
            };
            #[allow(clippy::redundant_closure_call)]
            ($body)(store).await;
        }
    };
}

azure_probe!(
    azure_p01_create_conflict,
    |s: Arc<dyn ObjectStore>| async move {
        probe_create_conflict(s.as_ref()).await;
    }
);
azure_probe!(azure_p02_cas_update_returns_fresh_etag, |s: Arc<
    dyn ObjectStore,
>| async move {
    probe_cas_update(s.as_ref(), false).await;
});
azure_probe!(
    azure_p03_stale_cas_rejected,
    |s: Arc<dyn ObjectStore>| async move {
        probe_stale_cas_rejected(s.as_ref()).await;
    }
);
azure_probe!(
    azure_p04_get_returns_etag,
    |s: Arc<dyn ObjectStore>| async move {
        // ETag is required; x-ms-version-id appears only with blob versioning
        // enabled, so version presence is recorded, not required.
        let tokens = probe_get_version_tokens(s.as_ref(), false).await;
        eprintln!("P4[azure]: {tokens}");
    }
);
azure_probe!(azure_p05_list_get_etag_equality, |s: Arc<
    dyn ObjectStore,
>| async move {
    probe_list_get_etag_equality(s.as_ref(), ListEtagShape::UnquotedInList).await;
});
azure_probe!(
    azure_p06_if_none_match,
    |s: Arc<dyn ObjectStore>| async move {
        let behavior = probe_if_none_match(s.as_ref()).await;
        eprintln!("P6[azure]: if_none_match(current) → {behavior}");
    }
);
azure_probe!(azure_p07_metadata_roundtrip_underscored, |s: Arc<
    dyn ObjectStore,
>| async move {
    // Azure metadata names must be valid C# identifiers — underscores only.
    probe_metadata_roundtrip(s.as_ref(), "zeppelin_namespace_incarnation")
        .await
        .expect("P7[azure]: underscored metadata name must be accepted");
});
azure_probe!(azure_p07b_metadata_hyphenated_recorded, |s: Arc<
    dyn ObjectStore,
>| async move {
    // Zeppelin's CURRENT name is hyphenated ("zeppelin-namespace-incarnation")
    // — illegal per Azure's C#-identifier rule. Record accept/reject; the
    // result drives plan 06's canonicalization decision. Real Azure rejects
    // it (400); if Azurite accepts it, that is an emulator infidelity to
    // document, not a license to skip canonicalization.
    match probe_metadata_roundtrip(s.as_ref(), "zeppelin-namespace-incarnation").await {
        Ok(()) => eprintln!(
            "P7b[azure]: hyphenated metadata name ACCEPTED (emulator deviation from real Azure)"
        ),
        Err(e) => eprintln!("P7b[azure]: hyphenated metadata name REJECTED ({e})"),
    }
});
azure_probe!(
    azure_p08_copy_if_not_exists,
    |s: Arc<dyn ObjectStore>| async move {
        probe_copy_if_not_exists(s.as_ref()).await;
    }
);
azure_probe!(
    azure_p09_delete_absent,
    |s: Arc<dyn ObjectStore>| async move {
        let behavior = probe_delete_absent(s.as_ref()).await;
        eprintln!("P9[azure]: delete-of-absent → {behavior}");
    }
);
azure_probe!(
    azure_p10_bulk_delete_1100,
    |s: Arc<dyn ObjectStore>| async move {
        probe_bulk_delete(&s).await;
    }
);
azure_probe!(
    azure_p11_list_with_delimiter,
    |s: Arc<dyn ObjectStore>| async move {
        probe_list_with_delimiter(s.as_ref()).await;
    }
);
