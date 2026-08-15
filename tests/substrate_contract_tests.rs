//! Substrate parity suite (multi-substrate plan 04).
//!
//! Seam-level contract tests exercising every object-store semantic Zeppelin
//! depends on, written against `ZeppelinStore` (plan 01's emulator probe
//! covers raw `object_store`) and parameterized by `TEST_BACKEND`. Cases gate
//! on `store.capabilities()` — never on backend names — and skip LOUDLY when
//! a capability is absent, so a silent skip can't masquerade as coverage.
//!
//! Proven green on memory/minio/local first: when a new transport lands, a
//! red parity run indicts the transport, not the suite.
//!
//! Case map (each pinned to the production dependency it protects):
//! C1 versioned reads, C2 create-only conflicts, C3 CAS happy/stale paths,
//! C4 declared token kind, C5 conditional-GET revalidation, C6 LIST-vs-GET
//! ETag comparability, C7 user-metadata round-trip, C8 copy_if_not_exists,
//! C9 delete-of-absent normalization, C10 bulk delete chunk contract,
//! C11 top-level prefix discovery, C12 CAS-unsupported fail-loud.

mod common;

use bytes::Bytes;
use common::harness::TestHarness;
use zeppelin::error::ZeppelinError;
use zeppelin::storage::capabilities::canonical_etag;
use zeppelin::storage::{
    CasTokenKind, ConditionalPutOutcome, CreateOnlyOutcome, ObjectUserMetadata, StorageVersion,
};

/// Loud capability skip: visible in `--nocapture` runs and never mistakable
/// for a green assertion.
macro_rules! skip_loudly {
    ($case:literal, $reason:expr) => {{
        eprintln!("SKIP (capability) {}: {}", $case, $reason);
        return;
    }};
}

/// C1: `put` → `get_with_meta` returns a non-empty version token
/// (`StorageVersion::from_parts` never yields an all-empty token).
/// Protects `read_versioned`.
#[tokio::test]
async fn c1_versioned_read_returns_non_empty_token() {
    let harness = TestHarness::new().await;
    let key = harness.key("c1/object.bin");
    harness
        .store
        .put(&key, Bytes::from_static(b"c1"))
        .await
        .expect("C1: put");
    let (bytes, version) = harness
        .store
        .get_with_meta(&key)
        .await
        .expect("C1: get_with_meta");
    assert_eq!(&bytes[..], b"c1");
    let version = version.expect("C1: versioned read must carry a token");
    assert!(
        version.etag().is_some() || version.backend_version().is_some(),
        "C1: from_parts non-empty rule"
    );
    harness.cleanup().await;
}

/// C2: create-only PUT conflicts on the second create — even a byte-identical
/// repeat (callers own idempotency). Protects manifest history retention,
/// snapshots, destruction records.
#[tokio::test]
async fn c2_create_only_put_conflicts_on_existing() {
    let harness = TestHarness::new().await;
    if !harness.store.capabilities().create_only_put {
        skip_loudly!("C2", "create_only_put not declared");
    }
    let key = harness.key("c2/history.bin");
    let created = harness
        .store
        .put_create_outcome(&key, Bytes::from_static(b"gen-1"))
        .await
        .expect("C2: first create");
    assert!(matches!(created, CreateOnlyOutcome::Created { .. }));

    let repeat_identical = harness
        .store
        .put_create_outcome(&key, Bytes::from_static(b"gen-1"))
        .await
        .expect("C2: identical repeat is a typed outcome");
    assert_eq!(repeat_identical, CreateOnlyOutcome::AlreadyExists);

    let repeat_different = harness
        .store
        .put_create_outcome(&key, Bytes::from_static(b"gen-2"))
        .await
        .expect("C2: different repeat is a typed outcome");
    assert_eq!(repeat_different, CreateOnlyOutcome::AlreadyExists);

    let body = harness.store.get(&key).await.expect("C2: read back");
    assert_eq!(&body[..], b"gen-1", "C2: loser never overwrites");
    harness.cleanup().await;
}

/// C3: CAS happy path succeeds with the current token; a stale token is a
/// typed `ManifestConflict` from `put_if_match` and a `Conflict` outcome (not
/// an `Err`) from `put_if_match_outcome`. Protects the manifest visibility
/// commit and every lease.
#[tokio::test]
async fn c3_cas_accepts_current_token_and_rejects_stale() {
    let harness = TestHarness::new().await;
    if harness.store.capabilities().conditional_put.is_none() {
        skip_loudly!("C3", "conditional_put not declared (covered by C12)");
    }
    let key = harness.key("c3/manifest.json");
    harness
        .store
        .put(&key, Bytes::from_static(b"v1"))
        .await
        .expect("C3: seed");
    let (_, version) = harness
        .store
        .get_with_meta(&key)
        .await
        .expect("C3: read token");
    let token = version.expect("C3: token present");

    harness
        .store
        .put_if_match(&key, Bytes::from_static(b"v2"), &token, "parity")
        .await
        .expect("C3: CAS with current token");

    let stale = harness
        .store
        .put_if_match(&key, Bytes::from_static(b"v3"), &token, "parity")
        .await;
    assert!(
        matches!(stale, Err(ZeppelinError::ManifestConflict { .. })),
        "C3: stale CAS must be ManifestConflict, got {stale:?}"
    );

    let outcome = harness
        .store
        .put_if_match_outcome(&key, Bytes::from_static(b"v3"), &token)
        .await
        .expect("C3: outcome form is Ok(Conflict), not Err");
    assert_eq!(outcome, ConditionalPutOutcome::Conflict);

    let body = harness.store.get(&key).await.expect("C3: read back");
    assert_eq!(&body[..], b"v2", "C3: stale writers never win");
    harness.cleanup().await;
}

/// C4: the returned token carries the field the declared `CasTokenKind`
/// requires. Protects every `StorageVersion::require` site.
#[tokio::test]
async fn c4_token_kind_matches_declared_capability() {
    let harness = TestHarness::new().await;
    let Some(kind) = harness.store.capabilities().conditional_put else {
        skip_loudly!("C4", "conditional_put not declared");
    };
    let key = harness.key("c4/token.bin");
    let created = harness
        .store
        .put_create_outcome(&key, Bytes::from_static(b"c4"))
        .await
        .expect("C4: create");
    let CreateOnlyOutcome::Created { version } = created else {
        panic!("C4: fresh key must be Created");
    };
    let token = version.expect("C4: create returns a token");
    match kind {
        CasTokenKind::ETag => assert!(
            token.etag().is_some(),
            "C4: ETag-CAS substrate must return an ETag"
        ),
        CasTokenKind::BackendVersion => assert!(
            token.backend_version().is_some(),
            "C4: generation-CAS substrate must return a backend version"
        ),
    }
    harness.cleanup().await;
}

/// C5: `get_if_none_match` with the current token returns `None` (strong
/// revalidation) or — the one sanctioned degradation — the identical body;
/// with a stale token it must return the new body; a token without an ETag
/// degrades to a full GET. Protects `manifest_cache` strong mode.
#[tokio::test]
async fn c5_conditional_get_revalidation_contract() {
    let harness = TestHarness::new().await;
    let key = harness.key("c5/cached.json");
    harness
        .store
        .put(&key, Bytes::from_static(b"cached-v1"))
        .await
        .expect("C5: seed");
    let (_, version) = harness
        .store
        .get_with_meta(&key)
        .await
        .expect("C5: read token");
    let current = version.expect("C5: token present");

    match harness
        .store
        .get_if_none_match(&key, &current)
        .await
        .expect("C5: revalidate current")
    {
        None => {}
        Some((bytes, _)) => {
            eprintln!("C5: degraded revalidation (full body for an unchanged object)");
            assert_eq!(
                &bytes[..],
                b"cached-v1",
                "C5: degraded revalidation must still return the exact cached content"
            );
        }
    }

    harness
        .store
        .put(&key, Bytes::from_static(b"cached-v2"))
        .await
        .expect("C5: overwrite");
    let stale = harness
        .store
        .get_if_none_match(&key, &current)
        .await
        .expect("C5: revalidate stale");
    let (bytes, new_version) = stale.expect("C5: stale token must fetch the new body");
    assert_eq!(&bytes[..], b"cached-v2");
    assert!(
        new_version.is_some(),
        "C5: refreshed read carries the new token"
    );

    let no_etag = StorageVersion::from_parts(None, Some("token-without-etag".to_string()))
        .expect("C5: backend-version-only token is constructible");
    let degraded = harness
        .store
        .get_if_none_match(&key, &no_etag)
        .await
        .expect("C5: no-ETag token degrades to a full GET");
    let (bytes, _) = degraded.expect("C5: degraded path returns the body");
    assert_eq!(&bytes[..], b"cached-v2");
    harness.cleanup().await;
}

/// C6: LIST-reported ETag identifies the same version GET reports, including
/// after an overwrite. Protects GC's LIST-vs-GET fail-closed checks (without
/// this, GC refuses deletions and storage grows without bound). Raw byte
/// equality is asserted alongside the canonical form because GC's comparisons
/// are byte-level today; the Azure transport (plan 06) revisits both together.
#[tokio::test]
async fn c6_list_etag_identifies_get_version() {
    let harness = TestHarness::new().await;
    if !harness.store.capabilities().list_etag_comparable {
        skip_loudly!("C6", "list_etag_comparable not declared");
    }
    let parent = harness.key("c6");
    let key = format!("{parent}/segment.bin");
    for round in ["initial", "overwritten"] {
        harness
            .store
            .put(&key, Bytes::from(format!("c6-{round}")))
            .await
            .expect("C6: put");
        let (_, get_version) = harness
            .store
            .get_with_meta(&key)
            .await
            .expect("C6: get_with_meta");
        let get_etag = get_version
            .as_ref()
            .and_then(StorageVersion::etag)
            .unwrap_or_else(|| panic!("C6: GET must carry an ETag ({round})"))
            .to_string();
        let listed = harness
            .store
            .list_prefix_meta(&parent)
            .await
            .expect("C6: list_prefix_meta");
        let listed_etag = listed
            .iter()
            .find(|object| object.key == key)
            .and_then(|object| object.version.as_ref())
            .and_then(StorageVersion::etag)
            .unwrap_or_else(|| panic!("C6: LIST must carry an ETag ({round})"));
        assert_eq!(
            listed_etag, get_etag,
            "C6: LIST ETag must byte-equal GET ETag ({round})"
        );
        assert_eq!(
            canonical_etag(listed_etag),
            canonical_etag(&get_etag),
            "C6: canonical forms must agree ({round})"
        );
    }
    harness.cleanup().await;
}

/// C7: user metadata round-trips through create-only and conditional PUTs
/// under Zeppelin's actual metadata key. Protects incarnation identity.
#[tokio::test]
async fn c7_user_metadata_round_trips_through_conditional_puts() {
    let harness = TestHarness::new().await;
    if !harness.store.capabilities().user_metadata {
        skip_loudly!("C7", "user_metadata not declared");
    }
    let key = harness.key("c7/meta.json");
    let metadata_key = "zeppelin-namespace-incarnation";

    let mut created_metadata = ObjectUserMetadata::new();
    created_metadata.insert(metadata_key, "incarnation-01");
    harness
        .store
        .put_if_not_exists_with_user_metadata(
            &key,
            Bytes::from_static(b"m1"),
            "parity",
            &created_metadata,
        )
        .await
        .expect("C7: create with metadata");
    let (_, read) = harness
        .store
        .get_with_object_metadata(&key)
        .await
        .expect("C7: read metadata");
    assert_eq!(
        read.user_metadata.get(metadata_key),
        Some("incarnation-01"),
        "C7: metadata survives a create-only PUT"
    );

    if harness.store.capabilities().conditional_put.is_some() {
        let token = read.version.expect("C7: versioned read");
        let mut updated_metadata = ObjectUserMetadata::new();
        updated_metadata.insert(metadata_key, "incarnation-02");
        harness
            .store
            .put_if_match_with_user_metadata(
                &key,
                Bytes::from_static(b"m2"),
                &token,
                "parity",
                &updated_metadata,
            )
            .await
            .expect("C7: CAS with metadata");
        let (_, reread) = harness
            .store
            .get_with_object_metadata(&key)
            .await
            .expect("C7: re-read metadata");
        assert_eq!(
            reread.user_metadata.get(metadata_key),
            Some("incarnation-02"),
            "C7: metadata survives a conditional PUT"
        );
    }
    harness.cleanup().await;
}

/// C8: `copy_if_not_exists` creates a fresh destination atomically and
/// refuses an existing one. Protects clone / restore-as-clone.
#[tokio::test]
async fn c8_copy_if_not_exists_is_atomic_create() {
    let harness = TestHarness::new().await;
    if !harness.store.capabilities().copy_if_not_exists {
        skip_loudly!("C8", "copy_if_not_exists not declared");
    }
    let src = harness.key("c8/src.bin");
    let dst = harness.key("c8/dst.bin");
    harness
        .store
        .put(&src, Bytes::from_static(b"clone-me"))
        .await
        .expect("C8: seed source");
    harness
        .store
        .copy_if_not_exists(&src, &dst, "parity")
        .await
        .expect("C8: copy to fresh destination");
    let body = harness.store.get(&dst).await.expect("C8: read copy");
    assert_eq!(&body[..], b"clone-me");

    let second = harness.store.copy_if_not_exists(&src, &dst, "parity").await;
    assert!(
        matches!(
            second,
            Err(ZeppelinError::Storage(
                object_store::Error::AlreadyExists { .. }
            ))
        ),
        "C8: copy onto an existing destination must be AlreadyExists-shaped, got {second:?}"
    );
    harness.cleanup().await;
}

/// C9: deleting an absent key reports success on EVERY backend — the plan-02
/// seam normalization. Protects GC drain idempotency.
#[tokio::test]
async fn c9_delete_of_absent_is_success_everywhere() {
    let harness = TestHarness::new().await;
    let absent = harness.key("c9/never-written.bin");
    harness
        .store
        .delete(&absent)
        .await
        .expect("C9: delete of an absent key is success");

    let absent_batch = (0..3)
        .map(|i| harness.key(&format!("c9/absent-{i}.bin")))
        .collect::<Vec<_>>();
    let deleted = harness
        .store
        .delete_many(absent_batch)
        .await
        .expect("C9: delete_many of absent keys is success");
    assert_eq!(deleted, 3, "C9: every absent key counts as deleted");
    harness.cleanup().await;
}

/// C10: the bulk-delete chunk contract — `delete_many` handles a full
/// 1,000-key chunk, refuses an oversized batch loudly, and chunked calls
/// clear 1,100 keys; `delete_prefix_paged` honors its exclusion. Protects
/// GC/manifest bulk paths.
#[tokio::test]
async fn c10_bulk_delete_chunk_contract() {
    let harness = TestHarness::new().await;
    let parent = harness.key("c10");
    let keys: Vec<String> = (0..1100).map(|i| format!("{parent}/k{i:04}.bin")).collect();
    for chunk in keys.chunks(100) {
        let puts = chunk.iter().map(|key| {
            let store = harness.store.clone();
            let key = key.clone();
            async move { store.put(&key, Bytes::from_static(b"x")).await }
        });
        for result in futures::future::join_all(puts).await {
            result.expect("C10: seed put");
        }
    }

    let oversized = harness.store.delete_many(keys.clone()).await;
    assert!(
        matches!(oversized, Err(ZeppelinError::Validation(_))),
        "C10: an oversized batch must be a loud Validation error, got {oversized:?}"
    );

    let mut deleted = 0usize;
    for chunk in keys.chunks(1000) {
        deleted += harness
            .store
            .delete_many(chunk.to_vec())
            .await
            .expect("C10: chunked delete");
    }
    assert_eq!(deleted, 1100, "C10: chunked calls clear every key");
    let leftover = harness
        .store
        .list_prefix_meta(&parent)
        .await
        .expect("C10: list after delete");
    assert!(
        leftover.is_empty(),
        "C10: prefix must be empty, {} keys left",
        leftover.len()
    );

    let excluded = format!("{parent}/meta.json");
    for name in ["a.bin", "b.bin", "c.bin"] {
        harness
            .store
            .put(&format!("{parent}/{name}"), Bytes::from_static(b"x"))
            .await
            .expect("C10: seed paged");
    }
    harness
        .store
        .put(&excluded, Bytes::from_static(b"tombstone"))
        .await
        .expect("C10: seed excluded");
    let outcome = harness
        .store
        .delete_prefix_paged(&parent, Some(&excluded), std::time::Duration::from_secs(30))
        .await
        .expect("C10: delete_prefix_paged");
    assert!(outcome.complete, "C10: generous budget completes the pass");
    assert!(
        harness
            .store
            .exists(&excluded)
            .await
            .expect("C10: exists check"),
        "C10: the excluded key must survive"
    );
    let survivors = harness
        .store
        .list_prefix_meta(&parent)
        .await
        .expect("C10: list survivors");
    assert_eq!(
        survivors.len(),
        1,
        "C10: only the exclusion survives, got {survivors:?}"
    );
    harness.cleanup().await;
}

/// C11: `list_common_prefixes("")` discovers top-level prefixes. Protects
/// boot namespace discovery.
#[tokio::test]
async fn c11_top_level_prefix_discovery() {
    let harness = TestHarness::new().await;
    harness
        .store
        .put(
            &harness.key("c11/meta.json"),
            Bytes::from_static(b"discover-me"),
        )
        .await
        .expect("C11: seed");
    let prefixes = harness
        .store
        .list_common_prefixes("")
        .await
        .expect("C11: list_common_prefixes");
    assert!(
        prefixes
            .iter()
            .any(|prefix| prefix.trim_end_matches('/') == harness.prefix),
        "C11: the harness prefix must be discoverable, got {prefixes:?}"
    );
    harness.cleanup().await;
}

/// C12: a CAS-unsupported backend fails loudly on `put_if_match` and never
/// silently overwrites. Protects the fail-closed contract.
#[tokio::test]
async fn c12_cas_unsupported_backend_fails_loudly() {
    let harness = TestHarness::new().await;
    if harness.store.capabilities().conditional_put.is_some() {
        skip_loudly!("C12", "backend declares conditional_put (covered by C3)");
    }
    let key = harness.key("c12/manifest.json");
    harness
        .store
        .put(&key, Bytes::from_static(b"v1"))
        .await
        .expect("C12: seed");
    let (_, version) = harness
        .store
        .get_with_meta(&key)
        .await
        .expect("C12: read token");
    let token = version.expect("C12: even CAS-less backends report a version on GET");

    let result = harness
        .store
        .put_if_match(&key, Bytes::from_static(b"v2"), &token, "parity")
        .await;
    assert!(
        matches!(result, Err(ZeppelinError::Storage(_))),
        "C12: CAS on an unsupported backend must fail loudly, got {result:?}"
    );
    let body = harness.store.get(&key).await.expect("C12: read back");
    assert_eq!(&body[..], b"v1", "C12: no silent overwrite");
    harness.cleanup().await;
}
