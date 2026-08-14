mod common;

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration as StdDuration, Instant};

use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result as OsResult,
};
use proptest::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use zeppelin::config::{Config, SecurityMode};
use zeppelin::security::{
    canonical_policy_checksum, verify_audit_day, Action, AuditRecord, AuditRuntime,
    DelegationNarrowing, NamespaceId, PolicyStore, SecurityKernel,
};
use zeppelin::storage::ZeppelinStore;
use zeppelin::time::{Clock, TimeSource};

use common::harness::TestHarness;
use common::server::{
    client_with_bearer, create_ns_api, scoped_test_security_store, start_test_server,
    start_test_server_full, start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer,
    start_test_server_full_with_security,
    start_test_server_full_without_rate_limit_override_and_admin_bearer, test_admin_bearer,
    TestSecurity,
};

#[derive(Debug)]
struct AdjustableDelegationClock(Mutex<chrono::DateTime<chrono::Utc>>);

impl AdjustableDelegationClock {
    fn advance(&self, duration: Duration) {
        let mut now = self
            .0
            .lock()
            .unwrap_or_else(|_| panic!("delegation test clock poisoned"));
        *now += duration;
    }

    fn set(&self, now: DateTime<Utc>) {
        *self
            .0
            .lock()
            .unwrap_or_else(|_| panic!("delegation test clock poisoned")) = now;
    }
}

impl TimeSource for AdjustableDelegationClock {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        *self
            .0
            .lock()
            .unwrap_or_else(|_| panic!("delegation test clock poisoned"))
    }
}

#[derive(Deserialize)]
struct DelegatedTokenTimeBounds {
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

fn delegated_token_time_bounds(token: &str) -> (DateTime<Utc>, DateTime<Utc>) {
    let (encoded_payload, _) = token
        .strip_prefix("zpt1_")
        .expect("delegated token prefix")
        .split_once('.')
        .expect("delegated token payload and signature");
    let payload = URL_SAFE_NO_PAD
        .decode(encoded_payload)
        .expect("delegated token payload must be base64url");
    let bounds: DelegatedTokenTimeBounds =
        serde_json::from_slice(&payload).expect("delegated token payload must be JSON");
    (bounds.issued_at, bounds.expires_at)
}

fn delegation_signing_key(seed_byte: u8) -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().expect("delegation signing-key fixture");
    std::fs::write(file.path(), format!("{seed_byte:02x}").repeat(32))
        .expect("write delegation signing-key fixture");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(0o600))
            .expect("restrict delegation signing-key fixture");
    }
    file
}

#[derive(Debug)]
struct FaultMatchingSignerOps {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    remaining_gets: AtomicUsize,
    remaining_puts: AtomicUsize,
    injected_gets: Arc<AtomicUsize>,
    injected_puts: Arc<AtomicUsize>,
}

impl fmt::Display for FaultMatchingSignerOps {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultMatchingSignerOps({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FaultMatchingSignerOps {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let should_fail = location.as_ref().contains(&self.needle)
            && self
                .remaining_puts
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    (remaining > 0).then(|| remaining - 1)
                })
                .is_ok();
        if should_fail {
            self.injected_puts.fetch_add(1, Ordering::SeqCst);
            return Err(object_store::Error::Generic {
                store: "signer_composition_test",
                source: Box::new(std::io::Error::other(format!(
                    "injected signer PUT failure for {location}"
                ))),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        let should_fail = location.as_ref().contains(&self.needle)
            && self
                .remaining_gets
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    (remaining > 0).then(|| remaining - 1)
                })
                .is_ok();
        if should_fail {
            self.injected_gets.fetch_add(1, Ordering::SeqCst);
            return Err(object_store::Error::Generic {
                store: "signer_composition_test",
                source: Box::new(std::io::Error::other(format!(
                    "injected signer GET failure for {location}"
                ))),
            });
        }
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn fault_signer_ops(
    store: &ZeppelinStore,
    get_failures: usize,
    put_failures: usize,
) -> (ZeppelinStore, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let injected_gets = Arc::new(AtomicUsize::new(0));
    let injected_puts = Arc::new(AtomicUsize::new(0));
    let faulted = FaultMatchingSignerOps {
        inner: store.inner(),
        needle: "_security/signers/".to_string(),
        remaining_gets: AtomicUsize::new(get_failures),
        remaining_puts: AtomicUsize::new(put_failures),
        injected_gets: Arc::clone(&injected_gets),
        injected_puts: Arc::clone(&injected_puts),
    };
    (
        ZeppelinStore::new(Arc::new(faulted)),
        injected_gets,
        injected_puts,
    )
}

fn fail_signer_gets(store: &ZeppelinStore, failures: usize) -> (ZeppelinStore, Arc<AtomicUsize>) {
    let (store, injected_gets, _) = fault_signer_ops(store, failures, 0);
    (store, injected_gets)
}

#[derive(Serialize, Deserialize)]
struct PolicySnapshotWire {
    version: u64,
    created_at: chrono::DateTime<chrono::Utc>,
    created_by: String,
    checksum: String,
    principals: Vec<Value>,
    keys: Vec<Value>,
    grants: Vec<Value>,
}

#[derive(Serialize)]
struct PolicyChecksumWire<'a> {
    version: u64,
    created_at: chrono::DateTime<chrono::Utc>,
    created_by: &'a str,
    principals: &'a [Value],
    keys: &'a [Value],
    grants: &'a [Value],
}

fn recompute_policy_checksum(snapshot: &mut PolicySnapshotWire) {
    let content = PolicyChecksumWire {
        version: snapshot.version,
        created_at: snapshot.created_at,
        created_by: &snapshot.created_by,
        principals: &snapshot.principals,
        keys: &snapshot.keys,
        grants: &snapshot.grants,
    };
    let value = serde_json::to_value(content).expect("legacy policy checksum content must encode");
    snapshot.checksum =
        canonical_policy_checksum(&value).expect("production checksum seam must accept fixture");
}

#[tokio::test]
async fn legacy_all_policy_is_cas_migrated_before_serving() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let mut config = Config::default();
    let _admin_bearer = test_admin_bearer(&mut config);
    let bootstrap_store = PolicyStore::new(store.clone(), true);
    let now = chrono::Utc::now();
    let bootstrap = bootstrap_store
        .load_or_bootstrap(&config.security, now)
        .await
        .expect("Phase 7 bootstrap policy");

    let mut legacy: PolicySnapshotWire = serde_json::from_value(
        serde_json::to_value(bootstrap.snapshot()).expect("bootstrap snapshot must encode"),
    )
    .expect("bootstrap snapshot wire must decode");
    legacy.grants[0]["actions"] = json!({"kind": "all"});
    recompute_policy_checksum(&mut legacy);
    let legacy_checksum = legacy.checksum.clone();
    let legacy_key = format!("_security/policies/{}.json", ulid::Ulid::new());
    store
        .put(
            &legacy_key,
            Bytes::from(serde_json::to_vec(&legacy).expect("legacy snapshot must encode")),
        )
        .await
        .expect("legacy snapshot write");
    store
        .put(
            "_security/heads/policy.json",
            Bytes::from(
                serde_json::to_vec(&json!({
                    "version": legacy.version,
                    "object_key": legacy_key,
                    "checksum": legacy_checksum,
                }))
                .expect("legacy head must encode"),
            ),
        )
        .await
        .expect("legacy head write");

    let left_store = PolicyStore::new(store.clone(), true);
    let right_store = PolicyStore::new(store.clone(), true);
    let (left, right) = tokio::join!(
        left_store.load_or_bootstrap(&config.security, now),
        right_store.load_or_bootstrap(&config.security, now)
    );
    let left = left.expect("left migration contender");
    let right = right.expect("right migration contender");
    assert_eq!(left.head().checksum(), right.head().checksum());
    assert_eq!(left.snapshot().version().get(), legacy.version + 1);
    assert!(!left
        .snapshot()
        .grants()
        .iter()
        .any(|grant| matches!(grant.actions(), zeppelin::security::GrantActions::All)));
    let migrated_grants =
        serde_json::to_value(left.snapshot().grants()).expect("migrated grants must encode");
    let migrated_grants = migrated_grants
        .as_array()
        .expect("migrated grants must be an array");
    assert!(migrated_grants.iter().any(|grant| {
        grant["actions"]["actions"]
            .as_array()
            .is_some_and(|actions| actions.iter().any(|action| action == "SecurityAdminWrite"))
    }));
    assert!(migrated_grants.iter().all(|grant| {
        grant["actions"]["actions"]
            .as_array()
            .is_none_or(|actions| actions.iter().all(|action| action != "CredentialDelegate"))
    }));
    left.snapshot()
        .validate_for_use()
        .expect("migrated snapshot must compile");
    let migration_records = store
        .list_prefix("_security/migrations/phase7-safe-all-v2/")
        .await
        .expect("migration evidence list");
    assert_eq!(migration_records.len(), 1);
}

#[tokio::test]
async fn open_unsafe_with_rbac_config_fails_boot() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let mut config = Config::default();
    config.security.mode = SecurityMode::OpenUnsafe;
    config.security.rbac = true;
    let key = delegation_signing_key(0x31);
    config.security.token_signing_key_path = key.path().to_string_lossy().into_owned();

    let result = SecurityKernel::compose(store, &config.security, Clock::system()).await;
    let error = match result {
        Ok(_) => panic!("rbac policy authority cannot compose under anonymous access"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("security.rbac = true requires security.mode = \"enforced\""),
        "{error}"
    );
}

#[tokio::test]
async fn delegation_signing_key_contract_fails_loud_at_boot() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let mut config = Config::default();
    let admin_bearer = test_admin_bearer(&mut config);
    config.security.rbac = true;

    // An empty signing-key path now means the delegation surface is off, not
    // misconfigured: boot succeeds and minting reports the disabled feature.
    let clock = Clock::system();
    let (keyless_kernel, keyless_adapter) =
        SecurityKernel::compose(store.clone(), &config.security, clock.clone())
            .await
            .expect("keyless boot must compose with delegation disabled");
    let parent = keyless_adapter
        .authenticate_bearer(&format!("Bearer {admin_bearer}"), clock.now())
        .expect("bootstrap administrator must authenticate");
    let narrowing = DelegationNarrowing::new(
        vec![Action::NamespaceRead],
        vec![NamespaceId::new("delegation-disabled-probe".to_string()).unwrap()],
        None,
        "delegation disabled probe".to_string(),
    )
    .expect("probe narrowing must be valid");
    let disabled = keyless_kernel
        .mint_delegated_token(&parent, narrowing, 60, clock.now())
        .expect_err("minting must report the disabled delegation surface");
    assert!(
        disabled
            .to_string()
            .contains("feature is disabled by server configuration: delegation"),
        "{disabled}"
    );
    drop(keyless_kernel);
    drop(keyless_adapter);

    let loose = delegation_signing_key(0x51);
    #[cfg(unix)]
    std::fs::set_permissions(loose.path(), {
        use std::os::unix::fs::PermissionsExt;
        std::fs::Permissions::from_mode(0o644)
    })
    .unwrap();
    config.security.token_signing_key_path = loose.path().to_string_lossy().into_owned();
    let loose_result =
        SecurityKernel::compose(store.clone(), &config.security, Clock::system()).await;
    #[cfg(unix)]
    {
        let loose_error = match loose_result {
            Ok(_) => panic!("delegation boot must reject a group-readable signing key"),
            Err(error) => error,
        };
        assert!(
            loose_error
                .to_string()
                .contains("must have 0600 permissions"),
            "{loose_error}"
        );
    }

    let invalid = delegation_signing_key(0x52);
    std::fs::write(invalid.path(), "not-a-32-byte-hex-seed").unwrap();
    config.security.token_signing_key_path = invalid.path().to_string_lossy().into_owned();
    let invalid_result = SecurityKernel::compose(store, &config.security, Clock::system()).await;
    let invalid_error = match invalid_result {
        Ok(_) => panic!("delegation boot must reject malformed signing material"),
        Err(error) => error,
    };
    assert!(
        invalid_error
            .to_string()
            .contains("invalid delegation signing key"),
        "{invalid_error}"
    );
}

#[tokio::test]
async fn direct_kernel_composition_supports_offline_audit_verification_after_root_ends() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let mut config = Config::default();
    let _admin_bearer = test_admin_bearer(&mut config);
    let key = delegation_signing_key(0x53);
    config.security.token_signing_key_path = key.path().to_string_lossy().into_owned();

    config.security.rbac = true;
    let (kernel, adapter) =
        SecurityKernel::compose(store.clone(), &config.security, Clock::system())
            .await
            .expect("direct kernel composition must publish a delegation signer");

    let (client, runtime) =
        AuditRuntime::start_for_published_signer(store.clone(), StdDuration::from_secs(60))
            .await
            .expect("direct kernel composition must install signing on its input store");
    let node_id = client.node_id().to_string();
    let now = Utc::now();
    client
        .submit_durable(AuditRecord::open_unsafe_boot(now, &node_id))
        .await
        .expect("input-store signer must durably sign audit evidence");
    runtime
        .shutdown()
        .await
        .expect("signed audit writer must seal its chain");

    // Verification uses the published signer inventory after the live signing
    // roots have ended; it must not retain or require either root.
    drop(adapter);
    drop(kernel);
    let verification = verify_audit_day(&store, now.date_naive(), &node_id)
        .await
        .expect("offline signed audit chain must verify from authoritative storage");
    assert!(verification.valid, "{verification:?}");
    assert_eq!(verification.verified_records, 1);

    drop(store);
    harness.cleanup().await;
}

/// Full servers publish signer inventory through a scoped security wrapper but
/// write audit evidence through the raw application wrapper. The durable
/// verifier must retain that explicit inventory view after the security graph
/// ends rather than infer it from the raw audit store.
#[tokio::test]
async fn split_security_inventory_verifies_raw_audit_after_root_ends() {
    let harness = TestHarness::new().await;
    let app_store = harness.store.clone();
    let security_store = scoped_test_security_store(&app_store, &harness.prefix);
    let mut config = Config::default();
    config.security.mode = SecurityMode::OpenUnsafe;
    let key = delegation_signing_key(0x55);
    config.security.token_signing_key_path = key.path().to_string_lossy().into_owned();
    config.security.audit_s3 = true;
    let (kernel, adapter) =
        SecurityKernel::compose(security_store, &config.security, Clock::system())
            .await
            .expect("scoped security kernel must publish its signer inventory");
    kernel
        .install_object_signer(&app_store)
        .expect("raw application store must receive the scoped signer");

    let now = Utc::now();
    let node_id = format!("test-node-{}-split-inventory", harness.prefix);
    let (client, runtime) = AuditRuntime::start_at(
        app_store.clone(),
        node_id.clone(),
        StdDuration::from_secs(60),
        now,
    )
    .await
    .expect("raw application audit writer must start");
    client
        .submit_durable(AuditRecord::open_unsafe_boot(now, &node_id))
        .await
        .expect("raw application audit record must become durable");
    runtime
        .shutdown()
        .await
        .expect("raw application audit writer must seal its chain");

    drop(adapter);
    drop(kernel);
    let verification = verify_audit_day(&app_store, now.date_naive(), &node_id)
        .await
        .expect("raw application audit verification must execute after roots end");
    assert!(verification.valid, "{verification:?}");
    assert_eq!(verification.verified_records, 1);

    drop(app_store);
    harness.cleanup().await;
}

/// A crash replacement composes before Rust drops the old server graph. The
/// same-key signer slot must move to the replacement root before that drop.
#[tokio::test]
async fn overlapping_kernel_composition_rebinds_same_node_signer_to_replacement_root() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let mut config = Config::default();
    let _admin_bearer = test_admin_bearer(&mut config);
    let key = delegation_signing_key(0x54);
    config.security.token_signing_key_path = key.path().to_string_lossy().into_owned();
    config.security.rbac = true;

    let (old_kernel, old_adapter) =
        SecurityKernel::compose(store.clone(), &config.security, Clock::system())
            .await
            .expect("original kernel must install its published signer");
    let (replacement_kernel, replacement_adapter) =
        SecurityKernel::compose(store.clone(), &config.security, Clock::system())
            .await
            .expect("replacement kernel must compose before the original drops");

    drop(old_kernel);
    let (_client, runtime) =
        AuditRuntime::start_for_published_signer(store.clone(), StdDuration::from_secs(60))
            .await
            .expect("replacement root must keep the application signer live after handoff");
    runtime
        .shutdown()
        .await
        .expect("replacement signer must seal its empty audit stream");

    // Adapters retain their policy caches independently of the kernel roots.
    // Release both before the harness deletes their scoped authoritative state.
    drop(old_adapter);
    drop(replacement_adapter);
    drop(replacement_kernel);
    drop(store);
    harness.cleanup().await;
}

#[tokio::test]
async fn replacement_composition_retries_one_signer_get_but_fails_loud_when_persistent() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let mut config = Config::default();
    let _admin_bearer = test_admin_bearer(&mut config);
    let key = delegation_signing_key(0x56);
    config.security.token_signing_key_path = key.path().to_string_lossy().into_owned();
    config.security.rbac = true;

    let (original_kernel, original_adapter) =
        SecurityKernel::compose(store.clone(), &config.security, Clock::system())
            .await
            .expect("original kernel must publish signer state before replacement");

    let (one_shot_store, one_shot_injected) = fail_signer_gets(&store, 1);
    let (replacement_kernel, replacement_adapter) =
        SecurityKernel::compose(one_shot_store, &config.security, Clock::system())
            .await
            .expect("one transient signer GET fault must not abort replacement composition");
    assert_eq!(one_shot_injected.load(Ordering::SeqCst), 1);

    let (persistent_store, persistent_injected) = fail_signer_gets(&store, usize::MAX);
    let persistent =
        SecurityKernel::compose(persistent_store, &config.security, Clock::system()).await;
    let error = match persistent {
        Ok(_) => panic!("persistent signer GET failure must abort composition"),
        Err(error) => error,
    };
    let attempts = persistent_injected.load(Ordering::SeqCst);
    assert_eq!(
        attempts, 3,
        "persistent signer GET failure must stop at the composition retry bound"
    );
    assert!(
        error.to_string().contains("injected signer GET failure"),
        "{error}"
    );

    drop(replacement_adapter);
    drop(replacement_kernel);
    drop(original_adapter);
    drop(original_kernel);
    drop(store);
    harness.cleanup().await;
}

#[tokio::test]
async fn replacement_composition_avoids_idempotent_signer_writes_but_new_signers_fail_loud() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let mut config = Config::default();
    let _admin_bearer = test_admin_bearer(&mut config);
    let key = delegation_signing_key(0x57);
    config.security.token_signing_key_path = key.path().to_string_lossy().into_owned();
    config.security.rbac = true;

    let (original_kernel, original_adapter) =
        SecurityKernel::compose(store.clone(), &config.security, Clock::system())
            .await
            .expect("original kernel must publish signer state before replacement");

    let (write_partitioned_store, _, replacement_puts) = fault_signer_ops(&store, 0, usize::MAX);
    let (replacement_kernel, replacement_adapter) =
        SecurityKernel::compose(write_partitioned_store, &config.security, Clock::system())
            .await
            .expect("replacement must reuse verified signer state during a write partition");
    assert_eq!(
        replacement_puts.load(Ordering::SeqCst),
        0,
        "replacement composition must not rewrite identical signer state"
    );

    let new_key = delegation_signing_key(0x58);
    config.security.token_signing_key_path = new_key.path().to_string_lossy().into_owned();
    let (new_signer_store, _, new_signer_puts) = fault_signer_ops(&store, 0, usize::MAX);
    let new_signer =
        SecurityKernel::compose(new_signer_store, &config.security, Clock::system()).await;
    let error = match new_signer {
        Ok(_) => panic!("a new signer must not compose without durable publication"),
        Err(error) => error,
    };
    assert_eq!(new_signer_puts.load(Ordering::SeqCst), 1);
    assert!(
        error.to_string().contains("injected signer PUT failure"),
        "{error}"
    );

    drop(replacement_adapter);
    drop(replacement_kernel);
    drop(original_adapter);
    drop(original_kernel);
    drop(store);
    harness.cleanup().await;
}

#[tokio::test]
async fn verifier_discovers_signer_published_after_verifier_boot() {
    let harness = TestHarness::new().await;
    let verifier_key = delegation_signing_key(0x41);
    let signer_key = delegation_signing_key(0x42);

    let mut verifier_config = Config::default();
    verifier_config.security.policy_refresh_secs = 1;
    verifier_config.security.token_signing_key_path =
        verifier_key.path().to_string_lossy().into_owned();
    let verifier_store = scoped_test_security_store(&harness.store, &harness.prefix);
    let verifier = start_test_server_full(verifier_store, None, verifier_config, false, None).await;

    let mut signer_config = Config::default();
    signer_config.security.policy_refresh_secs = 1;
    signer_config.security.token_signing_key_path =
        signer_key.path().to_string_lossy().into_owned();
    let signer_store = scoped_test_security_store(&harness.store, &harness.prefix);
    let signer = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        signer_store,
        None,
        signer_config,
        false,
        None,
        100 * 1024 * 1024,
        &verifier.admin_bearer,
    )
    .await;
    let admin = client_with_bearer(&signer.admin_bearer);
    let namespace = create_ns_api(&admin, &signer.base_url, 2).await;

    let grant = admin
        .post(format!("{}/v1/security/grants", signer.base_url))
        .json(&json!({
            "principal_id": "zpk1_test_admin",
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": ["CredentialDelegate"]}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());

    let minted = admin
        .post(format!("{}/v1/security/tokens", signer.base_url))
        .json(&json!({
            "actions": ["Query"],
            "namespaces": [namespace],
            "purpose": "cross-node signer refresh",
            "expires_in_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted.status(), 201, "{}", minted.text().await.unwrap());
    let token = minted.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();
    let agent = client_with_bearer(&token);

    let deadline = Instant::now() + StdDuration::from_secs(3);
    loop {
        let response = agent
            .post(format!(
                "{}/v1/namespaces/{namespace}/query",
                verifier.base_url
            ))
            .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
            .send()
            .await
            .unwrap();
        if response.status() == 200 {
            break;
        }
        assert_eq!(response.status(), 401, "{}", response.text().await.unwrap());
        assert!(
            Instant::now() <= deadline,
            "verifier did not discover the immutable signer within 2x refresh"
        );
        tokio::time::sleep(StdDuration::from_millis(25)).await;
    }

    signer.shutdown().await;
    verifier.shutdown().await;
}

#[tokio::test]
async fn concurrent_signer_registration_enforces_inventory_cap_atomically() {
    let harness = TestHarness::new().await;
    let mut base_config = Config::default();
    let _admin_bearer = test_admin_bearer(&mut base_config);
    base_config.security.rbac = true;
    let signing_keys = (0_u8..34)
        .map(|index| delegation_signing_key(0x60_u8 + index))
        .collect::<Vec<_>>();

    for (index, signing_key) in signing_keys.iter().take(31).enumerate() {
        let mut config = base_config.clone();
        config.security.token_signing_key_path = signing_key.path().to_string_lossy().into_owned();
        SecurityKernel::compose(
            scoped_test_security_store(&harness.store, &harness.prefix),
            &config.security,
            Clock::system(),
        )
        .await
        .unwrap_or_else(|error| panic!("signer {index} must register below the cap: {error}"));
    }

    let mut left_config = base_config.clone();
    left_config.security.token_signing_key_path =
        signing_keys[31].path().to_string_lossy().into_owned();
    let mut right_config = base_config.clone();
    right_config.security.token_signing_key_path =
        signing_keys[32].path().to_string_lossy().into_owned();
    let (left, right) = tokio::join!(
        SecurityKernel::compose(
            scoped_test_security_store(&harness.store, &harness.prefix),
            &left_config.security,
            Clock::system(),
        ),
        SecurityKernel::compose(
            scoped_test_security_store(&harness.store, &harness.prefix),
            &right_config.security,
            Clock::system(),
        )
    );
    let successful_registrations = usize::from(left.is_ok()) + usize::from(right.is_ok());
    assert_eq!(
        successful_registrations,
        1,
        "exactly one contender may claim the final signer slot; left={:?}, right={:?}",
        left.as_ref().err(),
        right.as_ref().err()
    );

    let mut overflow_config = base_config;
    overflow_config.security.token_signing_key_path =
        signing_keys[33].path().to_string_lossy().into_owned();
    let overflow = SecurityKernel::compose(
        scoped_test_security_store(&harness.store, &harness.prefix),
        &overflow_config.security,
        Clock::system(),
    )
    .await;
    let overflow = match overflow {
        Ok(_) => panic!("a distinct signer must not register above the inventory cap"),
        Err(error) => error,
    };
    assert!(
        overflow.to_string().contains("signer inventory is full"),
        "unexpected overflow error: {overflow}"
    );
}

proptest! {
    #[test]
    fn narrowing_effective_scope_never_exceeds_parent(
        parent_action_bits in any::<u32>(),
        narrowed_action_bits in any::<u32>(),
        parent_namespace_bits in any::<u8>(),
        narrowed_namespace_bits in any::<u8>(),
    ) {
        let actions = Action::ALL;
        let namespaces = (0..8)
            .map(|index| NamespaceId::new(format!("delegation-prop-{index}")).unwrap())
            .collect::<Vec<_>>();
        let narrowed_actions = actions
            .into_iter()
            .enumerate()
            .filter(|(index, action)|
                action.is_delegatable() && narrowed_action_bits & (1_u32 << index) != 0)
            .map(|(_, action)| action)
            .collect::<Vec<_>>();
        let narrowed_namespaces = namespaces
            .iter()
            .enumerate()
            .filter(|(index, _)| narrowed_namespace_bits & (1_u8 << index) != 0)
            .map(|(_, namespace)| namespace.clone())
            .collect::<Vec<_>>();
        prop_assume!(!narrowed_actions.is_empty() && !narrowed_namespaces.is_empty());
        let narrowing = DelegationNarrowing::new(
            narrowed_actions,
            narrowed_namespaces,
            None,
            "property narrowing".to_string(),
        ).unwrap();

        for (action_index, action) in actions.into_iter().enumerate() {
            for (namespace_index, namespace) in namespaces.iter().enumerate() {
                let parent_allows = parent_action_bits & (1_u32 << action_index) != 0
                    && parent_namespace_bits & (1_u8 << namespace_index) != 0;
                let effective = narrowing.effective_allows(action, namespace, parent_allows);
                prop_assert_eq!(effective, narrowing.allows(action, namespace) && parent_allows);
                prop_assert!(!effective || parent_allows);
            }
        }
    }
}

#[tokio::test]
async fn mint_narrow_use() {
    let (base_url, _harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let namespace_a = format!("delegation-a-{suffix}");
    let namespace_b = format!("delegation-b-{suffix}");

    for namespace in [&namespace_a, &namespace_b] {
        let response = admin
            .post(format!("{base_url}/v1/namespaces"))
            .json(&json!({"name": namespace, "dimensions": 2}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 201, "{}", response.text().await.unwrap());
    }

    let principal_id = format!("service:delegating-parent:{suffix}");
    let principal = admin
        .post(format!("{base_url}/v1/security/principals"))
        .json(&json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": "delegating-parent"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        principal.status(),
        201,
        "{}",
        principal.text().await.unwrap()
    );

    let key = admin
        .post(format!("{base_url}/v1/security/keys"))
        .json(&json!({
            "principal_id": principal_id,
            "name": "delegating-parent-primary"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(key.status(), 201, "{}", key.text().await.unwrap());
    let key_body: serde_json::Value = key.json().await.unwrap();
    let parent = client_with_bearer(key_body["api_key"].as_str().unwrap());

    for (scope, actions) in [
        (json!({"kind": "global"}), json!(["CredentialDelegate"])),
        (
            json!({"kind": "namespace", "namespace": namespace_a}),
            json!(["Query", "VectorUpsert"]),
        ),
        (
            json!({"kind": "namespace", "namespace": namespace_b}),
            json!(["Query", "VectorUpsert"]),
        ),
    ] {
        let grant = admin
            .post(format!("{base_url}/v1/security/grants"))
            .json(&json!({
                "principal_id": principal_id,
                "scope": scope,
                "actions": {"kind": "selected", "actions": actions}
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());
    }

    let minted = parent
        .post(format!("{base_url}/v1/security/tokens"))
        .json(&json!({
            "actions": ["Query"],
            "namespaces": [namespace_a],
            "purpose": "answer one scoped retrieval task",
            "expires_in_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted.status(), 201, "{}", minted.text().await.unwrap());
    let minted_body: serde_json::Value = minted.json().await.unwrap();
    let token = minted_body["token"].as_str().unwrap();
    assert!(token.starts_with("zpt1_"));
    let agent = client_with_bearer(token);

    let mut tampered_token = token.to_string();
    let replacement = if tampered_token.ends_with('A') {
        'B'
    } else {
        'A'
    };
    tampered_token.pop();
    tampered_token.push(replacement);
    let tampered = client_with_bearer(&tampered_token)
        .post(format!("{base_url}/v1/namespaces/{namespace_a}/query"))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(tampered.status(), 401);
    assert_eq!(
        tampered.json::<serde_json::Value>().await.unwrap()["code"],
        "credential_unknown"
    );

    let query_a = agent
        .post(format!("{base_url}/v1/namespaces/{namespace_a}/query"))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(query_a.status(), 200, "{}", query_a.text().await.unwrap());

    let upsert_a = agent
        .post(format!("{base_url}/v1/namespaces/{namespace_a}/vectors"))
        .json(&json!({"vectors": [{"id": "denied", "values": [1.0, 0.0]}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(upsert_a.status(), 403);

    let query_b = agent
        .post(format!("{base_url}/v1/namespaces/{namespace_b}/query"))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(query_b.status(), 403);
}

#[tokio::test]
async fn mint_beyond_parent_400() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let server = start_test_server_full(store.clone(), None, Config::default(), false, None).await;
    let base_url = server.base_url.clone();
    let admin = client_with_bearer(&server.admin_bearer);
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let namespace_a = format!("delegation-parent-a-{suffix}");
    let namespace_c = format!("delegation-outside-c-{suffix}");
    for namespace in [&namespace_a, &namespace_c] {
        let response = admin
            .post(format!("{base_url}/v1/namespaces"))
            .json(&json!({"name": namespace, "dimensions": 2}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 201, "{}", response.text().await.unwrap());
    }

    let principal_id = format!("service:narrow-parent:{suffix}");
    let principal = admin
        .post(format!("{base_url}/v1/security/principals"))
        .json(&json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": "narrow-parent"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(principal.status(), 201);
    let key = admin
        .post(format!("{base_url}/v1/security/keys"))
        .json(&json!({"principal_id": principal_id, "name": "narrow-parent-key"}))
        .send()
        .await
        .unwrap();
    assert_eq!(key.status(), 201);
    let parent = client_with_bearer(
        key.json::<serde_json::Value>().await.unwrap()["api_key"]
            .as_str()
            .unwrap(),
    );

    for (scope, actions) in [
        (json!({"kind": "global"}), json!(["CredentialDelegate"])),
        (
            json!({"kind": "namespace", "namespace": namespace_a}),
            json!(["Query"]),
        ),
    ] {
        let grant = admin
            .post(format!("{base_url}/v1/security/grants"))
            .json(&json!({
                "principal_id": principal_id,
                "scope": scope,
                "actions": {"kind": "selected", "actions": actions}
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());
    }

    let duplicate_scope = parent
        .post(format!("{base_url}/v1/security/tokens"))
        .json(&json!({
            "actions": ["Query", "Query"],
            "namespaces": [namespace_a, namespace_a],
            "purpose": "duplicates must fail strict parsing",
            "expires_in_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(duplicate_scope.status(), 400);
    assert_eq!(
        duplicate_scope.json::<serde_json::Value>().await.unwrap()["code"],
        "invalid_security_request"
    );

    for request in [
        json!({
            "actions": ["SecurityAdminWrite"],
            "namespaces": [namespace_a],
            "purpose": "control-plane authority cannot fit namespace narrowing",
            "expires_in_secs": 300
        }),
        json!({
            "actions": ["Query"],
            "namespaces": [namespace_a],
            "mandatory_filter": {"op": "and", "filters": []},
            "purpose": "invalid filters fail at mint",
            "expires_in_secs": 300
        }),
    ] {
        let response = parent
            .post(format!("{base_url}/v1/security/tokens"))
            .json(&request)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap()["code"],
            "invalid_security_request"
        );
    }

    for request in [
        json!({
            "actions": ["Query"],
            "namespaces": [namespace_c],
            "purpose": "must not cross namespace scope",
            "expires_in_secs": 300
        }),
        json!({
            "actions": ["NamespaceDelete"],
            "namespaces": [namespace_a],
            "purpose": "must not gain destructive authority",
            "expires_in_secs": 300
        }),
    ] {
        let response = parent
            .post(format!("{base_url}/v1/security/tokens"))
            .json(&request)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["code"], "delegation_scope_exceeds_parent");
        assert!(body.get("token").is_none());
    }

    server.flush_audit().await;
    let mut denied_mints = Vec::new();
    for key in store.list_prefix("_audit/").await.unwrap() {
        let body = store.get(&key).await.unwrap();
        denied_mints.extend(
            String::from_utf8(body.to_vec())
                .unwrap()
                .lines()
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .filter(|record| {
                    record["action"] == "CredentialDelegate"
                        && record["outcome"]["denied"]["reason"] == "action_not_granted"
                }),
        );
    }
    assert_eq!(denied_mints.len(), 2);
    let purposes = denied_mints
        .iter()
        .map(|record| {
            record["params"]["delegation_mint"]["purpose"]
                .as_str()
                .unwrap()
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        purposes,
        std::collections::BTreeSet::from([
            "must not cross namespace scope",
            "must not gain destructive authority",
        ])
    );
    assert!(denied_mints
        .iter()
        .all(|record| record["params"]["delegation_mint"]
            .get("token_id")
            .is_none()));

    server.shutdown().await;
}

#[tokio::test]
async fn expired_token_401_and_backward_clock_jump_does_not_resurrect() {
    let harness = TestHarness::new().await;
    let source = Arc::new(AdjustableDelegationClock(Mutex::new(chrono::Utc::now())));
    let server = start_test_server_full_with_security(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        Clock::from_source(source.clone()),
        TestSecurity::full(),
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&admin, &server.base_url, 2).await;
    let principal_id = format!("service:expiring-parent:{}", uuid::Uuid::new_v4().simple());

    let principal = admin
        .post(format!("{}/v1/security/principals", server.base_url))
        .json(&json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": "expiring-parent"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(principal.status(), 201);
    let key = admin
        .post(format!("{}/v1/security/keys", server.base_url))
        .json(&json!({"principal_id": principal_id, "name": "expiring-parent-key"}))
        .send()
        .await
        .unwrap();
    assert_eq!(key.status(), 201);
    let parent = client_with_bearer(
        key.json::<serde_json::Value>().await.unwrap()["api_key"]
            .as_str()
            .unwrap(),
    );
    for (scope, actions) in [
        (json!({"kind": "global"}), json!(["CredentialDelegate"])),
        (
            json!({"kind": "namespace", "namespace": namespace}),
            json!(["Query"]),
        ),
    ] {
        let grant = admin
            .post(format!("{}/v1/security/grants", server.base_url))
            .json(&json!({
                "principal_id": principal_id,
                "scope": scope,
                "actions": {"kind": "selected", "actions": actions}
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());
    }

    let minted = parent
        .post(format!("{}/v1/security/tokens", server.base_url))
        .json(&json!({
            "actions": ["Query"],
            "namespaces": [namespace],
            "purpose": "short-lived retrieval",
            "expires_in_secs": 30
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted.status(), 201, "{}", minted.text().await.unwrap());
    let old_mint = minted.json::<serde_json::Value>().await.unwrap();
    let token = old_mint["token"].as_str().unwrap().to_string();
    let old_response_expires_at = old_mint["expires_at"]
        .as_str()
        .expect("mint response expires_at")
        .parse::<DateTime<Utc>>()
        .expect("mint response expires_at must be RFC 3339");
    let (old_issued_at, old_expires_at) = delegated_token_time_bounds(&token);
    assert_eq!(old_response_expires_at, old_expires_at);
    assert_eq!(
        old_expires_at - old_issued_at,
        Duration::seconds(30),
        "old token must retain its requested signed TTL"
    );
    let agent = client_with_bearer(&token);

    source.advance(Duration::milliseconds(1_500));
    let first_observation = agent
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            server.base_url
        ))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        first_observation.status(),
        200,
        "{}",
        first_observation.text().await.unwrap()
    );

    // Drive expiry through the injected clock rather than a narrow real-time
    // sleep, using an explicit source instant beyond the signed expiry.
    source.set(old_expires_at + Duration::seconds(60));
    let expired_before_backjump = agent
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            server.base_url
        ))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(expired_before_backjump.status(), 401);
    assert_eq!(
        expired_before_backjump
            .json::<serde_json::Value>()
            .await
            .unwrap()["code"],
        "credential_expired"
    );

    let source_inside_old_validity = old_issued_at
        + Duration::milliseconds((old_expires_at - old_issued_at).num_milliseconds() / 2);
    assert!(old_issued_at < source_inside_old_validity);
    assert!(source_inside_old_validity < old_expires_at);
    // Return to a source instant proven strictly inside the old token's signed
    // validity interval. A naive verifier would accept it, so this 401 proves
    // the verifier's monotonic floor prevents resurrection.
    source.set(source_inside_old_validity);
    let old_after_backjump = agent
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            server.base_url
        ))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(old_after_backjump.status(), 401);
    assert_eq!(
        old_after_backjump
            .json::<serde_json::Value>()
            .await
            .unwrap()["code"],
        "credential_expired"
    );

    let minted_after_backjump = parent
        .post(format!("{}/v1/security/tokens", server.base_url))
        .json(&json!({
            "actions": ["Query"],
            "namespaces": [namespace],
            "purpose": "mint uses the verifier monotonic floor",
            "expires_in_secs": 30
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted_after_backjump.status(), 201);
    let fresh_mint = minted_after_backjump
        .json::<serde_json::Value>()
        .await
        .unwrap();
    let fresh_token = fresh_mint["token"].as_str().unwrap().to_string();
    let response_expires_at = fresh_mint["expires_at"]
        .as_str()
        .expect("mint response expires_at")
        .parse::<DateTime<Utc>>()
        .expect("mint response expires_at must be RFC 3339");
    let (fresh_issued_at, fresh_expires_at) = delegated_token_time_bounds(&fresh_token);
    assert_eq!(response_expires_at, fresh_expires_at);
    assert_eq!(
        fresh_expires_at - fresh_issued_at,
        Duration::seconds(30),
        "fresh token must retain its requested signed TTL"
    );
    let fresh_agent = client_with_bearer(&fresh_token);
    let immediate = fresh_agent
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            server.base_url
        ))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        immediate.status(),
        200,
        "{}",
        immediate.text().await.unwrap()
    );

    // A fresh token minted while the injected source is behind the verifier's
    // floor starts valid, then an explicit source instant beyond its signed
    // expiry expires it without depending on scheduler timing.
    source.set(fresh_expires_at + Duration::seconds(60));
    let expired_after_forward_jump = fresh_agent
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            server.base_url
        ))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(expired_after_forward_jump.status(), 401);
    assert_eq!(
        expired_after_forward_jump
            .json::<serde_json::Value>()
            .await
            .unwrap()["code"],
        "credential_expired"
    );

    let source_inside_fresh_validity = fresh_issued_at
        + Duration::milliseconds((fresh_expires_at - fresh_issued_at).num_milliseconds() / 2);
    assert!(fresh_issued_at < source_inside_fresh_validity);
    assert!(source_inside_fresh_validity < fresh_expires_at);
    source.set(source_inside_fresh_validity);
    let first_use_after_backjump = fresh_agent
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            server.base_url
        ))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(first_use_after_backjump.status(), 401);
    assert_eq!(
        first_use_after_backjump
            .json::<serde_json::Value>()
            .await
            .unwrap()["code"],
        "credential_expired"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn parent_revocation_kills_tokens_on_second_node_within_freshness_bound() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let mut config = Config::default();
    config.security.policy_refresh_secs = 1;
    let writer = start_test_server_full(store.clone(), None, config.clone(), false, None).await;
    let admin = client_with_bearer(&writer.admin_bearer);
    let namespace = create_ns_api(&admin, &writer.base_url, 2).await;
    let principal_id = format!(
        "service:revocable-delegation:{}",
        uuid::Uuid::new_v4().simple()
    );

    let principal = admin
        .post(format!("{}/v1/security/principals", writer.base_url))
        .json(&json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": "revocable-delegation"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(principal.status(), 201);
    let key = admin
        .post(format!("{}/v1/security/keys", writer.base_url))
        .json(&json!({
            "principal_id": principal_id,
            "name": "revocable-delegation-key"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(key.status(), 201);
    let key = key.json::<serde_json::Value>().await.unwrap();
    let parent_key_id = key["key_id"].as_str().unwrap().to_string();
    let parent = client_with_bearer(key["api_key"].as_str().unwrap());
    for (scope, actions) in [
        (json!({"kind": "global"}), json!(["CredentialDelegate"])),
        (
            json!({"kind": "namespace", "namespace": namespace}),
            json!(["Query"]),
        ),
    ] {
        let grant = admin
            .post(format!("{}/v1/security/grants", writer.base_url))
            .json(&json!({
                "principal_id": principal_id,
                "scope": scope,
                "actions": {"kind": "selected", "actions": actions}
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());
    }
    let minted = parent
        .post(format!("{}/v1/security/tokens", writer.base_url))
        .json(&json!({
            "actions": ["Query"],
            "namespaces": [namespace],
            "purpose": "revocation-bound test",
            "expires_in_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted.status(), 201, "{}", minted.text().await.unwrap());
    let token = minted.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();
    let agent = client_with_bearer(&token);

    let reader = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        store,
        None,
        config,
        false,
        None,
        100 * 1024 * 1024,
        &writer.admin_bearer,
    )
    .await;
    let before = agent
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            reader.base_url
        ))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(before.status(), 200, "{}", before.text().await.unwrap());

    let revoked = admin
        .delete(format!(
            "{}/v1/security/keys/{parent_key_id}",
            writer.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(revoked.status(), 200, "{}", revoked.text().await.unwrap());

    let deadline = Instant::now() + StdDuration::from_secs(2);
    loop {
        let response = agent
            .post(format!(
                "{}/v1/namespaces/{namespace}/query",
                reader.base_url
            ))
            .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
            .send()
            .await
            .unwrap();
        assert!(
            Instant::now() <= deadline,
            "delegated credential survived parent key revocation beyond 2x policy refresh"
        );
        if response.status() == 401 {
            assert_eq!(
                response.json::<serde_json::Value>().await.unwrap()["code"],
                "credential_unknown"
            );
            break;
        }
        assert_eq!(response.status(), 200);
        tokio::time::sleep(StdDuration::from_millis(50)).await;
    }

    writer.shutdown().await;
    reader.shutdown().await;
}

#[tokio::test]
async fn parent_grant_removal_rechecks_current_authority() {
    let (base_url, _harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_ns_api(&admin, &base_url, 2).await;
    let principal_id = format!(
        "service:grant-rechecked-delegation:{}",
        uuid::Uuid::new_v4().simple()
    );
    let principal = admin
        .post(format!("{base_url}/v1/security/principals"))
        .json(&json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": "grant-rechecked-delegation"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        principal.status(),
        201,
        "{}",
        principal.text().await.unwrap()
    );
    let key = admin
        .post(format!("{base_url}/v1/security/keys"))
        .json(&json!({
            "principal_id": principal_id,
            "name": "grant-rechecked-delegation-key"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(key.status(), 201, "{}", key.text().await.unwrap());
    let key = key.json::<serde_json::Value>().await.unwrap();
    let parent = client_with_bearer(key["api_key"].as_str().unwrap());
    for (scope, actions) in [
        (json!({"kind": "global"}), json!(["CredentialDelegate"])),
        (
            json!({"kind": "namespace", "namespace": namespace}),
            json!(["Query"]),
        ),
    ] {
        let grant = admin
            .post(format!("{base_url}/v1/security/grants"))
            .json(&json!({
                "principal_id": principal_id,
                "scope": scope,
                "actions": {"kind": "selected", "actions": actions}
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());
    }
    let minted = parent
        .post(format!("{base_url}/v1/security/tokens"))
        .json(&json!({
            "actions": ["Query"],
            "namespaces": [namespace],
            "purpose": "current authority test",
            "expires_in_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted.status(), 201, "{}", minted.text().await.unwrap());
    let token = minted.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();
    let agent = client_with_bearer(&token);
    let before = agent
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(before.status(), 200, "{}", before.text().await.unwrap());

    let removed = admin
        .delete(format!("{base_url}/v1/security/grants"))
        .json(&json!({
            "principal_id": principal_id,
            "scope": {"kind": "namespace", "namespace": namespace},
            "actions": {"kind": "selected", "actions": ["Query"]}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(removed.status(), 200, "{}", removed.text().await.unwrap());
    let after = agent
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(after.status(), 403);
    assert_eq!(
        after.json::<serde_json::Value>().await.unwrap()["code"],
        "forbidden"
    );
}

#[tokio::test]
async fn token_filter_intersects_parent_filter() {
    let (base_url, _harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_ns_api(&admin, &base_url, 2).await;
    let principal_id = format!(
        "service:filtered-delegation:{}",
        uuid::Uuid::new_v4().simple()
    );
    assert_eq!(
        admin
            .post(format!("{base_url}/v1/security/principals"))
            .json(&json!({
                "principal_id": principal_id,
                "kind": "service",
                "display_name": "filtered-delegation"
            }))
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    let key = admin
        .post(format!("{base_url}/v1/security/keys"))
        .json(&json!({"principal_id": principal_id, "name": "filtered-key"}))
        .send()
        .await
        .unwrap();
    assert_eq!(key.status(), 201);
    let parent = client_with_bearer(
        key.json::<serde_json::Value>().await.unwrap()["api_key"]
            .as_str()
            .unwrap(),
    );
    for body in [
        json!({
            "principal_id": principal_id,
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": ["CredentialDelegate"]}
        }),
        json!({
            "principal_id": principal_id,
            "scope": {"kind": "namespace", "namespace": namespace},
            "actions": {"kind": "selected", "actions": ["Query"]},
            "mandatory_filter": {"op": "eq", "field": "tenant", "value": "a"}
        }),
    ] {
        let grant = admin
            .post(format!("{base_url}/v1/security/grants"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());
    }
    let upsert = admin
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [
                {"id": "a-west", "values": [1.0, 0.0], "attributes": {"tenant": "a", "region": "west"}},
                {"id": "a-east", "values": [1.0, 0.0], "attributes": {"tenant": "a", "region": "east"}},
                {"id": "b-west", "values": [1.0, 0.0], "attributes": {"tenant": "b", "region": "west"}}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(upsert.status(), 200, "{}", upsert.text().await.unwrap());

    let minted = parent
        .post(format!("{base_url}/v1/security/tokens"))
        .json(&json!({
            "actions": ["Query"],
            "namespaces": [namespace],
            "mandatory_filter": {"op": "eq", "field": "region", "value": "west"},
            "purpose": "tenant and region scoped retrieval",
            "expires_in_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted.status(), 201, "{}", minted.text().await.unwrap());
    let token = minted.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();
    let response = client_with_bearer(&token)
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 10}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    let body: serde_json::Value = response.json().await.unwrap();
    let ids = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["a-west"]);
}

#[tokio::test]
async fn no_chained_minting() {
    let (base_url, _harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_ns_api(&admin, &base_url, 2).await;
    let principal_id = format!("service:no-chain-parent:{}", uuid::Uuid::new_v4().simple());
    assert_eq!(
        admin
            .post(format!("{base_url}/v1/security/principals"))
            .json(&json!({
                "principal_id": principal_id,
                "kind": "service",
                "display_name": "no-chain-parent"
            }))
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    let key = admin
        .post(format!("{base_url}/v1/security/keys"))
        .json(&json!({"principal_id": principal_id, "name": "no-chain-key"}))
        .send()
        .await
        .unwrap();
    assert_eq!(key.status(), 201);
    let parent = client_with_bearer(
        key.json::<serde_json::Value>().await.unwrap()["api_key"]
            .as_str()
            .unwrap(),
    );
    for body in [
        json!({
            "principal_id": principal_id,
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": ["CredentialDelegate"]}
        }),
        json!({
            "principal_id": principal_id,
            "scope": {"kind": "namespace", "namespace": namespace},
            "actions": {"kind": "selected", "actions": ["Query"]}
        }),
    ] {
        let grant = admin
            .post(format!("{base_url}/v1/security/grants"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());
    }
    let minted = parent
        .post(format!("{base_url}/v1/security/tokens"))
        .json(&json!({
            "actions": ["Query"],
            "namespaces": [namespace],
            "purpose": "one generation only",
            "expires_in_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted.status(), 201, "{}", minted.text().await.unwrap());
    let token = minted.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();

    let chained = client_with_bearer(&token)
        .post(format!("{base_url}/v1/security/tokens"))
        .json(&json!({
            "actions": ["Query"],
            "namespaces": [namespace],
            "purpose": "must be rejected",
            "expires_in_secs": 60
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(chained.status(), 403);
    assert_eq!(
        chained.json::<serde_json::Value>().await.unwrap()["code"],
        "forbidden"
    );
}

#[tokio::test]
async fn approval_two_person() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_ns_api(&admin, &base_url, 2).await;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let parent_id = format!("service:approval-parent:{suffix}");
    let approver_id = format!("human:approval-reviewer:{suffix}");

    let mut credentials = Vec::new();
    for (principal_id, kind, display_name) in [
        (&parent_id, "service", "approval-parent"),
        (&approver_id, "human", "approval-reviewer"),
    ] {
        let principal = admin
            .post(format!("{base_url}/v1/security/principals"))
            .json(&json!({
                "principal_id": principal_id,
                "kind": kind,
                "display_name": display_name
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(
            principal.status(),
            201,
            "{}",
            principal.text().await.unwrap()
        );
        let key = admin
            .post(format!("{base_url}/v1/security/keys"))
            .json(&json!({
                "principal_id": principal_id,
                "name": format!("{display_name}-key")
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(key.status(), 201, "{}", key.text().await.unwrap());
        credentials.push(
            key.json::<serde_json::Value>().await.unwrap()["api_key"]
                .as_str()
                .unwrap()
                .to_string(),
        );
    }
    let parent_bearer = credentials[0].clone();
    let approver_bearer = credentials[1].clone();
    let parent = client_with_bearer(&parent_bearer);

    for body in [
        json!({
            "principal_id": parent_id,
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": ["CredentialDelegate"]}
        }),
        json!({
            "principal_id": parent_id,
            "scope": {"kind": "namespace", "namespace": namespace},
            "actions": {"kind": "selected", "actions": ["NamespaceDelete"]},
            "require_approval": ["NamespaceDelete"]
        }),
        json!({
            "principal_id": approver_id,
            "scope": {"kind": "namespace", "namespace": namespace},
            "actions": {"kind": "selected", "actions": ["NamespaceDelete"]}
        }),
    ] {
        let grant = admin
            .post(format!("{base_url}/v1/security/grants"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());
    }

    let minted = parent
        .post(format!("{base_url}/v1/security/tokens"))
        .json(&json!({
            "actions": ["NamespaceDelete"],
            "namespaces": [namespace],
            "purpose": "delete only after independent approval",
            "expires_in_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted.status(), 201, "{}", minted.text().await.unwrap());
    let token = minted.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();
    let agent = client_with_bearer(&token);

    let missing = agent
        .delete(format!("{base_url}/v1/namespaces/{namespace}"))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 403);
    assert_eq!(
        missing.json::<serde_json::Value>().await.unwrap()["code"],
        "approval_required"
    );

    let malformed_request_id = format!("approval-malformed-{suffix}");
    let malformed = agent
        .delete(format!("{base_url}/v1/namespaces/{namespace}"))
        .header("X-Request-Id", &malformed_request_id)
        .header("X-Zeppelin-Approval", "not-a-zpk1-credential")
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), 403);
    assert_eq!(
        malformed.json::<serde_json::Value>().await.unwrap()["code"],
        "approval_required"
    );

    let parent_self_approval = agent
        .delete(format!("{base_url}/v1/namespaces/{namespace}"))
        .header("X-Zeppelin-Approval", &parent_bearer)
        .send()
        .await
        .unwrap();
    assert_eq!(parent_self_approval.status(), 403);
    assert_eq!(
        parent_self_approval
            .json::<serde_json::Value>()
            .await
            .unwrap()["code"],
        "approval_required"
    );

    let approved = agent
        .delete(format!("{base_url}/v1/namespaces/{namespace}"))
        .header("X-Zeppelin-Approval", &approver_bearer)
        .send()
        .await
        .unwrap();
    assert_eq!(approved.status(), 202, "{}", approved.text().await.unwrap());

    let mut records = Vec::new();
    for key in harness.store.list_prefix("_audit/").await.unwrap() {
        if !key.contains(&harness.prefix) {
            continue;
        }
        let body = harness.store.get(&key).await.unwrap();
        records.extend(
            String::from_utf8(body.to_vec())
                .unwrap()
                .lines()
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap()),
        );
    }
    let deletion = records
        .iter()
        .find(|record| {
            record["action"] == "NamespaceDelete"
                && record["outcome"] == serde_json::Value::String("success".to_string())
        })
        .expect("approved delegated deletion audit record");
    assert!(deletion["principal_id"]
        .as_str()
        .unwrap()
        .starts_with("zdt1_"));
    assert_eq!(deletion["principal_kind"], "agent");
    assert_eq!(deletion["delegation_parent"], parent_id);
    assert_eq!(deletion["approval_principal_id"], approver_id);
    assert!(records.iter().any(|record| {
        record["request_id"] == malformed_request_id
            && record["action"] == "NamespaceDelete"
            && record["outcome"]["authn_failed"]["reason"] == "credential_unknown"
    }));
}

#[tokio::test]
async fn approval_constraints_narrow_delegated_vector_delete() {
    let (base_url, _harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_ns_api(&admin, &base_url, 2).await;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let parent_id = format!("service:delete-parent:{suffix}");
    let approver_id = format!("human:delete-approver:{suffix}");
    let mut credentials = Vec::new();
    for (principal_id, kind) in [(&parent_id, "service"), (&approver_id, "human")] {
        let principal = admin
            .post(format!("{base_url}/v1/security/principals"))
            .json(&json!({
                "principal_id": principal_id,
                "kind": kind,
                "display_name": principal_id
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(
            principal.status(),
            201,
            "{}",
            principal.text().await.unwrap()
        );
        let key = admin
            .post(format!("{base_url}/v1/security/keys"))
            .json(&json!({"principal_id": principal_id, "name": "delete-key"}))
            .send()
            .await
            .unwrap();
        assert_eq!(key.status(), 201, "{}", key.text().await.unwrap());
        credentials.push(
            key.json::<serde_json::Value>().await.unwrap()["api_key"]
                .as_str()
                .unwrap()
                .to_string(),
        );
    }
    for body in [
        json!({
            "principal_id": parent_id,
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": ["CredentialDelegate"]}
        }),
        json!({
            "principal_id": parent_id,
            "scope": {"kind": "namespace", "namespace": namespace},
            "actions": {"kind": "selected", "actions": ["VectorDelete"]}
        }),
        json!({
            "principal_id": approver_id,
            "scope": {"kind": "namespace", "namespace": namespace},
            "actions": {"kind": "selected", "actions": ["VectorDelete"]},
            "mandatory_filter": {"op": "eq", "field": "group", "value": "a"}
        }),
    ] {
        let grant = admin
            .post(format!("{base_url}/v1/security/grants"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());
    }
    let upsert = admin
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [
                {"id": "approved-row", "values": [1.0, 0.0], "attributes": {"group": "a"}},
                {"id": "outside-approval", "values": [0.9, 0.1], "attributes": {"group": "b"}}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(upsert.status(), 200, "{}", upsert.text().await.unwrap());
    let parent = client_with_bearer(&credentials[0]);
    let minted = parent
        .post(format!("{base_url}/v1/security/tokens"))
        .json(&json!({
            "actions": ["VectorDelete"],
            "namespaces": [namespace],
            "purpose": "delete only inside independent approver slice",
            "expires_in_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted.status(), 201, "{}", minted.text().await.unwrap());
    let token = minted.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();
    let agent = client_with_bearer(&token);
    let deletion = agent
        .delete(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .header("X-Zeppelin-Approval", &credentials[1])
        .json(&json!({"ids": ["approved-row", "outside-approval"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(deletion.status(), 204, "{}", deletion.text().await.unwrap());

    let remaining = admin
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&json!({
            "vector": [1.0, 0.0],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        remaining.status(),
        200,
        "{}",
        remaining.text().await.unwrap()
    );
    let remaining = remaining.json::<serde_json::Value>().await.unwrap();
    let remaining_ids = remaining["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|result| result["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(remaining_ids, vec!["outside-approval"]);
}

#[tokio::test]
async fn agent_kind_in_audit_and_rate_limits() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let writer = start_test_server_full(store.clone(), None, Config::default(), false, None).await;
    let admin = client_with_bearer(&writer.admin_bearer);
    let namespace = create_ns_api(&admin, &writer.base_url, 2).await;
    let principal_id = format!("service:rate-parent:{}", uuid::Uuid::new_v4().simple());
    assert_eq!(
        admin
            .post(format!("{}/v1/security/principals", writer.base_url))
            .json(&json!({
                "principal_id": principal_id,
                "kind": "service",
                "display_name": "rate-parent"
            }))
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    let key = admin
        .post(format!("{}/v1/security/keys", writer.base_url))
        .json(&json!({"principal_id": principal_id, "name": "rate-parent-key"}))
        .send()
        .await
        .unwrap();
    assert_eq!(key.status(), 201);
    let parent_bearer = key.json::<serde_json::Value>().await.unwrap()["api_key"]
        .as_str()
        .unwrap()
        .to_string();
    let parent = client_with_bearer(&parent_bearer);
    for body in [
        json!({
            "principal_id": principal_id,
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": ["CredentialDelegate"]}
        }),
        json!({
            "principal_id": principal_id,
            "scope": {"kind": "namespace", "namespace": namespace},
            "actions": {"kind": "selected", "actions": ["Query"]}
        }),
    ] {
        let grant = admin
            .post(format!("{}/v1/security/grants", writer.base_url))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());
    }
    let minted = parent
        .post(format!("{}/v1/security/tokens", writer.base_url))
        .json(&json!({
            "actions": ["Query"],
            "namespaces": [namespace],
            "purpose": "isolated rate bucket",
            "expires_in_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted.status(), 201, "{}", minted.text().await.unwrap());
    let token = minted.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();

    let mut limited = Config::default();
    limited.server.rate_limit_rps = 1_000;
    limited.server.rate_limit_burst = 1_000;
    limited.server.principal_rate_limit_rps = 1;
    limited.server.principal_rate_limit_burst = 1;
    limited.server.write_rate_limit_rps = 1_000;
    limited.server.write_rate_limit_burst = 1_000;
    limited.server.principal_write_rate_limit_rps = 1_000;
    limited.server.principal_write_rate_limit_burst = 1_000;
    let reader = start_test_server_full_without_rate_limit_override_and_admin_bearer(
        store,
        None,
        limited,
        &writer.admin_bearer,
    )
    .await;
    let agent = client_with_bearer(&token);
    let query = || {
        json!({
            "vector": [1.0, 0.0],
            "top_k": 1
        })
    };
    let first = agent
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            reader.base_url
        ))
        .json(&query())
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 200, "{}", first.text().await.unwrap());
    let exhausted = agent
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            reader.base_url
        ))
        .json(&query())
        .send()
        .await
        .unwrap();
    assert_eq!(exhausted.status(), 429);

    let parent_after_agent_exhaustion = parent
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            reader.base_url
        ))
        .json(&query())
        .send()
        .await
        .unwrap();
    assert_eq!(
        parent_after_agent_exhaustion.status(),
        200,
        "{}",
        parent_after_agent_exhaustion.text().await.unwrap()
    );

    writer.shutdown().await;
    reader.shutdown().await;
}
