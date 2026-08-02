use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;

use axum::Router;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use dashmap::DashMap;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use rand::{rngs::OsRng, RngCore};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::task::{AbortHandle, JoinError, JoinHandle};

use super::counting::GetCounter;
use super::harness::{TestHarness, TestServerRuntime};

use zeppelin::cache::decoded_cache::DecodedArtifactCache;
use zeppelin::cache::hydration::{heat_policy_from_config, HydrationConfig, SegmentHydrator};
use zeppelin::cache::manifest_cache::ManifestCache;
use zeppelin::cache::DiskCache;
use zeppelin::compaction::background::{
    compaction_loop_with_governed_deletion, CompactionLifecycle, CompactionLoopOptions,
    GovernedDeletionWorker,
};
use zeppelin::compaction::Compactor;
use zeppelin::config::{ApiKeyConfig, Config, SecurityMode};
use zeppelin::embedding::{ConfiguredEncoderProvider, MultiVectorEncoderProvider};
use zeppelin::fts::wal_cache::WalFtsCache;
use zeppelin::namespace::{
    BranchGraphReadinessSnapshot, BranchReadinessObserver, NamespaceManager,
};
use zeppelin::runtime_config::{QueryKnobBounds, RuntimeQueryConfig};
use zeppelin::security::{
    ApiKeyAdapter, AuditClient, AuditRecord, AuditRuntime, CredentialAdapter, EntitlementLimits,
    EntitlementSource, Entitlements, Feature, SecurityKernel,
};
use zeppelin::server::{build_router, parse_trusted_proxies, AppState, ServerTaskSupervisor};
use zeppelin::storage::ZeppelinStore;
use zeppelin::time::Clock;
use zeppelin::wal::{LeaseManager, WalFragmentCache, WalReader, WalWriter};

tokio::task_local! {
    static BACKGROUND_COMPACTION_ORIGIN: bool;
}

/// Construct an integration-only entitlement fixture without adding a safe
/// production constructor or alternate trust root.
#[must_use]
pub fn test_entitlements(features: impl IntoIterator<Item = Feature>) -> Entitlements {
    test_entitlements_with_expiry(features, None)
}

/// Construct a fixed expired enforcement fixture for integration composition.
#[must_use]
pub fn expired_test_entitlements() -> Entitlements {
    test_entitlements_with_expiry(
        [Feature::Rbac, Feature::Constraints, Feature::AuditS3],
        Some(Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap()),
    )
}

fn test_entitlements_with_expiry(
    features: impl IntoIterator<Item = Feature>,
    expires_at: Option<chrono::DateTime<Utc>>,
) -> Entitlements {
    #[repr(C)]
    struct TestEntitlementsRepr {
        source: EntitlementSource,
        customer: Option<zeppelin::security::CustomerId>,
        customer_name: Option<String>,
        issued_at: Option<chrono::DateTime<Utc>>,
        expires_at: Option<chrono::DateTime<Utc>>,
        management_freeze_at: Option<chrono::DateTime<Utc>>,
        feature_bits: u16,
        limits: EntitlementLimits,
    }

    let feature_bits = features
        .into_iter()
        .fold(0_u16, |bits, feature| bits | (1_u16 << feature as u16));
    let representation = TestEntitlementsRepr {
        source: EntitlementSource::FileLicense,
        customer: None,
        customer_name: None,
        issued_at: expires_at.map(|expiry| expiry - ChronoDuration::days(365)),
        expires_at,
        management_freeze_at: expires_at.map(|expiry| expiry + ChronoDuration::days(14)),
        feature_bits,
        limits: EntitlementLimits::default(),
    };
    assert_eq!(
        std::mem::size_of::<TestEntitlementsRepr>(),
        std::mem::size_of::<Entitlements>()
    );
    assert_eq!(
        std::mem::align_of::<TestEntitlementsRepr>(),
        std::mem::align_of::<Entitlements>()
    );

    // SAFETY: both source and mirror are `repr(C)` with the exact same field
    // types and order. The owned value moves once and is dropped once. This
    // helper is compiled only into integration-test binaries; no safe release
    // constructor or alternate verification authority is exposed.
    unsafe { std::mem::transmute(representation) }
}

fn test_fragment_cache(config: &Config) -> Arc<WalFragmentCache> {
    let max_bytes = config
        .cache
        .wal_fragment_cache_max_mb
        .checked_mul(1024 * 1024)
        .expect("test WAL fragment cache capacity overflow");
    Arc::new(WalFragmentCache::new(max_bytes))
}

/// Build the same lazy, epoch-pinned encoder provider used by production.
#[must_use]
pub fn test_encoder_provider(
    config: &Config,
    store: &ZeppelinStore,
) -> Arc<dyn MultiVectorEncoderProvider> {
    Arc::new(ConfiguredEncoderProvider::new(store.clone(), &config.mmli))
}

fn test_decoded_artifact_cache(config: &Config) -> Arc<DecodedArtifactCache> {
    let max_bytes = config
        .cache
        .decoded_artifact_cache_max_mb
        .checked_mul(1024 * 1024)
        .expect("test decoded artifact cache capacity overflow");
    Arc::new(DecodedArtifactCache::new(max_bytes))
}

/// Returns whether the current future belongs to a spawned compaction loop.
///
/// Operational adversarial observers use this test-only marker to distinguish
/// background maintenance from foreground HTTP and explicit-compaction work.
#[must_use]
pub fn background_compaction_origin_active() -> bool {
    BACKGROUND_COMPACTION_ORIGIN
        .try_with(|active| *active)
        .unwrap_or(false)
}

/// Marks one spawned production compaction-loop future as background work.
pub(crate) async fn with_background_compaction_origin<F>(future: F) -> F::Output
where
    F: Future,
{
    BACKGROUND_COMPACTION_ORIGIN.scope(true, future).await
}

fn configure_test_server_limits(config: &mut Config) {
    // Integration tests share one loopback IP and often issue bursty setup or
    // perf requests. Keep production defaults intact, but avoid unrelated 429s
    // in tests that are not exercising rate limiting.
    config.server.rate_limit_rps = 1_000_000;
    config.server.rate_limit_burst = 1_000_000;
    config.server.write_rate_limit_rps = 1_000_000;
    config.server.write_rate_limit_burst = 1_000_000;
    config.server.principal_rate_limit_rps = 1_000_000;
    config.server.principal_rate_limit_burst = 1_000_000;
    config.server.principal_write_rate_limit_rps = 1_000_000;
    config.server.principal_write_rate_limit_burst = 1_000_000;
}

pub fn scoped_test_security_store(store: &ZeppelinStore, scope: &str) -> ZeppelinStore {
    ZeppelinStore::new(Arc::new(PrefixStore::new(
        store.inner(),
        Path::from(scope.to_string()),
    )))
}

pub async fn start_test_audit(
    config: &Config,
    store: &ZeppelinStore,
    cleanup_scope: Option<&str>,
    security: &SecurityKernel,
) -> (AuditClient, AuditRuntime, String) {
    let entitlements = test_entitlements(Feature::ALL);
    start_test_audit_with_entitlements(
        config,
        store,
        cleanup_scope,
        &entitlements,
        security,
        Utc::now(),
    )
    .await
}

async fn start_test_audit_with_entitlements(
    config: &Config,
    store: &ZeppelinStore,
    cleanup_scope: Option<&str>,
    entitlements: &Entitlements,
    security: &SecurityKernel,
    audit_now: DateTime<Utc>,
) -> (AuditClient, AuditRuntime, String) {
    let node_id = match cleanup_scope {
        Some(scope) => format!("test-node-{scope}-{}", uuid::Uuid::new_v4()),
        None => format!("test-node-{}", uuid::Uuid::new_v4()),
    };
    let durable_audit_enabled = config.security.audit_s3 && entitlements.has(Feature::AuditS3);
    if durable_audit_enabled {
        security
            .install_object_signer(store)
            .expect("test signing capability must be shared with the application store");
    }
    let (client, runtime) = if durable_audit_enabled {
        AuditRuntime::start_at(
            store.clone(),
            node_id.clone(),
            Duration::from_secs(config.security.audit_flush_secs),
            audit_now,
        )
        .await
        .expect("test audit runtime must start")
    } else {
        AuditRuntime::tracing_only(node_id.clone())
            .expect("test tracing-only audit runtime must start")
    };
    if config.security.mode == SecurityMode::OpenUnsafe {
        client
            .submit_buffered(AuditRecord::open_unsafe_boot(audit_now, client.node_id()))
            .expect("open_unsafe test boot audit must be accepted");
    }
    (client, runtime, node_id)
}

/// Spawn one HTTP server with isolated empty background owners.
///
/// Custom router fixtures that install authoritative background owners in
/// [`AppState`] must call [`spawn_test_router_with_lifecycle`] instead.
pub async fn spawn_test_router(
    harness: &TestHarness,
    app: Router,
    audit_signing_security: Arc<SecurityKernel>,
    audit_runtime: AuditRuntime,
) -> String {
    spawn_test_router_with_lifecycle(
        harness,
        app,
        Arc::new(ServerTaskSupervisor::new()),
        CompactionLifecycle::new(),
        audit_signing_security,
        audit_runtime,
    )
    .await
}

/// Spawn one HTTP server and register its exact authoritative lifecycle owners.
pub async fn spawn_test_router_with_lifecycle(
    harness: &TestHarness,
    app: Router,
    server_tasks: Arc<ServerTaskSupervisor>,
    compaction_lifecycle: CompactionLifecycle,
    audit_signing_security: Arc<SecurityKernel>,
    audit_runtime: AuditRuntime,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_http, mut shutdown_http_rx) = tokio::sync::watch::channel(false);
    let server_task = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            let _ = shutdown_http_rx.changed().await;
        })
        .await
        .unwrap();
    });
    harness.register_test_server(TestServerRuntime::new(
        shutdown_http,
        server_task,
        server_tasks,
        compaction_lifecycle,
        audit_runtime,
        audit_signing_security,
    ));
    format!("http://{addr}")
}

const TEST_ADMIN_KEY_ID: &str = "zpk1_test_admin";

/// Build default headers for a test client authenticated as the test admin.
#[must_use]
pub fn bearer_headers(bearer: &str) -> reqwest::header::HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();
    let authorization = reqwest::header::HeaderValue::from_str(&format!("Bearer {bearer}"))
        .expect("test bearer must be a valid Authorization header value");
    headers.insert(reqwest::header::AUTHORIZATION, authorization);
    headers
}

/// Build a reqwest client that authenticates every request as `bearer`.
#[must_use]
pub fn client_with_bearer(bearer: &str) -> reqwest::Client {
    reqwest::Client::builder()
        .default_headers(bearer_headers(bearer))
        .build()
        .expect("failed to build authenticated test client")
}

#[derive(Debug, Clone)]
pub struct WorkloadCredential {
    pub principal_id: String,
    pub key_id: String,
    pub bearer: String,
}

#[derive(Debug, Clone, Default)]
struct ActorCredentials {
    principal_id: String,
    current: Option<WorkloadCredential>,
    retired: Vec<WorkloadCredential>,
}

/// Per-seed credential registry used by the adversarial workload client.
///
/// Secrets remain in memory only. Artifacts persist actor indexes, principal
/// IDs, and key IDs, never bearer material.
#[derive(Debug, Clone, Default)]
pub struct WorkloadCredentialRegistry {
    actors: Arc<RwLock<BTreeMap<u8, ActorCredentials>>>,
}

impl WorkloadCredentialRegistry {
    #[must_use]
    pub fn with_admin(bearer: &str) -> Self {
        let registry = Self::default();
        let (key_id, _) = bearer
            .split_once('.')
            .expect("test admin bearer must contain a key id and secret");
        registry.register_principal(0, "service:test-admin");
        registry.install(0, key_id, bearer);
        registry
    }

    pub fn register_principal(&self, actor: u8, principal_id: &str) {
        let mut actors = self
            .actors
            .write()
            .unwrap_or_else(|_| panic!("workload credential registry lock poisoned"));
        let entry = actors.entry(actor).or_default();
        if entry.principal_id.is_empty() {
            entry.principal_id = principal_id.to_string();
        } else {
            assert_eq!(
                entry.principal_id, principal_id,
                "actor {actor} changed principal identity"
            );
        }
    }

    pub fn install(&self, actor: u8, key_id: &str, bearer: &str) {
        let mut actors = self
            .actors
            .write()
            .unwrap_or_else(|_| panic!("workload credential registry lock poisoned"));
        let entry = actors
            .get_mut(&actor)
            .unwrap_or_else(|| panic!("actor {actor} must register a principal before a key"));
        if let Some(current) = entry.current.replace(WorkloadCredential {
            principal_id: entry.principal_id.clone(),
            key_id: key_id.to_string(),
            bearer: bearer.to_string(),
        }) {
            entry.retired.insert(0, current);
        }
    }

    #[must_use]
    pub fn principal_id(&self, actor: u8) -> String {
        self.actors
            .read()
            .unwrap_or_else(|_| panic!("workload credential registry lock poisoned"))
            .get(&actor)
            .unwrap_or_else(|| panic!("unknown workload actor {actor}"))
            .principal_id
            .clone()
    }

    #[must_use]
    pub fn credential(&self, actor: u8, retired: u8) -> WorkloadCredential {
        let actors = self
            .actors
            .read()
            .unwrap_or_else(|_| panic!("workload credential registry lock poisoned"));
        let entry = actors
            .get(&actor)
            .unwrap_or_else(|| panic!("unknown workload actor {actor}"));
        if retired == 0 {
            return entry
                .current
                .clone()
                .unwrap_or_else(|| panic!("workload actor {actor} has no current credential"));
        }
        entry
            .retired
            .get(usize::from(retired - 1))
            .cloned()
            .unwrap_or_else(|| panic!("workload actor {actor} has no retired credential {retired}"))
    }

    #[must_use]
    pub fn client(&self, actor: u8, retired: u8) -> reqwest::Client {
        client_with_bearer(&self.credential(actor, retired).bearer)
    }

    #[must_use]
    pub fn all_secrets(&self) -> Vec<String> {
        self.actors
            .read()
            .unwrap_or_else(|_| panic!("workload credential registry lock poisoned"))
            .values()
            .flat_map(|entry| entry.current.iter().chain(entry.retired.iter()))
            .map(|credential| {
                credential
                    .bearer
                    .split_once('.')
                    .expect("registered bearer must contain key id and secret")
                    .1
                    .to_string()
            })
            .collect()
    }
}

/// Inject a freshly generated administrator and build the store-backed test runtime.
pub async fn test_security_runtime(
    store: &ZeppelinStore,
    config: &mut Config,
    clock: &Clock,
) -> (Arc<SecurityKernel>, Arc<ApiKeyAdapter>, String) {
    test_security_runtime_with_admin_bearer(
        store,
        config,
        clock,
        None,
        Arc::new(test_entitlements(Feature::ALL)),
    )
    .await
}

/// Inject a freshly generated administrator into an enforced test config.
#[must_use]
pub fn test_admin_bearer(config: &mut Config) -> String {
    inject_test_admin(config, None)
}

async fn test_security_runtime_with_admin_bearer(
    store: &ZeppelinStore,
    config: &mut Config,
    clock: &Clock,
    existing_admin_bearer: Option<&str>,
    entitlements: Arc<Entitlements>,
) -> (Arc<SecurityKernel>, Arc<ApiKeyAdapter>, String) {
    let admin_bearer = inject_test_admin(config, existing_admin_bearer);
    let delegation_key = if (entitlements.has(Feature::Delegation)
        || entitlements.has(Feature::AuditS3))
        && config.security.token_signing_key_path.is_empty()
    {
        let file = tempfile::NamedTempFile::new().expect("delegation test key file");
        std::fs::write(file.path(), "09".repeat(32)).expect("write delegation test seed");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(0o600))
                .expect("restrict delegation test seed permissions");
        }
        config.security.token_signing_key_path = file.path().to_string_lossy().into_owned();
        Some(file)
    } else {
        None
    };
    let (security, credential_adapter) = SecurityKernel::from_resolved_entitlements(
        store.clone(),
        &config.security,
        clock.clone(),
        entitlements,
    )
    .await
    .expect("test security authority must compose");
    drop(delegation_key);
    (security, credential_adapter, admin_bearer)
}

fn inject_test_admin(config: &mut Config, existing_admin_bearer: Option<&str>) -> String {
    config.security.set_cursor_hmac_key_hex("42".repeat(32));
    let secret = match existing_admin_bearer {
        Some(bearer) => {
            let secret = bearer
                .strip_prefix(&format!("{TEST_ADMIN_KEY_ID}."))
                .expect("reused test admin bearer must use the test admin key id");
            let decoded = URL_SAFE_NO_PAD
                .decode(secret)
                .expect("reused test admin secret must be canonical base64url");
            assert_eq!(
                decoded.len(),
                32,
                "reused test admin secret must decode to exactly 32 bytes"
            );
            secret.to_string()
        }
        None => {
            let mut secret_bytes = [0_u8; 32];
            OsRng.fill_bytes(&mut secret_bytes);
            URL_SAFE_NO_PAD.encode(secret_bytes)
        }
    };
    let digest = Sha256::digest(secret.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let admin_bearer = format!("{TEST_ADMIN_KEY_ID}.{secret}");

    if config.security.mode == SecurityMode::Enforced {
        config
            .security
            .api_keys
            .retain(|key| key.key_id != TEST_ADMIN_KEY_ID);
        config.security.api_keys.push(ApiKeyConfig {
            key_id: TEST_ADMIN_KEY_ID.to_string(),
            name: "test-admin".to_string(),
            sha256_hex: digest,
            actions: vec!["*".to_string()],
            namespaces: vec!["*".to_string()],
            expires_at: None,
        });
    }
    admin_bearer
}

/// Return the administrator credential already persisted for a security-store
/// scope, minting and remembering one on first use.
///
/// Policy authority is S3: once a scope's first server has bootstrapped a
/// policy head, config-injected keys are ignored, so a restart that minted a
/// fresh bearer would authenticate nothing. Servers restarted against the same
/// scope must therefore reuse the credential their first boot persisted. Keyed
/// by the isolation scope so concurrent suites never share a credential.
fn persisted_scope_admin_bearer(scope: &str) -> String {
    static SCOPE_ADMIN_BEARERS: OnceLock<Mutex<BTreeMap<String, String>>> = OnceLock::new();
    SCOPE_ADMIN_BEARERS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(|_| panic!("scoped admin bearer registry lock poisoned"))
        .entry(scope.to_string())
        .or_insert_with(|| {
            let mut secret_bytes = [0_u8; 32];
            OsRng.fill_bytes(&mut secret_bytes);
            format!(
                "{TEST_ADMIN_KEY_ID}.{}",
                URL_SAFE_NO_PAD.encode(secret_bytes)
            )
        })
        .clone()
}

fn runtime_query_state(config: &Config) -> (Arc<RuntimeQueryConfig>, QueryKnobBounds) {
    (
        Arc::new(RuntimeQueryConfig::from_config(config)),
        QueryKnobBounds::from_config(config),
    )
}

fn namespace_manager(
    config: &Config,
    store: &ZeppelinStore,
    clock: &Clock,
    security: &SecurityKernel,
) -> Arc<NamespaceManager> {
    Arc::new(
        NamespaceManager::with_clock(
            store.clone(),
            Duration::from_millis(config.cache.namespace_registry_ttl_ms),
            clock.clone(),
        )
        .with_preservation_service(security.preservation_service().cloned()),
    )
}

fn compactor(
    config: &Config,
    store: &ZeppelinStore,
    clock: &Clock,
    security: &SecurityKernel,
) -> Arc<Compactor> {
    Arc::new(
        Compactor::with_clock(
            store.clone(),
            WalReader::new(store.clone()),
            config.compaction.clone(),
            config.indexing.clone(),
            Duration::from_secs(config.gc.compaction_upload_window_secs),
            clock.clone(),
        )
        .with_mmli_config(config.mmli.clone())
        .with_preservation_service(security.preservation_service().cloned()),
    )
}

fn lease_manager(config: &Config, store: &ZeppelinStore, clock: &Clock) -> Arc<LeaseManager> {
    Arc::new(LeaseManager::with_clock(
        store.clone(),
        format!("test-{}", uuid::Uuid::new_v4()),
        Duration::from_secs(config.compaction.lease_duration_secs),
        clock.clone(),
    ))
}

fn maybe_hydrator(
    config: &Config,
    store: &ZeppelinStore,
    cache: &Arc<DiskCache>,
) -> Option<Arc<SegmentHydrator>> {
    if !config.cache.hydration_enabled {
        return None;
    }
    let hydration_config = HydrationConfig::from_cache_config(
        &config.cache,
        Duration::from_secs(config.server.request_timeout_secs),
    )
    .unwrap();
    let policy = heat_policy_from_config(&config.cache).unwrap();
    Some(SegmentHydrator::start(
        store.clone(),
        cache.clone(),
        policy,
        hydration_config,
    ))
}

/// Start a test server with optional config override.
///
/// Returns `(base_url, harness, cache, cache_dir, admin_bearer)`. The TempDir
/// and bearer must be retained for the lifetime of the server and its client.
pub async fn start_test_server_with_config(
    config_override: Option<Config>,
) -> (
    String,
    TestHarness,
    Arc<DiskCache>,
    tempfile::TempDir,
    String,
) {
    start_test_server_with_config_inner(config_override, true, None).await
}

/// Start a test server without overriding rate-limit settings.
pub async fn start_test_server_with_config_no_limit_override(
    config_override: Option<Config>,
) -> (
    String,
    TestHarness,
    Arc<DiskCache>,
    tempfile::TempDir,
    String,
) {
    start_test_server_with_config_inner(config_override, false, None).await
}

/// Start a test server with an explicit composition-root entitlement set.
pub async fn start_test_server_with_entitlements(
    config: Config,
    entitlements: Entitlements,
) -> (
    String,
    TestHarness,
    Arc<DiskCache>,
    tempfile::TempDir,
    String,
) {
    start_test_server_with_config_inner(Some(config), true, Some(entitlements)).await
}

async fn start_test_server_with_config_inner(
    config_override: Option<Config>,
    override_rate_limits: bool,
    entitlements_override: Option<Entitlements>,
) -> (
    String,
    TestHarness,
    Arc<DiskCache>,
    tempfile::TempDir,
    String,
) {
    // Ensure metrics are registered (idempotent)
    zeppelin::metrics::init();

    let harness = TestHarness::new().await;
    let mut config = config_override.unwrap_or_default();
    if override_rate_limits {
        configure_test_server_limits(&mut config);
    }
    let clock = Clock::system();
    let entitlements =
        Arc::new(entitlements_override.unwrap_or_else(|| test_entitlements(Feature::ALL)));
    let security_store = scoped_test_security_store(&harness.store, &harness.prefix);
    let (security, credential_adapter, admin_bearer) = test_security_runtime_with_admin_bearer(
        &security_store,
        &mut config,
        &clock,
        None,
        Arc::clone(&entitlements),
    )
    .await;
    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    let query_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_queries,
    ));
    let (runtime_query_config, query_knob_bounds) = runtime_query_state(&config);
    let hydrator = maybe_hydrator(&config, &harness.store, &cache);
    let compactor = compactor(&config, &harness.store, &clock, &security);
    let lease_manager = lease_manager(&config, &harness.store, &clock);
    let trusted_proxies = Arc::from(parse_trusted_proxies(&config.server.trusted_proxies).unwrap());
    let (audit, audit_runtime, _audit_node_id) = start_test_audit_with_entitlements(
        &config,
        &harness.store,
        Some(&harness.prefix),
        &entitlements,
        &security,
        clock.now(),
    )
    .await;
    let server_tasks = Arc::new(ServerTaskSupervisor::new());
    let compaction_lifecycle = CompactionLifecycle::new();
    let state = AppState {
        store: harness.store.clone(),
        clock: clock.clone(),
        security: Arc::clone(&security),
        audit,
        credential_adapter,
        namespace_manager: namespace_manager(&config, &harness.store, &clock, &security),
        namespace_name_prefix: Some(harness.prefix.clone()),
        branch_readiness: BranchGraphReadinessSnapshot::new(),
        wal_writer: Arc::new(WalWriter::with_clock(harness.store.clone(), clock)),
        wal_reader: Arc::new(WalReader::new(harness.store.clone())),
        encoder_provider: test_encoder_provider(&config, &harness.store),
        compactor,
        lease_manager,
        compaction_lifecycle: compaction_lifecycle.clone(),
        server_tasks: Arc::clone(&server_tasks),
        fragment_cache: test_fragment_cache(&config),
        decoded_artifact_cache: test_decoded_artifact_cache(&config),
        config: Arc::new(config),
        trusted_proxies,
        runtime_query_config,
        query_knob_bounds,
        cache: cache.clone(),
        manifest_cache: Arc::new(ManifestCache::new(Duration::from_millis(500))),
        hydrator,
        fts_cache: Arc::new(WalFtsCache::new()),
        query_semaphore,
        rate_limiters: Arc::new(DashMap::new()),
    };

    let app = build_router(state);
    let base_url = spawn_test_router_with_lifecycle(
        &harness,
        app,
        server_tasks,
        compaction_lifecycle,
        security,
        audit_runtime,
    )
    .await;

    (base_url, harness, cache, cache_dir, admin_bearer)
}

/// Start a test server on an already-constructed store.
///
/// Used by tests that wrap the harness store with instrumentation while still
/// relying on the harness's random prefix and cleanup.
pub async fn start_test_server_on_store(
    harness: &TestHarness,
    store: ZeppelinStore,
    namespace_name_prefix: Option<String>,
) -> (String, Arc<DiskCache>, tempfile::TempDir, String) {
    start_test_server_on_store_with_config(harness, store, namespace_name_prefix, Config::default())
        .await
}

/// Start a test server on an instrumented store with an explicit configuration.
///
/// This keeps fault-injection tests on the production router while allowing
/// correctness-sensitive intervals to remain valid during deliberately paused
/// object-store operations.
pub async fn start_test_server_on_store_with_config(
    harness: &TestHarness,
    store: ZeppelinStore,
    namespace_name_prefix: Option<String>,
    config: Config,
) -> (String, Arc<DiskCache>, tempfile::TempDir, String) {
    let (base_url, cache, cache_dir, admin_bearer, _readiness) =
        start_test_server_on_store_with_readiness(harness, store, namespace_name_prefix, config)
            .await;
    (base_url, cache, cache_dir, admin_bearer)
}

/// Same server, plus a worker that can publish one branch-readiness scan.
///
/// Readiness is answered from a snapshot that background maintenance owns.
/// This helper hands tests the scanning seam so they can observe a graph defect
/// deterministically instead of waiting on a maintenance tick.
pub async fn start_test_server_on_store_with_readiness(
    harness: &TestHarness,
    store: ZeppelinStore,
    namespace_name_prefix: Option<String>,
    mut config: Config,
) -> (
    String,
    Arc<DiskCache>,
    tempfile::TempDir,
    String,
    GovernedDeletionWorker,
) {
    zeppelin::metrics::init();

    configure_test_server_limits(&mut config);
    let clock = Clock::system();
    // Custom application-store wrappers still share the harness's underlying
    // backend. Keep policy authority isolated by the harness's random prefix
    // independently of whether the application enforces a namespace prefix.
    let security_store = scoped_test_security_store(&store, &harness.prefix);
    // Servers restarted against the same harness share one S3-authoritative
    // policy store, so they must reuse the administrator their first boot
    // persisted instead of minting a bearer the policy head does not know.
    let admin_bearer = persisted_scope_admin_bearer(&harness.prefix);
    let (security, credential_adapter, admin_bearer) = test_security_runtime_with_admin_bearer(
        &security_store,
        &mut config,
        &clock,
        Some(&admin_bearer),
        Arc::new(test_entitlements(Feature::ALL)),
    )
    .await;
    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    let query_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_queries,
    ));
    let (runtime_query_config, query_knob_bounds) = runtime_query_state(&config);
    let hydrator = maybe_hydrator(&config, &store, &cache);
    let compactor = compactor(&config, &store, &clock, &security);
    let lease_manager = lease_manager(&config, &store, &clock);
    let trusted_proxies = Arc::from(parse_trusted_proxies(&config.server.trusted_proxies).unwrap());
    let (audit, audit_runtime, _audit_node_id) =
        start_test_audit(&config, &store, Some(&harness.prefix), &security).await;
    let server_tasks = Arc::new(ServerTaskSupervisor::new());
    let compaction_lifecycle = CompactionLifecycle::new();
    let manifest_cache_for_readiness = Arc::new(ManifestCache::new(Duration::from_millis(
        config.cache.manifest_cache_ttl_ms,
    )));
    let namespace_manager_handle = namespace_manager(&config, &store, &clock, &security);
    let readiness = BranchReadinessObserver::scoped(namespace_name_prefix.clone());
    let readiness_worker = GovernedDeletionWorker::new(
        store.clone(),
        namespace_manager_handle.clone(),
        lease_manager.clone(),
        clock.clone(),
        manifest_cache_for_readiness,
        &config,
        security.clone(),
        readiness.clone(),
    );
    let state = AppState {
        store: store.clone(),
        clock: clock.clone(),
        security: Arc::clone(&security),
        audit,
        credential_adapter,
        namespace_manager: namespace_manager_handle,
        namespace_name_prefix,
        branch_readiness: readiness.snapshot,
        wal_writer: Arc::new(WalWriter::with_clock(store.clone(), clock)),
        wal_reader: Arc::new(WalReader::new(store.clone())),
        encoder_provider: test_encoder_provider(&config, &store),
        compactor,
        lease_manager,
        compaction_lifecycle: compaction_lifecycle.clone(),
        server_tasks: Arc::clone(&server_tasks),
        fragment_cache: test_fragment_cache(&config),
        decoded_artifact_cache: test_decoded_artifact_cache(&config),
        config: Arc::new(config),
        trusted_proxies,
        runtime_query_config,
        query_knob_bounds,
        cache: cache.clone(),
        manifest_cache: Arc::new(ManifestCache::new(Duration::from_millis(500))),
        hydrator,
        fts_cache: Arc::new(WalFtsCache::new()),
        query_semaphore,
        rate_limiters: Arc::new(DashMap::new()),
    };

    let app = build_router(state);
    let base_url = spawn_test_router_with_lifecycle(
        harness,
        app,
        server_tasks,
        compaction_lifecycle,
        security,
        audit_runtime,
    )
    .await;

    (base_url, cache, cache_dir, admin_bearer, readiness_worker)
}

/// Start a test server that also returns the `Arc<Compactor>` for manual compaction triggering.
/// Avoids config mismatch from constructing a separate compactor in tests.
pub async fn start_test_server_with_compactor(
    config_override: Option<Config>,
) -> (
    String,
    TestHarness,
    Arc<DiskCache>,
    tempfile::TempDir,
    Arc<Compactor>,
    String,
) {
    zeppelin::metrics::init();

    let harness = TestHarness::new().await;
    let mut config = config_override.unwrap_or_default();
    configure_test_server_limits(&mut config);
    let clock = Clock::system();
    let security_store = scoped_test_security_store(&harness.store, &harness.prefix);
    let (security, credential_adapter, admin_bearer) =
        test_security_runtime(&security_store, &mut config, &clock).await;
    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    let compactor = compactor(&config, &harness.store, &clock, &security);
    let lease_manager = lease_manager(&config, &harness.store, &clock);
    let manifest_cache = Arc::new(ManifestCache::new(Duration::ZERO));

    let query_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_queries,
    ));
    let (runtime_query_config, query_knob_bounds) = runtime_query_state(&config);
    let hydrator = maybe_hydrator(&config, &harness.store, &cache);
    let trusted_proxies = Arc::from(parse_trusted_proxies(&config.server.trusted_proxies).unwrap());
    let (audit, audit_runtime, _audit_node_id) =
        start_test_audit(&config, &harness.store, Some(&harness.prefix), &security).await;
    let server_tasks = Arc::new(ServerTaskSupervisor::new());
    let compaction_lifecycle = CompactionLifecycle::new();
    let state = AppState {
        store: harness.store.clone(),
        clock: clock.clone(),
        security: Arc::clone(&security),
        audit,
        credential_adapter,
        namespace_manager: namespace_manager(&config, &harness.store, &clock, &security),
        namespace_name_prefix: None,
        branch_readiness: BranchGraphReadinessSnapshot::new(),
        wal_writer: Arc::new(WalWriter::with_clock(harness.store.clone(), clock)),
        wal_reader: Arc::new(WalReader::new(harness.store.clone())),
        encoder_provider: test_encoder_provider(&config, &harness.store),
        compactor: compactor.clone(),
        lease_manager,
        compaction_lifecycle: compaction_lifecycle.clone(),
        server_tasks: Arc::clone(&server_tasks),
        fragment_cache: test_fragment_cache(&config),
        decoded_artifact_cache: test_decoded_artifact_cache(&config),
        config: Arc::new(config),
        trusted_proxies,
        runtime_query_config,
        query_knob_bounds,
        cache: cache.clone(),
        manifest_cache,
        hydrator,
        fts_cache: Arc::new(WalFtsCache::new()),
        query_semaphore,
        rate_limiters: Arc::new(DashMap::new()),
    };

    let app = build_router(state);
    let base_url = spawn_test_router_with_lifecycle(
        &harness,
        app,
        server_tasks,
        compaction_lifecycle,
        security,
        audit_runtime,
    )
    .await;

    (base_url, harness, cache, cache_dir, compactor, admin_bearer)
}

/// Start a test server with the real background compaction loop spawned,
/// mirroring production `main.rs`. Returns the shutdown sender so tests
/// can cleanly stop the loop.
pub async fn start_test_server_with_compaction(
    config_override: Option<Config>,
) -> (
    String,
    TestHarness,
    Arc<DiskCache>,
    tempfile::TempDir,
    tokio::sync::watch::Sender<bool>,
    String,
) {
    zeppelin::metrics::init();

    let harness = TestHarness::new().await;
    let mut config = config_override.unwrap_or_default();
    configure_test_server_limits(&mut config);
    let clock = Clock::system();
    let security_store = scoped_test_security_store(&harness.store, &harness.prefix);
    let (security, credential_adapter, admin_bearer) =
        test_security_runtime(&security_store, &mut config, &clock).await;
    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    let namespace_manager = namespace_manager(&config, &harness.store, &clock, &security);

    let compactor = compactor(&config, &harness.store, &clock, &security);

    // Spawn background compaction loop (mirrors main.rs)
    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_millis(500)));
    let lease_manager = lease_manager(&config, &harness.store, &clock);
    let compaction_lifecycle = CompactionLifecycle::new();
    let branch_readiness = BranchReadinessObserver::scoped(Some(harness.prefix.clone()));
    let deletion_worker = GovernedDeletionWorker::new(
        harness.store.clone(),
        namespace_manager.clone(),
        lease_manager.clone(),
        clock.clone(),
        manifest_cache.clone(),
        &config,
        security.clone(),
        branch_readiness.clone(),
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let gc_config = config.gc.clone();
    {
        let compactor = compactor.clone();
        let namespace_manager = namespace_manager.clone();
        let manifest_cache = manifest_cache.clone();
        let lease_manager = lease_manager.clone();
        let cache = cache.clone();
        let compaction_lifecycle = compaction_lifecycle.clone();
        let namespace_prefix = Some(harness.prefix.clone());
        tokio::spawn(with_background_compaction_origin(async move {
            compaction_loop_with_governed_deletion(
                compactor,
                namespace_manager,
                shutdown_rx,
                manifest_cache,
                lease_manager,
                cache,
                CompactionLoopOptions {
                    gc_config,
                    namespace_prefix,
                },
                deletion_worker,
                &compaction_lifecycle,
            )
            .await;
        }));
    }

    let query_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_queries,
    ));
    let (runtime_query_config, query_knob_bounds) = runtime_query_state(&config);
    let hydrator = maybe_hydrator(&config, &harness.store, &cache);
    let trusted_proxies = Arc::from(parse_trusted_proxies(&config.server.trusted_proxies).unwrap());
    let (audit, audit_runtime, _audit_node_id) =
        start_test_audit(&config, &harness.store, Some(&harness.prefix), &security).await;
    let server_tasks = Arc::new(ServerTaskSupervisor::new());
    let state = AppState {
        store: harness.store.clone(),
        clock: clock.clone(),
        security: Arc::clone(&security),
        audit,
        credential_adapter,
        namespace_manager: Arc::clone(&namespace_manager),
        namespace_name_prefix: Some(harness.prefix.clone()),
        branch_readiness: branch_readiness.snapshot,
        wal_writer: Arc::new(WalWriter::with_clock(harness.store.clone(), clock)),
        wal_reader: Arc::new(WalReader::new(harness.store.clone())),
        encoder_provider: test_encoder_provider(&config, &harness.store),
        compactor,
        lease_manager,
        compaction_lifecycle: compaction_lifecycle.clone(),
        server_tasks: Arc::clone(&server_tasks),
        fragment_cache: test_fragment_cache(&config),
        decoded_artifact_cache: test_decoded_artifact_cache(&config),
        config: Arc::new(config),
        trusted_proxies,
        runtime_query_config,
        query_knob_bounds,
        cache: cache.clone(),
        manifest_cache,
        hydrator,
        fts_cache: Arc::new(WalFtsCache::new()),
        query_semaphore,
        rate_limiters: Arc::new(DashMap::new()),
    };

    let app = build_router(state);
    let base_url = spawn_test_router_with_lifecycle(
        &harness,
        app,
        server_tasks,
        compaction_lifecycle,
        security,
        audit_runtime,
    )
    .await;

    (
        base_url,
        harness,
        cache,
        cache_dir,
        shutdown_tx,
        admin_bearer,
    )
}

/// Start an enforced test server with a freshly generated administrator credential.
pub async fn start_test_server() -> (String, TestHarness, String) {
    let (url, harness, _cache, dir, admin_bearer) = start_test_server_with_config(None).await;
    // The DiskCache lives in `dir`. This helper's caller does not receive the
    // TempDir handle, so if we let it drop here the cache directory is deleted
    // out from under the still-running server — every cache write then fails
    // (learnings rule 6: keep TempDir alive for the lifetime of anything using
    // its path). Disarm the auto-delete so the dir survives for the test
    // process; the OS reclaims the temp space afterwards.
    let _ = dir.keep();
    (url, harness, admin_bearer)
}

/// Start the explicit anonymous-access test server variant.
pub async fn start_test_server_open_unsafe() -> (String, TestHarness) {
    let mut config = Config::default();
    config.security.mode = SecurityMode::OpenUnsafe;
    let (url, harness, _cache, dir, _unused_bearer) =
        start_test_server_with_config(Some(config)).await;
    let _ = dir.keep();
    (url, harness)
}

/// Create a URL-safe namespace name scoped to this test's prefix.
pub fn api_ns(harness: &TestHarness, suffix: &str) -> String {
    format!("{}-{suffix}", harness.prefix)
}

/// Create a namespace via the API and return the server-generated UUID name.
pub async fn create_ns_api(client: &reqwest::Client, base_url: &str, dimensions: usize) -> String {
    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({ "dimensions": dimensions }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create namespace failed");
    let body: serde_json::Value = resp.json().await.unwrap();
    body["name"].as_str().unwrap().to_string()
}

/// Create a namespace with FTS config via the API and return the UUID name.
pub async fn create_ns_api_fts(
    client: &reqwest::Client,
    base_url: &str,
    dimensions: usize,
    fts: serde_json::Value,
) -> String {
    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "dimensions": dimensions,
            "full_text_search": fts,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create FTS namespace failed");
    let body: serde_json::Value = resp.json().await.unwrap();
    body["name"].as_str().unwrap().to_string()
}

/// Create a namespace with custom options via the API and return the UUID name.
pub async fn create_ns_api_with(
    client: &reqwest::Client,
    base_url: &str,
    body: serde_json::Value,
) -> String {
    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status, 201, "create namespace failed: {body}");
    body["name"].as_str().unwrap().to_string()
}

/// Clean up all S3 objects under a namespace prefix.
pub async fn cleanup_ns(store: &ZeppelinStore, ns: &str) {
    let prefix = format!("{ns}/");
    let _ = store.delete_prefix(&prefix).await;
}

/// Failure observed while crash retirement settles an already-stopped test server.
#[derive(Debug, Error)]
pub enum FullTestServerRetirementError {
    /// The HTTP task failed or was cancelled before retirement joined it.
    #[error("test HTTP crash retirement failed: {0}")]
    HttpTask(#[source] JoinError),
}

/// Cloneable emergency-retirement handle for a test server owned by a bounded task.
///
/// The normal owner still performs ordered graceful or crash retirement. A
/// per-seed watchdog uses this handle only after that owner has stopped making
/// progress, so the watchdog can stop admission before cancelling and joining
/// the owner task.
#[derive(Clone)]
pub struct FullTestServerWatchdogHandle {
    shutdown_http: tokio::sync::watch::Sender<bool>,
    http_task: AbortHandle,
    shutdown_compaction: Option<tokio::sync::watch::Sender<bool>>,
    compaction_task: Option<AbortHandle>,
    compaction_lifecycle: CompactionLifecycle,
    server_tasks: Arc<ServerTaskSupervisor>,
    security: Arc<SecurityKernel>,
}

impl FullTestServerWatchdogHandle {
    /// Stop new work and cancel the listener/background-loop task owners.
    pub fn begin_abort(&self) {
        let _ = self.shutdown_http.send_replace(true);
        if let Some(shutdown) = &self.shutdown_compaction {
            let _ = shutdown.send_replace(true);
        }
        self.http_task.abort();
        if let Some(task) = &self.compaction_task {
            task.abort();
        }
    }

    /// Join authoritative child work after the bounded owner task is cancelled.
    ///
    /// # Errors
    ///
    /// Returns a joined diagnostic when request-task or compaction-heartbeat
    /// retirement fails.
    pub async fn finish_cleanup(&self) -> Result<(), String> {
        let request_tasks = self
            .server_tasks
            .abort_and_join()
            .await
            .map_err(|error| format!("request tasks: {error}"));
        let heartbeats = self
            .compaction_lifecycle
            .close_and_abort_heartbeats()
            .await
            .map_err(|error| format!("compaction heartbeats: {error}"));
        self.security.shutdown_refresh_tasks().await;

        let failures = [request_tasks, heartbeats]
            .into_iter()
            .filter_map(Result::err)
            .collect::<Vec<_>>();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }

    /// Report whether the top-level HTTP and compaction owners have stopped.
    #[must_use]
    pub fn lifecycle_state(&self) -> serde_json::Value {
        serde_json::json!({
            "http_task_finished": self.http_task.is_finished(),
            "compaction_task_finished": self
                .compaction_task
                .as_ref()
                .is_none_or(AbortHandle::is_finished),
        })
    }
}

pub struct FullTestServer {
    pub base_url: String,
    pub admin_bearer: String,
    pub store: ZeppelinStore,
    pub clock: Clock,
    pub cache: Arc<DiskCache>,
    pub cache_dir: tempfile::TempDir,
    pub compactor: Arc<Compactor>,
    pub lease_manager: Arc<LeaseManager>,
    pub manifest_cache: Arc<ManifestCache>,
    pub audit: AuditClient,
    pub audit_node_id: String,
    pub security: Arc<SecurityKernel>,
    pub namespace_manager: Arc<NamespaceManager>,
    pub encoder_provider: Arc<dyn MultiVectorEncoderProvider>,
    pub object_store_counter: Option<GetCounter>,
    pub workload_credentials: WorkloadCredentialRegistry,
    compaction_lifecycle: CompactionLifecycle,
    server_tasks: Arc<ServerTaskSupervisor>,
    audit_runtime: Option<AuditRuntime>,
    fragment_cache: Arc<WalFragmentCache>,
    decoded_artifact_cache: Arc<DecodedArtifactCache>,
    wal_writer: Arc<WalWriter>,
    pub shutdown_compaction: Option<tokio::sync::watch::Sender<bool>>,
    pub compaction_loop_task: Option<JoinHandle<()>>,
    pub server_task: JoinHandle<()>,
    pub shutdown_http: tokio::sync::watch::Sender<bool>,
    shutdown_timeout: Duration,
}

impl FullTestServer {
    /// Build an emergency-retirement handle for a runner watchdog.
    #[must_use]
    pub fn watchdog_handle(&self) -> FullTestServerWatchdogHandle {
        FullTestServerWatchdogHandle {
            shutdown_http: self.shutdown_http.clone(),
            http_task: self.server_task.abort_handle(),
            shutdown_compaction: self.shutdown_compaction.clone(),
            compaction_task: self
                .compaction_loop_task
                .as_ref()
                .map(JoinHandle::abort_handle),
            compaction_lifecycle: self.compaction_lifecycle.clone(),
            server_tasks: Arc::clone(&self.server_tasks),
            security: Arc::clone(&self.security),
        }
    }

    /// Force this node to observe the authoritative policy head immediately.
    pub async fn force_policy_refresh(&self) {
        self.security
            .refresh_authoritative_policy_for_test()
            .await
            .expect("test security policy refresh must succeed");
    }

    /// Force all audit records accepted before this call to durable storage.
    pub async fn flush_audit(&self) {
        self.audit
            .flush()
            .await
            .expect("test audit flush must succeed");
    }

    /// Drop disposable decoded WAL state between isolated measured rounds.
    pub fn clear_wal_fragment_cache(&self) {
        self.fragment_cache.clear();
    }

    /// Drop disposable decoded segment FTS state between isolated rounds.
    pub fn clear_decoded_artifact_cache(&self) {
        self.decoded_artifact_cache.clear();
    }

    /// Return successful segment FTS decodes since server construction.
    #[must_use]
    pub fn decoded_artifact_cache_decode_count(&self) -> u64 {
        self.decoded_artifact_cache.decode_count()
    }

    /// Return successful global FTS decodes since server construction.
    #[must_use]
    pub fn decoded_artifact_cache_global_decode_count(&self) -> u64 {
        self.decoded_artifact_cache.global_decode_count()
    }

    /// Return successful legacy cluster FTS decodes since construction.
    #[must_use]
    pub fn decoded_artifact_cache_cluster_decode_count(&self) -> u64 {
        self.decoded_artifact_cache.cluster_decode_count()
    }

    /// Return the number of retained decoded segment FTS objects.
    #[must_use]
    pub fn decoded_artifact_cache_len(&self) -> usize {
        self.decoded_artifact_cache.len()
    }

    /// Drop disposable per-namespace writer state between measured test rounds.
    pub fn reset_wal_writer_state(&self, namespace: &str) {
        self.wal_writer.remove_lock(namespace);
    }

    /// Abort the HTTP server and compaction loop without draining in-flight work.
    pub fn abort(&mut self) {
        self.server_task.abort();
        if let Some(task) = &self.compaction_loop_task {
            task.abort();
        }
    }

    /// Simulate a process crash, then retire request-spawned authoritative work.
    ///
    /// Unlike [`Self::abort`], this consumes the server so a recovery test cannot
    /// compose its replacement while the crashed node still owns security refresh,
    /// audit, or request-spawned authoritative mutation work. Cache-only hydration
    /// is independently queue-owned and is disabled by the adversarial runner.
    ///
    /// # Errors
    ///
    /// Returns [`FullTestServerRetirementError::HttpTask`] when the HTTP task had
    /// already failed or been cancelled. The error is deferred until compaction,
    /// audit, request-task, and security-refresh retirement barriers all finish.
    pub async fn abort_and_drop(mut self) -> Result<(), FullTestServerRetirementError> {
        // Axum's graceful shutdown stops accepting new connections and does not
        // resolve until every accepted connection task has been joined. A task
        // abort only drops the listener owner and can leave connection-owned
        // `AppState` (including security refresh work) alive past recovery.
        let _ = self.shutdown_http.send_replace(true);

        let http_result = (&mut self.server_task)
            .await
            .map_err(FullTestServerRetirementError::HttpTask);

        // Let an in-flight periodic tick observe retirement before its next
        // lifecycle reservation. Without this signal, the close below is a
        // false operational compaction failure rather than shutdown control
        // flow for a tick racing between trigger evaluation and lease acquire.
        if let Some(shutdown) = self.shutdown_compaction.take() {
            let _ = shutdown.send(true);
        }
        self.compaction_lifecycle
            .close_and_abort_heartbeats()
            .await
            .unwrap_or_else(|error| {
                panic!("test compaction heartbeat crash retirement failed: {error}")
            });

        if let Some(task) = self.compaction_loop_task.as_ref() {
            // A simulated crash intentionally does not let compaction finish
            // work or publish another result. The lifecycle barrier above has
            // already stopped every lease renewer owned by this outer task.
            task.abort();
        }

        if let Some(task) = self.compaction_loop_task.take() {
            match task.await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                Err(error) => panic!("test compaction crash retirement failed: {error}"),
            }
        }

        self.audit_runtime
            .take()
            .expect("test audit runtime must be present during crash retirement")
            .abort_and_join()
            .await
            .unwrap_or_else(|error| panic!("test audit crash retirement failed: {error}"));
        self.server_tasks
            .abort_and_join()
            .await
            .unwrap_or_else(|error| panic!("test request-task crash retirement failed: {error}"));
        self.security.shutdown_refresh_tasks().await;
        drop(self);
        http_result
    }

    /// Gracefully stop the HTTP server and background compaction loop.
    pub async fn shutdown(mut self) {
        let _ = self.shutdown_http.send(true);
        if let Some(shutdown) = self.shutdown_compaction.take() {
            let _ = shutdown.send(true);
        }
        let server_result = self
            .server_task
            .await
            .map_err(|error| format!("test HTTP server failed: {error}"));
        // Concurrent servers and scoped restarts share one application store,
        // whose weak object-signer slot each boot rebinds to its own security
        // kernel. When a peer server retired first, that slot is dead; rebind
        // it to this server's kernel — alive for the remainder of this
        // shutdown — so graceful audit shutdown can still sign its sealed tail.
        if self.audit_runtime.is_some() {
            self.security
                .install_object_signer(&self.store)
                .expect("test server shutdown must rebind its own object signer");
        }
        let audit_result = match self.audit_runtime.take() {
            Some(runtime) => runtime
                .shutdown()
                .await
                .map_err(|error| format!("test audit runtime failed: {error}")),
            None => Ok(()),
        };
        let request_tasks_result = self
            .server_tasks
            .join_with_timeout(self.shutdown_timeout)
            .await
            .map_err(|error| format!("test request-spawned task failed: {error}"));
        let compaction_result = match self.compaction_loop_task.take() {
            Some(task) => task
                .await
                .map_err(|error| format!("test compaction loop failed: {error}")),
            None => Ok(()),
        };
        let heartbeat_result = self
            .compaction_lifecycle
            .close_and_abort_heartbeats()
            .await
            .map_err(|error| format!("test compaction heartbeat retirement failed: {error}"));
        self.security.shutdown_refresh_tasks().await;

        let errors = [
            compaction_result,
            server_result,
            audit_result,
            request_tasks_result,
            heartbeat_result,
        ]
        .into_iter()
        .filter_map(Result::err)
        .collect::<Vec<_>>();
        assert!(
            errors.is_empty(),
            "test server shutdown failed: {}",
            errors.join("; ")
        );
    }
}

/// Start a test server on an already-constructed store, returning the full set
/// of test handles needed by deterministic and later chaos adversarial runs.
pub async fn start_test_server_full(
    store: ZeppelinStore,
    namespace_name_prefix: Option<String>,
    config: Config,
    spawn_compaction_loop: bool,
    clock: Option<Clock>,
) -> FullTestServer {
    start_test_server_full_with_disk_cache_max_bytes(
        store,
        namespace_name_prefix,
        config,
        spawn_compaction_loop,
        clock,
        100 * 1024 * 1024,
    )
    .await
}

/// Starts the full test server with an explicit local disk-cache budget.
///
/// Operational adversarial profiles use this additive seam to exercise tiny
/// caches while existing callers retain the historical 100 MiB default.
#[allow(clippy::too_many_arguments)]
pub async fn start_test_server_full_with_disk_cache_max_bytes(
    store: ZeppelinStore,
    namespace_name_prefix: Option<String>,
    config: Config,
    spawn_compaction_loop: bool,
    clock: Option<Clock>,
    disk_cache_max_bytes: u64,
) -> FullTestServer {
    start_test_server_full_with_disk_cache_max_bytes_inner(
        store,
        namespace_name_prefix,
        config,
        spawn_compaction_loop,
        clock,
        disk_cache_max_bytes,
        None,
        None,
        None,
        None,
        None,
        true,
    )
    .await
}

/// Starts a full test server that reuses an existing generated administrator.
///
/// Adversarial second nodes and restarts use this seam so one workload actor
/// retains the same credential while server-local runtime state is replaced.
#[allow(clippy::too_many_arguments)]
pub async fn start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
    store: ZeppelinStore,
    namespace_name_prefix: Option<String>,
    config: Config,
    spawn_compaction_loop: bool,
    clock: Option<Clock>,
    disk_cache_max_bytes: u64,
    admin_bearer: &str,
) -> FullTestServer {
    start_test_server_full_with_disk_cache_max_bytes_inner(
        store,
        namespace_name_prefix,
        config,
        spawn_compaction_loop,
        clock,
        disk_cache_max_bytes,
        Some(admin_bearer),
        None,
        None,
        None,
        None,
        true,
    )
    .await
}

/// Starts the full test server with an explicit multi-vector encoder provider.
///
/// The adversarial late-interaction profile uses this additive test seam to
/// replay exact generated matrices through the production HTTP query path.
#[allow(clippy::too_many_arguments)]
pub async fn start_test_server_full_with_disk_cache_max_bytes_and_encoder_provider(
    store: ZeppelinStore,
    namespace_name_prefix: Option<String>,
    config: Config,
    spawn_compaction_loop: bool,
    clock: Option<Clock>,
    disk_cache_max_bytes: u64,
    encoder_provider: Arc<dyn MultiVectorEncoderProvider>,
    object_store_counter: GetCounter,
) -> FullTestServer {
    start_test_server_full_with_disk_cache_max_bytes_inner(
        store,
        namespace_name_prefix,
        config,
        spawn_compaction_loop,
        clock,
        disk_cache_max_bytes,
        None,
        None,
        None,
        Some(encoder_provider),
        Some(object_store_counter),
        true,
    )
    .await
}

/// Starts the full test server with a reused administrator and explicit encoder.
#[allow(clippy::too_many_arguments)]
pub async fn start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer_and_encoder_provider(
    store: ZeppelinStore,
    namespace_name_prefix: Option<String>,
    config: Config,
    spawn_compaction_loop: bool,
    clock: Option<Clock>,
    disk_cache_max_bytes: u64,
    admin_bearer: &str,
    encoder_provider: Arc<dyn MultiVectorEncoderProvider>,
    object_store_counter: GetCounter,
) -> FullTestServer {
    start_test_server_full_with_disk_cache_max_bytes_inner(
        store,
        namespace_name_prefix,
        config,
        spawn_compaction_loop,
        clock,
        disk_cache_max_bytes,
        Some(admin_bearer),
        None,
        None,
        Some(encoder_provider),
        Some(object_store_counter),
        true,
    )
    .await
}

/// Starts a full test server with an explicit credential adapter test double.
pub async fn start_test_server_full_with_credential_adapter(
    store: ZeppelinStore,
    namespace_name_prefix: Option<String>,
    config: Config,
    credential_adapter: Arc<dyn CredentialAdapter>,
) -> FullTestServer {
    start_test_server_full_with_disk_cache_max_bytes_inner(
        store,
        namespace_name_prefix,
        config,
        false,
        None,
        100 * 1024 * 1024,
        None,
        Some(credential_adapter),
        None,
        None,
        None,
        true,
    )
    .await
}

/// Starts a full test server without replacing the supplied rate-limit settings.
pub async fn start_test_server_full_without_rate_limit_override(
    store: ZeppelinStore,
    namespace_name_prefix: Option<String>,
    config: Config,
) -> FullTestServer {
    start_test_server_full_with_disk_cache_max_bytes_inner(
        store,
        namespace_name_prefix,
        config,
        false,
        None,
        100 * 1024 * 1024,
        None,
        None,
        None,
        None,
        None,
        false,
    )
    .await
}

/// Starts a full server with a reused administrator and exact configured limits.
pub async fn start_test_server_full_without_rate_limit_override_and_admin_bearer(
    store: ZeppelinStore,
    namespace_name_prefix: Option<String>,
    config: Config,
    admin_bearer: &str,
) -> FullTestServer {
    start_test_server_full_with_disk_cache_max_bytes_inner(
        store,
        namespace_name_prefix,
        config,
        false,
        None,
        100 * 1024 * 1024,
        Some(admin_bearer),
        None,
        None,
        None,
        None,
        false,
    )
    .await
}

/// Starts the full server with an explicit clock and entitlement set.
#[allow(clippy::too_many_arguments)]
pub async fn start_test_server_full_with_entitlements(
    store: ZeppelinStore,
    namespace_name_prefix: Option<String>,
    config: Config,
    clock: Clock,
    entitlements: Entitlements,
) -> FullTestServer {
    start_test_server_full_with_disk_cache_max_bytes_inner(
        store,
        namespace_name_prefix,
        config,
        false,
        Some(clock),
        100 * 1024 * 1024,
        None,
        None,
        Some(entitlements),
        None,
        None,
        true,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn start_test_server_full_with_disk_cache_max_bytes_inner(
    store: ZeppelinStore,
    namespace_name_prefix: Option<String>,
    mut config: Config,
    spawn_compaction_loop: bool,
    clock: Option<Clock>,
    disk_cache_max_bytes: u64,
    existing_admin_bearer: Option<&str>,
    credential_adapter_override: Option<Arc<dyn CredentialAdapter>>,
    entitlements_override: Option<Entitlements>,
    encoder_provider_override: Option<Arc<dyn MultiVectorEncoderProvider>>,
    object_store_counter: Option<GetCounter>,
    override_rate_limits: bool,
) -> FullTestServer {
    zeppelin::metrics::init();
    if override_rate_limits {
        configure_test_server_limits(&mut config);
    }
    let clock = clock.unwrap_or_else(Clock::system);
    let entitlements =
        Arc::new(entitlements_override.unwrap_or_else(|| test_entitlements(Feature::ALL)));
    let security_store = namespace_name_prefix.as_deref().map_or_else(
        || store.clone(),
        |scope| scoped_test_security_store(&store, scope),
    );
    // Scoped restarts share one S3-authoritative policy store, so they must
    // reuse the administrator their first boot persisted. An unscoped store
    // keeps minting a fresh administrator per boot so bootstrap-drift coverage
    // still observes a rejected second-boot credential.
    let persisted_admin_bearer;
    let existing_admin_bearer = match (existing_admin_bearer, namespace_name_prefix.as_deref()) {
        (Some(bearer), _) => Some(bearer),
        (None, Some(scope)) => {
            persisted_admin_bearer = persisted_scope_admin_bearer(scope);
            Some(persisted_admin_bearer.as_str())
        }
        (None, None) => None,
    };
    let (security, credential_adapter, admin_bearer) = test_security_runtime_with_admin_bearer(
        &security_store,
        &mut config,
        &clock,
        existing_admin_bearer,
        Arc::clone(&entitlements),
    )
    .await;
    let credential_adapter: Arc<dyn CredentialAdapter> =
        credential_adapter_override.unwrap_or(credential_adapter);

    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), disk_cache_max_bytes)
            .unwrap(),
    );

    let namespace_manager = namespace_manager(&config, &store, &clock, &security);
    let compactor = compactor(&config, &store, &clock, &security);
    let lease_manager = lease_manager(&config, &store, &clock);
    let compaction_lifecycle = CompactionLifecycle::new();
    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_millis(
        config.cache.manifest_cache_ttl_ms,
    )));
    let branch_readiness = BranchReadinessObserver::scoped(namespace_name_prefix.clone());
    let deletion_worker = GovernedDeletionWorker::new(
        store.clone(),
        namespace_manager.clone(),
        lease_manager.clone(),
        clock.clone(),
        manifest_cache.clone(),
        &config,
        security.clone(),
        branch_readiness.clone(),
    );

    let mut compaction_loop_task = None;
    let shutdown_compaction = if spawn_compaction_loop {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let gc_config = config.gc.clone();
        {
            let compactor = compactor.clone();
            let namespace_manager = namespace_manager.clone();
            let manifest_cache = manifest_cache.clone();
            let lease_manager = lease_manager.clone();
            let cache = cache.clone();
            let compaction_lifecycle = compaction_lifecycle.clone();
            let namespace_prefix = namespace_name_prefix.clone();
            compaction_loop_task = Some(tokio::spawn(with_background_compaction_origin(
                async move {
                    compaction_loop_with_governed_deletion(
                        compactor,
                        namespace_manager,
                        shutdown_rx,
                        manifest_cache,
                        lease_manager,
                        cache,
                        CompactionLoopOptions {
                            gc_config,
                            namespace_prefix,
                        },
                        deletion_worker,
                        &compaction_lifecycle,
                    )
                    .await;
                },
            )));
        }
        Some(shutdown_tx)
    } else {
        None
    };

    let query_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_queries,
    ));
    let (runtime_query_config, query_knob_bounds) = runtime_query_state(&config);
    let hydrator = maybe_hydrator(&config, &store, &cache);
    let trusted_proxies = Arc::from(parse_trusted_proxies(&config.server.trusted_proxies).unwrap());
    let wal_writer = Arc::new(WalWriter::with_clock(store.clone(), clock.clone()));
    let fragment_cache = test_fragment_cache(&config);
    let decoded_artifact_cache = test_decoded_artifact_cache(&config);
    let encoder_provider =
        encoder_provider_override.unwrap_or_else(|| test_encoder_provider(&config, &store));
    let shutdown_timeout = Duration::from_secs(config.server.shutdown_timeout_secs);
    let (audit, audit_runtime, audit_node_id) = start_test_audit_with_entitlements(
        &config,
        &store,
        namespace_name_prefix.as_deref(),
        &entitlements,
        &security,
        clock.now(),
    )
    .await;
    let workload_credentials = WorkloadCredentialRegistry::with_admin(&admin_bearer);
    let server_tasks = Arc::new(ServerTaskSupervisor::new());
    let state_security = Arc::clone(&security);
    let state = AppState {
        store: store.clone(),
        clock: clock.clone(),
        security: state_security,
        audit: audit.clone(),
        credential_adapter,
        namespace_manager: Arc::clone(&namespace_manager),
        namespace_name_prefix,
        branch_readiness: branch_readiness.snapshot,
        wal_writer: wal_writer.clone(),
        wal_reader: Arc::new(WalReader::new(store.clone())),
        encoder_provider: Arc::clone(&encoder_provider),
        compactor: compactor.clone(),
        lease_manager: lease_manager.clone(),
        compaction_lifecycle: compaction_lifecycle.clone(),
        server_tasks: Arc::clone(&server_tasks),
        fragment_cache: Arc::clone(&fragment_cache),
        decoded_artifact_cache: Arc::clone(&decoded_artifact_cache),
        config: Arc::new(config),
        trusted_proxies,
        runtime_query_config,
        query_knob_bounds,
        cache: cache.clone(),
        manifest_cache: manifest_cache.clone(),
        hydrator,
        fts_cache: Arc::new(WalFtsCache::new()),
        query_semaphore,
        rate_limiters: Arc::new(DashMap::new()),
    };

    let app = build_router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    let (shutdown_http, mut shutdown_http_rx) = tokio::sync::watch::channel(false);
    let server_task = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            let _ = shutdown_http_rx.changed().await;
        })
        .await
        .unwrap();
    });

    FullTestServer {
        base_url,
        admin_bearer,
        store,
        clock,
        cache,
        cache_dir,
        compactor,
        lease_manager,
        manifest_cache,
        audit,
        audit_node_id,
        security,
        namespace_manager,
        encoder_provider,
        object_store_counter,
        workload_credentials,
        compaction_lifecycle,
        server_tasks,
        audit_runtime: Some(audit_runtime),
        fragment_cache,
        decoded_artifact_cache,
        wal_writer,
        shutdown_compaction,
        compaction_loop_task,
        server_task,
        shutdown_http,
        shutdown_timeout,
    }
}
