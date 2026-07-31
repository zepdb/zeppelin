//! Builds the complete Zeppelin process graph and retires owned server work.
//!
//! [`crate::startup::build_app`] is the composition root between boot-time
//! [`crate::config::Config`] and the HTTP [`axum::Router`]. It validates
//! configuration, probes the configured S3/MinIO or local backend, discovers
//! namespaces, constructs WAL/index/cache/query services, starts optional
//! hydration and the dedicated compaction thread, and places shared handles
//! into [`crate::server::AppState`]. Extracting this work from the binary keeps
//! dependency wiring testable with local object storage.
//!
//! Object storage remains authoritative throughout startup. A successful probe
//! does not cache visibility state; it only proves the backend can answer. When
//! `storage.fail_fast=false`, probe/namespace-scan failures are logged and the
//! server is still assembled so health and API paths can expose the outage.
//! This is an explicit configured boot mode, not a silent local-data fallback.
//!
//! ## Reading map
//!
//! 1. Start with [`crate::startup::resolve_config_path`] and
//!    [`crate::startup::init_logging`] for pre-runtime process setup.
//! 2. Read [`crate::startup::build_app`] from validation through storage probing
//!    and namespace discovery.
//! 3. Continue through its service graph, hydration worker, manifest cache,
//!    lease manager, and compaction thread construction.
//! 4. Read `probe_storage` for the post-client LIST health check.
//! 5. Finish with [`crate::startup::shutdown_background_tasks`] for
//!    signal-and-join semantics.
//!
//! ## Startup lifecycle
//!
//! ```text
//! Config load (main)
//!       |
//!       v
//! validate + CPU budget + metrics
//!       |
//!       v
//! endpoint probe -> construct ZeppelinStore -> LIST probe
//!       |                    |
//!       | unavailable and    +--> namespace discovery (when available)
//!       | fail_fast=false
//!       v
//! construct WAL, caches, compactor, leases, runtime query state
//!       |
//!       +--> optional hydration Tokio task
//!       +--> dedicated compaction OS thread + Tokio runtime
//!       |
//!       v
//! AppState -> Router -> main binds listener
//! ```
//!
//! ## Shutdown ownership
//!
//! ```text
//! main owns watch::Sender + compaction JoinHandle + server lifecycle owners
//!                 |
//!                 | send true; join request mutation + background work against
//!                 | one absolute graceful deadline
//!                 v
//! compaction runtime observes shutdown and exits its OS thread
//!                 |
//!                 | close leased-compaction admission; abort/join heartbeats
//!                 | and authority refresh owners before return
//!                 v
//! success / thread panic / join failure / timeout
//! ```
//!
//! ## Invariants and current limits
//!
//! - Configuration is validated before storage clients or background workers
//!   are created.
//! - Higher layers receive one cloned [`crate::storage::ZeppelinStore`]
//!   abstraction; they do not construct independent S3 clients here.
//! - Per-namespace compaction uses both lease fencing and manifest CAS; startup
//!   creates one process-unique lease holder identity.
//! - The compaction runtime is isolated on its own OS thread. Hydration runs on
//!   the main Tokio runtime and query admission uses a semaphore in
//!   [`crate::server::AppState`].
//! - [`crate::startup::shutdown_background_tasks`] retires request-originated
//!   authoritative mutations, authority refresh tasks, and the compaction
//!   thread. Hydration remains independently queue-owned.
//!
//! TODO(doc): Verify where
//! [`CpuBudget::query_workers`][crate::config::CpuBudget::query_workers] and
//! [`CpuBudget::rayon_threads`][crate::config::CpuBudget::rayon_threads] should
//! be applied. [`crate::startup::build_app`] currently logs both values but only
//! passes `compaction_workers` into a runtime builder.
//!
//! ## Rust concepts used here
//!
//! [`std::sync::Arc`] gives the router and background services shared ownership
//! without a global singleton. Cloning `ZeppelinStore` and `Arc` values clones
//! handles, not remote data. A Tokio [`watch`][tokio::sync::watch] channel
//! carries the latest shutdown flag, while an owned
//! [`std::thread::JoinHandle`] proves exactly one caller can join the compaction
//! thread.
//!
//! [`crate::startup::build_app`] uses `?` across several concrete error types
//! and returns `Box<dyn Error>` because startup can fail in configuration, I/O,
//! storage, or runtime construction. Java would use a common exception
//! superclass; C would need a tagged error plus explicit cleanup of every
//! already-created service.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use dashmap::DashMap;
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

use crate::cache::decoded_cache::DecodedArtifactCache;
use crate::cache::manifest_cache::ManifestCache;
use crate::cache::{
    hydration::{heat_policy_from_config, HydrationConfig, SegmentHydrator},
    DiskCache,
};
use crate::compaction::background::{
    start_compaction_thread, CompactionLifecycle, CompactionThreadOptions, GovernedDeletionWorker,
};
use crate::compaction::Compactor;
use crate::config::{Config, CpuBudget, SecurityMode, StorageBackend};
use crate::embedding::{ConfiguredEncoderProvider, MultiVectorEncoderProvider};
use crate::error::{Result as ZeppelinResult, ZeppelinError};
use crate::fts::wal_cache::WalFtsCache;
use crate::namespace::{BranchReadinessObserver, NamespaceManager};
use crate::runtime_config::{QueryKnobBounds, RuntimeQueryConfig};
use crate::security::{
    AuditRecord, AuditRuntime, EntitlementResolver, Entitlements, Feature, FileLicenseResolver,
    SecurityKernel,
};
use crate::server::{build_router, AppState, ServerTaskSupervisor};
use crate::storage::ZeppelinStore;
use crate::time::Clock;
use crate::wal::{LeaseManager, WalFragmentCache, WalReader, WalWriter};

/// Maximum duration for each endpoint, LIST, or namespace-scan startup probe.
const STORAGE_STARTUP_TIMEOUT: Duration = Duration::from_secs(2);
const LICENSE_OBSERVATION_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

async fn resolve_entitlements(
    resolver: Arc<dyn EntitlementResolver>,
) -> Result<Arc<Entitlements>, ZeppelinError> {
    tokio::task::spawn_blocking(move || resolver.resolve())
        .await
        .map_err(|error| {
            ZeppelinError::Config(format!("license resolver task failed during boot: {error}"))
        })?
        .map(Arc::new)
        .map_err(ZeppelinError::from)
}

#[allow(clippy::manual_unwrap_or, clippy::manual_unwrap_or_default)]
fn observe_license_expiry(entitlements: &Entitlements, now: chrono::DateTime<chrono::Utc>) {
    // `None` is the intentional no-expiry state (community or a test
    // composition), not a recovery path for an invalid licensed value.
    let expiry_seconds = match entitlements.expiry_seconds(now) {
        Some(seconds) => seconds,
        None => 0,
    };
    crate::metrics::LICENSE_EXPIRY_SECONDS.set(expiry_seconds);
    if expiry_seconds < 0 {
        tracing::warn!(
            expiry_seconds,
            management_frozen = entitlements.management_frozen(now),
            "verified Zeppelin license is expired; enforcement remains active"
        );
    }
}

fn spawn_license_observer(
    entitlements: Arc<Entitlements>,
    clock: Clock,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    match changed {
                        Ok(()) if *shutdown.borrow() => return,
                        Ok(()) => continue,
                        Err(_) => return,
                    }
                }
                _ = tokio::time::sleep(LICENSE_OBSERVATION_INTERVAL) => {
                    observe_license_expiry(&entitlements, clock.now());
                }
            }
        }
    })
}

/// Unique lifecycle ownership for maintenance and request-spawned authoritative work.
pub struct BackgroundTasks {
    shutdown_tx: watch::Sender<bool>,
    compaction_handle: std::thread::JoinHandle<()>,
    license_observer: tokio::task::JoinHandle<()>,
    compaction_lifecycle: CompactionLifecycle,
    server_tasks: Arc<ServerTaskSupervisor>,
    security: Option<Arc<SecurityKernel>>,
}

impl BackgroundTasks {
    /// Combine already-spawned services under one unique shutdown owner.
    #[must_use]
    pub fn from_parts(
        shutdown_tx: watch::Sender<bool>,
        compaction_handle: std::thread::JoinHandle<()>,
        license_observer: tokio::task::JoinHandle<()>,
    ) -> Self {
        Self {
            shutdown_tx,
            compaction_handle,
            license_observer,
            compaction_lifecycle: CompactionLifecycle::new(),
            server_tasks: Arc::new(ServerTaskSupervisor::new()),
            security: None,
        }
    }

    /// Attach HTTP-owned authority and mutation-task lifecycle to this process owner.
    #[must_use]
    pub fn with_server_lifecycle(
        mut self,
        security: Arc<SecurityKernel>,
        server_tasks: Arc<ServerTaskSupervisor>,
        compaction_lifecycle: CompactionLifecycle,
    ) -> Self {
        self.security = Some(security);
        self.server_tasks = server_tasks;
        self.compaction_lifecycle = compaction_lifecycle;
        self
    }
}

/// Resolves the optional configuration file path from process context.
///
/// Priority is `ZEPPELIN_CONFIG`, then `./zeppelin.toml` in the current working
/// directory, then no path so [`Config::load`] fails closed on the missing
/// explicit `[security]` contract. The environment value is returned verbatim,
/// including an empty or nonexistent path; validation/opening belongs to the
/// loader.
///
/// # Returns
///
/// `Some(path)` from the environment or existing default file, otherwise
/// `None`.
///
/// # Side Effects
///
/// Reads one environment variable and may perform one local filesystem
/// existence check. It does not open or parse the file.
///
/// # Examples
///
/// `ZEPPELIN_CONFIG=/etc/zeppelin.toml` wins even when `./zeppelin.toml`
/// exists. With neither input, the caller receives `None`; loading then fails
/// the required explicit `[security]` contract.
pub fn resolve_config_path() -> Option<String> {
    std::env::var("ZEPPELIN_CONFIG").ok().or_else(|| {
        let default = "zeppelin.toml";
        std::path::Path::new(default)
            .exists()
            .then(|| default.to_string())
    })
}

/// Installs the process-global tracing subscriber from logging configuration.
///
/// A valid `RUST_LOG` filter overrides the configured level. An absent **or
/// invalid** environment filter falls back to `config.logging.level`. Format
/// `"json"` selects structured JSON; every other value selects text (validated
/// configuration normally restricts the choice earlier).
///
/// # Parameters
///
/// - `config`: Validated boot configuration containing log level and format.
///
/// # Returns
///
/// Returns unit after installing the subscriber.
///
/// # Panics
///
/// Panics if a global tracing subscriber was already installed or if the
/// selected formatter cannot become global. Call exactly once per process.
///
/// # Side Effects
///
/// Reads `RUST_LOG` and sets process-global tracing state.
///
/// # Examples
///
/// With JSON format and no `RUST_LOG`, an `info` configured level produces
/// structured events at `info` and above. `RUST_LOG=zeppelin=debug` overrides
/// that filter for this process.
pub fn init_logging(config: &Config) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.logging.level));

    match config.logging.format.as_str() {
        "json" => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .init();
        }
        _ => {
            tracing_subscriber::fmt().with_env_filter(filter).init();
        }
    }
}

/// Validates configuration and constructs the router plus background services.
///
/// This is the process composition root. Construction is ordered so validation
/// and storage probes occur before the compaction thread starts. When storage is
/// unavailable and `fail_fast` is disabled, the function skips namespace
/// discovery but still builds the service graph; requests continue to use the
/// configured remote store and surface its errors.
///
/// # Parameters
///
/// - `config`: Owned boot configuration. Validation runs again here even if the
///   caller loaded it through [`Config::load`]. The final value is moved into
///   [`AppState`].
///
/// # Returns
///
/// A ready [`Router`], an owned [`BackgroundTasks`] lifecycle containing the
/// maintenance, request-mutation, lease-heartbeat, and authority-refresh
/// owners, and the owned [`AuditRuntime`] that must drain after HTTP stops
/// accepting work. The caller must bind/serve the router, drain audit, and
/// eventually retire the background owner.
///
/// # Errors
///
/// Returns configuration, CPU-budget, storage-client/probe, namespace-scan,
/// cache/hydration, or trusted-proxy construction errors. With
/// `storage.fail_fast=true`, probe and scan failures abort startup. Metrics or
/// other process-local initialization may already have happened before a later
/// error, but no router is returned.
///
/// # Panics
///
/// Panics if the operating system refuses to spawn the dedicated compaction
/// thread. Failure to build its Tokio runtime occurs inside that new thread and
/// is later reported as a thread panic during shutdown.
///
/// # Side Effects
///
/// Reads environment/OS CPU information, initializes global metrics, probes
/// object storage, scans namespaces, creates local cache directories/state,
/// optionally spawns a hydration task, and starts a dedicated compaction OS
/// thread. It emits structured startup logs and metrics.
///
/// # Consistency
///
/// Namespace discovery reads authoritative storage. Manifest cache and disk
/// cache instances are empty optimizations at construction. The generated lease
/// holder ID is unique to this process and all compaction commits still require
/// fencing plus manifest CAS.
///
/// # Performance
///
/// Endpoint probing, storage LIST probing, and namespace scanning are each
/// bounded by two seconds. Discovery may perform object-store LIST/GET work for
/// existing namespaces. Component allocation is otherwise process-local.
///
/// # Examples
///
/// A local-storage test receives a router, owned background tasks, and an audit
/// runtime without S3. With a dead S3 endpoint and fail-fast enabled,
/// startup returns before serving; with fail-fast disabled, it logs the outage,
/// skips discovery, and returns a router whose storage-dependent requests may
/// fail.
///
/// # Rust Notes for Java/C Engineers
///
/// `config` is moved into this function and ultimately into an `Arc` inside
/// state. Service handles are cloned explicitly when several owners need them.
/// The tuple return transfers unique responsibility for the audit runtime,
/// background-task and audit-runtime owners to `main`; Rust prevents draining
/// or joining either owner twice.
pub async fn build_app(config: Config) -> ZeppelinResult<(Router, BackgroundTasks, AuditRuntime)> {
    let resolver = Arc::new(FileLicenseResolver::new(
        config.security.license_path.clone(),
    ));
    build_app_with_entitlement_resolver(config, resolver).await
}

/// Build the production process graph with an explicitly selected resolver.
///
/// This private seam supports crate-owned unit tests and future managed-service
/// composition without exposing resolver injection to downstream callers. The
/// self-hosted server entrypoint always calls [`build_app`], which selects
/// [`FileLicenseResolver`] and the embedded production verification key.
async fn build_app_with_entitlement_resolver(
    config: Config,
    resolver: Arc<dyn EntitlementResolver>,
) -> ZeppelinResult<(Router, BackgroundTasks, AuditRuntime)> {
    if let Err(error) = config.validate() {
        tracing::error!(error = %error, "invalid configuration; refusing to boot");
        return Err(error);
    }
    config.warn_if_unsafe_gc_horizon_override();

    tracing::info!("zeppelin starting");

    // Detect CPU budget and log allocation
    let cpu_budget = CpuBudget::auto()?;
    tracing::info!(
        query_workers = cpu_budget.query_workers,
        compaction_workers = cpu_budget.compaction_workers,
        rayon_threads = cpu_budget.rayon_threads,
        "CPU budget allocated"
    );

    tracing::info!(
        host = %config.server.host,
        port = config.server.port,
        bucket = %config.storage.bucket,
        backend = %config.storage.backend,
        cache_dir = %config.cache.dir.display(),
        cache_max_size_gb = config.cache.max_size_gb,
        wal_fragment_cache_max_mb = config.cache.wal_fragment_cache_max_mb,
        decoded_artifact_cache_max_mb = config.cache.decoded_artifact_cache_max_mb,
        compaction_interval_secs = config.compaction.interval_secs,
        max_wal_fragments = config.compaction.max_wal_fragments_before_compact,
        max_wal_age_secs = config.compaction.max_wal_age_before_compact_secs,
        max_wal_bytes = config.compaction.max_wal_bytes_before_compact,
        "configuration loaded"
    );

    // Initialize metrics
    crate::metrics::init();
    for mode in [SecurityMode::Enforced, SecurityMode::OpenUnsafe] {
        crate::metrics::SECURITY_MODE
            .with_label_values(&[mode.as_str()])
            .set(if mode == config.security.mode { 1 } else { 0 });
    }
    if config.security.mode == SecurityMode::OpenUnsafe {
        tracing::warn!(
            security_mode = config.security.mode.as_str(),
            "!!! ZEPPELIN SECURITY IS OPEN_UNSAFE: AUTHENTICATION AND AUTHORIZATION ARE DISABLED !!!"
        );
    }

    let clock = Clock::system();
    let entitlements = resolve_entitlements(resolver).await?;
    observe_license_expiry(&entitlements, clock.now());
    let durable_audit_enabled = config.security.audit_s3 && entitlements.has(Feature::AuditS3);
    if durable_audit_enabled && config.storage.backend != StorageBackend::S3 {
        return Err(ZeppelinError::Config(
            "durable audit requires an S3-compatible backend with ETag conditional PUT support"
                .to_string(),
        ));
    }

    let mut storage_available = true;
    match ZeppelinStore::probe_configured_endpoint(&config.storage, STORAGE_STARTUP_TIMEOUT).await {
        Ok(()) => {}
        Err(error) if config.storage.fail_fast => {
            let error = ZeppelinError::Config(format!("storage health probe failed: {error}"));
            tracing::error!(
                error = %error,
                backend = %config.storage.backend,
                bucket = %config.storage.bucket,
                "storage health probe failed; refusing to boot"
            );
            return Err(error);
        }
        Err(error) => {
            storage_available = false;
            tracing::warn!(
                error = %error,
                backend = %config.storage.backend,
                bucket = %config.storage.bucket,
                "storage health probe failed; continuing because storage.fail_fast=false"
            );
        }
    }
    // Initialize storage
    let store = ZeppelinStore::from_config(&config.storage)?;
    if storage_available {
        match probe_storage(&store).await {
            Ok(()) => tracing::info!("storage health probe succeeded"),
            Err(error) if config.storage.fail_fast => {
                tracing::error!(
                    error = %error,
                    backend = %config.storage.backend,
                    bucket = %config.storage.bucket,
                    "storage health probe failed; refusing to boot"
                );
                return Err(error);
            }
            Err(error) => {
                storage_available = false;
                tracing::warn!(
                    error = %error,
                    backend = %config.storage.backend,
                    bucket = %config.storage.bucket,
                    "storage health probe failed; continuing because storage.fail_fast=false"
                );
            }
        }
    }

    let (security, credential_adapter) = SecurityKernel::from_resolved_entitlements(
        store.clone(),
        &config.security,
        clock.clone(),
        Arc::clone(&entitlements),
    )
    .await?;
    if durable_audit_enabled {
        security.install_object_signer(&store)?;
    }
    let audit_now = clock.now();
    let (audit, audit_runtime) = if durable_audit_enabled {
        AuditRuntime::start_for_published_signer_at(
            store.clone(),
            Duration::from_secs(config.security.audit_flush_secs),
            audit_now,
        )
        .await?
    } else {
        AuditRuntime::tracing_only(format!("zeppelin-{}", uuid::Uuid::new_v4()))?
    };
    let node_id = audit.node_id().to_string();
    if config.security.mode == SecurityMode::OpenUnsafe {
        let record = AuditRecord::open_unsafe_boot(audit_now, audit.node_id());
        audit.submit_buffered(record)?;
    }
    if entitlements
        .expiry_seconds(audit_now)
        .is_some_and(|seconds| seconds < 0)
    {
        audit.submit_buffered(AuditRecord::license_expired_boot(
            audit_now,
            audit.node_id(),
        ))?;
    }

    // Initialize namespace manager and scan existing namespaces
    let namespace_manager = Arc::new(
        NamespaceManager::with_clock(
            store.clone(),
            Duration::from_millis(config.cache.namespace_registry_ttl_ms),
            clock.clone(),
        )
        .with_preservation_service(security.preservation_service().cloned()),
    );
    if storage_available {
        match tokio::time::timeout(
            STORAGE_STARTUP_TIMEOUT,
            namespace_manager.scan_and_register(),
        )
        .await
        {
            Ok(Ok(count)) => tracing::info!(count, "registered existing namespaces"),
            Ok(Err(error)) if config.storage.fail_fast => {
                let error =
                    ZeppelinError::Config(format!("namespace scan failed during startup: {error}"));
                tracing::error!(
                    error = %error,
                    backend = %config.storage.backend,
                    bucket = %config.storage.bucket,
                    "failed to scan namespaces on startup; refusing to boot"
                );
                return Err(error);
            }
            Ok(Err(error)) => {
                tracing::warn!(
                    error = %error,
                    "failed to scan namespaces on startup; continuing because storage.fail_fast=false"
                );
            }
            Err(_elapsed) if config.storage.fail_fast => {
                let error = ZeppelinError::Config(format!(
                    "namespace scan timed out after {}s during startup",
                    STORAGE_STARTUP_TIMEOUT.as_secs()
                ));
                tracing::error!(
                    error = %error,
                    backend = %config.storage.backend,
                    bucket = %config.storage.bucket,
                    "failed to scan namespaces on startup; refusing to boot"
                );
                return Err(error);
            }
            Err(_elapsed) => {
                tracing::warn!(
                    timeout_secs = STORAGE_STARTUP_TIMEOUT.as_secs(),
                    "namespace scan timed out on startup; continuing because storage.fail_fast=false"
                );
            }
        }
    } else {
        tracing::warn!("skipping namespace scan because storage health probe failed");
    }

    // Initialize WAL writer and reader
    let wal_writer = Arc::new(WalWriter::with_clock(store.clone(), clock.clone()));
    let wal_reader = Arc::new(WalReader::new(store.clone()));

    // Initialize disk cache
    let cache = Arc::new(DiskCache::new(&config.cache)?);

    let hydrator = if config.cache.hydration_enabled {
        let hydration_config = HydrationConfig::from_cache_config(
            &config.cache,
            Duration::from_secs(config.server.request_timeout_secs),
        )?;
        let policy = heat_policy_from_config(&config.cache)?;
        Some(SegmentHydrator::start(
            store.clone(),
            cache.clone(),
            policy,
            hydration_config,
        ))
    } else {
        None
    };

    // Initialize manifest cache — drops one S3 GET from eventual queries while
    // strong queries still verify freshness against S3.
    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_millis(
        config.cache.manifest_cache_ttl_ms,
    )));

    // Initialize compactor
    let compactor = Arc::new(
        Compactor::with_clock(
            store.clone(),
            WalReader::new(store.clone()),
            config.compaction.clone(),
            config.indexing.clone(),
            Duration::from_secs(config.gc.compaction_upload_window_secs),
            clock.clone(),
        )
        .with_preservation_service(security.preservation_service().cloned()),
    );

    // Per-namespace compaction lease: only one node compacts a namespace at
    // a time. The holder ID is unique per process; the fencing token from the
    // lease is threaded into every compaction commit.
    let lease_manager = Arc::new(LeaseManager::with_clock(
        store.clone(),
        node_id,
        Duration::from_secs(config.compaction.lease_duration_secs),
        clock.clone(),
    ));
    let compaction_lifecycle = CompactionLifecycle::new();
    // One snapshot shared by the scanning worker and the readiness handler.
    let branch_readiness = BranchReadinessObserver::unscoped();
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
    let encoder_provider: Arc<dyn MultiVectorEncoderProvider> =
        Arc::new(ConfiguredEncoderProvider::new(store.clone(), &config.mmli));

    // Spawn background compaction on a dedicated runtime (CPU isolation from queries)
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let compaction_handle = start_compaction_thread(
        compactor.clone(),
        namespace_manager.clone(),
        shutdown_rx,
        manifest_cache.clone(),
        lease_manager.clone(),
        cache.clone(),
        deletion_worker,
        encoder_provider,
        compaction_lifecycle.clone(),
        CompactionThreadOptions {
            compaction_workers: cpu_budget.compaction_workers,
            gc_config: config.gc.clone(),
            mmli: config.mmli.clone(),
        },
    );

    // Initialize WAL FTS cache (pre-tokenized BM25 data)
    let fts_cache = Arc::new(WalFtsCache::new());
    let fragment_cache_max_bytes = config
        .cache
        .wal_fragment_cache_max_mb
        .checked_mul(1024 * 1024)
        .ok_or_else(|| {
            ZeppelinError::Config(
                "cache.wal_fragment_cache_max_mb overflows the platform byte size".to_string(),
            )
        })?;
    let fragment_cache = Arc::new(WalFragmentCache::new(fragment_cache_max_bytes));
    let decoded_artifact_cache_max_bytes = config
        .cache
        .decoded_artifact_cache_max_mb
        .checked_mul(1024 * 1024)
        .ok_or_else(|| {
            ZeppelinError::Config(
                "cache.decoded_artifact_cache_max_mb overflows the platform byte size".to_string(),
            )
        })?;
    let decoded_artifact_cache =
        Arc::new(DecodedArtifactCache::new(decoded_artifact_cache_max_bytes));

    // Runtime query config uses a std-only RwLock<Arc<_>> snapshot holder.
    // Bounds remain boot-time values so mutable defaults cannot outgrow limits.
    let runtime_query_config = Arc::new(RuntimeQueryConfig::from_config(&config));
    let query_knob_bounds = QueryKnobBounds::from_config(&config);
    let query_knobs = runtime_query_config.snapshot();
    crate::metrics::RERANK_COALESCE_GAP_BYTES
        .set(i64::try_from(query_knobs.rerank_coalesce_gap_bytes).unwrap_or(i64::MAX));
    tracing::info!(
        rerank_coalesce_gap_bytes = query_knobs.rerank_coalesce_gap_bytes,
        default_nprobe = query_knobs.default_nprobe,
        default_top_k = query_knobs.default_top_k,
        bm25_max_full_scan_clusters = query_knobs.bm25_max_full_scan_clusters,
        bm25_max_full_scan_vectors = query_knobs.bm25_max_full_scan_vectors,
        "effective runtime query config"
    );
    drop(query_knobs);

    // Build application state
    let query_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_queries,
    ));
    let rate_limiters = Arc::new(DashMap::new());
    let trusted_proxies = Arc::from(crate::server::parse_trusted_proxies(
        &config.server.trusted_proxies,
    )?);
    let license_observer = spawn_license_observer(
        Arc::clone(&entitlements),
        clock.clone(),
        shutdown_tx.subscribe(),
    );
    let credential_adapter: Arc<dyn crate::security::CredentialAdapter> = credential_adapter;
    let background_security = Arc::clone(&security);
    let server_tasks = Arc::new(ServerTaskSupervisor::new());
    let state = AppState {
        store,
        clock,
        security,
        audit,
        credential_adapter,
        namespace_manager,
        namespace_name_prefix: None,
        branch_readiness: branch_readiness.snapshot,
        wal_writer,
        wal_reader,
        compactor,
        lease_manager,
        compaction_lifecycle: compaction_lifecycle.clone(),
        server_tasks: Arc::clone(&server_tasks),
        config: Arc::new(config),
        trusted_proxies,
        runtime_query_config,
        query_knob_bounds,
        cache,
        manifest_cache,
        hydrator,
        fts_cache,
        fragment_cache,
        decoded_artifact_cache,
        query_semaphore,
        rate_limiters,
    };

    // Build router
    let app = build_router(state);

    Ok((
        app,
        BackgroundTasks::from_parts(shutdown_tx, compaction_handle, license_observer)
            .with_server_lifecycle(background_security, server_tasks, compaction_lifecycle),
        audit_runtime,
    ))
}

/// Verifies a constructed store by listing top-level common prefixes.
///
/// The earlier configured-endpoint probe checks network/backend reachability;
/// this second probe exercises the actual [`ZeppelinStore`] instance used by
/// higher layers.
///
/// # Parameters
///
/// - `store`: Constructed storage abstraction to probe.
///
/// # Returns
///
/// `Ok(())` when the empty-prefix LIST completes within two seconds.
///
/// # Errors
///
/// Maps storage LIST failure or timeout into [`ZeppelinError::Config`] with
/// startup context.
///
/// # Side Effects
///
/// Performs one read-only object-store LIST request.
///
/// # Examples
///
/// An empty bucket still succeeds because the operation need not find a
/// namespace. A credential error or stalled endpoint fails the probe.
async fn probe_storage(store: &ZeppelinStore) -> ZeppelinResult<()> {
    match tokio::time::timeout(STORAGE_STARTUP_TIMEOUT, store.list_common_prefixes("")).await {
        Ok(result) => result.map(|_| ()).map_err(|error| {
            ZeppelinError::Config(format!("storage health probe failed: {error}"))
        }),
        Err(_elapsed) => Err(ZeppelinError::Config(format!(
            "storage health probe timed out after {}s",
            STORAGE_STARTUP_TIMEOUT.as_secs()
        ))),
    }
}

/// Signals and joins every owned non-audit background task within a deadline.
///
/// Sending is best-effort because the thread may already have stopped. Joining
/// runs on Tokio's blocking pool so it does not block an async worker. A timeout
/// drops the async join handle, but the underlying blocking join/thread may
/// continue until the compaction thread eventually exits.
///
/// # Parameters
///
/// - `backgrounds`: Unique compaction and license-observer lifecycle owner.
/// - `timeout_duration`: One shared graceful-drain budget for request mutation
///   work and the compaction/license background joins.
///
/// # Returns
///
/// `Ok(())` only when request-originated authoritative mutations and the
/// compaction/license observers complete their cooperative drain inside the
/// configured graceful budget, followed by completed heartbeat and
/// authority-refresh cancellation barriers. The latter are non-detaching safety
/// cleanup and may outlast the cooperative budget.
///
/// # Errors
///
/// Returns configuration errors when an owned task panics, the blocking join
/// task fails, or the shared graceful deadline expires. The shutdown send
/// result itself is ignored because a closed receiver already means no active
/// listener remains.
///
/// # Side Effects
///
/// Publishes `true` on the shared watch channel, then drains accepted
/// request-originated authoritative mutations and joins compaction/license work
/// against one absolute deadline. It subsequently closes leased-compaction
/// admission and joins the heartbeat and authority-refresh cancellation
/// barriers. Those barriers prove that request-owned mutation, lease-renewal,
/// publication, and authority-refresh capability cannot survive into a
/// replacement. If the compaction OS thread or license observer misses the
/// graceful deadline, this function returns a loud error; process exit, rather
/// than a replacement in the same process, contains that remaining background
/// work. Hydration remains independently queue-owned.
///
/// # Examples
///
/// After Axum finishes graceful HTTP shutdown, `main` passes the unique owner
/// with the configured timeout. Normal exit returns success; a panic becomes a
/// startup/configuration-class process error.
///
/// # Rust Notes for Java/C Engineers
///
/// Moving the contained compaction handle into `spawn_blocking` transfers join
/// ownership to that closure. Java's `Thread.join` can be invoked through shared
/// references; C's `pthread_join` requires convention to avoid double joins.
/// Rust makes a second join impossible after the move.
pub async fn shutdown_background_tasks(
    backgrounds: BackgroundTasks,
    timeout_duration: Duration,
) -> ZeppelinResult<()> {
    let BackgroundTasks {
        shutdown_tx,
        compaction_handle,
        license_observer,
        compaction_lifecycle,
        server_tasks,
        security,
    } = backgrounds;
    let deadline = tokio::time::Instant::now() + timeout_duration;
    // Shared background workers must see shutdown before this owner waits for
    // foreground mutation work. Existing periodic work receives its normal
    // completion path; no new HTTP request can admit manual work after Axum's
    // prior graceful drain.
    let _ = shutdown_tx.send(true);
    let request_task_result = server_tasks
        .join_until(deadline)
        .await
        .map_err(ZeppelinError::from);
    let join_task = tokio::task::spawn_blocking(move || compaction_handle.join());

    let background_result = match tokio::time::timeout_at(deadline, async {
        tokio::join!(join_task, license_observer)
    })
    .await
    {
        Ok((Ok(Ok(())), Ok(()))) => Ok(()),
        Ok((Ok(Err(_panic)), observer)) => Err(ZeppelinError::Config(format!(
            "background compaction thread panicked during shutdown; license observer: {observer:?}"
        ))),
        Ok((Err(error), observer)) => Err(ZeppelinError::Config(format!(
            "failed to join background compaction thread: {error}; license observer: {observer:?}"
        ))),
        Ok((Ok(Ok(())), Err(error))) => Err(ZeppelinError::Config(format!(
            "license observer task failed during shutdown: {error}"
        ))),
        Err(_elapsed) => Err(ZeppelinError::Config(format!(
            "timed out after {}s waiting for background task shutdown",
            timeout_duration.as_secs()
        ))),
    };
    let heartbeat_result = compaction_lifecycle
        .close_and_abort_heartbeats()
        .await
        .map_err(ZeppelinError::from);
    if let Some(security) = security {
        security.shutdown_refresh_tasks().await;
    }
    let errors = [request_task_result, background_result, heartbeat_result]
        .into_iter()
        .filter_map(Result::err)
        .map(|error| error.to_string())
        .collect::<Vec<_>>();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ZeppelinError::Config(errors.join("; ")))
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Local-storage startup tests for configuration discovery and thread cleanup.
    //!
    //! The two configuration-path tests mutate process-global environment and
    //! current-directory state. A shared mutex serializes them, and each test
    //! restores the previous values for later tests.

    use super::*;
    use crate::config::ApiKeyConfig;
    use crate::security::{canonical_payload_bytes, LicenseLimits, LicensePayload, SignedLicense};
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use chrono::{Duration as ChronoDuration, TimeZone, Utc};
    use ed25519_dalek::{Signer, SigningKey};
    use sha2::{Digest, Sha256};
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use tracing::field::{Field, Visit};
    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::Layer;

    static PROCESS_CONFIG_LOCK: Mutex<()> = Mutex::new(());
    static LICENSE_STARTUP_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    const TEST_LICENSE_SEED: [u8; 32] = [7_u8; 32];

    #[derive(Clone, Default)]
    struct CapturedLicenseWarnings(Arc<Mutex<Vec<Option<bool>>>>);

    #[derive(Default)]
    struct LicenseWarningVisitor {
        is_license_warning: bool,
        management_frozen: Option<bool>,
    }

    impl Visit for LicenseWarningVisitor {
        fn record_bool(&mut self, field: &Field, value: bool) {
            if field.name() == "management_frozen" {
                self.management_frozen = Some(value);
            }
        }

        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message"
                && format!("{value:?}").contains("verified Zeppelin license is expired")
            {
                self.is_license_warning = true;
            }
        }
    }

    impl<S> Layer<S> for CapturedLicenseWarnings
    where
        S: Subscriber,
    {
        fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
            if *event.metadata().level() != Level::WARN {
                return;
            }
            let mut visitor = LicenseWarningVisitor::default();
            event.record(&mut visitor);
            if visitor.is_license_warning {
                self.0
                    .lock()
                    .unwrap_or_else(|_| panic!("license warning capture lock poisoned"))
                    .push(visitor.management_frozen);
            }
        }
    }

    /// Builds a local-backend fixture whose compactor stays idle during a test.
    ///
    /// # Parameters
    ///
    /// - `tmp`: Temporary directory whose handle remains alive for the complete
    ///   application/test lifetime.
    ///
    /// # Returns
    ///
    /// A default configuration redirected to isolated local storage/cache paths
    /// with a long compaction interval.
    fn test_config(tmp: &tempfile::TempDir) -> Config {
        let mut config = Config::default();
        config.security.mode = SecurityMode::OpenUnsafe;
        config.storage.backend = StorageBackend::Local;
        config.storage.bucket = tmp.path().join("storage").to_string_lossy().to_string();
        config.cache.dir = tmp.path().join("cache");
        config.cache.max_size_gb = 1; // minimal but non-zero
        config.compaction.interval_secs = 9999; // don't trigger during test
        config
    }

    fn signed_license(
        features: Vec<Feature>,
        issued_at: chrono::DateTime<Utc>,
        expires_at: chrono::DateTime<Utc>,
    ) -> SignedLicense {
        let payload = LicensePayload {
            customer_id: "customer:test".to_string(),
            customer_name: "Test Customer".to_string(),
            issued_at,
            expires_at,
            features,
            limits: LicenseLimits::default(),
        };
        let signing_key = SigningKey::from_bytes(&TEST_LICENSE_SEED);
        let signature = signing_key.sign(&canonical_payload_bytes(&payload).unwrap());
        SignedLicense::new(payload, URL_SAFE_NO_PAD.encode(signature.to_bytes()))
    }

    fn licensed_startup_config(
        root: &tempfile::TempDir,
        license_path: &std::path::Path,
    ) -> (Config, String) {
        let mut config = Config::default();
        let secret = URL_SAFE_NO_PAD.encode([1_u8; 32]);
        let digest = Sha256::digest(secret.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let key_id = "zpk1_license_startup";
        let token_signing_key_path = root.path().join("token-signing.key");
        std::fs::write(&token_signing_key_path, "11".repeat(32)).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                &token_signing_key_path,
                std::fs::Permissions::from_mode(0o600),
            )
            .unwrap();
        }
        config.security.set_cursor_hmac_key_hex("42".repeat(32));
        config.security.token_signing_key_path =
            token_signing_key_path.to_string_lossy().into_owned();
        config.security.api_keys.push(ApiKeyConfig {
            key_id: key_id.to_string(),
            name: "license-startup-admin".to_string(),
            sha256_hex: digest,
            actions: vec!["*".to_string()],
            namespaces: vec!["*".to_string()],
            expires_at: None,
        });
        config.security.license_path = license_path.to_string_lossy().into_owned();
        if std::env::var("TEST_BACKEND").as_deref() == Ok("minio") {
            config.storage.backend = StorageBackend::S3;
            config.storage.bucket = std::env::var("ZEPPELIN_LICENSE_TEST_BUCKET").expect(
                "MinIO signed-startup tests require an isolated ZEPPELIN_LICENSE_TEST_BUCKET",
            );
            config.storage.s3_region = Some("us-east-1".to_string());
            config.storage.s3_endpoint = Some(
                std::env::var("MINIO_ENDPOINT")
                    .unwrap_or_else(|_| "http://localhost:9000".to_string()),
            );
            config.storage.s3_access_key_id = Some(
                std::env::var("MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string()),
            );
            config.storage.s3_secret_access_key = Some(
                std::env::var("MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string()),
            );
            config.storage.s3_allow_http = true;
        } else {
            config.storage.backend = StorageBackend::Local;
            config.storage.bucket = root.path().join("objects").to_string_lossy().into_owned();
        }
        config.cache.dir = root.path().join("cache");
        config.compaction.interval_secs = 3_600;
        (config, format!("{key_id}.{secret}"))
    }

    async fn clear_license_startup_store(config: &Config) {
        let store = ZeppelinStore::from_config(&config.storage).unwrap();
        store.delete_prefix("_security/").await.unwrap();
        store.delete_prefix("_audit/").await.unwrap();
    }

    /// Gives `ZEPPELIN_CONFIG` priority and restores its prior process value.
    #[test]
    fn test_resolve_config_path_from_env() {
        let _lock = PROCESS_CONFIG_LOCK.lock().unwrap();
        // Save original value
        let original = std::env::var("ZEPPELIN_CONFIG").ok();

        std::env::set_var("ZEPPELIN_CONFIG", "foo.toml");
        let path = resolve_config_path();

        // Restore original value
        match original {
            Some(v) => std::env::set_var("ZEPPELIN_CONFIG", v),
            None => std::env::remove_var("ZEPPELIN_CONFIG"),
        }

        assert_eq!(path, Some("foo.toml".to_string()));
    }

    /// Returns no path when neither environment nor working directory provides one.
    ///
    /// The test restores both process-global inputs before asserting.
    #[test]
    fn test_resolve_config_path_none() {
        let _lock = PROCESS_CONFIG_LOCK.lock().unwrap();
        // Save original values
        let original_env = std::env::var("ZEPPELIN_CONFIG").ok();
        let original_dir = std::env::current_dir().unwrap();

        // Temporarily unset env var and move to temp dir with no config file
        std::env::remove_var("ZEPPELIN_CONFIG");
        let temp_dir = tempfile::tempdir().unwrap();
        std::env::set_current_dir(&temp_dir).unwrap();

        let path = resolve_config_path();

        // Restore
        std::env::set_current_dir(original_dir).unwrap();
        if let Some(v) = original_env {
            std::env::set_var("ZEPPELIN_CONFIG", v);
        }

        assert_eq!(path, None);
    }

    /// Builds the complete local service graph and joins its compaction thread.
    #[tokio::test]
    async fn test_build_app_local_storage() {
        let _startup_guard = LICENSE_STARTUP_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(&tmp);

        let result = build_app(config).await;
        assert!(result.is_ok());

        let (_router, background_tasks, audit_runtime) = result.unwrap();

        audit_runtime.shutdown().await.unwrap();
        shutdown_background_tasks(background_tasks, Duration::from_secs(1))
            .await
            .unwrap();
    }

    /// Accepts an empty authoritative local namespace scan during startup.
    #[tokio::test]
    async fn test_build_app_with_namespace_scan() {
        let _startup_guard = LICENSE_STARTUP_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(&tmp);

        // build_app should successfully scan (even though there are no namespaces)
        let result = build_app(config).await;
        assert!(result.is_ok());

        let (_router, background_tasks, audit_runtime) = result.unwrap();
        audit_runtime.shutdown().await.unwrap();
        shutdown_background_tasks(background_tasks, Duration::from_secs(1))
            .await
            .unwrap();
    }

    /// Proves the watch signal and blocking join finish without hanging.
    #[tokio::test]
    async fn test_graceful_shutdown() {
        let _startup_guard = LICENSE_STARTUP_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(&tmp);

        let (router, background_tasks, audit_runtime) = build_app(config).await.unwrap();

        // Send shutdown signal
        audit_runtime.shutdown().await.unwrap();
        shutdown_background_tasks(background_tasks, Duration::from_secs(1))
            .await
            .unwrap();

        // If we get here without hanging, shutdown worked
        drop(router);
    }

    /// A cfg(test)-signed file traverses the real file resolver and production
    /// graph before exposing licensed RBAC routes.
    ///
    /// Boot bootstraps the policy head under the global publication lease,
    /// which releases through an ETag compare-and-swap. `LocalFileSystem`
    /// answers `PutMode::Update` with `NotImplemented`, so this case requires
    /// a backend that actually implements conditional writes.
    #[tokio::test]
    async fn licensed_file_boot_enables_rbac_routes() {
        if std::env::var("TEST_BACKEND").as_deref() != Ok("minio") {
            return;
        }
        let _startup_guard = LICENSE_STARTUP_LOCK.lock().await;
        let root = tempfile::TempDir::new().unwrap();
        let license_path = root.path().join("license.json");
        std::fs::write(
            &license_path,
            serde_json::to_vec(&signed_license(
                vec![Feature::Rbac, Feature::Constraints],
                Utc::now() - ChronoDuration::days(1),
                Utc::now() + ChronoDuration::days(30),
            ))
            .unwrap(),
        )
        .unwrap();
        let (config, admin_bearer) = licensed_startup_config(&root, &license_path);
        clear_license_startup_store(&config).await;
        let cleanup_config = config.clone();
        let (app, background_tasks, audit_runtime) = build_app(config).await.unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_http, mut shutdown_http_rx) = tokio::sync::watch::channel(false);
        let server = tokio::spawn(async move {
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

        let response = reqwest::Client::new()
            .get(format!("http://{address}/v1/security/keys"))
            .bearer_auth(&admin_bearer)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);

        let _ = shutdown_http.send(true);
        server.await.unwrap();
        audit_runtime.shutdown().await.unwrap();
        shutdown_background_tasks(background_tasks, Duration::from_secs(1))
            .await
            .unwrap();
        clear_license_startup_store(&cleanup_config).await;
    }

    /// Expired signed startup exports a negative gauge and, on real S3/MinIO,
    /// retains the licensed durable sink and persists the boot event.
    ///
    /// The local filesystem backend does not implement ETag conditional PUT.
    /// That branch must reject this configuration explicitly instead of
    /// silently weakening the single-writer audit protocol.
    #[tokio::test]
    async fn expired_file_boot_exports_metric_and_durable_audit() {
        let _startup_guard = LICENSE_STARTUP_LOCK.lock().await;
        let root = tempfile::TempDir::new().unwrap();
        let license_path = root.path().join("expired-license.json");
        std::fs::write(
            &license_path,
            serde_json::to_vec(&signed_license(
                vec![Feature::Rbac, Feature::Constraints, Feature::AuditS3],
                Utc::now() - ChronoDuration::days(30),
                Utc::now() - ChronoDuration::days(1),
            ))
            .unwrap(),
        )
        .unwrap();
        let (config, _admin_bearer) = licensed_startup_config(&root, &license_path);
        clear_license_startup_store(&config).await;
        let storage_config = config.storage.clone();
        let cleanup_config = config.clone();
        let startup = build_app(config).await;

        assert!(crate::metrics::LICENSE_EXPIRY_SECONDS.get() < 0);
        if storage_config.backend != StorageBackend::S3 {
            let error = match startup {
                Ok(_) => panic!("local durable-audit startup unexpectedly succeeded"),
                Err(error) => error,
            };
            assert_eq!(
                error.to_string(),
                "config error: durable audit requires an S3-compatible backend with ETag conditional PUT support"
            );
            clear_license_startup_store(&cleanup_config).await;
            return;
        }

        let (router, background_tasks, audit_runtime) = startup.unwrap();
        drop(router);
        audit_runtime.shutdown().await.unwrap();
        shutdown_background_tasks(background_tasks, Duration::from_secs(1))
            .await
            .unwrap();

        let store = ZeppelinStore::from_config(&storage_config).unwrap();
        let keys = store.list_prefix("_audit/").await.unwrap();
        assert!(!keys.is_empty());
        let mut found = false;
        for key in keys {
            let body = store.get(&key).await.unwrap();
            for line in body
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
            {
                let record: serde_json::Value = serde_json::from_slice(line).unwrap();
                found |= record["params"] == "license_expired_boot";
            }
        }
        assert!(found, "expired signed license boot audit was not durable");
        clear_license_startup_store(&cleanup_config).await;
    }

    /// Boot and daily observation both warn while an expired license is still
    /// inside the management grace window.
    #[tokio::test]
    async fn expired_within_grace_warns_on_boot_and_daily_observation() {
        let _startup_guard = LICENSE_STARTUP_LOCK.lock().await;
        let expires_at = Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap();
        let now = expires_at + ChronoDuration::days(1);
        let entitlements = Entitlements::licensed_for_testing(
            [Feature::Rbac, Feature::Constraints, Feature::AuditS3],
            Some(expires_at),
        );
        assert!(!entitlements.management_frozen(now));

        let captured = CapturedLicenseWarnings::default();
        let subscriber = tracing_subscriber::registry().with(captured.clone());
        tracing::subscriber::with_default(subscriber, || {
            observe_license_expiry(&entitlements, now);
            observe_license_expiry(&entitlements, now + ChronoDuration::days(1));
        });

        let warnings = captured
            .0
            .lock()
            .unwrap_or_else(|_| panic!("license warning capture lock poisoned"));
        assert_eq!(warnings.as_slice(), [Some(false), Some(false)]);
    }
}
