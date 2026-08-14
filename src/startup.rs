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
use crate::security::{AuditRecord, AuditRuntime, SecurityKernel};
use crate::server::{build_router, AppState, ServerTaskSupervisor};
use crate::storage::ZeppelinStore;
use crate::time::Clock;
use crate::wal::{LeaseManager, WalFragmentCache, WalReader, WalWriter};

/// Maximum duration for each endpoint, LIST, or namespace-scan startup probe.
const STORAGE_STARTUP_TIMEOUT: Duration = Duration::from_secs(2);

/// Unique lifecycle ownership for maintenance and request-spawned authoritative work.
pub struct BackgroundTasks {
    shutdown_tx: watch::Sender<bool>,
    compaction_handle: std::thread::JoinHandle<()>,
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
    ) -> Self {
        Self {
            shutdown_tx,
            compaction_handle,
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
    let durable_audit_enabled = config.security.audit_s3;
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

    let (security, credential_adapter) =
        SecurityKernel::compose(store.clone(), &config.security, clock.clone()).await?;
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
        .with_mmli_config(config.mmli.clone())
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
        Arc::clone(&encoder_provider),
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
        encoder_provider,
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
        BackgroundTasks::from_parts(shutdown_tx, compaction_handle).with_server_lifecycle(
            background_security,
            server_tasks,
            compaction_lifecycle,
        ),
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
/// - `backgrounds`: Unique compaction lifecycle owner.
/// - `timeout_duration`: One shared graceful-drain budget for request mutation
///   work and the compaction background join.
///
/// # Returns
///
/// `Ok(())` only when request-originated authoritative mutations and the
/// compaction thread complete their cooperative drain inside the
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
/// request-originated authoritative mutations and joins compaction work
/// against one absolute deadline. It subsequently closes leased-compaction
/// admission and joins the heartbeat and authority-refresh cancellation
/// barriers. Those barriers prove that request-owned mutation, lease-renewal,
/// publication, and authority-refresh capability cannot survive into a
/// replacement. If the compaction OS thread misses the
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

    let background_result = match tokio::time::timeout_at(deadline, join_task).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(_panic))) => Err(ZeppelinError::Config(
            "background compaction thread panicked during shutdown".to_string(),
        )),
        Ok(Err(error)) => Err(ZeppelinError::Config(format!(
            "failed to join background compaction thread: {error}"
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
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use sha2::{Digest, Sha256};
    use std::net::SocketAddr;
    use std::sync::Mutex;

    static PROCESS_CONFIG_LOCK: Mutex<()> = Mutex::new(());
    static RBAC_STARTUP_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

    fn rbac_startup_config(root: &tempfile::TempDir) -> (Config, String) {
        let mut config = Config::default();
        let secret = URL_SAFE_NO_PAD.encode([1_u8; 32]);
        let digest = Sha256::digest(secret.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let key_id = "zpk1_rbac_startup";
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
            name: "rbac-startup-admin".to_string(),
            sha256_hex: digest,
            actions: vec!["*".to_string()],
            namespaces: vec!["*".to_string()],
            expires_at: None,
        });
        config.security.rbac = true;
        if std::env::var("TEST_BACKEND").as_deref() == Ok("minio") {
            config.storage.backend = StorageBackend::S3;
            config.storage.bucket = std::env::var("ZEPPELIN_RBAC_TEST_BUCKET")
                .expect("MinIO rbac-startup tests require an isolated ZEPPELIN_RBAC_TEST_BUCKET");
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

    async fn clear_rbac_startup_store(config: &Config) {
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
        let _startup_guard = RBAC_STARTUP_LOCK.lock().await;
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
        let _startup_guard = RBAC_STARTUP_LOCK.lock().await;
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
        let _startup_guard = RBAC_STARTUP_LOCK.lock().await;
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

    /// A `security.rbac = true` boot traverses the production graph and
    /// exposes the real RBAC management routes.
    ///
    /// Boot bootstraps the policy head under the global publication lease,
    /// which releases through an ETag compare-and-swap. `LocalFileSystem`
    /// answers `PutMode::Update` with `NotImplemented`, so this case requires
    /// a backend that actually implements conditional writes and skips
    /// otherwise.
    #[tokio::test]
    async fn rbac_config_boot_enables_rbac_routes() {
        if std::env::var("TEST_BACKEND").as_deref() != Ok("minio") {
            return;
        }
        let _startup_guard = RBAC_STARTUP_LOCK.lock().await;
        let root = tempfile::TempDir::new().unwrap();
        let (config, admin_bearer) = rbac_startup_config(&root);
        clear_rbac_startup_store(&config).await;
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
        clear_rbac_startup_store(&cleanup_config).await;
    }
}
