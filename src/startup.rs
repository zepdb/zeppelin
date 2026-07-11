//! Builds the complete Zeppelin process graph and shuts down compaction.
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
//! main owns watch::Sender + compaction JoinHandle
//!                 |
//!                 | send true
//!                 v
//! compaction runtime observes shutdown and exits its OS thread
//!                 |
//!                 | spawn_blocking joins without blocking query workers
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
//! - [`crate::startup::shutdown_background_tasks`] signals/joins the compaction
//!   thread only. Hydration and request tasks end through normal Tokio/AppState
//!   lifecycle.
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

use crate::cache::manifest_cache::ManifestCache;
use crate::cache::{
    hydration::{heat_policy_from_config, HydrationConfig, SegmentHydrator},
    DiskCache,
};
use crate::compaction::background::{start_compaction_thread, CompactionThreadOptions};
use crate::compaction::Compactor;
use crate::config::{Config, CpuBudget};
use crate::error::{Result as ZeppelinResult, ZeppelinError};
use crate::fts::wal_cache::WalFtsCache;
use crate::namespace::NamespaceManager;
use crate::runtime_config::{QueryKnobBounds, RuntimeQueryConfig};
use crate::server::build_router;
use crate::server::AppState;
use crate::storage::ZeppelinStore;
use crate::time::Clock;
use crate::wal::{LeaseManager, WalReader, WalWriter};

/// Maximum duration for each endpoint, LIST, or namespace-scan startup probe.
const STORAGE_STARTUP_TIMEOUT: Duration = Duration::from_secs(2);

/// Resolves the optional configuration file path from process context.
///
/// Priority is `ZEPPELIN_CONFIG`, then `./zeppelin.toml` in the current working
/// directory, then no path so [`Config::load`] uses defaults/environment
/// overrides. The environment value is returned verbatim, including an empty or
/// nonexistent path; validation/opening belongs to the loader.
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
/// exists. With neither input, the caller receives `None` and loads defaults.
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
/// A ready [`Router`], a [`watch::Sender`] used to request compaction shutdown,
/// and the owned OS-thread join handle required by
/// [`shutdown_background_tasks`]. The caller must bind/serve the router and
/// eventually signal/join the thread.
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
/// A local-storage test receives a router, shutdown sender, and compaction
/// handle without S3. With a dead S3 endpoint and fail-fast enabled, startup
/// returns before serving; with fail-fast disabled, it logs the outage, skips
/// discovery, and returns a router whose storage-dependent requests may fail.
///
/// # Rust Notes for Java/C Engineers
///
/// `config` is moved into this function and ultimately into an `Arc` inside
/// state. Service handles are cloned explicitly when several owners need them.
/// The tuple return transfers unique responsibility for the shutdown sender and
/// thread handle to `main`; Rust prevents accidentally joining the handle twice.
pub async fn build_app(
    config: Config,
) -> Result<(Router, watch::Sender<bool>, std::thread::JoinHandle<()>), Box<dyn std::error::Error>>
{
    if let Err(error) = config.validate() {
        tracing::error!(error = %error, "invalid configuration; refusing to boot");
        return Err(Box::new(error));
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
        compaction_interval_secs = config.compaction.interval_secs,
        max_wal_fragments = config.compaction.max_wal_fragments_before_compact,
        max_wal_age_secs = config.compaction.max_wal_age_before_compact_secs,
        max_wal_bytes = config.compaction.max_wal_bytes_before_compact,
        "configuration loaded"
    );

    // Initialize metrics
    crate::metrics::init();

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
            return Err(Box::new(error));
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
                return Err(Box::new(error));
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

    let clock = Clock::system();

    // Initialize namespace manager and scan existing namespaces
    let namespace_manager = Arc::new(NamespaceManager::with_clock(
        store.clone(),
        Duration::from_millis(config.cache.namespace_registry_ttl_ms),
        clock.clone(),
    ));
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
                return Err(Box::new(error));
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
                return Err(Box::new(error));
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
        let hydration_config = HydrationConfig::from_cache_config(&config.cache)?;
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
    let compactor = Arc::new(Compactor::with_clock(
        store.clone(),
        WalReader::new(store.clone()),
        config.compaction.clone(),
        config.indexing.clone(),
        Duration::from_secs(config.gc.compaction_upload_window_secs),
        clock.clone(),
    ));

    // Per-namespace compaction lease: only one node compacts a namespace at
    // a time. The holder ID is unique per process; the fencing token from the
    // lease is threaded into every compaction commit.
    let lease_manager = Arc::new(LeaseManager::with_clock(
        store.clone(),
        format!("zeppelin-{}", uuid::Uuid::new_v4()),
        Duration::from_secs(config.compaction.lease_duration_secs),
        clock.clone(),
    ));

    // Spawn background compaction on a dedicated runtime (CPU isolation from queries)
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let compaction_handle = start_compaction_thread(
        compactor.clone(),
        namespace_manager.clone(),
        shutdown_rx,
        manifest_cache.clone(),
        lease_manager.clone(),
        cache.clone(),
        CompactionThreadOptions {
            compaction_workers: cpu_budget.compaction_workers,
            gc_config: config.gc.clone(),
        },
    );

    // Initialize WAL FTS cache (pre-tokenized BM25 data)
    let fts_cache = Arc::new(WalFtsCache::new());

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
    let state = AppState {
        store,
        clock,
        namespace_manager,
        namespace_name_prefix: None,
        wal_writer,
        wal_reader,
        compactor,
        lease_manager,
        config: Arc::new(config),
        trusted_proxies,
        runtime_query_config,
        query_knob_bounds,
        cache,
        manifest_cache,
        hydrator,
        fts_cache,
        query_semaphore,
        rate_limiters,
    };

    // Build router
    let app = build_router(state);

    Ok((app, shutdown_tx, compaction_handle))
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

/// Signals and joins the dedicated compaction thread within a deadline.
///
/// Sending is best-effort because the thread may already have stopped. Joining
/// runs on Tokio's blocking pool so it does not block an async worker. A timeout
/// drops the async join handle, but the underlying blocking join/thread may
/// continue until the compaction thread eventually exits.
///
/// # Parameters
///
/// - `shutdown_tx`: Owned watch sender connected to the compaction runtime.
/// - `compaction_handle`: Unique OS-thread join handle returned by [`build_app`].
/// - `timeout_duration`: Maximum time this caller waits for the spawned join
///   task.
///
/// # Returns
///
/// `Ok(())` only when the compaction OS thread exits normally before the
/// deadline.
///
/// # Errors
///
/// Returns configuration errors when the compaction thread panics, the blocking
/// join task fails, or the timeout expires. The shutdown send result itself is
/// ignored because a closed receiver already means no active listener remains.
///
/// # Side Effects
///
/// Publishes `true` on the watch channel and occupies a blocking-pool task while
/// joining. It does not explicitly stop hydration or request tasks.
///
/// # Examples
///
/// After Axum finishes graceful HTTP shutdown, `main` passes its sender and
/// handle with the configured timeout. Normal compaction exit returns success;
/// a panic becomes a startup/configuration-class process error.
///
/// # Rust Notes for Java/C Engineers
///
/// Moving `compaction_handle` into `spawn_blocking` transfers unique join
/// ownership to that closure. Java's `Thread.join` can be invoked through shared
/// references; C's `pthread_join` requires convention to avoid double joins.
/// Rust makes a second join impossible after the move.
pub async fn shutdown_background_tasks(
    shutdown_tx: watch::Sender<bool>,
    compaction_handle: std::thread::JoinHandle<()>,
    timeout_duration: Duration,
) -> ZeppelinResult<()> {
    let _ = shutdown_tx.send(true);
    let join_task = tokio::task::spawn_blocking(move || compaction_handle.join());

    match tokio::time::timeout(timeout_duration, join_task).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(_panic))) => Err(ZeppelinError::Config(
            "background compaction thread panicked during shutdown".to_string(),
        )),
        Ok(Err(error)) => Err(ZeppelinError::Config(format!(
            "failed to join background compaction thread: {error}"
        ))),
        Err(_elapsed) => Err(ZeppelinError::Config(format!(
            "timed out after {}s waiting for background compaction shutdown",
            timeout_duration.as_secs()
        ))),
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Local-storage startup tests for configuration discovery and thread cleanup.
    //!
    //! The two configuration-path tests mutate process-global environment and
    //! current-directory state. They are not safe to run concurrently with each
    //! other; execute this module with `--test-threads=1`. Each test restores the
    //! previous values for later tests.

    use super::*;
    use crate::config::StorageBackend;

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
        config.storage.backend = StorageBackend::Local;
        config.storage.bucket = tmp.path().join("storage").to_string_lossy().to_string();
        config.cache.dir = tmp.path().join("cache");
        config.cache.max_size_gb = 1; // minimal but non-zero
        config.compaction.interval_secs = 9999; // don't trigger during test
        config
    }

    /// Gives `ZEPPELIN_CONFIG` priority and restores its prior process value.
    #[test]
    fn test_resolve_config_path_from_env() {
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
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(&tmp);

        let result = build_app(config).await;
        assert!(result.is_ok());

        let (_router, shutdown_tx, compaction_handle) = result.unwrap();

        shutdown_background_tasks(shutdown_tx, compaction_handle, Duration::from_secs(1))
            .await
            .unwrap();
    }

    /// Accepts an empty authoritative local namespace scan during startup.
    #[tokio::test]
    async fn test_build_app_with_namespace_scan() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(&tmp);

        // build_app should successfully scan (even though there are no namespaces)
        let result = build_app(config).await;
        assert!(result.is_ok());

        let (_router, shutdown_tx, compaction_handle) = result.unwrap();
        shutdown_background_tasks(shutdown_tx, compaction_handle, Duration::from_secs(1))
            .await
            .unwrap();
    }

    /// Proves the watch signal and blocking join finish without hanging.
    #[tokio::test]
    async fn test_graceful_shutdown() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(&tmp);

        let (router, shutdown_tx, compaction_handle) = build_app(config).await.unwrap();

        // Send shutdown signal
        shutdown_background_tasks(shutdown_tx, compaction_handle, Duration::from_secs(1))
            .await
            .unwrap();

        // If we get here without hanging, shutdown worked
        drop(router);
    }
}
