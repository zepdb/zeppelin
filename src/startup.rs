//! Application startup and bootstrap logic.
//!
//! This module extracts initialization logic from `main.rs` to make it testable
//! under `cargo test --lib`. All functions use injected dependencies and can be
//! tested with `StorageBackend::Local` without needing S3 or MinIO.

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
use crate::wal::{LeaseManager, WalReader, WalWriter};

const STORAGE_STARTUP_TIMEOUT: Duration = Duration::from_secs(2);

/// Resolve the configuration file path.
///
/// Priority:
/// 1. `ZEPPELIN_CONFIG` environment variable
/// 2. `./zeppelin.toml` if it exists
/// 3. None (use defaults)
pub fn resolve_config_path() -> Option<String> {
    std::env::var("ZEPPELIN_CONFIG").ok().or_else(|| {
        let default = "zeppelin.toml";
        std::path::Path::new(default)
            .exists()
            .then(|| default.to_string())
    })
}

/// Initialize tracing subscriber from logging config.
///
/// Supports JSON and plain text formats. Uses `RUST_LOG` env var if set,
/// otherwise falls back to `config.logging.level`.
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

/// Build the application router and spawn background tasks.
///
/// Returns:
/// - The axum `Router` ready to be served
/// - A shutdown channel sender to gracefully stop background tasks
/// - The background compaction thread handle
///
/// This function:
/// - Initializes metrics
/// - Creates storage, namespace manager, WAL reader/writer, cache, compactor
/// - Scans existing namespaces
/// - Spawns background compaction loop
/// - Builds `AppState` and axum `Router`
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

    // Initialize namespace manager and scan existing namespaces
    let namespace_manager = Arc::new(NamespaceManager::new_with_registry_ttl(
        store.clone(),
        Duration::from_millis(config.cache.namespace_registry_ttl_ms),
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
    let wal_writer = Arc::new(WalWriter::new(store.clone()));
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
    let compactor = Arc::new(Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        config.compaction.clone(),
        config.indexing.clone(),
        Duration::from_secs(config.gc.compaction_upload_window_secs),
    ));

    // Per-namespace compaction lease: only one node compacts a namespace at
    // a time. The holder ID is unique per process; the fencing token from the
    // lease is threaded into every compaction commit.
    let lease_manager = Arc::new(LeaseManager::new(
        store.clone(),
        format!("zeppelin-{}", uuid::Uuid::new_v4()),
        Duration::from_secs(config.compaction.lease_duration_secs),
    ));

    // Spawn background compaction on a dedicated runtime (CPU isolation from queries)
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let compaction_handle = start_compaction_thread(
        compactor.clone(),
        namespace_manager.clone(),
        shutdown_rx,
        manifest_cache.clone(),
        lease_manager,
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
    let state = AppState {
        store,
        namespace_manager,
        namespace_name_prefix: None,
        wal_writer,
        wal_reader,
        config: Arc::new(config),
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

/// Signal background tasks and join the compaction thread within `timeout_duration`.
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
    use super::*;
    use crate::config::StorageBackend;

    fn test_config(tmp: &tempfile::TempDir) -> Config {
        let mut config = Config::default();
        config.storage.backend = StorageBackend::Local;
        config.storage.bucket = tmp.path().join("storage").to_string_lossy().to_string();
        config.cache.dir = tmp.path().join("cache");
        config.cache.max_size_gb = 1; // minimal but non-zero
        config.compaction.interval_secs = 9999; // don't trigger during test
        config
    }

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
