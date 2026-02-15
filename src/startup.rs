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
use crate::cache::DiskCache;
use crate::compaction::background::start_compaction_thread;
use crate::compaction::Compactor;
use crate::config::{Config, CpuBudget};
use crate::fts::wal_cache::WalFtsCache;
use crate::namespace::NamespaceManager;
use crate::server::routes::build_router;
use crate::server::AppState;
use crate::storage::ZeppelinStore;
use crate::wal::batch_writer::BatchWalWriter;
use crate::wal::{WalReader, WalWriter};

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
///
/// This function:
/// - Initializes metrics
/// - Creates storage, namespace manager, WAL reader/writer, cache, compactor
/// - Scans existing namespaces
/// - Spawns background compaction loop
/// - Builds `AppState` and axum `Router`
pub async fn build_app(
    config: Config,
) -> Result<(Router, watch::Sender<bool>), Box<dyn std::error::Error>> {
    tracing::info!("zeppelin starting");

    // Detect CPU budget and log allocation
    let cpu_budget = CpuBudget::auto();
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
        "configuration loaded"
    );

    // Initialize metrics
    crate::metrics::init();

    // Initialize storage
    let store = ZeppelinStore::from_config(&config.storage)?;

    // Initialize namespace manager and scan existing namespaces
    let namespace_manager = Arc::new(NamespaceManager::new(store.clone()));
    match namespace_manager.scan_and_register().await {
        Ok(count) => tracing::info!(count, "registered existing namespaces"),
        Err(e) => tracing::warn!(error = %e, "failed to scan namespaces on startup"),
    }

    // Initialize WAL writer and reader
    let wal_writer = Arc::new(WalWriter::new(store.clone()));
    let wal_reader = Arc::new(WalReader::new(store.clone()));

    // Initialize disk cache
    let cache = Arc::new(DiskCache::new(&config.cache)?);

    // Initialize manifest cache (500ms TTL — drops ~27ms S3 GET per query)
    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_millis(500)));

    // Initialize compactor
    let compactor = Arc::new(Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        config.compaction.clone(),
        config.indexing.clone(),
    ));

    // Spawn background compaction on a dedicated runtime (CPU isolation from queries)
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let _compaction_handle = start_compaction_thread(
        compactor.clone(),
        namespace_manager.clone(),
        shutdown_rx,
        cpu_budget.compaction_workers,
        manifest_cache.clone(),
    );

    // Initialize batch WAL writer (if batch_manifest_size > 1)
    let batch_wal_writer = if config.wal.batch_manifest_size > 1 {
        tracing::info!(
            batch_size = config.wal.batch_manifest_size,
            timeout_ms = config.wal.batch_manifest_timeout_ms,
            "batched manifest updates enabled"
        );
        Some(Arc::new(BatchWalWriter::new(
            store.clone(),
            config.wal.batch_manifest_size,
            config.wal.batch_manifest_timeout_ms,
        )))
    } else {
        None
    };

    // Initialize WAL FTS cache (pre-tokenized BM25 data)
    let fts_cache = Arc::new(WalFtsCache::new());

    // Build application state
    let query_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_queries,
    ));
    let rate_limiters = Arc::new(DashMap::new());
    let state = AppState {
        store,
        namespace_manager,
        wal_writer,
        wal_reader,
        config: Arc::new(config),
        compactor,
        cache,
        manifest_cache,
        fts_cache,
        query_semaphore,
        batch_wal_writer,
        rate_limiters,
    };

    // Build router
    let app = build_router(state);

    Ok((app, shutdown_tx))
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

        let (_router, shutdown_tx) = result.unwrap();

        // Signal shutdown
        let _ = shutdown_tx.send(true);
    }

    #[tokio::test]
    async fn test_build_app_with_namespace_scan() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(&tmp);

        // build_app should successfully scan (even though there are no namespaces)
        let result = build_app(config).await;
        assert!(result.is_ok());

        let (_router, shutdown_tx) = result.unwrap();
        let _ = shutdown_tx.send(true);
    }

    #[tokio::test]
    async fn test_graceful_shutdown() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(&tmp);

        let (router, shutdown_tx) = build_app(config).await.unwrap();

        // Send shutdown signal
        let result = shutdown_tx.send(true);
        assert!(result.is_ok());

        // Give the compaction loop a moment to see the signal
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // If we get here without hanging, shutdown worked
        drop(router);
    }
}
