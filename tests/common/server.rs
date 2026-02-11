use std::sync::Arc;

use tokio::net::TcpListener;

use super::harness::TestHarness;

use zeppelin::cache::DiskCache;
use zeppelin::compaction::Compactor;
use zeppelin::config::Config;
use zeppelin::namespace::NamespaceManager;
use zeppelin::server::routes::build_router;
use zeppelin::server::AppState;
use zeppelin::storage::ZeppelinStore;
use zeppelin::wal::{WalReader, WalWriter};

/// Start a test server with optional config override, returning (base_url, harness, cache, _cache_dir).
/// The TempDir must be kept alive for the cache to function.
pub async fn start_test_server_with_config(
    config_override: Option<Config>,
) -> (String, TestHarness, Arc<DiskCache>, tempfile::TempDir) {
    // Ensure metrics are registered (idempotent)
    zeppelin::metrics::init();

    let harness = TestHarness::new().await;
    let config = config_override.unwrap_or_else(|| Config::load(None).unwrap());

    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    let compactor = Arc::new(Compactor::new(
        harness.store.clone(),
        WalReader::new(harness.store.clone()),
        config.compaction.clone(),
        config.indexing.clone(),
    ));

    let state = AppState {
        store: harness.store.clone(),
        namespace_manager: Arc::new(NamespaceManager::new(harness.store.clone())),
        wal_writer: Arc::new(WalWriter::new(harness.store.clone())),
        wal_reader: Arc::new(WalReader::new(harness.store.clone())),
        config: Arc::new(config),
        compactor,
        cache: cache.clone(),
    };

    let app = build_router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (base_url, harness, cache, cache_dir)
}

/// Start a test server with default config, returning (base_url, harness).
pub async fn start_test_server() -> (String, TestHarness) {
    let (url, harness, _cache, _dir) = start_test_server_with_config(None).await;
    (url, harness)
}

/// Create a URL-safe namespace name scoped to this test's prefix.
pub fn api_ns(harness: &TestHarness, suffix: &str) -> String {
    format!("{}-{suffix}", harness.prefix)
}

/// Clean up all S3 objects under a namespace prefix.
pub async fn cleanup_ns(store: &ZeppelinStore, ns: &str) {
    let prefix = format!("{ns}/");
    let _ = store.delete_prefix(&prefix).await;
}
