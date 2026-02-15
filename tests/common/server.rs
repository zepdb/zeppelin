use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::net::TcpListener;

use super::harness::TestHarness;

use zeppelin::cache::manifest_cache::ManifestCache;
use zeppelin::cache::DiskCache;
use zeppelin::compaction::background::compaction_loop;
use zeppelin::compaction::Compactor;
use zeppelin::config::Config;
use zeppelin::fts::wal_cache::WalFtsCache;
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

    let query_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_queries,
    ));
    let state = AppState {
        store: harness.store.clone(),
        namespace_manager: Arc::new(NamespaceManager::new(harness.store.clone())),
        wal_writer: Arc::new(WalWriter::new(harness.store.clone())),
        wal_reader: Arc::new(WalReader::new(harness.store.clone())),
        config: Arc::new(config),
        compactor,
        cache: cache.clone(),
        manifest_cache: Arc::new(ManifestCache::new(Duration::from_millis(500))),
        fts_cache: Arc::new(WalFtsCache::new()),
        query_semaphore,
        batch_wal_writer: None,
        rate_limiters: Arc::new(DashMap::new()),
    };

    let app = build_router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    (base_url, harness, cache, cache_dir)
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
) {
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

    let query_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_queries,
    ));
    let state = AppState {
        store: harness.store.clone(),
        namespace_manager: Arc::new(NamespaceManager::new(harness.store.clone())),
        wal_writer: Arc::new(WalWriter::new(harness.store.clone())),
        wal_reader: Arc::new(WalReader::new(harness.store.clone())),
        config: Arc::new(config),
        compactor: compactor.clone(),
        cache: cache.clone(),
        manifest_cache: Arc::new(ManifestCache::new(Duration::from_millis(500))),
        fts_cache: Arc::new(WalFtsCache::new()),
        query_semaphore,
        batch_wal_writer: None,
        rate_limiters: Arc::new(DashMap::new()),
    };

    let app = build_router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    (base_url, harness, cache, cache_dir, compactor)
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
) {
    zeppelin::metrics::init();

    let harness = TestHarness::new().await;
    let config = config_override.unwrap_or_else(|| Config::load(None).unwrap());

    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    let namespace_manager = Arc::new(NamespaceManager::new(harness.store.clone()));

    let compactor = Arc::new(Compactor::new(
        harness.store.clone(),
        WalReader::new(harness.store.clone()),
        config.compaction.clone(),
        config.indexing.clone(),
    ));

    // Spawn background compaction loop (mirrors main.rs)
    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_millis(500)));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    {
        let compactor = compactor.clone();
        let namespace_manager = namespace_manager.clone();
        let manifest_cache = manifest_cache.clone();
        tokio::spawn(async move {
            compaction_loop(compactor, namespace_manager, shutdown_rx, manifest_cache).await;
        });
    }

    let query_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_queries,
    ));
    let state = AppState {
        store: harness.store.clone(),
        namespace_manager,
        wal_writer: Arc::new(WalWriter::new(harness.store.clone())),
        wal_reader: Arc::new(WalReader::new(harness.store.clone())),
        config: Arc::new(config),
        compactor,
        cache: cache.clone(),
        manifest_cache: Arc::new(ManifestCache::new(Duration::from_millis(500))),
        fts_cache: Arc::new(WalFtsCache::new()),
        query_semaphore,
        batch_wal_writer: None,
        rate_limiters: Arc::new(DashMap::new()),
    };

    let app = build_router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    (base_url, harness, cache, cache_dir, shutdown_tx)
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
    assert_eq!(resp.status(), 201, "create namespace failed");
    let body: serde_json::Value = resp.json().await.unwrap();
    body["name"].as_str().unwrap().to_string()
}

/// Clean up all S3 objects under a namespace prefix.
pub async fn cleanup_ns(store: &ZeppelinStore, ns: &str) {
    let prefix = format!("{ns}/");
    let _ = store.delete_prefix(&prefix).await;
}
