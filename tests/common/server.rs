use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::net::TcpListener;

use super::harness::TestHarness;

use zeppelin::cache::hydration::{heat_policy_from_config, HydrationConfig, SegmentHydrator};
use zeppelin::cache::manifest_cache::ManifestCache;
use zeppelin::cache::DiskCache;
use zeppelin::compaction::background::compaction_loop;
use zeppelin::compaction::Compactor;
use zeppelin::config::Config;
use zeppelin::fts::wal_cache::WalFtsCache;
use zeppelin::namespace::NamespaceManager;
use zeppelin::runtime_config::{QueryKnobBounds, RuntimeQueryConfig};
use zeppelin::server::build_router;
use zeppelin::server::AppState;
use zeppelin::storage::ZeppelinStore;
use zeppelin::wal::{WalReader, WalWriter};

fn configure_test_server_limits(config: &mut Config) {
    // Integration tests share one loopback IP and often issue bursty setup or
    // perf requests. Keep production defaults intact, but avoid unrelated 429s
    // in tests that are not exercising rate limiting.
    config.server.rate_limit_rps = 1_000_000;
    config.server.rate_limit_burst = 1_000_000;
}

fn runtime_query_state(config: &Config) -> (Arc<RuntimeQueryConfig>, QueryKnobBounds) {
    (
        Arc::new(RuntimeQueryConfig::from_config(config)),
        QueryKnobBounds::from_config(config),
    )
}

fn namespace_manager(config: &Config, store: &ZeppelinStore) -> Arc<NamespaceManager> {
    Arc::new(NamespaceManager::new_with_registry_ttl(
        store.clone(),
        Duration::from_millis(config.cache.namespace_registry_ttl_ms),
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
    let hydration_config = HydrationConfig::from_cache_config(&config.cache).unwrap();
    let policy = heat_policy_from_config(&config.cache).unwrap();
    Some(SegmentHydrator::start(
        store.clone(),
        cache.clone(),
        policy,
        hydration_config,
    ))
}

/// Start a test server with optional config override, returning (base_url, harness, cache, _cache_dir).
/// The TempDir must be kept alive for the cache to function.
pub async fn start_test_server_with_config(
    config_override: Option<Config>,
) -> (String, TestHarness, Arc<DiskCache>, tempfile::TempDir) {
    // Ensure metrics are registered (idempotent)
    zeppelin::metrics::init();

    let harness = TestHarness::new().await;
    let mut config = config_override.unwrap_or_else(|| Config::load(None).unwrap());
    configure_test_server_limits(&mut config);

    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    let query_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_queries,
    ));
    let (runtime_query_config, query_knob_bounds) = runtime_query_state(&config);
    let hydrator = maybe_hydrator(&config, &harness.store, &cache);
    let state = AppState {
        store: harness.store.clone(),
        namespace_manager: namespace_manager(&config, &harness.store),
        namespace_name_prefix: None,
        wal_writer: Arc::new(WalWriter::new(harness.store.clone())),
        wal_reader: Arc::new(WalReader::new(harness.store.clone())),
        config: Arc::new(config),
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

/// Start a test server on an already-constructed store.
///
/// Used by tests that wrap the harness store with instrumentation while still
/// relying on the harness's random prefix and cleanup.
pub async fn start_test_server_on_store(
    store: ZeppelinStore,
    namespace_name_prefix: Option<String>,
) -> (String, Arc<DiskCache>, tempfile::TempDir) {
    zeppelin::metrics::init();

    let mut config = Config::load(None).unwrap();
    configure_test_server_limits(&mut config);

    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    let query_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_queries,
    ));
    let (runtime_query_config, query_knob_bounds) = runtime_query_state(&config);
    let hydrator = maybe_hydrator(&config, &store, &cache);
    let state = AppState {
        store: store.clone(),
        namespace_manager: namespace_manager(&config, &store),
        namespace_name_prefix,
        wal_writer: Arc::new(WalWriter::new(store.clone())),
        wal_reader: Arc::new(WalReader::new(store.clone())),
        config: Arc::new(config),
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

    (base_url, cache, cache_dir)
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
    let mut config = config_override.unwrap_or_else(|| Config::load(None).unwrap());
    configure_test_server_limits(&mut config);

    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    let compactor = Arc::new(Compactor::new(
        harness.store.clone(),
        WalReader::new(harness.store.clone()),
        config.compaction.clone(),
        config.indexing.clone(),
        Duration::from_secs(config.gc.compaction_upload_window_secs),
    ));
    let manifest_cache = Arc::new(ManifestCache::new(Duration::ZERO));

    let query_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_queries,
    ));
    let (runtime_query_config, query_knob_bounds) = runtime_query_state(&config);
    let hydrator = maybe_hydrator(&config, &harness.store, &cache);
    let state = AppState {
        store: harness.store.clone(),
        namespace_manager: namespace_manager(&config, &harness.store),
        namespace_name_prefix: None,
        wal_writer: Arc::new(WalWriter::new(harness.store.clone())),
        wal_reader: Arc::new(WalReader::new(harness.store.clone())),
        config: Arc::new(config),
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
    let mut config = config_override.unwrap_or_else(|| Config::load(None).unwrap());
    configure_test_server_limits(&mut config);

    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    let namespace_manager = namespace_manager(&config, &harness.store);

    let compactor = Arc::new(Compactor::new(
        harness.store.clone(),
        WalReader::new(harness.store.clone()),
        config.compaction.clone(),
        config.indexing.clone(),
        Duration::from_secs(config.gc.compaction_upload_window_secs),
    ));

    // Spawn background compaction loop (mirrors main.rs)
    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_millis(500)));
    let lease_manager = Arc::new(zeppelin::wal::LeaseManager::new(
        harness.store.clone(),
        format!("test-{}", uuid::Uuid::new_v4()),
        Duration::from_secs(config.compaction.lease_duration_secs),
    ));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    {
        let compactor = compactor.clone();
        let namespace_manager = namespace_manager.clone();
        let manifest_cache = manifest_cache.clone();
        let cache = cache.clone();
        let namespace_prefix = Some(harness.prefix.clone());
        tokio::spawn(async move {
            compaction_loop(
                compactor,
                namespace_manager,
                shutdown_rx,
                manifest_cache,
                lease_manager,
                cache,
                namespace_prefix,
            )
            .await;
        });
    }

    let query_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_queries,
    ));
    let (runtime_query_config, query_knob_bounds) = runtime_query_state(&config);
    let hydrator = maybe_hydrator(&config, &harness.store, &cache);
    let state = AppState {
        store: harness.store.clone(),
        namespace_manager,
        namespace_name_prefix: Some(harness.prefix.clone()),
        wal_writer: Arc::new(WalWriter::new(harness.store.clone())),
        wal_reader: Arc::new(WalReader::new(harness.store.clone())),
        config: Arc::new(config),
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
    let (url, harness, _cache, dir) = start_test_server_with_config(None).await;
    // The DiskCache lives in `dir`. This helper's caller does not receive the
    // TempDir handle, so if we let it drop here the cache directory is deleted
    // out from under the still-running server — every cache write then fails
    // (learnings rule 6: keep TempDir alive for the lifetime of anything using
    // its path). Disarm the auto-delete so the dir survives for the test
    // process; the OS reclaims the temp space afterwards.
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
    assert_eq!(resp.status(), 201, "create namespace failed");
    let body: serde_json::Value = resp.json().await.unwrap();
    body["name"].as_str().unwrap().to_string()
}

/// Clean up all S3 objects under a namespace prefix.
pub async fn cleanup_ns(store: &ZeppelinStore, ns: &str) {
    let prefix = format!("{ns}/");
    let _ = store.delete_prefix(&prefix).await;
}
