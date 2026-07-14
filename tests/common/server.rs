use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use super::harness::TestHarness;

use zeppelin::cache::decoded_cache::DecodedArtifactCache;
use zeppelin::cache::hydration::{heat_policy_from_config, HydrationConfig, SegmentHydrator};
use zeppelin::cache::manifest_cache::ManifestCache;
use zeppelin::cache::DiskCache;
use zeppelin::compaction::background::{compaction_loop, CompactionLoopOptions};
use zeppelin::compaction::Compactor;
use zeppelin::config::Config;
use zeppelin::fts::wal_cache::WalFtsCache;
use zeppelin::namespace::NamespaceManager;
use zeppelin::runtime_config::{QueryKnobBounds, RuntimeQueryConfig};
use zeppelin::server::{build_router, parse_trusted_proxies, AppState};
use zeppelin::storage::ZeppelinStore;
use zeppelin::time::Clock;
use zeppelin::wal::{LeaseManager, WalFragmentCache, WalReader, WalWriter};

tokio::task_local! {
    static BACKGROUND_COMPACTION_ORIGIN: bool;
}

fn test_fragment_cache(config: &Config) -> Arc<WalFragmentCache> {
    let max_bytes = config
        .cache
        .wal_fragment_cache_max_mb
        .checked_mul(1024 * 1024)
        .expect("test WAL fragment cache capacity overflow");
    Arc::new(WalFragmentCache::new(max_bytes))
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
) -> Arc<NamespaceManager> {
    Arc::new(NamespaceManager::with_clock(
        store.clone(),
        Duration::from_millis(config.cache.namespace_registry_ttl_ms),
        clock.clone(),
    ))
}

fn compactor(config: &Config, store: &ZeppelinStore, clock: &Clock) -> Arc<Compactor> {
    Arc::new(Compactor::with_clock(
        store.clone(),
        WalReader::new(store.clone()),
        config.compaction.clone(),
        config.indexing.clone(),
        Duration::from_secs(config.gc.compaction_upload_window_secs),
        clock.clone(),
    ))
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
    start_test_server_with_config_inner(config_override, true).await
}

/// Start a test server without overriding rate-limit settings.
pub async fn start_test_server_with_config_no_limit_override(
    config_override: Option<Config>,
) -> (String, TestHarness, Arc<DiskCache>, tempfile::TempDir) {
    start_test_server_with_config_inner(config_override, false).await
}

async fn start_test_server_with_config_inner(
    config_override: Option<Config>,
    override_rate_limits: bool,
) -> (String, TestHarness, Arc<DiskCache>, tempfile::TempDir) {
    // Ensure metrics are registered (idempotent)
    zeppelin::metrics::init();

    let harness = TestHarness::new().await;
    let mut config = config_override.unwrap_or_else(|| Config::load(None).unwrap());
    if override_rate_limits {
        configure_test_server_limits(&mut config);
    }
    let clock = Clock::system();

    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    let query_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_queries,
    ));
    let (runtime_query_config, query_knob_bounds) = runtime_query_state(&config);
    let hydrator = maybe_hydrator(&config, &harness.store, &cache);
    let compactor = compactor(&config, &harness.store, &clock);
    let lease_manager = lease_manager(&config, &harness.store, &clock);
    let trusted_proxies = Arc::from(parse_trusted_proxies(&config.server.trusted_proxies).unwrap());
    let state = AppState {
        store: harness.store.clone(),
        clock: clock.clone(),
        namespace_manager: namespace_manager(&config, &harness.store, &clock),
        namespace_name_prefix: None,
        wal_writer: Arc::new(WalWriter::with_clock(harness.store.clone(), clock)),
        wal_reader: Arc::new(WalReader::new(harness.store.clone())),
        compactor,
        lease_manager,
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
    let clock = Clock::system();

    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    let query_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_queries,
    ));
    let (runtime_query_config, query_knob_bounds) = runtime_query_state(&config);
    let hydrator = maybe_hydrator(&config, &store, &cache);
    let compactor = compactor(&config, &store, &clock);
    let lease_manager = lease_manager(&config, &store, &clock);
    let trusted_proxies = Arc::from(parse_trusted_proxies(&config.server.trusted_proxies).unwrap());
    let state = AppState {
        store: store.clone(),
        clock: clock.clone(),
        namespace_manager: namespace_manager(&config, &store, &clock),
        namespace_name_prefix,
        wal_writer: Arc::new(WalWriter::with_clock(store.clone(), clock)),
        wal_reader: Arc::new(WalReader::new(store.clone())),
        compactor,
        lease_manager,
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
    let clock = Clock::system();

    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    let compactor = compactor(&config, &harness.store, &clock);
    let lease_manager = lease_manager(&config, &harness.store, &clock);
    let manifest_cache = Arc::new(ManifestCache::new(Duration::ZERO));

    let query_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_queries,
    ));
    let (runtime_query_config, query_knob_bounds) = runtime_query_state(&config);
    let hydrator = maybe_hydrator(&config, &harness.store, &cache);
    let trusted_proxies = Arc::from(parse_trusted_proxies(&config.server.trusted_proxies).unwrap());
    let state = AppState {
        store: harness.store.clone(),
        clock: clock.clone(),
        namespace_manager: namespace_manager(&config, &harness.store, &clock),
        namespace_name_prefix: None,
        wal_writer: Arc::new(WalWriter::with_clock(harness.store.clone(), clock)),
        wal_reader: Arc::new(WalReader::new(harness.store.clone())),
        compactor: compactor.clone(),
        lease_manager,
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
    let clock = Clock::system();

    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    let namespace_manager = namespace_manager(&config, &harness.store, &clock);

    let compactor = compactor(&config, &harness.store, &clock);

    // Spawn background compaction loop (mirrors main.rs)
    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_millis(500)));
    let lease_manager = lease_manager(&config, &harness.store, &clock);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let gc_config = config.gc.clone();
    {
        let compactor = compactor.clone();
        let namespace_manager = namespace_manager.clone();
        let manifest_cache = manifest_cache.clone();
        let lease_manager = lease_manager.clone();
        let cache = cache.clone();
        let namespace_prefix = Some(harness.prefix.clone());
        tokio::spawn(with_background_compaction_origin(async move {
            compaction_loop(
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
    let state = AppState {
        store: harness.store.clone(),
        clock: clock.clone(),
        namespace_manager,
        namespace_name_prefix: Some(harness.prefix.clone()),
        wal_writer: Arc::new(WalWriter::with_clock(harness.store.clone(), clock)),
        wal_reader: Arc::new(WalReader::new(harness.store.clone())),
        compactor,
        lease_manager,
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

pub struct FullTestServer {
    pub base_url: String,
    pub store: ZeppelinStore,
    pub clock: Clock,
    pub cache: Arc<DiskCache>,
    pub cache_dir: tempfile::TempDir,
    pub compactor: Arc<Compactor>,
    pub lease_manager: Arc<LeaseManager>,
    pub manifest_cache: Arc<ManifestCache>,
    fragment_cache: Arc<WalFragmentCache>,
    decoded_artifact_cache: Arc<DecodedArtifactCache>,
    wal_writer: Arc<WalWriter>,
    pub shutdown_compaction: Option<tokio::sync::watch::Sender<bool>>,
    pub compaction_loop_task: Option<JoinHandle<()>>,
    pub server_task: JoinHandle<()>,
    pub shutdown_http: tokio::sync::watch::Sender<bool>,
}

impl FullTestServer {
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

    /// Gracefully stop the HTTP server and background compaction loop.
    pub async fn shutdown(mut self) {
        let _ = self.shutdown_http.send(true);
        if let Some(shutdown) = self.shutdown_compaction.take() {
            let _ = shutdown.send(true);
        }
        if let Some(task) = self.compaction_loop_task.take() {
            task.await
                .unwrap_or_else(|error| panic!("test compaction loop failed: {error}"));
        }
        self.server_task
            .await
            .unwrap_or_else(|error| panic!("test HTTP server failed: {error}"));
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
    mut config: Config,
    spawn_compaction_loop: bool,
    clock: Option<Clock>,
    disk_cache_max_bytes: u64,
) -> FullTestServer {
    zeppelin::metrics::init();
    configure_test_server_limits(&mut config);
    let clock = clock.unwrap_or_else(Clock::system);

    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), disk_cache_max_bytes)
            .unwrap(),
    );

    let namespace_manager = namespace_manager(&config, &store, &clock);
    let compactor = compactor(&config, &store, &clock);
    let lease_manager = lease_manager(&config, &store, &clock);
    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_millis(
        config.cache.manifest_cache_ttl_ms,
    )));

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
            let namespace_prefix = namespace_name_prefix.clone();
            compaction_loop_task = Some(tokio::spawn(with_background_compaction_origin(
                async move {
                    compaction_loop(
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
    let state = AppState {
        store: store.clone(),
        clock: clock.clone(),
        namespace_manager,
        namespace_name_prefix,
        wal_writer: wal_writer.clone(),
        wal_reader: Arc::new(WalReader::new(store.clone())),
        compactor: compactor.clone(),
        lease_manager: lease_manager.clone(),
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
        store,
        clock,
        cache,
        cache_dir,
        compactor,
        lease_manager,
        manifest_cache,
        fragment_cache,
        decoded_artifact_cache,
        wal_writer,
        shutdown_compaction,
        compaction_loop_task,
        server_task,
        shutdown_http,
    }
}
