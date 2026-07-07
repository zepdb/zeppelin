mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use common::counting::{counting_store, ArtifactClass, GetCounter};
use common::harness::TestHarness;
use common::vectors::random_vectors;
use dashmap::DashMap;
use serde_json::json;
use tempfile::TempDir;
use tokio::net::TcpListener;
use zeppelin::cache::hydration::{heat_policy_from_config, HydrationConfig, SegmentHydrator};
use zeppelin::cache::manifest_cache::ManifestCache;
use zeppelin::cache::DiskCache;
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, Config, DEFAULT_RERANK_COALESCE_GAP_BYTES};
use zeppelin::fts::wal_cache::WalFtsCache;
use zeppelin::namespace::NamespaceManager;
use zeppelin::query::{execute_query, QueryParams};
use zeppelin::runtime_config::{QueryKnobBounds, RuntimeQueryConfig};
use zeppelin::server::{build_router, parse_trusted_proxies, AppState};
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::{ConsistencyLevel, DistanceMetric};
use zeppelin::wal::manifest::{Manifest, SegmentRef};
use zeppelin::wal::{LeaseManager, WalReader, WalWriter};

struct ApiServer {
    base_url: String,
    harness: TestHarness,
    store: ZeppelinStore,
    counter: GetCounter,
    cache: Arc<DiskCache>,
    _cache_dir: TempDir,
    compactor: Arc<Compactor>,
    oversample_factor: usize,
}

struct CompactedNamespace {
    namespace: String,
    query: Vec<f32>,
    segment: SegmentRef,
}

static ADMIN_REQUEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

fn admin_request_lock() -> &'static tokio::sync::Mutex<()> {
    ADMIN_REQUEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn hydration_api_config() -> Config {
    let mut config = Config::default();
    config.cache.hydration_enabled = true;
    config.cache.hydration_heat_queries = 100;
    config.cache.hydration_heat_window_secs = 60;
    config.cache.hydration_parallelism = 4;
    config.cache.hydration_max_segment_fraction = 0.9;
    config.indexing.default_num_centroids = 8;
    config.indexing.kmeans_max_iterations = 10;
    config.compaction = CompactionConfig {
        max_wal_fragments_before_compact: 1,
        ..Default::default()
    };
    config.server.rate_limit_rps = 1_000_000;
    config.server.rate_limit_burst = 1_000_000;
    config.server.write_rate_limit_rps = 1_000_000;
    config.server.write_rate_limit_burst = 1_000_000;
    config
}

async fn start_api_server(config: Config) -> ApiServer {
    zeppelin::metrics::init();

    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let cache_dir = TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 512 * 1024 * 1024).unwrap(),
    );
    let compactor = Arc::new(Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        config.compaction.clone(),
        config.indexing.clone(),
        Duration::from_secs(config.gc.compaction_upload_window_secs),
    ));
    let lease_manager = Arc::new(LeaseManager::new(
        store.clone(),
        format!("test-{}", uuid::Uuid::new_v4()),
        Duration::from_secs(config.compaction.lease_duration_secs),
    ));
    let oversample_factor = config.indexing.oversample_factor;
    let hydrator = if config.cache.hydration_enabled {
        Some(SegmentHydrator::start(
            store.clone(),
            cache.clone(),
            heat_policy_from_config(&config.cache).unwrap(),
            HydrationConfig::from_cache_config(&config.cache).unwrap(),
        ))
    } else {
        None
    };
    let query_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_queries,
    ));
    let runtime_query_config = Arc::new(RuntimeQueryConfig::from_config(&config));
    let query_knob_bounds = QueryKnobBounds::from_config(&config);
    let trusted_proxies = Arc::from(parse_trusted_proxies(&config.server.trusted_proxies).unwrap());
    let state = AppState {
        store: store.clone(),
        namespace_manager: Arc::new(NamespaceManager::new(store.clone())),
        namespace_name_prefix: None,
        wal_writer: Arc::new(WalWriter::new(store.clone())),
        wal_reader: Arc::new(WalReader::new(store.clone())),
        compactor: compactor.clone(),
        lease_manager,
        config: Arc::new(config),
        trusted_proxies,
        runtime_query_config,
        query_knob_bounds,
        cache: cache.clone(),
        manifest_cache: Arc::new(ManifestCache::new(Duration::ZERO)),
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

    ApiServer {
        base_url,
        harness,
        store,
        counter,
        cache,
        _cache_dir: cache_dir,
        compactor,
        oversample_factor,
    }
}

async fn create_compacted_namespace(
    server: &ApiServer,
    vector_count: usize,
    dimensions: usize,
) -> CompactedNamespace {
    let client = reqwest::Client::new();
    let create: serde_json::Value = client
        .post(format!("{}/v1/namespaces", server.base_url))
        .json(&json!({
            "dimensions": dimensions,
            "distance_metric": "euclidean"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let namespace = create["name"].as_str().unwrap().to_string();
    let vectors = random_vectors(vector_count, dimensions);
    let query = vectors[0].values.clone();
    WalWriter::new(server.store.clone())
        .append(&namespace, vectors, vec![])
        .await
        .unwrap();
    server.compactor.compact(&namespace).await.unwrap();
    let manifest = Manifest::read(&server.store, &namespace)
        .await
        .unwrap()
        .unwrap();
    let segment = active_segment_ref(&manifest).clone();
    assert!(
        !segment.cluster_objects.is_empty(),
        "fixture must compact into grouped cluster objects"
    );
    CompactedNamespace {
        namespace,
        query,
        segment,
    }
}

fn active_segment_ref(manifest: &Manifest) -> &SegmentRef {
    let active_segment = manifest
        .active_segment
        .as_ref()
        .expect("manifest must have an active segment");
    manifest
        .segments
        .iter()
        .find(|segment| segment.id == *active_segment)
        .expect("active segment must exist")
}

async fn all_cluster_objects_cached(cache: &DiskCache, segment: &SegmentRef) -> bool {
    for object in &segment.cluster_objects {
        match cache.get(&object.key).await {
            Some(bytes) if bytes.len() as u64 == object.size_bytes => {}
            _ => return false,
        }
    }
    true
}

async fn wait_for_cached_segment(cache: &DiskCache, segment: &SegmentRef) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if all_cluster_objects_cached(cache, segment).await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("admin hydration should cache the active segment");
}

fn hydration_jobs_total(trigger: &str) -> u64 {
    zeppelin::metrics::HYDRATION_JOBS_TOTAL
        .with_label_values(&[trigger])
        .get()
}

async fn post_hydrate(
    client: &reqwest::Client,
    base_url: &str,
    namespace: &str,
) -> reqwest::Response {
    let _guard = admin_request_lock().lock().await;
    client
        .post(format!("{base_url}/v1/namespaces/{namespace}/hydrate"))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn test_admin_hydrate_endpoint_hydrates_namespace() {
    let server = start_api_server(hydration_api_config()).await;
    let fixture = create_compacted_namespace(&server, 256, 32).await;
    let client = reqwest::Client::new();

    let resp = post_hydrate(&client, &server.base_url, &fixture.namespace).await;
    assert_eq!(resp.status(), 202);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["namespace"], fixture.namespace);
    assert_eq!(body["segment_id"], fixture.segment.id);

    wait_for_cached_segment(&server.cache, &fixture.segment).await;

    server.counter.reset();
    let wal_reader = WalReader::new(server.store.clone());
    let response = execute_query(QueryParams {
        store: &server.store,
        wal_reader: &wal_reader,
        namespace: &fixture.namespace,
        query: &fixture.query,
        top_k: 10,
        nprobe: 8,
        filter: None,
        consistency: ConsistencyLevel::Eventual,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: server.oversample_factor,
        rerank_coalesce_gap_bytes: DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: Some(&server.cache),
        manifest_cache: None,
        include_attributes: false,
    })
    .await
    .unwrap();
    assert!(!response.results.is_empty());
    assert_eq!(
        server.counter.gets_for(ArtifactClass::Cluster),
        0,
        "explicitly hydrated namespace should serve cluster data from cache"
    );

    server.harness.cleanup().await;
}

#[tokio::test]
async fn test_admin_hydrate_unknown_namespace_404() {
    let server = start_api_server(hydration_api_config()).await;
    let client = reqwest::Client::new();
    let missing = "missing-admin-hydrate-ns";

    let resp = post_hydrate(&client, &server.base_url, missing).await;
    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "NAMESPACE_NOT_FOUND");
    assert_eq!(body["status"], 404);
    assert_eq!(body["retryable"], false);
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("namespace not found"),
        "unexpected error body: {body}"
    );

    server.harness.cleanup().await;
}

#[tokio::test]
async fn test_admin_hydrate_disabled_config_errors() {
    let mut config = hydration_api_config();
    config.cache.hydration_enabled = false;
    let server = start_api_server(config).await;
    let client = reqwest::Client::new();
    let create: serde_json::Value = client
        .post(format!("{}/v1/namespaces", server.base_url))
        .json(&json!({"dimensions": 8, "distance_metric": "euclidean"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let namespace = create["name"].as_str().unwrap();

    let _guard = admin_request_lock().lock().await;
    let before_admin = hydration_jobs_total("admin");
    let resp = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/hydrate",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "HYDRATION_DISABLED");
    assert_eq!(body["status"], 409);
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("hydration is disabled by config"),
        "unexpected error body: {body}"
    );
    assert_eq!(
        hydration_jobs_total("admin"),
        before_admin,
        "disabled admin requests must not enqueue jobs"
    );

    server.harness.cleanup().await;
}

#[tokio::test]
async fn test_admin_hydrate_does_not_block() {
    let mut config = hydration_api_config();
    config.cache.hydration_parallelism = 1;
    config.indexing.default_num_centroids = 64;
    let server = start_api_server(config).await;
    let fixture = create_compacted_namespace(&server, 1024, 32).await;
    let client = reqwest::Client::new();

    let start = Instant::now();
    let resp = post_hydrate(&client, &server.base_url, &fixture.namespace).await;
    let elapsed = start.elapsed();
    assert_eq!(resp.status(), 202);
    assert!(
        elapsed < Duration::from_millis(500),
        "admin hydrate must accept asynchronously; elapsed={elapsed:?}"
    );
    assert!(
        !all_cluster_objects_cached(&server.cache, &fixture.segment).await,
        "admin hydrate response must not wait for all cluster objects to cache"
    );

    wait_for_cached_segment(&server.cache, &fixture.segment).await;
    server.harness.cleanup().await;
}

#[tokio::test]
async fn test_admin_hydrate_job_uses_admin_trigger_label() {
    let server = start_api_server(hydration_api_config()).await;
    let fixture = create_compacted_namespace(&server, 256, 32).await;
    let client = reqwest::Client::new();

    let _guard = admin_request_lock().lock().await;
    let before_admin = hydration_jobs_total("admin");
    let before_heat = hydration_jobs_total("heat");
    client
        .post(format!(
            "{}/v1/namespaces/{}/hydrate",
            server.base_url, fixture.namespace
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    assert_eq!(hydration_jobs_total("admin"), before_admin + 1);
    assert_eq!(
        hydration_jobs_total("heat"),
        before_heat,
        "explicit hydration must not be attributed to heat"
    );

    server.harness.cleanup().await;
}
