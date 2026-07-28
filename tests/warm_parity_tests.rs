mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::counting::{counting_store, ArtifactClass, GetCounter};
use common::harness::TestHarness;
use common::vectors::random_vectors;
use dashmap::DashMap;
use serde_json::json;
use tempfile::TempDir;
use zeppelin::cache::decoded_cache::DecodedArtifactCache;
use zeppelin::cache::hydration::{heat_policy_from_config, HydrationConfig, SegmentHydrator};
use zeppelin::cache::manifest_cache::ManifestCache;
use zeppelin::cache::DiskCache;
use zeppelin::compaction::background::CompactionLifecycle;
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, Config, DEFAULT_RERANK_COALESCE_GAP_BYTES};
use zeppelin::fts::wal_cache::WalFtsCache;
use zeppelin::namespace::NamespaceManager;
use zeppelin::query::{execute_query, QueryParams};
use zeppelin::runtime_config::{QueryKnobBounds, RuntimeQueryConfig};
use zeppelin::server::{build_router, parse_trusted_proxies, AppState, ServerTaskSupervisor};
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::{ConsistencyLevel, DistanceMetric, VectorEntry};
use zeppelin::wal::manifest::{Manifest, SegmentRef};
use zeppelin::wal::{LeaseManager, WalFragmentCache, WalReader, WalWriter};

struct ParityServer {
    base_url: String,
    admin_bearer: String,
    harness: TestHarness,
    store: ZeppelinStore,
    counter: GetCounter,
    cache: Arc<DiskCache>,
    cache_dir: TempDir,
    compactor: Arc<Compactor>,
    oversample_factor: usize,
}

struct NamespaceFixture {
    namespace: String,
    queries: Vec<Vec<f32>>,
    segment: SegmentRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResultSnapshot {
    id: String,
    score_bits: u32,
}

fn parity_config(num_centroids: usize) -> Config {
    let mut config = Config::default();
    config.cache.hydration_enabled = true;
    config.cache.hydration_heat_queries = 100;
    config.cache.hydration_heat_window_secs = 60;
    config.cache.hydration_parallelism = 4;
    config.cache.hydration_max_segment_fraction = 0.9;
    config.indexing.default_num_centroids = num_centroids;
    config.indexing.kmeans_max_iterations = 10;
    config.compaction = CompactionConfig {
        max_wal_fragments_before_compact: 1,
        retrain_imbalance_threshold: 0.0,
        ..Default::default()
    };
    config.server.rate_limit_rps = 1_000_000;
    config.server.rate_limit_burst = 1_000_000;
    config.server.write_rate_limit_rps = 1_000_000;
    config.server.write_rate_limit_burst = 1_000_000;
    config.server.principal_rate_limit_rps = 1_000_000;
    config.server.principal_rate_limit_burst = 1_000_000;
    config.server.principal_write_rate_limit_rps = 1_000_000;
    config.server.principal_write_rate_limit_burst = 1_000_000;
    config
}

async fn start_parity_server(mut config: Config) -> ParityServer {
    zeppelin::metrics::init();
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let clock = zeppelin::time::Clock::system();
    let security_store = common::server::scoped_test_security_store(&store, &harness.prefix);
    let (security, credential_adapter, admin_bearer) =
        common::server::test_security_runtime(&security_store, &mut config, &clock).await;
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
    let hydrator = SegmentHydrator::start(
        store.clone(),
        cache.clone(),
        heat_policy_from_config(&config.cache).unwrap(),
        HydrationConfig::from_cache_config(
            &config.cache,
            Duration::from_secs(config.server.request_timeout_secs),
        )
        .unwrap(),
    );
    let runtime_query_config = Arc::new(RuntimeQueryConfig::from_config(&config));
    let query_knob_bounds = QueryKnobBounds::from_config(&config);
    let trusted_proxies = Arc::from(parse_trusted_proxies(&config.server.trusted_proxies).unwrap());
    let (audit, audit_runtime, _audit_node_id) =
        common::server::start_test_audit(&config, &store, Some(&harness.prefix), &security).await;
    let server_tasks = Arc::new(ServerTaskSupervisor::new());
    let compaction_lifecycle = CompactionLifecycle::new();
    let state = AppState {
        store: store.clone(),
        clock: clock.clone(),
        security: Arc::clone(&security),
        audit,
        credential_adapter,
        namespace_manager: Arc::new(NamespaceManager::new(store.clone())),
        namespace_name_prefix: None,
        branch_readiness: zeppelin::namespace::BranchGraphReadinessSnapshot::new(),
        wal_writer: Arc::new(WalWriter::new(store.clone())),
        wal_reader: Arc::new(WalReader::new(store.clone())),
        compactor: compactor.clone(),
        lease_manager,
        compaction_lifecycle: compaction_lifecycle.clone(),
        server_tasks: Arc::clone(&server_tasks),
        fragment_cache: Arc::new(WalFragmentCache::new(
            config.cache.wal_fragment_cache_max_mb * 1024 * 1024,
        )),
        decoded_artifact_cache: Arc::new(DecodedArtifactCache::new(
            config.cache.decoded_artifact_cache_max_mb * 1024 * 1024,
        )),
        config: Arc::new(config),
        trusted_proxies,
        runtime_query_config,
        query_knob_bounds,
        cache: cache.clone(),
        manifest_cache: Arc::new(ManifestCache::new(Duration::ZERO)),
        hydrator: Some(hydrator),
        fts_cache: Arc::new(WalFtsCache::new()),
        query_semaphore: Arc::new(tokio::sync::Semaphore::new(128)),
        rate_limiters: Arc::new(DashMap::new()),
    };

    let app = build_router(state);
    let base_url = common::server::spawn_test_router_with_lifecycle(
        &harness,
        app,
        server_tasks,
        compaction_lifecycle,
        security,
        audit_runtime,
    )
    .await;

    ParityServer {
        base_url,
        admin_bearer,
        harness,
        store,
        counter,
        cache,
        cache_dir,
        compactor,
        oversample_factor,
    }
}

async fn create_compacted_namespace(
    server: &ParityServer,
    mut vectors: Vec<VectorEntry>,
    query_count: usize,
) -> NamespaceFixture {
    let client = crate::common::server::client_with_bearer(&server.admin_bearer);
    let dimensions = vectors[0].values.len();
    let queries = vectors
        .iter()
        .take(query_count)
        .map(|vector| vector.values.clone())
        .collect::<Vec<_>>();
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
    prefix_ids(&mut vectors, "gen1");
    WalWriter::new(server.store.clone())
        .append(&namespace, vectors, vec![])
        .await
        .unwrap();
    server.compactor.compact(&namespace).await.unwrap();
    let segment = active_segment(&server.store, &namespace).await;

    NamespaceFixture {
        namespace,
        queries,
        segment,
    }
}

fn prefix_ids(vectors: &mut [VectorEntry], prefix: &str) {
    for vector in vectors {
        vector.id = format!("{prefix}-{}", vector.id);
    }
}

async fn active_segment(store: &ZeppelinStore, namespace: &str) -> SegmentRef {
    let manifest = Manifest::read(store, namespace).await.unwrap().unwrap();
    let active_segment = manifest.active_segment.as_ref().unwrap();
    manifest
        .segments
        .iter()
        .find(|segment| segment.id == *active_segment)
        .unwrap()
        .clone()
}

async fn post_hydrate(server: &ParityServer, namespace: &str) {
    let body: serde_json::Value = crate::common::server::client_with_bearer(&server.admin_bearer)
        .post(format!(
            "{}/v1/namespaces/{namespace}/hydrate",
            server.base_url
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["namespace"], namespace);
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
    .expect("segment should hydrate");
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

async fn run_query_snapshots(
    server: &ParityServer,
    namespace: &str,
    queries: &[Vec<f32>],
    cache: Option<&Arc<DiskCache>>,
    manifest_cache: Option<&Arc<ManifestCache>>,
    nprobe: usize,
) -> zeppelin::error::Result<Vec<Vec<ResultSnapshot>>> {
    let wal_reader = WalReader::new(server.store.clone());
    let mut snapshots = Vec::with_capacity(queries.len());
    for query in queries {
        let response = execute_query(QueryParams {
            store: &server.store,
            wal_reader: &wal_reader,
            namespace,
            query,
            top_k: 10,
            nprobe,
            filter: None,
            consistency: ConsistencyLevel::Eventual,
            distance_metric: DistanceMetric::Euclidean,
            oversample_factor: server.oversample_factor,
            rerank_coalesce_gap_bytes: DEFAULT_RERANK_COALESCE_GAP_BYTES,
            cache,
            manifest_cache,
            include_attributes: false,
        })
        .await?;
        snapshots.push(
            response
                .results
                .into_iter()
                .map(|result| ResultSnapshot {
                    id: result.id,
                    score_bits: result.score.to_bits(),
                })
                .collect(),
        );
    }
    Ok(snapshots)
}

fn range_source_metric_value(source: &str) -> f64 {
    prometheus::gather()
        .into_iter()
        .find(|family| family.name() == "zeppelin_range_source_total")
        .map(|family| {
            family
                .get_metric()
                .iter()
                .filter(|metric| {
                    metric
                        .get_label()
                        .iter()
                        .any(|label| label.name() == "source" && label.value() == source)
                })
                .map(|metric| metric.get_counter().get_value())
                .sum()
        })
        .unwrap_or(0.0)
}

#[tokio::test]
async fn test_warm_parity_bit_identical() {
    let server = start_parity_server(parity_config(16)).await;
    let fixture = create_compacted_namespace(&server, random_vectors(512, 32), 8).await;
    let cold = run_query_snapshots(
        &server,
        &fixture.namespace,
        &fixture.queries,
        None,
        None,
        16,
    )
    .await
    .unwrap();

    post_hydrate(&server, &fixture.namespace).await;
    wait_for_cached_segment(&server.cache, &fixture.segment).await;

    let local_before = range_source_metric_value("local");
    server.counter.reset();
    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_secs(60)));
    let warm = run_query_snapshots(
        &server,
        &fixture.namespace,
        &fixture.queries,
        Some(&server.cache),
        Some(&manifest_cache),
        16,
    )
    .await
    .unwrap();

    assert_eq!(warm, cold, "warm results must be bit-identical to cold");
    assert!(
        range_source_metric_value("local") > local_before,
        "warm query set must serve at least one range from local cache"
    );
    assert_eq!(
        server.counter.gets_for(ArtifactClass::Cluster),
        0,
        "hydrated warm parity run must not fetch cluster objects from S3"
    );
    assert!(
        server.counter.total_gets() <= fixture.queries.len() as u64,
        "warm physical GETs must stay under 1/query: {} GETs for {} queries",
        server.counter.total_gets(),
        fixture.queries.len()
    );

    server.harness.cleanup().await;
}

#[tokio::test]
async fn test_warm_parity_detects_same_length_cached_corruption() {
    let server = start_parity_server(parity_config(16)).await;
    let fixture = create_compacted_namespace(&server, random_vectors(512, 32), 1).await;
    let cold = run_query_snapshots(
        &server,
        &fixture.namespace,
        &fixture.queries,
        None,
        None,
        16,
    )
    .await
    .unwrap();

    post_hydrate(&server, &fixture.namespace).await;
    wait_for_cached_segment(&server.cache, &fixture.segment).await;
    for object in &fixture.segment.cluster_objects {
        let mut bytes = server.cache.get(&object.key).await.unwrap().to_vec();
        for byte in bytes.iter_mut().step_by(97) {
            *byte ^= 0x5a;
        }
        server
            .cache
            .put(&object.key, &Bytes::from(bytes))
            .await
            .unwrap();
    }

    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_secs(60)));
    let warm = run_query_snapshots(
        &server,
        &fixture.namespace,
        &fixture.queries,
        Some(&server.cache),
        Some(&manifest_cache),
        16,
    )
    .await;
    if let Ok(warm) = warm {
        assert_ne!(
            warm, cold,
            "same-length cached corruption must not preserve bit-identical results"
        );
    }

    server.harness.cleanup().await;
}

#[tokio::test]
async fn test_rotation_gen1_to_gen2() {
    let server = start_parity_server(parity_config(16)).await;
    let fixture = create_compacted_namespace(&server, random_vectors(512, 32), 4).await;
    post_hydrate(&server, &fixture.namespace).await;
    wait_for_cached_segment(&server.cache, &fixture.segment).await;

    let mut gen2_vectors = random_vectors(96, 32);
    prefix_ids(&mut gen2_vectors, "gen2");
    WalWriter::new(server.store.clone())
        .append(&fixture.namespace, gen2_vectors, vec![])
        .await
        .unwrap();
    server.compactor.compact(&fixture.namespace).await.unwrap();
    let gen2_segment = active_segment(&server.store, &fixture.namespace).await;
    assert_ne!(fixture.segment.id, gen2_segment.id);
    assert!(
        !all_cluster_objects_cached(&server.cache, &gen2_segment).await,
        "new generation must not appear cached before gen2 hydration"
    );

    let cold_gen2 = run_query_snapshots(
        &server,
        &fixture.namespace,
        &fixture.queries,
        None,
        None,
        16,
    )
    .await
    .unwrap();
    server.counter.reset();
    let manifest_cache = Arc::new(ManifestCache::new(Duration::ZERO));
    let prehydrate_gen2 = run_query_snapshots(
        &server,
        &fixture.namespace,
        &fixture.queries,
        Some(&server.cache),
        Some(&manifest_cache),
        16,
    )
    .await
    .unwrap();
    assert_eq!(
        prehydrate_gen2, cold_gen2,
        "gen2 queries must be correct while gen2 is cold"
    );
    assert!(
        server.counter.gets_for(ArtifactClass::Cluster) > 0,
        "gen2 cold misses must fetch gen2 cluster objects instead of reusing gen1 cache"
    );

    post_hydrate(&server, &fixture.namespace).await;
    wait_for_cached_segment(&server.cache, &gen2_segment).await;
    server.counter.reset();
    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_secs(60)));
    let warm_gen2 = run_query_snapshots(
        &server,
        &fixture.namespace,
        &fixture.queries,
        Some(&server.cache),
        Some(&manifest_cache),
        16,
    )
    .await
    .unwrap();
    assert_eq!(warm_gen2, cold_gen2);
    assert_eq!(
        server.counter.gets_for(ArtifactClass::Cluster),
        0,
        "gen2 hydration must eliminate gen2 cluster S3 GETs"
    );

    server.harness.cleanup().await;
}

#[tokio::test]
async fn test_restart_rebuild_identical() {
    let server = start_parity_server(parity_config(16)).await;
    let fixture = create_compacted_namespace(&server, random_vectors(512, 32), 4).await;
    let cold = run_query_snapshots(
        &server,
        &fixture.namespace,
        &fixture.queries,
        None,
        None,
        16,
    )
    .await
    .unwrap();
    post_hydrate(&server, &fixture.namespace).await;
    wait_for_cached_segment(&server.cache, &fixture.segment).await;

    let rebuilt_cache = Arc::new(
        DiskCache::new_with_max_bytes(server.cache_dir.path().to_path_buf(), 512 * 1024 * 1024)
            .unwrap(),
    );
    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_secs(60)));
    let rebuilt = run_query_snapshots(
        &server,
        &fixture.namespace,
        &fixture.queries,
        Some(&rebuilt_cache),
        Some(&manifest_cache),
        16,
    )
    .await
    .unwrap();
    assert_eq!(rebuilt, cold, "rebuilt cache index must preserve results");

    tokio::fs::remove_dir_all(server.cache_dir.path())
        .await
        .unwrap();
    let empty_cache = Arc::new(
        DiskCache::new_with_max_bytes(server.cache_dir.path().to_path_buf(), 512 * 1024 * 1024)
            .unwrap(),
    );
    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_secs(60)));
    let after_loss = run_query_snapshots(
        &server,
        &fixture.namespace,
        &fixture.queries,
        Some(&empty_cache),
        Some(&manifest_cache),
        16,
    )
    .await
    .unwrap();
    assert_eq!(
        after_loss, cold,
        "cache directory loss must degrade to cold misses without changing results"
    );

    server.harness.cleanup().await;
}

#[tokio::test]
async fn test_np128_exact_sentinel_warm_and_cold() {
    let server = start_parity_server(parity_config(128)).await;
    let fixture = create_compacted_namespace(&server, random_vectors(1024, 32), 4).await;
    assert_eq!(fixture.segment.cluster_count, 128);
    let cold = run_query_snapshots(
        &server,
        &fixture.namespace,
        &fixture.queries,
        None,
        None,
        128,
    )
    .await
    .unwrap();

    post_hydrate(&server, &fixture.namespace).await;
    wait_for_cached_segment(&server.cache, &fixture.segment).await;
    server.counter.reset();
    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_secs(60)));
    let warm = run_query_snapshots(
        &server,
        &fixture.namespace,
        &fixture.queries,
        Some(&server.cache),
        Some(&manifest_cache),
        128,
    )
    .await
    .unwrap();
    assert_eq!(warm, cold, "np128 warm sentinel must match cold exactly");
    assert_eq!(
        server.counter.gets_for(ArtifactClass::Cluster),
        0,
        "np128 warm sentinel must not fetch hydrated cluster objects"
    );

    server.harness.cleanup().await;
}
