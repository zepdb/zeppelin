mod common;

use std::sync::Arc;
use std::time::Duration;

use common::counting::{counting_store, ArtifactClass};
use common::harness::TestHarness;
use common::server::start_test_server_with_compactor;
use common::vectors::random_vectors;
use serde_json::json;
use tempfile::TempDir;
use zeppelin::cache::hydration::{HydrationConfig, SegmentHydrator, SessionWindowPolicy};
use zeppelin::cache::DiskCache;
use zeppelin::compaction::Compactor;
use zeppelin::config::{
    CacheConfig, CompactionConfig, Config, IndexingConfig, DEFAULT_RERANK_COALESCE_GAP_BYTES,
};
use zeppelin::query::{execute_query, QueryParams};
use zeppelin::types::{ConsistencyLevel, DistanceMetric};
use zeppelin::wal::manifest::{Manifest, SegmentRef};
use zeppelin::wal::{WalReader, WalWriter};

struct HydrationFixture {
    harness: TestHarness,
    store: zeppelin::storage::ZeppelinStore,
    counter: common::counting::GetCounter,
    namespace: String,
    query: Vec<f32>,
    segment: SegmentRef,
}

fn test_compactor(store: &zeppelin::storage::ZeppelinStore) -> Compactor {
    Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        CompactionConfig {
            max_wal_fragments_before_compact: 1,
            ..Default::default()
        },
        IndexingConfig {
            default_num_centroids: 8,
            kmeans_max_iterations: 10,
            ..Default::default()
        },
    )
}

fn active_segment_ref(manifest: &Manifest) -> &SegmentRef {
    let active_segment = manifest
        .active_segment
        .as_ref()
        .expect("manifest must have active segment");
    manifest
        .segments
        .iter()
        .find(|segment| segment.id == *active_segment)
        .expect("active segment must be present")
}

async fn hydration_fixture(name: &str) -> HydrationFixture {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let namespace = harness.key(name);
    Manifest::new().write(&store, &namespace).await.unwrap();

    let vectors = random_vectors(256, 32);
    let query = vectors[0].values.clone();
    WalWriter::new(store.clone())
        .append(&namespace, vectors, vec![])
        .await
        .unwrap();
    test_compactor(&store).compact(&namespace).await.unwrap();

    let manifest = Manifest::read(&store, &namespace).await.unwrap().unwrap();
    let segment = active_segment_ref(&manifest).clone();
    assert!(
        !segment.cluster_objects.is_empty(),
        "fixture must create grouped cluster objects"
    );

    HydrationFixture {
        harness,
        store,
        counter,
        namespace,
        query,
        segment,
    }
}

fn test_cache(max_size_bytes: u64) -> (TempDir, Arc<DiskCache>) {
    let dir = TempDir::new().unwrap();
    let cache =
        Arc::new(DiskCache::new_with_max_bytes(dir.path().to_path_buf(), max_size_bytes).unwrap());
    (dir, cache)
}

fn test_hydration_config(max_segment_fraction: f64) -> HydrationConfig {
    HydrationConfig {
        parallelism: 4,
        max_segment_fraction,
        max_retries: 0,
        retry_backoff: Duration::from_millis(1),
    }
}

fn metric_value(name: &str, labels: &[(&str, &str)]) -> f64 {
    prometheus::gather()
        .into_iter()
        .find(|family| family.name() == name)
        .map(|family| {
            family
                .get_metric()
                .iter()
                .filter(|metric| {
                    labels.iter().all(|(name, value)| {
                        metric
                            .get_label()
                            .iter()
                            .any(|label| label.name() == *name && label.value() == *value)
                    })
                })
                .map(|metric| metric.get_counter().get_value())
                .sum()
        })
        .unwrap_or(0.0)
}

async fn wait_for_cached_segment(cache: &DiskCache, segment: &SegmentRef) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut all_cached = true;
            for object in &segment.cluster_objects {
                match cache.get(&object.key).await {
                    Some(bytes) if bytes.len() as u64 == object.size_bytes => {}
                    _ => {
                        all_cached = false;
                        break;
                    }
                }
            }
            if all_cached {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("segment objects should hydrate");
}

async fn wait_for_metric_increase(name: &str, labels: &[(&str, &str)], before: f64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if metric_value(name, labels) > before {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("metric should increase");
}

fn query_params<'a>(
    fixture: &'a HydrationFixture,
    wal_reader: &'a WalReader,
    cache: Option<&'a Arc<DiskCache>>,
) -> QueryParams<'a> {
    QueryParams {
        store: &fixture.store,
        wal_reader,
        namespace: &fixture.namespace,
        query: &fixture.query,
        top_k: 10,
        nprobe: 8,
        filter: None,
        consistency: ConsistencyLevel::Eventual,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 3,
        rerank_coalesce_gap_bytes: DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache,
        manifest_cache: None,
        include_attributes: false,
    }
}

#[tokio::test]
async fn test_hot_namespace_hydrates_active_segment() {
    zeppelin::metrics::init();
    let fixture = hydration_fixture("hydrate-hot").await;
    let (_cache_dir, cache) = test_cache(512 * 1024 * 1024);
    let policy = Arc::new(SessionWindowPolicy::new(3, Duration::from_secs(60)).unwrap());
    let hydrator = SegmentHydrator::start(
        fixture.store.clone(),
        cache.clone(),
        policy,
        test_hydration_config(0.5),
    );

    hydrator.observe_query(&fixture.namespace, &fixture.segment);
    hydrator.observe_query(&fixture.namespace, &fixture.segment);
    hydrator.observe_query(&fixture.namespace, &fixture.segment);
    wait_for_cached_segment(&cache, &fixture.segment).await;

    fixture.counter.reset();
    let wal_reader = WalReader::new(fixture.store.clone());
    let response = execute_query(query_params(&fixture, &wal_reader, Some(&cache)))
        .await
        .unwrap();
    assert!(!response.results.is_empty());
    assert_eq!(
        fixture.counter.gets_for(ArtifactClass::Cluster),
        0,
        "hydrated full cluster objects should eliminate cluster S3 GETs"
    );

    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn test_query_handler_triggers_hydration_when_enabled() {
    zeppelin::metrics::init();
    let mut config = Config::default();
    config.cache.hydration_enabled = true;
    config.cache.hydration_heat_queries = 3;
    config.cache.hydration_heat_window_secs = 60;
    config.indexing.default_num_centroids = 8;
    config.indexing.kmeans_max_iterations = 10;
    config.compaction.max_wal_fragments_before_compact = 1;

    let (base_url, harness, cache, _cache_dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let create: serde_json::Value = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&json!({"dimensions": 32, "distance_metric": "euclidean"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let namespace = create["name"]
        .as_str()
        .expect("create response must include namespace name")
        .to_string();

    let vectors = random_vectors(256, 32);
    let query = vectors[0].values.clone();
    WalWriter::new(harness.store.clone())
        .append(&namespace, vectors, vec![])
        .await
        .unwrap();
    compactor.compact(&namespace).await.unwrap();
    let manifest = Manifest::read(&harness.store, &namespace)
        .await
        .unwrap()
        .unwrap();
    let segment = active_segment_ref(&manifest).clone();

    for _ in 0..3 {
        client
            .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
            .json(&json!({
                "vector": query,
                "top_k": 10,
                "nprobe": 8,
                "include_attributes": false
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }
    wait_for_cached_segment(&cache, &segment).await;

    harness
        .store
        .delete_prefix(&format!("{namespace}/"))
        .await
        .unwrap();
    harness.cleanup().await;
}

#[tokio::test]
async fn test_hydration_disabled_is_inert() {
    let fixture = hydration_fixture("hydrate-disabled").await;
    let (_cache_dir, cache) = test_cache(512 * 1024 * 1024);
    let config = CacheConfig::default();
    assert!(
        !config.hydration_enabled,
        "hydration must dark-launch disabled by default"
    );

    let wal_reader = WalReader::new(fixture.store.clone());
    for _ in 0..3 {
        execute_query(query_params(&fixture, &wal_reader, Some(&cache)))
            .await
            .unwrap();
    }

    for object in &fixture.segment.cluster_objects {
        assert_eq!(cache.get(&object.key).await, None);
    }

    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn test_incremental_segment_refuses_hydration() {
    zeppelin::metrics::init();
    let fixture = hydration_fixture("hydrate-incremental-gate").await;
    let (_cache_dir, cache) = test_cache(512 * 1024 * 1024);
    let policy = Arc::new(SessionWindowPolicy::new(1, Duration::from_secs(60)).unwrap());
    let hydrator = SegmentHydrator::start(
        fixture.store.clone(),
        cache.clone(),
        policy,
        test_hydration_config(0.5),
    );
    let mut segment = fixture.segment.clone();
    segment.cluster_owners = vec!["older-segment".to_string(); segment.cluster_count];

    let before = metric_value(
        "zeppelin_hydration_skipped_total",
        &[("reason", "incremental_segment")],
    );
    fixture.counter.reset();
    hydrator.observe_query(&fixture.namespace, &segment);
    wait_for_metric_increase(
        "zeppelin_hydration_skipped_total",
        &[("reason", "incremental_segment")],
        before,
    )
    .await;

    assert_eq!(
        fixture.counter.gets_for(ArtifactClass::Cluster),
        0,
        "incremental-gated hydration must not fetch cluster objects"
    );

    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn test_size_mismatch_aborts_hydration() {
    zeppelin::metrics::init();
    let fixture = hydration_fixture("hydrate-size-mismatch").await;
    let (_cache_dir, cache) = test_cache(512 * 1024 * 1024);
    let policy = Arc::new(SessionWindowPolicy::new(1, Duration::from_secs(60)).unwrap());
    let hydrator = SegmentHydrator::start(
        fixture.store.clone(),
        cache.clone(),
        policy,
        test_hydration_config(0.5),
    );
    let mut segment = fixture.segment.clone();
    segment.cluster_objects[0].size_bytes += 1;
    let bad_key = segment.cluster_objects[0].key.clone();

    let before = metric_value("zeppelin_hydration_failures_total", &[]);
    hydrator.observe_query(&fixture.namespace, &segment);
    wait_for_metric_increase("zeppelin_hydration_failures_total", &[], before).await;

    assert_eq!(
        cache.get(&bad_key).await,
        None,
        "size-mismatched object must be evicted after failed hydration"
    );

    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn test_capacity_refusal_baseline() {
    zeppelin::metrics::init();
    let fixture = hydration_fixture("hydrate-capacity").await;
    let (_cache_dir, cache) = test_cache(1);
    let policy = Arc::new(SessionWindowPolicy::new(1, Duration::from_secs(60)).unwrap());
    let hydrator = SegmentHydrator::start(
        fixture.store.clone(),
        cache.clone(),
        policy,
        test_hydration_config(0.5),
    );

    let before = metric_value(
        "zeppelin_hydration_skipped_total",
        &[("reason", "capacity")],
    );
    fixture.counter.reset();
    hydrator.observe_query(&fixture.namespace, &fixture.segment);
    wait_for_metric_increase(
        "zeppelin_hydration_skipped_total",
        &[("reason", "capacity")],
        before,
    )
    .await;

    assert_eq!(
        fixture.counter.gets_for(ArtifactClass::Cluster),
        0,
        "capacity-refused hydration must not fetch cluster objects"
    );
    for object in &fixture.segment.cluster_objects {
        assert_eq!(cache.get(&object.key).await, None);
    }

    fixture.harness.cleanup().await;
}
