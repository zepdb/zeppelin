mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::counting::{counting_store, ArtifactClass};
use common::harness::TestHarness;
use common::server::start_test_server_with_compactor;
use common::vectors::{random_vectors, simple_attributes, with_attributes};
use serde_json::json;
use tempfile::TempDir;
use zeppelin::cache::hydration::{HydrationConfig, SegmentHydrator, SessionWindowPolicy};
use zeppelin::cache::DiskCache;
use zeppelin::compaction::Compactor;
use zeppelin::config::{
    CacheConfig, CompactionConfig, Config, IndexingConfig, DEFAULT_RERANK_COALESCE_GAP_BYTES,
};
use zeppelin::fts::global_index::global_fts_key;
use zeppelin::fts::rank_by::RankBy;
use zeppelin::fts::FtsFieldConfig;
use zeppelin::index::bitmap::bitmap_key;
use zeppelin::query::{execute_bm25_query, execute_query, QueryParams};
use zeppelin::types::{AttributeValue, ConsistencyLevel, DistanceMetric, Filter, VectorEntry};
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

fn base_indexing_config() -> IndexingConfig {
    IndexingConfig {
        default_num_centroids: 8,
        kmeans_max_iterations: 10,
        ..Default::default()
    }
}

fn test_compactor_with(
    store: &zeppelin::storage::ZeppelinStore,
    indexing: IndexingConfig,
) -> Compactor {
    Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        CompactionConfig {
            max_wal_fragments_before_compact: 1,
            ..Default::default()
        },
        indexing,
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
    hydration_fixture_with(
        name,
        random_vectors(256, 32),
        base_indexing_config(),
        &HashMap::new(),
    )
    .await
}

async fn hydration_fixture_with(
    name: &str,
    vectors: Vec<VectorEntry>,
    indexing: IndexingConfig,
    fts_configs: &HashMap<String, FtsFieldConfig>,
) -> HydrationFixture {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let namespace = harness.key(name);
    Manifest::new().write(&store, &namespace).await.unwrap();

    let query = vectors[0].values.clone();
    WalWriter::new(store.clone())
        .append(&namespace, vectors, vec![])
        .await
        .unwrap();
    let compactor = test_compactor_with(&store, indexing);
    if fts_configs.is_empty() {
        compactor.compact(&namespace).await.unwrap();
    } else {
        compactor
            .compact_with_fts(&namespace, None, fts_configs)
            .await
            .unwrap();
    }

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

fn test_attrs_key(namespace: &str, segment_id: &str, cluster_idx: usize) -> String {
    format!("{namespace}/segments/{segment_id}/attrs_{cluster_idx}.bin")
}

fn attrs_keys(namespace: &str, segment: &SegmentRef) -> Vec<String> {
    (0..segment.cluster_count)
        .map(|cluster_idx| {
            test_attrs_key(namespace, segment.cluster_owner(cluster_idx), cluster_idx)
        })
        .collect()
}

fn bitmap_keys(namespace: &str, segment: &SegmentRef) -> Vec<String> {
    if segment.bitmap_fields.is_empty() {
        return Vec::new();
    }
    (0..segment.cluster_count)
        .map(|cluster_idx| bitmap_key(namespace, segment.cluster_owner(cluster_idx), cluster_idx))
        .collect()
}

async fn wait_for_cached_keys(cache: &DiskCache, keys: &[String]) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if futures::future::join_all(keys.iter().map(|key| cache.get(key)))
                .await
                .into_iter()
                .all(|entry| entry.is_some())
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("sidecar keys should hydrate");
}

fn attributed_vectors() -> Vec<VectorEntry> {
    with_attributes(random_vectors(256, 32), simple_attributes)
}

fn category_filter() -> Filter {
    Filter::Eq {
        field: "category".to_string(),
        value: AttributeValue::String("a".to_string()),
    }
}

fn fts_configs() -> HashMap<String, FtsFieldConfig> {
    let mut configs = HashMap::new();
    configs.insert(
        "content".to_string(),
        FtsFieldConfig {
            stemming: true,
            remove_stopwords: true,
            ..Default::default()
        },
    );
    configs
}

fn content_doc(id: &str, text: &str) -> VectorEntry {
    let mut attrs = HashMap::new();
    attrs.insert(
        "content".to_string(),
        AttributeValue::String(text.to_string()),
    );
    VectorEntry {
        id: id.to_string(),
        values: vec![0.1, 0.2, 0.3, 0.4],
        attributes: Some(attrs),
    }
}

fn fts_vectors() -> Vec<VectorEntry> {
    vec![
        content_doc("doc1", "Rust programming language is fast and memory safe"),
        content_doc(
            "doc2",
            "Python programming language is interpreted and dynamic",
        ),
        content_doc("doc3", "Java programming language runs on the JVM"),
        content_doc(
            "doc4",
            "Cooking delicious Italian pasta requires fresh ingredients",
        ),
        content_doc(
            "doc5",
            "Rust compiler ensures memory safety without garbage collection",
        ),
        content_doc(
            "doc6",
            "Go programming language is designed for concurrency",
        ),
        content_doc("doc7", "JavaScript runs in web browsers and Node"),
        content_doc(
            "doc8",
            "Rust ownership model prevents data races at compile time",
        ),
    ]
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

fn start_test_hydrator(
    fixture: &HydrationFixture,
    cache: Arc<DiskCache>,
    max_segment_fraction: f64,
) -> Arc<SegmentHydrator> {
    let policy = Arc::new(SessionWindowPolicy::new(1, Duration::from_secs(60)).unwrap());
    SegmentHydrator::start(
        fixture.store.clone(),
        cache,
        policy,
        test_hydration_config(max_segment_fraction),
    )
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
async fn test_hydration_includes_attrs_sidecars() {
    zeppelin::metrics::init();
    let fixture = hydration_fixture_with(
        "hydrate-attrs",
        attributed_vectors(),
        base_indexing_config(),
        &HashMap::new(),
    )
    .await;
    let (_cache_dir, cache) = test_cache(512 * 1024 * 1024);
    let hydrator = start_test_hydrator(&fixture, cache.clone(), 0.5);
    let attrs_keys = attrs_keys(&fixture.namespace, &fixture.segment);

    hydrator.observe_query(&fixture.namespace, &fixture.segment);
    wait_for_cached_segment(&cache, &fixture.segment).await;
    let attrs_cached_before_query =
        futures::future::join_all(attrs_keys.iter().map(|key| cache.get(key)))
            .await
            .into_iter()
            .all(|entry| entry.is_some());

    fixture.counter.reset();
    let filter = category_filter();
    let wal_reader = WalReader::new(fixture.store.clone());
    let mut params = query_params(&fixture, &wal_reader, Some(&cache));
    params.filter = Some(&filter);
    let response = execute_query(params).await.unwrap();
    assert!(!response.results.is_empty());
    assert_eq!(
        fixture.counter.gets_for(ArtifactClass::Attrs),
        0,
        "hydrated attrs sidecars should eliminate attrs S3 GETs"
    );
    assert!(
        attrs_cached_before_query,
        "hydrator must cache attrs sidecars before query traffic naturally does"
    );

    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn test_hydration_includes_bitmap_sidecars() {
    zeppelin::metrics::init();
    let mut indexing = base_indexing_config();
    indexing.bitmap_index = true;
    let fixture = hydration_fixture_with(
        "hydrate-bitmap",
        attributed_vectors(),
        indexing,
        &HashMap::new(),
    )
    .await;
    assert!(
        !fixture.segment.bitmap_fields.is_empty(),
        "fixture must build bitmap sidecars"
    );
    let (_cache_dir, cache) = test_cache(512 * 1024 * 1024);
    let hydrator = start_test_hydrator(&fixture, cache.clone(), 0.5);
    let mut sidecar_keys = attrs_keys(&fixture.namespace, &fixture.segment);
    let bitmap_keys = bitmap_keys(&fixture.namespace, &fixture.segment);
    assert!(!bitmap_keys.is_empty(), "bitmap keys must be planned");
    sidecar_keys.extend(bitmap_keys);

    hydrator.observe_query(&fixture.namespace, &fixture.segment);
    wait_for_cached_segment(&cache, &fixture.segment).await;
    let sidecars_cached_before_query =
        futures::future::join_all(sidecar_keys.iter().map(|key| cache.get(key)))
            .await
            .into_iter()
            .all(|entry| entry.is_some());

    fixture.counter.reset();
    let filter = category_filter();
    let wal_reader = WalReader::new(fixture.store.clone());
    let mut params = query_params(&fixture, &wal_reader, Some(&cache));
    params.filter = Some(&filter);
    let response = execute_query(params).await.unwrap();
    assert!(!response.results.is_empty());
    assert_eq!(
        fixture.counter.gets_for(ArtifactClass::Bitmap),
        0,
        "hydrated bitmap sidecars should eliminate bitmap S3 GETs"
    );
    assert_eq!(
        fixture.counter.gets_for(ArtifactClass::Attrs),
        0,
        "bitmap-filtered warm queries should not re-fetch attrs from S3"
    );
    assert!(
        sidecars_cached_before_query,
        "hydrator must cache bitmap and attrs sidecars before query traffic naturally does"
    );

    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn test_hydration_includes_global_fts_index() {
    zeppelin::metrics::init();
    let mut indexing = base_indexing_config();
    indexing.default_num_centroids = 4;
    indexing.fts_index = true;
    let fts_configs = fts_configs();
    let fixture =
        hydration_fixture_with("hydrate-fts", fts_vectors(), indexing, &fts_configs).await;
    assert!(
        fixture.segment.has_global_fts,
        "fixture must build a global FTS sidecar"
    );
    let (_cache_dir, cache) = test_cache(512 * 1024 * 1024);
    let hydrator = start_test_hydrator(&fixture, cache.clone(), 0.5);
    let global_key = global_fts_key(&fixture.namespace, &fixture.segment.id);

    hydrator.observe_query(&fixture.namespace, &fixture.segment);
    wait_for_cached_segment(&cache, &fixture.segment).await;
    let global_cached_before_query = cache.get(&global_key).await.is_some();

    fixture.counter.reset();
    let wal_reader = WalReader::new(fixture.store.clone());
    let rank_by = RankBy::Bm25 {
        field: "content".to_string(),
        query: "rust programming".to_string(),
    };
    let response = execute_bm25_query(
        &fixture.store,
        &wal_reader,
        &fixture.namespace,
        &rank_by,
        &fts_configs,
        10,
        None,
        ConsistencyLevel::Eventual,
        false,
        None,
        None,
        Some(&cache),
        0,
        false,
    )
    .await
    .unwrap();
    assert!(!response.results.is_empty());
    assert_eq!(
        fixture.counter.gets_for(ArtifactClass::Fts),
        0,
        "hydrated global FTS sidecar should eliminate FTS S3 GETs"
    );
    assert!(
        global_cached_before_query,
        "hydrator must cache the global FTS sidecar before query traffic naturally does"
    );

    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn test_capacity_counts_sidecars() {
    zeppelin::metrics::init();
    let fixture = hydration_fixture_with(
        "hydrate-capacity-sidecars",
        attributed_vectors(),
        base_indexing_config(),
        &HashMap::new(),
    )
    .await;
    let cluster_bytes: u64 = fixture
        .segment
        .cluster_objects
        .iter()
        .map(|object| object.size_bytes)
        .sum();
    let mut attrs_bytes = 0u64;
    for key in attrs_keys(&fixture.namespace, &fixture.segment) {
        attrs_bytes += fixture.store.get(&key).await.unwrap().len() as u64;
    }
    assert!(
        attrs_bytes > 1,
        "fixture must have attrs sidecar bytes for the capacity boundary"
    );
    let (_cache_dir, cache) = test_cache(cluster_bytes + attrs_bytes - 1);
    let hydrator = start_test_hydrator(&fixture, cache.clone(), 1.0);

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
        "sidecar-aware capacity refusal must not fetch cluster objects"
    );
    for object in &fixture.segment.cluster_objects {
        assert_eq!(cache.get(&object.key).await, None);
    }

    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn test_no_sidecar_keys_hydrated_when_not_configured() {
    zeppelin::metrics::init();
    let fixture = hydration_fixture("hydrate-no-extra-sidecars").await;
    assert!(fixture.segment.bitmap_fields.is_empty());
    assert!(!fixture.segment.has_global_fts);
    let (_cache_dir, cache) = test_cache(512 * 1024 * 1024);
    let hydrator = start_test_hydrator(&fixture, cache.clone(), 0.5);
    let mut expected_keys = fixture
        .segment
        .cluster_objects
        .iter()
        .map(|object| object.key.clone())
        .collect::<Vec<_>>();
    expected_keys.extend(attrs_keys(&fixture.namespace, &fixture.segment));

    hydrator.observe_query(&fixture.namespace, &fixture.segment);
    wait_for_cached_keys(&cache, &expected_keys).await;

    assert_eq!(
        bitmap_keys(&fixture.namespace, &fixture.segment),
        Vec::<String>::new()
    );
    assert_eq!(
        cache
            .get(&global_fts_key(&fixture.namespace, &fixture.segment.id))
            .await,
        None
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
