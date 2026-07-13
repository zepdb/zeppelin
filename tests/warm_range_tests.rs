mod common;

use std::sync::Arc;

use common::counting::{counting_store, ArtifactClass};
use common::harness::TestHarness;
use common::vectors::random_vectors;
use tempfile::TempDir;
use zeppelin::cache::DiskCache;
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig, DEFAULT_RERANK_COALESCE_GAP_BYTES};
use zeppelin::query::{execute_query, QueryParams};
use zeppelin::types::{ConsistencyLevel, DistanceMetric};
use zeppelin::wal::manifest::{Manifest, SegmentRef};
use zeppelin::wal::{WalReader, WalWriter};

struct WarmRangeFixture {
    harness: TestHarness,
    store: zeppelin::storage::ZeppelinStore,
    counter: common::counting::GetCounter,
    namespace: String,
    query: Vec<f32>,
    cluster_keys: Vec<String>,
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
        common::default_gc_upload_window(),
    )
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
        .expect("active segment must be present in manifest segments")
}

async fn warm_range_fixture(name: &str) -> WarmRangeFixture {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let namespace = harness.key(name);
    common::write_active_namespace_metadata(&store, &namespace, 32, DistanceMetric::Euclidean)
        .await;
    Manifest::new().write(&store, &namespace).await.unwrap();

    let vectors = random_vectors(256, 32);
    let query = vectors[0].values.clone();
    WalWriter::new(store.clone())
        .append(&namespace, vectors, vec![])
        .await
        .unwrap();
    test_compactor(&store).compact(&namespace).await.unwrap();

    let manifest = Manifest::read(&store, &namespace).await.unwrap().unwrap();
    let segment = active_segment_ref(&manifest);
    let cluster_keys: Vec<String> = segment
        .cluster_objects
        .iter()
        .map(|object| object.key.clone())
        .collect();
    assert!(
        !cluster_keys.is_empty(),
        "fixture must create grouped cluster objects"
    );

    WarmRangeFixture {
        harness,
        store,
        counter,
        namespace,
        query,
        cluster_keys,
    }
}

fn test_cache() -> (TempDir, Arc<DiskCache>) {
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(dir.path().to_path_buf(), 512 * 1024 * 1024).unwrap(),
    );
    (dir, cache)
}

fn query_params<'a>(
    fixture: &'a WarmRangeFixture,
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

async fn run_query(
    fixture: &WarmRangeFixture,
    cache: Option<&Arc<DiskCache>>,
) -> Vec<(String, u32)> {
    let wal_reader = WalReader::new(fixture.store.clone());
    let response = execute_query(query_params(fixture, &wal_reader, cache))
        .await
        .unwrap();
    assert!(!response.results.is_empty(), "query must return results");
    response
        .results
        .into_iter()
        .map(|result| (result.id, result.score.to_bits()))
        .collect()
}

async fn cache_full_cluster_objects(fixture: &WarmRangeFixture, cache: &DiskCache) {
    for key in &fixture.cluster_keys {
        let bytes = fixture.store.get(key).await.unwrap();
        cache.put(key, &bytes).await.unwrap();
    }
}

fn range_source_metric_value(phase: &str, source: &str) -> f64 {
    prometheus::gather()
        .into_iter()
        .find(|family| family.name() == "zeppelin_range_source_total")
        .and_then(|family| {
            family
                .get_metric()
                .iter()
                .find(|metric| {
                    let mut phase_match = false;
                    let mut source_match = false;
                    for label in metric.get_label() {
                        match label.name() {
                            "phase" if label.value() == phase => phase_match = true,
                            "source" if label.value() == source => source_match = true,
                            _ => {}
                        }
                    }
                    phase_match && source_match
                })
                .map(|metric| metric.get_counter().get_value())
        })
        .unwrap_or(0.0)
}

fn range_source_metric_value_for_source(source: &str) -> f64 {
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
async fn test_cached_full_cluster_objects_eliminate_cluster_range_gets() {
    let fixture = warm_range_fixture("warm-range-local").await;
    let (_cache_dir, cache) = test_cache();
    cache_full_cluster_objects(&fixture, &cache).await;

    fixture.counter.reset();
    run_query(&fixture, Some(&cache)).await;

    assert_eq!(
        fixture.counter.gets_for(ArtifactClass::Cluster),
        0,
        "cached full cluster objects must serve SQ, rerank, and layout-header ranges locally"
    );

    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn test_empty_cache_range_plan_matches_no_cache_and_does_not_insert_ranges() {
    let no_cache_fixture = warm_range_fixture("warm-range-cold-plan-no-cache").await;

    no_cache_fixture.counter.reset();
    run_query(&no_cache_fixture, None).await;
    let no_cache_gets = no_cache_fixture.counter.gets_for(ArtifactClass::Cluster);
    let no_cache_bytes = no_cache_fixture
        .counter
        .get_bytes_for(ArtifactClass::Cluster);
    assert!(no_cache_gets > 0, "cold baseline must read cluster ranges");
    println!(
        "cold baseline without cache: cluster_gets={no_cache_gets} cluster_bytes={no_cache_bytes}"
    );

    let fixture = warm_range_fixture("warm-range-cold-plan-empty-cache").await;
    let (_cache_dir, cache) = test_cache();
    fixture.counter.reset();
    run_query(&fixture, Some(&cache)).await;
    let empty_cache_gets = fixture.counter.gets_for(ArtifactClass::Cluster);
    let empty_cache_bytes = fixture.counter.get_bytes_for(ArtifactClass::Cluster);
    println!(
        "cold baseline with empty cache: cluster_gets={empty_cache_gets} cluster_bytes={empty_cache_bytes}"
    );

    assert_eq!(
        empty_cache_gets, no_cache_gets,
        "empty-cache range path must preserve the cold physical GET plan"
    );
    assert_eq!(
        empty_cache_bytes, no_cache_bytes,
        "empty-cache range path must preserve cold cluster bytes"
    );
    for key in &fixture.cluster_keys {
        assert_eq!(
            cache.get(key).await,
            None,
            "range reads must not insert partial/full cluster object cache entries for {key}"
        );
    }

    no_cache_fixture.harness.cleanup().await;
    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn test_range_source_metric_records_s3_and_local_sources() {
    zeppelin::metrics::init();
    let fixture = warm_range_fixture("warm-range-metric").await;
    let (_cache_dir, cache) = test_cache();

    let s3_before = range_source_metric_value("sq", "s3");
    fixture.counter.reset();
    run_query(&fixture, Some(&cache)).await;
    let s3_after = range_source_metric_value("sq", "s3");
    assert!(
        s3_after > s3_before,
        "cold SQ range reads must increment source=s3 metric"
    );

    cache_full_cluster_objects(&fixture, &cache).await;
    let local_before = range_source_metric_value("sq", "local");
    fixture.counter.reset();
    run_query(&fixture, Some(&cache)).await;
    let local_after = range_source_metric_value("sq", "local");
    assert!(
        local_after > local_before,
        "warm SQ range reads must increment source=local metric"
    );

    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn test_corrupt_cached_object_evicted_and_served_from_s3() {
    zeppelin::metrics::init();
    let fixture = warm_range_fixture("warm-range-corrupt").await;
    let baseline = run_query(&fixture, None).await;
    let (_cache_dir, cache) = test_cache();

    for key in &fixture.cluster_keys {
        cache
            .put(key, &bytes::Bytes::from_static(b"truncated"))
            .await
            .unwrap();
    }

    let corrupt_before = range_source_metric_value_for_source("s3_after_corrupt_evict");
    fixture.counter.reset();
    let repaired = run_query(&fixture, Some(&cache)).await;
    let corrupt_after = range_source_metric_value_for_source("s3_after_corrupt_evict");

    assert_eq!(
        repaired, baseline,
        "corrupt cached objects must be repaired by serving the query from S3"
    );
    assert!(
        corrupt_after > corrupt_before,
        "known-size corrupt cached objects must meter s3_after_corrupt_evict"
    );
    for key in &fixture.cluster_keys {
        assert_eq!(
            cache.get(key).await,
            None,
            "corrupt cached object must be evicted for {key}"
        );
    }

    fixture.harness.cleanup().await;
}
