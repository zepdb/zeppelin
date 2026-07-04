mod common;

use std::sync::Arc;
use std::time::Duration;

use common::counting::{counting_store, ArtifactClass, GetCounter};
use common::harness::TestHarness;
use zeppelin::cache::manifest_cache::ManifestCache;
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig};
use zeppelin::index::quantization::QuantizationType;
use zeppelin::query::{execute_query, QueryParams};
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::{ConsistencyLevel, DistanceMetric, VectorEntry};
use zeppelin::wal::manifest::Manifest;
use zeppelin::wal::{WalReader, WalWriter};

const DIM: usize = 4;

fn test_compactor(store: &ZeppelinStore) -> Compactor {
    Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        CompactionConfig::default(),
        IndexingConfig {
            default_num_centroids: 1,
            default_nprobe: 1,
            quantization: QuantizationType::None,
            bitmap_index: false,
            ..Default::default()
        },
    )
}

fn vector(id: &str, values: [f32; DIM]) -> VectorEntry {
    VectorEntry {
        id: id.to_string(),
        values: values.to_vec(),
        attributes: None,
    }
}

async fn compact_baseline_and_cache_manifest(
    store: &ZeppelinStore,
    namespace: &str,
    manifest_cache: &Arc<ManifestCache>,
) {
    Manifest::new().write(store, namespace).await.unwrap();

    let writer = WalWriter::new(store.clone());
    writer
        .append(
            namespace,
            vec![vector("old", [10.0, 0.0, 0.0, 0.0])],
            vec![],
        )
        .await
        .unwrap();

    test_compactor(store).compact(namespace).await.unwrap();

    let wal_reader = WalReader::new(store.clone());
    let response = execute_query(QueryParams {
        store,
        wal_reader: &wal_reader,
        namespace,
        query: &[10.0, 0.0, 0.0, 0.0],
        top_k: 1,
        nprobe: 1,
        filter: None,
        consistency: ConsistencyLevel::Strong,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 1,
        cache: None,
        manifest_cache: Some(manifest_cache),
    })
    .await
    .unwrap();

    assert_eq!(
        response.results[0].id, "old",
        "precondition: baseline segment must be query-visible"
    );
    assert_eq!(
        response.scanned_fragments, 0,
        "precondition: cached manifest should contain only the compacted segment"
    );
}

async fn append_fresh_wal_vector(store: &ZeppelinStore, namespace: &str) {
    WalWriter::new(store.clone())
        .append(
            namespace,
            vec![vector("fresh", [0.0, 0.0, 0.0, 0.0])],
            vec![],
        )
        .await
        .unwrap();
}

async fn query_for_fresh_vector(
    store: &ZeppelinStore,
    namespace: &str,
    consistency: ConsistencyLevel,
    manifest_cache: &Arc<ManifestCache>,
) -> zeppelin::query::QueryResponse {
    let wal_reader = WalReader::new(store.clone());
    execute_query(QueryParams {
        store,
        wal_reader: &wal_reader,
        namespace,
        query: &[0.0, 0.0, 0.0, 0.0],
        top_k: 1,
        nprobe: 1,
        filter: None,
        consistency,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 1,
        cache: None,
        manifest_cache: Some(manifest_cache),
    })
    .await
    .unwrap()
}

fn manifest_get_attempts(counter: &GetCounter) -> u64 {
    counter.gets_matching("manifest.json")
}

#[tokio::test]
async fn strong_query_within_ttl_observes_manifest_advanced_on_s3() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let namespace = harness.key("strong-fresh-wal");
    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_secs(60)));

    compact_baseline_and_cache_manifest(&store, &namespace, &manifest_cache).await;
    append_fresh_wal_vector(&store, &namespace).await;

    counter.reset();
    let response = query_for_fresh_vector(
        &store,
        &namespace,
        ConsistencyLevel::Strong,
        &manifest_cache,
    )
    .await;

    assert_eq!(
        response.results[0].id, "fresh",
        "Strong must not serve the stale TTL-cached manifest after S3 advanced"
    );
    assert_eq!(
        response.scanned_fragments, 1,
        "Strong must re-read the fresh manifest and scan the newly appended WAL fragment"
    );
    assert_eq!(
        counter.gets_for(ArtifactClass::Manifest),
        1,
        "changed manifest should be fetched exactly once"
    );
    assert_eq!(
        manifest_get_attempts(&counter),
        1,
        "Strong freshness check should add one manifest GET"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn eventual_query_within_ttl_keeps_zero_manifest_get_fast_path() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let namespace = harness.key("eventual-stale-is-cheap");
    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_secs(60)));

    compact_baseline_and_cache_manifest(&store, &namespace, &manifest_cache).await;
    append_fresh_wal_vector(&store, &namespace).await;

    counter.reset();
    let response = query_for_fresh_vector(
        &store,
        &namespace,
        ConsistencyLevel::Eventual,
        &manifest_cache,
    )
    .await;

    assert_eq!(
        response.results[0].id, "old",
        "Eventual should keep serving the TTL-cached manifest within the freshness window"
    );
    assert_eq!(response.scanned_fragments, 0);
    assert_eq!(counter.gets_for(ArtifactClass::Manifest), 0);
    assert_eq!(manifest_get_attempts(&counter), 0);

    harness.cleanup().await;
}

#[tokio::test]
async fn strong_query_with_unchanged_manifest_uses_one_bodyless_freshness_get() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let namespace = harness.key("strong-unchanged-304");
    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_secs(60)));

    compact_baseline_and_cache_manifest(&store, &namespace, &manifest_cache).await;

    counter.reset();
    let eventual = query_for_fresh_vector(
        &store,
        &namespace,
        ConsistencyLevel::Eventual,
        &manifest_cache,
    )
    .await;
    assert_eq!(eventual.results[0].id, "old");
    let eventual_manifest_gets = manifest_get_attempts(&counter);
    assert_eq!(eventual_manifest_gets, 0);

    counter.reset();
    let strong = query_for_fresh_vector(
        &store,
        &namespace,
        ConsistencyLevel::Strong,
        &manifest_cache,
    )
    .await;

    assert_eq!(strong.results[0].id, "old");
    assert_eq!(
        manifest_get_attempts(&counter),
        eventual_manifest_gets + 1,
        "Strong should pay exactly one conditional manifest GET when the cache is fresh"
    );
    assert_eq!(
        counter.get_bytes_for(ArtifactClass::Manifest),
        0,
        "unchanged manifest should not be downloaded again"
    );

    harness.cleanup().await;
}
