mod common;

use std::collections::HashMap;
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
use zeppelin::types::{AttributeValue, ConsistencyLevel, DistanceMetric, Filter, VectorEntry};
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
        common::default_gc_upload_window(),
    )
}

fn vector(id: &str, values: [f32; DIM]) -> VectorEntry {
    VectorEntry {
        id: id.to_string(),
        values: values.to_vec(),
        attributes: None,
    }
}

fn vector_with_tenant(id: &str, values: [f32; DIM], tenant: &str) -> VectorEntry {
    let mut attrs = HashMap::new();
    attrs.insert(
        "tenant".to_string(),
        AttributeValue::String(tenant.to_string()),
    );
    VectorEntry {
        id: id.to_string(),
        values: values.to_vec(),
        attributes: Some(attrs),
    }
}

async fn compact_baseline_and_cache_manifest(
    store: &ZeppelinStore,
    namespace: &str,
    manifest_cache: &Arc<ManifestCache>,
) {
    common::write_active_namespace_metadata(store, namespace, DIM, DistanceMetric::Euclidean).await;
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
        rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: Some(manifest_cache),
        include_attributes: true,
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
        rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: Some(manifest_cache),
        include_attributes: true,
    })
    .await
    .unwrap()
}

async fn strong_query(
    store: &ZeppelinStore,
    namespace: &str,
    query: &[f32],
    top_k: usize,
    filter: Option<&Filter>,
) -> zeppelin::query::QueryResponse {
    let wal_reader = WalReader::new(store.clone());
    execute_query(QueryParams {
        store,
        wal_reader: &wal_reader,
        namespace,
        query,
        top_k,
        nprobe: 1,
        filter,
        consistency: ConsistencyLevel::Strong,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 1,
        rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: None,
        include_attributes: true,
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

#[tokio::test]
async fn strong_wal_update_outside_topk_still_overrides_stale_segment_vector() {
    let harness = TestHarness::new().await;
    let namespace = harness.key("strong-wal-topk-overrides-segment");
    let store = &harness.store;
    common::write_active_namespace_metadata(store, &namespace, DIM, DistanceMetric::Euclidean)
        .await;
    Manifest::new().write(store, &namespace).await.unwrap();

    let writer = WalWriter::new(store.clone());
    writer
        .append(&namespace, vec![vector("x", [0.0, 0.0, 0.0, 0.0])], vec![])
        .await
        .unwrap();
    test_compactor(store).compact(&namespace).await.unwrap();

    writer
        .append(
            &namespace,
            vec![
                vector("x", [100.0, 0.0, 0.0, 0.0]),
                vector("wal_a", [0.1, 0.0, 0.0, 0.0]),
                vector("wal_b", [0.2, 0.0, 0.0, 0.0]),
            ],
            vec![],
        )
        .await
        .unwrap();

    let response = strong_query(store, &namespace, &[0.0, 0.0, 0.0, 0.0], 2, None).await;
    let ids: Vec<&str> = response.results.iter().map(|r| r.id.as_str()).collect();

    assert_eq!(ids, vec!["wal_a", "wal_b"]);
    assert!(
        !ids.contains(&"x"),
        "the stale compacted version of x must stay suppressed even when the WAL version is outside top_k"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn strong_filtered_wal_update_still_overrides_stale_segment_attrs() {
    let harness = TestHarness::new().await;
    let namespace = harness.key("strong-wal-filter-overrides-segment");
    let store = &harness.store;
    common::write_active_namespace_metadata(store, &namespace, DIM, DistanceMetric::Euclidean)
        .await;
    Manifest::new().write(store, &namespace).await.unwrap();

    let writer = WalWriter::new(store.clone());
    writer
        .append(
            &namespace,
            vec![vector_with_tenant("x", [0.0, 0.0, 0.0, 0.0], "keep")],
            vec![],
        )
        .await
        .unwrap();
    test_compactor(store).compact(&namespace).await.unwrap();

    writer
        .append(
            &namespace,
            vec![
                vector_with_tenant("x", [0.0, 0.0, 0.0, 0.0], "drop"),
                vector_with_tenant("visible", [0.1, 0.0, 0.0, 0.0], "keep"),
            ],
            vec![],
        )
        .await
        .unwrap();

    let filter = Filter::Eq {
        field: "tenant".to_string(),
        value: AttributeValue::String("keep".to_string()),
    };
    let response = strong_query(store, &namespace, &[0.0, 0.0, 0.0, 0.0], 10, Some(&filter)).await;
    let ids: Vec<&str> = response.results.iter().map(|r| r.id.as_str()).collect();

    assert_eq!(ids, vec!["visible"]);
    assert!(
        !ids.contains(&"x"),
        "a filtered-out WAL update must still suppress the stale compacted attrs for the same id"
    );

    harness.cleanup().await;
}
