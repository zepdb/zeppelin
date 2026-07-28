mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::counting::{counting_store, ArtifactClass};
use common::harness::TestHarness;
use tempfile::TempDir;
use zeppelin::cache::DiskCache;
use zeppelin::compaction::Compactor;
use zeppelin::config::Config;
use zeppelin::query::{execute_query_with_fragment_cache, QueryParams, QueryResponse};
use zeppelin::types::{
    AttributeValue, ConsistencyLevel, DistanceMetric, Filter, SearchResult, VectorEntry,
};
use zeppelin::wal::{WalFragmentCache, WalReader, WalWriter};

const DIM: usize = 4;

fn vector(id: &str, values: [f32; DIM]) -> VectorEntry {
    VectorEntry {
        id: id.to_string(),
        values: values.to_vec(),
        attributes: None,
    }
}

fn attributed_vector(id: &str, distance: f32, category: &str, version: i64) -> VectorEntry {
    let mut attributes = HashMap::new();
    attributes.insert(
        "category".to_string(),
        AttributeValue::String(category.to_string()),
    );
    attributes.insert("version".to_string(), AttributeValue::Integer(version));
    VectorEntry {
        id: id.to_string(),
        values: vec![distance, 0.0, 0.0, 0.0],
        attributes: Some(attributes),
    }
}

fn result_signature(response: &QueryResponse) -> Vec<(String, u32)> {
    response
        .results
        .iter()
        .map(|SearchResult { id, score, .. }| (id.clone(), score.to_bits()))
        .collect()
}

async fn strong_query(
    store: &zeppelin::storage::ZeppelinStore,
    wal_reader: &WalReader,
    namespace: &str,
    cache: &Arc<DiskCache>,
    fragment_cache: &Arc<WalFragmentCache>,
) -> QueryResponse {
    strong_query_with_filter(store, wal_reader, namespace, cache, fragment_cache, None).await
}

async fn strong_query_with_filter(
    store: &zeppelin::storage::ZeppelinStore,
    wal_reader: &WalReader,
    namespace: &str,
    cache: &Arc<DiskCache>,
    fragment_cache: &Arc<WalFragmentCache>,
    filter: Option<&Filter>,
) -> QueryResponse {
    execute_query_with_fragment_cache(
        QueryParams {
            store,
            wal_reader,
            namespace,
            query: &[0.0, 0.0, 0.0, 0.0],
            top_k: 10,
            nprobe: 1,
            filter,
            consistency: ConsistencyLevel::Strong,
            distance_metric: DistanceMetric::Euclidean,
            oversample_factor: 1,
            rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
            cache: Some(cache),
            manifest_cache: None,
            include_attributes: true,
        },
        fragment_cache,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn warm_strong_query_serves_uncompacted_wal_fragments_from_cache() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let namespace = harness.artifact_origin_namespace("wal-fragment-cache");
    let writer = WalWriter::new(store.clone());
    let wal_reader = WalReader::new(store.clone());
    let cache_dir = TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );
    let fragment_cache = Arc::new(WalFragmentCache::new(100 * 1024 * 1024));

    common::seed_bound_manifest(&store, &namespace).await;
    writer
        .append(&namespace, vec![vector("v0", [0.0, 0.0, 0.0, 0.0])], vec![])
        .await
        .unwrap();
    writer
        .append(&namespace, vec![vector("v1", [1.0, 0.0, 0.0, 0.0])], vec![])
        .await
        .unwrap();
    writer
        .append(&namespace, vec![vector("v2", [2.0, 0.0, 0.0, 0.0])], vec![])
        .await
        .unwrap();

    counter.reset();
    let cold = strong_query(&store, &wal_reader, &namespace, &cache, &fragment_cache).await;
    assert_eq!(cold.scanned_fragments, 3);
    let cold_wal_gets = counter.gets_for(ArtifactClass::Wal);
    assert_eq!(
        cold_wal_gets, 3,
        "cold strong query should read each uncompacted WAL fragment once"
    );
    assert_eq!(fragment_cache.decode_count(), 3);

    counter.reset();
    let warm = strong_query(&store, &wal_reader, &namespace, &cache, &fragment_cache).await;
    assert_eq!(warm.scanned_fragments, 3);
    let warm_wal_gets = counter.gets_for(ArtifactClass::Wal);

    assert_eq!(result_signature(&warm), result_signature(&cold));
    assert_eq!(
        fragment_cache.decode_count(),
        3,
        "warm query must reuse all three decoded immutable fragments"
    );
    assert_eq!(
        warm_wal_gets, 0,
        "warm strong query should serve immutable WAL fragments from cache"
    );
    assert!(
        warm_wal_gets < cold_wal_gets,
        "warm query must perform fewer WAL GETs than the cold query"
    );

    writer
        .append(&namespace, vec![vector("v3", [3.0, 0.0, 0.0, 0.0])], vec![])
        .await
        .unwrap();
    counter.reset();
    let after_append = strong_query(&store, &wal_reader, &namespace, &cache, &fragment_cache).await;
    assert_eq!(after_append.scanned_fragments, 4);
    assert_eq!(fragment_cache.decode_count(), 4);
    assert_eq!(
        counter.gets_for(ArtifactClass::Wal),
        1,
        "only the newly appended immutable fragment should miss both caches"
    );

    let expected_after_append = result_signature(&after_append);
    fragment_cache.clear();
    assert!(fragment_cache.is_empty());
    counter.reset();
    let after_clear = strong_query(&store, &wal_reader, &namespace, &cache, &fragment_cache).await;
    assert_eq!(result_signature(&after_clear), expected_after_append);
    assert_eq!(fragment_cache.decode_count(), 8);
    assert_eq!(fragment_cache.len(), 4);
    assert_eq!(
        counter.gets_for(ArtifactClass::Wal),
        0,
        "clearing decoded state must fall back to already-authoritative bytes"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn decoded_fragment_cache_preserves_overwrite_delete_filter_and_attributes() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("wal-decoded-result-invariance");
    let writer = WalWriter::new(harness.store.clone());
    let wal_reader = WalReader::new(harness.store.clone());
    let cache_dir = TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );
    let fragment_cache = Arc::new(WalFragmentCache::new(100 * 1024 * 1024));

    common::seed_bound_manifest(&harness.store, &namespace).await;
    writer
        .append(
            &namespace,
            vec![
                attributed_vector("a", 9.0, "keep", 1),
                attributed_vector("b", 2.0, "keep", 1),
            ],
            vec![],
        )
        .await
        .unwrap();
    writer
        .append(
            &namespace,
            vec![
                attributed_vector("a", 1.0, "keep", 2),
                attributed_vector("c", 0.5, "drop", 1),
            ],
            vec![],
        )
        .await
        .unwrap();
    writer
        .append(
            &namespace,
            vec![attributed_vector("d", 3.0, "keep", 1)],
            vec!["b".to_string()],
        )
        .await
        .unwrap();

    let filter = Filter::Eq {
        field: "category".to_string(),
        value: AttributeValue::String("keep".to_string()),
    };
    let cold = strong_query_with_filter(
        &harness.store,
        &wal_reader,
        &namespace,
        &cache,
        &fragment_cache,
        Some(&filter),
    )
    .await;
    let cold_bytes = serde_json::to_vec(&cold).unwrap();
    assert_eq!(
        cold.results
            .iter()
            .map(|result| result.id.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "d"]
    );
    assert_eq!(
        cold.results[0]
            .attributes
            .as_ref()
            .and_then(|attrs| attrs.get("version")),
        Some(&AttributeValue::Integer(2))
    );
    assert_eq!(fragment_cache.decode_count(), 3);

    let warm = strong_query_with_filter(
        &harness.store,
        &wal_reader,
        &namespace,
        &cache,
        &fragment_cache,
        Some(&filter),
    )
    .await;
    assert_eq!(serde_json::to_vec(&warm).unwrap(), cold_bytes);
    assert_eq!(fragment_cache.decode_count(), 3);

    harness.cleanup().await;
}

#[tokio::test]
async fn zero_capacity_redecodes_from_cached_bytes_without_changing_results() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let namespace = harness.artifact_origin_namespace("wal-decoded-zero-capacity");
    let writer = WalWriter::new(store.clone());
    let wal_reader = WalReader::new(store.clone());
    let cache_dir = TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );
    let fragment_cache = Arc::new(WalFragmentCache::new(0));
    common::seed_bound_manifest(&store, &namespace).await;
    writer
        .append(&namespace, vec![vector("v0", [0.0, 0.0, 0.0, 0.0])], vec![])
        .await
        .unwrap();

    counter.reset();
    let cold = strong_query(&store, &wal_reader, &namespace, &cache, &fragment_cache).await;
    assert_eq!(fragment_cache.decode_count(), 1);
    assert!(fragment_cache.is_empty());
    assert_eq!(counter.gets_for(ArtifactClass::Wal), 1);

    counter.reset();
    let warm = strong_query(&store, &wal_reader, &namespace, &cache, &fragment_cache).await;
    assert_eq!(result_signature(&warm), result_signature(&cold));
    assert_eq!(fragment_cache.decode_count(), 2);
    assert!(fragment_cache.is_empty());
    assert_eq!(counter.gets_for(ArtifactClass::Wal), 0);

    harness.cleanup().await;
}

#[tokio::test]
async fn post_compaction_query_evicts_retired_decoded_fragments() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("wal-decoded-compaction-eviction");
    common::seed_active_namespace(&harness.store, &namespace, DIM, DistanceMetric::Euclidean).await;
    let writer = WalWriter::new(harness.store.clone());
    writer
        .append(&namespace, vec![vector("v0", [0.0, 0.0, 0.0, 0.0])], vec![])
        .await
        .unwrap();
    writer
        .append(&namespace, vec![vector("v1", [1.0, 0.0, 0.0, 0.0])], vec![])
        .await
        .unwrap();

    let wal_reader = WalReader::new(harness.store.clone());
    let cache_dir = TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );
    let fragment_cache = Arc::new(WalFragmentCache::new(100 * 1024 * 1024));
    let before = strong_query(
        &harness.store,
        &wal_reader,
        &namespace,
        &cache,
        &fragment_cache,
    )
    .await;
    assert_eq!(fragment_cache.len(), 2);

    let config = Config::default();
    let compactor = Compactor::new(
        harness.store.clone(),
        WalReader::new(harness.store.clone()),
        config.compaction,
        config.indexing,
        Duration::from_secs(config.gc.compaction_upload_window_secs),
    );
    let compacted = compactor.compact(&namespace).await.unwrap();
    assert_eq!(compacted.fragments_removed, 2);
    assert_eq!(
        fragment_cache.len(),
        2,
        "compaction does not reach into the query-owned decoded cache"
    );

    let after = strong_query(
        &harness.store,
        &wal_reader,
        &namespace,
        &cache,
        &fragment_cache,
    )
    .await;
    assert_eq!(after.scanned_fragments, 0);
    assert_eq!(result_signature(&after), result_signature(&before));
    assert!(fragment_cache.is_empty());

    harness.cleanup().await;
}

#[tokio::test]
async fn parallel_strong_queries_remain_correct_during_append() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("wal-decoded-concurrent-append");
    common::seed_bound_manifest(&harness.store, &namespace).await;
    let writer = WalWriter::new(harness.store.clone());
    writer
        .append(
            &namespace,
            vec![vector("base-0", [0.0, 0.0, 0.0, 0.0])],
            vec![],
        )
        .await
        .unwrap();
    writer
        .append(
            &namespace,
            vec![vector("base-1", [1.0, 0.0, 0.0, 0.0])],
            vec![],
        )
        .await
        .unwrap();

    let cache_dir = TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );
    let fragment_cache = Arc::new(WalFragmentCache::new(100 * 1024 * 1024));
    let barrier = Arc::new(tokio::sync::Barrier::new(9));
    let mut queries = Vec::new();
    for _ in 0..8 {
        let store = harness.store.clone();
        let wal_reader = WalReader::new(store.clone());
        let namespace = namespace.clone();
        let cache = Arc::clone(&cache);
        let fragment_cache = Arc::clone(&fragment_cache);
        let barrier = Arc::clone(&barrier);
        queries.push(tokio::spawn(async move {
            barrier.wait().await;
            strong_query(&store, &wal_reader, &namespace, &cache, &fragment_cache).await
        }));
    }

    barrier.wait().await;
    writer
        .append(
            &namespace,
            vec![vector("appended", [2.0, 0.0, 0.0, 0.0])],
            vec![],
        )
        .await
        .unwrap();
    for query in queries {
        let response = query.await.unwrap();
        let ids: Vec<&str> = response
            .results
            .iter()
            .map(|result| result.id.as_str())
            .collect();
        assert!(ids.starts_with(&["base-0", "base-1"]));
        assert!(ids == ["base-0", "base-1"] || ids == ["base-0", "base-1", "appended"]);
    }

    let final_response = strong_query(
        &harness.store,
        &WalReader::new(harness.store.clone()),
        &namespace,
        &cache,
        &fragment_cache,
    )
    .await;
    assert_eq!(
        final_response
            .results
            .iter()
            .map(|result| result.id.as_str())
            .collect::<Vec<_>>(),
        vec!["base-0", "base-1", "appended"]
    );

    harness.cleanup().await;
}
