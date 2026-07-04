mod common;

use std::sync::Arc;

use common::counting::{counting_store, ArtifactClass};
use common::harness::TestHarness;
use tempfile::TempDir;
use zeppelin::cache::DiskCache;
use zeppelin::query::{execute_query, QueryParams, QueryResponse};
use zeppelin::types::{ConsistencyLevel, DistanceMetric, SearchResult, VectorEntry};
use zeppelin::wal::manifest::Manifest;
use zeppelin::wal::{WalReader, WalWriter};

const DIM: usize = 4;

fn vector(id: &str, values: [f32; DIM]) -> VectorEntry {
    VectorEntry {
        id: id.to_string(),
        values: values.to_vec(),
        attributes: None,
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
) -> QueryResponse {
    execute_query(QueryParams {
        store,
        wal_reader,
        namespace,
        query: &[0.0, 0.0, 0.0, 0.0],
        top_k: 10,
        nprobe: 1,
        filter: None,
        consistency: ConsistencyLevel::Strong,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 1,
        cache: Some(cache),
        manifest_cache: None,
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn warm_strong_query_serves_uncompacted_wal_fragments_from_cache() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let namespace = harness.key("wal-fragment-cache");
    let writer = WalWriter::new(store.clone());
    let wal_reader = WalReader::new(store.clone());
    let cache_dir = TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    Manifest::new().write(&store, &namespace).await.unwrap();
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
    let cold = strong_query(&store, &wal_reader, &namespace, &cache).await;
    assert_eq!(cold.scanned_fragments, 3);
    let cold_wal_gets = counter.gets_for(ArtifactClass::Wal);
    assert_eq!(
        cold_wal_gets, 3,
        "cold strong query should read each uncompacted WAL fragment once"
    );

    counter.reset();
    let warm = strong_query(&store, &wal_reader, &namespace, &cache).await;
    assert_eq!(warm.scanned_fragments, 3);
    let warm_wal_gets = counter.gets_for(ArtifactClass::Wal);

    assert_eq!(result_signature(&warm), result_signature(&cold));
    assert_eq!(
        warm_wal_gets, 0,
        "warm strong query should serve immutable WAL fragments from cache"
    );
    assert!(
        warm_wal_gets < cold_wal_gets,
        "warm query must perform fewer WAL GETs than the cold query"
    );

    harness.cleanup().await;
}
