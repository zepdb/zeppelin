mod common;

use std::collections::HashMap;

use common::counting::counting_store;
use common::harness::TestHarness;
use common::server::{
    cleanup_ns, create_ns_api, create_ns_api_fts, start_test_server_with_compactor,
};
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, Config, IndexingConfig};
use zeppelin::fts::FtsFieldConfig;
use zeppelin::query::{execute_query, QueryParams};
use zeppelin::types::{AttributeValue, ConsistencyLevel, DistanceMetric, VectorEntry};
use zeppelin::wal::fragment::WalFragment;
use zeppelin::wal::manifest::Manifest;
use zeppelin::wal::{WalReader, WalWriter};

fn test_config(fts_index: bool) -> Config {
    let mut config = Config::load(None).unwrap();
    config.compaction = CompactionConfig {
        max_wal_fragments_before_compact: 3,
        ..Default::default()
    };
    config.indexing = IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 10,
        fts_index,
        bitmap_index: false,
        ..Default::default()
    };
    config
}

fn test_compactor(store: &zeppelin::storage::ZeppelinStore, fts_index: bool) -> Compactor {
    Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        CompactionConfig {
            max_wal_fragments_before_compact: 3,
            ..Default::default()
        },
        IndexingConfig {
            default_num_centroids: 4,
            kmeans_max_iterations: 10,
            fts_index,
            bitmap_index: false,
            ..Default::default()
        },
    )
}

fn vector_doc(id: &str, values: [f32; 4]) -> VectorEntry {
    VectorEntry {
        id: id.to_string(),
        values: values.to_vec(),
        attributes: None,
    }
}

fn content_doc(id: &str, content: &str, seed: f32) -> VectorEntry {
    let mut attrs = HashMap::new();
    attrs.insert(
        "content".to_string(),
        AttributeValue::String(content.to_string()),
    );
    VectorEntry {
        id: id.to_string(),
        values: vec![seed, seed + 0.1, seed + 0.2, seed + 0.3],
        attributes: Some(attrs),
    }
}

fn content_fts_json() -> serde_json::Value {
    serde_json::json!({
        "content": {"language": "english", "stemming": false, "remove_stopwords": false}
    })
}

fn content_fts_configs() -> HashMap<String, FtsFieldConfig> {
    let mut configs = HashMap::new();
    configs.insert(
        "content".to_string(),
        FtsFieldConfig {
            stemming: false,
            remove_stopwords: false,
            ..Default::default()
        },
    );
    configs
}

async fn upsert_vectors(
    client: &reqwest::Client,
    base_url: &str,
    ns: &str,
    vectors: &[VectorEntry],
) {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "upsert failed: {}",
        resp.text().await.unwrap()
    );
}

async fn delete_vectors(client: &reqwest::Client, base_url: &str, ns: &str, ids: &[&str]) {
    let resp = client
        .delete(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "ids": ids }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        204,
        "delete failed: {}",
        resp.text().await.unwrap()
    );
}

async fn query_json(
    client: &reqwest::Client,
    base_url: &str,
    ns: &str,
    body: serde_json::Value,
) -> serde_json::Value {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "query failed: {}",
        resp.text().await.unwrap()
    );
    resp.json().await.unwrap()
}

fn result_ids(body: &serde_json::Value) -> Vec<String> {
    body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn test_eventual_vector_query_filters_deleted_compacted_vector() {
    let config = test_config(false);
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 4).await;

    let target_id = "delete_me";
    let query_vec = vec![1.0, 0.0, 0.0, 0.0];
    let vectors = vec![
        vector_doc(target_id, [1.0, 0.0, 0.0, 0.0]),
        vector_doc("keep_near", [0.95, 0.05, 0.0, 0.0]),
        vector_doc("keep_mid", [0.0, 1.0, 0.0, 0.0]),
        vector_doc("keep_far", [0.0, 0.0, 1.0, 0.0]),
        vector_doc("keep_other", [0.0, 0.0, 0.0, 1.0]),
    ];
    upsert_vectors(&client, &base_url, &ns, &vectors).await;
    compactor.compact(&ns).await.unwrap();

    let before = query_json(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "vector": query_vec,
            "top_k": 5,
            "nprobe": 4,
            "consistency": "eventual",
        }),
    )
    .await;
    assert!(
        result_ids(&before).contains(&target_id.to_string()),
        "precondition: compacted vector must be visible before delete"
    );

    delete_vectors(&client, &base_url, &ns, &[target_id]).await;

    let after = query_json(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "vector": query_vec,
            "top_k": 5,
            "nprobe": 4,
            "consistency": "eventual",
        }),
    )
    .await;
    let ids = result_ids(&after);
    assert!(
        !ids.contains(&target_id.to_string()),
        "deleted vector {target_id} resurrected in Eventual vector query: {ids:?}"
    );
    assert_eq!(after["scanned_fragments"].as_u64().unwrap(), 0);
    assert!(
        !ids.is_empty(),
        "other segment vectors should remain visible"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_eventual_bm25_query_filters_deleted_compacted_doc() {
    let config = test_config(true);
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_fts(&client, &base_url, 4, content_fts_json()).await;

    let target_id = "delete_me";
    let docs = vec![
        content_doc(target_id, "rust searchable tombstone target", 0.1),
        content_doc("keep_rust", "rust searchable survivor", 0.2),
        content_doc("keep_other", "python searchable survivor", 0.3),
        content_doc("keep_more", "rust systems survivor", 0.4),
        content_doc("keep_tail", "storage indexing survivor", 0.5),
    ];
    upsert_vectors(&client, &base_url, &ns, &docs).await;
    compactor
        .compact_with_fts(&ns, None, &content_fts_configs())
        .await
        .unwrap();

    let before = query_json(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "rank_by": ["content", "BM25", "rust"],
            "top_k": 5,
            "consistency": "eventual",
        }),
    )
    .await;
    assert!(
        result_ids(&before).contains(&target_id.to_string()),
        "precondition: compacted BM25 document must be visible before delete"
    );

    delete_vectors(&client, &base_url, &ns, &[target_id]).await;

    let after = query_json(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "rank_by": ["content", "BM25", "rust"],
            "top_k": 5,
            "consistency": "eventual",
        }),
    )
    .await;
    let ids = result_ids(&after);
    assert!(
        !ids.contains(&target_id.to_string()),
        "deleted doc {target_id} resurrected in Eventual BM25 query: {ids:?}"
    );
    assert_eq!(after["scanned_fragments"].as_u64().unwrap(), 0);
    assert!(
        ids.contains(&"keep_rust".to_string()) || ids.contains(&"keep_more".to_string()),
        "other BM25 segment docs should remain visible: {ids:?}"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_eventual_query_gets_only_delete_fragments_not_vector_wal_fragments() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let ns = harness.key("eventual-delete-get-counts");
    let writer = WalWriter::new(store.clone());
    let wal_reader = WalReader::new(store.clone());

    Manifest::new().write(&store, &ns).await.unwrap();

    let target_id = "delete_me";
    let query_vec = vec![1.0, 0.0, 0.0, 0.0];
    writer
        .append(
            &ns,
            vec![
                vector_doc(target_id, [1.0, 0.0, 0.0, 0.0]),
                vector_doc("keep_1", [0.95, 0.05, 0.0, 0.0]),
                vector_doc("keep_2", [0.0, 1.0, 0.0, 0.0]),
                vector_doc("keep_3", [0.0, 0.0, 1.0, 0.0]),
                vector_doc("keep_4", [0.0, 0.0, 0.0, 1.0]),
            ],
            vec![],
        )
        .await
        .unwrap();
    test_compactor(&store, false).compact(&ns).await.unwrap();

    let (vector_fragment, _) = writer
        .append(
            &ns,
            vec![vector_doc("wal_only_update", [0.9, 0.1, 0.0, 0.0])],
            vec![],
        )
        .await
        .unwrap();
    let vector_fragment_key = WalFragment::s3_key(&ns, &vector_fragment.id);

    counter.reset();
    let without_deletes = execute_query(QueryParams {
        store: &store,
        wal_reader: &wal_reader,
        namespace: &ns,
        query: &query_vec,
        top_k: 5,
        nprobe: 4,
        filter: None,
        consistency: ConsistencyLevel::Eventual,
        distance_metric: DistanceMetric::Cosine,
        oversample_factor: 3,
        cache: None,
        manifest_cache: None,
    })
    .await
    .unwrap();
    assert_eq!(
        counter.gets_matching(".wal"),
        0,
        "delete-free WAL must add zero fragment GETs to Eventual queries"
    );
    assert_eq!(without_deletes.scanned_fragments, 0);
    assert!(
        without_deletes.results.iter().any(|r| r.id == target_id),
        "precondition: compacted vector must be visible before delete"
    );

    let (delete_fragment, _) = writer
        .append(&ns, vec![], vec![target_id.to_string()])
        .await
        .unwrap();
    let delete_fragment_key = WalFragment::s3_key(&ns, &delete_fragment.id);

    counter.reset();
    let after_delete = execute_query(QueryParams {
        store: &store,
        wal_reader: &wal_reader,
        namespace: &ns,
        query: &query_vec,
        top_k: 5,
        nprobe: 4,
        filter: None,
        consistency: ConsistencyLevel::Eventual,
        distance_metric: DistanceMetric::Cosine,
        oversample_factor: 3,
        cache: None,
        manifest_cache: None,
    })
    .await
    .unwrap();
    assert_eq!(
        counter.gets_matching(&vector_fragment_key),
        0,
        "Eventual tombstone reads must not fetch delete-free vector WAL fragments"
    );
    assert_eq!(
        counter.gets_matching(&delete_fragment_key),
        1,
        "Eventual tombstone reads must fetch tombstone-bearing fragments"
    );
    assert_eq!(
        counter.gets_matching(".wal"),
        1,
        "only the tombstone-bearing WAL fragment should be fetched"
    );
    assert_eq!(after_delete.scanned_fragments, 0);
    assert!(
        after_delete.results.iter().all(|r| r.id != target_id),
        "deleted vector {target_id} resurrected after tombstone read"
    );

    harness.cleanup().await;
}

// VERIFIER PASS-PROBE: cross-fragment ordering — a tombstone in a NEWER
// fragment must not be cancelled by an upsert of the same id in an OLDER
// fetched fragment, and a delete→re-upsert→delete chain across three
// fragments must end hidden. Guards hunt area 2 (manifest-order semantics
// of read_delete_ids_from_refs_unchecked).
#[tokio::test]
async fn test_eventual_tombstone_ordering_across_fragments() {
    let harness = TestHarness::new().await;
    let (store, _counter) = counting_store(&harness.store);
    let ns = harness.key("eventual-tombstone-ordering");
    let writer = WalWriter::new(store.clone());
    let wal_reader = WalReader::new(store.clone());

    Manifest::new().write(&store, &ns).await.unwrap();

    // Segment contains x, z, and fillers.
    writer
        .append(
            &ns,
            vec![
                vector_doc("x", [1.0, 0.0, 0.0, 0.0]),
                vector_doc("z", [0.9, 0.1, 0.0, 0.0]),
                vector_doc("keep_a", [0.0, 1.0, 0.0, 0.0]),
                vector_doc("keep_b", [0.0, 0.0, 1.0, 0.0]),
                vector_doc("keep_c", [0.0, 0.0, 0.0, 1.0]),
            ],
            vec![],
        )
        .await
        .unwrap();
    test_compactor(&store, false).compact(&ns).await.unwrap();

    // Fragment 1 (older, fetched because it has a delete): upserts x AND
    // deletes an unrelated id. Fragment 2 (newer): deletes x. The older
    // upsert of x must NOT cancel the newer tombstone for x.
    writer
        .append(
            &ns,
            vec![vector_doc("x", [0.8, 0.2, 0.0, 0.0])],
            vec!["unrelated_never_existed".to_string()],
        )
        .await
        .unwrap();
    writer
        .append(&ns, vec![], vec!["x".to_string()])
        .await
        .unwrap();

    let query_vec = vec![1.0, 0.0, 0.0, 0.0];
    let res = execute_query(QueryParams {
        store: &store,
        wal_reader: &wal_reader,
        namespace: &ns,
        query: &query_vec,
        top_k: 5,
        nprobe: 4,
        filter: None,
        consistency: ConsistencyLevel::Eventual,
        distance_metric: DistanceMetric::Cosine,
        oversample_factor: 3,
        cache: None,
        manifest_cache: None,
    })
    .await
    .unwrap();
    assert!(
        res.results.iter().all(|r| r.id != "x"),
        "tombstone for x in a NEWER fragment was cancelled by an upsert in an OLDER fragment: {:?}",
        res.results.iter().map(|r| &r.id).collect::<Vec<_>>()
    );

    // Three-fragment chain on z: delete z → re-upsert z (delete-bearing
    // fragment so it IS fetched) → delete z again. Final state deleted:
    // z must be hidden.
    writer
        .append(&ns, vec![], vec!["z".to_string()])
        .await
        .unwrap();
    writer
        .append(
            &ns,
            vec![vector_doc("z", [0.9, 0.1, 0.0, 0.0])],
            vec!["another_never_existed".to_string()],
        )
        .await
        .unwrap();
    writer
        .append(&ns, vec![], vec!["z".to_string()])
        .await
        .unwrap();

    let res2 = execute_query(QueryParams {
        store: &store,
        wal_reader: &wal_reader,
        namespace: &ns,
        query: &query_vec,
        top_k: 5,
        nprobe: 4,
        filter: None,
        consistency: ConsistencyLevel::Eventual,
        distance_metric: DistanceMetric::Cosine,
        oversample_factor: 3,
        cache: None,
        manifest_cache: None,
    })
    .await
    .unwrap();
    assert!(
        res2.results.iter().all(|r| r.id != "z"),
        "delete→re-upsert→delete chain must end hidden under Eventual: {:?}",
        res2.results.iter().map(|r| &r.id).collect::<Vec<_>>()
    );
    // Sanity: untouched vectors still visible.
    assert!(
        res2.results.iter().any(|r| r.id.starts_with("keep_")),
        "unrelated segment vectors must remain visible"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn test_eventual_query_may_return_stale_updated_segment_vector() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let ns = harness.key("eventual-stale-update");
    let writer = WalWriter::new(store.clone());
    let wal_reader = WalReader::new(store.clone());

    Manifest::new().write(&store, &ns).await.unwrap();

    let target_id = "update_me";
    let query_vec = vec![1.0, 0.0, 0.0, 0.0];
    writer
        .append(
            &ns,
            vec![
                vector_doc(target_id, [1.0, 0.0, 0.0, 0.0]),
                vector_doc("keep_1", [0.95, 0.05, 0.0, 0.0]),
                vector_doc("keep_2", [0.0, 1.0, 0.0, 0.0]),
                vector_doc("keep_3", [0.0, 0.0, 1.0, 0.0]),
                vector_doc("keep_4", [0.0, 0.0, 0.0, 1.0]),
            ],
            vec![],
        )
        .await
        .unwrap();
    test_compactor(&store, false).compact(&ns).await.unwrap();

    writer
        .append(
            &ns,
            vec![vector_doc(target_id, [0.0, 1.0, 0.0, 0.0])],
            vec![],
        )
        .await
        .unwrap();

    counter.reset();
    let result = execute_query(QueryParams {
        store: &store,
        wal_reader: &wal_reader,
        namespace: &ns,
        query: &query_vec,
        top_k: 5,
        nprobe: 4,
        filter: None,
        consistency: ConsistencyLevel::Eventual,
        distance_metric: DistanceMetric::Cosine,
        oversample_factor: 3,
        cache: None,
        manifest_cache: None,
    })
    .await
    .unwrap();

    assert_eq!(
        counter.gets_matching(".wal"),
        0,
        "updated-in-WAL fragments have no tombstones and must not be fetched by Eventual"
    );
    assert_eq!(result.scanned_fragments, 0);
    assert!(
        result.results.iter().any(|r| r.id == target_id),
        "Eventual may return the stale compacted version of an updated vector; only deletes vanish"
    );

    harness.cleanup().await;
}
