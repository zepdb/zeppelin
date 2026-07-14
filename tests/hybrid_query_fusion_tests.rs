mod common;

use common::server::{
    cleanup_ns, create_ns_api_with, start_test_server, start_test_server_with_compactor,
};
use std::collections::HashMap;
use zeppelin::config::{Config, IndexingConfig};
use zeppelin::fts::FtsFieldConfig;

fn fts_config() -> serde_json::Value {
    serde_json::json!({
        "content": {
            "stemming": false,
            "remove_stopwords": false
        }
    })
}

fn fts_field_configs() -> HashMap<String, FtsFieldConfig> {
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

fn hybrid_compaction_config() -> Config {
    Config {
        indexing: IndexingConfig {
            default_num_centroids: 4,
            kmeans_max_iterations: 10,
            fts_index: true,
            bitmap_index: false,
            ..Default::default()
        },
        ..Default::default()
    }
}

async fn create_hybrid_namespace(
    client: &reqwest::Client,
    base_url: &str,
    dimensions: usize,
) -> String {
    create_ns_api_with(
        client,
        base_url,
        serde_json::json!({
            "dimensions": dimensions,
            "distance_metric": "euclidean",
            "full_text_search": fts_config(),
        }),
    )
    .await
}

async fn upsert(client: &reqwest::Client, base_url: &str, ns: &str, vectors: serde_json::Value) {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

async fn query(
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
    assert_eq!(resp.status(), 200, "query body: {body}");
    resp.json().await.unwrap()
}

fn result_ids(body: &serde_json::Value) -> Vec<String> {
    body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|result| result["id"].as_str().unwrap().to_string())
        .collect()
}

fn base_sources() -> serde_json::Value {
    serde_json::json!([
        {
            "type": "ann",
            "vector": [0.0, 0.0]
        },
        {
            "type": "bm25",
            "rank_by": ["content", "BM25", "hybrid"]
        }
    ])
}

async fn seed_wal_fixture(client: &reqwest::Client, base_url: &str, ns: &str) {
    upsert(
        client,
        base_url,
        ns,
        serde_json::json!([
            {
                "id": "doc-ann",
                "values": [0.0, 0.0],
                "attributes": {"content": "plain vector"}
            },
            {
                "id": "doc-middle",
                "values": [0.1, 0.0],
                "attributes": {"content": "hybrid"}
            },
            {
                "id": "doc-bm25",
                "values": [10.0, 10.0],
                "attributes": {"content": "hybrid hybrid hybrid hybrid"}
            }
        ]),
    )
    .await;
}

#[tokio::test]
async fn test_hybrid_rrf_fuses_ann_and_bm25_deterministically() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_hybrid_namespace(&client, &base_url, 2).await;
    seed_wal_fixture(&client, &base_url, &ns).await;

    let request = serde_json::json!({
        "sources": base_sources(),
        "fusion": {"type": "rrf", "k": 60},
        "candidate_k": 2,
        "top_k": 3,
        "consistency": "strong",
        "projection": {"include_attributes": false}
    });

    let first = query(&client, &base_url, &ns, request.clone()).await;
    let second = query(&client, &base_url, &ns, request).await;
    let default_fusion = query(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "sources": base_sources(),
            "candidate_k": 2,
            "top_k": 3,
            "consistency": "strong",
            "projection": {"include_attributes": false}
        }),
    )
    .await;
    assert_eq!(first, second);
    assert_eq!(first, default_fusion);
    assert_eq!(
        result_ids(&first),
        vec!["doc-middle", "doc-ann", "doc-bm25"]
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_hybrid_weighted_fusion_uses_normalized_source_scores() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_hybrid_namespace(&client, &base_url, 2).await;
    seed_wal_fixture(&client, &base_url, &ns).await;

    let vector_heavy = query(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "sources": base_sources(),
            "fusion": {"type": "weighted", "weights": [1.0, 0.0]},
            "candidate_k": 2,
            "top_k": 3,
            "consistency": "strong",
            "projection": {"include_attributes": false}
        }),
    )
    .await;
    let bm25_heavy = query(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "sources": base_sources(),
            "fusion": {"type": "weighted", "weights": [0.0, 1.0]},
            "candidate_k": 2,
            "top_k": 3,
            "consistency": "strong",
            "projection": {"include_attributes": false}
        }),
    )
    .await;

    assert_eq!(result_ids(&vector_heavy)[0], "doc-ann");
    assert_eq!(result_ids(&bm25_heavy)[0], "doc-bm25");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_hybrid_candidate_k_bounds_each_source_before_fusion() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_hybrid_namespace(&client, &base_url, 2).await;
    seed_wal_fixture(&client, &base_url, &ns).await;

    let body = query(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "sources": base_sources(),
            "fusion": {"type": "rrf", "k": 60},
            "candidate_k": 1,
            "top_k": 3,
            "consistency": "strong",
            "projection": {"include_attributes": false}
        }),
    )
    .await;

    assert_eq!(result_ids(&body), vec!["doc-ann", "doc-bm25"]);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_hybrid_query_by_id_excludes_seed_after_fusion() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_hybrid_namespace(&client, &base_url, 2).await;

    upsert(
        &client,
        &base_url,
        &ns,
        serde_json::json!([
            {
                "id": "seed",
                "values": [0.0, 0.0],
                "attributes": {"content": "hybrid hybrid hybrid hybrid"}
            },
            {
                "id": "neighbor",
                "values": [0.1, 0.0],
                "attributes": {"content": "plain vector"}
            },
            {
                "id": "text-match",
                "values": [10.0, 10.0],
                "attributes": {"content": "hybrid hybrid"}
            }
        ]),
    )
    .await;

    let body = query(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "sources": [
                {
                    "type": "ann",
                    "id": "seed"
                },
                {
                    "type": "bm25",
                    "rank_by": ["content", "BM25", "hybrid"]
                }
            ],
            "fusion": {"type": "rrf", "k": 60},
            "candidate_k": 3,
            "top_k": 3,
            "consistency": "strong",
            "projection": {"include_attributes": false}
        }),
    )
    .await;

    assert!(!result_ids(&body).contains(&"seed".to_string()));

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_hybrid_fusion_includes_wal_and_segment_candidates() {
    let (base_url, harness, _cache, _cache_dir, compactor, admin_bearer) =
        start_test_server_with_compactor(Some(hybrid_compaction_config())).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_hybrid_namespace(&client, &base_url, 2).await;

    upsert(
        &client,
        &base_url,
        &ns,
        serde_json::json!([
            {
                "id": "seg-ann",
                "values": [0.0, 0.0],
                "attributes": {"content": "plain vector"}
            },
            {
                "id": "seg-bm25",
                "values": [10.0, 10.0],
                "attributes": {"content": "hybrid hybrid hybrid hybrid"}
            }
        ]),
    )
    .await;
    compactor
        .compact_with_fts(&ns, None, &fts_field_configs())
        .await
        .unwrap();
    upsert(
        &client,
        &base_url,
        &ns,
        serde_json::json!([
            {
                "id": "wal-middle",
                "values": [0.1, 0.0],
                "attributes": {"content": "hybrid"}
            }
        ]),
    )
    .await;

    let body = query(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "sources": [
                {
                    "type": "ann",
                    "vector": [0.0, 0.0],
                    "nprobe": 16
                },
                {
                    "type": "bm25",
                    "rank_by": ["content", "BM25", "hybrid"]
                }
            ],
            "fusion": {"type": "rrf", "k": 60},
            "candidate_k": 2,
            "top_k": 3,
            "consistency": "strong",
            "projection": {"include_attributes": false}
        }),
    )
    .await;

    assert_eq!(result_ids(&body), vec!["wal-middle", "seg-ann", "seg-bm25"]);
    assert!(body["scanned_fragments"].as_u64().unwrap() > 0);
    assert!(body["scanned_segments"].as_u64().unwrap() > 0);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
