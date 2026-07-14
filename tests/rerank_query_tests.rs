mod common;

use common::server::{cleanup_ns, create_ns_api_with, start_test_server};
use serde_json::{json, Value};

async fn create_namespace(client: &reqwest::Client, base_url: &str) -> String {
    create_ns_api_with(
        client,
        base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean",
            "full_text_search": {
                "content": {
                    "stemming": false,
                    "remove_stopwords": false
                }
            }
        }),
    )
    .await
}

async fn upsert(client: &reqwest::Client, base_url: &str, ns: &str, vectors: Value) {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "upsert failed: {vectors}");
}

async fn query(
    client: &reqwest::Client,
    base_url: &str,
    ns: &str,
    body: Value,
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

fn ids(body: &Value) -> Vec<String> {
    body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|result| result["id"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn vector_rerank_orders_candidates_by_rerank_vector() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_namespace(&client, &base_url).await;

    upsert(
        &client,
        &base_url,
        &ns,
        json!([
            {
                "id": "query-near",
                "values": [0.0, 0.0],
                "attributes": {"content": "plain", "tenant": "keep"}
            },
            {
                "id": "middle",
                "values": [1.0, 0.0],
                "attributes": {"content": "plain", "tenant": "keep"}
            },
            {
                "id": "rerank-near",
                "values": [10.0, 0.0],
                "attributes": {"content": "plain", "tenant": "keep"}
            }
        ]),
    )
    .await;

    let body = query(
        &client,
        &base_url,
        &ns,
        json!({
            "sources": [{
                "type": "ann",
                "vector": [0.0, 0.0]
            }],
            "rerank": {
                "type": "vector",
                "vector": [10.0, 0.0]
            },
            "candidate_k": 3,
            "top_k": 1,
            "consistency": "strong",
            "projection": {"include_attributes": false}
        }),
    )
    .await;

    assert_eq!(ids(&body), vec!["rerank-near"]);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn bm25_rerank_orders_candidates_by_expression_score() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_namespace(&client, &base_url).await;

    upsert(
        &client,
        &base_url,
        &ns,
        json!([
            {
                "id": "ann-near",
                "values": [0.0, 0.0],
                "attributes": {"content": "plain text", "tenant": "keep"}
            },
            {
                "id": "bm25-near",
                "values": [1.0, 0.0],
                "attributes": {
                    "content": "rerank rerank rerank rerank",
                    "tenant": "keep"
                }
            }
        ]),
    )
    .await;

    let body = query(
        &client,
        &base_url,
        &ns,
        json!({
            "sources": [{
                "type": "ann",
                "vector": [0.0, 0.0]
            }],
            "rerank": {
                "type": "bm25",
                "rank_by": ["content", "BM25", "rerank"]
            },
            "candidate_k": 2,
            "top_k": 1,
            "consistency": "strong",
            "projection": {"include_attributes": false}
        }),
    )
    .await;

    assert_eq!(ids(&body), vec!["bm25-near"]);
    assert!(body["results"][0]["attributes"].is_null());

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn rerank_uses_candidate_k_frontier_and_preserves_filtering() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_namespace(&client, &base_url).await;

    upsert(
        &client,
        &base_url,
        &ns,
        json!([
            {
                "id": "ann-only",
                "values": [0.0, 0.0],
                "attributes": {"content": "plain text", "tenant": "keep"}
            },
            {
                "id": "filtered-best",
                "values": [0.5, 0.0],
                "attributes": {
                    "content": "rerank rerank rerank rerank rerank",
                    "tenant": "drop"
                }
            },
            {
                "id": "middle",
                "values": [1.0, 0.0],
                "attributes": {"content": "rerank", "tenant": "keep"}
            },
            {
                "id": "bm25-best",
                "values": [2.0, 0.0],
                "attributes": {
                    "content": "rerank rerank rerank rerank",
                    "tenant": "keep"
                }
            }
        ]),
    )
    .await;

    let request = |candidate_k| {
        json!({
            "sources": [{
                "type": "ann",
                "vector": [0.0, 0.0]
            }],
            "rerank": {
                "type": "bm25",
                "rank_by": ["content", "BM25", "rerank"]
            },
            "candidate_k": candidate_k,
            "top_k": 1,
            "filter": {"op": "eq", "field": "tenant", "value": "keep"},
            "consistency": "strong",
            "projection": {"include_attributes": false}
        })
    };

    let narrow = query(&client, &base_url, &ns, request(1)).await;
    let wide = query(&client, &base_url, &ns, request(3)).await;

    assert_eq!(ids(&narrow), vec!["ann-only"]);
    assert_eq!(ids(&wide), vec!["bm25-best"]);
    assert_ne!(ids(&narrow), ids(&wide));
    assert!(!ids(&wide).contains(&"filtered-best".to_string()));

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn default_and_none_rerank_are_byte_identical_noops() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_namespace(&client, &base_url).await;

    upsert(
        &client,
        &base_url,
        &ns,
        json!([
            {
                "id": "a",
                "values": [0.0, 0.0],
                "attributes": {"content": "alpha", "tenant": "keep"}
            },
            {
                "id": "b",
                "values": [1.0, 0.0],
                "attributes": {"content": "beta", "tenant": "keep"}
            },
            {
                "id": "c",
                "values": [2.0, 0.0],
                "attributes": {"content": "gamma", "tenant": "keep"}
            }
        ]),
    )
    .await;

    let base = json!({
        "sources": [{
            "type": "ann",
            "vector": [0.0, 0.0]
        }],
        "top_k": 2,
        "consistency": "strong",
        "projection": {"include_attributes": false}
    });
    let baseline = query(&client, &base_url, &ns, base.clone()).await;

    let mut default_req = base.clone();
    default_req["rerank"] = json!({"type": "default"});
    let default_body = query(&client, &base_url, &ns, default_req).await;

    let mut none_req = base;
    none_req["rerank"] = json!({"type": "none"});
    let none_body = query(&client, &base_url, &ns, none_req).await;

    assert_eq!(default_body, baseline);
    assert_eq!(none_body, baseline);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
