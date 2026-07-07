mod common;

use common::server::{cleanup_ns, create_ns_api_with, start_test_server};
use serde_json::{json, Value};

async fn create_namespace(client: &reqwest::Client, base_url: &str) -> String {
    create_ns_api_with(
        client,
        base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await
}

async fn create_fts_namespace(client: &reqwest::Client, base_url: &str) -> String {
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

async fn query(client: &reqwest::Client, base_url: &str, ns: &str, body: Value) -> Value {
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

fn page_request(top_k: usize, cursor: Value) -> Value {
    json!({
        "sources": [{
            "type": "ann",
            "vector": [0.0, 0.0]
        }],
        "top_k": top_k,
        "cursor": cursor,
        "consistency": "strong",
        "projection": {"include_attributes": false}
    })
}

fn bounded_page_request(top_k: usize, candidate_k: usize, cursor: Value) -> Value {
    json!({
        "sources": [{
            "type": "ann",
            "vector": [0.0, 0.0]
        }],
        "candidate_k": candidate_k,
        "top_k": top_k,
        "cursor": cursor,
        "consistency": "strong",
        "projection": {"include_attributes": false}
    })
}

fn same_fingerprint_token_for_id(template: &str, id: &str) -> String {
    let parts: Vec<&str> = template.split(':').collect();
    assert_eq!(parts.len(), 4);
    let id_hex = id
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{}:{}:{}:{}", parts[0], parts[1], parts[2], id_hex)
}

#[tokio::test]
async fn malformed_cursor_token_is_validation_error() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_namespace(&client, &base_url).await;

    upsert(
        &client,
        &base_url,
        &ns,
        json!([
            {"id": "a", "values": [0.0, 0.0]},
            {"id": "b", "values": [1.0, 0.0]}
        ]),
    )
    .await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&json!({
            "sources": [{
                "type": "ann",
                "vector": [0.0, 0.0]
            }],
            "top_k": 1,
            "cursor": {"type": "after", "token": "not-a-valid-cursor"},
            "consistency": "strong",
            "projection": {"include_attributes": false}
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "VALIDATION_ERROR");
    assert!(body["error"].as_str().unwrap().contains("cursor"));

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn cursor_pages_continue_without_overlap_or_gaps() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_namespace(&client, &base_url).await;

    upsert(
        &client,
        &base_url,
        &ns,
        json!([
            {"id": "a", "values": [0.1, 0.0]},
            {"id": "b", "values": [0.2, 0.0]},
            {"id": "c", "values": [0.3, 0.0]},
            {"id": "d", "values": [0.4, 0.0]},
            {"id": "e", "values": [0.5, 0.0]}
        ]),
    )
    .await;

    let full = query(
        &client,
        &base_url,
        &ns,
        page_request(5, json!({"type": "none"})),
    )
    .await;
    let page1 = query(
        &client,
        &base_url,
        &ns,
        page_request(2, json!({"type": "none"})),
    )
    .await;
    let cursor1 = page1["next_cursor"].as_str().unwrap();
    let page2 = query(
        &client,
        &base_url,
        &ns,
        page_request(2, json!({"type": "after", "token": cursor1})),
    )
    .await;
    let cursor2 = page2["next_cursor"].as_str().unwrap();
    let page3 = query(
        &client,
        &base_url,
        &ns,
        page_request(2, json!({"type": "after", "token": cursor2})),
    )
    .await;

    let mut paged = ids(&page1);
    paged.extend(ids(&page2));
    paged.extend(ids(&page3));

    assert_eq!(ids(&full), vec!["a", "b", "c", "d", "e"]);
    assert_eq!(paged, ids(&full));
    assert!(page3.get("next_cursor").is_none());

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn cursor_ignores_non_result_response_options() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_namespace(&client, &base_url).await;

    upsert(
        &client,
        &base_url,
        &ns,
        json!([
            {"id": "a", "values": [0.1, 0.0]},
            {"id": "b", "values": [0.2, 0.0]},
            {"id": "c", "values": [0.3, 0.0]}
        ]),
    )
    .await;

    let page1 = query(
        &client,
        &base_url,
        &ns,
        json!({
            "sources": [{
                "type": "ann",
                "vector": [0.0, 0.0]
            }],
            "top_k": 1,
            "cursor": {"type": "none"},
            "consistency": "strong",
            "projection": {"include_attributes": false},
            "debug": true,
            "explain": "plan"
        }),
    )
    .await;
    let cursor = page1["next_cursor"].as_str().unwrap();
    let page2 = query(
        &client,
        &base_url,
        &ns,
        page_request(1, json!({"type": "after", "token": cursor})),
    )
    .await;

    assert_eq!(ids(&page1), vec!["a"]);
    assert_eq!(ids(&page2), vec!["b"]);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn cursor_pages_are_bounded_by_candidate_k_frontier() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_namespace(&client, &base_url).await;

    upsert(
        &client,
        &base_url,
        &ns,
        json!([
            {"id": "a", "values": [0.1, 0.0]},
            {"id": "b", "values": [0.2, 0.0]},
            {"id": "c", "values": [0.3, 0.0]},
            {"id": "d", "values": [0.4, 0.0]},
            {"id": "e", "values": [0.5, 0.0]}
        ]),
    )
    .await;

    let page1 = query(
        &client,
        &base_url,
        &ns,
        bounded_page_request(2, 3, json!({"type": "none"})),
    )
    .await;
    let cursor = page1["next_cursor"].as_str().unwrap();
    let page2 = query(
        &client,
        &base_url,
        &ns,
        bounded_page_request(2, 3, json!({"type": "after", "token": cursor})),
    )
    .await;

    let mut paged = ids(&page1);
    paged.extend(ids(&page2));
    assert_eq!(paged, vec!["a", "b", "c"]);
    assert!(page2.get("next_cursor").is_none());

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn cursor_preserves_vector_rerank_distance_order() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_namespace(&client, &base_url).await;

    upsert(
        &client,
        &base_url,
        &ns,
        json!([
            {"id": "far-source-near-rerank", "values": [0.5, 0.0]},
            {"id": "middle", "values": [0.4, 0.0]},
            {"id": "near-source-far-rerank", "values": [0.1, 0.0]}
        ]),
    )
    .await;

    let request = |cursor: Value| {
        json!({
            "sources": [{
                "type": "ann",
                "vector": [0.0, 0.0]
            }],
            "rerank": {
                "type": "vector",
                "vector": [0.6, 0.0]
            },
            "candidate_k": 3,
            "top_k": 1,
            "cursor": cursor,
            "consistency": "strong",
            "projection": {"include_attributes": false}
        })
    };

    let page1 = query(&client, &base_url, &ns, request(json!({"type": "none"}))).await;
    let cursor1 = page1["next_cursor"].as_str().unwrap();
    let page2 = query(
        &client,
        &base_url,
        &ns,
        request(json!({"type": "after", "token": cursor1})),
    )
    .await;
    let cursor2 = page2["next_cursor"].as_str().unwrap();
    let page3 = query(
        &client,
        &base_url,
        &ns,
        request(json!({"type": "after", "token": cursor2})),
    )
    .await;

    assert_eq!(ids(&page1), vec!["far-source-near-rerank"]);
    assert_eq!(ids(&page2), vec!["middle"]);
    assert_eq!(ids(&page3), vec!["near-source-far-rerank"]);
    assert!(page3.get("next_cursor").is_none());

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn cursor_preserves_bm25_rerank_score_order() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_fts_namespace(&client, &base_url).await;

    upsert(
        &client,
        &base_url,
        &ns,
        json!([
            {
                "id": "ann-near-zero-bm25",
                "values": [0.0, 0.0],
                "attributes": {"content": "plain text"}
            },
            {
                "id": "middle-bm25",
                "values": [0.1, 0.0],
                "attributes": {"content": "rerank"}
            },
            {
                "id": "best-bm25",
                "values": [0.2, 0.0],
                "attributes": {"content": "rerank rerank rerank rerank"}
            }
        ]),
    )
    .await;

    let request = |cursor: Value| {
        json!({
            "sources": [{
                "type": "ann",
                "vector": [0.0, 0.0]
            }],
            "rerank": {
                "type": "bm25",
                "rank_by": ["content", "BM25", "rerank"]
            },
            "candidate_k": 3,
            "top_k": 1,
            "cursor": cursor,
            "consistency": "strong",
            "projection": {"include_attributes": false}
        })
    };

    let page1 = query(&client, &base_url, &ns, request(json!({"type": "none"}))).await;
    let cursor1 = page1["next_cursor"].as_str().unwrap();
    let page2 = query(
        &client,
        &base_url,
        &ns,
        request(json!({"type": "after", "token": cursor1})),
    )
    .await;
    let cursor2 = page2["next_cursor"].as_str().unwrap();
    let page3 = query(
        &client,
        &base_url,
        &ns,
        request(json!({"type": "after", "token": cursor2})),
    )
    .await;

    assert_eq!(ids(&page1), vec!["best-bm25"]);
    assert_eq!(ids(&page2), vec!["middle-bm25"]);
    assert_eq!(ids(&page3), vec!["ann-near-zero-bm25"]);
    assert!(page3.get("next_cursor").is_none());

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn cursor_rejects_tokens_from_different_query_shape() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_namespace(&client, &base_url).await;

    upsert(
        &client,
        &base_url,
        &ns,
        json!([
            {"id": "a", "values": [0.0, 0.0]},
            {"id": "b", "values": [0.0, 0.0]},
            {"id": "c", "values": [0.0, 0.0]}
        ]),
    )
    .await;

    let page1 = query(
        &client,
        &base_url,
        &ns,
        page_request(1, json!({"type": "none"})),
    )
    .await;
    let cursor = page1["next_cursor"].as_str().unwrap();
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&json!({
            "sources": [{
                "type": "ann",
                "vector": [1.0, 0.0]
            }],
            "top_k": 1,
            "cursor": {"type": "after", "token": cursor},
            "consistency": "strong",
            "projection": {"include_attributes": false}
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "VALIDATION_ERROR");
    assert!(body["error"].as_str().unwrap().contains("cursor"));

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn cursor_after_last_result_returns_empty_page() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_namespace(&client, &base_url).await;

    upsert(
        &client,
        &base_url,
        &ns,
        json!([
            {"id": "a", "values": [0.0, 0.0]},
            {"id": "b", "values": [0.0, 0.0]}
        ]),
    )
    .await;

    let page1 = query(
        &client,
        &base_url,
        &ns,
        page_request(1, json!({"type": "none"})),
    )
    .await;
    let after_b = same_fingerprint_token_for_id(page1["next_cursor"].as_str().unwrap(), "b");
    let empty = query(
        &client,
        &base_url,
        &ns,
        page_request(1, json!({"type": "after", "token": after_b})),
    )
    .await;

    assert!(ids(&empty).is_empty());
    assert!(empty.get("next_cursor").is_none());

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
