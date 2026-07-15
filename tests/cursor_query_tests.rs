mod common;

use common::counting::counting_store;
use common::harness::TestHarness;
use common::server::{
    cleanup_ns, create_ns_api_with, start_test_server, start_test_server_on_store,
};
use serde_json::{json, Value};
use zeppelin::namespace::manager::NamespaceMetadata;

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

#[tokio::test]
async fn malformed_cursor_token_is_validation_error() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
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
async fn forged_cursor_is_rejected_before_query_execution_metrics() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
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

    let page = query(
        &client,
        &base_url,
        &ns,
        page_request(1, json!({"type": "none"})),
    )
    .await;
    let mut forged = page["next_cursor"]
        .as_str()
        .expect("first page must return a cursor")
        .to_string();
    let final_byte = forged.pop().expect("cursor must contain an HMAC tag");
    forged.push(if final_byte == '0' { '1' } else { '0' });

    let query_counter = zeppelin::metrics::QUERIES_TOTAL.with_label_values(&[&ns]);
    let before = query_counter.get();
    let response = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&page_request(1, json!({"type": "after", "token": forged})))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["code"], "VALIDATION_ERROR");
    assert_eq!(
        query_counter.get(),
        before,
        "an unauthenticated page marker must fail before query execution is counted"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn all_forged_cursor_batch_stops_before_namespace_storage_io() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let (base_url, _cache, _cache_dir, admin_bearer) =
        start_test_server_on_store(&harness, store, Some(harness.prefix.clone())).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let source_ns = create_namespace(&client, &base_url).await;
    upsert(
        &client,
        &base_url,
        &source_ns,
        json!([
            {"id": "a", "values": [0.1, 0.0]},
            {"id": "b", "values": [0.2, 0.0]}
        ]),
    )
    .await;
    let page = query(
        &client,
        &base_url,
        &source_ns,
        page_request(1, json!({"type": "none"})),
    )
    .await;
    let mut forged = page["next_cursor"]
        .as_str()
        .expect("first page must issue a cursor")
        .to_string();
    let final_byte = forged.pop().expect("cursor must contain an HMAC tag");
    forged.push(if final_byte == '0' { '1' } else { '0' });

    let missing_ns = format!("{}-forged-batch-missing", harness.prefix);
    let missing_meta_key = NamespaceMetadata::s3_key(&missing_ns);
    counter.reset();
    let response = client
        .post(format!("{base_url}/v1/namespaces/{missing_ns}/query/batch"))
        .json(&json!({
            "queries": [
                page_request(1, json!({"type": "after", "token": forged})),
                page_request(1, json!({"type": "after", "token": "zp3:forged"}))
            ]
        }))
        .send()
        .await
        .expect("all-forged cursor batch must complete");
    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.unwrap();
    let results = body["results"].as_array().expect("batch results");
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|entry| entry["ok"] == false));
    assert_eq!(
        counter.gets_matching(&missing_meta_key),
        0,
        "an all-invalid cursor batch must not look up namespace metadata"
    );

    cleanup_ns(&harness.store, &source_ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn cursor_pages_continue_without_overlap_or_gaps() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
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
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
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
async fn cursor_rejects_a_consistency_change_between_pages() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_namespace(&client, &base_url).await;
    upsert(
        &client,
        &base_url,
        &ns,
        json!([
            {"id": "a", "values": [0.1, 0.0]},
            {"id": "b", "values": [0.2, 0.0]}
        ]),
    )
    .await;

    let first = query(
        &client,
        &base_url,
        &ns,
        page_request(1, json!({"type": "none"})),
    )
    .await;
    let cursor = first["next_cursor"]
        .as_str()
        .expect("first page must issue a cursor");
    let mut changed = page_request(1, json!({"type": "after", "token": cursor}));
    changed["consistency"] = json!("eventual");
    let response = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&changed)
        .send()
        .await
        .expect("changed-consistency cursor request must complete");
    assert_eq!(response.status(), 400);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["code"], "VALIDATION_ERROR");
    assert!(body["error"]
        .as_str()
        .expect("cursor error message")
        .contains("does not match query"));

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn cursor_pages_are_bounded_by_candidate_k_frontier() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
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
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
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
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
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
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
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
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
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
    let cursor = page1["next_cursor"]
        .as_str()
        .expect("first page must return a server-authenticated cursor")
        .to_string();
    let deleted = client
        .delete(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&json!({"ids": ["b"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 204);

    let empty = query(
        &client,
        &base_url,
        &ns,
        page_request(1, json!({"type": "after", "token": cursor})),
    )
    .await;

    assert!(ids(&empty).is_empty());
    assert!(empty.get("next_cursor").is_none());

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
