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

async fn query_status(
    client: &reqwest::Client,
    base_url: &str,
    ns: &str,
    body: Value,
) -> (u16, Value) {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let body = resp.json().await.unwrap();
    (status, body)
}

fn group_ids(body: &Value, group_index: usize) -> Vec<String> {
    body["groups"][group_index]["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|result| result["id"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn field_grouping_limits_members_and_top_k_groups() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_namespace(&client, &base_url).await;

    upsert(
        &client,
        &base_url,
        &ns,
        json!([
            {
                "id": "doc-a-1",
                "values": [0.0, 0.0],
                "attributes": {"doc_id": "doc-a", "tenant": "keep"}
            },
            {
                "id": "doc-a-2",
                "values": [0.1, 0.0],
                "attributes": {"doc_id": "doc-a", "tenant": "keep"}
            },
            {
                "id": "doc-a-3",
                "values": [0.2, 0.0],
                "attributes": {"doc_id": "doc-a", "tenant": "keep"}
            },
            {
                "id": "doc-b-1",
                "values": [0.3, 0.0],
                "attributes": {"doc_id": "doc-b", "tenant": "keep"}
            },
            {
                "id": "doc-c-1",
                "values": [0.4, 0.0],
                "attributes": {"doc_id": "doc-c", "tenant": "keep"}
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
            "grouping": {
                "type": "field",
                "field": "doc_id",
                "max_per_group": 2
            },
            "candidate_k": 5,
            "top_k": 2,
            "filter": {"op": "eq", "field": "tenant", "value": "keep"},
            "consistency": "strong",
            "projection": {"include_attributes": false}
        }),
    )
    .await;

    assert_eq!(body["groups"].as_array().unwrap().len(), 2);
    assert_eq!(body["groups"][0]["key"], "doc-a");
    assert_eq!(group_ids(&body, 0), vec!["doc-a-1", "doc-a-2"]);
    assert_eq!(body["groups"][1]["key"], "doc-b");
    assert_eq!(group_ids(&body, 1), vec!["doc-b-1"]);
    assert!(body["groups"][0]["results"][0]["attributes"].is_null());

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn grouping_and_cursor_are_rejected_together() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_namespace(&client, &base_url).await;

    upsert(
        &client,
        &base_url,
        &ns,
        json!([
            {
                "id": "doc-a-1",
                "values": [0.0, 0.0],
                "attributes": {"doc_id": "doc-a"}
            },
            {
                "id": "doc-b-1",
                "values": [0.1, 0.0],
                "attributes": {"doc_id": "doc-b"}
            }
        ]),
    )
    .await;

    let (status, body) = query_status(
        &client,
        &base_url,
        &ns,
        json!({
            "sources": [{
                "type": "ann",
                "vector": [0.0, 0.0]
            }],
            "grouping": {
                "type": "field",
                "field": "doc_id",
                "max_per_group": 1
            },
            "cursor": {"type": "none"},
            "top_k": 1,
            "consistency": "strong"
        }),
    )
    .await;

    assert_eq!(status, 400);
    assert_eq!(body["code"], "VALIDATION_ERROR");
    assert!(body["error"].as_str().unwrap().contains("grouping"));
    assert!(body["error"].as_str().unwrap().contains("cursor"));

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn field_grouping_keeps_missing_field_hits_as_singletons() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_namespace(&client, &base_url).await;

    upsert(
        &client,
        &base_url,
        &ns,
        json!([
            {
                "id": "doc-a-1",
                "values": [0.0, 0.0],
                "attributes": {"doc_id": "doc-a", "tenant": "keep"}
            },
            {
                "id": "doc-b-1",
                "values": [0.0, 0.0],
                "attributes": {"doc_id": "doc-b", "tenant": "keep"}
            },
            {
                "id": "drop-1",
                "values": [0.0, 0.0],
                "attributes": {"doc_id": "drop", "tenant": "drop"}
            },
            {
                "id": "missing",
                "values": [0.0, 0.0],
                "attributes": {"tenant": "keep"}
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
            "grouping": {
                "type": "field",
                "field": "doc_id",
                "max_per_group": 1
            },
            "candidate_k": 4,
            "top_k": 10,
            "filter": {"op": "eq", "field": "tenant", "value": "keep"},
            "consistency": "strong",
            "projection": {"include_attributes": true}
        }),
    )
    .await;

    let groups = body["groups"].as_array().unwrap();
    let keys: Vec<&str> = groups
        .iter()
        .map(|group| group["key"].as_str().unwrap())
        .collect();
    assert_eq!(keys, vec!["doc-a", "doc-b", "missing"]);
    assert_eq!(group_ids(&body, 2), vec!["missing"]);
    assert!(body["groups"][2]["results"][0]["attributes"]["tenant"] == "keep");
    assert!(!body["results"]
        .as_array()
        .unwrap()
        .iter()
        .any(|result| result["id"] == "drop-1"));

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
