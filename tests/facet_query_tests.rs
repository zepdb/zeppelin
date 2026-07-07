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

#[tokio::test]
async fn facets_count_filtered_candidate_frontier_not_top_k() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_namespace(&client, &base_url).await;

    upsert(
        &client,
        &base_url,
        &ns,
        json!([
            {
                "id": "doc-a-1",
                "values": [0.0, 0.0],
                "attributes": {
                    "category": "a",
                    "tags": ["fresh", "red"],
                    "tenant": "keep"
                }
            },
            {
                "id": "doc-a-2",
                "values": [0.1, 0.0],
                "attributes": {
                    "category": "a",
                    "tags": ["fresh", "blue"],
                    "tenant": "keep"
                }
            },
            {
                "id": "drop-1",
                "values": [0.15, 0.0],
                "attributes": {
                    "category": "z",
                    "tags": ["drop"],
                    "tenant": "drop"
                }
            },
            {
                "id": "doc-b-1",
                "values": [0.2, 0.0],
                "attributes": {
                    "category": "b",
                    "tags": ["red"],
                    "tenant": "keep"
                }
            },
            {
                "id": "doc-a-3",
                "values": [0.3, 0.0],
                "attributes": {
                    "category": "a",
                    "tags": ["blue"],
                    "tenant": "keep"
                }
            },
            {
                "id": "doc-c-1",
                "values": [0.4, 0.0],
                "attributes": {
                    "category": "c",
                    "tags": ["outside"],
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
            "facets": ["category", "tags", "missing_field"],
            "candidate_k": 4,
            "top_k": 1,
            "filter": {"op": "eq", "field": "tenant", "value": "keep"},
            "consistency": "strong",
            "projection": {"include_attributes": false}
        }),
    )
    .await;

    assert_eq!(body["results"].as_array().unwrap().len(), 1);
    assert!(body["results"][0]["attributes"].is_null());
    assert_eq!(body["facets"]["category"]["a"], 3);
    assert_eq!(body["facets"]["category"]["b"], 1);
    assert!(body["facets"]["category"].get("c").is_none());
    assert!(body["facets"]["category"].get("z").is_none());
    assert_eq!(body["facets"]["tags"]["fresh"], 2);
    assert_eq!(body["facets"]["tags"]["red"], 2);
    assert_eq!(body["facets"]["tags"]["blue"], 2);
    assert_eq!(
        body["facets"]["missing_field"].as_object().unwrap().len(),
        0
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn facets_are_omitted_when_not_requested() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_namespace(&client, &base_url).await;

    upsert(
        &client,
        &base_url,
        &ns,
        json!([{
            "id": "doc-1",
            "values": [0.0, 0.0],
            "attributes": {"category": "a"}
        }]),
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
            "top_k": 1,
            "consistency": "strong"
        }),
    )
    .await;

    assert!(body.get("facets").is_none());

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
