mod common;

use common::server::{cleanup_ns, create_ns_api, start_test_server};
use common::vectors::random_vectors;

#[tokio::test]
async fn test_health_check() {
    let (base_url, harness) = start_test_server().await;

    let resp = reqwest::get(format!("{base_url}/healthz")).await.unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");

    harness.cleanup().await;
}

#[tokio::test]
async fn test_create_namespace_returns_uuid_and_warning() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({ "dimensions": 64 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    let body: serde_json::Value = resp.json().await.unwrap();

    // Name should be a valid UUID v4
    let name = body["name"].as_str().unwrap();
    assert!(
        uuid::Uuid::parse_str(name).is_ok(),
        "expected valid UUID, got: {name}"
    );

    // Warning should be present
    let warning = body["warning"].as_str().unwrap();
    assert!(
        warning.contains("Save"),
        "expected save warning, got: {warning}"
    );

    // Standard fields should be present
    assert_eq!(body["dimensions"], 64);
    assert!(body["created_at"].is_string());

    cleanup_ns(&harness.store, name).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_list_namespaces_disabled() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base_url}/v1/namespaces"))
        .send()
        .await
        .unwrap();
    // Only POST is routed on /v1/namespaces, GET should return 405
    assert_eq!(resp.status(), 405);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_namespace_crud() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 64).await;

    // Get
    let resp = client
        .get(format!("{base_url}/v1/namespaces/{ns}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["name"], ns);
    assert_eq!(body["dimensions"], 64);

    // Delete
    let resp = client
        .delete(format!("{base_url}/v1/namespaces/{ns}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // Verify deleted
    let resp = client
        .get(format!("{base_url}/v1/namespaces/{ns}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_vector_upsert() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 16).await;

    // Upsert vectors
    let vectors = random_vectors(5, 16);
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["upserted"], 5);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_dimension_mismatch_400() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 16).await;

    // Upsert with wrong dimension (32 instead of 16)
    let vectors = random_vectors(1, 32);
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_query_basic_wal_scan() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 8).await;

    // Upsert 10 vectors
    let vectors = random_vectors(10, 8);
    let query_vec = vectors[0].values.clone();

    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();

    // Query with the first vector — it should be the top result (distance ~0)
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 5,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0]["id"], "vec_0");
    assert!(body["scanned_fragments"].as_u64().unwrap() > 0);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_query_with_filter() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 4).await;

    // Upsert vectors with attributes
    let vectors = serde_json::json!({
        "vectors": [
            {"id": "v1", "values": [1.0, 0.0, 0.0, 0.0], "attributes": {"category": "a"}},
            {"id": "v2", "values": [0.9, 0.1, 0.0, 0.0], "attributes": {"category": "b"}},
            {"id": "v3", "values": [0.8, 0.2, 0.0, 0.0], "attributes": {"category": "a"}},
        ]
    });
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&vectors)
        .send()
        .await
        .unwrap();

    // Query with filter for category=a
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 10,
            "filter": {"op": "eq", "field": "category", "value": "a"},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    // Should only contain v1 and v3 (category=a)
    assert_eq!(results.len(), 2);
    let ids: Vec<&str> = results.iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert!(ids.contains(&"v1"));
    assert!(ids.contains(&"v3"));

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_query_empty_namespace() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 4).await;

    // Query empty namespace
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 5,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(results.is_empty());

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_query_dimension_mismatch() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 4).await;

    // Query with wrong dimension
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": [1.0, 0.0],
            "top_k": 5,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
