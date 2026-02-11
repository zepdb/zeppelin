mod common;

use common::server::{api_ns, cleanup_ns, start_test_server};
use common::vectors::random_vectors;

#[tokio::test]
async fn test_health_check() {
    let (base_url, harness) = start_test_server().await;

    let resp = reqwest::get(format!("{base_url}/healthz"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");

    harness.cleanup().await;
}

#[tokio::test]
async fn test_namespace_crud() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "api-ns");

    // Create
    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 64,
            "distance_metric": "cosine"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["name"], ns);
    assert_eq!(body["dimensions"], 64);

    // Get
    let resp = client
        .get(format!("{base_url}/v1/namespaces/{ns}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["name"], ns);

    // List
    let resp = client
        .get(format!("{base_url}/v1/namespaces"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(body.iter().any(|n| n["name"] == ns));

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

    // DELETE handler already cleaned up S3 objects
    harness.cleanup().await;
}

#[tokio::test]
async fn test_duplicate_create_409() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "api-dup");

    let body = serde_json::json!({
        "name": ns,
        "dimensions": 32,
    });

    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_vector_upsert() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "api-upsert");

    // Create namespace
    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 16,
        }))
        .send()
        .await
        .unwrap();

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
    let ns = api_ns(&harness, "api-dim");

    // Create namespace with dim=16
    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 16,
        }))
        .send()
        .await
        .unwrap();

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
    let ns = api_ns(&harness, "api-query");

    // Create namespace
    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 8,
        }))
        .send()
        .await
        .unwrap();

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
    let ns = api_ns(&harness, "api-filter");

    // Create namespace
    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 4,
        }))
        .send()
        .await
        .unwrap();

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
    let ns = api_ns(&harness, "api-empty");

    // Create namespace
    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 4,
        }))
        .send()
        .await
        .unwrap();

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
    let ns = api_ns(&harness, "api-qdim");

    // Create namespace with dim=4
    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 4,
        }))
        .send()
        .await
        .unwrap();

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
