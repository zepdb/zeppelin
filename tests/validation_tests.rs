mod common;

use common::server::{cleanup_ns, create_ns_api, start_test_server};

// --- Test 1: Dimensions too large rejected ---

#[tokio::test]
async fn test_dimensions_too_large_rejected() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({ "dimensions": 100_000 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    let error_msg = body["error"].as_str().unwrap().to_lowercase();
    assert!(error_msg.contains("dimensions"), "got: {error_msg}");

    harness.cleanup().await;
}

// --- Test 2: Dimensions zero rejected ---

#[tokio::test]
async fn test_dimensions_zero_rejected() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({ "dimensions": 0 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    let error_msg = body["error"].as_str().unwrap().to_lowercase();
    assert!(error_msg.contains("dimensions"), "got: {error_msg}");

    harness.cleanup().await;
}

// --- Test 3: Vector ID too long rejected ---

#[tokio::test]
async fn test_vector_id_too_long_rejected() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 4).await;

    let long_id = "x".repeat(1025);
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [{"id": long_id, "values": [1.0, 0.0, 0.0, 0.0]}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    let error_msg = body["error"].as_str().unwrap().to_lowercase();
    assert!(error_msg.contains("vector id"), "got: {error_msg}");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// --- Test 4: Vector ID empty rejected ---

#[tokio::test]
async fn test_vector_id_empty_rejected() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 4).await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [{"id": "", "values": [1.0, 0.0, 0.0, 0.0]}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    let error_msg = body["error"].as_str().unwrap().to_lowercase();
    assert!(error_msg.contains("vector id"), "got: {error_msg}");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// --- Test 5: Vector ID at max length accepted ---

#[tokio::test]
async fn test_vector_id_at_max_length_accepted() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 4).await;

    let max_id = "x".repeat(1024);
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [{"id": max_id, "values": [1.0, 0.0, 0.0, 0.0]}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
