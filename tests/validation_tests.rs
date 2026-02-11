mod common;

use common::server::{api_ns, cleanup_ns, start_test_server};

// --- Test 1: Namespace name with slash rejected ---

#[tokio::test]
async fn test_namespace_name_with_slash_rejected() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": "bad/name",
            "dimensions": 4,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    let error_msg = body["error"].as_str().unwrap().to_lowercase();
    assert!(error_msg.contains("namespace name"), "got: {error_msg}");

    harness.cleanup().await;
}

// --- Test 2: Namespace name with spaces rejected ---

#[tokio::test]
async fn test_namespace_name_with_spaces_rejected() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": "bad name",
            "dimensions": 4,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    let error_msg = body["error"].as_str().unwrap().to_lowercase();
    assert!(error_msg.contains("namespace name"), "got: {error_msg}");

    harness.cleanup().await;
}

// --- Test 3: Namespace name with special chars rejected ---

#[tokio::test]
async fn test_namespace_name_with_special_chars_rejected() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": "bad?name#here",
            "dimensions": 4,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    let error_msg = body["error"].as_str().unwrap().to_lowercase();
    assert!(error_msg.contains("namespace name"), "got: {error_msg}");

    harness.cleanup().await;
}

// --- Test 4: Namespace name too long rejected ---

#[tokio::test]
async fn test_namespace_name_too_long_rejected() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();

    let long_name = "a".repeat(256);
    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": long_name,
            "dimensions": 4,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    let error_msg = body["error"].as_str().unwrap().to_lowercase();
    assert!(error_msg.contains("namespace name"), "got: {error_msg}");

    harness.cleanup().await;
}

// --- Test 5: Valid namespace name accepted ---

#[tokio::test]
async fn test_namespace_name_valid_chars_accepted() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "my-ns_123");

    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 4,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// --- Test 6: Dimensions too large rejected ---

#[tokio::test]
async fn test_dimensions_too_large_rejected() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "val-dim-big");

    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 100_000,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    let error_msg = body["error"].as_str().unwrap().to_lowercase();
    assert!(error_msg.contains("dimensions"), "got: {error_msg}");

    harness.cleanup().await;
}

// --- Test 7: Dimensions zero rejected ---

#[tokio::test]
async fn test_dimensions_zero_rejected() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "val-dim-zero");

    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 0,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    let error_msg = body["error"].as_str().unwrap().to_lowercase();
    assert!(error_msg.contains("dimensions"), "got: {error_msg}");

    harness.cleanup().await;
}

// --- Test 8: Vector ID too long rejected ---

#[tokio::test]
async fn test_vector_id_too_long_rejected() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "val-vid-long");

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

// --- Test 9: Vector ID empty rejected ---

#[tokio::test]
async fn test_vector_id_empty_rejected() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "val-vid-empty");

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

// --- Test 10: Vector ID at max length accepted ---

#[tokio::test]
async fn test_vector_id_at_max_length_accepted() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "val-vid-max");

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
