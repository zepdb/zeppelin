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

// --- Dimension mismatch names the offending vector (Task 10, I5) ---

#[tokio::test]
async fn test_dimension_mismatch_names_vector_id() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 4).await;

    // Second vector in the batch has the wrong dimension — the error must
    // identify WHICH vector failed, not just "expected 4, got 2".
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [
                {"id": "ok-1", "values": [1.0, 0.0, 0.0, 0.0]},
                {"id": "short-2", "values": [1.0, 0.0]}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    let error_msg = body["error"].as_str().unwrap();
    assert!(
        error_msg.contains("short-2"),
        "dimension-mismatch error must name the offending vector id, got: {error_msg}"
    );
    assert!(
        error_msg.contains('4') && error_msg.contains('2'),
        "dimension-mismatch error must keep expected/actual dims, got: {error_msg}"
    );

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

// --- Task 14: documented request bounds enforced server-side ---

/// I1: `nprobe: 0` is below the documented minimum (api yaml `minimum: 1`) and
/// must be rejected with 400 — NOT silently accepted (which probes zero
/// clusters and returns an empty 200).
#[tokio::test]
async fn test_nprobe_zero_rejected() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 4).await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 10,
            "nprobe": 0,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "nprobe:0 must be a 400, not an empty 200"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "VALIDATION_ERROR");
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .to_lowercase()
            .contains("nprobe"),
        "got: {}",
        body["error"]
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

/// I2: request-shape validation runs BEFORE namespace resolution — an invalid
/// request to a nonexistent namespace is a 400 (bad request), not a 404. Here
/// `nprobe: 0` on a namespace that does not exist.
#[tokio::test]
async fn test_invalid_request_missing_namespace_is_400_not_404() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base_url}/v1/namespaces/does-not-exist-xyz/query"))
        .json(&serde_json::json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 0,
            "nprobe": 0,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "an invalid request must be a 400 regardless of whether the namespace exists"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "VALIDATION_ERROR");

    harness.cleanup().await;
}

/// I1: `top_k: 0` is below the documented minimum and must be 400. (Regression
/// pin — already enforced, but assert the code + that it doesn't need the
/// namespace to exist.)
#[tokio::test]
async fn test_top_k_zero_rejected_before_namespace() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base_url}/v1/namespaces/nope-xyz/query"))
        .json(&serde_json::json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 0,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "VALIDATION_ERROR");

    harness.cleanup().await;
}
