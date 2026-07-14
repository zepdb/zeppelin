mod common;

use common::counting::counting_store;
use common::harness::TestHarness;
use common::server::{cleanup_ns, create_ns_api, start_test_server, start_test_server_full};
use zeppelin::config::Config;

// --- Test 1: Dimensions too large rejected ---

#[tokio::test]
async fn test_dimensions_too_large_rejected() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);

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
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);

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

#[tokio::test]
async fn test_create_namespace_rejects_invalid_fts_field_config() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);

    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "dimensions": 4,
            "full_text_search": {
                "content": {
                    "k1": -1.0,
                    "b": 1.5,
                    "max_token_length": 0
                }
            }
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    let error_msg = body["error"].as_str().unwrap();
    for needle in [
        "full_text_search.content.k1",
        "full_text_search.content.b",
        "full_text_search.content.max_token_length",
    ] {
        assert!(
            error_msg.contains(needle),
            "expected invalid FTS create error to contain {needle:?}, got: {error_msg}"
        );
    }

    harness.cleanup().await;
}

// --- Test 3: Vector ID too long rejected ---

#[tokio::test]
async fn test_vector_id_too_long_rejected() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
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

#[tokio::test]
async fn test_vector_delete_rejects_untrusted_ids_before_wal_and_audit() {
    let harness = TestHarness::new().await;
    let (counted_store, counter) = counting_store(&harness.store);
    let config = Config::default();
    let max_vector_id_length = config.server.max_vector_id_length;
    let server = start_test_server_full(
        counted_store,
        Some(harness.prefix.clone()),
        config,
        false,
        None,
    )
    .await;
    let client = crate::common::server::client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&client, &server.base_url, 4).await;
    counter.reset();

    let arbitrary_marker = format!("delete-payload-marker-{}", harness.prefix);
    let arbitrary_id = format!(r#"{arbitrary_marker}/{{"secret":"do-not-audit"}}"#);
    let oversized_marker = format!("delete-oversized-marker-{}", harness.prefix);
    let oversized_id = format!(
        "{oversized_marker}-{}",
        "x".repeat(max_vector_id_length + 1)
    );

    let arbitrary_response = client
        .delete(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&serde_json::json!({ "ids": [arbitrary_id] }))
        .send()
        .await
        .unwrap();
    let oversized_response = client
        .delete(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&serde_json::json!({ "ids": [oversized_id] }))
        .send()
        .await
        .unwrap();

    server.flush_audit().await;
    let mut audit_keys = harness.store.list_prefix("_audit/").await.unwrap();
    audit_keys.retain(|key| key.contains(&format!("/{}/", server.audit_node_id)));
    let mut leaked_untrusted_id = false;
    for key in audit_keys {
        let body = harness.store.get(&key).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        leaked_untrusted_id |= body.contains(&arbitrary_marker) || body.contains(&oversized_marker);
    }
    let observed = (
        arbitrary_response.status().as_u16(),
        oversized_response.status().as_u16(),
        counter.puts_matching("/wal/"),
        leaked_untrusted_id,
    );

    server.shutdown().await;
    harness.cleanup().await;

    assert_eq!(observed, (400, 400, 0, false));
}

// --- Test 4: Vector ID empty rejected ---

#[tokio::test]
async fn test_vector_id_empty_rejected() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
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
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
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
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
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
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
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
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);

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
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);

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
