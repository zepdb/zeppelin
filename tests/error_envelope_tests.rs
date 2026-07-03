//! Task 11 — error responses: stable codes, no internals leakage, consistent
//! envelope across handler AND middleware-produced errors.
//!
//! Every error body must parse as `{code, error, status, retryable}` (+
//! `request_id` when known). Codes are stable and client-keyable; internal S3
//! detail never appears in a body.

mod common;

use common::server::{
    cleanup_ns, create_ns_api, start_test_server, start_test_server_with_compactor,
    start_test_server_with_config,
};
use common::vectors::random_vectors;

use serde_json::Value;
use zeppelin::config::Config;

/// Assert a JSON body is a well-formed error envelope with the expected code
/// and status, and that `retryable` is a bool.
fn assert_envelope(body: &Value, expected_code: &str, expected_status: u16) {
    assert_eq!(
        body["code"], expected_code,
        "wrong code in envelope: {body}"
    );
    assert_eq!(
        body["status"], expected_status,
        "wrong status in envelope: {body}"
    );
    assert!(
        body["error"].is_string(),
        "envelope missing human `error` string: {body}"
    );
    assert!(
        body["retryable"].is_boolean(),
        "envelope missing bool `retryable`: {body}"
    );
}

/// I1: namespace-not-found returns the canonical envelope with a stable code.
#[tokio::test]
async fn test_envelope_namespace_not_found() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base_url}/v1/namespaces/does-not-exist-xyz"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);
    let body: Value = resp.json().await.unwrap();
    assert_envelope(&body, "NAMESPACE_NOT_FOUND", 404);

    harness.cleanup().await;
}

/// I1: validation error (empty batch) returns the envelope, not retryable.
#[tokio::test]
async fn test_envelope_validation() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 4).await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": [] }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
    let body: Value = resp.json().await.unwrap();
    assert_envelope(&body, "VALIDATION_ERROR", 400);
    assert_eq!(body["retryable"], false);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

/// I5: malformed JSON to upsert → 400 envelope (custom rejection), NOT axum's
/// 422 plain-text.
#[tokio::test]
async fn test_envelope_malformed_upsert_body() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 4).await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .header("content-type", "application/json")
        .body("{not valid json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
    let body: Value = resp.json().await.unwrap();
    assert_envelope(&body, "VALIDATION_ERROR", 400);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

/// I4: an unmatched route returns the canonical envelope via the fallback,
/// not an empty body.
#[tokio::test]
async fn test_envelope_unmatched_route() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base_url}/v1/this/route/does/not/exist"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);
    let body: Value = resp.json().await.unwrap();
    assert_envelope(&body, "NOT_FOUND", 404);

    harness.cleanup().await;
}

/// I4: a body over the configured limit returns the canonical envelope (413)
/// via the outer normalization layer, not the bare tower-http plain body.
#[tokio::test]
async fn test_envelope_body_too_large() {
    // Tiny 1 MB body limit so a modest payload trips RequestBodyLimitLayer.
    let mut config = Config::load(None).unwrap();
    config.server.max_request_body_mb = 1;
    let (base_url, harness, _cache, _dir) = start_test_server_with_config(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 4).await;

    let big = "x".repeat(4 * 1024 * 1024); // 4 MB > 1 MB limit
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .header("content-type", "application/json")
        .body(big)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 413);
    let body: Value = resp.json().await.unwrap();
    assert_envelope(&body, "PAYLOAD_TOO_LARGE", 413);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

/// I1: the request_id supplied by the client is echoed into the error envelope
/// (for routes that carry the request_id middleware — namespace routes do).
#[tokio::test]
async fn test_envelope_carries_request_id() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base_url}/v1/namespaces/nope-xyz"))
        .header("x-request-id", "test-req-42")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);
    // Echoed in the header...
    assert_eq!(
        resp.headers()
            .get("x-request-id")
            .and_then(|v| v.to_str().ok()),
        Some("test-req-42")
    );
    // ...and stamped into the envelope body.
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["request_id"], "test-req-42");

    harness.cleanup().await;
}

/// I2/I3: an internal S3 object miss (a cluster the manifest references but
/// that is gone from S3) surfaces as 500 INTERNAL_DATA_MISSING — NOT a 404 —
/// and the raw S3 key never appears in the client body.
#[tokio::test]
async fn test_internal_data_missing_is_500_no_key_leak() {
    let (base_url, harness, _cache, _dir, compactor) = start_test_server_with_compactor(None).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 8).await;

    // Upsert enough vectors to build a real segment, then compact.
    let vectors: Vec<serde_json::Value> = random_vectors(60, 8)
        .into_iter()
        .enumerate()
        .map(|(i, v)| serde_json::json!({ "id": format!("v_{i}"), "values": v.values }))
        .collect();
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    compactor.compact(&ns).await.unwrap();

    // Delete the segment's centroids object — an artifact the manifest
    // references and the query path loads FAIL-LOUD (unlike per-cluster reads,
    // which the search currently warns-and-skips; that silent-partial behavior
    // is Task 9's concern, tracked separately). This simulates S3 corruption /
    // an object that vanished out from under the manifest.
    let manifest = zeppelin::wal::Manifest::read(&harness.store, &ns)
        .await
        .unwrap()
        .unwrap();
    let seg = manifest.active_segment.clone().unwrap();
    let centroids_key = format!("{ns}/segments/{seg}/centroids.bin");
    harness.store.delete(&centroids_key).await.unwrap();

    // A strong query must now read that missing cluster.
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": random_vectors(1, 8)[0].values,
            "top_k": 10,
            "nprobe": 8,
            "consistency": "strong",
        }))
        .send()
        .await
        .unwrap();

    let status = resp.status().as_u16();
    let raw_body = resp.text().await.unwrap();

    // Must be a 500 data-missing, never a 404 "namespace gone".
    assert_eq!(
        status, 500,
        "internal cluster miss must be 500, got {status}: {raw_body}"
    );
    // I3: the raw S3 key / prefix must not leak into the client body.
    assert!(
        !raw_body.contains("segments/") && !raw_body.contains(&seg),
        "client body leaked an internal S3 key: {raw_body}"
    );
    let body: Value = serde_json::from_str(&raw_body).unwrap();
    assert_envelope(&body, "INTERNAL_DATA_MISSING", 500);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
