mod common;

use common::server::create_ns_api_with;
use serde_json::json;

#[tokio::test]
async fn test_empty_body_upsert() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 4,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();

    // Missing `vectors` field should fail deserialization. Task 11 I5: upsert
    // now uses a custom rejection → 400 with the canonical envelope (was 422
    // plain-text from axum's Json extractor; that acceptance is revoked).
    assert_eq!(resp.status().as_u16(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "VALIDATION_ERROR");
    assert_eq!(body["status"], 400);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_empty_vectors_array() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 4,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&json!({ "vectors": [] }))
        .send()
        .await
        .unwrap();

    // Empty vectors should be rejected as 400
    assert_eq!(resp.status().as_u16(), 400);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_empty_delete_ids_array() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 4,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    let resp = client
        .delete(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&json!({ "ids": [] }))
        .send()
        .await
        .unwrap();

    // Empty ids should be rejected as 400
    assert_eq!(resp.status().as_u16(), 400);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_wrong_type_vector_values() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 4,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&json!({
            "vectors": [{
                "id": "test-1",
                "values": "not-a-vector"
            }]
        }))
        .send()
        .await
        .unwrap();

    // String instead of array should fail deserialization. Task 11 I5: 400 +
    // canonical envelope (was 422 plain-text).
    assert_eq!(resp.status().as_u16(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "VALIDATION_ERROR");

    harness.cleanup().await;
}

#[tokio::test]
async fn test_query_both_vector_and_rank_by() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 4,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&json!({
            "vector": [1.0, 2.0, 3.0, 4.0],
            "rank_by": ["title", "BM25", "hello"]
        }))
        .send()
        .await
        .unwrap();

    // Providing both should return 400
    assert_eq!(resp.status().as_u16(), 400);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_query_neither_vector_nor_rank_by() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 4,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&json!({ "top_k": 10 }))
        .send()
        .await
        .unwrap();

    // Providing neither should return 400
    assert_eq!(resp.status().as_u16(), 400);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_invalid_vector_id_characters() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 4,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&json!({
            "vectors": [{
                "id": "bad\x00id",
                "values": [1.0, 2.0, 3.0, 4.0]
            }]
        }))
        .send()
        .await
        .unwrap();

    // Invalid characters should return 400
    assert_eq!(resp.status().as_u16(), 400);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_nprobe_exceeds_max() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 4,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&json!({
            "vector": [1.0, 2.0, 3.0, 4.0],
            "nprobe": 9999
        }))
        .send()
        .await
        .unwrap();

    // nprobe exceeding max should return 400
    assert_eq!(resp.status().as_u16(), 400);

    harness.cleanup().await;
}

// --- Non-finite float rejection (Task 10) ---
//
// JSON cannot represent NaN or Infinity literally. serde_json rejects
// literals beyond f64 range (`1e999` → "number out of range" parse error),
// BUT a literal that fits in f64 while exceeding f32 range (`1e39`) is
// silently overflowed to +inf during the f64→f32 conversion — that is the
// realistic path for non-finite values arriving over HTTP. NaN itself cannot
// be expressed in JSON at all, so the inf-overflow cases below are the
// complete API-boundary coverage. (NaN reaching storage by other means is
// covered by the compaction defense-in-depth test in compaction_tests.rs.)
//
// The request bodies are raw strings, not serde-built: serde_json serializes
// f32::INFINITY as `null`, which fails deserialization with a different
// (type) error — we specifically want the overflow-to-inf path.

#[tokio::test]
async fn test_upsert_inf_rejected_nothing_durable() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 4,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    let body = r#"{"vectors":[
        {"id":"good-1","values":[0.1,0.2,0.3,0.4]},
        {"id":"bad-42","values":[0.1,0.2,1e39,0.4]}
    ]}"#;
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status().as_u16(),
        400,
        "upsert containing +inf (via 1e39 f32-overflow) must be rejected"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let msg = body["error"].as_str().unwrap();
    assert!(
        msg.contains("bad-42"),
        "error must name the offending vector id, got: {msg}"
    );
    assert!(
        msg.contains("dimension 2"),
        "error must name the offending dimension index, got: {msg}"
    );
    assert!(
        msg.contains("inf"),
        "error must name the non-finite kind, got: {msg}"
    );

    // I1: NOTHING durable written — no WAL fragment on S3.
    let wal_keys = harness
        .store
        .list_prefix(&format!("{ns}/wal/"))
        .await
        .unwrap();
    assert!(
        wal_keys.is_empty(),
        "rejected upsert must not write a WAL fragment, found: {wal_keys:?}"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn test_upsert_neg_inf_rejected_nothing_durable() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 4,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    let body = r#"{"vectors":[{"id":"neg-7","values":[-1e39,0.2,0.3,0.4]}]}"#;
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status().as_u16(),
        400,
        "upsert containing -inf (via -1e39 f32-overflow) must be rejected"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let msg = body["error"].as_str().unwrap();
    assert!(
        msg.contains("neg-7"),
        "error must name the offending vector id, got: {msg}"
    );
    assert!(
        msg.contains("dimension 0"),
        "error must name the offending dimension index, got: {msg}"
    );

    let wal_keys = harness
        .store
        .list_prefix(&format!("{ns}/wal/"))
        .await
        .unwrap();
    assert!(
        wal_keys.is_empty(),
        "rejected upsert must not write a WAL fragment, found: {wal_keys:?}"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn test_query_inf_rejected() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 4,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    // I2: query vector with a non-finite value → 400.
    let body = r#"{"vector":[0.1,0.2,1e39,0.4],"top_k":5}"#;
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status().as_u16(),
        400,
        "query containing +inf (via 1e39 f32-overflow) must be rejected"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let msg = body["error"].as_str().unwrap();
    assert!(
        msg.contains("dimension 2"),
        "error must name the offending dimension index, got: {msg}"
    );
    assert!(
        msg.contains("inf"),
        "error must name the non-finite kind, got: {msg}"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn test_top_k_zero() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 4,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&json!({
            "vector": [1.0, 2.0, 3.0, 4.0],
            "top_k": 0
        }))
        .send()
        .await
        .unwrap();

    // top_k=0 should return 400
    assert_eq!(resp.status().as_u16(), 400);

    harness.cleanup().await;
}
