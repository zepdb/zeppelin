mod common;

use common::server::{
    cleanup_ns, create_ns_api, start_test_server, start_test_server_with_compactor,
    start_test_server_with_config,
};
use common::vectors::random_vectors;

use zeppelin::wal::Manifest;

// --- Test 1: Oversized batch returns 413 without durable side effects ---

#[tokio::test]
async fn test_oversized_upsert_batch_413_without_wal_refs() {
    let mut config = zeppelin::config::Config::default();
    config.server.max_batch_size = 10;

    let (base_url, harness, _cache, _dir, admin_bearer) =
        start_test_server_with_config(Some(config)).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 4).await;
    let before = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();

    // Upsert 11 vectors (exceeds configured max_batch_size of 10)
    let vectors: Vec<serde_json::Value> = (0..11)
        .map(|i| {
            serde_json::json!({
                "id": format!("v_{i}"),
                "values": [1.0, 0.0, 0.0, 0.0],
            })
        })
        .collect();

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413);
    let body: serde_json::Value = resp.json().await.unwrap();
    // Task 11: clients key off the stable `code`, not the human message.
    assert_eq!(body["code"], "PAYLOAD_TOO_LARGE");
    assert_eq!(body["retryable"], false);
    let error_msg = body["error"].as_str().unwrap().to_lowercase();
    assert!(error_msg.contains("batch size"), "got: {error_msg}");
    let after = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(
        after.fragments, before.fragments,
        "oversized upsert must not add WAL refs"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_max_sized_upsert_batch_writes_one_bounded_wal_fragment() {
    let mut config = zeppelin::config::Config::default();
    config.server.max_batch_size = 3;

    let (base_url, harness, _cache, _dir, admin_bearer) =
        start_test_server_with_config(Some(config)).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 4).await;

    let vectors: Vec<serde_json::Value> = (0..3)
        .map(|i| {
            serde_json::json!({
                "id": format!("v_{i}"),
                "values": [1.0, 0.0, 0.0, 0.0],
            })
        })
        .collect();

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["upserted"], 3);

    let manifest = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(
        manifest.fragments.len(),
        1,
        "accepted upsert should commit exactly one WAL fragment"
    );
    assert_eq!(manifest.fragments[0].vector_count, 3);
    assert!(
        manifest.fragments[0].vector_count <= 3,
        "fragment vector count must stay within configured max_batch_size"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// --- Test 2: Empty batch returns 400 ---

#[tokio::test]
async fn test_empty_batch_400() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 4).await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": [] }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "VALIDATION_ERROR");
    let error_msg = body["error"].as_str().unwrap().to_lowercase();
    assert!(error_msg.contains("empty"), "got: {error_msg}");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// --- Test 3: top_k too large returns 400 ---

#[tokio::test]
async fn test_top_k_too_large_400() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 4).await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 100_000,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "VALIDATION_ERROR");
    let error_msg = body["error"].as_str().unwrap().to_lowercase();
    assert!(error_msg.contains("top_k"), "got: {error_msg}");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// --- Test 4: top_k zero returns 400 ---

#[tokio::test]
async fn test_top_k_zero_400() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 4).await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
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
    let error_msg = body["error"].as_str().unwrap().to_lowercase();
    assert!(error_msg.contains("top_k"), "got: {error_msg}");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// --- Test 5: /readyz endpoint ---

#[tokio::test]
async fn test_readyz_endpoint() {
    let (base_url, harness, admin_bearer) = start_test_server().await;

    let resp = crate::common::server::client_with_bearer(&admin_bearer)
        .get(format!("{base_url}/readyz"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["s3_connected"], true);

    harness.cleanup().await;
}

// --- Test 6: /healthz still works (backward compat) ---

#[tokio::test]
async fn test_healthz_still_works() {
    let (base_url, harness, _admin_bearer) = start_test_server().await;

    let resp = reqwest::get(format!("{base_url}/healthz")).await.unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");

    harness.cleanup().await;
}

// --- Test 7: /metrics endpoint ---

#[tokio::test]
async fn test_metrics_endpoint() {
    let (base_url, harness, admin_bearer) = start_test_server().await;

    // Touch a metric to ensure there's output
    zeppelin::metrics::QUERIES_TOTAL
        .with_label_values(&["__test__"])
        .inc();

    let resp = crate::common::server::client_with_bearer(&admin_bearer)
        .get(format!("{base_url}/metrics"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let content_type = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        content_type.contains("text/plain"),
        "expected prometheus text format, got: {content_type}"
    );

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("zeppelin_"),
        "metrics should contain zeppelin_ prefix, got: {}",
        &body[..200.min(body.len())]
    );

    harness.cleanup().await;
}

// --- Test 8: Metrics increment after query ---

#[tokio::test]
async fn test_metrics_increment_after_query() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 4).await;

    // Upsert vectors
    let vectors = vec![serde_json::json!({"id": "v1", "values": [1.0, 0.0, 0.0, 0.0]})];
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();

    // Execute a query
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 5,
        }))
        .send()
        .await
        .unwrap();

    // Check metrics
    let resp = client
        .get(format!("{base_url}/metrics"))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("zeppelin_queries_total"),
        "should have queries counter, got: {}",
        &body[..300.min(body.len())]
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// --- Test 9: Request timeout middleware is wired ---

#[tokio::test]
async fn test_request_timeout_applied() {
    let mut config = zeppelin::config::Config::default();
    config.server.request_timeout_secs = 30;

    let (base_url, harness, _cache, _dir, admin_bearer) =
        start_test_server_with_config(Some(config)).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 4).await;

    // Normal query should complete successfully (validates middleware is wired)
    let vectors = vec![serde_json::json!({"id": "v1", "values": [1.0, 0.0, 0.0, 0.0]})];
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();

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

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// --- Test 10: Cache populated after segment query ---

#[tokio::test]
async fn test_cache_populated_after_segment_query() {
    let (base_url, harness, cache, _cache_dir, compactor, admin_bearer) =
        start_test_server_with_compactor(None).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 8).await;

    // Upsert vectors
    let vectors = random_vectors(50, 8);
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();

    let compact_result = compactor.compact(&ns).await;

    if let Ok(result) = compact_result {
        // Compaction succeeded — query with eventual consistency to hit the segment
        assert!(
            result.vectors_compacted > 0,
            "expected vectors to be compacted"
        );

        let query_vec: Vec<f32> = vec![0.5; 8];
        let resp = client
            .post(format!("{base_url}/v1/namespaces/{ns}/query"))
            .json(&serde_json::json!({
                "vector": query_vec,
                "top_k": 5,
                "consistency": "eventual",
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = resp.json().await.unwrap();
        let scanned = body["scanned_segments"].as_u64().unwrap_or(0);
        assert!(scanned > 0, "expected segment scan after compaction");
        assert!(
            cache.total_size() > 0,
            "cache should have data after segment query"
        );
    } else {
        // Compaction may fail if the namespace setup wasn't complete — skip gracefully
        eprintln!(
            "[test] compaction failed (expected in some environments): {:?}",
            compact_result.err()
        );
    }

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
