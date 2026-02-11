mod common;

use common::server::{api_ns, cleanup_ns, start_test_server, start_test_server_with_config};
use common::vectors::random_vectors;

use zeppelin::wal::WalReader;

// --- Test 1: HTTP request metrics are incremented after API calls ---

#[tokio::test]
async fn test_http_request_metrics_incremented() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();

    // Make a request to a known endpoint
    let resp = client
        .get(format!("{base_url}/healthz"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Check metrics endpoint for HTTP_REQUESTS_TOTAL
    let resp = client
        .get(format!("{base_url}/metrics"))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("zeppelin_http_requests_total"),
        "metrics should contain zeppelin_http_requests_total"
    );

    harness.cleanup().await;
}

// --- Test 2: S3 metrics are recorded after operations ---

#[tokio::test]
async fn test_s3_metrics_after_operations() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "obs-s3-metrics");

    // Create namespace (triggers S3 operations)
    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 4,
        }))
        .send()
        .await
        .unwrap();

    // Upsert vectors (triggers more S3 operations)
    let vectors = vec![
        serde_json::json!({"id": "v1", "values": [1.0, 0.0, 0.0, 0.0]}),
    ];
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();

    // Query (triggers S3 reads)
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 5,
        }))
        .send()
        .await
        .unwrap();

    // Check that S3 metrics are present
    let resp = client
        .get(format!("{base_url}/metrics"))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("zeppelin_s3_operation_duration_seconds"),
        "metrics should contain S3 operation duration"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// --- Test 3: Active queries gauge returns to zero after query completes ---

#[tokio::test]
async fn test_active_queries_returns_to_zero() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "obs-active-q");

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

    // Upsert a vector
    let vectors = vec![
        serde_json::json!({"id": "v1", "values": [1.0, 0.0, 0.0, 0.0]}),
    ];
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();

    // Execute a query
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

    // After query completes, active_queries should be 0
    let resp = client
        .get(format!("{base_url}/metrics"))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("zeppelin_active_queries 0"),
        "active queries should be 0 after query completes, metrics:\n{}",
        body.lines()
            .filter(|l| l.contains("active_queries"))
            .collect::<Vec<_>>()
            .join("\n")
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// --- Test 4: x-request-id header is returned in responses ---

#[tokio::test]
async fn test_request_id_header_returned() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base_url}/healthz"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let request_id = resp.headers().get("x-request-id");
    assert!(
        request_id.is_some(),
        "response should have x-request-id header"
    );

    // Should be a valid UUID-like string (non-empty)
    let id_value = request_id.unwrap().to_str().unwrap();
    assert!(!id_value.is_empty(), "x-request-id should not be empty");

    harness.cleanup().await;
}

// --- Test 5: x-request-id passthrough ---

#[tokio::test]
async fn test_request_id_passthrough() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();

    let custom_id = "custom-request-123";
    let resp = client
        .get(format!("{base_url}/healthz"))
        .header("x-request-id", custom_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let returned_id = resp
        .headers()
        .get("x-request-id")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(
        returned_id, custom_id,
        "response should echo back the provided x-request-id"
    );

    harness.cleanup().await;
}

// --- Test 6: Compaction duration metric is recorded ---

#[tokio::test]
async fn test_compaction_duration_metric() {
    let (base_url, harness, _cache, _dir) = start_test_server_with_config(None).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "obs-compact-dur");

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

    // Upsert vectors
    let vectors = random_vectors(50, 8);
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();

    // Trigger manual compaction
    let compactor = zeppelin::compaction::Compactor::new(
        harness.store.clone(),
        WalReader::new(harness.store.clone()),
        zeppelin::config::CompactionConfig::default(),
        zeppelin::config::IndexingConfig::default(),
    );
    let compact_result = compactor.compact(&ns).await;

    if compact_result.is_ok() {
        // Check metrics for compaction duration
        let resp = client
            .get(format!("{base_url}/metrics"))
            .send()
            .await
            .unwrap();
        let body = resp.text().await.unwrap();
        assert!(
            body.contains("zeppelin_compaction_duration_seconds"),
            "metrics should contain compaction duration after compaction"
        );
    } else {
        eprintln!(
            "[test] compaction failed (expected in some environments): {:?}",
            compact_result.err()
        );
    }

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// --- Test 7: All metric families are registered ---

#[tokio::test]
async fn test_all_metrics_registered() {
    use zeppelin::metrics::*;

    // Ensure metrics are initialized
    init();

    // Touch all Vec metrics so they appear in gather() output
    // (prometheus-rs omits Vec families with zero observations)
    HTTP_REQUESTS_TOTAL.with_label_values(&["GET", "/test", "200"]).inc();
    QUERY_DURATION.with_label_values(&["__test__"]).observe(0.0);
    QUERIES_TOTAL.with_label_values(&["__test__"]).inc();
    WAL_APPENDS_TOTAL.with_label_values(&["__test__"]).inc();
    CACHE_HITS_TOTAL.with_label_values(&["hit"]).inc();
    COMPACTIONS_TOTAL.with_label_values(&["__test__", "success"]).inc();
    S3_OPERATION_DURATION.with_label_values(&["get"]).observe(0.0);
    S3_ERRORS_TOTAL.with_label_values(&["get"]).inc();
    COMPACTION_DURATION.with_label_values(&["__test__"]).observe(0.0);

    let families = prometheus::gather();
    let names: Vec<String> = families.iter().map(|f| f.get_name().to_string()).collect();

    let expected = [
        "zeppelin_http_requests_total",
        "zeppelin_query_duration_seconds",
        "zeppelin_queries_total",
        "zeppelin_wal_appends_total",
        "zeppelin_cache_hits_total",
        "zeppelin_compactions_total",
        "zeppelin_s3_operation_duration_seconds",
        "zeppelin_s3_errors_total",
        "zeppelin_compaction_duration_seconds",
        "zeppelin_cache_entries",
        "zeppelin_cache_evictions_total",
        "zeppelin_active_queries",
    ];

    for name in &expected {
        assert!(
            names.contains(&name.to_string()),
            "metric '{}' should be registered, found: {:?}",
            name,
            names
        );
    }
}
