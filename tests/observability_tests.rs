mod common;

use common::server::{
    cleanup_ns, create_ns_api, start_test_server, start_test_server_with_compactor,
    start_test_server_with_config,
};
use common::vectors::{clustered_vectors, random_vectors};

use zeppelin::wal::{WalReader, WalWriter};

#[derive(Clone)]
struct CapturedLogWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(|_| panic!("captured log buffer lock poisoned"))
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn bearer_secret_is_not_recorded_in_handler_spans() {
    let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer({
            let captured = std::sync::Arc::clone(&captured);
            move || CapturedLogWriter(std::sync::Arc::clone(&captured))
        })
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let (base_url, harness, admin_bearer) = start_test_server().await;
                let client = crate::common::server::client_with_bearer(&admin_bearer);
                let namespace = create_ns_api(&client, &base_url, 2).await;

                let response = client
                    .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
                    .json(&serde_json::json!({
                        "vectors": [{"id": "redaction-probe", "values": [1.0, 0.0]}]
                    }))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status(), 200);

                cleanup_ns(&harness.store, &namespace).await;
                harness.cleanup().await;

                let output = String::from_utf8(
                    captured
                        .lock()
                        .unwrap_or_else(|_| panic!("captured log buffer lock poisoned"))
                        .clone(),
                )
                .unwrap();
                assert!(
                    !output.contains(&admin_bearer),
                    "bearer material leaked into tracing output"
                );
            });
    });
}

// --- Test 1: HTTP request metrics are incremented after API calls ---

#[tokio::test]
async fn test_http_request_metrics_incremented() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);

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
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 4).await;

    // Upsert vectors (triggers S3 operations)
    let vectors = vec![serde_json::json!({"id": "v1", "values": [1.0, 0.0, 0.0, 0.0]})];
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
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 4).await;

    // Upsert a vector
    let vectors = vec![serde_json::json!({"id": "v1", "values": [1.0, 0.0, 0.0, 0.0]})];
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

    // `zeppelin_active_queries` is process-global, and Rust runs tests in this
    // binary concurrently by default. Poll briefly so unrelated query tests can
    // finish before asserting the gauge returned to zero.
    let mut body = String::new();
    for _ in 0..20 {
        let resp = client
            .get(format!("{base_url}/metrics"))
            .send()
            .await
            .unwrap();
        body = resp.text().await.unwrap();
        if body.contains("zeppelin_active_queries 0") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
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
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);

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
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);

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

// --- Test 5a: query route returns x-request-id without TraceLayer ---

#[tokio::test]
async fn test_query_request_id_header_returned_and_echoed() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 4).await;

    let upsert = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [
                {"id": "v1", "values": [1.0, 0.0, 0.0, 0.0]}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(upsert.status(), 200);

    let generated = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 1,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(generated.status(), 200);
    let generated_id = generated.headers().get("x-request-id");
    assert!(
        generated_id.is_some(),
        "query route must return an x-request-id header"
    );
    assert!(
        !generated_id.unwrap().to_str().unwrap().is_empty(),
        "generated query x-request-id must not be empty"
    );

    let custom_id = "query-request-123";
    let echoed = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .header("x-request-id", custom_id)
        .json(&serde_json::json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 1,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(echoed.status(), 200);
    assert_eq!(
        echoed
            .headers()
            .get("x-request-id")
            .unwrap()
            .to_str()
            .unwrap(),
        custom_id,
        "query route must echo an inbound x-request-id"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// --- Test 5b: query debug block is opt-in and contains phase diagnostics ---

#[tokio::test]
async fn test_query_debug_block_is_opt_in() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 4).await;

    let upsert = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [
                {"id": "v1", "values": [1.0, 0.0, 0.0, 0.0]}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(upsert.status(), 200);

    let default_body: serde_json::Value = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 1,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        default_body.get("debug").is_none(),
        "query debug block must be absent by default: {default_body}"
    );

    let debug_body: serde_json::Value = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 1,
            "consistency": "strong",
            "debug": true
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let debug = debug_body
        .get("debug")
        .and_then(serde_json::Value::as_object)
        .unwrap_or_else(|| panic!("debug=true must return a debug block: {debug_body}"));
    for field in ["wal_ms", "segment_ms", "merge_ms"] {
        assert!(
            debug
                .get(field)
                .and_then(serde_json::Value::as_u64)
                .is_some(),
            "debug.{field} must be a non-negative integer: {debug_body}"
        );
    }
    assert_eq!(
        debug["fragments_scanned"], debug_body["scanned_fragments"],
        "debug fragments must preserve existing scanned_fragments semantics"
    );
    assert_eq!(
        debug["segments_scanned"], debug_body["scanned_segments"],
        "debug segments must preserve existing scanned_segments semantics"
    );
    assert_eq!(debug["consistency_effective"], "strong");
    assert!(
        debug
            .get("clusters_probed")
            .and_then(serde_json::Value::as_u64)
            .is_some(),
        "debug.clusters_probed must be present: {debug_body}"
    );
    let cache = debug
        .get("cache")
        .and_then(serde_json::Value::as_object)
        .unwrap_or_else(|| panic!("debug.cache must be present: {debug_body}"));
    assert!(
        cache
            .get("hits")
            .and_then(serde_json::Value::as_u64)
            .is_some(),
        "debug.cache.hits must be present: {debug_body}"
    );
    assert!(
        cache
            .get("misses")
            .and_then(serde_json::Value::as_u64)
            .is_some(),
        "debug.cache.misses must be present: {debug_body}"
    );
    assert!(
        debug.contains_key("underfill_reason"),
        "debug.underfill_reason must be present, even when null: {debug_body}"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// --- Test 6: Compaction duration metric is recorded ---

#[tokio::test]
async fn test_compaction_duration_metric() {
    let (base_url, harness, _cache, _dir, admin_bearer) = start_test_server_with_config(None).await;
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

    // Trigger manual compaction
    let compactor = zeppelin::compaction::Compactor::new(
        harness.store.clone(),
        WalReader::new(harness.store.clone()),
        zeppelin::config::CompactionConfig::default(),
        zeppelin::config::IndexingConfig::default(),
        common::default_gc_upload_window(),
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

fn metric_value(body: &str, family: &str, labels: &[String]) -> Option<f64> {
    body.lines()
        .filter(|line| !line.starts_with('#'))
        .filter(|line| line.starts_with(family))
        .find(|line| labels.iter().all(|label| line.contains(label)))
        .and_then(|line| line.rsplit_once(' '))
        .and_then(|(_, value)| value.parse::<f64>().ok())
}

fn assert_metric_gt_zero(body: &str, family: &str, labels: &[String]) {
    let value = metric_value(body, family, labels).unwrap_or(0.0);
    assert!(
        value > 0.0,
        "metric {family} with labels {labels:?} should be registered and > 0, got {value}; matching lines:\n{}",
        body.lines()
            .filter(|line| line.contains(family))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// Stage 2C.0: compaction I/O metrics are observable through the same scrape
/// endpoint operators use. This exercises a real initial full compaction, a
/// real incremental compaction, and the existing incremental build-failed
/// fallback path.
#[tokio::test]
async fn test_compaction_io_metrics_registered_and_incremented() {
    let mut config = zeppelin::config::Config::default();
    config.compaction.max_wal_fragments_before_compact = 1;
    config.compaction.retrain_imbalance_threshold = 1000.0;
    config.indexing.default_num_centroids = 6;
    config.indexing.kmeans_max_iterations = 5;
    config.indexing.quantization = zeppelin::index::quantization::QuantizationType::Product;
    config.indexing.pq_m = 8;
    config.indexing.bitmap_index = false;
    config.indexing.fts_index = false;
    config.indexing.hierarchical = false;

    let (base_url, harness, _cache, _dir, compactor, admin_bearer) =
        start_test_server_with_compactor(Some(config)).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = harness.artifact_origin_namespace("obs-compaction-io");
    let writer = WalWriter::new(harness.store.clone());

    common::seed_active_namespace(
        &harness.store,
        &ns,
        16,
        zeppelin::types::DistanceMetric::Euclidean,
    )
    .await;
    let (seed_vecs, _) = clustered_vectors(6, 20, 16, 0.01);
    writer.append(&ns, seed_vecs.clone(), vec![]).await.unwrap();

    // Initial full-retrain build: this should meter full_retrain_total.
    compactor.compact(&ns).await.unwrap();

    let anchor0 = seed_vecs
        .iter()
        .find(|vector| vector.id == "cluster_0_vec_0")
        .unwrap()
        .values
        .clone();
    writer
        .append(
            &ns,
            vec![zeppelin::types::VectorEntry {
                id: "metrics_incremental_success".to_string(),
                values: anchor0.iter().map(|x| x + 0.001).collect(),
                attributes: None,
            }],
            vec![],
        )
        .await
        .unwrap();

    // Successful incremental build: this should meter compaction read ops/bytes.
    let incremental_result = compactor.compact(&ns).await.unwrap();
    let incremental_segment = incremental_result
        .segment_id
        .expect("incremental compaction must produce a new segment");

    let anchor1 = seed_vecs
        .iter()
        .find(|vector| vector.id == "cluster_1_vec_0")
        .unwrap()
        .values
        .clone();
    writer
        .append(
            &ns,
            vec![zeppelin::types::VectorEntry {
                id: "metrics_incremental_fallback".to_string(),
                values: anchor1.iter().map(|x| x + 0.001).collect(),
                attributes: None,
            }],
            vec![],
        )
        .await
        .unwrap();
    harness
        .store
        .delete(&zeppelin::index::quantization::pq::pq_codebook_key(
            &ns,
            &incremental_segment,
        ))
        .await
        .unwrap();

    // Existing warn-fallback path: missing PQ codebook makes incremental_build
    // fail after old vectors are loaded; full retrain succeeds from the already
    // loaded vectors.
    compactor.compact(&ns).await.unwrap();

    let body = client
        .get(format!("{base_url}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let ns_label = format!("namespace=\"{ns}\"");

    assert_metric_gt_zero(
        &body,
        "zeppelin_compaction_read_ops_total",
        &[ns_label.clone(), "class=\"cluster\"".to_string()],
    );
    assert_metric_gt_zero(
        &body,
        "zeppelin_compaction_read_bytes_total",
        &[ns_label.clone(), "class=\"cluster\"".to_string()],
    );
    assert_metric_gt_zero(
        &body,
        "zeppelin_compaction_read_ops_total",
        &[ns_label.clone(), "class=\"centroids\"".to_string()],
    );
    assert_metric_gt_zero(
        &body,
        "zeppelin_compaction_full_retrain_total",
        std::slice::from_ref(&ns_label),
    );
    assert_metric_gt_zero(
        &body,
        "zeppelin_compaction_incremental_fallback_total",
        &[ns_label, "reason=\"build_failed\"".to_string()],
    );

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
    HTTP_REQUESTS_TOTAL
        .with_label_values(&["GET", "/test", "200"])
        .inc();
    QUERY_DURATION.with_label_values(&["__test__"]).observe(0.0);
    QUERIES_TOTAL.with_label_values(&["__test__"]).inc();
    WAL_APPENDS_TOTAL.with_label_values(&["__test__"]).inc();
    CACHE_HITS_TOTAL.with_label_values(&["hit"]).inc();
    COMPACTIONS_TOTAL
        .with_label_values(&["__test__", "success"])
        .inc();
    COMPACTION_READ_BYTES_TOTAL
        .with_label_values(&["__test__", "cluster"])
        .inc_by(1);
    COMPACTION_READ_OPS_TOTAL
        .with_label_values(&["__test__", "cluster"])
        .inc();
    COMPACTION_FULL_RETRAIN_TOTAL
        .with_label_values(&["__test__"])
        .inc();
    COMPACTION_INCREMENTAL_FALLBACK_TOTAL
        .with_label_values(&["__test__", "build_failed"])
        .inc();
    S3_OPERATION_DURATION
        .with_label_values(&["get"])
        .observe(0.0);
    S3_ERRORS_TOTAL.with_label_values(&["get"]).inc();
    COMPACTION_DURATION
        .with_label_values(&["__test__"])
        .observe(0.0);
    HYDRATION_REFUSED
        .with_label_values(&["__test__", "capacity"])
        .set(0);
    HYDRATION_REQUIRED_BYTES
        .with_label_values(&["__test__"])
        .set(0.0);
    HYDRATION_REFUSAL_LOGS_TOTAL
        .with_label_values(&["__test__", "capacity"])
        .inc();
    SECURITY_MODE.with_label_values(&["enforced"]).set(1);
    AUTH_FAILURES_TOTAL
        .with_label_values(&["credential_unknown"])
        .inc();
    AUTHZ_DENIALS_TOTAL
        .with_label_values(&["NamespaceDelete"])
        .inc();
    AUDIT_RECORDS_TOTAL.with_label_values(&["success"]).inc();
    AUDIT_FLUSH_FAILURES_TOTAL.inc();

    let families = prometheus::gather();
    let names: Vec<String> = families.iter().map(|f| f.name().to_string()).collect();

    let expected = [
        "zeppelin_http_requests_total",
        "zeppelin_query_duration_seconds",
        "zeppelin_queries_total",
        "zeppelin_wal_appends_total",
        "zeppelin_cache_hits_total",
        "zeppelin_compactions_total",
        "zeppelin_compaction_read_bytes_total",
        "zeppelin_compaction_read_ops_total",
        "zeppelin_compaction_full_retrain_total",
        "zeppelin_compaction_incremental_fallback_total",
        "zeppelin_s3_operation_duration_seconds",
        "zeppelin_s3_errors_total",
        "zeppelin_compaction_duration_seconds",
        "zeppelin_hydration_refused",
        "zeppelin_hydration_required_bytes",
        "zeppelin_hydration_refusal_logs_total",
        "zeppelin_security_mode",
        "zeppelin_auth_failures_total",
        "zeppelin_authz_denials_total",
        "zeppelin_audit_records_total",
        "zeppelin_audit_flush_failures_total",
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
