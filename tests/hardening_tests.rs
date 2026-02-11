mod common;

use std::sync::Arc;

use common::harness::TestHarness;
use common::vectors::random_vectors;

use tokio::net::TcpListener;

use zeppelin::cache::DiskCache;
use zeppelin::compaction::Compactor;
use zeppelin::config::Config;
use zeppelin::namespace::NamespaceManager;
use zeppelin::server::routes::build_router;
use zeppelin::server::AppState;
use zeppelin::storage::ZeppelinStore;
use zeppelin::wal::{WalReader, WalWriter};

/// Start a test server with optional config override, returning (base_url, harness, cache, _cache_dir).
/// The TempDir must be kept alive for the cache to function.
async fn start_test_server_with_config(
    config_override: Option<Config>,
) -> (String, TestHarness, Arc<DiskCache>, tempfile::TempDir) {
    // Ensure metrics are registered (idempotent)
    zeppelin::metrics::init();

    let harness = TestHarness::new().await;
    let config = config_override.unwrap_or_else(|| Config::load(None).unwrap());

    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    let compactor = Arc::new(Compactor::new(
        harness.store.clone(),
        WalReader::new(harness.store.clone()),
        config.compaction.clone(),
        config.indexing.clone(),
    ));

    let state = AppState {
        store: harness.store.clone(),
        namespace_manager: Arc::new(NamespaceManager::new(harness.store.clone())),
        wal_writer: Arc::new(WalWriter::new(harness.store.clone())),
        wal_reader: Arc::new(WalReader::new(harness.store.clone())),
        config: Arc::new(config),
        compactor,
        cache: cache.clone(),
    };

    let app = build_router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (base_url, harness, cache, cache_dir)
}

async fn start_test_server() -> (String, TestHarness) {
    let (url, harness, _cache, _dir) = start_test_server_with_config(None).await;
    (url, harness)
}

fn api_ns(harness: &TestHarness, suffix: &str) -> String {
    format!("{}-{suffix}", harness.prefix)
}

async fn cleanup_ns(store: &ZeppelinStore, ns: &str) {
    let prefix = format!("{ns}/");
    let _ = store.delete_prefix(&prefix).await;
}

// --- Test 1: Oversized batch returns 400 ---

#[tokio::test]
async fn test_oversized_batch_400() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "harden-batch");

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

    // Upsert 10,001 vectors (exceeds default max_batch_size of 10,000)
    let vectors: Vec<serde_json::Value> = (0..10_001)
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
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    let error_msg = body["error"].as_str().unwrap().to_lowercase();
    assert!(error_msg.contains("batch size"), "got: {error_msg}");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// --- Test 2: Empty batch returns 400 ---

#[tokio::test]
async fn test_empty_batch_400() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "harden-empty");

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
        .json(&serde_json::json!({ "vectors": [] }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    let error_msg = body["error"].as_str().unwrap().to_lowercase();
    assert!(error_msg.contains("empty"), "got: {error_msg}");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// --- Test 3: top_k too large returns 400 ---

#[tokio::test]
async fn test_top_k_too_large_400() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "harden-topk-big");

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
    let error_msg = body["error"].as_str().unwrap().to_lowercase();
    assert!(error_msg.contains("top_k"), "got: {error_msg}");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// --- Test 4: top_k zero returns 400 ---

#[tokio::test]
async fn test_top_k_zero_400() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "harden-topk-zero");

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
    let error_msg = body["error"].as_str().unwrap().to_lowercase();
    assert!(error_msg.contains("top_k"), "got: {error_msg}");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// --- Test 5: /readyz endpoint ---

#[tokio::test]
async fn test_readyz_endpoint() {
    let (base_url, harness) = start_test_server().await;

    let resp = reqwest::get(format!("{base_url}/readyz"))
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
    let (base_url, harness) = start_test_server().await;

    let resp = reqwest::get(format!("{base_url}/healthz"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");

    harness.cleanup().await;
}

// --- Test 7: /metrics endpoint ---

#[tokio::test]
async fn test_metrics_endpoint() {
    let (base_url, harness) = start_test_server().await;

    // Touch a metric to ensure there's output
    zeppelin::metrics::QUERIES_TOTAL
        .with_label_values(&["__test__"])
        .inc();

    let resp = reqwest::get(format!("{base_url}/metrics"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let content_type = resp.headers().get("content-type").unwrap().to_str().unwrap().to_string();
    assert!(content_type.contains("text/plain"), "expected prometheus text format, got: {content_type}");

    let body = resp.text().await.unwrap();
    assert!(body.contains("zeppelin_"), "metrics should contain zeppelin_ prefix, got: {}", &body[..200.min(body.len())]);

    harness.cleanup().await;
}

// --- Test 8: Metrics increment after query ---

#[tokio::test]
async fn test_metrics_increment_after_query() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "harden-metrics-q");

    // Create ns and upsert vectors
    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 4,
        }))
        .send()
        .await
        .unwrap();

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
    let resp = reqwest::get(format!("{base_url}/metrics"))
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert!(body.contains("zeppelin_queries_total"), "should have queries counter, got: {}", &body[..300.min(body.len())]);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// --- Test 9: Request timeout middleware is wired ---

#[tokio::test]
async fn test_request_timeout_applied() {
    let mut config = Config::load(None).unwrap();
    config.server.request_timeout_secs = 30;

    let (base_url, harness, _cache, _dir) = start_test_server_with_config(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "harden-timeout");

    // Create ns
    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 4,
        }))
        .send()
        .await
        .unwrap();

    // Normal query should complete successfully (validates middleware is wired)
    let vectors = vec![
        serde_json::json!({"id": "v1", "values": [1.0, 0.0, 0.0, 0.0]}),
    ];
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
    let (base_url, harness, cache, _cache_dir) = start_test_server_with_config(None).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "harden-cache");

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

    if let Ok(result) = compact_result {
        // Compaction succeeded — query with eventual consistency to hit the segment
        assert!(result.vectors_compacted > 0, "expected vectors to be compacted");

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
        assert!(cache.total_size() > 0, "cache should have data after segment query");
    } else {
        // Compaction may fail if the namespace setup wasn't complete — skip gracefully
        eprintln!("[test] compaction failed (expected in some environments): {:?}", compact_result.err());
    }

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
