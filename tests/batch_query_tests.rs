mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::counting::{counting_store, ArtifactClass, GetCounter};
use common::harness::TestHarness;
use common::server::{cleanup_ns, create_ns_api_with};
use dashmap::DashMap;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use zeppelin::cache::manifest_cache::ManifestCache;
use zeppelin::cache::DiskCache;
use zeppelin::compaction::Compactor;
use zeppelin::config::{Config, IndexingConfig};
use zeppelin::fts::wal_cache::WalFtsCache;
use zeppelin::namespace::NamespaceManager;
use zeppelin::runtime_config::{QueryKnobBounds, RuntimeQueryConfig};
use zeppelin::server::{build_router, AppState};
use zeppelin::storage::ZeppelinStore;
use zeppelin::wal::{WalReader, WalWriter};

struct BatchApiServer {
    base_url: String,
    harness: TestHarness,
    store: ZeppelinStore,
    counter: Option<GetCounter>,
    compactor: Arc<Compactor>,
    _cache_dir: tempfile::TempDir,
}

async fn start_batch_server(config: Config, counted: bool) -> BatchApiServer {
    zeppelin::metrics::init();

    let harness = TestHarness::new().await;
    let (store, counter) = if counted {
        let (store, counter) = counting_store(&harness.store);
        (store, Some(counter))
    } else {
        (harness.store.clone(), None)
    };

    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );
    let runtime_query_config = Arc::new(RuntimeQueryConfig::from_config(&config));
    let query_knob_bounds = QueryKnobBounds::from_config(&config);
    let compactor = Arc::new(Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        config.compaction.clone(),
        config.indexing.clone(),
    ));

    let app = build_router(AppState {
        store: store.clone(),
        namespace_manager: Arc::new(NamespaceManager::new(store.clone())),
        wal_writer: Arc::new(WalWriter::new(store.clone())),
        wal_reader: Arc::new(WalReader::new(store.clone())),
        query_semaphore: Arc::new(tokio::sync::Semaphore::new(
            config.server.max_concurrent_queries,
        )),
        config: Arc::new(config),
        runtime_query_config,
        query_knob_bounds,
        cache,
        manifest_cache: Arc::new(ManifestCache::new(Duration::from_secs(60))),
        fts_cache: Arc::new(WalFtsCache::new()),
        rate_limiters: Arc::new(DashMap::new()),
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    BatchApiServer {
        base_url,
        harness,
        store,
        counter,
        compactor,
        _cache_dir: cache_dir,
    }
}

fn batch_fixture_config() -> Config {
    let mut config = Config::load(None).unwrap();
    config.server.rate_limit_rps = 1_000_000;
    config.server.rate_limit_burst = 1_000_000;
    config.indexing = IndexingConfig {
        default_num_centroids: 4,
        default_nprobe: 4,
        bitmap_index: false,
        quantization: zeppelin::index::quantization::QuantizationType::None,
        ..Default::default()
    };
    config
}

async fn upsert(client: &reqwest::Client, base_url: &str, ns: &str, body: Value) {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "upsert failed: {}",
        resp.text().await.unwrap()
    );
}

async fn single_query(client: &reqwest::Client, base_url: &str, ns: &str, body: Value) -> Value {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "query failed: {}",
        resp.text().await.unwrap()
    );
    resp.json().await.unwrap()
}

fn ids(response: &Value) -> Vec<String> {
    response["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|result| result["id"].as_str().unwrap().to_string())
        .collect()
}

fn batch_entry_ids(entry: &Value) -> Vec<String> {
    ids(&entry["response"])
}

#[tokio::test]
async fn batch_query_returns_positional_successes_and_errors() {
    let server = start_batch_server(batch_fixture_config(), false).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &server.base_url,
        json!({"dimensions": 4, "distance_metric": "euclidean"}),
    )
    .await;

    upsert(
        &client,
        &server.base_url,
        &ns,
        json!({
            "vectors": [
                {"id": "near", "values": [0.0, 0.0, 0.0, 0.0], "attributes": {"kind": "ok"}},
                {"id": "far", "values": [9.0, 0.0, 0.0, 0.0], "attributes": {"kind": "ok"}}
            ]
        }),
    )
    .await;

    let resp = client
        .post(format!(
            "{}/v1/namespaces/{ns}/query/batch",
            server.base_url
        ))
        .json(&json!({
            "queries": [
                {"vector": [0.0, 0.0, 0.0, 0.0], "top_k": 1},
                {"vector": [0.0, 0.0], "top_k": 1},
                {"vector": [9.0, 0.0, 0.0, 0.0], "top_k": 1}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "batch query failed: {}",
        resp.text().await.unwrap()
    );
    let body: Value = resp.json().await.unwrap();
    let entries = body["results"].as_array().unwrap();
    assert_eq!(entries.len(), 3);

    assert_eq!(entries[0]["ok"], true);
    assert_eq!(batch_entry_ids(&entries[0]), vec!["near"]);
    assert!(entries[0]["metadata"]["latency_ms"].is_u64());

    assert_eq!(entries[1]["ok"], false);
    assert_eq!(entries[1]["error"]["code"], "DIMENSION_MISMATCH");
    assert_eq!(entries[1]["error"]["status"], 400);
    assert!(entries[1]["metadata"]["latency_ms"].is_u64());

    assert_eq!(entries[2]["ok"], true);
    assert_eq!(batch_entry_ids(&entries[2]), vec!["far"]);

    cleanup_ns(&server.store, &ns).await;
    server.harness.cleanup().await;
}

#[tokio::test]
async fn batch_query_size_cap_returns_413() {
    let server = start_batch_server(batch_fixture_config(), false).await;
    let client = reqwest::Client::new();
    let queries: Vec<Value> = (0..257)
        .map(|_| json!({"vector": [0.0, 0.0, 0.0, 0.0]}))
        .collect();

    let resp = client
        .post(format!(
            "{}/v1/namespaces/missing/query/batch",
            server.base_url
        ))
        .json(&json!({ "queries": queries }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "PAYLOAD_TOO_LARGE");

    server.harness.cleanup().await;
}

#[tokio::test]
async fn batch_query_reuses_segment_setup_gets() {
    let server = start_batch_server(batch_fixture_config(), true).await;
    let counter = server.counter.as_ref().unwrap();
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &server.base_url,
        json!({"dimensions": 4, "distance_metric": "euclidean"}),
    )
    .await;

    upsert(
        &client,
        &server.base_url,
        &ns,
        json!({
            "vectors": [
                {"id": "v0", "values": [0.0, 0.0, 0.0, 0.0], "attributes": {"group": "a"}},
                {"id": "v1", "values": [1.0, 0.0, 0.0, 0.0], "attributes": {"group": "a"}},
                {"id": "v2", "values": [2.0, 0.0, 0.0, 0.0], "attributes": {"group": "a"}},
                {"id": "v3", "values": [3.0, 0.0, 0.0, 0.0], "attributes": {"group": "a"}},
                {"id": "v4", "values": [4.0, 0.0, 0.0, 0.0], "attributes": {"group": "a"}},
                {"id": "v5", "values": [5.0, 0.0, 0.0, 0.0], "attributes": {"group": "a"}},
                {"id": "v6", "values": [6.0, 0.0, 0.0, 0.0], "attributes": {"group": "a"}},
                {"id": "v7", "values": [7.0, 0.0, 0.0, 0.0], "attributes": {"group": "a"}}
            ]
        }),
    )
    .await;
    server.compactor.compact(&ns).await.unwrap();

    let queries = vec![
        json!({"vector": [0.0, 0.0, 0.0, 0.0], "top_k": 2, "nprobe": 4, "consistency": "strong"}),
        json!({"vector": [1.0, 0.0, 0.0, 0.0], "top_k": 2, "nprobe": 4, "consistency": "strong"}),
        json!({"vector": [2.0, 0.0, 0.0, 0.0], "top_k": 2, "nprobe": 4, "consistency": "strong"}),
    ];

    counter.reset();
    let mut single_gets = 0;
    for query in &queries {
        let body = single_query(&client, &server.base_url, &ns, query.clone()).await;
        assert_eq!(body["scanned_segments"], 1);
        single_gets += counter.total_gets();
        counter.reset();
    }

    let resp = client
        .post(format!(
            "{}/v1/namespaces/{ns}/query/batch",
            server.base_url
        ))
        .json(&json!({ "queries": queries }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "batch query failed: {}",
        resp.text().await.unwrap()
    );
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["results"].as_array().unwrap().len(), 3);
    let batch_gets = counter.total_gets();

    assert!(
        batch_gets < single_gets,
        "batch GETs ({batch_gets}) must be lower than repeated single-query GETs ({single_gets}); profile:\n{}",
        counter.report()
    );
    assert_eq!(
        counter.gets_for(ArtifactClass::Manifest),
        1,
        "batch should perform one manifest freshness read"
    );
    assert!(
        counter.gets_for(ArtifactClass::Centroids) + counter.gets_for(ArtifactClass::Bootstrap)
            <= 1,
        "batch should load segment metadata at most once; profile:\n{}",
        counter.report()
    );

    cleanup_ns(&server.store, &ns).await;
    server.harness.cleanup().await;
}

#[tokio::test]
async fn batch_query_rate_limit_counts_entries() {
    let mut config = batch_fixture_config();
    config.server.rate_limit_rps = 0;
    config.server.rate_limit_burst = 3;
    let server = start_batch_server(config, false).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!(
            "{}/v1/namespaces/missing/query/batch",
            server.base_url
        ))
        .json(&json!({
            "queries": [
                {"vector": [0.0, 0.0, 0.0, 0.0], "top_k": 1},
                {"vector": [0.0, 0.0, 0.0, 0.0], "top_k": 1},
                {"vector": [0.0, 0.0, 0.0, 0.0], "top_k": 1}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "first batch should consume exactly three tokens and report per-entry namespace errors"
    );
    let body: Value = resp.json().await.unwrap();
    for entry in body["results"].as_array().unwrap() {
        assert_eq!(entry["ok"], false);
        assert_eq!(entry["error"]["code"], "NAMESPACE_NOT_FOUND");
    }

    let resp = client
        .post(format!("{}/v1/namespaces/missing/query", server.base_url))
        .json(&json!({"vector": [0.0, 0.0, 0.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 429);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "RATE_LIMITED");

    server.harness.cleanup().await;
}
