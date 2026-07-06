//! C.0a — lazy final-result attrs loading for IVF/hierarchical search.
//!
//! These tests pin the read-path contract directly at the index layer:
//! unfiltered queries enrich returned results from attrs blobs, but they only
//! fetch attrs for clusters that actually contain the final top-k results.
//! Filtered SQ8 queries keep the existing eager attrs profile because attrs
//! are required before coarse-candidate truncation.

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use common::counting::{counting_store, ArtifactClass, GetCounter};
use common::harness::TestHarness;
use common::server::{cleanup_ns, create_ns_api_fts, create_ns_api_with};
use dashmap::DashMap;
use serde_json::{json, Value};
use tokio::net::TcpListener;

use zeppelin::cache::manifest_cache::ManifestCache;
use zeppelin::cache::DiskCache;
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, Config, IndexingConfig};
use zeppelin::fts::wal_cache::WalFtsCache;
use zeppelin::fts::FtsFieldConfig;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::index::{HierarchicalIndex, IvfFlatIndex, VectorIndex};
use zeppelin::namespace::NamespaceManager;
use zeppelin::runtime_config::{QueryKnobBounds, RuntimeQueryConfig};
use zeppelin::server::{build_router, AppState};
use zeppelin::types::{AttributeValue, DistanceMetric, Filter, SearchResult, VectorEntry};
use zeppelin::wal::{WalReader, WalWriter};

const IVF_PREFIX_ATTRS_GETS: u64 = 4;
const FILTERED_SQ8_ATTRS_GETS: u64 = 5;
const TOP_K: usize = 1;
const NPROBE_ALL: usize = 4;

struct CountingApiServer {
    base_url: String,
    harness: TestHarness,
    counter: GetCounter,
    compactor: Arc<Compactor>,
    _cache: Arc<DiskCache>,
    _cache_dir: tempfile::TempDir,
}

async fn spawn_router(app: Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    format!("http://{addr}")
}

async fn start_counting_api_server(mut config: Config) -> CountingApiServer {
    zeppelin::metrics::init();
    config.server.rate_limit_rps = 1_000_000;
    config.server.rate_limit_burst = 1_000_000;

    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );
    let compactor = Arc::new(Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        config.compaction.clone(),
        config.indexing.clone(),
    ));
    let manifest_cache = Arc::new(ManifestCache::new(Duration::ZERO));
    let runtime_query_config = Arc::new(RuntimeQueryConfig::from_config(&config));
    let query_knob_bounds = QueryKnobBounds::from_config(&config);
    let query_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_queries,
    ));

    let app = build_router(AppState {
        store: store.clone(),
        namespace_manager: Arc::new(NamespaceManager::new(store.clone())),
        namespace_name_prefix: None,
        wal_writer: Arc::new(WalWriter::new(store.clone())),
        wal_reader: Arc::new(WalReader::new(store.clone())),
        config: Arc::new(config),
        runtime_query_config,
        query_knob_bounds,
        cache: cache.clone(),
        manifest_cache,
        hydrator: None,
        fts_cache: Arc::new(WalFtsCache::new()),
        query_semaphore,
        rate_limiters: Arc::new(DashMap::new()),
    });
    let base_url = spawn_router(app).await;

    CountingApiServer {
        base_url,
        harness,
        counter,
        compactor,
        _cache: cache,
        _cache_dir: cache_dir,
    }
}

fn api_projection_config(quantization: QuantizationType) -> Config {
    let mut config = Config::load(None).unwrap();
    config.compaction = CompactionConfig {
        max_wal_fragments_before_compact: 1,
        ..Default::default()
    };
    config.indexing = IndexingConfig {
        default_num_centroids: NPROBE_ALL,
        default_nprobe: NPROBE_ALL,
        max_nprobe: NPROBE_ALL,
        kmeans_max_iterations: 10,
        quantization,
        bitmap_index: false,
        ..Default::default()
    };
    config
}

fn api_fts_config() -> Config {
    let mut config = api_projection_config(QuantizationType::None);
    config.indexing.fts_index = true;
    config
}

fn content_fts_configs() -> HashMap<String, FtsFieldConfig> {
    HashMap::from([(
        "content".to_string(),
        FtsFieldConfig {
            stemming: true,
            remove_stopwords: true,
            ..Default::default()
        },
    )])
}

async fn upsert_json(client: &reqwest::Client, base_url: &str, ns: &str, vectors: Value) {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&json!({ "vectors": vectors }))
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

async fn query_bytes(
    client: &reqwest::Client,
    base_url: &str,
    ns: &str,
    body: Value,
) -> bytes::Bytes {
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
    resp.bytes().await.unwrap()
}

async fn query_json(client: &reqwest::Client, base_url: &str, ns: &str, body: Value) -> Value {
    serde_json::from_slice(&query_bytes(client, base_url, ns, body).await).unwrap()
}

fn result_id_scores(body: &Value) -> Vec<(String, u64)> {
    body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            (
                r["id"].as_str().unwrap().to_string(),
                r["score"].as_f64().unwrap().to_bits(),
            )
        })
        .collect()
}

fn assert_attributes_are_null(body: &Value) {
    for result in body["results"].as_array().unwrap() {
        assert_eq!(
            result.get("attributes"),
            Some(&Value::Null),
            "result attributes should serialize as explicit null: {result}"
        );
    }
}

fn assert_attributes_are_objects(body: &Value) {
    for result in body["results"].as_array().unwrap() {
        assert!(
            result["attributes"].is_object(),
            "default query should include attributes: {result}"
        );
    }
}

fn api_vectors() -> Value {
    json!([
        {
            "id": "api_v0",
            "values": [0.0, 0.0, 0.0, 0.0],
            "attributes": { "tenant": "keep" }
        },
        {
            "id": "api_v1",
            "values": [1.0, 0.0, 0.0, 0.0],
            "attributes": { "tenant": "keep" }
        },
        {
            "id": "api_v2",
            "values": [10.0, 0.0, 0.0, 0.0],
            "attributes": { "tenant": "drop" }
        }
    ])
}

fn bm25_vectors() -> Value {
    json!([
        {
            "id": "bm25_rust_1",
            "values": [0.0, 0.0, 0.0, 0.0],
            "attributes": { "content": "rust rust programming language", "tenant": "hot" }
        },
        {
            "id": "bm25_rust_2",
            "values": [1.0, 0.0, 0.0, 0.0],
            "attributes": { "content": "rust systems programming", "tenant": "cold" }
        },
        {
            "id": "bm25_python",
            "values": [2.0, 0.0, 0.0, 0.0],
            "attributes": { "content": "python data science", "tenant": "hot" }
        },
        {
            "id": "bm25_cooking",
            "values": [3.0, 0.0, 0.0, 0.0],
            "attributes": { "content": "cooking pasta recipe", "tenant": "cold" }
        }
    ])
}

fn attr_map(id: &str, ordinal: i64, tenant: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("doc_id".to_string(), AttributeValue::String(id.to_string())),
        (
            "tenant".to_string(),
            AttributeValue::String(tenant.to_string()),
        ),
        ("ordinal".to_string(), AttributeValue::Integer(ordinal)),
    ])
}

fn ivf_vectors() -> Vec<VectorEntry> {
    [
        ("v0", [1.0, 0.0, 0.0, 0.0], "keep"),
        ("v1", [100.0, 0.0, 0.0, 0.0], "drop"),
        ("v2", [0.0, 100.0, 0.0, 0.0], "drop"),
        ("v3", [0.0, 0.0, 100.0, 0.0], "drop"),
    ]
    .into_iter()
    .enumerate()
    .map(|(i, (id, values, tenant))| VectorEntry {
        id: id.to_string(),
        values: values.to_vec(),
        attributes: Some(attr_map(id, i as i64, tenant)),
    })
    .collect()
}

fn hierarchical_vectors() -> Vec<VectorEntry> {
    let mut vectors = Vec::new();
    let centers = [
        [1.0, 0.0, 0.0, 0.0],
        [100.0, 0.0, 0.0, 0.0],
        [0.0, 100.0, 0.0, 0.0],
        [0.0, 0.0, 100.0, 0.0],
    ];

    for (cluster, center) in centers.into_iter().enumerate() {
        for offset in 0..8 {
            let id = if cluster == 0 && offset == 0 {
                "h0".to_string()
            } else {
                format!("h{cluster}_{offset}")
            };
            let mut values = center.to_vec();
            values[3] = offset as f32 * 0.01;
            vectors.push(VectorEntry {
                id: id.clone(),
                values,
                attributes: Some(attr_map(&id, (cluster * 8 + offset) as i64, "keep")),
            });
        }
    }

    vectors
}

fn attrs_by_id(
    vectors: &[VectorEntry],
) -> HashMap<String, Option<HashMap<String, AttributeValue>>> {
    vectors
        .iter()
        .map(|v| (v.id.clone(), v.attributes.clone()))
        .collect()
}

fn assert_attrs_match_vectors(
    results: &[SearchResult],
    expected: &HashMap<String, Option<HashMap<String, AttributeValue>>>,
) {
    for result in results {
        assert_eq!(
            &result.attributes,
            expected
                .get(&result.id)
                .unwrap_or_else(|| panic!("unexpected result id {}", result.id)),
            "attributes for {} changed",
            result.id
        );
    }
}

fn ivf_config(quantization: QuantizationType) -> IndexingConfig {
    IndexingConfig {
        default_num_centroids: NPROBE_ALL,
        kmeans_max_iterations: 10,
        quantization,
        pq_m: 2,
        bitmap_index: false,
        ..Default::default()
    }
}

#[tokio::test]
async fn ivf_unfiltered_attrs_are_lazy_but_enrichment_is_identical() {
    for quantization in [
        QuantizationType::None,
        QuantizationType::Scalar,
        QuantizationType::Product,
    ] {
        let harness = TestHarness::new().await;
        let (store, counter) = counting_store(&harness.store);
        let ns = harness.key(&format!("ivf-lazy-{quantization:?}"));
        let vectors = ivf_vectors();
        let expected_attrs = attrs_by_id(&vectors);

        let index =
            IvfFlatIndex::build(&vectors, &ivf_config(quantization), &store, &ns, "seg_lazy")
                .await
                .unwrap();

        counter.reset();
        let results = index
            .search(
                &vectors[0].values,
                TOP_K,
                NPROBE_ALL,
                None,
                DistanceMetric::Euclidean,
                &store,
            )
            .await
            .unwrap();

        assert_eq!(results.len(), TOP_K);
        assert_eq!(results[0].id, "v0");
        assert_attrs_match_vectors(&results, &expected_attrs);

        let attrs_gets = counter.gets_for(ArtifactClass::Attrs);
        assert_eq!(
            attrs_gets, 1,
            "unfiltered {quantization:?} search should fetch attrs only for the final top-k cluster",
        );
        assert!(
            attrs_gets < IVF_PREFIX_ATTRS_GETS,
            "unfiltered {quantization:?} search must reduce attrs GETs from the pre-fix eager baseline",
        );

        harness.cleanup().await;
    }
}

#[tokio::test]
async fn ivf_filtered_sq8_attrs_get_count_is_unchanged() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let ns = harness.key("ivf-filtered-sq8-unchanged");
    let vectors = ivf_vectors();
    let expected_attrs = attrs_by_id(&vectors);
    let index = IvfFlatIndex::build(
        &vectors,
        &ivf_config(QuantizationType::Scalar),
        &store,
        &ns,
        "seg_filtered",
    )
    .await
    .unwrap();
    let filter = Filter::Eq {
        field: "tenant".to_string(),
        value: AttributeValue::String("keep".to_string()),
    };

    counter.reset();
    let results = index
        .search(
            &vectors[0].values,
            TOP_K,
            NPROBE_ALL,
            Some(&filter),
            DistanceMetric::Euclidean,
            &store,
        )
        .await
        .unwrap();

    assert_eq!(results.len(), TOP_K);
    assert_eq!(results[0].id, "v0");
    assert_attrs_match_vectors(&results, &expected_attrs);
    assert_eq!(
        counter.gets_for(ArtifactClass::Attrs),
        FILTERED_SQ8_ATTRS_GETS,
        "filtered SQ8 must keep the pre-fix attrs GET profile: four coarse attrs GETs plus one rerank attrs GET",
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn hierarchical_flat_unfiltered_attrs_are_lazy_but_enrichment_is_identical() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let ns = harness.key("hierarchical-flat-lazy");
    let vectors = hierarchical_vectors();
    let expected_attrs = attrs_by_id(&vectors);
    let config = IndexingConfig {
        default_num_centroids: NPROBE_ALL,
        kmeans_max_iterations: 10,
        quantization: QuantizationType::None,
        hierarchical: true,
        leaf_size: Some(8),
        bitmap_index: false,
        ..Default::default()
    };

    let index = HierarchicalIndex::build(&vectors, &config, &store, &ns, "seg_h_lazy")
        .await
        .unwrap();

    counter.reset();
    let results = index
        .search(
            &vectors[0].values,
            TOP_K,
            NPROBE_ALL,
            None,
            DistanceMetric::Euclidean,
            &store,
        )
        .await
        .unwrap();

    assert_eq!(results.len(), TOP_K);
    assert_eq!(results[0].id, "h0");
    assert_attrs_match_vectors(&results, &expected_attrs);

    let attrs_gets = counter.gets_for(ArtifactClass::Attrs);
    assert_eq!(
        attrs_gets, 1,
        "unfiltered hierarchical flat search should fetch attrs only for the final top-k cluster",
    );
    assert!(
        attrs_gets < IVF_PREFIX_ATTRS_GETS,
        "unfiltered hierarchical flat search must reduce attrs GETs from the pre-fix eager baseline",
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn api_include_attributes_absent_matches_explicit_true_response_bytes() {
    let server = start_counting_api_server(api_projection_config(QuantizationType::None)).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &server.base_url,
        json!({ "dimensions": 4, "distance_metric": "euclidean" }),
    )
    .await;
    upsert_json(&client, &server.base_url, &ns, api_vectors()).await;

    let absent = query_bytes(
        &client,
        &server.base_url,
        &ns,
        json!({
            "vector": [0.0, 0.0, 0.0, 0.0],
            "top_k": 2,
            "consistency": "strong"
        }),
    )
    .await;
    let explicit_true = query_bytes(
        &client,
        &server.base_url,
        &ns,
        json!({
            "vector": [0.0, 0.0, 0.0, 0.0],
            "top_k": 2,
            "consistency": "strong",
            "include_attributes": true
        }),
    )
    .await;

    assert_eq!(absent, explicit_true);

    cleanup_ns(&server.harness.store, &ns).await;
    server.harness.cleanup().await;
}

#[tokio::test]
async fn api_include_attributes_false_strips_wal_response_attributes() {
    let server = start_counting_api_server(api_projection_config(QuantizationType::None)).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &server.base_url,
        json!({ "dimensions": 4, "distance_metric": "euclidean" }),
    )
    .await;
    upsert_json(&client, &server.base_url, &ns, api_vectors()).await;

    let default_body = query_json(
        &client,
        &server.base_url,
        &ns,
        json!({
            "vector": [0.0, 0.0, 0.0, 0.0],
            "top_k": 2,
            "consistency": "strong"
        }),
    )
    .await;
    let stripped_body = query_json(
        &client,
        &server.base_url,
        &ns,
        json!({
            "vector": [0.0, 0.0, 0.0, 0.0],
            "top_k": 2,
            "consistency": "strong",
            "include_attributes": false
        }),
    )
    .await;

    assert_eq!(
        result_id_scores(&default_body),
        result_id_scores(&stripped_body)
    );
    assert_attributes_are_objects(&default_body);
    assert_attributes_are_null(&stripped_body);

    cleanup_ns(&server.harness.store, &ns).await;
    server.harness.cleanup().await;
}

#[tokio::test]
async fn api_unfiltered_ann_false_skips_final_attrs_fetches() {
    let server = start_counting_api_server(api_projection_config(QuantizationType::None)).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &server.base_url,
        json!({ "dimensions": 4, "distance_metric": "euclidean" }),
    )
    .await;
    upsert_json(&client, &server.base_url, &ns, api_vectors()).await;
    server.compactor.compact(&ns).await.unwrap();

    server.counter.reset();
    let body = query_json(
        &client,
        &server.base_url,
        &ns,
        json!({
            "vector": [0.0, 0.0, 0.0, 0.0],
            "top_k": 2,
            "nprobe": NPROBE_ALL,
            "consistency": "eventual",
            "include_attributes": false
        }),
    )
    .await;

    assert_eq!(body["scanned_segments"].as_u64(), Some(1));
    assert_eq!(result_id_scores(&body).len(), 2);
    assert_attributes_are_null(&body);
    assert_eq!(
        server.counter.gets_for(ArtifactClass::Attrs),
        0,
        "unfiltered ANN include_attributes=false must skip final attrs enrichment",
    );

    cleanup_ns(&server.harness.store, &ns).await;
    server.harness.cleanup().await;
}

#[tokio::test]
async fn api_filtered_ann_false_keeps_filtering_but_strips_attributes() {
    let server = start_counting_api_server(api_projection_config(QuantizationType::Scalar)).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &server.base_url,
        json!({ "dimensions": 4, "distance_metric": "euclidean" }),
    )
    .await;

    let mut vectors = Vec::new();
    for i in 0..40 {
        vectors.push(json!({
            "id": format!("cold_{i}"),
            "values": [0.1, i as f32 * 0.001, 0.0, 0.0],
            "attributes": { "tenant": "cold" }
        }));
    }
    for i in 0..8 {
        vectors.push(json!({
            "id": format!("hot_{i}"),
            "values": [10.0, i as f32 * 0.001, 0.0, 0.0],
            "attributes": { "tenant": "hot" }
        }));
    }
    upsert_json(&client, &server.base_url, &ns, Value::Array(vectors)).await;
    server.compactor.compact(&ns).await.unwrap();

    server.counter.reset();
    let body = query_json(
        &client,
        &server.base_url,
        &ns,
        json!({
            "vector": [0.0, 0.0, 0.0, 0.0],
            "top_k": 3,
            "nprobe": NPROBE_ALL,
            "consistency": "eventual",
            "include_attributes": false,
            "filter": { "op": "eq", "field": "tenant", "value": "hot" }
        }),
    )
    .await;

    let ids: Vec<String> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids.len(), 3);
    assert!(
        ids.iter().all(|id| id.starts_with("hot_")),
        "filtered query returned non-matching ids: {ids:?}"
    );
    assert_attributes_are_null(&body);
    assert!(
        server.counter.gets_for(ArtifactClass::Attrs) > 0,
        "filtered include_attributes=false queries still need attrs for evaluation"
    );

    cleanup_ns(&server.harness.store, &ns).await;
    server.harness.cleanup().await;
}

#[tokio::test]
async fn api_bm25_unfiltered_false_skips_attrs_gets_but_keeps_cluster_id_reads() {
    let server = start_counting_api_server(api_fts_config()).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_fts(
        &client,
        &server.base_url,
        4,
        json!({
            "content": { "language": "english", "stemming": true, "remove_stopwords": true }
        }),
    )
    .await;
    upsert_json(&client, &server.base_url, &ns, bm25_vectors()).await;
    server
        .compactor
        .compact_with_fts(&ns, None, &content_fts_configs())
        .await
        .unwrap();

    server.counter.reset();
    let default_body = query_json(
        &client,
        &server.base_url,
        &ns,
        json!({
            "rank_by": ["content", "BM25", "rust programming"],
            "top_k": 10,
            "consistency": "eventual"
        }),
    )
    .await;
    let default_cluster_gets = server.counter.gets_for(ArtifactClass::Cluster);

    server.counter.reset();
    let stripped_body = query_json(
        &client,
        &server.base_url,
        &ns,
        json!({
            "rank_by": ["content", "BM25", "rust programming"],
            "top_k": 10,
            "consistency": "eventual",
            "include_attributes": false
        }),
    )
    .await;

    assert_eq!(
        result_id_scores(&default_body),
        result_id_scores(&stripped_body)
    );
    assert_attributes_are_objects(&default_body);
    assert_attributes_are_null(&stripped_body);
    assert_eq!(
        server.counter.gets_for(ArtifactClass::Attrs),
        0,
        "BM25 unfiltered include_attributes=false should not fetch attrs",
    );
    assert_eq!(
        server.counter.gets_for(ArtifactClass::Cluster),
        default_cluster_gets,
        "BM25 still needs the same cluster GETs to resolve ids"
    );

    cleanup_ns(&server.harness.store, &ns).await;
    server.harness.cleanup().await;
}

#[tokio::test]
async fn api_bm25_filtered_wal_false_keeps_filtering_but_strips_attributes() {
    let server = start_counting_api_server(api_fts_config()).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_fts(
        &client,
        &server.base_url,
        4,
        json!({
            "content": { "language": "english", "stemming": true, "remove_stopwords": true }
        }),
    )
    .await;
    upsert_json(&client, &server.base_url, &ns, bm25_vectors()).await;

    server.counter.reset();
    let body = query_json(
        &client,
        &server.base_url,
        &ns,
        json!({
            "rank_by": ["content", "BM25", "rust programming"],
            "top_k": 10,
            "consistency": "strong",
            "include_attributes": false,
            "filter": { "op": "eq", "field": "tenant", "value": "hot" }
        }),
    )
    .await;

    let ids: Vec<String> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, vec!["bm25_rust_1".to_string()]);
    assert!(body["scanned_fragments"].as_u64().unwrap() > 0);
    assert_attributes_are_null(&body);

    cleanup_ns(&server.harness.store, &ns).await;
    server.harness.cleanup().await;
}

#[tokio::test]
async fn api_bm25_filtered_false_keeps_filtering_but_strips_attributes() {
    let server = start_counting_api_server(api_fts_config()).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_fts(
        &client,
        &server.base_url,
        4,
        json!({
            "content": { "language": "english", "stemming": true, "remove_stopwords": true }
        }),
    )
    .await;
    upsert_json(&client, &server.base_url, &ns, bm25_vectors()).await;
    server
        .compactor
        .compact_with_fts(&ns, None, &content_fts_configs())
        .await
        .unwrap();

    server.counter.reset();
    let body = query_json(
        &client,
        &server.base_url,
        &ns,
        json!({
            "rank_by": ["content", "BM25", "rust programming"],
            "top_k": 10,
            "consistency": "eventual",
            "include_attributes": false,
            "filter": { "op": "eq", "field": "tenant", "value": "hot" }
        }),
    )
    .await;

    let ids: Vec<String> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, vec!["bm25_rust_1".to_string()]);
    assert_eq!(body["scanned_segments"].as_u64(), Some(1));
    assert_attributes_are_null(&body);
    assert!(
        server.counter.gets_for(ArtifactClass::Attrs) > 0,
        "BM25 filtered include_attributes=false still needs attrs for filter evaluation"
    );

    cleanup_ns(&server.harness.store, &ns).await;
    server.harness.cleanup().await;
}
