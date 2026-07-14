mod common;

use std::collections::HashMap;

use common::server::{
    cleanup_ns, create_ns_api_fts, create_ns_api_with, start_test_server,
    start_test_server_with_compactor,
};
use serde_json::{json, Value};
use zeppelin::config::Config;
use zeppelin::fts::{FtsFieldConfig, FtsLanguage};

fn ids(body: &Value) -> Vec<String> {
    body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|result| result["id"].as_str().unwrap().to_string())
        .collect()
}

async fn upsert(client: &reqwest::Client, base_url: &str, ns: &str, vectors: Value) {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&vectors)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "upsert failed");
}

async fn query(client: &reqwest::Client, base_url: &str, ns: &str, body: Value) -> Value {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "query failed");
    resp.json().await.unwrap()
}

fn fts_config() -> Config {
    let mut config = zeppelin::config::Config::default();
    config.indexing.fts_index = true;
    config
}

fn content_fts_configs() -> HashMap<String, FtsFieldConfig> {
    HashMap::from([(
        "content".to_string(),
        FtsFieldConfig {
            language: FtsLanguage::English,
            stemming: false,
            remove_stopwords: false,
            k1: 1.2,
            b: 0.75,
            ..Default::default()
        },
    )])
}

fn fts_json() -> Value {
    json!({
        "content": {
            "language": "english",
            "stemming": false,
            "remove_stopwords": false
        }
    })
}

fn ann_vectors() -> Value {
    json!({
        "vectors": [
            {"id": "ann_far", "values": [3.0, 0.0, 0.0, 0.0], "attributes": {"kind": "ann"}},
            {"id": "ann_near_2", "values": [0.2, 0.0, 0.0, 0.0], "attributes": {"kind": "ann"}},
            {"id": "ann_mid", "values": [0.8, 0.0, 0.0, 0.0], "attributes": {"kind": "ann"}},
            {"id": "ann_zero", "values": [0.0, 0.0, 0.0, 0.0], "attributes": {"kind": "ann"}},
            {"id": "ann_near_1", "values": [0.1, 0.0, 0.0, 0.0], "attributes": {"kind": "ann"}}
        ]
    })
}

fn bm25_docs() -> Value {
    json!({
        "vectors": [
            {"id": "bm25_tf1", "values": [1.0, 0.0, 0.0, 0.0], "attributes": {"content": "rust code code code"}},
            {"id": "bm25_tf4", "values": [0.0, 1.0, 0.0, 0.0], "attributes": {"content": "rust rust rust rust"}},
            {"id": "bm25_none", "values": [0.0, 0.0, 1.0, 0.0], "attributes": {"content": "python code code code"}},
            {"id": "bm25_tf2", "values": [0.0, 0.0, 0.0, 1.0], "attributes": {"content": "rust rust code code"}},
            {"id": "bm25_tf3", "values": [0.5, 0.5, 0.0, 0.0], "attributes": {"content": "rust rust rust code"}}
        ]
    })
}

#[tokio::test]
async fn ann_segment_order_pin() {
    let (base_url, harness, _cache, _dir, compactor, admin_bearer) =
        start_test_server_with_compactor(None).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({"dimensions": 4, "distance_metric": "euclidean"}),
    )
    .await;
    upsert(&client, &base_url, &ns, ann_vectors()).await;

    compactor.compact(&ns).await.unwrap();

    let body = query(
        &client,
        &base_url,
        &ns,
        json!({
            "vector": [0.0, 0.0, 0.0, 0.0],
            "top_k": 4,
            "nprobe": 16,
            "consistency": "eventual"
        }),
    )
    .await;

    assert_eq!(
        ids(&body),
        vec!["ann_zero", "ann_near_1", "ann_near_2", "ann_mid"]
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn ann_strong_wal_order_pin() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({"dimensions": 4, "distance_metric": "euclidean"}),
    )
    .await;
    upsert(&client, &base_url, &ns, ann_vectors()).await;

    let body = query(
        &client,
        &base_url,
        &ns,
        json!({
            "vector": [0.0, 0.0, 0.0, 0.0],
            "top_k": 4,
            "nprobe": 16,
            "consistency": "strong"
        }),
    )
    .await;

    assert_eq!(
        ids(&body),
        vec!["ann_zero", "ann_near_1", "ann_near_2", "ann_mid"]
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn bm25_segment_order_pin() {
    let (base_url, harness, _cache, _dir, compactor, admin_bearer) =
        start_test_server_with_compactor(Some(fts_config())).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_fts(&client, &base_url, 4, fts_json()).await;
    upsert(&client, &base_url, &ns, bm25_docs()).await;

    compactor
        .compact_with_fts(&ns, None, &content_fts_configs())
        .await
        .unwrap();

    let body = query(
        &client,
        &base_url,
        &ns,
        json!({
            "rank_by": ["content", "BM25", "rust"],
            "top_k": 4,
            "consistency": "eventual"
        }),
    )
    .await;

    assert_eq!(
        ids(&body),
        vec!["bm25_tf4", "bm25_tf3", "bm25_tf2", "bm25_tf1"]
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn bm25_wal_order_pin() {
    let (base_url, harness, _cache, _dir, _compactor, admin_bearer) =
        start_test_server_with_compactor(Some(fts_config())).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_fts(&client, &base_url, 4, fts_json()).await;
    upsert(&client, &base_url, &ns, bm25_docs()).await;

    let body = query(
        &client,
        &base_url,
        &ns,
        json!({
            "rank_by": ["content", "BM25", "rust"],
            "top_k": 4,
            "consistency": "strong"
        }),
    )
    .await;

    assert_eq!(
        ids(&body),
        vec!["bm25_tf4", "bm25_tf3", "bm25_tf2", "bm25_tf1"]
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
