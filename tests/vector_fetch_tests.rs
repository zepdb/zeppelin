mod common;

use common::counting::{counting_store, ArtifactClass};
use common::harness::TestHarness;
use common::server::{
    cleanup_ns, create_ns_api_with, start_test_server, start_test_server_on_store,
};
use zeppelin::compaction::Compactor;
use zeppelin::config::{Config, IndexingConfig};
use zeppelin::wal::WalReader;

async fn create_namespace(client: &reqwest::Client, base_url: &str) -> String {
    create_ns_api_with(
        client,
        base_url,
        serde_json::json!({
            "dimensions": 3,
            "distance_metric": "euclidean"
        }),
    )
    .await
}

async fn upsert(client: &reqwest::Client, base_url: &str, ns: &str, vectors: serde_json::Value) {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

async fn delete_ids(client: &reqwest::Client, base_url: &str, ns: &str, ids: &[&str]) {
    let resp = client
        .delete(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "ids": ids }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
}

async fn fetch(
    client: &reqwest::Client,
    base_url: &str,
    ns: &str,
    body: serde_json::Value,
) -> serde_json::Value {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors/get"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "fetch body: {body}");
    resp.json().await.unwrap()
}

fn compact_fetch_config() -> Config {
    Config {
        indexing: IndexingConfig {
            default_num_centroids: 4,
            kmeans_max_iterations: 10,
            bitmap_index: false,
            ..Default::default()
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn test_vector_get_strong_returns_requested_missing_and_deleted_ids() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_namespace(&client, &base_url).await;

    upsert(
        &client,
        &base_url,
        &ns,
        serde_json::json!([
            {
                "id": "vec-a",
                "values": [1.0, 0.0, 0.0],
                "attributes": {"tenant": "red", "color": "crimson"}
            },
            {
                "id": "vec-b",
                "values": [0.0, 1.0, 0.0],
                "attributes": {"tenant": "blue", "color": "navy"}
            },
            {
                "id": "vec-deleted",
                "values": [0.0, 0.0, 1.0],
                "attributes": {"tenant": "gone", "color": "gray"}
            }
        ]),
    )
    .await;
    delete_ids(&client, &base_url, &ns, &["vec-deleted"]).await;

    let body = fetch(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "ids": ["vec-b", "missing", "vec-deleted", "vec-a"],
            "include_vector": true,
            "include_attributes": true,
            "attribute_fields": ["tenant"],
            "consistency": "strong"
        }),
    )
    .await;

    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["id"], "vec-b");
    assert_eq!(results[0]["values"], serde_json::json!([0.0, 1.0, 0.0]));
    assert_eq!(
        results[0]["attributes"],
        serde_json::json!({"tenant": "blue"})
    );
    assert_eq!(results[1]["id"], "vec-a");
    assert_eq!(results[1]["values"], serde_json::json!([1.0, 0.0, 0.0]));
    assert_eq!(
        results[1]["attributes"],
        serde_json::json!({"tenant": "red"})
    );
    assert_eq!(
        body["missing"],
        serde_json::json!(["missing", "vec-deleted"])
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_vector_get_projection_avoids_omitted_segment_reads() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let (base_url, _cache, _cache_dir, admin_bearer) =
        start_test_server_on_store(&harness, store.clone(), Some(harness.prefix.clone())).await;
    let config = compact_fetch_config();
    let compactor = Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        config.compaction.clone(),
        config.indexing.clone(),
        common::default_gc_upload_window(),
    );
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_namespace(&client, &base_url).await;

    upsert(
        &client,
        &base_url,
        &ns,
        serde_json::json!([
            {
                "id": "vec-a",
                "values": [1.0, 0.0, 0.0],
                "attributes": {"tenant": "red"}
            },
            {
                "id": "vec-b",
                "values": [0.0, 1.0, 0.0],
                "attributes": {"tenant": "blue"}
            },
            {
                "id": "vec-c",
                "values": [0.0, 0.0, 1.0],
                "attributes": {"tenant": "green"}
            }
        ]),
    )
    .await;
    compactor.compact(&ns).await.unwrap();

    counter.reset();
    let id_only = fetch(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "ids": ["vec-a", "vec-b"],
            "include_vector": false,
            "include_attributes": false,
            "consistency": "strong"
        }),
    )
    .await;
    assert_eq!(id_only["results"].as_array().unwrap().len(), 2);
    assert_eq!(counter.gets_for(ArtifactClass::Cluster), 0);
    assert_eq!(counter.gets_for(ArtifactClass::Attrs), 0);

    counter.reset();
    let vectors_only = fetch(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "ids": ["vec-a", "vec-b"],
            "include_vector": true,
            "include_attributes": false,
            "consistency": "strong"
        }),
    )
    .await;
    assert_eq!(vectors_only["results"].as_array().unwrap().len(), 2);
    assert!(counter.gets_for(ArtifactClass::Cluster) > 0);
    assert_eq!(counter.gets_for(ArtifactClass::Attrs), 0);
    assert!(vectors_only["results"][0].get("values").is_some());
    assert!(vectors_only["results"][0].get("attributes").is_none());

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_vector_get_missing_requested_segment_artifact_fails_loud() {
    let harness = TestHarness::new().await;
    let (store, _counter) = counting_store(&harness.store);
    let (base_url, _cache, _cache_dir, admin_bearer) =
        start_test_server_on_store(&harness, store.clone(), Some(harness.prefix.clone())).await;
    let config = compact_fetch_config();
    let compactor = Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        config.compaction.clone(),
        config.indexing.clone(),
        common::default_gc_upload_window(),
    );
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_namespace(&client, &base_url).await;

    upsert(
        &client,
        &base_url,
        &ns,
        serde_json::json!([
            {
                "id": "vec-a",
                "values": [1.0, 0.0, 0.0],
                "attributes": {"tenant": "red"}
            }
        ]),
    )
    .await;
    compactor.compact(&ns).await.unwrap();

    let cluster_keys = store
        .list_prefix(&format!("{ns}/segments/"))
        .await
        .unwrap()
        .into_iter()
        .filter(|key| {
            key.rsplit('/')
                .next()
                .unwrap_or(key)
                .starts_with("cluster_")
        })
        .collect::<Vec<_>>();
    assert!(!cluster_keys.is_empty());
    store.delete(&cluster_keys[0]).await.unwrap();

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors/get"))
        .json(&serde_json::json!({
            "ids": ["vec-a"],
            "include_vector": true,
            "include_attributes": false,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();

    assert!(resp.status().is_server_error());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_ne!(body["code"], "VALIDATION_ERROR");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
