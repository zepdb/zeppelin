mod common;

use common::server::{
    cleanup_ns, create_ns_api_with, start_test_server, start_test_server_with_compactor,
};
use zeppelin::namespace::manager::NamespaceMetadata;
use zeppelin::wal::Manifest;

#[tokio::test]
async fn test_create_rejects_invalid_pq_params_before_compaction() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);

    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "dimensions": 10,
            "distance_metric": "euclidean",
            "index_config": {
                "nlist": 4,
                "quantization": "product",
                "pq_m": 3
            }
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "VALIDATION_ERROR");
    assert!(body["error"].as_str().unwrap().contains("pq_m"));

    harness.cleanup().await;
}

#[tokio::test]
async fn test_namespace_nlist_override_controls_compacted_cluster_count() {
    let (base_url, harness, _cache, _cache_dir, compactor, admin_bearer) =
        start_test_server_with_compactor(None).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_with(
        &client,
        &base_url,
        serde_json::json!({
            "dimensions": 4,
            "distance_metric": "euclidean",
            "index_config": {
                "nlist": 4,
                "quantization": "none",
                "hierarchical": false,
                "fts_index": false,
                "bitmap_index": false
            }
        }),
    )
    .await;

    let vectors: Vec<_> = (0..24)
        .map(|i| {
            serde_json::json!({
                "id": format!("vec-{i}"),
                "values": [i as f32, 0.0, 0.0, 0.0]
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

    compactor.compact(&ns).await.unwrap();
    let manifest = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    let active_id = manifest.active_segment.as_ref().unwrap();
    let active = manifest
        .segments
        .iter()
        .find(|segment| &segment.id == active_id)
        .unwrap();
    assert_eq!(active.cluster_count, 4);

    let resp = client
        .get(format!("{base_url}/v1/namespaces/{ns}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["index_config"]["nlist"], 4);
    assert_eq!(body["active_segment_vector_count"], 24);
    assert!(body.get("last_compaction_status").is_some());
    assert!(body.get("consecutive_compaction_failures").is_some());

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_patch_index_config_forces_next_compaction_rewrite() {
    let (base_url, harness, _cache, _cache_dir, compactor, admin_bearer) =
        start_test_server_with_compactor(None).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_with(
        &client,
        &base_url,
        serde_json::json!({
            "dimensions": 4,
            "distance_metric": "euclidean",
            "index_config": {
                "nlist": 4,
                "quantization": "none"
            }
        }),
    )
    .await;

    let vectors: Vec<_> = (0..16)
        .map(|i| {
            serde_json::json!({
                "id": format!("patch-vec-{i}"),
                "values": [i as f32, 1.0, 0.0, 0.0]
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

    compactor.compact(&ns).await.unwrap();
    let first = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    let first_active_id = first.active_segment.as_ref().unwrap();
    let first_active = first
        .segments
        .iter()
        .find(|segment| &segment.id == first_active_id)
        .unwrap();
    assert_eq!(first_active.cluster_count, 4);

    let resp = client
        .patch(format!("{base_url}/v1/namespaces/{ns}/index_config"))
        .json(&serde_json::json!({ "nlist": 2 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 202);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "accepted");
    assert_eq!(body["index_config"]["nlist"], 2);

    let manifest = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    let metadata = NamespaceMetadata::from_bytes(
        &harness
            .store
            .get(&NamespaceMetadata::object_store_key(&ns))
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(compactor.should_compact(&ns, &manifest, &metadata).unwrap());
    compactor.compact(&ns).await.unwrap();
    let rewritten = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    let active_id = rewritten.active_segment.as_ref().unwrap();
    let active = rewritten
        .segments
        .iter()
        .find(|segment| &segment.id == active_id)
        .unwrap();
    assert_eq!(active.cluster_count, 2);
    assert_eq!(active.vector_count, 16);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_stale_layout_trigger_cannot_override_fresh_compaction_metadata() {
    let (base_url, harness, _cache, _cache_dir, compactor, admin_bearer) =
        start_test_server_with_compactor(None).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_with(
        &client,
        &base_url,
        serde_json::json!({
            "dimensions": 4,
            "distance_metric": "euclidean",
            "index_config": {
                "nlist": 4,
                "quantization": "none"
            }
        }),
    )
    .await;
    let vectors: Vec<_> = (0..16)
        .map(|i| {
            serde_json::json!({
                "id": format!("stale-trigger-{i}"),
                "values": [i as f32, 1.0, 0.0, 0.0]
            })
        })
        .collect();
    let response = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    compactor.compact(&ns).await.unwrap();

    let response = client
        .patch(format!("{base_url}/v1/namespaces/{ns}/index_config"))
        .json(&serde_json::json!({ "nlist": 2 }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 202);
    let stale_metadata = NamespaceMetadata::from_bytes(
        &harness
            .store
            .get(&NamespaceMetadata::object_store_key(&ns))
            .await
            .unwrap(),
    )
    .unwrap();

    let response = client
        .patch(format!("{base_url}/v1/namespaces/{ns}/index_config"))
        .json(&serde_json::json!({ "nlist": 4 }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 202);

    let before = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert!(compactor
        .should_compact(&ns, &before, &stale_metadata)
        .unwrap());
    let result = compactor.compact(&ns).await.unwrap();
    assert!(result.segment_id.is_none());
    assert_eq!(result.vectors_compacted, 0);
    assert_eq!(result.fragments_removed, 0);

    let after = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(after.version(), before.version());
    assert_eq!(after.active_segment, before.active_segment);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[test]
fn test_namespace_metadata_without_index_config_and_health_decodes() {
    let raw = br#"{
        "name": "legacy",
        "dimensions": 8,
        "distance_metric": "cosine",
        "index_type": "ivf_flat",
        "vector_count": 0,
        "created_at": "2026-07-07T00:00:00Z",
        "updated_at": "2026-07-07T00:00:00Z",
        "state": "active",
        "full_text_search": {}
    }"#;

    let meta = NamespaceMetadata::from_bytes(raw).unwrap();
    assert_eq!(meta.name, "legacy");
    assert!(meta.index_config.is_none());
    assert_eq!(meta.compaction_health.consecutive_failures, 0);
}
