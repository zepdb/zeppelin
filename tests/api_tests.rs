mod common;

use common::counting::{counting_store, ArtifactClass};
use common::harness::TestHarness;
use common::server::{
    cleanup_ns, create_ns_api, create_ns_api_fts, create_ns_api_with, start_test_server,
    start_test_server_on_store, start_test_server_with_compactor,
};
use common::vectors::random_vectors;

#[tokio::test]
async fn test_health_check() {
    let (base_url, harness, _admin_bearer) = start_test_server().await;

    let resp = reqwest::get(format!("{base_url}/healthz")).await.unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");

    harness.cleanup().await;
}

#[tokio::test]
async fn test_create_namespace_returns_uuid_and_warning() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);

    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({ "dimensions": 64 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    let body: serde_json::Value = resp.json().await.unwrap();

    // Name should be a valid UUID v4
    let name = body["name"].as_str().unwrap();
    assert!(
        uuid::Uuid::parse_str(name).is_ok(),
        "expected valid UUID, got: {name}"
    );

    // Warning should be present
    let warning = body["warning"].as_str().unwrap();
    assert!(
        warning.contains("Save"),
        "expected save warning, got: {warning}"
    );

    // Standard fields should be present
    assert_eq!(body["dimensions"], 64);
    assert!(body["created_at"].is_string());

    cleanup_ns(&harness.store, name).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_list_namespaces_disabled() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);

    let resp = client
        .get(format!("{base_url}/v1/namespaces"))
        .send()
        .await
        .unwrap();
    // Only POST is routed on /v1/namespaces, GET should return 405
    assert_eq!(resp.status(), 405);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_namespace_crud() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 64).await;

    // Get
    let resp = client
        .get(format!("{base_url}/v1/namespaces/{ns}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["name"], ns);
    assert_eq!(body["dimensions"], 64);

    // Delete
    let resp = client
        .delete(format!("{base_url}/v1/namespaces/{ns}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);

    // Verify deleted
    let mut deleted = false;
    for _ in 0..50 {
        let resp = client
            .get(format!("{base_url}/v1/namespaces/{ns}"))
            .send()
            .await
            .unwrap();
        if resp.status() == 404 {
            deleted = true;
            break;
        }
        assert_eq!(resp.status(), 200);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(deleted, "namespace {ns} did not reach 404 after DELETE 202");

    harness.cleanup().await;
}

#[tokio::test]
async fn test_snapshot_crud_pins_current_generation() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 8).await;

    let created = client
        .put(format!("{base_url}/v1/namespaces/{ns}/snapshots/daily"))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    let created_body: serde_json::Value = created.json().await.unwrap();
    assert_eq!(created_body["name"], "daily");
    assert_eq!(created_body["generation"], 1);
    assert!(created_body["created_at"].is_string());

    let repeated = client
        .put(format!("{base_url}/v1/namespaces/{ns}/snapshots/daily"))
        .send()
        .await
        .unwrap();
    assert_eq!(repeated.status(), 201);
    let repeated_body: serde_json::Value = repeated.json().await.unwrap();
    assert_eq!(repeated_body["generation"], 1);
    assert_eq!(repeated_body["created_at"], created_body["created_at"]);

    let list = client
        .get(format!("{base_url}/v1/namespaces/{ns}/snapshots"))
        .send()
        .await
        .unwrap();
    assert_eq!(list.status(), 200);
    let list_body: serde_json::Value = list.json().await.unwrap();
    assert_eq!(list_body["snapshots"].as_array().unwrap().len(), 1);
    assert_eq!(list_body["snapshots"][0]["name"], "daily");

    let one = client
        .get(format!("{base_url}/v1/namespaces/{ns}/snapshots/daily"))
        .send()
        .await
        .unwrap();
    assert_eq!(one.status(), 200);
    let one_body: serde_json::Value = one.json().await.unwrap();
    assert_eq!(one_body, created_body);

    let vectors = random_vectors(1, 8);
    let upsert = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(upsert.status(), 200);

    let conflict = client
        .put(format!("{base_url}/v1/namespaces/{ns}/snapshots/daily"))
        .send()
        .await
        .unwrap();
    assert_eq!(conflict.status(), 409);
    let conflict_body: serde_json::Value = conflict.json().await.unwrap();
    assert_eq!(conflict_body["code"], "SNAPSHOT_ALREADY_EXISTS");

    let deleted = client
        .delete(format!("{base_url}/v1/namespaces/{ns}/snapshots/daily"))
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 204);

    let missing = client
        .get(format!("{base_url}/v1/namespaces/{ns}/snapshots/daily"))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_vector_upsert() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 16).await;

    // Upsert vectors
    let vectors = random_vectors(5, 16);
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["upserted"], 5);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_nprobe_above_cluster_count_matches_probe_all() {
    let mut config = zeppelin::config::Config::default();
    config.indexing.default_num_centroids = 4;
    config.indexing.default_nprobe = 1;
    config.indexing.max_nprobe = 256;

    let (base_url, harness, _cache, cache_dir, compactor, admin_bearer) =
        start_test_server_with_compactor(Some(config)).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_with(
        &client,
        &base_url,
        serde_json::json!({
            "dimensions": 4,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    let vectors = random_vectors(64, 4);
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    compactor.compact(&ns).await.unwrap();

    let all_clusters = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": [0.0, 0.0, 0.0, 0.0],
            "top_k": 10,
            "consistency": "strong",
            "nprobe": 4,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(all_clusters.status(), 200);
    let all_body: serde_json::Value = all_clusters.json().await.unwrap();
    assert_eq!(all_body["scanned_segments"].as_u64(), Some(1));
    assert_eq!(all_body["scanned_fragments"].as_u64(), Some(0));

    let over_cluster_count = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": [0.0, 0.0, 0.0, 0.0],
            "top_k": 10,
            "consistency": "strong",
            "nprobe": 200,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(over_cluster_count.status(), 200);
    let over_body: serde_json::Value = over_cluster_count.json().await.unwrap();

    assert_eq!(
        over_body["results"], all_body["results"],
        "nprobe above the segment cluster count must behave as probing all clusters"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
    drop(cache_dir);
}

#[tokio::test]
async fn test_get_namespace_reports_manifest_stats_after_upsert() {
    let harness = TestHarness::new().await;
    let (store, _counter) = counting_store(&harness.store);
    let (base_url, _cache, cache_dir, admin_bearer) =
        start_test_server_on_store(&harness, store, Some(harness.prefix.clone())).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 16).await;

    let vectors = random_vectors(7, 16);
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client
        .get(format!("{base_url}/v1/namespaces/{ns}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(body["name"], ns);
    assert_eq!(
        body["vector_count"], 7,
        "GET namespace must derive vector_count from the manifest after upsert"
    );
    assert_eq!(body["uncompacted_fragments"], 1);
    assert_eq!(body["segment_count"], 0);
    assert!(
        body["approximate_storage_bytes"].as_u64().unwrap() > 0,
        "WAL fragment size_bytes must contribute to approximate storage bytes"
    );
    assert_eq!(body["quantization"], serde_json::Value::Null);
    assert_eq!(body["distance_metric"], "cosine");
    assert_eq!(body["index_kind"], "ivf_flat");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
    drop(cache_dir);
}

#[tokio::test]
async fn test_get_namespace_stats_reuses_manifest_freshness_get_without_listing_or_head() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let (base_url, _cache, cache_dir, admin_bearer) =
        start_test_server_on_store(&harness, store, Some(harness.prefix.clone())).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 8).await;

    let vectors = random_vectors(3, 8);
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    counter.reset();
    let resp = client
        .get(format!("{base_url}/v1/namespaces/{ns}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["vector_count"], 3);

    assert_eq!(
        counter.gets_for(ArtifactClass::Manifest),
        1,
        "strong namespace stats read should perform exactly the manifest freshness GET"
    );
    assert_eq!(
        counter.gets_for(ArtifactClass::Wal),
        0,
        "stats must not read WAL fragments"
    );
    assert_eq!(
        counter.gets_for(ArtifactClass::Cluster),
        0,
        "stats must not read segment objects"
    );
    assert_eq!(
        counter.heads_matching(&ns),
        0,
        "stats must not HEAD namespace objects"
    );
    assert_eq!(
        counter.list_calls_for_prefix(&format!("{ns}/")),
        0,
        "stats must not recursively list namespace objects"
    );
    assert_eq!(
        counter.delimiter_list_calls_for_prefix(&format!("{ns}/")),
        0,
        "stats must not delimiter-list namespace objects"
    );
    assert_eq!(
        counter.total_heads(),
        0,
        "stats must not issue any HEAD requests"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
    drop(cache_dir);
}

#[tokio::test]
async fn test_dimension_mismatch_400() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 16).await;

    // Upsert with wrong dimension (32 instead of 16)
    let vectors = random_vectors(1, 32);
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_query_basic_wal_scan() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 8).await;

    // Upsert 10 vectors
    let vectors = random_vectors(10, 8);
    let query_vec = vectors[0].values.clone();

    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();

    // Query with the first vector — it should be the top result (distance ~0)
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 5,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0]["id"], "vec_0");
    assert!(body["scanned_fragments"].as_u64().unwrap() > 0);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_query_algebra_single_ann_source_matches_legacy() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 8).await;

    let vectors = random_vectors(10, 8);
    let query_vec = vectors[0].values.clone();

    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();

    let legacy = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec.clone(),
            "top_k": 5,
            "include_attributes": false,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(legacy.status(), 200);
    let legacy_body: serde_json::Value = legacy.json().await.unwrap();

    let algebra = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "sources": [{
                "type": "ann",
                "vector": query_vec
            }],
            "top_k": 5,
            "projection": {
                "include_attributes": false
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(algebra.status(), 200);
    let algebra_body: serde_json::Value = algebra.json().await.unwrap();

    assert_eq!(algebra_body, legacy_body);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_query_algebra_ann_source_can_use_stored_seed_id() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_with(
        &client,
        &base_url,
        serde_json::json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    let vectors = serde_json::json!({
        "vectors": [
            {
                "id": "seed",
                "values": [0.0, 0.0],
                "attributes": {"tenant": "red", "kind": "seed"}
            },
            {
                "id": "near",
                "values": [0.1, 0.0],
                "attributes": {"tenant": "red", "kind": "neighbor"}
            },
            {
                "id": "far",
                "values": [3.0, 0.0],
                "attributes": {"tenant": "red", "kind": "neighbor"}
            },
            {
                "id": "filtered",
                "values": [0.05, 0.0],
                "attributes": {"tenant": "blue", "kind": "neighbor"}
            }
        ]
    });
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&vectors)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let raw = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "sources": [{
                "type": "ann",
                "vector": [0.0, 0.0],
                "nprobe": 4
            }],
            "top_k": 3,
            "filter": {"op": "eq", "field": "tenant", "value": "red"},
            "consistency": "strong",
            "projection": {"include_attributes": true}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(raw.status(), 200);
    let raw_body: serde_json::Value = raw.json().await.unwrap();
    let expected: Vec<serde_json::Value> = raw_body["results"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|result| result["id"] != "seed")
        .cloned()
        .collect();

    let by_id = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "sources": [{
                "type": "ann",
                "id": "seed",
                "nprobe": 4
            }],
            "top_k": 2,
            "filter": {"op": "eq", "field": "tenant", "value": "red"},
            "consistency": "strong",
            "projection": {"include_attributes": true}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(by_id.status(), 200);
    let by_id_body: serde_json::Value = by_id.json().await.unwrap();

    assert_eq!(by_id_body["results"], serde_json::json!(expected));
    let ids: Vec<&str> = by_id_body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|result| result["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["near", "far"]);
    assert!(by_id_body["results"][0]["attributes"].is_object());

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_query_algebra_ann_seed_id_missing_or_deleted_is_404() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_with(
        &client,
        &base_url,
        serde_json::json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [
                {"id": "live", "values": [0.0, 0.0]},
                {"id": "deleted", "values": [1.0, 0.0]}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client
        .delete(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "ids": ["deleted"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    for seed_id in ["missing", "deleted"] {
        let resp = client
            .post(format!("{base_url}/v1/namespaces/{ns}/query"))
            .json(&serde_json::json!({
                "sources": [{
                    "type": "ann",
                    "id": seed_id
                }],
                "top_k": 1,
                "consistency": "strong"
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["code"], "VECTOR_NOT_FOUND");
        assert!(body["error"].as_str().unwrap().contains(seed_id));
    }

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_query_algebra_ann_source_rejects_id_and_vector_together() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);

    let resp = client
        .post(format!("{base_url}/v1/namespaces/missing/query"))
        .json(&serde_json::json!({
            "sources": [{
                "type": "ann",
                "id": "seed",
                "vector": [0.0, 0.0]
            }],
            "top_k": 1
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "VALIDATION_ERROR");
    assert!(body["error"]
        .as_str()
        .unwrap()
        .contains("exactly one of 'vector' or 'id'"));

    harness.cleanup().await;
}

#[tokio::test]
async fn test_query_algebra_single_bm25_source_matches_legacy() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_fts(
        &client,
        &base_url,
        4,
        serde_json::json!({
            "content": {
                "stemming": false,
                "remove_stopwords": false
            }
        }),
    )
    .await;

    let docs = serde_json::json!({
        "vectors": [
            {
                "id": "doc-rust",
                "values": [0.1, 0.2, 0.3, 0.4],
                "attributes": {"content": "rust programming systems"}
            },
            {
                "id": "doc-cooking",
                "values": [0.4, 0.3, 0.2, 0.1],
                "attributes": {"content": "recipe cooking kitchen"}
            }
        ]
    });
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&docs)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let legacy = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["content", "BM25", "rust programming"],
            "top_k": 2,
            "consistency": "strong",
            "include_attributes": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(legacy.status(), 200);
    let legacy_body: serde_json::Value = legacy.json().await.unwrap();

    let algebra = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "sources": [{
                "type": "bm25",
                "rank_by": ["content", "BM25", "rust programming"]
            }],
            "top_k": 2,
            "consistency": "strong",
            "projection": {
                "include_attributes": false
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(algebra.status(), 200);
    let algebra_body: serde_json::Value = algebra.json().await.unwrap();

    assert_eq!(algebra_body, legacy_body);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_query_with_filter() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 4).await;

    // Upsert vectors with attributes
    let vectors = serde_json::json!({
        "vectors": [
            {"id": "v1", "values": [1.0, 0.0, 0.0, 0.0], "attributes": {"category": "a"}},
            {"id": "v2", "values": [0.9, 0.1, 0.0, 0.0], "attributes": {"category": "b"}},
            {"id": "v3", "values": [0.8, 0.2, 0.0, 0.0], "attributes": {"category": "a"}},
        ]
    });
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&vectors)
        .send()
        .await
        .unwrap();

    // Query with filter for category=a
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 10,
            "filter": {"op": "eq", "field": "category", "value": "a"},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    // Should only contain v1 and v3 (category=a)
    assert_eq!(results.len(), 2);
    let ids: Vec<&str> = results.iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert!(ids.contains(&"v1"));
    assert!(ids.contains(&"v3"));

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_query_empty_namespace() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 4).await;

    // Query empty namespace
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

    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(results.is_empty());

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_query_dimension_mismatch() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api(&client, &base_url, 4).await;

    // Query with wrong dimension
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": [1.0, 0.0],
            "top_k": 5,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
