mod common;

use std::collections::BTreeSet;

use bytes::Bytes;
use chrono::{Duration, Utc};
use common::fault_injection::{
    assert_snapshot_on_copy, fail_copy_once_matching, fail_put_once_matching,
};
use common::server::{
    api_ns, cleanup_ns, create_ns_api_with, start_test_server, start_test_server_on_store,
    start_test_server_with_compactor,
};
use reqwest::StatusCode;
use serde_json::{json, Value};
use ulid::Ulid;
use zeppelin::compaction::gc::reachable_keys;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::storage::ZeppelinStore;
use zeppelin::wal::manifest::{FragmentRef, NamedSnapshot};
use zeppelin::wal::Manifest;

async fn upsert(client: &reqwest::Client, base_url: &str, ns: &str, vectors: Value) {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

async fn query_ids(
    client: &reqwest::Client,
    base_url: &str,
    ns: &str,
    vector: [f32; 2],
) -> Vec<String> {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&json!({
            "vector": vector,
            "top_k": 10,
            "nprobe": 2,
            "consistency": "strong",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.json::<Value>().await.unwrap();
    body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|result| result["id"].as_str().unwrap().to_string())
        .collect()
}

async fn clone_namespace(
    client: &reqwest::Client,
    base_url: &str,
    source: &str,
    target: &str,
    as_of: &str,
) -> (StatusCode, Value) {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{source}/clone"))
        .json(&json!({
            "target": target,
            "as_of": as_of,
        }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.json::<Value>().await.unwrap();
    (status, body)
}

async fn wait_namespace_gone(client: &reqwest::Client, base_url: &str, ns: &str) {
    for _ in 0..50 {
        let resp = client
            .get(format!("{base_url}/v1/namespaces/{ns}"))
            .send()
            .await
            .unwrap();
        if resp.status() == StatusCode::NOT_FOUND {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("namespace {ns} did not reach 404 after delete");
}

fn set(ids: Vec<String>) -> BTreeSet<String> {
    ids.into_iter().collect()
}

fn fragment(id: u128) -> FragmentRef {
    FragmentRef {
        id: Ulid::from_parts(76_000, id),
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 16,
        artifact_origin: None,
    }
}

fn manifest_json_bytes_with_version(manifest: &Manifest, version: u64) -> Bytes {
    let mut value = serde_json::to_value(manifest).expect("manifest must serialize");
    value
        .as_object_mut()
        .expect("manifest must serialize as an object")
        .insert("version".to_string(), json!(version));
    Bytes::from(serde_json::to_vec(&value).expect("manifest json must serialize"))
}

async fn rewrite_history_updated_at(
    store: &ZeppelinStore,
    ns: &str,
    manifest: &Manifest,
    updated_at: chrono::DateTime<Utc>,
) {
    let mut rewritten = manifest.clone();
    rewritten.updated_at = updated_at;
    store
        .put(
            &Manifest::history_key(ns, rewritten.version()),
            rewritten.to_bytes().unwrap(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn clone_as_of_ignores_history_generation_ahead_of_live_manifest() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let source = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;
    let target = api_ns(&harness, "orphan-history-target");

    let (mut pending, version) = Manifest::read_versioned(&harness.store, &source)
        .await
        .unwrap()
        .unwrap();
    let live_version = pending.version();
    pending.add_fragment(fragment(1));
    let orphan_generation = live_version + 1;

    let (failing_store, failures) =
        fail_put_once_matching(&harness.store, Manifest::s3_key(&source));
    pending
        .write_conditional(&failing_store, &source, &version)
        .await
        .unwrap_err();
    assert_eq!(failures.failures_injected(), 1);
    assert!(
        Manifest::read_history(&harness.store, &source, orphan_generation)
            .await
            .unwrap()
            .is_none(),
        "a failed live PUT must not publish speculative future history"
    );
    harness
        .store
        .put(
            &Manifest::history_key(&source, orphan_generation),
            manifest_json_bytes_with_version(&pending, orphan_generation),
        )
        .await
        .unwrap();
    assert!(
        Manifest::read_history(&harness.store, &source, orphan_generation)
            .await
            .unwrap()
            .is_some(),
        "the fixture must inject an unreferenced future history object"
    );

    let (status, body) = clone_namespace(
        &client,
        &base_url,
        &source,
        &target,
        &orphan_generation.to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::GONE);
    assert_eq!(body["code"], "POINT_IN_TIME_NOT_RETAINED");

    let get_target = client
        .get(format!("{base_url}/v1/namespaces/{target}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_target.status(), StatusCode::NOT_FOUND);

    cleanup_ns(&harness.store, &source).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn clone_as_of_timestamp_scans_full_history_under_clock_skew() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let source = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;
    let target = api_ns(&harness, "timestamp-skew-clone");

    upsert(
        &client,
        &base_url,
        &source,
        json!([{ "id": "first", "values": [0.0, 0.0] }]),
    )
    .await;
    let first = Manifest::read(&harness.store, &source)
        .await
        .unwrap()
        .unwrap();
    upsert(
        &client,
        &base_url,
        &source,
        json!([{ "id": "second", "values": [10.0, 10.0] }]),
    )
    .await;
    let second = Manifest::read(&harness.store, &source)
        .await
        .unwrap()
        .unwrap();
    upsert(
        &client,
        &base_url,
        &source,
        json!([{ "id": "third", "values": [20.0, 20.0] }]),
    )
    .await;

    let base = Utc::now() - Duration::minutes(10);
    rewrite_history_updated_at(
        &harness.store,
        &source,
        &first,
        base + Duration::seconds(100),
    )
    .await;
    rewrite_history_updated_at(
        &harness.store,
        &source,
        &second,
        base + Duration::seconds(50),
    )
    .await;

    let as_of = (base + Duration::seconds(75)).to_rfc3339();
    let (status, body) = clone_namespace(&client, &base_url, &source, &target, &as_of).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["generation"], second.version());

    let target_ids = query_ids(&client, &base_url, &target, [10.0, 10.0]).await;
    assert_eq!(target_ids.first().unwrap(), "second");
    assert!(!target_ids.iter().any(|id| id == "third"));

    cleanup_ns(&harness.store, &source).await;
    cleanup_ns(&harness.store, &target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn clone_compacted_generation_is_writable_and_survives_source_delete() {
    let mut config = zeppelin::config::Config::default();
    config.indexing.default_num_centroids = 2;
    config.indexing.default_nprobe = 2;
    config.indexing.max_nprobe = 8;
    config.indexing.quantization = QuantizationType::None;
    config.indexing.bitmap_index = false;
    config.indexing.fts_index = false;

    let (base_url, harness, _cache, _cache_dir, compactor, admin_bearer) =
        start_test_server_with_compactor(Some(config)).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let source = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;
    let target = api_ns(&harness, "restore-clone");

    upsert(
        &client,
        &base_url,
        &source,
        json!([
            { "id": "old-0", "values": [0.0, 0.0] },
            { "id": "old-1", "values": [0.1, 0.0] },
            { "id": "old-2", "values": [0.0, 0.1] },
            { "id": "old-3", "values": [1.0, 1.0] }
        ]),
    )
    .await;
    compactor.compact(&source).await.unwrap();
    let source_at_g = Manifest::read(&harness.store, &source)
        .await
        .unwrap()
        .unwrap();
    assert!(!source_at_g.segments.is_empty());
    assert!(source_at_g.fragments.is_empty());
    let generation = source_at_g.version();

    upsert(
        &client,
        &base_url,
        &source,
        json!([{ "id": "new-source", "values": [10.0, 10.0] }]),
    )
    .await;
    assert_eq!(
        query_ids(&client, &base_url, &source, [10.0, 10.0])
            .await
            .first()
            .unwrap(),
        "new-source"
    );

    let (status, body) = clone_namespace(
        &client,
        &base_url,
        &source,
        &target,
        &generation.to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["source"], source);
    assert_eq!(body["target"], target);
    assert_eq!(body["generation"], generation);
    assert_eq!(body["mode"], "copy");

    let target_manifest = Manifest::read(&harness.store, &target)
        .await
        .unwrap()
        .unwrap();
    let target_prefix = format!("{target}/");
    let source_prefix = format!("{source}/");
    for key in reachable_keys(&target, &target_manifest) {
        assert!(key.starts_with(&target_prefix), "target key escaped: {key}");
        assert!(
            !key.starts_with(&source_prefix),
            "target retained source key: {key}"
        );
    }

    let target_ids = set(query_ids(&client, &base_url, &target, [10.0, 10.0]).await);
    assert!(target_ids.contains("old-0"));
    assert!(!target_ids.contains("new-source"));

    upsert(
        &client,
        &base_url,
        &target,
        json!([{ "id": "clone-new", "values": [20.0, 20.0] }]),
    )
    .await;
    let target_after_write = set(query_ids(&client, &base_url, &target, [20.0, 20.0]).await);
    assert!(target_after_write.contains("clone-new"));
    let source_after_target_write = set(query_ids(&client, &base_url, &source, [20.0, 20.0]).await);
    assert!(!source_after_target_write.contains("clone-new"));

    let delete = client
        .delete(format!("{base_url}/v1/namespaces/{source}"))
        .send()
        .await
        .unwrap();
    assert_eq!(delete.status(), StatusCode::ACCEPTED);
    wait_namespace_gone(&client, &base_url, &source).await;

    let target_after_source_delete = set(query_ids(&client, &base_url, &target, [0.0, 0.0]).await);
    assert!(target_after_source_delete.contains("old-0"));

    cleanup_ns(&harness.store, &target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn clone_target_exists_returns_409() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let source = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;
    let target = api_ns(&harness, "target-exists");
    let created_target = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "name": target,
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;
    assert_eq!(created_target, target);

    let (status, body) = clone_namespace(&client, &base_url, &source, &target, "1").await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "NAMESPACE_ALREADY_EXISTS");

    cleanup_ns(&harness.store, &source).await;
    cleanup_ns(&harness.store, &target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn clone_current_generation_uses_live_manifest_when_history_copy_is_missing() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let source = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;
    let target = api_ns(&harness, "pruned-target");

    harness
        .store
        .delete(&Manifest::history_key(&source, 1))
        .await
        .unwrap();

    let (status, body) = clone_namespace(&client, &base_url, &source, &target, "1").await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["generation"], 1);

    let get_target = client
        .get(format!("{base_url}/v1/namespaces/{target}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_target.status(), StatusCode::OK);

    cleanup_ns(&harness.store, &source).await;
    cleanup_ns(&harness.store, &target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn clone_copy_failure_retains_activated_target_and_blocks_retry() {
    let harness = common::harness::TestHarness::new().await;
    let (failing_store, failures) = fail_copy_once_matching(&harness.store, "/wal/");
    let (base_url, _cache, _cache_dir, admin_bearer) =
        start_test_server_on_store(&harness, failing_store, None).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let source = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;
    let target = api_ns(&harness, "copy-failure-target");

    upsert(
        &client,
        &base_url,
        &source,
        json!([{ "id": "copy-me", "values": [0.0, 0.0] }]),
    )
    .await;
    let generation = Manifest::read(&harness.store, &source)
        .await
        .unwrap()
        .unwrap()
        .version()
        .to_string();

    let (status, body) = clone_namespace(&client, &base_url, &source, &target, &generation).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["code"], "STORAGE_ERROR");
    assert_eq!(failures.failures_injected(), 1);

    let get_target = client
        .get(format!("{base_url}/v1/namespaces/{target}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_target.status(), StatusCode::OK);
    assert!(
        !harness
            .store
            .list_prefix(&format!("{target}/"))
            .await
            .unwrap()
            .is_empty(),
        "failed clone must retain the activated target instead of racing concurrent writes"
    );
    assert!(
        NamedSnapshot::list(&harness.store, &source)
            .await
            .unwrap()
            .iter()
            .all(|snapshot| !snapshot.name.starts_with("__clone_")),
        "temporary source pin must be released after clone failure"
    );

    let (retry_status, retry_body) =
        clone_namespace(&client, &base_url, &source, &target, &generation).await;
    assert_eq!(retry_status, StatusCode::CONFLICT);
    assert_eq!(retry_body["code"], "NAMESPACE_ALREADY_EXISTS");

    cleanup_ns(&harness.store, &source).await;
    cleanup_ns(&harness.store, &target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn clone_copy_collision_surfaces_storage_error_and_retains_target() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let source = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;
    let target = api_ns(&harness, "copy-collision-target");

    upsert(
        &client,
        &base_url,
        &source,
        json!([{ "id": "copy-me", "values": [0.0, 0.0] }]),
    )
    .await;
    let source_manifest = Manifest::read(&harness.store, &source)
        .await
        .unwrap()
        .unwrap();
    let source_key = reachable_keys(&source, &source_manifest)
        .into_iter()
        .next()
        .expect("source manifest must reference a copyable artifact");
    let target_key = source_key.replacen(&format!("{source}/"), &format!("{target}/"), 1);
    harness
        .store
        .put(
            &target_key,
            bytes::Bytes::from_static(b"preexisting target object"),
        )
        .await
        .unwrap();

    let (status, body) = clone_namespace(
        &client,
        &base_url,
        &source,
        &target,
        &source_manifest.version().to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["code"], "STORAGE_ERROR");

    let get_target = client
        .get(format!("{base_url}/v1/namespaces/{target}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_target.status(), StatusCode::OK);
    assert!(
        harness.store.exists(&target_key).await.unwrap(),
        "clone failure must not destructively clean an activated target prefix"
    );

    cleanup_ns(&harness.store, &source).await;
    cleanup_ns(&harness.store, &target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn clone_holds_internal_source_pin_while_copying() {
    let harness = common::harness::TestHarness::new().await;
    let (asserting_store, snapshot_observer) = assert_snapshot_on_copy(&harness.store);
    let (base_url, _cache, _cache_dir, admin_bearer) =
        start_test_server_on_store(&harness, asserting_store, None).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let source = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;
    let target = api_ns(&harness, "copy-pin-target");

    upsert(
        &client,
        &base_url,
        &source,
        json!([{ "id": "copy-me", "values": [0.0, 0.0] }]),
    )
    .await;
    let source_manifest = Manifest::read(&harness.store, &source)
        .await
        .unwrap()
        .unwrap();
    snapshot_observer.expect_snapshot(&source, source_manifest.version(), "__clone_");

    let (status, body) = clone_namespace(
        &client,
        &base_url,
        &source,
        &target,
        &source_manifest.version().to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["target"], target);
    assert!(
        snapshot_observer.observations() > 0,
        "copy wrapper must observe the temporary source snapshot pin"
    );
    assert!(
        NamedSnapshot::list(&harness.store, &source)
            .await
            .unwrap()
            .iter()
            .all(|snapshot| !snapshot.name.starts_with("__clone_")),
        "temporary source pin must be released after clone publish"
    );

    cleanup_ns(&harness.store, &source).await;
    cleanup_ns(&harness.store, &target).await;
    harness.cleanup().await;
}
