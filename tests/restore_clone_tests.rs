mod common;

use std::collections::BTreeSet;
use std::time::Duration as StdDuration;

use bytes::Bytes;
use chrono::{Duration, Utc};
use common::fault_injection::{
    assert_snapshot_on_copy, fail_copy_once_matching, fail_put_once_matching,
    pause_next_copy_matching,
};
use common::server::{
    api_ns, cleanup_ns, create_ns_api_with, start_test_server, start_test_server_on_store,
    start_test_server_on_store_with_config, start_test_server_with_compactor,
};
use reqwest::StatusCode;
use serde_json::{json, Value};
use ulid::Ulid;
use zeppelin::compaction::gc::reachable_keys;
use zeppelin::config::Config;
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
    assert!(
        target_manifest.artifact_origins.is_empty(),
        "a raw clone must not retain an explicit source-incarnation origin table"
    );
    assert!(
        target_manifest
            .uncompacted_fragments()
            .iter()
            .all(|fragment| fragment.artifact_origin.is_none()),
        "raw-cloned WAL must use implicit target-local ownership"
    );
    assert!(
        target_manifest
            .segments
            .iter()
            .all(|segment| segment.artifact_origin.is_none()),
        "raw-cloned segments must use implicit target-local ownership"
    );
    let target_manifest_json = serde_json::to_value(&target_manifest).unwrap();
    assert!(target_manifest_json["branch_lineage"].is_null());
    assert_eq!(target_manifest_json["branch_roots"], json!({}));
    assert!(target_manifest.visible_refs_are_local().unwrap());
    let target_prefix = format!("{target}/");
    let source_prefix = format!("{source}/");
    for key in reachable_keys(&target, &target_manifest).unwrap() {
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
async fn clone_materialized_branch_rebinds_copied_artifacts_to_the_target() {
    let mut config = Config::default();
    config.branching.enabled = true;
    config.security.policy_refresh_secs = 3_600;
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
    let branch = api_ns(&harness, "materialized-branch-source");
    let target = api_ns(&harness, "materialized-branch-clone");

    upsert(
        &client,
        &base_url,
        &source,
        json!([
            { "id": "branch-row-0", "values": [0.0, 0.0] },
            { "id": "branch-row-1", "values": [0.1, 0.0] },
            { "id": "branch-row-2", "values": [0.0, 0.1] },
            { "id": "branch-row-3", "values": [1.0, 1.0] }
        ]),
    )
    .await;
    compactor.compact(&source).await.unwrap();

    let fork = client
        .post(format!("{base_url}/v1/namespaces/{source}/branches"))
        .json(&json!({ "target": branch }))
        .send()
        .await
        .unwrap();
    assert_eq!(fork.status(), StatusCode::CREATED);

    compactor.compact(&branch).await.unwrap();
    let materialized = Manifest::read(&harness.store, &branch)
        .await
        .unwrap()
        .unwrap();
    assert!(materialized.visible_refs_are_local().unwrap());
    assert_eq!(
        materialized.artifact_origins.len(),
        1,
        "ordinary compaction makes local ownership explicit before raw clone normalization"
    );
    assert!(materialized
        .segments
        .iter()
        .all(|segment| segment.artifact_origin.is_some()));

    let (status, body) = clone_namespace(
        &client,
        &base_url,
        &branch,
        &target,
        &materialized.version().to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["mode"], "copy");
    assert!(body["namespace"]["branch"].is_null());

    let cloned = Manifest::read(&harness.store, &target)
        .await
        .unwrap()
        .unwrap();
    assert!(cloned.artifact_origins.is_empty());
    assert!(cloned
        .segments
        .iter()
        .all(|segment| segment.artifact_origin.is_none()));
    assert!(cloned.visible_refs_are_local().unwrap());
    let cloned_json = serde_json::to_value(&cloned).unwrap();
    assert!(cloned_json["branch_lineage"].is_null());
    assert_eq!(cloned_json["branch_roots"], json!({}));
    assert_eq!(
        set(query_ids(&client, &base_url, &target, [0.0, 0.0]).await),
        BTreeSet::from([
            "branch-row-0".to_string(),
            "branch-row-1".to_string(),
            "branch-row-2".to_string(),
            "branch-row-3".to_string(),
        ])
    );

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
        .unwrap()
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

#[tokio::test]
async fn clone_timeout_cancels_paused_copy_and_leaves_history_pin() {
    let harness = common::harness::TestHarness::new().await;
    let source = api_ns(&harness, "timeout-pin-source");
    let target = api_ns(&harness, "timeout-pin-target");
    let (paused_store, pause) = pause_next_copy_matching(&harness.store, format!("{source}/wal/"));
    let mut config = Config::default();
    config.server.request_timeout_secs = 1;
    let (base_url, _cache, _cache_dir, admin_bearer) =
        start_test_server_on_store_with_config(&harness, paused_store, None, config).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);

    let created_source = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "name": source,
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;
    assert_eq!(created_source, source);

    upsert(
        &client,
        &base_url,
        &source,
        json!([{ "id": "selected", "values": [0.0, 0.0] }]),
    )
    .await;
    let selected_generation = Manifest::read(&harness.store, &source)
        .await
        .unwrap()
        .unwrap()
        .version();
    upsert(
        &client,
        &base_url,
        &source,
        json!([{ "id": "newer-one", "values": [1.0, 1.0] }]),
    )
    .await;
    upsert(
        &client,
        &base_url,
        &source,
        json!([{ "id": "newer-two", "values": [2.0, 2.0] }]),
    )
    .await;
    assert!(
        Manifest::read_history(&harness.store, &source, selected_generation)
            .await
            .unwrap()
            .is_some(),
        "selected generation must be retained before clone starts"
    );

    pause.arm();
    let clone_client = client.clone();
    let clone_base_url = base_url.clone();
    let clone_source = source.clone();
    let clone_target = target.clone();
    let clone_task = tokio::spawn(async move {
        clone_namespace(
            &clone_client,
            &clone_base_url,
            &clone_source,
            &clone_target,
            &selected_generation.to_string(),
        )
        .await
    });

    tokio::time::timeout(StdDuration::from_secs(10), pause.wait_until_paused())
        .await
        .expect("clone COPY must reach the deterministic pause boundary");
    let paused_pin = NamedSnapshot::list(&harness.store, &source)
        .await
        .unwrap()
        .into_iter()
        .find(|snapshot| {
            snapshot.name.starts_with("__clone_") && snapshot.generation == selected_generation
        })
        .expect("internal clone pin must be durable before COPY starts");

    let (status, body) = tokio::time::timeout(StdDuration::from_secs(10), clone_task)
        .await
        .expect("request timeout must bound the paused clone")
        .expect("clone task must not panic");
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT, "{body}");
    assert_eq!(body["code"], "REQUEST_TIMEOUT", "{body}");
    tokio::time::timeout(StdDuration::from_secs(10), pause.wait_until_exited())
        .await
        .expect("cancelled COPY future must exit");
    assert!(
        pause.was_cancelled_before_storage(),
        "request timeout must cancel COPY before it reaches storage"
    );

    assert!(
        NamedSnapshot::list(&harness.store, &source)
            .await
            .unwrap()
            .iter()
            .any(|snapshot| {
                snapshot.name == paused_pin.name && snapshot.generation == selected_generation
            }),
        "timeout cancellation must fail conservatively by retaining the internal pin"
    );
    let pruned = Manifest::prune_history(&harness.store, &source, 1)
        .await
        .unwrap();
    assert!(
        pruned > 0,
        "fixture must prune an unprotected older generation"
    );
    assert!(
        Manifest::read_history(&harness.store, &source, selected_generation)
            .await
            .unwrap()
            .is_some(),
        "retained internal clone pin must protect the selected history generation"
    );

    cleanup_ns(&harness.store, &source).await;
    cleanup_ns(&harness.store, &target).await;
    harness.cleanup().await;
}
