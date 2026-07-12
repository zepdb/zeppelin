mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use common::harness::TestHarness;
use common::server::start_test_server;
use reqwest::StatusCode;
use serde_json::json;
use zeppelin::namespace::manager::NamespaceMetadata;
use zeppelin::namespace::NamespaceManager;
use zeppelin::time::{Clock, TimeSource};
use zeppelin::types::DistanceMetric;

#[derive(Debug)]
struct AdjustableWallClock(Mutex<DateTime<Utc>>);

impl AdjustableWallClock {
    fn new(now: DateTime<Utc>) -> Self {
        Self(Mutex::new(now))
    }

    fn jump(&self, delta: chrono::Duration) {
        let mut now = self.0.lock().expect("test wall clock mutex poisoned");
        *now += delta;
    }
}

impl TimeSource for AdjustableWallClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().expect("test wall clock mutex poisoned")
    }
}

fn ns(harness: &TestHarness, suffix: &str) -> String {
    format!("{}-{suffix}", harness.prefix)
}

async fn write_deleting_meta(harness: &TestHarness, ns: &str) {
    let now = Utc::now().to_rfc3339();
    let meta = json!({
        "name": ns,
        "dimensions": 2,
        "distance_metric": "cosine",
        "index_type": "ivf_flat",
        "vector_count": 0,
        "created_at": now,
        "updated_at": now,
        "state": "deleting",
        "full_text_search": {}
    });
    harness
        .store
        .put(
            &NamespaceMetadata::s3_key(ns),
            Bytes::from(serde_json::to_vec_pretty(&meta).unwrap()),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_delete_resumes_from_deleting_tombstone_with_data_left() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "delete-resumes-tombstone");
    let manager = NamespaceManager::new(harness.store.clone());

    write_deleting_meta(&harness, &name).await;
    harness
        .store
        .put(
            &format!("{name}/wal/left-behind.wal"),
            Bytes::from_static(b"wal"),
        )
        .await
        .unwrap();

    let result = manager.delete(&name).await;
    assert!(
        result.is_ok(),
        "delete retry must be able to resume from a deleting tombstone, got {result:?}"
    );
    let remaining = harness
        .store
        .list_prefix(&format!("{name}/"))
        .await
        .unwrap();
    assert!(
        remaining.is_empty(),
        "completed namespace delete must leave zero keys, got {remaining:?}"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn test_deleting_namespace_rejects_manager_ops_with_410() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "deleting-manager-ops");
    let manager = NamespaceManager::new(harness.store.clone());
    write_deleting_meta(&harness, &name).await;

    let get_result = manager.get(&name).await;
    assert_eq!(
        get_result.unwrap_err().status_code(),
        410,
        "get on a deleting namespace must surface 410 Gone"
    );

    let create_result = manager.create(&name, 2, DistanceMetric::Cosine).await;
    assert_eq!(
        create_result.unwrap_err().status_code(),
        410,
        "create during delete must not reopen the Bug-37 zombie window"
    );

    let _ = harness.store.delete_prefix(&format!("{name}/")).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_cross_node_registry_delete_converges_within_ttl() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "registry-converges");
    let manager_a = NamespaceManager::new(harness.store.clone());
    let manager_b =
        NamespaceManager::new_with_registry_ttl(harness.store.clone(), Duration::from_millis(100));

    manager_a
        .create(&name, 2, DistanceMetric::Cosine)
        .await
        .unwrap();
    manager_b.get(&name).await.unwrap();
    manager_a.delete(&name).await.unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match manager_b.get(&name).await {
            Err(err) if err.status_code() == 404 || err.status_code() == 410 => break,
            Ok(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await
            }
            Ok(meta) => panic!("cached namespace did not expire within one registry TTL: {meta:?}"),
            Err(err) => panic!("unexpected registry convergence error: {err:?}"),
        }
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn test_registry_ttl_is_not_extended_by_backward_frozen_wall_clock() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "registry-monotonic-clock-domain");
    let manager_a = NamespaceManager::new(harness.store.clone());
    let wall_clock = Arc::new(AdjustableWallClock::new(Utc::now()));
    let manager_b = NamespaceManager::with_clock(
        harness.store.clone(),
        Duration::from_millis(50),
        Clock::from_source(wall_clock.clone()),
    );

    manager_a
        .create(&name, 2, DistanceMetric::Cosine)
        .await
        .unwrap();
    manager_b.get(&name).await.unwrap();

    wall_clock.jump(chrono::Duration::minutes(-5));
    manager_a.start_delete(&name).await.unwrap();
    tokio::time::sleep(Duration::from_millis(75)).await;

    let result = manager_b.get(&name).await;
    assert!(
        matches!(result, Err(ref error) if error.status_code() == 410),
        "expired local registry entry hid the authoritative tombstone: {result:?}"
    );

    manager_a.finish_delete(&name, Duration::MAX).await.unwrap();
    harness.cleanup().await;
}

#[tokio::test]
async fn test_create_same_name_blocked_until_deleting_tombstone_removed() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "create-blocked-delete");
    let manager = NamespaceManager::new(harness.store.clone());

    manager
        .create(&name, 2, DistanceMetric::Cosine)
        .await
        .unwrap();
    harness
        .store
        .put(&format!("{name}/wal/old.wal"), Bytes::from_static(b"old"))
        .await
        .unwrap();

    manager.start_delete(&name).await.unwrap();
    let create_while_deleting = manager.create(&name, 2, DistanceMetric::Cosine).await;
    assert_eq!(
        create_while_deleting.unwrap_err().status_code(),
        410,
        "meta.json tombstone must block recreation while old data remains"
    );

    let outcome = manager.finish_delete(&name, Duration::MAX).await.unwrap();
    assert!(outcome.complete);
    assert!(harness
        .store
        .list_prefix(&format!("{name}/"))
        .await
        .unwrap()
        .is_empty());

    manager
        .create(&name, 2, DistanceMetric::Cosine)
        .await
        .unwrap();
    let keys = harness
        .store
        .list_prefix(&format!("{name}/"))
        .await
        .unwrap();
    assert!(
        keys.iter().any(|key| key.ends_with("/meta.json")),
        "fresh namespace must have meta.json: {keys:?}"
    );
    assert!(
        keys.iter().any(|key| key.ends_with("/manifest.json")),
        "fresh namespace must have manifest.json: {keys:?}"
    );
    assert!(
        keys.iter().all(|key| !key.ends_with(".wal")),
        "fresh namespace must not inherit old WAL data: {keys:?}"
    );

    let _ = harness.store.delete_prefix(&format!("{name}/")).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_api_deleting_namespace_status_and_ops_410() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let name = format!("{}-api-deleting", harness.prefix);

    write_deleting_meta(&harness, &name).await;

    let get_resp = client
        .get(format!("{base_url}/v1/namespaces/{name}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);
    let body: serde_json::Value = get_resp.json().await.unwrap();
    assert_eq!(body["state"], "deleting");

    let upsert_resp = client
        .post(format!("{base_url}/v1/namespaces/{name}/vectors"))
        .json(&json!({
            "vectors": [
                {"id": "a", "values": [1.0, 0.0], "attributes": {"tenant": "red"}}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(upsert_resp.status(), StatusCode::GONE);

    let query_resp = client
        .post(format!("{base_url}/v1/namespaces/{name}/query"))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(query_resp.status(), StatusCode::GONE);

    let _ = harness.store.delete_prefix(&format!("{name}/")).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_api_delete_returns_202_and_eventually_leaves_zero_keys() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();

    let create_resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&json!({"dimensions": 2}))
        .send()
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let created: serde_json::Value = create_resp.json().await.unwrap();
    let name = created["name"].as_str().unwrap().to_string();

    client
        .post(format!("{base_url}/v1/namespaces/{name}/vectors"))
        .json(&json!({
            "vectors": [
                {"id": "a", "values": [1.0, 0.0], "attributes": {"tenant": "red"}}
            ]
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let delete_resp = client
        .delete(format!("{base_url}/v1/namespaces/{name}"))
        .send()
        .await
        .unwrap();
    assert_eq!(delete_resp.status(), StatusCode::ACCEPTED);
    let body: serde_json::Value = delete_resp.json().await.unwrap();
    assert_eq!(body["state"], "deleting");

    for _ in 0..50 {
        let get_resp = client
            .get(format!("{base_url}/v1/namespaces/{name}"))
            .send()
            .await
            .unwrap();
        match get_resp.status() {
            StatusCode::OK => {
                let body: serde_json::Value = get_resp.json().await.unwrap();
                assert_eq!(body["state"], "deleting");
            }
            StatusCode::NOT_FOUND => break,
            other => panic!("unexpected namespace status while deleting: {other}"),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let keys = harness
        .store
        .list_prefix(&format!("{name}/"))
        .await
        .unwrap();
    assert!(keys.is_empty(), "delete must leave zero keys, got {keys:?}");
    harness.cleanup().await;
}
