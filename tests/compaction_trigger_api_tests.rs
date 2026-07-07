mod common;

use std::time::Duration;

use common::server::{cleanup_ns, create_ns_api, start_test_server_with_config};
use common::vectors::random_vectors;
use serde_json::Value;
use uuid::Uuid;
use zeppelin::wal::LeaseManager;

async fn upsert_vectors(client: &reqwest::Client, base_url: &str, ns: &str, count: usize) {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": random_vectors(count, 16) }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

async fn get_compaction_status(client: &reqwest::Client, base_url: &str, ns: &str) -> Value {
    let resp = client
        .get(format!("{base_url}/v1/namespaces/{ns}/compact/status"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    resp.json().await.unwrap()
}

#[tokio::test]
async fn test_post_compact_reaches_quiescence_without_query_polling() {
    let (base_url, harness, _cache, cache_dir) = start_test_server_with_config(None).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 16).await;

    upsert_vectors(&client, &base_url, &ns, 64).await;

    let before = get_compaction_status(&client, &base_url, &ns).await;
    assert_eq!(before["uncompacted_fragments"], 1);
    assert_eq!(before["segment_count"], 0);
    assert_eq!(before["ready"], false);

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/compact"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let accepted: Value = resp.json().await.unwrap();
    assert_eq!(accepted["status"], "accepted");
    assert_eq!(
        accepted["manifest_generation"],
        before["manifest_generation"]
    );
    assert_eq!(accepted["uncompacted_fragments"], 1);
    assert_eq!(accepted["segment_count"], 0);

    let mut after = get_compaction_status(&client, &base_url, &ns).await;
    for _ in 0..200 {
        if after["uncompacted_fragments"] == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        after = get_compaction_status(&client, &base_url, &ns).await;
    }
    assert_eq!(after["uncompacted_fragments"], 0);
    assert_eq!(after["segment_count"], 1);
    assert_eq!(after["ready"], true);
    assert!(
        after["manifest_generation"].as_u64().unwrap()
            > before["manifest_generation"].as_u64().unwrap()
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
    drop(cache_dir);
}

#[tokio::test]
async fn test_post_compact_returns_409_when_lease_is_held() {
    let (base_url, harness, _cache, cache_dir) = start_test_server_with_config(None).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 16).await;

    upsert_vectors(&client, &base_url, &ns, 32).await;

    let lease_manager = LeaseManager::new(
        harness.store.clone(),
        format!("external-{}", Uuid::new_v4()),
        Duration::from_secs(60),
    );
    let lease = lease_manager.acquire(&ns).await.unwrap();

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/compact"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 409);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "CONFLICT_RETRY");
    assert_eq!(body["retryable"], true);

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/compact"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 409);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "CONFLICT_RETRY");
    assert_eq!(body["retryable"], true);

    let status = get_compaction_status(&client, &base_url, &ns).await;
    assert_eq!(status["uncompacted_fragments"], 1);
    assert_eq!(status["segment_count"], 0);

    lease_manager.release(&ns, &lease).await.unwrap();
    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
    drop(cache_dir);
}

#[tokio::test]
async fn test_post_compact_empty_namespace_is_noop() {
    let (base_url, harness, _cache, cache_dir) = start_test_server_with_config(None).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 16).await;

    let before = get_compaction_status(&client, &base_url, &ns).await;
    assert_eq!(before["uncompacted_fragments"], 0);
    assert_eq!(before["segment_count"], 0);
    assert_eq!(before["ready"], true);

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/compact"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "noop");
    assert_eq!(body["manifest_generation"], before["manifest_generation"]);
    assert_eq!(body["uncompacted_fragments"], 0);
    assert_eq!(body["segment_count"], 0);

    let after = get_compaction_status(&client, &base_url, &ns).await;
    assert_eq!(after["manifest_generation"], before["manifest_generation"]);
    assert_eq!(after["uncompacted_fragments"], 0);
    assert_eq!(after["segment_count"], 0);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
    drop(cache_dir);
}
