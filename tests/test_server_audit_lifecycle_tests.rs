mod common;

use std::time::Duration;

use common::harness::TestHarness;
use common::server::{
    client_with_bearer, start_test_server_full, start_test_server_on_store,
    start_test_server_with_config,
};
use zeppelin::config::Config;

static LIFECYCLE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn harness_cleanup_stops_server_before_removing_buffered_audit_objects() {
    let _serial = LIFECYCLE_TEST_LOCK.lock().await;
    let mut config = Config::default();
    config.security.audit_flush_secs = 1;
    let (base_url, harness, _cache, _cache_dir, admin_bearer) =
        start_test_server_with_config(Some(config)).await;
    let client = client_with_bearer(&admin_bearer);

    let response = client
        .get(format!("{base_url}/v1/config/query"))
        .send()
        .await
        .expect("audited request should reach the live test server");
    assert_eq!(response.status(), 200);

    harness.cleanup().await;
    tokio::time::sleep(Duration::from_millis(1_200)).await;

    let marker = format!("/test-node-{}-", harness.prefix);
    let leaked = harness
        .store
        .list_prefix("_audit/")
        .await
        .expect("audit prefix should remain listable")
        .into_iter()
        .filter(|key| key.contains(&marker))
        .collect::<Vec<_>>();
    assert!(
        leaked.is_empty(),
        "cleanup returned before the audit timer was quiescent: {leaked:?}"
    );

    let post_cleanup = client
        .get(format!("{base_url}/v1/config/query"))
        .send()
        .await;
    assert!(
        post_cleanup.is_err(),
        "cleanup must stop the detached HTTP server that owns audit clients"
    );
}

#[tokio::test]
async fn on_store_server_without_namespace_prefix_still_uses_harness_audit_scope() {
    let _serial = LIFECYCLE_TEST_LOCK.lock().await;
    let harness = TestHarness::new().await;
    let (base_url, _cache, _cache_dir, admin_bearer) =
        start_test_server_on_store(&harness, harness.store.clone(), None).await;
    let request_id = format!("audit-scope-{}", harness.prefix);

    let response = client_with_bearer(&admin_bearer)
        .patch(format!("{base_url}/v1/config/query"))
        .header("x-request-id", &request_id)
        .json(&serde_json::json!({"default_top_k": 17}))
        .send()
        .await
        .expect("audited request should reach the on-store test server");
    assert_eq!(response.status(), 200);

    let marker = format!("/test-node-{}-", harness.prefix);
    let scoped_keys = harness
        .store
        .list_prefix("_audit/")
        .await
        .expect("audit prefix should be listable before cleanup")
        .into_iter()
        .filter(|key| key.contains(&marker));
    let mut matching_keys = Vec::new();
    for key in scoped_keys {
        let body = harness
            .store
            .get(&key)
            .await
            .expect("listed audit object should be readable");
        if body
            .windows(request_id.len())
            .any(|window| window == request_id.as_bytes())
        {
            matching_keys.push(key);
        }
    }
    assert_eq!(
        matching_keys.len(),
        1,
        "request-specific audit evidence must identify exactly one object"
    );
    let own_audit_key = matching_keys.pop().unwrap();

    harness.cleanup().await;
    let remaining = harness
        .store
        .list_prefix("_audit/")
        .await
        .expect("audit prefix should be listable after cleanup")
        .into_iter()
        .any(|key| key == own_audit_key);
    assert!(
        !remaining,
        "namespace_name_prefix=None left its own audit object behind: {own_audit_key}"
    );
}

#[tokio::test]
async fn crash_retirement_aborts_audit_without_writing_a_terminal_seal() {
    let _serial = LIFECYCLE_TEST_LOCK.lock().await;
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
    )
    .await;
    let retired_node_id = server.audit_node_id.clone();
    let response = client_with_bearer(&server.admin_bearer)
        .get(format!("{}/v1/config/query", server.base_url))
        .send()
        .await
        .expect("audited request should reach the live test server");
    assert_eq!(response.status(), 200);
    drop(response);
    server.flush_audit().await;

    server
        .abort_and_drop()
        .await
        .expect("audit crash retirement must join its HTTP task");

    let stream_marker = format!("/{retired_node_id}/");
    let stream_keys = harness
        .store
        .list_prefix("_audit/")
        .await
        .expect("retired audit stream should remain listable")
        .into_iter()
        .filter(|key| key.contains(&stream_marker) && key.ends_with(".jsonl"))
        .collect::<Vec<_>>();
    assert!(
        !stream_keys.is_empty(),
        "the crash fixture must have durable pre-crash audit evidence"
    );
    for key in stream_keys {
        let body = harness
            .store
            .get(&key)
            .await
            .expect("retired audit object should remain readable");
        let has_terminal_seal = String::from_utf8(body.to_vec())
            .expect("retired audit object must be UTF-8 JSON lines")
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .any(|value| {
                value.get("format").and_then(serde_json::Value::as_str)
                    == Some("zeppelin_audit_terminal_seal_v1")
            });
        assert!(
            !has_terminal_seal,
            "crash retirement must abort the writer instead of sealing {key}"
        );
    }
    harness.cleanup().await;
}
