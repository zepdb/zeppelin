mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use common::harness::TestHarness;
use common::server::{client_with_bearer, start_test_server_full};
use serde_json::json;
use zeppelin::config::Config;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_loop_death_withholds_readiness() {
    let harness = TestHarness::new().await;
    let mut config = Config::default();
    config.compaction.interval_secs = 1;
    config.security.readyz_public = true;
    config.security.policy_refresh_secs = 3_600;

    let mut server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        config,
        true,
        None,
    )
    .await;
    let client = client_with_bearer(&server.admin_bearer);
    let health = Arc::clone(&server.compaction_loop_health);
    let initial_tick = health.last_tick_unix_secs();

    tokio::time::timeout(Duration::from_secs(5), async {
        while health.last_tick_unix_secs() <= initial_tick {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("background compaction heartbeat must advance");
    assert!(health.is_alive());

    let ready = client
        .get(format!("{}/readyz", server.base_url))
        .send()
        .await
        .expect("healthy readiness probe must complete");
    assert_eq!(ready.status(), StatusCode::OK);

    let compaction_task = server
        .compaction_loop_task
        .take()
        .expect("test server must own a compaction loop");
    compaction_task.abort();
    let cancellation = compaction_task
        .await
        .expect_err("aborted compaction loop must report cancellation");
    assert!(cancellation.is_cancelled());
    assert!(
        !health.is_alive(),
        "AppState health handle must observe loop exit"
    );

    let not_ready = client
        .get(format!("{}/readyz", server.base_url))
        .send()
        .await
        .expect("failed readiness probe must complete");
    assert_eq!(not_ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        not_ready
            .json::<serde_json::Value>()
            .await
            .expect("readiness failure body must be JSON"),
        json!({
            "status": "not_ready",
            "compaction_loop_alive": false,
            "error": "background compaction loop is not running",
        })
    );

    server.shutdown().await;
    harness.cleanup().await;
}
