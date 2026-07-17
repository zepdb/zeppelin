mod common;

use std::time::Duration;

use common::counting::counting_store;
use common::fault_injection::pause_next_get_matching;
use common::harness::TestHarness;
use common::server::{
    client_with_bearer, create_ns_api, start_test_server_full,
    start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer,
    FullTestServerRetirementError,
};
use common::vectors::random_vectors;
use zeppelin::config::Config;

async fn wait_for_lease_renewal(counter: &common::counting::GetCounter, lease_key: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if counter.update_puts_matching(lease_key) > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("manual compaction never renewed {lease_key} before retirement"));
}

#[tokio::test]
async fn crash_retirement_joins_manual_compaction_heartbeat_before_replacement() {
    let harness = TestHarness::new().await;
    let (counted_store, counter) = counting_store(&harness.store);
    let (store, compaction_pause) = pause_next_get_matching(&counted_store, "/wal/");
    let mut config = Config::default();
    config.compaction.lease_duration_secs = 1;
    config.security.policy_refresh_secs = 60;

    let server = start_test_server_full(
        store.clone(),
        Some(harness.prefix.clone()),
        config.clone(),
        false,
        None,
    )
    .await;
    let admin_bearer = server.admin_bearer.clone();
    let client = client_with_bearer(&admin_bearer);
    let namespace = create_ns_api(&client, &server.base_url, 16).await;
    let upsert = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&serde_json::json!({ "vectors": random_vectors(32, 16) }))
        .send()
        .await
        .expect("upsert fixture must reach the live server");
    assert_eq!(upsert.status(), 200);

    let lease_key = format!("{namespace}/lease.json");
    counter.reset();
    compaction_pause.arm();
    let accepted = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/compact",
            server.base_url
        ))
        .send()
        .await
        .expect("manual compaction request must reach the live server");
    assert_eq!(accepted.status(), 202);
    drop(accepted);
    tokio::time::timeout(Duration::from_secs(5), compaction_pause.wait_until_paused())
        .await
        .expect("manual compaction must pause at its real WAL GET");
    wait_for_lease_renewal(&counter, &lease_key).await;

    server
        .abort_and_drop()
        .await
        .expect("manual-compaction crash retirement must join its HTTP task");
    assert!(
        compaction_pause.has_exited(),
        "crash retirement returned before joining the old manual compaction task"
    );
    assert!(
        compaction_pause.was_cancelled_before_storage(),
        "the old manual compaction reached S3 after crash retirement began"
    );
    let renewals_after_retirement = counter.update_puts_matching(&lease_key);

    let replacement = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        store,
        Some(harness.prefix.clone()),
        config,
        false,
        None,
        100 * 1024 * 1024,
        &admin_bearer,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    assert_eq!(
        counter.update_puts_matching(&lease_key),
        renewals_after_retirement,
        "a retired heartbeat renewed {lease_key} after replacement started"
    );

    let replacement_client = client_with_bearer(&admin_bearer);
    let replacement_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let replacement_compact = replacement_client
            .post(format!(
                "{}/v1/namespaces/{namespace}/compact",
                replacement.base_url
            ))
            .send()
            .await
            .expect("replacement manual compaction request must reach the live server");
        if replacement_compact.status() == reqwest::StatusCode::ACCEPTED {
            break;
        }
        assert_eq!(
            replacement_compact.status(),
            reqwest::StatusCode::CONFLICT,
            "replacement compaction may only wait for the pre-crash lease"
        );
        assert!(
            tokio::time::Instant::now() < replacement_deadline,
            "replacement never acquired after the pre-crash lease naturally expired"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    replacement.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn crash_retirement_joins_periodic_compaction_heartbeat_before_replacement() {
    let harness = TestHarness::new().await;
    let (counted_store, counter) = counting_store(&harness.store);
    let (store, compaction_pause) = pause_next_get_matching(&counted_store, "/wal/");
    let mut config = Config::default();
    config.compaction.interval_secs = 1;
    config.compaction.max_wal_fragments_before_compact = 1;
    config.compaction.lease_duration_secs = 1;
    config.security.policy_refresh_secs = 60;

    let server = start_test_server_full(
        store.clone(),
        Some(harness.prefix.clone()),
        config.clone(),
        true,
        None,
    )
    .await;
    let admin_bearer = server.admin_bearer.clone();
    let client = client_with_bearer(&admin_bearer);
    let namespace = create_ns_api(&client, &server.base_url, 16).await;
    let upsert = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&serde_json::json!({ "vectors": random_vectors(32, 16) }))
        .send()
        .await
        .expect("upsert fixture must reach the live server");
    assert_eq!(upsert.status(), 200);

    let lease_key = format!("{namespace}/lease.json");
    counter.reset();
    compaction_pause.arm();
    tokio::time::timeout(Duration::from_secs(5), compaction_pause.wait_until_paused())
        .await
        .expect("periodic compaction must pause at its real WAL GET");
    wait_for_lease_renewal(&counter, &lease_key).await;

    server
        .abort_and_drop()
        .await
        .expect("periodic-compaction crash retirement must join its HTTP task");
    assert!(
        compaction_pause.has_exited(),
        "crash retirement returned before joining the old periodic compaction task"
    );
    assert!(
        compaction_pause.was_cancelled_before_storage(),
        "the old periodic compaction reached S3 after crash retirement began"
    );
    let renewals_after_retirement = counter.update_puts_matching(&lease_key);

    let replacement = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        store,
        Some(harness.prefix.clone()),
        config,
        false,
        None,
        100 * 1024 * 1024,
        &admin_bearer,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    assert_eq!(
        counter.update_puts_matching(&lease_key),
        renewals_after_retirement,
        "a retired periodic heartbeat renewed {lease_key} after replacement started"
    );

    let replacement_client = client_with_bearer(&admin_bearer);
    let replacement_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let replacement_compact = replacement_client
            .post(format!(
                "{}/v1/namespaces/{namespace}/compact",
                replacement.base_url
            ))
            .send()
            .await
            .expect("replacement manual compaction request must reach the live server");
        if replacement_compact.status() == reqwest::StatusCode::ACCEPTED {
            break;
        }
        assert_eq!(
            replacement_compact.status(),
            reqwest::StatusCode::CONFLICT,
            "replacement compaction may only wait for the pre-crash lease"
        );
        assert!(
            tokio::time::Instant::now() < replacement_deadline,
            "replacement never acquired after the pre-crash lease naturally expired"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    replacement.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn crash_retirement_finishes_authority_barriers_after_http_task_cancellation() {
    let harness = TestHarness::new().await;
    let (store, policy_pause) =
        pause_next_get_matching(&harness.store, "_security/heads/policy.json");
    let mut config = Config::default();
    config.security.policy_refresh_secs = 1;

    let server =
        start_test_server_full(store, Some(harness.prefix.clone()), config, false, None).await;
    let retained_security = server.security.clone();

    policy_pause.arm();
    tokio::time::timeout(Duration::from_secs(5), policy_pause.wait_until_paused())
        .await
        .expect("policy refresh must pause at its real policy-head GET");

    server.server_task.abort();
    let retirement = tokio::spawn(async move { server.abort_and_drop().await });
    let retirement_error = tokio::time::timeout(Duration::from_secs(5), retirement)
        .await
        .expect("crash retirement must not hang after the HTTP task has failed")
        .expect("crash retirement must return its typed HTTP failure after cleanup")
        .expect_err("cancelled HTTP task must remain a loud retirement failure");
    assert!(matches!(
        retirement_error,
        FullTestServerRetirementError::HttpTask(_)
    ));

    assert!(
        policy_pause.has_exited(),
        "HTTP join failure prevented policy-refresh retirement"
    );
    assert!(
        policy_pause.was_cancelled_before_storage(),
        "policy refresh reached storage after crash retirement"
    );

    drop(retained_security);
    harness.cleanup().await;
}
