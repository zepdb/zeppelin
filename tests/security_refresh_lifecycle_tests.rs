mod common;

use std::time::Duration;

use common::counting::counting_store;
use common::fault_injection::pause_next_get_matching;
use common::harness::TestHarness;
use common::server::start_test_server_full;
use zeppelin::config::Config;

async fn crash_retirement_cancels_and_joins_refresh_before_replacement(
    refresh_key: &str,
    refresh_name: &str,
) {
    let harness = TestHarness::new().await;
    let (counted_store, counter) = counting_store(&harness.store);
    let (store, pause) = pause_next_get_matching(&counted_store, refresh_key);
    let mut config = Config::default();
    config.security.policy_refresh_secs = 1;

    let server = start_test_server_full(
        store.clone(),
        Some(harness.prefix.clone()),
        config.clone(),
        false,
        None,
    )
    .await;
    pause.arm();
    counter.reset();
    tokio::time::timeout(Duration::from_secs(5), pause.wait_until_paused())
        .await
        .unwrap_or_else(|_| {
            panic!("the old node's {refresh_name} refresh must reach the real-storage boundary")
        });
    assert_eq!(
        counter.gets_matching(refresh_key),
        0,
        "the pause must stop the old refresh before its S3 GET"
    );

    server
        .abort_and_drop()
        .await
        .expect("security-refresh crash retirement must join its HTTP task");
    assert!(
        pause.has_exited(),
        "crash retirement returned before joining the old {refresh_name} refresh"
    );
    assert!(
        pause.was_cancelled_before_storage(),
        "crash retirement returned without cancelling the old {refresh_name} refresh before S3"
    );

    let mut replacement_config = config;
    replacement_config.security.policy_refresh_secs = 60;
    let replacement = start_test_server_full(
        store,
        Some(harness.prefix.clone()),
        replacement_config,
        false,
        None,
    )
    .await;
    let replacement_bootstrap_reads = counter.gets_matching(refresh_key);
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        counter.gets_matching(refresh_key),
        replacement_bootstrap_reads,
        "a retired {refresh_name} refresh performed an S3 GET after replacement bootstrap"
    );
    replacement.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn crash_retirement_cancels_and_joins_policy_refresh_before_replacement() {
    crash_retirement_cancels_and_joins_refresh_before_replacement(
        "_security/heads/policy.json",
        "policy",
    )
    .await;
}

#[tokio::test]
async fn crash_retirement_cancels_and_joins_preservation_refresh_before_replacement() {
    crash_retirement_cancels_and_joins_refresh_before_replacement(
        "_security/preservation/heads/locks.json",
        "preservation",
    )
    .await;
}

#[tokio::test]
async fn crash_retirement_cancels_and_joins_signer_refresh_before_replacement() {
    crash_retirement_cancels_and_joins_refresh_before_replacement(
        "_security/signer-slots/",
        "delegation signer",
    )
    .await;
}
