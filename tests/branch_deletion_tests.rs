#![cfg(feature = "branching-test-support")]

mod common;

use chrono::Utc;
use common::fault_injection::pause_next_cas_matching;
use common::server::{
    client_with_bearer, start_test_server_on_store_with_config, start_test_server_with_config,
};
use serde_json::{json, Value};
use zeppelin::config::Config;
use zeppelin::namespace::branching::test_support::{
    insert_prepared_branch_root, prepare_head_branch_root,
};
use zeppelin::namespace::branching::{BranchId, ForkViewDigest};
use zeppelin::namespace::NamespaceManager;
use zeppelin::types::DistanceMetric;

#[tokio::test]
async fn http_delete_source_with_live_child_is_a_non_mutating_conflict() {
    let mut config = Config::default();
    config.branching.enabled = true;
    config.security.policy_refresh_secs = 3_600;
    let (base_url, harness, _cache, _cache_dir, admin_bearer) =
        start_test_server_with_config(Some(config)).await;
    let client = client_with_bearer(&admin_bearer);
    let source = harness.artifact_origin_namespace("http-delete-source");
    let target = harness.artifact_origin_namespace("http-delete-child");

    let create = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&json!({
            "name": source,
            "dimensions": 4,
            "distance_metric": "cosine"
        }))
        .send()
        .await
        .expect("source create request must complete");
    assert_eq!(create.status(), reqwest::StatusCode::CREATED);

    let fork = client
        .post(format!("{base_url}/v1/namespaces/{source}/branches"))
        .json(&json!({ "target": target }))
        .send()
        .await
        .expect("fork request must complete");
    assert_eq!(fork.status(), reqwest::StatusCode::CREATED);

    let deletion = client
        .delete(format!("{base_url}/v1/namespaces/{source}"))
        .send()
        .await
        .expect("source delete request must complete");
    assert_eq!(deletion.status(), reqwest::StatusCode::CONFLICT);
    let body: Value = deletion
        .json()
        .await
        .expect("delete conflict must use the JSON error envelope");
    assert_eq!(body["code"], "namespace_has_live_branches");

    let source_get = client
        .get(format!("{base_url}/v1/namespaces/{source}"))
        .send()
        .await
        .expect("source status request must complete");
    assert_eq!(source_get.status(), reqwest::StatusCode::OK);

    harness.cleanup().await;
}

#[tokio::test]
async fn fork_root_winning_http_delete_race_keeps_source_active() {
    let harness = common::harness::TestHarness::new().await;
    let source = harness.artifact_origin_namespace("fork-wins-delete-source");
    let target = harness.artifact_origin_namespace("fork-wins-delete-target");
    let (store, deletion_meta_cas) =
        pause_next_cas_matching(&harness.store, format!("{source}/meta.json"));
    let mut config = Config::default();
    config.security.policy_refresh_secs = 3_600;
    let (base_url, _cache, _cache_dir, admin_bearer) =
        start_test_server_on_store_with_config(&harness, store.clone(), None, config).await;
    let client = client_with_bearer(&admin_bearer);

    NamespaceManager::new(store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .expect("source fixture must be created before arming the deletion CAS pause");
    deletion_meta_cas.arm();

    let delete_client = client.clone();
    let delete_url = format!("{base_url}/v1/namespaces/{source}");
    let mut deletion = tokio::spawn(async move {
        delete_client
            .delete(delete_url)
            .send()
            .await
            .expect("source delete request must complete")
    });
    tokio::select! {
        () = deletion_meta_cas.wait_until_paused() => {}
        response = &mut deletion => {
            let response = response.expect("delete task must not panic");
            panic!("delete returned {} before reaching its metadata CAS", response.status());
        }
    }
    let root = prepare_head_branch_root(
        store.clone(),
        &source,
        BranchId::new(),
        &target,
        uuid::Uuid::new_v4(),
        ForkViewDigest::new([0x5a; 32]),
        Utc::now(),
    )
    .await
    .expect("fork root candidate must bind the active source head");
    insert_prepared_branch_root(store, &source, root, 8)
        .await
        .expect("fork root must win while delete metadata CAS is paused");
    deletion_meta_cas.release();

    let deletion = deletion.await.expect("delete task must not panic");
    assert_eq!(deletion.status(), reqwest::StatusCode::CONFLICT);
    let body: Value = deletion
        .json()
        .await
        .expect("delete conflict must use the JSON error envelope");
    assert_eq!(body["code"], "namespace_has_live_branches");

    let status = client
        .get(format!("{base_url}/v1/namespaces/{source}"))
        .send()
        .await
        .expect("source status request must complete");
    assert_eq!(status.status(), reqwest::StatusCode::OK);
    let body: Value = status
        .json()
        .await
        .expect("source status must use the JSON namespace response");
    assert_eq!(body["state"], "active");

    harness.cleanup().await;
}
