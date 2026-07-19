mod common;

use std::time::Duration;

use common::fault_injection::synchronize_cas_pair_matching_payloads_with_winner;
use common::harness::TestHarness;
use common::server::{
    client_with_bearer, start_test_server_on_store_with_config, start_test_server_with_config,
};
use serde_json::{json, Value};
use zeppelin::config::Config;

#[tokio::test]
async fn authorized_http_fork_is_immediately_queryable_and_exact_retry_is_idempotent() {
    let mut config = Config::default();
    config.branching.enabled = true;
    config.security.policy_refresh_secs = 3_600;
    let (base_url, harness, _cache, _cache_dir, admin_bearer) =
        start_test_server_with_config(Some(config)).await;
    let client = client_with_bearer(&admin_bearer);
    let source = harness.artifact_origin_namespace("secure-fork-source");
    let target = harness.artifact_origin_namespace("secure-fork-target");

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

    let upsert = client
        .post(format!("{base_url}/v1/namespaces/{source}/vectors"))
        .json(&json!({
            "vectors": [{
                "id": "forked-row",
                "values": [1.0, 0.0, 0.0, 0.0]
            }]
        }))
        .send()
        .await
        .expect("source upsert request must complete");
    assert_eq!(upsert.status(), reqwest::StatusCode::OK);

    let fork = client
        .post(format!("{base_url}/v1/namespaces/{source}/branches"))
        .json(&json!({ "target": target }))
        .send()
        .await
        .expect("fork request must complete");
    assert_eq!(fork.status(), reqwest::StatusCode::CREATED);
    let first: Value = fork.json().await.expect("fork response must decode");
    assert_eq!(first["created"], true);

    let query = client
        .post(format!("{base_url}/v1/namespaces/{target}/query"))
        .json(&json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("target query request must complete");
    let query_status = query.status();
    let query_body: Value = query
        .json()
        .await
        .expect("target query response must decode");
    assert_eq!(
        query_status,
        reqwest::StatusCode::OK,
        "newly returned fork must already be active: {query_body}"
    );
    assert_eq!(query_body["results"][0]["id"], "forked-row");

    let retry = client
        .post(format!("{base_url}/v1/namespaces/{source}/branches"))
        .json(&json!({ "target": target }))
        .send()
        .await
        .expect("exact fork retry must complete");
    assert_eq!(retry.status(), reqwest::StatusCode::OK);
    let retried: Value = retry.json().await.expect("retry response must decode");
    assert_eq!(retried["created"], false);
    assert_eq!(retried["branch_id"], first["branch_id"]);

    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn activation_and_prepared_cancellation_cas_have_one_winner_without_resurrection() {
    const ACTIVE_PAYLOAD: &[u8] = b"\"state\": \"active\"";
    const CANCELLATION_PAYLOAD: &[u8] = b"\"deletion_intent\": {";

    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("activation-cancel-source");
    let cancelled_target = harness.artifact_origin_namespace("cancel-wins-target");
    let active_target = harness.artifact_origin_namespace("activation-wins-target");
    let (store, cancellation_wins) = synchronize_cas_pair_matching_payloads_with_winner(
        &harness.store,
        format!("{cancelled_target}/meta.json"),
        ACTIVE_PAYLOAD,
        CANCELLATION_PAYLOAD,
        CANCELLATION_PAYLOAD,
    );
    let (store, activation_wins) = synchronize_cas_pair_matching_payloads_with_winner(
        &store,
        format!("{active_target}/meta.json"),
        ACTIVE_PAYLOAD,
        CANCELLATION_PAYLOAD,
        ACTIVE_PAYLOAD,
    );
    let mut config = Config::default();
    config.branching.enabled = true;
    config.security.policy_refresh_secs = 3_600;
    config.cache.manifest_cache_ttl_ms = 0;
    config.cache.namespace_registry_ttl_ms = 0;
    let (base_url, _cache, _cache_dir, admin_bearer) =
        start_test_server_on_store_with_config(&harness, store, None, config).await;
    let client = client_with_bearer(&admin_bearer);

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
    let upsert = client
        .post(format!("{base_url}/v1/namespaces/{source}/vectors"))
        .json(&json!({
            "vectors": [{
                "id": "race-row",
                "values": [1.0, 0.0, 0.0, 0.0]
            }]
        }))
        .send()
        .await
        .expect("source upsert request must complete");
    assert_eq!(upsert.status(), reqwest::StatusCode::OK);

    // First force cancellation's exact intent CAS ahead of the target Active
    // CAS. Both writes have already captured the same ActivationPending ETag.
    cancellation_wins.enable();
    let fork_client = client.clone();
    let fork_url = format!("{base_url}/v1/namespaces/{source}/branches");
    let cancelled_target_for_fork = cancelled_target.clone();
    let cancelled_fork = tokio::spawn(async move {
        fork_client
            .post(fork_url)
            .json(&json!({ "target": cancelled_target_for_fork }))
            .send()
            .await
            .expect("cancel-winner fork request must complete")
    });
    tokio::time::timeout(
        Duration::from_secs(15),
        cancellation_wins.wait_until_arrivals(1),
    )
    .await
    .expect("fork activation must reach the exact target Active CAS");
    let delete_client = client.clone();
    let delete_url = format!("{base_url}/v1/namespaces/{cancelled_target}");
    let cancelled_delete = tokio::spawn(async move {
        delete_client
            .delete(delete_url)
            .send()
            .await
            .expect("cancel-winner delete request must complete")
    });
    tokio::time::timeout(
        Duration::from_secs(15),
        cancellation_wins.wait_until_arrivals(2),
    )
    .await
    .expect("prepared cancellation must reach its exact target intent CAS");
    let cancelled_fork = cancelled_fork
        .await
        .expect("cancel-winner fork task must not panic");
    let cancelled_delete = cancelled_delete
        .await
        .expect("cancel-winner delete task must not panic");
    assert_eq!(cancellation_wins.arrivals(), 2);
    assert_eq!(
        cancellation_wins.conflicts(),
        1,
        "the target Active and cancellation-intent writes must have one CAS winner"
    );
    let cancelled_fork_status = cancelled_fork.status();
    let cancelled_fork_body = cancelled_fork
        .text()
        .await
        .expect("cancel-winner fork response body must be readable");
    assert_eq!(
        cancelled_fork_status,
        reqwest::StatusCode::CONFLICT,
        "a lost Active CAS must not report the cancelled target as active: {cancelled_fork_body}"
    );
    let cancelled_fork_envelope: serde_json::Value = serde_json::from_str(&cancelled_fork_body)
        .expect("cancel-winner fork response must use the canonical JSON error envelope");
    assert_eq!(
        cancelled_fork_envelope["code"], "branch_intent_mismatch",
        "a durable cancellation winner is an intent conflict, not an internal error"
    );
    assert_eq!(cancelled_fork_envelope["retryable"], false);
    assert!(
        matches!(
            cancelled_delete.status(),
            reqwest::StatusCode::ACCEPTED | reqwest::StatusCode::CONFLICT
        ),
        "cancellation may await activation-guard recovery, but must not fail outside its resumable contract: {}",
        cancelled_delete.status()
    );

    let cancelled_query = client
        .post(format!("{base_url}/v1/namespaces/{cancelled_target}/query"))
        .json(&json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("cancelled target query must complete");
    assert_ne!(
        cancelled_query.status(),
        reqwest::StatusCode::OK,
        "the stale activation task must never resurrect the cancellation winner"
    );
    if cancelled_delete.status() == reqwest::StatusCode::CONFLICT {
        let retry = client
            .delete(format!("{base_url}/v1/namespaces/{cancelled_target}"))
            .send()
            .await
            .expect("cancel-winner delete retry must complete");
        assert_eq!(retry.status(), reqwest::StatusCode::ACCEPTED);
    }
    let cancelled_status = client
        .get(format!("{base_url}/v1/namespaces/{cancelled_target}"))
        .send()
        .await
        .expect("cancelled target status request must complete");
    assert_eq!(cancelled_status.status(), reqwest::StatusCode::NOT_FOUND);

    // Then force the exact Active payload first. The losing cancellation CAS
    // must not remove the already-linearized parent root.
    activation_wins.enable();
    let fork_client = client.clone();
    let fork_url = format!("{base_url}/v1/namespaces/{source}/branches");
    let active_target_for_fork = active_target.clone();
    let active_fork = tokio::spawn(async move {
        fork_client
            .post(fork_url)
            .json(&json!({ "target": active_target_for_fork }))
            .send()
            .await
            .expect("activation-winner fork request must complete")
    });
    tokio::time::timeout(
        Duration::from_secs(15),
        activation_wins.wait_until_arrivals(1),
    )
    .await
    .expect("second fork must reach the exact target Active CAS");
    let delete_client = client.clone();
    let delete_url = format!("{base_url}/v1/namespaces/{active_target}");
    let active_delete = tokio::spawn(async move {
        delete_client
            .delete(delete_url)
            .send()
            .await
            .expect("activation-winner delete request must complete")
    });
    tokio::time::timeout(
        Duration::from_secs(15),
        activation_wins.wait_until_arrivals(2),
    )
    .await
    .expect("second prepared cancellation must reach its exact target intent CAS");
    let active_fork = active_fork
        .await
        .expect("activation-winner fork task must not panic");
    let active_delete = active_delete
        .await
        .expect("activation-winner delete task must not panic");
    assert_eq!(activation_wins.arrivals(), 2);
    assert_eq!(
        activation_wins.conflicts(),
        1,
        "the reverse ordering must also have one target-metadata CAS winner"
    );
    assert_eq!(active_fork.status(), reqwest::StatusCode::CREATED);
    let active_delete_status = active_delete.status();
    let active_delete_body = active_delete
        .text()
        .await
        .expect("activation-winner delete response body must be readable");
    assert_eq!(
        active_delete_status,
        reqwest::StatusCode::CONFLICT,
        "the cancellation loser must not delete an active branch: {active_delete_body}"
    );
    let active_delete_envelope: serde_json::Value = serde_json::from_str(&active_delete_body)
        .expect("activation-winner delete response must use the canonical JSON error envelope");
    assert_eq!(active_delete_envelope["code"], "branch_target_exists");
    assert_eq!(active_delete_envelope["retryable"], false);

    let active_query = client
        .post(format!("{base_url}/v1/namespaces/{active_target}/query"))
        .json(&json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("active target query must complete");
    let active_query_status = active_query.status();
    let active_query_body: Value = active_query
        .json()
        .await
        .expect("active target query response must decode");
    assert_eq!(
        active_query_status,
        reqwest::StatusCode::OK,
        "losing cancellation must preserve active visibility: {active_query_body}"
    );
    assert_eq!(active_query_body["results"][0]["id"], "race-row");

    let branches = client
        .get(format!("{base_url}/v1/namespaces/{source}/branches"))
        .send()
        .await
        .expect("source branch listing must complete");
    assert_eq!(branches.status(), reqwest::StatusCode::OK);
    let branches: Value = branches.json().await.expect("branch listing must decode");
    assert_eq!(branches["branches"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        branches["branches"][0]["target"]["namespace"],
        active_target
    );
    assert_eq!(branches["branches"][0]["lifecycle"], "active");

    harness
        .cleanup_artifact_origin_namespace(&cancelled_target)
        .await;
    harness
        .cleanup_artifact_origin_namespace(&active_target)
        .await;
    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup().await;
}
