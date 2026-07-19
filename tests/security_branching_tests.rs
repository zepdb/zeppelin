mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use common::fault_injection::{
    pause_next_cas_matching_payload, synchronize_cas_pair_matching_payloads_with_winner,
    toggle_cas_precondition_failure_matching,
};
use common::harness::TestHarness;
use common::server::{
    client_with_bearer, scoped_test_security_store, start_test_server_full,
    start_test_server_on_store_with_config, start_test_server_with_config,
};
use serde_json::{json, Value};
use zeppelin::config::Config;
use zeppelin::namespace::manager::NamespaceMetadata;
use zeppelin::time::{Clock, TimeSource};

#[derive(Debug)]
struct AdjustableActivationClock(Mutex<DateTime<Utc>>);

impl AdjustableActivationClock {
    fn new(now: DateTime<Utc>) -> Self {
        Self(Mutex::new(now))
    }

    fn advance(&self, duration: ChronoDuration) {
        let mut now = self
            .0
            .lock()
            .expect("activation test clock mutex must not be poisoned");
        *now += duration;
    }
}

impl TimeSource for AdjustableActivationClock {
    fn now(&self) -> DateTime<Utc> {
        *self
            .0
            .lock()
            .expect("activation test clock mutex must not be poisoned")
    }
}

fn assert_sha256_hex(value: &Value, field: &str) {
    let digest = value[field]
        .as_str()
        .unwrap_or_else(|| panic!("{field} must be a string in {value}"));
    assert_eq!(digest.len(), 64, "{field} must be one SHA-256 digest");
    assert!(
        digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "{field} must contain only hexadecimal digits: {digest}"
    );
}

#[tokio::test]
async fn readyz_reports_bounded_orphan_root_repair_identity_without_storage_or_policy_data() {
    let harness = TestHarness::new().await;
    let mut config = Config::default();
    config.branching.enabled = true;
    config.security.policy_refresh_secs = 3_600;
    let (base_url, _cache, _cache_dir, admin_bearer) = start_test_server_on_store_with_config(
        &harness,
        harness.store.clone(),
        Some(harness.prefix.clone()),
        config,
    )
    .await;
    let client = client_with_bearer(&admin_bearer);
    let source = harness.artifact_origin_namespace("ready-orphan-source");
    let target = harness.artifact_origin_namespace("ready-orphan-target");

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
    let fork_body: Value = fork.json().await.expect("fork response must decode");

    let healthy = client
        .get(format!("{base_url}/readyz"))
        .send()
        .await
        .expect("healthy readiness request must complete");
    assert_eq!(healthy.status(), reqwest::StatusCode::OK);
    assert_eq!(
        healthy.json::<Value>().await.unwrap(),
        json!({"status": "ready", "s3_connected": true})
    );

    harness
        .store
        .delete(&NamespaceMetadata::s3_key(&target))
        .await
        .expect("fixture must remove only the exact child metadata object");

    let response = client
        .get(format!("{base_url}/readyz"))
        .send()
        .await
        .expect("orphan-root readiness request must complete");
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let body: Value = response
        .json()
        .await
        .expect("readiness response must decode");
    assert_eq!(body["status"], "not_ready");
    assert_eq!(body["s3_connected"], true);
    assert_eq!(body["branch_graph_healthy"], false);
    assert_eq!(
        body["error"],
        "branch graph integrity requires operator repair"
    );

    let repair = &body["operator_repair"];
    assert_eq!(repair["orphan_branch_roots_limit"], 16);
    assert_eq!(repair["has_additional_orphan_branch_roots"], false);
    let findings = repair["orphan_branch_roots"]
        .as_array()
        .expect("repair bundle must contain a bounded finding array");
    assert!(
        findings.len() <= 16,
        "repair bundle exceeded its wire limit"
    );
    assert_eq!(findings.len(), 1);
    let finding = &findings[0];
    assert_eq!(finding["source_namespace"], source);
    assert_eq!(finding["branch_id"], fork_body["branch_id"]);
    assert_eq!(finding["target_namespace"], target);
    assert_eq!(
        finding["target_incarnation"],
        fork_body["target"]["incarnation"]
    );
    assert_eq!(
        finding["source_generation"],
        fork_body["source"]["generation"]
    );
    assert_sha256_hex(finding, "source_manifest_sha256");
    assert_sha256_hex(finding, "fork_view_sha256");
    assert_sha256_hex(finding, "source_config_sha256");
    let finding_keys = finding
        .as_object()
        .expect("repair finding must be an object")
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        finding_keys,
        std::collections::BTreeSet::from([
            "branch_id",
            "fork_view_sha256",
            "source_config_sha256",
            "source_generation",
            "source_manifest_sha256",
            "source_namespace",
            "target_incarnation",
            "target_namespace",
        ]),
        "operator repair findings may expose only identities and digests"
    );
    let raw = body.to_string();
    for forbidden in [
        "http://",
        "https://",
        "manifest.json",
        "meta.json",
        "bucket",
        "access_key",
        "secret",
        "policy",
        "created_at",
    ] {
        assert!(
            !raw.contains(forbidden),
            "readiness repair bundle leaked forbidden data {forbidden:?}: {raw}"
        );
    }

    let mut public_config = Config::default();
    public_config.branching.enabled = true;
    public_config.security.readyz_public = true;
    public_config.security.policy_refresh_secs = 3_600;
    let (public_base_url, _public_cache, _public_cache_dir, _public_admin_bearer) =
        start_test_server_on_store_with_config(
            &harness,
            harness.store.clone(),
            Some(harness.prefix.clone()),
            public_config,
        )
        .await;
    let public_response = reqwest::Client::new()
        .get(format!("{public_base_url}/readyz"))
        .send()
        .await
        .expect("public orphan-root readiness request must complete");
    assert_eq!(
        public_response.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    let public_body: Value = public_response
        .json()
        .await
        .expect("public readiness response must decode");
    assert_eq!(public_body["status"], "not_ready");
    assert_eq!(public_body["s3_connected"], true);
    assert_eq!(public_body["branch_graph_healthy"], false);
    assert!(
        public_body.get("operator_repair").is_none(),
        "public readiness must not disclose the operator repair bundle: {public_body}"
    );
    let public_raw = public_body.to_string();
    for private_identity in [
        source.as_str(),
        target.as_str(),
        fork_body["branch_id"].as_str().unwrap(),
        fork_body["target"]["incarnation"].as_str().unwrap(),
    ] {
        assert!(
            !public_raw.contains(private_identity),
            "public readiness leaked branch identity {private_identity:?}: {public_raw}"
        );
    }

    harness.cleanup().await;
}

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
async fn exact_active_retry_finalizes_a_crash_retained_policy_guard() {
    const ACTIVE_PAYLOAD: &[u8] = b"\"state\": \"active\"";

    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("retained-guard-source");
    let target = harness.artifact_origin_namespace("retained-guard-target");
    let scoped_store = scoped_test_security_store(&harness.store, &harness.prefix);
    let (store, target_activation) = pause_next_cas_matching_payload(
        &scoped_store,
        format!("{target}/meta.json"),
        ACTIVE_PAYLOAD,
    );
    let (store, guard_cleanup_failure) =
        toggle_cas_precondition_failure_matching(&store, "_security/heads/policy.json");
    let wall_clock = Arc::new(AdjustableActivationClock::new(Utc::now()));
    let clock = Clock::from_source(wall_clock.clone());
    let mut config = Config::default();
    config.branching.enabled = true;
    config.security.policy_refresh_secs = 3_600;
    let server = start_test_server_full(store, None, config, false, Some(clock)).await;
    let client = client_with_bearer(&server.admin_bearer);

    let create = client
        .post(format!("{}/v1/namespaces", server.base_url))
        .json(&json!({
            "name": source,
            "dimensions": 4,
            "distance_metric": "cosine"
        }))
        .send()
        .await
        .expect("source create request must complete");
    assert_eq!(create.status(), reqwest::StatusCode::CREATED);

    target_activation.arm();
    let fork_client = client.clone();
    let fork_url = format!("{}/v1/namespaces/{source}/branches", server.base_url);
    let target_for_fork = target.clone();
    let fork = tokio::spawn(async move {
        fork_client
            .post(fork_url)
            .json(&json!({ "target": target_for_fork }))
            .send()
            .await
            .expect("fork request must complete")
    });
    tokio::time::timeout(
        Duration::from_secs(15),
        target_activation.wait_until_paused(),
    )
    .await
    .expect("guarded activation must reach the target Active CAS");
    guard_cleanup_failure.enable();
    target_activation.release();
    let fork = tokio::time::timeout(Duration::from_secs(30), fork)
        .await
        .expect("fork must finish after the target activation is released")
        .expect("fork task must not panic");
    let fork_status = fork.status();
    let fork_body = fork
        .text()
        .await
        .expect("fork response body must be readable");
    assert_eq!(
        fork_status,
        reqwest::StatusCode::CREATED,
        "committed activation must survive lost guard cleanup: body={fork_body} policy_cas_failures={}",
        guard_cleanup_failure.failures_injected()
    );
    assert_eq!(
        guard_cleanup_failure.failures_injected(),
        1,
        "the committed activation must lose exactly its first guard-finalization CAS"
    );

    guard_cleanup_failure.disable();
    wall_clock.advance(ChronoDuration::seconds(31));
    let retry = client
        .post(format!(
            "{}/v1/namespaces/{source}/branches",
            server.base_url
        ))
        .json(&json!({ "target": target }))
        .send()
        .await
        .expect("exact active retry must complete");
    let retry_status = retry.status();
    let retry_body: Value = retry
        .json()
        .await
        .expect("exact active retry response must decode");
    assert_eq!(
        retry_status,
        reqwest::StatusCode::OK,
        "exact active retry must converge the retained guard: {retry_body}"
    );
    assert_eq!(retry_body["created"], false);

    let policy_mutation = client
        .post(format!("{}/v1/security/principals", server.base_url))
        .json(&json!({
            "principal_id": "service:post-activation-guard",
            "kind": "service",
            "display_name": "post activation guard"
        }))
        .send()
        .await
        .expect("policy mutation after guard recovery must complete");
    let mutation_status = policy_mutation.status();
    let mutation_body: Value = policy_mutation
        .json()
        .await
        .expect("policy mutation response must decode");
    assert_eq!(
        mutation_status,
        reqwest::StatusCode::CREATED,
        "active retry must remove the guard before a later policy mutation: {mutation_body}"
    );

    server.shutdown().await;
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
