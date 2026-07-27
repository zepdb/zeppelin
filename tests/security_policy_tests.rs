mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::Utc;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use sha2::{Digest, Sha256};
use zeppelin::config::Config;
use zeppelin::error::ZeppelinError;
use zeppelin::metrics::AUTHZ_DENIALS_TOTAL;
use zeppelin::security::{
    Action, ApiKeyId, Decision, DenyReason, Entitlements, Feature, KeyState, PolicyHead,
    PolicySnapshot, RequestContext, Resource, SecurityKernel,
};
use zeppelin::storage::ZeppelinStore;
use zeppelin::time::{Clock, TimeSource};

use common::fault_injection::{
    delay_get_matching, fail_get_after_cas_conflict_matching, pause_next_cas_matching,
    toggle_cas_precondition_failure_matching, toggle_get_failure_matching,
};
use common::harness::TestHarness;
use common::server::{
    client_with_bearer, start_test_server_full,
    start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer, test_admin_bearer,
    test_entitlements,
};

fn scoped_store(harness: &TestHarness) -> ZeppelinStore {
    let scoped_backend =
        PrefixStore::new(harness.store.inner(), Path::from(harness.prefix.clone()));
    ZeppelinStore::new(Arc::new(scoped_backend))
}

fn rbac_entitlements() -> Arc<Entitlements> {
    Arc::new(test_entitlements([Feature::Rbac]))
}

#[derive(Debug)]
struct AdjustableSecurityClock(Mutex<chrono::DateTime<chrono::Utc>>);

impl AdjustableSecurityClock {
    fn new(now: chrono::DateTime<chrono::Utc>) -> Self {
        Self(Mutex::new(now))
    }

    fn advance(&self, duration: chrono::Duration) {
        let mut now = self
            .0
            .lock()
            .unwrap_or_else(|_| panic!("security test clock lock poisoned"));
        *now += duration;
    }
}

impl TimeSource for AdjustableSecurityClock {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        *self
            .0
            .lock()
            .unwrap_or_else(|_| panic!("security test clock lock poisoned"))
    }
}

async fn audit_records(store: &ZeppelinStore, node_id: &str) -> Vec<serde_json::Value> {
    let mut keys = store.list_prefix("_audit/").await.expect("audit LIST");
    keys.retain(|key| key.contains(&format!("/{node_id}/")));
    let mut records = Vec::new();
    for key in keys {
        let bytes = store.get(&key).await.expect("audit GET");
        records.extend(
            String::from_utf8(bytes.to_vec())
                .expect("audit JSONL must be UTF-8")
                .lines()
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_str(line).expect("audit line must be JSON")),
        );
    }
    records
}

#[tokio::test]
async fn bootstrap_publishes_v1_from_config() {
    let harness = TestHarness::new().await;
    let scoped_store = scoped_store(&harness);
    let server = start_test_server_full(scoped_store, None, Config::default(), false, None).await;
    let head_key = "_security/heads/policy.json";

    let head_bytes = server
        .store
        .get(head_key)
        .await
        .expect("enforced boot must publish the authoritative policy head");
    let head: PolicyHead =
        serde_json::from_slice(&head_bytes).expect("policy head must use the strict public schema");

    assert_eq!(head.version().get(), 1);
    assert!(head.object_key().starts_with("_security/policies/"));
    assert!(head.object_key().ends_with(".json"));

    let snapshot_bytes = server
        .store
        .get(head.object_key())
        .await
        .expect("the head must reference a reachable immutable snapshot");
    let snapshot: PolicySnapshot = serde_json::from_slice(&snapshot_bytes)
        .expect("policy snapshot must use the strict public schema");

    snapshot
        .verify_checksum()
        .expect("the snapshot checksum must cover canonical policy content");
    assert_eq!(snapshot.version(), head.version());
    assert_eq!(snapshot.checksum(), head.checksum());
    assert_eq!(snapshot.principals().len(), 1);
    assert_eq!(snapshot.keys().len(), 1);
    assert_eq!(snapshot.grants().len(), 1);

    let (key_id, secret) = server
        .admin_bearer
        .split_once('.')
        .expect("test admin bearer must contain key id and secret");
    let expected_digest = Sha256::digest(secret.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let key = &snapshot.keys()[0];
    assert_eq!(key.key_id().as_str(), key_id);
    assert_eq!(key.sha256_hex(), expected_digest);
    assert!(!snapshot_bytes
        .windows(secret.len())
        .any(|window| window == secret.as_bytes()));

    server.shutdown().await;
}

#[tokio::test]
async fn s3_policy_supersedes_drifted_boot_config() {
    let harness = TestHarness::new().await;
    let store = scoped_store(&harness);
    let first = start_test_server_full(store.clone(), None, Config::default(), false, None).await;
    let first_bearer = first.admin_bearer.clone();
    let second = start_test_server_full(store, None, Config::default(), false, None).await;

    assert_ne!(first_bearer, second.admin_bearer);

    let authoritative = client_with_bearer(&first_bearer)
        .get(format!("{}/readyz", second.base_url))
        .send()
        .await
        .expect("authoritative credential request must complete");
    assert_eq!(authoritative.status(), 200);

    let drifted = client_with_bearer(&second.admin_bearer)
        .get(format!("{}/readyz", second.base_url))
        .send()
        .await
        .expect("drifted credential request must complete");
    assert_eq!(drifted.status(), 401);
    let body: serde_json::Value = drifted.json().await.expect("401 must be JSON");
    assert_eq!(body["code"], "credential_unknown");

    first.shutdown().await;
    second.shutdown().await;
}

#[tokio::test]
async fn stale_policy_fails_closed() {
    let harness = TestHarness::new().await;
    let scoped = scoped_store(&harness);
    let (faulted_store, faults) =
        toggle_get_failure_matching(&scoped, "_security/heads/policy.json");
    let mut config = Config::default();
    config.security.policy_refresh_secs = 1;
    let server = start_test_server_full(faulted_store, None, config, false, None).await;
    let client = client_with_bearer(&server.admin_bearer);

    let warm = client
        .get(format!("{}/readyz", server.base_url))
        .send()
        .await
        .expect("warm policy request must complete");
    assert_eq!(warm.status(), 200);

    faults.enable();
    let deadline = Instant::now() + Duration::from_secs(4);
    while faults.failures_injected() < 2 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(faults.failures_injected() >= 2);
    tokio::time::sleep(Duration::from_millis(150)).await;

    let request_id = "phase3-stale-policy-denial";
    let stale = client
        .get(format!("{}/readyz", server.base_url))
        .header("x-request-id", request_id)
        .send()
        .await
        .expect("stale policy request must complete");
    assert_eq!(stale.status(), 403);
    let body: serde_json::Value = stale.json().await.expect("403 must be JSON");
    assert_eq!(body["code"], "security_stale");

    server.flush_audit().await;
    let matching = audit_records(&server.store, &server.audit_node_id)
        .await
        .into_iter()
        .filter(|record| record["request_id"] == request_id)
        .collect::<Vec<_>>();
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0]["policy_version"], 1);
    assert_eq!(matching[0]["outcome"]["denied"]["reason"], "security_stale");

    let unknown_secret = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    for (case, client) in [
        ("missing", reqwest::Client::new()),
        ("malformed", client_with_bearer("malformed")),
        (
            "unknown",
            client_with_bearer(&format!("zpk1_unknown.{unknown_secret}")),
        ),
    ] {
        let case_request_id = format!("phase3-stale-{case}");
        let response = client
            .get(format!("{}/readyz", server.base_url))
            .header("x-request-id", &case_request_id)
            .send()
            .await
            .expect("stale credential-shape request must complete");
        assert_eq!(response.status(), 403, "stale case {case}");
        let body: serde_json::Value = response.json().await.expect("403 must be JSON");
        assert_eq!(body["code"], "security_stale", "stale case {case}");
        server.flush_audit().await;
        let records = audit_records(&server.store, &server.audit_node_id)
            .await
            .into_iter()
            .filter(|record| record["request_id"] == case_request_id)
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 1, "stale audit case {case}");
        assert_eq!(records[0]["policy_version"], 1);
        assert_eq!(records[0]["outcome"]["denied"]["reason"], "security_stale");
    }

    faults.disable();
    let recovery_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let recovered = client
            .get(format!("{}/readyz", server.base_url))
            .send()
            .await
            .expect("recovery request must complete");
        if recovered.status() == 200 {
            break;
        }
        assert_eq!(recovered.status(), 403);
        assert!(Instant::now() < recovery_deadline, "policy did not recover");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    server.shutdown().await;
}

#[tokio::test]
async fn delayed_initial_snapshot_load_does_not_reset_freshness_origin() {
    let harness = TestHarness::new().await;
    let store = scoped_store(&harness);
    let mut config = Config::default();
    config.security.policy_refresh_secs = 1;
    let admin_bearer = test_admin_bearer(&mut config);
    let clock = Clock::system();
    let (_seed_kernel, seed_adapter) = SecurityKernel::from_resolved_entitlements(
        store.clone(),
        &config.security,
        clock.clone(),
        rbac_entitlements(),
    )
    .await
    .expect("seed policy runtime must bootstrap");
    let principal = seed_adapter
        .authenticate_bearer(&format!("Bearer {admin_bearer}"), clock.now())
        .expect("bootstrap administrator must authenticate");

    let delayed = delay_get_matching(&store, "_security/policies/", Duration::from_millis(2_250));
    let load_started = Instant::now();
    let (kernel, _adapter) = SecurityKernel::from_resolved_entitlements(
        delayed,
        &config.security,
        clock.clone(),
        rbac_entitlements(),
    )
    .await
    .expect("delayed policy runtime must load the authoritative snapshot");
    assert!(
        load_started.elapsed() >= Duration::from_secs(2),
        "fault seam must consume the complete 2x refresh budget"
    );

    let Decision::Deny(denied) = kernel.authorize(
        &principal,
        Action::SystemRead,
        &Resource::System,
        &RequestContext::new("delayed-initial-policy-load"),
    ) else {
        panic!("a snapshot loaded beyond the 2x refresh bound must fail closed");
    };
    assert_eq!(denied.reason, DenyReason::SecurityStale);
    assert_eq!(denied.policy_version.get(), 1);
}

#[tokio::test]
async fn create_key_returns_secret_once_digest_stored() {
    let harness = TestHarness::new().await;
    let server =
        start_test_server_full(scoped_store(&harness), None, Config::default(), false, None).await;
    let admin = client_with_bearer(&server.admin_bearer);

    let principal = admin
        .post(format!("{}/v1/security/principals", server.base_url))
        .json(&serde_json::json!({
            "principal_id": "service:search",
            "kind": "service",
            "display_name": "search-service"
        }))
        .send()
        .await
        .expect("principal creation request must complete");
    assert_eq!(principal.status(), 201);
    let principal_body: serde_json::Value = principal.json().await.expect("201 must be JSON");
    assert_eq!(principal_body["policy_version"], 2);
    assert_eq!(
        principal_body["principal"]["principal_id"],
        "service:search"
    );

    let created = admin
        .post(format!("{}/v1/security/keys", server.base_url))
        .json(&serde_json::json!({
            "principal_id": "service:search",
            "name": "search-primary"
        }))
        .send()
        .await
        .expect("key creation request must complete");
    assert_eq!(created.status(), 201);
    let created_body: serde_json::Value = created.json().await.expect("201 must be JSON");
    assert_eq!(created_body["policy_version"], 3);
    let key_id = created_body["key_id"]
        .as_str()
        .expect("creation response must contain key_id");
    let api_key = created_body["api_key"]
        .as_str()
        .expect("creation response must contain the one-time API key");
    let (bearer_key_id, secret) = api_key
        .split_once('.')
        .expect("created API key must use the bearer grammar");
    assert_eq!(bearer_key_id, key_id);

    let authenticated = client_with_bearer(api_key)
        .get(format!("{}/readyz", server.base_url))
        .send()
        .await
        .expect("new credential request must complete");
    assert_eq!(authenticated.status(), 403);
    let authenticated_body: serde_json::Value =
        authenticated.json().await.expect("403 must be JSON");
    assert_eq!(authenticated_body["code"], "forbidden");

    let keys = admin
        .get(format!("{}/v1/security/keys", server.base_url))
        .send()
        .await
        .expect("key listing request must complete");
    assert_eq!(keys.status(), 200);
    let keys_text = keys.text().await.expect("key listing body");
    assert!(keys_text.contains(key_id));
    assert!(!keys_text.contains(secret));
    assert!(!keys_text.contains(api_key));

    let principals = admin
        .get(format!("{}/v1/security/principals", server.base_url))
        .send()
        .await
        .expect("principal listing request must complete");
    assert_eq!(principals.status(), 200);
    let principals_text = principals.text().await.expect("principal listing body");
    assert!(!principals_text.contains(secret));
    assert!(!principals_text.contains(api_key));

    let head_bytes = server
        .store
        .get("_security/heads/policy.json")
        .await
        .expect("policy head must exist");
    let head: PolicyHead = serde_json::from_slice(&head_bytes).expect("strict policy head");
    let snapshot_bytes = server
        .store
        .get(head.object_key())
        .await
        .expect("active policy snapshot must exist");
    let snapshot: PolicySnapshot =
        serde_json::from_slice(&snapshot_bytes).expect("strict policy snapshot");
    let stored = snapshot
        .keys()
        .iter()
        .find(|key| key.key_id().as_str() == key_id)
        .expect("created key must be stored in the active snapshot");
    let expected_digest = Sha256::digest(secret.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(stored.sha256_hex(), expected_digest);
    assert!(!snapshot_bytes
        .windows(secret.len())
        .any(|window| window == secret.as_bytes()));
    assert!(!snapshot_bytes
        .windows(api_key.len())
        .any(|window| window == api_key.as_bytes()));

    server.shutdown().await;
}

#[tokio::test]
async fn revoked_key_denied_within_bound() {
    let harness = TestHarness::new().await;
    let store = scoped_store(&harness);
    let mut config = Config::default();
    config.security.policy_refresh_secs = 1;
    let writer = start_test_server_full(store.clone(), None, config.clone(), false, None).await;
    let admin = client_with_bearer(&writer.admin_bearer);

    let principal = admin
        .post(format!("{}/v1/security/principals", writer.base_url))
        .json(&serde_json::json!({
            "principal_id": "service:revocable",
            "kind": "service",
            "display_name": "revocable-service"
        }))
        .send()
        .await
        .expect("principal creation request must complete");
    assert_eq!(principal.status(), 201);

    let created = admin
        .post(format!("{}/v1/security/keys", writer.base_url))
        .json(&serde_json::json!({
            "principal_id": "service:revocable",
            "name": "revocable-primary"
        }))
        .send()
        .await
        .expect("key creation request must complete");
    assert_eq!(created.status(), 201);
    let created_body: serde_json::Value = created.json().await.expect("201 must be JSON");
    let key_id = created_body["key_id"]
        .as_str()
        .expect("creation response must contain key_id")
        .to_string();
    let api_key = created_body["api_key"]
        .as_str()
        .expect("creation response must contain one-time API key")
        .to_string();

    let reader = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        store,
        None,
        config,
        false,
        None,
        100 * 1024 * 1024,
        &writer.admin_bearer,
    )
    .await;
    let credential = client_with_bearer(&api_key);
    for base_url in [&writer.base_url, &reader.base_url] {
        let authenticated = credential
            .get(format!("{base_url}/readyz"))
            .send()
            .await
            .expect("pre-revocation request must complete");
        assert_eq!(authenticated.status(), 403);
        let body: serde_json::Value = authenticated
            .json()
            .await
            .expect("pre-revocation denial must be JSON");
        assert_eq!(
            body["code"], "forbidden",
            "pre-revocation credential must authenticate and reach authorization"
        );
    }

    let revoked = admin
        .delete(format!("{}/v1/security/keys/{key_id}", writer.base_url))
        .send()
        .await
        .expect("revocation request must complete");
    assert_eq!(revoked.status(), 200);
    let revoked_body: serde_json::Value = revoked.json().await.expect("200 must be JSON");
    assert_eq!(revoked_body["policy_version"], 4);
    assert_eq!(revoked_body["key_id"], key_id);

    let immediate = credential
        .get(format!("{}/readyz", writer.base_url))
        .send()
        .await
        .expect("same-node post-revocation request must complete");
    assert_eq!(immediate.status(), 401);
    let immediate_body: serde_json::Value = immediate.json().await.expect("401 must be JSON");
    assert_eq!(immediate_body["code"], "credential_unknown");

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let observed = credential
            .get(format!("{}/readyz", reader.base_url))
            .send()
            .await
            .expect("read-only node post-revocation request must complete");
        assert!(
            Instant::now() <= deadline,
            "revocation was not enforced within 2x the refresh interval"
        );
        if observed.status() == 401 {
            let body: serde_json::Value = observed.json().await.expect("401 must be JSON");
            assert_eq!(body["code"], "credential_unknown");
            break;
        }
        assert_eq!(observed.status(), 403);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    writer.shutdown().await;
    reader.shutdown().await;
}

#[tokio::test]
async fn explicit_revocation_is_independent_of_reader_clock_skew() {
    let harness = TestHarness::new().await;
    let store = scoped_store(&harness);
    let mut config = Config::default();
    config.security.policy_refresh_secs = 1;
    let writer_now = Utc::now();
    let writer_clock = Clock::from_source(Arc::new(AdjustableSecurityClock::new(writer_now)));
    let reader_clock = Clock::from_source(Arc::new(AdjustableSecurityClock::new(
        writer_now - chrono::Duration::hours(1),
    )));
    let writer = start_test_server_full(
        store.clone(),
        None,
        config.clone(),
        false,
        Some(writer_clock),
    )
    .await;
    let admin = client_with_bearer(&writer.admin_bearer);

    let principal = admin
        .post(format!("{}/v1/security/principals", writer.base_url))
        .json(&serde_json::json!({
            "principal_id": "service:skew-revocable",
            "kind": "service",
            "display_name": "skew revocable"
        }))
        .send()
        .await
        .expect("principal creation request must complete");
    assert_eq!(principal.status(), 201);
    let created = admin
        .post(format!("{}/v1/security/keys", writer.base_url))
        .json(&serde_json::json!({
            "principal_id": "service:skew-revocable",
            "name": "skew-revocable-primary"
        }))
        .send()
        .await
        .expect("key creation request must complete");
    assert_eq!(created.status(), 201);
    let created_body: serde_json::Value = created.json().await.expect("201 must be JSON");
    let key_id = created_body["key_id"]
        .as_str()
        .expect("creation response must contain key_id")
        .to_string();
    let api_key = created_body["api_key"]
        .as_str()
        .expect("creation response must contain one-time API key")
        .to_string();

    let reader = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        store,
        None,
        config,
        false,
        Some(reader_clock),
        100 * 1024 * 1024,
        &writer.admin_bearer,
    )
    .await;
    let credential = client_with_bearer(&api_key);
    let before = credential
        .get(format!("{}/readyz", reader.base_url))
        .send()
        .await
        .expect("pre-revocation skewed request must complete");
    assert_eq!(
        before.status(),
        403,
        "credential must authenticate before revoke"
    );
    let before_body: serde_json::Value = before
        .json()
        .await
        .expect("pre-revocation denial must be JSON");
    assert_eq!(
        before_body["code"], "forbidden",
        "pre-revocation credential must authenticate and reach authorization"
    );

    let revoked = admin
        .delete(format!("{}/v1/security/keys/{key_id}", writer.base_url))
        .send()
        .await
        .expect("revocation request must complete");
    assert_eq!(revoked.status(), 200);

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let observed = credential
            .get(format!("{}/readyz", reader.base_url))
            .send()
            .await
            .expect("skewed reader request must complete");
        assert!(
            Instant::now() <= deadline,
            "explicit revoke remained clock-dependent after the refresh bound"
        );
        if observed.status() == 401 {
            let body: serde_json::Value = observed.json().await.expect("401 must be JSON");
            assert_eq!(body["code"], "credential_unknown");
            break;
        }
        assert_eq!(observed.status(), 403);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    writer.shutdown().await;
    reader.shutdown().await;
}

#[tokio::test]
async fn mutation_reauthorization_uses_fresh_clock_after_overlap_expiry() {
    let harness = TestHarness::new().await;
    let store = scoped_store(&harness);
    let mut config = Config::default();
    let admin_bearer = test_admin_bearer(&mut config);
    let initial_now = Utc::now();
    let clock_source = Arc::new(AdjustableSecurityClock::new(initial_now));
    let clock = Clock::from_source(clock_source.clone());
    let (kernel, adapter) = SecurityKernel::from_resolved_entitlements(
        store,
        &config.security,
        clock.clone(),
        rbac_entitlements(),
    )
    .await
    .expect("S3 policy runtime must start");
    let actor = adapter
        .authenticate_bearer(&format!("Bearer {admin_bearer}"), initial_now)
        .expect("bootstrap administrator must authenticate");
    let key_id = ApiKeyId::new(admin_bearer.split_once('.').expect("test bearer grammar").0)
        .expect("test key id grammar");

    kernel
        .rotate_key(&actor, &key_id, 60)
        .await
        .expect("overlap rotation must publish");
    clock_source.advance(chrono::Duration::seconds(120));

    let error = kernel
        .create_principal(
            &actor,
            zeppelin::security::PrincipalId::new("service:stale-retry")
                .expect("principal id must be valid"),
            zeppelin::security::PrincipalKind::Service,
            "stale retry".to_string(),
        )
        .await
        .expect_err("expired overlap credential must not publish a later mutation")
        .into_error();
    assert!(matches!(
        error,
        ZeppelinError::Security(zeppelin::security::SecurityError::Authorization(
            DenyReason::CredentialUnknown
        ))
    ));
}

#[tokio::test]
async fn rotation_overlap_semantics() {
    let harness = TestHarness::new().await;
    let server =
        start_test_server_full(scoped_store(&harness), None, Config::default(), false, None).await;
    let old_bearer = server.admin_bearer.clone();
    let old_key_id = old_bearer
        .split_once('.')
        .expect("test admin bearer grammar")
        .0
        .to_string();
    let old_client = client_with_bearer(&old_bearer);

    let rotated = old_client
        .post(format!(
            "{}/v1/security/keys/{old_key_id}/rotate",
            server.base_url
        ))
        .json(&serde_json::json!({"overlap_secs": 0}))
        .send()
        .await
        .expect("rotation request must complete");
    assert_eq!(rotated.status(), 201);
    let body: serde_json::Value = rotated.json().await.expect("201 must be JSON");
    assert_eq!(body["policy_version"], 2);
    assert_eq!(body["rotated_from"], old_key_id);
    let new_key_id = body["key_id"]
        .as_str()
        .expect("rotation response must contain new key_id");
    let new_bearer = body["api_key"]
        .as_str()
        .expect("rotation response must return the new secret once");
    assert_ne!(new_key_id, old_key_id);

    let old_response = old_client
        .get(format!("{}/readyz", server.base_url))
        .send()
        .await
        .expect("old credential request must complete");
    assert_eq!(old_response.status(), 401);
    let old_body: serde_json::Value = old_response.json().await.expect("401 must be JSON");
    assert_eq!(old_body["code"], "credential_unknown");

    let new_response = client_with_bearer(new_bearer)
        .get(format!("{}/readyz", server.base_url))
        .send()
        .await
        .expect("new credential request must complete");
    assert_eq!(new_response.status(), 200);

    let head: PolicyHead = serde_json::from_slice(
        &server
            .store
            .get("_security/heads/policy.json")
            .await
            .expect("policy head must exist"),
    )
    .expect("strict policy head");
    let snapshot: PolicySnapshot = serde_json::from_slice(
        &server
            .store
            .get(head.object_key())
            .await
            .expect("rotated policy snapshot must exist"),
    )
    .expect("strict policy snapshot");
    let old_record = snapshot
        .keys()
        .iter()
        .find(|key| key.key_id().as_str() == old_key_id)
        .expect("old key record must remain immutable policy history");
    let new_record = snapshot
        .keys()
        .iter()
        .find(|key| key.key_id().as_str() == new_key_id)
        .expect("new key record must be present");
    assert_eq!(old_record.state(), KeyState::Revoked);
    assert_eq!(
        old_record.revokes_at(),
        None,
        "zero-overlap rotation must be clock-independent"
    );
    assert_eq!(new_record.state(), KeyState::Active);
    assert_eq!(
        new_record
            .rotated_from()
            .expect("rotation lineage")
            .as_str(),
        old_key_id
    );

    server.shutdown().await;
}

#[tokio::test]
async fn rotation_positive_overlap_accepts_old_key_only_until_deadline() {
    let harness = TestHarness::new().await;
    let server =
        start_test_server_full(scoped_store(&harness), None, Config::default(), false, None).await;
    let old_bearer = server.admin_bearer.clone();
    let old_key_id = old_bearer
        .split_once('.')
        .expect("test admin bearer grammar")
        .0;
    let old_client = client_with_bearer(&old_bearer);

    let rotated = old_client
        .post(format!(
            "{}/v1/security/keys/{old_key_id}/rotate",
            server.base_url
        ))
        .json(&serde_json::json!({"overlap_secs": 1}))
        .send()
        .await
        .expect("rotation request must complete");
    assert_eq!(rotated.status(), 201);
    let body: serde_json::Value = rotated.json().await.expect("201 must be JSON");
    let new_bearer = body["api_key"]
        .as_str()
        .expect("rotation response must return replacement secret")
        .to_string();

    let old_during_overlap = old_client
        .get(format!("{}/readyz", server.base_url))
        .send()
        .await
        .expect("old overlap credential request must complete");
    assert_eq!(old_during_overlap.status(), 200);
    let new_during_overlap = client_with_bearer(&new_bearer)
        .get(format!("{}/readyz", server.base_url))
        .send()
        .await
        .expect("new overlap credential request must complete");
    assert_eq!(new_during_overlap.status(), 200);

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let old_after_overlap = old_client
        .get(format!("{}/readyz", server.base_url))
        .send()
        .await
        .expect("expired overlap credential request must complete");
    assert_eq!(old_after_overlap.status(), 401);
    let old_body: serde_json::Value = old_after_overlap.json().await.expect("401 must be JSON");
    assert_eq!(old_body["code"], "credential_unknown");
    let new_after_overlap = client_with_bearer(&new_bearer)
        .get(format!("{}/readyz", server.base_url))
        .send()
        .await
        .expect("replacement credential request must complete");
    assert_eq!(new_after_overlap.status(), 200);

    server.shutdown().await;
}

#[tokio::test]
async fn rotation_overlap_predecessor_can_be_revoked_immediately() {
    let harness = TestHarness::new().await;
    let server =
        start_test_server_full(scoped_store(&harness), None, Config::default(), false, None).await;
    let old_bearer = server.admin_bearer.clone();
    let old_key_id = old_bearer
        .split_once('.')
        .expect("test admin bearer grammar")
        .0;
    let old_client = client_with_bearer(&old_bearer);

    let rotated = old_client
        .post(format!(
            "{}/v1/security/keys/{old_key_id}/rotate",
            server.base_url
        ))
        .json(&serde_json::json!({"overlap_secs": 60}))
        .send()
        .await
        .expect("rotation request must complete");
    assert_eq!(rotated.status(), 201);
    let body: serde_json::Value = rotated.json().await.expect("201 must be JSON");
    let new_bearer = body["api_key"]
        .as_str()
        .expect("rotation response must return replacement secret")
        .to_string();

    let still_overlapping = old_client
        .get(format!("{}/readyz", server.base_url))
        .send()
        .await
        .expect("old overlap credential request must complete");
    assert_eq!(still_overlapping.status(), 200);

    let revoked = old_client
        .delete(format!("{}/v1/security/keys/{old_key_id}", server.base_url))
        .send()
        .await
        .expect("emergency revocation request must complete");
    assert_eq!(revoked.status(), 200);
    let revoke_body: serde_json::Value = revoked.json().await.expect("200 must be JSON");
    assert_eq!(revoke_body["policy_version"], 3);

    let old_after_revoke = old_client
        .get(format!("{}/readyz", server.base_url))
        .send()
        .await
        .expect("revoked overlap credential request must complete");
    assert_eq!(old_after_revoke.status(), 401);
    let old_body: serde_json::Value = old_after_revoke.json().await.expect("401 must be JSON");
    assert_eq!(old_body["code"], "credential_unknown");

    let replacement = client_with_bearer(&new_bearer)
        .get(format!("{}/readyz", server.base_url))
        .send()
        .await
        .expect("replacement credential request must complete");
    assert_eq!(replacement.status(), 200);

    server.shutdown().await;
}

#[tokio::test]
async fn revoked_credential_cannot_authorize_captured_principal() {
    let harness = TestHarness::new().await;
    let store = scoped_store(&harness);
    let mut config = Config::default();
    let admin_bearer = test_admin_bearer(&mut config);
    let clock = Clock::system();
    let (kernel, adapter) = SecurityKernel::from_resolved_entitlements(
        store,
        &config.security,
        clock.clone(),
        rbac_entitlements(),
    )
    .await
    .expect("S3 policy runtime must start");
    let principal = adapter
        .authenticate_bearer(&format!("Bearer {admin_bearer}"), clock.now())
        .expect("bootstrap administrator must authenticate");
    let key_id = ApiKeyId::new(admin_bearer.split_once('.').expect("test bearer grammar").0)
        .expect("test key id grammar");

    kernel
        .revoke_key(&principal, &key_id)
        .await
        .expect("self-revocation must publish");
    let decision = kernel.authorize(
        &principal,
        Action::SystemRead,
        &Resource::System,
        &RequestContext::new("captured-principal-after-revoke"),
    );
    let Decision::Deny(deny) = decision else {
        panic!("a revoked credential's captured principal must not authorize");
    };
    assert_eq!(deny.reason, DenyReason::CredentialUnknown);
    assert_eq!(deny.policy_version.get(), 2);
}

#[tokio::test]
async fn malformed_key_path_is_400_without_policy_change() {
    let harness = TestHarness::new().await;
    let server =
        start_test_server_full(scoped_store(&harness), None, Config::default(), false, None).await;
    let admin = client_with_bearer(&server.admin_bearer);

    for (method, suffix) in [
        (reqwest::Method::DELETE, "not-a-key"),
        (reqwest::Method::POST, "not-a-key/rotate"),
    ] {
        let response = admin
            .request(
                method,
                format!("{}/v1/security/keys/{suffix}", server.base_url),
            )
            .json(&serde_json::json!({"overlap_secs": 0}))
            .send()
            .await
            .expect("invalid key request must complete");
        assert_eq!(response.status(), 400);
        let body: serde_json::Value = response.json().await.expect("400 must be JSON");
        assert_eq!(body["code"], "invalid_security_request");
    }

    let head: PolicyHead = serde_json::from_slice(
        &server
            .store
            .get("_security/heads/policy.json")
            .await
            .expect("policy head must remain reachable"),
    )
    .expect("strict policy head");
    assert_eq!(head.version().get(), 1);

    server.shutdown().await;
}

#[tokio::test]
async fn namespace_rbac_grants() {
    let harness = TestHarness::new().await;
    let server =
        start_test_server_full(scoped_store(&harness), None, Config::default(), false, None).await;
    let admin = client_with_bearer(&server.admin_bearer);
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let namespace_a = format!("rbac-a-{suffix}");
    let namespace_b = format!("rbac-b-{suffix}");
    for namespace in [&namespace_a, &namespace_b] {
        let created = admin
            .post(format!("{}/v1/namespaces", server.base_url))
            .json(&serde_json::json!({"name": namespace, "dimensions": 2}))
            .send()
            .await
            .expect("namespace creation must complete");
        assert_eq!(created.status(), 201);
    }

    let principal_id = "service:rbac-worker";
    let principal = admin
        .post(format!("{}/v1/security/principals", server.base_url))
        .json(&serde_json::json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": "rbac-worker"
        }))
        .send()
        .await
        .expect("principal creation must complete");
    assert_eq!(principal.status(), 201);
    let key = admin
        .post(format!("{}/v1/security/keys", server.base_url))
        .json(&serde_json::json!({
            "principal_id": principal_id,
            "name": "rbac-worker-primary"
        }))
        .send()
        .await
        .expect("key creation must complete");
    assert_eq!(key.status(), 201);
    let key_body: serde_json::Value = key.json().await.expect("201 must be JSON");
    let worker_bearer = key_body["api_key"]
        .as_str()
        .expect("key response must include one-time credential")
        .to_string();

    let query_grant = admin
        .post(format!("{}/v1/security/grants", server.base_url))
        .json(&serde_json::json!({
            "principal_id": principal_id,
            "scope": {"kind": "namespace", "namespace": namespace_a},
            "actions": {"kind": "selected", "actions": ["Query"]}
        }))
        .send()
        .await
        .expect("query grant request must complete");
    assert_eq!(query_grant.status(), 201);
    let query_grant_body: serde_json::Value = query_grant.json().await.expect("201 JSON");
    assert_eq!(query_grant_body["policy_version"], 4);

    let worker = client_with_bearer(&worker_bearer);
    let query_a = worker
        .post(format!(
            "{}/v1/namespaces/{namespace_a}/query",
            server.base_url
        ))
        .json(&serde_json::json!({
            "vector": [1.0, 0.0],
            "top_k": 1,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("authorized query must complete");
    assert_eq!(query_a.status(), 200);

    let query_b = worker
        .post(format!(
            "{}/v1/namespaces/{namespace_b}/query",
            server.base_url
        ))
        .json(&serde_json::json!({
            "vector": [1.0, 0.0],
            "top_k": 1,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("cross-namespace query must complete");
    assert_eq!(query_b.status(), 403);
    let query_b_body: serde_json::Value = query_b.json().await.expect("403 JSON");
    assert_eq!(query_b_body["code"], "namespace_not_granted");

    let denied_upsert = worker
        .post(format!(
            "{}/v1/namespaces/{namespace_a}/vectors",
            server.base_url
        ))
        .json(&serde_json::json!({
            "vectors": [{"id": "denied", "values": [1.0, 0.0]}]
        }))
        .send()
        .await
        .expect("denied upsert must complete");
    assert_eq!(denied_upsert.status(), 403);
    let denied_upsert_body: serde_json::Value =
        denied_upsert.json().await.expect("403 must be JSON");
    assert_eq!(denied_upsert_body["code"], "forbidden");

    let write_grant = admin
        .post(format!("{}/v1/security/grants", server.base_url))
        .json(&serde_json::json!({
            "principal_id": principal_id,
            "scope": {"kind": "namespace", "namespace": namespace_a},
            "actions": {"kind": "selected", "actions": ["VectorUpsert"]}
        }))
        .send()
        .await
        .expect("writer grant request must complete");
    assert_eq!(write_grant.status(), 201);
    let write_grant_body: serde_json::Value = write_grant.json().await.expect("201 JSON");
    assert_eq!(write_grant_body["policy_version"], 5);

    let allowed_upsert = worker
        .post(format!(
            "{}/v1/namespaces/{namespace_a}/vectors",
            server.base_url
        ))
        .json(&serde_json::json!({
            "vectors": [{"id": "allowed", "values": [1.0, 0.0]}]
        }))
        .send()
        .await
        .expect("authorized upsert must complete");
    assert_eq!(allowed_upsert.status(), 200);

    for (destructive_action, path) in [
        ("NamespaceDelete", format!("/v1/namespaces/{namespace_a}")),
        (
            "SnapshotDelete",
            format!("/v1/namespaces/{namespace_a}/snapshots/not-granted"),
        ),
        (
            "VectorDelete",
            format!("/v1/namespaces/{namespace_a}/vectors"),
        ),
    ] {
        let denied = worker
            .delete(format!("{}{path}", server.base_url))
            .json(&serde_json::json!({"ids": ["allowed"]}))
            .send()
            .await
            .unwrap_or_else(|error| panic!("denied {destructive_action} must complete: {error}"));
        assert_eq!(
            denied.status(),
            403,
            "selected namespace read/write grants must not imply {destructive_action}"
        );
        let body: serde_json::Value = denied.json().await.expect("403 must be JSON");
        assert_eq!(body["code"], "forbidden");
    }

    server.shutdown().await;
}

#[tokio::test]
async fn policy_cas_conflict_second_writer_gets_retryable_conflict() {
    let harness = TestHarness::new().await;
    let (store, publication) =
        pause_next_cas_matching(&scoped_store(&harness), "_security/heads/policy.json");
    let server = start_test_server_full(store, None, Config::default(), false, None).await;
    let admin = client_with_bearer(&server.admin_bearer);
    for principal_id in ["service:cas-a", "service:cas-b"] {
        let response = admin
            .post(format!("{}/v1/security/principals", server.base_url))
            .json(&serde_json::json!({
                "principal_id": principal_id,
                "kind": "service",
                "display_name": principal_id
            }))
            .send()
            .await
            .expect("principal setup must complete");
        assert_eq!(response.status(), 201);
    }

    publication.arm();
    let grant_a_admin = admin.clone();
    let grant_a_url = format!("{}/v1/security/grants", server.base_url);
    let grant_a = tokio::spawn(async move {
        grant_a_admin
            .post(grant_a_url)
            .json(&serde_json::json!({
                "principal_id": "service:cas-a",
                "scope": {"kind": "global"},
                "actions": {"kind": "selected", "actions": ["SystemRead"]}
            }))
            .send()
            .await
            .expect("first concurrent grant must complete")
    });
    publication.wait_until_paused().await;
    let grant_b = admin
        .post(format!("{}/v1/security/grants", server.base_url))
        .json(&serde_json::json!({
            "principal_id": "service:cas-b",
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": ["MetricsRead"]}
        }))
        .send()
        .await
        .expect("second concurrent grant must complete");
    assert_eq!(grant_b.status(), 409);
    assert_eq!(
        grant_b
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok()),
        Some("1")
    );
    let conflict_body: serde_json::Value =
        grant_b.json().await.expect("conflicting grant must be JSON");
    assert_eq!(conflict_body["code"], "security_conflict");
    assert_eq!(conflict_body["retryable"], true);

    publication.release();
    let grant_a = grant_a.await.expect("first concurrent grant task must join");
    assert_eq!(grant_a.status(), 201);
    let published_body: serde_json::Value =
        grant_a.json().await.expect("published grant must be JSON");
    assert_eq!(published_body["policy_version"], 4);

    let head: PolicyHead = serde_json::from_slice(
        &server
            .store
            .get("_security/heads/policy.json")
            .await
            .expect("policy head must exist"),
    )
    .expect("strict policy head");
    assert_eq!(head.version().get(), 4);
    let active: PolicySnapshot = serde_json::from_slice(
        &server
            .store
            .get(head.object_key())
            .await
            .expect("active policy snapshot must exist"),
    )
    .expect("strict active policy");
    assert!(active
        .grants()
        .iter()
        .any(|grant| grant.principal_id().as_str() == "service:cas-a"));
    assert!(!active
        .grants()
        .iter()
        .any(|grant| grant.principal_id().as_str() == "service:cas-b"));

    let policy_keys = server
        .store
        .list_prefix("_security/policies/")
        .await
        .expect("immutable policy LIST must succeed");
    let mut version_four_objects = 0;
    let mut version_five_objects = 0;
    for key in policy_keys {
        let snapshot: PolicySnapshot = serde_json::from_slice(
            &server
                .store
                .get(&key)
                .await
                .expect("every listed immutable snapshot must remain reachable"),
        )
        .expect("every immutable object must retain strict policy JSON");
        snapshot
            .verify_checksum()
            .expect("every immutable snapshot checksum must remain valid");
        match snapshot.version().get() {
            4 => version_four_objects += 1,
            5 => version_five_objects += 1,
            _ => {}
        }
    }
    assert_eq!(version_four_objects, 1);
    assert_eq!(version_five_objects, 0);

    server.shutdown().await;
}

#[tokio::test]
async fn policy_cas_conflict_storm_is_bounded_and_retryable() {
    let harness = TestHarness::new().await;
    let (store, faults) = toggle_cas_precondition_failure_matching(
        &scoped_store(&harness),
        "_security/heads/policy.json",
    );
    let server = start_test_server_full(store, None, Config::default(), false, None).await;
    let admin = client_with_bearer(&server.admin_bearer);
    faults.enable();
    let request_id = "policy-cas-conflict-latest-allow";

    let response = admin
        .post(format!("{}/v1/security/principals", server.base_url))
        .header("x-request-id", request_id)
        .json(&serde_json::json!({
            "principal_id": "service:never-published",
            "kind": "service",
            "display_name": "never-published"
        }))
        .send()
        .await
        .expect("bounded conflict request must complete");
    assert_eq!(response.status(), 409);
    assert_eq!(
        response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok()),
        Some("1")
    );
    let body: serde_json::Value = response.json().await.expect("409 must be JSON");
    assert_eq!(body["code"], "security_conflict");
    assert_eq!(body["retryable"], true);
    assert_eq!(faults.failures_injected(), 25);

    let head: PolicyHead = serde_json::from_slice(
        &server
            .store
            .get("_security/heads/policy.json")
            .await
            .expect("original head must remain reachable"),
    )
    .expect("strict policy head");
    assert_eq!(head.version().get(), 1);
    let active: PolicySnapshot = serde_json::from_slice(
        &server
            .store
            .get(head.object_key())
            .await
            .expect("active bootstrap snapshot must remain reachable"),
    )
    .expect("strict active policy");
    assert!(!active
        .principals()
        .iter()
        .any(|principal| { principal.principal_id().as_str() == "service:never-published" }));

    server.flush_audit().await;
    let records = audit_records(&server.store, &server.audit_node_id).await;
    let record = records
        .iter()
        .find(|record| record["request_id"] == request_id)
        .expect("bounded conflict audit record");
    assert_eq!(record["policy_version"], 1);
    assert_eq!(record["outcome"]["error"]["code"], "security_conflict");
    assert!(record["decision_id"].is_string());

    server.shutdown().await;
}

#[tokio::test]
async fn security_admin_disjoint() {
    let harness = TestHarness::new().await;
    let server =
        start_test_server_full(scoped_store(&harness), None, Config::default(), false, None).await;
    let admin = client_with_bearer(&server.admin_bearer);
    let principal_id = "service:data-plane-admin";
    let principal = admin
        .post(format!("{}/v1/security/principals", server.base_url))
        .json(&serde_json::json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": "data-plane-admin"
        }))
        .send()
        .await
        .expect("principal creation must complete");
    assert_eq!(principal.status(), 201);
    let key = admin
        .post(format!("{}/v1/security/keys", server.base_url))
        .json(&serde_json::json!({
            "principal_id": principal_id,
            "name": "data-plane-admin-primary"
        }))
        .send()
        .await
        .expect("key creation must complete");
    assert_eq!(key.status(), 201);
    let key_body: serde_json::Value = key.json().await.expect("201 must be JSON");
    let key_id = key_body["key_id"]
        .as_str()
        .expect("key response must contain key_id")
        .to_string();
    let bearer = key_body["api_key"]
        .as_str()
        .expect("key response must contain one-time credential")
        .to_string();
    let grant = admin
        .post(format!("{}/v1/security/grants", server.base_url))
        .json(&serde_json::json!({
            "principal_id": principal_id,
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": [
                "SystemRead", "MetricsRead", "RuntimeConfigRead", "RuntimeConfigWrite",
                "NamespaceCreate", "NamespaceRead", "NamespaceDelete", "SnapshotRead",
                "SnapshotWrite", "SnapshotDelete", "NamespaceClone", "IndexConfigWrite",
                "CompactionTrigger", "CompactionStatusRead", "HydrationTrigger",
                "VectorFetch", "VectorUpsert", "VectorDelete", "Query"
            ]}
        }))
        .send()
        .await
        .expect("data-plane grant must complete");
    assert_eq!(grant.status(), 201);

    let data_plane = client_with_bearer(&bearer);
    let mutations = vec![
        (
            reqwest::Method::POST,
            "/v1/security/principals".to_string(),
            serde_json::json!({
                "principal_id": "service:should-not-exist",
                "kind": "service",
                "display_name": "should-not-exist"
            }),
        ),
        (
            reqwest::Method::POST,
            "/v1/security/keys".to_string(),
            serde_json::json!({"principal_id": principal_id, "name": "forbidden-key"}),
        ),
        (
            reqwest::Method::DELETE,
            format!("/v1/security/keys/{key_id}"),
            serde_json::json!({}),
        ),
        (
            reqwest::Method::POST,
            format!("/v1/security/keys/{key_id}/rotate"),
            serde_json::json!({"overlap_secs": 0}),
        ),
        (
            reqwest::Method::POST,
            "/v1/security/grants".to_string(),
            serde_json::json!({
                "principal_id": principal_id,
                "scope": {"kind": "global"},
                "actions": {"kind": "selected", "actions": ["SecurityAdminWrite"]}
            }),
        ),
        (
            reqwest::Method::DELETE,
            "/v1/security/grants".to_string(),
            serde_json::json!({
                "principal_id": principal_id,
                "scope": {"kind": "global"},
                "actions": {"kind": "selected", "actions": ["SystemRead"]}
            }),
        ),
    ];
    for (method, path, body) in mutations {
        let response = data_plane
            .request(method, format!("{}{}", server.base_url, path))
            .json(&body)
            .send()
            .await
            .expect("disjoint admin request must complete");
        assert_eq!(response.status(), 403, "mutation path {path}");
        let body: serde_json::Value = response.json().await.expect("403 must be JSON");
        assert_eq!(body["code"], "forbidden", "mutation path {path}");
    }

    let read = data_plane
        .get(format!("{}/v1/security/policy", server.base_url))
        .send()
        .await
        .expect("disjoint admin read must complete");
    assert_eq!(read.status(), 403);
    let read_body: serde_json::Value = read.json().await.expect("403 must be JSON");
    assert_eq!(read_body["code"], "forbidden");

    let head: PolicyHead = serde_json::from_slice(
        &server
            .store
            .get("_security/heads/policy.json")
            .await
            .expect("policy head must remain reachable"),
    )
    .expect("strict policy head");
    assert_eq!(head.version().get(), 4);
    let active: PolicySnapshot = serde_json::from_slice(
        &server
            .store
            .get(head.object_key())
            .await
            .expect("active policy must remain reachable"),
    )
    .expect("strict active policy");
    assert!(!active
        .principals()
        .iter()
        .any(|principal| { principal.principal_id().as_str() == "service:should-not-exist" }));
    assert_eq!(
        active
            .keys()
            .iter()
            .find(|key| key.key_id().as_str() == key_id)
            .expect("data-plane key must remain present")
            .state(),
        KeyState::Active
    );

    server.shutdown().await;
}

#[tokio::test]
async fn policy_survives_namespace_delete() {
    let harness = TestHarness::new().await;
    let server =
        start_test_server_full(scoped_store(&harness), None, Config::default(), false, None).await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = format!("reserved-delete-{}", uuid::Uuid::new_v4().simple());
    let created = admin
        .post(format!("{}/v1/namespaces", server.base_url))
        .json(&serde_json::json!({"name": namespace, "dimensions": 2}))
        .send()
        .await
        .expect("namespace creation must complete");
    assert_eq!(created.status(), 201);
    server.flush_audit().await;

    let security_keys_before = server
        .store
        .list_prefix("_security/")
        .await
        .expect("security LIST before delete");
    let audit_keys_before = server
        .store
        .list_prefix("_audit/")
        .await
        .expect("audit LIST before delete");
    let mut reserved_before = std::collections::BTreeMap::new();
    for key in security_keys_before.iter().chain(audit_keys_before.iter()) {
        reserved_before.insert(
            key.clone(),
            server
                .store
                .get(key)
                .await
                .expect("reserved object must be readable before delete"),
        );
    }

    let deleted = admin
        .delete(format!("{}/v1/namespaces/{namespace}", server.base_url))
        .send()
        .await
        .expect("namespace deletion must complete");
    assert_eq!(deleted.status(), 202);
    server.flush_audit().await;

    let security_keys_after = server
        .store
        .list_prefix("_security/")
        .await
        .expect("security LIST after delete");
    assert_eq!(security_keys_after, security_keys_before);
    let audit_keys_after = server
        .store
        .list_prefix("_audit/")
        .await
        .expect("audit LIST after delete");
    for key in &audit_keys_before {
        assert!(audit_keys_after.contains(key));
    }
    for (key, before) in reserved_before {
        let after = server
            .store
            .get(&key)
            .await
            .expect("pre-existing reserved object must remain reachable");
        assert_eq!(after, before, "reserved object changed: {key}");
    }

    let still_authorized = admin
        .get(format!("{}/readyz", server.base_url))
        .send()
        .await
        .expect("policy-backed readiness must complete after namespace deletion");
    assert_eq!(still_authorized.status(), 200);

    server.shutdown().await;
}

#[tokio::test]
async fn security_admin_events_use_actual_policy_versions() {
    let harness = TestHarness::new().await;
    let server =
        start_test_server_full(scoped_store(&harness), None, Config::default(), false, None).await;
    let admin = client_with_bearer(&server.admin_bearer);
    let read_request_id = "phase3-policy-read-audit";
    let write_request_id = "phase3-policy-write-audit";

    let read = admin
        .get(format!("{}/v1/security/policy", server.base_url))
        .header("x-request-id", read_request_id)
        .send()
        .await
        .expect("policy read must complete");
    assert_eq!(read.status(), 200);
    let read_body: serde_json::Value = read.json().await.expect("policy metadata JSON");
    assert_eq!(read_body["policy_version"], 1);

    let write = admin
        .post(format!("{}/v1/security/principals", server.base_url))
        .header("x-request-id", write_request_id)
        .json(&serde_json::json!({
            "principal_id": "service:audit-version",
            "kind": "service",
            "display_name": "audit-version"
        }))
        .send()
        .await
        .expect("policy mutation must complete");
    assert_eq!(write.status(), 201);
    let write_body: serde_json::Value = write.json().await.expect("mutation JSON");
    assert_eq!(write_body["policy_version"], 2);

    server.flush_audit().await;
    let records = audit_records(&server.store, &server.audit_node_id).await;
    let read_record = records
        .iter()
        .find(|record| record["request_id"] == read_request_id)
        .expect("policy read audit record");
    assert_eq!(read_record["action"], "SecurityAdminRead");
    assert_eq!(read_record["resource"], "security_policy");
    assert_eq!(read_record["policy_version"], 1);
    assert_eq!(read_record["params"]["security_policy_read"]["version"], 1);
    assert_eq!(read_record["outcome"], "success");

    let write_record = records
        .iter()
        .find(|record| record["request_id"] == write_request_id)
        .expect("policy write audit record");
    assert_eq!(write_record["action"], "SecurityAdminWrite");
    assert_eq!(write_record["resource"], "security_policy");
    assert_eq!(write_record["policy_version"], 1);
    assert_eq!(
        write_record["params"]["security_policy_change"]["old_version"],
        1
    );
    assert_eq!(
        write_record["params"]["security_policy_change"]["new_version"],
        2
    );
    assert_eq!(write_record["outcome"], "success");

    server.shutdown().await;
}

#[tokio::test]
async fn security_admin_mutation_audits_fresh_authoritative_denial() {
    let harness = TestHarness::new().await;
    let store = scoped_store(&harness);
    let writer_store = ZeppelinStore::new(store.inner());
    let mut config = Config::default();
    config.security.policy_refresh_secs = 60;
    let stale = start_test_server_full(store.clone(), None, config.clone(), false, None).await;
    let writer = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        writer_store,
        None,
        config,
        false,
        None,
        100 * 1024 * 1024,
        &stale.admin_bearer,
    )
    .await;
    let key_id = stale
        .admin_bearer
        .split_once('.')
        .expect("test admin bearer grammar")
        .0;
    let revoke = client_with_bearer(&writer.admin_bearer)
        .delete(format!("{}/v1/security/keys/{key_id}", writer.base_url))
        .header("x-request-id", "fresh-base-denial-revoke")
        .send()
        .await
        .expect("authoritative revoke must complete");
    assert_eq!(revoke.status(), 200);

    let denials_before = AUTHZ_DENIALS_TOTAL
        .with_label_values(&["SecurityAdminWrite"])
        .get();
    let request_id = "fresh-base-denial-audit";
    let denied = client_with_bearer(&stale.admin_bearer)
        .post(format!("{}/v1/security/principals", stale.base_url))
        .header("x-request-id", request_id)
        .json(&serde_json::json!({
            "principal_id": "service:must-not-publish",
            "kind": "service",
            "display_name": "must not publish"
        }))
        .send()
        .await
        .expect("stale-node mutation request must complete");
    assert_eq!(denied.status(), 401);

    stale.flush_audit().await;
    let records = audit_records(&stale.store, &stale.audit_node_id).await;
    let record = records
        .iter()
        .find(|record| record["request_id"] == request_id)
        .expect("fresh-base denial audit record");
    assert_eq!(record["policy_version"], 2);
    assert_eq!(record["outcome"]["denied"]["reason"], "credential_unknown");
    assert!(record["decision_id"].is_string());
    assert!(
        AUTHZ_DENIALS_TOTAL
            .with_label_values(&["SecurityAdminWrite"])
            .get()
            > denials_before
    );

    writer.shutdown().await;
    stale.shutdown().await;
}

#[tokio::test]
async fn security_admin_mutation_audits_latest_allow_before_build_error() {
    let harness = TestHarness::new().await;
    let store = scoped_store(&harness);
    let writer_store = ZeppelinStore::new(store.inner());
    let mut config = Config::default();
    config.security.policy_refresh_secs = 60;
    let stale = start_test_server_full(store.clone(), None, config.clone(), false, None).await;
    let writer = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        writer_store,
        None,
        config,
        false,
        None,
        100 * 1024 * 1024,
        &stale.admin_bearer,
    )
    .await;
    let advance = client_with_bearer(&writer.admin_bearer)
        .post(format!("{}/v1/security/principals", writer.base_url))
        .header("x-request-id", "fresh-base-error-advance")
        .json(&serde_json::json!({
            "principal_id": "service:fresh-base-existing",
            "kind": "service",
            "display_name": "fresh base existing"
        }))
        .send()
        .await
        .expect("authoritative policy advance must complete");
    assert_eq!(advance.status(), 201);

    let request_id = "fresh-base-build-error-audit";
    let failed = client_with_bearer(&stale.admin_bearer)
        .post(format!("{}/v1/security/keys", stale.base_url))
        .header("x-request-id", request_id)
        .json(&serde_json::json!({
            "principal_id": "service:missing-from-policy",
            "name": "must fail after authorization"
        }))
        .send()
        .await
        .expect("stale-node key request must complete");
    assert_eq!(failed.status(), 404);

    stale.flush_audit().await;
    let records = audit_records(&stale.store, &stale.audit_node_id).await;
    let record = records
        .iter()
        .find(|record| record["request_id"] == request_id)
        .expect("fresh-base build-error audit record");
    assert_eq!(record["policy_version"], 2);
    assert_eq!(
        record["outcome"]["error"]["code"],
        "security_entity_not_found"
    );
    assert!(record["decision_id"].is_string());

    writer.shutdown().await;
    stale.shutdown().await;
}

#[tokio::test]
async fn security_admin_read_surfaces_cache_swap_denial_for_handler_audit() {
    let harness = TestHarness::new().await;
    let store = scoped_store(&harness);
    let mut config = Config::default();
    config.security.policy_refresh_secs = 1;
    let admin_bearer = test_admin_bearer(&mut config);
    let clock = Clock::system();
    let (reader_kernel, reader_adapter) = SecurityKernel::from_resolved_entitlements(
        store.clone(),
        &config.security,
        clock.clone(),
        rbac_entitlements(),
    )
    .await
    .expect("reader policy cache must start");
    let (writer_kernel, writer_adapter) = SecurityKernel::from_resolved_entitlements(
        store,
        &config.security,
        clock.clone(),
        rbac_entitlements(),
    )
    .await
    .expect("writer policy cache must start");
    let actor = reader_adapter
        .authenticate_bearer(&format!("Bearer {admin_bearer}"), clock.now())
        .expect("reader actor must authenticate against V1");
    let writer_actor = writer_adapter
        .authenticate_bearer(&format!("Bearer {admin_bearer}"), clock.now())
        .expect("writer actor must authenticate against V1");
    let outer = reader_kernel.authorize(
        &actor,
        Action::SecurityAdminRead,
        &Resource::SecurityPolicy,
        &RequestContext::at("read-before-cache-swap", clock.now()),
    );
    let Decision::Allow(outer_allow) = outer else {
        panic!("outer read must authorize before the cache swap");
    };
    assert_eq!(outer_allow.policy_version.get(), 1);

    let key_id =
        ApiKeyId::new(admin_bearer.split_once('.').unwrap().0).expect("test key id must be valid");
    writer_kernel
        .revoke_key(&writer_actor, &key_id)
        .await
        .expect("writer must publish V2 revocation");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let decision = reader_kernel.authorize(
                &actor,
                Action::SecurityAdminRead,
                &Resource::SecurityPolicy,
                &RequestContext::at("wait-for-read-cache-swap", clock.now()),
            );
            if matches!(
                decision,
                Decision::Deny(ref deny) if deny.policy_version.get() == 2
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("reader cache must observe V2");

    let error = reader_kernel
        .policy_snapshot(&actor, clock.now())
        .expect_err("handler-time read must deny under the swapped cache");
    let Some(Decision::Deny(deny)) = error.decision() else {
        panic!("read failure must carry the handler-time deny decision");
    };
    assert_eq!(deny.policy_version.get(), 2);
    assert_eq!(deny.reason, DenyReason::CredentialUnknown);

    drop(reader_kernel);
    drop(reader_adapter);
    drop(writer_kernel);
    drop(writer_adapter);
    harness.cleanup().await;
}

#[tokio::test]
async fn security_admin_retry_load_error_audits_latest_authoritative_allow() {
    let harness = TestHarness::new().await;
    let base_store = scoped_store(&harness);
    let (faulted_store, cas_faults, get_faults) = fail_get_after_cas_conflict_matching(
        &base_store,
        "_security/heads/policy.json",
        "_security/heads/policy.json",
    );
    let mut config = Config::default();
    config.security.policy_refresh_secs = 60;
    let stale = start_test_server_full(faulted_store, None, config.clone(), false, None).await;
    let writer = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        base_store,
        None,
        config,
        false,
        None,
        100 * 1024 * 1024,
        &stale.admin_bearer,
    )
    .await;
    let advance = client_with_bearer(&writer.admin_bearer)
        .post(format!("{}/v1/security/principals", writer.base_url))
        .header("x-request-id", "retry-load-error-advance")
        .json(&serde_json::json!({
            "principal_id": "service:retry-load-v2",
            "kind": "service",
            "display_name": "retry load V2"
        }))
        .send()
        .await
        .expect("writer must publish V2");
    assert_eq!(advance.status(), 201);

    let request_id = "retry-load-error-audit";
    let failed = client_with_bearer(&stale.admin_bearer)
        .post(format!("{}/v1/security/principals", stale.base_url))
        .header("x-request-id", request_id)
        .json(&serde_json::json!({
            "principal_id": "service:retry-load-never-published",
            "kind": "service",
            "display_name": "retry load never published"
        }))
        .send()
        .await
        .expect("faulted mutation must return a canonical response");
    assert_eq!(failed.status(), 500);
    let body: serde_json::Value = failed.json().await.expect("500 must be JSON");
    assert_eq!(body["code"], "STORAGE_ERROR");
    assert_eq!(cas_faults.failures_injected(), 1);
    assert_eq!(get_faults.failures_injected(), 1);

    stale.flush_audit().await;
    let records = audit_records(&stale.store, &stale.audit_node_id).await;
    let record = records
        .iter()
        .find(|record| record["request_id"] == request_id)
        .expect("retry load-error audit record");
    assert_eq!(record["policy_version"], 2);
    assert_eq!(record["outcome"]["error"]["code"], "STORAGE_ERROR");
    assert!(record["decision_id"].is_string());

    writer.shutdown().await;
    stale.shutdown().await;
}

#[tokio::test]
async fn grant_list_and_delete_publish_exact_versions() {
    let harness = TestHarness::new().await;
    let server =
        start_test_server_full(scoped_store(&harness), None, Config::default(), false, None).await;
    let admin = client_with_bearer(&server.admin_bearer);
    let principal_id = "service:grant-lifecycle";
    let principal = admin
        .post(format!("{}/v1/security/principals", server.base_url))
        .json(&serde_json::json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": "grant-lifecycle"
        }))
        .send()
        .await
        .expect("principal creation must complete");
    assert_eq!(principal.status(), 201);

    let grant_body = serde_json::json!({
        "principal_id": principal_id,
        "scope": {"kind": "global"},
        "actions": {"kind": "selected", "actions": ["SystemRead"]}
    });
    let created = admin
        .post(format!("{}/v1/security/grants", server.base_url))
        .json(&grant_body)
        .send()
        .await
        .expect("grant creation must complete");
    assert_eq!(created.status(), 201);
    let created: serde_json::Value = created.json().await.expect("201 JSON");
    assert_eq!(created["policy_version"], 3);
    assert_eq!(created["grant"]["principal_id"], principal_id);

    let listed = admin
        .get(format!("{}/v1/security/grants", server.base_url))
        .send()
        .await
        .expect("grant list must complete");
    assert_eq!(listed.status(), 200);
    let listed: serde_json::Value = listed.json().await.expect("200 JSON");
    assert_eq!(listed["policy_version"], 3);
    assert_eq!(
        listed["grants"]
            .as_array()
            .expect("grant array")
            .iter()
            .filter(|grant| grant["principal_id"] == principal_id)
            .count(),
        1
    );

    let deleted = admin
        .delete(format!("{}/v1/security/grants", server.base_url))
        .json(&grant_body)
        .send()
        .await
        .expect("grant deletion must complete");
    assert_eq!(deleted.status(), 200);
    let deleted: serde_json::Value = deleted.json().await.expect("200 JSON");
    assert_eq!(deleted["policy_version"], 4);
    assert_eq!(deleted["grant"]["principal_id"], principal_id);

    let listed = admin
        .get(format!("{}/v1/security/grants", server.base_url))
        .send()
        .await
        .expect("post-delete grant list must complete");
    assert_eq!(listed.status(), 200);
    let listed: serde_json::Value = listed.json().await.expect("200 JSON");
    assert_eq!(listed["policy_version"], 4);
    assert!(!listed["grants"]
        .as_array()
        .expect("grant array")
        .iter()
        .any(|grant| grant["principal_id"] == principal_id));

    server.shutdown().await;
}
