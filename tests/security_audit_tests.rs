mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::http::HeaderMap;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use futures::future::join_all;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use zeppelin::config::{ApiKeyConfig, Config, SecurityMode};
use zeppelin::metrics::{
    AUDIT_FLUSH_FAILURES_TOTAL, AUDIT_RECORDS_TOTAL, AUTHZ_DENIALS_TOTAL, AUTH_FAILURES_TOTAL,
};
use zeppelin::security::Feature;
use zeppelin::security::{
    verify_audit_day, AuthenticationOutcome, AuthnFailure, CredentialAdapter, PolicyVersion,
};
use zeppelin::storage::ZeppelinStore;
use zeppelin::time::{Clock, TimeSource};

use common::counting::counting_store;
use common::fault_injection::{delay_delete_matching, fail_put_once_matching};
use common::harness::TestHarness;
use common::server::{
    client_with_bearer, create_ns_api, start_test_server_full,
    start_test_server_full_with_credential_adapter, start_test_server_full_with_entitlements,
    test_entitlements,
};

const READER_KEY_ID: &str = "zpk1_audit_reader";
const READER_SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

fn audit_config() -> Config {
    let mut config = Config::default();
    let digest = Sha256::digest(READER_SECRET.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    config.security.api_keys.push(ApiKeyConfig {
        key_id: READER_KEY_ID.to_string(),
        name: "audit-reader".to_string(),
        sha256_hex: digest,
        actions: vec!["NamespaceRead".to_string()],
        namespaces: vec!["*".to_string()],
        expires_at: None,
    });
    config
}

fn reader_bearer() -> String {
    format!("{READER_KEY_ID}.{READER_SECRET}")
}

struct AdvancingFailureAdapter {
    advanced: AtomicBool,
}

struct BecomesStaleDuringAuthenticationAdapter;

#[derive(Debug)]
struct FixedAuditClock(DateTime<Utc>);

impl TimeSource for FixedAuditClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

impl CredentialAdapter for BecomesStaleDuringAuthenticationAdapter {
    fn authenticate_with_policy(
        &self,
        _headers: &HeaderMap,
        _now: DateTime<Utc>,
    ) -> AuthenticationOutcome {
        AuthenticationOutcome {
            result: Err(AuthnFailure::CredentialUnknown),
            policy_version: PolicyVersion::persisted(2).unwrap(),
            policy_fresh: false,
        }
    }

    fn policy_freshness(&self) -> (PolicyVersion, bool) {
        (PolicyVersion::persisted(1).unwrap(), true)
    }
}

impl CredentialAdapter for AdvancingFailureAdapter {
    fn authenticate_with_policy(
        &self,
        _headers: &HeaderMap,
        _now: DateTime<Utc>,
    ) -> AuthenticationOutcome {
        self.advanced.store(true, Ordering::SeqCst);
        AuthenticationOutcome {
            result: Err(AuthnFailure::CredentialUnknown),
            policy_version: PolicyVersion::persisted(1).unwrap(),
            policy_fresh: true,
        }
    }

    fn policy_freshness(&self) -> (PolicyVersion, bool) {
        let version = if self.advanced.load(Ordering::SeqCst) {
            2
        } else {
            1
        };
        (PolicyVersion::persisted(version).unwrap(), true)
    }
}

async fn read_audit_objects(store: &ZeppelinStore, node_id: &str) -> Vec<(String, String)> {
    let mut keys = store.list_prefix("_audit/").await.unwrap();
    keys.retain(|key| key.contains(&format!("/{node_id}/")));
    keys.sort();
    let mut objects = Vec::with_capacity(keys.len());
    for key in keys {
        let bytes = store.get(&key).await.unwrap();
        objects.push((key, String::from_utf8(bytes.to_vec()).unwrap()));
    }
    objects
}

async fn read_audit_records(store: &ZeppelinStore, node_id: &str) -> Vec<Value> {
    read_audit_objects(store, node_id)
        .await
        .into_iter()
        .flat_map(|(_key, body)| {
            body.lines()
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_str(line).unwrap())
                .collect::<Vec<Value>>()
        })
        .collect()
}

fn record_for_request<'a>(records: &'a [Value], request_id: &str) -> &'a Value {
    let matching: Vec<_> = records
        .iter()
        .filter(|record| record["request_id"] == request_id)
        .collect();
    assert_eq!(
        matching.len(),
        1,
        "expected exactly one audit record for request {request_id}, got {matching:?}"
    );
    matching[0]
}

#[tokio::test]
async fn denial_writes_audit_record() {
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        audit_config(),
        false,
        None,
    )
    .await;
    let request_id = "audit-denial-exact";
    let response = client_with_bearer(&reader_bearer())
        .delete(format!(
            "{}/v1/namespaces/{}-denied",
            server.base_url, harness.prefix
        ))
        .header("x-request-id", request_id)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403);

    server.flush_audit().await;
    let records = read_audit_records(&harness.store, &server.audit_node_id).await;
    let record = record_for_request(&records, request_id);
    assert_eq!(record["principal_id"], READER_KEY_ID);
    assert_eq!(record["principal_kind"], "service");
    assert_eq!(record["action"], "NamespaceDelete");
    assert_eq!(record["policy_version"], 1);
    assert_eq!(record["outcome"]["denied"]["reason"], "action_not_granted");
    assert!(record["decision_id"].is_string());
    assert!(record["source_ip"].is_string());
    assert!(record["node_id"].is_string());
    assert!(record["prev_hash"].is_null());

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn authn_failure_writes_audit_record() {
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        audit_config(),
        false,
        None,
    )
    .await;
    let request_id = "audit-authn-failure-exact";
    let response = reqwest::Client::new()
        .get(format!("{}/readyz", server.base_url))
        .bearer_auth("zpk1_missing.BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
        .header("x-request-id", request_id)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401);

    server.flush_audit().await;
    let records = read_audit_records(&harness.store, &server.audit_node_id).await;
    let record = record_for_request(&records, request_id);
    assert_eq!(record["principal_id"], "anonymous");
    assert_eq!(record["principal_kind"], "anonymous");
    assert_eq!(
        record["outcome"]["authn_failed"]["reason"],
        "credential_unknown"
    );
    assert!(record["decision_id"].is_null());
    assert!(record["prev_hash"].is_null());

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn authn_failure_audits_the_snapshot_evaluated_before_policy_advances() {
    let harness = TestHarness::new().await;
    let adapter = Arc::new(AdvancingFailureAdapter {
        advanced: AtomicBool::new(false),
    });
    let server = start_test_server_full_with_credential_adapter(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        adapter,
    )
    .await;
    let request_id = "audit-authn-evaluated-version";

    let response = reqwest::Client::new()
        .get(format!("{}/v1/security/policy", server.base_url))
        .bearer_auth("zpk1_missing.BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
        .header("x-request-id", request_id)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401);

    server.flush_audit().await;
    let records = read_audit_records(&harness.store, &server.audit_node_id).await;
    let record = record_for_request(&records, request_id);
    assert_eq!(record["policy_version"], 1);
    assert_eq!(
        record["outcome"]["authn_failed"]["reason"],
        "credential_unknown"
    );

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn authentication_fails_closed_when_the_evaluated_snapshot_is_stale() {
    let harness = TestHarness::new().await;
    let server = start_test_server_full_with_credential_adapter(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        Arc::new(BecomesStaleDuringAuthenticationAdapter),
    )
    .await;
    let request_id = "audit-authn-atomic-staleness";

    let response = reqwest::Client::new()
        .get(format!("{}/v1/security/policy", server.base_url))
        .bearer_auth("zpk1_missing.BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
        .header("x-request-id", request_id)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403);

    server.flush_audit().await;
    let records = read_audit_records(&harness.store, &server.audit_node_id).await;
    let record = record_for_request(&records, request_id);
    assert_eq!(record["policy_version"], 2);
    assert_eq!(record["outcome"]["denied"]["reason"], "security_stale");

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn config_update_audits_old_and_new() {
    let harness = TestHarness::new().await;
    let (counted_store, counter) = counting_store(&harness.store);
    let server = start_test_server_full(
        counted_store,
        Some(harness.prefix.clone()),
        audit_config(),
        false,
        None,
    )
    .await;
    let client = client_with_bearer(&server.admin_bearer);
    counter.reset();
    let request_id = "audit-config-update-exact";
    let response = client
        .patch(format!("{}/v1/config/query", server.base_url))
        .header("x-request-id", request_id)
        .json(&json!({"default_top_k": 17}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(
        counter.puts_matching("_audit/") >= 1,
        "must_audit success returned before an audit object PUT"
    );

    // A must_audit response is not successful until the matching object is durable.
    let records = read_audit_records(&harness.store, &server.audit_node_id).await;
    let record = record_for_request(&records, request_id);
    assert_eq!(record["action"], "RuntimeConfigWrite");
    assert_ne!(
        record["params"]["runtime_config_update"]["old"],
        record["params"]["runtime_config_update"]["new"]
    );
    assert_eq!(
        record["params"]["runtime_config_update"]["new"]["default_top_k"],
        17
    );
    assert_eq!(record["outcome"], "success");

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn timeout_after_namespace_tombstone_still_writes_audit_record() {
    let harness = TestHarness::new().await;
    let mut config = audit_config();
    config.server.request_timeout_secs = 1;
    config.security.audit_flush_secs = 60;
    let delayed_store = delay_delete_matching(
        &harness.store,
        "/manifest.json",
        Duration::from_millis(1_500),
    );
    let server = start_test_server_full(
        delayed_store,
        Some(harness.prefix.clone()),
        config,
        false,
        None,
    )
    .await;
    let client = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&client, &server.base_url, 4).await;
    let request_id = "audit-delete-timeout-after-tombstone";

    let response = client
        .delete(format!("{}/v1/namespaces/{namespace}", server.base_url))
        .header("x-request-id", request_id)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 408);

    server.flush_audit().await;
    let records = read_audit_records(&harness.store, &server.audit_node_id).await;
    let record = record_for_request(&records, request_id);
    assert_eq!(record["action"], "NamespaceDelete");
    assert_eq!(record["resource"]["namespace"]["namespace"], namespace);
    assert_eq!(record["outcome"]["error"]["code"], "http_408");

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn namespace_delete_is_must_audit() {
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        audit_config(),
        false,
        None,
    )
    .await;
    let client = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&client, &server.base_url, 4).await;
    let request_id = "audit-namespace-delete-exact";
    let response = client
        .delete(format!("{}/v1/namespaces/{namespace}", server.base_url))
        .header("x-request-id", request_id)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 202);

    // DELETE may be asynchronous, but its success acknowledgement is audit-durable.
    let records = read_audit_records(&harness.store, &server.audit_node_id).await;
    let record = record_for_request(&records, request_id);
    assert_eq!(record["action"], "NamespaceDelete");
    assert_eq!(record["params"]["namespace_delete"]["namespace"], namespace);
    assert_eq!(record["outcome"], "success");

    server.shutdown().await;
    harness.cleanup().await;

    let failing_harness = TestHarness::new().await;
    let (failing_store, failure) = fail_put_once_matching(&failing_harness.store, ".jsonl");
    let mut config = audit_config();
    config.security.audit_flush_secs = 60;
    let failing_server = start_test_server_full(
        failing_store,
        Some(failing_harness.prefix.clone()),
        config,
        false,
        None,
    )
    .await;
    let client = client_with_bearer(&failing_server.admin_bearer);
    let namespace = create_ns_api(&client, &failing_server.base_url, 4).await;
    let flush_failures_before = AUDIT_FLUSH_FAILURES_TOTAL.get();
    let response = client
        .delete(format!(
            "{}/v1/namespaces/{namespace}",
            failing_server.base_url
        ))
        .header("x-request-id", "audit-delete-failure")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 500);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["code"], "audit_unavailable");
    assert_eq!(
        body["error"],
        "operation may have completed, but durable audit evidence is unavailable"
    );
    assert_eq!(failure.failures_injected(), 1);
    assert!(AUDIT_FLUSH_FAILURES_TOTAL.get() > flush_failures_before);

    let status = client
        .get(format!(
            "{}/v1/namespaces/{namespace}",
            failing_server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(status.status(), 200);
    let status_body: Value = status.json().await.unwrap();
    assert_eq!(status_body["state"], "deleting");

    failing_server.shutdown().await;
    failing_harness.cleanup().await;
}

#[tokio::test]
async fn no_secrets_in_audit() {
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        audit_config(),
        false,
        None,
    )
    .await;
    let secret = "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC";
    let response = reqwest::Client::new()
        .get(format!("{}/readyz", server.base_url))
        .bearer_auth(format!("zpk1_missing.{secret}"))
        .header("x-request-id", "audit-secret-redaction")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401);

    server.flush_audit().await;
    let objects = read_audit_objects(&harness.store, &server.audit_node_id).await;
    assert!(
        !objects.is_empty(),
        "redaction proof requires audit evidence"
    );
    for (key, body) in objects {
        assert!(!key.contains(secret));
        assert!(!body.contains(secret));
    }

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn audit_batching_bounds() {
    let harness = TestHarness::new().await;
    let (counted_store, counter) = counting_store(&harness.store);
    let mut config = audit_config();
    config.security.audit_flush_secs = 60;
    let server = start_test_server_full(
        counted_store,
        Some(harness.prefix.clone()),
        config,
        false,
        None,
    )
    .await;
    let client = client_with_bearer(&reader_bearer());
    counter.reset();

    // Issue all 257 denials, but cap how many connects are in flight at once.
    //
    // What this test pins is the audit *batch* bound: 257 denied requests must
    // collapse into exactly two audit objects. The simultaneity was incidental,
    // and on macOS it broke the test for a reason that has nothing to do with
    // the product: `kern.ipc.somaxconn` is 128, so a 257-way simultaneous
    // connect overruns the listener backlog and the kernel resets the
    // overflow. The failure landed deterministically on request 128 with
    // `ConnectionReset`, and reproduces at commits whose archived sweep logs
    // show this test passing — it tracks accept-queue drain speed, not any
    // change in behaviour.
    //
    // 64 keeps every request concurrent enough to exercise batching while
    // staying well inside the backlog on any supported host.
    const TOTAL: usize = 257;
    const IN_FLIGHT: usize = 64;
    let mut responses = Vec::with_capacity(TOTAL);
    let mut start = 0;
    while start < TOTAL {
        let end = (start + IN_FLIGHT).min(TOTAL);
        let requests = (start..end).map(|index| {
            client
                .delete(format!(
                    "{}/v1/namespaces/{}-batch-{index}",
                    server.base_url, harness.prefix
                ))
                .header("x-request-id", format!("audit-batch-{index}"))
                .send()
        });
        responses.extend(join_all(requests).await);
        start = end;
    }
    assert_eq!(responses.len(), TOTAL);
    for response in responses {
        assert_eq!(response.unwrap().status(), 403);
    }
    server.flush_audit().await;
    assert_eq!(counter.puts_matching("_audit/"), 2);

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn open_unsafe_boot_is_audited() {
    let harness = TestHarness::new().await;
    let mut config = Config::default();
    config.security.mode = SecurityMode::OpenUnsafe;
    let audit_now = Utc
        .with_ymd_and_hms(2020, 1, 14, 12, 0, 0)
        .single()
        .expect("historical open-unsafe audit timestamp must exist");
    let server = start_test_server_full_with_entitlements(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        config,
        Clock::from_source(Arc::new(FixedAuditClock(audit_now))),
        test_entitlements([Feature::AuditS3]),
    )
    .await;

    server.flush_audit().await;
    let records = read_audit_records(&harness.store, &server.audit_node_id).await;
    let boots: Vec<_> = records
        .iter()
        .filter(|record| record["params"] == "open_unsafe_boot")
        .collect();
    assert_eq!(boots.len(), 1);
    assert_eq!(boots[0]["principal_id"], "anonymous");
    assert_eq!(boots[0]["outcome"], "success");
    let boot_timestamp = DateTime::parse_from_rfc3339(
        boots[0]["ts"]
            .as_str()
            .expect("open-unsafe audit timestamp must be a string"),
    )
    .expect("open-unsafe audit timestamp must be RFC 3339")
    .with_timezone(&Utc);
    assert_eq!(boot_timestamp, audit_now);

    let audit_node_id = server.audit_node_id.clone();
    server.shutdown().await;
    let verification = verify_audit_day(&harness.store, audit_now.date_naive(), &audit_node_id)
        .await
        .expect("historical open-unsafe audit day verification must execute");
    assert!(verification.valid, "{verification:?}");
    assert_eq!(verification.verified_records, 1);
    harness.cleanup().await;
}

#[tokio::test]
async fn metrics_have_no_principal_labels() {
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        audit_config(),
        false,
        None,
    )
    .await;
    let authn_before = AUTH_FAILURES_TOTAL
        .with_label_values(&["unauthenticated"])
        .get();
    let denied_before = AUTHZ_DENIALS_TOTAL
        .with_label_values(&["NamespaceDelete"])
        .get();
    let authn_audit_before = AUDIT_RECORDS_TOTAL
        .with_label_values(&["authn_failed"])
        .get();
    let denied_audit_before = AUDIT_RECORDS_TOTAL.with_label_values(&["denied"]).get();

    let response = reqwest::Client::new()
        .get(format!("{}/readyz", server.base_url))
        .header("x-request-id", "audit-metrics-authn")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401);
    let denied = client_with_bearer(&reader_bearer())
        .delete(format!(
            "{}/v1/namespaces/{}-metrics-denied",
            server.base_url, harness.prefix
        ))
        .header("x-request-id", "audit-metrics-denied")
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 403);
    server.flush_audit().await;

    assert!(
        AUTH_FAILURES_TOTAL
            .with_label_values(&["unauthenticated"])
            .get()
            > authn_before
    );
    assert!(
        AUTHZ_DENIALS_TOTAL
            .with_label_values(&["NamespaceDelete"])
            .get()
            > denied_before
    );
    assert!(
        AUDIT_RECORDS_TOTAL
            .with_label_values(&["authn_failed"])
            .get()
            > authn_audit_before
    );
    assert!(AUDIT_RECORDS_TOTAL.with_label_values(&["denied"]).get() > denied_audit_before);

    let metrics = client_with_bearer(&server.admin_bearer)
        .get(format!("{}/metrics", server.base_url))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("zeppelin_auth_failures_total"));
    assert!(metrics.contains("zeppelin_authz_denials_total"));
    assert!(metrics.contains("zeppelin_audit_records_total"));
    assert!(metrics.contains("zeppelin_audit_flush_failures_total"));
    assert!(!metrics.contains("principal="));
    assert!(!metrics.contains("principal_id="));

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_harness_audit_chain_anchor_detects_persisted_record_drop() {
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        audit_config(),
        false,
        None,
    )
    .await;
    let node_id = server.audit_node_id.clone();
    let day = server.clock.now().date_naive();
    let response = reqwest::Client::new()
        .get(format!("{}/readyz", server.base_url))
        .header("x-request-id", "audit-chain-integration")
        .send()
        .await
        .expect("audited integration request must complete");
    assert_eq!(response.status(), 401);
    server.shutdown().await;

    let baseline = verify_audit_day(&harness.store, day, &node_id)
        .await
        .expect("signed audit chain must verify");
    assert!(baseline.valid, "{baseline:?}");
    assert!(baseline.verified_records > 0);

    let prefix = format!("_audit/{}/{node_id}/", day.format("%Y-%m-%d"));
    let mut keys = harness
        .store
        .list_prefix(&prefix)
        .await
        .expect("audit chain objects must list");
    keys.sort();
    let first = keys
        .first()
        .expect("audit chain must contain one JSONL object");
    let original = harness
        .store
        .get(first)
        .await
        .expect("audit chain object must read");
    harness
        .store
        .put(first, Bytes::new())
        .await
        .expect("out-of-band record drop must be injected");
    let broken = verify_audit_day(&harness.store, day, &node_id)
        .await
        .expect("mutated audit chain verification must execute");
    assert!(!broken.valid, "record drop must diverge: {broken:?}");
    harness
        .store
        .put(first, original)
        .await
        .expect("audit chain object must be restored");

    harness.cleanup().await;
}
