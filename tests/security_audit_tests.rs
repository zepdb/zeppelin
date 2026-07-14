mod common;

use std::time::Duration;

use futures::future::join_all;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use zeppelin::config::{ApiKeyConfig, Config, SecurityMode};
use zeppelin::metrics::{
    AUDIT_FLUSH_FAILURES_TOTAL, AUDIT_RECORDS_TOTAL, AUTHZ_DENIALS_TOTAL, AUTH_FAILURES_TOTAL,
};
use zeppelin::storage::ZeppelinStore;

use common::counting::counting_store;
use common::fault_injection::{delay_delete_matching, fail_put_once_matching};
use common::harness::TestHarness;
use common::server::{client_with_bearer, create_ns_api, start_test_server_full};

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
    assert_eq!(record["policy_version"], 0);
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
    let (failing_store, failure) = fail_put_once_matching(&failing_harness.store, "_audit/");
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

    let requests = (0..257).map(|index| {
        client
            .delete(format!(
                "{}/v1/namespaces/{}-batch-{index}",
                server.base_url, harness.prefix
            ))
            .header("x-request-id", format!("audit-batch-{index}"))
            .send()
    });
    let responses = join_all(requests).await;
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
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        config,
        false,
        None,
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

    server.shutdown().await;
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
