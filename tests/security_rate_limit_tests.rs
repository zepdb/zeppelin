mod common;

use common::server::{
    client_with_bearer, start_test_server_full_without_rate_limit_override,
    start_test_server_with_config, start_test_server_with_config_no_limit_override,
};
use serde_json::{json, Value};
use zeppelin::config::Config;

use common::harness::TestHarness;

const SHARED_CLIENT_IP: &str = "203.0.113.70";

async fn audit_record_for_request(
    store: &zeppelin::storage::ZeppelinStore,
    node_id: &str,
    request_id: &str,
) -> Value {
    let mut records = Vec::new();
    for key in store.list_prefix("_audit/").await.unwrap() {
        if !key.contains(&format!("/{node_id}/")) {
            continue;
        }
        let body = store.get(&key).await.unwrap();
        for line in String::from_utf8(body.to_vec()).unwrap().lines() {
            let record: Value = serde_json::from_str(line).unwrap();
            if record["request_id"] == request_id {
                records.push(record);
            }
        }
    }
    assert_eq!(
        records.len(),
        1,
        "expected one audit record for request {request_id}, got {records:?}"
    );
    records.pop().unwrap()
}

fn security_rate_config() -> Config {
    let mut config = Config::default();
    config.server.rate_limit_rps = 1_000;
    config.server.rate_limit_burst = 1_000;
    config.server.principal_rate_limit_rps = 1_000;
    config.server.principal_rate_limit_burst = 1_000;
    config.server.write_rate_limit_rps = 1_000;
    config.server.write_rate_limit_burst = 1_000;
    config.server.principal_write_rate_limit_rps = 1_000;
    config.server.principal_write_rate_limit_burst = 1_000;
    config
}

#[tokio::test]
async fn security_admin_outer_ip_rate_limit_rejection_is_audited() {
    let harness = TestHarness::new().await;
    let mut config = security_rate_config();
    config.server.rate_limit_rps = 1;
    config.server.rate_limit_burst = 1;
    let server = start_test_server_full_without_rate_limit_override(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        config,
    )
    .await;
    let client = client_with_bearer(&server.admin_bearer);

    let admitted = client
        .get(format!("{}/v1/security/policy", server.base_url))
        .header("x-request-id", "security-ip-rate-admitted")
        .send()
        .await
        .unwrap();
    assert_eq!(admitted.status(), 200);
    let rejected = client
        .get(format!("{}/v1/security/policy", server.base_url))
        .header("x-request-id", "security-ip-rate-rejected")
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 429);

    server.flush_audit().await;
    let record = audit_record_for_request(
        &harness.store,
        &server.audit_node_id,
        "security-ip-rate-rejected",
    )
    .await;
    assert_eq!(record["principal_id"], "anonymous");
    assert_eq!(record["action"], "SecurityAdminRead");
    assert_eq!(record["resource"], "security_policy");
    assert_eq!(record["policy_version"], 1);
    assert_eq!(record["outcome"]["error"]["code"], "RATE_LIMITED");
    assert!(record["decision_id"].is_null());

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn security_admin_principal_rate_limit_rejection_is_audited() {
    let harness = TestHarness::new().await;
    let mut config = security_rate_config();
    config.server.principal_rate_limit_rps = 1;
    config.server.principal_rate_limit_burst = 1;
    let server = start_test_server_full_without_rate_limit_override(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        config,
    )
    .await;
    let client = client_with_bearer(&server.admin_bearer);

    let admitted = client
        .get(format!("{}/v1/security/policy", server.base_url))
        .header("x-request-id", "security-principal-rate-admitted")
        .send()
        .await
        .unwrap();
    assert_eq!(admitted.status(), 200);
    let rejected = client
        .get(format!("{}/v1/security/policy", server.base_url))
        .header("x-request-id", "security-principal-rate-rejected")
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 429);

    server.flush_audit().await;
    let record = audit_record_for_request(
        &harness.store,
        &server.audit_node_id,
        "security-principal-rate-rejected",
    )
    .await;
    assert_eq!(record["principal_id"], "zpk1_test_admin");
    assert_eq!(record["action"], "SecurityAdminRead");
    assert_eq!(record["resource"], "security_policy");
    assert_eq!(record["policy_version"], 1);
    assert_eq!(record["outcome"]["error"]["code"], "RATE_LIMITED");
    assert!(record["decision_id"].is_null());

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn fresh_harnesses_isolate_authoritative_security_policy() {
    let (left, right) = tokio::join!(
        start_test_server_with_config(None),
        start_test_server_with_config(None),
    );
    let (left_url, left_harness, _left_cache, _left_dir, left_admin) = left;
    let (right_url, right_harness, _right_cache, _right_dir, right_admin) = right;

    let left_own = client_with_bearer(&left_admin)
        .get(format!("{left_url}/v1/config/query"))
        .send()
        .await
        .expect("left authority request must complete");
    assert_eq!(left_own.status(), 200);
    let right_own = client_with_bearer(&right_admin)
        .get(format!("{right_url}/v1/config/query"))
        .send()
        .await
        .expect("right authority request must complete");
    assert_eq!(right_own.status(), 200);

    let left_on_right = client_with_bearer(&left_admin)
        .get(format!("{right_url}/v1/config/query"))
        .send()
        .await
        .expect("cross-authority request must complete");
    assert_eq!(left_on_right.status(), 401);
    let right_on_left = client_with_bearer(&right_admin)
        .get(format!("{left_url}/v1/config/query"))
        .send()
        .await
        .expect("cross-authority request must complete");
    assert_eq!(right_on_left.status(), 401);

    for harness in [&left_harness, &right_harness] {
        harness
            .store
            .get(&harness.key("_security/heads/policy.json"))
            .await
            .expect("each harness must own a prefixed authoritative policy head");
    }

    left_harness.cleanup().await;
    right_harness.cleanup().await;
}

async fn provision_reader(
    base_url: &str,
    admin_bearer: &str,
    principal_id: &str,
    key_name: &str,
) -> String {
    let admin = client_with_bearer(admin_bearer);
    let principal = admin
        .post(format!("{base_url}/v1/security/principals"))
        .json(&json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": principal_id,
        }))
        .send()
        .await
        .expect("principal create request must complete");
    assert_eq!(principal.status(), 201);

    let grant = admin
        .post(format!("{base_url}/v1/security/grants"))
        .json(&json!({
            "principal_id": principal_id,
            "scope": { "kind": "global" },
            "actions": {
                "kind": "selected",
                "actions": ["RuntimeConfigRead"],
            },
        }))
        .send()
        .await
        .expect("grant create request must complete");
    assert_eq!(grant.status(), 201);

    let key = admin
        .post(format!("{base_url}/v1/security/keys"))
        .json(&json!({
            "principal_id": principal_id,
            "name": key_name,
        }))
        .send()
        .await
        .expect("key create request must complete");
    assert_eq!(key.status(), 201);
    let body: Value = key.json().await.expect("key response must be JSON");
    body["api_key"]
        .as_str()
        .expect("key response must return the secret once")
        .to_string()
}

async fn issue_key(
    base_url: &str,
    admin_bearer: &str,
    principal_id: &str,
    key_name: &str,
) -> String {
    let key = client_with_bearer(admin_bearer)
        .post(format!("{base_url}/v1/security/keys"))
        .json(&json!({
            "principal_id": principal_id,
            "name": key_name,
        }))
        .send()
        .await
        .expect("key create request must complete");
    assert_eq!(key.status(), 201);
    let body: Value = key.json().await.expect("key response must be JSON");
    body["api_key"]
        .as_str()
        .expect("key response must return the secret once")
        .to_string()
}

#[tokio::test]
async fn two_principals_on_one_ip_have_independent_primary_buckets() {
    let mut config = Config::default();
    config.server.trusted_proxies = vec!["127.0.0.1/32".to_string()];
    config.server.rate_limit_rps = 1_000;
    config.server.rate_limit_burst = 1_000;
    config.server.principal_rate_limit_rps = 1;
    config.server.principal_rate_limit_burst = 1;
    config.server.write_rate_limit_rps = 1_000;
    config.server.write_rate_limit_burst = 1_000;
    config.server.principal_write_rate_limit_rps = 1_000;
    config.server.principal_write_rate_limit_burst = 1_000;

    let (base_url, harness, _cache, _dir, admin_bearer) =
        start_test_server_with_config_no_limit_override(Some(config)).await;
    let first_bearer =
        provision_reader(&base_url, &admin_bearer, "service:rate-limit-a", "reader-a").await;
    let second_bearer =
        provision_reader(&base_url, &admin_bearer, "service:rate-limit-b", "reader-b").await;
    let first = client_with_bearer(&first_bearer);
    let second = client_with_bearer(&second_bearer);

    let admitted = first
        .get(format!("{base_url}/v1/config/query"))
        .header("x-forwarded-for", SHARED_CLIENT_IP)
        .send()
        .await
        .expect("first principal request must complete");
    assert_eq!(admitted.status(), 200);

    let exhausted = first
        .get(format!("{base_url}/v1/config/query"))
        .header("x-forwarded-for", SHARED_CLIENT_IP)
        .send()
        .await
        .expect("exhausted principal request must complete");
    assert_eq!(exhausted.status(), 429);

    let independent = second
        .get(format!("{base_url}/v1/config/query"))
        .header("x-forwarded-for", SHARED_CLIENT_IP)
        .send()
        .await
        .expect("independent principal request must complete");
    assert_eq!(independent.status(), 200);

    harness.cleanup().await;
}

#[tokio::test]
async fn two_keys_for_one_principal_share_one_primary_bucket() {
    let mut config = Config::default();
    config.server.trusted_proxies = vec!["127.0.0.1/32".to_string()];
    config.server.rate_limit_rps = 1_000;
    config.server.rate_limit_burst = 1_000;
    config.server.principal_rate_limit_rps = 1;
    config.server.principal_rate_limit_burst = 1;
    config.server.write_rate_limit_rps = 1_000;
    config.server.write_rate_limit_burst = 1_000;
    config.server.principal_write_rate_limit_rps = 1_000;
    config.server.principal_write_rate_limit_burst = 1_000;

    let (base_url, harness, _cache, _dir, admin_bearer) =
        start_test_server_with_config_no_limit_override(Some(config)).await;
    let principal_id = "service:shared-rate-limit";
    let first_bearer = provision_reader(&base_url, &admin_bearer, principal_id, "reader-one").await;
    let second_bearer = issue_key(&base_url, &admin_bearer, principal_id, "reader-two").await;

    let admitted = client_with_bearer(&first_bearer)
        .get(format!("{base_url}/v1/config/query"))
        .header("x-forwarded-for", SHARED_CLIENT_IP)
        .send()
        .await
        .expect("first key request must complete");
    assert_eq!(admitted.status(), 200);

    let shared_exhaustion = client_with_bearer(&second_bearer)
        .get(format!("{base_url}/v1/config/query"))
        .header("x-forwarded-for", SHARED_CLIENT_IP)
        .send()
        .await
        .expect("second key request must complete");
    assert_eq!(shared_exhaustion.status(), 429);

    harness.cleanup().await;
}

#[tokio::test]
async fn secondary_ip_bucket_caps_aggregate_authenticated_principals() {
    let mut config = Config::default();
    config.server.trusted_proxies = vec!["127.0.0.1/32".to_string()];
    config.server.rate_limit_rps = 1;
    config.server.rate_limit_burst = 1;
    config.server.principal_rate_limit_rps = 1_000;
    config.server.principal_rate_limit_burst = 1_000;
    config.server.write_rate_limit_rps = 1_000;
    config.server.write_rate_limit_burst = 1_000;
    config.server.principal_write_rate_limit_rps = 1_000;
    config.server.principal_write_rate_limit_burst = 1_000;

    let (base_url, harness, _cache, _dir, admin_bearer) =
        start_test_server_with_config_no_limit_override(Some(config)).await;
    let first_bearer = provision_reader(
        &base_url,
        &admin_bearer,
        "service:ip-cap-a",
        "ip-cap-reader-a",
    )
    .await;
    let second_bearer = provision_reader(
        &base_url,
        &admin_bearer,
        "service:ip-cap-b",
        "ip-cap-reader-b",
    )
    .await;

    let admitted = client_with_bearer(&first_bearer)
        .get(format!("{base_url}/v1/config/query"))
        .header("x-forwarded-for", SHARED_CLIENT_IP)
        .send()
        .await
        .expect("first principal request must complete");
    assert_eq!(admitted.status(), 200);

    let aggregate_exhausted = client_with_bearer(&second_bearer)
        .get(format!("{base_url}/v1/config/query"))
        .header("x-forwarded-for", SHARED_CLIENT_IP)
        .send()
        .await
        .expect("aggregate IP-capped request must complete");
    assert_eq!(aggregate_exhausted.status(), 429);

    harness.cleanup().await;
}
