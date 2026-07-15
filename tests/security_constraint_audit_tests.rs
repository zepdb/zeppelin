mod common;

use serde_json::{json, Value};
use zeppelin::config::Config;
use zeppelin::storage::ZeppelinStore;

use common::fault_injection::fail_put_once_matching;
use common::harness::TestHarness;
use common::server::{
    cleanup_ns, client_with_bearer, create_ns_api, start_test_server_full,
    start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer, FullTestServer,
};

async fn expect_json(
    response: reqwest::Response,
    expected: reqwest::StatusCode,
    context: &str,
) -> Value {
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .unwrap_or_else(|error| panic!("{context} response body failed: {error}"));
    assert_eq!(
        status,
        expected,
        "{context}: {}",
        String::from_utf8_lossy(&bytes)
    );
    if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|error| panic!("{context} response must be JSON: {error}"))
    }
}

async fn create_constrained_principal(
    admin: &reqwest::Client,
    server: &FullTestServer,
    namespace: &str,
    label: &str,
    actions: &[&str],
    constraints: Value,
) -> reqwest::Client {
    let principal_id = format!("service:{label}-{}", uuid::Uuid::new_v4().simple());
    let response = admin
        .post(format!("{}/v1/security/principals", server.base_url))
        .json(&json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": label,
        }))
        .send()
        .await
        .expect("principal creation request must complete");
    expect_json(response, reqwest::StatusCode::CREATED, "principal creation").await;

    let response = admin
        .post(format!("{}/v1/security/keys", server.base_url))
        .json(&json!({"principal_id": principal_id, "name": label}))
        .send()
        .await
        .expect("key creation request must complete");
    let key = expect_json(response, reqwest::StatusCode::CREATED, "key creation").await;
    let bearer = key["api_key"]
        .as_str()
        .expect("key response must contain one-time bearer")
        .to_string();

    let mut grant = json!({
        "principal_id": principal_id,
        "scope": {"kind": "namespace", "namespace": namespace},
        "actions": {"kind": "selected", "actions": actions},
    });
    let grant_object = grant
        .as_object_mut()
        .expect("grant fixture must be an object");
    for (field, value) in constraints
        .as_object()
        .expect("constraint fixture must be an object")
    {
        grant_object.insert(field.clone(), value.clone());
    }
    let response = admin
        .post(format!("{}/v1/security/grants", server.base_url))
        .json(&grant)
        .send()
        .await
        .expect("grant creation request must complete");
    expect_json(response, reqwest::StatusCode::CREATED, "grant creation").await;

    client_with_bearer(&bearer)
}

async fn read_audit_records(store: &ZeppelinStore, node_id: &str) -> Vec<Value> {
    let mut keys = store
        .list_prefix("_audit/")
        .await
        .expect("audit prefix LIST must succeed");
    keys.retain(|key| key.contains(&format!("/{node_id}/")));
    keys.sort();
    let mut records = Vec::new();
    for key in keys {
        let body = store
            .get(&key)
            .await
            .expect("audit object GET must succeed");
        let body = String::from_utf8(body.to_vec()).expect("audit object must be UTF-8 JSONL");
        records.extend(
            body.lines()
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_str(line).expect("audit line must be JSON")),
        );
    }
    records
}

fn record_for_request<'a>(records: &'a [Value], request_id: &str) -> &'a Value {
    let matching = records
        .iter()
        .filter(|record| record["request_id"] == request_id)
        .collect::<Vec<_>>();
    assert_eq!(
        matching.len(),
        1,
        "expected one audit record for {request_id}, got {matching:?}"
    );
    matching[0]
}

#[tokio::test]
async fn constrained_query_violation_is_audited_as_authorization_denial() {
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&admin, &server.base_url, 2).await;
    let response = admin
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&json!({"vectors": [{
            "id": "row-1",
            "values": [0.0, 0.0],
            "attributes": {"tenant_id": "acme", "secret": "hidden"}
        }]}))
        .send()
        .await
        .expect("fixture upsert must complete");
    expect_json(response, reqwest::StatusCode::OK, "fixture upsert").await;

    let constrained = create_constrained_principal(
        &admin,
        &server,
        &namespace,
        "constraint-audit-query",
        &["Query"],
        json!({"field_mask": {"deny": ["secret"]}}),
    )
    .await;
    let request_id = "phase4-constraint-query-denial";
    let response = constrained
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            server.base_url
        ))
        .header("x-request-id", request_id)
        .json(&json!({
            "vector": [0.0, 0.0],
            "top_k": 1,
            "filter": {"op": "eq", "field": "secret", "value": "hidden"}
        }))
        .send()
        .await
        .expect("masked-field query must complete");
    let body = expect_json(
        response,
        reqwest::StatusCode::FORBIDDEN,
        "masked-field query",
    )
    .await;
    assert_eq!(body["code"], "constraint_violation");

    server.flush_audit().await;
    let records = read_audit_records(&harness.store, &server.audit_node_id).await;
    let record = record_for_request(&records, request_id);
    assert_eq!(record["action"], "Query");
    assert_eq!(
        record["outcome"]["denied"]["reason"],
        "constraint_violation"
    );
    assert_eq!(record["params"], "authz_denial");

    cleanup_ns(&harness.store, &namespace).await;
    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn batch_constraint_violation_is_audited_despite_top_level_success() {
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&admin, &server.base_url, 2).await;
    let constrained = create_constrained_principal(
        &admin,
        &server,
        &namespace,
        "constraint-audit-batch",
        &["Query"],
        json!({"field_mask": {"deny": ["secret"]}}),
    )
    .await;

    let request_id = "phase4-constraint-batch-denial";
    let response = constrained
        .post(format!(
            "{}/v1/namespaces/{namespace}/query/batch",
            server.base_url
        ))
        .header("x-request-id", request_id)
        .json(&json!({"queries": [
            {
                "vector": [0.0, 0.0],
                "top_k": 1,
                "filter": {"op": "eq", "field": "secret", "value": "hidden"}
            },
            {"vector": [0.0, 0.0], "top_k": 1}
        ]}))
        .send()
        .await
        .expect("batch query must complete");
    let body = expect_json(response, reqwest::StatusCode::OK, "batch query").await;
    assert_eq!(body["results"][0]["ok"], false);
    assert_eq!(body["results"][0]["error"]["code"], "constraint_violation");
    assert_eq!(body["results"][1]["ok"], true);

    server.flush_audit().await;
    let records = read_audit_records(&harness.store, &server.audit_node_id).await;
    let record = record_for_request(&records, request_id);
    assert_eq!(record["action"], "Query");
    assert_eq!(
        record["outcome"]["denied"]["reason"],
        "constraint_violation"
    );
    assert_eq!(
        record["params"]["batch_query_constraint_denial"]["denied_entries"],
        1
    );
    assert_eq!(
        record["params"]["batch_query_constraint_denial"]["total_entries"],
        2
    );

    cleanup_ns(&harness.store, &namespace).await;
    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn forbid_set_upsert_denial_is_durably_audited_as_constraint_violation() {
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&admin, &server.base_url, 2).await;
    let constrained = create_constrained_principal(
        &admin,
        &server,
        &namespace,
        "constraint-audit-forbid-set",
        &["VectorUpsert"],
        json!({"write_constraints": {"forbid_set": ["classification"]}}),
    )
    .await;

    let request_id = "phase4-constraint-forbid-set-denial";
    let response = constrained
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .header("x-request-id", request_id)
        .json(&json!({"vectors": [{
            "id": "forbidden-row",
            "values": [0.0, 0.0],
            "attributes": {"classification": "secret"}
        }]}))
        .send()
        .await
        .expect("forbid-set upsert must complete");
    let body = expect_json(
        response,
        reqwest::StatusCode::FORBIDDEN,
        "forbid-set upsert",
    )
    .await;
    assert_eq!(body["code"], "constraint_violation");

    server.flush_audit().await;
    let records = read_audit_records(&harness.store, &server.audit_node_id).await;
    let record = record_for_request(&records, request_id);
    assert_eq!(record["action"], "VectorUpsert");
    assert_eq!(
        record["outcome"]["denied"]["reason"],
        "constraint_violation"
    );
    assert_eq!(record["params"], "authz_denial");

    cleanup_ns(&harness.store, &namespace).await;
    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn attribute_admin_write_is_durably_audited_with_typed_marker() {
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&admin, &server.base_url, 2).await;
    let attribute_admin = create_constrained_principal(
        &admin,
        &server,
        &namespace,
        "attribute-admin-audit",
        &["VectorUpsert", "AttributeAdmin"],
        json!({
            "write_constraints": {
                "stamp": {"tenant_id": "acme"},
                "forbid_set": ["tenant_id", "classification"]
            }
        }),
    )
    .await;

    let request_id = "phase4-attribute-admin-durable";
    let response = attribute_admin
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .header("x-request-id", request_id)
        .json(&json!({"vectors": [{
            "id": "privileged-row",
            "values": [0.0, 0.0],
            "attributes": {
                "tenant_id": "forged",
                "classification": "declassified"
            }
        }]}))
        .send()
        .await
        .expect("attribute-admin upsert must complete");
    expect_json(response, reqwest::StatusCode::OK, "attribute-admin upsert").await;

    // No explicit flush: the AttributeAdmin obligation must make the successful
    // response wait for durable audit publication.
    let records = read_audit_records(&harness.store, &server.audit_node_id).await;
    let record = record_for_request(&records, request_id);
    assert_eq!(record["action"], "VectorUpsert");
    assert_eq!(record["outcome"], "success");
    assert_eq!(record["params"]["vector_upsert"]["namespace"], namespace);
    assert_eq!(record["params"]["vector_upsert"]["count"], 1);
    assert_eq!(record["params"]["vector_upsert"]["attribute_admin"], true);

    cleanup_ns(&harness.store, &namespace).await;
    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn attribute_admin_write_cannot_succeed_when_durable_audit_put_fails() {
    let harness = TestHarness::new().await;
    let setup_server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&setup_server.admin_bearer);
    let namespace = create_ns_api(&admin, &setup_server.base_url, 2).await;
    let attribute_admin = create_constrained_principal(
        &admin,
        &setup_server,
        &namespace,
        "attribute-admin-audit-failure",
        &["VectorUpsert", "AttributeAdmin"],
        json!({
            "write_constraints": {
                "stamp": {"tenant_id": "acme"},
                "forbid_set": ["classification"]
            }
        }),
    )
    .await;
    let admin_bearer = setup_server.admin_bearer.clone();
    setup_server.shutdown().await;

    let (failing_store, failure) = fail_put_once_matching(&harness.store, "_audit/");
    let mut config = Config::default();
    config.security.audit_flush_secs = 60;
    let server = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        failing_store,
        Some(harness.prefix.clone()),
        config,
        false,
        None,
        100 * 1024 * 1024,
        &admin_bearer,
    )
    .await;

    let response = attribute_admin
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .header("x-request-id", "phase4-attribute-admin-audit-failure")
        .json(&json!({"vectors": [{
            "id": "privileged-row",
            "values": [0.0, 0.0],
            "attributes": {"classification": "declassified"}
        }]}))
        .send()
        .await
        .expect("attribute-admin upsert must complete");
    let body = expect_json(
        response,
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        "attribute-admin upsert with failed audit settlement",
    )
    .await;
    assert_eq!(body["code"], "audit_unavailable");
    assert_eq!(
        body["error"],
        "operation may have completed, but durable audit evidence is unavailable"
    );
    assert_eq!(failure.failures_injected(), 1);

    let response = admin
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors/get",
            server.base_url
        ))
        .json(&json!({
            "ids": ["privileged-row"],
            "include_attributes": true,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("post-failure fetch must complete");
    let body = expect_json(
        response,
        reqwest::StatusCode::OK,
        "post-failure privileged row fetch",
    )
    .await;
    assert_eq!(body["results"][0]["id"], "privileged-row");
    assert_eq!(body["results"][0]["attributes"]["tenant_id"], "acme");
    assert_eq!(
        body["results"][0]["attributes"]["classification"],
        "declassified"
    );

    cleanup_ns(&harness.store, &namespace).await;
    server.shutdown().await;
    harness.cleanup().await;
}
