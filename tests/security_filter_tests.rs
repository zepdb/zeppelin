mod common;

use std::collections::BTreeSet;

use bytes::Bytes;
use common::counting::counting_store;
use common::harness::TestHarness;
use common::server::{
    api_ns, cleanup_ns, client_with_bearer, create_ns_api_fts, create_ns_api_with,
    start_test_server, start_test_server_on_store,
};
use reqwest::{Response, StatusCode};
use serde_json::{json, Map, Value};
use zeppelin::namespace::manager::NamespaceMetadata;
use zeppelin::security::PolicyHead;
use zeppelin::wal::Manifest;

const TENANT_A: &str = "acme";
const TENANT_B: &str = "bravo";

struct PrincipalFixture {
    principal_id: String,
    client: reqwest::Client,
}

async fn expect_status(response: Response, expected: StatusCode, context: &str) -> Value {
    let actual = response.status();
    let bytes = response
        .bytes()
        .await
        .unwrap_or_else(|error| panic!("{context} response body must be readable: {error}"));
    assert_eq!(
        actual,
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

async fn create_namespace(client: &reqwest::Client, base_url: &str) -> String {
    create_ns_api_with(
        client,
        base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await
}

async fn upsert(client: &reqwest::Client, base_url: &str, namespace: &str, vectors: Value) {
    let response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({"vectors": vectors}))
        .send()
        .await
        .expect("upsert request must complete");
    expect_status(response, StatusCode::OK, "fixture upsert").await;
}

fn corpus() -> Value {
    json!([
        {
            "id": "acme-01",
            "values": [0.10, 0.0],
            "attributes": {
                "tenant_id": TENANT_A,
                "content": "shared acme alpha",
                "tier": "gold",
                "group_id": "acme-group-1",
                "ssn": "111-11-1111",
                "salary": 101
            }
        },
        {
            "id": "acme-02",
            "values": [0.20, 0.0],
            "attributes": {
                "tenant_id": TENANT_A,
                "content": "shared acme beta",
                "tier": "silver",
                "group_id": "acme-group-1",
                "ssn": "222-22-2222",
                "salary": 102
            }
        },
        {
            "id": "acme-03",
            "values": [0.30, 0.0],
            "attributes": {
                "tenant_id": TENANT_A,
                "content": "shared acme gamma",
                "tier": "gold",
                "group_id": "acme-group-2",
                "ssn": "333-33-3333",
                "salary": 103
            }
        },
        {
            "id": "bravo-01",
            "values": [0.05, 0.0],
            "attributes": {
                "tenant_id": TENANT_B,
                "content": "shared bravo alpha",
                "tier": "gold",
                "group_id": "bravo-group-1",
                "ssn": "444-44-4444",
                "salary": 201
            }
        },
        {
            "id": "bravo-02",
            "values": [0.15, 0.0],
            "attributes": {
                "tenant_id": TENANT_B,
                "content": "shared bravo beta",
                "tier": "silver",
                "group_id": "bravo-group-1",
                "ssn": "555-55-5555",
                "salary": 202
            }
        },
        {
            "id": "bravo-03",
            "values": [0.25, 0.0],
            "attributes": {
                "tenant_id": TENANT_B,
                "content": "shared bravo gamma",
                "tier": "bronze",
                "group_id": "bravo-group-2",
                "ssn": "666-66-6666",
                "salary": 203
            }
        }
    ])
}

fn tenant_filter(tenant: &str) -> Value {
    json!({"op": "eq", "field": "tenant_id", "value": tenant})
}

fn namespace_scope(namespace: &str) -> Value {
    json!({"kind": "namespace", "namespace": namespace})
}

fn global_scope() -> Value {
    json!({"kind": "global"})
}

fn constrained_grant_fields() -> Value {
    json!({"mandatory_filter": tenant_filter(TENANT_A)})
}

async fn create_principal_with_grant(
    admin: &reqwest::Client,
    base_url: &str,
    label: &str,
    scope: Value,
    actions: &[&str],
    grant_fields: Value,
) -> PrincipalFixture {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let principal_id = format!("service:phase4-{label}-{suffix}");
    let response = admin
        .post(format!("{base_url}/v1/security/principals"))
        .json(&json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": format!("phase4-{label}")
        }))
        .send()
        .await
        .expect("principal creation request must complete");
    expect_status(response, StatusCode::CREATED, "principal creation").await;

    let response = admin
        .post(format!("{base_url}/v1/security/keys"))
        .json(&json!({
            "principal_id": principal_id,
            "name": format!("phase4-{label}-primary")
        }))
        .send()
        .await
        .expect("key creation request must complete");
    let key = expect_status(response, StatusCode::CREATED, "key creation").await;
    let bearer = key["api_key"]
        .as_str()
        .expect("key creation must return the one-time api_key")
        .to_string();

    let mut grant = Map::new();
    grant.insert("principal_id".to_string(), json!(principal_id));
    grant.insert("scope".to_string(), scope);
    grant.insert(
        "actions".to_string(),
        json!({"kind": "selected", "actions": actions}),
    );
    let fields = grant_fields
        .as_object()
        .expect("grant fields must be a JSON object");
    for (name, value) in fields {
        assert!(
            grant.insert(name.clone(), value.clone()).is_none(),
            "grant fixture field {name} must not replace a core grant field"
        );
    }
    let response = admin
        .post(format!("{base_url}/v1/security/grants"))
        .json(&Value::Object(grant))
        .send()
        .await
        .expect("grant creation request must complete");
    expect_status(response, StatusCode::CREATED, "constrained grant creation").await;

    PrincipalFixture {
        principal_id,
        client: client_with_bearer(&bearer),
    }
}

async fn add_grant(
    admin: &reqwest::Client,
    base_url: &str,
    principal_id: &str,
    scope: Value,
    actions: &[&str],
    grant_fields: Value,
) {
    let mut grant = Map::new();
    grant.insert("principal_id".to_string(), json!(principal_id));
    grant.insert("scope".to_string(), scope);
    grant.insert(
        "actions".to_string(),
        json!({"kind": "selected", "actions": actions}),
    );
    for (name, value) in grant_fields
        .as_object()
        .expect("additional grant fields must be an object")
    {
        assert!(
            grant.insert(name.clone(), value.clone()).is_none(),
            "additional grant field {name} replaced a core field"
        );
    }
    let response = admin
        .post(format!("{base_url}/v1/security/grants"))
        .json(&Value::Object(grant))
        .send()
        .await
        .expect("additional grant creation must complete");
    expect_status(response, StatusCode::CREATED, "additional grant creation").await;
}

fn query_body(top_k: usize) -> Value {
    json!({
        "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
        "candidate_k": 16,
        "top_k": top_k,
        "consistency": "strong",
        "projection": {"include_attributes": true}
    })
}

async fn query(client: &reqwest::Client, base_url: &str, namespace: &str, body: Value) -> Value {
    let response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&body)
        .send()
        .await
        .expect("query request must complete");
    expect_status(response, StatusCode::OK, "query").await
}

fn result_ids(body: &Value) -> BTreeSet<String> {
    body["results"]
        .as_array()
        .expect("query response must contain a results array")
        .iter()
        .map(|result| {
            result["id"]
                .as_str()
                .expect("every result must contain an id")
                .to_string()
        })
        .collect()
}

fn expected_acme_ids() -> BTreeSet<String> {
    ["acme-01", "acme-02", "acme-03"]
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn assert_masked_results(body: &Value) {
    for result in body["results"]
        .as_array()
        .expect("response must contain results")
    {
        let attributes = result["attributes"]
            .as_object()
            .expect("masked response must retain unmasked attributes");
        assert!(!attributes.contains_key("ssn"));
        assert!(!attributes.contains_key("salary"));
        assert!(attributes.contains_key("tenant_id"));
    }
}

async fn finish(harness: TestHarness, namespaces: &[&str]) {
    for namespace in namespaces {
        cleanup_ns(&harness.store, namespace).await;
    }
    harness.cleanup().await;
}

async fn policy_publication_state(harness: &TestHarness) -> (Bytes, Vec<String>) {
    let policy_head_key = format!("{}/_security/heads/policy.json", harness.prefix);
    let policy_objects_prefix = format!("{}/_security/policies/", harness.prefix);
    let head = harness
        .store
        .get(&policy_head_key)
        .await
        .expect("authoritative policy head must exist");
    let objects = harness
        .store
        .list_prefix(&policy_objects_prefix)
        .await
        .expect("authoritative policy object LIST must succeed");
    (head, objects)
}

#[tokio::test]
async fn mandatory_filter_scopes_query() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_namespace(&admin, &base_url).await;
    upsert(&admin, &base_url, &namespace, corpus()).await;
    let tenant = create_principal_with_grant(
        &admin,
        &base_url,
        "mandatory-query",
        namespace_scope(&namespace),
        &["Query"],
        constrained_grant_fields(),
    )
    .await;

    let unfiltered = query(&tenant.client, &base_url, &namespace, query_body(16)).await;
    assert_eq!(result_ids(&unfiltered), expected_acme_ids());
    assert_eq!(unfiltered["scanned_fragments"], 0);
    assert_eq!(unfiltered["scanned_segments"], 0);
    assert!(unfiltered.get("debug").is_none());

    let admin_view = query(&admin, &base_url, &namespace, query_body(16)).await;
    assert!(
        admin_view["scanned_fragments"].as_u64().unwrap_or(0) > 0,
        "fixture must prove the scoped zero is redaction, not absent work"
    );

    let mut debug_request = query_body(16);
    debug_request["debug"] = json!(true);
    let debug_response = tenant
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&debug_request)
        .send()
        .await
        .expect("policy-scoped debug request must complete");
    let debug_body = expect_status(
        debug_response,
        StatusCode::FORBIDDEN,
        "policy-scoped debug request",
    )
    .await;
    assert_eq!(debug_body["code"], "constraint_violation");

    let mut caller_b = query_body(16);
    caller_b["filter"] = tenant_filter(TENANT_B);
    let caller_b = query(&tenant.client, &base_url, &namespace, caller_b).await;
    assert_eq!(result_ids(&caller_b), BTreeSet::new());

    finish(harness, &[&namespace]).await;
}

#[tokio::test]
async fn mandatory_filter_scopes_bm25_hybrid_and_by_id_sources() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_ns_api_fts(
        &admin,
        &base_url,
        2,
        json!({
            "content": {
                "language": "english",
                "stemming": true,
                "remove_stopwords": true
            }
        }),
    )
    .await;
    upsert(&admin, &base_url, &namespace, corpus()).await;
    let tenant = create_principal_with_grant(
        &admin,
        &base_url,
        "multi-source-scope",
        namespace_scope(&namespace),
        &["Query"],
        constrained_grant_fields(),
    )
    .await;

    let bm25 = query(
        &tenant.client,
        &base_url,
        &namespace,
        json!({
            "rank_by": ["content", "BM25", "shared"],
            "top_k": 16,
            "consistency": "strong",
            "include_attributes": true
        }),
    )
    .await;
    assert_eq!(result_ids(&bm25), expected_acme_ids());

    let hybrid = query(
        &tenant.client,
        &base_url,
        &namespace,
        json!({
            "sources": [
                {"type": "ann", "vector": [0.0, 0.0]},
                {"type": "bm25", "rank_by": ["content", "BM25", "shared"]}
            ],
            "fusion": {"type": "rrf", "k": 60},
            "candidate_k": 16,
            "top_k": 16,
            "consistency": "strong",
            "projection": {"include_attributes": true}
        }),
    )
    .await;
    assert_eq!(result_ids(&hybrid), expected_acme_ids());

    let in_scope_seed = query(
        &tenant.client,
        &base_url,
        &namespace,
        json!({
            "sources": [{"type": "ann", "id": "acme-01"}],
            "candidate_k": 16,
            "top_k": 16,
            "consistency": "strong",
            "projection": {"include_attributes": true}
        }),
    )
    .await;
    assert!(result_ids(&in_scope_seed).is_subset(&expected_acme_ids()));
    assert!(!result_ids(&in_scope_seed).contains("acme-01"));

    let response = tenant
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&json!({
            "sources": [{"type": "ann", "id": "bravo-01"}],
            "candidate_k": 16,
            "top_k": 16,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("out-of-scope by-ID seed request must complete");
    let body = expect_status(response, StatusCode::NOT_FOUND, "out-of-scope by-ID seed").await;
    assert_eq!(body["code"], "VECTOR_NOT_FOUND");

    finish(harness, &[&namespace]).await;
}

#[tokio::test]
async fn cannot_negate_or_widen() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_namespace(&admin, &base_url).await;
    upsert(&admin, &base_url, &namespace, corpus()).await;
    let tenant = create_principal_with_grant(
        &admin,
        &base_url,
        "cannot-widen",
        namespace_scope(&namespace),
        &["Query"],
        constrained_grant_fields(),
    )
    .await;

    let mut negated = query_body(16);
    negated["filter"] = json!({"op": "not", "filter": tenant_filter(TENANT_A)});
    let negated = query(&tenant.client, &base_url, &namespace, negated).await;
    assert_eq!(result_ids(&negated), BTreeSet::new());

    let mut widened = query_body(16);
    widened["filter"] = json!({
        "op": "or",
        "filters": [tenant_filter(TENANT_A), tenant_filter(TENANT_B)]
    });
    let widened = query(&tenant.client, &base_url, &namespace, widened).await;
    assert_eq!(result_ids(&widened), expected_acme_ids());

    let mut partial_widen = query_body(16);
    partial_widen["filter"] = json!({
        "op": "or",
        "filters": [
            tenant_filter(TENANT_B),
            {"op": "eq", "field": "tier", "value": "gold"}
        ]
    });
    let partial_widen = query(&tenant.client, &base_url, &namespace, partial_widen).await;
    assert_eq!(
        result_ids(&partial_widen),
        ["acme-01", "acme-03"]
            .into_iter()
            .map(str::to_string)
            .collect()
    );

    finish(harness, &[&namespace]).await;
}

#[tokio::test]
async fn all_surfaces_scoped() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_namespace(&admin, &base_url).await;
    let empty_namespace = create_namespace(&admin, &base_url).await;
    upsert(&admin, &base_url, &namespace, corpus()).await;
    upsert(
        &admin,
        &base_url,
        &empty_namespace,
        json!([{
            "id": "comparison-only",
            "values": [9.0, 9.0],
            "attributes": {"tenant_id": TENANT_A}
        }]),
    )
    .await;
    let retained = Manifest::read(&harness.store, &namespace)
        .await
        .expect("fixture manifest read must succeed")
        .expect("fixture manifest must exist")
        .version();
    let snapshot_response = admin
        .put(format!(
            "{base_url}/v1/namespaces/{namespace}/snapshots/phase4-retained"
        ))
        .send()
        .await
        .expect("named snapshot creation must complete");
    let snapshot = expect_status(
        snapshot_response,
        StatusCode::CREATED,
        "named snapshot creation",
    )
    .await;
    assert_eq!(snapshot["generation"], retained);
    upsert(
        &admin,
        &base_url,
        &namespace,
        json!([{
            "id": "bravo-current-only",
            "values": [0.01, 0.0],
            "attributes": {
                "tenant_id": TENANT_B,
                "tier": "gold",
                "group_id": "bravo-current",
                "ssn": "777-77-7777",
                "salary": 204
            }
        }]),
    )
    .await;
    let tenant = create_principal_with_grant(
        &admin,
        &base_url,
        "all-surfaces",
        global_scope(),
        &["Query", "VectorFetch"],
        constrained_grant_fields(),
    )
    .await;

    let batch_response = tenant
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query/batch"))
        .json(&json!({"queries": [query_body(16), query_body(16)]}))
        .send()
        .await
        .expect("batch query must complete");
    let batch = expect_status(batch_response, StatusCode::OK, "batch query").await;
    for entry in batch["results"]
        .as_array()
        .expect("batch response must contain entries")
    {
        assert_eq!(entry["ok"], true);
        assert_eq!(
            entry["metadata"]["latency_ms"], 0,
            "mandatory-filter batch metadata must redact per-query latency"
        );
        assert_eq!(result_ids(&entry["response"]), expected_acme_ids());
    }

    let mut faceted = query_body(16);
    faceted["facets"] = json!(["tenant_id", "tier"]);
    let faceted = query(&tenant.client, &base_url, &namespace, faceted).await;
    assert_eq!(faceted["facets"]["tenant_id"], json!({"acme": 3}));
    assert_eq!(faceted["facets"]["tier"], json!({"gold": 2, "silver": 1}));

    let mut grouped = query_body(16);
    grouped["grouping"] = json!({"type": "field", "field": "group_id", "max_per_group": 16});
    let grouped = query(&tenant.client, &base_url, &namespace, grouped).await;
    let grouped_ids = grouped["groups"]
        .as_array()
        .expect("grouped response must contain groups")
        .iter()
        .flat_map(|group| {
            group["results"]
                .as_array()
                .expect("every group must contain results")
                .iter()
        })
        .map(|result| result["id"].as_str().expect("group result id").to_string())
        .collect::<BTreeSet<_>>();
    assert_eq!(grouped_ids, expected_acme_ids());

    let mut cursor = json!({"type": "none"});
    let mut paged_ids = BTreeSet::new();
    loop {
        let mut page_request = query_body(2);
        page_request["cursor"] = cursor;
        let page = query(&tenant.client, &base_url, &namespace, page_request).await;
        for id in result_ids(&page) {
            assert!(paged_ids.insert(id), "cursor pages must not overlap");
        }
        let Some(next) = page.get("next_cursor").and_then(Value::as_str) else {
            break;
        };
        cursor = json!({"type": "after", "token": next});
    }
    assert_eq!(paged_ids, expected_acme_ids());

    let mut explained = query_body(16);
    explained["explain"] = json!("full");
    let explained = query(&tenant.client, &base_url, &namespace, explained).await;
    assert_eq!(explained["explain"]["plan"]["policy_filter_applied"], true);
    assert_eq!(
        explained["explain"]["plan"]["sources"][0]["nprobe"],
        Value::Null,
        "mandatory-filter explain must redact ANN probe breadth"
    );
    let explain_text =
        serde_json::to_string(&explained["explain"]["plan"]).expect("explain plan must serialize");
    assert!(!explain_text.contains("tenant_id"));
    assert!(!explain_text.contains(TENANT_A));

    let as_of_response = tenant
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .query(&[("as_of", retained.to_string())])
        .json(&query_body(16))
        .send()
        .await
        .expect("as_of query must complete");
    let as_of = expect_status(as_of_response, StatusCode::OK, "as_of query").await;
    assert_eq!(result_ids(&as_of), expected_acme_ids());

    let snapshot_response = tenant
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .query(&[("as_of", "snapshot:phase4-retained")])
        .json(&query_body(16))
        .send()
        .await
        .expect("named-snapshot query must complete");
    let snapshot_query =
        expect_status(snapshot_response, StatusCode::OK, "named-snapshot query").await;
    assert_eq!(result_ids(&snapshot_query), expected_acme_ids());

    let mut snapshot_page_request = query_body(1);
    snapshot_page_request["cursor"] = json!({"type": "none"});
    let snapshot_page_response = tenant
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .query(&[("as_of", "snapshot:phase4-retained")])
        .json(&snapshot_page_request)
        .send()
        .await
        .expect("named-snapshot cursor page must complete");
    let snapshot_page = expect_status(
        snapshot_page_response,
        StatusCode::OK,
        "named-snapshot cursor page",
    )
    .await;
    let snapshot_cursor = snapshot_page["next_cursor"]
        .as_str()
        .expect("retained three-row slice must return a cursor")
        .to_string();
    let mut mismatched_continuation = query_body(1);
    mismatched_continuation["cursor"] = json!({"type": "after", "token": snapshot_cursor});
    let response = tenant
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&mismatched_continuation)
        .send()
        .await
        .expect("live reuse of a snapshot cursor must complete");
    let body = expect_status(
        response,
        StatusCode::BAD_REQUEST,
        "snapshot cursor reused without as_of",
    )
    .await;
    assert_eq!(body["code"], "VALIDATION_ERROR");

    let fetch_body = json!({
        "ids": ["bravo-01"],
        "include_vector": true,
        "include_attributes": true,
        "consistency": "strong"
    });
    let filtered = tenant
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors/get"))
        .json(&fetch_body)
        .send()
        .await
        .expect("filtered fetch must complete");
    assert_eq!(filtered.status(), StatusCode::OK);
    let filtered = filtered.bytes().await.expect("filtered fetch body");
    let nonexistent = tenant
        .client
        .post(format!(
            "{base_url}/v1/namespaces/{empty_namespace}/vectors/get"
        ))
        .json(&fetch_body)
        .send()
        .await
        .expect("nonexistent fetch must complete");
    assert_eq!(nonexistent.status(), StatusCode::OK);
    let nonexistent = nonexistent.bytes().await.expect("nonexistent fetch body");
    assert_eq!(filtered, nonexistent);

    finish(harness, &[&namespace, &empty_namespace]).await;
}

#[tokio::test]
async fn clone_copies_source_data_without_inheriting_source_grants() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let source = create_namespace(&admin, &base_url).await;
    upsert(&admin, &base_url, &source, corpus()).await;
    let generation = Manifest::read(&harness.store, &source)
        .await
        .expect("source manifest read must succeed")
        .expect("source manifest must exist")
        .version();
    let cloner = create_principal_with_grant(
        &admin,
        &base_url,
        "clone-no-grant-inheritance",
        namespace_scope(&source),
        &["NamespaceClone", "NamespaceRead", "Query"],
        json!({}),
    )
    .await;
    let create_grant = admin
        .post(format!("{base_url}/v1/security/grants"))
        .json(&json!({
            "principal_id": cloner.principal_id.clone(),
            "scope": global_scope(),
            "actions": {"kind": "selected", "actions": ["NamespaceCreate"]}
        }))
        .send()
        .await
        .expect("global namespace-create grant must complete");
    expect_status(
        create_grant,
        StatusCode::CREATED,
        "global namespace-create grant",
    )
    .await;

    assert_eq!(
        result_ids(&query(&cloner.client, &base_url, &source, query_body(16)).await),
        ["acme-01", "acme-02", "acme-03", "bravo-01", "bravo-02", "bravo-03"]
            .into_iter()
            .map(str::to_string)
            .collect(),
        "unconstrained source access makes a raw copy non-widening"
    );

    let target = api_ns(&harness, "phase4-clone-target");
    let clone_response = cloner
        .client
        .post(format!("{base_url}/v1/namespaces/{source}/clone"))
        .json(&json!({
            "target": target.clone(),
            "as_of": generation.to_string()
        }))
        .send()
        .await
        .expect("authorized clone request must complete");
    let clone_body = expect_status(
        clone_response,
        StatusCode::CREATED,
        "authorized clone request",
    )
    .await;
    assert_eq!(clone_body["source"], source);
    assert_eq!(clone_body["target"], target);
    assert_eq!(clone_body["generation"], generation);

    let denied = cloner
        .client
        .post(format!("{base_url}/v1/namespaces/{target}/query"))
        .json(&query_body(16))
        .send()
        .await
        .expect("target query without a target grant must complete");
    let denied = expect_status(
        denied,
        StatusCode::FORBIDDEN,
        "target query without a target grant",
    )
    .await;
    assert_eq!(denied["code"], "namespace_not_granted");

    let listed = admin
        .get(format!("{base_url}/v1/security/grants"))
        .send()
        .await
        .expect("grant list after clone must complete");
    let listed = expect_status(listed, StatusCode::OK, "grant list after clone").await;
    assert!(
        !listed["grants"]
            .as_array()
            .expect("grant list must contain an array")
            .iter()
            .any(|grant| {
                grant["principal_id"].as_str() == Some(cloner.principal_id.as_str())
                    && grant["scope"]["kind"] == "namespace"
                    && grant["scope"]["namespace"].as_str() == Some(target.as_str())
            }),
        "clone must not synthesize a target-scoped grant"
    );

    assert_eq!(
        result_ids(&query(&admin, &base_url, &target, query_body(16)).await),
        ["acme-01", "acme-02", "acme-03", "bravo-01", "bravo-02", "bravo-03",]
            .into_iter()
            .map(str::to_string)
            .collect(),
        "clone must copy source data as-is for a separately authorized admin"
    );

    finish(harness, &[&source, &target]).await;
}

#[tokio::test]
async fn clone_rejects_target_read_widening_for_raw_copy() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let source = create_namespace(&admin, &base_url).await;
    upsert(&admin, &base_url, &source, corpus()).await;
    let generation = Manifest::read(&harness.store, &source)
        .await
        .expect("source manifest read must succeed")
        .expect("source manifest must exist")
        .version();
    let cloner = create_principal_with_grant(
        &admin,
        &base_url,
        "raw-copy-controller",
        namespace_scope(&source),
        &["NamespaceClone", "NamespaceRead"],
        json!({}),
    )
    .await;
    let create_grant = admin
        .post(format!("{base_url}/v1/security/grants"))
        .json(&json!({
            "principal_id": cloner.principal_id.clone(),
            "scope": global_scope(),
            "actions": {"kind": "selected", "actions": ["NamespaceCreate"]}
        }))
        .send()
        .await
        .expect("global create grant must complete");
    expect_status(create_grant, StatusCode::CREATED, "global create grant").await;

    let observer = create_principal_with_grant(
        &admin,
        &base_url,
        "separate-global-reader",
        namespace_scope(&source),
        &["Query", "VectorFetch"],
        constrained_grant_fields(),
    )
    .await;
    let global_read_grant = admin
        .post(format!("{base_url}/v1/security/grants"))
        .json(&json!({
            "principal_id": observer.principal_id.clone(),
            "scope": global_scope(),
            "actions": {"kind": "selected", "actions": ["Query", "VectorFetch"]}
        }))
        .send()
        .await
        .expect("observer global read grant must complete");
    expect_status(
        global_read_grant,
        StatusCode::CREATED,
        "observer global read grant",
    )
    .await;

    assert_eq!(
        result_ids(&query(&observer.client, &base_url, &source, query_body(16)).await),
        expected_acme_ids(),
        "a separate principal's source grant must constrain its global read"
    );

    let target = api_ns(&harness, "phase4-rejected-clone-target");
    let response = cloner
        .client
        .post(format!("{base_url}/v1/namespaces/{source}/clone"))
        .json(&json!({"target": target.clone(), "as_of": generation.to_string()}))
        .send()
        .await
        .expect("widening clone request must complete");
    let body = expect_status(response, StatusCode::FORBIDDEN, "widening raw clone").await;
    assert_eq!(body["code"], "constraint_violation");

    let absent = admin
        .get(format!("{base_url}/v1/namespaces/{target}"))
        .send()
        .await
        .expect("rejected clone target lookup must complete");
    expect_status(absent, StatusCode::NOT_FOUND, "rejected clone target").await;

    finish(harness, &[&source]).await;
}

#[tokio::test]
async fn clone_rejects_each_constrained_control_decision() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let source = create_namespace(&admin, &base_url).await;
    upsert(&admin, &base_url, &source, corpus()).await;
    let generation = Manifest::read(&harness.store, &source)
        .await
        .expect("source manifest read must succeed")
        .expect("source manifest must exist")
        .version();

    for (label, clone_fields, read_fields, create_fields) in [
        (
            "source-clone",
            constrained_grant_fields(),
            json!({}),
            json!({}),
        ),
        (
            "source-read",
            json!({}),
            constrained_grant_fields(),
            json!({}),
        ),
        (
            "target-create",
            json!({}),
            json!({}),
            constrained_grant_fields(),
        ),
    ] {
        let target = api_ns(&harness, &format!("phase4-{label}-constraint-target"));
        let controller = create_principal_with_grant(
            &admin,
            &base_url,
            &format!("{label}-constraint"),
            namespace_scope(&source),
            &["NamespaceClone"],
            clone_fields,
        )
        .await;
        add_grant(
            &admin,
            &base_url,
            &controller.principal_id,
            namespace_scope(&source),
            &["NamespaceRead"],
            read_fields,
        )
        .await;
        add_grant(
            &admin,
            &base_url,
            &controller.principal_id,
            namespace_scope(&target),
            &["NamespaceCreate"],
            create_fields,
        )
        .await;

        let response = controller
            .client
            .post(format!("{base_url}/v1/namespaces/{source}/clone"))
            .json(&json!({"target": target.clone(), "as_of": generation.to_string()}))
            .send()
            .await
            .unwrap_or_else(|error| panic!("{label} constrained clone must complete: {error}"));
        let body = expect_status(
            response,
            StatusCode::FORBIDDEN,
            &format!("{label} constrained clone"),
        )
        .await;
        assert_eq!(body["code"], "constraint_violation");

        let absent = admin
            .get(format!("{base_url}/v1/namespaces/{target}"))
            .send()
            .await
            .unwrap_or_else(|error| panic!("{label} target lookup must complete: {error}"));
        expect_status(
            absent,
            StatusCode::NOT_FOUND,
            &format!("{label} rejected target"),
        )
        .await;
    }

    finish(harness, &[&source]).await;
}

#[tokio::test]
async fn constrained_whole_namespace_surfaces_fail_closed() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_namespace(&admin, &base_url).await;
    upsert(&admin, &base_url, &namespace, corpus()).await;
    let reader = create_principal_with_grant(
        &admin,
        &base_url,
        "constrained-whole-namespace",
        namespace_scope(&namespace),
        &[
            "NamespaceRead",
            "CompactionStatusRead",
            "CompactionTrigger",
            "SnapshotRead",
            "SnapshotWrite",
            "SnapshotDelete",
            "IndexConfigWrite",
            "HydrationTrigger",
            "NamespaceDelete",
        ],
        constrained_grant_fields(),
    )
    .await;

    for (label, path) in [
        ("namespace metadata", format!("/v1/namespaces/{namespace}")),
        (
            "compaction status",
            format!("/v1/namespaces/{namespace}/compact/status"),
        ),
    ] {
        let response = reader
            .client
            .get(format!("{base_url}{path}"))
            .send()
            .await
            .unwrap_or_else(|error| panic!("{label} request must complete: {error}"));
        let body = expect_status(response, StatusCode::FORBIDDEN, label).await;
        assert_eq!(body["code"], "constraint_violation");
    }

    let response = reader
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/compact"))
        .send()
        .await
        .expect("manual compaction trigger must complete");
    let body = expect_status(
        response,
        StatusCode::FORBIDDEN,
        "constrained manual compaction trigger",
    )
    .await;
    assert_eq!(body["code"], "constraint_violation");

    for (label, path) in [
        (
            "snapshot list",
            format!("/v1/namespaces/{namespace}/snapshots"),
        ),
        (
            "snapshot metadata",
            format!("/v1/namespaces/{namespace}/snapshots/constrained-pin"),
        ),
    ] {
        let response = reader
            .client
            .get(format!("{base_url}{path}"))
            .send()
            .await
            .unwrap_or_else(|error| panic!("{label} request must complete: {error}"));
        let body = expect_status(response, StatusCode::FORBIDDEN, label).await;
        assert_eq!(body["code"], "constraint_violation");
    }

    let response = reader
        .client
        .put(format!(
            "{base_url}/v1/namespaces/{namespace}/snapshots/constrained-pin"
        ))
        .send()
        .await
        .expect("constrained snapshot PUT must complete");
    let body = expect_status(response, StatusCode::FORBIDDEN, "constrained snapshot PUT").await;
    assert_eq!(body["code"], "constraint_violation");

    let response = reader
        .client
        .delete(format!(
            "{base_url}/v1/namespaces/{namespace}/snapshots/constrained-pin"
        ))
        .send()
        .await
        .expect("constrained snapshot DELETE must complete");
    let body = expect_status(
        response,
        StatusCode::FORBIDDEN,
        "constrained snapshot DELETE",
    )
    .await;
    assert_eq!(body["code"], "constraint_violation");

    let response = reader
        .client
        .patch(format!("{base_url}/v1/namespaces/{namespace}/index_config"))
        .json(&json!({"nlist": 2}))
        .send()
        .await
        .expect("constrained index-config PATCH must complete");
    let body = expect_status(
        response,
        StatusCode::FORBIDDEN,
        "constrained index-config PATCH",
    )
    .await;
    assert_eq!(body["code"], "constraint_violation");

    let response = reader
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/hydrate"))
        .send()
        .await
        .expect("constrained hydration trigger must complete");
    let body = expect_status(
        response,
        StatusCode::FORBIDDEN,
        "constrained hydration trigger",
    )
    .await;
    assert_eq!(body["code"], "constraint_violation");

    let response = reader
        .client
        .delete(format!("{base_url}/v1/namespaces/{namespace}"))
        .send()
        .await
        .expect("constrained namespace delete must complete");
    let body = expect_status(
        response,
        StatusCode::FORBIDDEN,
        "constrained namespace delete",
    )
    .await;
    assert_eq!(body["code"], "constraint_violation");

    assert_eq!(
        result_ids(&query(&admin, &base_url, &namespace, query_body(16)).await),
        ["acme-01", "acme-02", "acme-03", "bravo-01", "bravo-02", "bravo-03"]
            .into_iter()
            .map(str::to_string)
            .collect(),
        "rejected whole-namespace operations must leave every row visible"
    );

    finish(harness, &[&namespace]).await;
}

#[tokio::test]
async fn unsupported_constrained_control_actions_fail_closed() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let constrained = create_principal_with_grant(
        &admin,
        &base_url,
        "unsupported-control-constraints",
        global_scope(),
        &["RuntimeConfigRead", "SecurityAdminRead", "NamespaceCreate"],
        constrained_grant_fields(),
    )
    .await;

    for (label, path) in [
        ("runtime config read", "/v1/config/query"),
        ("security grant read", "/v1/security/grants"),
    ] {
        let response = constrained
            .client
            .get(format!("{base_url}{path}"))
            .send()
            .await
            .unwrap_or_else(|error| panic!("{label} must complete: {error}"));
        let body = expect_status(response, StatusCode::FORBIDDEN, label).await;
        assert_eq!(body["code"], "constraint_violation");
    }

    let target = api_ns(&harness, "constrained-create-target");
    let response = constrained
        .client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&json!({
            "name": target.clone(),
            "dimensions": 2,
            "distance_metric": "euclidean"
        }))
        .send()
        .await
        .expect("constrained namespace create must complete");
    let body = expect_status(
        response,
        StatusCode::FORBIDDEN,
        "constrained namespace create",
    )
    .await;
    assert_eq!(body["code"], "constraint_violation");
    assert!(
        harness
            .store
            .list_prefix(&format!("{target}/"))
            .await
            .expect("target prefix LIST must succeed")
            .is_empty(),
        "rejected target-scoped create wrote namespace objects"
    );

    finish(harness, &[]).await;
}

#[tokio::test]
async fn field_mask_all_surfaces() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_namespace(&admin, &base_url).await;
    upsert(&admin, &base_url, &namespace, corpus()).await;
    let masked = create_principal_with_grant(
        &admin,
        &base_url,
        "masked-reader",
        namespace_scope(&namespace),
        &["Query", "VectorFetch"],
        json!({
            "mandatory_filter": tenant_filter(TENANT_A),
            "field_mask": {"deny": ["salary", "ssn"]}
        }),
    )
    .await;
    let unmasked = create_principal_with_grant(
        &admin,
        &base_url,
        "unmasked-reader",
        namespace_scope(&namespace),
        &["Query", "VectorFetch"],
        constrained_grant_fields(),
    )
    .await;

    let masked_query = query(&masked.client, &base_url, &namespace, query_body(16)).await;
    assert_masked_results(&masked_query);

    let mut grouped_request = query_body(16);
    grouped_request["grouping"] =
        json!({"type": "field", "field": "group_id", "max_per_group": 16});
    let grouped = query(&masked.client, &base_url, &namespace, grouped_request).await;
    for result in grouped["groups"]
        .as_array()
        .expect("masked grouped response must contain groups")
        .iter()
        .flat_map(|group| {
            group["results"]
                .as_array()
                .expect("masked group must contain results")
        })
    {
        let attributes = result["attributes"]
            .as_object()
            .expect("masked grouped result must retain unmasked attributes");
        assert!(!attributes.contains_key("ssn"));
        assert!(!attributes.contains_key("salary"));
    }

    let batch_response = masked
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query/batch"))
        .json(&json!({"queries": [query_body(16)]}))
        .send()
        .await
        .expect("masked batch query must complete");
    let batch = expect_status(batch_response, StatusCode::OK, "masked batch query").await;
    assert_masked_results(&batch["results"][0]["response"]);

    let fetch_response = masked
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors/get"))
        .json(&json!({
            "ids": ["acme-01", "acme-02"],
            "include_vector": true,
            "include_attributes": true,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("masked fetch must complete");
    let fetch = expect_status(fetch_response, StatusCode::OK, "masked fetch").await;
    assert_masked_results(&fetch);

    let mut explain_request = query_body(16);
    explain_request["explain"] = json!("full");
    let explained = query(&masked.client, &base_url, &namespace, explain_request).await;
    assert_masked_results(&explained);
    let explained_text = serde_json::to_string(&explained).expect("explain JSON must serialize");
    assert!(!explained_text.contains("111-11-1111"));
    assert!(!explained_text.contains("222-22-2222"));
    assert!(!explained_text.contains("333-33-3333"));

    let mut masked_filter = query_body(16);
    masked_filter["filter"] = json!({"op": "eq", "field": "salary", "value": 101});
    let response = masked
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&masked_filter)
        .send()
        .await
        .expect("masked caller-filter request must complete");
    let body = expect_status(response, StatusCode::FORBIDDEN, "masked caller filter").await;
    assert_eq!(body["code"], "constraint_violation");

    let masked_rank = json!({
        "rank_by": ["salary", "BM25", "101"],
        "top_k": 16
    });
    let response = masked
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&masked_rank)
        .send()
        .await
        .expect("masked ranking request must complete");
    let body = expect_status(response, StatusCode::FORBIDDEN, "masked ranking field").await;
    assert_eq!(body["code"], "constraint_violation");

    let mut masked_facet = query_body(16);
    masked_facet["facets"] = json!(["salary"]);
    let response = masked
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&masked_facet)
        .send()
        .await
        .expect("masked facet request must complete");
    let body = expect_status(response, StatusCode::FORBIDDEN, "masked facet").await;
    assert_eq!(body["code"], "constraint_violation");

    let mut masked_group = query_body(16);
    masked_group["grouping"] = json!({"type": "field", "field": "ssn", "max_per_group": 16});
    let response = masked
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&masked_group)
        .send()
        .await
        .expect("masked grouping request must complete");
    let body = expect_status(response, StatusCode::FORBIDDEN, "masked grouping").await;
    assert_eq!(body["code"], "constraint_violation");

    let visible = query(&unmasked.client, &base_url, &namespace, query_body(16)).await;
    for result in visible["results"]
        .as_array()
        .expect("unmasked response must contain results")
    {
        assert!(result["attributes"].get("ssn").is_some());
        assert!(result["attributes"].get("salary").is_some());
    }

    finish(harness, &[&namespace]).await;
}

#[tokio::test]
async fn write_stamp_and_forbid() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let (base_url, _cache, _cache_dir, admin_bearer) =
        start_test_server_on_store(&harness, store, Some(harness.prefix.clone())).await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_namespace(&admin, &base_url).await;
    upsert(
        &admin,
        &base_url,
        &namespace,
        json!([
            {
                "id": "protected-acme",
                "values": [0.7, 0.0],
                "attributes": {
                    "tenant_id": TENANT_A,
                    "classification": "internal",
                    "kind": "original"
                }
            },
            {
                "id": "protected-bravo",
                "values": [0.8, 0.0],
                "attributes": {
                    "tenant_id": TENANT_B,
                    "classification": "restricted",
                    "kind": "other-tenant"
                }
            }
        ]),
    )
    .await;
    let writer = create_principal_with_grant(
        &admin,
        &base_url,
        "constrained-writer",
        namespace_scope(&namespace),
        &["VectorUpsert"],
        json!({
            "mandatory_filter": tenant_filter(TENANT_A),
            "write_constraints": {
                "stamp": {"tenant_id": TENANT_A},
                "forbid_set": ["classification", "is_public", "tenant_id"]
            }
        }),
    )
    .await;

    let response = writer
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({"vectors": [{
            "values": [0.0, 0.0],
            "attributes": {"kind": "ordinary"}
        }]}))
        .send()
        .await
        .expect("scoped server-owned create must complete");
    let created = expect_status(response, StatusCode::OK, "scoped server-owned create").await;
    let writer_stamped_id = created["generated_ids"][0]["id"]
        .as_str()
        .expect("scoped create must return its generated identity")
        .to_string();
    let fetched = admin
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors/get"))
        .json(&json!({
            "ids": [&writer_stamped_id],
            "include_vector": false,
            "include_attributes": true,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("admin verification fetch must complete");
    let fetched = expect_status(fetched, StatusCode::OK, "admin verification fetch").await;
    assert_eq!(fetched["results"][0]["attributes"]["tenant_id"], TENANT_A);

    upsert(
        &writer.client,
        &base_url,
        &namespace,
        json!([{
            "id": "protected-acme",
            "values": [0.71, 0.0],
            "attributes": {"kind": "updated"}
        }]),
    )
    .await;
    let preserved = admin
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors/get"))
        .json(&json!({
            "ids": ["protected-acme"],
            "include_vector": false,
            "include_attributes": true,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("protected-field preservation fetch must complete");
    let preserved = expect_status(
        preserved,
        StatusCode::OK,
        "protected-field preservation fetch",
    )
    .await;
    assert_eq!(preserved["results"][0]["attributes"]["kind"], "updated");
    assert_eq!(
        preserved["results"][0]["attributes"]["classification"], "internal",
        "omitting a caller-forbidden field must not erase its existing value"
    );

    counter.reset();
    let capture = writer
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({"vectors": [{
            "id": "protected-bravo",
            "values": [0.01, 0.0],
            "attributes": {"kind": "captured"}
        }]}))
        .send()
        .await
        .expect("cross-scope overwrite request must complete");
    let capture = expect_status(capture, StatusCode::FORBIDDEN, "cross-scope overwrite").await;
    assert_eq!(capture["code"], "constraint_violation");
    assert_eq!(
        counter.puts_matching(&format!("{namespace}/wal/")),
        0,
        "rejected cross-scope overwrite attempted a WAL PUT"
    );
    let uncaptured = admin
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors/get"))
        .json(&json!({
            "ids": ["protected-bravo"],
            "include_vector": true,
            "include_attributes": true,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("cross-scope overwrite verification must complete");
    let uncaptured = expect_status(
        uncaptured,
        StatusCode::OK,
        "cross-scope overwrite verification",
    )
    .await;
    assert_eq!(uncaptured["results"][0]["values"], json!([0.8, 0.0]));
    assert_eq!(
        uncaptured["results"][0]["attributes"]["tenant_id"],
        TENANT_B
    );

    for (case, violating_attributes) in [
        ("tenant", json!({"tenant_id": TENANT_B})),
        ("public", json!({"is_public": true})),
    ] {
        let innocent_id = format!("innocent-{case}");
        let rejected_id = format!("rejected-{case}");
        upsert(
            &admin,
            &base_url,
            &namespace,
            json!([
                {
                    "id": innocent_id,
                    "values": [0.01, 0.0],
                    "attributes": {"tenant_id": TENANT_A, "kind": "before"}
                },
                {
                    "id": rejected_id,
                    "values": [0.02, 0.0],
                    "attributes": {"tenant_id": TENANT_A, "kind": "before"}
                }
            ]),
        )
        .await;
        counter.reset();
        let response = writer
            .client
            .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
            .json(&json!({
                "vectors": [
                    {
                        "id": innocent_id,
                        "values": [0.1, 0.0],
                        "attributes": {"kind": "innocent"}
                    },
                    {
                        "id": rejected_id,
                        "values": [0.2, 0.0],
                        "attributes": violating_attributes
                    }
                ]
            }))
            .send()
            .await
            .expect("forbidden upsert must complete");
        let body = expect_status(
            response,
            StatusCode::FORBIDDEN,
            &format!("forbidden {case} upsert"),
        )
        .await;
        assert_eq!(body["code"], "constraint_violation");
        assert_eq!(
            counter.puts_matching(&format!("{namespace}/wal/")),
            0,
            "rejected {case} batch attempted a WAL PUT"
        );

        let verification = admin
            .post(format!("{base_url}/v1/namespaces/{namespace}/vectors/get"))
            .json(&json!({
                "ids": [innocent_id, rejected_id],
                "include_vector": true,
                "include_attributes": true,
                "consistency": "strong"
            }))
            .send()
            .await
            .expect("atomicity verification fetch must complete");
        let verification =
            expect_status(verification, StatusCode::OK, "atomicity verification fetch").await;
        let results = verification["results"].as_array().expect("fetch results");
        assert_eq!(results.len(), 2);
        for result in results {
            assert_eq!(result["attributes"]["kind"], "before");
            assert!(
                result["values"] == json!([0.01, 0.0]) || result["values"] == json!([0.02, 0.0])
            );
        }
    }

    finish(harness, &[&namespace]).await;
}

#[tokio::test]
async fn constrained_write_migrates_a_legacy_namespace_and_manifest_once() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_namespace(&admin, &base_url).await;
    upsert(&admin, &base_url, &namespace, corpus()).await;

    let metadata_key = NamespaceMetadata::object_store_key(&namespace);
    let (metadata_body, metadata_object) = harness
        .store
        .get_with_object_metadata(&metadata_key)
        .await
        .expect("fixture namespace metadata read must succeed");
    let original_incarnation = metadata_object
        .user_metadata
        .get("zeppelin-namespace-incarnation")
        .map(str::to_string)
        .expect("new namespace metadata must begin incarnation-bound");
    harness
        .store
        .put(&metadata_key, metadata_body.clone())
        .await
        .expect("legacy metadata fixture write must succeed");

    let legacy = Manifest::read(&harness.store, &namespace)
        .await
        .expect("fixture manifest read must succeed")
        .expect("fixture manifest must exist");
    let legacy_generation = legacy.version();
    let mut legacy_value = serde_json::to_value(&legacy).expect("manifest must serialize to JSON");
    assert!(
        legacy_value
            .as_object_mut()
            .expect("manifest JSON must be an object")
            .remove("namespace_incarnation")
            .is_some(),
        "fixture must actually remove the Phase 4 incarnation field"
    );
    harness
        .store
        .delete(&Manifest::history_key(&namespace, legacy_generation))
        .await
        .expect("legacy manifest fixture must not retain immutable history");
    harness
        .store
        .put(
            &Manifest::object_store_key(&namespace),
            Bytes::from(serde_json::to_vec(&legacy_value).unwrap()),
        )
        .await
        .expect("legacy live-manifest fixture write must succeed");

    let writer = create_principal_with_grant(
        &admin,
        &base_url,
        "legacy-manifest-writer",
        namespace_scope(&namespace),
        &["VectorUpsert"],
        json!({
            "mandatory_filter": tenant_filter(TENANT_A),
            "write_constraints": {"stamp": {"tenant_id": TENANT_A}}
        }),
    )
    .await;
    let response = writer
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({"vectors": [{
            "values": [0.04, 0.0],
            "attributes": {"tier": "gold"}
        }]}))
        .send()
        .await
        .expect("legacy scoped create must complete");
    let created = expect_status(response, StatusCode::OK, "legacy scoped create").await;
    let migrated_write_id = created["generated_ids"][0]["id"]
        .as_str()
        .expect("legacy scoped create must return its generated identity")
        .to_string();

    let (migrated, version) = Manifest::read_versioned(&harness.store, &namespace)
        .await
        .expect("migrated manifest read must succeed")
        .expect("migrated manifest must exist");
    let (migrated_metadata_body, migrated_metadata_object) = harness
        .store
        .get_with_object_metadata(&metadata_key)
        .await
        .expect("migrated namespace metadata read must succeed");
    assert_eq!(
        migrated_metadata_body, metadata_body,
        "incarnation migration must leave namespace metadata bytes unchanged"
    );
    let migrated_incarnation = migrated_metadata_object
        .user_metadata
        .get("zeppelin-namespace-incarnation")
        .expect("the constrained-write seam must restore metadata incarnation");
    assert_ne!(
        migrated_incarnation, original_incarnation,
        "a fully legacy namespace must not trust the process-local pre-migration identity"
    );
    assert_eq!(
        migrated.version(),
        legacy_generation + 2,
        "one generation binds the legacy incarnation and one publishes the guarded upsert"
    );
    zeppelin::wal::ManifestAppendGuard::new(&namespace, &migrated, version)
        .expect("the migrated manifest must be valid guarded-write authority");
    let migrated_uuid = uuid::Uuid::parse_str(migrated_incarnation)
        .expect("migrated metadata incarnation must be a UUID");
    let (same_manifest, _) = Manifest::read_versioned_required_for_incarnation(
        &harness.store,
        &namespace,
        migrated_uuid,
    )
    .await
    .expect("metadata and manifest must converge on one durable incarnation");
    assert_eq!(same_manifest.version(), migrated.version());

    let fetched = admin
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors/get"))
        .json(&json!({
            "ids": [&migrated_write_id],
            "include_vector": false,
            "include_attributes": true,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("migrated write verification must complete");
    let fetched = expect_status(fetched, StatusCode::OK, "migrated write verification").await;
    assert_eq!(fetched["results"][0]["attributes"]["tenant_id"], TENANT_A);

    finish(harness, &[&namespace]).await;
}

#[tokio::test]
async fn scoped_delete_cannot_cross() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_namespace(&admin, &base_url).await;
    upsert(&admin, &base_url, &namespace, corpus()).await;
    let tenant = create_principal_with_grant(
        &admin,
        &base_url,
        "scoped-delete",
        namespace_scope(&namespace),
        &["VectorDelete"],
        constrained_grant_fields(),
    )
    .await;

    let wal_before_invalid = harness
        .store
        .list_prefix(&format!("{namespace}/wal/"))
        .await
        .expect("WAL LIST before invalid delete selectors must succeed");
    for (case, body) in [
        (
            "both selectors",
            json!({
                "ids": ["acme-01"],
                "filter": {"op": "and", "filters": []}
            }),
        ),
        ("neither selector", json!({})),
    ] {
        let response = tenant
            .client
            .delete(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
            .json(&body)
            .send()
            .await
            .unwrap_or_else(|error| panic!("invalid {case} delete must complete: {error}"));
        let body = expect_status(
            response,
            StatusCode::BAD_REQUEST,
            &format!("invalid {case} delete"),
        )
        .await;
        assert_eq!(body["code"], "VALIDATION_ERROR");
        assert_eq!(
            harness
                .store
                .list_prefix(&format!("{namespace}/wal/"))
                .await
                .expect("WAL LIST after invalid delete selector must succeed"),
            wal_before_invalid,
            "invalid {case} delete selector wrote WAL"
        );
    }
    assert_eq!(
        result_ids(&query(&admin, &base_url, &namespace, query_body(16)).await),
        ["acme-01", "acme-02", "acme-03", "bravo-01", "bravo-02", "bravo-03",]
            .into_iter()
            .map(str::to_string)
            .collect(),
        "invalid selector shapes must not delete any rows"
    );

    let response = tenant
        .client
        .delete(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({"ids": ["acme-01", "bravo-01", "missing-id"]}))
        .send()
        .await
        .expect("scoped delete-by-id must complete");
    expect_status(response, StatusCode::NO_CONTENT, "scoped delete-by-id").await;
    assert_eq!(
        result_ids(&query(&admin, &base_url, &namespace, query_body(16)).await),
        ["acme-02", "acme-03", "bravo-01", "bravo-02", "bravo-03"]
            .into_iter()
            .map(str::to_string)
            .collect(),
        "delete-by-id must hide out-of-scope and nonexistent ids behind the same no-op behavior"
    );

    let response = tenant
        .client
        .delete(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({"filter": {"op": "and", "filters": []}}))
        .send()
        .await
        .expect("scoped delete-by-filter must complete");
    expect_status(response, StatusCode::NO_CONTENT, "scoped delete-by-filter").await;

    let remaining = query(&admin, &base_url, &namespace, query_body(16)).await;
    assert_eq!(
        result_ids(&remaining),
        ["bravo-01", "bravo-02", "bravo-03"]
            .into_iter()
            .map(str::to_string)
            .collect()
    );

    finish(harness, &[&namespace]).await;
}

#[tokio::test]
async fn attrless_upsert_cannot_satisfy_a_negative_mandatory_filter() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_namespace(&admin, &base_url).await;
    let writer = create_principal_with_grant(
        &admin,
        &base_url,
        "negative-filter-attrless-writer",
        namespace_scope(&namespace),
        &["VectorUpsert"],
        json!({
            "mandatory_filter": {
                "op": "not_eq",
                "field": "tenant_id",
                "value": TENANT_B
            }
        }),
    )
    .await;
    let wal_before = harness
        .store
        .list_prefix(&format!("{namespace}/wal/"))
        .await
        .expect("WAL LIST before attrless upsert must succeed");

    for (case, vector) in [
        (
            "absent attributes",
            json!({"id": "attrless", "values": [0.1, 0.0]}),
        ),
        (
            "empty attributes",
            json!({"id": "empty-attributes", "values": [0.2, 0.0], "attributes": {}}),
        ),
        (
            "unrelated attributes",
            json!({
                "id": "unrelated-attributes",
                "values": [0.3, 0.0],
                "attributes": {"display_name": "not a tenant scope"}
            }),
        ),
    ] {
        let response = writer
            .client
            .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
            .json(&json!({"vectors": [vector]}))
            .send()
            .await
            .unwrap_or_else(|error| panic!("{case} constrained upsert must complete: {error}"));
        let body = expect_status(
            response,
            StatusCode::FORBIDDEN,
            &format!("{case} constrained upsert"),
        )
        .await;
        assert_eq!(body["code"], "constraint_violation", "case: {case}");
    }
    assert_eq!(
        harness
            .store
            .list_prefix(&format!("{namespace}/wal/"))
            .await
            .expect("WAL LIST after attrless upsert must succeed"),
        wal_before,
        "rejected attrless upsert must not append WAL"
    );

    finish(harness, &[&namespace]).await;
}

#[tokio::test]
async fn constrained_create_without_id_returns_server_owned_identity() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_namespace(&admin, &base_url).await;
    let writer = create_principal_with_grant(
        &admin,
        &base_url,
        "server-owned-vector-identity",
        namespace_scope(&namespace),
        &["VectorUpsert", "VectorFetch"],
        json!({
            "mandatory_filter": tenant_filter(TENANT_A),
            "write_constraints": {"stamp": {"tenant_id": TENANT_A}}
        }),
    )
    .await;

    let response = writer
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [{
                "values": [0.25, 0.5],
                "attributes": {"kind": "server-created"}
            }]
        }))
        .send()
        .await
        .expect("server-owned create must complete");
    let created = expect_status(response, StatusCode::OK, "server-owned create").await;
    assert_eq!(created["upserted"], 1);
    let generated = created["generated_ids"]
        .as_array()
        .expect("server-owned create must return generated_ids");
    assert_eq!(generated.len(), 1);
    assert_eq!(generated[0]["index"], 0);
    let id = generated[0]["id"]
        .as_str()
        .expect("generated identity must be a string");
    assert!(id.starts_with("zv1_"));
    assert!(id.len() <= 128);

    let fetched = writer
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors/get"))
        .json(&json!({
            "ids": [id],
            "include_vector": true,
            "include_attributes": true,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("server-owned identity fetch must complete");
    let fetched = expect_status(fetched, StatusCode::OK, "server-owned identity fetch").await;
    assert!(fetched["missing"].as_array().is_some_and(Vec::is_empty));
    assert_eq!(fetched["results"][0]["id"], id);
    assert_eq!(fetched["results"][0]["attributes"]["tenant_id"], TENANT_A);
    assert_eq!(
        fetched["results"][0]["attributes"]["kind"],
        "server-created"
    );

    let msgpack_body = rmp_serde::to_vec_named(&json!({
        "vectors": [{
            "values": [0.5, 0.25],
            "attributes": {"kind": "msgpack-server-created"}
        }]
    }))
    .expect("scoped MessagePack create body must encode");
    let response = writer
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .header("content-type", "application/msgpack")
        .body(msgpack_body)
        .send()
        .await
        .expect("scoped MessagePack create must complete");
    let created = expect_status(
        response,
        StatusCode::OK,
        "scoped MessagePack server-owned create",
    )
    .await;
    let msgpack_id = created["generated_ids"][0]["id"]
        .as_str()
        .expect("MessagePack create must return a generated identity");
    assert!(msgpack_id.starts_with("zv1_"));
    assert_ne!(msgpack_id, id);

    let fetched = writer
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors/get"))
        .json(&json!({
            "ids": [msgpack_id],
            "include_vector": false,
            "include_attributes": true,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("MessagePack server-owned identity fetch must complete");
    let fetched = expect_status(
        fetched,
        StatusCode::OK,
        "MessagePack server-owned identity fetch",
    )
    .await;
    assert_eq!(fetched["results"][0]["id"], msgpack_id);
    assert_eq!(fetched["results"][0]["attributes"]["tenant_id"], TENANT_A);
    assert_eq!(
        fetched["results"][0]["attributes"]["kind"],
        "msgpack-server-created"
    );

    finish(harness, &[&namespace]).await;
}

#[tokio::test]
async fn constrained_upsert_cannot_reveal_hidden_id_collision() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let absent_world = create_namespace(&admin, &base_url).await;
    let hidden_world = create_namespace(&admin, &base_url).await;
    upsert(
        &admin,
        &base_url,
        &hidden_world,
        json!([{
            "id": "probe",
            "values": [0.9, 0.0],
            "attributes": {"tenant_id": TENANT_B}
        }]),
    )
    .await;
    let writer = create_principal_with_grant(
        &admin,
        &base_url,
        "constrained-upsert-id-oracle",
        global_scope(),
        &["VectorUpsert", "VectorFetch"],
        json!({
            "mandatory_filter": tenant_filter(TENANT_A),
            "write_constraints": {"stamp": {"tenant_id": TENANT_A}}
        }),
    )
    .await;

    async fn normalized_json(response: reqwest::Response) -> (StatusCode, Value) {
        let status = response.status();
        let mut body: Value = response.json().await.expect("response must be JSON");
        if let Some(object) = body.as_object_mut() {
            object.remove("request_id");
        }
        (status, body)
    }

    async fn attempt_upsert(
        client: &reqwest::Client,
        base_url: &str,
        namespace: &str,
    ) -> (StatusCode, Value) {
        normalized_json(
            client
                .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
                .json(&json!({
                    "vectors": [{"id": "probe", "values": [0.1, 0.0]}]
                }))
                .send()
                .await
                .expect("constrained probe upsert must complete"),
        )
        .await
    }

    async fn fetch_probe(
        client: &reqwest::Client,
        base_url: &str,
        namespace: &str,
    ) -> (StatusCode, Value) {
        normalized_json(
            client
                .post(format!("{base_url}/v1/namespaces/{namespace}/vectors/get"))
                .json(&json!({"ids": ["probe"], "include_attributes": true}))
                .send()
                .await
                .expect("constrained probe fetch must complete"),
        )
        .await
    }

    let absent_upsert = attempt_upsert(&writer.client, &base_url, &absent_world).await;
    let hidden_upsert = attempt_upsert(&writer.client, &base_url, &hidden_world).await;
    let absent_fetch = fetch_probe(&writer.client, &base_url, &absent_world).await;
    let hidden_fetch = fetch_probe(&writer.client, &base_url, &hidden_world).await;

    finish(harness, &[&absent_world, &hidden_world]).await;

    assert_eq!(
        (absent_upsert, absent_fetch),
        (hidden_upsert, hidden_fetch),
        "caller-chosen constrained upsert ID distinguishes an absent row from an existing hidden collision"
    );
}

#[tokio::test]
async fn delete_filter_cannot_reference_a_masked_field() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_namespace(&admin, &base_url).await;
    upsert(&admin, &base_url, &namespace, corpus()).await;
    let deleter = create_principal_with_grant(
        &admin,
        &base_url,
        "masked-delete-filter",
        namespace_scope(&namespace),
        &["VectorDelete"],
        json!({"field_mask": {"deny": ["ssn"]}}),
    )
    .await;
    let wal_before = harness
        .store
        .list_prefix(&format!("{namespace}/wal/"))
        .await
        .expect("WAL LIST before masked delete must succeed");

    let response = deleter
        .client
        .delete(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "filter": {"op": "eq", "field": "ssn", "value": "111-11-1111"}
        }))
        .send()
        .await
        .expect("masked-field delete must complete");
    let body = expect_status(response, StatusCode::FORBIDDEN, "masked-field delete").await;
    assert_eq!(body["code"], "constraint_violation");
    assert_eq!(
        harness
            .store
            .list_prefix(&format!("{namespace}/wal/"))
            .await
            .expect("WAL LIST after masked delete must succeed"),
        wal_before,
        "masked-field delete must fail before WAL append"
    );
    assert_eq!(
        result_ids(&query(&admin, &base_url, &namespace, query_body(16)).await),
        ["acme-01", "acme-02", "acme-03", "bravo-01", "bravo-02", "bravo-03"]
            .into_iter()
            .map(str::to_string)
            .collect(),
        "masked-field delete must not remove any row"
    );

    finish(harness, &[&namespace]).await;
}

#[tokio::test]
async fn cursor_stale_after_policy_change() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_namespace(&admin, &base_url).await;
    upsert(&admin, &base_url, &namespace, corpus()).await;
    let tenant = create_principal_with_grant(
        &admin,
        &base_url,
        "cursor-policy",
        namespace_scope(&namespace),
        &["Query"],
        constrained_grant_fields(),
    )
    .await;
    let other_tenant = create_principal_with_grant(
        &admin,
        &base_url,
        "cursor-policy-other-tenant",
        namespace_scope(&namespace),
        &["Query"],
        json!({"mandatory_filter": tenant_filter(TENANT_B)}),
    )
    .await;

    let mut first_page_request = query_body(1);
    first_page_request["cursor"] = json!({"type": "none"});
    let first_page = query(
        &tenant.client,
        &base_url,
        &namespace,
        first_page_request.clone(),
    )
    .await;
    let cursor = first_page["next_cursor"]
        .as_str()
        .expect("first page must return a continuation cursor")
        .to_string();
    let other_cursor = query(
        &other_tenant.client,
        &base_url,
        &namespace,
        first_page_request.clone(),
    )
    .await["next_cursor"]
        .as_str()
        .expect("other tenant first page must return a continuation cursor")
        .to_string();
    let cursor_parts = cursor.split(':').collect::<Vec<_>>();
    let other_cursor_parts = other_cursor.split(':').collect::<Vec<_>>();
    assert_eq!(cursor_parts.len(), 6);
    assert_eq!(other_cursor_parts.len(), 6);
    assert_eq!(cursor_parts[1], other_cursor_parts[1]);
    assert_eq!(
        cursor_parts[2], other_cursor_parts[2],
        "opaque cursor must fingerprint only caller-visible query shape, not a server-only policy predicate"
    );

    let response = admin
        .post(format!("{base_url}/v1/security/grants"))
        .json(&json!({
            "principal_id": tenant.principal_id,
            "scope": namespace_scope(&namespace),
            "actions": {"kind": "selected", "actions": ["NamespaceRead"]}
        }))
        .send()
        .await
        .expect("policy-version bump grant must complete");
    expect_status(response, StatusCode::CREATED, "policy-version bump grant").await;

    let fresh_page = query(
        &tenant.client,
        &base_url,
        &namespace,
        first_page_request.clone(),
    )
    .await;
    let fresh_cursor = fresh_page["next_cursor"]
        .as_str()
        .expect("current policy must issue a fresh continuation cursor");
    let fresh_parts = fresh_cursor.split(':').collect::<Vec<_>>();
    let mut forged_parts = cursor.split(':').map(str::to_string).collect::<Vec<_>>();
    forged_parts[1] = fresh_parts[1].to_string();
    let mut forged_continuation = query_body(1);
    forged_continuation["cursor"] = json!({"type": "after", "token": forged_parts.join(":")});
    let forged_response = tenant
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&forged_continuation)
        .send()
        .await
        .expect("forged cursor request must complete");
    let forged_body = expect_status(
        forged_response,
        StatusCode::BAD_REQUEST,
        "forged current-version cursor",
    )
    .await;
    assert_eq!(forged_body["code"], "VALIDATION_ERROR");
    assert_eq!(
        forged_body["error"], "validation error: invalid cursor token authentication",
        "splicing a fresh policy version onto a stale cursor must fail its authentication tag"
    );

    let mut continuation = query_body(1);
    continuation["cursor"] = json!({"type": "after", "token": cursor});
    let response = tenant
        .client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&continuation)
        .send()
        .await
        .expect("stale cursor request must complete");
    let body = expect_status(response, StatusCode::BAD_REQUEST, "stale policy cursor").await;
    assert_eq!(body["code"], "cursor_policy_stale");

    finish(harness, &[&namespace]).await;
}

#[tokio::test]
async fn conflicting_stamps_are_rejected_before_policy_publication() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_namespace(&admin, &base_url).await;
    let principal = create_principal_with_grant(
        &admin,
        &base_url,
        "conflicting-stamp-publication",
        global_scope(),
        &["Query"],
        json!({"write_constraints": {"stamp": {"tenant_id": TENANT_A}}}),
    )
    .await;

    let policy_head_key = format!("{}/_security/heads/policy.json", harness.prefix);
    let policy_objects_prefix = format!("{}/_security/policies/", harness.prefix);
    let head_before = harness
        .store
        .get(&policy_head_key)
        .await
        .expect("authoritative policy head must exist");
    let parsed_before: PolicyHead =
        serde_json::from_slice(&head_before).expect("policy head must decode");
    let objects_before = harness
        .store
        .list_prefix(&policy_objects_prefix)
        .await
        .expect("policy object LIST before rejection must succeed");

    let response = admin
        .post(format!("{base_url}/v1/security/grants"))
        .json(&json!({
            "principal_id": principal.principal_id,
            "scope": namespace_scope(&namespace),
            "actions": {"kind": "selected", "actions": ["Query"]},
            "write_constraints": {"stamp": {"tenant_id": TENANT_B}}
        }))
        .send()
        .await
        .expect("conflicting grant request must complete");
    let body = expect_status(
        response,
        StatusCode::BAD_REQUEST,
        "conflicting grant publication",
    )
    .await;
    assert_eq!(body["code"], "invalid_security_request");

    let head_after = harness
        .store
        .get(&policy_head_key)
        .await
        .expect("authoritative policy head must remain readable");
    assert_eq!(
        head_after, head_before,
        "rejected grant changed policy head"
    );
    let parsed_after: PolicyHead =
        serde_json::from_slice(&head_after).expect("policy head must still decode");
    assert_eq!(parsed_after.version(), parsed_before.version());
    assert_eq!(
        harness
            .store
            .list_prefix(&policy_objects_prefix)
            .await
            .expect("policy object LIST after rejection must succeed"),
        objects_before,
        "rejected grant uploaded an immutable policy orphan"
    );

    finish(harness, &[&namespace]).await;
}

#[tokio::test]
async fn unknown_nested_mandatory_filter_member_is_rejected_before_publication() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_namespace(&admin, &base_url).await;
    let principal = create_principal_with_grant(
        &admin,
        &base_url,
        "strict-mandatory-filter",
        namespace_scope(&namespace),
        &["NamespaceRead"],
        json!({}),
    )
    .await;
    let policy_head_key = format!("{}/_security/heads/policy.json", harness.prefix);
    let policy_objects_prefix = format!("{}/_security/policies/", harness.prefix);
    let head_before = harness
        .store
        .get(&policy_head_key)
        .await
        .expect("authoritative policy head must exist");
    let objects_before = harness
        .store
        .list_prefix(&policy_objects_prefix)
        .await
        .expect("policy object LIST before strict rejection must succeed");

    let response = admin
        .post(format!("{base_url}/v1/security/grants"))
        .json(&json!({
            "principal_id": principal.principal_id,
            "scope": namespace_scope(&namespace),
            "actions": {"kind": "selected", "actions": ["Query"]},
            "mandatory_filter": {
                "op": "eq",
                "field": "tenant_id",
                "value": TENANT_A,
                "unknown_member": true
            }
        }))
        .send()
        .await
        .expect("unknown nested filter member request must complete");
    assert_eq!(
        response.status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "unknown nested mandatory-filter member must fail strict deserialization"
    );

    assert_eq!(
        harness
            .store
            .get(&policy_head_key)
            .await
            .expect("authoritative policy head must remain readable"),
        head_before,
        "invalid nested filter advanced the authoritative head"
    );
    assert_eq!(
        harness
            .store
            .list_prefix(&policy_objects_prefix)
            .await
            .expect("policy object LIST after strict rejection must succeed"),
        objects_before,
        "invalid nested filter uploaded an immutable policy object"
    );

    finish(harness, &[&namespace]).await;
}

#[tokio::test]
async fn duplicate_stamp_key_is_rejected_before_policy_publication() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let principal = create_principal_with_grant(
        &admin,
        &base_url,
        "duplicate-stamp-key",
        global_scope(),
        &["NamespaceRead"],
        json!({}),
    )
    .await;
    let state_before = policy_publication_state(&harness).await;
    let principal_id = serde_json::to_string(&principal.principal_id)
        .expect("principal ID must encode as JSON string");
    let request = format!(
        r#"{{"principal_id":{principal_id},"scope":{{"kind":"global"}},"actions":{{"kind":"selected","actions":["Query"]}},"write_constraints":{{"stamp":{{"tenant_id":"acme","tenant_id":"bravo"}}}}}}"#
    );

    let response = admin
        .post(format!("{base_url}/v1/security/grants"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(request)
        .send()
        .await
        .expect("duplicate stamp-key request must complete");
    assert_eq!(
        response.status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "duplicate raw stamp keys must fail strict JSON deserialization"
    );
    assert_eq!(
        policy_publication_state(&harness).await,
        state_before,
        "duplicate stamp key changed the policy head or immutable object inventory"
    );

    finish(harness, &[]).await;
}

#[tokio::test]
async fn mandatory_token_entries_empty_after_analysis_are_rejected_before_publication() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let principal = create_principal_with_grant(
        &admin,
        &base_url,
        "analyzed-empty-token",
        global_scope(),
        &["NamespaceRead"],
        json!({}),
    )
    .await;
    let state_before = policy_publication_state(&harness).await;

    for (label, operation, token) in [
        ("whitespace", "contains_all_tokens", "   "),
        ("punctuation", "contains_token_sequence", "!!!"),
    ] {
        let response = admin
            .post(format!("{base_url}/v1/security/grants"))
            .json(&json!({
                "principal_id": principal.principal_id,
                "scope": {"kind": "global"},
                "actions": {"kind": "selected", "actions": ["Query"]},
                "mandatory_filter": {
                    "op": operation,
                    "field": "content",
                    "tokens": [token]
                }
            }))
            .send()
            .await
            .unwrap_or_else(|error| panic!("{label} token request must complete: {error}"));
        let body = expect_status(
            response,
            StatusCode::BAD_REQUEST,
            &format!("{label} mandatory token publication"),
        )
        .await;
        assert_eq!(body["code"], "invalid_security_request");
        assert_eq!(
            policy_publication_state(&harness).await,
            state_before,
            "{label} token changed the policy head or immutable object inventory"
        );
    }

    finish(harness, &[]).await;
}

#[tokio::test]
async fn constrained_attribute_admin_grant_requires_vector_upsert() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let principal = create_principal_with_grant(
        &admin,
        &base_url,
        "attribute-admin-constraint-validation",
        global_scope(),
        &["NamespaceRead"],
        json!({}),
    )
    .await;
    let state_before = policy_publication_state(&harness).await;

    for (label, constraint) in [
        (
            "mandatory_filter",
            json!({"mandatory_filter": tenant_filter(TENANT_A)}),
        ),
        ("field_mask", json!({"field_mask": {"deny": ["ssn"]}})),
        (
            "write_constraints",
            json!({"write_constraints": {"forbid_set": ["classification"]}}),
        ),
    ] {
        let mut request = json!({
            "principal_id": principal.principal_id,
            "scope": global_scope(),
            "actions": {"kind": "selected", "actions": ["AttributeAdmin"]}
        });
        request
            .as_object_mut()
            .expect("grant request must be an object")
            .extend(
                constraint
                    .as_object()
                    .expect("constraint fixture must be an object")
                    .clone(),
            );
        let response = admin
            .post(format!("{base_url}/v1/security/grants"))
            .json(&request)
            .send()
            .await
            .unwrap_or_else(|error| panic!("{label} grant request must complete: {error}"));
        let body = expect_status(
            response,
            StatusCode::BAD_REQUEST,
            &format!("AttributeAdmin-only {label} grant"),
        )
        .await;
        assert_eq!(body["code"], "invalid_security_request");
        assert_eq!(
            policy_publication_state(&harness).await,
            state_before,
            "AttributeAdmin-only {label} grant changed authoritative policy state"
        );
    }

    let response = admin
        .post(format!("{base_url}/v1/security/grants"))
        .json(&json!({
            "principal_id": principal.principal_id,
            "scope": global_scope(),
            "actions": {
                "kind": "selected",
                "actions": ["AttributeAdmin", "VectorUpsert"]
            },
            "mandatory_filter": tenant_filter(TENANT_A),
            "field_mask": {"deny": ["ssn"]},
            "write_constraints": {
                "stamp": {"tenant_id": TENANT_A},
                "forbid_set": ["classification"]
            }
        }))
        .send()
        .await
        .expect("combined AttributeAdmin and VectorUpsert grant request must complete");
    expect_status(
        response,
        StatusCode::CREATED,
        "combined AttributeAdmin and VectorUpsert constrained grant",
    )
    .await;

    finish(harness, &[]).await;
}

#[tokio::test]
async fn attribute_admin_exception() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_namespace(&admin, &base_url).await;
    let attribute_admin = create_principal_with_grant(
        &admin,
        &base_url,
        "attribute-admin",
        namespace_scope(&namespace),
        &["VectorUpsert", "AttributeAdmin"],
        json!({
            "write_constraints": {
                "stamp": {"tenant_id": TENANT_A},
                "forbid_set": ["classification", "is_public", "tenant_id"]
            }
        }),
    )
    .await;

    upsert(
        &attribute_admin.client,
        &base_url,
        &namespace,
        json!([{
            "id": "attribute-admin-write",
            "values": [0.0, 0.0],
            "attributes": {
                "tenant_id": TENANT_B,
                "is_public": true,
                "classification": "declassified"
            }
        }]),
    )
    .await;

    let fetched = admin
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors/get"))
        .json(&json!({
            "ids": ["attribute-admin-write"],
            "include_vector": false,
            "include_attributes": true,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("attribute-admin verification fetch must complete");
    let fetched = expect_status(
        fetched,
        StatusCode::OK,
        "attribute-admin verification fetch",
    )
    .await;
    let attributes = &fetched["results"][0]["attributes"];
    assert_eq!(
        attributes["tenant_id"], TENANT_A,
        "AttributeAdmin bypasses forbid_set but never overrides server stamps"
    );
    assert_eq!(attributes["is_public"], true);
    assert_eq!(attributes["classification"], "declassified");

    finish(harness, &[&namespace]).await;
}
