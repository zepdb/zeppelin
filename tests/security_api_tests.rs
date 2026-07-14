mod common;

use std::str::FromStr;

use serde_json::json;
use zeppelin::config::Config;
use zeppelin::security::{RouteClass, ROUTE_ACTIONS};

use common::server::{
    start_test_server_with_config, start_test_server_with_config_no_limit_override,
};

fn enforced_config() -> Config {
    Config::from_str(
        r#"
[security]
mode = "enforced"
readyz_public = false

[[security.api_keys]]
key_id = "zpk1_admin"
name = "bootstrap-admin"
sha256_hex = "0f007385b6f9d4b7eeb2748605afe1a984a0a3bfa3f014d09e2a784ce9e5cd1a"
actions = ["*"]
namespaces = ["*"]
"#,
    )
    .unwrap()
}

fn enforced_config_with_expired_key() -> Config {
    Config::from_str(
        r#"
[security]
mode = "enforced"

[[security.api_keys]]
key_id = "zpk1_admin"
name = "bootstrap-admin"
sha256_hex = "0f007385b6f9d4b7eeb2748605afe1a984a0a3bfa3f014d09e2a784ce9e5cd1a"
actions = ["*"]
namespaces = ["*"]

[[security.api_keys]]
key_id = "zpk1_expired"
name = "expired"
sha256_hex = "0f007385b6f9d4b7eeb2748605afe1a984a0a3bfa3f014d09e2a784ce9e5cd1a"
actions = ["Query"]
namespaces = ["*"]
expires_at = "2000-01-01T00:00:00Z"
"#,
    )
    .unwrap()
}

fn bearer_client(token: &str) -> reqwest::Client {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        reqwest::header::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
    );
    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap()
}

#[tokio::test]
async fn healthz_public_readyz_gated() {
    let (base_url, _harness, _cache, _cache_dir, _admin_bearer) =
        start_test_server_with_config(Some(enforced_config())).await;
    let client = reqwest::Client::new();

    let health = client
        .get(format!("{base_url}/healthz"))
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), 200);

    let ready = client
        .get(format!("{base_url}/readyz"))
        .send()
        .await
        .unwrap();
    assert_eq!(ready.status(), 401);
    let body: serde_json::Value = ready.json().await.unwrap();
    let request_id = body["request_id"].as_str().unwrap();
    assert_eq!(
        body,
        json!({
            "code": "unauthenticated",
            "error": "authentication required",
            "request_id": request_id,
            "retryable": false,
            "status": 401
        })
    );

    let mut ready_public_config = enforced_config();
    ready_public_config.security.readyz_public = true;
    let (public_base_url, _harness, _cache, _cache_dir, _admin_bearer) =
        start_test_server_with_config(Some(ready_public_config)).await;
    let public_ready = client
        .get(format!("{public_base_url}/readyz"))
        .send()
        .await
        .unwrap();
    assert_eq!(public_ready.status(), 200);
}

#[tokio::test]
async fn implicit_head_routes_inherit_get_security() {
    let (base_url, _harness, _cache, _cache_dir, admin_bearer) =
        start_test_server_with_config(Some(enforced_config())).await;
    let anonymous = reqwest::Client::new();

    let health = anonymous
        .head(format!("{base_url}/healthz"))
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), 200);

    let denied = anonymous
        .head(format!("{base_url}/readyz"))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 401);

    let allowed = bearer_client(&admin_bearer)
        .head(format!("{base_url}/readyz"))
        .send()
        .await
        .unwrap();
    assert_eq!(allowed.status(), 200);
}

#[tokio::test]
async fn authentication_failures_remain_ip_rate_limited() {
    let mut config = enforced_config();
    config.server.rate_limit_rps = 1;
    config.server.rate_limit_burst = 1;
    let (base_url, _harness, _cache, _cache_dir, _admin_bearer) =
        start_test_server_with_config_no_limit_override(Some(config)).await;
    let client = reqwest::Client::new();

    let first = client
        .get(format!("{base_url}/v1/namespaces/security-rate-limit"))
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 401);

    let second = client
        .get(format!("{base_url}/v1/namespaces/security-rate-limit"))
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), 429);
}

#[tokio::test]
async fn unauthenticated_all_protected_routes_401() {
    let (base_url, _harness, _cache, _cache_dir, _admin_bearer) =
        start_test_server_with_config(Some(enforced_config())).await;
    let client = reqwest::Client::new();

    for entry in ROUTE_ACTIONS {
        if entry.class == RouteClass::Public {
            continue;
        }
        let path = entry
            .path
            .replace(":ns", "security-test")
            .replace(":name", "snapshot-1");
        let method = reqwest::Method::from_bytes(entry.method.as_str().as_bytes()).unwrap();
        let response = client
            .request(method, format!("{base_url}{path}"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            401,
            "{} {} must reject missing authentication",
            entry.method,
            entry.path
        );
        let body: serde_json::Value = response.json().await.unwrap();
        let request_id = body["request_id"].as_str().unwrap();
        assert_eq!(
            body,
            json!({
                "code": "unauthenticated",
                "error": "authentication required",
                "request_id": request_id,
                "retryable": false,
                "status": 401
            }),
            "{} {} returned the wrong security envelope",
            entry.method,
            entry.path
        );
    }
}

#[tokio::test]
async fn wrong_secret_right_key_id_401() {
    let (base_url, _harness, _cache, _cache_dir, _admin_bearer) =
        start_test_server_with_config(Some(enforced_config())).await;
    let response = reqwest::Client::new()
        .get(format!("{base_url}/readyz"))
        .bearer_auth("zpk1_admin.BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        .send()
        .await
        .unwrap();

    assert_security_envelope(
        response,
        401,
        "credential_unknown",
        "authentication required",
    )
    .await;
}

#[tokio::test]
async fn malformed_bearer_401() {
    let (base_url, _harness, _cache, _cache_dir, _admin_bearer) =
        start_test_server_with_config(Some(enforced_config())).await;
    let response = reqwest::Client::new()
        .get(format!("{base_url}/readyz"))
        .header("authorization", "Bearer zpk1_admin.too-short")
        .send()
        .await
        .unwrap();

    assert_security_envelope(
        response,
        401,
        "credential_unknown",
        "authentication required",
    )
    .await;
}

#[tokio::test]
async fn expired_key_401() {
    let (base_url, _harness, _cache, _cache_dir, _admin_bearer) =
        start_test_server_with_config(Some(enforced_config_with_expired_key())).await;
    let response = reqwest::Client::new()
        .get(format!("{base_url}/readyz"))
        .bearer_auth("zpk1_expired.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        .send()
        .await
        .unwrap();

    assert_security_envelope(
        response,
        401,
        "credential_expired",
        "authentication required",
    )
    .await;
}

#[tokio::test]
async fn admin_actions_disjoint() {
    let config = Config::from_str(
        r#"
[security]
mode = "enforced"

[[security.api_keys]]
key_id = "zpk1_writer"
name = "writer"
sha256_hex = "0f007385b6f9d4b7eeb2748605afe1a984a0a3bfa3f014d09e2a784ce9e5cd1a"
actions = ["VectorUpsert"]
namespaces = ["*"]
"#,
    )
    .unwrap();
    let (base_url, _harness, _cache, _cache_dir, _admin_bearer) =
        start_test_server_with_config(Some(config)).await;
    let client = bearer_client("zpk1_writer.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");

    for (method, path) in [
        (reqwest::Method::PATCH, "/v1/config/query"),
        (
            reqwest::Method::POST,
            "/v1/namespaces/security-test/compact",
        ),
        (reqwest::Method::DELETE, "/v1/namespaces/security-test"),
    ] {
        let response = client
            .request(method.clone(), format!("{base_url}{path}"))
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        assert_security_envelope(response, 403, "forbidden", "access forbidden").await;
    }
}

#[tokio::test]
async fn namespace_scoped_key_cannot_cross() {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let namespace_a = format!("security-a-{suffix}");
    let namespace_b = format!("security-b-{suffix}");
    let config = Config::from_str(&format!(
        r#"
[security]
mode = "enforced"

[[security.api_keys]]
key_id = "zpk1_admin"
name = "admin"
sha256_hex = "0f007385b6f9d4b7eeb2748605afe1a984a0a3bfa3f014d09e2a784ce9e5cd1a"
actions = ["*"]
namespaces = ["*"]

[[security.api_keys]]
key_id = "zpk1_scoped"
name = "scoped"
sha256_hex = "0f007385b6f9d4b7eeb2748605afe1a984a0a3bfa3f014d09e2a784ce9e5cd1a"
actions = ["NamespaceRead", "Query", "VectorFetch", "SnapshotRead"]
namespaces = ["{namespace_a}"]
"#
    ))
    .unwrap();
    let (base_url, _harness, _cache, _cache_dir, _admin_bearer) =
        start_test_server_with_config(Some(config)).await;
    let admin = bearer_client("zpk1_admin.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    for namespace in [&namespace_a, &namespace_b] {
        let response = admin
            .post(format!("{base_url}/v1/namespaces"))
            .json(&json!({"name": namespace, "dimensions": 2}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 201);
    }
    let scoped = bearer_client("zpk1_scoped.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");

    let allowed = scoped
        .get(format!("{base_url}/v1/namespaces/{namespace_a}"))
        .send()
        .await
        .unwrap();
    assert_eq!(allowed.status(), 200);
    let denied = scoped
        .get(format!("{base_url}/v1/namespaces/{namespace_b}"))
        .send()
        .await
        .unwrap();
    assert_security_envelope(denied, 403, "namespace_not_granted", "access forbidden").await;

    let denied_query = scoped
        .post(format!("{base_url}/v1/namespaces/{namespace_b}/query"))
        .json(&json!({
            "vector": [1.0, 0.0],
            "top_k": 1,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();
    assert_security_envelope(
        denied_query,
        403,
        "namespace_not_granted",
        "access forbidden",
    )
    .await;
}

#[tokio::test]
async fn namespace_create_is_scoped_to_requested_name() {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let allowed_name = format!("tenant-a-{suffix}");
    let denied_name = format!("tenant-b-{suffix}");
    let config = Config::from_str(&format!(
        r#"
[security]
mode = "enforced"

[[security.api_keys]]
key_id = "zpk1_creator"
name = "scoped-creator"
sha256_hex = "0f007385b6f9d4b7eeb2748605afe1a984a0a3bfa3f014d09e2a784ce9e5cd1a"
actions = ["NamespaceCreate"]
namespaces = ["{allowed_name}"]
"#,
    ))
    .unwrap();
    let (base_url, _harness, _cache, _cache_dir, _admin_bearer) =
        start_test_server_with_config(Some(config)).await;
    let client = bearer_client("zpk1_creator.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");

    let allowed = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&json!({"name": allowed_name, "dimensions": 2}))
        .send()
        .await
        .unwrap();
    assert_eq!(allowed.status(), 201);

    let denied = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&json!({"name": denied_name, "dimensions": 2}))
        .send()
        .await
        .unwrap();
    assert_security_envelope(denied, 403, "namespace_not_granted", "access forbidden").await;
}

#[tokio::test]
async fn clone_requires_target_namespace_create_scope() {
    let config = Config::from_str(
        r#"
[security]
mode = "enforced"

[[security.api_keys]]
key_id = "zpk1_cloner"
name = "scoped-cloner"
sha256_hex = "0f007385b6f9d4b7eeb2748605afe1a984a0a3bfa3f014d09e2a784ce9e5cd1a"
actions = ["NamespaceClone", "NamespaceRead", "NamespaceCreate"]
namespaces = ["source", "allowed-target"]
"#,
    )
    .unwrap();
    let (base_url, _harness, _cache, _cache_dir, _admin_bearer) =
        start_test_server_with_config(Some(config)).await;

    let denied = bearer_client("zpk1_cloner.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        .post(format!("{base_url}/v1/namespaces/source/clone"))
        .json(&json!({"target": "denied-target", "as_of": "1"}))
        .send()
        .await
        .unwrap();
    assert_security_envelope(denied, 403, "namespace_not_granted", "access forbidden").await;
}

#[tokio::test]
async fn clone_requires_source_read_and_namespace_create() {
    let config = Config::from_str(
        r#"
[security]
mode = "enforced"

[[security.api_keys]]
key_id = "zpk1_clone_only"
name = "clone-only"
sha256_hex = "0f007385b6f9d4b7eeb2748605afe1a984a0a3bfa3f014d09e2a784ce9e5cd1a"
actions = ["NamespaceClone"]
namespaces = ["source"]

[[security.api_keys]]
key_id = "zpk1_clone_reader"
name = "clone-reader"
sha256_hex = "0f007385b6f9d4b7eeb2748605afe1a984a0a3bfa3f014d09e2a784ce9e5cd1a"
actions = ["NamespaceClone", "NamespaceRead"]
namespaces = ["source"]
"#,
    )
    .unwrap();
    let (base_url, _harness, _cache, _cache_dir, _admin_bearer) =
        start_test_server_with_config(Some(config)).await;

    for bearer in [
        "zpk1_clone_only.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "zpk1_clone_reader.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    ] {
        let response = bearer_client(bearer)
            .post(format!("{base_url}/v1/namespaces/source/clone"))
            .json(&json!({"target": "target", "as_of": "generation:1"}))
            .send()
            .await
            .unwrap();
        assert_security_envelope(response, 403, "forbidden", "access forbidden").await;
    }
}

#[tokio::test]
async fn open_unsafe_allows_anonymous() {
    let config = Config::from_str(
        r#"
[security]
mode = "open_unsafe"
"#,
    )
    .unwrap();
    let (base_url, _harness, _cache, _cache_dir, _admin_bearer) =
        start_test_server_with_config(Some(config)).await;

    let response = reqwest::Client::new()
        .get(format!("{base_url}/metrics"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn metrics_read_key_can_read_metrics() {
    let config = Config::from_str(
        r#"
[security]
mode = "enforced"

[[security.api_keys]]
key_id = "zpk1_metrics"
name = "metrics-reader"
sha256_hex = "0f007385b6f9d4b7eeb2748605afe1a984a0a3bfa3f014d09e2a784ce9e5cd1a"
actions = ["MetricsRead"]
namespaces = ["*"]
"#,
    )
    .unwrap();
    let (base_url, _harness, _cache, _cache_dir, _admin_bearer) =
        start_test_server_with_config(Some(config)).await;

    let response = bearer_client("zpk1_metrics.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        .get(format!("{base_url}/metrics"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn read_only_key_can_query_cannot_mutate() {
    let namespace = format!("security-read-{}", uuid::Uuid::new_v4().simple());
    let config = Config::from_str(&format!(
        r#"
[security]
mode = "enforced"

[[security.api_keys]]
key_id = "zpk1_admin"
name = "admin"
sha256_hex = "0f007385b6f9d4b7eeb2748605afe1a984a0a3bfa3f014d09e2a784ce9e5cd1a"
actions = ["*"]
namespaces = ["*"]

[[security.api_keys]]
key_id = "zpk1_reader"
name = "reader"
sha256_hex = "0f007385b6f9d4b7eeb2748605afe1a984a0a3bfa3f014d09e2a784ce9e5cd1a"
actions = ["Query", "VectorFetch", "NamespaceRead"]
namespaces = ["{namespace}"]
"#
    ))
    .unwrap();
    let (base_url, _harness, _cache, _cache_dir, _admin_bearer) =
        start_test_server_with_config(Some(config)).await;
    let admin = bearer_client("zpk1_admin.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    let created = admin
        .post(format!("{base_url}/v1/namespaces"))
        .json(&json!({"name": namespace, "dimensions": 2}))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    let upserted = admin
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({"vectors": [{"id": "v1", "values": [1.0, 0.0]}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(upserted.status(), 200);

    let reader = bearer_client("zpk1_reader.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    let query = reader
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&json!({
            "vector": [1.0, 0.0],
            "top_k": 1,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(query.status(), 200);
    let fetch = reader
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors/get"))
        .json(&json!({
            "ids": ["v1"],
            "include_vector": true,
            "include_attributes": true,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(fetch.status(), 200);

    for (method, path) in [
        (
            reqwest::Method::POST,
            format!("/v1/namespaces/{namespace}/vectors"),
        ),
        (
            reqwest::Method::DELETE,
            format!("/v1/namespaces/{namespace}/vectors"),
        ),
        (
            reqwest::Method::DELETE,
            format!("/v1/namespaces/{namespace}"),
        ),
        (
            reqwest::Method::PATCH,
            format!("/v1/namespaces/{namespace}/index_config"),
        ),
        (
            reqwest::Method::POST,
            format!("/v1/namespaces/{namespace}/compact"),
        ),
        (
            reqwest::Method::POST,
            format!("/v1/namespaces/{namespace}/hydrate"),
        ),
        (
            reqwest::Method::PUT,
            format!("/v1/namespaces/{namespace}/snapshots/s1"),
        ),
        (
            reqwest::Method::DELETE,
            format!("/v1/namespaces/{namespace}/snapshots/s1"),
        ),
        (reqwest::Method::PATCH, "/v1/config/query".to_string()),
    ] {
        let response = reader
            .request(method, format!("{base_url}{path}"))
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        assert_security_envelope(response, 403, "forbidden", "access forbidden").await;
    }
}

async fn assert_security_envelope(
    response: reqwest::Response,
    status: u16,
    code: &str,
    error: &str,
) {
    assert_eq!(response.status().as_u16(), status);
    let body: serde_json::Value = response.json().await.unwrap();
    let request_id = body["request_id"].as_str().unwrap();
    assert_eq!(
        body,
        json!({
            "code": code,
            "error": error,
            "request_id": request_id,
            "retryable": false,
            "status": status
        })
    );
}

#[test]
fn route_map_complete() {
    use std::collections::HashSet;

    let source = include_str!("../src/server/mod.rs");
    let registered = parse_registered_routes(source);
    let mapped: HashSet<_> = ROUTE_ACTIONS
        .iter()
        .map(|entry| (entry.method.as_str().to_string(), entry.path.to_string()))
        .collect();

    assert_eq!(registered, mapped);
}

#[tokio::test]
async fn unmapped_fallback_stays_404() {
    let (base_url, _harness, _cache, _cache_dir, _admin_bearer) =
        start_test_server_with_config(Some(enforced_config())).await;

    let response = reqwest::Client::new()
        .get(format!("{base_url}/v1/security/unmapped"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["code"], "NOT_FOUND");
}

fn parse_registered_routes(source: &str) -> std::collections::HashSet<(String, String)> {
    let mut routes = std::collections::HashSet::new();
    let mut remainder = source;
    while let Some(route_offset) = remainder.find(".route(") {
        remainder = &remainder[route_offset + ".route(".len()..];
        let Some(first_quote) = remainder.find('"') else {
            break;
        };
        let after_quote = &remainder[first_quote + 1..];
        let Some(second_quote) = after_quote.find('"') else {
            break;
        };
        let path = &after_quote[..second_quote];
        let call = balanced_route_call(remainder);
        assert!(
            call.contains("secure_route("),
            "registered route {path} must use the central security method wrapper"
        );
        for (needle, method) in [
            ("get(", "GET"),
            ("post(", "POST"),
            ("patch(", "PATCH"),
            (".put(", "PUT"),
            (".delete(", "DELETE"),
        ] {
            if call.contains(needle) {
                routes.insert((method.to_string(), path.to_string()));
            }
        }
        remainder = &remainder[call.len()..];
    }
    #[cfg(not(feature = "profiling"))]
    routes.remove(&("GET".to_string(), "/debug/pprof/cpu".to_string()));
    routes
}

fn balanced_route_call(source: &str) -> &str {
    let mut depth = 1_usize;
    let mut in_string = false;
    let mut escaped = false;
    for (index, character) in source.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
            }
            continue;
        }
        match character {
            '"' => in_string = true,
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return &source[..=index];
                }
            }
            _ => {}
        }
    }
    source
}
