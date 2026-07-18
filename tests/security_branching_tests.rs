mod common;

use common::server::{client_with_bearer, start_test_server_with_config};
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
    let query_body: Value = query.json().await.expect("target query response must decode");
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
