mod common;

use std::time::Duration;

use common::server::{client_with_bearer, start_test_server_with_config};
use reqwest::StatusCode;
use serde_json::{json, Value};
use zeppelin::config::Config;

async fn wait_for_compaction(client: &reqwest::Client, base_url: &str, namespace: &str) {
    let accepted = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/compact"))
        .send()
        .await
        .expect("manual branch compaction request must complete");
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);

    for _ in 0..200 {
        let response = client
            .get(format!(
                "{base_url}/v1/namespaces/{namespace}/compact/status"
            ))
            .send()
            .await
            .expect("branch compaction status request must complete");
        assert_eq!(response.status(), StatusCode::OK);
        let status: Value = response
            .json()
            .await
            .expect("branch compaction status must decode");
        if status["uncompacted_fragments"] == 0
            && status["segment_count"] == 1
            && status["ready"] == true
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    panic!("manual branch compaction did not reach quiescence");
}

#[tokio::test]
async fn public_status_reports_branch_materialization_after_first_compaction() {
    let mut config = Config::default();
    config.branching.enabled = true;
    config.security.policy_refresh_secs = 3_600;
    let (base_url, harness, _cache, _cache_dir, admin_bearer) =
        start_test_server_with_config(Some(config)).await;
    let client = client_with_bearer(&admin_bearer);
    let source = harness.artifact_origin_namespace("phase10-materialized-source");
    let target = harness.artifact_origin_namespace("phase10-materialized-target");

    let created = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&json!({
            "name": source,
            "dimensions": 4,
            "distance_metric": "cosine"
        }))
        .send()
        .await
        .expect("source create request must complete");
    assert_eq!(created.status(), StatusCode::CREATED);

    let upserted = client
        .post(format!("{base_url}/v1/namespaces/{source}/vectors"))
        .json(&json!({
            "vectors": [{
                "id": "inherited-row",
                "values": [1.0, 0.0, 0.0, 0.0]
            }]
        }))
        .send()
        .await
        .expect("source upsert request must complete");
    assert_eq!(upserted.status(), StatusCode::OK);

    let forked = client
        .post(format!("{base_url}/v1/namespaces/{source}/branches"))
        .json(&json!({ "target": target }))
        .send()
        .await
        .expect("fork request must complete");
    assert_eq!(forked.status(), StatusCode::CREATED);
    let forked: Value = forked.json().await.expect("fork response must decode");
    assert_eq!(forked["materialized"], false);

    wait_for_compaction(&client, &base_url, &target).await;

    let target_status = client
        .get(format!("{base_url}/v1/namespaces/{target}"))
        .send()
        .await
        .expect("target status request must complete");
    assert_eq!(target_status.status(), StatusCode::OK);
    let target_status: Value = target_status
        .json()
        .await
        .expect("target status must decode");
    assert_eq!(
        target_status["branch"]["materialized"], true,
        "target status must derive materialization from its live manifest"
    );

    let children = client
        .get(format!("{base_url}/v1/namespaces/{source}/branches"))
        .send()
        .await
        .expect("direct-child list request must complete");
    assert_eq!(children.status(), StatusCode::OK);
    let children: Value = children.json().await.expect("child list must decode");
    assert_eq!(children["branches"].as_array().map(Vec::len), Some(1));
    assert_eq!(children["branches"][0]["target"]["namespace"], target);
    assert_eq!(
        children["branches"][0]["materialized"], true,
        "direct-child status must derive materialization from the target live manifest"
    );

    harness.cleanup().await;
}
