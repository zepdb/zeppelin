mod common;

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use common::fault_injection::{pause_next_cas_matching, pause_next_get_matching};
use common::harness::TestHarness;
use common::server::{
    client_with_bearer, start_test_server_full, start_test_server_on_store_with_config,
    start_test_server_with_config,
};
use reqwest::StatusCode;
use serde_json::{json, Value};
use zeppelin::config::Config;
use zeppelin::time::{Clock, TimeSource};
use zeppelin::wal::Manifest;

#[derive(Debug)]
struct AdjustableCloneClock(Mutex<DateTime<Utc>>);

impl AdjustableCloneClock {
    fn new(now: DateTime<Utc>) -> Self {
        Self(Mutex::new(now))
    }

    fn advance(&self, duration: ChronoDuration) {
        let mut now = self
            .0
            .lock()
            .expect("clone clock mutex must not be poisoned");
        *now += duration;
    }
}

impl TimeSource for AdjustableCloneClock {
    fn now(&self) -> DateTime<Utc> {
        *self
            .0
            .lock()
            .expect("clone clock mutex must not be poisoned")
    }
}

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

async fn wait_for_branch_materialization(
    client: &reqwest::Client,
    base_url: &str,
    namespace: &str,
) -> Value {
    for _ in 0..200 {
        let response = client
            .get(format!("{base_url}/v1/namespaces/{namespace}"))
            .send()
            .await
            .expect("branch status request must complete while materialization is pending");
        assert_eq!(response.status(), StatusCode::OK);
        let status: Value = response
            .json()
            .await
            .expect("branch namespace status must decode");
        if status["branch"]["materialized"] == true {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    panic!("branch did not report materialized after accepted compaction");
}

async fn create_namespace(client: &reqwest::Client, base_url: &str, namespace: &str) {
    let response = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&json!({
            "name": namespace,
            "dimensions": 4,
            "distance_metric": "cosine"
        }))
        .send()
        .await
        .expect("namespace create request must complete");
    let status = response.status();
    let body = response
        .text()
        .await
        .expect("namespace create response body must be readable");
    assert_eq!(status, StatusCode::CREATED, "{body}");
}

async fn upsert_rows(client: &reqwest::Client, base_url: &str, namespace: &str, vectors: Value) {
    let response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({ "vectors": vectors }))
        .send()
        .await
        .expect("vector upsert request must complete");
    let status = response.status();
    let body = response
        .text()
        .await
        .expect("vector upsert response body must be readable");
    assert_eq!(status, StatusCode::OK, "{body}");
}

async fn fork_namespace(
    client: &reqwest::Client,
    base_url: &str,
    source: &str,
    target: &str,
) -> Value {
    let response = client
        .post(format!("{base_url}/v1/namespaces/{source}/branches"))
        .json(&json!({ "target": target }))
        .send()
        .await
        .expect("fork request must complete");
    let status = response.status();
    let body: Value = response.json().await.expect("fork response must decode");
    assert_eq!(status, StatusCode::CREATED, "{body}");
    body
}

async fn manifest_generation(client: &reqwest::Client, base_url: &str, namespace: &str) -> u64 {
    let response = client
        .get(format!(
            "{base_url}/v1/namespaces/{namespace}/compact/status"
        ))
        .send()
        .await
        .expect("compaction status request must complete");
    assert_eq!(response.status(), StatusCode::OK);
    let status: Value = response
        .json()
        .await
        .expect("compaction status must decode");
    status["manifest_generation"]
        .as_u64()
        .expect("compaction status must expose a manifest generation")
}

async fn create_compacted_source_and_fresh_branch(
    client: &reqwest::Client,
    base_url: &str,
    source: &str,
    branch: &str,
) -> u64 {
    create_namespace(client, base_url, source).await;
    upsert_rows(
        client,
        base_url,
        source,
        json!([
            { "id": "source-row", "values": [1.0, 0.0, 0.0, 0.0] },
            { "id": "second-source-row", "values": [0.0, 1.0, 0.0, 0.0] }
        ]),
    )
    .await;
    wait_for_compaction(client, base_url, source).await;
    let forked = fork_namespace(client, base_url, source, branch).await;
    assert_eq!(forked["materialized"], false);
    manifest_generation(client, base_url, branch).await
}

async fn delete_until_missing(client: &reqwest::Client, base_url: &str, namespace: &str) {
    for _ in 0..20 {
        let status = client
            .get(format!("{base_url}/v1/namespaces/{namespace}"))
            .send()
            .await
            .expect("namespace status request during deletion must complete")
            .status();
        if status == StatusCode::NOT_FOUND {
            return;
        }
        assert!(
            matches!(status, StatusCode::OK | StatusCode::GONE),
            "namespace deletion status must be active, deleting, or absent: {status}"
        );

        let response = client
            .delete(format!("{base_url}/v1/namespaces/{namespace}"))
            .send()
            .await
            .expect("namespace deletion retry must complete");
        assert!(
            matches!(
                response.status(),
                StatusCode::ACCEPTED | StatusCode::NOT_FOUND
            ),
            "namespace deletion retry returned {}: {}",
            response.status(),
            response
                .text()
                .await
                .expect("namespace deletion response body must be readable")
        );
    }

    panic!("namespace {namespace} did not finish deletion after bounded retries");
}

async fn strong_query_ids(
    client: &reqwest::Client,
    base_url: &str,
    namespace: &str,
) -> BTreeSet<String> {
    let response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("strong query request must complete");
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = response.json().await.expect("strong query must decode");
    body["results"]
        .as_array()
        .expect("strong query must return results")
        .iter()
        .map(|result| {
            result["id"]
                .as_str()
                .expect("query result must contain an id")
                .to_string()
        })
        .collect()
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

#[tokio::test]
async fn copy_clone_materializes_an_uncompacted_foreign_branch_view() {
    let mut config = Config::default();
    config.branching.enabled = true;
    config.security.policy_refresh_secs = 3_600;
    let (base_url, harness, _cache, _cache_dir, admin_bearer) =
        start_test_server_with_config(Some(config)).await;
    let client = client_with_bearer(&admin_bearer);
    let source = harness.artifact_origin_namespace("phase10-clone-source");
    let branch = harness.artifact_origin_namespace("phase10-clone-branch");
    let clone = harness.artifact_origin_namespace("phase10-clone-owned");

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

    let source_rows = client
        .post(format!("{base_url}/v1/namespaces/{source}/vectors"))
        .json(&json!({
            "vectors": [
                { "id": "keep", "values": [1.0, 0.0, 0.0, 0.0] },
                { "id": "remove", "values": [0.0, 1.0, 0.0, 0.0] },
                { "id": "update", "values": [0.0, 0.0, 1.0, 0.0] }
            ]
        }))
        .send()
        .await
        .expect("source upsert request must complete");
    assert_eq!(source_rows.status(), StatusCode::OK);
    wait_for_compaction(&client, &base_url, &source).await;

    let forked = client
        .post(format!("{base_url}/v1/namespaces/{source}/branches"))
        .json(&json!({ "target": branch }))
        .send()
        .await
        .expect("fork request must complete");
    assert_eq!(forked.status(), StatusCode::CREATED);

    let branch_rows = client
        .post(format!("{base_url}/v1/namespaces/{branch}/vectors"))
        .json(&json!({
            "vectors": [
                { "id": "local", "values": [1.0, 1.0, 0.0, 0.0] },
                { "id": "update", "values": [0.0, 0.0, 0.0, 1.0] }
            ]
        }))
        .send()
        .await
        .expect("branch upsert request must complete");
    assert_eq!(branch_rows.status(), StatusCode::OK);

    let removed = client
        .delete(format!("{base_url}/v1/namespaces/{branch}/vectors"))
        .json(&json!({ "ids": ["remove"] }))
        .send()
        .await
        .expect("branch delete request must complete");
    assert_eq!(removed.status(), StatusCode::NO_CONTENT);

    assert_eq!(
        strong_query_ids(&client, &base_url, &branch).await,
        BTreeSet::from([
            "keep".to_string(),
            "local".to_string(),
            "update".to_string(),
        ])
    );

    let status = client
        .get(format!("{base_url}/v1/namespaces/{branch}/compact/status"))
        .send()
        .await
        .expect("branch status request must complete");
    assert_eq!(status.status(), StatusCode::OK);
    let status: Value = status.json().await.expect("branch status must decode");
    let generation = status["manifest_generation"]
        .as_u64()
        .expect("branch status must expose its generation")
        .to_string();

    let cloned = client
        .post(format!("{base_url}/v1/namespaces/{branch}/clone"))
        .json(&json!({
            "target": clone,
            "as_of": generation
        }))
        .send()
        .await
        .expect("copy-clone request must complete");
    let cloned_status = cloned.status();
    let cloned: Value = cloned
        .json()
        .await
        .expect("copy-clone response must decode");
    assert_eq!(cloned_status, StatusCode::CREATED, "{cloned}");
    assert_eq!(cloned["mode"], "copy");
    assert_eq!(cloned["namespace"]["branch"], Value::Null);
    assert_eq!(cloned["namespace"]["segment_count"], 1);

    let clone_status = client
        .get(format!("{base_url}/v1/namespaces/{clone}/compact/status"))
        .send()
        .await
        .expect("clone compaction status request must complete");
    assert_eq!(clone_status.status(), StatusCode::OK);
    let clone_status: Value = clone_status
        .json()
        .await
        .expect("clone compaction status must decode");
    assert_eq!(clone_status["ready"], true);
    assert_eq!(clone_status["segment_count"], 1);
    assert_eq!(clone_status["uncompacted_fragments"], 0);

    assert_eq!(
        strong_query_ids(&client, &base_url, &clone).await,
        BTreeSet::from([
            "keep".to_string(),
            "local".to_string(),
            "update".to_string(),
        ])
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn manual_compact_materializes_a_fresh_foreign_segment_branch_without_local_wal() {
    let mut config = Config::default();
    config.branching.enabled = true;
    config.security.policy_refresh_secs = 3_600;
    let (base_url, harness, _cache, _cache_dir, admin_bearer) =
        start_test_server_with_config(Some(config)).await;
    let client = client_with_bearer(&admin_bearer);
    let source = harness.artifact_origin_namespace("phase10-manual-source");
    let branch = harness.artifact_origin_namespace("phase10-manual-branch");

    let before_generation =
        create_compacted_source_and_fresh_branch(&client, &base_url, &source, &branch).await;
    let before = client
        .get(format!("{base_url}/v1/namespaces/{branch}"))
        .send()
        .await
        .expect("fresh branch status request must complete");
    assert_eq!(before.status(), StatusCode::OK);
    let before: Value = before
        .json()
        .await
        .expect("fresh branch status must decode");
    assert_eq!(before["branch"]["materialized"], false);
    assert_eq!(before["segment_count"], 1);

    let compact = client
        .post(format!("{base_url}/v1/namespaces/{branch}/compact"))
        .send()
        .await
        .expect("manual foreign-backed branch compaction must complete");
    let compact_status = compact.status();
    let compact_body: Value = compact
        .json()
        .await
        .expect("manual compaction response must decode");
    assert_eq!(
        compact_status,
        StatusCode::ACCEPTED,
        "foreign visibility is mandatory work even with zero local WAL: {compact_body}"
    );
    assert_eq!(compact_body["status"], "accepted");
    assert_eq!(compact_body["uncompacted_fragments"], 0);

    let materialized = wait_for_branch_materialization(&client, &base_url, &branch).await;
    assert_eq!(materialized["segment_count"], 1);
    let after_generation = manifest_generation(&client, &base_url, &branch).await;
    assert!(
        after_generation > before_generation,
        "materialization must publish a newer target manifest generation"
    );
    assert_eq!(
        strong_query_ids(&client, &base_url, &branch).await,
        BTreeSet::from(["second-source-row".to_string(), "source-row".to_string(),])
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn acknowledged_target_write_wins_over_foreign_branch_clone_publication() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("phase10-race-source");
    let branch = harness.artifact_origin_namespace("phase10-race-branch");
    let clone = harness.artifact_origin_namespace("phase10-race-clone");
    let (store, inherited_segment_read) =
        pause_next_get_matching(&harness.store, format!("{source}/segments/"));
    let mut config = Config::default();
    config.branching.enabled = true;
    config.security.policy_refresh_secs = 3_600;
    config.cache.manifest_cache_ttl_ms = 0;
    config.cache.namespace_registry_ttl_ms = 0;
    let (base_url, _cache, _cache_dir, admin_bearer) = start_test_server_on_store_with_config(
        &harness,
        store,
        Some(harness.prefix.clone()),
        config,
    )
    .await;
    let client = client_with_bearer(&admin_bearer);
    let generation =
        create_compacted_source_and_fresh_branch(&client, &base_url, &source, &branch).await;

    inherited_segment_read.arm();
    let clone_client = client.clone();
    let clone_url = format!("{base_url}/v1/namespaces/{branch}/clone");
    let clone_target = clone.clone();
    let clone_task = tokio::spawn(async move {
        clone_client
            .post(clone_url)
            .json(&json!({
                "target": clone_target,
                "as_of": generation.to_string()
            }))
            .send()
            .await
            .expect("racing clone request must complete")
    });
    tokio::time::timeout(
        Duration::from_secs(15),
        inherited_segment_read.wait_until_paused(),
    )
    .await
    .expect("clone must reach the inherited-segment build barrier");

    upsert_rows(
        &client,
        &base_url,
        &clone,
        json!([
            { "id": "acknowledged-target-row", "values": [0.0, 0.0, 1.0, 0.0] }
        ]),
    )
    .await;
    inherited_segment_read.release();

    let clone_response = tokio::time::timeout(Duration::from_secs(30), clone_task)
        .await
        .expect("clone request must finish after releasing its build barrier")
        .expect("clone task must not panic");
    let clone_status = clone_response.status();
    let clone_body: Value = clone_response
        .json()
        .await
        .expect("clone conflict response must decode");
    assert_eq!(
        clone_status,
        StatusCode::CONFLICT,
        "the exact target bootstrap CAS must lose to the acknowledged write: {clone_body}"
    );
    assert_eq!(
        strong_query_ids(&client, &base_url, &clone).await,
        BTreeSet::from(["acknowledged-target-row".to_string()]),
        "a failed clone must retain the acknowledged target write without adopting source rows"
    );
    assert_eq!(
        strong_query_ids(&client, &base_url, &branch).await,
        BTreeSet::from(["second-source-row".to_string(), "source-row".to_string(),]),
        "a target publication conflict must not mutate the source branch"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn acknowledged_target_write_before_clone_base_capture_is_not_adopted() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("phase10-prebase-source");
    let branch = harness.artifact_origin_namespace("phase10-prebase-branch");
    let clone = harness.artifact_origin_namespace("phase10-prebase-clone");
    let (store, target_base_read) =
        pause_next_get_matching(&harness.store, Manifest::s3_key(&clone));
    let (store, target_activation) = pause_next_cas_matching(&store, format!("{clone}/meta.json"));
    let mut config = Config::default();
    config.branching.enabled = true;
    config.security.policy_refresh_secs = 3_600;
    config.cache.manifest_cache_ttl_ms = 0;
    config.cache.namespace_registry_ttl_ms = 0;
    let (base_url, _cache, _cache_dir, admin_bearer) = start_test_server_on_store_with_config(
        &harness,
        store,
        Some(harness.prefix.clone()),
        config,
    )
    .await;
    let client = client_with_bearer(&admin_bearer);
    let generation =
        create_compacted_source_and_fresh_branch(&client, &base_url, &source, &branch).await;

    target_activation.arm();
    let clone_client = client.clone();
    let clone_url = format!("{base_url}/v1/namespaces/{branch}/clone");
    let clone_target = clone.clone();
    let clone_task = tokio::spawn(async move {
        clone_client
            .post(clone_url)
            .json(&json!({
                "target": clone_target,
                "as_of": generation.to_string()
            }))
            .send()
            .await
            .expect("pre-base-racing clone request must complete")
    });
    tokio::time::timeout(
        Duration::from_secs(15),
        target_activation.wait_until_paused(),
    )
    .await
    .expect("clone must pause after publishing the empty target bootstrap");
    target_base_read.arm();
    target_activation.release();
    tokio::time::timeout(
        Duration::from_secs(15),
        target_base_read.wait_until_paused(),
    )
    .await
    .expect("clone must pause before reading its target bootstrap manifest");

    upsert_rows(
        &client,
        &base_url,
        &clone,
        json!([
            { "id": "pre-base-target-row", "values": [0.0, 0.0, 1.0, 0.0] }
        ]),
    )
    .await;
    let raced_target = Manifest::read(&harness.store, &clone)
        .await
        .expect("racing target manifest read must succeed")
        .expect("racing target manifest must exist");
    assert!(
        raced_target.version() > 1 && !raced_target.uncompacted_fragments().is_empty(),
        "the acknowledged write must be authoritative before clone captures its base"
    );
    target_base_read.release();

    let clone_response = tokio::time::timeout(Duration::from_secs(30), clone_task)
        .await
        .expect("clone request must finish after target base read resumes")
        .expect("clone task must not panic");
    let clone_status = clone_response.status();
    let clone_body: Value = clone_response
        .json()
        .await
        .expect("pre-base conflict response must decode");
    assert_eq!(
        clone_status,
        StatusCode::CONFLICT,
        "a nonempty captured target base must reject clone publication: {clone_body}"
    );
    assert_eq!(
        strong_query_ids(&client, &base_url, &clone).await,
        BTreeSet::from(["pre-base-target-row".to_string()]),
        "clone must neither overwrite nor adopt a write acknowledged before base capture"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn policy_change_during_foreign_branch_clone_leaves_target_empty() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("phase10-policy-source");
    let branch = harness.artifact_origin_namespace("phase10-policy-branch");
    let clone = harness.artifact_origin_namespace("phase10-policy-clone");
    let (store, inherited_segment_read) =
        pause_next_get_matching(&harness.store, format!("{source}/segments/"));
    let mut config = Config::default();
    config.branching.enabled = true;
    config.security.policy_refresh_secs = 3_600;
    config.cache.manifest_cache_ttl_ms = 0;
    config.cache.namespace_registry_ttl_ms = 0;
    let (base_url, _cache, _cache_dir, admin_bearer) = start_test_server_on_store_with_config(
        &harness,
        store,
        Some(harness.prefix.clone()),
        config,
    )
    .await;
    let client = client_with_bearer(&admin_bearer);
    let generation =
        create_compacted_source_and_fresh_branch(&client, &base_url, &source, &branch).await;

    inherited_segment_read.arm();
    let clone_client = client.clone();
    let clone_url = format!("{base_url}/v1/namespaces/{branch}/clone");
    let clone_target = clone.clone();
    let clone_task = tokio::spawn(async move {
        clone_client
            .post(clone_url)
            .json(&json!({
                "target": clone_target,
                "as_of": generation.to_string()
            }))
            .send()
            .await
            .expect("policy-racing clone request must complete")
    });
    tokio::time::timeout(
        Duration::from_secs(15),
        inherited_segment_read.wait_until_paused(),
    )
    .await
    .expect("clone must reach the inherited-segment build barrier");

    let policy_change = client
        .post(format!("{base_url}/v1/security/principals"))
        .json(&json!({
            "principal_id": format!("service:{}-clone-policy-race", harness.prefix),
            "kind": "service",
            "display_name": "clone policy race"
        }))
        .send()
        .await
        .expect("authoritative policy mutation must complete");
    let policy_status = policy_change.status();
    let policy_body: Value = policy_change
        .json()
        .await
        .expect("policy mutation response must decode");
    assert_eq!(policy_status, StatusCode::CREATED, "{policy_body}");
    inherited_segment_read.release();

    let clone_response = tokio::time::timeout(Duration::from_secs(30), clone_task)
        .await
        .expect("clone request must finish after releasing its build barrier")
        .expect("clone task must not panic");
    let clone_status = clone_response.status();
    let clone_body: Value = clone_response
        .json()
        .await
        .expect("authorization failure response must decode");
    assert_eq!(
        clone_status,
        StatusCode::FORBIDDEN,
        "a newer authoritative policy must invalidate the clone proof: {clone_body}"
    );

    let target_status = client
        .get(format!("{base_url}/v1/namespaces/{clone}/compact/status"))
        .send()
        .await
        .expect("failed clone target status request must complete");
    assert_eq!(target_status.status(), StatusCode::OK);
    let target_status: Value = target_status
        .json()
        .await
        .expect("failed clone target status must decode");
    assert_eq!(target_status["segment_count"], 0);
    assert_eq!(target_status["uncompacted_fragments"], 0);
    assert_eq!(target_status["ready"], true);
    assert!(
        strong_query_ids(&client, &base_url, &clone)
            .await
            .is_empty(),
        "policy invalidation must occur before any source row becomes target-visible"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn owned_clone_survives_materialization_and_deletion_of_its_branch_ancestry() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("phase10-ancestry-source");
    let branch = harness.artifact_origin_namespace("phase10-ancestry-branch");
    let clone = harness.artifact_origin_namespace("phase10-ancestry-clone");
    let wall_clock = Arc::new(AdjustableCloneClock::new(Utc::now()));
    let clock = Clock::from_source(wall_clock.clone());
    let mut config = Config::default();
    config.branching.enabled = true;
    config.security.policy_refresh_secs = 3_600;
    config.cache.manifest_cache_ttl_ms = 0;
    config.cache.namespace_registry_ttl_ms = 0;
    config.server.request_timeout_secs = 30;
    config.gc.compaction_upload_window_secs = 1;
    config.gc.skew_slop_secs = 0;
    config.gc.horizon_secs = 31;
    config
        .validate()
        .expect("clone ancestry test config must pass production validation");
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        config,
        false,
        Some(clock),
    )
    .await;
    let client = client_with_bearer(&server.admin_bearer);
    let generation =
        create_compacted_source_and_fresh_branch(&client, &server.base_url, &source, &branch).await;

    let cloned = client
        .post(format!("{}/v1/namespaces/{branch}/clone", server.base_url))
        .json(&json!({
            "target": clone,
            "as_of": generation.to_string()
        }))
        .send()
        .await
        .expect("ancestry clone request must complete");
    let cloned_status = cloned.status();
    let cloned_body: Value = cloned
        .json()
        .await
        .expect("ancestry clone response must decode");
    assert_eq!(cloned_status, StatusCode::CREATED, "{cloned_body}");
    assert_eq!(cloned_body["namespace"]["branch"], Value::Null);
    assert_eq!(cloned_body["namespace"]["segment_count"], 1);

    let branch_compaction = client
        .post(format!(
            "{}/v1/namespaces/{branch}/compact",
            server.base_url
        ))
        .send()
        .await
        .expect("ancestry branch materialization request must complete");
    assert_eq!(branch_compaction.status(), StatusCode::ACCEPTED);
    wait_for_branch_materialization(&client, &server.base_url, &branch).await;

    let initial_branch_delete = client
        .delete(format!("{}/v1/namespaces/{branch}", server.base_url))
        .send()
        .await
        .expect("initial ancestry branch deletion must complete");
    assert_eq!(initial_branch_delete.status(), StatusCode::ACCEPTED);
    wall_clock.advance(ChronoDuration::hours(2));
    delete_until_missing(&client, &server.base_url, &branch).await;
    delete_until_missing(&client, &server.base_url, &source).await;

    assert_eq!(
        strong_query_ids(&client, &server.base_url, &clone).await,
        BTreeSet::from(["second-source-row".to_string(), "source-row".to_string(),]),
        "an ordinary owned clone must not retain physical dependencies on deleted ancestry"
    );

    server.shutdown().await;
    harness.cleanup().await;
}
