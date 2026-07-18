#![cfg(feature = "branching-test-support")]

mod common;

use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use common::fault_injection::{
    fail_after_put_once_matching, fail_put_once_matching, pause_next_cas_matching,
    pause_next_get_matching,
};
use common::server::{
    client_with_bearer, start_test_server_full,
    start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer,
    start_test_server_on_store_with_config, start_test_server_with_config,
};
use serde_json::{json, Value};
use zeppelin::config::Config;
use zeppelin::namespace::branching::test_support::{
    activate_fork_for_test, branch_control_snapshot, insert_prepared_branch_root,
    prepare_head_branch_root,
};
use zeppelin::namespace::branching::{BranchId, ForkViewDigest};
use zeppelin::namespace::manager::{NamespaceMetadata, NamespaceState};
use zeppelin::namespace::{NamespaceId, NamespaceManager};
use zeppelin::storage::ZeppelinStore;
use zeppelin::time::{Clock, TimeSource};
use zeppelin::types::DistanceMetric;
use zeppelin::wal::Manifest;

#[derive(Debug)]
struct AdjustableWallClock(Mutex<DateTime<Utc>>);

impl AdjustableWallClock {
    fn new(now: DateTime<Utc>) -> Self {
        Self(Mutex::new(now))
    }

    fn set(&self, now: DateTime<Utc>) {
        *self.0.lock().expect("test wall-clock mutex poisoned") = now;
    }
}

impl TimeSource for AdjustableWallClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().expect("test wall-clock mutex poisoned")
    }
}

fn branch_grace_config() -> Config {
    let mut config = Config::default();
    config.branching.enabled = true;
    config.security.policy_refresh_secs = 3_600;
    config.cache.manifest_cache_ttl_ms = 0;
    config.cache.namespace_registry_ttl_ms = 0;
    config.server.request_timeout_secs = 30;
    config.gc.compaction_upload_window_secs = 1;
    config.gc.skew_slop_secs = 0;
    config.gc.horizon_secs = 31;
    config.security.set_cursor_hmac_key_hex("42".repeat(32));
    config
        .validate()
        .expect("branch grace test config must pass production validation");
    assert_eq!(config.gc_horizon_floor_secs(), Some(31));
    config
}

async fn read_namespace_metadata(store: &ZeppelinStore, namespace: &str) -> NamespaceMetadata {
    let bytes = store
        .get(&NamespaceMetadata::s3_key(namespace))
        .await
        .expect("namespace metadata must remain authoritative");
    NamespaceMetadata::from_bytes(&bytes).expect("namespace metadata must decode")
}

fn expected_grace_deadline(observed_at: DateTime<Utc>, floor_secs: u64) -> DateTime<Utc> {
    let rounded_seconds = observed_at
        .checked_add_signed(chrono::Duration::seconds(1))
        .expect("test marker timestamp must not overflow")
        .timestamp();
    DateTime::<Utc>::from_timestamp(rounded_seconds, 0)
        .expect("test marker timestamp must be representable")
        .checked_add_signed(chrono::Duration::seconds(
            i64::try_from(floor_secs).expect("test floor must fit chrono"),
        ))
        .expect("test grace deadline must not overflow")
}

async fn create_source_with_one_row(client: &reqwest::Client, base_url: &str, source: &str) {
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
                "id": "retained-row",
                "values": [1.0, 0.0, 0.0, 0.0]
            }]
        }))
        .send()
        .await
        .expect("source upsert request must complete");
    assert_eq!(upsert.status(), reqwest::StatusCode::OK);
}

async fn activate_branch(store: ZeppelinStore, source: &str, target: &str, config: &Config) {
    activate_fork_for_test(
        store,
        NamespaceId::new(source.to_string()).expect("source namespace must be valid"),
        NamespaceId::new(target.to_string()).expect("target namespace must be valid"),
        config.indexing.clone(),
        config.branching.clone(),
    )
    .await
    .expect("test activation adapter must publish one active branch");
}

async fn assert_parent_delete_is_blocked(client: &reqwest::Client, base_url: &str, source: &str) {
    let response = client
        .delete(format!("{base_url}/v1/namespaces/{source}"))
        .send()
        .await
        .expect("parent delete request must complete");
    assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
    let body: Value = response
        .json()
        .await
        .expect("parent delete conflict must use the JSON envelope");
    assert_eq!(body["code"], "namespace_has_live_branches");
}

#[tokio::test]
async fn http_delete_source_with_live_child_is_a_non_mutating_conflict() {
    let mut config = Config::default();
    config.branching.enabled = true;
    config.security.policy_refresh_secs = 3_600;
    let (base_url, harness, _cache, _cache_dir, admin_bearer) =
        start_test_server_with_config(Some(config)).await;
    let client = client_with_bearer(&admin_bearer);
    let source = harness.artifact_origin_namespace("http-delete-source");
    let target = harness.artifact_origin_namespace("http-delete-child");

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

    let fork = client
        .post(format!("{base_url}/v1/namespaces/{source}/branches"))
        .json(&json!({ "target": target }))
        .send()
        .await
        .expect("fork request must complete");
    assert_eq!(fork.status(), reqwest::StatusCode::CREATED);

    let deletion = client
        .delete(format!("{base_url}/v1/namespaces/{source}"))
        .send()
        .await
        .expect("source delete request must complete");
    assert_eq!(deletion.status(), reqwest::StatusCode::CONFLICT);
    let body: Value = deletion
        .json()
        .await
        .expect("delete conflict must use the JSON error envelope");
    assert_eq!(body["code"], "namespace_has_live_branches");

    let source_get = client
        .get(format!("{base_url}/v1/namespaces/{source}"))
        .send()
        .await
        .expect("source status request must complete");
    assert_eq!(source_get.status(), reqwest::StatusCode::OK);

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn fork_root_winning_http_delete_race_keeps_source_active() {
    let harness = common::harness::TestHarness::new().await;
    let source = harness.artifact_origin_namespace("fork-wins-delete-source");
    let target = harness.artifact_origin_namespace("fork-wins-delete-target");
    let (store, deletion_meta_cas) =
        pause_next_cas_matching(&harness.store, format!("{source}/meta.json"));
    let mut config = Config::default();
    config.security.policy_refresh_secs = 3_600;
    let (base_url, _cache, _cache_dir, admin_bearer) =
        start_test_server_on_store_with_config(&harness, store.clone(), None, config).await;
    let client = client_with_bearer(&admin_bearer);

    NamespaceManager::new(store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .expect("source fixture must be created before arming the deletion CAS pause");
    deletion_meta_cas.arm();

    let delete_client = client.clone();
    let delete_url = format!("{base_url}/v1/namespaces/{source}");
    let mut deletion = tokio::spawn(async move {
        delete_client
            .delete(delete_url)
            .send()
            .await
            .expect("source delete request must complete")
    });
    tokio::select! {
        () = deletion_meta_cas.wait_until_paused() => {}
        response = &mut deletion => {
            let response = response.expect("delete task must not panic");
            panic!("delete returned {} before reaching its metadata CAS", response.status());
        }
    }
    let root = prepare_head_branch_root(
        store.clone(),
        &source,
        BranchId::new(),
        &target,
        uuid::Uuid::new_v4(),
        ForkViewDigest::new([0x5a; 32]),
        Utc::now(),
    )
    .await
    .expect("fork root candidate must bind the active source head");
    insert_prepared_branch_root(store, &source, root, 8)
        .await
        .expect("fork root must win while delete metadata CAS is paused");
    deletion_meta_cas.release();

    let deletion = deletion.await.expect("delete task must not panic");
    assert_eq!(deletion.status(), reqwest::StatusCode::CONFLICT);
    let body: Value = deletion
        .json()
        .await
        .expect("delete conflict must use the JSON error envelope");
    assert_eq!(body["code"], "namespace_has_live_branches");

    let status = client
        .get(format!("{base_url}/v1/namespaces/{source}"))
        .send()
        .await
        .expect("source status request must complete");
    assert_eq!(status.status(), reqwest::StatusCode::OK);
    let body: Value = status
        .json()
        .await
        .expect("source status must use the JSON namespace response");
    assert_eq!(body["state"], "active");

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn branch_delete_keeps_admitted_reader_and_root_through_restart() {
    let harness = common::harness::TestHarness::new().await;
    let source = harness.artifact_origin_namespace("grace-reader-source");
    let target = harness.artifact_origin_namespace("grace-reader-target");
    let (store, source_wal_get) = pause_next_get_matching(&harness.store, format!("{source}/wal/"));
    let wall_clock = Arc::new(AdjustableWallClock::new(Utc::now()));
    let clock = Clock::from_source(wall_clock.clone());
    let config = branch_grace_config();
    let server = start_test_server_full(
        store.clone(),
        Some(harness.prefix.clone()),
        config.clone(),
        false,
        Some(clock.clone()),
    )
    .await;
    let admin_bearer = server.admin_bearer.clone();
    let client = client_with_bearer(&admin_bearer);

    create_source_with_one_row(&client, &server.base_url, &source).await;
    activate_branch(store.clone(), &source, &target, &config).await;
    server.clear_wal_fragment_cache();
    source_wal_get.arm();

    let query_client = client.clone();
    let query_url = format!("{}/v1/namespaces/{target}/query", server.base_url);
    let mut admitted_query = tokio::spawn(async move {
        query_client
            .post(query_url)
            .json(&json!({
                "vector": [1.0, 0.0, 0.0, 0.0],
                "top_k": 10,
                "consistency": "strong"
            }))
            .send()
            .await
            .expect("admitted branch query must complete")
    });
    tokio::select! {
        () = source_wal_get.wait_until_paused() => {}
        response = &mut admitted_query => {
            let response = response.expect("branch query task must not panic");
            let status = response.status();
            let body = response
                .text()
                .await
                .expect("premature branch query response body must be readable");
            panic!(
                "branch query returned {status} before reaching its inherited WAL read: {body}"
            );
        }
    }

    let deletion = client
        .delete(format!("{}/v1/namespaces/{target}", server.base_url))
        .send()
        .await
        .expect("branch delete request must complete");
    assert_eq!(deletion.status(), reqwest::StatusCode::ACCEPTED);
    assert!(
        Manifest::read(&harness.store, &target)
            .await
            .expect("target manifest absence must be readable")
            .is_none(),
        "accepted deletion must remove target live visibility"
    );

    let hidden_query = client
        .post(format!("{}/v1/namespaces/{target}/query", server.base_url))
        .json(&json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("post-delete target query must complete");
    assert_eq!(hidden_query.status(), reqwest::StatusCode::GONE);
    assert_parent_delete_is_blocked(&client, &server.base_url, &source).await;

    let root_snapshot = branch_control_snapshot(&harness.store, &source)
        .await
        .expect("parent root map must remain readable");
    assert_eq!(root_snapshot.roots.len(), 1);
    let metadata = read_namespace_metadata(&harness.store, &target).await;
    assert_eq!(metadata.state, NamespaceState::Deleting);
    let intent = metadata
        .deletion_intent
        .as_ref()
        .expect("deleting branch must retain its intent");
    let visibility = intent
        .visibility
        .clone()
        .expect("deleting branch must persist visibility removal");
    let marker_head = harness
        .store
        .head(&visibility.marker_key)
        .await
        .expect("visibility marker head must be authoritative");
    assert_eq!(marker_head.last_modified, visibility.observed_at);
    assert_eq!(
        visibility.not_before,
        expected_grace_deadline(marker_head.last_modified, 31)
    );
    let marker: Value = serde_json::from_slice(
        &harness
            .store
            .get(&visibility.marker_key)
            .await
            .expect("visibility marker body must exist"),
    )
    .expect("visibility marker body must decode");
    assert_eq!(marker["domain"], "zeppelin.branch-visibility-removed.v1");
    assert_eq!(marker["target_namespace"], target);
    assert_eq!(marker["reader_safety_floor_secs"], 31);
    assert_eq!(
        marker["fenced_generation"].as_u64(),
        intent.fenced_generation
    );
    assert_eq!(
        marker["destruction_record_key"].as_str(),
        Some(intent.destruction_record_key.as_str())
    );
    for digest in ["intent_sha256", "parent_root_sha256"] {
        let value = marker[digest]
            .as_str()
            .expect("marker digest must be a string");
        assert_eq!(value.len(), 64);
        assert!(value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
    }

    source_wal_get.release();
    let admitted_response = admitted_query
        .await
        .expect("admitted branch query task must not panic");
    assert_eq!(admitted_response.status(), reqwest::StatusCode::OK);
    let body: Value = admitted_response
        .json()
        .await
        .expect("admitted query response must decode");
    assert_eq!(body["results"][0]["id"], "retained-row");

    server.shutdown().await;
    wall_clock.set(
        visibility
            .not_before
            .checked_sub_signed(chrono::Duration::nanoseconds(1))
            .expect("pre-deadline test time must be representable"),
    );
    let restarted = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        store,
        Some(harness.prefix.clone()),
        config,
        false,
        Some(clock),
        100 * 1024 * 1024,
        &admin_bearer,
    )
    .await;
    let restarted_client = client_with_bearer(&admin_bearer);
    let restarted_metadata = read_namespace_metadata(&harness.store, &target).await;
    assert_eq!(
        restarted_metadata
            .deletion_intent
            .as_ref()
            .and_then(|intent| intent.visibility.as_ref()),
        Some(&visibility),
        "restart must preserve the exact S3-derived grace deadline"
    );
    assert!(!branch_control_snapshot(&harness.store, &source)
        .await
        .expect("parent root map must remain readable after restart")
        .roots
        .is_empty());
    assert_parent_delete_is_blocked(&restarted_client, &restarted.base_url, &source).await;
    let still_hidden = restarted_client
        .post(format!(
            "{}/v1/namespaces/{target}/query",
            restarted.base_url
        ))
        .json(&json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("restarted target query must complete");
    assert_eq!(still_hidden.status(), reqwest::StatusCode::GONE);

    restarted.shutdown().await;
    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn restart_after_marker_gap_starts_a_fresh_full_grace() {
    let harness = common::harness::TestHarness::new().await;
    let source = harness.artifact_origin_namespace("marker-gap-source");
    let target = harness.artifact_origin_namespace("marker-gap-target");
    let marker_prefix = format!("{target}/_lifecycle/branch_visibility_removed/");
    let (store, marker_failure) = fail_put_once_matching(&harness.store, marker_prefix.clone());
    let wall_clock = Arc::new(AdjustableWallClock::new(Utc::now()));
    let clock = Clock::from_source(wall_clock.clone());
    let config = branch_grace_config();
    let server = start_test_server_full(
        store.clone(),
        Some(harness.prefix.clone()),
        config.clone(),
        false,
        Some(clock.clone()),
    )
    .await;
    let admin_bearer = server.admin_bearer.clone();
    let client = client_with_bearer(&admin_bearer);
    create_source_with_one_row(&client, &server.base_url, &source).await;
    activate_branch(store.clone(), &source, &target, &config).await;

    let failed_delete = client
        .delete(format!("{}/v1/namespaces/{target}", server.base_url))
        .send()
        .await
        .expect("faulted branch delete request must complete");
    assert_eq!(
        failed_delete.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(marker_failure.failures_injected(), 1);
    assert!(Manifest::read(&harness.store, &target)
        .await
        .expect("target manifest absence must be readable")
        .is_none());
    let interrupted = read_namespace_metadata(&harness.store, &target).await;
    assert_eq!(interrupted.state, NamespaceState::Deleting);
    assert_eq!(
        interrupted
            .deletion_intent
            .as_ref()
            .and_then(|intent| intent.visibility.as_ref()),
        None
    );
    assert!(
        harness
            .store
            .list_prefix(&marker_prefix)
            .await
            .expect("marker prefix must be listable")
            .is_empty(),
        "failed marker publication must not invent an observation time"
    );
    assert_parent_delete_is_blocked(&client, &server.base_url, &source).await;
    server.shutdown().await;

    let restarted = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        store.clone(),
        Some(harness.prefix.clone()),
        config.clone(),
        false,
        Some(clock.clone()),
        100 * 1024 * 1024,
        &admin_bearer,
    )
    .await;
    let restarted_client = client_with_bearer(&admin_bearer);
    let retry = restarted_client
        .delete(format!("{}/v1/namespaces/{target}", restarted.base_url))
        .send()
        .await
        .expect("branch delete retry must complete");
    assert_eq!(retry.status(), reqwest::StatusCode::ACCEPTED);
    restarted.shutdown().await;

    let resumed = read_namespace_metadata(&harness.store, &target).await;
    let visibility = resumed
        .deletion_intent
        .as_ref()
        .and_then(|intent| intent.visibility.clone())
        .expect("restart recovery must persist a fresh visibility marker");
    let marker_head = harness
        .store
        .head(&visibility.marker_key)
        .await
        .expect("recovered marker head must exist");
    assert_eq!(visibility.observed_at, marker_head.last_modified);
    assert_eq!(
        visibility.not_before,
        expected_grace_deadline(marker_head.last_modified, 31)
    );
    assert!(!branch_control_snapshot(&harness.store, &source)
        .await
        .expect("parent root map must remain readable")
        .roots
        .is_empty());

    wall_clock.set(
        visibility
            .not_before
            .checked_sub_signed(chrono::Duration::nanoseconds(1))
            .expect("pre-deadline test time must be representable"),
    );
    let before_deadline = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        store,
        Some(harness.prefix.clone()),
        config,
        false,
        Some(clock),
        100 * 1024 * 1024,
        &admin_bearer,
    )
    .await;
    let deadline_client = client_with_bearer(&admin_bearer);
    assert_parent_delete_is_blocked(&deadline_client, &before_deadline.base_url, &source).await;
    before_deadline.shutdown().await;

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn restart_after_marker_commit_adopts_its_creator_bound_grace_floor() {
    let harness = common::harness::TestHarness::new().await;
    let source = harness.artifact_origin_namespace("marker-floor-source");
    let target = harness.artifact_origin_namespace("marker-floor-target");
    let marker_prefix = format!("{target}/_lifecycle/branch_visibility_removed/");
    let (store, marker_lost_reply) =
        fail_after_put_once_matching(&harness.store, marker_prefix.clone());
    let wall_clock = Arc::new(AdjustableWallClock::new(Utc::now()));
    let clock = Clock::from_source(wall_clock.clone());
    let mut creator_config = branch_grace_config();
    creator_config.cache.namespace_registry_ttl_ms = 10_000;
    creator_config.gc.horizon_secs = 41;
    creator_config
        .validate()
        .expect("marker creator config must pass production validation");
    assert_eq!(creator_config.gc_horizon_floor_secs(), Some(41));
    let server = start_test_server_full(
        store.clone(),
        Some(harness.prefix.clone()),
        creator_config.clone(),
        false,
        Some(clock.clone()),
    )
    .await;
    let admin_bearer = server.admin_bearer.clone();
    let client = client_with_bearer(&admin_bearer);
    create_source_with_one_row(&client, &server.base_url, &source).await;
    activate_branch(store.clone(), &source, &target, &creator_config).await;

    let interrupted = client
        .delete(format!("{}/v1/namespaces/{target}", server.base_url))
        .send()
        .await
        .expect("lost marker reply delete request must complete");
    assert_eq!(
        interrupted.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(marker_lost_reply.failures_injected(), 1);
    let interrupted_metadata = read_namespace_metadata(&harness.store, &target).await;
    assert_eq!(interrupted_metadata.state, NamespaceState::Deleting);
    assert!(interrupted_metadata
        .deletion_intent
        .as_ref()
        .and_then(|intent| intent.visibility.as_ref())
        .is_none());
    let marker_keys = harness
        .store
        .list_prefix(&marker_prefix)
        .await
        .expect("committed marker prefix must be listable");
    assert_eq!(marker_keys.len(), 1);
    let marker_head = harness
        .store
        .head(&marker_keys[0])
        .await
        .expect("committed marker head must exist");
    server.shutdown().await;

    let adopter_config = branch_grace_config();
    assert_eq!(adopter_config.gc_horizon_floor_secs(), Some(31));
    let restarted = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        store,
        Some(harness.prefix.clone()),
        adopter_config,
        false,
        Some(clock),
        100 * 1024 * 1024,
        &admin_bearer,
    )
    .await;
    let retry = client_with_bearer(&admin_bearer)
        .delete(format!("{}/v1/namespaces/{target}", restarted.base_url))
        .send()
        .await
        .expect("branch deletion retry must adopt the committed marker");
    assert_eq!(retry.status(), reqwest::StatusCode::ACCEPTED);
    restarted.shutdown().await;

    let resumed = read_namespace_metadata(&harness.store, &target).await;
    let visibility = resumed
        .deletion_intent
        .as_ref()
        .and_then(|intent| intent.visibility.as_ref())
        .expect("retry must persist creator-bound visibility grace");
    assert_eq!(visibility.observed_at, marker_head.last_modified);
    assert_eq!(
        visibility.not_before,
        expected_grace_deadline(marker_head.last_modified, 41),
        "retry must not shorten the marker creator's durable floor"
    );
    let marker: Value = serde_json::from_slice(
        &harness
            .store
            .get(&visibility.marker_key)
            .await
            .expect("creator-bound marker body must remain readable"),
    )
    .expect("creator-bound marker must decode");
    assert_eq!(marker["reader_safety_floor_secs"], 41);

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn branch_delete_uses_live_lineage_after_generation_one_history_is_pruned() {
    let harness = common::harness::TestHarness::new().await;
    let source = harness.artifact_origin_namespace("pruned-genesis-source");
    let target = harness.artifact_origin_namespace("pruned-genesis-target");
    let config = branch_grace_config();
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        config.clone(),
        false,
        None,
    )
    .await;
    let client = client_with_bearer(&server.admin_bearer);
    create_source_with_one_row(&client, &server.base_url, &source).await;
    activate_branch(harness.store.clone(), &source, &target, &config).await;
    harness
        .store
        .delete(&Manifest::history_key(&target, 1))
        .await
        .expect("test must simulate legitimate generation-one history pruning");

    let deletion = client
        .delete(format!("{}/v1/namespaces/{target}", server.base_url))
        .send()
        .await
        .expect("branch deletion after history pruning must complete");
    assert_eq!(deletion.status(), reqwest::StatusCode::ACCEPTED);
    let metadata = read_namespace_metadata(&harness.store, &target).await;
    assert_eq!(metadata.state, NamespaceState::Deleting);
    assert!(metadata
        .deletion_intent
        .as_ref()
        .and_then(|intent| intent.visibility.as_ref())
        .is_some());

    server.shutdown().await;
    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}
