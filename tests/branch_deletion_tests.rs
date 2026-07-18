#![cfg(feature = "branching-test-support")]

mod common;

use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use common::fault_injection::{
    fail_after_put_once_matching, fail_delete_once_matching, fail_put_once_matching,
    pause_next_cas_matching, pause_next_get_matching,
};
use common::server::{
    client_with_bearer, start_test_server_full,
    start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer,
    start_test_server_on_store_with_config, start_test_server_with_config, FullTestServer,
};
use serde_json::{json, Value};
use zeppelin::config::Config;
use zeppelin::namespace::branching::test_support::{
    activate_fork_for_test, branch_control_snapshot, insert_prepared_branch_root,
    prepare_head_branch_root, remove_prepared_branch_root,
    resume_delete_with_config_and_clock_for_test,
};
use zeppelin::namespace::branching::{
    BranchError, BranchId, ForkViewDigest, NamespaceDeleteOutcome,
};
use zeppelin::namespace::manager::{
    NamespaceMetadata, NamespaceState, RootReleaseState, VisibilityRemoval,
};
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

struct EstablishedBranchGrace {
    config: Config,
    wall_clock: Arc<AdjustableWallClock>,
    clock: Clock,
    admin_bearer: String,
    visibility: VisibilityRemoval,
}

async fn establish_branch_grace(
    harness: &common::harness::TestHarness,
    source: &str,
    target: &str,
) -> EstablishedBranchGrace {
    let config = branch_grace_config();
    let wall_clock = Arc::new(AdjustableWallClock::new(Utc::now()));
    let clock = Clock::from_source(wall_clock.clone());
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        config.clone(),
        false,
        Some(clock.clone()),
    )
    .await;
    let admin_bearer = server.admin_bearer.clone();
    let client = client_with_bearer(&admin_bearer);
    create_source_with_one_row(&client, &server.base_url, source).await;
    activate_branch(server.store.clone(), source, target, &config).await;

    let deletion = client
        .delete(format!("{}/v1/namespaces/{target}", server.base_url))
        .send()
        .await
        .expect("initial branch deletion must complete");
    assert_eq!(deletion.status(), reqwest::StatusCode::ACCEPTED);
    let deleting = read_namespace_metadata(&harness.store, target).await;
    let visibility = deleting
        .deletion_intent
        .as_ref()
        .and_then(|intent| intent.visibility.clone())
        .expect("initial branch deletion must persist its grace boundary");
    assert_eq!(
        branch_control_snapshot(&harness.store, source)
            .await
            .expect("parent root must remain readable during grace")
            .roots
            .len(),
        1,
        "initial branch deletion must retain exactly one parent root"
    );
    server.shutdown().await;

    EstablishedBranchGrace {
        config,
        wall_clock,
        clock,
        admin_bearer,
        visibility,
    }
}

async fn start_branch_recovery_server(
    harness: &common::harness::TestHarness,
    store: ZeppelinStore,
    fixture: &EstablishedBranchGrace,
) -> FullTestServer {
    start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        store,
        Some(harness.prefix.clone()),
        fixture.config.clone(),
        false,
        Some(fixture.clock.clone()),
        100 * 1024 * 1024,
        &fixture.admin_bearer,
    )
    .await
}

async fn resume_branch_delete(
    server: &FullTestServer,
    target: &str,
    fixture: &EstablishedBranchGrace,
) -> zeppelin::error::Result<NamespaceDeleteOutcome> {
    resume_delete_with_config_and_clock_for_test(
        server.store.clone(),
        NamespaceId::new(target.to_string()).expect("target namespace must be valid"),
        &fixture.config,
        fixture.clock.clone(),
        std::time::Duration::from_secs(1),
    )
    .await
}

fn assert_final_root_release(metadata: &NamespaceMetadata, converged: bool) {
    let release = metadata
        .deletion_intent
        .as_ref()
        .and_then(|intent| intent.root_release.as_ref())
        .expect("interrupted cleanup must retain a final root-release acknowledgement");
    assert_eq!(
        matches!(release, RootReleaseState::Converged { .. }),
        converged,
        "root-release acknowledgement variant must match whether this pass removed the root"
    );
    assert_eq!(
        matches!(release, RootReleaseState::Released { .. }),
        !converged,
        "root-release acknowledgement must be final"
    );
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

#[tokio::test]
async fn branch_root_absent_before_grace_is_an_integrity_error() {
    let harness = common::harness::TestHarness::new().await;
    let source = harness.artifact_origin_namespace("early-root-loss-source");
    let target = harness.artifact_origin_namespace("early-root-loss-target");
    let fixture = establish_branch_grace(&harness, &source, &target).await;
    fixture.wall_clock.set(
        fixture
            .visibility
            .not_before
            .checked_sub_signed(chrono::Duration::nanoseconds(1))
            .expect("pre-grace test instant must be representable"),
    );
    let server = start_branch_recovery_server(&harness, harness.store.clone(), &fixture).await;
    let root = branch_control_snapshot(&harness.store, &source)
        .await
        .expect("parent root must remain authoritative")
        .roots
        .into_iter()
        .next()
        .expect("active branch must retain one parent root");
    remove_prepared_branch_root(server.store.clone(), &source, root)
        .await
        .expect("test must simulate premature root loss");

    let resumed = resume_branch_delete(&server, &target, &fixture).await;
    server.shutdown().await;

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;

    assert!(matches!(
        resumed,
        Err(zeppelin::error::ZeppelinError::Branch(error))
            if matches!(error.as_ref(), BranchError::BranchRootMissing { .. })
    ));
}

#[tokio::test]
async fn slice_five_crash_before_parent_root_cas_retains_root_and_retry_completes() {
    let harness = common::harness::TestHarness::new().await;
    let source = harness.artifact_origin_namespace("root-cas-crash-source");
    let target = harness.artifact_origin_namespace("root-cas-crash-target");
    let fixture = establish_branch_grace(&harness, &source, &target).await;
    fixture.wall_clock.set(fixture.visibility.not_before);

    let (paused_store, parent_root_cas) =
        pause_next_cas_matching(&harness.store, format!("{source}/manifest.json"));
    let server = start_branch_recovery_server(&harness, paused_store.clone(), &fixture).await;
    parent_root_cas.arm();
    let resume_store = server.store.clone();
    let resume_target = NamespaceId::new(target.clone()).expect("target namespace must be valid");
    let resume_config = fixture.config.clone();
    let resume_clock = fixture.clock.clone();
    let mut resume = tokio::spawn(async move {
        resume_delete_with_config_and_clock_for_test(
            resume_store,
            resume_target,
            &resume_config,
            resume_clock,
            std::time::Duration::from_secs(1),
        )
        .await
    });
    tokio::select! {
        () = parent_root_cas.wait_until_paused() => {}
        outcome = &mut resume => {
            panic!("branch recovery returned before its parent-root CAS: {outcome:?}");
        }
    }
    resume.abort();
    assert!(
        resume
            .await
            .expect_err("crash simulation must cancel the paused recovery task")
            .is_cancelled(),
        "paused recovery task must end by cancellation"
    );
    server
        .abort_and_drop()
        .await
        .expect("crashed recovery server must retire cleanly");

    assert_eq!(
        branch_control_snapshot(&harness.store, &source)
            .await
            .expect("parent root must remain authoritative after the crash")
            .roots
            .len(),
        1,
        "a crash before the CAS must not remove the parent root"
    );
    let interrupted = read_namespace_metadata(&harness.store, &target).await;
    let interrupted_intent = interrupted
        .deletion_intent
        .as_ref()
        .expect("crashed branch deletion must retain its recovery intent");
    assert_eq!(
        interrupted_intent.visibility.as_ref(),
        Some(&fixture.visibility),
        "crash recovery must not rewrite the durable grace boundary"
    );
    assert_eq!(interrupted_intent.root_release, None);
    assert!(Manifest::read(&harness.store, &target)
        .await
        .expect("target visibility absence must remain readable")
        .is_none());

    fixture.wall_clock.set(
        fixture
            .visibility
            .not_before
            .checked_add_signed(chrono::Duration::seconds(
                i64::try_from(fixture.config.compaction.lease_duration_secs)
                    .expect("test lease duration must fit chrono")
                    .checked_add(1)
                    .expect("test lease duration increment must not overflow"),
            ))
            .expect("post-crash parent lease expiry time must be representable"),
    );
    let restarted = start_branch_recovery_server(&harness, paused_store, &fixture).await;
    assert_eq!(
        resume_branch_delete(&restarted, &target, &fixture)
            .await
            .expect("retry after the crashed CAS must complete"),
        NamespaceDeleteOutcome::Deleted
    );
    restarted.shutdown().await;
    assert!(
        branch_control_snapshot(&harness.store, &source)
            .await
            .expect("parent root map must remain readable after retry")
            .roots
            .is_empty(),
        "retry must remove the exact retained parent root"
    );
    assert!(matches!(
        harness.store.get(&NamespaceMetadata::s3_key(&target)).await,
        Err(zeppelin::error::ZeppelinError::NotFound { .. })
    ));

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn slice_five_lost_parent_root_cas_reply_converges_before_final_cleanup() {
    let harness = common::harness::TestHarness::new().await;
    let source = harness.artifact_origin_namespace("lost-root-reply-source");
    let target = harness.artifact_origin_namespace("lost-root-reply-target");
    let fixture = establish_branch_grace(&harness, &source, &target).await;
    fixture.wall_clock.set(fixture.visibility.not_before);

    let (lost_reply_store, lost_reply) =
        fail_after_put_once_matching(&harness.store, format!("{source}/manifest.json"));
    let interrupted_server =
        start_branch_recovery_server(&harness, lost_reply_store, &fixture).await;
    resume_branch_delete(&interrupted_server, &target, &fixture)
        .await
        .expect_err("lost parent-root CAS reply must interrupt before target acknowledgement");
    interrupted_server.shutdown().await;
    assert_eq!(lost_reply.failures_injected(), 1);
    assert!(
        branch_control_snapshot(&harness.store, &source)
            .await
            .expect("parent root absence must be authoritative after the lost reply")
            .roots
            .is_empty(),
        "the failed reply must hide a committed parent-root removal"
    );
    let interrupted = read_namespace_metadata(&harness.store, &target).await;
    assert_eq!(
        interrupted
            .deletion_intent
            .as_ref()
            .and_then(|intent| intent.root_release.as_ref()),
        None,
        "a lost root-CAS reply must not manufacture a target acknowledgement"
    );

    let (convergence_store, marker_delete_failure) =
        fail_delete_once_matching(&harness.store, fixture.visibility.marker_key.clone());
    let convergence_server =
        start_branch_recovery_server(&harness, convergence_store, &fixture).await;
    resume_branch_delete(&convergence_server, &target, &fixture)
        .await
        .expect_err("cleanup fault must stop after convergence acknowledgement");
    convergence_server.shutdown().await;
    assert_eq!(marker_delete_failure.failures_injected(), 1);
    let converged = read_namespace_metadata(&harness.store, &target).await;
    assert_final_root_release(&converged, true);
    assert!(
        harness
            .store
            .get(&fixture.visibility.marker_key)
            .await
            .is_ok(),
        "pre-delete cleanup fault must leave the durable visibility marker intact"
    );

    let final_server =
        start_branch_recovery_server(&harness, harness.store.clone(), &fixture).await;
    assert_eq!(
        resume_branch_delete(&final_server, &target, &fixture)
            .await
            .expect("final convergence retry must complete target cleanup"),
        NamespaceDeleteOutcome::Deleted
    );
    final_server.shutdown().await;
    assert!(matches!(
        harness.store.get(&NamespaceMetadata::s3_key(&target)).await,
        Err(zeppelin::error::ZeppelinError::NotFound { .. })
    ));

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn slice_five_retry_after_marker_cleanup_does_not_recreate_visibility() {
    let harness = common::harness::TestHarness::new().await;
    let source = harness.artifact_origin_namespace("marker-cleanup-source");
    let target = harness.artifact_origin_namespace("marker-cleanup-target");
    let fixture = establish_branch_grace(&harness, &source, &target).await;
    fixture.wall_clock.set(fixture.visibility.not_before);

    let meta_key = NamespaceMetadata::s3_key(&target);
    let (metadata_failure_store, metadata_delete_failure) =
        fail_delete_once_matching(&harness.store, meta_key.clone());
    let interrupted_server =
        start_branch_recovery_server(&harness, metadata_failure_store, &fixture).await;
    resume_branch_delete(&interrupted_server, &target, &fixture)
        .await
        .expect_err("metadata-last fault must interrupt after target-owned cleanup");
    interrupted_server.shutdown().await;
    assert_eq!(metadata_delete_failure.failures_injected(), 1);
    let interrupted = read_namespace_metadata(&harness.store, &target).await;
    assert_final_root_release(&interrupted, false);
    assert!(
        branch_control_snapshot(&harness.store, &source)
            .await
            .expect("parent root map must remain readable after release")
            .roots
            .is_empty(),
        "durable release acknowledgement must follow parent-root removal"
    );
    assert!(matches!(
        harness.store.get(&fixture.visibility.marker_key).await,
        Err(zeppelin::error::ZeppelinError::NotFound { .. })
    ));
    assert!(
        harness.store.get(&meta_key).await.is_ok(),
        "metadata-last failure must preserve the deletion tombstone"
    );

    let final_server =
        start_branch_recovery_server(&harness, harness.store.clone(), &fixture).await;
    assert_eq!(
        resume_branch_delete(&final_server, &target, &fixture)
            .await
            .expect("retry with a final release acknowledgement must complete"),
        NamespaceDeleteOutcome::Deleted
    );
    final_server.shutdown().await;
    assert!(matches!(
        harness.store.get(&fixture.visibility.marker_key).await,
        Err(zeppelin::error::ZeppelinError::NotFound { .. })
    ));
    assert!(matches!(
        harness.store.get(&meta_key).await,
        Err(zeppelin::error::ZeppelinError::NotFound { .. })
    ));

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn slice_five_root_absence_without_destruction_evidence_cannot_converge() {
    let harness = common::harness::TestHarness::new().await;
    let source = harness.artifact_origin_namespace("missing-evidence-source");
    let target = harness.artifact_origin_namespace("missing-evidence-target");
    let fixture = establish_branch_grace(&harness, &source, &target).await;
    fixture.wall_clock.set(fixture.visibility.not_before);
    let server = start_branch_recovery_server(&harness, harness.store.clone(), &fixture).await;

    let root = branch_control_snapshot(&harness.store, &source)
        .await
        .expect("parent root must remain readable before corruption injection")
        .roots
        .into_iter()
        .next()
        .expect("branch fixture must retain one parent root");
    remove_prepared_branch_root(server.store.clone(), &source, root)
        .await
        .expect("test must simulate an already-absent exact parent root");
    let deleting = read_namespace_metadata(&harness.store, &target).await;
    let evidence_key = deleting
        .deletion_intent
        .as_ref()
        .map(|intent| intent.destruction_record_key.clone())
        .expect("deleting branch must retain its destruction-evidence binding");
    server
        .store
        .delete(&evidence_key)
        .await
        .expect("test must remove the bound destruction evidence");

    let error = resume_branch_delete(&server, &target, &fixture)
        .await
        .expect_err("missing destruction evidence must fail closed");
    assert!(
        matches!(error, zeppelin::error::ZeppelinError::NotFound { .. }),
        "missing evidence must surface as an authoritative not-found error: {error}"
    );
    let unchanged = read_namespace_metadata(&harness.store, &target).await;
    assert_eq!(
        unchanged
            .deletion_intent
            .as_ref()
            .and_then(|intent| intent.root_release.as_ref()),
        None,
        "root absence alone must never authorize convergence"
    );
    assert!(
        server
            .store
            .get(&fixture.visibility.marker_key)
            .await
            .is_ok(),
        "failed convergence must retain its visibility marker"
    );
    server.shutdown().await;

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn slice_five_parent_incarnation_replacement_is_rejected_before_root_release() {
    let harness = common::harness::TestHarness::new().await;
    let source = harness.artifact_origin_namespace("replacement-parent-source");
    let target = harness.artifact_origin_namespace("replacement-parent-target");
    let fixture = establish_branch_grace(&harness, &source, &target).await;
    fixture.wall_clock.set(fixture.visibility.not_before);
    let server = start_branch_recovery_server(&harness, harness.store.clone(), &fixture).await;

    let original = read_namespace_metadata(&server.store, &source).await;
    server
        .store
        .delete_prefix(&format!("{source}/"))
        .await
        .expect("test must remove the original parent lifetime");
    let replacement = server
        .namespace_manager
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .expect("same parent name must be recreated with a fresh lifetime");
    assert_ne!(replacement.incarnation_id, original.incarnation_id);

    let resumed = resume_branch_delete(&server, &target, &fixture).await;
    assert!(matches!(
        resumed,
        Err(zeppelin::error::ZeppelinError::Branch(error))
            if matches!(error.as_ref(), BranchError::SourceIncarnationChanged { .. })
    ));
    let unchanged = read_namespace_metadata(&harness.store, &target).await;
    assert_eq!(
        unchanged
            .deletion_intent
            .as_ref()
            .and_then(|intent| intent.root_release.as_ref()),
        None,
        "a replacement parent lifetime must not receive an old release acknowledgement"
    );
    assert!(
        branch_control_snapshot(&harness.store, &source)
            .await
            .expect("replacement parent manifest must remain readable")
            .roots
            .is_empty(),
        "root release must not mutate the replacement parent"
    );
    assert!(
        server
            .store
            .get(&fixture.visibility.marker_key)
            .await
            .is_ok(),
        "incarnation failure must retain target recovery state"
    );
    server.shutdown().await;

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn slice_five_lost_root_reply_can_converge_after_parent_is_fully_deleted() {
    let harness = common::harness::TestHarness::new().await;
    let source = harness.artifact_origin_namespace("deleted-parent-source");
    let target = harness.artifact_origin_namespace("deleted-parent-target");
    let fixture = establish_branch_grace(&harness, &source, &target).await;
    fixture.wall_clock.set(fixture.visibility.not_before);

    let (lost_reply_store, lost_reply) =
        fail_after_put_once_matching(&harness.store, format!("{source}/manifest.json"));
    let interrupted_server =
        start_branch_recovery_server(&harness, lost_reply_store, &fixture).await;
    resume_branch_delete(&interrupted_server, &target, &fixture)
        .await
        .expect_err("lost parent-root reply must interrupt before acknowledgement");
    interrupted_server.shutdown().await;
    assert_eq!(lost_reply.failures_injected(), 1);
    assert!(branch_control_snapshot(&harness.store, &source)
        .await
        .expect("parent manifest must remain readable immediately after root release")
        .roots
        .is_empty());
    let interrupted = read_namespace_metadata(&harness.store, &target).await;
    assert_eq!(
        interrupted
            .deletion_intent
            .as_ref()
            .and_then(|intent| intent.root_release.as_ref()),
        None
    );

    harness
        .store
        .delete_prefix(&format!("{source}/"))
        .await
        .expect("test must simulate the parent completing deletion after root release");
    let (convergence_store, marker_delete_failure) =
        fail_delete_once_matching(&harness.store, fixture.visibility.marker_key.clone());
    let convergence_server =
        start_branch_recovery_server(&harness, convergence_store, &fixture).await;
    resume_branch_delete(&convergence_server, &target, &fixture)
        .await
        .expect_err("cleanup fault must stop after absent-parent convergence acknowledgement");
    convergence_server.shutdown().await;
    assert_eq!(marker_delete_failure.failures_injected(), 1);
    let converged = read_namespace_metadata(&harness.store, &target).await;
    assert_final_root_release(&converged, true);

    let final_server =
        start_branch_recovery_server(&harness, harness.store.clone(), &fixture).await;
    assert_eq!(
        resume_branch_delete(&final_server, &target, &fixture)
            .await
            .expect("final retry must complete even though the parent is absent"),
        NamespaceDeleteOutcome::Deleted
    );
    final_server.shutdown().await;
    assert!(matches!(
        harness.store.get(&NamespaceMetadata::s3_key(&target)).await,
        Err(zeppelin::error::ZeppelinError::NotFound { .. })
    ));

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}
