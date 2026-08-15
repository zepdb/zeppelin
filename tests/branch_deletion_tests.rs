#![cfg(feature = "branching-test-support")]

mod common;

use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use common::counting::counting_store;
use common::fault_injection::{
    fail_after_delete_once_matching, fail_after_put_once_matching, fail_delete_once_matching,
    fail_put_once_matching, pause_next_cas_matching, pause_next_get_matching,
    record_delete_operations,
};
use common::harness::TestHarness;
use common::server::{
    client_with_bearer, scoped_test_security_store, start_test_server_full,
    start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer,
    start_test_server_on_store_with_config, start_test_server_with_config, FullTestServer,
};
use serde_json::{json, Value};
use zeppelin::cache::hydration::{
    HydrationConfig, HydrationTarget, SegmentHydrator, SessionWindowPolicy,
};
use zeppelin::cache::DiskCache;
use zeppelin::compaction::Compactor;
use zeppelin::config::{BranchingConfig, CompactionConfig, Config, IndexingConfig};
use zeppelin::index::quantization::QuantizationType;
use zeppelin::namespace::branching::test_support::{
    activate_fork_for_test, branch_control_snapshot, delete_namespace_for_test,
    insert_prepared_branch_root, maintain_branches_with_config_and_clock_for_test,
    prepare_head_branch_root, remove_prepared_branch_root,
    resume_delete_with_config_and_clock_for_test,
};
use zeppelin::namespace::branching::{
    BranchError, BranchId, BranchMaintenanceReport, ForkViewDigest, NamespaceDeleteOutcome,
};
use zeppelin::namespace::manager::{
    NamespaceMetadata, NamespaceState, RootReleaseState, VisibilityRemoval,
};
use zeppelin::namespace::{NamespaceId, NamespaceManager};
use zeppelin::storage::ZeppelinStore;
use zeppelin::time::{Clock, TimeSource};
use zeppelin::types::{DistanceMetric, VectorEntry};
use zeppelin::wal::fragment::WalFragment;
use zeppelin::wal::{Manifest, WalReader, WalWriter};

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
    establish_branch_grace_on_store(harness, harness.store.clone(), source, target).await
}

async fn establish_branch_grace_on_store(
    harness: &common::harness::TestHarness,
    store: ZeppelinStore,
    source: &str,
    target: &str,
) -> EstablishedBranchGrace {
    let config = branch_grace_config();
    let wall_clock = Arc::new(AdjustableWallClock::new(Utc::now()));
    let clock = Clock::from_source(wall_clock.clone());
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
    create_source_with_one_row(&client, &server.base_url, source).await;
    activate_branch(server.store.clone(), source, target, &config).await;

    let deletion = client
        .delete(format!("{}/v1/namespaces/{target}", server.base_url))
        .send()
        .await
        .expect("initial branch deletion must complete");
    assert_eq!(deletion.status(), reqwest::StatusCode::ACCEPTED);
    let deleting = read_namespace_metadata(&store, target).await;
    let visibility = deleting
        .deletion_intent
        .as_ref()
        .and_then(|intent| intent.visibility.clone())
        .expect("initial branch deletion must persist its grace boundary");
    assert_eq!(
        branch_control_snapshot(&store, source)
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

async fn root_release_audit_progress(
    store: &ZeppelinStore,
    namespace: &str,
    decision_evidence_ref: &str,
) -> Vec<Value> {
    let mut progress = Vec::new();
    for key in store
        .list_prefix("_audit/deletion-lifecycle/")
        .await
        .expect("deletion lifecycle audit prefix must be listable")
    {
        let record: Value = serde_json::from_slice(
            &store
                .get(&key)
                .await
                .expect("lifecycle audit record must be readable"),
        )
        .expect("lifecycle audit record must decode");
        let Some(params) = record["params"]["namespace_delete_root_release"].as_object() else {
            continue;
        };
        if params.get("namespace") != Some(&json!(namespace)) {
            continue;
        }
        assert_eq!(
            params.get("decision_evidence_ref"),
            Some(&json!(decision_evidence_ref))
        );
        progress.push(
            params
                .get("progress")
                .cloned()
                .expect("root-release audit must carry typed progress"),
        );
    }
    progress
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
async fn queued_branch_hydration_stops_at_deletion_fence_and_delete_resumes() {
    TestHarness::require_cas_backend();
    zeppelin::metrics::init();

    let harness = common::harness::TestHarness::new().await;
    let source = harness.artifact_origin_namespace("hydration-delete-source");
    let target = harness.artifact_origin_namespace("hydration-delete-target");
    let indexing = IndexingConfig {
        default_num_centroids: 1,
        kmeans_max_iterations: 5,
        quantization: QuantizationType::None,
        bitmap_index: false,
        ..IndexingConfig::default()
    };
    let branching = BranchingConfig {
        enabled: true,
        ..BranchingConfig::default()
    };
    NamespaceManager::new(harness.store.clone())
        .create(&source, 4, DistanceMetric::Euclidean)
        .await
        .expect("hydration source namespace must be created");
    WalWriter::new(harness.store.clone())
        .append(
            &source,
            (0..32)
                .map(|index| VectorEntry {
                    id: format!("source-{index:02}"),
                    values: vec![index as f32, 0.0, 0.0, 0.0],
                    attributes: None,
                })
                .collect(),
            vec![],
        )
        .await
        .expect("hydration source WAL must be published");
    Compactor::new(
        harness.store.clone(),
        WalReader::new(harness.store.clone()),
        CompactionConfig {
            max_wal_fragments_before_compact: 1,
            ..CompactionConfig::default()
        },
        indexing.clone(),
        common::default_gc_upload_window(),
    )
    .compact(&source)
    .await
    .expect("hydration source must compact into immutable segment artifacts");
    activate_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).expect("source namespace must be valid"),
        NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        indexing.clone(),
        branching.clone(),
    )
    .await
    .expect("foreign-backed target branch must activate");
    let target_manifest = Manifest::read(&harness.store, &target)
        .await
        .expect("target manifest read must succeed")
        .expect("active target manifest must exist");
    let hydration_target = HydrationTarget::from_active_manifest(&target_manifest)
        .expect("target manifest origin table must be valid")
        .expect("compacted target must expose one hydration target");
    assert_eq!(hydration_target.logical_namespace(), target);
    assert_eq!(hydration_target.physical_namespace(), source);
    let first_cluster = hydration_target
        .segment()
        .cluster_objects
        .first()
        .expect("full compaction must publish a grouped cluster object")
        .key
        .clone();
    let physical_source_prefix = format!("{source}/segments/");
    assert!(
        first_cluster.starts_with(&physical_source_prefix),
        "the operation-recorder filter must cover the selected physical source artifact"
    );

    let (counted_store, operations) = counting_store(&harness.store);
    let (paused_store, authority_read) =
        pause_next_get_matching(&counted_store, Manifest::s3_key(&target));
    let (crash_store, evidence_lost_reply) =
        fail_after_put_once_matching(&paused_store, "_audit/destruction/");
    authority_read.arm();
    let cache_dir = tempfile::tempdir().expect("hydration cache directory must be created");
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().join("hydration"), 64 * 1024 * 1024)
            .expect("hydration cache must be created"),
    );
    let hydrator = SegmentHydrator::start(
        crash_store.clone(),
        Arc::clone(&cache),
        Arc::new(
            SessionWindowPolicy::new(1, Duration::from_secs(60))
                .expect("test heat policy must be valid"),
        ),
        HydrationConfig {
            parallelism: 2,
            max_segment_fraction: 1.0,
            max_retries: 0,
            retry_backoff: Duration::from_millis(1),
            job_timeout: Duration::from_secs(30),
        },
    );
    let hydration = {
        let hydrator = Arc::clone(&hydrator);
        let target = hydration_target.clone();
        tokio::spawn(async move { hydrator.request_hydration_and_wait_for_test(&target).await })
    };

    tokio::time::timeout(Duration::from_secs(15), authority_read.wait_until_paused())
        .await
        .expect("queued hydration must reach its authoritative manifest barrier");
    delete_namespace_for_test(
        crash_store.clone(),
        NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        indexing.clone(),
        branching.clone(),
    )
    .await
    .expect_err("lost destruction-evidence reply must interrupt deletion after fencing");
    assert_eq!(evidence_lost_reply.failures_injected(), 1);
    assert!(
        branch_control_snapshot(&harness.store, &target)
            .await
            .expect("fenced target manifest must remain authoritative")
            .deletion_fenced,
        "the deletion crash must leave the exact target manifest fenced"
    );

    authority_read.release();
    tokio::time::timeout(Duration::from_secs(15), hydration)
        .await
        .expect("fenced hydration must terminate")
        .expect("hydration task must join")
        .expect("deterministic hydration enqueue must remain connected");
    assert_eq!(
        operations.gets_matching(&physical_source_prefix),
        0,
        "a queued target hydration must reject the fence before any physical source GET"
    );
    assert_eq!(
        operations.heads_matching(&physical_source_prefix),
        0,
        "a queued target hydration must reject the fence before physical planning"
    );
    assert_eq!(
        cache.get(&hydration_target.cache_key(&first_cluster)).await,
        None,
        "a fenced hydration must not populate the physical source artifact cache"
    );

    let resumed = delete_namespace_for_test(
        crash_store,
        NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        indexing,
        branching,
    )
    .await
    .expect("retry must resume the durable fenced deletion intent");
    assert!(matches!(
        resumed,
        NamespaceDeleteOutcome::BranchGraceWait { .. }
    ));
    let deleting = read_namespace_metadata(&harness.store, &target).await;
    assert_eq!(deleting.state, NamespaceState::Deleting);
    assert!(
        deleting
            .deletion_intent
            .as_ref()
            .and_then(|intent| intent.visibility.as_ref())
            .is_some(),
        "resumed deletion must durably remove target visibility and retain its grace boundary"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn branch_cleanup_rejects_foreign_pending_delete_without_source_delete() {
    let harness = common::harness::TestHarness::new().await;
    let (recording_store, deletes) = record_delete_operations(&harness.store);
    let source = harness.artifact_origin_namespace("owned-cleanup-source");
    let target = harness.artifact_origin_namespace("owned-cleanup-target");
    let mut config = Config::default();
    config.branching.enabled = true;

    NamespaceManager::new(recording_store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .expect("source namespace must be created");
    activate_branch(recording_store.clone(), &source, &target, &config).await;

    let foreign_key = WalFragment::s3_key(&source, &ulid::Ulid::new());
    recording_store
        .put(
            &foreign_key,
            bytes::Bytes::from_static(b"source-owned artifact"),
        )
        .await
        .expect("source-owned artifact fixture must be persisted");
    let (mut target_manifest, target_version) = Manifest::read_versioned(&recording_store, &target)
        .await
        .expect("target manifest read must succeed")
        .expect("active target manifest must exist");
    target_manifest.pending_deletes.push(foreign_key.clone());
    target_manifest
        .write_conditional(&recording_store, &target, &target_version)
        .await
        .expect("corrupt foreign pending-delete fixture must be installed");

    deletes.reset().await;
    let initial = delete_namespace_for_test(
        recording_store.clone(),
        NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        config.indexing.clone(),
        config.branching.clone(),
    )
    .await;
    let cleanup_error = match initial {
        Err(error) => error,
        Ok(_) => {
            let deleting = read_namespace_metadata(&recording_store, &target).await;
            let not_before = deleting
                .deletion_intent
                .as_ref()
                .and_then(|intent| intent.visibility.as_ref())
                .map(|visibility| visibility.not_before)
                .expect("accepted branch deletion must persist a grace deadline");
            let clock = Clock::from_source(Arc::new(AdjustableWallClock::new(not_before)));
            resume_delete_with_config_and_clock_for_test(
                recording_store.clone(),
                NamespaceId::new(target.clone()).expect("target namespace must be valid"),
                &branch_grace_config(),
                clock,
                Duration::from_secs(1),
            )
            .await
            .expect_err("foreign pending-delete corruption must halt owned cleanup")
        }
    };

    let message = cleanup_error.to_string();
    assert!(
        message.contains("pending") && (message.contains("foreign") || message.contains("local")),
        "cleanup must identify the foreign pending-delete corruption: {message}"
    );
    assert!(
        recording_store
            .get(&NamespaceMetadata::s3_key(&target))
            .await
            .is_ok(),
        "cleanup corruption must retain target metadata as its recovery handle"
    );
    assert!(
        recording_store.get(&foreign_key).await.is_ok(),
        "target cleanup must not delete the source-owned artifact"
    );
    let source_prefix = format!("{source}/");
    let observed = deletes.deleted_keys().await;
    assert!(
        observed.iter().all(|key| !key.starts_with(&source_prefix)),
        "target cleanup issued source-prefix DELETEs: {observed:?}"
    );

    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup_artifact_origin_namespace(&source).await;
}

#[tokio::test]
async fn successful_branch_cleanup_deletes_only_target_inventory() {
    let harness = common::harness::TestHarness::new().await;
    let (recording_store, deletes) = record_delete_operations(&harness.store);
    let source = harness.artifact_origin_namespace("owned-cleanup-success-source");
    let target = harness.artifact_origin_namespace("owned-cleanup-success-target");
    let fixture =
        establish_branch_grace_on_store(&harness, recording_store.clone(), &source, &target).await;
    let source_wal_keys = recording_store
        .list_prefix(&format!("{source}/wal/"))
        .await
        .expect("source WAL inventory must remain readable");
    assert!(
        !source_wal_keys.is_empty(),
        "the source-survival fixture requires one physical source artifact"
    );
    let expected_target_deletes = recording_store
        .list_prefix(&format!("{target}/"))
        .await
        .expect("target cleanup inventory must remain readable")
        .into_iter()
        .collect::<BTreeSet<_>>();
    assert!(
        expected_target_deletes.contains(&NamespaceMetadata::s3_key(&target)),
        "the cleanup inventory must include metadata for the final DELETE"
    );

    fixture.wall_clock.set(fixture.visibility.not_before);
    let source_server =
        start_branch_recovery_server(&harness, recording_store.clone(), &fixture).await;
    deletes.reset().await;
    let outcome = resume_delete_with_config_and_clock_for_test(
        source_server.store.clone(),
        NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        &fixture.config,
        fixture.clock.clone(),
        Duration::from_secs(5),
    )
    .await
    .expect("owned target cleanup must complete");
    assert_eq!(outcome, NamespaceDeleteOutcome::Deleted);

    let observed = deletes.deleted_keys().await;
    let target_prefix = format!("{target}/");
    let observed_target_deletes = observed
        .iter()
        .filter(|key| key.starts_with(&target_prefix))
        .cloned()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        observed_target_deletes, expected_target_deletes,
        "the recorder must observe every object from the authorized target inventory"
    );
    let source_prefix = format!("{source}/");
    assert!(
        observed.iter().all(|key| !key.starts_with(&source_prefix)),
        "target cleanup issued source-prefix DELETEs: {observed:?}"
    );
    for source_key in &source_wal_keys {
        assert!(
            recording_store.get(source_key).await.is_ok(),
            "source-owned artifact disappeared during target cleanup: {source_key}"
        );
    }

    let source_query = client_with_bearer(&fixture.admin_bearer)
        .post(format!(
            "{}/v1/namespaces/{source}/query",
            source_server.base_url
        ))
        .json(&json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("source query after child cleanup must complete");
    assert_eq!(source_query.status(), reqwest::StatusCode::OK);
    let source_body: Value = source_query
        .json()
        .await
        .expect("source query response must decode");
    assert_eq!(source_body["results"][0]["id"], "retained-row");
    source_server.shutdown().await;

    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup_artifact_origin_namespace(&source).await;
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
async fn source_delete_conflict_discloses_only_readable_children_without_a_count_oracle() {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = format!("disclosure-source-{suffix}");
    let visible = format!("disclosure-visible-{suffix}");
    let hidden = format!("disclosure-hidden-{suffix}");
    let mut config = Config::from_str(&format!(
        r#"
[branching]
enabled = true

[security]
mode = "enforced"
policy_refresh_secs = 3600
cursor_hmac_key_hex = "1111111111111111111111111111111111111111111111111111111111111111"

[[security.api_keys]]
key_id = "zpk1_branch_disclosure"
name = "branch-disclosure"
sha256_hex = "0f007385b6f9d4b7eeb2748605afe1a984a0a3bfa3f014d09e2a784ce9e5cd1a"
actions = ["NamespaceRead", "NamespaceDelete"]
namespaces = ["{source}", "{visible}"]
"#,
    ))
    .expect("disclosure test config must decode");
    config.security.audit_flush_secs = 1;
    let (base_url, harness, _cache, _cache_dir, admin_bearer) =
        start_test_server_with_config(Some(config)).await;
    let admin = client_with_bearer(&admin_bearer);

    create_source_with_one_row(&admin, &base_url, &source).await;
    let mut visible_branch_id = None;
    for target in [&visible, &hidden] {
        let fork = admin
            .post(format!("{base_url}/v1/namespaces/{source}/branches"))
            .json(&json!({ "target": target }))
            .send()
            .await
            .expect("fork request must complete");
        assert_eq!(fork.status(), reqwest::StatusCode::CREATED);
        let body: Value = fork.json().await.expect("fork response must decode");
        if target == &visible {
            visible_branch_id = body["branch_id"].as_str().map(ToOwned::to_owned);
        }
    }
    let visible_branch_id = visible_branch_id.expect("visible fork must return its branch ID");
    let scoped =
        client_with_bearer("zpk1_branch_disclosure.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    let visible_probe = scoped
        .get(format!("{base_url}/v1/namespaces/{visible}"))
        .send()
        .await
        .expect("visible-child authorization probe must complete");
    let visible_probe_status = visible_probe.status();
    let visible_probe_body = visible_probe
        .text()
        .await
        .expect("visible-child authorization response must be readable");
    assert!(
        !matches!(
            visible_probe_status,
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
        ),
        "fixture principal must pass visible-child authorization: status={visible_probe_status}, body={visible_probe_body}"
    );
    assert_eq!(
        scoped
            .get(format!("{base_url}/v1/namespaces/{hidden}"))
            .send()
            .await
            .expect("hidden-child authorization probe must complete")
            .status(),
        reqwest::StatusCode::FORBIDDEN,
        "fixture principal must be denied read access to the hidden child"
    );

    let listing_before_corruption = scoped
        .get(format!("{base_url}/v1/namespaces/{source}/branches"))
        .send()
        .await
        .expect("branch listing precondition must complete");
    assert_eq!(
        listing_before_corruption.status(),
        reqwest::StatusCode::OK,
        "valid denied-child metadata must not affect the filtered listing"
    );
    let listing_before_corruption: Value = listing_before_corruption
        .json()
        .await
        .expect("branch listing precondition must decode");
    assert_eq!(
        listing_before_corruption["branches"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );
    assert_eq!(
        listing_before_corruption["branches"][0]["target"]["namespace"],
        visible
    );
    assert!(!listing_before_corruption.to_string().contains(&hidden));

    harness
        .store
        .delete(&NamespaceMetadata::s3_key(&hidden))
        .await
        .expect("denied child corruption fixture must be installed");

    let listing = scoped
        .get(format!("{base_url}/v1/namespaces/{source}/branches"))
        .send()
        .await
        .expect("branch listing must complete");
    let listing_status = listing.status();
    let listing: Value = listing.json().await.expect("branch listing must decode");
    assert_eq!(
        listing_status,
        reqwest::StatusCode::OK,
        "unexpected branch-list response: {listing}"
    );
    assert_eq!(listing["branches"].as_array().map(Vec::len), Some(1));
    assert_eq!(listing["branches"][0]["target"]["namespace"], visible);
    assert!(
        !listing.to_string().contains(&hidden),
        "denied child identity must not appear anywhere in the listing"
    );

    let deletion = scoped
        .delete(format!("{base_url}/v1/namespaces/{source}"))
        .send()
        .await
        .expect("source delete conflict must complete");
    assert_eq!(deletion.status(), reqwest::StatusCode::CONFLICT);
    let body: Value = deletion
        .json()
        .await
        .expect("source delete conflict must decode");
    assert_eq!(body["code"], "namespace_has_live_branches");
    assert_eq!(
        body["visible_children"],
        json!([{ "namespace": visible, "branch_id": visible_branch_id }])
    );
    assert_eq!(body["has_additional_children"], true);
    assert!(body.get("child_count").is_none());
    assert!(body.get("total").is_none());
    assert!(
        !body.to_string().contains(&hidden),
        "denied child identity must not appear anywhere in the conflict"
    );

    harness.cleanup_artifact_origin_namespace(&hidden).await;
    harness.cleanup_artifact_origin_namespace(&visible).await;
    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn readable_child_corruption_fails_branch_listing_with_a_generic_integrity_error() {
    let mut config = Config::default();
    config.branching.enabled = true;
    config.security.policy_refresh_secs = 3_600;
    let (base_url, harness, _cache, _cache_dir, admin_bearer) =
        start_test_server_with_config(Some(config)).await;
    let client = client_with_bearer(&admin_bearer);
    let source = harness.artifact_origin_namespace("list-corrupt-source");
    let target = harness.artifact_origin_namespace("list-corrupt-target");

    create_source_with_one_row(&client, &base_url, &source).await;
    let fork = client
        .post(format!("{base_url}/v1/namespaces/{source}/branches"))
        .json(&json!({ "target": target }))
        .send()
        .await
        .expect("fork request must complete");
    assert_eq!(fork.status(), reqwest::StatusCode::CREATED);

    harness
        .store
        .delete(&NamespaceMetadata::s3_key(&target))
        .await
        .expect("target metadata corruption fixture must be installed");
    let listing = client
        .get(format!("{base_url}/v1/namespaces/{source}/branches"))
        .send()
        .await
        .expect("corrupt branch listing must complete");
    assert_eq!(listing.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);
    let body: Value = listing.json().await.expect("error envelope must decode");
    assert_eq!(body["code"], "branch_integrity_error");
    assert!(
        !body.to_string().contains(&target),
        "generic integrity failure must not disclose the corrupt child identity"
    );

    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup_artifact_origin_namespace(&source).await;
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
async fn failed_and_successful_root_release_attempts_write_typed_lifecycle_audit() {
    let harness = common::harness::TestHarness::new().await;
    let source = harness.artifact_origin_namespace("root-release-audit-source");
    let target = harness.artifact_origin_namespace("root-release-audit-target");
    let fixture = establish_branch_grace(&harness, &source, &target).await;
    fixture.wall_clock.set(fixture.visibility.not_before);
    let deleting = read_namespace_metadata(&harness.store, &target).await;
    let decision_evidence_ref = deleting
        .deletion_intent
        .as_ref()
        .map(|intent| intent.decision_evidence_ref.clone())
        .expect("branch grace fixture must retain decision evidence linkage");

    let (faulted_store, failure) =
        fail_put_once_matching(&harness.store, format!("{source}/manifest.json"));
    let faulted = start_branch_recovery_server(&harness, faulted_store, &fixture).await;
    let response = client_with_bearer(&fixture.admin_bearer)
        .delete(format!("{}/v1/namespaces/{target}", faulted.base_url))
        .send()
        .await
        .expect("faulted branch deletion retry must complete");
    assert_eq!(
        response.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(failure.failures_injected(), 1);
    faulted.shutdown().await;

    let resumed = start_branch_recovery_server(&harness, harness.store.clone(), &fixture).await;
    let response = client_with_bearer(&fixture.admin_bearer)
        .delete(format!("{}/v1/namespaces/{target}", resumed.base_url))
        .send()
        .await
        .expect("successful branch deletion retry must complete");
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    resumed.shutdown().await;

    let progress =
        root_release_audit_progress(&harness.store, &target, &decision_evidence_ref).await;
    assert!(
        progress.contains(&json!({
            "grace_pending": {"not_before": fixture.visibility.not_before}
        })),
        "initial visibility removal must durably record the persisted grace deadline: {progress:?}"
    );
    assert!(
        progress.contains(&json!({"failed": {"class": "storage_unavailable"}})),
        "root-removal storage failure must be durably classified: {progress:?}"
    );
    assert!(
        progress.contains(&json!("released")),
        "successful retry must durably record exact-root release: {progress:?}"
    );

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn root_release_audit_outage_fails_closed_then_retry_records_convergence() {
    let harness = common::harness::TestHarness::new().await;
    let source = harness.artifact_origin_namespace("root-release-audit-outage-source");
    let target = harness.artifact_origin_namespace("root-release-audit-outage-target");
    let fixture = establish_branch_grace(&harness, &source, &target).await;
    fixture.wall_clock.set(fixture.visibility.not_before);
    let deleting = read_namespace_metadata(&harness.store, &target).await;
    let decision_evidence_ref = deleting
        .deletion_intent
        .as_ref()
        .map(|intent| intent.decision_evidence_ref.clone())
        .expect("branch grace fixture must retain decision evidence linkage");

    let (audit_failure_store, audit_failure) =
        fail_put_once_matching(&harness.store, "_audit/deletion-lifecycle/");
    let faulted = start_branch_recovery_server(&harness, audit_failure_store, &fixture).await;
    let response = client_with_bearer(&fixture.admin_bearer)
        .delete(format!("{}/v1/namespaces/{target}", faulted.base_url))
        .send()
        .await
        .expect("audit-faulted branch deletion retry must return a response");
    assert_eq!(
        response.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        "root removal must never return success when its required lifecycle audit is unavailable"
    );
    assert_eq!(audit_failure.failures_injected(), 1);
    faulted.shutdown().await;

    assert!(
        branch_control_snapshot(&harness.store, &source)
            .await
            .expect("parent root map must remain authoritative after audit failure")
            .roots
            .is_empty(),
        "the audit outage is injected only after the exact parent root is removed"
    );
    let interrupted = read_namespace_metadata(&harness.store, &target).await;
    assert_eq!(
        interrupted
            .deletion_intent
            .as_ref()
            .and_then(|intent| intent.root_release.as_ref()),
        None,
        "an unaudited root removal must not receive a durable release acknowledgement"
    );

    let resumed = start_branch_recovery_server(&harness, harness.store.clone(), &fixture).await;
    let response = client_with_bearer(&fixture.admin_bearer)
        .delete(format!("{}/v1/namespaces/{target}", resumed.base_url))
        .send()
        .await
        .expect("retry after lifecycle-audit recovery must return a response");
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    resumed.shutdown().await;

    let progress =
        root_release_audit_progress(&harness.store, &target, &decision_evidence_ref).await;
    assert!(
        progress.contains(&json!({
            "grace_pending": {"not_before": fixture.visibility.not_before}
        })),
        "the durable grace observation must survive the later audit outage: {progress:?}"
    );
    assert!(
        progress.contains(&json!("converged")),
        "the retry must durably classify the already-absent exact root as converged: {progress:?}"
    );
    assert!(
        !progress.contains(&json!("released")),
        "the failed audit attempt must not manufacture a durable released event: {progress:?}"
    );

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
async fn concurrent_stale_resume_after_marker_cleanup_converges_without_recreation() {
    let harness = common::harness::TestHarness::new().await;
    let source = harness.artifact_origin_namespace("concurrent-marker-cleanup-source");
    let target = harness.artifact_origin_namespace("concurrent-marker-cleanup-target");
    let fixture = establish_branch_grace(&harness, &source, &target).await;
    fixture.wall_clock.set(fixture.visibility.not_before);

    let (paused_store, marker_get) =
        pause_next_get_matching(&harness.store, fixture.visibility.marker_key.clone());
    // Both concurrent graph instances need their own disposable gateway view.
    // The fixture server's signer root ended at shutdown, so reusing the
    // application gateway for a manifest CAS would correctly fail its signer
    // lifecycle check instead of exercising the deletion race.
    let winning_store = ZeppelinStore::new(harness.store.inner());
    marker_get.arm();
    let stale_target = NamespaceId::new(target.clone()).expect("target namespace must be valid");
    let stale_config = fixture.config.clone();
    let stale_clock = fixture.clock.clone();
    let mut stale_resume = tokio::spawn(async move {
        resume_delete_with_config_and_clock_for_test(
            paused_store,
            stale_target,
            &stale_config,
            stale_clock,
            Duration::from_secs(1),
        )
        .await
    });
    tokio::select! {
        () = marker_get.wait_until_paused() => {}
        outcome = &mut stale_resume => {
            panic!("stale resume returned before its marker read was paused: {outcome:?}");
        }
    }

    assert_eq!(
        resume_delete_with_config_and_clock_for_test(
            winning_store,
            NamespaceId::new(target.clone()).expect("target namespace must be valid"),
            &fixture.config,
            fixture.clock.clone(),
            Duration::from_secs(1),
        )
        .await
        .expect("the winning concurrent resume must finish deletion"),
        NamespaceDeleteOutcome::Deleted
    );
    assert!(
        branch_control_snapshot(&harness.store, &source)
            .await
            .expect("parent root map must remain authoritative")
            .roots
            .is_empty(),
        "the winning resume must release the exact parent root"
    );
    assert!(matches!(
        harness.store.get(&fixture.visibility.marker_key).await,
        Err(zeppelin::error::ZeppelinError::NotFound { .. })
    ));
    assert!(matches!(
        harness.store.get(&NamespaceMetadata::s3_key(&target)).await,
        Err(zeppelin::error::ZeppelinError::NotFound { .. })
    ));

    marker_get.release();
    assert_eq!(
        stale_resume
            .await
            .expect("stale resume task must not panic")
            .expect("stale resume must prove completed deletion after marker loss"),
        NamespaceDeleteOutcome::Deleted
    );
    assert!(matches!(
        harness.store.get(&fixture.visibility.marker_key).await,
        Err(zeppelin::error::ZeppelinError::NotFound { .. })
    ));
    assert!(
        harness
            .store
            .list_prefix(&format!("{target}/"))
            .await
            .expect("target prefix must remain listable")
            .is_empty(),
        "the stale worker must not recreate target-owned lifecycle state"
    );

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

#[derive(Debug, Clone, Copy)]
enum DeletingMaintenanceRow {
    ActiveIntentUnfenced,
    ActiveIntentFencedWithEvidence,
    ActiveIntentRootWonConflict,
    DeletingFencedManifestPresent,
    BranchDeletingWithoutVisibility,
    BranchGracePending,
    BranchGraceElapsedRootPresent,
    BranchRootAbsentBeforeGrace,
    BranchRootReplyLost,
    BranchCleanupPartial,
    ActiveBranchVerified,
    ActiveBranchMissingParentRoot,
    BranchParentIncarnationReplaced,
    OrdinaryGovernedDeleting,
    FinalMetadataDeleteReplyLost,
    ZeroBudget,
}

#[derive(Debug, Clone, Copy)]
struct ExpectedDeletionMaintenance {
    inspected: usize,
    completed: usize,
    in_progress: usize,
    grace_waiting: usize,
}

const SLICE_SIX_MAINTENANCE_BUDGET: Duration = Duration::from_secs(25);

fn assert_deletion_maintenance_report(
    row: DeletingMaintenanceRow,
    report: &BranchMaintenanceReport,
    expected: ExpectedDeletionMaintenance,
) {
    assert_eq!(
        report.deletions_inspected, expected.inspected,
        "{row:?}: maintenance must report each governed deletion state it dispatches"
    );
    assert_eq!(
        report.deletions_completed, expected.completed,
        "{row:?}: maintenance completion count must match metadata-last deletion"
    );
    assert_eq!(
        report.deletions_in_progress, expected.in_progress,
        "{row:?}: durable unfinished deletion must be reported as in progress"
    );
    assert_eq!(
        report.branch_grace_waiting, expected.grace_waiting,
        "{row:?}: only a branch still inside reader-safety grace may wait"
    );
}

async fn maintain_deletions(
    harness: &common::harness::TestHarness,
    store: ZeppelinStore,
    fixture: &EstablishedBranchGrace,
    budget: Duration,
) -> (BranchMaintenanceReport, FullTestServer) {
    maintain_deletions_with_config(
        harness,
        store,
        &fixture.config,
        fixture.clock.clone(),
        budget,
    )
    .await
}

async fn maintain_deletions_with_config(
    harness: &common::harness::TestHarness,
    store: ZeppelinStore,
    config: &Config,
    clock: Clock,
    budget: Duration,
) -> (BranchMaintenanceReport, FullTestServer) {
    let server = start_test_server_full(
        store,
        Some(harness.prefix.clone()),
        config.clone(),
        false,
        Some(clock.clone()),
    )
    .await;
    let report = maintain_branches_with_config_and_clock_for_test(
        server.store.clone(),
        config,
        clock,
        budget,
    )
    .await
    .expect("bounded deletion maintenance pass must complete");
    (report, server)
}

async fn establish_ordinary_governed_deleting(
    store: &ZeppelinStore,
    namespace: &str,
    config: &Config,
) {
    NamespaceManager::new(store.clone())
        .create(namespace, 4, DistanceMetric::Cosine)
        .await
        .expect("ordinary namespace fixture must be created");
    assert_eq!(
        delete_namespace_for_test(
            store.clone(),
            NamespaceId::new(namespace.to_string()).expect("namespace must be valid"),
            config.indexing.clone(),
            config.branching.clone(),
        )
        .await
        .expect("graph delete must establish an ordinary governed tombstone"),
        NamespaceDeleteOutcome::AlreadyDeleting
    );
    assert_eq!(
        read_namespace_metadata(store, namespace).await.state,
        NamespaceState::Deleting,
        "ordinary fixture must stop at a durable governed Deleting state"
    );
}

async fn cleanup_branch_maintenance_fixture(harness: &common::harness::TestHarness) {
    harness.cleanup().await;
}

#[tokio::test]
async fn slice_six_maintain_resumes_governed_deletion_recovery_matrix() {
    Box::pin(run_slice_six_maintain_recovery_matrix()).await;
}

async fn run_slice_six_maintain_recovery_matrix() {
    let rows = [
        DeletingMaintenanceRow::ActiveIntentUnfenced,
        DeletingMaintenanceRow::ActiveIntentFencedWithEvidence,
        DeletingMaintenanceRow::ActiveIntentRootWonConflict,
        DeletingMaintenanceRow::DeletingFencedManifestPresent,
        DeletingMaintenanceRow::BranchDeletingWithoutVisibility,
        DeletingMaintenanceRow::BranchGracePending,
        DeletingMaintenanceRow::BranchGraceElapsedRootPresent,
        DeletingMaintenanceRow::BranchRootAbsentBeforeGrace,
        DeletingMaintenanceRow::BranchRootReplyLost,
        DeletingMaintenanceRow::BranchCleanupPartial,
        DeletingMaintenanceRow::ActiveBranchVerified,
        DeletingMaintenanceRow::ActiveBranchMissingParentRoot,
        DeletingMaintenanceRow::BranchParentIncarnationReplaced,
        DeletingMaintenanceRow::OrdinaryGovernedDeleting,
        DeletingMaintenanceRow::FinalMetadataDeleteReplyLost,
        DeletingMaintenanceRow::ZeroBudget,
    ];

    for row in rows {
        run_slice_six_maintain_recovery_row(row).await;
    }
}

fn run_slice_six_maintain_recovery_row(
    row: DeletingMaintenanceRow,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()>>> {
    match row {
        DeletingMaintenanceRow::ActiveIntentUnfenced => Box::pin(async move {
            let harness = common::harness::TestHarness::new().await;
            let store = scoped_test_security_store(&harness.store, &harness.prefix);
            let namespace = harness.artifact_origin_namespace("maintain-active-unfenced");
            let config = branch_grace_config();
            let clock = Clock::from_source(Arc::new(AdjustableWallClock::new(Utc::now())));
            NamespaceManager::new(store.clone())
                .create(&namespace, 4, DistanceMetric::Cosine)
                .await
                .expect("unfenced crash fixture namespace must be created");
            let (crashed_store, fence_failure) =
                fail_put_once_matching(&store, Manifest::s3_key(&namespace));
            let crashed_server = start_test_server_full(
                crashed_store,
                Some(harness.prefix.clone()),
                config.clone(),
                false,
                Some(clock.clone()),
            )
            .await;

            delete_namespace_for_test(
                crashed_server.store.clone(),
                NamespaceId::new(namespace.clone()).expect("namespace must be valid"),
                config.indexing.clone(),
                config.branching.clone(),
            )
            .await
            .expect_err("fence publication failure must stop after installing the intent");
            assert_eq!(fence_failure.failures_injected(), 1);
            let interrupted = read_namespace_metadata(&crashed_server.store, &namespace).await;
            assert_eq!(interrupted.state, NamespaceState::Active);
            let intent = interrupted
                .deletion_intent
                .as_ref()
                .expect("unfenced crash must retain the durable deletion intent");
            assert_eq!(intent.fenced_generation, None);
            assert!(
                !branch_control_snapshot(&crashed_server.store, &namespace)
                    .await
                    .expect("unfenced live manifest must remain readable")
                    .deletion_fenced,
                "crash before the fence must leave the live manifest writable"
            );
            crashed_server.shutdown().await;

            let (report, server) = maintain_deletions_with_config(
                &harness,
                store.clone(),
                &config,
                clock,
                SLICE_SIX_MAINTENANCE_BUDGET,
            )
            .await;
            assert_deletion_maintenance_report(
                row,
                &report,
                ExpectedDeletionMaintenance {
                    inspected: 1,
                    completed: 1,
                    in_progress: 0,
                    grace_waiting: 0,
                },
            );
            assert!(matches!(
                store.get(&NamespaceMetadata::s3_key(&namespace)).await,
                Err(zeppelin::error::ZeppelinError::NotFound { .. })
            ));
            assert!(
                store
                    .list_prefix(&format!("{namespace}/"))
                    .await
                    .expect("completed unfenced recovery prefix must be listable")
                    .is_empty(),
                "metadata-last completion must leave no namespace-owned objects"
            );
            server.shutdown().await;
            harness.cleanup().await;
        }),
        DeletingMaintenanceRow::ActiveIntentFencedWithEvidence => Box::pin(async move {
            let harness = common::harness::TestHarness::new().await;
            let store = scoped_test_security_store(&harness.store, &harness.prefix);
            let namespace = harness.artifact_origin_namespace("maintain-active-fenced");
            let config = branch_grace_config();
            let clock = Clock::from_source(Arc::new(AdjustableWallClock::new(Utc::now())));
            NamespaceManager::new(store.clone())
                .create(&namespace, 4, DistanceMetric::Cosine)
                .await
                .expect("fenced crash fixture namespace must be created");
            let (crashed_store, evidence_lost_reply) =
                fail_after_put_once_matching(&store, "_audit/destruction/");
            let crashed_server = start_test_server_full(
                crashed_store,
                Some(harness.prefix.clone()),
                config.clone(),
                false,
                Some(clock.clone()),
            )
            .await;

            delete_namespace_for_test(
                crashed_server.store.clone(),
                NamespaceId::new(namespace.clone()).expect("namespace must be valid"),
                config.indexing.clone(),
                config.branching.clone(),
            )
            .await
            .expect_err("lost evidence PUT reply must stop before the tombstone CAS");
            assert_eq!(evidence_lost_reply.failures_injected(), 1);
            let interrupted = read_namespace_metadata(&crashed_server.store, &namespace).await;
            assert_eq!(interrupted.state, NamespaceState::Active);
            let intent = interrupted
                .deletion_intent
                .as_ref()
                .expect("fenced crash must retain the durable deletion intent");
            assert!(
                intent.fenced_generation.is_some(),
                "fenced crash must persist the exact fenced generation"
            );
            assert!(
                branch_control_snapshot(&crashed_server.store, &namespace)
                    .await
                    .expect("fenced live manifest must remain readable")
                    .deletion_fenced,
                "crash after fence publication must leave data paths fenced"
            );
            assert!(
                crashed_server
                    .store
                    .get(&intent.destruction_record_key)
                    .await
                    .is_ok(),
                "lost evidence response must hide a durably committed evidence object"
            );
            crashed_server.shutdown().await;

            let (report, server) = maintain_deletions_with_config(
                &harness,
                store.clone(),
                &config,
                clock,
                SLICE_SIX_MAINTENANCE_BUDGET,
            )
            .await;
            assert_deletion_maintenance_report(
                row,
                &report,
                ExpectedDeletionMaintenance {
                    inspected: 1,
                    completed: 1,
                    in_progress: 0,
                    grace_waiting: 0,
                },
            );
            assert!(matches!(
                store.get(&NamespaceMetadata::s3_key(&namespace)).await,
                Err(zeppelin::error::ZeppelinError::NotFound { .. })
            ));
            assert!(
                store
                    .list_prefix(&format!("{namespace}/"))
                    .await
                    .expect("completed fenced recovery prefix must be listable")
                    .is_empty(),
                "metadata-last completion must leave no namespace-owned objects"
            );
            server.shutdown().await;
            harness.cleanup().await;
        }),
        DeletingMaintenanceRow::ActiveIntentRootWonConflict => Box::pin(async move {
            let harness = common::harness::TestHarness::new().await;
            let store = scoped_test_security_store(&harness.store, &harness.prefix);
            let source = harness.artifact_origin_namespace("maintain-root-won-source");
            let target = harness.artifact_origin_namespace("maintain-root-won-target");
            let config = branch_grace_config();
            let clock = Clock::from_source(Arc::new(AdjustableWallClock::new(Utc::now())));
            NamespaceManager::new(store.clone())
                .create(&source, 4, DistanceMetric::Cosine)
                .await
                .expect("root-won source fixture must be created");
            let (crashed_store, fence_failure) =
                fail_put_once_matching(&store, Manifest::s3_key(&source));
            let crashed_server = start_test_server_full(
                crashed_store,
                Some(harness.prefix.clone()),
                config.clone(),
                false,
                Some(clock.clone()),
            )
            .await;
            delete_namespace_for_test(
                crashed_server.store.clone(),
                NamespaceId::new(source.clone()).expect("source namespace must be valid"),
                config.indexing.clone(),
                config.branching.clone(),
            )
            .await
            .expect_err("root-won fixture must stop before the destruction fence");
            assert_eq!(fence_failure.failures_injected(), 1);
            assert!(read_namespace_metadata(&crashed_server.store, &source)
                .await
                .deletion_intent
                .is_some());
            crashed_server.shutdown().await;

            let server = start_test_server_full(
                store.clone(),
                Some(harness.prefix.clone()),
                config.clone(),
                false,
                Some(clock.clone()),
            )
            .await;
            let root = prepare_head_branch_root(
                server.store.clone(),
                &source,
                BranchId::new(),
                &target,
                uuid::Uuid::new_v4(),
                ForkViewDigest::new([0x6c; 32]),
                clock.now(),
            )
            .await
            .expect("root-won fixture must bind the current source head");
            insert_prepared_branch_root(server.store.clone(), &source, root.clone(), 8)
                .await
                .expect("branch root must win before deletion fencing");
            let roots_before = branch_control_snapshot(&server.store, &source)
                .await
                .expect("winning root collection must remain readable")
                .roots;
            assert_eq!(roots_before, vec![root]);

            let error = maintain_branches_with_config_and_clock_for_test(
                server.store.clone(),
                &config,
                clock,
                SLICE_SIX_MAINTENANCE_BUDGET,
            )
            .await
            .expect_err("root-won deletion recovery must fail the maintenance pass");
            assert!(matches!(
                error,
                zeppelin::error::ZeppelinError::Branch(inner)
                    if matches!(
                        inner.as_ref(),
                        BranchError::NamespaceHasLiveBranches { namespace, .. }
                            if namespace == &source
                    )
            ));
            let recovered = read_namespace_metadata(&server.store, &source).await;
            assert_eq!(recovered.state, NamespaceState::Active);
            assert_eq!(
                recovered.deletion_intent, None,
                "root-won recovery must clear only the still-unfenced deletion intent"
            );
            assert_eq!(
                branch_control_snapshot(&server.store, &source)
                    .await
                    .expect("winning root collection must remain authoritative")
                    .roots,
                roots_before,
                "root-won recovery must not mutate any exact child root"
            );
            server.shutdown().await;
            harness.cleanup().await;
        }),
        DeletingMaintenanceRow::DeletingFencedManifestPresent => Box::pin(async move {
            let harness = common::harness::TestHarness::new().await;
            let store = scoped_test_security_store(&harness.store, &harness.prefix);
            let namespace = harness.artifact_origin_namespace("maintain-deleting-fenced");
            let config = branch_grace_config();
            let clock = Clock::from_source(Arc::new(AdjustableWallClock::new(Utc::now())));
            NamespaceManager::new(store.clone())
                .create(&namespace, 4, DistanceMetric::Cosine)
                .await
                .expect("fenced-live fixture namespace must be created");
            let (crashed_store, visibility_delete_failure) =
                fail_delete_once_matching(&store, Manifest::s3_key(&namespace));
            let crashed_server = start_test_server_full(
                crashed_store,
                Some(harness.prefix.clone()),
                config.clone(),
                false,
                Some(clock.clone()),
            )
            .await;
            delete_namespace_for_test(
                crashed_server.store.clone(),
                NamespaceId::new(namespace.clone()).expect("namespace must be valid"),
                config.indexing.clone(),
                config.branching.clone(),
            )
            .await
            .expect_err("visibility DELETE failure must retain the fenced live manifest");
            assert_eq!(visibility_delete_failure.failures_injected(), 1);
            let interrupted = read_namespace_metadata(&crashed_server.store, &namespace).await;
            assert_eq!(interrupted.state, NamespaceState::Deleting);
            assert!(interrupted
                .deletion_intent
                .as_ref()
                .and_then(|intent| intent.fenced_generation)
                .is_some());
            assert!(
                branch_control_snapshot(&crashed_server.store, &namespace)
                    .await
                    .expect("fenced live manifest must remain readable")
                    .deletion_fenced,
                "Deleting fixture must retain the exact destruction fence"
            );
            crashed_server.shutdown().await;

            let (report, server) = maintain_deletions_with_config(
                &harness,
                store.clone(),
                &config,
                clock,
                SLICE_SIX_MAINTENANCE_BUDGET,
            )
            .await;
            assert_deletion_maintenance_report(
                row,
                &report,
                ExpectedDeletionMaintenance {
                    inspected: 1,
                    completed: 1,
                    in_progress: 0,
                    grace_waiting: 0,
                },
            );
            assert!(matches!(
                store.get(&NamespaceMetadata::s3_key(&namespace)).await,
                Err(zeppelin::error::ZeppelinError::NotFound { .. })
            ));
            server.shutdown().await;
            harness.cleanup().await;
        }),
        DeletingMaintenanceRow::BranchDeletingWithoutVisibility => Box::pin(async move {
            let harness = common::harness::TestHarness::new().await;
            let store = scoped_test_security_store(&harness.store, &harness.prefix);
            let source = harness.artifact_origin_namespace("maintain-no-marker-source");
            let target = harness.artifact_origin_namespace("maintain-no-marker-target");
            let config = branch_grace_config();
            let wall_clock = Arc::new(AdjustableWallClock::new(Utc::now()));
            let clock = Clock::from_source(wall_clock.clone());
            let setup_server = start_test_server_full(
                store.clone(),
                Some(harness.prefix.clone()),
                config.clone(),
                false,
                Some(clock.clone()),
            )
            .await;
            NamespaceManager::new(setup_server.store.clone())
                .create(&source, 4, DistanceMetric::Cosine)
                .await
                .expect("marker-gap source fixture must be created");
            activate_branch(setup_server.store.clone(), &source, &target, &config).await;
            setup_server.shutdown().await;

            let marker_prefix = format!("{target}/_lifecycle/branch_visibility_removed/");
            let (crashed_store, marker_failure) =
                fail_put_once_matching(&store, marker_prefix.clone());
            let crashed_server = start_test_server_full(
                crashed_store,
                Some(harness.prefix.clone()),
                config.clone(),
                false,
                Some(clock.clone()),
            )
            .await;
            delete_namespace_for_test(
                crashed_server.store.clone(),
                NamespaceId::new(target.clone()).expect("target namespace must be valid"),
                config.indexing.clone(),
                config.branching.clone(),
            )
            .await
            .expect_err("marker PUT failure must leave a marker-free branch tombstone");
            assert_eq!(marker_failure.failures_injected(), 1);
            let interrupted = read_namespace_metadata(&crashed_server.store, &target).await;
            assert_eq!(interrupted.state, NamespaceState::Deleting);
            assert_eq!(
                interrupted
                    .deletion_intent
                    .as_ref()
                    .and_then(|intent| intent.visibility.as_ref()),
                None
            );
            let roots_before = branch_control_snapshot(&crashed_server.store, &source)
                .await
                .expect("marker-free branch root must remain readable")
                .roots;
            crashed_server.shutdown().await;

            let (waiting_report, waiting_server) = maintain_deletions_with_config(
                &harness,
                store.clone(),
                &config,
                clock.clone(),
                SLICE_SIX_MAINTENANCE_BUDGET,
            )
            .await;
            assert_deletion_maintenance_report(
                row,
                &waiting_report,
                ExpectedDeletionMaintenance {
                    inspected: 1,
                    completed: 0,
                    in_progress: 1,
                    grace_waiting: 1,
                },
            );
            let recovered = read_namespace_metadata(&waiting_server.store, &target).await;
            let visibility = recovered
                .deletion_intent
                .as_ref()
                .and_then(|intent| intent.visibility.clone())
                .expect("maintenance must persist a fresh full grace boundary");
            assert!(waiting_server
                .store
                .get(&visibility.marker_key)
                .await
                .is_ok());
            assert_eq!(
                branch_control_snapshot(&waiting_server.store, &source)
                    .await
                    .expect("fresh grace must retain the exact root collection")
                    .roots,
                roots_before
            );
            waiting_server.shutdown().await;

            wall_clock.set(visibility.not_before);
            let (completed_report, completed_server) = maintain_deletions_with_config(
                &harness,
                store.clone(),
                &config,
                clock,
                SLICE_SIX_MAINTENANCE_BUDGET,
            )
            .await;
            assert_deletion_maintenance_report(
                row,
                &completed_report,
                ExpectedDeletionMaintenance {
                    inspected: 1,
                    completed: 1,
                    in_progress: 0,
                    grace_waiting: 0,
                },
            );
            assert!(matches!(
                store.get(&NamespaceMetadata::s3_key(&target)).await,
                Err(zeppelin::error::ZeppelinError::NotFound { .. })
            ));
            completed_server.shutdown().await;
            harness.cleanup().await;
        }),
        DeletingMaintenanceRow::BranchGracePending => Box::pin(async move {
            let harness = common::harness::TestHarness::new().await;
            let store = scoped_test_security_store(&harness.store, &harness.prefix);
            let source = harness.artifact_origin_namespace("maintain-grace-source");
            let target = harness.artifact_origin_namespace("maintain-grace-target");
            let fixture =
                establish_branch_grace_on_store(&harness, store.clone(), &source, &target).await;
            fixture.wall_clock.set(
                fixture
                    .visibility
                    .not_before
                    .checked_sub_signed(chrono::Duration::nanoseconds(1))
                    .expect("pre-grace maintenance instant must be representable"),
            );
            let roots_before = branch_control_snapshot(&store, &source)
                .await
                .expect("grace-pending root collection must be readable")
                .roots;

            let (report, server) = maintain_deletions(
                &harness,
                store.clone(),
                &fixture,
                SLICE_SIX_MAINTENANCE_BUDGET,
            )
            .await;
            assert_deletion_maintenance_report(
                row,
                &report,
                ExpectedDeletionMaintenance {
                    inspected: 1,
                    completed: 0,
                    in_progress: 1,
                    grace_waiting: 1,
                },
            );
            assert_eq!(
                branch_control_snapshot(&store, &source)
                    .await
                    .expect("grace-pending parent manifest must remain readable")
                    .roots,
                roots_before,
                "maintenance must retain the full exact root collection through grace"
            );
            assert_eq!(
                read_namespace_metadata(&store, &target).await.state,
                NamespaceState::Deleting
            );
            server.shutdown().await;
            cleanup_branch_maintenance_fixture(&harness).await;
        }),
        DeletingMaintenanceRow::BranchGraceElapsedRootPresent => Box::pin(async move {
            let harness = common::harness::TestHarness::new().await;
            let store = scoped_test_security_store(&harness.store, &harness.prefix);
            let source = harness.artifact_origin_namespace("maintain-release-source");
            let target = harness.artifact_origin_namespace("maintain-release-target");
            let fixture =
                establish_branch_grace_on_store(&harness, store.clone(), &source, &target).await;
            fixture.wall_clock.set(fixture.visibility.not_before);

            let (report, server) = maintain_deletions(
                &harness,
                store.clone(),
                &fixture,
                SLICE_SIX_MAINTENANCE_BUDGET,
            )
            .await;
            assert_deletion_maintenance_report(
                row,
                &report,
                ExpectedDeletionMaintenance {
                    inspected: 1,
                    completed: 1,
                    in_progress: 0,
                    grace_waiting: 0,
                },
            );
            assert!(
                branch_control_snapshot(&store, &source)
                    .await
                    .expect("released parent manifest must remain readable")
                    .roots
                    .is_empty(),
                "elapsed maintenance must remove the exact parent root"
            );
            assert!(matches!(
                store.get(&NamespaceMetadata::s3_key(&target)).await,
                Err(zeppelin::error::ZeppelinError::NotFound { .. })
            ));
            server.shutdown().await;
            cleanup_branch_maintenance_fixture(&harness).await;
        }),
        DeletingMaintenanceRow::BranchRootAbsentBeforeGrace => Box::pin(async move {
            let harness = common::harness::TestHarness::new().await;
            let store = scoped_test_security_store(&harness.store, &harness.prefix);
            let source = harness.artifact_origin_namespace("maintain-early-root-source");
            let target = harness.artifact_origin_namespace("maintain-early-root-target");
            let fixture =
                establish_branch_grace_on_store(&harness, store.clone(), &source, &target).await;
            fixture.wall_clock.set(
                fixture
                    .visibility
                    .not_before
                    .checked_sub_signed(chrono::Duration::nanoseconds(1))
                    .expect("pre-grace integrity instant must be representable"),
            );
            let server = start_branch_recovery_server(&harness, store.clone(), &fixture).await;
            let root = branch_control_snapshot(&server.store, &source)
                .await
                .expect("pre-grace root must remain readable")
                .roots
                .into_iter()
                .next()
                .expect("active branch must retain one exact parent root");
            remove_prepared_branch_root(server.store.clone(), &source, root.clone())
                .await
                .expect("test must inject premature exact-root absence");

            let error = maintain_branches_with_config_and_clock_for_test(
                server.store.clone(),
                &fixture.config,
                fixture.clock.clone(),
                SLICE_SIX_MAINTENANCE_BUDGET,
            )
            .await
            .expect_err("pre-grace root absence must fail the maintenance pass closed");
            assert!(matches!(
                error,
                zeppelin::error::ZeppelinError::Branch(inner)
                    if matches!(
                        inner.as_ref(),
                        BranchError::BranchRootMissing { branch_id }
                            if branch_id == &root.branch_id
                    )
            ));
            let unchanged = read_namespace_metadata(&server.store, &target).await;
            assert_eq!(unchanged.state, NamespaceState::Deleting);
            assert_eq!(
                unchanged
                    .deletion_intent
                    .as_ref()
                    .and_then(|intent| intent.root_release.as_ref()),
                None,
                "premature root absence must not manufacture release acknowledgement"
            );
            assert!(server
                .store
                .get(&fixture.visibility.marker_key)
                .await
                .is_ok());
            server.shutdown().await;
            cleanup_branch_maintenance_fixture(&harness).await;
        }),
        DeletingMaintenanceRow::BranchRootReplyLost => Box::pin(async move {
            let harness = common::harness::TestHarness::new().await;
            let store = scoped_test_security_store(&harness.store, &harness.prefix);
            let source = harness.artifact_origin_namespace("maintain-lost-root-source");
            let target = harness.artifact_origin_namespace("maintain-lost-root-target");
            let fixture =
                establish_branch_grace_on_store(&harness, store.clone(), &source, &target).await;
            fixture.wall_clock.set(fixture.visibility.not_before);
            let (lost_reply_store, lost_reply) =
                fail_after_put_once_matching(&store, format!("{source}/manifest.json"));
            let interrupted =
                start_branch_recovery_server(&harness, lost_reply_store, &fixture).await;
            resume_branch_delete(&interrupted, &target, &fixture)
                .await
                .expect_err("lost parent-root CAS reply must leave convergence work");
            assert_eq!(lost_reply.failures_injected(), 1);
            assert!(branch_control_snapshot(&interrupted.store, &source)
                .await
                .expect("committed root removal must remain authoritative")
                .roots
                .is_empty());
            assert_eq!(
                read_namespace_metadata(&interrupted.store, &target)
                    .await
                    .deletion_intent
                    .as_ref()
                    .and_then(|intent| intent.root_release.as_ref()),
                None,
                "lost root reply must leave the target acknowledgement absent"
            );
            interrupted.shutdown().await;

            let (report, server) = maintain_deletions(
                &harness,
                store.clone(),
                &fixture,
                SLICE_SIX_MAINTENANCE_BUDGET,
            )
            .await;
            assert_deletion_maintenance_report(
                row,
                &report,
                ExpectedDeletionMaintenance {
                    inspected: 1,
                    completed: 1,
                    in_progress: 0,
                    grace_waiting: 0,
                },
            );
            assert!(matches!(
                store.get(&NamespaceMetadata::s3_key(&target)).await,
                Err(zeppelin::error::ZeppelinError::NotFound { .. })
            ));
            server.shutdown().await;
            cleanup_branch_maintenance_fixture(&harness).await;
        }),
        DeletingMaintenanceRow::BranchCleanupPartial => Box::pin(async move {
            let harness = common::harness::TestHarness::new().await;
            let store = scoped_test_security_store(&harness.store, &harness.prefix);
            let source = harness.artifact_origin_namespace("maintain-partial-source");
            let target = harness.artifact_origin_namespace("maintain-partial-target");
            let fixture =
                establish_branch_grace_on_store(&harness, store.clone(), &source, &target).await;
            fixture.wall_clock.set(fixture.visibility.not_before);
            let target_meta_key = NamespaceMetadata::s3_key(&target);
            let (partial_store, metadata_delete_failure) =
                fail_delete_once_matching(&store, target_meta_key.clone());
            let interrupted = start_branch_recovery_server(&harness, partial_store, &fixture).await;
            resume_branch_delete(&interrupted, &target, &fixture)
                .await
                .expect_err("metadata-last failure must leave partial cleanup resumable");
            assert_eq!(metadata_delete_failure.failures_injected(), 1);
            let partial = read_namespace_metadata(&interrupted.store, &target).await;
            assert_final_root_release(&partial, false);
            assert!(matches!(
                interrupted.store.get(&fixture.visibility.marker_key).await,
                Err(zeppelin::error::ZeppelinError::NotFound { .. })
            ));
            interrupted.shutdown().await;

            let (report, server) = maintain_deletions(
                &harness,
                store.clone(),
                &fixture,
                SLICE_SIX_MAINTENANCE_BUDGET,
            )
            .await;
            assert_deletion_maintenance_report(
                row,
                &report,
                ExpectedDeletionMaintenance {
                    inspected: 1,
                    completed: 1,
                    in_progress: 0,
                    grace_waiting: 0,
                },
            );
            assert!(matches!(
                store.get(&target_meta_key).await,
                Err(zeppelin::error::ZeppelinError::NotFound { .. })
            ));
            server.shutdown().await;
            cleanup_branch_maintenance_fixture(&harness).await;
        }),
        DeletingMaintenanceRow::ActiveBranchVerified => Box::pin(async move {
            let harness = common::harness::TestHarness::new().await;
            let store = scoped_test_security_store(&harness.store, &harness.prefix);
            let source = harness.artifact_origin_namespace("maintain-active-source");
            let target = harness.artifact_origin_namespace("maintain-active-target");
            let config = branch_grace_config();
            let clock = Clock::from_source(Arc::new(AdjustableWallClock::new(Utc::now())));
            let server = start_test_server_full(
                store,
                Some(harness.prefix.clone()),
                config.clone(),
                false,
                Some(clock.clone()),
            )
            .await;
            NamespaceManager::new(server.store.clone())
                .create(&source, 4, DistanceMetric::Cosine)
                .await
                .expect("active source fixture must be created");
            activate_branch(server.store.clone(), &source, &target, &config).await;

            let roots_before = branch_control_snapshot(&server.store, &source)
                .await
                .expect("active branch roots must be readable before maintenance")
                .roots;
            assert_eq!(
                roots_before.len(),
                1,
                "active fixture must begin with exactly one parent root"
            );
            let source_metadata_before = server
                .store
                .get(&NamespaceMetadata::s3_key(&source))
                .await
                .expect("source metadata bytes must be readable before maintenance");
            let target_metadata_before = server
                .store
                .get(&NamespaceMetadata::s3_key(&target))
                .await
                .expect("target metadata bytes must be readable before maintenance");
            let source_manifest_before = server
                .store
                .get(&Manifest::s3_key(&source))
                .await
                .expect("source live manifest must be readable before maintenance");
            let target_manifest_before = server
                .store
                .get(&Manifest::s3_key(&target))
                .await
                .expect("target live manifest must be readable before maintenance");

            let report = maintain_branches_with_config_and_clock_for_test(
                server.store.clone(),
                &config,
                clock,
                SLICE_SIX_MAINTENANCE_BUDGET,
            )
            .await
            .expect("maintenance must verify a consistent active branch");
            assert_eq!(
                report,
                BranchMaintenanceReport {
                    active_verified: 1,
                    ..BranchMaintenanceReport::default()
                },
                "active verification must not dispatch deletion or mutate another lifecycle"
            );

            let roots_after = branch_control_snapshot(&server.store, &source)
                .await
                .expect("active branch roots must remain readable after maintenance")
                .roots;
            assert_eq!(roots_after, roots_before);
            assert_eq!(
                server
                    .store
                    .get(&NamespaceMetadata::s3_key(&source))
                    .await
                    .expect("source metadata bytes must remain readable"),
                source_metadata_before
            );
            assert_eq!(
                server
                    .store
                    .get(&NamespaceMetadata::s3_key(&target))
                    .await
                    .expect("target metadata bytes must remain readable"),
                target_metadata_before
            );
            assert_eq!(
                server
                    .store
                    .get(&Manifest::s3_key(&source))
                    .await
                    .expect("source live manifest must remain readable"),
                source_manifest_before
            );
            assert_eq!(
                server
                    .store
                    .get(&Manifest::s3_key(&target))
                    .await
                    .expect("target live manifest must remain readable"),
                target_manifest_before
            );

            server.shutdown().await;
            harness.cleanup().await;
        }),
        DeletingMaintenanceRow::ActiveBranchMissingParentRoot => Box::pin(async move {
            let harness = common::harness::TestHarness::new().await;
            let store = scoped_test_security_store(&harness.store, &harness.prefix);
            let source = harness.artifact_origin_namespace("maintain-missing-root-source");
            let target = harness.artifact_origin_namespace("maintain-missing-root-target");
            let config = branch_grace_config();
            let clock = Clock::from_source(Arc::new(AdjustableWallClock::new(Utc::now())));
            let server = start_test_server_full(
                store.clone(),
                Some(harness.prefix.clone()),
                config.clone(),
                false,
                Some(clock.clone()),
            )
            .await;
            NamespaceManager::new(server.store.clone())
                .create(&source, 4, DistanceMetric::Cosine)
                .await
                .expect("missing-root source fixture must be created");
            activate_branch(server.store.clone(), &source, &target, &config).await;
            let root = branch_control_snapshot(&server.store, &source)
                .await
                .expect("active branch root must remain readable")
                .roots
                .into_iter()
                .next()
                .expect("active target must begin with one exact parent root");
            remove_prepared_branch_root(server.store.clone(), &source, root.clone())
                .await
                .expect("test must inject an active target with no parent root");

            let error = maintain_branches_with_config_and_clock_for_test(
                server.store.clone(),
                &config,
                clock,
                SLICE_SIX_MAINTENANCE_BUDGET,
            )
            .await
            .expect_err("active target root absence must fail graph maintenance closed");
            assert!(matches!(
                error,
                zeppelin::error::ZeppelinError::Branch(inner)
                    if matches!(
                        inner.as_ref(),
                        BranchError::BranchRootMissing { branch_id }
                            if branch_id == &root.branch_id
                    )
            ));
            let target_metadata = read_namespace_metadata(&server.store, &target).await;
            assert_eq!(target_metadata.state, NamespaceState::Active);
            assert_eq!(target_metadata.deletion_intent, None);
            assert!(Manifest::read(&server.store, &target)
                .await
                .expect("active target visibility must remain readable")
                .is_some());
            assert!(branch_control_snapshot(&server.store, &source)
                .await
                .expect("parent manifest must remain readable")
                .roots
                .is_empty());
            server.shutdown().await;
            harness.cleanup().await;
        }),
        DeletingMaintenanceRow::BranchParentIncarnationReplaced => Box::pin(async move {
            let harness = common::harness::TestHarness::new().await;
            let store = scoped_test_security_store(&harness.store, &harness.prefix);
            let source = harness.artifact_origin_namespace("maintain-replaced-source");
            let target = harness.artifact_origin_namespace("maintain-replaced-target");
            let fixture =
                establish_branch_grace_on_store(&harness, store.clone(), &source, &target).await;
            fixture.wall_clock.set(fixture.visibility.not_before);
            let server = start_branch_recovery_server(&harness, store, &fixture).await;
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
                .expect("same parent name must be recreated with a new incarnation");
            assert_ne!(replacement.incarnation_id, original.incarnation_id);

            let error = maintain_branches_with_config_and_clock_for_test(
                server.store.clone(),
                &fixture.config,
                fixture.clock.clone(),
                SLICE_SIX_MAINTENANCE_BUDGET,
            )
            .await
            .expect_err("replacement parent lifetime must fail graph maintenance closed");
            assert!(matches!(
                error,
                zeppelin::error::ZeppelinError::Branch(inner)
                    if matches!(
                        inner.as_ref(),
                        BranchError::SourceIncarnationChanged { namespace }
                            if namespace.as_str() == source
                    )
            ));
            let unchanged = read_namespace_metadata(&server.store, &target).await;
            assert_eq!(unchanged.state, NamespaceState::Deleting);
            assert_eq!(
                unchanged
                    .deletion_intent
                    .as_ref()
                    .and_then(|intent| intent.root_release.as_ref()),
                None,
                "replacement parent lifetime must not acknowledge old root release"
            );
            assert!(server
                .store
                .get(&fixture.visibility.marker_key)
                .await
                .is_ok());
            assert!(branch_control_snapshot(&server.store, &source)
                .await
                .expect("replacement parent manifest must remain readable")
                .roots
                .is_empty());
            server.shutdown().await;
            harness.cleanup().await;
        }),
        DeletingMaintenanceRow::OrdinaryGovernedDeleting => Box::pin(async move {
            let harness = common::harness::TestHarness::new().await;
            let store = scoped_test_security_store(&harness.store, &harness.prefix);
            let namespace = harness.artifact_origin_namespace("maintain-ordinary");
            let config = branch_grace_config();
            let wall_clock = Arc::new(AdjustableWallClock::new(Utc::now()));
            let clock = Clock::from_source(wall_clock);
            let server = start_test_server_full(
                store.clone(),
                Some(harness.prefix.clone()),
                config.clone(),
                false,
                Some(clock.clone()),
            )
            .await;
            establish_ordinary_governed_deleting(&store, &namespace, &config).await;

            let report = maintain_branches_with_config_and_clock_for_test(
                server.store.clone(),
                &config,
                clock,
                SLICE_SIX_MAINTENANCE_BUDGET,
            )
            .await
            .expect("ordinary governed deletion maintenance must complete");
            assert_deletion_maintenance_report(
                row,
                &report,
                ExpectedDeletionMaintenance {
                    inspected: 1,
                    completed: 1,
                    in_progress: 0,
                    grace_waiting: 0,
                },
            );
            assert!(matches!(
                store.get(&NamespaceMetadata::s3_key(&namespace)).await,
                Err(zeppelin::error::ZeppelinError::NotFound { .. })
            ));
            server.shutdown().await;
            harness.cleanup().await;
        }),
        DeletingMaintenanceRow::FinalMetadataDeleteReplyLost => Box::pin(async move {
            let harness = common::harness::TestHarness::new().await;
            let store = scoped_test_security_store(&harness.store, &harness.prefix);
            let namespace = harness.artifact_origin_namespace("maintain-lost-meta-reply");
            let config = branch_grace_config();
            let clock = Clock::from_source(Arc::new(AdjustableWallClock::new(Utc::now())));
            let setup_server = start_test_server_full(
                store.clone(),
                Some(harness.prefix.clone()),
                config.clone(),
                false,
                Some(clock.clone()),
            )
            .await;
            establish_ordinary_governed_deleting(&store, &namespace, &config).await;
            setup_server.shutdown().await;

            let metadata_key = NamespaceMetadata::s3_key(&namespace);
            let (lost_reply_store, lost_reply) =
                fail_after_delete_once_matching(&store, metadata_key.clone());
            let lost_reply_server = start_test_server_full(
                lost_reply_store,
                Some(harness.prefix.clone()),
                config.clone(),
                false,
                Some(clock.clone()),
            )
            .await;
            let report = maintain_branches_with_config_and_clock_for_test(
                lost_reply_server.store.clone(),
                &config,
                clock.clone(),
                SLICE_SIX_MAINTENANCE_BUDGET,
            )
            .await
            .expect("lost final metadata DELETE reply must converge in the same pass");
            assert_deletion_maintenance_report(
                row,
                &report,
                ExpectedDeletionMaintenance {
                    inspected: 1,
                    completed: 1,
                    in_progress: 0,
                    grace_waiting: 0,
                },
            );
            assert_eq!(lost_reply.failures_injected(), 1);
            assert!(matches!(
                lost_reply_server.store.get(&metadata_key).await,
                Err(zeppelin::error::ZeppelinError::NotFound { .. })
            ));
            assert!(
                lost_reply_server
                    .store
                    .list_prefix(&format!("{namespace}/"))
                    .await
                    .expect("lost-reply namespace prefix must remain listable")
                    .is_empty(),
                "the hidden successful DELETE must still complete metadata-last cleanup"
            );
            lost_reply_server.shutdown().await;
            harness.cleanup().await;
        }),
        DeletingMaintenanceRow::ZeroBudget => Box::pin(async move {
            let harness = common::harness::TestHarness::new().await;
            let store = scoped_test_security_store(&harness.store, &harness.prefix);
            let namespace = harness.artifact_origin_namespace("maintain-zero-budget");
            let config = branch_grace_config();
            let wall_clock = Arc::new(AdjustableWallClock::new(Utc::now()));
            let clock = Clock::from_source(wall_clock);
            let server = start_test_server_full(
                store.clone(),
                Some(harness.prefix.clone()),
                config.clone(),
                false,
                Some(clock.clone()),
            )
            .await;
            establish_ordinary_governed_deleting(&store, &namespace, &config).await;

            let (counted_store, counter) = counting_store(&server.store);
            let report = maintain_branches_with_config_and_clock_for_test(
                counted_store,
                &config,
                clock,
                Duration::ZERO,
            )
            .await
            .expect("zero-budget maintenance must return without destructive work");
            assert_deletion_maintenance_report(
                row,
                &report,
                ExpectedDeletionMaintenance {
                    inspected: 0,
                    completed: 0,
                    in_progress: 0,
                    grace_waiting: 0,
                },
            );
            assert_eq!(
                counter.delimiter_list_calls_for_prefix(""),
                0,
                "zero budget must stop before authoritative namespace discovery"
            );
            assert_eq!(
                read_namespace_metadata(&store, &namespace).await.state,
                NamespaceState::Deleting,
                "zero budget must leave the durable recovery handle untouched"
            );
            server.shutdown().await;
            harness.cleanup().await;
        }),
    }
}

#[tokio::test]
async fn slice_six_maintain_fails_closed_on_orphan_parent_root() {
    let harness = common::harness::TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let source = harness.artifact_origin_namespace("maintain-orphan-source");
    let target = harness.artifact_origin_namespace("maintain-orphan-target");
    let config = branch_grace_config();
    let wall_clock = Arc::new(AdjustableWallClock::new(Utc::now()));
    let clock = Clock::from_source(wall_clock);

    NamespaceManager::new(store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .expect("orphan-root parent fixture must be created");
    activate_branch(store.clone(), &source, &target, &config).await;

    let before = branch_control_snapshot(&store, &source)
        .await
        .expect("parent root must be authoritative before corruption injection");
    assert_eq!(before.roots.len(), 1);
    let expected_root = before.roots[0].clone();

    store
        .delete_prefix(&format!("{target}/"))
        .await
        .expect("test must simulate target metadata disappearing before root release");
    assert!(matches!(
        store.get(&NamespaceMetadata::s3_key(&target)).await,
        Err(zeppelin::error::ZeppelinError::NotFound { .. })
    ));

    let error = maintain_branches_with_config_and_clock_for_test(
        store.clone(),
        &config,
        clock,
        SLICE_SIX_MAINTENANCE_BUDGET,
    )
    .await
    .expect_err("maintenance must fail closed on an orphan parent root");

    match error {
        zeppelin::error::ZeppelinError::Branch(error) => match *error {
            BranchError::OrphanBranchRoot {
                source_namespace,
                root,
            } => {
                assert_eq!(source_namespace.as_str(), source);
                assert_eq!(root.branch_id, expected_root.branch_id);
                assert_eq!(root.target_namespace.as_str(), target);
                assert_eq!(
                    root.target_incarnation, expected_root.target_incarnation,
                    "operator repair identity must name the exact missing target lifetime"
                );
                assert_eq!(
                    root, expected_root,
                    "the typed integrity error must carry the exact bounded root proof"
                );
            }
            other => panic!("expected orphan-root integrity error, got {other:?}"),
        },
        other => panic!("expected typed branch integrity error, got {other:?}"),
    }

    let after = branch_control_snapshot(&store, &source)
        .await
        .expect("orphan root must remain authoritative after failed maintenance");
    assert_eq!(
        after.roots, before.roots,
        "maintenance must never auto-release or rewrite an orphan root"
    );

    harness.cleanup().await;
}
