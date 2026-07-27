mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use chrono::{DateTime, Utc};
#[cfg(feature = "branching-test-support")]
use common::fault_injection::pause_first_after_get_matching;
use common::harness::TestHarness;
use common::server::start_test_server;
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::json;
#[cfg(feature = "branching-test-support")]
use zeppelin::config::{BranchingConfig, Config, IndexingConfig};
#[cfg(feature = "branching-test-support")]
use zeppelin::namespace::branching::test_support::{
    activate_fork_for_test, branch_control_snapshot, delete_namespace_for_test,
    publish_deletion_fence, resume_delete_for_test, resume_delete_with_config_and_clock_for_test,
};
#[cfg(feature = "branching-test-support")]
use zeppelin::namespace::branching::NamespaceDeleteOutcome;
#[cfg(feature = "branching-test-support")]
use zeppelin::namespace::manager::NamespaceState;
use zeppelin::namespace::manager::{NamespaceDeletionIntent, NamespaceMetadata};
#[cfg(feature = "branching-test-support")]
use zeppelin::namespace::NamespaceId;
use zeppelin::namespace::{NamespaceIncarnationId, NamespaceManager};
#[cfg(feature = "branching-test-support")]
use zeppelin::security::{DecisionId, PrincipalId};
use zeppelin::time::{Clock, TimeSource};
use zeppelin::types::DistanceMetric;
#[cfg(feature = "branching-test-support")]
use zeppelin::wal::Manifest;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreExtensionDeletionIntent {
    incarnation: NamespaceIncarnationId,
    destruction_record_key: String,
    decision_evidence_ref: String,
    #[serde(default)]
    parent_root: Option<zeppelin::namespace::branching::BranchRoot>,
}

#[cfg(feature = "branching-test-support")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreExtensionDestructionRecord {
    namespace: NamespaceId,
    manifest_version_destroyed: u64,
    object_count: usize,
    byte_count: u64,
    actor: PrincipalId,
    approver: Option<PrincipalId>,
    decision_id: DecisionId,
    #[serde(default)]
    parent_root: Option<zeppelin::namespace::branching::BranchRoot>,
    ts: DateTime<Utc>,
}

#[derive(Debug)]
struct AdjustableWallClock(Mutex<DateTime<Utc>>);

impl AdjustableWallClock {
    fn new(now: DateTime<Utc>) -> Self {
        Self(Mutex::new(now))
    }

    fn jump(&self, delta: chrono::Duration) {
        let mut now = self.0.lock().expect("test wall clock mutex poisoned");
        *now += delta;
    }
}

impl TimeSource for AdjustableWallClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().expect("test wall clock mutex poisoned")
    }
}

fn ns(harness: &TestHarness, suffix: &str) -> String {
    format!("{}-{suffix}", harness.prefix)
}

#[cfg(feature = "branching-test-support")]
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
    config.validate().unwrap();
    assert_eq!(config.gc_horizon_floor_secs(), Some(31));
    config
}

async fn write_deleting_meta(harness: &TestHarness, ns: &str) {
    let now = Utc::now().to_rfc3339();
    let meta = json!({
        "name": ns,
        "dimensions": 2,
        "distance_metric": "cosine",
        "index_type": "ivf_flat",
        "vector_count": 0,
        "created_at": now,
        "updated_at": now,
        "state": "deleting",
        "full_text_search": {}
    });
    harness
        .store
        .put(
            &NamespaceMetadata::s3_key(ns),
            Bytes::from(serde_json::to_vec_pretty(&meta).unwrap()),
        )
        .await
        .unwrap();
}

#[test]
fn pre_extension_reader_rejects_a_fenced_current_deletion_intent() {
    let incarnation: NamespaceIncarnationId =
        serde_json::from_str("\"11111111-2222-4333-8444-555555555555\"").unwrap();
    let mut intent = NamespaceDeletionIntent {
        incarnation: incarnation.clone(),
        destruction_record_key: "_audit/destruction/legacy.json".to_string(),
        decision_evidence_ref: "_audit/destruction/legacy.json".to_string(),
        branch_activation_nonce: None,
        parent_root: None,
        fenced_generation: None,
        visibility: None,
        root_release: None,
    };

    let old_wire_bytes = serde_json::to_vec(&intent).unwrap();
    let decoded: PreExtensionDeletionIntent = serde_json::from_slice(&old_wire_bytes)
        .expect("old wire shape must validate the simulator");
    assert_eq!(decoded.incarnation, incarnation);
    assert_eq!(
        decoded.destruction_record_key,
        intent.destruction_record_key
    );
    assert_eq!(decoded.decision_evidence_ref, intent.decision_evidence_ref);
    assert!(decoded.parent_root.is_none());

    intent.fenced_generation = Some(7);
    let current_wire_bytes = serde_json::to_vec(&intent).unwrap();
    let error = serde_json::from_slice::<PreExtensionDeletionIntent>(&current_wire_bytes)
        .expect_err("an old reader must fail closed on a fenced current intent");
    assert_eq!(error.classify(), serde_json::error::Category::Data);
    assert!(
        error
            .to_string()
            .contains("unknown field `fenced_generation`"),
        "unexpected old-reader diagnostic: {error}"
    );
}

#[cfg(feature = "branching-test-support")]
#[tokio::test]
async fn old_format_governed_intent_and_evidence_resume_to_completion() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "legacy-governed-delete");
    let manager = NamespaceManager::new(harness.store.clone());
    let created = manager
        .create(&name, 2, DistanceMetric::Cosine)
        .await
        .unwrap();
    let incarnation = created
        .incarnation_id
        .as_ref()
        .expect("new namespace must carry an incarnation")
        .to_string();
    let destruction_record_key =
        format!("_audit/destruction/{}.json", incarnation.replace('-', ""));
    let metadata_key = NamespaceMetadata::s3_key(&name);

    harness
        .store
        .put(
            &format!("{name}/wal/left-behind.wal"),
            Bytes::from_static(b"legacy-wal"),
        )
        .await
        .unwrap();

    let (metadata_body, object_metadata) = harness
        .store
        .get_with_object_metadata(&metadata_key)
        .await
        .unwrap();
    let mut metadata: serde_json::Value = serde_json::from_slice(&metadata_body).unwrap();
    let legacy_intent = include_str!("fixtures/namespace_deletion/v1_deletion_intent.json")
        .replace("__INCARNATION__", &incarnation)
        .replace("__DESTRUCTION_RECORD_KEY__", &destruction_record_key);
    let frozen_intent: PreExtensionDeletionIntent = serde_json::from_str(&legacy_intent)
        .expect("legacy intent fixture must match the frozen pre-extension decoder");
    assert_eq!(frozen_intent.incarnation.to_string(), incarnation);
    assert_eq!(frozen_intent.destruction_record_key, destruction_record_key);
    assert_eq!(
        frozen_intent.decision_evidence_ref,
        frozen_intent.destruction_record_key
    );
    assert!(frozen_intent.parent_root.is_none());
    metadata["state"] = json!("deleting");
    metadata["destruction_record_key"] = json!(destruction_record_key.clone());
    metadata["deletion_intent"] = serde_json::from_str(&legacy_intent).unwrap();
    let version = object_metadata
        .version
        .as_ref()
        .expect("real object storage must return a metadata version token");
    harness
        .store
        .put_if_match_with_user_metadata(
            &metadata_key,
            Bytes::from(serde_json::to_vec_pretty(&metadata).unwrap()),
            version,
            &name,
            &object_metadata.user_metadata,
        )
        .await
        .unwrap();

    publish_deletion_fence(harness.store.clone(), &name, &destruction_record_key)
        .await
        .unwrap();
    let fenced_manifest = Manifest::read(&harness.store, &name)
        .await
        .unwrap()
        .expect("legacy fixture requires a fenced manifest");
    let census = harness
        .store
        .list_prefix_meta(&format!("{name}/"))
        .await
        .unwrap();
    let object_count = census.len();
    let byte_count: u64 = census.iter().map(|object| object.size).sum();
    let decision_id = DecisionId::new();
    let legacy_evidence = include_str!("fixtures/namespace_deletion/v1_destruction_record.json")
        .replace("__NAMESPACE__", &name)
        .replace(
            "__MANIFEST_VERSION_DESTROYED__",
            &fenced_manifest.version().to_string(),
        )
        .replace("__OBJECT_COUNT__", &object_count.to_string())
        .replace("__BYTE_COUNT__", &byte_count.to_string())
        .replace("__DECISION_ID__", &decision_id.get().to_string());
    let frozen_evidence: PreExtensionDestructionRecord = serde_json::from_str(&legacy_evidence)
        .expect("legacy evidence fixture must match the frozen pre-extension decoder");
    assert_eq!(frozen_evidence.namespace.as_str(), name);
    assert_eq!(
        frozen_evidence.manifest_version_destroyed,
        fenced_manifest.version()
    );
    assert_eq!(frozen_evidence.object_count, object_count);
    assert_eq!(frozen_evidence.byte_count, byte_count);
    assert_eq!(frozen_evidence.actor.as_str(), "legacy-delete-actor");
    assert!(frozen_evidence.approver.is_none());
    assert_eq!(frozen_evidence.decision_id, decision_id);
    assert!(frozen_evidence.parent_root.is_none());
    assert_eq!(
        frozen_evidence.ts,
        "2026-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
    );
    harness
        .store
        .put(
            &destruction_record_key,
            Bytes::from(legacy_evidence.into_bytes()),
        )
        .await
        .unwrap();
    harness
        .store
        .delete(&Manifest::s3_key(&name))
        .await
        .unwrap();

    let result = resume_delete_for_test(
        harness.store.clone(),
        NamespaceId::new(name.clone()).unwrap(),
        IndexingConfig::default(),
        BranchingConfig {
            enabled: true,
            max_children_per_namespace: 8,
            max_depth: 4,
        },
        Duration::from_secs(30),
    )
    .await;
    let remaining = harness
        .store
        .list_prefix(&format!("{name}/"))
        .await
        .unwrap();

    let _ = harness.store.delete(&destruction_record_key).await;
    let _ = harness.store.delete_prefix(&format!("{name}/")).await;
    harness.cleanup().await;

    assert!(
        result.is_ok(),
        "old-format governed deletion must resume, got {result:?}"
    );
    assert!(
        remaining.is_empty(),
        "old-format governed deletion must remove all namespace keys, got {remaining:?}"
    );
}

#[cfg(feature = "branching-test-support")]
#[tokio::test]
async fn old_format_branch_intent_and_evidence_resume_through_grace() {
    let harness = TestHarness::new().await;
    let source = ns(&harness, "legacy-branch-source");
    let target = ns(&harness, "legacy-branch-target");
    let config = branch_grace_config();
    NamespaceManager::new(harness.store.clone())
        .create(&source, 2, DistanceMetric::Cosine)
        .await
        .unwrap();
    activate_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(target.clone()).unwrap(),
        config.indexing.clone(),
        config.branching.clone(),
    )
    .await
    .unwrap();

    let source_before = branch_control_snapshot(&harness.store, &source)
        .await
        .unwrap();
    assert_eq!(source_before.roots.len(), 1);
    let parent_root = source_before.roots[0].clone();
    let target_manager = NamespaceManager::new(harness.store.clone());
    let target_metadata = target_manager.get(&target).await.unwrap();
    let incarnation = target_metadata
        .incarnation_id
        .as_ref()
        .expect("branch target must carry an incarnation")
        .to_string();
    let destruction_record_key =
        format!("_audit/destruction/{}.json", incarnation.replace('-', ""));
    let metadata_key = NamespaceMetadata::s3_key(&target);
    let sentinel_key = format!("{target}/wal/legacy-left-behind.wal");
    harness
        .store
        .put(&sentinel_key, Bytes::from_static(b"legacy-branch-wal"))
        .await
        .unwrap();

    let (metadata_body, object_metadata) = harness
        .store
        .get_with_object_metadata(&metadata_key)
        .await
        .unwrap();
    let mut metadata: serde_json::Value = serde_json::from_slice(&metadata_body).unwrap();
    let rendered_intent = include_str!("fixtures/namespace_deletion/v1_deletion_intent.json")
        .replace("__INCARNATION__", &incarnation)
        .replace("__DESTRUCTION_RECORD_KEY__", &destruction_record_key);
    let mut old_intent: serde_json::Value = serde_json::from_str(&rendered_intent).unwrap();
    old_intent["parent_root"] = serde_json::to_value(&parent_root).unwrap();
    let frozen_intent: PreExtensionDeletionIntent =
        serde_json::from_value(old_intent.clone()).expect("branch intent must be exact legacy V1");
    assert_eq!(frozen_intent.parent_root.as_ref(), Some(&parent_root));
    assert_eq!(
        frozen_intent.decision_evidence_ref,
        frozen_intent.destruction_record_key
    );
    metadata["state"] = json!("deleting");
    metadata["destruction_record_key"] = json!(destruction_record_key.clone());
    metadata["deletion_intent"] = old_intent;
    let version = object_metadata
        .version
        .as_ref()
        .expect("real object storage must return a metadata version token");
    harness
        .store
        .put_if_match_with_user_metadata(
            &metadata_key,
            Bytes::from(serde_json::to_vec_pretty(&metadata).unwrap()),
            version,
            &target,
            &object_metadata.user_metadata,
        )
        .await
        .unwrap();

    publish_deletion_fence(harness.store.clone(), &target, &destruction_record_key)
        .await
        .unwrap();
    let fenced_manifest = Manifest::read(&harness.store, &target)
        .await
        .unwrap()
        .expect("legacy branch fixture requires a retained fence");
    let census = harness
        .store
        .list_prefix_meta(&format!("{target}/"))
        .await
        .unwrap();
    let object_count = census.len();
    let byte_count: u64 = census.iter().map(|object| object.size).sum();
    let decision_id = DecisionId::new();
    let rendered_evidence = include_str!("fixtures/namespace_deletion/v1_destruction_record.json")
        .replace("__NAMESPACE__", &target)
        .replace(
            "__MANIFEST_VERSION_DESTROYED__",
            &fenced_manifest.version().to_string(),
        )
        .replace("__OBJECT_COUNT__", &object_count.to_string())
        .replace("__BYTE_COUNT__", &byte_count.to_string())
        .replace("__DECISION_ID__", &decision_id.get().to_string());
    let mut old_evidence: serde_json::Value = serde_json::from_str(&rendered_evidence).unwrap();
    old_evidence["parent_root"] = serde_json::to_value(&parent_root).unwrap();
    let frozen_evidence: PreExtensionDestructionRecord =
        serde_json::from_value(old_evidence.clone())
            .expect("branch destruction evidence must be exact legacy V1");
    assert_eq!(frozen_evidence.parent_root.as_ref(), Some(&parent_root));
    harness
        .store
        .put(
            &destruction_record_key,
            Bytes::from(serde_json::to_vec_pretty(&old_evidence).unwrap()),
        )
        .await
        .unwrap();

    let wall_clock = Arc::new(AdjustableWallClock::new(Utc::now()));
    let clock = Clock::from_source(wall_clock.clone());
    let first = resume_delete_with_config_and_clock_for_test(
        harness.store.clone(),
        NamespaceId::new(target.clone()).unwrap(),
        &config,
        clock.clone(),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let not_before = match first {
        NamespaceDeleteOutcome::BranchGraceWait { not_before } => not_before,
        other => panic!("expected legacy branch grace wait, got {other:?}"),
    };

    assert!(Manifest::read(&harness.store, &target)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        branch_control_snapshot(&harness.store, &source)
            .await
            .unwrap()
            .roots,
        source_before.roots
    );
    harness.store.get(&sentinel_key).await.unwrap();
    let deleting = NamespaceManager::new(harness.store.clone())
        .get_including_deleting(&target)
        .await
        .unwrap();
    assert_eq!(deleting.state, NamespaceState::Deleting);
    let migrated = deleting
        .deletion_intent
        .as_ref()
        .expect("legacy branch intent must remain durable through grace");
    assert_eq!(migrated.fenced_generation, Some(fenced_manifest.version()));
    assert_eq!(migrated.parent_root.as_ref(), Some(&parent_root));
    assert_eq!(
        migrated.visibility.as_ref().map(|value| value.not_before),
        Some(not_before)
    );
    assert!(migrated.root_release.is_none());
    harness
        .store
        .get(
            &migrated
                .visibility
                .as_ref()
                .expect("branch visibility must be durable")
                .marker_key,
        )
        .await
        .unwrap();

    let delta = not_before.signed_duration_since(wall_clock.now());
    wall_clock.jump(delta);
    assert_eq!(wall_clock.now(), not_before);
    let second = resume_delete_with_config_and_clock_for_test(
        harness.store.clone(),
        NamespaceId::new(target.clone()).unwrap(),
        &config,
        clock,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(matches!(second, NamespaceDeleteOutcome::Deleted));

    assert!(
        branch_control_snapshot(&harness.store, &source)
            .await
            .unwrap()
            .roots
            .is_empty(),
        "legacy branch resume must release the exact parent root"
    );
    assert!(Manifest::read(&harness.store, &source)
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        NamespaceManager::new(harness.store.clone())
            .get(&source)
            .await
            .unwrap()
            .state,
        NamespaceState::Active
    );
    assert!(harness
        .store
        .list_prefix(&format!("{target}/"))
        .await
        .unwrap()
        .is_empty());
    harness.store.get(&destruction_record_key).await.unwrap();

    let _ = harness.store.delete(&destruction_record_key).await;
    let _ = harness.store.delete_prefix(&format!("{source}/")).await;
    let _ = harness.store.delete_prefix(&format!("{target}/")).await;
    harness.cleanup().await;
}

#[cfg(feature = "branching-test-support")]
#[tokio::test]
async fn graph_delete_retry_converges_when_cleanup_removes_metadata_between_reads() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "delete-retry-metadata-race");
    let manager = NamespaceManager::new(harness.store.clone());
    manager
        .create(&name, 2, DistanceMetric::Cosine)
        .await
        .unwrap();
    harness
        .store
        .put(
            &format!("{name}/wal/concurrent-cleanup.wal"),
            Bytes::from_static(b"concurrent-cleanup"),
        )
        .await
        .unwrap();
    manager.start_delete(&name).await.unwrap();

    let metadata_key = NamespaceMetadata::s3_key(&name);
    let (paused_store, first_metadata_read) =
        pause_first_after_get_matching(&harness.store, metadata_key.clone());
    let request_name = name.clone();
    let mut request_retry = tokio::spawn(async move {
        delete_namespace_for_test(
            paused_store,
            NamespaceId::new(request_name).unwrap(),
            IndexingConfig::default(),
            BranchingConfig {
                enabled: true,
                max_children_per_namespace: 8,
                max_depth: 4,
            },
        )
        .await
    });

    tokio::select! {
        () = first_metadata_read.wait_until_paused() => {}
        outcome = &mut request_retry => {
            panic!("delete retry returned before its Deleting snapshot was paused: {outcome:?}");
        }
    }

    let cleanup = NamespaceManager::new(harness.store.clone())
        .finish_delete(&name, Duration::MAX)
        .await
        .unwrap();
    assert!(cleanup.complete);
    assert!(matches!(
        harness.store.get(&metadata_key).await,
        Err(zeppelin::error::ZeppelinError::NotFound { .. })
    ));

    first_metadata_read.release();
    let outcome = request_retry
        .await
        .expect("delete retry task must not panic")
        .expect("metadata disappearance after a Deleting read must converge");
    assert!(matches!(outcome, NamespaceDeleteOutcome::Deleted));
    assert!(harness
        .store
        .list_prefix(&format!("{name}/"))
        .await
        .unwrap()
        .is_empty());

    harness.cleanup().await;
}

#[tokio::test]
async fn test_delete_resumes_from_deleting_tombstone_with_data_left() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "delete-resumes-tombstone");
    let manager = NamespaceManager::new(harness.store.clone());

    write_deleting_meta(&harness, &name).await;
    harness
        .store
        .put(
            &format!("{name}/wal/left-behind.wal"),
            Bytes::from_static(b"wal"),
        )
        .await
        .unwrap();

    let result = manager.delete(&name).await;
    assert!(
        result.is_ok(),
        "delete retry must be able to resume from a deleting tombstone, got {result:?}"
    );
    let remaining = harness
        .store
        .list_prefix(&format!("{name}/"))
        .await
        .unwrap();
    assert!(
        remaining.is_empty(),
        "completed namespace delete must leave zero keys, got {remaining:?}"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn test_deleting_namespace_rejects_manager_ops_with_410() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "deleting-manager-ops");
    let manager = NamespaceManager::new(harness.store.clone());
    write_deleting_meta(&harness, &name).await;

    let get_result = manager.get(&name).await;
    assert_eq!(
        get_result.unwrap_err().status_code(),
        410,
        "get on a deleting namespace must surface 410 Gone"
    );

    let create_result = manager.create(&name, 2, DistanceMetric::Cosine).await;
    assert_eq!(
        create_result.unwrap_err().status_code(),
        410,
        "create during delete must not reopen the Bug-37 zombie window"
    );

    let _ = harness.store.delete_prefix(&format!("{name}/")).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_cross_node_registry_delete_converges_within_ttl() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "registry-converges");
    let manager_a = NamespaceManager::new(harness.store.clone());
    let manager_b =
        NamespaceManager::new_with_registry_ttl(harness.store.clone(), Duration::from_millis(100));

    manager_a
        .create(&name, 2, DistanceMetric::Cosine)
        .await
        .unwrap();
    manager_b.get(&name).await.unwrap();
    manager_a.delete(&name).await.unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match manager_b.get(&name).await {
            Err(err) if err.status_code() == 404 || err.status_code() == 410 => break,
            Ok(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await
            }
            Ok(meta) => panic!("cached namespace did not expire within one registry TTL: {meta:?}"),
            Err(err) => panic!("unexpected registry convergence error: {err:?}"),
        }
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn test_registry_ttl_is_not_extended_by_backward_frozen_wall_clock() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "registry-monotonic-clock-domain");
    let manager_a = NamespaceManager::new(harness.store.clone());
    let wall_clock = Arc::new(AdjustableWallClock::new(Utc::now()));
    let manager_b = NamespaceManager::with_clock(
        harness.store.clone(),
        Duration::from_millis(50),
        Clock::from_source(wall_clock.clone()),
    );

    manager_a
        .create(&name, 2, DistanceMetric::Cosine)
        .await
        .unwrap();
    manager_b.get(&name).await.unwrap();

    wall_clock.jump(chrono::Duration::minutes(-5));
    manager_a.start_delete(&name).await.unwrap();
    tokio::time::sleep(Duration::from_millis(75)).await;

    let result = manager_b.get(&name).await;
    assert!(
        matches!(result, Err(ref error) if error.status_code() == 410),
        "expired local registry entry hid the authoritative tombstone: {result:?}"
    );

    manager_a.finish_delete(&name, Duration::MAX).await.unwrap();
    harness.cleanup().await;
}

#[tokio::test]
async fn test_create_same_name_blocked_until_deleting_tombstone_removed() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "create-blocked-delete");
    let manager = NamespaceManager::new(harness.store.clone());

    manager
        .create(&name, 2, DistanceMetric::Cosine)
        .await
        .unwrap();
    harness
        .store
        .put(&format!("{name}/wal/old.wal"), Bytes::from_static(b"old"))
        .await
        .unwrap();

    manager.start_delete(&name).await.unwrap();
    let create_while_deleting = manager.create(&name, 2, DistanceMetric::Cosine).await;
    assert_eq!(
        create_while_deleting.unwrap_err().status_code(),
        410,
        "meta.json tombstone must block recreation while old data remains"
    );

    let outcome = manager.finish_delete(&name, Duration::MAX).await.unwrap();
    assert!(outcome.complete);
    assert!(harness
        .store
        .list_prefix(&format!("{name}/"))
        .await
        .unwrap()
        .is_empty());

    manager
        .create(&name, 2, DistanceMetric::Cosine)
        .await
        .unwrap();
    let keys = harness
        .store
        .list_prefix(&format!("{name}/"))
        .await
        .unwrap();
    assert!(
        keys.iter().any(|key| key.ends_with("/meta.json")),
        "fresh namespace must have meta.json: {keys:?}"
    );
    assert!(
        keys.iter().any(|key| key.ends_with("/manifest.json")),
        "fresh namespace must have manifest.json: {keys:?}"
    );
    assert!(
        keys.iter().all(|key| !key.ends_with(".wal")),
        "fresh namespace must not inherit old WAL data: {keys:?}"
    );

    let _ = harness.store.delete_prefix(&format!("{name}/")).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_api_deleting_namespace_status_and_ops_410() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let name = format!("{}-api-deleting", harness.prefix);

    write_deleting_meta(&harness, &name).await;

    let get_resp = client
        .get(format!("{base_url}/v1/namespaces/{name}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);
    let body: serde_json::Value = get_resp.json().await.unwrap();
    assert_eq!(body["state"], "deleting");

    let upsert_resp = client
        .post(format!("{base_url}/v1/namespaces/{name}/vectors"))
        .json(&json!({
            "vectors": [
                {"id": "a", "values": [1.0, 0.0], "attributes": {"tenant": "red"}}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(upsert_resp.status(), StatusCode::GONE);

    let query_resp = client
        .post(format!("{base_url}/v1/namespaces/{name}/query"))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(query_resp.status(), StatusCode::GONE);

    let _ = harness.store.delete_prefix(&format!("{name}/")).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_api_delete_returns_202_and_eventually_leaves_zero_keys() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);

    let create_resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&json!({"dimensions": 2}))
        .send()
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let created: serde_json::Value = create_resp.json().await.unwrap();
    let name = created["name"].as_str().unwrap().to_string();

    client
        .post(format!("{base_url}/v1/namespaces/{name}/vectors"))
        .json(&json!({
            "vectors": [
                {"id": "a", "values": [1.0, 0.0], "attributes": {"tenant": "red"}}
            ]
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let delete_resp = client
        .delete(format!("{base_url}/v1/namespaces/{name}"))
        .send()
        .await
        .unwrap();
    assert_eq!(delete_resp.status(), StatusCode::ACCEPTED);
    let body: serde_json::Value = delete_resp.json().await.unwrap();
    assert_eq!(body["state"], "deleting");

    for _ in 0..50 {
        let get_resp = client
            .get(format!("{base_url}/v1/namespaces/{name}"))
            .send()
            .await
            .unwrap();
        match get_resp.status() {
            StatusCode::OK => {
                let body: serde_json::Value = get_resp.json().await.unwrap();
                assert_eq!(body["state"], "deleting");
            }
            StatusCode::NOT_FOUND => break,
            other => panic!("unexpected namespace status while deleting: {other}"),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let keys = harness
        .store
        .list_prefix(&format!("{name}/"))
        .await
        .unwrap();
    assert!(keys.is_empty(), "delete must leave zero keys, got {keys:?}");
    harness.cleanup().await;
}
