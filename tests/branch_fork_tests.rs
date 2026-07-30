#![cfg(feature = "branching-test-support")]

mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::fault_injection::{
    fail_delete_once_matching, fail_put_once_matching, pause_first_after_get_matching,
    pause_first_create_matching, pause_next_cas_matching,
};
use common::harness::TestHarness;
use common::server::scoped_test_security_store;
use common::vectors::random_vectors;
use zeppelin::compaction::{CompactionResult, Compactor};
use zeppelin::config::{BranchingConfig, CompactionConfig, IndexingConfig};
use zeppelin::embedding::{InputModality, LateInteractionNamespaceConfig};
use zeppelin::error::ZeppelinError;
use zeppelin::fts::FtsFieldConfig;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::namespace::branching::test_support::{
    activate_fork_for_test, branch_control_snapshot, branch_metadata_snapshot,
    delete_namespace_for_test, list_children_for_test, maintain_branches_for_test,
    prepare_fork_for_test, prepare_fork_until_activation_pending_for_test,
    prepare_fork_until_reserved_for_test, prepare_fork_until_root_for_test,
    prepared_manifest_snapshot, publish_deletion_fence, resume_delete_for_test,
};
use zeppelin::namespace::branching::{
    BranchError, BranchLifecycleState, BranchPrepareStage, NamespaceDeleteOutcome,
    PrepareForkOutcome,
};
use zeppelin::namespace::manager::{
    CompactionStatus, NamespaceIndexConfig, NamespaceMetadata, NamespaceState,
};
use zeppelin::namespace::{NamespaceId, NamespaceManager};
use zeppelin::types::{DistanceMetric, IndexType, VectorEntry};
use zeppelin::wal::{LeaseManager, Manifest, WalReader, WalWriter};

fn fork_indexing() -> IndexingConfig {
    IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 10,
        ..IndexingConfig::default()
    }
}

fn fork_limits() -> BranchingConfig {
    BranchingConfig {
        enabled: true,
        max_children_per_namespace: 8,
        max_depth: 4,
    }
}

fn test_compactor(store: &zeppelin::storage::ZeppelinStore) -> Compactor {
    Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        CompactionConfig {
            max_old_segments: 0,
            retrain_imbalance_threshold: 0.0,
            ..CompactionConfig::default()
        },
        fork_indexing(),
        common::default_gc_upload_window(),
    )
}

#[tokio::test]
async fn activated_foreign_branch_compaction_materializes_target_owned_segment() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("materialize-source");
    let target = harness.artifact_origin_namespace("materialize-target");
    let (_vectors, _) = compact_source(&harness, &source).await;
    activate_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();

    test_compactor(&harness.store)
        .compact(&target)
        .await
        .unwrap();
    let manifest = Manifest::read(&harness.store, &target)
        .await
        .unwrap()
        .unwrap();
    let active = manifest.active_segment.as_ref().unwrap();
    let segment = manifest
        .segments
        .iter()
        .find(|item| &item.id == active)
        .unwrap();
    assert!(segment.artifact_origin.is_some());
    assert!(manifest.visible_refs_are_local().unwrap());
    assert!(Manifest::read(&harness.store, &source)
        .await
        .unwrap()
        .is_some());

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn graph_delete_rejects_a_source_with_a_live_child_root() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("delete-source");
    let target = harness.artifact_origin_namespace("delete-target");
    NamespaceManager::new(harness.store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .unwrap();
    prepare_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();

    let error = delete_namespace_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        ZeppelinError::Branch(inner)
            if matches!(*inner, BranchError::NamespaceHasLiveBranches { .. })
    ));
    assert_eq!(
        NamespaceManager::new(harness.store.clone())
            .get(&source)
            .await
            .unwrap()
            .name,
        source
    );

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn ungoverned_manager_delete_rejects_a_source_with_a_live_child_root() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("manager-delete-source");
    let target = harness.artifact_origin_namespace("manager-delete-target");
    NamespaceManager::new(harness.store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .unwrap();
    prepare_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();
    let before = branch_control_snapshot(&harness.store, &source)
        .await
        .unwrap();
    let error = NamespaceManager::new(harness.store.clone())
        .delete(&source)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ZeppelinError::Branch(inner)
            if matches!(*inner, BranchError::NamespaceHasLiveBranches { .. })
    ));
    assert_eq!(
        NamespaceManager::new(harness.store.clone())
            .get(&source)
            .await
            .unwrap()
            .state,
        NamespaceState::Active
    );
    let after = branch_control_snapshot(&harness.store, &source)
        .await
        .unwrap();
    assert_eq!(after.roots, before.roots);
    assert_eq!(after.manifest_generation, before.manifest_generation);
    assert_eq!(after.deletion_fenced, before.deletion_fenced);
    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn graph_resume_completes_an_ordinary_deleting_namespace() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("resume-ordinary");
    NamespaceManager::new(harness.store.clone())
        .create(&namespace, 4, DistanceMetric::Cosine)
        .await
        .unwrap();
    NamespaceManager::new(harness.store.clone())
        .start_delete(&namespace)
        .await
        .unwrap();
    let outcome = resume_delete_for_test(
        harness.store.clone(),
        NamespaceId::new(namespace.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        zeppelin::namespace::branching::NamespaceDeleteOutcome::Deleted
    ));
    assert!(NamespaceManager::new(harness.store.clone())
        .get(&namespace)
        .await
        .is_err());
    harness.cleanup_artifact_origin_namespace(&namespace).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn graph_delete_retains_branch_root_until_grace_resume() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("drop-source");
    let target = harness.artifact_origin_namespace("drop-target");
    NamespaceManager::new(harness.store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .unwrap();
    activate_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();
    let outcome = delete_namespace_for_test(
        harness.store.clone(),
        NamespaceId::new(target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        zeppelin::namespace::branching::NamespaceDeleteOutcome::BranchGraceWait { .. }
    ));
    assert!(!branch_control_snapshot(&harness.store, &source)
        .await
        .unwrap()
        .roots
        .is_empty());

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn graph_lists_direct_children_in_target_order() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("list-source");
    let target_b = harness.artifact_origin_namespace("list-target-b");
    let target_a = harness.artifact_origin_namespace("list-target-a");
    NamespaceManager::new(harness.store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .unwrap();
    for target in [&target_b, &target_a] {
        prepare_fork_for_test(
            harness.store.clone(),
            NamespaceId::new(source.clone()).unwrap(),
            NamespaceId::new((*target).clone()).unwrap(),
            fork_indexing(),
            fork_limits(),
        )
        .await
        .unwrap();
    }
    let children = list_children_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();
    let names = children
        .iter()
        .map(|child| child.target.to_string())
        .collect::<Vec<_>>();
    assert_eq!(names, vec![target_a.clone(), target_b.clone()]);
    assert!(children
        .iter()
        .all(|child| child.state == BranchLifecycleState::Preparing));

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target_a).await;
    harness.cleanup_artifact_origin_namespace(&target_b).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn nested_branching_tracks_depth_and_enforces_limit() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("nested-source");
    let child = harness.artifact_origin_namespace("nested-child");
    let grandchild = harness.artifact_origin_namespace("nested-grandchild");
    let over_limit = harness.artifact_origin_namespace("nested-over-limit");
    NamespaceManager::new(harness.store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .unwrap();
    activate_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(child.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();
    let nested = activate_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(child.clone()).unwrap(),
        NamespaceId::new(grandchild.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();
    let depth = match nested {
        PrepareForkOutcome::Prepared(branch) | PrepareForkOutcome::ExistingPrepared(branch) => {
            branch.identity.depth
        }
    };
    assert_eq!(depth, 2);

    let mut limited = fork_limits();
    limited.max_depth = 2;
    let error = prepare_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(grandchild.clone()).unwrap(),
        NamespaceId::new(over_limit.clone()).unwrap(),
        fork_indexing(),
        limited,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        ZeppelinError::Branch(inner)
            if matches!(*inner, BranchError::BranchDepthExceeded { .. })
    ));

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&child).await;
    harness.cleanup_artifact_origin_namespace(&grandchild).await;
    harness.cleanup_artifact_origin_namespace(&over_limit).await;
    harness.cleanup().await;
}

async fn compact_source(
    harness: &TestHarness,
    source: &str,
) -> (Vec<VectorEntry>, CompactionResult) {
    NamespaceManager::new(harness.store.clone())
        .create(source, 4, DistanceMetric::Cosine)
        .await
        .unwrap();
    let vectors = random_vectors(24, 4);
    WalWriter::new(harness.store.clone())
        .append(source, vectors.clone(), Vec::new())
        .await
        .unwrap();
    let compacted = test_compactor(&harness.store)
        .compact(source)
        .await
        .unwrap();
    (vectors, compacted)
}

async fn raw_namespace_metadata(harness: &TestHarness, namespace: &str) -> NamespaceMetadata {
    raw_namespace_metadata_from_store(&harness.store, namespace).await
}

async fn raw_namespace_metadata_from_store(
    store: &zeppelin::storage::ZeppelinStore,
    namespace: &str,
) -> NamespaceMetadata {
    let bytes = store
        .get(&NamespaceMetadata::s3_key(namespace))
        .await
        .unwrap();
    NamespaceMetadata::from_bytes(&bytes).unwrap()
}

fn branch_error(error: ZeppelinError) -> BranchError {
    match error {
        ZeppelinError::Branch(error) => *error,
        other => panic!("expected a typed branch error, got {other}"),
    }
}

#[tokio::test]
async fn compacted_source_prepares_a_non_visible_zero_copy_branch() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("branch-fork-prepare-source");
    let target = harness.artifact_origin_namespace("branch-fork-prepare-target");
    let (_vectors, compacted) = compact_source(&harness, &source).await;
    assert_eq!(compacted.vectors_compacted, 24);
    WalWriter::new(harness.store.clone())
        .append(
            &source,
            vec![VectorEntry {
                id: "uncompacted-tail".to_string(),
                values: vec![0.4, 0.3, 0.2, 0.1],
                attributes: None,
            }],
            Vec::new(),
        )
        .await
        .unwrap();

    let outcome = prepare_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();
    let prepared = match outcome {
        PrepareForkOutcome::Prepared(prepared) => prepared,
        PrepareForkOutcome::ExistingPrepared(_) => {
            panic!("the first prepare call must perform the protocol")
        }
    };

    let metadata = branch_metadata_snapshot(&harness.store, &target)
        .await
        .unwrap();
    assert_eq!(metadata.state, "creating");
    assert_eq!(
        metadata.prepare_stage,
        Some(BranchPrepareStage::ManifestPublished)
    );
    assert_eq!(metadata.branch_identity.as_ref(), Some(&prepared.identity));
    assert!(
        NamespaceManager::new(harness.store.clone())
            .get(&target)
            .await
            .is_err(),
        "a prepared branch must remain outside the active namespace lifecycle"
    );

    let target_manifest = prepared_manifest_snapshot(&harness.store, &target)
        .await
        .unwrap();
    assert_eq!(target_manifest.generation, 1);
    assert_eq!(target_manifest.lineage, prepared.lineage);
    assert_eq!(target_manifest.target_namespace, target);
    assert_eq!(
        target_manifest.target_incarnation,
        prepared.identity.target_incarnation
    );
    assert_eq!(target_manifest.segment_origins.len(), 1);
    assert_eq!(
        target_manifest.segment_origins[0].namespace.as_str(),
        source
    );
    assert_eq!(target_manifest.fragment_origins.len(), 1);
    assert_eq!(
        target_manifest.fragment_origins[0].namespace.as_str(),
        source
    );

    let source_control = branch_control_snapshot(&harness.store, &source)
        .await
        .unwrap();
    assert_eq!(source_control.roots, vec![prepared.root.clone()]);

    let target_objects = harness
        .store
        .list_prefix(&format!("{target}/"))
        .await
        .unwrap();
    assert!(
        target_objects
            .iter()
            .all(|key| !key.contains("/wal/") && !key.contains("/segments/")),
        "zero-copy preparation must not materialize inherited artifacts under the target"
    );

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn empty_source_prepares_an_empty_view_and_retry_is_byte_exact() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("branch-fork-empty-source");
    let target = harness.artifact_origin_namespace("branch-fork-empty-target");
    NamespaceManager::new(harness.store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .unwrap();

    let first = prepare_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();
    let first = match first {
        PrepareForkOutcome::Prepared(prepared) => prepared,
        PrepareForkOutcome::ExistingPrepared(_) => {
            panic!("the first empty-source prepare must advance durable state")
        }
    };
    let live_before = harness.store.get(&Manifest::s3_key(&target)).await.unwrap();
    let history_before = harness
        .store
        .get(&Manifest::history_key(&target, 1))
        .await
        .unwrap();
    let source_control_before = branch_control_snapshot(&harness.store, &source)
        .await
        .unwrap();

    let target_manifest = Manifest::read(&harness.store, &target)
        .await
        .unwrap()
        .expect("prepare must publish target generation one");
    assert_eq!(target_manifest.version(), 1);
    assert!(target_manifest.fragments.is_empty());
    assert!(target_manifest.segments.is_empty());
    assert!(target_manifest.artifact_origins.is_empty());
    assert!(target_manifest.active_segment.is_none());

    let retried = prepare_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();
    match retried {
        PrepareForkOutcome::ExistingPrepared(existing) => assert_eq!(existing, first),
        PrepareForkOutcome::Prepared(_) => {
            panic!("a fully prepared retry must report ExistingPrepared")
        }
    }

    assert_eq!(
        harness.store.get(&Manifest::s3_key(&target)).await.unwrap(),
        live_before
    );
    assert_eq!(
        harness
            .store
            .get(&Manifest::history_key(&target, 1))
            .await
            .unwrap(),
        history_before
    );
    assert!(!harness
        .store
        .exists(&Manifest::history_key(&target, 2))
        .await
        .unwrap());
    let source_control_after = branch_control_snapshot(&harness.store, &source)
        .await
        .unwrap();
    assert_eq!(
        source_control_after.manifest_generation,
        source_control_before.manifest_generation
    );
    assert_eq!(source_control_after.roots, source_control_before.roots);

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn prepare_rejects_same_name_depth_and_conflicting_target_intents() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("branch-fork-collision-source");
    let other_source = harness.artifact_origin_namespace("branch-fork-collision-other-source");
    let active_target = harness.artifact_origin_namespace("branch-fork-active-target");
    let prepared_target = harness.artifact_origin_namespace("branch-fork-prepared-target");
    let depth_target = harness.artifact_origin_namespace("branch-fork-depth-target");
    let manager = NamespaceManager::new(harness.store.clone());
    for namespace in [&source, &other_source, &active_target] {
        manager
            .create(namespace, 4, DistanceMetric::Cosine)
            .await
            .unwrap();
    }

    let same_name = prepare_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(source.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        branch_error(same_name),
        BranchError::IntentMismatch { target } if target.as_str() == source
    ));

    let active_collision = prepare_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(active_target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        branch_error(active_collision),
        BranchError::TargetAlreadyExists { target } if target.as_str() == active_target
    ));

    prepare_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(prepared_target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();
    let different_parent = prepare_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(other_source.clone()).unwrap(),
        NamespaceId::new(prepared_target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        branch_error(different_parent),
        BranchError::IntentMismatch { target } if target.as_str() == prepared_target
    ));

    let mut no_depth = fork_limits();
    no_depth.max_depth = 0;
    let depth_error = prepare_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(depth_target.clone()).unwrap(),
        fork_indexing(),
        no_depth,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        branch_error(depth_error),
        BranchError::BranchDepthExceeded { depth: 1, limit: 0 }
    ));
    assert!(!harness
        .store
        .exists(&NamespaceMetadata::s3_key(&depth_target))
        .await
        .unwrap());
    assert!(NamespaceId::new("invalid@branch-target".to_string()).is_err());

    for namespace in [
        &source,
        &other_source,
        &active_target,
        &prepared_target,
        &depth_target,
    ] {
        harness.cleanup_artifact_origin_namespace(namespace).await;
    }
    harness.cleanup().await;
}

#[tokio::test]
async fn prepare_copies_data_plane_config_and_resets_operational_metadata() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("branch-fork-metadata-source");
    let target = harness.artifact_origin_namespace("branch-fork-metadata-target");
    let mut full_text_search = HashMap::new();
    full_text_search.insert(
        "body".to_string(),
        FtsFieldConfig {
            stemming: false,
            case_sensitive: true,
            max_token_length: 17,
            ..FtsFieldConfig::default()
        },
    );
    let source_indexing = IndexingConfig {
        default_num_centroids: 3,
        quantization: QuantizationType::Scalar,
        pq_m: 2,
        hierarchical: false,
        fts_index: true,
        bitmap_index: true,
        ..IndexingConfig::default()
    };
    let source_index_config = NamespaceIndexConfig::from_indexing_config(&source_indexing);
    let manager = NamespaceManager::new(harness.store.clone());
    manager
        .create_with_fts_and_index_config(
            &source,
            4,
            DistanceMetric::Euclidean,
            full_text_search.clone(),
            Some(source_index_config.clone()),
        )
        .await
        .unwrap();
    manager
        .record_compaction_failure(
            &source,
            &ZeppelinError::Validation("source-health-sentinel".to_string()),
        )
        .await
        .unwrap();
    let source_before = manager.get(&source).await.unwrap();
    assert_eq!(source_before.compaction_health.consecutive_failures, 1);

    let prepared = prepare_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();
    let prepared = match prepared {
        PrepareForkOutcome::Prepared(prepared) => prepared,
        PrepareForkOutcome::ExistingPrepared(_) => {
            panic!("the first metadata fork must advance durable state")
        }
    };

    let target_metadata = raw_namespace_metadata(&harness, &target).await;
    assert_eq!(target_metadata.state, NamespaceState::Creating);
    assert_eq!(target_metadata.dimensions, source_before.dimensions);
    assert_eq!(
        target_metadata.distance_metric,
        source_before.distance_metric
    );
    assert_eq!(target_metadata.index_type, source_before.index_type);
    assert_eq!(
        serde_json::to_value(&target_metadata.full_text_search).unwrap(),
        serde_json::to_value(&source_before.full_text_search).unwrap()
    );
    assert_eq!(
        target_metadata.index_config.as_ref(),
        Some(&source_index_config)
    );
    assert_eq!(target_metadata.vector_count, 0);
    assert_eq!(target_metadata.created_at, prepared.identity.created_at);
    assert!(target_metadata.updated_at >= target_metadata.created_at);
    assert_eq!(
        target_metadata.compaction_health.last_compaction_status,
        CompactionStatus::Never
    );
    assert!(target_metadata
        .compaction_health
        .last_compaction_at
        .is_none());
    assert!(target_metadata
        .compaction_health
        .last_compaction_error
        .is_none());
    assert_eq!(target_metadata.compaction_health.consecutive_failures, 0);
    assert!(target_metadata.destruction_record_key.is_none());

    let target_manifest = Manifest::read(&harness.store, &target)
        .await
        .unwrap()
        .expect("prepared metadata target must have generation one");
    assert_eq!(target_manifest.version(), 1);
    assert_eq!(target_manifest.fencing_token(), 0);
    assert!(target_manifest.pending_deletes.is_empty());
    assert!(target_manifest.compaction_watermark.is_none());

    let source_after = manager.get(&source).await.unwrap();
    assert_eq!(
        source_after.index_config.as_ref(),
        Some(&source_index_config)
    );
    assert_eq!(source_after.compaction_health.consecutive_failures, 1);
    assert_eq!(
        source_after.compaction_health.last_compaction_status,
        CompactionStatus::Failure
    );
    assert_eq!(
        serde_json::to_value(&source_after.full_text_search).unwrap(),
        serde_json::to_value(&full_text_search).unwrap()
    );

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn late_namespace_activates_zero_copy_without_dense_index_config() {
    let harness = TestHarness::new().await;
    let first_source = harness.artifact_origin_namespace("late-branch-source-text");
    let first_target = harness.artifact_origin_namespace("late-branch-target-text");
    let second_source = harness.artifact_origin_namespace("late-branch-source-image");
    let second_target = harness.artifact_origin_namespace("late-branch-target-image");
    let text_config = LateInteractionNamespaceConfig {
        accepted_modalities: vec![InputModality::Text],
    };
    let image_config = LateInteractionNamespaceConfig {
        accepted_modalities: vec![InputModality::Image],
    };
    let manager = NamespaceManager::new(harness.store.clone());
    for (namespace, config) in [
        (&first_source, text_config.clone()),
        (&second_source, image_config.clone()),
    ] {
        manager
            .create_typed_with_fts_and_index_config(
                namespace,
                0,
                DistanceMetric::DotProduct,
                IndexType::LateInteractionFde,
                Some(config),
                HashMap::new(),
                None,
            )
            .await
            .unwrap();
    }

    let first = activate_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(first_source.clone()).unwrap(),
        NamespaceId::new(first_target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();
    let second = activate_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(second_source.clone()).unwrap(),
        NamespaceId::new(second_target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();
    let first = match first {
        PrepareForkOutcome::Prepared(prepared) | PrepareForkOutcome::ExistingPrepared(prepared) => {
            prepared
        }
    };
    let second = match second {
        PrepareForkOutcome::Prepared(prepared) | PrepareForkOutcome::ExistingPrepared(prepared) => {
            prepared
        }
    };

    assert_ne!(
        first.identity.source_config_sha256, second.identity.source_config_sha256,
        "the frozen source proof must bind the late-interaction admission config"
    );
    for (target, expected_config) in [
        (&first_target, &text_config),
        (&second_target, &image_config),
    ] {
        let metadata = manager.get(target).await.unwrap();
        assert_eq!(metadata.state, NamespaceState::Active);
        assert_eq!(metadata.dimensions, 0);
        assert_eq!(metadata.index_type, IndexType::LateInteractionFde);
        assert_eq!(metadata.late_interaction.as_ref(), Some(expected_config));
        assert!(
            metadata.index_config.is_none(),
            "late branches must not synthesize dense index configuration"
        );
        let objects = harness
            .store
            .list_prefix(&format!("{target}/"))
            .await
            .unwrap();
        assert!(
            objects
                .iter()
                .all(|key| !key.contains("/wal/") && !key.contains("/segments/")),
            "late branch activation must remain zero-copy"
        );
    }

    for namespace in [&first_source, &first_target, &second_source, &second_target] {
        harness.cleanup_artifact_origin_namespace(namespace).await;
    }
    harness.cleanup().await;
}

#[tokio::test]
async fn maintenance_leaves_an_unrooted_reservation_for_authenticated_retry() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let source = harness.artifact_origin_namespace("branch-maintain-reserved-source");
    let target = harness.artifact_origin_namespace("branch-maintain-reserved-target");
    NamespaceManager::new(store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .unwrap();

    prepare_fork_until_reserved_for_test(
        store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();
    let reserved_before = branch_metadata_snapshot(&store, &target).await.unwrap();

    let report = maintain_branches_for_test(
        store.clone(),
        fork_indexing(),
        fork_limits(),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(report.awaiting_authenticated_retry >= 1);
    assert!(branch_control_snapshot(&store, &source)
        .await
        .unwrap()
        .roots
        .is_empty());
    let reserved_after = branch_metadata_snapshot(&store, &target).await.unwrap();
    assert_eq!(
        reserved_after.prepare_stage,
        Some(BranchPrepareStage::Reserved)
    );
    assert_eq!(reserved_after.reservation, reserved_before.reservation);
    assert!(reserved_after.branch_identity.is_none());
    assert!(Manifest::read(&store, &target).await.unwrap().is_none());

    let retry = prepare_fork_for_test(
        store,
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();
    let prepared = match retry {
        PrepareForkOutcome::Prepared(prepared) | PrepareForkOutcome::ExistingPrepared(prepared) => {
            prepared
        }
    };
    assert_eq!(
        prepared.identity.branch_id,
        reserved_before.reservation.branch_id
    );

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn fenced_source_reservation_waits_for_authorized_cancellation() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let source = harness.artifact_origin_namespace("branch-maintain-fenced-source");
    let target = harness.artifact_origin_namespace("branch-maintain-fenced-target");
    NamespaceManager::new(store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .unwrap();
    prepare_fork_until_reserved_for_test(
        store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();
    let reserved = branch_metadata_snapshot(&store, &target).await.unwrap();
    publish_deletion_fence(
        store.clone(),
        &source,
        "_audit/destruction/0123456789abcdef0123456789abcdef.json",
    )
    .await
    .unwrap();

    let report = maintain_branches_for_test(
        store.clone(),
        fork_indexing(),
        fork_limits(),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(report.awaiting_authorized_cancellation >= 1);
    assert!(branch_control_snapshot(&store, &source)
        .await
        .unwrap()
        .roots
        .is_empty());
    assert!(Manifest::read(&store, &target).await.unwrap().is_none());

    let retry = prepare_fork_for_test(
        store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        branch_error(retry),
        BranchError::SourceDeleting { namespace } if namespace.as_str() == source
    ));
    let after = branch_metadata_snapshot(&store, &target).await.unwrap();
    assert_eq!(after.reservation, reserved.reservation);
    assert_eq!(after.prepare_stage, Some(BranchPrepareStage::Reserved));
    assert!(after.branch_identity.is_none());
    assert!(Manifest::read(&store, &target).await.unwrap().is_none());

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn retry_after_root_publication_reuses_the_same_branch_and_generation() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("branch-fork-root-crash-source");
    let target = harness.artifact_origin_namespace("branch-fork-root-crash-target");
    compact_source(&harness, &source).await;

    prepare_fork_until_root_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();

    let reserved = branch_metadata_snapshot(&harness.store, &target)
        .await
        .unwrap();
    assert_eq!(reserved.prepare_stage, Some(BranchPrepareStage::Reserved));
    assert!(reserved.branch_identity.is_none());
    let rooted = branch_control_snapshot(&harness.store, &source)
        .await
        .unwrap()
        .roots
        .into_iter()
        .find(|root| root.branch_id == reserved.reservation.branch_id)
        .expect("the simulated crash must happen after the exact root CAS");
    assert!(Manifest::read(&harness.store, &target)
        .await
        .unwrap()
        .is_none());

    assert!(
        NamespaceManager::new(harness.store.clone())
            .get(&target)
            .await
            .is_err(),
        "generic namespace recovery must not activate or bootstrap a creating fork"
    );
    let still_reserved = branch_metadata_snapshot(&harness.store, &target)
        .await
        .unwrap();
    assert_eq!(
        still_reserved.prepare_stage,
        Some(BranchPrepareStage::Reserved)
    );
    assert!(still_reserved.branch_identity.is_none());
    assert!(Manifest::read(&harness.store, &target)
        .await
        .unwrap()
        .is_none());

    WalWriter::new(harness.store.clone())
        .append(
            &source,
            vec![VectorEntry {
                id: "source-advanced-after-root".to_string(),
                values: vec![0.1, 0.2, 0.3, 0.4],
                attributes: None,
            }],
            Vec::new(),
        )
        .await
        .unwrap();

    let retried = prepare_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();
    let prepared = match retried {
        PrepareForkOutcome::Prepared(prepared) | PrepareForkOutcome::ExistingPrepared(prepared) => {
            prepared
        }
    };
    assert_eq!(prepared.identity.branch_id, reserved.reservation.branch_id);
    assert_eq!(
        prepared.identity.source_generation,
        rooted.source_generation
    );
    assert_eq!(prepared.root, rooted);
    assert_eq!(
        branch_metadata_snapshot(&harness.store, &target)
            .await
            .unwrap()
            .prepare_stage,
        Some(BranchPrepareStage::ManifestPublished)
    );

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[derive(Clone, Copy, Debug)]
enum NeverActiveStage {
    ReservedNoRoot,
    ReservedWithRoot,
    Rooted,
    ManifestPublished,
}

impl NeverActiveStage {
    fn label(self) -> &'static str {
        match self {
            Self::ReservedNoRoot => "reserved-no-root",
            Self::ReservedWithRoot => "reserved-with-root",
            Self::Rooted => "rooted",
            Self::ManifestPublished => "manifest-published",
        }
    }

    fn durable_stage(self) -> BranchPrepareStage {
        match self {
            Self::ReservedNoRoot | Self::ReservedWithRoot => BranchPrepareStage::Reserved,
            Self::Rooted => BranchPrepareStage::Rooted,
            Self::ManifestPublished => BranchPrepareStage::ManifestPublished,
        }
    }

    fn has_parent_root(self) -> bool {
        !matches!(self, Self::ReservedNoRoot)
    }
}

async fn prepare_never_active_stage(
    store: zeppelin::storage::ZeppelinStore,
    source: &str,
    target: &str,
    stage: NeverActiveStage,
) {
    let source = NamespaceId::new(source.to_string()).expect("source namespace must be valid");
    let target_id = NamespaceId::new(target.to_string()).expect("target namespace must be valid");
    match stage {
        NeverActiveStage::ReservedNoRoot => {
            prepare_fork_until_reserved_for_test(
                store.clone(),
                source.clone(),
                target_id.clone(),
                fork_indexing(),
                fork_limits(),
            )
            .await
            .expect("fixture must stop after target reservation");
        }
        NeverActiveStage::ReservedWithRoot => {
            prepare_fork_until_root_for_test(
                store.clone(),
                source.clone(),
                target_id.clone(),
                fork_indexing(),
                fork_limits(),
            )
            .await
            .expect("fixture must stop after the parent-root CAS");
        }
        NeverActiveStage::Rooted => {
            let (faulted_store, manifest_failure) =
                fail_put_once_matching(&store, Manifest::s3_key(target));
            prepare_fork_for_test(
                faulted_store,
                source.clone(),
                target_id.clone(),
                fork_indexing(),
                fork_limits(),
            )
            .await
            .expect_err("fixture must stop after Rooted metadata and before live publication");
            assert_eq!(manifest_failure.failures_injected(), 1);
        }
        NeverActiveStage::ManifestPublished => {
            prepare_fork_for_test(
                store.clone(),
                source.clone(),
                target_id.clone(),
                fork_indexing(),
                fork_limits(),
            )
            .await
            .expect("fixture must publish a never-active target manifest");
        }
    }

    let metadata = branch_metadata_snapshot(&store, target)
        .await
        .expect("never-active target metadata must remain observable");
    assert_eq!(metadata.prepare_stage, Some(stage.durable_stage()));
    assert_eq!(
        branch_control_snapshot(&store, source.as_str())
            .await
            .expect("parent branch-root map must be readable")
            .roots
            .iter()
            .any(|root| root.branch_id == metadata.reservation.branch_id),
        stage.has_parent_root()
    );
}

async fn assert_never_active_stage_cancels(stage: NeverActiveStage) {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let source = harness.artifact_origin_namespace(&format!("cancel-{}-source", stage.label()));
    let target = harness.artifact_origin_namespace(&format!("cancel-{}-target", stage.label()));
    NamespaceManager::new(store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .expect("cancellation fixture parent must be created");
    prepare_never_active_stage(store.clone(), &source, &target, stage).await;
    let reservation = branch_metadata_snapshot(&store, &target)
        .await
        .expect("cancellation fixture reservation must be readable")
        .reservation;
    let evidence_before = store
        .list_prefix("_audit/destruction/")
        .await
        .expect("pre-cancellation evidence prefix must be listable");

    let (cancelled, evidence_key, evidence_preceded_root_removal, root_still_present_at_evidence) =
        if stage.has_parent_root() {
            let expected_root = branch_control_snapshot(&store, &source)
                .await
                .expect("rooted cancellation fixture must have a readable parent")
                .roots
                .into_iter()
                .find(|root| root.branch_id == reservation.branch_id)
                .expect("rooted cancellation fixture must retain its exact parent root");
            let (paused_store, root_removal) =
                pause_next_cas_matching(&store, Manifest::s3_key(&source));
            root_removal.arm();
            let cancel_target =
                NamespaceId::new(target.clone()).expect("target namespace must be valid");
            let cancel_store = paused_store.clone();
            let mut cancellation = tokio::spawn(async move {
                delete_namespace_for_test(
                    cancel_store,
                    cancel_target,
                    fork_indexing(),
                    fork_limits(),
                )
                .await
            });
            tokio::select! {
                () = root_removal.wait_until_paused() => {}
                outcome = &mut cancellation => {
                    let outcome = outcome.expect("cancellation task must not panic");
                    harness.cleanup().await;
                    panic!(
                        "{stage:?} cancellation returned before its evidence-ordered parent-root CAS: {outcome:?}"
                    );
                }
            }

            let cancelling = raw_namespace_metadata_from_store(&paused_store, &target).await;
            let evidence_key = cancelling
                .deletion_intent
                .as_ref()
                .map(|intent| intent.destruction_record_key.clone())
                .expect("cancellation intent must bind immutable destruction evidence");
            let evidence_preceded_root_removal = paused_store.get(&evidence_key).await.is_ok();
            let root_still_present_at_evidence = branch_control_snapshot(&paused_store, &source)
                .await
                .expect("paused parent root must remain readable")
                .roots
                == vec![expected_root];
            root_removal.release();
            let cancelled = cancellation
                .await
                .expect("cancellation task must not panic after root CAS release");
            (
                cancelled,
                Some(evidence_key),
                evidence_preceded_root_removal,
                root_still_present_at_evidence,
            )
        } else {
            let cancelled = delete_namespace_for_test(
                store.clone(),
                NamespaceId::new(target.clone()).expect("target namespace must be valid"),
                fork_indexing(),
                fork_limits(),
            )
            .await;
            let evidence_after = store
                .list_prefix("_audit/destruction/")
                .await
                .expect("post-cancellation evidence prefix must be listable");
            let evidence_key = evidence_after
                .iter()
                .find(|key| !evidence_before.contains(key))
                .cloned();
            (cancelled, evidence_key, true, true)
        };

    let source_roots = branch_control_snapshot(&store, &source)
        .await
        .expect("post-cancellation parent must remain readable")
        .roots;
    let target_keys = store
        .list_prefix(&format!("{target}/"))
        .await
        .expect("post-cancellation target prefix must be listable");
    let visibility_markers = store
        .list_prefix(&format!("{target}/_lifecycle/branch_visibility_removed/"))
        .await
        .expect("visibility-marker prefix must be listable");
    let evidence_survived = match evidence_key.as_ref() {
        Some(key) => store.exists(key).await.unwrap_or(false),
        None => false,
    };
    harness.cleanup().await;

    assert_eq!(
        cancelled.expect("authorized never-active cancellation must succeed"),
        NamespaceDeleteOutcome::Deleted,
        "never-active cancellation must complete without reader grace"
    );
    assert!(
        evidence_preceded_root_removal,
        "destruction evidence must exist before the exact parent-root CAS"
    );
    assert!(
        root_still_present_at_evidence,
        "the exact parent root must remain authoritative while evidence is checked"
    );
    assert!(
        source_roots
            .iter()
            .all(|root| root.branch_id != reservation.branch_id),
        "cancellation must remove its exact root when one was published"
    );
    assert!(target_keys.is_empty(), "target-owned cleanup must finish");
    assert!(
        visibility_markers.is_empty(),
        "a never-active target must not enter reader grace"
    );
    assert!(
        evidence_survived,
        "immutable cancellation evidence must survive cleanup"
    );
}

#[tokio::test]
async fn never_active_cancellation_cleans_reserved_no_root() {
    assert_never_active_stage_cancels(NeverActiveStage::ReservedNoRoot).await;
}

#[tokio::test]
async fn never_active_cancellation_cleans_reserved_root_crash() {
    assert_never_active_stage_cancels(NeverActiveStage::ReservedWithRoot).await;
}

#[tokio::test]
async fn never_active_cancellation_cleans_rooted_target() {
    assert_never_active_stage_cancels(NeverActiveStage::Rooted).await;
}

#[tokio::test]
async fn never_active_cancellation_cleans_manifest_published_target() {
    assert_never_active_stage_cancels(NeverActiveStage::ManifestPublished).await;
}

#[tokio::test]
async fn never_active_cancellation_retries_after_root_release_before_cleanup() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let source = harness.artifact_origin_namespace("cancel-root-released-retry-source");
    let target = harness.artifact_origin_namespace("cancel-root-released-retry-target");
    NamespaceManager::new(store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .expect("root-release retry fixture source must be created");
    prepare_never_active_stage(
        store.clone(),
        &source,
        &target,
        NeverActiveStage::ManifestPublished,
    )
    .await;

    let (faulted_store, cleanup_failure) =
        fail_delete_once_matching(&store, Manifest::history_key(&target, 1));
    let first = delete_namespace_for_test(
        faulted_store,
        NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        fork_indexing(),
        fork_limits(),
    )
    .await;
    assert!(
        first.is_err(),
        "the target cleanup fault must interrupt cancellation"
    );
    assert_eq!(cleanup_failure.failures_injected(), 1);
    assert!(
        branch_control_snapshot(&store, &source)
            .await
            .expect("parent root map must remain readable after cleanup failure")
            .roots
            .is_empty(),
        "the exact parent root must already be released before cleanup starts"
    );
    let interrupted = raw_namespace_metadata_from_store(&store, &target).await;
    assert_eq!(interrupted.state, NamespaceState::Creating);
    assert!(interrupted.deletion_intent.is_some());

    let retried = delete_namespace_for_test(
        store.clone(),
        NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .expect("authorized retry must resume after the root-release crash boundary");
    let target_keys = store
        .list_prefix(&format!("{target}/"))
        .await
        .expect("retried target prefix must be listable");
    harness.cleanup().await;

    assert_eq!(retried, NamespaceDeleteOutcome::Deleted);
    assert!(target_keys.is_empty());
}

#[tokio::test]
async fn never_active_cancellation_retry_survives_parent_history_deletion() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let source = harness.artifact_origin_namespace("cancel-parent-gone-retry-source");
    let target = harness.artifact_origin_namespace("cancel-parent-gone-retry-target");
    NamespaceManager::new(store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .expect("parent-deletion retry fixture source must be created");
    prepare_never_active_stage(
        store.clone(),
        &source,
        &target,
        NeverActiveStage::ManifestPublished,
    )
    .await;

    let (faulted_store, cleanup_failure) =
        fail_delete_once_matching(&store, Manifest::history_key(&target, 1));
    delete_namespace_for_test(
        faulted_store,
        NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .expect_err("target-history cleanup fault must interrupt cancellation");
    assert_eq!(cleanup_failure.failures_injected(), 1);
    assert!(
        branch_control_snapshot(&store, &source)
            .await
            .expect("parent root map must remain readable after cleanup failure")
            .roots
            .is_empty(),
        "exact parent-root release must precede target-owned cleanup"
    );
    let interrupted = raw_namespace_metadata_from_store(&store, &target).await;
    let evidence_key = interrupted
        .deletion_intent
        .as_ref()
        .map(|intent| intent.destruction_record_key.clone())
        .expect("interrupted cancellation must retain its durable intent");
    assert!(
        store
            .exists(&evidence_key)
            .await
            .expect("immutable cancellation evidence must be observable"),
        "immutable evidence must precede root release and cleanup"
    );
    assert!(
        store
            .exists(&Manifest::history_key(&target, 1))
            .await
            .expect("faulted target history key must be observable"),
        "the injected cleanup failure must leave target-owned work to resume"
    );

    NamespaceManager::new(store.clone())
        .delete(&source)
        .await
        .expect("rootless parent namespace deletion must complete");
    let source_keys = store
        .list_prefix(&format!("{source}/"))
        .await
        .expect("deleted parent prefix must be listable");
    assert!(
        source_keys.is_empty(),
        "parent metadata, manifest, and immutable history must all be absent before retry"
    );

    let retried = delete_namespace_for_test(
        store.clone(),
        NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .expect("authorized retry must rely on durable intent/evidence after parent deletion");
    let target_keys = store
        .list_prefix(&format!("{target}/"))
        .await
        .expect("retried target prefix must be listable");
    let evidence_survived = store
        .exists(&evidence_key)
        .await
        .expect("immutable cancellation evidence must remain observable");
    harness.cleanup().await;

    assert_eq!(retried, NamespaceDeleteOutcome::Deleted);
    assert!(target_keys.is_empty());
    assert!(
        evidence_survived,
        "parent and target cleanup must not delete immutable cancellation evidence"
    );
}

#[tokio::test]
async fn maintenance_reports_but_never_executes_an_authorized_cancellation_intent() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let source = harness.artifact_origin_namespace("cancel-maintain-source");
    let target = harness.artifact_origin_namespace("cancel-maintain-target");
    NamespaceManager::new(store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .expect("maintenance cancellation fixture source must be created");
    prepare_never_active_stage(
        store.clone(),
        &source,
        &target,
        NeverActiveStage::ReservedWithRoot,
    )
    .await;
    let reservation = branch_metadata_snapshot(&store, &target)
        .await
        .expect("maintenance cancellation reservation must be readable")
        .reservation;
    let evidence_key = format!(
        "_audit/destruction/{}.json",
        reservation.target_incarnation.to_string().replace('-', "")
    );
    let (faulted_store, evidence_failure) = fail_put_once_matching(&store, evidence_key);
    let interrupted = delete_namespace_for_test(
        faulted_store,
        NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        fork_indexing(),
        fork_limits(),
    )
    .await;
    assert!(interrupted.is_err());
    assert_eq!(evidence_failure.failures_injected(), 1);
    let metadata_before = store
        .get(&NamespaceMetadata::s3_key(&target))
        .await
        .expect("interrupted cancellation metadata must be readable");
    let roots_before = branch_control_snapshot(&store, &source)
        .await
        .expect("interrupted cancellation root must be readable")
        .roots;

    let report = maintain_branches_for_test(
        store.clone(),
        fork_indexing(),
        fork_limits(),
        Duration::from_secs(5),
    )
    .await
    .expect("maintenance must report an authorized cancellation intent");
    let metadata_after = store
        .get(&NamespaceMetadata::s3_key(&target))
        .await
        .expect("maintenance must retain cancellation metadata");
    let roots_after = branch_control_snapshot(&store, &source)
        .await
        .expect("maintenance must retain the cancellation root")
        .roots;
    assert!(report.awaiting_authorized_cancellation >= 1);
    assert_eq!(metadata_after, metadata_before);
    assert_eq!(roots_after, roots_before);

    let retried = delete_namespace_for_test(
        store.clone(),
        NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .expect("fresh authorized retry must finish cancellation");
    harness.cleanup().await;
    assert_eq!(retried, NamespaceDeleteOutcome::Deleted);
}

/// Maintenance must still finish a cancellation that revoked an activation.
///
/// This is the counterpart to the preceding test. An intent carrying the
/// activation nonce owns a retained policy guard that only the graph can
/// release, so leaving it for a fresh authorized request would strand the
/// guard. The nonce is therefore the exact discriminator between the two
/// cases, and narrowing maintenance to it must not re-strand this one.
#[tokio::test]
async fn maintenance_resumes_an_activation_cancelled_fork() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let source = harness.artifact_origin_namespace("activation-cancel-source");
    let target = harness.artifact_origin_namespace("activation-cancel-target");
    NamespaceManager::new(store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .expect("activation cancellation fixture source must be created");
    prepare_fork_until_activation_pending_for_test(
        store.clone(),
        NamespaceId::new(source.clone()).expect("source namespace must be valid"),
        NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .expect("fixture must retain an activation attempt");

    let reservation = branch_metadata_snapshot(&store, &target)
        .await
        .expect("activation cancellation reservation must be readable")
        .reservation;
    let evidence_key = format!(
        "_audit/destruction/{}.json",
        reservation.target_incarnation.to_string().replace('-', "")
    );

    // Interrupt the authorized cancellation after it has durably recorded the
    // intent and its activation nonce, but before destruction evidence lands.
    let (faulted_store, evidence_failure) = fail_put_once_matching(&store, evidence_key);
    let interrupted = delete_namespace_for_test(
        faulted_store,
        NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        fork_indexing(),
        fork_limits(),
    )
    .await;
    assert!(interrupted.is_err());
    assert_eq!(evidence_failure.failures_injected(), 1);

    let report = maintain_branches_for_test(
        store.clone(),
        fork_indexing(),
        fork_limits(),
        Duration::from_secs(5),
    )
    .await
    .expect("maintenance must resume an activation-cancelled fork");
    let roots_after = branch_control_snapshot(&store, &source)
        .await
        .expect("source branch control must remain readable")
        .roots;
    let metadata_after = store.get(&NamespaceMetadata::s3_key(&target)).await;
    harness.cleanup().await;

    assert_eq!(report.awaiting_authorized_cancellation, 0);
    assert!(report.deletions_completed >= 1);
    assert!(roots_after.is_empty());
    assert!(metadata_after.is_err());
}

#[tokio::test]
async fn maintenance_stale_reserved_read_cannot_advance_a_cancellation_intent() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    // The target sorts before the source so maintenance's first matching
    // metadata read is the outer Creating-target snapshot, not source graph
    // verification reaching through to the child.
    let source = harness.artifact_origin_namespace("cancel-maintain-race-z-source");
    let target = harness.artifact_origin_namespace("cancel-maintain-race-a-target");
    NamespaceManager::new(store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .expect("maintenance race fixture source must be created");
    prepare_never_active_stage(
        store.clone(),
        &source,
        &target,
        NeverActiveStage::ReservedWithRoot,
    )
    .await;
    let reservation = branch_metadata_snapshot(&store, &target)
        .await
        .expect("maintenance race reservation must be readable")
        .reservation;
    let evidence_key = format!(
        "_audit/destruction/{}.json",
        reservation.target_incarnation.to_string().replace('-', "")
    );
    let (faulted_store, evidence_failure) = fail_put_once_matching(&store, evidence_key);
    let (race_store, maintenance_stale_read) =
        pause_first_after_get_matching(&faulted_store, NamespaceMetadata::s3_key(&target));
    let maintenance_store = race_store.clone();
    let mut maintenance = tokio::spawn(async move {
        maintain_branches_for_test(
            maintenance_store,
            fork_indexing(),
            fork_limits(),
            Duration::from_secs(120),
        )
        .await
    });
    tokio::select! {
        () = maintenance_stale_read.wait_until_paused() => {}
        outcome = &mut maintenance => {
            harness.cleanup().await;
            panic!("maintenance returned before its stale target snapshot: {outcome:?}");
        }
    }

    let cancellation = delete_namespace_for_test(
        race_store.clone(),
        NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        fork_indexing(),
        fork_limits(),
    )
    .await;
    assert!(cancellation.is_err());
    assert_eq!(evidence_failure.failures_injected(), 1);
    let metadata_before = store
        .get(&NamespaceMetadata::s3_key(&target))
        .await
        .expect("interrupted cancellation metadata must be readable");
    let roots_before = branch_control_snapshot(&store, &source)
        .await
        .expect("interrupted cancellation root must be readable")
        .roots;

    maintenance_stale_read.release();
    let maintenance_result = maintenance
        .await
        .expect("maintenance race task must not panic");
    let metadata_after = store
        .get(&NamespaceMetadata::s3_key(&target))
        .await
        .expect("stale maintenance must retain cancellation metadata");
    let roots_after = branch_control_snapshot(&store, &source)
        .await
        .expect("stale maintenance must retain the exact root")
        .roots;
    assert!(
        matches!(
        maintenance_result,
        Err(ZeppelinError::Branch(ref inner))
            if matches!(inner.as_ref(), BranchError::CancellationInProgress { .. })
        ),
        "stale maintenance must stop at the fresh cancellation guard: {maintenance_result:?}"
    );
    assert_eq!(metadata_after, metadata_before);
    assert_eq!(roots_after, roots_before);

    let retried = delete_namespace_for_test(
        store.clone(),
        NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .expect("fresh authorized retry must complete after stale maintenance exits");
    harness.cleanup().await;
    assert_eq!(retried, NamespaceDeleteOutcome::Deleted);
}

#[derive(Clone, Copy, Debug)]
enum UnavailableParent {
    Fenced,
    Absent,
}

async fn assert_unavailable_parent_cancellation_skips_lease(parent: UnavailableParent) {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let suffix = match parent {
        UnavailableParent::Fenced => "fenced",
        UnavailableParent::Absent => "absent",
    };
    let source = harness.artifact_origin_namespace(&format!("cancel-{suffix}-parent"));
    let target = harness.artifact_origin_namespace(&format!("cancel-{suffix}-target"));
    NamespaceManager::new(store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .expect("unavailable-parent fixture must create its source");
    prepare_never_active_stage(
        store.clone(),
        &source,
        &target,
        NeverActiveStage::ReservedNoRoot,
    )
    .await;

    match parent {
        UnavailableParent::Fenced => {
            publish_deletion_fence(
                store.clone(),
                &source,
                "_audit/destruction/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json",
            )
            .await
            .expect("fixture must fence the exact parent incarnation");
            assert!(
                branch_control_snapshot(&store, &source)
                    .await
                    .expect("fenced parent manifest must remain readable")
                    .deletion_fenced
            );
        }
        UnavailableParent::Absent => {
            NamespaceManager::new(store.clone())
                .delete(&source)
                .await
                .expect("fixture must remove the exact parent incarnation");
            assert!(Manifest::read(&store, &source)
                .await
                .expect("parent manifest absence must be readable")
                .is_none());
            assert!(!store
                .exists(&NamespaceMetadata::s3_key(&source))
                .await
                .expect("parent metadata absence must be readable"));
        }
    }

    // A live lease record is deliberately left behind after the parent becomes
    // unable to publish. Authorized cancellation must prove the fenced/absent
    // parent state and must not try to acquire or rewrite this lease.
    let lease_manager = LeaseManager::new(
        store.clone(),
        format!("unavailable-parent-holder-{suffix}"),
        Duration::from_secs(30),
    );
    let lease = lease_manager
        .acquire(&source)
        .await
        .expect("fixture must hold the parent lease");
    let lease_key = format!("{source}/lease.json");
    let lease_before = store
        .get(&lease_key)
        .await
        .expect("held parent lease must be readable");

    let cancelled = delete_namespace_for_test(
        store.clone(),
        NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        fork_indexing(),
        fork_limits(),
    )
    .await;
    let lease_after = store
        .get(&lease_key)
        .await
        .expect("cancellation must leave the held parent lease readable");
    let target_keys = store
        .list_prefix(&format!("{target}/"))
        .await
        .expect("cancelled target prefix must be listable");
    let evidence = store
        .list_prefix("_audit/destruction/")
        .await
        .expect("cancellation evidence prefix must be listable");
    lease_manager
        .release(&source, &lease)
        .await
        .expect("fixture must release its unchanged parent lease");
    harness.cleanup().await;

    assert_eq!(
        cancelled.expect("fenced/absent-parent cancellation must not require a parent lease"),
        NamespaceDeleteOutcome::Deleted
    );
    assert_eq!(
        lease_after, lease_before,
        "cancellation must not acquire, renew, or release the unavailable parent's lease"
    );
    assert!(target_keys.is_empty());
    assert_eq!(
        evidence.len(),
        1,
        "cancellation must leave one immutable destruction record"
    );
}

#[tokio::test]
async fn never_active_cancellation_of_fenced_parent_skips_parent_lease() {
    assert_unavailable_parent_cancellation_skips_lease(UnavailableParent::Fenced).await;
}

#[tokio::test]
async fn never_active_cancellation_of_absent_parent_skips_parent_lease() {
    assert_unavailable_parent_cancellation_skips_lease(UnavailableParent::Absent).await;
}

#[tokio::test]
async fn never_active_cancellation_treats_recreated_parent_as_exact_parent_absence() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let source = harness.artifact_origin_namespace("cancel-recreated-parent");
    let target = harness.artifact_origin_namespace("cancel-recreated-target");
    let manager = NamespaceManager::new(store.clone());
    let original = manager
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .expect("recreated-parent fixture must create its original source");
    prepare_never_active_stage(
        store.clone(),
        &source,
        &target,
        NeverActiveStage::ReservedNoRoot,
    )
    .await;
    manager
        .delete(&source)
        .await
        .expect("fixture must delete the exact original parent");
    let replacement = manager
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .expect("fixture must recreate the parent name with a new incarnation");
    assert_ne!(original.incarnation_id, replacement.incarnation_id);

    let lease_manager = LeaseManager::new(
        store.clone(),
        "recreated-parent-holder".to_string(),
        Duration::from_secs(30),
    );
    let lease = lease_manager
        .acquire(&source)
        .await
        .expect("fixture must hold the replacement parent's lease");
    let lease_key = format!("{source}/lease.json");
    let lease_before = store
        .get(&lease_key)
        .await
        .expect("replacement parent lease must be readable");

    let cancelled = delete_namespace_for_test(
        store.clone(),
        NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .expect("the absent exact parent must not require the replacement lease");
    let lease_after = store
        .get(&lease_key)
        .await
        .expect("replacement parent lease must remain readable");
    let replacement_after = manager
        .get(&source)
        .await
        .expect("replacement parent must remain active");
    lease_manager
        .release(&source, &lease)
        .await
        .expect("fixture must release the unchanged replacement lease");
    harness.cleanup().await;

    assert_eq!(cancelled, NamespaceDeleteOutcome::Deleted);
    assert_eq!(lease_after, lease_before);
    assert_eq!(replacement_after.incarnation_id, replacement.incarnation_id);
}

#[tokio::test]
async fn concurrent_no_lease_cancellation_uses_the_winning_durable_decision() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let source = harness.artifact_origin_namespace("cancel-no-lease-race-source");
    let target = harness.artifact_origin_namespace("cancel-no-lease-race-target");
    NamespaceManager::new(store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .expect("no-lease race fixture source must be created");
    prepare_never_active_stage(
        store.clone(),
        &source,
        &target,
        NeverActiveStage::ReservedNoRoot,
    )
    .await;
    let reservation = branch_metadata_snapshot(&store, &target)
        .await
        .expect("no-lease race reservation must be readable")
        .reservation;
    publish_deletion_fence(
        store.clone(),
        &source,
        "_audit/destruction/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json",
    )
    .await
    .expect("no-lease race parent must be fenced");
    let evidence_key = format!(
        "_audit/destruction/{}.json",
        reservation.target_incarnation.to_string().replace('-', "")
    );
    let (evidence_store, evidence_pause) =
        pause_first_create_matching(&store, evidence_key.clone());
    let (race_store, first_intent_pause) =
        pause_next_cas_matching(&evidence_store, NamespaceMetadata::s3_key(&target));
    first_intent_pause.arm();

    let first_store = race_store.clone();
    let first_target = NamespaceId::new(target.clone()).expect("target namespace must be valid");
    let mut first = tokio::spawn(async move {
        delete_namespace_for_test(first_store, first_target, fork_indexing(), fork_limits()).await
    });
    tokio::select! {
        () = first_intent_pause.wait_until_paused() => {}
        outcome = &mut first => {
            harness.cleanup().await;
            panic!("first cancellation returned before its intent CAS pause: {outcome:?}");
        }
    }

    let second_store = race_store.clone();
    let second_target = NamespaceId::new(target.clone()).expect("target namespace must be valid");
    let mut second = tokio::spawn(async move {
        delete_namespace_for_test(second_store, second_target, fork_indexing(), fork_limits()).await
    });
    tokio::select! {
        () = evidence_pause.wait_until_paused() => {}
        outcome = &mut second => {
            first_intent_pause.release();
            let _ = first.await;
            harness.cleanup().await;
            panic!("second cancellation returned before its evidence pause: {outcome:?}");
        }
    }
    let winning_metadata = raw_namespace_metadata_from_store(&race_store, &target).await;
    let winning_decision_ref = winning_metadata
        .deletion_intent
        .as_ref()
        .expect("winning cancellation intent must be durable")
        .decision_evidence_ref
        .clone();

    first_intent_pause.release();
    let first_outcome = first.await.expect("first cancellation task must not panic");
    evidence_pause.release();
    let second_outcome = second
        .await
        .expect("second cancellation task must not panic");
    let evidence: serde_json::Value = serde_json::from_slice(
        &store
            .get(&evidence_key)
            .await
            .expect("one canonical cancellation evidence object must survive"),
    )
    .expect("cancellation evidence must be valid JSON");
    let decision: serde_json::Value = serde_json::from_slice(
        &store
            .get(&winning_decision_ref)
            .await
            .expect("the winning durable decision evidence must survive"),
    )
    .expect("decision evidence must be valid JSON");
    let target_keys = store
        .list_prefix(&format!("{target}/"))
        .await
        .expect("concurrent cancellation target prefix must be listable");
    harness.cleanup().await;

    assert!(
        matches!(first_outcome, Ok(NamespaceDeleteOutcome::Deleted))
            || matches!(second_outcome, Ok(NamespaceDeleteOutcome::Deleted)),
        "one authorized cancellation must complete"
    );
    assert_eq!(
        evidence.get("decision_id"),
        decision
            .get("decision")
            .and_then(|value| value.get("decision_id")),
        "destruction evidence must bind the decision that won the durable target intent"
    );
    assert!(target_keys.is_empty());
}

#[derive(Clone, Copy, Debug)]
enum AmbiguousParent {
    ManifestWithoutMetadata,
    MetadataWithoutManifest,
}

async fn assert_ambiguous_parent_fails_closed(parent: AmbiguousParent) {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let suffix = match parent {
        AmbiguousParent::ManifestWithoutMetadata => "manifest-without-meta",
        AmbiguousParent::MetadataWithoutManifest => "meta-without-manifest",
    };
    let source = harness.artifact_origin_namespace(&format!("cancel-{suffix}-source"));
    let target = harness.artifact_origin_namespace(&format!("cancel-{suffix}-target"));
    NamespaceManager::new(store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .expect("ambiguous-parent fixture source must be created");
    prepare_never_active_stage(
        store.clone(),
        &source,
        &target,
        NeverActiveStage::ReservedNoRoot,
    )
    .await;
    match parent {
        AmbiguousParent::ManifestWithoutMetadata => store
            .delete(&NamespaceMetadata::s3_key(&source))
            .await
            .expect("fixture must remove only parent metadata"),
        AmbiguousParent::MetadataWithoutManifest => store
            .delete(&Manifest::s3_key(&source))
            .await
            .expect("fixture must remove only parent manifest"),
    }
    let target_before = store
        .get(&NamespaceMetadata::s3_key(&target))
        .await
        .expect("ambiguous-parent target metadata must be readable");

    let cancellation = delete_namespace_for_test(
        store.clone(),
        NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        fork_indexing(),
        fork_limits(),
    )
    .await;
    let target_after = store
        .get(&NamespaceMetadata::s3_key(&target))
        .await
        .expect("failed cancellation must retain target metadata");
    let evidence = store
        .list_prefix("_audit/destruction/")
        .await
        .expect("ambiguous-parent evidence prefix must be listable");
    harness.cleanup().await;

    assert!(matches!(cancellation, Err(ZeppelinError::Validation(_))));
    assert_eq!(target_after, target_before);
    assert!(evidence.is_empty());
}

#[tokio::test]
async fn never_active_cancellation_rejects_parent_manifest_without_metadata() {
    assert_ambiguous_parent_fails_closed(AmbiguousParent::ManifestWithoutMetadata).await;
}

#[tokio::test]
async fn never_active_cancellation_rejects_parent_metadata_without_manifest() {
    assert_ambiguous_parent_fails_closed(AmbiguousParent::MetadataWithoutManifest).await;
}

#[tokio::test]
async fn never_active_cancellation_loses_when_publisher_holds_parent_lease() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let source = harness.artifact_origin_namespace("cancel-publisher-first-source");
    let target = harness.artifact_origin_namespace("cancel-publisher-first-target");
    NamespaceManager::new(store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .expect("publisher-first fixture source must be created");
    prepare_never_active_stage(
        store.clone(),
        &source,
        &target,
        NeverActiveStage::ReservedNoRoot,
    )
    .await;
    let reservation = branch_metadata_snapshot(&store, &target)
        .await
        .expect("publisher-first reservation must be readable")
        .reservation;

    let (paused_store, root_publication) =
        pause_next_cas_matching(&store, Manifest::s3_key(&source));
    root_publication.arm();
    let publisher_store = paused_store.clone();
    let publisher_source =
        NamespaceId::new(source.clone()).expect("source namespace must be valid");
    let publisher_target =
        NamespaceId::new(target.clone()).expect("target namespace must be valid");
    let mut publisher = tokio::spawn(async move {
        prepare_fork_for_test(
            publisher_store,
            publisher_source,
            publisher_target,
            fork_indexing(),
            fork_limits(),
        )
        .await
    });
    tokio::select! {
        () = root_publication.wait_until_paused() => {}
        outcome = &mut publisher => {
            harness.cleanup().await;
            panic!("publisher returned before its deterministic root boundary: {outcome:?}");
        }
    }

    let cancellation = delete_namespace_for_test(
        paused_store.clone(),
        NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        fork_indexing(),
        fork_limits(),
    )
    .await;
    let reservation_intact = branch_metadata_snapshot(&paused_store, &target)
        .await
        .expect("losing cancellation must retain the reservation")
        .reservation
        == reservation;
    let root_absent_while_paused = branch_control_snapshot(&paused_store, &source)
        .await
        .expect("publisher-first parent root map must be readable")
        .roots
        .is_empty();
    root_publication.release();
    publisher
        .await
        .expect("publisher task must not panic")
        .expect("lease-owning publisher must finish preparation");
    let final_target = branch_metadata_snapshot(&store, &target)
        .await
        .expect("winning publication must leave prepared metadata");
    let final_roots = branch_control_snapshot(&store, &source)
        .await
        .expect("winning publication root must be readable")
        .roots;
    harness.cleanup().await;

    assert!(matches!(
        cancellation,
        Err(ZeppelinError::LeaseHeld { ref namespace, .. }) if namespace == &source
    ));
    assert!(reservation_intact);
    assert!(root_absent_while_paused);
    assert_eq!(
        final_target.prepare_stage,
        Some(BranchPrepareStage::ManifestPublished)
    );
    assert_eq!(final_roots.len(), 1);
    assert_eq!(final_roots[0].branch_id, reservation.branch_id);
    assert_eq!(final_roots[0].target_namespace.as_str(), target);
}

#[tokio::test]
async fn never_active_cancellation_wins_when_canceller_holds_parent_lease() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let source = harness.artifact_origin_namespace("cancel-canceller-first-source");
    let target = harness.artifact_origin_namespace("cancel-canceller-first-target");
    NamespaceManager::new(store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .expect("canceller-first fixture source must be created");
    prepare_never_active_stage(
        store.clone(),
        &source,
        &target,
        NeverActiveStage::ReservedWithRoot,
    )
    .await;

    let (paused_store, cancellation_intent) =
        pause_next_cas_matching(&store, NamespaceMetadata::s3_key(&target));
    cancellation_intent.arm();
    let cancel_store = paused_store.clone();
    let cancel_target = NamespaceId::new(target.clone()).expect("target namespace must be valid");
    let mut cancellation = tokio::spawn(async move {
        delete_namespace_for_test(cancel_store, cancel_target, fork_indexing(), fork_limits()).await
    });
    tokio::select! {
        () = cancellation_intent.wait_until_paused() => {}
        outcome = &mut cancellation => {
            let outcome = outcome.expect("cancellation task must not panic");
            harness.cleanup().await;
            panic!(
                "cancellation returned before its target-intent CAS under the parent lease: {outcome:?}"
            );
        }
    }

    let publication = prepare_fork_for_test(
        paused_store.clone(),
        NamespaceId::new(source.clone()).expect("source namespace must be valid"),
        NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        fork_indexing(),
        fork_limits(),
    )
    .await;
    let root_retained_while_intent_paused = branch_control_snapshot(&paused_store, &source)
        .await
        .expect("canceller-first parent root must remain readable")
        .roots
        .len()
        == 1;
    cancellation_intent.release();
    let cancelled = cancellation
        .await
        .expect("cancellation task must not panic after intent release");
    let final_roots = branch_control_snapshot(&store, &source)
        .await
        .expect("post-cancellation parent must remain readable")
        .roots;
    let target_keys = store
        .list_prefix(&format!("{target}/"))
        .await
        .expect("post-cancellation target prefix must be listable");
    let evidence = store
        .list_prefix("_audit/destruction/")
        .await
        .expect("post-cancellation evidence prefix must be listable");
    harness.cleanup().await;

    assert!(matches!(
        publication,
        Err(ZeppelinError::LeaseHeld { ref namespace, .. }) if namespace == &source
    ));
    assert!(root_retained_while_intent_paused);
    assert_eq!(
        cancelled.expect("lease-owning cancellation must finish"),
        NamespaceDeleteOutcome::Deleted
    );
    assert!(
        final_roots.is_empty(),
        "winning cancellation must remove the exact root"
    );
    assert!(
        target_keys.is_empty(),
        "winning cancellation must remove its reservation"
    );
    assert_eq!(evidence.len(), 1);
}
