#![cfg(feature = "branching-test-support")]

mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::harness::TestHarness;
use common::vectors::random_vectors;
use zeppelin::compaction::{CompactionResult, Compactor};
use zeppelin::config::{BranchingConfig, CompactionConfig, IndexingConfig};
use zeppelin::error::ZeppelinError;
use zeppelin::fts::FtsFieldConfig;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::namespace::branching::test_support::{
    activate_fork_for_test, branch_control_snapshot, branch_metadata_snapshot,
    delete_namespace_for_test, list_children_for_test, maintain_branches_for_test,
    prepare_fork_for_test, prepare_fork_until_reserved_for_test, prepare_fork_until_root_for_test,
    prepared_manifest_snapshot, publish_deletion_fence, resume_delete_for_test,
};
use zeppelin::namespace::branching::{BranchError, BranchPrepareStage, PrepareForkOutcome};
use zeppelin::namespace::manager::{
    CompactionStatus, NamespaceIndexConfig, NamespaceMetadata, NamespaceState,
};
use zeppelin::namespace::{NamespaceId, NamespaceManager};
use zeppelin::types::{DistanceMetric, VectorEntry};
use zeppelin::wal::{Manifest, WalReader, WalWriter};

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
    let error = NamespaceManager::new(harness.store.clone())
        .delete(&source)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ZeppelinError::Branch(inner)
            if matches!(*inner, BranchError::NamespaceHasLiveBranches { .. })
    ));
    assert!(NamespaceManager::new(harness.store.clone())
        .get(&source)
        .await
        .is_ok());
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
    assert!(children.iter().all(|child| child.state == "creating"));

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
    let bytes = harness
        .store
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
async fn maintenance_leaves_an_unrooted_reservation_for_authenticated_retry() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("branch-maintain-reserved-source");
    let target = harness.artifact_origin_namespace("branch-maintain-reserved-target");
    NamespaceManager::new(harness.store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .unwrap();

    prepare_fork_until_reserved_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(target.clone()).unwrap(),
        fork_indexing(),
        fork_limits(),
    )
    .await
    .unwrap();
    let reserved_before = branch_metadata_snapshot(&harness.store, &target)
        .await
        .unwrap();

    let report = maintain_branches_for_test(
        harness.store.clone(),
        fork_indexing(),
        fork_limits(),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(report.awaiting_authenticated_retry >= 1);
    assert!(branch_control_snapshot(&harness.store, &source)
        .await
        .unwrap()
        .roots
        .is_empty());
    let reserved_after = branch_metadata_snapshot(&harness.store, &target)
        .await
        .unwrap();
    assert_eq!(
        reserved_after.prepare_stage,
        Some(BranchPrepareStage::Reserved)
    );
    assert_eq!(reserved_after.reservation, reserved_before.reservation);
    assert!(reserved_after.branch_identity.is_none());
    assert!(Manifest::read(&harness.store, &target)
        .await
        .unwrap()
        .is_none());

    let retry = prepare_fork_for_test(
        harness.store.clone(),
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
    let source = harness.artifact_origin_namespace("branch-maintain-fenced-source");
    let target = harness.artifact_origin_namespace("branch-maintain-fenced-target");
    NamespaceManager::new(harness.store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .unwrap();
    prepare_fork_until_reserved_for_test(
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
    publish_deletion_fence(
        harness.store.clone(),
        &source,
        "_audit/destruction/0123456789abcdef0123456789abcdef.json",
    )
    .await
    .unwrap();

    let report = maintain_branches_for_test(
        harness.store.clone(),
        fork_indexing(),
        fork_limits(),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(report.awaiting_authorized_cancellation >= 1);
    assert!(branch_control_snapshot(&harness.store, &source)
        .await
        .unwrap()
        .roots
        .is_empty());
    assert!(Manifest::read(&harness.store, &target)
        .await
        .unwrap()
        .is_none());

    let retry = prepare_fork_for_test(
        harness.store.clone(),
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
    let after = branch_metadata_snapshot(&harness.store, &target)
        .await
        .unwrap();
    assert_eq!(after.reservation, reserved.reservation);
    assert_eq!(after.prepare_stage, Some(BranchPrepareStage::Reserved));
    assert!(after.branch_identity.is_none());
    assert!(Manifest::read(&harness.store, &target)
        .await
        .unwrap()
        .is_none());

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
