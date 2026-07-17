#![cfg(feature = "branching-test-support")]

mod common;

use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;

use common::assertions::{assert_s3_object_exists, assert_s3_object_not_exists};
use common::counting::counting_store;
use common::fault_injection::{pause_first_after_get_matching, synchronize_cas_pair_matching};
use common::harness::TestHarness;
use common::server::test_security_runtime;
use common::vectors::random_vectors;
use zeppelin::compaction::gc::{
    gc_candidate_store_key, load_gc_candidates, run_gc_cycle, run_gc_cycle_at,
};
use zeppelin::compaction::Compactor;
use zeppelin::config::{
    CompactionConfig, Config, GcConfig, IndexingConfig, DEFAULT_RERANK_COALESCE_GAP_BYTES,
};
use zeppelin::error::ZeppelinError;
use zeppelin::namespace::branching::test_support::{
    branch_control_snapshot, insert_prepared_branch_root, prepare_head_branch_root,
    publish_deletion_fence, publish_manifest_fencing_token,
};
use zeppelin::namespace::branching::{
    BranchError, BranchId, ForkViewDigest, SourceDataPlaneConfigDigest,
};
use zeppelin::namespace::NamespaceManager;
use zeppelin::query::{execute_query, QueryParams};
use zeppelin::time::Clock;
use zeppelin::types::{ConsistencyLevel, DistanceMetric, VectorEntry};
use zeppelin::wal::manifest::ManifestHistoryRetention;
use zeppelin::wal::{Manifest, WalFragment, WalReader, WalWriter};

fn test_compactor(store: &zeppelin::storage::ZeppelinStore) -> Compactor {
    Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        CompactionConfig {
            max_old_segments: 0,
            retrain_imbalance_threshold: 0.0,
            ..CompactionConfig::default()
        },
        IndexingConfig {
            default_num_centroids: 4,
            kmeans_max_iterations: 10,
            ..IndexingConfig::default()
        },
        common::default_gc_upload_window(),
    )
}

fn immediate_gc() -> GcConfig {
    GcConfig {
        horizon_secs: 0,
        allow_unsafe_short_horizon: true,
        manifest_history_keep_count: 1,
        pitr_retention_secs: 0,
        ..GcConfig::default()
    }
}

#[tokio::test]
async fn rooted_predecessor_history_and_unique_artifact_survive_prune_and_gc() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let source = harness.artifact_origin_namespace("branch-root-retention-source");
    let target = harness.artifact_origin_namespace("branch-root-retention-target");
    let manager = NamespaceManager::new(store.clone());
    manager
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .unwrap();
    let writer = WalWriter::new(store.clone());
    let compactor = test_compactor(&store);
    writer
        .append(&source, random_vectors(24, 4), Vec::new())
        .await
        .unwrap();
    assert_eq!(
        compactor.compact(&source).await.unwrap().vectors_compacted,
        24
    );
    let manifest = Manifest::read(&store, &source).await.unwrap().unwrap();
    let rooted_generation = manifest.version();
    let old_cluster_keys = manifest
        .active_segment
        .as_ref()
        .and_then(|active| {
            manifest
                .segments
                .iter()
                .find(|segment| &segment.id == active)
        })
        .map(|segment| {
            segment
                .cluster_objects
                .iter()
                .map(|object| object.key.clone())
                .collect::<Vec<_>>()
        })
        .filter(|keys| !keys.is_empty())
        .expect("the compacted fork generation must own cluster artifacts");

    let prepared = prepare_head_branch_root(
        store.clone(),
        &source,
        BranchId::new(),
        &target,
        uuid::Uuid::new_v4(),
        ForkViewDigest::new([0x44; 32]),
        Utc::now(),
    )
    .await
    .unwrap();
    let inserted = insert_prepared_branch_root(store.clone(), &source, prepared, 256)
        .await
        .unwrap();
    assert_eq!(inserted.source_generation().get(), rooted_generation);

    writer
        .append(
            &source,
            vec![VectorEntry {
                id: "post-fork-generation".to_string(),
                values: vec![0.1, 0.2, 0.3, 0.4],
                attributes: None,
            }],
            Vec::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        compactor.compact(&source).await.unwrap().vectors_compacted,
        25
    );
    let current = Manifest::read(&store, &source).await.unwrap().unwrap();
    let old_key = old_cluster_keys
        .into_iter()
        .find(|old_key| {
            current.segments.iter().all(|segment| {
                segment
                    .cluster_objects
                    .iter()
                    .all(|object| &object.key != old_key)
            })
        })
        .expect("the successor compaction must retire at least one G-only cluster artifact");
    for offset in 1..=4 {
        let (mut current, version) = Manifest::read_versioned(&store, &source)
            .await
            .unwrap()
            .unwrap();
        current.updated_at = Utc::now() + chrono::Duration::seconds(offset);
        current
            .write_conditional(&store, &source, &version)
            .await
            .unwrap();
    }

    Manifest::prune_history_with_retention(
        &store,
        &source,
        ManifestHistoryRetention {
            keep_count: 1,
            pitr_retention_secs: 0,
            skew_slop_secs: 0,
        },
    )
    .await
    .unwrap();
    run_gc_cycle(&store, &source, &immediate_gc())
        .await
        .unwrap();
    run_gc_cycle(&store, &source, &immediate_gc())
        .await
        .unwrap();

    assert!(
        Manifest::read_history(&store, &source, rooted_generation)
            .await
            .unwrap()
            .is_some(),
        "the current live root must retain its exact predecessor generation"
    );
    assert_s3_object_exists(&store, &old_key).await;
    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn branch_root_and_deletion_fence_race_has_exactly_one_cas_winner() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("branch-root-delete-race-source");
    let target = harness.artifact_origin_namespace("branch-root-delete-race-target");
    NamespaceManager::new(harness.store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .unwrap();

    let prepared = prepare_head_branch_root(
        harness.store.clone(),
        &source,
        BranchId::new(),
        &target,
        uuid::Uuid::new_v4(),
        ForkViewDigest::new([0x66; 32]),
        Utc::now(),
    )
    .await
    .unwrap();

    let (racing_store, barrier) =
        synchronize_cas_pair_matching(&harness.store, Manifest::s3_key(&source));
    barrier.enable();
    let destruction_record_key =
        format!("_audit/destruction/{}.json", uuid::Uuid::new_v4().simple());
    let root_store = racing_store.clone();
    let root_source = source.clone();
    let root_task = tokio::spawn(async move {
        insert_prepared_branch_root(root_store, &root_source, prepared, 256).await
    });
    let fence_store = racing_store.clone();
    let fence_source = source.clone();
    let fence_task = tokio::spawn(async move {
        publish_deletion_fence(fence_store, &fence_source, &destruction_record_key).await
    });
    let (root_result, fence_result) = tokio::join!(root_task, fence_task);
    let root_result = root_result.unwrap();
    let fence_result = fence_result.unwrap();

    assert_eq!(barrier.arrivals(), 2);
    assert_eq!(barrier.conflicts(), 1);
    assert_eq!(
        usize::from(root_result.is_ok()) + usize::from(fence_result.is_ok()),
        1,
        "exactly one branch-control CAS must win: root={root_result:?}, fence={fence_result:?}"
    );
    let control = branch_control_snapshot(&harness.store, &source)
        .await
        .unwrap();
    assert_eq!(control.roots.is_empty(), control.deletion_fenced);
    assert_eq!(
        control.binding_version,
        Some(zeppelin::wal::manifest::ReceiptBindingVersion::V3Roots)
    );
    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn root_mutation_is_exact_bounded_and_never_scans_all_namespaces() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("branch-root-exact-source");
    let target = harness.artifact_origin_namespace("branch-root-exact-target");
    let other_target = harness.artifact_origin_namespace("branch-root-limit-target");
    NamespaceManager::new(harness.store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .unwrap();

    let (store, counter) = counting_store(&harness.store);
    counter.reset();
    let root = prepare_head_branch_root(
        store.clone(),
        &source,
        BranchId::new(),
        &target,
        uuid::Uuid::new_v4(),
        ForkViewDigest::new([0x71; 32]),
        Utc::now(),
    )
    .await
    .unwrap();
    insert_prepared_branch_root(store.clone(), &source, root.clone(), 1)
        .await
        .unwrap();
    let after_insert = branch_control_snapshot(&store, &source).await.unwrap();

    let retried = insert_prepared_branch_root(store.clone(), &source, root.clone(), 1)
        .await
        .unwrap();
    assert_eq!(retried, root);
    let after_retry = branch_control_snapshot(&store, &source).await.unwrap();
    assert_eq!(
        after_retry.manifest_generation,
        after_insert.manifest_generation
    );
    assert_eq!(after_retry.roots, vec![root.clone()]);

    let mut conflicting = root.clone();
    conflicting.fork_view_sha256 = ForkViewDigest::new([0x72; 32]);
    let error = insert_prepared_branch_root(store.clone(), &source, conflicting, 1)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ZeppelinError::Branch(error)
            if matches!(error.as_ref(), BranchError::BranchRootConflict { branch_id } if *branch_id == root.branch_id)
    ));

    let second = prepare_head_branch_root(
        store.clone(),
        &source,
        BranchId::new(),
        &other_target,
        uuid::Uuid::new_v4(),
        ForkViewDigest::new([0x73; 32]),
        Utc::now(),
    )
    .await
    .unwrap();
    let error = insert_prepared_branch_root(store.clone(), &source, second.clone(), 1)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ZeppelinError::Branch(error)
            if matches!(error.as_ref(), BranchError::BranchRootLimitExceeded { limit: 1 })
    ));

    let mut config_mismatch = second;
    config_mismatch.source_config_sha256 = SourceDataPlaneConfigDigest::new([0xff; 32]);
    let error = insert_prepared_branch_root(store.clone(), &source, config_mismatch, 2)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ZeppelinError::Branch(error)
            if matches!(error.as_ref(), BranchError::BranchRootInvalid { .. })
    ));

    let destruction_record_key =
        format!("_audit/destruction/{}.json", uuid::Uuid::new_v4().simple());
    let error = publish_deletion_fence(store.clone(), &source, &destruction_record_key)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ZeppelinError::Branch(error)
            if matches!(error.as_ref(), BranchError::NamespaceHasLiveBranches { .. })
    ));
    assert_eq!(
        branch_control_snapshot(&store, &source)
            .await
            .unwrap()
            .roots,
        vec![root]
    );
    assert_eq!(counter.list_calls_for_prefix(""), 0);
    assert_eq!(counter.delimiter_list_calls_for_prefix(""), 0);

    let fenced_source = harness.artifact_origin_namespace("branch-root-fenced-source");
    let fenced_target = harness.artifact_origin_namespace("branch-root-fenced-target");
    NamespaceManager::new(harness.store.clone())
        .create(&fenced_source, 4, DistanceMetric::Cosine)
        .await
        .unwrap();
    let prepared_before_fence = prepare_head_branch_root(
        harness.store.clone(),
        &fenced_source,
        BranchId::new(),
        &fenced_target,
        uuid::Uuid::new_v4(),
        ForkViewDigest::new([0x74; 32]),
        Utc::now(),
    )
    .await
    .unwrap();
    let destruction_record_key =
        format!("_audit/destruction/{}.json", uuid::Uuid::new_v4().simple());
    publish_deletion_fence(
        harness.store.clone(),
        &fenced_source,
        &destruction_record_key,
    )
    .await
    .unwrap();
    let error = insert_prepared_branch_root(
        harness.store.clone(),
        &fenced_source,
        prepared_before_fence,
        1,
    )
    .await
    .unwrap_err();
    assert!(matches!(error, ZeppelinError::NamespaceDeleting { .. }));

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness
        .cleanup_artifact_origin_namespace(&other_target)
        .await;
    harness
        .cleanup_artifact_origin_namespace(&fenced_source)
        .await;
    harness
        .cleanup_artifact_origin_namespace(&fenced_target)
        .await;
    harness.cleanup().await;
}

#[tokio::test]
async fn stale_manifest_fencing_token_rejects_root_without_publication() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("branch-root-stale-fence-source");
    let target = harness.artifact_origin_namespace("branch-root-stale-fence-target");
    NamespaceManager::new(harness.store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .unwrap();
    publish_manifest_fencing_token(&harness.store, &source, 9)
        .await
        .unwrap();
    let root = prepare_head_branch_root(
        harness.store.clone(),
        &source,
        BranchId::new(),
        &target,
        uuid::Uuid::new_v4(),
        ForkViewDigest::new([0x81; 32]),
        Utc::now(),
    )
    .await
    .unwrap();
    let expected_generation = root.source_generation.get();

    let error = insert_prepared_branch_root(harness.store.clone(), &source, root, 1)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ZeppelinError::FencingTokenStale {
            manifest_token: 9,
            ..
        }
    ));
    let control = branch_control_snapshot(&harness.store, &source)
        .await
        .unwrap();
    assert!(control.roots.is_empty());
    assert_eq!(control.manifest_generation, expected_generation);
    assert_eq!(control.fencing_token, 9);

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn root_is_query_inert_and_survives_normal_manifest_publishers() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("branch-root-publishers-source");
    let target = harness.artifact_origin_namespace("branch-root-publishers-target");
    let manager = NamespaceManager::new(harness.store.clone());
    manager
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .unwrap();
    let writer = WalWriter::new(harness.store.clone());
    let wal_reader = WalReader::new(harness.store.clone());
    let vectors = random_vectors(24, 4);
    let query = vectors[0].values.clone();
    writer.append(&source, vectors, Vec::new()).await.unwrap();

    let before_manifest = Manifest::read(&harness.store, &source)
        .await
        .unwrap()
        .unwrap();
    let before_query = execute_query(QueryParams {
        store: &harness.store,
        wal_reader: &wal_reader,
        namespace: &source,
        query: &query,
        top_k: 8,
        nprobe: 4,
        filter: None,
        consistency: ConsistencyLevel::Strong,
        distance_metric: DistanceMetric::Cosine,
        oversample_factor: 3,
        rerank_coalesce_gap_bytes: DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: None,
        include_attributes: true,
    })
    .await
    .unwrap();
    let before_results = before_query
        .results
        .iter()
        .map(|result| (result.id.clone(), result.score.to_bits()))
        .collect::<Vec<_>>();

    let root = prepare_head_branch_root(
        harness.store.clone(),
        &source,
        BranchId::new(),
        &target,
        uuid::Uuid::new_v4(),
        ForkViewDigest::new([0x91; 32]),
        Utc::now(),
    )
    .await
    .unwrap();
    insert_prepared_branch_root(harness.store.clone(), &source, root.clone(), 8)
        .await
        .unwrap();

    let after_root_manifest = Manifest::read(&harness.store, &source)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after_root_manifest.vector_count(),
        before_manifest.vector_count()
    );
    let after_root_query = execute_query(QueryParams {
        store: &harness.store,
        wal_reader: &wal_reader,
        namespace: &source,
        query: &query,
        top_k: 8,
        nprobe: 4,
        filter: None,
        consistency: ConsistencyLevel::Strong,
        distance_metric: DistanceMetric::Cosine,
        oversample_factor: 3,
        rerank_coalesce_gap_bytes: DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: None,
        include_attributes: true,
    })
    .await
    .unwrap();
    assert_eq!(
        after_root_query
            .results
            .iter()
            .map(|result| (result.id.clone(), result.score.to_bits()))
            .collect::<Vec<_>>(),
        before_results
    );

    writer
        .append(
            &source,
            vec![VectorEntry {
                id: "after-root".to_string(),
                values: vec![0.25, -0.5, 0.75, -1.0],
                attributes: None,
            }],
            Vec::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        branch_control_snapshot(&harness.store, &source)
            .await
            .unwrap()
            .roots,
        vec![root.clone()]
    );

    manager.record_compaction_success(&source).await.unwrap();
    assert_eq!(
        branch_control_snapshot(&harness.store, &source)
            .await
            .unwrap()
            .roots,
        vec![root.clone()]
    );

    let compacted = test_compactor(&harness.store)
        .compact(&source)
        .await
        .unwrap();
    assert_eq!(compacted.vectors_compacted, 25);
    let gc_delete_id = ulid::Ulid::from_parts(
        (Utc::now() - chrono::Duration::minutes(10)).timestamp_millis() as u64,
        0x95,
    );
    let gc_delete_key = WalFragment::s3_key(&source, &gc_delete_id);
    harness
        .store
        .put(
            &gc_delete_key,
            Bytes::from_static(b"unrooted pending-delete publisher fixture"),
        )
        .await
        .unwrap();
    let (mut pending_manifest, pending_version) = Manifest::read_versioned(&harness.store, &source)
        .await
        .unwrap()
        .unwrap();
    pending_manifest.pending_deletes.push(gc_delete_key.clone());
    pending_manifest
        .write_conditional(&harness.store, &source, &pending_version)
        .await
        .unwrap();
    harness
        .store
        .delete(&Manifest::history_key(&source, pending_manifest.version()))
        .await
        .unwrap();
    let before_gc = Manifest::read(&harness.store, &source)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !before_gc.pending_deletes.is_empty(),
        "compaction must exercise the pending-delete publisher"
    );
    let before_gc_generation = before_gc.version();
    let before_gc_pending = before_gc.pending_deletes.len();
    let gc_report = run_gc_cycle(&harness.store, &source, &immediate_gc())
        .await
        .unwrap();
    assert!(
        gc_report.pending_deletes_pruned > 0,
        "GC must exercise the pending-delete manifest CAS"
    );
    assert_s3_object_not_exists(&harness.store, &gc_delete_key).await;
    let after_gc = Manifest::read(&harness.store, &source)
        .await
        .unwrap()
        .unwrap();
    assert!(
        after_gc.version() > before_gc_generation,
        "pending-delete cleanup must publish a newer manifest generation"
    );
    assert!(
        after_gc.pending_deletes.len() < before_gc_pending,
        "pending-delete cleanup must shrink the authoritative queue"
    );
    let final_control = branch_control_snapshot(&harness.store, &source)
        .await
        .unwrap();
    assert_eq!(final_control.roots, vec![root]);
    assert_eq!(
        final_control.binding_version,
        Some(zeppelin::wal::manifest::ReceiptBindingVersion::V3Roots)
    );

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn signed_receipt_upgrade_preserves_exact_branch_roots() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("branch-root-receipt-upgrade-source");
    let target = harness.artifact_origin_namespace("branch-root-receipt-upgrade-target");
    let mut security_config = Config::default();
    let (security, _adapter, _bearer) =
        test_security_runtime(&harness.store, &mut security_config, &Clock::system()).await;
    security
        .install_object_signer(&harness.store)
        .expect("receipt signer must install on the test store");

    NamespaceManager::new(harness.store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .unwrap();
    let root = prepare_head_branch_root(
        harness.store.clone(),
        &source,
        BranchId::new(),
        &target,
        uuid::Uuid::new_v4(),
        ForkViewDigest::new([0x94; 32]),
        Utc::now(),
    )
    .await
    .unwrap();
    insert_prepared_branch_root(harness.store.clone(), &source, root.clone(), 8)
        .await
        .unwrap();

    let rooted = Manifest::read(&harness.store, &source)
        .await
        .unwrap()
        .unwrap();
    let rooted_generation = rooted.version();
    let mut upgrade_fixture = serde_json::to_value(rooted).unwrap();
    let object = upgrade_fixture.as_object_mut().unwrap();
    for field in [
        "artifact_hashes",
        "merkle_root",
        "root_signature",
        "root_signer_node",
    ] {
        assert!(
            object.remove(field).is_some(),
            "signed root fixture must contain {field}"
        );
    }
    harness
        .store
        .put(
            &Manifest::s3_key(&source),
            Bytes::from(serde_json::to_vec(&upgrade_fixture).unwrap()),
        )
        .await
        .unwrap();
    harness
        .store
        .delete(&Manifest::history_key(&source, rooted_generation))
        .await
        .unwrap();

    let upgraded = test_compactor(&harness.store)
        .compact(&source)
        .await
        .unwrap();
    assert_eq!(upgraded.vectors_compacted, 0);
    let manifest = Manifest::read(&harness.store, &source)
        .await
        .unwrap()
        .unwrap();
    assert!(manifest.version() > rooted_generation);
    let control = branch_control_snapshot(&harness.store, &source)
        .await
        .unwrap();
    assert_eq!(control.roots, vec![root]);
    assert_eq!(
        control.binding_version,
        Some(zeppelin::wal::manifest::ReceiptBindingVersion::V3Roots)
    );
    assert!(manifest.control_state_digest().is_some());
    assert!(manifest.root_signer_node().is_some());

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn root_growth_between_gc_mark_and_sweep_prevents_physical_delete() {
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("branch-root-gc-race-source");
    let target = harness.artifact_origin_namespace("branch-root-gc-race-target");
    NamespaceManager::new(harness.store.clone())
        .create(&source, 4, DistanceMetric::Cosine)
        .await
        .unwrap();

    let old_id = ulid::Ulid::from_parts(
        (Utc::now() - chrono::Duration::minutes(10)).timestamp_millis() as u64,
        0x92,
    );
    let orphan_key = WalFragment::s3_key(&source, &old_id);
    harness
        .store
        .put(
            &orphan_key,
            Bytes::from_static(b"unreachable branch-race artifact"),
        )
        .await
        .unwrap();

    let mut gc = immediate_gc();
    gc.horizon_secs = 1;
    let first_now = Utc::now();
    let first = run_gc_cycle_at(&harness.store, &source, &gc, first_now)
        .await
        .unwrap();
    assert_eq!(first.objects_deleted, 0, "first pass must only mark");
    assert!(load_gc_candidates(&harness.store, &source)
        .await
        .unwrap()
        .iter()
        .any(|candidate| candidate.key == orphan_key));

    let (mut advanced, version) = Manifest::read_versioned(&harness.store, &source)
        .await
        .unwrap()
        .unwrap();
    advanced.updated_at += chrono::Duration::seconds(1);
    advanced
        .write_conditional(&harness.store, &source, &version)
        .await
        .unwrap();

    let (paused_store, pause) =
        pause_first_after_get_matching(&harness.store, gc_candidate_store_key(&source));
    let gc_source = source.clone();
    let second_gc = gc.clone();
    let gc_task = tokio::spawn(async move {
        run_gc_cycle_at(
            &paused_store,
            &gc_source,
            &second_gc,
            first_now + chrono::Duration::seconds(2),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(30), pause.wait_until_paused())
        .await
        .expect("GC must reach the durable mark boundary");

    let root = prepare_head_branch_root(
        harness.store.clone(),
        &source,
        BranchId::new(),
        &target,
        uuid::Uuid::new_v4(),
        ForkViewDigest::new([0x93; 32]),
        Utc::now(),
    )
    .await
    .unwrap();
    insert_prepared_branch_root(harness.store.clone(), &source, root.clone(), 8)
        .await
        .unwrap();
    pause.release();

    let raced = gc_task.await.unwrap().unwrap();
    assert_eq!(
        raced.objects_deleted, 0,
        "changed root authority must abort sweep before physical deletion"
    );
    assert_s3_object_exists(&harness.store, &orphan_key).await;
    assert_eq!(
        branch_control_snapshot(&harness.store, &source)
            .await
            .unwrap()
            .roots,
        vec![root]
    );

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}
