mod common;

use bytes::Bytes;
use chrono::Utc;
use zeppelin::compaction::gc::{drain_pending_deletes_at, run_gc_cycle};
use zeppelin::config::GcConfig;
use zeppelin::error::ZeppelinError;
use zeppelin::namespace::branching::{ArtifactOrigin, ArtifactOriginIndex};
use zeppelin::wal::{LateStateSection, Manifest, ManifestSectionRef};

use common::assertions::{assert_s3_object_exists, assert_s3_object_not_exists};
use common::harness::TestHarness;

fn require_minio() {
    assert_eq!(
        std::env::var("TEST_BACKEND").as_deref(),
        Ok("minio"),
        "late-section CAS and GC coverage requires TEST_BACKEND=minio"
    );
}

fn unsafe_short_gc(horizon_secs: u64) -> GcConfig {
    GcConfig {
        horizon_secs,
        compaction_upload_window_secs: 1,
        skew_slop_secs: 0,
        allow_unsafe_short_horizon: true,
        ..GcConfig::default()
    }
}

fn reference(namespace: &str, bytes: &[u8]) -> ManifestSectionRef {
    let checksum = LateStateSection::checksum(bytes);
    ManifestSectionRef {
        key: LateStateSection::s3_key(namespace, &checksum),
        checksum,
        size_bytes: bytes.len() as u64,
        format_version: 1,
        artifact_origin: None,
    }
}

async fn create_manifest(
    harness: &TestHarness,
    namespace: &str,
    incarnation: uuid::Uuid,
) -> Manifest {
    let mut manifest = Manifest::new();
    manifest
        .bind_namespace_incarnation(incarnation)
        .expect("test namespace incarnation must bind");
    manifest
        .write(&harness.store, namespace)
        .await
        .expect("initial manifest must publish");
    manifest
}

#[tokio::test]
async fn late_section_publish_supersede_history_and_gc_lifecycle() {
    require_minio();
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("late-section-lifecycle");
    let incarnation = uuid::Uuid::new_v4();
    create_manifest(&harness, &namespace, incarnation).await;

    // Artificial alternate empty-struct MessagePack bytes let empty v1 exercise
    // real supersession without adding a Phase-5 domain field or writer format.
    let artificial_bytes = Bytes::from_static(b"ZLS1\x01\x80");
    LateStateSection::from_bytes(&artificial_bytes)
        .expect("alternate empty-map representation must decode as empty v1");
    let mut artificial_ref = reference(&namespace, &artificial_bytes);
    artificial_ref.artifact_origin = Some(ArtifactOriginIndex::new(0));
    harness
        .store
        .put_create(&artificial_ref.key, artificial_bytes)
        .await
        .expect("artificial predecessor section must be create-only");

    let (mut predecessor, predecessor_version) =
        Manifest::read_versioned(&harness.store, &namespace)
            .await
            .expect("manifest read must succeed")
            .expect("manifest must exist");
    predecessor.artifact_origins =
        vec![serde_json::from_value::<ArtifactOrigin>(serde_json::json!({
            "namespace": namespace,
            "incarnation": incarnation.to_string(),
        }))
        .expect("local artifact origin fixture must decode")];
    predecessor.late_state = Some(artificial_ref.clone());
    predecessor
        .write_conditional(&harness.store, &namespace, &predecessor_version)
        .await
        .expect("artificial predecessor ref must publish");
    assert!(
        predecessor
            .late_state
            .as_ref()
            .expect("predecessor ref must remain visible")
            .artifact_origin
            .is_some(),
        "regression requires explicit local-origin indexing"
    );
    let predecessor_generation = predecessor.version();

    let (mut current, current_version) = Manifest::read_versioned(&harness.store, &namespace)
        .await
        .expect("manifest read must succeed")
        .expect("manifest must exist");
    current
        .publish_with_late_state(
            &harness.store,
            &namespace,
            &current_version,
            &LateStateSection::new(),
        )
        .await
        .expect("canonical empty v1 section must publish");

    let canonical_ref = current
        .late_state
        .clone()
        .expect("published manifest must carry a section ref");
    assert_ne!(canonical_ref.key, artificial_ref.key);
    assert_eq!(
        current
            .load_late_state(&harness.store)
            .await
            .expect("published section must load"),
        Some(LateStateSection::new())
    );
    assert!(current
        .pending_deletes
        .iter()
        .any(|key| key == &artificial_ref.key));
    assert_s3_object_exists(&harness.store, &canonical_ref.key).await;

    // The predecessor generation remains a live root until its history is
    // removed, so the first drain must retain the superseded section.
    let retained = drain_pending_deletes_at(
        &harness.store,
        &namespace,
        &unsafe_short_gc(5),
        Utc::now() + chrono::Duration::seconds(10),
    )
    .await
    .expect("history-aware pending-delete drain must succeed");
    assert_eq!(retained.objects_deleted, 0);
    assert_s3_object_exists(&harness.store, &artificial_ref.key).await;

    harness
        .store
        .delete(&Manifest::history_key(&namespace, predecessor_generation))
        .await
        .expect("predecessor history fixture must be removable");
    harness
        .store
        .delete(&Manifest::history_key(&namespace, current.version()))
        .await
        .expect("winner history fixture must be removable");

    let collected = drain_pending_deletes_at(
        &harness.store,
        &namespace,
        &unsafe_short_gc(5),
        Utc::now() + chrono::Duration::seconds(10),
    )
    .await
    .expect("positive-horizon unrooted superseded section must drain");
    assert_eq!(collected.objects_deleted, 1);
    assert_eq!(collected.entries_pruned, 1);
    assert_s3_object_not_exists(&harness.store, &artificial_ref.key).await;

    let final_manifest = Manifest::read(&harness.store, &namespace)
        .await
        .expect("final manifest read must succeed")
        .expect("final manifest must exist");
    assert!(final_manifest.pending_deletes.is_empty());
    assert_eq!(final_manifest.late_state, Some(canonical_ref));

    harness.cleanup_artifact_origin_namespace(&namespace).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn late_section_orphan_missing_and_corrupt_fail_loud() {
    require_minio();
    let harness = TestHarness::new().await;

    let orphan_namespace = harness.artifact_origin_namespace("late-section-orphan");
    create_manifest(&harness, &orphan_namespace, uuid::Uuid::new_v4()).await;
    let orphan_bytes = Bytes::from_static(b"ZLS1\x01\x80");
    let orphan_ref = reference(&orphan_namespace, &orphan_bytes);
    harness
        .store
        .put_create(&orphan_ref.key, orphan_bytes)
        .await
        .expect("orphan section must be create-only");
    run_gc_cycle(&harness.store, &orphan_namespace, &unsafe_short_gc(0))
        .await
        .expect("orphan mark cycle must succeed");
    run_gc_cycle(&harness.store, &orphan_namespace, &unsafe_short_gc(0))
        .await
        .expect("orphan sweep cycle must succeed");
    assert_s3_object_not_exists(&harness.store, &orphan_ref.key).await;

    let missing_namespace = harness.artifact_origin_namespace("late-section-missing");
    create_manifest(&harness, &missing_namespace, uuid::Uuid::new_v4()).await;
    let missing_bytes = b"ZLS1\x01\x80";
    let missing_ref = reference(&missing_namespace, missing_bytes);
    let (mut missing_manifest, missing_version) =
        Manifest::read_versioned(&harness.store, &missing_namespace)
            .await
            .expect("missing fixture manifest read must succeed")
            .expect("missing fixture manifest must exist");
    missing_manifest.late_state = Some(missing_ref);
    missing_manifest
        .write_conditional(&harness.store, &missing_namespace, &missing_version)
        .await
        .expect("missing-section reference must publish for integrity test");
    assert!(matches!(
        missing_manifest
            .load_late_state(&harness.store)
            .await
            .expect_err("missing referenced section must fail loud"),
        ZeppelinError::NotFound { .. }
    ));

    let corrupt_namespace = harness.artifact_origin_namespace("late-section-corrupt");
    create_manifest(&harness, &corrupt_namespace, uuid::Uuid::new_v4()).await;
    let expected_bytes = b"ZLS1\x01\x80";
    let corrupt_ref = reference(&corrupt_namespace, expected_bytes);
    harness
        .store
        .put_create(&corrupt_ref.key, Bytes::from_static(b"ZLS1\x01\x81"))
        .await
        .expect("corrupt fixture bytes must be create-only");
    let (mut corrupt_manifest, corrupt_version) =
        Manifest::read_versioned(&harness.store, &corrupt_namespace)
            .await
            .expect("corrupt fixture manifest read must succeed")
            .expect("corrupt fixture manifest must exist");
    corrupt_manifest.late_state = Some(corrupt_ref);
    corrupt_manifest
        .write_conditional(&harness.store, &corrupt_namespace, &corrupt_version)
        .await
        .expect("corrupt-section reference must publish for integrity test");
    let error = corrupt_manifest
        .load_late_state(&harness.store)
        .await
        .expect_err("corrupt referenced section must fail loud");
    assert!(
        error.to_string().contains("checksum mismatch"),
        "corrupt bytes must fail checksum validation: {error}"
    );

    for namespace in [&orphan_namespace, &missing_namespace, &corrupt_namespace] {
        harness.cleanup_artifact_origin_namespace(namespace).await;
    }
    harness.cleanup().await;
}
