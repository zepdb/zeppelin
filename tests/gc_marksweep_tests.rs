mod common;

use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use ulid::Ulid;
use zeppelin::compaction::gc::{load_gc_candidates, run_gc_cycle};
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, GcConfig, IndexingConfig};
use zeppelin::error::ZeppelinError;
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::VectorEntry;
use zeppelin::wal::fragment::WalFragment;
use zeppelin::wal::manifest::{FragmentRef, Manifest};
use zeppelin::wal::{WalReader, WalWriter};

use common::assertions::{assert_s3_object_exists, assert_s3_object_not_exists};
use common::harness::TestHarness;
use common::vectors::random_vectors;

fn unsafe_short_gc(horizon_secs: u64) -> GcConfig {
    GcConfig {
        horizon_secs,
        compaction_upload_window_secs: 1,
        skew_slop_secs: 0,
        allow_unsafe_short_horizon: true,
        ..GcConfig::default()
    }
}

fn prefixed_vectors(prefix: &str, n: usize, dims: usize) -> Vec<VectorEntry> {
    random_vectors(n, dims)
        .into_iter()
        .enumerate()
        .map(|(i, mut vector)| {
            vector.id = format!("{prefix}_{i}");
            vector
        })
        .collect()
}

fn old_ulid(seconds_ago: i64, entropy: u128) -> Ulid {
    let ts = (Utc::now() - chrono::Duration::seconds(seconds_ago))
        .timestamp_millis()
        .try_into()
        .expect("test timestamp must be after epoch");
    Ulid::from_parts(ts, entropy)
}

async fn orphan_segment_keys_after_upload_abort(
    store: &ZeppelinStore,
    namespace: &str,
) -> Vec<String> {
    let mut compactor = Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        CompactionConfig {
            max_wal_fragments_before_compact: 1,
            ..Default::default()
        },
        IndexingConfig {
            default_num_centroids: 2,
            kmeans_max_iterations: 5,
            ..Default::default()
        },
        Duration::from_secs(1),
    );
    compactor.set_test_pre_cas_delay(Duration::from_millis(1200));
    let result = compactor.compact(namespace).await;
    assert!(
        result.is_err(),
        "test setup should abort compaction after upload but before manifest CAS"
    );
    let keys = store
        .list_prefix(&format!("{namespace}/segments/"))
        .await
        .unwrap();
    assert!(
        !keys.is_empty(),
        "test setup must leave uploaded segment objects orphaned"
    );
    keys
}

async fn wait_for_segment_uploads(store: &ZeppelinStore, namespace: &str) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let keys = store
            .list_prefix(&format!("{namespace}/segments/"))
            .await
            .unwrap();
        if !keys.is_empty() {
            return keys;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for compaction segment uploads"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn gc_reclaims_orphaned_segment_objects_only_after_horizon() {
    let harness = TestHarness::new().await;
    let ns = harness.key("gc-19d-orphan-segment");
    let store = harness.store.clone();
    common::write_active_namespace_metadata(
        &store,
        &ns,
        16,
        zeppelin::types::DistanceMetric::Euclidean,
    )
    .await;
    Manifest::new().write(&store, &ns).await.unwrap();
    WalWriter::new(store.clone())
        .append(&ns, prefixed_vectors("segment_orphan", 24, 16), vec![])
        .await
        .unwrap();

    let orphan_keys = orphan_segment_keys_after_upload_abort(&store, &ns).await;
    let config = unsafe_short_gc(1);

    let first = run_gc_cycle(&store, &ns, &config).await.unwrap();
    assert_eq!(first.objects_deleted, 0, "first pass must only mark");
    for key in &orphan_keys {
        assert_s3_object_exists(&store, key).await;
    }
    assert!(
        !load_gc_candidates(&store, &ns).await.unwrap().is_empty(),
        "first pass must persist unreachable candidates"
    );

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let second = run_gc_cycle(&store, &ns, &config).await.unwrap();
    assert!(
        second.objects_deleted >= orphan_keys.len(),
        "second pass after horizon should delete orphan segment objects"
    );
    for key in &orphan_keys {
        assert_s3_object_not_exists(&store, key).await;
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_reclaims_full_segment_orphan_after_compaction_manifest_bump() {
    let harness = TestHarness::new().await;
    let ns = harness.key("gc-19d-compaction-bump");
    let store = harness.store.clone();
    common::write_active_namespace_metadata(
        &store,
        &ns,
        16,
        zeppelin::types::DistanceMetric::Euclidean,
    )
    .await;
    Manifest::new().write(&store, &ns).await.unwrap();
    WalWriter::new(store.clone())
        .append(&ns, prefixed_vectors("manifest_bump", 24, 16), vec![])
        .await
        .unwrap();

    let mut compactor = Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        CompactionConfig {
            max_wal_fragments_before_compact: 1,
            ..Default::default()
        },
        IndexingConfig {
            default_num_centroids: 2,
            kmeans_max_iterations: 5,
            ..Default::default()
        },
        Duration::from_secs(30),
    );
    compactor.set_test_pre_cas_delay(Duration::from_millis(500));

    let compaction = {
        let ns = ns.clone();
        tokio::spawn(async move { compactor.compact_with_lease(&ns, Some(1)).await })
    };
    let orphan_keys = wait_for_segment_uploads(&store, &ns).await;

    let mut bumped_manifest = Manifest::read(&store, &ns).await.unwrap().unwrap();
    bumped_manifest.fencing_token = 2;
    bumped_manifest.write(&store, &ns).await.unwrap();

    let result = compaction.await.unwrap();
    assert!(
        matches!(result, Err(ZeppelinError::FencingTokenStale { .. })),
        "manifest fencing bump should abort compaction after upload, got {result:?}"
    );
    for key in &orphan_keys {
        assert_s3_object_exists(&store, key).await;
    }

    let config = unsafe_short_gc(1);
    let first = run_gc_cycle(&store, &ns, &config).await.unwrap();
    assert_eq!(first.objects_deleted, 0, "first pass must only mark");
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let second = run_gc_cycle(&store, &ns, &config).await.unwrap();
    assert!(
        second.objects_deleted >= orphan_keys.len(),
        "second pass after horizon should delete segment objects orphaned by compaction failure"
    );
    for key in &orphan_keys {
        assert_s3_object_not_exists(&store, key).await;
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_reclaims_orphaned_fragment_from_failed_write_cas() {
    let harness = TestHarness::new().await;
    let ns = harness.key("gc-19d-orphan-fragment");
    let store = harness.store.clone();
    Manifest::new().write(&store, &ns).await.unwrap();

    let orphan_id = old_ulid(60, 42);
    let orphan_key = WalFragment::s3_key(&ns, &orphan_id);
    store
        .put(&orphan_key, Bytes::from_static(b"orphan fragment bytes"))
        .await
        .unwrap();

    let config = unsafe_short_gc(1);
    run_gc_cycle(&store, &ns, &config).await.unwrap();
    assert_s3_object_exists(&store, &orphan_key).await;

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let report = run_gc_cycle(&store, &ns, &config).await.unwrap();
    assert_eq!(report.objects_deleted, 1);
    assert_s3_object_not_exists(&store, &orphan_key).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn old_fragment_that_just_left_manifest_is_not_collected_before_horizon() {
    let harness = TestHarness::new().await;
    let ns = harness.key("gc-19d-break-a");
    let store = harness.store.clone();
    let old_id = old_ulid(60, 77);
    let old_key = WalFragment::s3_key(&ns, &old_id);

    store
        .put(
            &old_key,
            Bytes::from_static(b"old but recently unreachable"),
        )
        .await
        .unwrap();
    let mut manifest = Manifest::new();
    manifest.add_fragment(FragmentRef {
        id: old_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 28,
    });
    manifest.write(&store, &ns).await.unwrap();

    Manifest::new().write(&store, &ns).await.unwrap();
    let config = unsafe_short_gc(2);

    run_gc_cycle(&store, &ns, &config).await.unwrap();
    run_gc_cycle(&store, &ns, &config).await.unwrap();
    assert_s3_object_exists(&store, &old_key).await;

    harness.cleanup().await;
}
