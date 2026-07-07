mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use ulid::Ulid;
use zeppelin::cache::manifest_cache::ManifestCache;
use zeppelin::cache::DiskCache;
use zeppelin::compaction::background::{compaction_loop, CompactionLoopOptions};
use zeppelin::compaction::gc::{load_gc_candidates, run_gc_cycle};
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, GcConfig, IndexingConfig};
use zeppelin::namespace::NamespaceManager;
use zeppelin::types::DistanceMetric;
use zeppelin::wal::fragment::WalFragment;
use zeppelin::wal::manifest::{FragmentRef, Manifest};
use zeppelin::wal::{LeaseManager, WalReader};

use common::assertions::{assert_s3_object_exists, assert_s3_object_not_exists};
use common::counting::counting_store;
use common::harness::TestHarness;

fn unsafe_short_gc(horizon_secs: u64) -> GcConfig {
    GcConfig {
        horizon_secs,
        compaction_upload_window_secs: 1,
        skew_slop_secs: 0,
        allow_unsafe_short_horizon: true,
        ..GcConfig::default()
    }
}

fn old_ulid(seconds_ago: i64, entropy: u128) -> Ulid {
    let ts = (Utc::now() - chrono::Duration::seconds(seconds_ago))
        .timestamp_millis()
        .try_into()
        .expect("test timestamp must be after epoch");
    Ulid::from_parts(ts, entropy)
}

#[tokio::test]
async fn gc_cycle_deletes_then_prunes_pending_deletes() {
    let harness = TestHarness::new().await;
    let ns = harness.key("storage-gc-pending");
    let store = harness.store.clone();
    let pending_id = old_ulid(60, 19);
    let pending_key = WalFragment::s3_key(&ns, &pending_id);

    store
        .put(&pending_key, Bytes::from_static(b"pending delete body"))
        .await
        .unwrap();
    let mut manifest = Manifest::new();
    manifest.pending_deletes.push(pending_key.clone());
    manifest.write(&store, &ns).await.unwrap();
    // Legacy namespaces created before manifest history have no retained
    // snapshot pinning pending-delete keys.
    store.delete(&Manifest::history_key(&ns, 1)).await.unwrap();

    run_gc_cycle(&store, &ns, &unsafe_short_gc(0))
        .await
        .unwrap();

    assert_s3_object_not_exists(&store, &pending_key).await;
    let manifest = Manifest::read(&store, &ns).await.unwrap().unwrap();
    assert!(
        manifest.pending_deletes.is_empty(),
        "pending_deletes must be pruned only after the object is deleted"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_cycle_keeps_pending_delete_entry_when_delete_fails() {
    let harness = TestHarness::new().await;
    let ns = harness.key("storage-gc-pending-failure");
    let store = harness.store.clone();
    let invalid_key = format!("{ns}/wal//invalid.wal");

    let mut manifest = Manifest::new();
    manifest.pending_deletes.push(invalid_key.clone());
    manifest.write(&store, &ns).await.unwrap();

    let report = run_gc_cycle(&store, &ns, &unsafe_short_gc(0))
        .await
        .unwrap();

    assert_eq!(report.pending_deletes_pruned, 0);
    assert_eq!(report.pending_deletes_retained, 1);
    let manifest = Manifest::read(&store, &ns).await.unwrap().unwrap();
    assert_eq!(
        manifest.pending_deletes,
        vec![invalid_key],
        "failed pending-delete entries must remain queued for retry"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_cycle_retains_pending_deletes_inside_horizon() {
    let harness = TestHarness::new().await;
    let ns = harness.key("storage-gc-pending-horizon");
    let store = harness.store.clone();
    let pending_id = old_ulid(60, 39);
    let pending_key = WalFragment::s3_key(&ns, &pending_id);

    store
        .put(&pending_key, Bytes::from_static(b"pending delete body"))
        .await
        .unwrap();
    let mut manifest = Manifest::new();
    manifest.pending_deletes.push(pending_key.clone());
    manifest.write(&store, &ns).await.unwrap();

    let report = run_gc_cycle(&store, &ns, &unsafe_short_gc(60))
        .await
        .unwrap();

    assert_eq!(report.pending_deletes_deleted, 0);
    assert_eq!(report.pending_deletes_pruned, 0);
    assert_eq!(report.pending_deletes_retained, 1);
    assert_s3_object_exists(&store, &pending_key).await;
    let manifest = Manifest::read(&store, &ns).await.unwrap().unwrap();
    assert_eq!(manifest.pending_deletes, vec![pending_key]);

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_cycle_retains_objects_referenced_only_by_manifest_history() {
    let harness = TestHarness::new().await;
    let ns = harness.key("storage-gc-history-mark-sweep");
    let store = harness.store.clone();
    let old_id = old_ulid(60, 49);
    let old_key = WalFragment::s3_key(&ns, &old_id);

    store
        .put(&old_key, Bytes::from_static(b"history-only fragment body"))
        .await
        .unwrap();
    let mut manifest = Manifest::new();
    manifest.add_fragment(FragmentRef {
        id: old_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 26,
    });
    manifest.write(&store, &ns).await.unwrap();

    let (mut current, etag) = Manifest::read_versioned(&store, &ns)
        .await
        .unwrap()
        .unwrap();
    current.fragments.clear();
    current.updated_at = Utc::now();
    current.write_conditional(&store, &ns, &etag).await.unwrap();

    run_gc_cycle(&store, &ns, &unsafe_short_gc(0))
        .await
        .unwrap();
    run_gc_cycle(&store, &ns, &unsafe_short_gc(0))
        .await
        .unwrap();

    assert_s3_object_exists(&store, &old_key).await;
    assert!(
        load_gc_candidates(&store, &ns)
            .await
            .unwrap()
            .iter()
            .all(|candidate| candidate.key != old_key),
        "history-referenced objects must not enter the candidate ledger"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_cycle_retains_pending_deletes_referenced_by_manifest_history() {
    let harness = TestHarness::new().await;
    let ns = harness.key("storage-gc-history-pending");
    let store = harness.store.clone();
    let old_id = old_ulid(60, 59);
    let old_key = WalFragment::s3_key(&ns, &old_id);

    store
        .put(&old_key, Bytes::from_static(b"history pending-delete body"))
        .await
        .unwrap();
    let mut manifest = Manifest::new();
    manifest.add_fragment(FragmentRef {
        id: old_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 27,
    });
    manifest.write(&store, &ns).await.unwrap();

    let (mut current, etag) = Manifest::read_versioned(&store, &ns)
        .await
        .unwrap()
        .unwrap();
    current.fragments.clear();
    current.pending_deletes.push(old_key.clone());
    current.updated_at = Utc::now();
    current.write_conditional(&store, &ns, &etag).await.unwrap();

    let report = run_gc_cycle(&store, &ns, &unsafe_short_gc(0))
        .await
        .unwrap();

    assert_eq!(report.pending_deletes_deleted, 0);
    assert_eq!(report.pending_deletes_pruned, 0);
    assert_eq!(report.pending_deletes_retained, 1);
    assert_s3_object_exists(&store, &old_key).await;
    let manifest = Manifest::read(&store, &ns).await.unwrap().unwrap();
    assert_eq!(manifest.pending_deletes, vec![old_key]);

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_cycle_reads_retained_manifest_history_once() {
    let harness = TestHarness::new().await;
    let ns = harness.key("storage-gc-history-cost");
    let store = harness.store.clone();
    let history_snapshots = 4;
    let history_prefix = Manifest::history_prefix(&ns);
    let old_id = old_ulid(60, 69);
    let old_key = WalFragment::s3_key(&ns, &old_id);
    let orphan_id = old_ulid(60, 79);
    let orphan_key = WalFragment::s3_key(&ns, &orphan_id);

    store
        .put(&old_key, Bytes::from_static(b"history reachable body"))
        .await
        .unwrap();
    store
        .put(&orphan_key, Bytes::from_static(b"true orphan body"))
        .await
        .unwrap();

    let mut manifest = Manifest::new();
    manifest.add_fragment(FragmentRef {
        id: old_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 22,
    });
    manifest.write(&store, &ns).await.unwrap();

    for _ in 1..history_snapshots {
        let (mut current, etag) = Manifest::read_versioned(&store, &ns)
            .await
            .unwrap()
            .unwrap();
        current.fragments.clear();
        current.pending_deletes = vec![old_key.clone()];
        current.updated_at = Utc::now();
        current.write_conditional(&store, &ns, &etag).await.unwrap();
    }

    let (counted_store, counter) = counting_store(&store);
    let report = run_gc_cycle(
        &counted_store,
        &ns,
        &GcConfig {
            manifest_history_keep_count: history_snapshots,
            ..unsafe_short_gc(0)
        },
    )
    .await
    .unwrap();

    assert_eq!(report.pending_deletes_deleted, 0);
    assert_eq!(report.pending_deletes_pruned, 0);
    assert_eq!(report.pending_deletes_retained, 1);
    assert_eq!(report.objects_deleted, 1);
    assert_s3_object_exists(&counted_store, &old_key).await;
    assert_s3_object_not_exists(&counted_store, &orphan_key).await;
    let manifest = Manifest::read(&counted_store, &ns).await.unwrap().unwrap();
    assert_eq!(manifest.pending_deletes, vec![old_key]);

    assert_eq!(
        counter.gets_matching(&history_prefix),
        history_snapshots as u64,
        "one GC cycle must GET each retained history snapshot at most once"
    );
    assert!(
        counter.list_calls_for_prefix(&history_prefix) <= 2,
        "one GC cycle should list retained history only for pruning and one shared reachability pass"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn background_loop_runs_gc_for_active_namespaces() {
    let harness = TestHarness::new().await;
    let ns = format!("{}-storage-gc-background", harness.prefix);
    let store = harness.store.clone();
    let namespace_manager = Arc::new(NamespaceManager::new(store.clone()));
    namespace_manager
        .create(&ns, 4, DistanceMetric::Euclidean)
        .await
        .unwrap();

    let orphan_id = old_ulid(60, 29);
    let orphan_key = WalFragment::s3_key(&ns, &orphan_id);
    store
        .put(&orphan_key, Bytes::from_static(b"orphan fragment body"))
        .await
        .unwrap();
    assert_s3_object_exists(&store, &orphan_key).await;

    let compactor = Arc::new(Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        CompactionConfig {
            interval_secs: 1,
            max_wal_fragments_before_compact: 1000,
            ..Default::default()
        },
        IndexingConfig::default(),
        Duration::from_secs(1),
    ));
    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_millis(10)));
    let lease_manager = Arc::new(LeaseManager::new(
        store.clone(),
        "storage-gc-test".to_string(),
        Duration::from_secs(5),
    ));
    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let handle = {
        let compactor = compactor.clone();
        let namespace_manager = namespace_manager.clone();
        let manifest_cache = manifest_cache.clone();
        let lease_manager = lease_manager.clone();
        let cache = cache.clone();
        let namespace_prefix = Some(harness.prefix.clone());
        tokio::spawn(async move {
            compaction_loop(
                compactor,
                namespace_manager,
                shutdown_rx,
                manifest_cache,
                lease_manager,
                cache,
                CompactionLoopOptions {
                    gc_config: unsafe_short_gc(0),
                    namespace_prefix,
                },
            )
            .await;
        })
    };

    for _ in 0..16 {
        if !store.exists(&orphan_key).await.unwrap() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let _ = shutdown_tx.send(true);
    handle.await.unwrap();

    assert_s3_object_not_exists(&store, &orphan_key).await;

    store.delete_prefix(&format!("{ns}/")).await.unwrap();
    harness.cleanup().await;
}
