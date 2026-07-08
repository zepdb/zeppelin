mod common;

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result as OsResult,
};
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
use zeppelin::wal::manifest::{FragmentRef, Manifest, NamedSnapshot};
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

#[derive(Debug)]
struct PutOnNthManifestReadStore {
    inner: Arc<dyn ObjectStore>,
    manifest_key: String,
    put_key: String,
    put_body: Bytes,
    trigger_read: usize,
    manifest_reads: AtomicUsize,
    puts_injected: Arc<AtomicUsize>,
}

#[derive(Clone, Debug)]
struct PutOnNthManifestReadHandle {
    puts_injected: Arc<AtomicUsize>,
}

impl PutOnNthManifestReadHandle {
    fn puts_injected(&self) -> usize {
        self.puts_injected.load(Ordering::Relaxed)
    }
}

impl PutOnNthManifestReadStore {
    fn wrap(
        inner: Arc<dyn ObjectStore>,
        manifest_key: String,
        put_key: String,
        put_body: Bytes,
        trigger_read: usize,
    ) -> (Self, PutOnNthManifestReadHandle) {
        let puts_injected = Arc::new(AtomicUsize::new(0));
        let handle = PutOnNthManifestReadHandle {
            puts_injected: Arc::clone(&puts_injected),
        };
        (
            Self {
                inner,
                manifest_key,
                put_key,
                put_body,
                trigger_read,
                manifest_reads: AtomicUsize::new(0),
                puts_injected,
            },
            handle,
        )
    }
}

impl fmt::Display for PutOnNthManifestReadStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PutOnNthManifestReadStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for PutOnNthManifestReadStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        if location.as_ref() == self.manifest_key {
            let read = self.manifest_reads.fetch_add(1, Ordering::SeqCst) + 1;
            if read == self.trigger_read {
                let put_path =
                    Path::parse(&self.put_key).map_err(|error| object_store::Error::Generic {
                        store: "put_on_nth_manifest_read",
                        source: Box::new(error),
                    })?;
                self.inner
                    .put(&put_path, PutPayload::from(self.put_body.clone()))
                    .await?;
                self.puts_injected.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn manifest_json_bytes_with_version(manifest: &Manifest, version: u64) -> Bytes {
    let mut value = serde_json::to_value(manifest).expect("manifest must serialize");
    value
        .as_object_mut()
        .expect("manifest must serialize as an object")
        .insert("version".to_string(), serde_json::json!(version));
    Bytes::from(serde_json::to_vec(&value).expect("manifest json must serialize"))
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
async fn gc_sweep_rereads_retained_history_before_deleting_candidate() {
    let harness = TestHarness::new().await;
    let ns = harness.key("storage-gc-history-sweep-race");
    let store = harness.store.clone();
    let old_id = old_ulid(60, 129);
    let old_key = WalFragment::s3_key(&ns, &old_id);

    store
        .put(&old_key, Bytes::from_static(b"history race fragment body"))
        .await
        .unwrap();

    let mut history_manifest = Manifest::new();
    history_manifest.add_fragment(FragmentRef {
        id: old_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 26,
    });
    history_manifest.write(&store, &ns).await.unwrap();

    let (mut current, etag) = Manifest::read_versioned(&store, &ns)
        .await
        .unwrap()
        .unwrap();
    current.fragments.clear();
    current.updated_at = Utc::now();
    current.write_conditional(&store, &ns, &etag).await.unwrap();

    store
        .delete(&Manifest::history_key(&ns, history_manifest.version()))
        .await
        .unwrap();

    let injected_history_version = current.version() + 1;
    let injected_history_key = Manifest::history_key(&ns, injected_history_version);
    let injected_history =
        manifest_json_bytes_with_version(&history_manifest, injected_history_version);
    let (injecting_store, injection) = PutOnNthManifestReadStore::wrap(
        store.inner(),
        Manifest::s3_key(&ns),
        injected_history_key,
        injected_history,
        3,
    );
    let injecting_store = zeppelin::storage::ZeppelinStore::new(Arc::new(injecting_store));

    let report = run_gc_cycle(&injecting_store, &ns, &unsafe_short_gc(0))
        .await
        .unwrap();

    assert_eq!(
        injection.puts_injected(),
        1,
        "test must inject retained history between mark and sweep"
    );
    assert_eq!(
        report.objects_deleted, 0,
        "sweep must not delete an object protected by retained history that appeared after mark"
    );
    assert_s3_object_exists(&injecting_store, &old_key).await;
    assert!(
        load_gc_candidates(&injecting_store, &ns)
            .await
            .unwrap()
            .iter()
            .any(|candidate| candidate.key == old_key),
        "reachable-at-sweep candidates stay recorded for a later cycle"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_pitr_time_retention_keeps_old_generation_and_artifacts() {
    let harness = TestHarness::new().await;
    let ns = harness.key("storage-gc-pitr-time");
    let store = harness.store.clone();
    let old_id = old_ulid(60, 89);
    let old_key = WalFragment::s3_key(&ns, &old_id);

    store
        .put(&old_key, Bytes::from_static(b"time retained body"))
        .await
        .unwrap();
    let mut manifest = Manifest::new();
    manifest.add_fragment(FragmentRef {
        id: old_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 18,
    });
    manifest.write(&store, &ns).await.unwrap();

    for _ in 0..2 {
        let (mut current, etag) = Manifest::read_versioned(&store, &ns)
            .await
            .unwrap()
            .unwrap();
        current.fragments.clear();
        current.updated_at = Utc::now();
        current.write_conditional(&store, &ns, &etag).await.unwrap();
    }

    run_gc_cycle(
        &store,
        &ns,
        &GcConfig {
            manifest_history_keep_count: 1,
            pitr_retention_secs: 3_600,
            ..unsafe_short_gc(0)
        },
    )
    .await
    .unwrap();
    run_gc_cycle(
        &store,
        &ns,
        &GcConfig {
            manifest_history_keep_count: 1,
            pitr_retention_secs: 3_600,
            ..unsafe_short_gc(0)
        },
    )
    .await
    .unwrap();

    assert!(
        Manifest::read_history(&store, &ns, 1)
            .await
            .unwrap()
            .is_some(),
        "generation 1 is outside keep_count=1 but inside PITR time retention"
    );
    assert_s3_object_exists(&store, &old_key).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_prunes_expired_history_and_collects_artifacts_after_horizon() {
    let harness = TestHarness::new().await;
    let ns = harness.key("storage-gc-pitr-expired");
    let store = harness.store.clone();
    let old_id = old_ulid(60, 99);
    let old_key = WalFragment::s3_key(&ns, &old_id);

    store
        .put(&old_key, Bytes::from_static(b"expired history body"))
        .await
        .unwrap();
    let mut manifest = Manifest::new();
    manifest.updated_at = Utc::now() - chrono::Duration::seconds(60);
    manifest.add_fragment(FragmentRef {
        id: old_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 20,
    });
    manifest.updated_at = Utc::now() - chrono::Duration::seconds(60);
    manifest.write(&store, &ns).await.unwrap();

    for _ in 0..2 {
        let (mut current, etag) = Manifest::read_versioned(&store, &ns)
            .await
            .unwrap()
            .unwrap();
        current.fragments.clear();
        current.updated_at = Utc::now();
        current.write_conditional(&store, &ns, &etag).await.unwrap();
    }

    let config = GcConfig {
        manifest_history_keep_count: 1,
        pitr_retention_secs: 1,
        ..unsafe_short_gc(0)
    };
    run_gc_cycle(&store, &ns, &config).await.unwrap();
    run_gc_cycle(&store, &ns, &config).await.unwrap();

    assert!(
        Manifest::read_history(&store, &ns, 1)
            .await
            .unwrap()
            .is_none(),
        "generation 1 is outside count and time retention"
    );
    assert_s3_object_not_exists(&store, &old_key).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_named_snapshot_pin_keeps_generation_until_released() {
    let harness = TestHarness::new().await;
    let ns = harness.key("storage-gc-pitr-pin");
    let store = harness.store.clone();
    let old_id = old_ulid(60, 109);
    let old_key = WalFragment::s3_key(&ns, &old_id);

    store
        .put(&old_key, Bytes::from_static(b"snapshot pinned body"))
        .await
        .unwrap();
    let mut manifest = Manifest::new();
    manifest.updated_at = Utc::now() - chrono::Duration::seconds(60);
    manifest.add_fragment(FragmentRef {
        id: old_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 20,
    });
    manifest.updated_at = Utc::now() - chrono::Duration::seconds(60);
    manifest.write(&store, &ns).await.unwrap();
    NamedSnapshot::create(&store, &ns, "before-delete", 1)
        .await
        .unwrap();

    for _ in 0..2 {
        let (mut current, etag) = Manifest::read_versioned(&store, &ns)
            .await
            .unwrap()
            .unwrap();
        current.fragments.clear();
        current.updated_at = Utc::now();
        current.write_conditional(&store, &ns, &etag).await.unwrap();
    }

    let config = GcConfig {
        manifest_history_keep_count: 1,
        pitr_retention_secs: 0,
        ..unsafe_short_gc(0)
    };
    run_gc_cycle(&store, &ns, &config).await.unwrap();
    run_gc_cycle(&store, &ns, &config).await.unwrap();
    assert!(Manifest::read_history(&store, &ns, 1)
        .await
        .unwrap()
        .is_some());
    assert_s3_object_exists(&store, &old_key).await;

    NamedSnapshot::delete(&store, &ns, "before-delete")
        .await
        .unwrap();
    run_gc_cycle(&store, &ns, &config).await.unwrap();
    run_gc_cycle(&store, &ns, &config).await.unwrap();
    assert!(Manifest::read_history(&store, &ns, 1)
        .await
        .unwrap()
        .is_none());
    assert_s3_object_not_exists(&store, &old_key).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_snapshot_pin_does_not_retain_unreferenced_pending_delete() {
    let harness = TestHarness::new().await;
    let ns = harness.key("storage-gc-pitr-pin-pending");
    let store = harness.store.clone();
    let pending_id = old_ulid(60, 119);
    let pending_key = WalFragment::s3_key(&ns, &pending_id);

    store
        .put(
            &pending_key,
            Bytes::from_static(b"unreferenced pending body"),
        )
        .await
        .unwrap();
    Manifest::new().write(&store, &ns).await.unwrap();
    NamedSnapshot::create(&store, &ns, "empty", 1)
        .await
        .unwrap();

    let (mut current, etag) = Manifest::read_versioned(&store, &ns)
        .await
        .unwrap()
        .unwrap();
    current.pending_deletes.push(pending_key.clone());
    current.updated_at = Utc::now();
    current.write_conditional(&store, &ns, &etag).await.unwrap();
    store
        .delete(&Manifest::history_key(&ns, current.version()))
        .await
        .unwrap();

    let report = run_gc_cycle(
        &store,
        &ns,
        &GcConfig {
            manifest_history_keep_count: 1,
            pitr_retention_secs: 0,
            ..unsafe_short_gc(0)
        },
    )
    .await
    .unwrap();

    assert_eq!(report.pending_deletes_deleted, 1);
    assert_eq!(report.pending_deletes_pruned, 1);
    assert_s3_object_not_exists(&store, &pending_key).await;

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
async fn gc_cycle_rereads_retained_manifest_history_before_sweep() {
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
        (history_snapshots * 2) as u64,
        "one GC cycle reads retained history once for mark and once before sweep"
    );
    assert!(
        counter.list_calls_for_prefix(&history_prefix) <= 2,
        "one GC cycle should list retained history only for pruning and sweep revalidation"
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
