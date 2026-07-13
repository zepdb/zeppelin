mod common;

use std::collections::BTreeSet;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result as OsResult,
};
use ulid::Ulid;
use zeppelin::cache::manifest_cache::ManifestCache;
use zeppelin::cache::DiskCache;
use zeppelin::compaction::background::{compaction_loop, CompactionLoopOptions};
use zeppelin::compaction::gc::{
    load_gc_candidates, run_gc_cycle, write_compaction_staging, GcCycleReport,
    GcNamespaceIncarnation, GcRunner,
};
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, GcConfig, IndexingConfig};
use zeppelin::namespace::NamespaceManager;
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::DistanceMetric;
use zeppelin::wal::fragment::WalFragment;
use zeppelin::wal::manifest::{FragmentRef, Manifest, NamedSnapshot};
use zeppelin::wal::{LeaseManager, WalReader};

use common::assertions::{assert_s3_object_exists, assert_s3_object_not_exists};
use common::counting::{counting_store, GetCounter};
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

#[derive(Debug)]
struct PutOnNthDeleteStore {
    inner: Arc<dyn ObjectStore>,
    delete_key: String,
    put_key: String,
    put_body: Bytes,
    trigger_delete: usize,
    deletes_seen: AtomicUsize,
    puts_injected: Arc<AtomicUsize>,
}

#[derive(Clone, Debug)]
struct PutOnNthDeleteHandle {
    puts_injected: Arc<AtomicUsize>,
}

impl PutOnNthDeleteHandle {
    fn puts_injected(&self) -> usize {
        self.puts_injected.load(Ordering::Relaxed)
    }
}

impl PutOnNthDeleteStore {
    fn wrap(
        inner: Arc<dyn ObjectStore>,
        delete_key: String,
        put_key: String,
        put_body: Bytes,
        trigger_delete: usize,
    ) -> (Self, PutOnNthDeleteHandle) {
        let puts_injected = Arc::new(AtomicUsize::new(0));
        let handle = PutOnNthDeleteHandle {
            puts_injected: Arc::clone(&puts_injected),
        };
        (
            Self {
                inner,
                delete_key,
                put_key,
                put_body,
                trigger_delete,
                deletes_seen: AtomicUsize::new(0),
                puts_injected,
            },
            handle,
        )
    }
}

impl fmt::Display for PutOnNthDeleteStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PutOnNthDeleteStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for PutOnNthDeleteStore {
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
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        if location.as_ref() == self.delete_key {
            let delete = self.deletes_seen.fetch_add(1, Ordering::SeqCst) + 1;
            if delete == self.trigger_delete {
                let put_path =
                    Path::parse(&self.put_key).map_err(|error| object_store::Error::Generic {
                        store: "put_on_nth_delete",
                        source: Box::new(error),
                    })?;
                self.inner
                    .put(&put_path, PutPayload::from(self.put_body.clone()))
                    .await?;
                self.puts_injected.fetch_add(1, Ordering::Relaxed);
            }
        }
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

/// Transparent control layer for history LIST metadata and one-shot GET faults.
///
/// The wrapped backend remains the real MinIO store supplied by [`TestHarness`].
/// Tests place [`common::counting::CountingStore`] outside this layer so every
/// attempted GET, including an injected failure, remains observable.
#[derive(Debug)]
struct LateListMutation {
    prefix: Path,
    operations: Vec<LateListOperation>,
}

#[derive(Debug)]
enum LateListOperation {
    Put { key: Path, body: Bytes },
    Delete { key: Path },
}

impl LateListMutation {
    fn prefix(&self) -> &Path {
        &self.prefix
    }

    async fn apply(self, inner: &Arc<dyn ObjectStore>) -> OsResult<()> {
        for operation in self.operations {
            match operation {
                LateListOperation::Put { key, body } => {
                    inner.put(&key, PutPayload::from(body)).await?;
                }
                LateListOperation::Delete { key } => inner.delete(&key).await?,
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct HistoryMetadataControlStore {
    inner: Arc<dyn ObjectStore>,
    strip_list_version_prefixes: Arc<Mutex<Vec<Path>>>,
    fail_next_get: Arc<Mutex<Option<String>>>,
    late_list_mutation: Arc<Mutex<Option<LateListMutation>>>,
    list_calls: Arc<AtomicUsize>,
    delete_calls: Arc<AtomicUsize>,
}

#[derive(Clone, Debug)]
struct HistoryMetadataControlHandle {
    default_strip_prefix: Path,
    strip_list_version_prefixes: Arc<Mutex<Vec<Path>>>,
    fail_next_get: Arc<Mutex<Option<String>>>,
    late_list_mutation: Arc<Mutex<Option<LateListMutation>>>,
    list_calls: Arc<AtomicUsize>,
    delete_calls: Arc<AtomicUsize>,
}

impl HistoryMetadataControlHandle {
    fn set_strip_list_versions(&self, strip: bool) {
        self.set_strip_list_versions_for_path(self.default_strip_prefix.clone(), strip);
    }

    fn set_strip_list_versions_for(&self, prefix: &str, strip: bool) {
        let prefix = Path::parse(prefix).expect("controlled LIST prefix must be valid");
        self.set_strip_list_versions_for_path(prefix, strip);
    }

    fn set_strip_list_versions_for_path(&self, prefix: Path, strip: bool) {
        let mut prefixes = self
            .strip_list_version_prefixes
            .lock()
            .expect("LIST metadata control mutex poisoned");
        if strip {
            if !prefixes.contains(&prefix) {
                prefixes.push(prefix);
            }
        } else {
            prefixes.retain(|candidate| candidate != &prefix);
        }
    }

    fn fail_next_get(&self, key: String) {
        *self
            .fail_next_get
            .lock()
            .expect("history GET fault mutex poisoned") = Some(key);
    }

    fn put_on_next_list(&self, prefix: &str, key: &str, body: Bytes) {
        let mutation = LateListMutation {
            prefix: Path::parse(prefix).expect("controlled LIST prefix must be valid"),
            operations: vec![LateListOperation::Put {
                key: Path::parse(key).expect("injected PUT key must be valid"),
                body,
            }],
        };
        self.set_late_list_mutation(mutation);
    }

    fn replace_on_next_list(&self, prefix: &str, key: &str, body: Bytes) {
        let key = Path::parse(key).expect("injected replacement key must be valid");
        let mutation = LateListMutation {
            prefix: Path::parse(prefix).expect("controlled LIST prefix must be valid"),
            operations: vec![
                LateListOperation::Delete { key: key.clone() },
                LateListOperation::Put { key, body },
            ],
        };
        self.set_late_list_mutation(mutation);
    }

    fn set_late_list_mutation(&self, mutation: LateListMutation) {
        *self
            .late_list_mutation
            .lock()
            .expect("late LIST mutation mutex poisoned") = Some(mutation);
    }

    fn reset_observed_operations(&self) {
        self.list_calls.store(0, Ordering::SeqCst);
        self.delete_calls.store(0, Ordering::SeqCst);
    }

    fn list_calls(&self) -> usize {
        self.list_calls.load(Ordering::SeqCst)
    }

    fn delete_calls(&self) -> usize {
        self.delete_calls.load(Ordering::SeqCst)
    }
}

impl HistoryMetadataControlStore {
    fn wrap(
        store: &ZeppelinStore,
        history_prefix: String,
    ) -> (ZeppelinStore, HistoryMetadataControlHandle) {
        let default_strip_prefix = Path::parse(history_prefix)
            .expect("history control prefix must be a valid object path");
        let strip_list_version_prefixes = Arc::new(Mutex::new(Vec::new()));
        let fail_next_get = Arc::new(Mutex::new(None));
        let late_list_mutation = Arc::new(Mutex::new(None));
        let list_calls = Arc::new(AtomicUsize::new(0));
        let delete_calls = Arc::new(AtomicUsize::new(0));
        let handle = HistoryMetadataControlHandle {
            default_strip_prefix,
            strip_list_version_prefixes: Arc::clone(&strip_list_version_prefixes),
            fail_next_get: Arc::clone(&fail_next_get),
            late_list_mutation: Arc::clone(&late_list_mutation),
            list_calls: Arc::clone(&list_calls),
            delete_calls: Arc::clone(&delete_calls),
        };
        let controlled = Self {
            inner: store.inner(),
            strip_list_version_prefixes,
            fail_next_get,
            late_list_mutation,
            list_calls,
            delete_calls,
        };
        (ZeppelinStore::new(Arc::new(controlled)), handle)
    }
}

impl fmt::Display for HistoryMetadataControlStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HistoryMetadataControlStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for HistoryMetadataControlStore {
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
        let should_fail = {
            let mut fail_next_get = self
                .fail_next_get
                .lock()
                .expect("history GET fault mutex poisoned");
            if fail_next_get.as_deref() == Some(location.as_ref()) {
                fail_next_get.take();
                true
            } else {
                false
            }
        };
        if should_fail {
            return Err(object_store::Error::Generic {
                store: "history_metadata_control",
                source: Box::new(std::io::Error::other(
                    "injected one-shot history GET failure",
                )),
            });
        }
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.delete_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, OsResult<Path>>,
    ) -> BoxStream<'a, OsResult<Path>> {
        self.delete_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        let mutation = {
            let mut mutation = self
                .late_list_mutation
                .lock()
                .expect("late LIST mutation mutex poisoned");
            if mutation
                .as_ref()
                .is_some_and(|mutation| prefix == Some(mutation.prefix()))
            {
                mutation.take()
            } else {
                None
            }
        };
        let strip_versions = prefix.is_some_and(|prefix| {
            self.strip_list_version_prefixes
                .lock()
                .expect("LIST metadata control mutex poisoned")
                .contains(prefix)
        });
        if let Some(mutation) = mutation {
            let inner = Arc::clone(&self.inner);
            let prefix = prefix.cloned();
            return futures::stream::once(async move {
                mutation.apply(&inner).await?;
                inner.list(prefix.as_ref()).try_collect::<Vec<_>>().await
            })
            .map(|result| match result {
                Ok(objects) => futures::stream::iter(objects.into_iter().map(Ok)).boxed(),
                Err(error) => futures::stream::once(async move { Err(error) }).boxed(),
            })
            .flatten()
            .map(move |result| {
                result.map(|mut object| {
                    if strip_versions {
                        object.e_tag = None;
                        object.version = None;
                    }
                    object
                })
            })
            .boxed();
        }
        self.inner
            .list(prefix)
            .map(move |result| {
                result.map(|mut object| {
                    if strip_versions {
                        object.e_tag = None;
                        object.version = None;
                    }
                    object
                })
            })
            .boxed()
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

fn memo_gc() -> GcConfig {
    GcConfig {
        manifest_history_keep_count: 64,
        pitr_retention_secs: 0,
        ..unsafe_short_gc(0)
    }
}

async fn seed_manifest_history(store: &ZeppelinStore, namespace: &str, generations: u64) {
    assert!(generations > 0, "history fixture must contain a generation");
    Manifest::new().write(store, namespace).await.unwrap();
    for generation in 2..=generations {
        let (mut manifest, etag) = Manifest::read_versioned(store, namespace)
            .await
            .unwrap()
            .unwrap();
        manifest.updated_at = DateTime::<Utc>::from_timestamp(1_700_000_000 + generation as i64, 0)
            .expect("fixed fixture timestamp must be valid");
        manifest
            .write_conditional(store, namespace, &etag)
            .await
            .unwrap();
    }
}

async fn put_history_revision(store: &ZeppelinStore, namespace: &str, version: u64, revision: i64) {
    let updated_at = DateTime::<Utc>::from_timestamp(1_710_000_000 + revision, 0)
        .expect("fixed history revision timestamp must be valid");
    let manifest = Manifest::new_at(updated_at);
    store
        .put(
            &Manifest::history_key(namespace, version),
            manifest_json_bytes_with_version(&manifest, version),
        )
        .await
        .unwrap();
}

async fn run_counted_gc_cycle(
    runner: &mut GcRunner,
    incarnation: &GcNamespaceIncarnation,
    now: DateTime<Utc>,
    counter: &GetCounter,
) -> GcCycleReport {
    counter.reset();
    runner.run_cycle_at(incarnation.clone(), now).await.unwrap()
}

async fn run_observed_gc_cycle(
    runner: &mut GcRunner,
    incarnation: &GcNamespaceIncarnation,
    now: DateTime<Utc>,
    counter: &GetCounter,
    control: &HistoryMetadataControlHandle,
) -> GcCycleReport {
    counter.reset();
    control.reset_observed_operations();
    runner.run_cycle_at(incarnation.clone(), now).await.unwrap()
}

fn assert_idle_cycle_only_listed_namespace(
    namespace: &str,
    counter: &GetCounter,
    control: &HistoryMetadataControlHandle,
) {
    let namespace_prefix = Path::parse(format!("{namespace}/"))
        .expect("namespace prefix must be a valid object path")
        .to_string();
    assert_eq!(control.list_calls(), 1, "idle cycle must issue one LIST");
    assert_eq!(
        counter.list_calls_for_prefix(&namespace_prefix),
        1,
        "idle cycle must LIST the full namespace metadata exactly once"
    );
    assert_eq!(counter.total_gets(), 0, "idle cycle must issue no GETs");
    assert_eq!(
        counter.puts_matching(""),
        0,
        "idle cycle must issue no PUTs"
    );
    assert_eq!(
        control.delete_calls(),
        0,
        "idle cycle must issue no DELETEs"
    );
}

fn ulid_at(now: DateTime<Utc>, entropy: u128) -> Ulid {
    let timestamp = now
        .timestamp_millis()
        .try_into()
        .expect("test timestamp must be after epoch");
    Ulid::from_parts(timestamp, entropy)
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
async fn gc_pending_delete_drain_rereads_retained_history_before_deleting() {
    let harness = TestHarness::new().await;
    let ns = harness.key("storage-gc-history-drain-race");
    let store = harness.store.clone();
    let pending_id = old_ulid(60, 139);
    let pending_key = WalFragment::s3_key(&ns, &pending_id);

    store
        .put(
            &pending_key,
            Bytes::from_static(b"pending history race body"),
        )
        .await
        .unwrap();

    Manifest::new().write(&store, &ns).await.unwrap();
    let (mut second, etag) = Manifest::read_versioned(&store, &ns)
        .await
        .unwrap()
        .unwrap();
    second.updated_at = Utc::now();
    second.write_conditional(&store, &ns, &etag).await.unwrap();
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

    let mut injected_history = Manifest::new();
    injected_history.pending_deletes.push(pending_key.clone());
    let injected_history_version = current.version() + 1;
    let injected_history_key = Manifest::history_key(&ns, injected_history_version);
    let injected_history_body =
        manifest_json_bytes_with_version(&injected_history, injected_history_version);
    let pruned_history_key = Manifest::history_key(&ns, 1);
    let (injecting_store, injection) = PutOnNthDeleteStore::wrap(
        store.inner(),
        pruned_history_key,
        injected_history_key,
        injected_history_body,
        1,
    );
    let injecting_store = zeppelin::storage::ZeppelinStore::new(Arc::new(injecting_store));

    let report = run_gc_cycle(
        &injecting_store,
        &ns,
        &GcConfig {
            manifest_history_keep_count: 1,
            ..unsafe_short_gc(0)
        },
    )
    .await
    .unwrap();

    assert_eq!(
        injection.puts_injected(),
        1,
        "test must inject retained history between prune and pending-delete drain"
    );
    assert_eq!(
        report.pending_deletes_deleted, 0,
        "drain must not delete an object protected by retained history that appeared after prune"
    );
    assert_eq!(report.pending_deletes_pruned, 0);
    assert_eq!(report.pending_deletes_retained, 1);
    assert_s3_object_exists(&injecting_store, &pending_key).await;
    let manifest = Manifest::read(&injecting_store, &ns)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manifest.pending_deletes, vec![pending_key]);

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
        (history_snapshots * 3) as u64,
        "one GC cycle reads retained history during prune, before drain/mark, and before sweep"
    );
    assert!(
        counter.list_calls_for_prefix(&history_prefix) <= 3,
        "one GC cycle should list retained history only for pruning, drain/mark, and sweep revalidation"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_history_memo_tracks_etags_and_lifecycle() {
    let harness = TestHarness::new().await;
    let namespace = harness.key("storage-gc-runner-history-memo");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);
    seed_manifest_history(&store, &namespace, 3).await;

    let (counted_store, counter) = counting_store(&store);
    let gc = memo_gc();
    let mut runner = GcRunner::new(counted_store.clone(), gc.clone());
    let created_at = Utc::now();
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), created_at);
    let now = Utc::now();

    run_counted_gc_cycle(&mut runner, &incarnation, now, &counter).await;
    assert_eq!(counter.gets_matching(&history_prefix), 9);
    for version in 1..=3 {
        assert_eq!(
            counter.gets_matching(&Manifest::history_key(&namespace, version)),
            3,
            "cold runner must read generation {version} in every GC history phase"
        );
    }

    run_counted_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
    )
    .await;
    assert_eq!(
        counter.gets_matching(&history_prefix),
        0,
        "an unchanged completed cycle must reuse every validated history body"
    );

    put_history_revision(&store, &namespace, 4, 1).await;
    run_counted_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(2),
        &counter,
    )
    .await;
    assert_eq!(counter.gets_matching(&history_prefix), 3);
    for version in 1..=3 {
        assert_eq!(
            counter.gets_matching(&Manifest::history_key(&namespace, version)),
            0,
            "existing generation {version} must remain memoized"
        );
    }
    assert_eq!(
        counter.gets_matching(&Manifest::history_key(&namespace, 4)),
        3,
        "a newly listed generation must be read in every authority phase"
    );

    run_counted_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(3),
        &counter,
    )
    .await;
    assert_eq!(counter.gets_matching(&history_prefix), 0);

    put_history_revision(&store, &namespace, 4, 2).await;
    run_counted_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(4),
        &counter,
    )
    .await;
    assert_eq!(counter.gets_matching(&history_prefix), 3);
    assert_eq!(
        counter.gets_matching(&Manifest::history_key(&namespace, 4)),
        3,
        "a changed ETag must invalidate the exact cached generation"
    );
    for version in 1..=3 {
        assert_eq!(
            counter.gets_matching(&Manifest::history_key(&namespace, version)),
            0,
            "a changed sibling must not invalidate generation {version}"
        );
    }

    let recreated =
        GcNamespaceIncarnation::new(namespace.clone(), created_at + chrono::Duration::seconds(1));
    run_counted_gc_cycle(
        &mut runner,
        &recreated,
        now + chrono::Duration::seconds(5),
        &counter,
    )
    .await;
    assert_eq!(
        counter.gets_matching(&history_prefix),
        12,
        "a new incarnation of the same namespace name must start cold"
    );

    let mut restarted = GcRunner::new(counted_store, gc);
    run_counted_gc_cycle(
        &mut restarted,
        &recreated,
        now + chrono::Duration::seconds(6),
        &counter,
    )
    .await;
    assert_eq!(
        counter.gets_matching(&history_prefix),
        12,
        "a fresh process-local runner must never inherit memo state"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_missing_history_etag_is_never_cacheable() {
    let harness = TestHarness::new().await;
    let namespace = harness.key("storage-gc-runner-history-unversioned");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);
    seed_manifest_history(&store, &namespace, 3).await;

    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, history_prefix.clone());
    control.set_strip_list_versions(true);
    control.set_strip_list_versions_for(&format!("{namespace}/"), true);
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(counted_store, memo_gc());
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());
    let now = Utc::now();

    run_counted_gc_cycle(&mut runner, &incarnation, now, &counter).await;
    assert_eq!(counter.gets_matching(&history_prefix), 9);

    run_counted_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
    )
    .await;
    assert_eq!(
        counter.gets_matching(&history_prefix),
        9,
        "history with no LIST ETag/backend version must be reread every cycle"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_failed_refresh_does_not_commit_partial_memo() {
    let harness = TestHarness::new().await;
    let namespace = harness.key("storage-gc-runner-history-refresh-failure");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);
    let first_key = Manifest::history_key(&namespace, 1);
    let second_key = Manifest::history_key(&namespace, 2);
    let third_key = Manifest::history_key(&namespace, 3);
    seed_manifest_history(&store, &namespace, 3).await;

    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, history_prefix.clone());
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(counted_store, memo_gc());
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());
    let now = Utc::now();

    run_counted_gc_cycle(&mut runner, &incarnation, now, &counter).await;
    assert_eq!(counter.gets_matching(&history_prefix), 9);
    run_counted_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
    )
    .await;
    assert_eq!(counter.gets_matching(&history_prefix), 0);

    put_history_revision(&store, &namespace, 1, 11).await;
    put_history_revision(&store, &namespace, 2, 12).await;
    control.fail_next_get(second_key.clone());

    let failed = run_counted_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(2),
        &counter,
    )
    .await;
    assert_eq!(failed, GcCycleReport::default());
    assert_eq!(counter.gets_matching(&history_prefix), 2);
    assert_eq!(counter.gets_matching(&first_key), 1);
    assert_eq!(counter.gets_matching(&second_key), 1);
    assert_eq!(counter.gets_matching(&third_key), 0);

    run_counted_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(3),
        &counter,
    )
    .await;
    assert_eq!(
        counter.gets_matching(&history_prefix),
        6,
        "retry must reload both changed entries; no partial failed refresh may commit"
    );
    assert_eq!(counter.gets_matching(&first_key), 3);
    assert_eq!(counter.gets_matching(&second_key), 3);
    assert_eq!(counter.gets_matching(&third_key), 0);

    run_counted_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(4),
        &counter,
    )
    .await;
    assert_eq!(
        counter.gets_matching(&history_prefix),
        0,
        "the first complete retry should commit the refreshed memo"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_idle_gate_skips_unchanged_and_wakes_on_inventory_change() {
    let harness = TestHarness::new().await;
    let namespace = harness.key("storage-gc-runner-idle-inventory");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);
    seed_manifest_history(&store, &namespace, 2).await;

    let (controlled_store, control) = HistoryMetadataControlStore::wrap(&store, history_prefix);
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(counted_store, memo_gc());
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());
    let now = Utc::now();

    run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;
    assert!(
        counter.total_gets() > 0,
        "the cold cycle must establish a complete authoritative memo"
    );

    let unchanged = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
        &control,
    )
    .await;
    assert_eq!(unchanged, GcCycleReport::default());
    assert_idle_cycle_only_listed_namespace(&namespace, &counter, &control);

    let probe_key = format!("{namespace}/idle-gate-probe.bin");
    store
        .put(&probe_key, Bytes::from_static(b"new inventory object"))
        .await
        .unwrap();
    run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(2),
        &counter,
        &control,
    )
    .await;
    assert!(
        counter.total_gets() > 0,
        "a new namespace object must force a full cycle"
    );

    run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(3),
        &counter,
        &control,
    )
    .await;
    assert_idle_cycle_only_listed_namespace(&namespace, &counter, &control);

    store
        .put(
            &probe_key,
            Bytes::from_static(b"changed inventory object with a new version"),
        )
        .await
        .unwrap();
    run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(4),
        &counter,
        &control,
    )
    .await;
    assert!(
        counter.total_gets() > 0,
        "a changed namespace object must force a full cycle"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_idle_gate_reconciles_history_published_during_late_inventory_list() {
    let harness = TestHarness::new().await;
    let namespace = harness.key("storage-gc-runner-idle-late-history");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);
    seed_manifest_history(&store, &namespace, 2).await;

    let late_history_key = Manifest::history_key(&namespace, 3);
    let late_history = Manifest::new_at(
        DateTime::<Utc>::from_timestamp(1_720_000_003, 0)
            .expect("fixed late-history timestamp must be valid"),
    );
    let (controlled_store, control) = HistoryMetadataControlStore::wrap(&store, history_prefix);
    control.put_on_next_list(
        &format!("{namespace}/"),
        &late_history_key,
        manifest_json_bytes_with_version(&late_history, 3),
    );
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(
        counted_store,
        GcConfig {
            manifest_history_keep_count: 1,
            pitr_retention_secs: 0,
            ..unsafe_short_gc(0)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());
    let now = Utc::now();

    run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;
    assert!(
        Manifest::read_history(&store, &namespace, 2)
            .await
            .unwrap()
            .is_some(),
        "the late generation arrives after the current cycle's prune decision"
    );
    assert!(
        Manifest::read_history(&store, &namespace, 3)
            .await
            .unwrap()
            .is_some(),
        "the injected history generation must be visible in MinIO"
    );

    run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
        &control,
    )
    .await;
    assert!(
        counter.total_gets() > 0,
        "history published after prune but before inventory LIST must force one reconciliation cycle"
    );
    assert!(
        Manifest::read_history(&store, &namespace, 2)
            .await
            .unwrap()
            .is_none(),
        "the reconciliation cycle must apply keep-count retention to the displaced generation"
    );

    run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(2),
        &counter,
        &control,
    )
    .await;
    assert_idle_cycle_only_listed_namespace(&namespace, &counter, &control);

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_idle_gate_reconciles_history_published_after_prune_observation() {
    let harness = TestHarness::new().await;
    let namespace = harness.key("storage-gc-runner-idle-mid-prune-history");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);
    seed_manifest_history(&store, &namespace, 2).await;

    let pruned_history_key = Manifest::history_key(&namespace, 1);
    let injected_history_key = Manifest::history_key(&namespace, 3);
    let injected_history = Manifest::new_at(
        DateTime::<Utc>::from_timestamp(1_720_000_013, 0)
            .expect("fixed mid-prune history timestamp must be valid"),
    );
    let (injecting_store, injection) = PutOnNthDeleteStore::wrap(
        store.inner(),
        pruned_history_key,
        injected_history_key,
        manifest_json_bytes_with_version(&injected_history, 3),
        1,
    );
    let injecting_store = ZeppelinStore::new(Arc::new(injecting_store));
    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&injecting_store, history_prefix);
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(
        counted_store,
        GcConfig {
            manifest_history_keep_count: 1,
            pitr_retention_secs: 0,
            ..unsafe_short_gc(0)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());
    let now = Utc::now();

    run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;
    assert_eq!(
        injection.puts_injected(),
        1,
        "the fixture must publish generation 3 after prune listed generations 1 and 2"
    );
    assert!(
        Manifest::read_history(&store, &namespace, 2)
            .await
            .unwrap()
            .is_some(),
        "generation 2 was retained by the stale pre-injection keep-count decision"
    );

    run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
        &control,
    )
    .await;
    assert!(
        counter.total_gets() > 0,
        "history published after prune's LIST but before the retained-history scan must force reconciliation"
    );
    assert!(
        Manifest::read_history(&store, &namespace, 2)
            .await
            .unwrap()
            .is_none(),
        "reconciliation must apply keep-count retention to the displaced generation"
    );

    run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(2),
        &counter,
        &control,
    )
    .await;
    assert_idle_cycle_only_listed_namespace(&namespace, &counter, &control);

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_idle_gate_reconciles_snapshot_recreated_during_late_inventory_list() {
    let harness = TestHarness::new().await;
    let namespace = harness.key("storage-gc-runner-idle-late-snapshot-recreate");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);
    seed_manifest_history(&store, &namespace, 2).await;
    NamedSnapshot::create(&store, &namespace, "moving-pin", 1)
        .await
        .unwrap();

    let snapshot_key = NamedSnapshot::key(&namespace, "moving-pin").unwrap();
    let replacement = NamedSnapshot {
        generation: 2,
        created_at: Utc::now(),
    }
    .to_bytes()
    .unwrap();
    let (controlled_store, control) = HistoryMetadataControlStore::wrap(&store, history_prefix);
    control.replace_on_next_list(&format!("{namespace}/"), &snapshot_key, replacement);
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(
        counted_store,
        GcConfig {
            manifest_history_keep_count: 1,
            pitr_retention_secs: 0,
            ..unsafe_short_gc(0)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());
    let now = Utc::now();

    run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;
    assert_eq!(
        NamedSnapshot::read(&store, &namespace, "moving-pin")
            .await
            .unwrap()
            .unwrap()
            .generation,
        2,
        "the late LIST hook must recreate the same snapshot key with a new identity"
    );
    assert!(
        Manifest::read_history(&store, &namespace, 1)
            .await
            .unwrap()
            .is_some(),
        "the early prune decision observed generation 1 as pinned"
    );

    run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
        &control,
    )
    .await;
    assert!(
        counter.total_gets() > 0,
        "a snapshot delete/recreate after pin scan must force one reconciliation cycle"
    );
    assert!(
        Manifest::read_history(&store, &namespace, 1)
            .await
            .unwrap()
            .is_none(),
        "reconciliation must prune the generation no longer pinned by the recreated snapshot"
    );

    run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(2),
        &counter,
        &control,
    )
    .await;
    assert_idle_cycle_only_listed_namespace(&namespace, &counter, &control);

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_idle_gate_reconciles_mature_pending_delete_published_during_late_inventory_list()
{
    let harness = TestHarness::new().await;
    let namespace = harness.key("storage-gc-runner-idle-late-pending");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);
    let now = Utc::now();
    let pending_id = old_ulid(60, 151);
    let pending_key = WalFragment::s3_key(&namespace, &pending_id);
    store
        .put(
            &pending_key,
            Bytes::from_static(b"late mature pending delete"),
        )
        .await
        .unwrap();

    let mut manifest = Manifest::new_at(now);
    manifest.write(&store, &namespace).await.unwrap();
    let mut late_manifest = manifest;
    late_manifest.pending_deletes.push(pending_key.clone());
    late_manifest.updated_at = now;
    let (controlled_store, control) = HistoryMetadataControlStore::wrap(&store, history_prefix);
    control.put_on_next_list(
        &format!("{namespace}/"),
        &Manifest::s3_key(&namespace),
        manifest_json_bytes_with_version(&late_manifest, 2),
    );
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(
        counted_store,
        GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(10)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());

    let raced = run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;
    assert_eq!(raced.pending_deletes_deleted, 0);
    assert_s3_object_exists(&store, &pending_key).await;
    assert_eq!(
        Manifest::read(&store, &namespace)
            .await
            .unwrap()
            .unwrap()
            .pending_deletes,
        vec![pending_key.clone()],
        "the late publication must land after this cycle's pending-delete drain"
    );

    let reconciled = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
        &control,
    )
    .await;
    assert!(
        counter.total_gets() > 0,
        "a mature pending delete published after drain must force one reconciliation cycle"
    );
    assert_eq!(reconciled.pending_deletes_deleted, 1);
    assert_eq!(reconciled.pending_deletes_pruned, 1);
    assert_s3_object_not_exists(&store, &pending_key).await;

    run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(2),
        &counter,
        &control,
    )
    .await;
    assert!(
        counter.total_gets() > 0,
        "the drain CAS publishes history after prune, requiring one conservative history reconciliation"
    );

    run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(3),
        &counter,
        &control,
    )
    .await;
    assert_idle_cycle_only_listed_namespace(&namespace, &counter, &control);

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_idle_gate_requires_versioned_namespace_inventory() {
    let harness = TestHarness::new().await;
    let namespace = harness.key("storage-gc-runner-idle-unversioned");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);
    seed_manifest_history(&store, &namespace, 2).await;

    let (controlled_store, control) = HistoryMetadataControlStore::wrap(&store, history_prefix);
    control.set_strip_list_versions_for(&format!("{namespace}/"), true);
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(counted_store, memo_gc());
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());
    let now = Utc::now();

    run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;
    run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
        &control,
    )
    .await;
    assert!(
        counter.total_gets() > 0,
        "missing namespace LIST ETags/backend versions must disable the idle gate"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_idle_gate_wakes_at_candidate_pending_pitr_and_lease_deadlines() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let now = DateTime::<Utc>::from_timestamp(1_750_000_000, 0)
        .expect("fixed idle-gate timestamp must be valid");

    let candidate_namespace = harness.key("storage-gc-runner-idle-candidate-deadline");
    let mut candidate_manifest = Manifest::new_at(now);
    candidate_manifest
        .write(&store, &candidate_namespace)
        .await
        .unwrap();
    let candidate_key = WalFragment::s3_key(
        &candidate_namespace,
        &ulid_at(now - chrono::Duration::seconds(60), 101),
    );
    store
        .put(&candidate_key, Bytes::from_static(b"candidate deadline"))
        .await
        .unwrap();
    let (candidate_controlled, candidate_control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&candidate_namespace));
    let (candidate_counted, candidate_counter) = counting_store(&candidate_controlled);
    let mut candidate_runner = GcRunner::new(
        candidate_counted,
        GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(10)
        },
    );
    let candidate_incarnation =
        GcNamespaceIncarnation::new(candidate_namespace.clone(), Utc::now());
    let candidate_first = run_observed_gc_cycle(
        &mut candidate_runner,
        &candidate_incarnation,
        now,
        &candidate_counter,
        &candidate_control,
    )
    .await;
    assert_eq!(candidate_first.candidates_marked, 1);
    run_observed_gc_cycle(
        &mut candidate_runner,
        &candidate_incarnation,
        now + chrono::Duration::seconds(9),
        &candidate_counter,
        &candidate_control,
    )
    .await;
    assert_idle_cycle_only_listed_namespace(
        &candidate_namespace,
        &candidate_counter,
        &candidate_control,
    );
    let candidate_due = run_observed_gc_cycle(
        &mut candidate_runner,
        &candidate_incarnation,
        now + chrono::Duration::seconds(10),
        &candidate_counter,
        &candidate_control,
    )
    .await;
    assert_eq!(candidate_due.objects_deleted, 1);
    assert!(candidate_counter.total_gets() > 0);
    assert_s3_object_not_exists(&store, &candidate_key).await;
    run_observed_gc_cycle(
        &mut candidate_runner,
        &candidate_incarnation,
        now + chrono::Duration::seconds(11),
        &candidate_counter,
        &candidate_control,
    )
    .await;
    assert_idle_cycle_only_listed_namespace(
        &candidate_namespace,
        &candidate_counter,
        &candidate_control,
    );

    let pending_namespace = harness.key("storage-gc-runner-idle-pending-deadline");
    let pending_key = WalFragment::s3_key(&pending_namespace, &ulid_at(now, 102));
    store
        .put(&pending_key, Bytes::from_static(b"pending deadline"))
        .await
        .unwrap();
    let mut pending_manifest = Manifest::new_at(now);
    pending_manifest.pending_deletes.push(pending_key.clone());
    pending_manifest
        .write(&store, &pending_namespace)
        .await
        .unwrap();
    store
        .delete(&Manifest::history_key(&pending_namespace, 1))
        .await
        .unwrap();
    let (pending_controlled, pending_control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&pending_namespace));
    let (pending_counted, pending_counter) = counting_store(&pending_controlled);
    let mut pending_runner = GcRunner::new(
        pending_counted,
        GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(10)
        },
    );
    let pending_incarnation = GcNamespaceIncarnation::new(pending_namespace.clone(), Utc::now());
    let pending_first = run_observed_gc_cycle(
        &mut pending_runner,
        &pending_incarnation,
        now,
        &pending_counter,
        &pending_control,
    )
    .await;
    assert_eq!(pending_first.pending_deletes_retained, 1);
    run_observed_gc_cycle(
        &mut pending_runner,
        &pending_incarnation,
        now + chrono::Duration::seconds(9),
        &pending_counter,
        &pending_control,
    )
    .await;
    assert_idle_cycle_only_listed_namespace(&pending_namespace, &pending_counter, &pending_control);
    let pending_due = run_observed_gc_cycle(
        &mut pending_runner,
        &pending_incarnation,
        now + chrono::Duration::seconds(10),
        &pending_counter,
        &pending_control,
    )
    .await;
    assert_eq!(pending_due.pending_deletes_deleted, 1);
    assert!(pending_counter.total_gets() > 0);
    assert_s3_object_not_exists(&store, &pending_key).await;

    let pitr_namespace = harness.key("storage-gc-runner-idle-pitr-deadline");
    let mut pitr_manifest = Manifest::new_at(now);
    pitr_manifest.write(&store, &pitr_namespace).await.unwrap();
    let (mut pitr_manifest, pitr_version) = Manifest::read_versioned(&store, &pitr_namespace)
        .await
        .unwrap()
        .unwrap();
    pitr_manifest.updated_at = now;
    pitr_manifest
        .write_conditional(&store, &pitr_namespace, &pitr_version)
        .await
        .unwrap();
    let pitr_history_one = Manifest::history_key(&pitr_namespace, 1);
    let (pitr_controlled, pitr_control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&pitr_namespace));
    let (pitr_counted, pitr_counter) = counting_store(&pitr_controlled);
    let mut pitr_runner = GcRunner::new(
        pitr_counted,
        GcConfig {
            manifest_history_keep_count: 1,
            pitr_retention_secs: 10,
            ..unsafe_short_gc(0)
        },
    );
    let pitr_incarnation = GcNamespaceIncarnation::new(pitr_namespace.clone(), Utc::now());
    run_observed_gc_cycle(
        &mut pitr_runner,
        &pitr_incarnation,
        now,
        &pitr_counter,
        &pitr_control,
    )
    .await;
    run_observed_gc_cycle(
        &mut pitr_runner,
        &pitr_incarnation,
        now + chrono::Duration::seconds(10),
        &pitr_counter,
        &pitr_control,
    )
    .await;
    assert_idle_cycle_only_listed_namespace(&pitr_namespace, &pitr_counter, &pitr_control);
    run_observed_gc_cycle(
        &mut pitr_runner,
        &pitr_incarnation,
        now + chrono::Duration::seconds(11),
        &pitr_counter,
        &pitr_control,
    )
    .await;
    assert!(
        pitr_counter.total_gets() > 0,
        "PITR expiry must force a full cycle at the first whole-second boundary"
    );
    assert_s3_object_not_exists(&store, &pitr_history_one).await;

    let lease_namespace = harness.key("storage-gc-runner-idle-lease-deadline");
    let mut lease_manifest = Manifest::new_at(now);
    lease_manifest
        .write(&store, &lease_namespace)
        .await
        .unwrap();
    let staged_key = WalFragment::s3_key(
        &lease_namespace,
        &ulid_at(now - chrono::Duration::seconds(60), 103),
    );
    store
        .put(&staged_key, Bytes::from_static(b"lease deadline"))
        .await
        .unwrap();
    let lease_token = 41;
    let lease = serde_json::json!({
        "holder_id": "idle-gate-holder",
        "fencing_token": lease_token,
        "acquired_at": now,
        "expires_at": now + chrono::Duration::seconds(10),
    });
    store
        .put(
            &format!("{lease_namespace}/lease.json"),
            Bytes::from(serde_json::to_vec(&lease).unwrap()),
        )
        .await
        .unwrap();
    write_compaction_staging(
        &store,
        &lease_namespace,
        lease_token,
        BTreeSet::from([staged_key.clone()]),
    )
    .await
    .unwrap();
    let (lease_controlled, lease_control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&lease_namespace));
    let (lease_counted, lease_counter) = counting_store(&lease_controlled);
    let mut lease_runner = GcRunner::new(lease_counted, memo_gc());
    let lease_incarnation = GcNamespaceIncarnation::new(lease_namespace.clone(), Utc::now());
    run_observed_gc_cycle(
        &mut lease_runner,
        &lease_incarnation,
        now,
        &lease_counter,
        &lease_control,
    )
    .await;
    run_observed_gc_cycle(
        &mut lease_runner,
        &lease_incarnation,
        now + chrono::Duration::seconds(9),
        &lease_counter,
        &lease_control,
    )
    .await;
    assert_idle_cycle_only_listed_namespace(&lease_namespace, &lease_counter, &lease_control);
    let lease_due = run_observed_gc_cycle(
        &mut lease_runner,
        &lease_incarnation,
        now + chrono::Duration::seconds(10),
        &lease_counter,
        &lease_control,
    )
    .await;
    assert_eq!(lease_due.objects_deleted, 1);
    assert!(lease_counter.total_gets() > 0);
    assert_s3_object_not_exists(&store, &staged_key).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_idle_gate_rejects_backward_clock_config_change_and_partial_failure() {
    let harness = TestHarness::new().await;
    let namespace = harness.key("storage-gc-runner-idle-invalidations");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);
    let first_history = Manifest::history_key(&namespace, 1);
    seed_manifest_history(&store, &namespace, 2).await;

    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, history_prefix.clone());
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(counted_store, memo_gc());
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());
    let now = Utc::now();

    run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;
    run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
        &control,
    )
    .await;
    assert_idle_cycle_only_listed_namespace(&namespace, &counter, &control);

    run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now - chrono::Duration::seconds(1),
        &counter,
        &control,
    )
    .await;
    assert!(
        counter.total_gets() > 0,
        "a backward wall-clock jump must disable the idle gate"
    );

    run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(2),
        &counter,
        &control,
    )
    .await;
    assert_idle_cycle_only_listed_namespace(&namespace, &counter, &control);

    runner.update_config(GcConfig {
        horizon_secs: 1,
        ..memo_gc()
    });
    run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(3),
        &counter,
        &control,
    )
    .await;
    assert!(
        counter.total_gets() > 0,
        "a GC policy change must force a full cycle"
    );
    assert_eq!(
        counter.gets_matching(&history_prefix),
        0,
        "a policy change may retain ETag-validated immutable history bodies"
    );

    run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(4),
        &counter,
        &control,
    )
    .await;
    assert_idle_cycle_only_listed_namespace(&namespace, &counter, &control);

    let probe_key = format!("{namespace}/partial-refresh-probe.bin");
    store
        .put(&probe_key, Bytes::from_static(b"force full refresh"))
        .await
        .unwrap();
    control.set_strip_list_versions(true);
    control.fail_next_get(first_history);
    let failed = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(5),
        &counter,
        &control,
    )
    .await;
    assert_eq!(failed, GcCycleReport::default());

    control.set_strip_list_versions(false);
    store.delete(&probe_key).await.unwrap();
    run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(6),
        &counter,
        &control,
    )
    .await;
    assert!(
        counter.total_gets() > 0,
        "a partial refresh must invalidate the idle gate even when inventory rolls back"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_pending_delete_failure_does_not_publish_history_memo() {
    let harness = TestHarness::new().await;
    let namespace = harness.key("storage-gc-runner-pending-delete-failure");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);

    Manifest::new().write(&store, &namespace).await.unwrap();
    let (mut live, etag) = Manifest::read_versioned(&store, &namespace)
        .await
        .unwrap()
        .unwrap();
    live.pending_deletes
        .push(format!("{namespace}/wal//invalid.wal"));
    live.write_conditional(&store, &namespace, &etag)
        .await
        .unwrap();
    store
        .delete(&Manifest::history_key(&namespace, 2))
        .await
        .unwrap();

    let (counted_store, counter) = counting_store(&store);
    let mut runner = GcRunner::new(counted_store, memo_gc());
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());
    let now = Utc::now();

    let first = run_counted_gc_cycle(&mut runner, &incarnation, now, &counter).await;
    assert_eq!(first.pending_deletes_retained, 1);
    assert_eq!(counter.gets_matching(&history_prefix), 3);

    let second = run_counted_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
    )
    .await;
    assert_eq!(second.pending_deletes_retained, 1);
    assert_eq!(
        counter.gets_matching(&history_prefix),
        3,
        "a caught pending-delete storage failure must keep the next cycle cold"
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
