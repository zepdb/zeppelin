mod common;

use std::collections::BTreeSet;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream};
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
    drain_pending_deletes_at, load_gc_candidates, run_gc_cycle, run_gc_cycle_at,
    save_gc_candidates, write_compaction_staging, GcCandidate, GcCycleReport,
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

fn gc_manifest() -> Manifest {
    let mut manifest = Manifest::new();
    manifest
        .bind_namespace_incarnation(uuid::Uuid::from_u128(0x6c69_7665_2d67_632d_7465_7374))
        .expect("GC fixture incarnation must be valid");
    manifest
}

fn gc_manifest_at(now: DateTime<Utc>) -> Manifest {
    let mut manifest = Manifest::new_at(now);
    manifest
        .bind_namespace_incarnation(uuid::Uuid::from_u128(0x6c69_7665_2d67_632d_7465_7374))
        .expect("GC fixture incarnation must be valid");
    manifest
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

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, OsResult<Path>>,
    ) -> BoxStream<'a, OsResult<Path>> {
        self.inner.delete_stream(locations)
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

    async fn inject_before_delete(&self, location: &Path) -> OsResult<()> {
        if location.as_ref() != self.delete_key {
            return Ok(());
        }
        let delete = self.deletes_seen.fetch_add(1, Ordering::SeqCst) + 1;
        if delete != self.trigger_delete {
            return Ok(());
        }
        let put_path =
            Path::parse(&self.put_key).map_err(|error| object_store::Error::Generic {
                store: "put_on_nth_delete",
                source: Box::new(error),
            })?;
        self.inner
            .put(&put_path, PutPayload::from(self.put_body.clone()))
            .await?;
        self.puts_injected.fetch_add(1, Ordering::Relaxed);
        Ok(())
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
        self.inject_before_delete(location).await?;
        self.inner.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, OsResult<Path>>,
    ) -> BoxStream<'a, OsResult<Path>> {
        let locations = locations
            .then(move |result| async move {
                match result {
                    Ok(location) => {
                        self.inject_before_delete(&location).await?;
                        Ok(location)
                    }
                    Err(error) => Err(error),
                }
            })
            .boxed();
        self.inner.delete_stream(locations)
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

/// Transparent real-store race seam that publishes a concurrent manifest
/// immediately after the pending-delete drain retains its predecessor history
/// and immediately before its stale live-manifest CAS. The concurrent writer
/// publishes its live manifest followed by its own immutable history snapshot,
/// matching the production publication order.
#[derive(Debug)]
struct PublishManifestOnHistoryWriteStore {
    inner: Arc<dyn ObjectStore>,
    trigger_history_key: Path,
    concurrent_history_key: Path,
    manifest_key: Path,
    concurrent_history: Bytes,
    concurrent_manifest: Bytes,
    remove_history_on_second_manifest_read: bool,
    published: Arc<AtomicBool>,
    history_removed: Arc<AtomicBool>,
    manifest_reads_after_publish: AtomicUsize,
}

#[derive(Clone, Debug)]
struct PublishManifestOnHistoryWriteHandle {
    published: Arc<AtomicBool>,
    history_removed: Arc<AtomicBool>,
}

impl PublishManifestOnHistoryWriteHandle {
    fn published(&self) -> bool {
        self.published.load(Ordering::SeqCst)
    }

    fn history_removed(&self) -> bool {
        self.history_removed.load(Ordering::SeqCst)
    }
}

impl PublishManifestOnHistoryWriteStore {
    fn wrap(
        inner: Arc<dyn ObjectStore>,
        trigger_history_key: String,
        concurrent_history_key: String,
        manifest_key: String,
        concurrent_history: Bytes,
        concurrent_manifest: Bytes,
        remove_history_on_second_manifest_read: bool,
    ) -> (Self, PublishManifestOnHistoryWriteHandle) {
        let published = Arc::new(AtomicBool::new(false));
        let history_removed = Arc::new(AtomicBool::new(false));
        let handle = PublishManifestOnHistoryWriteHandle {
            published: Arc::clone(&published),
            history_removed: Arc::clone(&history_removed),
        };
        (
            Self {
                inner,
                trigger_history_key: Path::parse(trigger_history_key)
                    .expect("trigger history key must be valid"),
                concurrent_history_key: Path::parse(concurrent_history_key)
                    .expect("concurrent history key must be valid"),
                manifest_key: Path::parse(manifest_key).expect("manifest key must be valid"),
                concurrent_history,
                concurrent_manifest,
                remove_history_on_second_manifest_read,
                published,
                history_removed,
                manifest_reads_after_publish: AtomicUsize::new(0),
            },
            handle,
        )
    }
}

impl fmt::Display for PublishManifestOnHistoryWriteStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PublishManifestOnHistoryWriteStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for PublishManifestOnHistoryWriteStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let result = self.inner.put_opts(location, payload, opts).await?;
        if location == &self.trigger_history_key
            && self
                .published
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.inner
                .put(
                    &self.manifest_key,
                    PutPayload::from(self.concurrent_manifest.clone()),
                )
                .await?;
            self.inner
                .put(
                    &self.concurrent_history_key,
                    PutPayload::from(self.concurrent_history.clone()),
                )
                .await?;
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        if location == &self.manifest_key && self.published.load(Ordering::SeqCst) {
            let read = self
                .manifest_reads_after_publish
                .fetch_add(1, Ordering::SeqCst)
                + 1;
            if self.remove_history_on_second_manifest_read
                && read == 2
                && self
                    .history_removed
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                self.inner.delete(&self.concurrent_history_key).await?;
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

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, OsResult<Path>>,
    ) -> BoxStream<'a, OsResult<Path>> {
        self.inner.delete_stream(locations)
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
    remaining_prefix_matches: usize,
    operations: Vec<LateListOperation>,
}

#[derive(Debug)]
struct LateGetMutation {
    key: Path,
    remaining_key_matches: usize,
    operations: Vec<LateListOperation>,
}

#[derive(Debug)]
struct FailNthPut {
    key: Path,
    remaining_key_matches: usize,
}

#[derive(Clone, Copy, Debug)]
enum DeleteBatchFault {
    MatchingNotFound { member: usize },
    PerKeyErrorAfterApply { member: usize },
    OverallResponseLostAfterApply,
}

#[derive(Debug)]
struct FailNthDeleteBatch {
    remaining_calls: usize,
    fault: DeleteBatchFault,
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
    strip_list_version_keys: Arc<Mutex<Vec<Path>>>,
    fail_next_get: Arc<Mutex<Option<String>>>,
    miss_next_get: Arc<Mutex<Option<String>>>,
    fail_nth_put: Arc<Mutex<Option<FailNthPut>>>,
    late_list_mutation: Arc<Mutex<Option<LateListMutation>>>,
    late_get_mutation: Arc<Mutex<Option<LateGetMutation>>>,
    fail_nth_delete_batch: Arc<Mutex<Option<FailNthDeleteBatch>>>,
    delete_batches: Arc<Mutex<Vec<Vec<String>>>>,
    list_calls: Arc<AtomicUsize>,
    delete_calls: Arc<AtomicUsize>,
}

#[derive(Clone, Debug)]
struct HistoryMetadataControlHandle {
    default_strip_prefix: Path,
    strip_list_version_prefixes: Arc<Mutex<Vec<Path>>>,
    strip_list_version_keys: Arc<Mutex<Vec<Path>>>,
    fail_next_get: Arc<Mutex<Option<String>>>,
    miss_next_get: Arc<Mutex<Option<String>>>,
    fail_nth_put: Arc<Mutex<Option<FailNthPut>>>,
    late_list_mutation: Arc<Mutex<Option<LateListMutation>>>,
    late_get_mutation: Arc<Mutex<Option<LateGetMutation>>>,
    fail_nth_delete_batch: Arc<Mutex<Option<FailNthDeleteBatch>>>,
    delete_batches: Arc<Mutex<Vec<Vec<String>>>>,
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

    fn set_strip_list_version_for_key(&self, key: &str, strip: bool) {
        let key = Path::parse(key).expect("controlled LIST key must be valid");
        let mut keys = self
            .strip_list_version_keys
            .lock()
            .expect("LIST key metadata control mutex poisoned");
        if strip {
            if !keys.contains(&key) {
                keys.push(key);
            }
        } else {
            keys.retain(|candidate| candidate != &key);
        }
    }

    fn fail_next_get(&self, key: String) {
        *self
            .fail_next_get
            .lock()
            .expect("history GET fault mutex poisoned") = Some(key);
    }

    fn miss_next_get(&self, key: String) {
        *self
            .miss_next_get
            .lock()
            .expect("history missing-GET fault mutex poisoned") = Some(key);
    }

    fn fail_nth_put(&self, key: &str, nth: usize) {
        assert!(nth > 0, "controlled PUT occurrence must be nonzero");
        *self.fail_nth_put.lock().expect("PUT fault mutex poisoned") = Some(FailNthPut {
            key: Path::parse(key).expect("controlled PUT key must be valid"),
            remaining_key_matches: nth - 1,
        });
    }

    fn put_on_next_list(&self, prefix: &str, key: &str, body: Bytes) {
        self.put_on_nth_list(prefix, key, body, 1);
    }

    fn put_on_nth_list(&self, prefix: &str, key: &str, body: Bytes, nth: usize) {
        assert!(nth > 0, "controlled LIST occurrence must be nonzero");
        let mutation = LateListMutation {
            prefix: Path::parse(prefix).expect("controlled LIST prefix must be valid"),
            remaining_prefix_matches: nth - 1,
            operations: vec![LateListOperation::Put {
                key: Path::parse(key).expect("injected PUT key must be valid"),
                body,
            }],
        };
        self.set_late_list_mutation(mutation);
    }

    fn replace_on_next_list(&self, prefix: &str, key: &str, body: Bytes) {
        self.replace_on_nth_list(prefix, key, body, 1);
    }

    fn replace_on_nth_list(&self, prefix: &str, key: &str, body: Bytes, nth: usize) {
        assert!(nth > 0, "controlled LIST occurrence must be nonzero");
        let key = Path::parse(key).expect("injected replacement key must be valid");
        let mutation = LateListMutation {
            prefix: Path::parse(prefix).expect("controlled LIST prefix must be valid"),
            remaining_prefix_matches: nth - 1,
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

    fn put_on_nth_get(&self, trigger_key: &str, key: &str, body: Bytes, nth: usize) {
        assert!(nth > 0, "controlled GET occurrence must be nonzero");
        *self
            .late_get_mutation
            .lock()
            .expect("late GET mutation mutex poisoned") = Some(LateGetMutation {
            key: Path::parse(trigger_key).expect("controlled GET key must be valid"),
            remaining_key_matches: nth - 1,
            operations: vec![LateListOperation::Put {
                key: Path::parse(key).expect("injected PUT key must be valid"),
                body,
            }],
        });
    }

    fn fault_nth_delete_batch(&self, nth: usize, fault: DeleteBatchFault) {
        assert!(
            nth > 0,
            "controlled DELETE batch occurrence must be nonzero"
        );
        *self
            .fail_nth_delete_batch
            .lock()
            .expect("DELETE batch fault mutex poisoned") = Some(FailNthDeleteBatch {
            remaining_calls: nth - 1,
            fault,
        });
    }

    fn delete_batches(&self) -> Vec<Vec<String>> {
        self.delete_batches
            .lock()
            .expect("DELETE batch observation mutex poisoned")
            .clone()
    }

    fn reset_observed_operations(&self) {
        self.list_calls.store(0, Ordering::SeqCst);
        self.delete_calls.store(0, Ordering::SeqCst);
        self.delete_batches
            .lock()
            .expect("DELETE batch observation mutex poisoned")
            .clear();
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
        let strip_list_version_keys = Arc::new(Mutex::new(Vec::new()));
        let fail_next_get = Arc::new(Mutex::new(None));
        let miss_next_get = Arc::new(Mutex::new(None));
        let fail_nth_put = Arc::new(Mutex::new(None));
        let late_list_mutation = Arc::new(Mutex::new(None));
        let late_get_mutation = Arc::new(Mutex::new(None));
        let fail_nth_delete_batch = Arc::new(Mutex::new(None));
        let delete_batches = Arc::new(Mutex::new(Vec::new()));
        let list_calls = Arc::new(AtomicUsize::new(0));
        let delete_calls = Arc::new(AtomicUsize::new(0));
        let handle = HistoryMetadataControlHandle {
            default_strip_prefix,
            strip_list_version_prefixes: Arc::clone(&strip_list_version_prefixes),
            strip_list_version_keys: Arc::clone(&strip_list_version_keys),
            fail_next_get: Arc::clone(&fail_next_get),
            miss_next_get: Arc::clone(&miss_next_get),
            fail_nth_put: Arc::clone(&fail_nth_put),
            late_list_mutation: Arc::clone(&late_list_mutation),
            late_get_mutation: Arc::clone(&late_get_mutation),
            fail_nth_delete_batch: Arc::clone(&fail_nth_delete_batch),
            delete_batches: Arc::clone(&delete_batches),
            list_calls: Arc::clone(&list_calls),
            delete_calls: Arc::clone(&delete_calls),
        };
        let controlled = Self {
            inner: store.inner(),
            strip_list_version_prefixes,
            strip_list_version_keys,
            fail_next_get,
            miss_next_get,
            fail_nth_put,
            late_list_mutation,
            late_get_mutation,
            fail_nth_delete_batch,
            delete_batches,
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
        let should_fail = {
            let mut fault = self.fail_nth_put.lock().expect("PUT fault mutex poisoned");
            match fault.as_mut() {
                Some(pending) if location == &pending.key => {
                    if pending.remaining_key_matches == 0 {
                        fault.take();
                        true
                    } else {
                        pending.remaining_key_matches -= 1;
                        false
                    }
                }
                _ => false,
            }
        };
        if should_fail {
            return Err(object_store::Error::Generic {
                store: "history_metadata_control",
                source: Box::new(std::io::Error::other(
                    "injected candidate-ledger PUT failure",
                )),
            });
        }
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
        let mutation = {
            let mut mutation = self
                .late_get_mutation
                .lock()
                .expect("late GET mutation mutex poisoned");
            match mutation.as_mut() {
                Some(pending) if location == &pending.key => {
                    if pending.remaining_key_matches == 0 {
                        mutation.take()
                    } else {
                        pending.remaining_key_matches -= 1;
                        None
                    }
                }
                _ => None,
            }
        };
        if let Some(mutation) = mutation {
            for operation in mutation.operations {
                match operation {
                    LateListOperation::Put { key, body } => {
                        self.inner.put(&key, PutPayload::from(body)).await?;
                    }
                    LateListOperation::Delete { key } => self.inner.delete(&key).await?,
                }
            }
        }
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
        let should_be_missing = {
            let mut miss_next_get = self
                .miss_next_get
                .lock()
                .expect("history missing-GET fault mutex poisoned");
            if miss_next_get.as_deref() == Some(location.as_ref()) {
                miss_next_get.take();
                true
            } else {
                false
            }
        };
        if should_be_missing {
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: "injected missing history body".into(),
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
        let batch_index = {
            let mut batches = self
                .delete_batches
                .lock()
                .expect("DELETE batch observation mutex poisoned");
            batches.push(Vec::new());
            batches.len() - 1
        };
        let observed_batches = Arc::clone(&self.delete_batches);
        let locations = locations
            .inspect(move |result| {
                if let Ok(location) = result {
                    observed_batches
                        .lock()
                        .expect("DELETE batch observation mutex poisoned")[batch_index]
                        .push(location.to_string());
                }
            })
            .boxed();
        let fault = {
            let mut pending = self
                .fail_nth_delete_batch
                .lock()
                .expect("DELETE batch fault mutex poisoned");
            match pending.as_ref() {
                Some(armed) if armed.remaining_calls == 0 => {
                    pending.take().map(|pending| pending.fault)
                }
                Some(armed) => {
                    let remaining = armed.remaining_calls - 1;
                    pending
                        .as_mut()
                        .expect("DELETE batch fault must remain armed")
                        .remaining_calls = remaining;
                    None
                }
                None => None,
            }
        };
        let results = self.inner.delete_stream(locations);
        match fault {
            None => results,
            Some(DeleteBatchFault::MatchingNotFound { member }) => results
                .enumerate()
                .map(move |(index, result)| {
                    if index != member {
                        return result;
                    }
                    result.and_then(|path| {
                        Err(object_store::Error::NotFound {
                            path: path.to_string(),
                            source: "injected matching NotFound".into(),
                        })
                    })
                })
                .boxed(),
            Some(DeleteBatchFault::PerKeyErrorAfterApply { member }) => results
                .enumerate()
                .map(move |(index, result)| {
                    if index != member {
                        return result;
                    }
                    result.and_then(|_| {
                        Err(object_store::Error::Generic {
                            store: "history_metadata_control",
                            source: Box::new(std::io::Error::other(
                                "injected per-key DELETE failure after apply",
                            )),
                        })
                    })
                })
                .boxed(),
            Some(DeleteBatchFault::OverallResponseLostAfterApply) => stream::once(async move {
                let _applied_results = results.collect::<Vec<_>>().await;
                Err(object_store::Error::Generic {
                    store: "history_metadata_control",
                    source: Box::new(std::io::Error::other(
                        "injected DELETE response loss after apply",
                    )),
                })
            })
            .boxed(),
        }
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        let mutation = {
            let mut mutation = self
                .late_list_mutation
                .lock()
                .expect("late LIST mutation mutex poisoned");
            match mutation.as_mut() {
                Some(pending) if prefix == Some(pending.prefix()) => {
                    if pending.remaining_prefix_matches == 0 {
                        mutation.take()
                    } else {
                        pending.remaining_prefix_matches -= 1;
                        None
                    }
                }
                _ => None,
            }
        };
        let strip_versions = prefix.is_some_and(|prefix| {
            self.strip_list_version_prefixes
                .lock()
                .expect("LIST metadata control mutex poisoned")
                .contains(prefix)
        });
        let strip_version_keys = self
            .strip_list_version_keys
            .lock()
            .expect("LIST key metadata control mutex poisoned")
            .clone();
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
                    if strip_versions || strip_version_keys.contains(&object.location) {
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
                    if strip_versions || strip_version_keys.contains(&object.location) {
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
    gc_manifest().write(store, namespace).await.unwrap();
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

async fn seed_pending_delete_manifest(
    store: &ZeppelinStore,
    namespace: &str,
    count: usize,
    now: DateTime<Utc>,
) -> Vec<String> {
    gc_manifest_at(now).write(store, namespace).await.unwrap();
    let keys = (0..count)
        .map(|index| {
            WalFragment::s3_key(
                namespace,
                &ulid_at(
                    now - chrono::Duration::seconds(120),
                    u128::try_from(index + 1).expect("fixture index must fit ULID entropy"),
                ),
            )
        })
        .collect::<Vec<_>>();
    if keys.is_empty() {
        return keys;
    }
    let (mut manifest, version) = Manifest::read_versioned(store, namespace)
        .await
        .unwrap()
        .unwrap();
    manifest.pending_deletes = keys.clone();
    manifest.updated_at = now;
    manifest
        .write_conditional(store, namespace, &version)
        .await
        .unwrap();
    store
        .delete(&Manifest::history_key(namespace, manifest.version()))
        .await
        .unwrap();
    keys
}

async fn put_history_revision(store: &ZeppelinStore, namespace: &str, version: u64, revision: i64) {
    let updated_at = DateTime::<Utc>::from_timestamp(1_710_000_000 + revision, 0)
        .expect("fixed history revision timestamp must be valid");
    let manifest = gc_manifest_at(updated_at);
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

fn assert_only_full_namespace_inventory_lists(
    namespace: &str,
    counter: &GetCounter,
    control: &HistoryMetadataControlHandle,
    expected: u64,
) {
    let namespace_prefix = Path::parse(format!("{namespace}/"))
        .expect("namespace prefix must be a valid object path")
        .to_string();
    assert_eq!(
        counter.list_calls_for_prefix(&namespace_prefix),
        expected,
        "GC must issue exactly {expected} full namespace inventory LISTs"
    );
    assert_eq!(
        control.list_calls(),
        usize::try_from(expected).expect("expected LIST count must fit usize"),
        "GC must not issue history, snapshot, staging, or other sub-prefix LISTs"
    );
}

fn assert_at_most_two_full_namespace_inventory_lists(namespace: &str, counter: &GetCounter) {
    let namespace_prefix = Path::parse(format!("{namespace}/"))
        .expect("namespace prefix must be a valid object path")
        .to_string();
    let calls = counter.list_calls_for_prefix(&namespace_prefix);
    assert!(
        (1..=2).contains(&calls),
        "GC must use one initial and at most one fresh pre-delete namespace inventory, got {calls}"
    );
}

fn normalized_list_prefix(prefix: &str) -> String {
    Path::parse(prefix)
        .expect("LIST prefix must be a valid object path")
        .to_string()
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
    let ns = harness.artifact_origin_namespace("storage-gc-pending");
    let store = harness.store.clone();
    let pending_id = old_ulid(60, 19);
    let pending_key = WalFragment::s3_key(&ns, &pending_id);

    store
        .put(&pending_key, Bytes::from_static(b"pending delete body"))
        .await
        .unwrap();
    let mut manifest = gc_manifest();
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
    let ns = harness.artifact_origin_namespace("storage-gc-pending-failure");
    let store = harness.store.clone();
    let invalid_key = format!("{ns}/wal//invalid.wal");

    let mut manifest = gc_manifest();
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
    let ns = harness.artifact_origin_namespace("storage-gc-pending-horizon");
    let store = harness.store.clone();
    let pending_id = old_ulid(60, 39);
    let pending_key = WalFragment::s3_key(&ns, &pending_id);

    store
        .put(&pending_key, Bytes::from_static(b"pending delete body"))
        .await
        .unwrap();
    let mut manifest = gc_manifest();
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
async fn pending_delete_drain_chunks_zero_one_thousand_and_one_thousand_one() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let now = Utc::now();

    for count in [0usize, 1, 1_000, 1_001] {
        let namespace =
            harness.artifact_origin_namespace(&format!("storage-gc-pending-batch-{count}"));
        let keys = seed_pending_delete_manifest(&store, &namespace, count, now).await;
        let (controlled_store, control) =
            HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));

        let report =
            drain_pending_deletes_at(&controlled_store, &namespace, &unsafe_short_gc(0), now)
                .await
                .unwrap();

        let expected_batch_lengths = match count {
            0 => Vec::new(),
            1 => vec![1],
            1_000 => vec![1_000],
            1_001 => vec![1_000, 1],
            _ => unreachable!("fixture cardinality is closed"),
        };
        assert_eq!(
            control
                .delete_batches()
                .iter()
                .map(Vec::len)
                .collect::<Vec<_>>(),
            expected_batch_lengths,
            "pending-delete cardinality {count} must use deterministic S3-sized batches"
        );
        assert_eq!(report.objects_deleted, count);
        assert_eq!(report.entries_pruned, count);
        assert_eq!(report.entries_retained, 0);
        assert_eq!(
            Manifest::read(&store, &namespace)
                .await
                .unwrap()
                .unwrap()
                .pending_deletes,
            Vec::<String>::new()
        );
        assert_eq!(keys.len(), count);
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn pending_delete_drain_1001_retains_only_the_uncertain_batch() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let now = Utc::now();
    let namespace = harness.artifact_origin_namespace("storage-gc-pending-batch-partial-progress");
    let keys = seed_pending_delete_manifest(&store, &namespace, 1_001, now).await;
    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
    control.fault_nth_delete_batch(1, DeleteBatchFault::PerKeyErrorAfterApply { member: 999 });

    let partial = drain_pending_deletes_at(&controlled_store, &namespace, &unsafe_short_gc(0), now)
        .await
        .unwrap();

    assert_eq!(
        control
            .delete_batches()
            .iter()
            .map(Vec::len)
            .collect::<Vec<_>>(),
        vec![1_000, 1]
    );
    assert_eq!(partial.objects_deleted, 1);
    assert_eq!(partial.entries_pruned, 1);
    assert_eq!(partial.entries_retained, 1_000);
    let after_partial = Manifest::read(&store, &namespace).await.unwrap().unwrap();
    assert_eq!(
        after_partial.pending_deletes,
        keys[..1_000],
        "only the later confirmed batch may advance manifest metadata"
    );
    store
        .delete(&Manifest::history_key(&namespace, after_partial.version()))
        .await
        .unwrap();

    control.reset_observed_operations();
    let retry = drain_pending_deletes_at(
        &controlled_store,
        &namespace,
        &unsafe_short_gc(0),
        now + chrono::Duration::seconds(1),
    )
    .await
    .unwrap();
    assert_eq!(
        control
            .delete_batches()
            .iter()
            .map(Vec::len)
            .collect::<Vec<_>>(),
        Vec::<usize>::new(),
        "the repaired predecessor history must pin the uncertain batch"
    );
    assert_eq!(retry.objects_deleted, 0);
    assert_eq!(retry.entries_pruned, 0);
    assert_eq!(retry.entries_retained, 1_000);

    store
        .delete(&Manifest::history_key(&namespace, 2))
        .await
        .unwrap();
    control.reset_observed_operations();
    let after_retention = drain_pending_deletes_at(
        &controlled_store,
        &namespace,
        &unsafe_short_gc(0),
        now + chrono::Duration::seconds(2),
    )
    .await
    .unwrap();
    assert_eq!(
        control
            .delete_batches()
            .iter()
            .map(Vec::len)
            .collect::<Vec<_>>(),
        vec![1_000]
    );
    assert_eq!(after_retention.objects_deleted, 1_000);
    assert_eq!(after_retention.entries_pruned, 1_000);
    assert_eq!(after_retention.entries_retained, 0);

    harness.cleanup().await;
}

#[tokio::test]
async fn pending_delete_drain_accepts_matching_not_found_batch_members() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let now = Utc::now();
    let namespace = harness.artifact_origin_namespace("storage-gc-pending-batch-not-found");
    let keys = seed_pending_delete_manifest(&store, &namespace, 2, now).await;
    for key in &keys {
        store
            .put(key, Bytes::from_static(b"pending batch not-found fixture"))
            .await
            .unwrap();
    }
    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
    control.fault_nth_delete_batch(1, DeleteBatchFault::MatchingNotFound { member: 0 });

    let report = drain_pending_deletes_at(&controlled_store, &namespace, &unsafe_short_gc(0), now)
        .await
        .unwrap();

    assert_eq!(control.delete_batches(), vec![keys.clone()]);
    assert_eq!(report.objects_deleted, 2);
    assert_eq!(report.entries_pruned, 2);
    assert_eq!(report.entries_retained, 0);
    assert!(Manifest::read(&store, &namespace)
        .await
        .unwrap()
        .unwrap()
        .pending_deletes
        .is_empty());

    harness.cleanup().await;
}

#[tokio::test]
async fn pending_delete_drain_retains_every_member_of_uncertain_batches() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let now = Utc::now();

    for (suffix, fault) in [
        (
            "per-key",
            DeleteBatchFault::PerKeyErrorAfterApply { member: 1 },
        ),
        (
            "response-lost",
            DeleteBatchFault::OverallResponseLostAfterApply,
        ),
    ] {
        let namespace =
            harness.artifact_origin_namespace(&format!("storage-gc-pending-batch-{suffix}"));
        let keys = seed_pending_delete_manifest(&store, &namespace, 3, now).await;
        for key in &keys {
            store
                .put(key, Bytes::from_static(b"pending uncertain batch fixture"))
                .await
                .unwrap();
        }
        let (controlled_store, control) =
            HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
        control.fault_nth_delete_batch(1, fault);

        let uncertain =
            drain_pending_deletes_at(&controlled_store, &namespace, &unsafe_short_gc(0), now)
                .await
                .unwrap();

        assert_eq!(control.delete_batches(), vec![keys.clone()]);
        assert_eq!(uncertain.objects_deleted, 0);
        assert_eq!(uncertain.entries_pruned, 0);
        assert_eq!(uncertain.entries_retained, keys.len());
        assert_eq!(
            Manifest::read(&store, &namespace)
                .await
                .unwrap()
                .unwrap()
                .pending_deletes,
            keys,
            "an uncertain response cannot authorize manifest pruning"
        );

        control.reset_observed_operations();
        let retry = drain_pending_deletes_at(
            &controlled_store,
            &namespace,
            &unsafe_short_gc(0),
            now + chrono::Duration::seconds(1),
        )
        .await
        .unwrap();
        assert_eq!(control.delete_batches().len(), 1);
        assert_eq!(retry.objects_deleted, 3);
        assert_eq!(retry.entries_pruned, 3);
        assert_eq!(retry.entries_retained, 0);
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn standalone_pending_delete_validates_every_live_overlap_before_batching() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let namespace =
        harness.artifact_origin_namespace("storage-gc-standalone-pending-live-overlap-barrier");
    let now = Utc::now();
    let safe_id = ulid_at(now - chrono::Duration::seconds(120), 1);
    let live_id = ulid_at(now - chrono::Duration::seconds(120), 2);
    let safe_key = WalFragment::s3_key(&namespace, &safe_id);
    let live_key = WalFragment::s3_key(&namespace, &live_id);

    gc_manifest_at(now).write(&store, &namespace).await.unwrap();
    for key in [&safe_key, &live_key] {
        store
            .put(key, Bytes::from_static(b"pending live-overlap fixture"))
            .await
            .unwrap();
    }
    let (mut manifest, version) = Manifest::read_versioned(&store, &namespace)
        .await
        .unwrap()
        .unwrap();
    manifest.add_fragment(FragmentRef {
        id: live_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 28,
        artifact_origin: None,
    });
    manifest.pending_deletes = vec![safe_key.clone(), live_key.clone()];
    manifest.updated_at = now;
    manifest
        .write_conditional(&store, &namespace, &version)
        .await
        .unwrap();
    store
        .delete(&Manifest::history_key(&namespace, manifest.version()))
        .await
        .unwrap();
    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));

    let error = drain_pending_deletes_at(&controlled_store, &namespace, &unsafe_short_gc(0), now)
        .await
        .expect_err("a live pending-delete overlap must fail before any batch starts");

    assert!(error.to_string().contains(&live_key));
    assert!(control.delete_batches().is_empty());
    assert_eq!(control.delete_calls(), 0);
    assert_s3_object_exists(&store, &safe_key).await;
    assert_s3_object_exists(&store, &live_key).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn warm_pending_delete_validates_every_live_overlap_before_batching() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let namespace = harness.artifact_origin_namespace("storage-gc-pending-live-overlap-barrier");
    let now = Utc::now();
    let safe_id = ulid_at(now - chrono::Duration::seconds(120), 1);
    let live_id = ulid_at(now - chrono::Duration::seconds(120), 2);
    let safe_key = WalFragment::s3_key(&namespace, &safe_id);
    let live_key = WalFragment::s3_key(&namespace, &live_id);

    gc_manifest_at(now).write(&store, &namespace).await.unwrap();
    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), now);
    let mut runner = GcRunner::new(controlled_store, memo_gc());
    runner.run_cycle_at(incarnation.clone(), now).await.unwrap();

    store
        .put(
            &safe_key,
            Bytes::from_static(b"eligible earlier pending key"),
        )
        .await
        .unwrap();
    store
        .put(&live_key, Bytes::from_static(b"still-live pending key"))
        .await
        .unwrap();
    let (mut manifest, version) = Manifest::read_versioned(&store, &namespace)
        .await
        .unwrap()
        .unwrap();
    manifest.add_fragment(FragmentRef {
        id: live_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 22,
        artifact_origin: None,
    });
    manifest.pending_deletes = vec![safe_key.clone(), live_key.clone()];
    manifest.updated_at = now + chrono::Duration::seconds(1);
    manifest
        .write_conditional(&store, &namespace, &version)
        .await
        .unwrap();
    store
        .delete(&Manifest::history_key(&namespace, manifest.version()))
        .await
        .unwrap();

    control.reset_observed_operations();
    let report = runner
        .run_cycle_at(incarnation, now + chrono::Duration::seconds(1))
        .await
        .unwrap();

    assert_eq!(report, GcCycleReport::default());
    assert!(control.delete_batches().is_empty());
    assert_eq!(control.delete_calls(), 0);
    assert_s3_object_exists(&store, &safe_key).await;
    assert_s3_object_exists(&store, &live_key).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_cycle_retains_objects_referenced_only_by_manifest_history() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("storage-gc-history-mark-sweep");
    let store = harness.store.clone();
    let old_id = old_ulid(60, 49);
    let old_key = WalFragment::s3_key(&ns, &old_id);

    store
        .put(&old_key, Bytes::from_static(b"history-only fragment body"))
        .await
        .unwrap();
    let mut manifest = gc_manifest();
    manifest.add_fragment(FragmentRef {
        id: old_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 26,
        artifact_origin: None,
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
    let ns = harness.artifact_origin_namespace("storage-gc-history-sweep-race");
    let store = harness.store.clone();
    let old_id = old_ulid(60, 129);
    let old_key = WalFragment::s3_key(&ns, &old_id);

    store
        .put(&old_key, Bytes::from_static(b"history race fragment body"))
        .await
        .unwrap();

    let mut history_manifest = gc_manifest();
    history_manifest.add_fragment(FragmentRef {
        id: old_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 26,
        artifact_origin: None,
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
    let ns = harness.artifact_origin_namespace("storage-gc-history-drain-race");
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

    gc_manifest().write(&store, &ns).await.unwrap();
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

    let mut injected_history = gc_manifest();
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
    let ns = harness.artifact_origin_namespace("storage-gc-pitr-time");
    let store = harness.store.clone();
    let old_id = old_ulid(60, 89);
    let old_key = WalFragment::s3_key(&ns, &old_id);

    store
        .put(&old_key, Bytes::from_static(b"time retained body"))
        .await
        .unwrap();
    let mut manifest = gc_manifest();
    manifest.add_fragment(FragmentRef {
        id: old_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 18,
        artifact_origin: None,
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
    let ns = harness.artifact_origin_namespace("storage-gc-pitr-expired");
    let store = harness.store.clone();
    let old_id = old_ulid(60, 99);
    let old_key = WalFragment::s3_key(&ns, &old_id);

    store
        .put(&old_key, Bytes::from_static(b"expired history body"))
        .await
        .unwrap();
    let mut manifest = gc_manifest();
    manifest.updated_at = Utc::now() - chrono::Duration::seconds(60);
    manifest.add_fragment(FragmentRef {
        id: old_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 20,
        artifact_origin: None,
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
    let ns = harness.artifact_origin_namespace("storage-gc-pitr-pin");
    let store = harness.store.clone();
    let old_id = old_ulid(60, 109);
    let old_key = WalFragment::s3_key(&ns, &old_id);

    store
        .put(&old_key, Bytes::from_static(b"snapshot pinned body"))
        .await
        .unwrap();
    let mut manifest = gc_manifest();
    manifest.updated_at = Utc::now() - chrono::Duration::seconds(60);
    manifest.add_fragment(FragmentRef {
        id: old_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 20,
        artifact_origin: None,
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
    let ns = harness.artifact_origin_namespace("storage-gc-pitr-pin-pending");
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
    gc_manifest().write(&store, &ns).await.unwrap();
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
    let ns = harness.artifact_origin_namespace("storage-gc-history-pending");
    let store = harness.store.clone();
    let old_id = old_ulid(60, 59);
    let old_key = WalFragment::s3_key(&ns, &old_id);

    store
        .put(&old_key, Bytes::from_static(b"history pending-delete body"))
        .await
        .unwrap();
    let mut manifest = gc_manifest();
    manifest.add_fragment(FragmentRef {
        id: old_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 27,
        artifact_origin: None,
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
    let ns = harness.artifact_origin_namespace("storage-gc-history-cost");
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

    let mut manifest = gc_manifest();
    manifest.add_fragment(FragmentRef {
        id: old_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 22,
        artifact_origin: None,
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
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-history-memo");
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
    assert_eq!(counter.gets_matching(&history_prefix), 2);
    for version in 1..=3 {
        assert_eq!(
            counter.gets_matching(&Manifest::history_key(&namespace, version)),
            0,
            "existing generation {version} must remain memoized"
        );
    }
    assert_eq!(
        counter.gets_matching(&Manifest::history_key(&namespace, 4)),
        2,
        "a newly listed generation must be read during prune and final sweep"
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
    assert_eq!(counter.gets_matching(&history_prefix), 2);
    assert_eq!(
        counter.gets_matching(&Manifest::history_key(&namespace, 4)),
        2,
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
async fn gc_runner_warm_empty_pending_reuses_prune_history_roots() {
    let harness = TestHarness::new().await;
    let namespace =
        harness.artifact_origin_namespace("storage-gc-runner-prune-roots-empty-pending");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);
    seed_manifest_history(&store, &namespace, 3).await;

    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, history_prefix.clone());
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut gc = memo_gc();
    let mut runner = GcRunner::new(counted_store, gc.clone());
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());
    let now = Utc::now();

    run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;

    // Keep the retained-history policy identical while forcing a warm full
    // cycle. The single full namespace inventory supplies history, snapshot,
    // mark, and non-destructive sweep inputs; no sub-prefix LIST is required.
    gc.compaction_upload_window_secs += 1;
    runner.update_config(gc);
    let report = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
        &control,
    )
    .await;

    assert_eq!(report.pending_deletes_deleted, 0);
    assert_eq!(report.pending_deletes_pruned, 0);
    assert_eq!(report.pending_deletes_retained, 0);
    assert_eq!(
        counter.list_calls_for_prefix(&normalized_list_prefix(&history_prefix)),
        0,
        "warm empty-pending GC must derive history from the namespace inventory"
    );
    assert_only_full_namespace_inventory_lists(&namespace, &counter, &control, 1);
    assert_eq!(
        counter.gets_matching(&history_prefix),
        0,
        "unchanged retained generation bodies must remain memoized"
    );
    assert_eq!(
        counter.puts_matching(&format!("{namespace}/_gc/candidates.json")),
        0,
        "a canonical candidate ledger whose contents did not change must not be rewritten"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_warm_all_young_pending_reuses_prune_history_roots() {
    let harness = TestHarness::new().await;
    let namespace =
        harness.artifact_origin_namespace("storage-gc-runner-prune-roots-young-pending");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);
    let now = Utc::now();
    seed_manifest_history(&store, &namespace, 3).await;

    let pending_id = ulid_at(now, 201);
    let pending_key = WalFragment::s3_key(&namespace, &pending_id);
    store
        .put(&pending_key, Bytes::from_static(b"young pending delete"))
        .await
        .unwrap();
    let (mut current, etag) = Manifest::read_versioned(&store, &namespace)
        .await
        .unwrap()
        .unwrap();
    current.pending_deletes.push(pending_key.clone());
    current.updated_at = now;
    current
        .write_conditional(&store, &namespace, &etag)
        .await
        .unwrap();
    store
        .delete(&Manifest::history_key(&namespace, current.version()))
        .await
        .unwrap();

    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, history_prefix.clone());
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut gc = GcConfig {
        manifest_history_keep_count: 64,
        pitr_retention_secs: 0,
        ..unsafe_short_gc(60)
    };
    let mut runner = GcRunner::new(counted_store, gc.clone());
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());

    let cold = run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;
    assert_eq!(cold.pending_deletes_retained, 1);

    gc.compaction_upload_window_secs += 1;
    runner.update_config(gc);
    let warm = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
        &control,
    )
    .await;

    assert_eq!(warm.pending_deletes_deleted, 0);
    assert_eq!(warm.pending_deletes_pruned, 0);
    assert_eq!(warm.pending_deletes_retained, 1);
    assert_s3_object_exists(&store, &pending_key).await;
    assert_eq!(
        counter.list_calls_for_prefix(&normalized_list_prefix(&history_prefix)),
        0,
        "a too-young pending queue must derive history from the namespace inventory"
    );
    assert_only_full_namespace_inventory_lists(&namespace, &counter, &control, 1);

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_warm_prune_roots_protect_every_pending_delete_without_refresh() {
    let harness = TestHarness::new().await;
    let namespace =
        harness.artifact_origin_namespace("storage-gc-runner-prune-roots-protected-pending");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);
    let now = Utc::now();
    let pending_id = ulid_at(now - chrono::Duration::seconds(60), 202);
    let pending_key = WalFragment::s3_key(&namespace, &pending_id);
    store
        .put(
            &pending_key,
            Bytes::from_static(b"history-protected pending delete"),
        )
        .await
        .unwrap();

    let mut retained = gc_manifest_at(now - chrono::Duration::seconds(30));
    retained.add_fragment(FragmentRef {
        id: pending_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 32,
        artifact_origin: None,
    });
    retained.write(&store, &namespace).await.unwrap();
    let (mut current, etag) = Manifest::read_versioned(&store, &namespace)
        .await
        .unwrap()
        .unwrap();
    current.fragments.clear();
    current.pending_deletes.push(pending_key.clone());
    current.updated_at = now;
    current
        .write_conditional(&store, &namespace, &etag)
        .await
        .unwrap();

    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, history_prefix.clone());
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut gc = memo_gc();
    let mut runner = GcRunner::new(counted_store, gc.clone());
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());

    let cold = run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;
    assert_eq!(cold.pending_deletes_retained, 1);

    gc.compaction_upload_window_secs += 1;
    runner.update_config(gc);
    let warm = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
        &control,
    )
    .await;

    assert_eq!(warm.pending_deletes_deleted, 0);
    assert_eq!(warm.pending_deletes_pruned, 0);
    assert_eq!(warm.pending_deletes_retained, 1);
    assert_s3_object_exists(&store, &pending_key).await;
    assert_eq!(
        counter.list_calls_for_prefix(&normalized_list_prefix(&history_prefix)),
        0,
        "inventory-decoded roots that protect every pending key need no sub-prefix refresh"
    );
    assert_only_full_namespace_inventory_lists(&namespace, &counter, &control, 1);

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_warm_eligible_pending_refresh_sees_new_history_root_before_delete() {
    let harness = TestHarness::new().await;
    let namespace =
        harness.artifact_origin_namespace("storage-gc-runner-eligible-pending-history-race");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);
    let now = Utc::now();
    let pending_id = ulid_at(now - chrono::Duration::seconds(60), 203);
    let pending_key = WalFragment::s3_key(&namespace, &pending_id);
    seed_manifest_history(&store, &namespace, 2).await;

    let mut injected_history = gc_manifest_at(now);
    injected_history.add_fragment(FragmentRef {
        id: pending_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 29,
        artifact_origin: None,
    });
    let (injecting_store, injection) = PutOnNthDeleteStore::wrap(
        store.inner(),
        Manifest::history_key(&namespace, 1),
        Manifest::history_key(&namespace, 4),
        manifest_json_bytes_with_version(&injected_history, 4),
        1,
    );
    let injecting_store = ZeppelinStore::new(Arc::new(injecting_store));
    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&injecting_store, history_prefix.clone());
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(
        counted_store,
        GcConfig {
            manifest_history_keep_count: 2,
            pitr_retention_secs: 0,
            ..unsafe_short_gc(0)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());
    run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;

    store
        .put(
            &pending_key,
            Bytes::from_static(b"eligible pending history race"),
        )
        .await
        .unwrap();
    let (mut current, etag) = Manifest::read_versioned(&store, &namespace)
        .await
        .unwrap()
        .unwrap();
    current.pending_deletes.push(pending_key.clone());
    current.updated_at = now;
    current
        .write_conditional(&store, &namespace, &etag)
        .await
        .unwrap();
    store
        .delete(&Manifest::history_key(&namespace, current.version()))
        .await
        .unwrap();

    runner.update_config(GcConfig {
        manifest_history_keep_count: 1,
        pitr_retention_secs: 0,
        ..unsafe_short_gc(0)
    });
    let raced = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
        &control,
    )
    .await;

    assert_eq!(
        injection.puts_injected(),
        1,
        "the fixture must publish a retained root after prune and before the pending-delete decision"
    );
    assert_eq!(raced.pending_deletes_deleted, 0);
    assert_eq!(raced.pending_deletes_pruned, 0);
    assert_eq!(raced.pending_deletes_retained, 1);
    assert_s3_object_exists(&store, &pending_key).await;
    assert_eq!(
        Manifest::read(&store, &namespace)
            .await
            .unwrap()
            .unwrap()
            .pending_deletes,
        vec![pending_key.clone()]
    );
    assert_eq!(
        counter.list_calls_for_prefix(&normalized_list_prefix(&history_prefix)),
        0,
        "an eligible pending DELETE must derive fresh retained history from the second namespace inventory"
    );
    assert_only_full_namespace_inventory_lists(&namespace, &counter, &control, 2);

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_warm_prune_root_reuse_keeps_final_sweep_history_fresh() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-prune-roots-fresh-sweep");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);
    let now = Utc::now();
    let orphan_id = ulid_at(now - chrono::Duration::seconds(60), 204);
    let orphan_key = WalFragment::s3_key(&namespace, &orphan_id);
    seed_manifest_history(&store, &namespace, 1).await;

    let mut injected_history = gc_manifest_at(now);
    injected_history.add_fragment(FragmentRef {
        id: orphan_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 26,
        artifact_origin: None,
    });
    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, history_prefix.clone());
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(counted_store, memo_gc());
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());
    run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;

    store
        .put(&orphan_key, Bytes::from_static(b"fresh sweep history race"))
        .await
        .unwrap();
    let injected_history_key = Manifest::history_key(&namespace, 2);
    control.put_on_nth_list(
        &format!("{namespace}/"),
        &injected_history_key,
        manifest_json_bytes_with_version(&injected_history, 2),
        2,
    );
    let raced = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
        &control,
    )
    .await;

    assert!(
        Manifest::read_history(&store, &namespace, 2)
            .await
            .unwrap()
            .is_some(),
        "the second full namespace LIST must publish the retained history root"
    );
    assert_eq!(
        raced.objects_deleted, 0,
        "the final sweep history refresh must protect a newly rooted candidate"
    );
    assert_s3_object_exists(&store, &orphan_key).await;
    assert!(
        load_gc_candidates(&store, &namespace)
            .await
            .unwrap()
            .iter()
            .any(|candidate| candidate.key == orphan_key),
        "a candidate that becomes reachable at sweep remains recorded for later reconciliation"
    );
    assert_only_full_namespace_inventory_lists(&namespace, &counter, &control, 2);

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_failed_eligible_pending_history_refresh_cannot_authorize_idle() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-pending-refresh-failure");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);
    let now = Utc::now();
    let pending_id = ulid_at(now - chrono::Duration::seconds(60), 205);
    let pending_key = WalFragment::s3_key(&namespace, &pending_id);
    seed_manifest_history(&store, &namespace, 2).await;

    let mut injected_history = gc_manifest_at(now);
    injected_history.add_fragment(FragmentRef {
        id: pending_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 30,
        artifact_origin: None,
    });
    let injected_history_key = Manifest::history_key(&namespace, 4);
    let (injecting_store, injection) = PutOnNthDeleteStore::wrap(
        store.inner(),
        Manifest::history_key(&namespace, 1),
        injected_history_key.clone(),
        manifest_json_bytes_with_version(&injected_history, 4),
        1,
    );
    let injecting_store = ZeppelinStore::new(Arc::new(injecting_store));
    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&injecting_store, history_prefix.clone());
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(
        counted_store,
        GcConfig {
            manifest_history_keep_count: 2,
            pitr_retention_secs: 0,
            ..unsafe_short_gc(0)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());
    run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;

    store
        .put(&pending_key, Bytes::from_static(b"pending refresh failure"))
        .await
        .unwrap();
    let (mut current, etag) = Manifest::read_versioned(&store, &namespace)
        .await
        .unwrap()
        .unwrap();
    current.pending_deletes.push(pending_key.clone());
    current.updated_at = now;
    current
        .write_conditional(&store, &namespace, &etag)
        .await
        .unwrap();
    store
        .delete(&Manifest::history_key(&namespace, current.version()))
        .await
        .unwrap();

    control.fail_next_get(injected_history_key);
    runner.update_config(GcConfig {
        manifest_history_keep_count: 1,
        pitr_retention_secs: 0,
        ..unsafe_short_gc(0)
    });
    let failed = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
        &control,
    )
    .await;
    assert_eq!(injection.puts_injected(), 1);
    assert_eq!(failed, GcCycleReport::default());
    assert_s3_object_exists(&store, &pending_key).await;

    let retry = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(2),
        &counter,
        &control,
    )
    .await;
    assert_only_full_namespace_inventory_lists(&namespace, &counter, &control, 1);
    assert!(
        counter.total_gets() > 0,
        "the one-inventory retry must reload the history generation whose prior refresh failed instead of taking the idle fast path"
    );
    assert_eq!(retry.pending_deletes_deleted, 0);
    assert_eq!(retry.pending_deletes_pruned, 0);
    assert_eq!(retry.pending_deletes_retained, 1);
    assert_s3_object_exists(&store, &pending_key).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_pending_cas_retry_refreshes_history_and_invalidates_idle_on_transient_root() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-pending-cas-retry-root");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);
    let now = Utc::now();
    let first_id = ulid_at(now - chrono::Duration::seconds(120), 206);
    let first_key = WalFragment::s3_key(&namespace, &first_id);
    let concurrent_id = ulid_at(now - chrono::Duration::seconds(120), 207);
    let concurrent_key = WalFragment::s3_key(&namespace, &concurrent_id);
    seed_manifest_history(&store, &namespace, 2).await;

    let mut concurrent = gc_manifest_at(now);
    concurrent.pending_deletes.push(concurrent_key.clone());
    let concurrent_body = manifest_json_bytes_with_version(&concurrent, 4);
    let missing_predecessor_history = Manifest::history_key(&namespace, 3);
    let history_four = Manifest::history_key(&namespace, 4);
    let (publishing_store, publication) = PublishManifestOnHistoryWriteStore::wrap(
        store.inner(),
        missing_predecessor_history,
        history_four.clone(),
        Manifest::s3_key(&namespace),
        concurrent_body.clone(),
        concurrent_body,
        true,
    );
    let publishing_store = ZeppelinStore::new(Arc::new(publishing_store));
    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&publishing_store, history_prefix.clone());
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(counted_store, memo_gc());
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());
    run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;

    store
        .put(&first_key, Bytes::from_static(b"first pending CAS delete"))
        .await
        .unwrap();
    store
        .put(
            &concurrent_key,
            Bytes::from_static(b"concurrently protected pending delete"),
        )
        .await
        .unwrap();
    let (mut current, etag) = Manifest::read_versioned(&store, &namespace)
        .await
        .unwrap()
        .unwrap();
    current.pending_deletes.push(first_key.clone());
    current.updated_at = now;
    current
        .write_conditional(&store, &namespace, &etag)
        .await
        .unwrap();
    store
        .delete(&Manifest::history_key(&namespace, current.version()))
        .await
        .unwrap();

    let raced = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
        &control,
    )
    .await;
    assert!(
        publication.published(),
        "the fixture must replace live manifest authority after the first physical DELETE and before its stale CAS"
    );
    assert!(
        !publication.history_removed(),
        "after the CAS conflict the cycle must not perform another manifest read that could authorize new DELETEs"
    );
    assert_eq!(raced.pending_deletes_deleted, 1);
    assert_eq!(raced.pending_deletes_pruned, 0);
    assert_eq!(raced.pending_deletes_retained, 1);
    assert_s3_object_not_exists(&store, &first_key).await;
    assert_s3_object_exists(&store, &concurrent_key).await;
    assert_eq!(
        Manifest::read(&store, &namespace)
            .await
            .unwrap()
            .unwrap()
            .pending_deletes,
        vec![concurrent_key.clone()],
        "the CAS retry must preserve the new queue published by the concurrent writer"
    );
    assert_eq!(
        control.delete_batches(),
        vec![vec![first_key.clone()]],
        "a confirmed batch must not be reissued after the manifest CAS conflict"
    );
    assert_at_most_two_full_namespace_inventory_lists(&namespace, &counter);

    store.delete(&history_four).await.unwrap();

    let candidate_phase = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(2),
        &counter,
        &control,
    )
    .await;
    assert_at_most_two_full_namespace_inventory_lists(&namespace, &counter);
    assert!(
        counter.total_gets() > 0,
        "removing the conflict-time history root must force real reconciliation work on the next inventory"
    );
    assert_eq!(candidate_phase.pending_deletes_deleted, 0);
    assert_s3_object_exists(&store, &concurrent_key).await;

    let retry = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(3),
        &counter,
        &control,
    )
    .await;
    assert_at_most_two_full_namespace_inventory_lists(&namespace, &counter);
    assert_eq!(retry.pending_deletes_deleted, 1);
    assert_s3_object_not_exists(&store, &concurrent_key).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_pending_cas_retry_skips_history_refresh_for_empty_or_young_queue() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let now = Utc::now();

    for (suffix, publish_young, entropy) in [("empty", false, 208), ("young", true, 209)] {
        let namespace =
            harness.artifact_origin_namespace(&format!("storage-gc-runner-pending-cas-{suffix}"));
        let history_prefix = Manifest::history_prefix(&namespace);
        let first_id = ulid_at(now - chrono::Duration::seconds(120), entropy);
        let first_key = WalFragment::s3_key(&namespace, &first_id);
        let young_key = WalFragment::s3_key(&namespace, &ulid_at(now, entropy + 10));
        seed_manifest_history(&store, &namespace, 2).await;

        let mut concurrent = gc_manifest_at(now);
        if publish_young {
            concurrent.pending_deletes.push(young_key.clone());
        }
        let concurrent_body = manifest_json_bytes_with_version(&concurrent, 4);
        let (publishing_store, publication) = PublishManifestOnHistoryWriteStore::wrap(
            store.inner(),
            Manifest::history_key(&namespace, 3),
            Manifest::history_key(&namespace, 4),
            Manifest::s3_key(&namespace),
            concurrent_body.clone(),
            concurrent_body,
            false,
        );
        let publishing_store = ZeppelinStore::new(Arc::new(publishing_store));
        let (controlled_store, control) =
            HistoryMetadataControlStore::wrap(&publishing_store, history_prefix.clone());
        let (counted_store, counter) = counting_store(&controlled_store);
        let mut runner = GcRunner::new(
            counted_store,
            GcConfig {
                manifest_history_keep_count: 64,
                pitr_retention_secs: 0,
                ..unsafe_short_gc(60)
            },
        );
        let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());
        run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;

        store
            .put(&first_key, Bytes::from_static(b"first pending CAS delete"))
            .await
            .unwrap();
        if publish_young {
            store
                .put(&young_key, Bytes::from_static(b"young concurrent pending"))
                .await
                .unwrap();
        }
        let (mut current, etag) = Manifest::read_versioned(&store, &namespace)
            .await
            .unwrap()
            .unwrap();
        current.pending_deletes.push(first_key.clone());
        current.updated_at = now;
        current
            .write_conditional(&store, &namespace, &etag)
            .await
            .unwrap();
        store
            .delete(&Manifest::history_key(&namespace, current.version()))
            .await
            .unwrap();

        let raced = run_observed_gc_cycle(
            &mut runner,
            &incarnation,
            now + chrono::Duration::seconds(1),
            &counter,
            &control,
        )
        .await;
        assert!(publication.published());
        assert_eq!(raced.pending_deletes_deleted, 1);
        assert_s3_object_not_exists(&store, &first_key).await;
        if publish_young {
            assert_eq!(raced.pending_deletes_retained, 1);
            assert_s3_object_exists(&store, &young_key).await;
        } else {
            assert_eq!(raced.pending_deletes_retained, 0);
        }
        assert_eq!(
            Manifest::read(&store, &namespace)
                .await
                .unwrap()
                .unwrap()
                .pending_deletes,
            if publish_young {
                vec![young_key]
            } else {
                Vec::new()
            }
        );
        assert_at_most_two_full_namespace_inventory_lists(&namespace, &counter);
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_missing_history_etag_is_never_cacheable() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-history-unversioned");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);
    seed_manifest_history(&store, &namespace, 3).await;

    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, history_prefix.clone());
    control.set_strip_list_versions(true);
    for generation in 1..=3 {
        control
            .set_strip_list_version_for_key(&Manifest::history_key(&namespace, generation), true);
    }
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
        6,
        "uncacheable history must be reread during prune and final sweep on every warm cycle"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_failed_refresh_does_not_commit_partial_memo() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-history-refresh-failure");
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
        4,
        "retry must reload both changed entries; no partial failed refresh may commit"
    );
    assert_eq!(counter.gets_matching(&first_key), 2);
    assert_eq!(counter.gets_matching(&second_key), 2);
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

#[derive(Clone, Copy)]
enum LateHistoryBodyFailure {
    Corrupt,
    Missing,
}

async fn assert_late_history_body_failure_prevents_prune_deletes(failure: LateHistoryBodyFailure) {
    let harness = TestHarness::new().await;
    let suffix = match failure {
        LateHistoryBodyFailure::Corrupt => "corrupt",
        LateHistoryBodyFailure::Missing => "missing",
    };
    let namespace =
        harness.artifact_origin_namespace(&format!("storage-gc-runner-prune-barrier-{suffix}"));
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
    run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;

    put_history_revision(&store, &namespace, 1, 31).await;
    put_history_revision(&store, &namespace, 2, 32).await;
    match failure {
        LateHistoryBodyFailure::Corrupt => {
            store
                .put(&third_key, Bytes::from_static(b"corrupt retained history"))
                .await
                .unwrap();
        }
        LateHistoryBodyFailure::Missing => {
            put_history_revision(&store, &namespace, 3, 33).await;
            control.miss_next_get(third_key.clone());
        }
    }

    let mut pruning_gc = memo_gc();
    pruning_gc.manifest_history_keep_count = 1;
    runner.update_config(pruning_gc);
    let report = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
        &control,
    )
    .await;

    assert_eq!(report, GcCycleReport::default());
    assert_eq!(counter.gets_matching(&history_prefix), 3);
    assert_eq!(counter.gets_matching(&first_key), 1);
    assert_eq!(counter.gets_matching(&second_key), 1);
    assert_eq!(counter.gets_matching(&third_key), 1);
    assert_eq!(
        control.delete_calls(),
        0,
        "no prunable generation may be deleted before every retention body validates"
    );
    assert_s3_object_exists(&store, &first_key).await;
    assert_s3_object_exists(&store, &second_key).await;
    assert_s3_object_exists(&store, &third_key).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_warm_corrupt_history_aborts_before_any_prune_delete() {
    assert_late_history_body_failure_prevents_prune_deletes(LateHistoryBodyFailure::Corrupt).await;
}

#[tokio::test]
async fn gc_runner_warm_missing_history_aborts_before_any_prune_delete() {
    assert_late_history_body_failure_prevents_prune_deletes(LateHistoryBodyFailure::Missing).await;
}

#[tokio::test]
async fn manifest_history_prune_batches_only_after_every_body_validates() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let namespace = harness.artifact_origin_namespace("storage-gc-direct-history-batch");
    seed_manifest_history(&store, &namespace, 3).await;
    let old_keys = vec![
        Manifest::history_key(&namespace, 1),
        Manifest::history_key(&namespace, 2),
    ];
    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
    control.fault_nth_delete_batch(1, DeleteBatchFault::MatchingNotFound { member: 0 });

    let pruned = Manifest::prune_history(&controlled_store, &namespace, 1)
        .await
        .unwrap();

    assert_eq!(pruned, 2);
    assert_eq!(control.delete_batches(), vec![old_keys.clone()]);
    for key in old_keys {
        assert_s3_object_not_exists(&store, &key).await;
    }
    assert_s3_object_exists(&store, &Manifest::history_key(&namespace, 3)).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn manifest_history_prune_corrupt_late_body_prevents_every_batch() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let namespace =
        harness.artifact_origin_namespace("storage-gc-direct-history-validation-barrier");
    seed_manifest_history(&store, &namespace, 3).await;
    let history_keys = (1..=3)
        .map(|version| Manifest::history_key(&namespace, version))
        .collect::<Vec<_>>();
    store
        .put(
            &history_keys[2],
            Bytes::from_static(b"corrupt newest retained history"),
        )
        .await
        .unwrap();
    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));

    Manifest::prune_history(&controlled_store, &namespace, 1)
        .await
        .expect_err("every history body must validate before the first DELETE batch");

    assert!(control.delete_batches().is_empty());
    assert_eq!(control.delete_calls(), 0);
    for key in history_keys {
        assert_s3_object_exists(&store, &key).await;
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn stateless_gc_history_prune_corrupt_late_body_prevents_every_batch() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let namespace =
        harness.artifact_origin_namespace("storage-gc-stateless-history-validation-barrier");
    seed_manifest_history(&store, &namespace, 3).await;
    let history_keys = (1..=3)
        .map(|version| Manifest::history_key(&namespace, version))
        .collect::<Vec<_>>();
    store
        .put(
            &history_keys[2],
            Bytes::from_static(b"corrupt newest retained history"),
        )
        .await
        .unwrap();
    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));

    let report = run_gc_cycle_at(
        &controlled_store,
        &namespace,
        &GcConfig {
            manifest_history_keep_count: 1,
            ..unsafe_short_gc(0)
        },
        Utc::now(),
    )
    .await
    .unwrap();

    assert_eq!(report, GcCycleReport::default());
    assert!(control.delete_batches().is_empty());
    assert_eq!(control.delete_calls(), 0);
    for key in history_keys {
        assert_s3_object_exists(&store, &key).await;
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn manifest_history_prune_fails_loud_on_uncertain_delete_batches() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();

    for (suffix, fault) in [
        (
            "per-key",
            DeleteBatchFault::PerKeyErrorAfterApply { member: 1 },
        ),
        (
            "response-lost",
            DeleteBatchFault::OverallResponseLostAfterApply,
        ),
    ] {
        let namespace =
            harness.artifact_origin_namespace(&format!("storage-gc-direct-history-{suffix}"));
        seed_manifest_history(&store, &namespace, 3).await;
        let old_keys = vec![
            Manifest::history_key(&namespace, 1),
            Manifest::history_key(&namespace, 2),
        ];
        let (controlled_store, control) =
            HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
        control.fault_nth_delete_batch(1, fault);

        Manifest::prune_history(&controlled_store, &namespace, 1)
            .await
            .expect_err("an uncertain history batch cannot return a successful prune result");
        assert_eq!(control.delete_batches(), vec![old_keys.clone()]);
        for key in &old_keys {
            assert_s3_object_not_exists(&store, key).await;
        }

        control.reset_observed_operations();
        assert_eq!(
            Manifest::prune_history(&controlled_store, &namespace, 1)
                .await
                .unwrap(),
            0,
            "retry must reconcile the already-applied ambiguous batch from S3 authority"
        );
        assert!(control.delete_batches().is_empty());
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn stateless_and_warm_gc_history_prune_use_one_bounded_batch() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let now = Utc::now();
    let pruning_gc = GcConfig {
        manifest_history_keep_count: 1,
        ..unsafe_short_gc(0)
    };

    let stateless_namespace =
        harness.artifact_origin_namespace("storage-gc-stateless-history-batch");
    seed_manifest_history(&store, &stateless_namespace, 3).await;
    let (stateless_store, stateless_control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&stateless_namespace));
    run_gc_cycle_at(&stateless_store, &stateless_namespace, &pruning_gc, now)
        .await
        .unwrap();
    assert_eq!(
        stateless_control.delete_batches(),
        vec![vec![
            Manifest::history_key(&stateless_namespace, 1),
            Manifest::history_key(&stateless_namespace, 2),
        ]]
    );

    let warm_namespace = harness.artifact_origin_namespace("storage-gc-warm-history-batch");
    seed_manifest_history(&store, &warm_namespace, 3).await;
    let (warm_store, warm_control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&warm_namespace));
    let incarnation = GcNamespaceIncarnation::new(warm_namespace.clone(), now);
    let mut runner = GcRunner::new(warm_store, memo_gc());
    runner.run_cycle_at(incarnation.clone(), now).await.unwrap();
    warm_control.reset_observed_operations();
    runner.update_config(pruning_gc);
    runner
        .run_cycle_at(incarnation, now + chrono::Duration::seconds(1))
        .await
        .unwrap();
    assert_eq!(
        warm_control.delete_batches(),
        vec![vec![
            Manifest::history_key(&warm_namespace, 1),
            Manifest::history_key(&warm_namespace, 2),
        ]]
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_idle_gate_skips_unchanged_and_wakes_on_inventory_change() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-idle-inventory");
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
async fn gc_runner_warm_due_non_delete_uses_one_full_namespace_inventory() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-one-inventory-non-delete");
    let store = harness.store.clone();
    let now = Utc::now();
    let orphan_id = ulid_at(now - chrono::Duration::seconds(60), 301);
    let orphan_key = WalFragment::s3_key(&namespace, &orphan_id);

    gc_manifest_at(now).write(&store, &namespace).await.unwrap();
    store
        .put(
            &orphan_key,
            Bytes::from_static(b"due candidate becomes live"),
        )
        .await
        .unwrap();

    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(
        counted_store,
        GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(10)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());

    let marked = run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;
    assert_eq!(marked.candidates_marked, 1);

    let (mut live, version) = Manifest::read_versioned(&store, &namespace)
        .await
        .unwrap()
        .unwrap();
    live.add_fragment(FragmentRef {
        id: orphan_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 26,
        artifact_origin: None,
    });
    live.updated_at = now + chrono::Duration::seconds(1);
    live.write_conditional(&store, &namespace, &version)
        .await
        .unwrap();

    let due = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(10),
        &counter,
        &control,
    )
    .await;

    assert_eq!(due.objects_deleted, 0);
    assert_s3_object_exists(&store, &orphan_key).await;
    assert_only_full_namespace_inventory_lists(&namespace, &counter, &control, 1);

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_mature_candidate_delete_uses_two_full_namespace_inventories() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-two-inventories-delete");
    let store = harness.store.clone();
    let now = Utc::now();
    let orphan_id = ulid_at(now - chrono::Duration::seconds(60), 302);
    let orphan_key = WalFragment::s3_key(&namespace, &orphan_id);

    gc_manifest_at(now).write(&store, &namespace).await.unwrap();
    store
        .put(&orphan_key, Bytes::from_static(b"mature candidate delete"))
        .await
        .unwrap();

    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(
        counted_store,
        GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(10)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());

    let marked = run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;
    assert_eq!(marked.candidates_marked, 1);

    let due = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(11),
        &counter,
        &control,
    )
    .await;

    assert_eq!(due.objects_deleted, 1);
    assert_s3_object_not_exists(&store, &orphan_key).await;
    assert_only_full_namespace_inventory_lists(&namespace, &counter, &control, 2);
    assert_eq!(
        counter.puts_matching(&format!("{namespace}/_gc/candidates.json")),
        1,
        "a mature unchanged mark needs only the final cleanup PUT after deletion"
    );
    assert!(
        load_gc_candidates(&store, &namespace)
            .await
            .unwrap()
            .is_empty(),
        "the cleanup PUT must durably remove the deleted candidate"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_sweep_retains_every_candidate_when_batch_response_is_lost() {
    let harness = TestHarness::new().await;
    let namespace =
        harness.artifact_origin_namespace("storage-gc-runner-sweep-batch-response-lost");
    let store = harness.store.clone();
    let now = Utc::now();
    let orphan_keys = (0..3)
        .map(|index| {
            WalFragment::s3_key(
                &namespace,
                &ulid_at(now - chrono::Duration::seconds(60), 400 + index),
            )
        })
        .collect::<Vec<_>>();

    gc_manifest_at(now).write(&store, &namespace).await.unwrap();
    for key in &orphan_keys {
        store
            .put(key, Bytes::from_static(b"uncertain sweep candidate"))
            .await
            .unwrap();
    }
    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
    let mut runner = GcRunner::new(
        controlled_store,
        GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(10)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), now);
    let marked = runner.run_cycle_at(incarnation.clone(), now).await.unwrap();
    assert_eq!(marked.candidates_marked, orphan_keys.len());

    control.reset_observed_operations();
    control.fault_nth_delete_batch(1, DeleteBatchFault::OverallResponseLostAfterApply);
    let uncertain = runner
        .run_cycle_at(incarnation.clone(), now + chrono::Duration::seconds(11))
        .await
        .unwrap();

    assert_eq!(control.delete_batches(), vec![orphan_keys.clone()]);
    assert_eq!(uncertain.objects_deleted, 0);
    assert!(
        uncertain.candidates_skipped >= orphan_keys.len(),
        "every uncertain member must contribute a delete-failed skip"
    );
    assert_eq!(
        load_gc_candidates(&store, &namespace)
            .await
            .unwrap()
            .into_iter()
            .map(|candidate| candidate.key)
            .collect::<Vec<_>>(),
        orphan_keys,
        "an uncertain batch must remain durably retryable as a whole"
    );

    control.reset_observed_operations();
    let reconciled = runner
        .run_cycle_at(incarnation, now + chrono::Duration::seconds(12))
        .await
        .unwrap();
    assert_eq!(reconciled.objects_deleted, 0);
    assert!(control.delete_batches().is_empty());
    assert!(load_gc_candidates(&store, &namespace)
        .await
        .unwrap()
        .is_empty());

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_changed_candidate_ledger_after_mark_prevents_sweep() {
    let harness = TestHarness::new().await;
    let namespace =
        harness.artifact_origin_namespace("storage-gc-runner-ledger-changed-before-sweep");
    let store = harness.store.clone();
    let now = Utc::now();
    let orphan_id = ulid_at(now - chrono::Duration::seconds(60), 318);
    let orphan_key = WalFragment::s3_key(&namespace, &orphan_id);
    let candidate_ledger_key = format!("{namespace}/_gc/candidates.json");

    gc_manifest_at(now).write(&store, &namespace).await.unwrap();
    store
        .put(
            &orphan_key,
            Bytes::from_static(b"candidate whose durable mark is replaced"),
        )
        .await
        .unwrap();

    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(
        counted_store,
        GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(10)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), now);

    let marked = run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;
    assert_eq!(marked.candidates_marked, 1);

    control.replace_on_nth_list(
        &format!("{namespace}/"),
        &candidate_ledger_key,
        Bytes::from(
            serde_json::to_vec_pretty(&serde_json::json!({
                "version": 1,
                "candidates": [],
            }))
            .unwrap(),
        ),
        2,
    );
    let raced = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(11),
        &counter,
        &control,
    )
    .await;

    assert_eq!(raced.objects_deleted, 0);
    assert_eq!(
        control.delete_calls(),
        0,
        "a candidate cannot be swept after its durable mark identity changes"
    );
    assert_s3_object_exists(&store, &orphan_key).await;
    assert!(
        load_gc_candidates(&store, &namespace)
            .await
            .unwrap()
            .is_empty(),
        "the injected replacement ledger must remain authoritative"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_warm_new_immature_candidate_is_persisted_once_and_survives() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-new-candidate-one-put");
    let store = harness.store.clone();
    let now = Utc::now();
    let orphan_id = ulid_at(now - chrono::Duration::seconds(60), 314);
    let orphan_key = WalFragment::s3_key(&namespace, &orphan_id);
    let candidate_ledger_key = format!("{namespace}/_gc/candidates.json");

    gc_manifest_at(now).write(&store, &namespace).await.unwrap();

    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(
        counted_store,
        GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(10)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), now);

    run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;
    store
        .put(
            &orphan_key,
            Bytes::from_static(b"new candidate must be durable before sweep"),
        )
        .await
        .unwrap();

    let marked = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
        &control,
    )
    .await;

    assert_eq!(marked.candidates_marked, 1);
    assert_eq!(marked.objects_deleted, 0);
    assert_eq!(
        counter.puts_matching(&candidate_ledger_key),
        1,
        "a new immature candidate needs one durable mark PUT and no unchanged final PUT"
    );
    assert_eq!(
        load_gc_candidates(&store, &namespace)
            .await
            .unwrap()
            .into_iter()
            .map(|candidate| candidate.key)
            .collect::<Vec<_>>(),
        vec![orphan_key.clone()]
    );
    assert_s3_object_exists(&store, &orphan_key).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_warm_equal_legacy_candidate_ledger_migrates_once() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-legacy-candidate-one-put");
    let store = harness.store.clone();
    let now = Utc::now();
    let orphan_id = ulid_at(now - chrono::Duration::seconds(120), 315);
    let orphan_key = WalFragment::s3_key(&namespace, &orphan_id);
    let candidate_ledger_key = format!("{namespace}/_gc/candidates.json");
    let candidate = GcCandidate {
        key: orphan_key.clone(),
        first_seen_unreachable_at: now,
        unreachable_since_manifest_version: 1,
    };

    gc_manifest_at(now).write(&store, &namespace).await.unwrap();
    store
        .put(&orphan_key, Bytes::from_static(b"legacy candidate"))
        .await
        .unwrap();
    save_gc_candidates(&store, &namespace, std::slice::from_ref(&candidate))
        .await
        .unwrap();

    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(
        counted_store,
        GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(60)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), now);
    run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;

    store
        .put(
            &candidate_ledger_key,
            Bytes::from(serde_json::to_vec(&vec![candidate.clone()]).unwrap()),
        )
        .await
        .unwrap();

    let report = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
        &control,
    )
    .await;

    assert_eq!(report.objects_deleted, 0);
    assert_eq!(
        counter.puts_matching(&candidate_ledger_key),
        1,
        "semantic equality must not suppress the one required legacy-to-canonical migration PUT"
    );
    assert_eq!(
        load_gc_candidates(&store, &namespace).await.unwrap(),
        vec![candidate]
    );
    let persisted: serde_json::Value =
        serde_json::from_slice(&store.get(&candidate_ledger_key).await.unwrap()).unwrap();
    assert_eq!(persisted.get("version"), Some(&serde_json::json!(1)));
    assert!(persisted.get("candidates").is_some());
    assert_s3_object_exists(&store, &orphan_key).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_required_mark_put_failure_prevents_candidate_delete() {
    let harness = TestHarness::new().await;
    let namespace =
        harness.artifact_origin_namespace("storage-gc-runner-required-mark-put-failure");
    let store = harness.store.clone();
    let now = Utc::now();
    let orphan_id = ulid_at(now - chrono::Duration::seconds(60), 316);
    let orphan_key = WalFragment::s3_key(&namespace, &orphan_id);
    let candidate_ledger_key = format!("{namespace}/_gc/candidates.json");

    gc_manifest_at(now).write(&store, &namespace).await.unwrap();
    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(
        counted_store,
        GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(0)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), now);
    run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;

    store
        .put(
            &orphan_key,
            Bytes::from_static(b"candidate cannot be swept before its mark is durable"),
        )
        .await
        .unwrap();
    control.fail_nth_put(&candidate_ledger_key, 1);

    let failed = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
        &control,
    )
    .await;

    assert_eq!(failed.candidates_marked, 0);
    assert_eq!(failed.objects_deleted, 0);
    assert_eq!(control.delete_calls(), 0);
    assert_s3_object_exists(&store, &orphan_key).await;
    assert!(load_gc_candidates(&store, &namespace)
        .await
        .unwrap()
        .is_empty());

    harness.cleanup().await;
}

#[tokio::test]
async fn cold_one_shot_gc_preserves_two_exact_candidate_ledger_puts() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("storage-gc-cold-ledger-two-puts");
    let store = harness.store.clone();
    let now = Utc::now();
    let candidate_ledger_key = format!("{namespace}/_gc/candidates.json");

    gc_manifest_at(now).write(&store, &namespace).await.unwrap();
    let (counted_store, counter) = counting_store(&store);
    counter.reset();

    let report = run_gc_cycle_at(&counted_store, &namespace, &memo_gc(), now)
        .await
        .unwrap();

    assert_eq!(report.candidates_marked, 0);
    assert_eq!(report.objects_deleted, 0);
    assert_eq!(
        counter.puts_matching(&candidate_ledger_key),
        2,
        "the frozen cold one-shot path intentionally preserves its two exact ledger PUTs"
    );
    assert!(load_gc_candidates(&store, &namespace)
        .await
        .unwrap()
        .is_empty());

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_unversioned_sibling_cannot_hide_candidate_replacement_before_delete() {
    let harness = TestHarness::new().await;
    let namespace =
        harness.artifact_origin_namespace("storage-gc-runner-unversioned-sibling-replacement");
    let store = harness.store.clone();
    let now = Utc::now();
    let orphan_id = ulid_at(now - chrono::Duration::seconds(60), 306);
    let orphan_key = WalFragment::s3_key(&namespace, &orphan_id);
    let unversioned_sibling_key = format!("{namespace}/ordinary-unversioned.bin");

    gc_manifest_at(now).write(&store, &namespace).await.unwrap();
    store
        .put(
            &orphan_key,
            Bytes::from_static(b"original mature candidate body"),
        )
        .await
        .unwrap();
    store
        .put(
            &unversioned_sibling_key,
            Bytes::from_static(b"unrelated ordinary object"),
        )
        .await
        .unwrap();

    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(
        counted_store,
        GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(10)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());

    let marked = run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;
    assert_eq!(marked.candidates_marked, 1);

    control.set_strip_list_version_for_key(&unversioned_sibling_key, true);
    control.replace_on_nth_list(
        &format!("{namespace}/"),
        &orphan_key,
        Bytes::from_static(b"replacement candidate body with a new identity"),
        2,
    );
    let due = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(10),
        &counter,
        &control,
    )
    .await;

    assert_eq!(
        due.objects_deleted, 0,
        "a candidate replaced between authoritative inventories must be retained even when an unrelated object lacks LIST identity"
    );
    assert_eq!(
        control.delete_calls(),
        0,
        "GC must not issue a DELETE for the replaced candidate"
    );
    assert_s3_object_exists(&store, &orphan_key).await;
    assert_only_full_namespace_inventory_lists(&namespace, &counter, &control, 2);

    let next_cycle = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(11),
        &counter,
        &control,
    )
    .await;
    assert_eq!(
        next_cycle.objects_deleted, 0,
        "the replacement must wait through its own complete GC horizon"
    );
    assert_s3_object_exists(&store, &orphan_key).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_pending_predelete_inventory_cannot_hide_candidate_replacement() {
    let harness = TestHarness::new().await;
    let namespace =
        harness.artifact_origin_namespace("storage-gc-runner-pending-inventory-replacement");
    let store = harness.store.clone();
    let now = Utc::now();
    let candidate_id = ulid_at(now - chrono::Duration::seconds(60), 311);
    let candidate_key = WalFragment::s3_key(&namespace, &candidate_id);
    let pending_id = ulid_at(now - chrono::Duration::seconds(60), 312);
    let pending_key = WalFragment::s3_key(&namespace, &pending_id);

    gc_manifest_at(now).write(&store, &namespace).await.unwrap();
    store
        .put(
            &candidate_key,
            Bytes::from_static(b"original mature candidate body"),
        )
        .await
        .unwrap();

    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(
        counted_store,
        GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(10)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());

    let marked = run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;
    assert_eq!(marked.candidates_marked, 1);

    store
        .put(
            &pending_key,
            Bytes::from_static(b"late history retains pending delete"),
        )
        .await
        .unwrap();
    let (mut live, version) = Manifest::read_versioned(&store, &namespace)
        .await
        .unwrap()
        .unwrap();
    live.pending_deletes.push(pending_key.clone());
    live.write_conditional(&store, &namespace, &version)
        .await
        .unwrap();
    // The just-published history generation would make the warm prune result
    // retain the pending key and skip its pre-delete refresh. Remove that
    // generation so a new root can appear only in the second inventory.
    store
        .delete(&Manifest::history_key(&namespace, live.version()))
        .await
        .unwrap();

    let late_history_generation = live.version() + 1;
    let mut late_history = gc_manifest_at(now + chrono::Duration::seconds(1));
    late_history.add_fragment(FragmentRef {
        id: pending_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 43,
        artifact_origin: None,
    });
    let late_history_key = Manifest::history_key(&namespace, late_history_generation);
    let replacement_body = Bytes::from_static(b"replacement candidate body with new identity");
    control.set_late_list_mutation(LateListMutation {
        prefix: Path::parse(format!("{namespace}/"))
            .expect("namespace prefix must be a valid object path"),
        remaining_prefix_matches: 1,
        operations: vec![
            LateListOperation::Delete {
                key: Path::parse(&candidate_key).expect("candidate key must be valid"),
            },
            LateListOperation::Put {
                key: Path::parse(&candidate_key).expect("candidate key must be valid"),
                body: replacement_body.clone(),
            },
            LateListOperation::Put {
                key: Path::parse(&late_history_key).expect("history key must be valid"),
                body: manifest_json_bytes_with_version(&late_history, late_history_generation),
            },
        ],
    });

    let due = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(10),
        &counter,
        &control,
    )
    .await;

    assert_eq!(
        due.pending_deletes_deleted, 0,
        "the late history root must retain the eligible pending key without a physical DELETE"
    );
    assert_eq!(due.pending_deletes_pruned, 0);
    assert_eq!(due.pending_deletes_retained, 1);
    assert_eq!(
        due.objects_deleted, 0,
        "the pending pre-delete inventory must not authorize deletion of a candidate whose identity changed since the opening inventory"
    );
    assert_eq!(
        control.delete_calls(),
        0,
        "GC must not issue a DELETE for the replacement candidate"
    );
    assert_eq!(
        store.get(&candidate_key).await.unwrap(),
        replacement_body,
        "the replacement candidate body must survive the raced cycle"
    );
    assert_s3_object_exists(&store, &pending_key).await;
    assert_s3_object_exists(&store, &late_history_key).await;
    assert_only_full_namespace_inventory_lists(&namespace, &counter, &control, 2);

    let next_cycle = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(11),
        &counter,
        &control,
    )
    .await;
    assert_eq!(
        next_cycle.objects_deleted, 0,
        "the pending-path replacement must wait through its own complete GC horizon"
    );
    assert_s3_object_exists(&store, &candidate_key).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_replacement_horizon_survives_candidate_cleanup_put_failure() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-replacement-put-failure");
    let store = harness.store.clone();
    let now = Utc::now() - chrono::Duration::seconds(30);
    let candidate_id = ulid_at(now - chrono::Duration::seconds(60), 313);
    let candidate_key = WalFragment::s3_key(&namespace, &candidate_id);
    let candidate_ledger_key = format!("{namespace}/_gc/candidates.json");

    gc_manifest_at(now).write(&store, &namespace).await.unwrap();
    store
        .put(
            &candidate_key,
            Bytes::from_static(b"original candidate before failed re-mark"),
        )
        .await
        .unwrap();

    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(
        counted_store,
        GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(10)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), now);

    let marked = run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;
    assert_eq!(marked.candidates_marked, 1);

    control.replace_on_nth_list(
        &format!("{namespace}/"),
        &candidate_key,
        Bytes::from_static(b"replacement whose durable re-mark will fail"),
        2,
    );
    control.fail_nth_put(&candidate_ledger_key, 1);
    let raced = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(10),
        &counter,
        &control,
    )
    .await;
    assert_eq!(raced.objects_deleted, 0);
    assert_s3_object_exists(&store, &candidate_key).await;

    let mut restarted = GcRunner::new(
        controlled_store,
        GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(10)
        },
    );
    let retry = restarted
        .run_cycle_at(incarnation, now + chrono::Duration::seconds(11))
        .await
        .unwrap();
    assert_eq!(
        retry.objects_deleted, 0,
        "a cold restart after failed re-mark must not revive the original candidate's stale horizon"
    );
    assert_s3_object_exists(&store, &candidate_key).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn cold_and_stateless_candidate_replacement_requires_fresh_predelete_inventory() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let now = DateTime::<Utc>::from_timestamp(1_900_000_000, 0)
        .expect("fixed cold replacement timestamp must be valid");

    for (suffix, use_runner, entropy) in [("runner", true, 316), ("stateless", false, 317)] {
        let namespace =
            harness.artifact_origin_namespace(&format!("storage-gc-cold-replacement-{suffix}"));
        let candidate_key = WalFragment::s3_key(
            &namespace,
            &ulid_at(now - chrono::Duration::seconds(60), entropy),
        );
        let candidate_ledger_key = format!("{namespace}/_gc/candidates.json");
        gc_manifest_at(now).write(&store, &namespace).await.unwrap();
        store
            .put(
                &candidate_key,
                Bytes::from_static(b"original cold candidate"),
            )
            .await
            .unwrap();

        let (controlled_store, control) =
            HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
        let gc = GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(10)
        };
        let incarnation = GcNamespaceIncarnation::new(namespace.clone(), now);
        let marked = GcRunner::new(controlled_store.clone(), gc.clone())
            .run_cycle_at(incarnation.clone(), now)
            .await
            .unwrap();
        assert_eq!(marked.candidates_marked, 1);

        control.reset_observed_operations();
        let replacement = Bytes::from_static(b"replacement published after opening inventory");
        control.put_on_nth_get(
            &candidate_ledger_key,
            &candidate_key,
            replacement.clone(),
            1,
        );
        let report = if use_runner {
            GcRunner::new(controlled_store.clone(), gc.clone())
                .run_cycle_at(incarnation.clone(), now + chrono::Duration::seconds(11))
                .await
        } else {
            run_gc_cycle_at(
                &controlled_store,
                &namespace,
                &gc,
                now + chrono::Duration::seconds(11),
            )
            .await
        }
        .unwrap();

        assert_eq!(
            report.objects_deleted, 0,
            "{suffix} path must not delete a candidate replaced after its opening inventory"
        );
        assert_eq!(control.delete_calls(), 0);
        assert_eq!(store.get(&candidate_key).await.unwrap(), replacement);
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_malformed_reserved_inventory_key_aborts_before_delete() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-malformed-control-key");
    let store = harness.store.clone();
    let now = Utc::now();
    let orphan_id = ulid_at(now - chrono::Duration::seconds(60), 303);
    let orphan_key = WalFragment::s3_key(&namespace, &orphan_id);

    gc_manifest_at(now).write(&store, &namespace).await.unwrap();
    store
        .put(
            &orphan_key,
            Bytes::from_static(b"must survive malformed control key"),
        )
        .await
        .unwrap();

    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(
        counted_store,
        GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(10)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());

    let marked = run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;
    assert_eq!(marked.candidates_marked, 1);

    let malformed_staging_key = format!("{namespace}/_staging/not-a-token.json");
    store
        .put(
            &malformed_staging_key,
            Bytes::from_static(b"{\"fencing_token\":0,\"keys\":[]}"),
        )
        .await
        .unwrap();

    counter.reset();
    control.reset_observed_operations();
    let error = runner
        .run_cycle_at(incarnation, now + chrono::Duration::seconds(10))
        .await
        .expect_err("malformed keys under reserved control prefixes must fail loud");
    assert!(
        error.to_string().contains(&malformed_staging_key),
        "the validation error must identify the malformed remote key: {error}"
    );
    assert_eq!(
        control.delete_calls(),
        0,
        "inventory validation must complete before any physical DELETE"
    );
    assert_s3_object_exists(&store, &orphan_key).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn malformed_reserved_inventory_key_fails_loud_on_cold_and_stateless_paths() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("storage-gc-cold-malformed-control-key");
    let store = harness.store.clone();
    let now = Utc::now();
    gc_manifest_at(now).write(&store, &namespace).await.unwrap();
    let malformed_key = format!("{namespace}/_staging/not-a-token.json");
    store
        .put(
            &malformed_key,
            Bytes::from_static(b"{\"fencing_token\":0,\"keys\":[]}"),
        )
        .await
        .unwrap();

    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
    let gc = GcConfig {
        manifest_history_keep_count: 64,
        ..unsafe_short_gc(0)
    };
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), now);
    let mut runner = GcRunner::new(controlled_store.clone(), gc.clone());

    let cold_error = runner
        .run_cycle_at(incarnation, now)
        .await
        .expect_err("a cold runner must reject malformed reserved keys");
    assert!(cold_error.to_string().contains(&malformed_key));

    control.reset_observed_operations();
    let stateless_error = run_gc_cycle_at(&controlled_store, &namespace, &gc, now)
        .await
        .expect_err("the stateless GC entrypoint must reject malformed reserved keys");
    assert!(stateless_error.to_string().contains(&malformed_key));

    harness.cleanup().await;
}

#[tokio::test]
async fn malformed_reserved_inventory_key_precedes_cold_candidate_ledger_decode_failure() {
    let harness = TestHarness::new().await;
    let namespace =
        harness.artifact_origin_namespace("storage-gc-cold-malformed-key-corrupt-candidates");
    let store = harness.store.clone();
    let now = Utc::now();
    gc_manifest_at(now).write(&store, &namespace).await.unwrap();
    let malformed_key = format!("{namespace}/_staging/not-a-token.json");
    store
        .put(
            &malformed_key,
            Bytes::from_static(b"{\"fencing_token\":0,\"keys\":[]}"),
        )
        .await
        .unwrap();
    store
        .put(
            &format!("{namespace}/_gc/candidates.json"),
            Bytes::from_static(b"not valid candidate-ledger JSON"),
        )
        .await
        .unwrap();

    let (controlled_store, _) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
    let gc = GcConfig {
        manifest_history_keep_count: 64,
        ..unsafe_short_gc(0)
    };
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), now);
    let mut runner = GcRunner::new(controlled_store.clone(), gc.clone());

    let cold_error = runner
        .run_cycle_at(incarnation, now)
        .await
        .expect_err("a corrupt candidate ledger must not mask a malformed reserved LIST key");
    assert!(
        matches!(
            &cold_error,
            zeppelin::error::ZeppelinError::MalformedControlKey { family, key, .. }
                if *family == "staging" && key == &malformed_key
        ),
        "cold GC must return the malformed-key error, got {cold_error:?}"
    );

    let stateless_error = run_gc_cycle_at(&controlled_store, &namespace, &gc, now)
        .await
        .expect_err(
            "the stateless entrypoint must not let a corrupt candidate ledger mask a malformed reserved LIST key",
        );
    assert!(
        matches!(
            &stateless_error,
            zeppelin::error::ZeppelinError::MalformedControlKey { family, key, .. }
                if *family == "staging" && key == &malformed_key
        ),
        "stateless GC must return the malformed-key error, got {stateless_error:?}"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn malformed_reserved_inventory_key_precedes_cold_pending_delete() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let now = Utc::now();

    for (suffix, use_runner, entropy) in [("runner", true, 314), ("stateless", false, 315)] {
        let namespace = harness
            .artifact_origin_namespace(&format!("storage-gc-cold-pending-malformed-{suffix}"));
        let pending_key = WalFragment::s3_key(
            &namespace,
            &ulid_at(now - chrono::Duration::seconds(60), entropy),
        );
        let malformed_key = format!("{namespace}/_staging/not-a-token.json");
        let mut manifest = gc_manifest_at(now);
        manifest.pending_deletes.push(pending_key.clone());
        manifest.write(&store, &namespace).await.unwrap();
        store
            .delete(&Manifest::history_key(&namespace, manifest.version()))
            .await
            .unwrap();
        store
            .put(&pending_key, Bytes::from_static(b"eligible pending delete"))
            .await
            .unwrap();
        store
            .put(
                &malformed_key,
                Bytes::from_static(b"{\"fencing_token\":0,\"keys\":[]}"),
            )
            .await
            .unwrap();

        let (controlled_store, control) =
            HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
        let gc = GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(0)
        };
        let result = if use_runner {
            GcRunner::new(controlled_store, gc)
                .run_cycle_at(GcNamespaceIncarnation::new(namespace.clone(), now), now)
                .await
        } else {
            run_gc_cycle_at(&controlled_store, &namespace, &gc, now).await
        };

        let error = result.expect_err("malformed reserved keys must preempt pending DELETEs");
        assert!(
            matches!(&error, zeppelin::error::ZeppelinError::MalformedControlKey { key, .. }
                if key == &malformed_key),
            "typed malformed-key error must win over pending work, got {error:?}"
        );
        assert_eq!(
            control.delete_calls(),
            0,
            "{suffix} path must validate the inventory before any physical DELETE"
        );
        assert_s3_object_exists(&store, &pending_key).await;
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_cold_retained_history_reread_fails_loud_on_late_malformed_key() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("storage-gc-cold-late-malformed-history-key");
    let store = harness.store.clone();
    let now = Utc::now();
    gc_manifest_at(now).write(&store, &namespace).await.unwrap();

    let history_prefix = Manifest::history_prefix(&namespace);
    let malformed_key = format!("{history_prefix}not-a-generation.msgpack");
    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, history_prefix.clone());
    control.put_on_nth_list(
        &history_prefix,
        &malformed_key,
        Bytes::from_static(b"malformed history control key"),
        2,
    );
    let mut runner = GcRunner::new(
        controlled_store,
        GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(0)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), now);

    let result = runner.run_cycle_at(incarnation, now).await;
    assert_s3_object_exists(&store, &malformed_key).await;
    let error = result.expect_err(
        "a malformed history key discovered after cold pruning must fail the cycle loud",
    );
    assert!(
        matches!(
            &error,
            zeppelin::error::ZeppelinError::MalformedControlKey { family, key, .. }
                if *family == "manifest-history" && key == &malformed_key
        ),
        "late malformed history key must retain its typed error, got {error:?}"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_second_namespace_inventory_sees_new_history_root_before_delete() {
    let harness = TestHarness::new().await;
    let namespace =
        harness.artifact_origin_namespace("storage-gc-runner-second-inventory-history-root");
    let store = harness.store.clone();
    let now = Utc::now();
    let orphan_id = ulid_at(now - chrono::Duration::seconds(60), 304);
    let orphan_key = WalFragment::s3_key(&namespace, &orphan_id);

    gc_manifest_at(now).write(&store, &namespace).await.unwrap();
    store
        .put(
            &orphan_key,
            Bytes::from_static(b"late history root must protect candidate"),
        )
        .await
        .unwrap();

    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(
        counted_store,
        GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(10)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());

    let marked = run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;
    assert_eq!(marked.candidates_marked, 1);

    let mut late_history = gc_manifest_at(now + chrono::Duration::seconds(1));
    late_history.add_fragment(FragmentRef {
        id: orphan_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 40,
        artifact_origin: None,
    });
    let late_history_key = Manifest::history_key(&namespace, 2);
    control.put_on_nth_list(
        &format!("{namespace}/"),
        &late_history_key,
        manifest_json_bytes_with_version(&late_history, 2),
        2,
    );

    let due = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(10),
        &counter,
        &control,
    )
    .await;

    assert_eq!(
        due.objects_deleted, 0,
        "a retained history root on the pre-delete inventory must protect the candidate"
    );
    assert_s3_object_exists(&store, &orphan_key).await;
    assert!(
        Manifest::read_history(&store, &namespace, 2)
            .await
            .unwrap()
            .is_some(),
        "the second namespace LIST hook must publish the retained history root"
    );
    assert_only_full_namespace_inventory_lists(&namespace, &counter, &control, 2);

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_same_token_staging_published_after_predelete_list_protects_candidate() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-late-active-staging");
    let store = harness.store.clone();
    let now = Utc::now();
    let orphan_id = ulid_at(now - chrono::Duration::seconds(60), 305);
    let orphan_key = WalFragment::s3_key(&namespace, &orphan_id);
    let lease_key = format!("{namespace}/lease.json");
    let fencing_token = 57;
    let staging_key = format!("{namespace}/_staging/{fencing_token}.json");

    gc_manifest_at(now).write(&store, &namespace).await.unwrap();
    store
        .put(
            &orphan_key,
            Bytes::from_static(b"late active staging must protect candidate"),
        )
        .await
        .unwrap();
    let lease = serde_json::json!({
        "holder_id": "late-staging-holder",
        "fencing_token": fencing_token,
        "acquired_at": now,
        "expires_at": now + chrono::Duration::seconds(60),
    });
    store
        .put(&lease_key, Bytes::from(serde_json::to_vec(&lease).unwrap()))
        .await
        .unwrap();

    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(
        counted_store,
        GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(10)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());

    let marked = run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;
    assert_eq!(marked.candidates_marked, 1);

    let staging = serde_json::json!({
        "fencing_token": fencing_token,
        "keys": [orphan_key.clone()],
    });
    // The due warm cycle reads the lease once from its initial inventory, then
    // takes the full pre-delete inventory, then reads the lease again for the
    // destructive decision. Publish on that second lease GET so the staging
    // record was not present in either inventory but has the same live token.
    control.put_on_nth_get(
        &lease_key,
        &staging_key,
        Bytes::from(serde_json::to_vec(&staging).unwrap()),
        2,
    );
    let due = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(10),
        &counter,
        &control,
    )
    .await;

    assert_eq!(
        due.objects_deleted, 0,
        "the post-LIST active staging root must veto the candidate DELETE"
    );
    assert_s3_object_exists(&store, &orphan_key).await;
    assert_s3_object_exists(&store, &staging_key).await;
    assert_eq!(
        control.delete_calls(),
        0,
        "GC must not attempt any physical DELETE after discovering the late active staging root"
    );
    assert_only_full_namespace_inventory_lists(&namespace, &counter, &control, 2);

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_eligible_pending_delete_requires_history_and_manifest_list_etags() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let now = Utc::now();

    for (case, strip_history) in [("history", true), ("manifest", false)] {
        let namespace =
            harness.artifact_origin_namespace(&format!("storage-gc-runner-pending-{case}-etag"));
        let pending_id = ulid_at(now, if strip_history { 306 } else { 307 });
        let pending_key = WalFragment::s3_key(&namespace, &pending_id);
        store
            .put(
                &pending_key,
                Bytes::from_static(b"pending delete requires versioned authority"),
            )
            .await
            .unwrap();
        gc_manifest_at(now).write(&store, &namespace).await.unwrap();
        let (mut manifest, version) = Manifest::read_versioned(&store, &namespace)
            .await
            .unwrap()
            .unwrap();
        manifest.pending_deletes.push(pending_key.clone());
        manifest
            .write_conditional(&store, &namespace, &version)
            .await
            .unwrap();
        store
            .delete(&Manifest::history_key(&namespace, manifest.version()))
            .await
            .unwrap();

        let history_prefix = Manifest::history_prefix(&namespace);
        let (controlled_store, control) = HistoryMetadataControlStore::wrap(&store, history_prefix);
        let (counted_store, counter) = counting_store(&controlled_store);
        let mut runner = GcRunner::new(
            counted_store,
            GcConfig {
                manifest_history_keep_count: 64,
                ..unsafe_short_gc(10)
            },
        );
        let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());

        let young = run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;
        assert_eq!(young.pending_deletes_deleted, 0);
        assert_s3_object_exists(&store, &pending_key).await;

        let stripped_key = if strip_history {
            Manifest::history_key(&namespace, 1)
        } else {
            Manifest::s3_key(&namespace)
        };
        control.set_strip_list_version_for_key(&stripped_key, true);
        let due = run_observed_gc_cycle(
            &mut runner,
            &incarnation,
            now + chrono::Duration::seconds(11),
            &counter,
            &control,
        )
        .await;

        assert_eq!(
            due.pending_deletes_deleted, 0,
            "missing {case} LIST ETag must fail closed before a pending DELETE"
        );
        assert_eq!(
            control.delete_calls(),
            0,
            "missing {case} LIST ETag must prevent every physical DELETE"
        );
        assert_s3_object_exists(&store, &pending_key).await;
        assert_eq!(
            Manifest::read(&store, &namespace)
                .await
                .unwrap()
                .unwrap()
                .pending_deletes,
            vec![pending_key],
            "the pending entry must remain queued when {case} authority is unversioned"
        );
        assert_at_most_two_full_namespace_inventory_lists(&namespace, &counter);
        let namespace_prefix = Path::parse(format!("{namespace}/"))
            .expect("namespace prefix must be a valid object path")
            .to_string();
        assert_eq!(
            control.list_calls(),
            usize::try_from(counter.list_calls_for_prefix(&namespace_prefix))
                .expect("observed LIST count must fit usize"),
            "GC must not issue history, snapshot, staging, or other sub-prefix LISTs"
        );
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_candidate_phase_progresses_during_pending_delete_churn() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-pending-fairness");
    let store = harness.store.clone();
    let now = Utc::now();

    gc_manifest_at(now).write(&store, &namespace).await.unwrap();
    let (controlled_store, control) =
        HistoryMetadataControlStore::wrap(&store, Manifest::history_prefix(&namespace));
    let (counted_store, counter) = counting_store(&controlled_store);
    let mut runner = GcRunner::new(
        counted_store,
        GcConfig {
            manifest_history_keep_count: 64,
            ..unsafe_short_gc(0)
        },
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), Utc::now());
    run_observed_gc_cycle(&mut runner, &incarnation, now, &counter, &control).await;

    let first_pending_key = WalFragment::s3_key(
        &namespace,
        &ulid_at(now - chrono::Duration::seconds(60), 308),
    );
    store
        .put(
            &first_pending_key,
            Bytes::from_static(b"first eligible pending delete"),
        )
        .await
        .unwrap();
    let (mut manifest, version) = Manifest::read_versioned(&store, &namespace)
        .await
        .unwrap()
        .unwrap();
    manifest.pending_deletes.push(first_pending_key.clone());
    manifest
        .write_conditional(&store, &namespace, &version)
        .await
        .unwrap();
    store
        .delete(&Manifest::history_key(&namespace, manifest.version()))
        .await
        .unwrap();

    let mutated = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(1),
        &counter,
        &control,
    )
    .await;
    assert_eq!(mutated.pending_deletes_deleted, 1);
    assert_eq!(mutated.pending_deletes_pruned, 1);
    assert_s3_object_not_exists(&store, &first_pending_key).await;

    let second_pending_key = WalFragment::s3_key(
        &namespace,
        &ulid_at(now - chrono::Duration::seconds(60), 309),
    );
    let orphan_key = WalFragment::s3_key(
        &namespace,
        &ulid_at(now - chrono::Duration::seconds(60), 310),
    );
    store
        .put(
            &second_pending_key,
            Bytes::from_static(b"second eligible pending delete"),
        )
        .await
        .unwrap();
    store
        .put(
            &orphan_key,
            Bytes::from_static(b"candidate must progress during pending churn"),
        )
        .await
        .unwrap();
    let (mut manifest, version) = Manifest::read_versioned(&store, &namespace)
        .await
        .unwrap()
        .unwrap();
    manifest.pending_deletes.push(second_pending_key.clone());
    manifest
        .write_conditional(&store, &namespace, &version)
        .await
        .unwrap();

    let candidate_phase = run_observed_gc_cycle(
        &mut runner,
        &incarnation,
        now + chrono::Duration::seconds(2),
        &counter,
        &control,
    )
    .await;

    assert_eq!(
        candidate_phase.pending_deletes_deleted, 0,
        "the fairness cycle must defer the newly eligible pending queue"
    );
    assert_eq!(
        candidate_phase.pending_deletes_retained, 1,
        "the fairness cycle report must expose the deferred queue"
    );
    assert_eq!(
        candidate_phase.objects_deleted, 1,
        "the fairness cycle must make destructive progress on the mature orphan"
    );
    assert_eq!(candidate_phase.candidates_marked, 1);
    assert_s3_object_not_exists(&store, &orphan_key).await;
    assert_s3_object_exists(&store, &second_pending_key).await;
    assert_eq!(
        Manifest::read(&store, &namespace)
            .await
            .unwrap()
            .unwrap()
            .pending_deletes,
        vec![second_pending_key],
        "deferred pending work must remain queued for the next cycle"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn gc_runner_idle_gate_reconciles_history_published_during_late_inventory_list() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-idle-late-history");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);
    seed_manifest_history(&store, &namespace, 2).await;

    let late_history_key = Manifest::history_key(&namespace, 3);
    let late_history = gc_manifest_at(
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
        !store
            .exists(&Manifest::history_key(&namespace, 2))
            .await
            .unwrap(),
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
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-idle-mid-prune-history");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);
    seed_manifest_history(&store, &namespace, 2).await;

    let pruned_history_key = Manifest::history_key(&namespace, 1);
    let injected_history_key = Manifest::history_key(&namespace, 3);
    let injected_history = gc_manifest_at(
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
        !store
            .exists(&Manifest::history_key(&namespace, 2))
            .await
            .unwrap(),
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
    let namespace =
        harness.artifact_origin_namespace("storage-gc-runner-idle-late-snapshot-recreate");
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
async fn gc_runner_idle_gate_reconciles_mature_pending_delete_published_after_cold_inventory() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-idle-late-pending");
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

    let mut manifest = gc_manifest_at(now);
    manifest.write(&store, &namespace).await.unwrap();
    let mut late_manifest = manifest;
    late_manifest.pending_deletes.push(pending_key.clone());
    late_manifest.updated_at = now;
    let (controlled_store, control) = HistoryMetadataControlStore::wrap(&store, history_prefix);
    control.put_on_nth_get(
        &format!("{namespace}/_gc/candidates.json"),
        &Manifest::s3_key(&namespace),
        manifest_json_bytes_with_version(&late_manifest, 2),
        1,
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
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-idle-unversioned");
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
    let now = DateTime::<Utc>::from_timestamp(1_900_000_000, 0)
        .expect("fixed idle-gate timestamp must be valid");

    let candidate_namespace =
        harness.artifact_origin_namespace("storage-gc-runner-idle-candidate-deadline");
    let mut candidate_manifest = gc_manifest_at(now);
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

    let pending_namespace =
        harness.artifact_origin_namespace("storage-gc-runner-idle-pending-deadline");
    let pending_key = WalFragment::s3_key(&pending_namespace, &ulid_at(now, 102));
    store
        .put(&pending_key, Bytes::from_static(b"pending deadline"))
        .await
        .unwrap();
    let mut pending_manifest = gc_manifest_at(now);
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

    let pitr_namespace = harness.artifact_origin_namespace("storage-gc-runner-idle-pitr-deadline");
    let mut pitr_manifest = gc_manifest_at(now);
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

    let lease_namespace =
        harness.artifact_origin_namespace("storage-gc-runner-idle-lease-deadline");
    let mut lease_manifest = gc_manifest_at(now);
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
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-idle-invalidations");
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
    control.set_strip_list_versions_for(&format!("{namespace}/"), true);
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
    control.set_strip_list_versions_for(&format!("{namespace}/"), false);
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
    let namespace = harness.artifact_origin_namespace("storage-gc-runner-pending-delete-failure");
    let store = harness.store.clone();
    let history_prefix = Manifest::history_prefix(&namespace);

    gc_manifest().write(&store, &namespace).await.unwrap();
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
