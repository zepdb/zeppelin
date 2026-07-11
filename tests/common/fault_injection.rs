//! Fault-injecting `ObjectStore` wrappers for storage failure tests.

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result as OsResult,
};
use zeppelin::storage::ZeppelinStore;
use zeppelin::wal::manifest::NamedSnapshot;

/// Shared handle for inspecting an injected fail-once PUT rule.
#[derive(Clone, Debug)]
pub struct PutFailureHandle {
    failures_injected: Arc<AtomicUsize>,
}

impl PutFailureHandle {
    /// Number of PUT failures injected by the wrapped store.
    #[must_use]
    pub fn failures_injected(&self) -> usize {
        self.failures_injected.load(Ordering::Relaxed)
    }
}

/// `ObjectStore` decorator that fails the first PUT whose key contains `needle`.
#[derive(Debug)]
pub struct FailPutOnceStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    remaining: AtomicUsize,
    failures_injected: Arc<AtomicUsize>,
}

impl FailPutOnceStore {
    /// Wrap an existing store, failing the first matching PUT.
    pub fn wrap(
        inner: Arc<dyn ObjectStore>,
        needle: impl Into<String>,
    ) -> (Self, PutFailureHandle) {
        let failures_injected = Arc::new(AtomicUsize::new(0));
        let handle = PutFailureHandle {
            failures_injected: Arc::clone(&failures_injected),
        };
        (
            Self {
                inner,
                needle: needle.into(),
                remaining: AtomicUsize::new(1),
                failures_injected,
            },
            handle,
        )
    }

    fn should_fail(&self, location: &Path) -> bool {
        location.as_ref().contains(&self.needle)
            && self
                .remaining
                .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }
}

/// Wrap a `ZeppelinStore` in a fail-once PUT layer.
pub fn fail_put_once_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, PutFailureHandle) {
    let (failing, handle) = FailPutOnceStore::wrap(store.inner(), needle);
    (ZeppelinStore::new(Arc::new(failing)), handle)
}

/// `ObjectStore` decorator that reports one matching PUT as failed only after
/// the wrapped store has committed it.
#[derive(Debug)]
pub struct FailAfterPutOnceStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    remaining: AtomicUsize,
    failures_injected: Arc<AtomicUsize>,
}

impl FailAfterPutOnceStore {
    /// Wrap an existing store, losing the first successful matching PUT reply.
    pub fn wrap(
        inner: Arc<dyn ObjectStore>,
        needle: impl Into<String>,
    ) -> (Self, PutFailureHandle) {
        let failures_injected = Arc::new(AtomicUsize::new(0));
        let handle = PutFailureHandle {
            failures_injected: Arc::clone(&failures_injected),
        };
        (
            Self {
                inner,
                needle: needle.into(),
                remaining: AtomicUsize::new(1),
                failures_injected,
            },
            handle,
        )
    }

    fn should_fail(&self, location: &Path) -> bool {
        location.as_ref().contains(&self.needle)
            && self
                .remaining
                .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }
}

/// Wrap a `ZeppelinStore` in a layer that loses one successful PUT reply.
pub fn fail_after_put_once_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, PutFailureHandle) {
    let (failing, handle) = FailAfterPutOnceStore::wrap(store.inner(), needle);
    (ZeppelinStore::new(Arc::new(failing)), handle)
}

/// Shared handle for inspecting an injected fail-once COPY rule.
#[derive(Clone, Debug)]
pub struct CopyFailureHandle {
    failures_injected: Arc<AtomicUsize>,
}

impl CopyFailureHandle {
    /// Number of COPY failures injected by the wrapped store.
    #[must_use]
    pub fn failures_injected(&self) -> usize {
        self.failures_injected.load(Ordering::Relaxed)
    }
}

/// `ObjectStore` decorator that fails the first copy whose source or
/// destination key contains `needle`.
#[derive(Debug)]
pub struct FailCopyOnceStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    remaining: AtomicUsize,
    failures_injected: Arc<AtomicUsize>,
}

impl FailCopyOnceStore {
    /// Wrap an existing store, failing the first matching copy.
    pub fn wrap(
        inner: Arc<dyn ObjectStore>,
        needle: impl Into<String>,
    ) -> (Self, CopyFailureHandle) {
        let failures_injected = Arc::new(AtomicUsize::new(0));
        let handle = CopyFailureHandle {
            failures_injected: Arc::clone(&failures_injected),
        };
        (
            Self {
                inner,
                needle: needle.into(),
                remaining: AtomicUsize::new(1),
                failures_injected,
            },
            handle,
        )
    }

    fn should_fail(&self, from: &Path, to: &Path) -> bool {
        (from.as_ref().contains(&self.needle) || to.as_ref().contains(&self.needle))
            && self
                .remaining
                .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }
}

/// Wrap a `ZeppelinStore` in a fail-once COPY layer.
pub fn fail_copy_once_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, CopyFailureHandle) {
    let (failing, handle) = FailCopyOnceStore::wrap(store.inner(), needle);
    (ZeppelinStore::new(Arc::new(failing)), handle)
}

impl fmt::Display for FailCopyOnceStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailCopyOnceStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FailCopyOnceStore {
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
        if self.should_fail(from, to) {
            self.failures_injected.fetch_add(1, Ordering::Relaxed);
            return Err(object_store::Error::Generic {
                store: "fail_copy_once",
                source: Box::new(std::io::Error::other(format!(
                    "injected copy failure from {from} to {to}"
                ))),
            });
        }
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[derive(Clone, Debug)]
struct ExpectedSnapshot {
    namespace: String,
    generation: u64,
    name_prefix: String,
}

/// Shared handle for asserting a snapshot pin exists during clone copy.
#[derive(Clone, Debug)]
pub struct SnapshotOnCopyHandle {
    expected: Arc<Mutex<Option<ExpectedSnapshot>>>,
    observations: Arc<AtomicUsize>,
}

impl SnapshotOnCopyHandle {
    /// Expect copy operations to observe a snapshot pin for `generation`.
    pub fn expect_snapshot(
        &self,
        namespace: impl Into<String>,
        generation: u64,
        name_prefix: impl Into<String>,
    ) {
        *self.expected.lock().expect("snapshot expectation poisoned") = Some(ExpectedSnapshot {
            namespace: namespace.into(),
            generation,
            name_prefix: name_prefix.into(),
        });
    }

    /// Number of copy operations that observed the expected snapshot pin.
    #[must_use]
    pub fn observations(&self) -> usize {
        self.observations.load(Ordering::Relaxed)
    }
}

/// `ObjectStore` decorator that fails a copy if the expected snapshot pin is
/// not present while the copy starts.
#[derive(Debug)]
pub struct AssertSnapshotOnCopyStore {
    inner: Arc<dyn ObjectStore>,
    expected: Arc<Mutex<Option<ExpectedSnapshot>>>,
    observations: Arc<AtomicUsize>,
}

impl AssertSnapshotOnCopyStore {
    /// Wrap an existing store and return the assertion handle.
    pub fn wrap(inner: Arc<dyn ObjectStore>) -> (Self, SnapshotOnCopyHandle) {
        let expected = Arc::new(Mutex::new(None));
        let observations = Arc::new(AtomicUsize::new(0));
        let handle = SnapshotOnCopyHandle {
            expected: Arc::clone(&expected),
            observations: Arc::clone(&observations),
        };
        (
            Self {
                inner,
                expected,
                observations,
            },
            handle,
        )
    }
}

/// Wrap a `ZeppelinStore` in a snapshot-asserting COPY layer.
pub fn assert_snapshot_on_copy(store: &ZeppelinStore) -> (ZeppelinStore, SnapshotOnCopyHandle) {
    let (asserting, handle) = AssertSnapshotOnCopyStore::wrap(store.inner());
    (ZeppelinStore::new(Arc::new(asserting)), handle)
}

impl fmt::Display for AssertSnapshotOnCopyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AssertSnapshotOnCopyStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for AssertSnapshotOnCopyStore {
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
        let expected = self
            .expected
            .lock()
            .expect("snapshot expectation poisoned")
            .clone();
        if let Some(expected) = expected {
            let store = ZeppelinStore::new(Arc::clone(&self.inner));
            let snapshots = NamedSnapshot::list(&store, &expected.namespace)
                .await
                .map_err(|error| object_store::Error::Generic {
                    store: "assert_snapshot_on_copy",
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;
            let found = snapshots.iter().any(|snapshot| {
                snapshot.generation == expected.generation
                    && snapshot.name.starts_with(&expected.name_prefix)
            });
            if !found {
                return Err(object_store::Error::Generic {
                    store: "assert_snapshot_on_copy",
                    source: Box::new(std::io::Error::other(format!(
                        "missing snapshot pin for {} generation {} during copy from {from} to {to}",
                        expected.namespace, expected.generation
                    ))),
                });
            }
            self.observations.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.copy_if_not_exists(from, to).await
    }
}

impl fmt::Display for FailPutOnceStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailPutOnceStore({})", self.inner)
    }
}

impl fmt::Display for FailAfterPutOnceStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailAfterPutOnceStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FailAfterPutOnceStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let result = self.inner.put_opts(location, payload, opts).await?;
        if self.should_fail(location) {
            self.failures_injected.fetch_add(1, Ordering::Relaxed);
            return Err(object_store::Error::Generic {
                store: "fail_after_put_once",
                source: Box::new(std::io::Error::other(format!(
                    "injected lost acknowledgement after put for {location}"
                ))),
            });
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

#[async_trait]
impl ObjectStore for FailPutOnceStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        if self.should_fail(location) {
            self.failures_injected.fetch_add(1, Ordering::Relaxed);
            return Err(object_store::Error::Generic {
                store: "fail_put_once",
                source: Box::new(std::io::Error::other(format!(
                    "injected put failure for {location}"
                ))),
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
