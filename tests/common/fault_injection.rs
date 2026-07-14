//! Fault-injecting `ObjectStore` wrappers for storage failure tests.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use zeppelin::storage::ZeppelinStore;
use zeppelin::wal::manifest::NamedSnapshot;

/// Controller for a toggleable matching-GET fault.
#[derive(Clone, Debug)]
pub struct GetFailureHandle {
    enabled: Arc<AtomicBool>,
    failures_injected: Arc<AtomicUsize>,
}

impl GetFailureHandle {
    /// Begin failing every matching GET.
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }

    /// Stop injecting failures so recovery can be verified.
    pub fn disable(&self) {
        self.enabled.store(false, Ordering::SeqCst);
    }

    /// Return the exact number of failures injected so far.
    #[must_use]
    pub fn failures_injected(&self) -> usize {
        self.failures_injected.load(Ordering::SeqCst)
    }
}

/// Object-store decorator that can fail all matching GET requests.
#[derive(Debug)]
pub struct ToggleGetFailureStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    enabled: Arc<AtomicBool>,
    failures_injected: Arc<AtomicUsize>,
}

/// Wrap a store in a disabled matching-GET fault that a test can toggle.
pub fn toggle_get_failure_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, GetFailureHandle) {
    let enabled = Arc::new(AtomicBool::new(false));
    let failures_injected = Arc::new(AtomicUsize::new(0));
    let wrapper = ToggleGetFailureStore {
        inner: store.inner(),
        needle: needle.into(),
        enabled: Arc::clone(&enabled),
        failures_injected: Arc::clone(&failures_injected),
    };
    (
        ZeppelinStore::new(Arc::new(wrapper)),
        GetFailureHandle {
            enabled,
            failures_injected,
        },
    )
}

/// Controller for a toggleable matching CAS-precondition fault.
#[derive(Clone, Debug)]
pub struct CasPreconditionFailureHandle {
    enabled: Arc<AtomicBool>,
    failures_injected: Arc<AtomicUsize>,
}

impl CasPreconditionFailureHandle {
    /// Begin rejecting every matching ETag-update PUT as a CAS conflict.
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }

    /// Stop rejecting matching CAS writes so recovery can be verified.
    pub fn disable(&self) {
        self.enabled.store(false, Ordering::SeqCst);
    }

    /// Return the exact number of precondition failures injected so far.
    #[must_use]
    pub fn failures_injected(&self) -> usize {
        self.failures_injected.load(Ordering::SeqCst)
    }
}

/// Object-store decorator that can reject matching ETag-update PUTs.
#[derive(Debug)]
pub struct ToggleCasPreconditionFailureStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    enabled: Arc<AtomicBool>,
    failures_injected: Arc<AtomicUsize>,
    enable_get_on_failure: Option<Arc<AtomicBool>>,
}

/// Controller for synchronizing the first two enabled matching CAS writes.
#[derive(Clone, Debug)]
pub struct CasPairBarrierHandle {
    enabled: Arc<AtomicBool>,
    arrivals: Arc<AtomicUsize>,
    conflicts: Arc<AtomicUsize>,
}

impl CasPairBarrierHandle {
    /// Arm the two-writer barrier after single-writer setup mutations finish.
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }

    /// Return how many matching CAS calls reached the armed wrapper.
    #[must_use]
    pub fn arrivals(&self) -> usize {
        self.arrivals.load(Ordering::SeqCst)
    }

    /// Return how many inner CAS calls lost with an ETag precondition error.
    #[must_use]
    pub fn conflicts(&self) -> usize {
        self.conflicts.load(Ordering::SeqCst)
    }
}

/// Object-store decorator that makes two CAS writers race from the same base.
#[derive(Debug)]
pub struct CasPairBarrierStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    enabled: Arc<AtomicBool>,
    arrivals: Arc<AtomicUsize>,
    conflicts: Arc<AtomicUsize>,
    barrier: Arc<tokio::sync::Barrier>,
}

/// Controller for inspecting a deterministic two-writer create-only race.
#[derive(Clone, Debug)]
pub struct CreatePairBarrierHandle {
    arrivals: Arc<AtomicUsize>,
    conflicts: Arc<AtomicUsize>,
}

impl CreatePairBarrierHandle {
    /// Return how many matching create-only calls reached the wrapper.
    #[must_use]
    pub fn arrivals(&self) -> usize {
        self.arrivals.load(Ordering::SeqCst)
    }

    /// Return how many matching create-only calls lost to an existing object.
    #[must_use]
    pub fn conflicts(&self) -> usize {
        self.conflicts.load(Ordering::SeqCst)
    }
}

/// Object-store decorator that makes two create-only writers race on one key.
#[derive(Debug)]
pub struct CreatePairBarrierStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    arrivals: Arc<AtomicUsize>,
    conflicts: Arc<AtomicUsize>,
    barrier: Arc<tokio::sync::Barrier>,
}

/// Wrap a store with a disabled deterministic two-CAS synchronization point.
pub fn synchronize_cas_pair_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, CasPairBarrierHandle) {
    let enabled = Arc::new(AtomicBool::new(false));
    let arrivals = Arc::new(AtomicUsize::new(0));
    let conflicts = Arc::new(AtomicUsize::new(0));
    let wrapper = CasPairBarrierStore {
        inner: store.inner(),
        needle: needle.into(),
        enabled: Arc::clone(&enabled),
        arrivals: Arc::clone(&arrivals),
        conflicts: Arc::clone(&conflicts),
        barrier: Arc::new(tokio::sync::Barrier::new(2)),
    };
    (
        ZeppelinStore::new(Arc::new(wrapper)),
        CasPairBarrierHandle {
            enabled,
            arrivals,
            conflicts,
        },
    )
}

/// Wrap a store with a deterministic two-writer create-only synchronization point.
pub fn synchronize_create_pair_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, CreatePairBarrierHandle) {
    let arrivals = Arc::new(AtomicUsize::new(0));
    let conflicts = Arc::new(AtomicUsize::new(0));
    let wrapper = CreatePairBarrierStore {
        inner: store.inner(),
        needle: needle.into(),
        arrivals: Arc::clone(&arrivals),
        conflicts: Arc::clone(&conflicts),
        barrier: Arc::new(tokio::sync::Barrier::new(2)),
    };
    (
        ZeppelinStore::new(Arc::new(wrapper)),
        CreatePairBarrierHandle {
            arrivals,
            conflicts,
        },
    )
}

/// Wrap a store in a disabled matching CAS-precondition fault that a test can toggle.
pub fn toggle_cas_precondition_failure_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, CasPreconditionFailureHandle) {
    let enabled = Arc::new(AtomicBool::new(false));
    let failures_injected = Arc::new(AtomicUsize::new(0));
    let wrapper = ToggleCasPreconditionFailureStore {
        inner: store.inner(),
        needle: needle.into(),
        enabled: Arc::clone(&enabled),
        failures_injected: Arc::clone(&failures_injected),
        enable_get_on_failure: None,
    };
    (
        ZeppelinStore::new(Arc::new(wrapper)),
        CasPreconditionFailureHandle {
            enabled,
            failures_injected,
        },
    )
}

/// Fail the first policy CAS attempt, then fail the following matching reload GET.
pub fn fail_get_after_cas_conflict_matching(
    store: &ZeppelinStore,
    cas_needle: impl Into<String>,
    get_needle: impl Into<String>,
) -> (
    ZeppelinStore,
    CasPreconditionFailureHandle,
    GetFailureHandle,
) {
    let get_enabled = Arc::new(AtomicBool::new(false));
    let get_failures = Arc::new(AtomicUsize::new(0));
    let get_store = ToggleGetFailureStore {
        inner: store.inner(),
        needle: get_needle.into(),
        enabled: Arc::clone(&get_enabled),
        failures_injected: Arc::clone(&get_failures),
    };
    let cas_enabled = Arc::new(AtomicBool::new(true));
    let cas_failures = Arc::new(AtomicUsize::new(0));
    let cas_store = ToggleCasPreconditionFailureStore {
        inner: Arc::new(get_store),
        needle: cas_needle.into(),
        enabled: Arc::clone(&cas_enabled),
        failures_injected: Arc::clone(&cas_failures),
        enable_get_on_failure: Some(Arc::clone(&get_enabled)),
    };
    (
        ZeppelinStore::new(Arc::new(cas_store)),
        CasPreconditionFailureHandle {
            enabled: cas_enabled,
            failures_injected: cas_failures,
        },
        GetFailureHandle {
            enabled: get_enabled,
            failures_injected: get_failures,
        },
    )
}

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

/// `ObjectStore` decorator that delays every DELETE whose key contains `needle`.
#[derive(Debug)]
pub struct DelayDeleteStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    delay: Duration,
}

impl DelayDeleteStore {
    /// Wrap an existing store, delaying matching DELETEs before they begin.
    pub fn wrap(inner: Arc<dyn ObjectStore>, needle: impl Into<String>, delay: Duration) -> Self {
        Self {
            inner,
            needle: needle.into(),
            delay,
        }
    }
}

/// Wrap a `ZeppelinStore` in a matching-DELETE delay layer.
pub fn delay_delete_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
    delay: Duration,
) -> ZeppelinStore {
    ZeppelinStore::new(Arc::new(DelayDeleteStore::wrap(
        store.inner(),
        needle,
        delay,
    )))
}

/// `ObjectStore` decorator that delays every GET whose key contains `needle`.
#[derive(Debug)]
pub struct DelayGetStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    delay: Duration,
}

impl DelayGetStore {
    /// Wrap an existing store, delaying matching GETs before they begin.
    pub fn wrap(inner: Arc<dyn ObjectStore>, needle: impl Into<String>, delay: Duration) -> Self {
        Self {
            inner,
            needle: needle.into(),
            delay,
        }
    }
}

/// Wrap a `ZeppelinStore` in a matching-GET delay layer.
pub fn delay_get_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
    delay: Duration,
) -> ZeppelinStore {
    ZeppelinStore::new(Arc::new(DelayGetStore::wrap(store.inner(), needle, delay)))
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

/// `ObjectStore` decorator that acknowledges one matching PUT after writing
/// its payload to a sibling key instead of the requested destination.
#[derive(Debug)]
pub struct MisdirectPutOnceStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    remaining: AtomicUsize,
    failures_injected: Arc<AtomicUsize>,
}

impl MisdirectPutOnceStore {
    /// Wrap an existing store, misdirecting the first matching PUT.
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

    fn should_misdirect(&self, location: &Path) -> bool {
        location.as_ref().contains(&self.needle)
            && self
                .remaining
                .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }
}

/// Wrap a `ZeppelinStore` in a one-shot misdirected PUT layer.
pub fn misdirect_put_once_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, PutFailureHandle) {
    let (misdirecting, handle) = MisdirectPutOnceStore::wrap(store.inner(), needle);
    (ZeppelinStore::new(Arc::new(misdirecting)), handle)
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

impl fmt::Display for ToggleGetFailureStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ToggleGetFailureStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ToggleGetFailureStore {
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
        if self.enabled.load(Ordering::SeqCst) && location.as_ref().contains(&self.needle) {
            self.failures_injected.fetch_add(1, Ordering::SeqCst);
            return Err(object_store::Error::Generic {
                store: "toggle_get_failure",
                source: Box::new(std::io::Error::other(format!(
                    "injected get failure for {location}"
                ))),
            });
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

impl fmt::Display for ToggleCasPreconditionFailureStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ToggleCasPreconditionFailureStore({})", self.inner)
    }
}

impl fmt::Display for CasPairBarrierStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CasPairBarrierStore({})", self.inner)
    }
}

impl fmt::Display for CreatePairBarrierStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CreatePairBarrierStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CasPairBarrierStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let synchronize = self.enabled.load(Ordering::SeqCst)
            && location.as_ref().contains(&self.needle)
            && matches!(&opts.mode, PutMode::Update(_));
        if synchronize {
            let arrival = self.arrivals.fetch_add(1, Ordering::SeqCst);
            if arrival < 2 {
                self.barrier.wait().await;
            }
        }
        let result = self.inner.put_opts(location, payload, opts).await;
        if synchronize && matches!(&result, Err(object_store::Error::Precondition { .. })) {
            self.conflicts.fetch_add(1, Ordering::SeqCst);
        }
        result
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
impl ObjectStore for CreatePairBarrierStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let synchronize =
            location.as_ref().contains(&self.needle) && matches!(&opts.mode, PutMode::Create);
        if synchronize {
            let arrival = self.arrivals.fetch_add(1, Ordering::SeqCst);
            if arrival < 2 {
                self.barrier.wait().await;
            }
        }
        let result = self.inner.put_opts(location, payload, opts).await;
        if synchronize && matches!(&result, Err(object_store::Error::AlreadyExists { .. })) {
            self.conflicts.fetch_add(1, Ordering::SeqCst);
        }
        result
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
impl ObjectStore for ToggleCasPreconditionFailureStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        if self.enabled.load(Ordering::SeqCst)
            && location.as_ref().contains(&self.needle)
            && matches!(&opts.mode, PutMode::Update(_))
        {
            self.failures_injected.fetch_add(1, Ordering::SeqCst);
            if let Some(enabled) = &self.enable_get_on_failure {
                enabled.store(true, Ordering::SeqCst);
            }
            return Err(object_store::Error::Precondition {
                path: location.to_string(),
                source: Box::new(std::io::Error::other(format!(
                    "injected CAS precondition failure for {location}"
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

impl fmt::Display for DelayDeleteStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DelayDeleteStore({})", self.inner)
    }
}

impl fmt::Display for DelayGetStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DelayGetStore({})", self.inner)
    }
}

impl fmt::Display for FailAfterPutOnceStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailAfterPutOnceStore({})", self.inner)
    }
}

impl fmt::Display for MisdirectPutOnceStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MisdirectPutOnceStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for MisdirectPutOnceStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        if self.should_misdirect(location) {
            self.failures_injected.fetch_add(1, Ordering::Relaxed);
            let redirected = Path::from(format!("{location}.misdirected"));
            let mut redirected_opts = opts;
            redirected_opts.mode = PutMode::Overwrite;
            return self
                .inner
                .put_opts(&redirected, payload, redirected_opts)
                .await;
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

#[async_trait]
impl ObjectStore for DelayDeleteStore {
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
        if location.as_ref().contains(&self.needle) {
            tokio::time::sleep(self.delay).await;
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

#[async_trait]
impl ObjectStore for DelayGetStore {
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
        if location.as_ref().contains(&self.needle) {
            tokio::time::sleep(self.delay).await;
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use object_store::memory::InMemory;
    use zeppelin::storage::{ConditionalPutOutcome, ZeppelinStore};

    use super::toggle_cas_precondition_failure_matching;

    #[tokio::test]
    async fn matching_cas_precondition_failure_can_be_enabled_and_recovered() {
        let base = ZeppelinStore::new(Arc::new(InMemory::new()));
        let key = "_security/heads/policy.json";
        base.put(key, Bytes::from_static(b"v1"))
            .await
            .expect("seed CAS object");
        let (_, initial_etag) = base
            .get_with_meta(key)
            .await
            .expect("read initial CAS version");
        let initial_etag = initial_etag.expect("in-memory store must return an ETag");
        let (faulted, handle) = toggle_cas_precondition_failure_matching(&base, "_security/heads/");

        assert!(matches!(
            faulted
                .put_if_match_outcome(key, Bytes::from_static(b"v2"), &initial_etag)
                .await
                .expect("disabled fault must forward CAS"),
            ConditionalPutOutcome::Updated { .. }
        ));

        let (_, current_etag) = faulted
            .get_with_meta(key)
            .await
            .expect("read forwarded CAS version");
        let current_etag = current_etag.expect("in-memory store must return an ETag");
        handle.enable();
        assert_eq!(
            faulted
                .put_if_match_outcome(key, Bytes::from_static(b"v3"), &current_etag)
                .await
                .expect("injected precondition must be a typed CAS conflict"),
            ConditionalPutOutcome::Conflict
        );
        assert_eq!(handle.failures_injected(), 1);
        assert_eq!(
            faulted.get(key).await.expect("read after conflict"),
            b"v2"[..]
        );

        handle.disable();
        assert!(matches!(
            faulted
                .put_if_match_outcome(key, Bytes::from_static(b"v3"), &current_etag)
                .await
                .expect("disabled fault must recover"),
            ConditionalPutOutcome::Updated { .. }
        ));
        assert_eq!(handle.failures_injected(), 1);
        assert_eq!(
            faulted.get(key).await.expect("read recovered body"),
            b"v3"[..]
        );
    }
}
