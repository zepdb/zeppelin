//! Fault-injecting `ObjectStore` wrappers for storage failure tests.

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result as OsResult,
};
use zeppelin::storage::ZeppelinStore;

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

impl fmt::Display for FailPutOnceStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailPutOnceStore({})", self.inner)
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
