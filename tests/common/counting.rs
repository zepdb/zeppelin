//! GET-counting wrapper around any `ObjectStore`, for asserting how many
//! S3 GETs a code path performs per key pattern.
//!
//! Wraps the harness's real backend (S3/MinIO/memory), so tests still hit
//! real object storage — the wrapper only observes.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result as OsResult,
};
use zeppelin::storage::ZeppelinStore;

/// Shared handle for inspecting GET counts recorded by a [`CountingStore`].
#[derive(Clone, Debug, Default)]
pub struct GetCounter {
    gets: Arc<DashMap<String, u64>>,
}

impl GetCounter {
    /// Total number of GETs whose key contains `substr`.
    pub fn gets_matching(&self, substr: &str) -> u64 {
        self.gets
            .iter()
            .filter(|r| r.key().contains(substr))
            .map(|r| *r.value())
            .sum()
    }

    /// Reset all recorded counts.
    pub fn reset(&self) {
        self.gets.clear();
    }
}

/// `ObjectStore` decorator that counts GET operations per key.
#[derive(Debug)]
pub struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    counter: GetCounter,
}

impl CountingStore {
    /// Wrap an existing store, returning the wrapper and its counter handle.
    pub fn wrap(inner: Arc<dyn ObjectStore>) -> (Self, GetCounter) {
        let counter = GetCounter::default();
        (
            Self {
                inner,
                counter: counter.clone(),
            },
            counter,
        )
    }
}

/// Wrap a harness store in a counting layer, returning a new `ZeppelinStore`
/// backed by the same objects plus the counter handle.
pub fn counting_store(store: &ZeppelinStore) -> (ZeppelinStore, GetCounter) {
    let (counting, counter) = CountingStore::wrap(store.inner());
    (ZeppelinStore::new(Arc::new(counting)), counter)
}

impl fmt::Display for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
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

    // All GET variants funnel through get_opts (the trait's `get`,
    // `get_range`, and `get_ranges` default impls delegate here).
    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        self.counter
            .gets
            .entry(location.to_string())
            .and_modify(|v| *v += 1)
            .or_insert(1);
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
