//! Test-side cost-regression injections for `perf_selftest`.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOpts, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use zeppelin::storage::ZeppelinStore;

/// One mechanical regression injected outside the counting and depth layers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Injection {
    ExtraManifestGet,
    SerializeClusterGets,
    ExtraHistoryPut,
}

impl Injection {
    #[must_use]
    pub fn key(self) -> &'static str {
        match self {
            Self::ExtraManifestGet => "extra-manifest-get",
            Self::SerializeClusterGets => "serialize-cluster-gets",
            Self::ExtraHistoryPut => "extra-history-put",
        }
    }

    #[must_use]
    pub fn parse(key: &str) -> Self {
        match key {
            "extra-manifest-get" => Self::ExtraManifestGet,
            "serialize-cluster-gets" => Self::SerializeClusterGets,
            "extra-history-put" => Self::ExtraHistoryPut,
            _ => panic!("unknown ZEPPELIN_PERF_SELFTEST injection: {key}"),
        }
    }
}

#[derive(Debug)]
struct InjectionStore {
    inner: Arc<dyn ObjectStore>,
    injection: Injection,
    cluster_get: tokio::sync::Mutex<()>,
}

/// Place an injection outside DepthStore and CountingStore so every injected
/// operation is measured like a production operation.
#[must_use]
pub fn inject_store(store: &ZeppelinStore, injection: Injection) -> ZeppelinStore {
    ZeppelinStore::new_with_native_batch_delete(Arc::new(InjectionStore {
        inner: store.inner(),
        injection,
        cluster_get: tokio::sync::Mutex::new(()),
    }))
}

/// Control and proof handle for one same-owner lease rewrite before CAS retry.
#[derive(Clone, Debug)]
pub(crate) struct LeaseRetryConflictHandle {
    armed: Arc<AtomicBool>,
    conditional_puts: Arc<AtomicUsize>,
    injections: Arc<AtomicUsize>,
}

impl LeaseRetryConflictHandle {
    pub(crate) fn arm(&self) {
        self.conditional_puts.store(0, Ordering::SeqCst);
        self.injections.store(0, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
    }

    #[must_use]
    pub(crate) fn injections(&self) -> usize {
        self.injections.load(Ordering::SeqCst)
    }
}

/// Real-store decorator that advances a lease ETag immediately before the
/// second armed conditional PUT.
///
/// The first conditional PUT is expected to expose an already-stale memo. The
/// production code then performs its one classification GET. Before its retry,
/// this decorator writes the same lease body with extra insignificant JSON
/// whitespace through the real backend, guaranteeing a different ETag without
/// changing holder or fencing token. The retry therefore loses a genuine S3
/// CAS race.
#[derive(Debug)]
struct LeaseRetryConflictStore {
    inner: Arc<dyn ObjectStore>,
    armed: Arc<AtomicBool>,
    conditional_puts: Arc<AtomicUsize>,
    injections: Arc<AtomicUsize>,
}

pub(crate) fn inject_lease_retry_conflict(
    store: &ZeppelinStore,
) -> (ZeppelinStore, LeaseRetryConflictHandle) {
    let armed = Arc::new(AtomicBool::new(false));
    let conditional_puts = Arc::new(AtomicUsize::new(0));
    let injections = Arc::new(AtomicUsize::new(0));
    let wrapped = LeaseRetryConflictStore {
        inner: store.inner(),
        armed: Arc::clone(&armed),
        conditional_puts: Arc::clone(&conditional_puts),
        injections: Arc::clone(&injections),
    };
    (
        ZeppelinStore::new_with_native_batch_delete(Arc::new(wrapped)),
        LeaseRetryConflictHandle {
            armed,
            conditional_puts,
            injections,
        },
    )
}

impl fmt::Display for LeaseRetryConflictStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LeaseRetryConflictStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for LeaseRetryConflictStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let is_lease_update =
            location.as_ref().ends_with("lease.json") && matches!(opts.mode, PutMode::Update(_));
        if self.armed.load(Ordering::SeqCst) && is_lease_update {
            let ordinal = self.conditional_puts.fetch_add(1, Ordering::SeqCst) + 1;
            if ordinal == 2 {
                self.armed.store(false, Ordering::SeqCst);
                let mut body = bytes::BytesMut::with_capacity(payload.content_length() + 1);
                for chunk in payload.as_ref() {
                    body.extend_from_slice(chunk);
                }
                body.extend_from_slice(b"\n");
                let mut overwrite = opts.clone();
                overwrite.mode = PutMode::Overwrite;
                self.inner
                    .put_opts(location, PutPayload::from(body.freeze()), overwrite)
                    .await?;
                self.injections.fetch_add(1, Ordering::SeqCst);
            }
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

/// Proof handle for a backend response that omits conditional-PUT ETags.
#[derive(Clone, Debug)]
pub(crate) struct MissingLeasePutEtagHandle {
    stripped: Arc<AtomicUsize>,
}

impl MissingLeasePutEtagHandle {
    #[must_use]
    pub(crate) fn stripped(&self) -> usize {
        self.stripped.load(Ordering::SeqCst)
    }
}

/// Real-store decorator that preserves a successful lease CAS but removes its
/// response ETag, exercising object-store implementations that omit it.
#[derive(Debug)]
struct MissingLeasePutEtagStore {
    inner: Arc<dyn ObjectStore>,
    stripped: Arc<AtomicUsize>,
}

pub(crate) fn inject_missing_lease_put_etag(
    store: &ZeppelinStore,
) -> (ZeppelinStore, MissingLeasePutEtagHandle) {
    let stripped = Arc::new(AtomicUsize::new(0));
    let wrapped = MissingLeasePutEtagStore {
        inner: store.inner(),
        stripped: Arc::clone(&stripped),
    };
    (
        ZeppelinStore::new_with_native_batch_delete(Arc::new(wrapped)),
        MissingLeasePutEtagHandle { stripped },
    )
}

/// Proof handle for a backend response that omits manifest-CAS ETags.
#[derive(Clone, Debug)]
pub(crate) struct MissingManifestPutEtagHandle {
    stripped: Arc<AtomicUsize>,
}

impl MissingManifestPutEtagHandle {
    #[must_use]
    pub(crate) fn stripped(&self) -> usize {
        self.stripped.load(Ordering::SeqCst)
    }
}

/// Real-store decorator that preserves successful live-manifest CAS writes but
/// removes their response ETags. The writer must commit successfully while
/// declining to populate its process-local memo.
#[derive(Debug)]
struct MissingManifestPutEtagStore {
    inner: Arc<dyn ObjectStore>,
    stripped: Arc<AtomicUsize>,
}

pub(crate) fn inject_missing_manifest_put_etag(
    store: &ZeppelinStore,
) -> (ZeppelinStore, MissingManifestPutEtagHandle) {
    let stripped = Arc::new(AtomicUsize::new(0));
    let wrapped = MissingManifestPutEtagStore {
        inner: store.inner(),
        stripped: Arc::clone(&stripped),
    };
    (
        ZeppelinStore::new_with_native_batch_delete(Arc::new(wrapped)),
        MissingManifestPutEtagHandle { stripped },
    )
}

impl fmt::Display for MissingManifestPutEtagStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MissingManifestPutEtagStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for MissingManifestPutEtagStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let omit_etag =
            location.as_ref().ends_with("manifest.json") && matches!(opts.mode, PutMode::Update(_));
        let mut result = self.inner.put_opts(location, payload, opts).await?;
        if omit_etag {
            result.e_tag = None;
            self.stripped.fetch_add(1, Ordering::SeqCst);
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

impl fmt::Display for MissingLeasePutEtagStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MissingLeasePutEtagStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for MissingLeasePutEtagStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let omit_etag =
            location.as_ref().ends_with("lease.json") && matches!(opts.mode, PutMode::Update(_));
        let mut result = self.inner.put_opts(location, payload, opts).await?;
        if omit_etag {
            result.e_tag = None;
            self.stripped.fetch_add(1, Ordering::SeqCst);
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

impl fmt::Display for InjectionStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "InjectionStore({}, {})",
            self.injection.key(),
            self.inner
        )
    }
}

#[async_trait]
impl ObjectStore for InjectionStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        if self.injection == Injection::ExtraHistoryPut && location.as_ref().contains("manifests/")
        {
            // Put the same create-if-absent request first. The production call
            // then observes AlreadyExists and verifies identical bytes through
            // its normal immutable-history collision path.
            self.inner
                .put_opts(location, payload.clone(), opts.clone())
                .await?;
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
        if self.injection == Injection::ExtraManifestGet
            && location.as_ref().ends_with("manifest.json")
        {
            self.inner
                .get_opts(location, GetOptions::default())
                .await?
                .bytes()
                .await?;
        }
        if self.injection == Injection::SerializeClusterGets
            && location
                .as_ref()
                .rsplit('/')
                .next()
                .is_some_and(|name| name.starts_with("cluster_"))
        {
            let _guard = self.cluster_get.lock().await;
            let result = self.inner.get_opts(location, options).await?;
            let meta = result.meta.clone();
            let range = result.range.clone();
            let attributes = result.attributes.clone();
            let bytes = result.bytes().await?;
            return Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(stream::once(async move { Ok(bytes) }))),
                meta,
                range,
                attributes,
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
