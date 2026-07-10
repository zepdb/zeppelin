//! Test-side cost-regression injections for `perf_selftest`.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result as OsResult,
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
    ZeppelinStore::new(Arc::new(InjectionStore {
        inner: store.inner(),
        injection,
        cluster_get: tokio::sync::Mutex::new(()),
    }))
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
