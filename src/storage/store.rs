use bytes::Bytes;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path;
use object_store::{ClientOptions, ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion};
use std::sync::Arc;
use tracing::{debug, instrument};

use crate::config::StorageConfig;
use crate::error::{Result, ZeppelinError};

/// Wrapper around the `object_store` crate providing a unified interface
/// for S3, GCS, Azure, and local storage backends.
#[derive(Clone)]
pub struct ZeppelinStore {
    inner: Arc<dyn ObjectStore>,
}

impl ZeppelinStore {
    /// Create a new store from configuration.
    pub fn from_config(config: &StorageConfig) -> Result<Self> {
        let store: Arc<dyn ObjectStore> =
            match config.backend {
                crate::config::StorageBackend::S3 => {
                    let mut builder = AmazonS3Builder::new().with_bucket_name(&config.bucket);

                    if let Some(ref region) = config.s3_region {
                        builder = builder.with_region(region);
                    }
                    if let Some(ref endpoint) = config.s3_endpoint {
                        if !endpoint.is_empty() {
                            builder = builder.with_endpoint(endpoint);
                        }
                    }
                    if let Some(ref key_id) = config.s3_access_key_id {
                        builder = builder.with_access_key_id(key_id);
                    }
                    if let Some(ref secret) = config.s3_secret_access_key {
                        builder = builder.with_secret_access_key(secret);
                    }
                    if config.s3_allow_http {
                        builder = builder.with_allow_http(true);
                    }

                    // Enable conditional PUT (ETag-based CAS) — required for
                    // manifest conflict detection and lease CAS operations.
                    builder = builder.with_conditional_put(S3ConditionalPut::ETagMatch);

                    // Connection pool tuning: increase idle connections and timeouts
                    // to prevent 28% sustained throughput degradation observed in Run-007.
                    let client_options = ClientOptions::new()
                        .with_pool_max_idle_per_host(64)
                        .with_timeout(std::time::Duration::from_secs(30))
                        .with_connect_timeout(std::time::Duration::from_secs(10))
                        .with_pool_idle_timeout(std::time::Duration::from_secs(90));
                    builder = builder.with_client_options(client_options);

                    Arc::new(builder.build().map_err(|e| {
                        ZeppelinError::Config(format!("failed to build S3 store: {e}"))
                    })?)
                }
                crate::config::StorageBackend::Local => {
                    let path = std::path::Path::new(&config.bucket);
                    if !path.exists() {
                        std::fs::create_dir_all(path)?;
                    }
                    Arc::new(
                        object_store::local::LocalFileSystem::new_with_prefix(path).map_err(
                            |e| ZeppelinError::Config(format!("failed to build local store: {e}")),
                        )?,
                    )
                }
                backend => {
                    return Err(ZeppelinError::Config(format!(
                        "unsupported storage backend: {backend} (gcs/azure not yet implemented)"
                    )));
                }
            };

        Ok(Self { inner: store })
    }

    /// Create a store directly from an ObjectStore instance (for testing).
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { inner: store }
    }

    /// Put an object at the given key.
    #[instrument(skip(self, data), fields(key = key, size = data.len()))]
    pub async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        self.inner.put(&path, PutPayload::from(data)).await?;
        let elapsed = start.elapsed();
        debug!(elapsed_ms = elapsed.as_millis(), "s3 put");
        crate::metrics::S3_OPERATION_DURATION
            .with_label_values(&["put"])
            .observe(elapsed.as_secs_f64());
        Ok(())
    }

    /// Get an object by key. Returns NotFound if it doesn't exist.
    #[instrument(skip(self), fields(key = key))]
    pub async fn get(&self, key: &str) -> Result<Bytes> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let result = self.inner.get(&path).await.map_err(|e| {
            crate::metrics::S3_ERRORS_TOTAL
                .with_label_values(&["get"])
                .inc();
            match e {
                object_store::Error::NotFound { path, .. } => ZeppelinError::NotFound {
                    key: path.to_string(),
                },
                other => ZeppelinError::Storage(other),
            }
        })?;
        let bytes = result.bytes().await?;
        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            size = bytes.len(),
            "s3 get"
        );
        crate::metrics::S3_OPERATION_DURATION
            .with_label_values(&["get"])
            .observe(elapsed.as_secs_f64());
        Ok(bytes)
    }

    /// Get an object by key, returning data along with the ETag for CAS operations.
    #[instrument(skip(self), fields(key = key))]
    pub async fn get_with_meta(&self, key: &str) -> Result<(Bytes, Option<String>)> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let result = self.inner.get(&path).await.map_err(|e| {
            crate::metrics::S3_ERRORS_TOTAL
                .with_label_values(&["get"])
                .inc();
            match e {
                object_store::Error::NotFound { path, .. } => ZeppelinError::NotFound {
                    key: path.to_string(),
                },
                other => ZeppelinError::Storage(other),
            }
        })?;
        let etag = result.meta.e_tag.clone();
        let bytes = result.bytes().await?;
        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            size = bytes.len(),
            etag = ?etag,
            "s3 get_with_meta"
        );
        crate::metrics::S3_OPERATION_DURATION
            .with_label_values(&["get"])
            .observe(elapsed.as_secs_f64());
        Ok((bytes, etag))
    }

    /// Put an object only if the ETag matches (compare-and-swap).
    /// Returns ManifestConflict if the ETag has changed (concurrent write).
    #[instrument(skip(self, data), fields(key = key))]
    pub async fn put_if_match(
        &self,
        key: &str,
        data: Bytes,
        etag: &str,
        namespace: &str,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let options = PutOptions {
            mode: PutMode::Update(UpdateVersion {
                e_tag: Some(etag.to_string()),
                version: None,
            }),
            ..PutOptions::default()
        };
        self.inner
            .put_opts(&path, PutPayload::from(data), options)
            .await
            .map_err(|e| match e {
                object_store::Error::Precondition { .. } => ZeppelinError::ManifestConflict {
                    namespace: namespace.to_string(),
                },
                other => {
                    crate::metrics::S3_ERRORS_TOTAL
                        .with_label_values(&["put"])
                        .inc();
                    ZeppelinError::Storage(other)
                }
            })?;
        let elapsed = start.elapsed();
        debug!(elapsed_ms = elapsed.as_millis(), "s3 put_if_match");
        crate::metrics::S3_OPERATION_DURATION
            .with_label_values(&["put"])
            .observe(elapsed.as_secs_f64());
        Ok(())
    }

    /// Put an object only if it does NOT already exist (atomic create).
    /// Returns `NamespaceAlreadyExists` if the key already exists.
    /// Uses S3's `If-None-Match: *` header via `PutMode::Create`.
    #[instrument(skip(self, data), fields(key = key))]
    pub async fn put_if_not_exists(
        &self,
        key: &str,
        data: Bytes,
        namespace: &str,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        self.inner
            .put_opts(&path, PutPayload::from(data), options)
            .await
            .map_err(|e| match e {
                object_store::Error::AlreadyExists { path, .. } => {
                    tracing::debug!(key = %path, "put_if_not_exists: object already exists");
                    ZeppelinError::NamespaceAlreadyExists {
                        namespace: namespace.to_string(),
                    }
                }
                other => {
                    crate::metrics::S3_ERRORS_TOTAL
                        .with_label_values(&["put"])
                        .inc();
                    ZeppelinError::Storage(other)
                }
            })?;
        let elapsed = start.elapsed();
        debug!(elapsed_ms = elapsed.as_millis(), "s3 put_if_not_exists");
        crate::metrics::S3_OPERATION_DURATION
            .with_label_values(&["put"])
            .observe(elapsed.as_secs_f64());
        Ok(())
    }

    /// Delete an object by key.
    #[instrument(skip(self), fields(key = key))]
    pub async fn delete(&self, key: &str) -> Result<()> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        self.inner.delete(&path).await?;
        let elapsed = start.elapsed();
        debug!(elapsed_ms = elapsed.as_millis(), "s3 delete");
        crate::metrics::S3_OPERATION_DURATION
            .with_label_values(&["delete"])
            .observe(elapsed.as_secs_f64());
        Ok(())
    }

    /// List objects under a prefix.
    #[instrument(skip(self), fields(prefix = prefix))]
    pub async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let start = std::time::Instant::now();
        use futures::TryStreamExt;
        let path = Path::parse(prefix)?;
        let stream = self.inner.list(Some(&path));
        let objects: Vec<_> = stream.try_collect().await?;
        let keys: Vec<String> = objects.iter().map(|o| o.location.to_string()).collect();
        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            count = keys.len(),
            "s3 list_prefix"
        );
        crate::metrics::S3_OPERATION_DURATION
            .with_label_values(&["list_prefix"])
            .observe(elapsed.as_secs_f64());
        Ok(keys)
    }

    /// Check if an object exists.
    #[instrument(skip(self), fields(key = key))]
    pub async fn exists(&self, key: &str) -> Result<bool> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let result = match self.inner.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => {
                crate::metrics::S3_ERRORS_TOTAL
                    .with_label_values(&["exists"])
                    .inc();
                Err(ZeppelinError::Storage(e))
            }
        };
        let elapsed = start.elapsed();
        debug!(elapsed_ms = elapsed.as_millis(), "s3 exists");
        crate::metrics::S3_OPERATION_DURATION
            .with_label_values(&["exists"])
            .observe(elapsed.as_secs_f64());
        result
    }

    /// Head request - get metadata without downloading the object.
    #[instrument(skip(self), fields(key = key))]
    pub async fn head(&self, key: &str) -> Result<object_store::ObjectMeta> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let meta = self.inner.head(&path).await.map_err(|e| {
            crate::metrics::S3_ERRORS_TOTAL
                .with_label_values(&["head"])
                .inc();
            match e {
                object_store::Error::NotFound { path, .. } => ZeppelinError::NotFound {
                    key: path.to_string(),
                },
                other => ZeppelinError::Storage(other),
            }
        })?;
        let elapsed = start.elapsed();
        debug!(elapsed_ms = elapsed.as_millis(), "s3 head");
        crate::metrics::S3_OPERATION_DURATION
            .with_label_values(&["head"])
            .observe(elapsed.as_secs_f64());
        Ok(meta)
    }

    /// Delete all objects under a prefix (for cleanup).
    #[instrument(skip(self), fields(prefix = prefix))]
    pub async fn delete_prefix(&self, prefix: &str) -> Result<usize> {
        let start = std::time::Instant::now();
        let keys = self.list_prefix(prefix).await?;
        let count = keys.len();
        let inner = &self.inner;
        let delete_futs: Vec<_> = keys
            .iter()
            .map(|key| async move {
                let path = Path::parse(key)?;
                inner.delete(&path).await?;
                Ok::<_, ZeppelinError>(())
            })
            .collect();
        let results = futures::future::join_all(delete_futs).await;
        for result in results {
            result?;
        }
        let elapsed = start.elapsed();
        debug!(elapsed_ms = elapsed.as_millis(), count, "s3 delete_prefix");
        crate::metrics::S3_OPERATION_DURATION
            .with_label_values(&["delete_prefix"])
            .observe(elapsed.as_secs_f64());
        Ok(count)
    }
}
