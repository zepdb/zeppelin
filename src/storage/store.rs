use bytes::Bytes;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path;
use object_store::{
    BackoffConfig, ClientOptions, GetOptions, ObjectStore, PutMode, PutOptions, PutPayload,
    RetryConfig, UpdateVersion,
};
use std::net::{IpAddr, SocketAddr};
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, instrument};

use crate::config::StorageConfig;
use crate::error::{Result, ZeppelinError};

/// Wrapper around the `object_store` crate providing a unified interface
/// for S3, S3-compatible, and local storage backends.
#[derive(Clone)]
pub struct ZeppelinStore {
    inner: Arc<dyn ObjectStore>,
}

/// Result of a bounded prefix deletion pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeletePrefixOutcome {
    /// Number of objects successfully deleted in this pass.
    pub deleted: usize,
    /// Whether the prefix listing was fully consumed before the time budget.
    pub complete: bool,
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
                            builder = builder
                                .with_endpoint(endpoint)
                                .with_virtual_hosted_style_request(false);
                        }
                    }
                    if let Some(ref key_id) = config.s3_access_key_id {
                        builder = builder.with_access_key_id(key_id);
                    }
                    if let Some(ref secret) = config.s3_secret_access_key {
                        builder = builder.with_secret_access_key(secret);
                    }
                    // Enable conditional PUT (ETag-based CAS) — required for
                    // manifest conflict detection and lease CAS operations.
                    builder = builder.with_conditional_put(S3ConditionalPut::ETagMatch);
                    builder = builder.with_retry(RetryConfig {
                        backoff: BackoffConfig {
                            init_backoff: Duration::from_millis(100),
                            max_backoff: Duration::from_millis(500),
                            base: 2.0,
                        },
                        max_retries: 2,
                        retry_timeout: Duration::from_secs(2),
                    });

                    // Connection pool tuning: increase idle connections and timeouts
                    // to prevent 28% sustained throughput degradation observed in Run-007.
                    let client_options = ClientOptions::new()
                        .with_allow_http(config.s3_allow_http)
                        .with_pool_max_idle_per_host(64)
                        .with_timeout(std::time::Duration::from_secs(30))
                        .with_connect_timeout(std::time::Duration::from_secs(2))
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

    /// Check that an explicitly configured S3-compatible endpoint accepts TCP connections.
    pub async fn probe_configured_endpoint(
        config: &StorageConfig,
        timeout_duration: Duration,
    ) -> Result<()> {
        if config.backend != crate::config::StorageBackend::S3 {
            return Ok(());
        }
        let Some(endpoint) = config
            .s3_endpoint
            .as_deref()
            .filter(|value| !value.is_empty())
        else {
            return Ok(());
        };
        let (host, port) = endpoint_host_port(endpoint)?;
        if let Ok(ip) = host.parse::<IpAddr>() {
            let addr = SocketAddr::new(ip, port);
            if ip.is_loopback() {
                match std::net::TcpListener::bind(addr) {
                    Ok(listener) => {
                        drop(listener);
                        return Err(ZeppelinError::Config(format!(
                            "storage endpoint {endpoint} is unreachable at {host}:{port}: no listener on loopback port"
                        )));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => return Ok(()),
                    Err(_error) => {}
                }
            }
            match tokio::task::spawn_blocking(move || {
                std::net::TcpStream::connect_timeout(&addr, timeout_duration)
            })
            .await
            {
                Ok(Ok(_stream)) => Ok(()),
                Ok(Err(error)) => Err(ZeppelinError::Config(format!(
                    "storage endpoint {endpoint} is unreachable at {host}:{port}: {error}"
                ))),
                Err(error) => Err(ZeppelinError::Config(format!(
                    "storage endpoint probe task failed: {error}"
                ))),
            }
        } else {
            match tokio::time::timeout(
                timeout_duration,
                tokio::net::TcpStream::connect((host.as_str(), port)),
            )
            .await
            {
                Ok(Ok(_stream)) => Ok(()),
                Ok(Err(error)) => Err(ZeppelinError::Config(format!(
                    "storage endpoint {endpoint} is unreachable at {host}:{port}: {error}"
                ))),
                Err(_elapsed) => Err(ZeppelinError::Config(format!(
                    "storage endpoint {endpoint} did not accept a connection within {}s",
                    timeout_duration.as_secs()
                ))),
            }
        }
    }

    /// Create a store directly from an ObjectStore instance (for testing).
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { inner: store }
    }

    /// Access the underlying `ObjectStore` (for test instrumentation such as
    /// GET-counting wrappers). Production code above the storage layer must
    /// not use this to bypass `ZeppelinStore`.
    pub fn inner(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.inner)
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

    /// Get a byte range from an object by key. Returns NotFound if it doesn't exist.
    #[instrument(skip(self), fields(key = key, range_start = range.start, range_end = range.end))]
    pub async fn get_range(&self, key: &str, range: Range<usize>) -> Result<Bytes> {
        if range.start >= range.end {
            return Err(ZeppelinError::Storage(object_store::Error::Generic {
                store: "zeppelin",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "invalid empty or reversed range for {key}: {}..{}",
                        range.start, range.end
                    ),
                )),
            }));
        }

        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let result = self.inner.get_range(&path, range).await.map_err(|e| {
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
        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            size = result.len(),
            "s3 get_range"
        );
        crate::metrics::S3_OPERATION_DURATION
            .with_label_values(&["get"])
            .observe(elapsed.as_secs_f64());
        Ok(result)
    }

    /// Get multiple byte ranges from an object by key. Returns NotFound if it doesn't exist.
    #[instrument(skip(self, ranges), fields(key = key, ranges = ranges.len()))]
    pub async fn get_ranges(&self, key: &str, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let result = self.inner.get_ranges(&path, ranges).await.map_err(|e| {
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
        let elapsed = start.elapsed();
        let size: usize = result.iter().map(Bytes::len).sum();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            size,
            ranges = ranges.len(),
            "s3 get_ranges"
        );
        crate::metrics::S3_OPERATION_DURATION
            .with_label_values(&["get"])
            .observe(elapsed.as_secs_f64());
        Ok(result)
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

    /// Get an object only if its ETag differs from `etag`.
    ///
    /// Returns `Ok(None)` when storage reports the object has not changed
    /// (`304 Not Modified`, surfaced by `object_store` as `NotModified`;
    /// some S3-compatible stores surface this as `Precondition`).
    #[instrument(skip(self), fields(key = key, etag = %etag))]
    pub async fn get_if_none_match(
        &self,
        key: &str,
        etag: &str,
    ) -> Result<Option<(Bytes, Option<String>)>> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let options = GetOptions {
            if_none_match: Some(etag.to_string()),
            ..GetOptions::default()
        };

        let result = match self.inner.get_opts(&path, options).await {
            Ok(result) => result,
            Err(
                object_store::Error::NotModified { .. } | object_store::Error::Precondition { .. },
            ) => {
                let elapsed = start.elapsed();
                debug!(
                    elapsed_ms = elapsed.as_millis(),
                    etag = %etag,
                    "s3 get_if_none_match not modified"
                );
                crate::metrics::S3_OPERATION_DURATION
                    .with_label_values(&["get"])
                    .observe(elapsed.as_secs_f64());
                return Ok(None);
            }
            Err(e) => {
                crate::metrics::S3_ERRORS_TOTAL
                    .with_label_values(&["get"])
                    .inc();
                return Err(match e {
                    object_store::Error::NotFound { path, .. } => ZeppelinError::NotFound {
                        key: path.to_string(),
                    },
                    other => ZeppelinError::Storage(other),
                });
            }
        };

        let next_etag = result.meta.e_tag.clone();
        let bytes = result.bytes().await?;
        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            size = bytes.len(),
            etag = ?next_etag,
            "s3 get_if_none_match modified"
        );
        crate::metrics::S3_OPERATION_DURATION
            .with_label_values(&["get"])
            .observe(elapsed.as_secs_f64());
        Ok(Some((bytes, next_etag)))
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
    pub async fn put_if_not_exists(&self, key: &str, data: Bytes, namespace: &str) -> Result<()> {
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
        self.inner.delete(&path).await.map_err(|e| match e {
            object_store::Error::NotFound { path, .. } => ZeppelinError::NotFound {
                key: path.to_string(),
            },
            other => ZeppelinError::Storage(other),
        })?;
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
        debug_assert!(
            !prefix.is_empty(),
            "recursive root listing must use list_common_prefixes"
        );
        if prefix.is_empty() {
            return Err(ZeppelinError::Validation(
                "list_prefix requires a non-empty prefix; use list_common_prefixes for namespace discovery"
                    .to_string(),
            ));
        }

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

    /// List immediate child prefixes under a prefix using the object-store delimiter.
    #[instrument(skip(self), fields(prefix = prefix))]
    pub async fn list_common_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        let start = std::time::Instant::now();
        let path = Path::parse(prefix)?;
        let result = self.inner.list_with_delimiter(Some(&path)).await?;
        let mut prefixes = std::collections::BTreeSet::new();
        for common_prefix in &result.common_prefixes {
            prefixes.insert(common_prefix.to_string());
        }
        for object in &result.objects {
            let key = object.location.to_string();
            let Some(remainder) = key.strip_prefix(prefix) else {
                continue;
            };
            let Some(delimiter_idx) = remainder.find('/') else {
                continue;
            };
            prefixes.insert(format!("{}{}", prefix, &remainder[..=delimiter_idx]));
        }
        let prefixes: Vec<String> = prefixes.into_iter().collect();
        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            count = prefixes.len(),
            "s3 list_common_prefixes"
        );
        crate::metrics::S3_OPERATION_DURATION
            .with_label_values(&["list_common_prefixes"])
            .observe(elapsed.as_secs_f64());
        Ok(prefixes)
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
        let outcome = self
            .delete_prefix_paged(prefix, None, Duration::MAX)
            .await?;
        Ok(outcome.deleted)
    }

    /// Delete objects under a prefix without materializing the full key list.
    ///
    /// Objects are deleted in chunks with bounded concurrency. If `exclude` is
    /// provided, that exact key is left untouched; namespace deletion uses this
    /// to keep `meta.json` as the tombstone until all other data is gone.
    #[instrument(skip(self), fields(prefix = prefix, exclude = exclude.unwrap_or("<none>")))]
    pub async fn delete_prefix_paged(
        &self,
        prefix: &str,
        exclude: Option<&str>,
        budget: Duration,
    ) -> Result<DeletePrefixOutcome> {
        debug_assert!(
            !prefix.is_empty(),
            "recursive root deletion is never allowed"
        );
        if prefix.is_empty() {
            return Err(ZeppelinError::Validation(
                "delete_prefix requires a non-empty prefix".to_string(),
            ));
        }

        let start = std::time::Instant::now();
        use futures::TryStreamExt;

        let path = Path::parse(prefix)?;
        let mut listed = self.inner.list(Some(&path));
        let mut chunk = Vec::with_capacity(1000);
        let mut deleted = 0usize;
        let mut complete = true;

        while let Some(object) = listed.try_next().await? {
            let key = object.location.to_string();
            if exclude == Some(key.as_str()) {
                continue;
            }
            chunk.push(key);
            if chunk.len() == 1000 {
                deleted += self.delete_key_chunk(std::mem::take(&mut chunk)).await?;
                if start.elapsed() >= budget {
                    complete = false;
                    break;
                }
            }
        }

        if complete && !chunk.is_empty() {
            deleted += self.delete_key_chunk(chunk).await?;
        }

        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            count = deleted,
            complete,
            "s3 delete_prefix"
        );
        crate::metrics::S3_OPERATION_DURATION
            .with_label_values(&["delete_prefix"])
            .observe(elapsed.as_secs_f64());
        Ok(DeletePrefixOutcome { deleted, complete })
    }

    async fn delete_key_chunk(&self, keys: Vec<String>) -> Result<usize> {
        use futures::StreamExt;

        let count = keys.len();
        let inner = Arc::clone(&self.inner);
        let mut deletes = futures::stream::iter(keys.into_iter().map(move |key| {
            let inner = Arc::clone(&inner);
            async move {
                let path = Path::parse(&key)?;
                match inner.delete(&path).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => return Err(ZeppelinError::Storage(e)),
                }
                Ok::<_, ZeppelinError>(())
            }
        }))
        .buffer_unordered(32);

        while let Some(result) = deletes.next().await {
            result?;
        }

        Ok(count)
    }
}

fn endpoint_host_port(endpoint: &str) -> Result<(String, u16)> {
    let (default_port, without_scheme) = if let Some(rest) = endpoint.strip_prefix("http://") {
        (80, rest)
    } else if let Some(rest) = endpoint.strip_prefix("https://") {
        (443, rest)
    } else {
        (443, endpoint)
    };
    let authority = without_scheme
        .split('/')
        .next()
        .filter(|authority| !authority.is_empty())
        .ok_or_else(|| ZeppelinError::Config(format!("invalid S3 endpoint URL: {endpoint}")))?;
    let authority = authority.rsplit('@').next().unwrap_or(authority);

    if let Some(rest) = authority.strip_prefix('[') {
        let Some((host, after_host)) = rest.split_once(']') else {
            return Err(ZeppelinError::Config(format!(
                "invalid bracketed S3 endpoint host: {endpoint}"
            )));
        };
        let port = after_host
            .strip_prefix(':')
            .map(parse_endpoint_port)
            .transpose()?
            .unwrap_or(default_port);
        return Ok((host.to_string(), port));
    }

    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() => (host, parse_endpoint_port(port)?),
        Some((_host, _port)) => {
            return Err(ZeppelinError::Config(format!(
                "invalid S3 endpoint host: {endpoint}"
            )));
        }
        None => (authority, default_port),
    };
    Ok((host.to_string(), port))
}

fn parse_endpoint_port(port: &str) -> Result<u16> {
    port.parse::<u16>()
        .map_err(|error| ZeppelinError::Config(format!("invalid S3 endpoint port {port}: {error}")))
}
