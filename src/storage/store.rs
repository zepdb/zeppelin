//! Object-storage construction and the only object-store API exposed to
//! Zeppelin domains.
//!
//! [`crate::storage::store::ZeppelinStore`] sits below manifests, WAL
//! management, compaction, indexing,
//! query execution, cache hydration, and namespace management. Those layers pass
//! object keys and immutable byte buffers into this wrapper; this file translates
//! them into [`object_store::ObjectStore`] operations, normalizes selected
//! errors into [`crate::error::ZeppelinError`], and records storage latency and
//! error metrics. Production code above this module must not call
//! `object_store` directly.
//!
//! S3 or MinIO remains authoritative. A successful
//! [`crate::storage::store::ZeppelinStore::put`] only
//! creates or replaces an object; it does **not** make a WAL fragment or segment
//! visible. Visibility is a separate manifest operation built from
//! [`crate::storage::store::ZeppelinStore::get_with_meta`] and
//! [`crate::storage::store::ZeppelinStore::put_if_match`]. The local
//! backend implements the same interface for local runs and tests, but does not
//! change that production authority model.
//!
//! ```text
//! WAL / manifest / compaction / query / cache / namespace layers
//!                              |
//!                              | object keys and Bytes
//!                              v
//!                       ZeppelinStore
//!                    /        |         \
//!          ordinary I/O   conditional I/O   discovery / cleanup
//!          get, put, head  ETag CAS, create  list, delete prefix
//!                    \        |         /
//!                              v
//!                  dyn ObjectStore implementation
//!                    /                     \
//!          S3 or MinIO (authority)       local filesystem
//! ```
//!
//! ## Reading map
//!
//! 1. Start with [`crate::storage::store::ZeppelinStore`] and
//!    [`crate::storage::store::ZeppelinStore::from_config`] to see the
//!    backend boundary and S3 client policy.
//! 2. Read [`crate::storage::store::ZeppelinStore::get`],
//!    [`crate::storage::store::ZeppelinStore::get_range`], and
//!    [`crate::storage::store::ZeppelinStore::get_ranges`] for ordinary and
//!    query-path reads.
//! 3. Read [`crate::storage::store::ZeppelinStore::get_with_meta`],
//!    [`crate::storage::store::ZeppelinStore::get_if_none_match`],
//!    [`crate::storage::store::ZeppelinStore::put_if_match`], and
//!    [`crate::storage::store::ZeppelinStore::put_if_not_exists`] for conditional
//!    consistency operations.
//! 4. Finish with [`crate::storage::store::ZeppelinStore::delete_prefix_paged`]
//!    and [`crate::storage::store::DeletePrefixOutcome`] for bounded namespace
//!    cleanup.
//!
//! ## Invariants not to break
//!
//! - Higher layers use this wrapper rather than bypassing storage policy and
//!   instrumentation through the raw [`object_store::ObjectStore`].
//! - S3 conditional PUT support stays enabled; manifest publication and leases
//!   require an ETag check, not a read-then-unconditional-write sequence.
//! - Object existence is distinct from manifest visibility.
//! - Recursive listing and deletion reject an empty prefix where it could scan
//!   or erase the entire store accidentally.
//! - Range reads reject empty and reversed ranges before issuing remote I/O.
//! - Prefix deletion is allowed to make partial progress and reports whether
//!   the listing was completely consumed.
//!
//! ## Rust concepts used here
//!
//! The wrapper stores `Arc<dyn ObjectStore>`. `dyn ObjectStore` is trait-object
//! dispatch: the concrete S3, local, in-memory, or test-instrumented backend is
//! selected at runtime. `Arc` gives clones shared ownership of that backend and
//! keeps it alive across async tasks. A Java reader can think of an interface
//! reference held by a thread-safe reference-counted owner. In C this would
//! require a function table plus explicit lifetime and synchronization rules;
//! Rust checks the trait's `Send + Sync` requirements and drops the backend only
//! after the last `Arc` is gone.
//!
//! Payloads use [`bytes::Bytes`], whose clones share an immutable buffer instead
//! of copying every byte. Conditional operations use `Result`, `Option`, and
//! exhaustive `match` arms so callers must distinguish success, unchanged state,
//! conflicts, missing objects, and storage failures explicitly.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use object_store::aws::{AmazonS3Builder, S3ConditionalPut, S3CopyIfNotExists};
use object_store::path::Path;
use object_store::{
    BackoffConfig, ClientOptions, GetOptions, ObjectStore, PutMode, PutOptions, PutPayload,
    RetryConfig, UpdateVersion,
};
use std::collections::BTreeSet;
use std::net::{IpAddr, SocketAddr};
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, instrument};

use crate::config::StorageConfig;
use crate::error::{Result, ZeppelinError};

/// Shared object-storage gateway used by every Zeppelin domain layer.
///
/// Cloning this value clones the [`Arc`] handle, not the backend or its data.
/// All clones therefore use the same connection pool and underlying store.
/// The type deliberately exposes domain-oriented operations such as ETag
/// compare-and-swap and bounded prefix deletion rather than every
/// [`ObjectStore`] primitive.
///
/// # Examples
///
/// A WAL writer and a query executor may each own a cloned `ZeppelinStore`.
/// Both handles address the same S3 bucket, while Rust keeps the shared backend
/// alive until both components have been dropped.
#[derive(Clone)]
pub struct ZeppelinStore {
    /// Runtime-selected backend shared by all clones of this gateway.
    inner: Arc<dyn ObjectStore>,
    /// Prefix-cleanup behavior selected when the backend is constructed.
    prefix_delete_mode: PrefixDeleteMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrefixDeleteMode {
    LegacyPerKeyUnordered32,
    NativeBatch,
}

/// Progress made by one bounded recursive prefix-deletion pass.
///
/// A caller resumes cleanup when [`Self::complete`] is `false`; it should not
/// infer that the remaining key set is unchanged because other actors may have
/// created or deleted objects between passes.
///
/// # Examples
///
/// `DeletePrefixOutcome { deleted: 1_000, complete: false }` means this pass
/// accepted 1,000 keys as deleted but stopped before consuming the entire
/// listing. A later pass should list the prefix again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeletePrefixOutcome {
    /// Number of selected keys deleted or already absent during this pass.
    pub deleted: usize,
    /// Whether the pass observed the end of the source listing before stopping.
    pub complete: bool,
}

/// One non-empty opaque backend identity observed for an object-store object.
///
/// ETag takes precedence when a backend supplies both forms because S3 exposes
/// it consistently on LIST and GET. Absence is represented by `None` on the
/// containing [`ListedObject`], so two unversioned observations cannot compare
/// equal as if they authorized cache reuse.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StorageVersion {
    /// Entity tag supplied by the backend.
    Etag(String),
    /// Backend-specific version used only when no ETag is available.
    BackendVersion(String),
}

impl StorageVersion {
    /// Returns the ETag when this identity came from an ETag observation.
    #[must_use]
    pub fn etag(&self) -> Option<&str> {
        match self {
            Self::Etag(etag) => Some(etag),
            Self::BackendVersion(_) => None,
        }
    }

    fn from_parts(etag: Option<String>, backend_version: Option<String>) -> Option<Self> {
        etag.filter(|value| !value.is_empty())
            .map(Self::Etag)
            .or_else(|| {
                backend_version
                    .filter(|value| !value.is_empty())
                    .map(Self::BackendVersion)
            })
    }
}

/// Storage-owned metadata for one object returned by a recursive LIST.
///
/// Domain layers receive this type instead of depending directly on
/// `object_store::ObjectMeta`. The metadata is an observation from S3, not a
/// replacement for an authoritative body read when the version is absent or
/// has changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedObject {
    /// Exact object key returned by the backend.
    pub key: String,
    /// Object payload size in bytes.
    pub size: u64,
    /// Backend-reported last modification timestamp.
    pub last_modified: DateTime<Utc>,
    /// Non-empty opaque identity, or `None` when this observation is unversioned.
    pub version: Option<StorageVersion>,
}

/// Constructs backends and performs Zeppelin's normalized storage operations.
impl ZeppelinStore {
    /// Builds the configured storage backend and wraps it in the Zeppelin gateway.
    ///
    /// S3 configuration enables ETag conditional PUTs for manifest and lease
    /// compare-and-swap, atomic destination creation for clone copies, bounded
    /// retries, and a shared HTTP connection pool. The local backend creates its
    /// configured root directory when it does not already exist. GCS and Azure
    /// values are rejected because this constructor does not implement them.
    ///
    /// # Parameters
    ///
    /// - `config`: Borrowed boot-time backend, bucket/path, endpoint, credential,
    ///   and transport settings. The returned store owns everything it needs;
    ///   it does not retain this borrow.
    ///
    /// # Returns
    ///
    /// A store whose concrete backend is hidden behind a shared
    /// [`ObjectStore`] trait object.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Config`] when an S3 or local backend cannot be
    /// built or when the selected backend is unsupported. Local directory
    /// creation can also return a filesystem error. No alternate backend is
    /// selected after a failure.
    ///
    /// # Side Effects
    ///
    /// Building an S3 backend configures a client but does not contact the
    /// endpoint. Building a local backend may synchronously create directories
    /// on disk.
    ///
    /// # Consistency
    ///
    /// `S3ConditionalPut::ETagMatch` is essential: without it,
    /// [`Self::put_if_match`] cannot enforce the CAS half of manifest and lease
    /// correctness. Retry policy belongs to the backend, but conditional
    /// preconditions still decide whether a write is allowed.
    ///
    /// # Performance
    ///
    /// S3 requests use at most two configured retries, 100-500 ms exponential
    /// backoff, a two-second retry window, a 30-second request timeout, and up
    /// to 64 idle pooled connections per host. Local root creation performs
    /// blocking filesystem work during construction.
    ///
    /// # Examples
    ///
    /// ```text
    /// StorageBackend::S3 + bucket "vectors"
    ///         -> configured S3 client, no request yet
    ///
    /// StorageBackend::Local + missing root "/tmp/zeppelin"
    ///         -> directory is created, then a local ObjectStore is wrapped
    ///
    /// StorageBackend::Gcs
    ///         -> configuration error; Zeppelin does not fall back to local disk
    /// ```
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The `match` must account for every [`crate::config::StorageBackend`]
    /// variant. The final `backend` arm rejects currently unsupported variants;
    /// it is an explicit error, not Java-style `null` or a C sentinel. Each
    /// builder call consumes and returns the builder, so rebinding `builder`
    /// models a checked construction pipeline without shared mutable state.
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
                    // Enable atomic create semantics for server-side copy,
                    // used by restore-as-clone materialization.
                    builder = builder.with_copy_if_not_exists(S3CopyIfNotExists::Multipart);
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

        let prefix_delete_mode = if config.backend == crate::config::StorageBackend::S3 {
            PrefixDeleteMode::NativeBatch
        } else {
            PrefixDeleteMode::LegacyPerKeyUnordered32
        };
        Ok(Self {
            inner: store,
            prefix_delete_mode,
        })
    }

    /// Fails startup early when an explicit S3-compatible endpoint is not reachable.
    ///
    /// The probe applies only to S3 configurations with a non-empty custom
    /// endpoint. Hostnames are tested with an asynchronous TCP connection under
    /// a Tokio timeout. Numeric IPs use `connect_timeout` in a blocking worker;
    /// for loopback IPs, an occupied port is accepted after a bind check without
    /// verifying that the listener is an object store. This is a transport
    /// reachability check, not an authentication, bucket, or S3-protocol check.
    ///
    /// # Parameters
    ///
    /// - `config`: Borrowed storage configuration containing the backend and
    ///   optional endpoint URL.
    /// - `timeout_duration`: Maximum connection duration. For loopback ports
    ///   already found to be occupied, the function returns before using it.
    ///
    /// # Returns
    ///
    /// `Ok(())` when no probe is applicable or the configured transport appears
    /// reachable.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Config`] for a malformed endpoint, invalid port,
    /// refused or timed-out connection, or failure to join the blocking probe
    /// task. The function does not build a fallback endpoint.
    ///
    /// # Side Effects
    ///
    /// May bind a loopback address temporarily and may open one TCP connection.
    /// Numeric non-loopback probes spawn blocking work on Tokio's blocking pool.
    ///
    /// # Performance
    ///
    /// Performs no object-store request. At most one socket probe is attempted.
    ///
    /// # Examples
    ///
    /// ```text
    /// local backend                         -> no probe
    /// S3 with no custom endpoint            -> no probe; AWS resolution happens later
    /// S3 endpoint http://127.0.0.1:9000     -> checks whether the loopback port is occupied
    /// S3 endpoint http://minio:9000         -> resolves and connects within the timeout
    /// ```
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `let Some(endpoint) = ... else` narrows an optional value and returns
    /// early when it is absent. Rust then knows `endpoint` is a valid `&str` for
    /// the rest of the function. `spawn_blocking` moves the numeric socket
    /// address and timeout into work that may block, preventing that work from
    /// stalling an async executor thread; Java commonly uses a separate executor
    /// for the same reason, while C event loops require an equivalent worker
    /// handoff to be designed manually.
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

    /// Wraps an already constructed backend, primarily for tests and instrumentation.
    ///
    /// # Parameters
    ///
    /// - `store`: Shared ownership of a backend implementing [`ObjectStore`].
    ///   The caller may retain another [`Arc`] to the same backend.
    ///
    /// # Returns
    ///
    /// A gateway that delegates to the supplied backend without changing its
    /// retry, consistency, or transport behavior.
    ///
    /// # Examples
    ///
    /// An integration test can wrap an in-memory store in a GET-counting
    /// implementation and pass its `Arc<dyn ObjectStore>` here. Production code
    /// normally uses [`Self::from_config`] instead.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner: store,
            prefix_delete_mode: PrefixDeleteMode::LegacyPerKeyUnordered32,
        }
    }

    /// Wraps an instrumented backend whose `delete_stream` preserves native S3 batches.
    ///
    /// This constructor exists for the dedicated performance harness. Generic
    /// fault decorators use [`Self::new`] so their established per-key failure
    /// scheduling remains unchanged; the perf harness explicitly opts into this
    /// constructor after installing transparent forwarding decorators.
    #[doc(hidden)]
    pub fn new_with_native_batch_delete(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner: store,
            prefix_delete_mode: PrefixDeleteMode::NativeBatch,
        }
    }

    /// Returns a shared handle to the raw backend for test instrumentation.
    ///
    /// Production domain code must not use this escape hatch: bypassing
    /// `ZeppelinStore` also bypasses its error normalization, metrics, range
    /// validation, and conditional-operation vocabulary.
    ///
    /// # Returns
    ///
    /// A newly cloned [`Arc`] pointing at the same backend. Cloning increments
    /// the strong reference count; it does not clone the remote client or data.
    ///
    /// # Examples
    ///
    /// A test can obtain this handle, put a fault-injecting `ObjectStore` around
    /// it, and pass the wrapped backend to [`Self::new`].
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `Arc::clone` makes shared ownership visible at the call site. It resembles
    /// copying a Java reference with an explicit atomic retain, or incrementing a
    /// C reference count, but Rust automatically releases it and prevents a
    /// dangling backend pointer.
    pub fn inner(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.inner)
    }

    /// Writes a complete byte payload to an object key.
    ///
    /// This is an ordinary unconditional PUT. It is suitable for new immutable
    /// artifact keys, but the storage API itself does not prevent replacement if
    /// a caller reuses a key. Artifact visibility remains controlled by the
    /// manifest rather than by this successful upload.
    ///
    /// # Parameters
    ///
    /// - `key`: Object-store key, using object-key path rules rather than HTTP
    ///   URL-segment rules.
    /// - `data`: Owned immutable payload. Moving [`Bytes`] into this method is
    ///   cheap because it transfers a shared buffer descriptor.
    ///
    /// # Returns
    ///
    /// `Ok(())` after the backend has atomically accepted the complete payload.
    ///
    /// # Errors
    ///
    /// Returns an error when the key cannot be parsed or the backend rejects or
    /// cannot complete the write. The function does not silently write elsewhere.
    ///
    /// # Side Effects
    ///
    /// Performs one logical object-store PUT and records successful operation
    /// latency. The configured S3 backend may retry transient failures internally.
    ///
    /// # Consistency
    ///
    /// The underlying [`ObjectStore`] contract publishes the payload atomically,
    /// so readers do not observe a partial object. A successful write alone does
    /// not publish the object through a namespace manifest.
    ///
    /// # Performance
    ///
    /// Uploads the entire payload and allocates no second application-level copy.
    ///
    /// # Examples
    ///
    /// A compactor uploads `namespaces/acme/segments/42.cvec`. The object now
    /// exists, but queries continue to use the previous segment set until a
    /// later conditional manifest update references it.
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

    /// Downloads the complete object stored at a key.
    ///
    /// # Parameters
    ///
    /// - `key`: Key for an authoritative object or an immutable artifact already
    ///   discovered through authoritative metadata.
    ///
    /// # Returns
    ///
    /// Owned [`Bytes`] containing the full object body. The buffer may be shared
    /// cheaply by cloning it after return.
    ///
    /// # Errors
    ///
    /// Maps a missing object to [`ZeppelinError::NotFound`]. Invalid keys, request
    /// failures, and body-download failures remain explicit storage errors; no
    /// cached or empty value is substituted.
    ///
    /// # Side Effects
    ///
    /// Performs one logical full-object GET, increments the GET error counter
    /// when the initial request fails, and records latency on success.
    ///
    /// # Consistency
    ///
    /// This method reads the configured backend directly. Higher-level cache
    /// code may call it after a miss, but cache contents never override the
    /// authoritative bytes returned here.
    ///
    /// # Performance
    ///
    /// Transfers and retains the entire object in memory. Query code should use
    /// [`Self::get_range`] or [`Self::get_ranges`] when it needs only selected
    /// index regions.
    ///
    /// # Examples
    ///
    /// Reading an existing 4 KiB manifest returns all 4 KiB. If the referenced
    /// key is absent, the caller receives `NotFound` and must treat the missing
    /// authoritative state according to its own contract rather than continuing
    /// with an empty manifest.
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

    /// Downloads one non-empty, half-open byte range from an object.
    ///
    /// # Parameters
    ///
    /// - `key`: Object key to read.
    /// - `range`: Zero-based byte offsets `start..end`; `start` is included and
    ///   `end` is excluded. The range must contain at least one byte.
    ///
    /// # Returns
    ///
    /// [`Bytes`] containing exactly the range returned by the backend.
    ///
    /// # Errors
    ///
    /// Empty or reversed ranges are rejected before network I/O. A missing key
    /// becomes [`ZeppelinError::NotFound`]; invalid keys, out-of-bounds ranges,
    /// and transport failures remain storage errors.
    ///
    /// # Side Effects
    ///
    /// Performs one logical ranged GET and records GET error and duration
    /// metrics in the same way as [`Self::get`].
    ///
    /// # Performance
    ///
    /// Transfers only the selected region. This is a core query-path primitive
    /// for reading cluster or vector blocks without downloading a whole segment.
    ///
    /// # Examples
    ///
    /// For a segment object whose header occupies bytes `0..128`, requesting
    /// `0..128` returns those 128 bytes. Requests `128..128` and `200..100`
    /// fail locally and issue no object-store request.
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

    /// Downloads multiple requested byte ranges from one object.
    ///
    /// The returned vector preserves the input range order. The underlying
    /// `object_store` implementation may coalesce nearby ranges into fewer
    /// physical requests; this wrapper exposes the logical result, not a fixed
    /// physical GET count.
    ///
    /// # Parameters
    ///
    /// - `key`: Object key shared by all requested ranges.
    /// - `ranges`: Borrowed half-open byte ranges. This wrapper delegates range
    ///   validation to `object_store` for the batched form.
    ///
    /// # Returns
    ///
    /// One [`Bytes`] value per input range in corresponding order. An empty
    /// input produces an empty vector if the backend accepts it.
    ///
    /// # Errors
    ///
    /// Maps a missing key to [`ZeppelinError::NotFound`]. Invalid keys or ranges
    /// and backend failures are returned without a partial result vector.
    ///
    /// # Side Effects
    ///
    /// Issues the backend's multi-range read plan and records total returned
    /// bytes and logical GET latency. A failed initial request increments the
    /// GET error counter.
    ///
    /// # Performance
    ///
    /// Can reduce request overhead compared with independent range reads because
    /// `object_store` coalesces suitable ranges. It still retains every returned
    /// range in memory at once.
    ///
    /// # Examples
    ///
    /// A rerank stage can request vector regions `[0..128, 1024..1152]`. The
    /// result contains two buffers in that order even if the backend fetches one
    /// wider physical region and slices it internally.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `&[Range<usize>]` is a borrowed slice: the method can inspect the caller's
    /// contiguous range array during the call but cannot resize, free, or retain
    /// it beyond the borrow. It resembles a Java array reference plus a read-only
    /// API, or `const Range *` with a length in C, with compiler-checked bounds
    /// and lifetime.
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

    /// Downloads an object together with the backend version tag used for CAS.
    ///
    /// Higher layers use the returned ETag as the version they observed, then
    /// pass it to [`Self::put_if_match`] when publishing a replacement. The body
    /// and metadata come from the same backend GET result.
    ///
    /// # Parameters
    ///
    /// - `key`: Key of the object whose body and version metadata are required.
    ///
    /// # Returns
    ///
    /// A pair of full object bytes and an optional ETag. `None` means the backend
    /// supplied no ETag; callers requiring CAS cannot invent one.
    ///
    /// # Errors
    ///
    /// Missing objects become [`ZeppelinError::NotFound`]. Key parsing, request,
    /// and body-download failures are returned explicitly.
    ///
    /// # Side Effects
    ///
    /// Performs one full-object GET and records GET metrics on the same paths as
    /// [`Self::get`].
    ///
    /// # Consistency
    ///
    /// The ETag identifies the version returned by this request. A later
    /// [`Self::put_if_match`] uses it to prevent a stale caller from overwriting
    /// a newer authoritative manifest or lease.
    ///
    /// # Examples
    ///
    /// ```text
    /// read manifest bytes + ETag "v12"
    ///                 |
    ///                 v
    /// derive replacement manifest
    ///                 |
    ///                 v
    /// put_if_match(..., "v12") -> succeeds only if v12 is still current
    /// ```
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

    /// Downloads an object only when its current ETag differs from a known version.
    ///
    /// This supports cache revalidation without treating the cache as
    /// authoritative. `NotModified` and `Precondition` responses are both
    /// interpreted as "the supplied version is still current" because supported
    /// S3-compatible stores use either representation for `If-None-Match`.
    ///
    /// # Parameters
    ///
    /// - `key`: Object to revalidate against authoritative storage.
    /// - `etag`: Version already held by the caller. The value is copied into the
    ///   request options and must use the backend's expected ETag representation.
    ///
    /// # Returns
    ///
    /// `None` when the object has not changed. `Some((bytes, next_etag))` contains
    /// the complete current object and its optional new ETag when it changed.
    ///
    /// # Errors
    ///
    /// Missing objects become [`ZeppelinError::NotFound`]. Other request, key,
    /// and body-download failures are returned explicitly and do not cause a
    /// caller to keep using cached data as if it had been revalidated.
    ///
    /// # Side Effects
    ///
    /// Performs one conditional full-object GET. Unchanged and changed responses
    /// both record successful duration; request failures increment the GET error
    /// counter.
    ///
    /// # Consistency
    ///
    /// `None` is evidence from the object store, not a cache-only decision. A
    /// caller may retain its cached bytes only after receiving that response.
    ///
    /// # Performance
    ///
    /// An unchanged response avoids transferring the object body. A changed
    /// response transfers and retains the complete body.
    ///
    /// # Examples
    ///
    /// A manifest cache holding ETag `v12` asks for the same key. If S3 still has
    /// `v12`, this returns `None`; if S3 has `v13`, it returns the `v13` bytes and
    /// ETag; if S3 is unavailable, it returns an error rather than declaring the
    /// cached `v12` authoritative.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The nested `Option<(Bytes, Option<String>)>` separates two independent
    /// facts: whether a body was transferred and whether that response included
    /// an ETag. Unlike `null`, Rust forces callers to handle each absence. The
    /// `match` also groups two concrete backend errors into the same deliberate
    /// unchanged outcome while preserving every other error.
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

    /// Replaces an object only when its current ETag matches the observed version.
    ///
    /// This is the storage half of compare-and-swap publication. Manifest and
    /// lease code first reads an object and ETag, derives a replacement, and then
    /// calls this method. A competing update changes the ETag and turns this
    /// request into an explicit conflict instead of a lost update.
    ///
    /// # Parameters
    ///
    /// - `key`: Object key to replace conditionally.
    /// - `data`: Complete owned replacement payload.
    /// - `etag`: Version on which the caller based the replacement.
    /// - `namespace`: Domain namespace reported if the precondition loses a race.
    ///
    /// # Returns
    ///
    /// `Ok(())` only after the conditional replacement succeeds.
    ///
    /// # Errors
    ///
    /// An ETag precondition failure becomes
    /// [`ZeppelinError::ManifestConflict`], telling the caller to reload and
    /// rebase. Invalid keys and other backend failures remain storage errors.
    /// The old authoritative object remains current after a conflict.
    ///
    /// # Side Effects
    ///
    /// Performs one conditional PUT and records PUT latency on success. Non-CAS
    /// backend failures increment the PUT error counter.
    ///
    /// # Consistency
    ///
    /// S3 construction must retain `S3ConditionalPut::ETagMatch`. This method
    /// supplies CAS but does not perform a lease fencing check; manifest/lease
    /// callers are responsible for any additional protocol layer they require.
    ///
    /// # Performance
    ///
    /// Uploads one complete replacement object. The method performs no preceding
    /// read because the caller already supplies the observed ETag.
    ///
    /// # Examples
    ///
    /// ```text
    /// caller A and B both read ETag v12
    ///              |
    /// A PUT if v12 succeeds -> object becomes v13
    ///              |
    /// B PUT if v12 ---------> ManifestConflict; v13 is not overwritten
    /// ```
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Moving `Bytes` into the request transfers ownership of a small shared
    /// buffer handle; it does not imply a byte-for-byte clone. `map_err` converts
    /// the backend's error enum into Zeppelin's domain error while the `?`
    /// operator returns early on failure. Java would commonly throw and catch an
    /// exception here; C would need an explicit status plus disciplined cleanup.
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

    /// Creates an object only when its key does not already exist.
    ///
    /// Namespace creation and first-time manifest publication use this operation
    /// to prevent a caller from replacing established authoritative state. For
    /// S3, `PutMode::Create` is implemented with `If-None-Match: *`.
    ///
    /// # Parameters
    ///
    /// - `key`: Destination object key.
    /// - `data`: Complete owned payload to create.
    /// - `namespace`: Domain name included in the already-exists error.
    ///
    /// # Returns
    ///
    /// `Ok(())` only when this call created the destination.
    ///
    /// # Errors
    ///
    /// An existing destination becomes
    /// [`ZeppelinError::NamespaceAlreadyExists`]. Invalid keys and other backend
    /// failures remain errors. The existing object's bytes are never replaced by
    /// the collision path.
    ///
    /// # Side Effects
    ///
    /// Performs one create-only PUT and records PUT latency on success. Backend
    /// failures other than an expected collision increment the PUT error metric.
    ///
    /// # Consistency
    ///
    /// This is an atomic create precondition supplied by the backend, not a
    /// racy `exists` check followed by [`Self::put`].
    ///
    /// # Examples
    ///
    /// The first request creating `acme/meta.json` succeeds. A concurrent request
    /// for the same namespace receives `NamespaceAlreadyExists`, and the first
    /// metadata object remains authoritative.
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

    /// Copies an object within the backend without replacing an existing destination.
    ///
    /// Namespace clone materialization uses this to copy immutable artifacts
    /// while preserving collision safety at the target. The source remains in
    /// place; this is a copy, not a rename, and it does not publish a target
    /// manifest by itself.
    ///
    /// # Parameters
    ///
    /// - `from`: Existing source object key.
    /// - `to`: Destination key that must be absent.
    /// - `namespace`: Target namespace context retained by the public API. The
    ///   current implementation does not use it to rewrite backend errors.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the destination was created from the source.
    ///
    /// # Errors
    ///
    /// Returns a storage error if either key is invalid, the source is missing,
    /// the destination exists, or the backend cannot complete its configured
    /// copy-if-absent operation. A failed clone workflow may already have copied
    /// other keys and must clean them up at its orchestration layer.
    ///
    /// # Side Effects
    ///
    /// Performs a server-side copy operation within the configured store and
    /// records copy latency on success. Failures increment the copy error metric.
    ///
    /// # Consistency
    ///
    /// The destination precondition is delegated to the backend. S3 construction
    /// explicitly enables the multipart copy-if-not-exists strategy. This method
    /// neither deletes the source nor changes manifest visibility.
    ///
    /// # Examples
    ///
    /// Cloning a snapshot may copy source segment `segments/42.cvec` to a fresh
    /// target key. If that target key already exists, the copy fails rather than
    /// replacing an artifact that may belong to another clone attempt.
    #[instrument(skip(self), fields(from = from, to = to))]
    pub async fn copy_if_not_exists(&self, from: &str, to: &str, namespace: &str) -> Result<()> {
        let start = std::time::Instant::now();
        let from_path = Path::parse(from)?;
        let to_path = Path::parse(to)?;
        self.inner
            .copy_if_not_exists(&from_path, &to_path)
            .await
            .map_err(|e| {
                crate::metrics::S3_ERRORS_TOTAL
                    .with_label_values(&["copy"])
                    .inc();
                ZeppelinError::Storage(e)
            })?;
        let elapsed = start.elapsed();
        debug!(elapsed_ms = elapsed.as_millis(), "s3 copy_if_not_exists");
        crate::metrics::S3_OPERATION_DURATION
            .with_label_values(&["copy"])
            .observe(elapsed.as_secs_f64());
        Ok(())
    }

    /// Deletes one object key from the configured backend.
    ///
    /// Callers must establish that deletion is safe. This low-level method does
    /// not check manifest reachability, GC horizons, snapshots, or namespace
    /// tombstones.
    ///
    /// # Parameters
    ///
    /// - `key`: Exact object key to remove.
    ///
    /// # Returns
    ///
    /// `Ok(())` after the backend accepts the deletion.
    ///
    /// # Errors
    ///
    /// A backend that reports an absent object produces
    /// [`ZeppelinError::NotFound`]. Invalid keys and other delete failures remain
    /// errors. Different backends may treat deletion of an absent key differently.
    ///
    /// # Side Effects
    ///
    /// Performs one logical DELETE and records delete latency on success.
    ///
    /// # Consistency
    ///
    /// Removing an object still referenced by an authoritative manifest violates
    /// Zeppelin's artifact invariant. Reachability-aware callers must update or
    /// inspect authoritative state before invoking this primitive.
    ///
    /// # Examples
    ///
    /// Garbage collection may delete an old segment only after proving no
    /// retained manifest or snapshot references it. Calling this method directly
    /// on the active segment would remove authoritative data and is unsafe.
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

    /// Deletes one bounded batch of exact object keys.
    ///
    /// Every key is parsed and the batch is checked for duplicates before the
    /// backend sees any input. The 1,000-key limit matches one S3 DeleteObjects
    /// request in the pinned object-store backend; callers with more work must
    /// split it into explicit batches so retry and progress semantics remain
    /// visible.
    ///
    /// The result is successful only after the backend returns one successful
    /// or matching `NotFound` outcome for every unique input key, in input
    /// order. Any other per-key or request error is returned after the stream is
    /// drained. A short, long, reordered, or wrong-key result stream is a
    /// storage error rather than silent partial completion.
    #[instrument(skip(self, keys), fields(count = keys.len()))]
    pub async fn delete_many(&self, keys: Vec<String>) -> Result<usize> {
        const MAX_DELETE_BATCH: usize = 1_000;

        if keys.len() > MAX_DELETE_BATCH {
            return Err(ZeppelinError::Validation(format!(
                "delete_many accepts at most {MAX_DELETE_BATCH} keys, got {}",
                keys.len()
            )));
        }

        let paths = keys
            .iter()
            .map(Path::parse)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let unique = paths.iter().cloned().collect::<BTreeSet<_>>();
        if unique.len() != paths.len() {
            return Err(ZeppelinError::Validation(
                "delete_many requires unique object keys".to_string(),
            ));
        }
        let expected = paths.len();
        if expected == 0 {
            return Ok(0);
        }

        let start = std::time::Instant::now();
        use futures::StreamExt;
        let input = futures::stream::iter(paths.iter().cloned().map(Ok)).boxed();
        let mut results = self.inner.delete_stream(input);
        let mut observed = 0usize;
        let mut first_error = None;
        while let Some(result) = results.next().await {
            let expected_path = paths.get(observed);
            match result {
                Ok(actual) if expected_path == Some(&actual) => {}
                Ok(actual) => {
                    first_error.get_or_insert_with(|| object_store::Error::Generic {
                        store: "ZeppelinStore",
                        source: Box::new(std::io::Error::other(format!(
                            "delete_many result {observed} returned unexpected key {actual}"
                        ))),
                    });
                }
                Err(object_store::Error::NotFound { path, .. })
                    if expected_path.is_some_and(|expected| expected.as_ref() == path) => {}
                Err(object_store::Error::NotFound { path, source }) => {
                    first_error.get_or_insert(object_store::Error::NotFound { path, source });
                }
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(_) => {}
            }
            if let Some(next_observed) = observed.checked_add(1) {
                observed = next_observed;
            } else if first_error.is_none() {
                first_error = Some(object_store::Error::Generic {
                    store: "ZeppelinStore",
                    source: Box::new(std::io::Error::other(
                        "delete_many result count overflowed usize",
                    )),
                });
            }
        }

        if observed != expected && first_error.is_none() {
            first_error = Some(object_store::Error::Generic {
                store: "ZeppelinStore",
                source: Box::new(std::io::Error::other(format!(
                    "delete_many returned {observed} results for {expected} keys"
                ))),
            });
        }

        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            count = expected,
            "s3 delete_many"
        );
        crate::metrics::S3_OPERATION_DURATION
            .with_label_values(&["delete_many"])
            .observe(elapsed.as_secs_f64());
        if let Some(error) = first_error {
            crate::metrics::S3_ERRORS_TOTAL
                .with_label_values(&["delete_many"])
                .inc();
            return Err(ZeppelinError::Storage(error));
        }

        Ok(expected)
    }

    /// Lists every object recursively beneath a non-empty prefix.
    ///
    /// The complete listing is materialized before return. Namespace discovery
    /// at the store root must use [`Self::list_common_prefixes`] instead, which
    /// avoids an unrestricted recursive scan.
    ///
    /// # Parameters
    ///
    /// - `prefix`: Non-empty object-key prefix. Prefix matching follows
    ///   `object_store` path-segment semantics rather than raw string matching.
    ///
    /// # Returns
    ///
    /// Every matching object key. The backend does not guarantee ordering, so
    /// callers must sort when deterministic order matters.
    ///
    /// # Errors
    ///
    /// Invalid prefixes and any paginated listing failure return an error without
    /// exposing a partial key vector.
    ///
    /// # Panics
    ///
    /// An empty prefix panics in every build because unrestricted recursive root
    /// listing is forbidden. Use [`Self::list_common_prefixes`] for root namespace
    /// discovery.
    ///
    /// # Side Effects
    ///
    /// Performs a recursive object-store LIST, which may require multiple remote
    /// pages, and records total listing latency on success.
    ///
    /// # Performance
    ///
    /// Memory use is linear in the number of matching objects because both
    /// backend metadata and returned key strings are collected in memory.
    ///
    /// # Examples
    ///
    /// Listing `acme/segments/` can return every immutable segment object below
    /// that path. Listing `""` is rejected; root namespace discovery should ask
    /// [`Self::list_common_prefixes`] for immediate children instead.
    #[instrument(skip(self), fields(prefix = prefix))]
    pub async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .list_prefix_meta_inner(prefix)
            .await?
            .into_iter()
            .map(|object| object.key)
            .collect())
    }

    /// Lists every object and its backend metadata beneath a non-empty prefix.
    ///
    /// This performs the same one logical recursive LIST as
    /// [`Self::list_prefix`] while preserving the object identity needed to
    /// validate disposable caches. Backend pagination may issue multiple remote
    /// requests. An absent ETag and backend version produces `version: None`;
    /// Zeppelin never invents a version token.
    #[instrument(skip(self), fields(prefix = prefix))]
    pub async fn list_prefix_meta(&self, prefix: &str) -> Result<Vec<ListedObject>> {
        self.list_prefix_meta_inner(prefix).await
    }

    async fn list_prefix_meta_inner(&self, prefix: &str) -> Result<Vec<ListedObject>> {
        assert!(
            !prefix.is_empty(),
            "recursive root listing must use list_common_prefixes"
        );

        let start = std::time::Instant::now();
        use futures::TryStreamExt;
        let path = Path::parse(prefix)?;
        let stream = self.inner.list(Some(&path));
        let objects: Vec<_> = stream.try_collect().await?;
        let objects = objects
            .into_iter()
            .map(|object| {
                let key = object.location.to_string();
                let size = u64::try_from(object.size).map_err(|_| {
                    ZeppelinError::Validation(format!(
                        "listed object {key} size does not fit in u64: {}",
                        object.size
                    ))
                })?;
                Ok(ListedObject {
                    key,
                    size,
                    last_modified: object.last_modified,
                    version: StorageVersion::from_parts(object.e_tag, object.version),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            count = objects.len(),
            "s3 list_prefix"
        );
        crate::metrics::S3_OPERATION_DURATION
            .with_label_values(&["list_prefix"])
            .observe(elapsed.as_secs_f64());
        Ok(objects)
    }

    /// Discovers unique immediate child prefixes without recursively returning all objects.
    ///
    /// In addition to backend-reported common prefixes, this method derives a
    /// child prefix from any returned object's first slash-delimited component.
    /// That normalization keeps root namespace discovery consistent across S3
    /// and local/test backends. Direct child objects without another slash do not
    /// themselves become prefixes.
    ///
    /// # Parameters
    ///
    /// - `prefix`: Parent object-key prefix. The empty string intentionally means
    ///   the store root and is valid for namespace discovery.
    ///
    /// # Returns
    ///
    /// Lexicographically sorted, duplicate-free child prefixes, normally ending
    /// in `/` because they represent key hierarchy rather than concrete objects.
    ///
    /// # Errors
    ///
    /// Returns an error if the prefix cannot be parsed or the delimiter listing
    /// fails. No partial prefix set is returned.
    ///
    /// # Side Effects
    ///
    /// Performs one logical delimiter LIST and records its latency on success.
    ///
    /// # Performance
    ///
    /// Stores unique prefixes in a `BTreeSet`, using `O(n log n)` insertion to
    /// provide deterministic order without a separate sort.
    ///
    /// # Examples
    ///
    /// Given `acme/meta.json`, `acme/segments/1`, and `beta/meta.json`, listing
    /// immediate prefixes at `""` returns `acme/` and `beta/`, not every object
    /// below those namespaces.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `let Some(remainder) = ... else { continue; }` makes the successful
    /// optional parse path explicit and skips malformed/unrelated entries.
    /// `BTreeSet` owns inserted strings and combines uniqueness with sorted
    /// iteration. Java's `TreeSet<String>` is a close data-structure analogy; C
    /// needs a tree/set implementation plus explicit string ownership.
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

    /// Checks object existence without downloading its body.
    ///
    /// # Parameters
    ///
    /// - `key`: Exact object key to test.
    ///
    /// # Returns
    ///
    /// `true` after a successful HEAD and `false` only when the backend reports
    /// `NotFound`.
    ///
    /// # Errors
    ///
    /// Invalid keys and all non-`NotFound` backend errors are returned. A timeout
    /// is not misreported as absence.
    ///
    /// # Side Effects
    ///
    /// Performs one metadata request, records existence-check latency, and
    /// increments the `exists` error counter for backend failures.
    ///
    /// # Consistency
    ///
    /// Existence does not imply manifest visibility. This method is useful for
    /// validation and cleanup, not for deciding which artifacts queries may read.
    ///
    /// # Examples
    ///
    /// A freshly uploaded but not yet published segment returns `true`; queries
    /// must still ignore it until the authoritative manifest references it.
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

    /// Loads object metadata without downloading the body.
    ///
    /// # Parameters
    ///
    /// - `key`: Exact object key whose metadata is required.
    ///
    /// # Returns
    ///
    /// Backend [`object_store::ObjectMeta`], including location, size, modified
    /// time, and available version identifiers.
    ///
    /// # Errors
    ///
    /// Missing objects become [`ZeppelinError::NotFound`]. Invalid keys and other
    /// backend failures remain errors; metadata is never synthesized.
    ///
    /// # Side Effects
    ///
    /// Performs one HEAD-style metadata request, records latency on success, and
    /// increments the HEAD error metric if the request fails.
    ///
    /// # Performance
    ///
    /// Avoids body transfer but still incurs an object-store roundtrip.
    ///
    /// # Examples
    ///
    /// Cache hydration can inspect a segment's size before scheduling a download.
    /// It receives metadata only; the segment bytes remain in object storage.
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

    /// Deletes every object recursively under a non-empty prefix.
    ///
    /// This convenience method runs [`Self::delete_prefix_paged`] with no
    /// excluded key and an unlimited time budget. It is intended for cleanup
    /// only after the caller has established that every matching object is safe
    /// to remove.
    ///
    /// # Parameters
    ///
    /// - `prefix`: Non-empty key prefix that bounds the deletion.
    ///
    /// # Returns
    ///
    /// Number of listed keys deleted or found already absent.
    ///
    /// # Errors
    ///
    /// Empty prefixes, listing failures, malformed keys, and delete failures are
    /// returned. Some matching objects may already be gone when an error occurs.
    ///
    /// # Panics
    ///
    /// In debug builds, an empty prefix triggers the guard in
    /// [`Self::delete_prefix_paged`].
    ///
    /// # Side Effects
    ///
    /// Lists and deletes all matching objects in bounded backend batches with no
    /// overall time budget.
    ///
    /// # Consistency
    ///
    /// This method does not perform reachability analysis. Namespace deletion and
    /// GC must preserve tombstones, manifests, snapshots, and safety horizons in
    /// their own orchestration before using it.
    ///
    /// # Examples
    ///
    /// A test cleanup may remove its random namespace prefix after the test has
    /// finished. Production namespace deletion uses the paged form when it must
    /// preserve `meta.json` as a tombstone during incremental cleanup.
    #[instrument(skip(self), fields(prefix = prefix))]
    pub async fn delete_prefix(&self, prefix: &str) -> Result<usize> {
        let outcome = self
            .delete_prefix_paged(prefix, None, Duration::MAX)
            .await?;
        Ok(outcome.deleted)
    }

    /// Deletes a bounded amount of a prefix without materializing its full key list.
    ///
    /// The listing is streamed, exact excluded keys are skipped, and selected
    /// objects are deleted in chunks of at most 1,000. Configured S3 stores and
    /// the dedicated perf harness use [`Self::delete_many`]; generic decorated
    /// stores retain the original 32-way per-key path so fault semantics do not
    /// silently change. The time budget is checked after each full chunk, so it
    /// is a coarse stopping signal rather than a deadline: a pass can exceed the
    /// budget while finishing a chunk. A final partial chunk is deleted only
    /// when the listing reaches its end.
    ///
    /// ```text
    /// stream prefix listing
    ///          |
    ///          +-- exact excluded key --> keep it
    ///          |
    ///          v
    /// collect 1,000 keys --> configured S3: one delete_many batch
    ///                    \-> generic decorator: 32-way per-key deletes
    ///          |                         |
    ///          | budget exhausted       | delete error
    ///          v                         v
    /// complete = false             return error; partial work may exist
    ///          |
    ///          | listing exhausted
    ///          v
    /// delete final partial chunk --> complete = true
    /// ```
    ///
    /// # Parameters
    ///
    /// - `prefix`: Non-empty key prefix that bounds both listing and deletion.
    /// - `exclude`: Optional exact key to preserve. Descendants or similarly
    ///   named keys are not excluded.
    /// - `budget`: Approximate elapsed-time budget checked between full chunks.
    ///   `Duration::MAX` means effectively unbounded; a zero budget can still
    ///   delete a full chunk, and a listing shorter than one chunk is completed.
    ///
    /// # Returns
    ///
    /// [`DeletePrefixOutcome`] with the accepted delete count and whether the
    /// pass observed the end of the listing. `complete = false` is conservative:
    /// a later pass must list current state again even if the previous chunk
    /// happened to contain the final object.
    ///
    /// # Errors
    ///
    /// Empty prefixes are validation errors. Listing, key parsing, and delete
    /// failures return an error, possibly after other objects were deleted. A
    /// key that disappeared between listing and deletion is treated as already
    /// deleted and counted.
    ///
    /// # Panics
    ///
    /// In debug builds, an empty prefix triggers `debug_assert!` before the
    /// release-mode validation error path.
    ///
    /// # Side Effects
    ///
    /// Performs a recursive LIST and zero or more DELETEs. Namespace deletion
    /// passes `meta.json` as `exclude` so the authoritative tombstone remains
    /// present while other data is removed.
    ///
    /// # Consistency
    ///
    /// The listing is not a transaction or snapshot. Concurrent creation may
    /// require another pass. Concurrent disappearance is idempotent because
    /// `NotFound` counts as successful cleanup. The method never removes the
    /// exact excluded key.
    ///
    /// # Performance
    ///
    /// Memory is bounded to one 1,000-key chunk plus backend listing buffers.
    /// Chunks run sequentially so the budget can be checked between them. The
    /// configured S3 adapter maps each chunk to one DeleteObjects request.
    ///
    /// # Examples
    ///
    /// A namespace deletion pass lists `acme/`, skips `acme/meta.json`, deletes
    /// 1,000 data objects, and then notices its budget has elapsed. It returns
    /// `{ deleted: 1000, complete: false }`; the tombstone remains, and a later
    /// pass resumes by listing `acme/` again.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `Option<&str>` expresses "no excluded key" without a nullable pointer.
    /// `std::mem::take(&mut chunk)` moves the full vector into async deletion and
    /// leaves a new empty vector in its place, avoiding a clone of 1,000 strings.
    /// Java would typically swap list references; C would transfer the buffer and
    /// manually reset the original owner. Rust makes the moved vector unusable in
    /// the listing loop, preventing double free or accidental reuse.
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
                deleted += self.delete_prefix_chunk(std::mem::take(&mut chunk)).await?;
                if start.elapsed() >= budget {
                    complete = false;
                    break;
                }
            }
        }

        if complete && !chunk.is_empty() {
            deleted += self.delete_prefix_chunk(chunk).await?;
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

    async fn delete_prefix_chunk(&self, keys: Vec<String>) -> Result<usize> {
        match self.prefix_delete_mode {
            PrefixDeleteMode::NativeBatch => self.delete_many(keys).await,
            PrefixDeleteMode::LegacyPerKeyUnordered32 => self.delete_key_chunk(keys).await,
        }
    }

    /// Preserves the established behavior of generic fault-instrumented stores.
    ///
    /// Instrumentation layers created through [`Self::new`] may attach
    /// independent semantics to every per-key DELETE. They therefore retain the
    /// original 32-way unordered scheduling and first-error cancellation path.
    /// Configured S3 and the dedicated perf harness instead select
    /// [`PrefixDeleteMode::NativeBatch`].
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
                    Err(error) => return Err(ZeppelinError::Storage(error)),
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

/// Extracts a probe host and port from the endpoint forms accepted at startup.
///
/// Explicit `http://` and `https://` schemes select ports 80 and 443 when no
/// port is present; an endpoint without a scheme defaults to 443. The helper
/// ignores a path and user-info prefix and supports bracketed IPv6 literals.
/// Raw IPv6 literals must be bracketed so their colons are not mistaken for the
/// host/port separator.
///
/// # Parameters
///
/// - `endpoint`: Borrowed configured endpoint text.
///
/// # Returns
///
/// An owned host string and resolved port suitable for the TCP startup probe.
///
/// # Errors
///
/// Returns [`ZeppelinError::Config`] when the authority is empty, a bracketed
/// IPv6 host has no closing bracket, a host is empty, or a port is not a valid
/// `u16`.
///
/// # Examples
///
/// `http://127.0.0.1:9000/path` becomes `(127.0.0.1, 9000)`,
/// `https://minio.internal` becomes `(minio.internal, 443)`, and
/// `http://[::1]:9000` becomes `(::1, 9000)`.
///
/// # Rust Notes for Java/C Engineers
///
/// The function returns an owned `String` because temporary slices such as
/// `without_scheme` borrow the input and cannot outlive it. `strip_prefix`,
/// `split_once`, and `transpose` combine `Option` and `Result` without nullable
/// pointers or out-parameters: absence can choose a default, while invalid text
/// still returns an error.
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

/// Parses a decimal endpoint port into the socket API's unsigned 16-bit type.
///
/// # Parameters
///
/// - `port`: Borrowed decimal text after the endpoint's final colon.
///
/// # Returns
///
/// A port in `0..=65535`. This helper validates numeric width but does not reject
/// port zero.
///
/// # Errors
///
/// Returns [`ZeppelinError::Config`] with the original text and parse error when
/// the value is non-numeric, signed, or outside the `u16` range.
///
/// # Examples
///
/// `"9000"` returns `9000`; `"70000"` and `"minio"` return configuration
/// errors before any socket is opened.
fn parse_endpoint_port(port: &str) -> Result<u16> {
    port.parse::<u16>()
        .map_err(|error| ZeppelinError::Config(format!("invalid S3 endpoint port {port}: {error}")))
}
