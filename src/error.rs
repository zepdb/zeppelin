//! Central error vocabulary and client-safety policy for Zeppelin.
//!
//! Domain, storage, indexing, cache, WAL, and query code return
//! [`crate::error::ZeppelinError`] through the module's
//! [`crate::error::Result`] alias. The HTTP layer then uses
//! [`crate::error::ZeppelinError::status_code`],
//! [`crate::error::ZeppelinError::error_code`],
//! [`crate::error::ZeppelinError::retryable`],
//! [`crate::error::ZeppelinError::retry_after_secs`], and
//! [`crate::error::ZeppelinError::client_message`] to build one stable response
//! envelope. This keeps low-level failures descriptive in logs without exposing
//! S3 keys, endpoints, lease-holder IDs, or fencing tokens to clients.
//!
//! ```text
//! storage / WAL / index / query operation
//!                  |
//!                  | Result<T, ZeppelinError>
//!                  v
//!        full internal error (log this)
//!                  |
//!       +----------+-----------+----------------+
//!       |                      |                |
//!       v                      v                v
//! HTTP status + stable code  retry policy  sanitized client message
//!       |                      |                |
//!       +----------------------+----------------+
//!                              |
//!                              v
//!                    canonical HTTP envelope
//! ```
//!
//! The most important classification invariant is that an S3 object missing
//! beneath an existing manifest is [`crate::error::ZeppelinError::NotFound`], a
//! server-side integrity failure. It is not interchangeable with
//! [`crate::error::ZeppelinError::NamespaceNotFound`] or
//! [`crate::error::ZeppelinError::ManifestNotFound`], which describe a
//! client-visible missing namespace. Likewise, publication conflicts and stale
//! fencing tokens are explicit retryable conflicts; they must never be silently
//! treated as a successful write.
//!
//! ## Reading map
//!
//! 1. Read [`crate::error::ZeppelinError`] by subsystem to see which failures
//!    cross module boundaries.
//! 2. Read the `From` implementation and [`crate::error::Result`] alias to
//!    understand error propagation with `?`.
//! 3. Read [`crate::error::ZeppelinError::status_code`] and
//!    [`crate::error::ZeppelinError::error_code`] for the public classification
//!    contract.
//! 4. Finish with [`crate::error::ZeppelinError::retryable`],
//!    [`crate::error::ZeppelinError::retry_after_secs`], and
//!    [`crate::error::ZeppelinError::client_message`] for client behavior and
//!    redaction.
//!
//! ## Rust concepts used here
//!
//! [`thiserror::Error`] is a derive macro that generates Rust's standard
//! [`std::error::Error`] and [`std::fmt::Display`] implementations from the
//! `#[error(...)]` annotations. `#[from]` also generates conversions for nested
//! source errors, allowing `?` to propagate them as
//! [`crate::error::ZeppelinError`] without boilerplate. This is closest to a
//! checked Java exception hierarchy, but the error is a returned enum value
//! rather than an exception that unwinds the stack. In C, it replaces a numeric
//! status plus separate out-of-band error storage with one tagged value that
//! carries the relevant context.
//!
//! Exhaustive `match` expressions make the compiler part of API maintenance.
//! In particular, adding a new error variant cannot compile until
//! [`crate::error::ZeppelinError::error_code`] assigns it a stable code.

use thiserror::Error;

/// A typed failure produced by a Zeppelin operation.
///
/// Variants preserve enough context for structured logs and programmatic
/// handling. That internal detail is not automatically safe for an HTTP body;
/// callers serving external requests must use [`ZeppelinError::client_message`]
/// rather than [`std::string::ToString::to_string`].
///
/// # Example
///
/// A missing S3 cluster object becomes [`ZeppelinError::NotFound`] and maps to
/// an internal error, while a request for a namespace that does not exist
/// becomes [`ZeppelinError::NamespaceNotFound`] and maps to a client 404. A
/// manifest compare-and-swap loss becomes [`ZeppelinError::ManifestConflict`]
/// so the caller can reload authoritative state and retry.
#[derive(Error, Debug)]
pub enum ZeppelinError {
    // Storage errors
    /// An object was not found at the given S3 key.
    #[error("object not found: {key}")]
    NotFound {
        /// The S3 object key that was not found.
        key: String,
    },

    /// An error from the underlying object_store layer.
    #[error("storage error: {0}")]
    Storage(#[from] object_store::Error),

    /// An invalid or unparseable object_store path.
    #[error("storage path error: {0}")]
    StoragePath(#[from] object_store::path::Error),

    /// A compare-and-swap was required but the backend reported no version token.
    ///
    /// Conditional writes are keyed on a backend identity — an ETag on S3,
    /// MinIO and Azure, an object generation on GCS. A read that returns none
    /// leaves the caller nothing to compare against, and substituting an empty
    /// precondition would turn the CAS into an unconditional overwrite. That is
    /// a backend contract violation, so it is raised rather than degraded.
    #[error(
        "storage backend returned no usable version token for {key} (backend requires one of etag/generation for CAS)"
    )]
    MissingVersionToken {
        /// Object key whose read carried no backend identity.
        key: String,
    },

    // Serialization errors
    /// A JSON serialization or deserialization failure.
    #[error("json serialization error: {0}")]
    Json(#[from] serde_json::Error),

    /// A bincode serialization or deserialization failure.
    #[error("bincode serialization error: {0}")]
    Bincode(String),

    /// A generic serialization error (e.g. MessagePack).
    #[error("serialization error: {0}")]
    Serialization(String),

    /// A listed key under a reserved storage-control prefix has invalid grammar.
    #[error("malformed {family} control key {key}: {reason}")]
    MalformedControlKey {
        /// Reserved key family whose canonical grammar was violated.
        family: &'static str,
        /// Exact invalid object-store key.
        key: String,
        /// Canonical parser's diagnostic.
        reason: String,
    },

    // WAL errors
    /// Data integrity failure: stored checksum does not match computed checksum.
    #[error("checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch {
        /// The checksum value that was expected.
        expected: u64,
        /// The checksum value that was actually computed.
        actual: u64,
    },

    /// No manifest exists yet for the given namespace.
    #[error("manifest not found for namespace: {namespace}")]
    ManifestNotFound {
        /// The namespace whose manifest is missing.
        namespace: String,
    },

    /// A concurrent write caused a manifest CAS conflict.
    #[error("manifest conflict (concurrent write) for namespace: {namespace}")]
    ManifestConflict {
        /// The namespace where the conflict occurred.
        namespace: String,
    },

    // Lease errors
    /// Another writer currently holds the lease on this namespace.
    #[error("lease held on namespace {namespace} by {holder}")]
    LeaseHeld {
        /// The namespace with the held lease.
        namespace: String,
        /// The identifier of the current lease holder.
        holder: String,
    },

    /// The caller's lease has expired and is no longer valid.
    #[error("lease expired for namespace {namespace}")]
    LeaseExpired {
        /// The namespace whose lease expired.
        namespace: String,
    },

    /// The caller's fencing token is behind the manifest's token (zombie writer).
    #[error("fencing token stale for namespace {namespace}: ours={our_token}, manifest={manifest_token}")]
    FencingTokenStale {
        /// The namespace where the stale token was detected.
        namespace: String,
        /// The caller's outdated fencing token.
        our_token: u64,
        /// The current fencing token stored in the manifest.
        manifest_token: u64,
    },

    // Namespace errors
    /// The requested namespace does not exist.
    #[error("namespace not found: {namespace}")]
    NamespaceNotFound {
        /// The name of the missing namespace.
        namespace: String,
    },

    /// A namespace with this name already exists.
    #[error("namespace already exists: {namespace}")]
    NamespaceAlreadyExists {
        /// The name of the already-existing namespace.
        namespace: String,
    },

    /// A named snapshot already exists for a different manifest generation.
    #[error(
        "snapshot {name} already exists on namespace {namespace}: existing generation {existing_generation}, requested generation {requested_generation}"
    )]
    SnapshotAlreadyExists {
        /// Namespace containing the snapshot.
        namespace: String,
        /// Caller-supplied snapshot name.
        name: String,
        /// Generation already pinned by this name.
        existing_generation: u64,
        /// Generation the caller attempted to pin.
        requested_generation: u64,
    },

    /// The requested named snapshot does not exist.
    #[error("snapshot not found on namespace {namespace}: {name}")]
    SnapshotNotFound {
        /// Namespace searched for the snapshot.
        namespace: String,
        /// Snapshot name that was absent.
        name: String,
    },

    /// A requested historical manifest generation is no longer retained.
    #[error("point-in-time {target} is no longer retained for namespace {namespace}")]
    PointInTimeNotRetained {
        /// Namespace searched for historical state.
        namespace: String,
        /// Caller-supplied generation, timestamp, or snapshot reference.
        target: String,
    },

    /// The namespace exists but is in the middle of deletion.
    #[error("namespace is being deleted: {namespace}")]
    NamespaceDeleting {
        /// The name of the deleting namespace.
        namespace: String,
    },

    /// Namespace deletion reached final verification with non-tombstone keys left.
    #[error("namespace delete incomplete for {namespace}: {remaining_keys} keys remain")]
    NamespaceDeleteIncomplete {
        /// The namespace whose prefix still contains data.
        namespace: String,
        /// Number of non-tombstone keys still present.
        remaining_keys: usize,
    },

    // Index errors
    /// A vector indexing operation failed.
    #[error("index error: {0}")]
    Index(String),

    /// A policy-scope retrieval artifact is malformed or failed construction.
    #[error("retrieval scope error: {0}")]
    RetrievalScope(#[from] crate::retrieval_scope::RetrievalScopeError),

    /// A persisted resident coarse-sketch artifact is corrupt or inconsistent.
    #[error("coarse sketch error: {0}")]
    CoarseSketch(String),

    /// A RaBitQ rotation, encoding, or scoring operation failed.
    #[error("RaBitQ error: {0}")]
    Rabitq(String),

    /// A two-bit RaBitQ cluster payload is invalid or inconsistent.
    #[error("RaBitQ cluster payload error: {0}")]
    Rq(String),

    /// A segment membership artifact operation failed.
    #[error("membership artifact error: {0}")]
    Membership(String),

    /// K-means clustering did not converge within the iteration limit.
    #[error("k-means failed to converge after {iterations} iterations")]
    KMeansConvergence {
        /// The number of iterations attempted before giving up.
        iterations: usize,
    },

    // Validation errors
    /// Vector dimensions do not match the namespace's configured dimensionality.
    #[error("dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch {
        /// The expected number of dimensions.
        expected: usize,
        /// The actual number of dimensions provided.
        actual: usize,
    },

    /// A requested vector ID does not exist in the namespace.
    #[error("vector not found in namespace {namespace}: {id}")]
    VectorNotFound {
        /// Namespace searched for the vector.
        namespace: String,
        /// Vector ID that was absent or tombstoned.
        id: String,
    },

    /// A request failed input validation.
    #[error("validation error: {0}")]
    Validation(String),

    /// A logical request payload exceeded a configured limit.
    #[error("{resource} size {actual} exceeds maximum of {limit}")]
    PayloadTooLarge {
        /// Logical resource that exceeded the cap.
        resource: &'static str,
        /// Submitted logical size.
        actual: usize,
        /// Configured maximum.
        limit: usize,
    },

    /// The request selected a recognized feature that is reserved but not implemented yet.
    #[error("not implemented: {feature}")]
    NotImplemented {
        /// Stable feature name or short description.
        feature: &'static str,
    },

    // Config errors
    /// An invalid or missing configuration value.
    #[error("config error: {0}")]
    Config(String),

    /// Authentication, authorization, or security-state construction failed.
    #[error("security error: {0}")]
    Security(#[from] crate::security::SecurityError),

    /// Namespace-branching integrity or lifecycle validation failed.
    #[error("branch error: {0}")]
    Branch(Box<crate::namespace::branching::BranchError>),

    /// Durable security-audit delivery or lifecycle management failed.
    #[error("audit sink error: {0}")]
    AuditSink(#[from] crate::security::AuditSinkError),

    /// Request-spawned authoritative work failed to retire cleanly.
    #[error("server task lifecycle error: {0}")]
    ServerTaskSupervisor(#[from] crate::server::ServerTaskSupervisorError),

    /// Leased-compaction heartbeat admission or retirement failed.
    #[error("compaction lifecycle error: {0}")]
    CompactionLifecycle(#[from] crate::compaction::background::CompactionLifecycleError),

    /// Signed-license parsing or verification failed during composition.
    #[error("license error: {0}")]
    License(#[from] crate::security::LicenseError),

    // IO errors
    /// A local filesystem I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    // Cache errors
    /// A local disk or memory cache operation failed.
    #[error("cache error: {0}")]
    Cache(String),

    /// Warm-set hydration was requested while disabled in configuration.
    #[error("hydration is disabled by config")]
    HydrationDisabled,

    // Full-text search errors
    /// A full-text search indexing or query error.
    #[error("full-text search error: {0}")]
    FullTextSearch(String),

    /// The requested FTS field is not configured on the namespace.
    #[error("FTS field not configured on namespace {namespace}: {field}")]
    FtsFieldNotConfigured {
        /// The namespace that lacks the FTS field.
        namespace: String,
        /// The FTS field name that is not configured.
        field: String,
    },

    /// A server-side index required to answer the query is not available yet.
    #[error("index unavailable: {0}")]
    IndexUnavailable(String),

    /// The query concurrency semaphore is exhausted; the caller should retry.
    #[error("query concurrency limit reached, try again later")]
    QueryConcurrencyExhausted,

    /// Per-IP rate limit exceeded.
    #[error("rate limit exceeded, retry after {retry_after_secs}s")]
    RateLimitExceeded {
        /// Seconds until the next token becomes available.
        retry_after_secs: u64,
    },
}

impl From<crate::namespace::branching::BranchError> for ZeppelinError {
    fn from(error: crate::namespace::branching::BranchError) -> Self {
        Self::Branch(Box::new(error))
    }
}

impl From<Box<bincode::ErrorKind>> for ZeppelinError {
    /// Converts bincode's boxed error representation into Zeppelin's error enum.
    ///
    /// The conversion preserves the diagnostic text but deliberately erases
    /// bincode's concrete error kind so callers handle it through the common
    /// [`ZeppelinError::Bincode`] category.
    ///
    /// # Parameters
    ///
    /// - `e`: Owned bincode error returned while encoding or decoding a binary
    ///   artifact.
    ///
    /// # Returns
    ///
    /// Returns an owned [`ZeppelinError::Bincode`] containing the source error's
    /// human-readable text.
    ///
    /// # Example
    ///
    /// When a decoder returns a boxed `bincode::ErrorKind`, the `?` operator can
    /// invoke this conversion and return a `ZeppelinError` from the enclosing
    /// function.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `From<Source> for Target` is a compile-time conversion protocol. The `?`
    /// operator uses it to convert an incompatible error before returning. It is
    /// similar in purpose to catching a Java exception and wrapping it, or
    /// translating one C error-code domain into another, but the compiler checks
    /// that the conversion exists. Ownership of the box moves into this method
    /// and its allocation is released after the message is copied into a new
    /// [`String`].
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        ZeppelinError::Bincode(e.to_string())
    }
}

impl From<crate::index::quantization::rabitq::RabitqError> for ZeppelinError {
    fn from(error: crate::index::quantization::rabitq::RabitqError) -> Self {
        Self::Rabitq(error.to_string())
    }
}

impl From<crate::index::quantization::rq::RqError> for ZeppelinError {
    fn from(error: crate::index::quantization::rq::RqError) -> Self {
        Self::Rq(error.to_string())
    }
}

/// The standard return shape for fallible Zeppelin operations.
///
/// `Ok(T)` carries a successful value and `Err(ZeppelinError)` carries one
/// typed failure. The alias keeps signatures short without introducing a new
/// runtime wrapper or changing the layout of [`std::result::Result`].
///
/// # Example
///
/// A function returning `Result<Manifest>` either yields an authoritative
/// manifest or an explicit storage, decoding, or domain error; it never needs a
/// sentinel manifest value.
///
/// # Rust Notes for Java/C Engineers
///
/// This resembles a Java method that declares a closed family of failures, or a
/// C function returning a status alongside an output value, but success and
/// failure are mutually exclusive enum variants. Callers must inspect or
/// propagate the result before accessing `T`.
pub type Result<T> = std::result::Result<T, ZeppelinError>;

impl ZeppelinError {
    /// Classifies this error as an HTTP status code for the canonical API envelope.
    ///
    /// Note the deliberate split of the S3-level [`ZeppelinError::NotFound`]: an object
    /// key miss below the namespace layer (a segment/cluster/fragment that the
    /// manifest references but S3 can't return) is NOT a client-facing 404 —
    /// it's a server-side data-integrity failure (500). Only *namespace*-level
    /// misses such as [`ZeppelinError::NamespaceNotFound`] are true 404s.
    ///
    /// # Returns
    ///
    /// Returns the numeric status consumed by the Axum response layer. Unknown
    /// future variants fall into the internal-server-error category until they
    /// receive a more specific mapping.
    ///
    /// # Example
    ///
    /// A dimension mismatch maps to `400`; a manifest CAS conflict maps to
    /// `409`; and a manifest-referenced S3 key that cannot be fetched maps to
    /// `500`, not `404`.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The method borrows `&self`, so classification neither consumes nor
    /// mutates the error. Java would pass an object reference; C would pass a
    /// pointer to a tagged error struct. Rust additionally guarantees the
    /// borrowed error stays valid for this call and cannot be mutated through
    /// this shared reference.
    pub fn status_code(&self) -> u16 {
        match self {
            ZeppelinError::NamespaceNotFound { .. }
            | ZeppelinError::SnapshotNotFound { .. }
            | ZeppelinError::ManifestNotFound { .. }
            | ZeppelinError::VectorNotFound { .. } => 404,

            ZeppelinError::NamespaceDeleting { .. } => 410,

            ZeppelinError::PointInTimeNotRetained { .. } => 410,

            ZeppelinError::NamespaceAlreadyExists { .. }
            | ZeppelinError::SnapshotAlreadyExists { .. }
            | ZeppelinError::ManifestConflict { .. }
            | ZeppelinError::LeaseHeld { .. }
            | ZeppelinError::LeaseExpired { .. }
            | ZeppelinError::FencingTokenStale { .. }
            | ZeppelinError::HydrationDisabled => 409,

            ZeppelinError::DimensionMismatch { .. }
            | ZeppelinError::Validation(_)
            | ZeppelinError::FtsFieldNotConfigured { .. } => 400,

            ZeppelinError::PayloadTooLarge { .. } => 413,

            ZeppelinError::NotImplemented { .. } => 501,

            ZeppelinError::Branch(error)
                if matches!(
                    error.as_ref(),
                    crate::namespace::branching::BranchError::TargetAlreadyExists { .. }
                        | crate::namespace::branching::BranchError::NamespaceHasLiveBranches { .. }
                        | crate::namespace::branching::BranchError::BranchHasLiveChildren { .. }
                        | crate::namespace::branching::BranchError::CancellationInProgress { .. }
                ) =>
            {
                409
            }

            ZeppelinError::IndexUnavailable(_) | ZeppelinError::QueryConcurrencyExhausted => 503,

            ZeppelinError::RateLimitExceeded { .. } => 429,

            ZeppelinError::Security(error) => error.status_code(),

            // Internal S3 key miss = data-integrity failure, not a 404.
            ZeppelinError::NotFound { .. } => 500,

            _ => 500,
        }
    }

    /// Returns the stable, machine-readable code for this error variant.
    ///
    /// Every variant maps to exactly one stable code. Existing domain codes use
    /// `SCREAMING_SNAKE_CASE`; security-envelope codes use the plan's canonical
    /// `snake_case` vocabulary. Clients should branch on this code rather than
    /// parsing the human message or assuming every condition with the same HTTP
    /// status has the same meaning.
    ///
    /// # Returns
    ///
    /// Returns a borrowed string literal with process-long (`'static`) lifetime;
    /// classification performs no allocation.
    ///
    /// # Example
    ///
    /// [`ZeppelinError::ManifestConflict`] and lease conflicts all return
    /// `"CONFLICT_RETRY"`, while [`ZeppelinError::DimensionMismatch`] returns
    /// `"DIMENSION_MISMATCH"`.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The match intentionally has no wildcard arm. This is stronger than a
    /// Java `switch` with `default`, or a C `switch` that silently falls through
    /// to a generic code: adding an enum variant produces a compile error here
    /// until the public contract is updated. Returning `&'static str` is like a
    /// pointer to immutable static C storage and does not allocate a Java-style
    /// string object per call.
    pub fn error_code(&self) -> &'static str {
        match self {
            ZeppelinError::NotFound { .. } => "INTERNAL_DATA_MISSING",
            ZeppelinError::Storage(_) => "STORAGE_ERROR",
            ZeppelinError::StoragePath(_) => "STORAGE_ERROR",
            ZeppelinError::MissingVersionToken { .. } => "STORAGE_ERROR",
            ZeppelinError::Json(_) => "INTERNAL_ERROR",
            ZeppelinError::Bincode(_) => "INTERNAL_ERROR",
            ZeppelinError::Serialization(_) => "INTERNAL_ERROR",
            ZeppelinError::MalformedControlKey { .. } => "DATA_CORRUPTION",
            ZeppelinError::ChecksumMismatch { .. } => "DATA_CORRUPTION",
            ZeppelinError::ManifestNotFound { .. } => "NAMESPACE_NOT_FOUND",
            ZeppelinError::ManifestConflict { .. } => "CONFLICT_RETRY",
            ZeppelinError::LeaseHeld { .. } => "CONFLICT_RETRY",
            ZeppelinError::LeaseExpired { .. } => "CONFLICT_RETRY",
            ZeppelinError::FencingTokenStale { .. } => "CONFLICT_RETRY",
            ZeppelinError::NamespaceNotFound { .. } => "NAMESPACE_NOT_FOUND",
            ZeppelinError::NamespaceAlreadyExists { .. } => "NAMESPACE_ALREADY_EXISTS",
            ZeppelinError::SnapshotAlreadyExists { .. } => "SNAPSHOT_ALREADY_EXISTS",
            ZeppelinError::SnapshotNotFound { .. } => "SNAPSHOT_NOT_FOUND",
            ZeppelinError::PointInTimeNotRetained { .. } => "POINT_IN_TIME_NOT_RETAINED",
            ZeppelinError::NamespaceDeleting { .. } => "NAMESPACE_DELETING",
            ZeppelinError::NamespaceDeleteIncomplete { .. } => "INTERNAL_ERROR",
            ZeppelinError::Index(_) => "INTERNAL_ERROR",
            ZeppelinError::RetrievalScope(_) => "INTERNAL_ERROR",
            ZeppelinError::CoarseSketch(_) => "INTERNAL_ERROR",
            ZeppelinError::Rabitq(_) => "INTERNAL_ERROR",
            ZeppelinError::Rq(_) => "INTERNAL_ERROR",
            ZeppelinError::Membership(_) => "INTERNAL_ERROR",
            ZeppelinError::KMeansConvergence { .. } => "INTERNAL_ERROR",
            ZeppelinError::DimensionMismatch { .. } => "DIMENSION_MISMATCH",
            ZeppelinError::VectorNotFound { .. } => "VECTOR_NOT_FOUND",
            ZeppelinError::Validation(_) => "VALIDATION_ERROR",
            ZeppelinError::PayloadTooLarge { .. } => "PAYLOAD_TOO_LARGE",
            ZeppelinError::NotImplemented { .. } => "NOT_IMPLEMENTED",
            ZeppelinError::Config(_) => "INTERNAL_ERROR",
            ZeppelinError::Security(error) => error.code(),
            ZeppelinError::Branch(error) => match error.as_ref() {
                crate::namespace::branching::BranchError::TargetAlreadyExists { .. } => {
                    "branch_target_exists"
                }
                crate::namespace::branching::BranchError::NamespaceHasLiveBranches { .. } => {
                    "namespace_has_live_branches"
                }
                crate::namespace::branching::BranchError::BranchHasLiveChildren { .. } => {
                    "branch_has_live_children"
                }
                crate::namespace::branching::BranchError::BranchIntegrity => {
                    "branch_integrity_error"
                }
                crate::namespace::branching::BranchError::CancellationInProgress { .. } => {
                    "branch_intent_mismatch"
                }
                _ => "INTERNAL_ERROR",
            },
            ZeppelinError::AuditSink(_) => "INTERNAL_ERROR",
            ZeppelinError::ServerTaskSupervisor(_) => "INTERNAL_ERROR",
            ZeppelinError::CompactionLifecycle(_) => "INTERNAL_ERROR",
            ZeppelinError::License(_) => "INTERNAL_ERROR",
            ZeppelinError::Io(_) => "INTERNAL_ERROR",
            ZeppelinError::Cache(_) => "INTERNAL_ERROR",
            ZeppelinError::HydrationDisabled => "HYDRATION_DISABLED",
            ZeppelinError::FullTextSearch(_) => "INTERNAL_ERROR",
            ZeppelinError::FtsFieldNotConfigured { .. } => "FTS_FIELD_NOT_CONFIGURED",
            ZeppelinError::IndexUnavailable(_) => "INDEX_UNAVAILABLE",
            ZeppelinError::QueryConcurrencyExhausted => "CONCURRENCY_LIMIT",
            ZeppelinError::RateLimitExceeded { .. } => "RATE_LIMITED",
        }
    }

    /// Reports whether retrying the same logical request can reasonably succeed.
    ///
    /// Compare-and-swap or lease conflicts, temporary concurrency pressure,
    /// rate limits, and storage-layer failures are retryable. Validation and
    /// namespace-shape failures require a changed request or external action.
    /// This is advice to clients, not an automatic retry loop inside Zeppelin.
    ///
    /// # Returns
    ///
    /// Returns `true` when an unchanged request may succeed after delay or state
    /// refresh, and `false` when immediate repetition is not useful.
    ///
    /// # Example
    ///
    /// A client that receives [`ZeppelinError::ManifestConflict`] can reload and
    /// retry. Retrying [`ZeppelinError::DimensionMismatch`] with the same vector
    /// cannot succeed.
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            ZeppelinError::ManifestConflict { .. }
                | ZeppelinError::LeaseHeld { .. }
                | ZeppelinError::LeaseExpired { .. }
                | ZeppelinError::FencingTokenStale { .. }
                | ZeppelinError::QueryConcurrencyExhausted
                | ZeppelinError::RateLimitExceeded { .. }
                | ZeppelinError::Security(crate::security::SecurityError::PolicyConflict)
                | ZeppelinError::Security(
                    crate::security::SecurityError::PreservationConflict
                        | crate::security::SecurityError::PreservationStateUnavailable
                )
                | ZeppelinError::Storage(_)
        )
    }

    /// Returns the delay to advertise in an HTTP `Retry-After` header.
    ///
    /// Rate-limit errors carry their computed budget. Conflicts and concurrency
    /// pressure receive a one-second hint so clients do not hot-loop. Other
    /// variants do not prescribe a delay even when a broader policy may regard
    /// them as retryable.
    ///
    /// # Returns
    ///
    /// Returns `Some(seconds)` when the response should include a delay, or
    /// `None` when Zeppelin has no variant-specific delay to advertise.
    ///
    /// # Example
    ///
    /// A rate-limit error carrying `7` returns `Some(7)`; a manifest conflict
    /// returns `Some(1)`; a validation error returns `None`.
    pub fn retry_after_secs(&self) -> Option<u64> {
        match self {
            ZeppelinError::RateLimitExceeded { retry_after_secs } => Some(*retry_after_secs),
            ZeppelinError::QueryConcurrencyExhausted
            | ZeppelinError::ManifestConflict { .. }
            | ZeppelinError::LeaseHeld { .. }
            | ZeppelinError::LeaseExpired { .. }
            | ZeppelinError::FencingTokenStale { .. }
            | ZeppelinError::Security(
                crate::security::SecurityError::PolicyConflict
                | crate::security::SecurityError::PreservationConflict
                | crate::security::SecurityError::PreservationStateUnavailable,
            ) => Some(1),
            _ => None,
        }
    }

    /// Builds the human-readable error message that is safe to return to a client.
    ///
    /// S3 keys, bucket names, endpoints, fencing-token values, and lease-holder
    /// IDs belong only in structured server logs. Variants whose generated
    /// [`std::fmt::Display`] text contains internal detail are replaced with a
    /// generic message; variants carrying only safe caller or schema context can
    /// reuse their display text.
    ///
    /// # Returns
    ///
    /// Returns an owned, sanitized [`String`] suitable for the canonical HTTP
    /// error envelope. Each call allocates a message so the result can outlive
    /// the borrowed error.
    ///
    /// # Example
    ///
    /// If S3 reports that `secret-bucket/ns/segments/7` is missing, the server
    /// log retains that path, but this method returns a generic internal-data
    /// message. A stale fencing-token message may include the safe namespace
    /// name but never the old or current numeric token.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Pattern matching can borrow selected fields such as `namespace` without
    /// moving them out of `self`. The returned `String` owns its buffer, similar
    /// to constructing a new Java `String`; in C the caller and callee would need
    /// an explicit allocation and ownership convention. Rust ties cleanup to the
    /// returned value's lifetime through RAII.
    pub fn client_message(&self) -> String {
        match self {
            // Internal detail — return generic text, log the specifics elsewhere.
            ZeppelinError::NotFound { .. } => {
                "an internal data object is missing; this is a server-side error".to_string()
            }
            ZeppelinError::Storage(_) | ZeppelinError::StoragePath(_) => {
                "a transient storage error occurred; please retry".to_string()
            }
            // Backend contract violation: the operator's substrate cannot supply
            // the identity its own conditional writes require. Not retryable.
            ZeppelinError::MissingVersionToken { .. } => {
                "the storage backend did not supply a version token; this is a server-side \
                 configuration error"
                    .to_string()
            }
            ZeppelinError::Branch(error)
                if matches!(
                    error.as_ref(),
                    crate::namespace::branching::BranchError::TargetAlreadyExists { .. }
                        | crate::namespace::branching::BranchError::CancellationInProgress { .. }
                ) =>
            {
                error.to_string()
            }
            ZeppelinError::Json(_)
            | ZeppelinError::Bincode(_)
            | ZeppelinError::Serialization(_)
            | ZeppelinError::MalformedControlKey { .. }
            | ZeppelinError::Index(_)
            | ZeppelinError::RetrievalScope(_)
            | ZeppelinError::CoarseSketch(_)
            | ZeppelinError::Rabitq(_)
            | ZeppelinError::Rq(_)
            | ZeppelinError::Membership(_)
            | ZeppelinError::KMeansConvergence { .. }
            | ZeppelinError::Config(_)
            | ZeppelinError::Branch(_)
            | ZeppelinError::AuditSink(_)
            | ZeppelinError::ServerTaskSupervisor(_)
            | ZeppelinError::CompactionLifecycle(_)
            | ZeppelinError::License(_)
            | ZeppelinError::Io(_)
            | ZeppelinError::Cache(_)
            | ZeppelinError::FullTextSearch(_)
            | ZeppelinError::NamespaceDeleteIncomplete { .. } => {
                "an internal error occurred".to_string()
            }
            ZeppelinError::IndexUnavailable(_) => {
                "the requested FTS index is unavailable; contact the server operator".to_string()
            }
            ZeppelinError::Security(error) => error.client_message(),
            ZeppelinError::ChecksumMismatch { .. } => {
                "stored data failed an integrity check; this is a server-side error".to_string()
            }
            ZeppelinError::LeaseHeld { namespace, .. } => {
                format!("namespace {namespace} is being written by another process; retry shortly")
            }
            ZeppelinError::LeaseExpired { namespace } => {
                format!("write lease for namespace {namespace} expired; retry")
            }
            ZeppelinError::FencingTokenStale { namespace, .. } => {
                format!("a newer writer has taken over namespace {namespace}; retry")
            }
            ZeppelinError::ManifestConflict { namespace } => {
                format!("concurrent write conflict on namespace {namespace}; retry")
            }
            // Safe: only caller-supplied / structural context.
            ZeppelinError::ManifestNotFound { namespace }
            | ZeppelinError::NamespaceNotFound { namespace } => {
                format!("namespace not found: {namespace}")
            }
            ZeppelinError::SnapshotNotFound { namespace, name } => {
                format!("snapshot not found on namespace {namespace}: {name}")
            }
            ZeppelinError::PointInTimeNotRetained { namespace, target } => {
                format!("point-in-time {target} is no longer retained for namespace {namespace}")
            }
            ZeppelinError::NamespaceDeleting { namespace } => {
                format!("namespace is being deleted: {namespace}")
            }
            ZeppelinError::NamespaceAlreadyExists { .. }
            | ZeppelinError::SnapshotAlreadyExists { .. }
            | ZeppelinError::DimensionMismatch { .. }
            | ZeppelinError::VectorNotFound { .. }
            | ZeppelinError::Validation(_)
            | ZeppelinError::PayloadTooLarge { .. }
            | ZeppelinError::NotImplemented { .. }
            | ZeppelinError::HydrationDisabled
            | ZeppelinError::FtsFieldNotConfigured { .. }
            | ZeppelinError::QueryConcurrencyExhausted
            | ZeppelinError::RateLimitExceeded { .. } => self.to_string(),
        }
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
/// Regression tests for public classification, redaction, and conversion contracts.
mod tests {
    use super::*;

    /// Verifies audit-writer failures retain a typed top-level classification.
    #[test]
    fn audit_sink_error_converts_to_typed_zeppelin_error() {
        let error = ZeppelinError::from(crate::security::AuditSinkError::WriterAlreadyActive);
        assert!(matches!(error, ZeppelinError::AuditSink(_)));
        assert_eq!(error.status_code(), 500);
        assert_eq!(error.error_code(), "INTERNAL_ERROR");
        assert_eq!(error.client_message(), "an internal error occurred");
    }

    /// Verifies an internal S3 key miss is a redacted server failure, not a client 404.
    #[test]
    fn test_internal_notfound_is_500_not_404() {
        // An S3 key miss below the namespace layer is a server-side data
        // integrity failure, not a client 404 (Task 11 I2).
        let err = ZeppelinError::NotFound {
            key: "ns/segments/seg_x/cluster_3.bin".into(),
        };
        assert_eq!(err.status_code(), 500);
        assert_eq!(err.error_code(), "INTERNAL_DATA_MISSING");
        // I3: the raw S3 key must NOT appear in the client-facing message.
        assert!(
            !err.client_message().contains("segments/"),
            "client message leaked an internal S3 key: {}",
            err.client_message()
        );
    }

    /// Verifies a missing namespace is reported as a client-visible 404.
    #[test]
    fn test_namespace_not_found_status_code() {
        let err = ZeppelinError::NamespaceNotFound {
            namespace: "ns".into(),
        };
        assert_eq!(err.status_code(), 404);
    }

    /// Verifies a namespace with no manifest is reported as a client-visible 404.
    #[test]
    fn test_manifest_not_found_status_code() {
        let err = ZeppelinError::ManifestNotFound {
            namespace: "ns".into(),
        };
        assert_eq!(err.status_code(), 404);
    }

    /// Verifies creating an existing namespace is classified as a conflict.
    #[test]
    fn test_namespace_already_exists_status_code() {
        let err = ZeppelinError::NamespaceAlreadyExists {
            namespace: "ns".into(),
        };
        assert_eq!(err.status_code(), 409);
    }

    /// Verifies a vector with the wrong dimension is classified as bad input.
    #[test]
    fn test_dimension_mismatch_status_code() {
        let err = ZeppelinError::DimensionMismatch {
            expected: 128,
            actual: 256,
        };
        assert_eq!(err.status_code(), 400);
    }

    /// Verifies general request-validation failures are classified as bad input.
    #[test]
    fn test_validation_status_code() {
        let err = ZeppelinError::Validation("bad input".into());
        assert_eq!(err.status_code(), 400);
    }

    /// Verifies rate limiting produces status 429 and a useful internal display string.
    #[test]
    fn test_rate_limit_exceeded_status_code() {
        let err = ZeppelinError::RateLimitExceeded {
            retry_after_secs: 1,
        };
        assert_eq!(err.status_code(), 429);
        assert!(err.to_string().contains("rate limit exceeded"));
    }

    /// Verifies uncategorized internal subsystem failures default to status 500.
    #[test]
    fn test_default_status_code() {
        let err = ZeppelinError::Bincode("bad data".into());
        assert_eq!(err.status_code(), 500);

        let err = ZeppelinError::Config("missing key".into());
        assert_eq!(err.status_code(), 500);

        let err = ZeppelinError::Cache("disk full".into());
        assert_eq!(err.status_code(), 500);

        let err = ZeppelinError::Index("corrupt".into());
        assert_eq!(err.status_code(), 500);
    }

    /// Verifies generated display messages retain diagnostic fields for server logs.
    #[test]
    fn test_display_formatting() {
        let err = ZeppelinError::NotFound {
            key: "my/key".into(),
        };
        assert!(err.to_string().contains("my/key"));

        let err = ZeppelinError::DimensionMismatch {
            expected: 128,
            actual: 256,
        };
        let msg = err.to_string();
        assert!(msg.contains("128"));
        assert!(msg.contains("256"));

        let err = ZeppelinError::ChecksumMismatch {
            expected: 111,
            actual: 222,
        };
        let msg = err.to_string();
        assert!(msg.contains("111"));
        assert!(msg.contains("222"));
    }

    /// Verifies representative variants yield non-empty stable wire codes.
    ///
    /// Exhaustiveness comes from the wildcard-free match in [`ZeppelinError::error_code`]:
    /// a new variant cannot compile until it receives a code. This test separately
    /// pins the format for representative constructible variants.
    #[test]
    fn test_every_variant_has_stable_code() {
        // Keep representative values in sync with new public error categories;
        // the exhaustive production match is the compiler-enforced backstop.
        let variants: Vec<ZeppelinError> = vec![
            ZeppelinError::NotFound { key: "k".into() },
            ZeppelinError::Storage(object_store::Error::NotFound {
                path: "p".into(),
                source: "x".into(),
            }),
            ZeppelinError::Bincode("b".into()),
            ZeppelinError::Serialization("s".into()),
            ZeppelinError::ChecksumMismatch {
                expected: 1,
                actual: 2,
            },
            ZeppelinError::ManifestNotFound {
                namespace: "n".into(),
            },
            ZeppelinError::ManifestConflict {
                namespace: "n".into(),
            },
            ZeppelinError::LeaseHeld {
                namespace: "n".into(),
                holder: "h".into(),
            },
            ZeppelinError::LeaseExpired {
                namespace: "n".into(),
            },
            ZeppelinError::FencingTokenStale {
                namespace: "n".into(),
                our_token: 1,
                manifest_token: 2,
            },
            ZeppelinError::NamespaceNotFound {
                namespace: "n".into(),
            },
            ZeppelinError::NamespaceAlreadyExists {
                namespace: "n".into(),
            },
            ZeppelinError::SnapshotAlreadyExists {
                namespace: "n".into(),
                name: "s".into(),
                existing_generation: 1,
                requested_generation: 2,
            },
            ZeppelinError::SnapshotNotFound {
                namespace: "n".into(),
                name: "s".into(),
            },
            ZeppelinError::PointInTimeNotRetained {
                namespace: "n".into(),
                target: "generation 1".into(),
            },
            ZeppelinError::NamespaceDeleting {
                namespace: "n".into(),
            },
            ZeppelinError::NamespaceDeleteIncomplete {
                namespace: "n".into(),
                remaining_keys: 1,
            },
            ZeppelinError::Index("i".into()),
            ZeppelinError::Rq("bad payload".into()),
            ZeppelinError::Membership("m".into()),
            ZeppelinError::KMeansConvergence { iterations: 3 },
            ZeppelinError::DimensionMismatch {
                expected: 1,
                actual: 2,
            },
            ZeppelinError::VectorNotFound {
                namespace: "n".into(),
                id: "v".into(),
            },
            ZeppelinError::Validation("v".into()),
            ZeppelinError::PayloadTooLarge {
                resource: "query batch",
                actual: 257,
                limit: 256,
            },
            ZeppelinError::NotImplemented {
                feature: "retrieval algebra",
            },
            ZeppelinError::Config("c".into()),
            ZeppelinError::Cache("c".into()),
            ZeppelinError::HydrationDisabled,
            ZeppelinError::FullTextSearch("f".into()),
            ZeppelinError::FtsFieldNotConfigured {
                namespace: "n".into(),
                field: "f".into(),
            },
            ZeppelinError::IndexUnavailable("fts missing".into()),
            ZeppelinError::QueryConcurrencyExhausted,
            ZeppelinError::RateLimitExceeded {
                retry_after_secs: 1,
            },
            ZeppelinError::Security(crate::security::SecurityError::Authentication(
                crate::security::AuthnFailure::Unauthenticated,
            )),
        ];
        for e in &variants {
            let code = e.error_code();
            assert!(!code.is_empty(), "empty code for {e:?}");
            if matches!(e, ZeppelinError::Security(_)) {
                assert!(
                    code.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                    "security code {code} for {e:?} is not snake_case"
                );
            } else {
                assert!(
                    code.chars().all(|c| c.is_ascii_uppercase() || c == '_'),
                    "code {code} for {e:?} is not SCREAMING_SNAKE_CASE"
                );
            }
        }
    }

    /// Verifies retry advice and delay hints distinguish transient from permanent errors.
    #[test]
    fn test_retryable_and_retry_after() {
        // Conflicts / concurrency / rate-limit are retryable with a Retry-After.
        assert!(ZeppelinError::ManifestConflict {
            namespace: "n".into()
        }
        .retryable());
        assert_eq!(
            ZeppelinError::ManifestConflict {
                namespace: "n".into()
            }
            .retry_after_secs(),
            Some(1)
        );
        assert!(ZeppelinError::QueryConcurrencyExhausted.retryable());
        assert_eq!(
            ZeppelinError::QueryConcurrencyExhausted.retry_after_secs(),
            Some(1)
        );
        assert_eq!(
            ZeppelinError::RateLimitExceeded {
                retry_after_secs: 7
            }
            .retry_after_secs(),
            Some(7)
        );
        // Validation / namespace-shape errors are not retryable.
        assert!(!ZeppelinError::Validation("bad".into()).retryable());
        assert!(!ZeppelinError::DimensionMismatch {
            expected: 1,
            actual: 2
        }
        .retryable());
        assert_eq!(
            ZeppelinError::Validation("bad".into()).retry_after_secs(),
            None
        );
    }

    /// Verifies client messages redact fencing, lease-holder, and storage details.
    #[test]
    fn test_client_message_hides_internals() {
        // Fencing token values and lease holder IDs must never reach clients.
        let e = ZeppelinError::FencingTokenStale {
            namespace: "ns".into(),
            our_token: 3,
            manifest_token: 9,
        };
        let msg = e.client_message();
        assert!(
            !msg.contains('3') && !msg.contains('9'),
            "leaked token: {msg}"
        );
        assert!(msg.contains("ns"));

        let e = ZeppelinError::LeaseHeld {
            namespace: "ns".into(),
            holder: "holder-uuid-abc".into(),
        };
        assert!(
            !e.client_message().contains("holder-uuid-abc"),
            "leaked lease holder: {}",
            e.client_message()
        );

        // Storage errors collapse to a generic retry message.
        let e = ZeppelinError::Storage(object_store::Error::NotFound {
            path: "secret-bucket/ns/wal/x.wal".into(),
            source: "endpoint https://internal:9000".into(),
        });
        let msg = e.client_message();
        assert!(
            !msg.contains("secret-bucket") && !msg.contains("internal"),
            "leaked storage detail: {msg}"
        );
    }

    #[test]
    fn index_unavailable_client_message_hides_manifest_counts() {
        let error = ZeppelinError::IndexUnavailable(
            "BM25 fallback would scan 413 clusters and 98,765 vectors (limit 7)".into(),
        );
        assert_eq!(
            error.client_message(),
            "the requested FTS index is unavailable; contact the server operator"
        );
    }

    /// Verifies bincode errors convert into the common Zeppelin error category.
    #[test]
    fn test_from_bincode_error() {
        let bincode_err: Box<bincode::ErrorKind> =
            Box::new(bincode::ErrorKind::Custom("test error".into()));
        let err: ZeppelinError = bincode_err.into();
        match &err {
            ZeppelinError::Bincode(msg) => assert!(msg.contains("test error")),
            other => panic!("expected Bincode, got {:?}", other),
        }
    }
}
