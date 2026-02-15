use thiserror::Error;

/// All errors that can occur within the Zeppelin vector search engine.
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

    // Index errors
    /// A vector indexing operation failed.
    #[error("index error: {0}")]
    Index(String),

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

    /// A request failed input validation.
    #[error("validation error: {0}")]
    Validation(String),

    // Config errors
    /// An invalid or missing configuration value.
    #[error("config error: {0}")]
    Config(String),

    // IO errors
    /// A local filesystem I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    // Cache errors
    /// A local disk or memory cache operation failed.
    #[error("cache error: {0}")]
    Cache(String),

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

impl From<Box<bincode::ErrorKind>> for ZeppelinError {
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        ZeppelinError::Bincode(e.to_string())
    }
}

/// A convenience type alias for `std::result::Result<T, ZeppelinError>`.
pub type Result<T> = std::result::Result<T, ZeppelinError>;

impl ZeppelinError {
    /// Returns the HTTP status code appropriate for this error variant.
    pub fn status_code(&self) -> u16 {
        match self {
            ZeppelinError::NotFound { .. }
            | ZeppelinError::NamespaceNotFound { .. }
            | ZeppelinError::ManifestNotFound { .. } => 404,

            ZeppelinError::NamespaceAlreadyExists { .. }
            | ZeppelinError::ManifestConflict { .. }
            | ZeppelinError::LeaseHeld { .. }
            | ZeppelinError::LeaseExpired { .. }
            | ZeppelinError::FencingTokenStale { .. } => 409,

            ZeppelinError::DimensionMismatch { .. }
            | ZeppelinError::Validation(_)
            | ZeppelinError::FtsFieldNotConfigured { .. } => 400,

            ZeppelinError::QueryConcurrencyExhausted => 503,

            ZeppelinError::RateLimitExceeded { .. } => 429,

            _ => 500,
        }
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_found_status_code() {
        let err = ZeppelinError::NotFound {
            key: "some/key".into(),
        };
        assert_eq!(err.status_code(), 404);
    }

    #[test]
    fn test_namespace_not_found_status_code() {
        let err = ZeppelinError::NamespaceNotFound {
            namespace: "ns".into(),
        };
        assert_eq!(err.status_code(), 404);
    }

    #[test]
    fn test_manifest_not_found_status_code() {
        let err = ZeppelinError::ManifestNotFound {
            namespace: "ns".into(),
        };
        assert_eq!(err.status_code(), 404);
    }

    #[test]
    fn test_namespace_already_exists_status_code() {
        let err = ZeppelinError::NamespaceAlreadyExists {
            namespace: "ns".into(),
        };
        assert_eq!(err.status_code(), 409);
    }

    #[test]
    fn test_dimension_mismatch_status_code() {
        let err = ZeppelinError::DimensionMismatch {
            expected: 128,
            actual: 256,
        };
        assert_eq!(err.status_code(), 400);
    }

    #[test]
    fn test_validation_status_code() {
        let err = ZeppelinError::Validation("bad input".into());
        assert_eq!(err.status_code(), 400);
    }

    #[test]
    fn test_rate_limit_exceeded_status_code() {
        let err = ZeppelinError::RateLimitExceeded {
            retry_after_secs: 1,
        };
        assert_eq!(err.status_code(), 429);
        assert!(err.to_string().contains("rate limit exceeded"));
    }

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
