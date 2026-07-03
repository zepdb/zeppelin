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
    ///
    /// Note the deliberate split of the S3-level `NotFound { key }`: an object
    /// key miss below the namespace layer (a segment/cluster/fragment that the
    /// manifest references but S3 can't return) is NOT a client-facing 404 —
    /// it's a server-side data-integrity failure (500). Only *namespace*-level
    /// misses (`NamespaceNotFound`, `ManifestNotFound`) are true 404s. Leaking
    /// the former as 404 made "your namespace is gone" out of "a segment file
    /// went missing" (Task 11 I2).
    pub fn status_code(&self) -> u16 {
        match self {
            ZeppelinError::NamespaceNotFound { .. } | ZeppelinError::ManifestNotFound { .. } => 404,

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

            // Internal S3 key miss = data-integrity failure, not a 404.
            ZeppelinError::NotFound { .. } => 500,

            _ => 500,
        }
    }

    /// Stable, machine-readable error code (SCREAMING_SNAKE_CASE).
    ///
    /// Every variant maps to exactly one code; this is an EXHAUSTIVE match (no
    /// wildcard arm) so a newly-added variant fails to compile until it is
    /// assigned a code — see `test_every_variant_has_stable_code`. Clients key
    /// off this, never the human `error` string or the numeric status.
    pub fn error_code(&self) -> &'static str {
        match self {
            ZeppelinError::NotFound { .. } => "INTERNAL_DATA_MISSING",
            ZeppelinError::Storage(_) => "STORAGE_ERROR",
            ZeppelinError::StoragePath(_) => "STORAGE_ERROR",
            ZeppelinError::Json(_) => "INTERNAL_ERROR",
            ZeppelinError::Bincode(_) => "INTERNAL_ERROR",
            ZeppelinError::Serialization(_) => "INTERNAL_ERROR",
            ZeppelinError::ChecksumMismatch { .. } => "DATA_CORRUPTION",
            ZeppelinError::ManifestNotFound { .. } => "NAMESPACE_NOT_FOUND",
            ZeppelinError::ManifestConflict { .. } => "CONFLICT_RETRY",
            ZeppelinError::LeaseHeld { .. } => "CONFLICT_RETRY",
            ZeppelinError::LeaseExpired { .. } => "CONFLICT_RETRY",
            ZeppelinError::FencingTokenStale { .. } => "CONFLICT_RETRY",
            ZeppelinError::NamespaceNotFound { .. } => "NAMESPACE_NOT_FOUND",
            ZeppelinError::NamespaceAlreadyExists { .. } => "NAMESPACE_ALREADY_EXISTS",
            ZeppelinError::Index(_) => "INTERNAL_ERROR",
            ZeppelinError::KMeansConvergence { .. } => "INTERNAL_ERROR",
            ZeppelinError::DimensionMismatch { .. } => "DIMENSION_MISMATCH",
            ZeppelinError::Validation(_) => "VALIDATION_ERROR",
            ZeppelinError::Config(_) => "INTERNAL_ERROR",
            ZeppelinError::Io(_) => "INTERNAL_ERROR",
            ZeppelinError::Cache(_) => "INTERNAL_ERROR",
            ZeppelinError::FullTextSearch(_) => "INTERNAL_ERROR",
            ZeppelinError::FtsFieldNotConfigured { .. } => "FTS_FIELD_NOT_CONFIGURED",
            ZeppelinError::QueryConcurrencyExhausted => "CONCURRENCY_LIMIT",
            ZeppelinError::RateLimitExceeded { .. } => "RATE_LIMITED",
        }
    }

    /// Whether the client can reasonably retry the same request unchanged.
    /// Conflicts (CAS/lease), concurrency, rate limits, and transient storage
    /// errors are retryable; validation and namespace-shape errors are not.
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            ZeppelinError::ManifestConflict { .. }
                | ZeppelinError::LeaseHeld { .. }
                | ZeppelinError::LeaseExpired { .. }
                | ZeppelinError::FencingTokenStale { .. }
                | ZeppelinError::QueryConcurrencyExhausted
                | ZeppelinError::RateLimitExceeded { .. }
                | ZeppelinError::Storage(_)
        )
    }

    /// Seconds to advertise in a `Retry-After` header, if applicable.
    /// Rate-limit carries its own budget; conflict/concurrency get a short
    /// default nudge so clients back off instead of hot-looping.
    pub fn retry_after_secs(&self) -> Option<u64> {
        match self {
            ZeppelinError::RateLimitExceeded { retry_after_secs } => Some(*retry_after_secs),
            ZeppelinError::QueryConcurrencyExhausted
            | ZeppelinError::ManifestConflict { .. }
            | ZeppelinError::LeaseHeld { .. }
            | ZeppelinError::LeaseExpired { .. }
            | ZeppelinError::FencingTokenStale { .. } => Some(1),
            _ => None,
        }
    }

    /// Human-readable message SAFE to return to clients — never leaks S3 keys,
    /// bucket names, endpoints, fencing-token values, or lease-holder IDs
    /// (those go to structured logs only; Task 11 I3). Variants whose `Display`
    /// already carries only safe, caller-supplied context (validation text,
    /// namespace names, dimensions) pass through; variants whose `Display`
    /// embeds internal detail get a generic, actionable string instead.
    pub fn client_message(&self) -> String {
        match self {
            // Internal detail — return generic text, log the specifics elsewhere.
            ZeppelinError::NotFound { .. } => {
                "an internal data object is missing; this is a server-side error".to_string()
            }
            ZeppelinError::Storage(_) | ZeppelinError::StoragePath(_) => {
                "a transient storage error occurred; please retry".to_string()
            }
            ZeppelinError::Json(_)
            | ZeppelinError::Bincode(_)
            | ZeppelinError::Serialization(_)
            | ZeppelinError::Index(_)
            | ZeppelinError::KMeansConvergence { .. }
            | ZeppelinError::Config(_)
            | ZeppelinError::Io(_)
            | ZeppelinError::Cache(_)
            | ZeppelinError::FullTextSearch(_) => "an internal error occurred".to_string(),
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
            ZeppelinError::NamespaceAlreadyExists { .. }
            | ZeppelinError::DimensionMismatch { .. }
            | ZeppelinError::Validation(_)
            | ZeppelinError::FtsFieldNotConfigured { .. }
            | ZeppelinError::QueryConcurrencyExhausted
            | ZeppelinError::RateLimitExceeded { .. } => self.to_string(),
        }
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    use super::*;

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

    /// Exhaustive guard: every variant must yield a non-empty, uppercase-snake
    /// stable code. The `match` in `error_code()` has no wildcard, so a new
    /// variant won't compile until coded; this test additionally pins the
    /// FORMAT so codes stay client-stable.
    #[test]
    fn test_every_variant_has_stable_code() {
        // One representative per variant. If a variant is added, `error_code`'s
        // exhaustive match fails to compile first; keep this list in sync so
        // the format assertions cover it too.
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
            ZeppelinError::Index("i".into()),
            ZeppelinError::KMeansConvergence { iterations: 3 },
            ZeppelinError::DimensionMismatch {
                expected: 1,
                actual: 2,
            },
            ZeppelinError::Validation("v".into()),
            ZeppelinError::Config("c".into()),
            ZeppelinError::Cache("c".into()),
            ZeppelinError::FullTextSearch("f".into()),
            ZeppelinError::FtsFieldNotConfigured {
                namespace: "n".into(),
                field: "f".into(),
            },
            ZeppelinError::QueryConcurrencyExhausted,
            ZeppelinError::RateLimitExceeded {
                retry_after_secs: 1,
            },
        ];
        for e in &variants {
            let code = e.error_code();
            assert!(!code.is_empty(), "empty code for {e:?}");
            assert!(
                code.chars().all(|c| c.is_ascii_uppercase() || c == '_'),
                "code {code} for {e:?} is not SCREAMING_SNAKE_CASE"
            );
        }
    }

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
