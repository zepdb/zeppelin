use thiserror::Error;

#[derive(Error, Debug)]
pub enum ZeppelinError {
    // Storage errors
    #[error("object not found: {key}")]
    NotFound { key: String },

    #[error("storage error: {0}")]
    Storage(#[from] object_store::Error),

    #[error("storage path error: {0}")]
    StoragePath(#[from] object_store::path::Error),

    // Serialization errors
    #[error("json serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("bincode serialization error: {0}")]
    Bincode(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    // WAL errors
    #[error("checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: u64, actual: u64 },

    #[error("manifest not found for namespace: {namespace}")]
    ManifestNotFound { namespace: String },

    #[error("manifest conflict (concurrent write) for namespace: {namespace}")]
    ManifestConflict { namespace: String },

    // Lease errors
    #[error("lease held on namespace {namespace} by {holder}")]
    LeaseHeld { namespace: String, holder: String },

    #[error("lease expired for namespace {namespace}")]
    LeaseExpired { namespace: String },

    #[error("fencing token stale for namespace {namespace}: ours={our_token}, manifest={manifest_token}")]
    FencingTokenStale {
        namespace: String,
        our_token: u64,
        manifest_token: u64,
    },

    // Namespace errors
    #[error("namespace not found: {namespace}")]
    NamespaceNotFound { namespace: String },

    #[error("namespace already exists: {namespace}")]
    NamespaceAlreadyExists { namespace: String },

    // Index errors
    #[error("index error: {0}")]
    Index(String),

    #[error("k-means failed to converge after {iterations} iterations")]
    KMeansConvergence { iterations: usize },

    // Validation errors
    #[error("dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },

    #[error("validation error: {0}")]
    Validation(String),

    // Config errors
    #[error("config error: {0}")]
    Config(String),

    // IO errors
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    // Cache errors
    #[error("cache error: {0}")]
    Cache(String),

    // Full-text search errors
    #[error("full-text search error: {0}")]
    FullTextSearch(String),

    #[error("FTS field not configured on namespace {namespace}: {field}")]
    FtsFieldNotConfigured { namespace: String, field: String },

    #[error("query concurrency limit reached, try again later")]
    QueryConcurrencyExhausted,
}

impl From<Box<bincode::ErrorKind>> for ZeppelinError {
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        ZeppelinError::Bincode(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, ZeppelinError>;

impl ZeppelinError {
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

            _ => 500,
        }
    }
}

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
