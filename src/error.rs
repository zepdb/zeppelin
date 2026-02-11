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

    // WAL errors
    #[error("checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: u64, actual: u64 },

    #[error("manifest not found for namespace: {namespace}")]
    ManifestNotFound { namespace: String },

    // Namespace errors
    #[error("namespace not found: {namespace}")]
    NamespaceNotFound { namespace: String },

    #[error("namespace already exists: {namespace}")]
    NamespaceAlreadyExists { namespace: String },

    // Index errors
    #[error("index not built for namespace: {namespace}")]
    IndexNotBuilt { namespace: String },

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

    // Query errors
    #[error("query error: {0}")]
    Query(String),

    // IO errors
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    // Compaction errors
    #[error("compaction error: {0}")]
    Compaction(String),

    // Cache errors
    #[error("cache error: {0}")]
    Cache(String),

    // Internal
    #[error("internal error: {0}")]
    Internal(String),
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

            ZeppelinError::NamespaceAlreadyExists { .. } => 409,

            ZeppelinError::DimensionMismatch { .. } | ZeppelinError::Validation(_) => 400,

            ZeppelinError::IndexNotBuilt { .. } => 503,

            _ => 500,
        }
    }
}
