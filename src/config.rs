use crate::error::{Result, ZeppelinError};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub indexing: IndexingConfig,
    #[serde(default)]
    pub compaction: CompactionConfig,
    #[serde(default)]
    pub consistency: ConsistencyConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_request_timeout")]
    pub request_timeout_secs: u64,
    #[serde(default = "default_max_concurrent_queries")]
    pub max_concurrent_queries: usize,
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,
    #[serde(default = "default_max_top_k")]
    pub max_top_k: usize,
    #[serde(default = "default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,
    #[serde(default = "default_max_dimensions")]
    pub max_dimensions: usize,
    #[serde(default = "default_max_vector_id_length")]
    pub max_vector_id_length: usize,
    #[serde(default = "default_max_request_body_mb")]
    pub max_request_body_mb: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_backend")]
    pub backend: String,
    #[serde(default = "default_bucket")]
    pub bucket: String,

    // S3 / MinIO / R2
    #[serde(default)]
    pub s3_region: Option<String>,
    #[serde(default)]
    pub s3_endpoint: Option<String>,
    #[serde(default)]
    pub s3_access_key_id: Option<String>,
    #[serde(default)]
    pub s3_secret_access_key: Option<String>,
    #[serde(default)]
    pub s3_allow_http: bool,

    // GCS
    #[serde(default)]
    pub gcs_service_account_path: Option<String>,

    // Azure
    #[serde(default)]
    pub azure_account: Option<String>,
    #[serde(default)]
    pub azure_access_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "default_cache_dir")]
    pub dir: PathBuf,
    #[serde(default = "default_max_size_gb")]
    pub max_size_gb: u64,
    #[serde(default = "default_eviction")]
    pub eviction: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexingConfig {
    #[serde(default = "default_num_centroids")]
    pub default_num_centroids: usize,
    #[serde(default = "default_nprobe")]
    pub default_nprobe: usize,
    #[serde(default = "default_max_nprobe")]
    pub max_nprobe: usize,
    #[serde(default = "default_kmeans_max_iterations")]
    pub kmeans_max_iterations: usize,
    #[serde(default = "default_kmeans_convergence_epsilon")]
    pub kmeans_convergence_epsilon: f64,
    #[serde(default = "default_oversample_factor")]
    pub oversample_factor: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    #[serde(default = "default_compaction_interval")]
    pub interval_secs: u64,
    #[serde(default = "default_max_wal_fragments")]
    pub max_wal_fragments_before_compact: usize,
    #[serde(default = "default_retrain_threshold")]
    pub retrain_imbalance_threshold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsistencyConfig {
    #[serde(default = "default_consistency")]
    pub default: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default = "default_log_format")]
    pub format: String,
}

// Default value functions
fn default_host() -> String {
    std::env::var("ZEPPELIN_HOST").unwrap_or_else(|_| "0.0.0.0".to_string())
}
fn default_port() -> u16 {
    std::env::var("ZEPPELIN_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080)
}
fn default_request_timeout() -> u64 {
    std::env::var("ZEPPELIN_REQUEST_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30)
}
fn default_max_concurrent_queries() -> usize {
    std::env::var("ZEPPELIN_MAX_CONCURRENT_QUERIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64)
}
fn default_max_batch_size() -> usize {
    std::env::var("ZEPPELIN_MAX_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000)
}
fn default_max_top_k() -> usize {
    std::env::var("ZEPPELIN_MAX_TOP_K")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000)
}
fn default_shutdown_timeout_secs() -> u64 {
    std::env::var("ZEPPELIN_SHUTDOWN_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30)
}
fn default_max_dimensions() -> usize {
    std::env::var("ZEPPELIN_MAX_DIMENSIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(65_536)
}
fn default_max_vector_id_length() -> usize {
    std::env::var("ZEPPELIN_MAX_VECTOR_ID_LENGTH")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1024)
}
fn default_max_request_body_mb() -> usize {
    std::env::var("ZEPPELIN_MAX_REQUEST_BODY_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50)
}
fn default_backend() -> String {
    std::env::var("STORAGE_BACKEND").unwrap_or_else(|_| "s3".to_string())
}
fn default_bucket() -> String {
    std::env::var("S3_BUCKET").unwrap_or_else(|_| "zeppelin".to_string())
}
fn default_cache_dir() -> PathBuf {
    std::env::var("ZEPPELIN_CACHE_DIR")
        .ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/var/cache/zeppelin"))
}
fn default_max_size_gb() -> u64 {
    std::env::var("ZEPPELIN_CACHE_MAX_SIZE_GB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50)
}
fn default_eviction() -> String {
    "lru".to_string()
}
fn default_num_centroids() -> usize {
    std::env::var("ZEPPELIN_DEFAULT_NUM_CENTROIDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(256)
}
fn default_nprobe() -> usize {
    std::env::var("ZEPPELIN_DEFAULT_NPROBE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16)
}
fn default_max_nprobe() -> usize {
    128
}
fn default_kmeans_max_iterations() -> usize {
    25
}
fn default_kmeans_convergence_epsilon() -> f64 {
    1e-4
}
fn default_oversample_factor() -> usize {
    3
}
fn default_compaction_interval() -> u64 {
    std::env::var("ZEPPELIN_COMPACTION_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30)
}
fn default_max_wal_fragments() -> usize {
    1000
}
fn default_retrain_threshold() -> f64 {
    5.0
}
fn default_consistency() -> String {
    "strong".to_string()
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_log_format() -> String {
    std::env::var("ZEPPELIN_LOG_FORMAT").unwrap_or_else(|_| "json".to_string())
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            request_timeout_secs: default_request_timeout(),
            max_concurrent_queries: default_max_concurrent_queries(),
            max_batch_size: default_max_batch_size(),
            max_top_k: default_max_top_k(),
            shutdown_timeout_secs: default_shutdown_timeout_secs(),
            max_dimensions: default_max_dimensions(),
            max_vector_id_length: default_max_vector_id_length(),
            max_request_body_mb: default_max_request_body_mb(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: default_backend(),
            bucket: default_bucket(),
            s3_region: std::env::var("AWS_REGION").ok(),
            s3_endpoint: std::env::var("S3_ENDPOINT").ok().filter(|s| !s.is_empty()),
            s3_access_key_id: std::env::var("AWS_ACCESS_KEY_ID").ok(),
            s3_secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
            s3_allow_http: std::env::var("S3_ALLOW_HTTP")
                .ok()
                .map(|v| v == "true")
                .unwrap_or(false),
            gcs_service_account_path: std::env::var("GCS_SERVICE_ACCOUNT_PATH")
                .ok()
                .filter(|s| !s.is_empty()),
            azure_account: std::env::var("AZURE_ACCOUNT").ok().filter(|s| !s.is_empty()),
            azure_access_key: std::env::var("AZURE_ACCESS_KEY")
                .ok()
                .filter(|s| !s.is_empty()),
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            dir: default_cache_dir(),
            max_size_gb: default_max_size_gb(),
            eviction: default_eviction(),
        }
    }
}

impl Default for IndexingConfig {
    fn default() -> Self {
        Self {
            default_num_centroids: default_num_centroids(),
            default_nprobe: default_nprobe(),
            max_nprobe: default_max_nprobe(),
            kmeans_max_iterations: default_kmeans_max_iterations(),
            kmeans_convergence_epsilon: default_kmeans_convergence_epsilon(),
            oversample_factor: default_oversample_factor(),
        }
    }
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            interval_secs: default_compaction_interval(),
            max_wal_fragments_before_compact: default_max_wal_fragments(),
            retrain_imbalance_threshold: default_retrain_threshold(),
        }
    }
}

impl Default for ConsistencyConfig {
    fn default() -> Self {
        Self {
            default: default_consistency(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}

impl Config {
    /// Load config from a TOML file, falling back to defaults.
    /// After loading, env var overrides are applied so that:
    /// env var > TOML file > defaults.
    pub fn load(path: Option<&str>) -> Result<Self> {
        let mut config = match path {
            Some(p) => {
                let content = std::fs::read_to_string(p).map_err(|e| {
                    ZeppelinError::Config(format!("failed to read config file {p}: {e}"))
                })?;
                toml::from_str(&content)
                    .map_err(|e| ZeppelinError::Config(format!("failed to parse config: {e}")))?
            }
            None => Config::default(),
        };
        config.apply_env_overrides();
        Ok(config)
    }

    /// Apply environment variable overrides on top of file/default values.
    /// This ensures env vars always take priority over TOML settings.
    fn apply_env_overrides(&mut self) {
        // Server
        if let Ok(v) = std::env::var("ZEPPELIN_HOST") {
            self.server.host = v;
        }
        if let Some(v) = std::env::var("ZEPPELIN_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            self.server.port = v;
        }
        if let Some(v) = std::env::var("ZEPPELIN_REQUEST_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            self.server.request_timeout_secs = v;
        }
        if let Some(v) = std::env::var("ZEPPELIN_MAX_CONCURRENT_QUERIES")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            self.server.max_concurrent_queries = v;
        }
        if let Some(v) = std::env::var("ZEPPELIN_MAX_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            self.server.max_batch_size = v;
        }
        if let Some(v) = std::env::var("ZEPPELIN_MAX_TOP_K")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            self.server.max_top_k = v;
        }
        if let Some(v) = std::env::var("ZEPPELIN_SHUTDOWN_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            self.server.shutdown_timeout_secs = v;
        }
        if let Some(v) = std::env::var("ZEPPELIN_MAX_DIMENSIONS")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            self.server.max_dimensions = v;
        }
        if let Some(v) = std::env::var("ZEPPELIN_MAX_VECTOR_ID_LENGTH")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            self.server.max_vector_id_length = v;
        }
        if let Some(v) = std::env::var("ZEPPELIN_MAX_REQUEST_BODY_MB")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            self.server.max_request_body_mb = v;
        }

        // Storage
        if let Ok(v) = std::env::var("STORAGE_BACKEND") {
            self.storage.backend = v;
        }
        if let Ok(v) = std::env::var("S3_BUCKET") {
            self.storage.bucket = v;
        }
        if let Ok(v) = std::env::var("AWS_REGION") {
            self.storage.s3_region = Some(v);
        }
        if let Some(v) = std::env::var("S3_ENDPOINT").ok().filter(|s| !s.is_empty()) {
            self.storage.s3_endpoint = Some(v);
        }
        if let Ok(v) = std::env::var("AWS_ACCESS_KEY_ID") {
            self.storage.s3_access_key_id = Some(v);
        }
        if let Ok(v) = std::env::var("AWS_SECRET_ACCESS_KEY") {
            self.storage.s3_secret_access_key = Some(v);
        }
        if let Ok(v) = std::env::var("S3_ALLOW_HTTP") {
            self.storage.s3_allow_http = v == "true";
        }
        if let Some(v) = std::env::var("GCS_SERVICE_ACCOUNT_PATH")
            .ok()
            .filter(|s| !s.is_empty())
        {
            self.storage.gcs_service_account_path = Some(v);
        }
        if let Some(v) = std::env::var("AZURE_ACCOUNT")
            .ok()
            .filter(|s| !s.is_empty())
        {
            self.storage.azure_account = Some(v);
        }
        if let Some(v) = std::env::var("AZURE_ACCESS_KEY")
            .ok()
            .filter(|s| !s.is_empty())
        {
            self.storage.azure_access_key = Some(v);
        }

        // Cache
        if let Ok(v) = std::env::var("ZEPPELIN_CACHE_DIR") {
            self.cache.dir = PathBuf::from(v);
        }
        if let Some(v) = std::env::var("ZEPPELIN_CACHE_MAX_SIZE_GB")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            self.cache.max_size_gb = v;
        }

        // Indexing
        if let Some(v) = std::env::var("ZEPPELIN_DEFAULT_NUM_CENTROIDS")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            self.indexing.default_num_centroids = v;
        }
        if let Some(v) = std::env::var("ZEPPELIN_DEFAULT_NPROBE")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            self.indexing.default_nprobe = v;
        }

        // Compaction
        if let Some(v) = std::env::var("ZEPPELIN_COMPACTION_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            self.compaction.interval_secs = v;
        }

        // Logging
        if let Ok(v) = std::env::var("ZEPPELIN_LOG_FORMAT") {
            self.logging.format = v;
        }
    }
}
