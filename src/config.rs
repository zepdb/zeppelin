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
    #[serde(default = "default_top_k")]
    pub default_top_k: usize,
}

fn default_top_k() -> usize {
    10
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend {
    #[default]
    S3,
    Gcs,
    Azure,
    Local,
}

impl std::fmt::Display for StorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageBackend::S3 => write!(f, "s3"),
            StorageBackend::Gcs => write!(f, "gcs"),
            StorageBackend::Azure => write!(f, "azure"),
            StorageBackend::Local => write!(f, "local"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    #[serde(default)]
    pub backend: StorageBackend,
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvictionPolicy {
    #[default]
    Lru,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "default_cache_dir")]
    pub dir: PathBuf,
    #[serde(default = "default_max_size_gb")]
    pub max_size_gb: u64,
    #[serde(default)]
    pub eviction: EvictionPolicy,
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
    /// Quantization type for vector compression.
    #[serde(default)]
    pub quantization: crate::index::quantization::QuantizationType,
    /// Number of PQ subquantizers (only used when quantization = product).
    /// Must divide vector dimension evenly. Default: 8.
    #[serde(default = "default_pq_m")]
    pub pq_m: usize,
    /// Reranking factor: how many candidates to fetch with approximate
    /// distances before reranking with full-precision vectors.
    /// Only used when quantization is enabled. Default: 4.
    #[serde(default = "default_rerank_factor")]
    pub rerank_factor: usize,
    /// Whether to use hierarchical (multi-level centroid tree) indexing.
    /// When true, build produces a hierarchical index instead of flat IVF.
    /// Default: false.
    #[serde(default)]
    pub hierarchical: bool,
    /// Beam width for hierarchical search (candidates kept per level).
    /// Higher = better recall but more S3 reads. Default: 10.
    #[serde(default = "default_beam_width")]
    pub beam_width: usize,
    /// Maximum vectors per leaf cluster in hierarchical index.
    /// When `None`, uses the default of 1000. Set to a small value
    /// (e.g., 5–10) in tests to force multi-level trees with small datasets.
    #[serde(default)]
    pub leaf_size: Option<usize>,
    /// Whether to build bitmap indexes for pre-filtering.
    /// When true, each cluster gets a roaring bitmap index per attribute field,
    /// enabling filter evaluation before distance computation.
    #[serde(default = "default_bitmap_index")]
    pub bitmap_index: bool,
    /// Whether to build FTS inverted indexes during compaction.
    #[serde(default)]
    pub fts_index: bool,
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConsistencyConfig {
    #[serde(default)]
    pub default: crate::types::ConsistencyLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default = "default_log_format")]
    pub format: String,
}

// Default value functions — hardcoded defaults only.
// Env var overrides are applied in `apply_env_overrides()`.
fn default_host() -> String {
    "0.0.0.0".to_string()
}
fn default_port() -> u16 {
    8080
}
fn default_request_timeout() -> u64 {
    30
}
fn default_max_concurrent_queries() -> usize {
    64
}
fn default_max_batch_size() -> usize {
    10_000
}
fn default_max_top_k() -> usize {
    10_000
}
fn default_shutdown_timeout_secs() -> u64 {
    30
}
fn default_max_dimensions() -> usize {
    65_536
}
fn default_max_vector_id_length() -> usize {
    1024
}
fn default_max_request_body_mb() -> usize {
    50
}
fn default_bucket() -> String {
    "zeppelin".to_string()
}
fn default_cache_dir() -> PathBuf {
    PathBuf::from("/var/cache/zeppelin")
}
fn default_max_size_gb() -> u64 {
    50
}
fn default_num_centroids() -> usize {
    256
}
fn default_nprobe() -> usize {
    16
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
fn default_pq_m() -> usize {
    8
}
fn default_rerank_factor() -> usize {
    4
}
fn default_beam_width() -> usize {
    10
}
fn default_bitmap_index() -> bool {
    true
}
fn default_compaction_interval() -> u64 {
    30
}
fn default_max_wal_fragments() -> usize {
    1000
}
fn default_retrain_threshold() -> f64 {
    5.0
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_log_format() -> String {
    "json".to_string()
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
            default_top_k: default_top_k(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: StorageBackend::default(),
            bucket: default_bucket(),
            s3_region: None,
            s3_endpoint: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_allow_http: false,
            gcs_service_account_path: None,
            azure_account: None,
            azure_access_key: None,
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            dir: default_cache_dir(),
            max_size_gb: default_max_size_gb(),
            eviction: EvictionPolicy::default(),
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
            quantization: Default::default(),
            pq_m: default_pq_m(),
            rerank_factor: default_rerank_factor(),
            hierarchical: false,
            beam_width: default_beam_width(),
            leaf_size: None,
            bitmap_index: default_bitmap_index(),
            fts_index: false,
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

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}

/// CPU budget for distributing workers across runtimes.
///
/// Detected at startup via `available_parallelism()`, then allocated:
/// - **Query workers**: 2× CPUs (overcommit OK — 80%+ I/O-blocked on S3 GETs)
/// - **Compaction workers**: CPUs - 1 (CPU-bound k-means/FTS, reserve 1 for queries)
/// - **Rayon threads**: match physical cores (work-stealing at core count)
#[derive(Debug, Clone)]
pub struct CpuBudget {
    pub query_workers: usize,
    pub compaction_workers: usize,
    pub rayon_threads: usize,
}

impl CpuBudget {
    /// Auto-detect CPU count and allocate budgets.
    pub fn auto() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        let mut budget = Self {
            query_workers: (cpus * 2).max(4),
            compaction_workers: cpus.saturating_sub(1).max(1),
            rayon_threads: cpus,
        };

        // Allow env var overrides
        if let Some(v) = std::env::var("ZEPPELIN_QUERY_WORKERS")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            budget.query_workers = v;
        }
        if let Some(v) = std::env::var("ZEPPELIN_COMPACTION_WORKERS")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            budget.compaction_workers = v;
        }
        if let Some(v) = std::env::var("ZEPPELIN_RAYON_THREADS")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            budget.rayon_threads = v;
        }

        budget
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
        if let Some(v) = std::env::var("ZEPPELIN_DEFAULT_TOP_K")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            self.server.default_top_k = v;
        }

        // Storage
        if let Ok(v) = std::env::var("STORAGE_BACKEND") {
            match v.to_lowercase().as_str() {
                "s3" => self.storage.backend = StorageBackend::S3,
                "gcs" => self.storage.backend = StorageBackend::Gcs,
                "azure" => self.storage.backend = StorageBackend::Azure,
                "local" => self.storage.backend = StorageBackend::Local,
                _ => tracing::warn!("Unknown STORAGE_BACKEND value: {v}"),
            }
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

        // Indexing (continued)
        if let Ok(v) = std::env::var("ZEPPELIN_QUANTIZATION") {
            match v.to_lowercase().as_str() {
                "none" => {
                    self.indexing.quantization = crate::index::quantization::QuantizationType::None
                }
                "scalar" | "sq8" => {
                    self.indexing.quantization =
                        crate::index::quantization::QuantizationType::Scalar
                }
                "product" | "pq" => {
                    self.indexing.quantization =
                        crate::index::quantization::QuantizationType::Product
                }
                _ => tracing::warn!("Unknown ZEPPELIN_QUANTIZATION value: {v}"),
            }
        }
        if let Ok(v) = std::env::var("ZEPPELIN_BITMAP_INDEX") {
            self.indexing.bitmap_index = v == "true";
        }
        if let Ok(v) = std::env::var("ZEPPELIN_FTS_INDEX") {
            self.indexing.fts_index = v == "true";
        }
        // Hierarchical indexing
        if let Ok(v) = std::env::var("ZEPPELIN_HIERARCHICAL") {
            self.indexing.hierarchical = v == "true";
        }
        if let Some(v) = std::env::var("ZEPPELIN_BEAM_WIDTH")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            self.indexing.beam_width = v;
        }
        if let Some(v) = std::env::var("ZEPPELIN_LEAF_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            self.indexing.leaf_size = Some(v);
        }

        // Compaction
        if let Some(v) = std::env::var("ZEPPELIN_COMPACTION_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            self.compaction.interval_secs = v;
        }
        if let Some(v) = std::env::var("ZEPPELIN_MAX_WAL_FRAGMENTS")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            self.compaction.max_wal_fragments_before_compact = v;
        }

        // Logging
        if let Ok(v) = std::env::var("ZEPPELIN_LOG_FORMAT") {
            self.logging.format = v;
        }
    }
}
