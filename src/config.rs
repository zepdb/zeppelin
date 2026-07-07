use crate::error::{Result, ZeppelinError};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::net::IpAddr;
use std::path::PathBuf;
use std::str::FromStr;

const RERANK_COALESCE_GAP_ENV: &str = "ZEPPELIN_RERANK_COALESCE_GAP_BYTES";
const HYDRATION_POLICY_ENV: &str = "ZEPPELIN_HYDRATION_POLICY";
const HYDRATION_HEAT_QUERIES_ENV: &str = "ZEPPELIN_HYDRATION_HEAT_QUERIES";
const HYDRATION_HEAT_WINDOW_SECS_ENV: &str = "ZEPPELIN_HYDRATION_HEAT_WINDOW_SECS";
const GC_HORIZON_SECS_ENV: &str = "ZEPPELIN_GC_HORIZON_SECS";
const GC_COMPACTION_UPLOAD_WINDOW_SECS_ENV: &str = "ZEPPELIN_GC_COMPACTION_UPLOAD_WINDOW_SECS";
const GC_SKEW_SLOP_SECS_ENV: &str = "ZEPPELIN_GC_SKEW_SLOP_SECS";
const GC_ALLOW_UNSAFE_SHORT_HORIZON_ENV: &str = "ZEPPELIN_GC_ALLOW_UNSAFE_SHORT_HORIZON";
const GC_MANIFEST_HISTORY_KEEP_COUNT_ENV: &str = "ZEPPELIN_GC_MANIFEST_HISTORY_KEEP_COUNT";

/// Default maximum gap, in bytes, between rerank f32 ranges that are merged
/// into one physical GET.
///
/// `ZEPPELIN_RERANK_COALESCE_GAP_BYTES` is the throughput <-> request-cost
/// dial for the two-phase fetch: rerank f32 ranges whose gap is smaller than
/// this are merged into one physical GET. Recall is unaffected at any setting
/// (the candidate set is identical; only the fetch plan changes).
///
/// The default is now 1 MiB. The 128 KiB "knee" below was measured on
/// dbpedia100k np16 against loopback MinIO, so it is a Mac-loopback-MinIO
/// local optimum. On real S3, GETs are the dominant query cost ($0.40 per
/// million requests, in-region bytes free), so fewer, fatter GETs usually win.
///
/// Measured points (dbpedia100k np16, 8 workers; scale GETs ~2.3x for a 1M
/// corpus):
///
///   gap        GETs/q   MB/q   QPS    ~$/M queries (S3 Standard)
///   1 MiB       19.5    49.5    8.3     $7.80   <- default: cost-optimized
///   512 KiB     30.6    41.4    9.8    $12.24
///   256 KiB     50.4    34.1   11.6    $20.16
///   128 KiB     79.9    28.6   13.4    $31.96   <- loopback throughput knee
///   64 KiB     127.5    25.2    8.8    $51.00   <- past the knee; never use
///
/// Examples:
///   ZEPPELIN_RERANK_COALESCE_GAP_BYTES=1048576  # 1 MiB: ~4x cheaper per
///       query than 128 KiB at ~60% of its loopback QPS; right when request
///       cost dominates (high query volume, S3 Standard).
///   ZEPPELIN_RERANK_COALESCE_GAP_BYTES=131072   # 128 KiB: max QPS on this
///       loopback bench; right when node count / latency dominates cost.
///
/// These numbers are from loopback MinIO (~410 MB/s wall). Real S3 has higher
/// per-request latency but wider aggregate bandwidth, which pushes the optimal
/// gap UP (fewer, fatter GETs); S3 Express One Zone halves request price and
/// cuts first-byte latency, pushing it back DOWN. Re-run the gap sweep
/// (qpsbench with this env var) on the target deployment before fixing a value.
pub const DEFAULT_RERANK_COALESCE_GAP_BYTES: usize = 1024 * 1024;

/// Top-level application configuration loaded from a TOML file, env vars, or defaults.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// HTTP server settings (host, port, limits).
    #[serde(default)]
    pub server: ServerConfig,
    /// Object storage backend and credentials.
    #[serde(default)]
    pub storage: StorageConfig,
    /// Local disk and in-memory cache settings.
    #[serde(default)]
    pub cache: CacheConfig,
    /// Vector indexing parameters (centroids, quantization, hierarchical).
    #[serde(default)]
    pub indexing: IndexingConfig,
    /// Background compaction schedule and thresholds.
    #[serde(default)]
    pub compaction: CompactionConfig,
    /// Structured logging level and format.
    #[serde(default)]
    pub logging: LoggingConfig,
    /// Write-ahead log batching configuration.
    #[serde(default)]
    pub wal: WalConfig,
    /// Query-time tuning knobs.
    #[serde(default)]
    pub query: QueryConfig,
    /// Garbage-collection safety horizons and interlocks.
    #[serde(default)]
    pub gc: GcConfig,
}

/// Garbage-collection horizon configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcConfig {
    /// Time-since-unreachable grace period GC waits before deleting objects.
    ///
    /// This must be at least:
    /// `manifest_cache_ttl_secs + request_timeout_secs + compaction_upload_window_secs + skew_slop_secs`.
    /// The manifest cache TTL is configured as `cache.manifest_cache_ttl_ms` and rounded
    /// up to whole seconds for this floor. An interval-derived horizon is wrong because
    /// the compaction interval is causally unrelated to the reader-staleness window; the
    /// safe horizon is determined by how long readers may observe old manifests, continue
    /// requests, race object uploads, and disagree on wall clocks.
    #[serde(default = "default_gc_horizon_secs")]
    pub horizon_secs: u64,
    /// Maximum time a compaction cycle may expose uploaded objects before the manifest
    /// update that makes their reachability authoritative. Default: `300`.
    #[serde(default = "default_gc_compaction_upload_window_secs")]
    pub compaction_upload_window_secs: u64,
    /// Wall-clock skew allowance in seconds. Default: `5`.
    #[serde(default = "default_gc_skew_slop_secs")]
    pub skew_slop_secs: u64,
    /// Permit boot with a horizon below the computed safety floor. Default: `false`.
    ///
    /// This is an explicit operator override for emergency or test deployments only.
    /// Boot logs a structured warning when this accepts an unsafe horizon.
    #[serde(default)]
    pub allow_unsafe_short_horizon: bool,
    /// Number of committed manifest snapshots retained by the app-level history log.
    ///
    /// GC treats retained snapshots as live roots. Pruning happens during explicit
    /// GC cycles; once a snapshot is pruned, objects referenced only by that snapshot
    /// become collectible after the normal GC horizon. Default: `128`.
    #[serde(default = "default_gc_manifest_history_keep_count")]
    pub manifest_history_keep_count: usize,
}

/// Query-time configuration loaded at boot.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryConfig {
    /// Maximum gap, in bytes, between rerank f32 ranges merged into one GET.
    ///
    /// `None` means the value was not set in TOML; `Config::load` resolves it
    /// from `cost_latency_profile`, the environment override, or the default.
    #[serde(default)]
    pub rerank_coalesce_gap_bytes: Option<usize>,
    /// Preset profile that resolves to `rerank_coalesce_gap_bytes` at load time.
    #[serde(default)]
    pub cost_latency_profile: Option<CostLatencyProfile>,
}

/// Preset tradeoff profiles for rerank range coalescing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CostLatencyProfile {
    /// Minimize S3 request cost by using fewer, larger rerank GETs.
    LowCost,
    /// Product default, currently equivalent to low cost.
    Balanced,
    /// Prefer loopback-benchmark throughput and lower single-query latency.
    LowLatency,
}

/// Resolve a cost/latency profile to a concrete rerank coalesce gap.
#[must_use]
pub const fn rerank_coalesce_gap_bytes_for_profile(profile: CostLatencyProfile) -> usize {
    match profile {
        CostLatencyProfile::LowCost | CostLatencyProfile::Balanced => {
            DEFAULT_RERANK_COALESCE_GAP_BYTES
        }
        CostLatencyProfile::LowLatency => 128 * 1024,
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        original: Vec<(&'static str, Option<OsString>)>,
    }

    impl EnvGuard {
        fn clear() -> Self {
            let env_names = [
                RERANK_COALESCE_GAP_ENV,
                HYDRATION_POLICY_ENV,
                HYDRATION_HEAT_QUERIES_ENV,
                HYDRATION_HEAT_WINDOW_SECS_ENV,
                GC_HORIZON_SECS_ENV,
                GC_COMPACTION_UPLOAD_WINDOW_SECS_ENV,
                GC_SKEW_SLOP_SECS_ENV,
                GC_ALLOW_UNSAFE_SHORT_HORIZON_ENV,
                "ZEPPELIN_QUERY_WORKERS",
                "ZEPPELIN_COMPACTION_WORKERS",
                "ZEPPELIN_RAYON_THREADS",
                "ZEPPELIN_HOST",
                "ZEPPELIN_PORT",
                "ZEPPELIN_REQUEST_TIMEOUT_SECS",
                "ZEPPELIN_MAX_CONCURRENT_QUERIES",
                "ZEPPELIN_MAX_BATCH_SIZE",
                "ZEPPELIN_MAX_QUERY_BATCH_SIZE",
                "ZEPPELIN_MAX_TOP_K",
                "ZEPPELIN_SHUTDOWN_TIMEOUT_SECS",
                "ZEPPELIN_MAX_DIMENSIONS",
                "ZEPPELIN_MAX_VECTOR_ID_LENGTH",
                "ZEPPELIN_MAX_REQUEST_BODY_MB",
                "ZEPPELIN_DEFAULT_TOP_K",
                "ZEPPELIN_RATE_LIMIT_RPS",
                "ZEPPELIN_RATE_LIMIT_BURST",
                "ZEPPELIN_WRITE_RATE_LIMIT_RPS",
                "ZEPPELIN_WRITE_RATE_LIMIT_BURST",
                "ZEPPELIN_RATE_LIMIT_IDLE_TTL_SECS",
                "ZEPPELIN_TRUSTED_PROXIES",
                "STORAGE_BACKEND",
                "S3_BUCKET",
                "AWS_REGION",
                "S3_ENDPOINT",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "S3_ALLOW_HTTP",
                "ZEPPELIN_STORAGE_FAIL_FAST",
                "ZEPPELIN_CACHE_DIR",
                "ZEPPELIN_CACHE_MAX_SIZE_GB",
                "ZEPPELIN_MEMORY_CACHE_MAX_MB",
                "ZEPPELIN_MANIFEST_CACHE_TTL_MS",
                "ZEPPELIN_NAMESPACE_REGISTRY_TTL_MS",
                "ZEPPELIN_DEFAULT_NUM_CENTROIDS",
                "ZEPPELIN_DEFAULT_NPROBE",
                "ZEPPELIN_QUANTIZATION",
                "ZEPPELIN_BITMAP_INDEX",
                "ZEPPELIN_FTS_INDEX",
                "ZEPPELIN_BM25_MAX_FULL_SCAN_CLUSTERS",
                "ZEPPELIN_BM25_MAX_FULL_SCAN_VECTORS",
                "ZEPPELIN_HIERARCHICAL",
                "ZEPPELIN_LEAF_SIZE",
                "ZEPPELIN_COMPACTION_INTERVAL_SECS",
                "ZEPPELIN_MAX_WAL_FRAGMENTS",
                "ZEPPELIN_MAX_WAL_AGE_SECS",
                "ZEPPELIN_MAX_WAL_BYTES",
                "ZEPPELIN_MAX_PENDING_DELETES",
                "ZEPPELIN_MAX_OLD_SEGMENTS",
                "ZEPPELIN_LOG_FORMAT",
            ];
            let original = env_names
                .into_iter()
                .map(|name| {
                    let value = std::env::var_os(name);
                    std::env::remove_var(name);
                    (name, value)
                })
                .collect();
            Self { original }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (name, value) in self.original.drain(..) {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }

    fn load_toml(contents: &str) -> Result<Config> {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), contents).unwrap();
        Config::load(Some(file.path().to_str().unwrap()))
    }

    fn assert_config_error_contains(result: Result<Config>, needles: &[&str]) {
        let err = result.unwrap_err();
        let message = err.to_string();
        for needle in needles {
            assert!(
                message.contains(needle),
                "expected config error to contain {needle:?}, got: {message}"
            );
        }
    }

    #[test]
    fn env_override_rejects_present_but_unparseable_port() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::clear();

        std::env::set_var("ZEPPELIN_PORT", "80eighty");

        assert_config_error_contains(load_toml(""), &["ZEPPELIN_PORT", "80eighty"]);
    }

    #[test]
    fn toml_unknown_key_is_startup_error() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::clear();

        assert_config_error_contains(
            load_toml(
                r#"
                [server]
                max_topk = 5
                "#,
            ),
            &["max_topk"],
        );
    }

    #[test]
    fn validate_reports_each_cross_field_rule() {
        type ValidateCase = (&'static str, Box<dyn Fn(&mut Config)>, Vec<&'static str>);
        let cases: Vec<ValidateCase> = vec![
            (
                "server port must be nonzero",
                Box::new(|config| config.server.port = 0),
                vec!["server.port"],
            ),
            (
                "read rate limit burst must be nonzero when rps is enabled",
                Box::new(|config| {
                    config.server.rate_limit_rps = 10;
                    config.server.rate_limit_burst = 0;
                }),
                vec!["server.rate_limit_burst"],
            ),
            (
                "write rate limit burst must be nonzero when rps is enabled",
                Box::new(|config| {
                    config.server.write_rate_limit_rps = 10;
                    config.server.write_rate_limit_burst = 0;
                }),
                vec!["server.write_rate_limit_burst"],
            ),
            (
                "rate limiter idle ttl must be nonzero",
                Box::new(|config| config.server.rate_limit_idle_ttl_secs = 0),
                vec!["server.rate_limit_idle_ttl_secs"],
            ),
            (
                "trusted proxies must be CIDRs",
                Box::new(|config| config.server.trusted_proxies = vec!["127.0.0.1".to_string()]),
                vec!["server.trusted_proxies"],
            ),
            (
                "default nprobe must not exceed max nprobe",
                Box::new(|config| {
                    config.indexing.max_nprobe = 4;
                    config.indexing.default_nprobe = 5;
                }),
                vec!["indexing.default_nprobe", "indexing.max_nprobe"],
            ),
            (
                "default top k must be nonzero",
                Box::new(|config| config.server.default_top_k = 0),
                vec!["server.default_top_k"],
            ),
            (
                "default top k must not exceed max top k",
                Box::new(|config| {
                    config.server.max_top_k = 4;
                    config.server.default_top_k = 5;
                }),
                vec!["server.default_top_k", "server.max_top_k"],
            ),
            (
                "max wal age threshold must be nonzero",
                Box::new(|config| config.compaction.max_wal_age_before_compact_secs = 0),
                vec!["compaction.max_wal_age_before_compact_secs"],
            ),
            (
                "max wal bytes threshold must be nonzero",
                Box::new(|config| config.compaction.max_wal_bytes_before_compact = 0),
                vec!["compaction.max_wal_bytes_before_compact"],
            ),
            (
                "max wal fragment threshold must be at least one",
                Box::new(|config| config.compaction.max_wal_fragments_before_compact = 0),
                vec!["compaction.max_wal_fragments_before_compact"],
            ),
            (
                "request timeout must be nonzero",
                Box::new(|config| config.server.request_timeout_secs = 0),
                vec!["server.request_timeout_secs"],
            ),
            (
                "shutdown timeout must be nonzero",
                Box::new(|config| config.server.shutdown_timeout_secs = 0),
                vec!["server.shutdown_timeout_secs"],
            ),
            (
                "lease duration must be nonzero",
                Box::new(|config| config.compaction.lease_duration_secs = 0),
                vec!["compaction.lease_duration_secs"],
            ),
            (
                "gc compaction upload window must be nonzero",
                Box::new(|config| config.gc.compaction_upload_window_secs = 0),
                vec!["gc.compaction_upload_window_secs"],
            ),
            (
                "gc manifest history keep count must be nonzero",
                Box::new(|config| config.gc.manifest_history_keep_count = 0),
                vec!["gc.manifest_history_keep_count"],
            ),
            (
                "existing cache validation remains enforced",
                Box::new(|config| config.cache.hydration_parallelism = 0),
                vec!["cache.hydration_parallelism"],
            ),
        ];

        for (name, mutate, needles) in cases {
            let mut config = Config::default();
            mutate(&mut config);
            let err = config.validate().unwrap_err();
            let message = err.to_string();
            for needle in needles {
                assert!(
                    message.contains(needle),
                    "{name}: expected {needle:?} in {message}"
                );
            }
        }
    }

    #[test]
    fn validate_reports_all_violations_at_once() {
        let mut config = Config::default();
        config.server.port = 0;
        config.server.request_timeout_secs = 0;
        config.server.default_top_k = 0;
        config.indexing.max_nprobe = 4;
        config.indexing.default_nprobe = 5;
        config.compaction.max_wal_bytes_before_compact = 0;
        config.cache.hydration_parallelism = 0;

        let err = config.validate().unwrap_err();
        let message = err.to_string();
        for needle in [
            "server.port",
            "server.request_timeout_secs",
            "server.default_top_k",
            "indexing.default_nprobe",
            "compaction.max_wal_bytes_before_compact",
            "cache.hydration_parallelism",
        ] {
            assert!(
                message.contains(needle),
                "expected aggregate validation error to contain {needle:?}, got: {message}"
            );
        }
    }

    #[test]
    fn default_max_nprobe_covers_default_centroid_count() {
        let config = Config::default();

        assert_eq!(config.indexing.default_num_centroids, 256);
        assert!(
            config.indexing.max_nprobe >= config.indexing.default_num_centroids,
            "default max_nprobe must allow probing all default centroids"
        );
    }

    #[test]
    fn query_config_parses_explicit_gap_and_defaults_when_absent() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::clear();

        let explicit = load_toml(
            r#"
            [query]
            rerank_coalesce_gap_bytes = 4096
            "#,
        )
        .unwrap();
        assert_eq!(explicit.effective_rerank_coalesce_gap_bytes(), 4096);

        let absent = load_toml("").unwrap();
        assert_eq!(
            absent.effective_rerank_coalesce_gap_bytes(),
            DEFAULT_RERANK_COALESCE_GAP_BYTES
        );
    }

    #[test]
    fn cost_latency_profiles_map_to_expected_gaps() {
        assert_eq!(
            rerank_coalesce_gap_bytes_for_profile(CostLatencyProfile::LowCost),
            1_048_576
        );
        assert_eq!(
            rerank_coalesce_gap_bytes_for_profile(CostLatencyProfile::Balanced),
            1_048_576
        );
        assert_eq!(
            rerank_coalesce_gap_bytes_for_profile(CostLatencyProfile::LowLatency),
            131_072
        );
    }

    #[test]
    fn query_file_rejects_mutually_exclusive_gap_and_profile() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::clear();

        let err = load_toml(
            r#"
            [query]
            rerank_coalesce_gap_bytes = 4096
            cost_latency_profile = "low_latency"
            "#,
        )
        .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("query.rerank_coalesce_gap_bytes"));
        assert!(message.contains("query.cost_latency_profile"));
    }

    #[test]
    fn rerank_gap_env_overrides_file_and_rejects_malformed_value() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::clear();
        let contents = r#"
            [query]
            rerank_coalesce_gap_bytes = 4096
        "#;

        std::env::set_var(RERANK_COALESCE_GAP_ENV, "8192");
        let overridden = load_toml(contents).unwrap();
        assert_eq!(overridden.effective_rerank_coalesce_gap_bytes(), 8192);

        std::env::set_var(RERANK_COALESCE_GAP_ENV, "not-a-number");
        let err = load_toml(contents).unwrap_err();
        assert!(matches!(err, ZeppelinError::Config(_)));
        assert!(err.to_string().contains(RERANK_COALESCE_GAP_ENV));
    }

    #[test]
    fn rerank_gap_zero_is_valid_at_load() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::clear();

        let config = load_toml(
            r#"
            [query]
            rerank_coalesce_gap_bytes = 0
            "#,
        )
        .unwrap();

        assert_eq!(config.effective_rerank_coalesce_gap_bytes(), 0);
    }

    #[test]
    fn test_unknown_policy_name_fails_boot() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::clear();

        let err = load_toml(
            r#"
            [cache]
            hydration_policy = "bogus"
            "#,
        )
        .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("hydration_policy"));
        assert!(message.contains("bogus"));
    }

    #[test]
    fn hydration_policy_defaults_and_parses() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::clear();

        let defaulted = load_toml("").unwrap();
        assert_eq!(
            defaulted.cache.hydration_policy,
            HydrationPolicyKind::SessionWindow
        );
        assert!(!defaulted.cache.hydration_enabled);
        assert_eq!(defaulted.cache.hydration_heat_queries, 3);
        assert_eq!(defaulted.cache.hydration_heat_window_secs, 60);
        assert_eq!(defaulted.cache.hydration_parallelism, 4);
        assert_eq!(defaulted.cache.hydration_max_segment_fraction, 0.5);

        let explicit = load_toml(
            r#"
            [cache]
            hydration_enabled = true
            hydration_policy = "session_window"
            hydration_heat_queries = 5
            hydration_heat_window_secs = 90
            hydration_parallelism = 8
            hydration_max_segment_fraction = 0.25
            "#,
        )
        .unwrap();
        assert!(explicit.cache.hydration_enabled);
        assert_eq!(explicit.cache.hydration_heat_queries, 5);
        assert_eq!(explicit.cache.hydration_heat_window_secs, 90);
        assert_eq!(explicit.cache.hydration_parallelism, 8);
        assert_eq!(explicit.cache.hydration_max_segment_fraction, 0.25);
    }

    #[test]
    fn gc_horizon_below_floor_is_rejected_with_all_inputs() {
        let mut config = Config::default();
        config.cache.manifest_cache_ttl_ms = 2_500;
        config.server.request_timeout_secs = 30;
        config.gc.compaction_upload_window_secs = 20;
        config.gc.skew_slop_secs = 3;
        config.gc.horizon_secs = 55;

        let err = config.validate().unwrap_err();
        let message = err.to_string();
        for needle in [
            "gc.horizon_secs (55)",
            "cache.manifest_cache_ttl_ms (2500ms => 3s)",
            "server.request_timeout_secs (30)",
            "gc.compaction_upload_window_secs (20)",
            "gc.skew_slop_secs (3)",
            "floor (56)",
            "gc.allow_unsafe_short_horizon",
        ] {
            assert!(
                message.contains(needle),
                "expected horizon floor error to contain {needle:?}, got: {message}"
            );
        }
    }

    #[test]
    fn gc_horizon_override_accepts_short_horizon_and_warns() {
        let mut config = Config::default();
        config.cache.manifest_cache_ttl_ms = 1_000;
        config.server.request_timeout_secs = 30;
        config.gc.compaction_upload_window_secs = 20;
        config.gc.skew_slop_secs = 5;
        config.gc.horizon_secs = 10;
        config.gc.allow_unsafe_short_horizon = true;

        config.validate().unwrap();
        assert_eq!(config.gc_horizon_floor_secs(), Some(56));
        assert!(config.gc_horizon_is_unsafe_short());
        config.warn_if_unsafe_gc_horizon_override();
    }

    #[test]
    fn default_gc_horizon_passes_floor() {
        let config = Config::default();

        config.validate().unwrap();
        assert!(config.gc.horizon_secs >= config.gc_horizon_floor_secs().unwrap());
        assert!(!config.gc.allow_unsafe_short_horizon);
    }

    #[test]
    fn gc_config_toml_roundtrips_and_doc_mentions_floor() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::clear();

        let config = load_toml(
            r#"
            [gc]
            horizon_secs = 120
            compaction_upload_window_secs = 15
            skew_slop_secs = 4
            allow_unsafe_short_horizon = true
            manifest_history_keep_count = 7
            "#,
        )
        .unwrap();

        assert_eq!(config.gc.horizon_secs, 120);
        assert_eq!(config.gc.compaction_upload_window_secs, 15);
        assert_eq!(config.gc.skew_slop_secs, 4);
        assert!(config.gc.allow_unsafe_short_horizon);
        assert_eq!(config.gc.manifest_history_keep_count, 7);

        let source = include_str!("config.rs");
        assert!(source.contains("manifest_cache_ttl_secs + request_timeout_secs + compaction_upload_window_secs + skew_slop_secs"));
        assert!(source.contains("causally unrelated to the reader-staleness window"));
    }

    #[test]
    fn old_compaction_upload_window_toml_is_rejected_as_removed() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::clear();

        let err = load_toml(
            r#"
            [compaction]
            compaction_upload_window_secs = 15
            "#,
        )
        .unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("unknown field") && message.contains("compaction_upload_window_secs"),
            "removed compaction upload-window key must fail as an unknown field, got: {message}"
        );
    }
}

/// WAL configuration.
///
/// Group commit (coalescing concurrent appends into a shared manifest CAS) is
/// now unconditional in `WalWriter` — there is no batching knob to tune. The
/// former `batch_manifest_size` / `batch_manifest_timeout_ms` fields are gone.
/// With strict boot config, stale WAL keys are rejected instead of ignored. The
/// struct is retained as the home for future WAL settings.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalConfig {}

/// HTTP server configuration including bind address, timeouts, and request limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Bind address for the HTTP server. Default: `"0.0.0.0"`.
    #[serde(default = "default_host")]
    pub host: String,
    /// TCP port to listen on. Default: `8080`.
    #[serde(default = "default_port")]
    pub port: u16,
    /// Per-request timeout in seconds. Default: `30`.
    #[serde(default = "default_request_timeout")]
    pub request_timeout_secs: u64,
    /// Maximum number of concurrent query handlers. Default: `64`.
    #[serde(default = "default_max_concurrent_queries")]
    pub max_concurrent_queries: usize,
    /// Maximum vectors per upsert batch. Default: `50_000`.
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,
    /// Maximum query entries per batch query request. Default: `256`.
    #[serde(default = "default_max_query_batch_size")]
    pub max_query_batch_size: usize,
    /// Hard upper bound on `top_k` query parameter. Default: `10_000`.
    #[serde(default = "default_max_top_k")]
    pub max_top_k: usize,
    /// Graceful shutdown timeout in seconds. Default: `30`.
    #[serde(default = "default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,
    /// Maximum allowed vector dimensionality. Default: `65_536`.
    #[serde(default = "default_max_dimensions")]
    pub max_dimensions: usize,
    /// Maximum byte length for vector IDs. Default: `1024`.
    #[serde(default = "default_max_vector_id_length")]
    pub max_vector_id_length: usize,
    /// Maximum request body size in megabytes. Default: `512`.
    #[serde(default = "default_max_request_body_mb")]
    pub max_request_body_mb: usize,
    /// Default `top_k` when the client omits it. Default: `10`.
    #[serde(default = "default_top_k")]
    pub default_top_k: usize,
    /// Maximum sustained read/query requests per second per client. Default: `100`.
    #[serde(default = "default_rate_limit_rps")]
    pub rate_limit_rps: u32,
    /// Maximum read/query burst capacity per client. Default: `200`.
    #[serde(default = "default_rate_limit_burst")]
    pub rate_limit_burst: u32,
    /// Maximum sustained write/admin requests per second per client. Default: `50`.
    #[serde(default = "default_write_rate_limit_rps")]
    pub write_rate_limit_rps: u32,
    /// Maximum write/admin burst capacity per client. Default: `100`.
    #[serde(default = "default_write_rate_limit_burst")]
    pub write_rate_limit_burst: u32,
    /// Idle token-bucket TTL in seconds. Default: `600`.
    #[serde(default = "default_rate_limit_idle_ttl_secs")]
    pub rate_limit_idle_ttl_secs: u64,
    /// Trusted proxy CIDR ranges whose X-Forwarded-For headers are honored.
    #[serde(default)]
    pub trusted_proxies: Vec<String>,
}

fn default_top_k() -> usize {
    10
}
fn default_rate_limit_rps() -> u32 {
    100
}
fn default_rate_limit_burst() -> u32 {
    200
}
fn default_write_rate_limit_rps() -> u32 {
    50
}
fn default_write_rate_limit_burst() -> u32 {
    100
}
fn default_rate_limit_idle_ttl_secs() -> u64 {
    600
}
fn default_max_query_batch_size() -> usize {
    256
}

fn is_valid_ip_cidr(value: &str) -> bool {
    let Some((ip, prefix)) = value.split_once('/') else {
        return false;
    };
    let Ok(ip) = ip.parse::<IpAddr>() else {
        return false;
    };
    let Ok(prefix) = prefix.parse::<u8>() else {
        return false;
    };
    let max_prefix = match ip {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    };
    prefix <= max_prefix
}

/// Supported object storage backends.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend {
    /// Amazon S3, MinIO, or any S3-compatible endpoint (default).
    #[default]
    S3,
    /// Google Cloud Storage.
    Gcs,
    /// Azure Blob Storage.
    Azure,
    /// Local filesystem (development/testing only).
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

/// Object storage backend selection and credential configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// Which storage backend to use. Default: `S3`.
    #[serde(default)]
    pub backend: StorageBackend,
    /// Bucket (or container) name. Default: `"zeppelin"`.
    #[serde(default = "default_bucket")]
    pub bucket: String,

    // S3 / MinIO / R2
    /// AWS region for S3 (e.g. `"us-east-1"`).
    #[serde(default)]
    pub s3_region: Option<String>,
    /// Custom S3-compatible endpoint URL (MinIO, R2, etc.).
    #[serde(default)]
    pub s3_endpoint: Option<String>,
    /// AWS access key ID for static credentials.
    #[serde(default)]
    pub s3_access_key_id: Option<String>,
    /// AWS secret access key for static credentials.
    #[serde(default)]
    pub s3_secret_access_key: Option<String>,
    /// Allow plain HTTP (non-TLS) connections to S3. Default: `false`.
    #[serde(default)]
    pub s3_allow_http: bool,
    /// Probe storage during boot and refuse to serve if it is unavailable. Default: `true`.
    #[serde(default = "default_storage_fail_fast")]
    pub fail_fast: bool,
}

/// Local disk and in-memory cache settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    /// Directory for on-disk cache files. Default: `/var/cache/zeppelin`.
    #[serde(default = "default_cache_dir")]
    pub dir: PathBuf,
    /// Maximum disk cache size in gigabytes. Default: `50`.
    #[serde(default = "default_max_size_gb")]
    pub max_size_gb: u64,
    /// Maximum memory cache size in MB. Set to 0 to disable.
    /// Default: 256 MB. Override via ZEPPELIN_MEMORY_CACHE_MAX_MB.
    #[serde(default = "default_memory_cache_max_mb")]
    pub memory_cache_max_mb: usize,
    /// Manifest cache TTL in milliseconds. Default: `500`.
    #[serde(default = "default_manifest_cache_ttl_ms")]
    pub manifest_cache_ttl_ms: u64,
    /// Namespace metadata positive-cache TTL in milliseconds. Default: `5000`.
    #[serde(default = "default_namespace_registry_ttl_ms")]
    pub namespace_registry_ttl_ms: u64,
    /// Enable background warm-set hydration. Default: `false` (dark launch).
    #[serde(default)]
    pub hydration_enabled: bool,
    /// Boot-selected hydration heat policy.
    ///
    /// Per-namespace policy selection is intentionally deferred until real
    /// traffic validates the need; adding it now would multiply the test
    /// matrix before the policy surface has production feedback.
    #[serde(default = "default_hydration_policy")]
    pub hydration_policy: HydrationPolicyKind,
    /// Query observations required inside the heat window before hydration.
    #[serde(default = "default_hydration_heat_queries")]
    pub hydration_heat_queries: u64,
    /// Heat window length in seconds. Default: `60`.
    #[serde(default = "default_hydration_heat_window_secs")]
    pub hydration_heat_window_secs: u64,
    /// Maximum concurrent object downloads per hydration job. Default: `4`.
    #[serde(default = "default_hydration_parallelism")]
    pub hydration_parallelism: usize,
    /// Maximum fraction of the disk cache one segment may occupy. Default: `0.5`.
    #[serde(default = "default_hydration_max_segment_fraction")]
    pub hydration_max_segment_fraction: f64,
}

/// Globally selected warm-set hydration heat policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HydrationPolicyKind {
    /// Count queries in a per-namespace session window.
    SessionWindow,
}

/// Vector indexing parameters controlling IVF-Flat, quantization, and hierarchical trees.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexingConfig {
    /// Default number of IVF centroids per segment. Default: `256`.
    #[serde(default = "default_num_centroids")]
    pub default_num_centroids: usize,
    /// Default number of clusters to probe at query time. Default: `16`.
    #[serde(default = "default_nprobe")]
    pub default_nprobe: usize,
    /// Hard upper bound on nprobe to prevent expensive full scans. Default: `256`.
    #[serde(default = "default_max_nprobe")]
    pub max_nprobe: usize,
    /// Maximum k-means iterations during centroid training. Default: `25`.
    #[serde(default = "default_kmeans_max_iterations")]
    pub kmeans_max_iterations: usize,
    /// k-means convergence threshold (stop when delta < epsilon). Default: `1e-4`.
    #[serde(default = "default_kmeans_convergence_epsilon")]
    pub kmeans_convergence_epsilon: f64,
    /// Oversampling factor for k-means initialization. Default: `3`.
    #[serde(default = "default_oversample_factor")]
    pub oversample_factor: usize,
    /// Quantization type for vector compression.
    /// Default: Scalar (SQ8) for 4x compression and better cache utilization.
    #[serde(default = "default_quantization")]
    pub quantization: crate::index::quantization::QuantizationType,
    /// Number of PQ subquantizers (only used when quantization = product).
    /// Must divide vector dimension evenly. Default: 8.
    #[serde(default = "default_pq_m")]
    pub pq_m: usize,
    /// Whether to use hierarchical (multi-level centroid tree) indexing.
    /// When true, build produces a hierarchical index instead of flat IVF.
    /// Default: false.
    #[serde(default)]
    pub hierarchical: bool,
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
    /// Maximum clusters to scan in BM25 full-scan fallback before returning an error.
    /// Set to 0 to disable the circuit breaker (allow unlimited scan). Default: 500.
    #[serde(default = "default_bm25_max_full_scan_clusters")]
    pub bm25_max_full_scan_clusters: usize,
    /// Maximum vectors to scan in BM25 full-scan fallback before returning an error.
    /// Set to 0 to disable the vector-count breaker. Default: 100000.
    #[serde(default = "default_bm25_max_full_scan_vectors")]
    pub bm25_max_full_scan_vectors: usize,
}

/// Background WAL-to-segment compaction schedule and thresholds.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionConfig {
    /// Polling interval between compaction checks, in seconds. Default: `30`.
    #[serde(default = "default_compaction_interval")]
    pub interval_secs: u64,
    /// Trigger compaction when pending WAL fragments reach this count. Default: `100`.
    #[serde(default = "default_max_wal_fragments")]
    pub max_wal_fragments_before_compact: usize,
    /// Trigger compaction when the oldest uncompacted WAL fragment is at
    /// least this many seconds old (age derived from the fragment's ULID
    /// timestamp). Guarantees any namespace with pending WAL data converges
    /// to a compacted state within a bounded window, regardless of fragment
    /// count. Default: `300` (5 minutes).
    #[serde(default = "default_max_wal_age_secs")]
    pub max_wal_age_before_compact_secs: u64,
    /// Trigger compaction when total uncompacted WAL bytes reach this
    /// threshold, so few-but-large fragments don't linger under the count
    /// trigger. Fragment sizes are recorded in the manifest at write time
    /// (no extra S3 reads). Default: `67_108_864` (64 MB).
    #[serde(default = "default_max_wal_bytes")]
    pub max_wal_bytes_before_compact: u64,
    /// Ratio of new-to-existing vectors that triggers centroid retraining. Default: `5.0`.
    #[serde(default = "default_retrain_threshold")]
    pub retrain_imbalance_threshold: f64,
    /// Legacy compatibility knob for deferred deletes. Pending-delete entries
    /// are never pruned by count; GC removes them only after delete/absence is
    /// confirmed. Default: 1000.
    #[serde(default = "default_max_pending_deletes")]
    pub max_pending_deletes: usize,
    /// Maximum old (non-active) segments to retain in the manifest.
    /// Default: 10.
    #[serde(default = "default_max_old_segments")]
    pub max_old_segments: usize,
    /// Duration of the per-namespace compaction lease, in seconds.
    /// Prevents multiple nodes from compacting the same namespace
    /// concurrently. Must exceed the longest expected compaction cycle;
    /// if it expires mid-cycle, the fencing token + CAS layers still
    /// prevent a stale commit. Default: `300`.
    #[serde(default = "default_compaction_lease_secs")]
    pub lease_duration_secs: u64,
}

/// Structured logging configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    /// Log verbosity filter (e.g. `"info"`, `"debug"`). Default: `"info"`.
    #[serde(default = "default_log_level")]
    pub level: String,
    /// Output format: `"json"` or `"pretty"`. Default: `"json"`.
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
    50_000
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
    512
}
fn default_bucket() -> String {
    "zeppelin".to_string()
}
fn default_storage_fail_fast() -> bool {
    true
}
fn default_cache_dir() -> PathBuf {
    PathBuf::from("/var/cache/zeppelin")
}
fn default_max_size_gb() -> u64 {
    50
}
fn default_memory_cache_max_mb() -> usize {
    256
}
fn default_manifest_cache_ttl_ms() -> u64 {
    500
}
fn default_namespace_registry_ttl_ms() -> u64 {
    5000
}
fn default_hydration_policy() -> HydrationPolicyKind {
    HydrationPolicyKind::SessionWindow
}
fn default_hydration_heat_queries() -> u64 {
    3
}
fn default_hydration_heat_window_secs() -> u64 {
    60
}
fn default_hydration_parallelism() -> usize {
    4
}
fn default_hydration_max_segment_fraction() -> f64 {
    0.5
}
fn default_num_centroids() -> usize {
    256
}
fn default_nprobe() -> usize {
    16
}
fn default_max_nprobe() -> usize {
    256
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
fn default_quantization() -> crate::index::quantization::QuantizationType {
    crate::index::quantization::QuantizationType::Scalar
}
fn default_bitmap_index() -> bool {
    true
}
fn default_compaction_interval() -> u64 {
    30
}
fn default_max_wal_fragments() -> usize {
    100
}
fn default_max_wal_age_secs() -> u64 {
    300
}
fn default_max_wal_bytes() -> u64 {
    64 * 1024 * 1024
}
fn default_retrain_threshold() -> f64 {
    5.0
}
fn default_bm25_max_full_scan_clusters() -> usize {
    500
}
fn default_bm25_max_full_scan_vectors() -> usize {
    100_000
}
fn default_max_pending_deletes() -> usize {
    1000
}
fn default_max_old_segments() -> usize {
    10
}
fn default_compaction_lease_secs() -> u64 {
    300
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_log_format() -> String {
    "json".to_string()
}
fn default_gc_horizon_secs() -> u64 {
    900
}
fn default_gc_compaction_upload_window_secs() -> u64 {
    300
}
fn default_gc_skew_slop_secs() -> u64 {
    5
}
fn default_gc_manifest_history_keep_count() -> usize {
    128
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            request_timeout_secs: default_request_timeout(),
            max_concurrent_queries: default_max_concurrent_queries(),
            max_batch_size: default_max_batch_size(),
            max_query_batch_size: default_max_query_batch_size(),
            max_top_k: default_max_top_k(),
            shutdown_timeout_secs: default_shutdown_timeout_secs(),
            max_dimensions: default_max_dimensions(),
            max_vector_id_length: default_max_vector_id_length(),
            max_request_body_mb: default_max_request_body_mb(),
            default_top_k: default_top_k(),
            rate_limit_rps: default_rate_limit_rps(),
            rate_limit_burst: default_rate_limit_burst(),
            write_rate_limit_rps: default_write_rate_limit_rps(),
            write_rate_limit_burst: default_write_rate_limit_burst(),
            rate_limit_idle_ttl_secs: default_rate_limit_idle_ttl_secs(),
            trusted_proxies: Vec::new(),
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
            fail_fast: default_storage_fail_fast(),
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            dir: default_cache_dir(),
            max_size_gb: default_max_size_gb(),
            memory_cache_max_mb: default_memory_cache_max_mb(),
            manifest_cache_ttl_ms: default_manifest_cache_ttl_ms(),
            namespace_registry_ttl_ms: default_namespace_registry_ttl_ms(),
            hydration_enabled: false,
            hydration_policy: default_hydration_policy(),
            hydration_heat_queries: default_hydration_heat_queries(),
            hydration_heat_window_secs: default_hydration_heat_window_secs(),
            hydration_parallelism: default_hydration_parallelism(),
            hydration_max_segment_fraction: default_hydration_max_segment_fraction(),
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
            quantization: default_quantization(),
            pq_m: default_pq_m(),
            hierarchical: false,
            leaf_size: None,
            bitmap_index: default_bitmap_index(),
            fts_index: false,
            bm25_max_full_scan_clusters: default_bm25_max_full_scan_clusters(),
            bm25_max_full_scan_vectors: default_bm25_max_full_scan_vectors(),
        }
    }
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            interval_secs: default_compaction_interval(),
            max_wal_fragments_before_compact: default_max_wal_fragments(),
            max_wal_age_before_compact_secs: default_max_wal_age_secs(),
            max_wal_bytes_before_compact: default_max_wal_bytes(),
            retrain_imbalance_threshold: default_retrain_threshold(),
            max_pending_deletes: default_max_pending_deletes(),
            max_old_segments: default_max_old_segments(),
            lease_duration_secs: default_compaction_lease_secs(),
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

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            horizon_secs: default_gc_horizon_secs(),
            compaction_upload_window_secs: default_gc_compaction_upload_window_secs(),
            skew_slop_secs: default_gc_skew_slop_secs(),
            allow_unsafe_short_horizon: false,
            manifest_history_keep_count: default_gc_manifest_history_keep_count(),
        }
    }
}

/// CPU budget for distributing workers across runtimes.
///
/// Detected at startup via `available_parallelism()`, then allocated:
/// - **Query workers**: 2× CPUs (overcommit OK — 80%+ I/O-blocked on S3 GETs)
/// - **Compaction workers**: max(1, CPUs/4) — reserve most cores for queries.
///   On a 4-vCPU c7i.xlarge, this gives 1 compaction worker + 3 for queries.
/// - **Rayon threads**: match physical cores (work-stealing at core count)
#[derive(Debug, Clone)]
pub struct CpuBudget {
    /// Number of tokio workers dedicated to query handling (2x CPUs).
    pub query_workers: usize,
    /// Number of tokio workers dedicated to background compaction (CPUs/4, min 1).
    pub compaction_workers: usize,
    /// Rayon thread pool size for CPU-bound work (matches physical core count).
    pub rayon_threads: usize,
}

impl CpuBudget {
    /// Auto-detect CPU count and allocate budgets.
    pub fn auto() -> Result<Self> {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        // Cap compaction to 25% of cores (max 1 on ≤4 cores) to leave
        // CPU headroom for queries. Compaction is I/O-heavy anyway.
        let mut budget = Self {
            query_workers: (cpus * 2).max(4),
            compaction_workers: (cpus / 4).max(1),
            rayon_threads: cpus,
        };

        // Allow env var overrides
        if let Some(v) = env_override("ZEPPELIN_QUERY_WORKERS")? {
            budget.query_workers = v;
        }
        if let Some(v) = env_override("ZEPPELIN_COMPACTION_WORKERS")? {
            budget.compaction_workers = v;
        }
        if let Some(v) = env_override("ZEPPELIN_RAYON_THREADS")? {
            budget.rayon_threads = v;
        }

        Ok(budget)
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
        config.apply_env_overrides()?;
        config.resolve_query_config()?;
        config.validate()?;
        Ok(config)
    }

    /// Validate all boot-time configuration and report every violation at once.
    pub fn validate(&self) -> Result<()> {
        let mut violations = Vec::new();

        if self.server.port == 0 {
            violations.push("server.port must be greater than zero".to_string());
        }
        if self.server.request_timeout_secs == 0 {
            violations.push("server.request_timeout_secs must be greater than zero".to_string());
        }
        if self.server.shutdown_timeout_secs == 0 {
            violations.push("server.shutdown_timeout_secs must be greater than zero".to_string());
        }
        if self.server.rate_limit_rps > 0 && self.server.rate_limit_burst == 0 {
            violations.push(
                "server.rate_limit_burst must be at least 1 when rate limiting is enabled"
                    .to_string(),
            );
        }
        if self.server.write_rate_limit_rps > 0 && self.server.write_rate_limit_burst == 0 {
            violations.push(
                "server.write_rate_limit_burst must be at least 1 when write rate limiting is enabled"
                    .to_string(),
            );
        }
        if self.server.rate_limit_idle_ttl_secs == 0 {
            violations
                .push("server.rate_limit_idle_ttl_secs must be greater than zero".to_string());
        }
        for proxy in &self.server.trusted_proxies {
            if !is_valid_ip_cidr(proxy) {
                violations.push(format!(
                    "server.trusted_proxies entry {proxy:?} must be an IP CIDR range"
                ));
            }
        }
        if self.server.max_top_k == 0 {
            violations.push("server.max_top_k must be greater than zero".to_string());
        }
        if self.server.default_top_k == 0 {
            violations.push("server.default_top_k must be greater than zero".to_string());
        } else if self.server.default_top_k > self.server.max_top_k {
            violations.push(format!(
                "server.default_top_k ({}) must be <= server.max_top_k ({})",
                self.server.default_top_k, self.server.max_top_k
            ));
        }

        if self.indexing.default_nprobe == 0 {
            violations.push("indexing.default_nprobe must be greater than zero".to_string());
        }
        if self.indexing.max_nprobe == 0 {
            violations.push("indexing.max_nprobe must be greater than zero".to_string());
        }
        if self.indexing.default_nprobe > self.indexing.max_nprobe {
            violations.push(format!(
                "indexing.default_nprobe ({}) must be <= indexing.max_nprobe ({})",
                self.indexing.default_nprobe, self.indexing.max_nprobe
            ));
        }

        if self.compaction.max_wal_age_before_compact_secs == 0 {
            violations.push(
                "compaction.max_wal_age_before_compact_secs must be greater than zero".to_string(),
            );
        }
        if self.compaction.max_wal_bytes_before_compact == 0 {
            violations.push(
                "compaction.max_wal_bytes_before_compact must be greater than zero".to_string(),
            );
        }
        if self.compaction.max_wal_fragments_before_compact == 0 {
            violations
                .push("compaction.max_wal_fragments_before_compact must be at least 1".to_string());
        }
        if self.compaction.lease_duration_secs == 0 {
            violations.push("compaction.lease_duration_secs must be greater than zero".to_string());
        }
        if self.gc.compaction_upload_window_secs == 0 {
            violations
                .push("gc.compaction_upload_window_secs must be greater than zero".to_string());
        }
        if self.gc.manifest_history_keep_count == 0 {
            violations.push("gc.manifest_history_keep_count must be greater than zero".to_string());
        }

        if self.cache.hydration_heat_queries == 0 {
            violations.push("cache.hydration_heat_queries must be greater than zero".to_string());
        }
        if self.cache.hydration_heat_window_secs == 0 {
            violations
                .push("cache.hydration_heat_window_secs must be greater than zero".to_string());
        }
        if self.cache.hydration_parallelism == 0 {
            violations.push("cache.hydration_parallelism must be greater than zero".to_string());
        }
        if !self.cache.hydration_max_segment_fraction.is_finite()
            || self.cache.hydration_max_segment_fraction <= 0.0
            || self.cache.hydration_max_segment_fraction > 1.0
        {
            violations.push(
                "cache.hydration_max_segment_fraction must be finite and in (0, 1]".to_string(),
            );
        }

        match self.checked_gc_horizon_floor_secs() {
            Some(floor_secs)
                if self.gc.horizon_secs < floor_secs
                    && !self.gc.allow_unsafe_short_horizon =>
            {
                violations.push(format!(
                    "gc.horizon_secs ({}) must be >= floor ({}) unless gc.allow_unsafe_short_horizon=true; floor inputs: cache.manifest_cache_ttl_ms ({}ms => {}s), server.request_timeout_secs ({}), gc.compaction_upload_window_secs ({}), gc.skew_slop_secs ({})",
                    self.gc.horizon_secs,
                    floor_secs,
                    self.cache.manifest_cache_ttl_ms,
                    self.manifest_cache_ttl_secs_for_gc_floor(),
                    self.server.request_timeout_secs,
                    self.gc.compaction_upload_window_secs,
                    self.gc.skew_slop_secs
                ));
            }
            Some(_) => {}
            None => violations.push(format!(
                "gc horizon floor overflows u64; floor inputs: cache.manifest_cache_ttl_ms ({}ms => {}s), server.request_timeout_secs ({}), gc.compaction_upload_window_secs ({}), gc.skew_slop_secs ({})",
                self.cache.manifest_cache_ttl_ms,
                self.manifest_cache_ttl_secs_for_gc_floor(),
                self.server.request_timeout_secs,
                self.gc.compaction_upload_window_secs,
                self.gc.skew_slop_secs
            )),
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(ZeppelinError::Config(format!(
                "invalid configuration:\n- {}",
                violations.join("\n- ")
            )))
        }
    }

    /// Minimum safe GC horizon in whole seconds.
    #[must_use]
    pub fn gc_horizon_floor_secs(&self) -> Option<u64> {
        self.checked_gc_horizon_floor_secs()
    }

    /// True when the explicit override permits a horizon below the safety floor.
    #[must_use]
    pub fn gc_horizon_is_unsafe_short(&self) -> bool {
        self.gc.allow_unsafe_short_horizon
            && self
                .checked_gc_horizon_floor_secs()
                .is_some_and(|floor_secs| self.gc.horizon_secs < floor_secs)
    }

    /// Emit the loud boot warning required when the unsafe short-horizon override is active.
    pub fn warn_if_unsafe_gc_horizon_override(&self) {
        let Some(floor_secs) = self.checked_gc_horizon_floor_secs() else {
            return;
        };
        if !self.gc.allow_unsafe_short_horizon || self.gc.horizon_secs >= floor_secs {
            return;
        }

        tracing::warn!(
            gc_horizon_secs = self.gc.horizon_secs,
            gc_horizon_floor_secs = floor_secs,
            cache_manifest_cache_ttl_ms = self.cache.manifest_cache_ttl_ms,
            manifest_cache_ttl_floor_secs = self.manifest_cache_ttl_secs_for_gc_floor(),
            request_timeout_secs = self.server.request_timeout_secs,
            compaction_upload_window_secs = self.gc.compaction_upload_window_secs,
            skew_slop_secs = self.gc.skew_slop_secs,
            allow_unsafe_short_horizon = self.gc.allow_unsafe_short_horizon,
            "accepting unsafe gc horizon below computed safety floor"
        );
    }

    fn checked_gc_horizon_floor_secs(&self) -> Option<u64> {
        self.manifest_cache_ttl_secs_for_gc_floor()
            .checked_add(self.server.request_timeout_secs)?
            .checked_add(self.gc.compaction_upload_window_secs)?
            .checked_add(self.gc.skew_slop_secs)
    }

    fn manifest_cache_ttl_secs_for_gc_floor(&self) -> u64 {
        self.cache.manifest_cache_ttl_ms.div_ceil(1_000)
    }

    fn resolve_query_config(&mut self) -> Result<()> {
        if self.query.rerank_coalesce_gap_bytes.is_some()
            && self.query.cost_latency_profile.is_some()
        {
            return Err(ZeppelinError::Config(
                "query.rerank_coalesce_gap_bytes and query.cost_latency_profile are mutually exclusive; set exactly one".into(),
            ));
        }

        let effective = self
            .query
            .rerank_coalesce_gap_bytes
            .or_else(|| {
                self.query
                    .cost_latency_profile
                    .map(rerank_coalesce_gap_bytes_for_profile)
            })
            .unwrap_or(DEFAULT_RERANK_COALESCE_GAP_BYTES);
        self.query.rerank_coalesce_gap_bytes = Some(effective);
        self.query.cost_latency_profile = None;
        Ok(())
    }

    /// Resolved rerank coalesce gap after file/default/env processing.
    #[must_use]
    pub fn effective_rerank_coalesce_gap_bytes(&self) -> usize {
        self.query
            .rerank_coalesce_gap_bytes
            .unwrap_or(DEFAULT_RERANK_COALESCE_GAP_BYTES)
    }

    /// Apply environment variable overrides on top of file/default values.
    /// This ensures env vars always take priority over TOML settings.
    fn apply_env_overrides(&mut self) -> Result<()> {
        // Server
        if let Some(v) = env_override::<String>("ZEPPELIN_HOST")? {
            self.server.host = v;
        }
        if let Some(v) = env_override("ZEPPELIN_PORT")? {
            self.server.port = v;
        }
        if let Some(v) = env_override("ZEPPELIN_REQUEST_TIMEOUT_SECS")? {
            self.server.request_timeout_secs = v;
        }
        if let Some(v) = env_override("ZEPPELIN_MAX_CONCURRENT_QUERIES")? {
            self.server.max_concurrent_queries = v;
        }
        if let Some(v) = env_override("ZEPPELIN_MAX_BATCH_SIZE")? {
            self.server.max_batch_size = v;
        }
        if let Some(v) = env_override("ZEPPELIN_MAX_QUERY_BATCH_SIZE")? {
            self.server.max_query_batch_size = v;
        }
        if let Some(v) = env_override("ZEPPELIN_MAX_TOP_K")? {
            self.server.max_top_k = v;
        }
        if let Some(v) = env_override("ZEPPELIN_SHUTDOWN_TIMEOUT_SECS")? {
            self.server.shutdown_timeout_secs = v;
        }
        if let Some(v) = env_override("ZEPPELIN_MAX_DIMENSIONS")? {
            self.server.max_dimensions = v;
        }
        if let Some(v) = env_override("ZEPPELIN_MAX_VECTOR_ID_LENGTH")? {
            self.server.max_vector_id_length = v;
        }
        if let Some(v) = env_override("ZEPPELIN_MAX_REQUEST_BODY_MB")? {
            self.server.max_request_body_mb = v;
        }
        if let Some(v) = env_override("ZEPPELIN_DEFAULT_TOP_K")? {
            self.server.default_top_k = v;
        }
        if let Some(v) = env_override("ZEPPELIN_RATE_LIMIT_RPS")? {
            self.server.rate_limit_rps = v;
        }
        if let Some(v) = env_override("ZEPPELIN_RATE_LIMIT_BURST")? {
            self.server.rate_limit_burst = v;
        }
        if let Some(v) = env_override("ZEPPELIN_WRITE_RATE_LIMIT_RPS")? {
            self.server.write_rate_limit_rps = v;
        }
        if let Some(v) = env_override("ZEPPELIN_WRITE_RATE_LIMIT_BURST")? {
            self.server.write_rate_limit_burst = v;
        }
        if let Some(v) = env_override("ZEPPELIN_RATE_LIMIT_IDLE_TTL_SECS")? {
            self.server.rate_limit_idle_ttl_secs = v;
        }
        if let Some(v) = env_override::<String>("ZEPPELIN_TRUSTED_PROXIES")? {
            self.server.trusted_proxies = v
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(ToOwned::to_owned)
                .collect();
        }

        // Storage
        if let Some(v) = env_override::<String>("STORAGE_BACKEND")? {
            match v.to_lowercase().as_str() {
                "s3" => self.storage.backend = StorageBackend::S3,
                "gcs" => self.storage.backend = StorageBackend::Gcs,
                "azure" => self.storage.backend = StorageBackend::Azure,
                "local" => self.storage.backend = StorageBackend::Local,
                _ => {
                    return Err(ZeppelinError::Config(format!(
                        "env var STORAGE_BACKEND={v} is not a valid storage backend; expected one of s3, gcs, azure, local"
                    )));
                }
            }
        }
        if let Some(v) = env_override::<String>("S3_BUCKET")? {
            self.storage.bucket = v;
        }
        if let Some(v) = env_override::<String>("AWS_REGION")? {
            self.storage.s3_region = Some(v);
        }
        if let Some(v) = env_override::<String>("S3_ENDPOINT")? {
            self.storage.s3_endpoint = if v.is_empty() { None } else { Some(v) };
        }
        if let Some(v) = env_override::<String>("AWS_ACCESS_KEY_ID")? {
            self.storage.s3_access_key_id = Some(v);
        }
        if let Some(v) = env_override::<String>("AWS_SECRET_ACCESS_KEY")? {
            self.storage.s3_secret_access_key = Some(v);
        }
        if let Some(v) = env_override("S3_ALLOW_HTTP")? {
            self.storage.s3_allow_http = v;
        }
        if let Some(v) = env_override("ZEPPELIN_STORAGE_FAIL_FAST")? {
            self.storage.fail_fast = v;
        }

        // Cache
        if let Some(v) = env_override::<String>("ZEPPELIN_CACHE_DIR")? {
            self.cache.dir = PathBuf::from(v);
        }
        if let Some(v) = env_override("ZEPPELIN_CACHE_MAX_SIZE_GB")? {
            self.cache.max_size_gb = v;
        }
        if let Some(v) = env_override("ZEPPELIN_MEMORY_CACHE_MAX_MB")? {
            self.cache.memory_cache_max_mb = v;
        }
        if let Some(v) = env_override("ZEPPELIN_MANIFEST_CACHE_TTL_MS")? {
            self.cache.manifest_cache_ttl_ms = v;
        }
        if let Some(v) = env_override("ZEPPELIN_NAMESPACE_REGISTRY_TTL_MS")? {
            self.cache.namespace_registry_ttl_ms = v;
        }
        if let Some(v) = env_override::<String>(HYDRATION_POLICY_ENV)? {
            self.cache.hydration_policy = match v.to_lowercase().as_str() {
                "session_window" => HydrationPolicyKind::SessionWindow,
                _ => {
                    return Err(ZeppelinError::Config(format!(
                        "env var {HYDRATION_POLICY_ENV}={v} is not a valid hydration policy; expected session_window"
                    )));
                }
            };
        }
        if let Some(value) = env_override(HYDRATION_HEAT_QUERIES_ENV)? {
            self.cache.hydration_heat_queries = value;
        }
        if let Some(value) = env_override(HYDRATION_HEAT_WINDOW_SECS_ENV)? {
            self.cache.hydration_heat_window_secs = value;
        }

        // Indexing
        if let Some(v) = env_override("ZEPPELIN_DEFAULT_NUM_CENTROIDS")? {
            self.indexing.default_num_centroids = v;
        }
        if let Some(v) = env_override("ZEPPELIN_DEFAULT_NPROBE")? {
            self.indexing.default_nprobe = v;
        }
        if let Some(v) = env_override::<String>("ZEPPELIN_QUANTIZATION")? {
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
                _ => {
                    return Err(ZeppelinError::Config(format!(
                        "env var ZEPPELIN_QUANTIZATION={v} is not a valid quantization; expected one of none, scalar, sq8, product, pq"
                    )));
                }
            }
        }
        if let Some(v) = env_override("ZEPPELIN_BITMAP_INDEX")? {
            self.indexing.bitmap_index = v;
        }
        if let Some(v) = env_override("ZEPPELIN_FTS_INDEX")? {
            self.indexing.fts_index = v;
        }
        if let Some(v) = env_override("ZEPPELIN_BM25_MAX_FULL_SCAN_CLUSTERS")? {
            self.indexing.bm25_max_full_scan_clusters = v;
        }
        if let Some(v) = env_override("ZEPPELIN_BM25_MAX_FULL_SCAN_VECTORS")? {
            self.indexing.bm25_max_full_scan_vectors = v;
        }
        if let Some(v) = env_override("ZEPPELIN_HIERARCHICAL")? {
            self.indexing.hierarchical = v;
        }
        if let Some(v) = env_override("ZEPPELIN_LEAF_SIZE")? {
            self.indexing.leaf_size = Some(v);
        }

        // Compaction
        if let Some(v) = env_override("ZEPPELIN_COMPACTION_INTERVAL_SECS")? {
            self.compaction.interval_secs = v;
        }
        if let Some(v) = env_override("ZEPPELIN_MAX_WAL_FRAGMENTS")? {
            self.compaction.max_wal_fragments_before_compact = v;
        }
        if let Some(v) = env_override("ZEPPELIN_MAX_WAL_AGE_SECS")? {
            self.compaction.max_wal_age_before_compact_secs = v;
        }
        if let Some(v) = env_override("ZEPPELIN_MAX_WAL_BYTES")? {
            self.compaction.max_wal_bytes_before_compact = v;
        }
        if let Some(v) = env_override("ZEPPELIN_MAX_PENDING_DELETES")? {
            self.compaction.max_pending_deletes = v;
        }
        if let Some(v) = env_override("ZEPPELIN_MAX_OLD_SEGMENTS")? {
            self.compaction.max_old_segments = v;
        }

        // Logging
        if let Some(v) = env_override::<String>("ZEPPELIN_LOG_FORMAT")? {
            self.logging.format = v;
        }

        // Query
        if let Some(v) = env_override(RERANK_COALESCE_GAP_ENV)? {
            self.query.rerank_coalesce_gap_bytes = Some(v);
            self.query.cost_latency_profile = None;
        }

        // GC
        if let Some(v) = env_override(GC_HORIZON_SECS_ENV)? {
            self.gc.horizon_secs = v;
        }
        if let Some(v) = env_override(GC_COMPACTION_UPLOAD_WINDOW_SECS_ENV)? {
            self.gc.compaction_upload_window_secs = v;
        }
        if let Some(v) = env_override(GC_SKEW_SLOP_SECS_ENV)? {
            self.gc.skew_slop_secs = v;
        }
        if let Some(v) = env_override(GC_ALLOW_UNSAFE_SHORT_HORIZON_ENV)? {
            self.gc.allow_unsafe_short_horizon = v;
        }
        if let Some(v) = env_override(GC_MANIFEST_HISTORY_KEEP_COUNT_ENV)? {
            self.gc.manifest_history_keep_count = v;
        }

        Ok(())
    }
}

fn env_override<T>(name: &'static str) -> Result<Option<T>>
where
    T: FromStr,
    T::Err: Display,
{
    match std::env::var(name) {
        Ok(value) => value.parse::<T>().map(Some).map_err(|error| {
            ZeppelinError::Config(format!(
                "env var {name}={value} is not a valid {}: {error}",
                std::any::type_name::<T>()
            ))
        }),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(ZeppelinError::Config(format!(
            "failed to read {name}: {error}"
        ))),
    }
}
