//! Boot-time configuration for every Zeppelin subsystem.
//!
//! This module is the process-wide boundary between operator-supplied text and
//! the strongly typed settings consumed by storage, caching, indexing,
//! compaction, query execution, garbage collection, logging, and the HTTP
//! server. Startup code normally enters through [`crate::config::Config::load`]; subsystem
//! constructors then borrow the relevant nested configuration such as
//! [`crate::config::StorageConfig`] or [`crate::config::IndexingConfig`]. Configuration is local process
//! input, not authoritative search data: manifests and immutable artifacts in
//! object storage remain the source of truth.
//!
//! Loading deliberately fails loudly. Unknown TOML keys, unreadable files,
//! malformed environment values, mutually exclusive options, and unsafe
//! cross-field combinations become [`crate::error::ZeppelinError::Config`] errors before the
//! server starts. No invalid input is silently replaced with a default.
//!
//! ## Configuration flow
//!
//! ```text
//! compiled defaults
//!        |
//!        v
//! optional TOML file  -- unknown/malformed key --> startup error
//!        |
//!        v
//! environment overrides -- malformed value ----> startup error
//!        |
//!        v
//! resolve derived choices (for example, query profile -> byte gap)
//!        |
//!        v
//! validate cross-field invariants -- violation --> startup error
//!        |
//!        v
//! typed Config borrowed by subsystem constructors
//! ```
//!
//! ## Reading map
//!
//! 1. Start with [`crate::config::Config`] to see the complete subsystem map.
//! 2. Read [`crate::config::ServerConfig`], [`crate::config::StorageConfig`],
//!    [`crate::config::CacheConfig`], [`crate::config::IndexingConfig`],
//!    [`crate::config::CompactionConfig`], and [`crate::config::GcConfig`] for the major
//!    operational controls.
//! 3. Follow [`crate::config::Config::load`] to understand precedence and
//!    [`crate::config::Config::validate`] to understand boot-time invariants.
//! 4. Read [`crate::config::CpuBudget::auto`] for the independently loaded thread-pool budget.
//! 5. Finish with `env_override` to see how a single environment value is
//!    parsed without a type-specific conversion table.
//!
//! ## Invariants not to break
//!
//! - Precedence is environment variable over TOML over compiled default.
//! - Present but malformed values are errors; only absent values may fall back.
//! - `#[serde(deny_unknown_fields)]` keeps misspelled and removed keys from
//!   being ignored.
//! - [`crate::config::Config::validate`] reports all independent violations together so an
//!   operator can fix one boot attempt rather than discovering errors serially.
//! - The GC horizon must cover every interval during which a reader or
//!   compactor can legitimately depend on an older manifest view.
//!
//! ## Rust concepts used here
//!
//! Serde derives turn TOML into nested Rust structs, while enums such as
//! [`crate::config::StorageBackend`] and [`crate::config::CostLatencyProfile`] limit configuration to named,
//! compiler-checked choices. Java code often models this with POJOs plus a
//! validation framework, and C code typically uses structs plus handwritten
//! parsing. Rust adds exhaustive `match` checking and an ownership model that
//! lets startup build one owned [`crate::config::Config`] which later code can borrow without
//! copying it. `env_override` demonstrates bounded generics: one function can
//! parse any type implementing [`std::str::FromStr`] and still return the crate's single
//! [`crate::error::Result`] error channel.

use crate::error::{Result, ZeppelinError};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::net::IpAddr;
use std::path::PathBuf;
use std::str::FromStr;

/// Environment key for an explicit rerank range-coalescing gap in bytes.
const RERANK_COALESCE_GAP_ENV: &str = "ZEPPELIN_RERANK_COALESCE_GAP_BYTES";
/// Environment key selecting the cache hydration heat policy.
const HYDRATION_POLICY_ENV: &str = "ZEPPELIN_HYDRATION_POLICY";
/// Environment key for the observations needed to mark a namespace hot.
const HYDRATION_HEAT_QUERIES_ENV: &str = "ZEPPELIN_HYDRATION_HEAT_QUERIES";
/// Environment key for the hydration heat-observation window in seconds.
const HYDRATION_HEAT_WINDOW_SECS_ENV: &str = "ZEPPELIN_HYDRATION_HEAT_WINDOW_SECS";
/// Environment key for the minimum age of unreachable objects before GC.
const GC_HORIZON_SECS_ENV: &str = "ZEPPELIN_GC_HORIZON_SECS";
/// Environment key for the maximum time between compaction upload and publication.
const GC_COMPACTION_UPLOAD_WINDOW_SECS_ENV: &str = "ZEPPELIN_GC_COMPACTION_UPLOAD_WINDOW_SECS";
/// Environment key for clock-skew allowance in GC safety calculations.
const GC_SKEW_SLOP_SECS_ENV: &str = "ZEPPELIN_GC_SKEW_SLOP_SECS";
/// Environment key for the explicit unsafe-short-GC-horizon interlock.
const GC_ALLOW_UNSAFE_SHORT_HORIZON_ENV: &str = "ZEPPELIN_GC_ALLOW_UNSAFE_SHORT_HORIZON";
/// Environment key for the count of manifest history generations retained by GC.
const GC_MANIFEST_HISTORY_KEEP_COUNT_ENV: &str = "ZEPPELIN_GC_MANIFEST_HISTORY_KEEP_COUNT";
/// Environment key for time-based point-in-time-recovery retention in seconds.
const GC_PITR_RETENTION_SECS_ENV: &str = "ZEPPELIN_GC_PITR_RETENTION_SECS";

/// Default maximum gap, in bytes, between rerank `f32` ranges that are merged
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
/// # Examples
///
/// ```text
/// ZEPPELIN_RERANK_COALESCE_GAP_BYTES=1048576
/// # 1 MiB: favor fewer S3 GETs when request cost dominates.
///
/// ZEPPELIN_RERANK_COALESCE_GAP_BYTES=131072
/// # 128 KiB: favor the measured loopback throughput knee.
/// ```
///
/// These numbers are from loopback MinIO (~410 MB/s wall). Real S3 has higher
/// per-request latency but wider aggregate bandwidth, which pushes the optimal
/// gap UP (fewer, fatter GETs); S3 Express One Zone halves request price and
/// cuts first-byte latency, pushing it back DOWN. Re-run the gap sweep
/// (qpsbench with this env var) on the target deployment before fixing a value.
pub const DEFAULT_RERANK_COALESCE_GAP_BYTES: usize = 1024 * 1024;

/// Complete boot-time configuration after defaults, TOML, and environment input merge.
///
/// Each field owns one subsystem's settings. A successfully loaded value has
/// passed [`Config::validate`], but callers that construct or mutate a value
/// directly must validate it themselves before starting services.
///
/// # Example
///
/// With a TOML file that sets `server.port = 9000` and an environment override
/// `ZEPPELIN_PORT=9001`, [`Config::load`] returns a `Config` whose
/// [`ServerConfig::port`] is `9001`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// HTTP bind, timeout, admission-control, and request-size settings.
    #[serde(default)]
    pub server: ServerConfig,
    /// Object-storage backend, bucket, endpoint, credentials, and boot probe policy.
    #[serde(default)]
    pub storage: StorageConfig,
    /// Disposable local disk and memory cache settings; these never supersede S3 state.
    #[serde(default)]
    pub cache: CacheConfig,
    /// Vector and lexical indexing parameters used when immutable segments are built.
    #[serde(default)]
    pub indexing: IndexingConfig,
    /// Background WAL-to-segment compaction schedule, triggers, retention, and lease.
    #[serde(default)]
    pub compaction: CompactionConfig,
    /// Structured logging level and output format.
    #[serde(default)]
    pub logging: LoggingConfig,
    /// Reserved home for WAL settings; group commit currently has no tuning knobs.
    #[serde(default)]
    pub wal: WalConfig,
    /// Query-time object-read cost and latency tuning.
    #[serde(default)]
    pub query: QueryConfig,
    /// Garbage-collection safety horizons, history retention, and unsafe override.
    #[serde(default)]
    pub gc: GcConfig,
}

/// Safety and retention controls for deleting unreachable immutable objects.
///
/// GC may see an object as unreachable while an in-flight reader still uses an
/// older cached manifest or a compactor has uploaded but not yet published the
/// object. The configured horizon covers those intervals before physical
/// deletion is allowed.
///
/// ```text
/// cached-manifest lifetime
///        + request lifetime
///        + upload-before-publication window
///        + clock-skew allowance
///        = minimum safe GC horizon
/// ```
///
/// Retained manifest history and named snapshots remain live roots regardless
/// of object age. Reducing retention can make formerly referenced artifacts
/// eligible for collection, but only after the normal horizon.
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
    /// Time-based PITR retention window for committed manifest snapshots.
    ///
    /// A history generation is retained if it is within the count window OR
    /// its commit timestamp is younger than this window OR a named snapshot
    /// pins it. `0` disables time-based retention. Default: `0`.
    #[serde(default)]
    pub pitr_retention_secs: u64,
}

/// Query-time object-fetch tuning resolved during [`Config::load`].
///
/// Operators may choose either an exact byte gap or a named
/// [`CostLatencyProfile`]. After loading, the exact gap is stored and the
/// profile is cleared so query execution has one unambiguous value to read.
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

/// Named tradeoff profiles for rerank range coalescing.
///
/// Using an enum makes an unsupported profile impossible after parsing. In
/// Java this resembles an `enum`; in C it resembles a tagged integer plus a
/// validated parser. Rust additionally requires [`rerank_coalesce_gap_bytes_for_profile`]
/// to handle every variant when the enum changes.
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

/// Resolves a named cost/latency profile to a concrete byte gap.
///
/// # Parameters
///
/// - `profile`: Validated profile chosen in TOML.
///
/// # Returns
///
/// The maximum gap between adjacent rerank ranges that may be covered by one
/// object-store GET.
///
/// # Example
///
/// `LowLatency` returns 128 KiB, while `LowCost` and `Balanced` return the
/// 1 MiB [`DEFAULT_RERANK_COALESCE_GAP_BYTES`]. This changes the fetch plan,
/// not the candidate set or recall.
///
/// # Rust Notes for Java/C Engineers
///
/// The exhaustive `match` has no default branch. Adding an enum variant forces
/// this function to define its behavior at compile time, unlike a Java `switch`
/// with a permissive default or a C switch over an arbitrary integer.
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
/// Unit tests for precedence, strict parsing, defaults, and cross-field invariants.
///
/// Environment variables are process-global, so tests that change them hold
/// [`ENV_LOCK`] and use [`EnvGuard`] to restore the caller's environment even
/// when a test panics.
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::sync::Mutex;

    /// Serializes tests that mutate the process environment.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// RAII guard that restores every configuration environment variable on drop.
    ///
    /// This gives each test a clean environment without leaking changes to
    /// later tests. Java would normally express the cleanup with `try/finally`;
    /// C would use a single cleanup label. Rust invokes [`Drop::drop`]
    /// automatically on ordinary returns and panic unwinding.
    struct EnvGuard {
        /// Original value of each removed variable, or `None` if it was absent.
        original: Vec<(&'static str, Option<OsString>)>,
    }

    impl EnvGuard {
        /// Removes all recognized configuration variables and remembers their values.
        ///
        /// # Returns
        ///
        /// An owned guard whose destructor restores the captured environment.
        ///
        /// # Side Effects
        ///
        /// Mutates the process environment. The caller must hold [`ENV_LOCK`]
        /// so another configuration test cannot observe the temporary state.
        ///
        /// # Example
        ///
        /// A test can create `_env = EnvGuard::clear()`, set one override, and
        /// then return normally; `_env` restores all prior values automatically.
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
                GC_PITR_RETENTION_SECS_ENV,
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
        /// Restores every captured environment variable when the guard leaves scope.
        ///
        /// # Parameters
        ///
        /// - `self`: Mutable access to the guard so captured entries can be
        ///   drained exactly once.
        ///
        /// # Side Effects
        ///
        /// Reinstates present values and removes variables that were originally
        /// absent. The implementation performs no allocation for copied names;
        /// each name is a `'static` string literal.
        fn drop(&mut self) {
            for (name, value) in self.original.drain(..) {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }

    /// Writes TOML to a temporary file and loads it through the production path.
    ///
    /// # Parameters
    ///
    /// - `contents`: Complete TOML source for one test configuration.
    ///
    /// # Returns
    ///
    /// The same [`Result`] that [`Config::load`] produces. The temporary file
    /// remains alive until loading finishes and is then removed by RAII.
    ///
    /// # Panics
    ///
    /// Panics if the test process cannot create or write its temporary file, or
    /// if the generated path is not valid UTF-8.
    ///
    /// # Example
    ///
    /// Passing `"[server]\nport = 9000"` exercises file parsing, overrides,
    /// derived-value resolution, and validation rather than bypassing startup.
    fn load_toml(contents: &str) -> Result<Config> {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), contents).unwrap();
        Config::load(Some(file.path().to_str().unwrap()))
    }

    /// Asserts that a failed load reports every expected diagnostic fragment.
    ///
    /// # Parameters
    ///
    /// - `result`: Configuration result expected to be an error.
    /// - `needles`: Substrings that must all appear in the rendered error.
    ///
    /// # Panics
    ///
    /// Panics if loading succeeded or if any expected substring is absent.
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

    /// Verifies that a present but malformed numeric override fails startup.
    #[test]
    fn env_override_rejects_present_but_unparseable_port() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::clear();

        std::env::set_var("ZEPPELIN_PORT", "80eighty");

        assert_config_error_contains(load_toml(""), &["ZEPPELIN_PORT", "80eighty"]);
    }

    /// Verifies that strict TOML parsing rejects misspelled configuration keys.
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

    /// Exercises each independent validation rule with one focused mutation.
    ///
    /// The table uses boxed `Fn` trait objects so closures with different
    /// concrete types can share one vector. Java would store objects implementing
    /// a functional interface; C would commonly store function pointers plus
    /// optional context. Rust checks that every closure can only mutably borrow
    /// the supplied [`Config`] for the duration of the call.
    #[test]
    fn validate_reports_each_cross_field_rule() {
        /// One named mutation plus the validation fragments it must produce.
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

    /// Verifies that validation aggregates independent errors in one response.
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

    /// Protects the default invariant that every default centroid can be probed.
    #[test]
    fn default_max_nprobe_covers_default_centroid_count() {
        let config = Config::default();

        assert_eq!(config.indexing.default_num_centroids, 256);
        assert!(
            config.indexing.max_nprobe >= config.indexing.default_num_centroids,
            "default max_nprobe must allow probing all default centroids"
        );
    }

    /// Verifies explicit query gaps and the compiled fallback through full loading.
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

    /// Pins each named query profile to its intended concrete byte gap.
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

    /// Verifies that a TOML file cannot set both forms of the same query choice.
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

    /// Verifies environment precedence and strict parsing for the rerank gap.
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

    /// Documents that a zero coalescing gap is an intentional valid setting.
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

    /// Verifies that an unknown hydration policy name fails during deserialization.
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

    /// Pins hydration defaults and verifies explicit TOML values survive loading.
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

    /// Verifies that GC floor failures name every interval used in the calculation.
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

    /// Verifies that the explicit unsafe override permits, detects, and warns on a short horizon.
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

    /// Protects the invariant that compiled GC defaults satisfy their own safety floor.
    #[test]
    fn default_gc_horizon_passes_floor() {
        let config = Config::default();

        config.validate().unwrap();
        assert!(config.gc.horizon_secs >= config.gc_horizon_floor_secs().unwrap());
        assert!(!config.gc.allow_unsafe_short_horizon);
    }

    /// Round-trips GC TOML and protects the reader-safety explanation in source docs.
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
            pitr_retention_secs = 86400
            "#,
        )
        .unwrap();

        assert_eq!(config.gc.horizon_secs, 120);
        assert_eq!(config.gc.compaction_upload_window_secs, 15);
        assert_eq!(config.gc.skew_slop_secs, 4);
        assert!(config.gc.allow_unsafe_short_horizon);
        assert_eq!(config.gc.manifest_history_keep_count, 7);
        assert_eq!(config.gc.pitr_retention_secs, 86_400);

        let source = include_str!("config.rs");
        assert!(source.contains("manifest_cache_ttl_secs + request_timeout_secs + compaction_upload_window_secs + skew_slop_secs"));
        assert!(source.contains("causally unrelated to the reader-staleness window"));
    }

    /// Verifies that a removed compaction key is rejected instead of silently ignored.
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
/// now unconditional in the WAL writer—there is no batching knob to tune. The
/// former `batch_manifest_size` / `batch_manifest_timeout_ms` fields are gone.
/// With strict boot config, stale WAL keys are rejected instead of ignored. The
/// struct is retained as the home for future WAL settings.
///
/// # Example
///
/// An empty `[wal]` table is accepted. A removed batching key is an unknown
/// field and fails startup rather than pretending to configure group commit.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalConfig {}

/// HTTP server bind, lifecycle, admission-control, and request-limit settings.
///
/// These values bound work before it reaches the domain layer. Rate limits are
/// per client, request limits protect memory and CPU, and timeout values also
/// participate in safety calculations such as the GC horizon floor.
///
/// # Example
///
/// A deployment may bind `0.0.0.0:8080`, allow bursts of 200 query requests,
/// and accept `top_k` up to 10,000 while supplying 10 when a client omits it.
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
    /// Maximum vectors per accepted upsert batch/WAL fragment. Default: `50_000`.
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

/// Returns the default number of nearest neighbors for a query that omits `top_k`.
fn default_top_k() -> usize {
    10
}
/// Returns the default sustained query/read request rate per client.
fn default_rate_limit_rps() -> u32 {
    100
}
/// Returns the default query/read token-bucket burst capacity per client.
fn default_rate_limit_burst() -> u32 {
    200
}
/// Returns the default sustained write/admin request rate per client.
fn default_write_rate_limit_rps() -> u32 {
    50
}
/// Returns the default write/admin token-bucket burst capacity per client.
fn default_write_rate_limit_burst() -> u32 {
    100
}
/// Returns the default lifetime, in seconds, of an idle client rate-limit bucket.
fn default_rate_limit_idle_ttl_secs() -> u64 {
    600
}
/// Returns the default maximum number of query entries accepted in one batch request.
fn default_max_query_batch_size() -> usize {
    256
}

/// Checks whether a trusted-proxy entry is a syntactically valid IP CIDR range.
///
/// # Parameters
///
/// - `value`: Candidate containing a literal IPv4 or IPv6 address, `/`, and a
///   prefix length. Host addresses without a prefix are intentionally rejected.
///
/// # Returns
///
/// `true` when the address parses and the prefix is at most 32 for IPv4 or 128
/// for IPv6; otherwise `false`.
///
/// # Example
///
/// `10.0.0.0/8` and `2001:db8::/32` are valid, while `127.0.0.1` and
/// `10.0.0.0/33` are not.
///
/// # Rust Notes for Java/C Engineers
///
/// `let ... else` exits early when splitting or parsing fails, and the
/// exhaustive [`IpAddr`] match selects the address-family limit. In Java this
/// would usually be exceptions plus `instanceof`; in C it would be explicit
/// return-code checks and an address-family tag. Rust's parsed enum guarantees
/// the final match receives either a valid IPv4 or valid IPv6 address.
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

/// Object-store implementations selectable at boot.
///
/// The selected backend changes transport construction, not Zeppelin's
/// authority model: persistent artifacts in the configured object store remain
/// authoritative and local cache remains disposable.
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
    /// Writes the stable lowercase operator-facing name of this backend.
    ///
    /// # Parameters
    ///
    /// - `self`: Backend variant to render; it is borrowed and not copied.
    /// - `f`: Formatter supplied by Rust's formatting machinery.
    ///
    /// # Returns
    ///
    /// Formatting success or the formatter's error.
    ///
    /// # Example
    ///
    /// Formatting [`StorageBackend::Gcs`] produces `"gcs"`, matching the TOML
    /// and environment spelling.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageBackend::S3 => write!(f, "s3"),
            StorageBackend::Gcs => write!(f, "gcs"),
            StorageBackend::Azure => write!(f, "azure"),
            StorageBackend::Local => write!(f, "local"),
        }
    }
}

/// Object-store backend selection, location, credentials, and boot-probe policy.
///
/// This struct configures the durable source-of-truth connection. It does not
/// hold a client or any cached state; the storage layer borrows these settings
/// to construct the object-store abstraction used by all higher layers.
///
/// # Example
///
/// A local MinIO deployment uses the `S3` backend with a custom endpoint and
/// may explicitly permit HTTP. Production S3 normally leaves the endpoint and
/// static credentials unset so the platform's standard region and credential
/// providers can be used.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// Which storage backend to use. Default: `S3`.
    #[serde(default)]
    pub backend: StorageBackend,
    /// Bucket (or container) name. Default: `"zeppelin"`.
    #[serde(default = "default_bucket")]
    pub bucket: String,

    // These settings specialize the S3-compatible transport without exposing
    // object-store implementation details to higher layers.
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

/// Disposable local disk, in-memory cache, and background hydration settings.
///
/// These values affect latency and local resource use only. Cached manifests
/// may be briefly stale within their TTL, but cache contents never override the
/// authoritative manifest and immutable artifacts in object storage.
///
/// # Example
///
/// With hydration disabled, misses populate cache on demand. Enabling the
/// session-window policy allows a namespace observed three times within 60
/// seconds to hydrate segments with at most four concurrent downloads, subject
/// to the per-segment disk-fraction limit.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    /// Directory for on-disk cache files. Default: `/var/cache/zeppelin`.
    #[serde(default = "default_cache_dir")]
    pub dir: PathBuf,
    /// Maximum disk cache size in gigabytes. Default: `50`.
    #[serde(default = "default_max_size_gb")]
    pub max_size_gb: u64,
    /// Maximum memory cache size in MB. Set to `0` to disable. Default: `256`.
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

/// Globally selected policy for deciding when a namespace is hot enough to hydrate.
///
/// This is an enum rather than a free-form string so downstream hydration code
/// can match exhaustively and cannot encounter an unknown policy after boot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HydrationPolicyKind {
    /// Count queries in a per-namespace session window.
    SessionWindow,
}

/// Segment-build and search limits for vector, bitmap, and lexical indexes.
///
/// IVF-Flat groups vectors around centroids. `nprobe` controls how many of
/// those groups a query searches: probing more clusters generally improves
/// recall but increases object-store reads, bytes processed, and distance
/// calculations. Quantization reduces the bytes stored and fetched at the cost
/// of approximation. Bitmap and full-text indexes add prefiltering and BM25
/// retrieval structures during compaction.
///
/// ```text
/// vectors entering compaction
///        |
///        +--> train/select centroids --> assign vectors to IVF clusters
///        |
///        +--> optional quantization --> smaller stored vector representation
///        |
///        +--> optional bitmap/FTS indexes
///        v
/// immutable segment consumed by query planning
/// ```
///
/// # Example
///
/// With 256 centroids and `default_nprobe = 16`, an ordinary query searches 16
/// candidate clusters. A caller may request a larger value, but the API must
/// reject a value above `max_nprobe`.
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

/// Background WAL-to-segment compaction schedule, triggers, retention, and lease.
///
/// The background scheduler checks on [`CompactionConfig::interval_secs`] and
/// may compact when *any* count, age, or byte trigger is reached. Compaction
/// writes new immutable segment artifacts before a manifest publication makes
/// them authoritative. Its namespace lease reduces duplicate work, while the
/// fencing-token and manifest-CAS layers still prevent a stale worker from
/// committing after lease loss.
///
/// ```text
/// pending immutable WAL fragments
///        |
///        | count OR oldest age OR total bytes reaches threshold
///        v
/// lease-protected compaction builds immutable segment
///        |
///        | fencing check + manifest CAS
///        v
/// manifest makes segment visible; later GC reclaims unreachable artifacts
/// ```
///
/// # Example
///
/// A namespace with only two fragments still compacts when those fragments are
/// five minutes old or total 64 MiB, even though the count trigger of 100 has
/// not fired.
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

/// Structured logging verbosity and renderer selected during process startup.
///
/// # Example
///
/// Production uses `level = "info"` and `format = "json"` by default so fields
/// are machine searchable. A developer may choose `"pretty"` locally.
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

// These helpers provide compiled defaults to Serde and `Default` implementations.
// They intentionally do not inspect the environment; `apply_env_overrides()` owns
// the precedence boundary.
/// Returns the default HTTP bind address, `0.0.0.0`.
fn default_host() -> String {
    "0.0.0.0".to_string()
}
/// Returns the default HTTP listen port, `8080`.
fn default_port() -> u16 {
    8080
}
/// Returns the default per-request timeout in seconds.
fn default_request_timeout() -> u64 {
    30
}
/// Returns the default maximum number of concurrent query handlers.
fn default_max_concurrent_queries() -> usize {
    64
}
/// Returns the default maximum vector count accepted in one write batch.
fn default_max_batch_size() -> usize {
    50_000
}
/// Returns the default hard upper bound for query `top_k`.
fn default_max_top_k() -> usize {
    10_000
}
/// Returns the default graceful-shutdown deadline in seconds.
fn default_shutdown_timeout_secs() -> u64 {
    30
}
/// Returns the default maximum accepted vector dimensionality.
fn default_max_dimensions() -> usize {
    65_536
}
/// Returns the default maximum vector-ID length in bytes.
fn default_max_vector_id_length() -> usize {
    1024
}
/// Returns the default maximum HTTP request-body size in megabytes.
fn default_max_request_body_mb() -> usize {
    512
}
/// Returns the default object-store bucket name.
fn default_bucket() -> String {
    "zeppelin".to_string()
}
/// Returns whether startup probes object storage by default.
fn default_storage_fail_fast() -> bool {
    true
}
/// Returns the default directory for disposable on-disk cache data.
fn default_cache_dir() -> PathBuf {
    PathBuf::from("/var/cache/zeppelin")
}
/// Returns the default disk-cache capacity in gigabytes.
fn default_max_size_gb() -> u64 {
    50
}
/// Returns the default in-memory cache capacity in megabytes.
fn default_memory_cache_max_mb() -> usize {
    256
}
/// Returns the default manifest-cache TTL in milliseconds.
fn default_manifest_cache_ttl_ms() -> u64 {
    500
}
/// Returns the default positive namespace-registry TTL in milliseconds.
fn default_namespace_registry_ttl_ms() -> u64 {
    5000
}
/// Returns the default warm-set heat-detection policy.
fn default_hydration_policy() -> HydrationPolicyKind {
    HydrationPolicyKind::SessionWindow
}
/// Returns the default query count needed to mark a namespace hot.
fn default_hydration_heat_queries() -> u64 {
    3
}
/// Returns the default hydration heat window in seconds.
fn default_hydration_heat_window_secs() -> u64 {
    60
}
/// Returns the default maximum number of parallel hydration downloads.
fn default_hydration_parallelism() -> usize {
    4
}
/// Returns the default maximum disk-cache fraction available to one hydrated segment.
fn default_hydration_max_segment_fraction() -> f64 {
    0.5
}
/// Returns the default number of IVF centroids built per segment.
fn default_num_centroids() -> usize {
    256
}
/// Returns the default number of IVF clusters probed by a vector query.
fn default_nprobe() -> usize {
    16
}
/// Returns the default maximum number of IVF clusters a query may probe.
fn default_max_nprobe() -> usize {
    256
}
/// Returns the default cap on k-means training iterations.
fn default_kmeans_max_iterations() -> usize {
    25
}
/// Returns the default k-means convergence threshold.
fn default_kmeans_convergence_epsilon() -> f64 {
    1e-4
}
/// Returns the default k-means initialization oversampling factor.
fn default_oversample_factor() -> usize {
    3
}
/// Returns the default number of product-quantization subquantizers.
fn default_pq_m() -> usize {
    8
}
/// Returns scalar quantization as the default stored-vector representation.
fn default_quantization() -> crate::index::quantization::QuantizationType {
    crate::index::quantization::QuantizationType::Scalar
}
/// Returns whether immutable segments build bitmap indexes by default.
fn default_bitmap_index() -> bool {
    true
}
/// Returns the default interval between background compaction checks in seconds.
fn default_compaction_interval() -> u64 {
    30
}
/// Returns the default pending-WAL-fragment count that triggers compaction.
fn default_max_wal_fragments() -> usize {
    100
}
/// Returns the default oldest-pending-WAL age that triggers compaction in seconds.
fn default_max_wal_age_secs() -> u64 {
    300
}
/// Returns the default pending-WAL byte total that triggers compaction.
fn default_max_wal_bytes() -> u64 {
    64 * 1024 * 1024
}
/// Returns the default vector-imbalance ratio that retrains centroids.
fn default_retrain_threshold() -> f64 {
    5.0
}
/// Returns the default BM25 fallback cluster-scan circuit breaker.
fn default_bm25_max_full_scan_clusters() -> usize {
    500
}
/// Returns the default BM25 fallback vector-scan circuit breaker.
fn default_bm25_max_full_scan_vectors() -> usize {
    100_000
}
/// Returns the legacy pending-delete compatibility limit.
fn default_max_pending_deletes() -> usize {
    1000
}
/// Returns the default number of inactive segments retained in a manifest.
fn default_max_old_segments() -> usize {
    10
}
/// Returns the default namespace compaction-lease duration in seconds.
fn default_compaction_lease_secs() -> u64 {
    300
}
/// Returns the default structured-log verbosity filter.
fn default_log_level() -> String {
    "info".to_string()
}
/// Returns the default machine-readable structured-log format.
fn default_log_format() -> String {
    "json".to_string()
}
/// Returns the default age threshold for collecting unreachable objects in seconds.
fn default_gc_horizon_secs() -> u64 {
    900
}
/// Returns the default maximum compaction upload-to-publication window in seconds.
fn default_gc_compaction_upload_window_secs() -> u64 {
    300
}
/// Returns the default wall-clock skew allowance for GC in seconds.
fn default_gc_skew_slop_secs() -> u64 {
    5
}
/// Returns the default number of committed manifest-history generations retained by GC.
fn default_gc_manifest_history_keep_count() -> usize {
    128
}

impl Default for ServerConfig {
    /// Builds server settings from the compiled, environment-independent defaults.
    ///
    /// Environment variables are intentionally applied later by
    /// `Config::apply_env_overrides` so precedence remains explicit.
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
    /// Builds an S3-oriented storage configuration with no explicit credentials.
    ///
    /// The storage layer may use its normal credential chain when the optional
    /// static credential fields remain `None`.
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
    /// Builds the default cache policy with background hydration disabled.
    ///
    /// On-demand caching remains available; disabling hydration only prevents
    /// proactive segment downloads.
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
    /// Builds the default IVF-Flat indexing and search policy.
    ///
    /// Scalar quantization and bitmap indexes are enabled, while hierarchical
    /// IVF and full-text indexes remain opt-in.
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
    /// Builds the default background compaction triggers and lease duration.
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
    /// Builds production-oriented JSON logging at `info` verbosity.
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}

impl Default for GcConfig {
    /// Builds conservative GC retention settings with no unsafe override.
    ///
    /// The resulting horizon passes the floor derived from the other compiled
    /// defaults and time-based point-in-time retention is disabled.
    fn default() -> Self {
        Self {
            horizon_secs: default_gc_horizon_secs(),
            compaction_upload_window_secs: default_gc_compaction_upload_window_secs(),
            skew_slop_secs: default_gc_skew_slop_secs(),
            allow_unsafe_short_horizon: false,
            manifest_history_keep_count: default_gc_manifest_history_keep_count(),
            pitr_retention_secs: 0,
        }
    }
}

/// CPU budget for distributing workers across runtimes.
///
/// Detected at startup via `available_parallelism()`, then allocated:
/// - **Query workers**: 2× CPUs (overcommit is useful because queries wait on S3 GETs)
/// - **Compaction workers**: max(1, CPUs/4) — reserve most cores for queries.
///   On a 4-vCPU c7i.xlarge, this gives 1 compaction worker + 3 for queries.
/// - **Rayon threads**: match physical cores (work-stealing at core count)
///
/// ```text
/// OS-visible parallelism
///        |
///        +--> query Tokio runtime: max(4, CPUs * 2)
///        +--> compaction Tokio runtime: max(1, CPUs / 4)
///        +--> Rayon CPU pool: CPUs
///        |
///        v
/// optional environment overrides for each pool
/// ```
///
/// # Example
///
/// On eight visible CPUs, the computed budget is 16 query workers, two
/// compaction workers, and eight Rayon threads before overrides.
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
    /// Detects available CPUs, computes runtime budgets, and applies overrides.
    ///
    /// # Returns
    ///
    /// A complete worker allocation. If the operating system cannot report
    /// available parallelism, this existing policy computes from four CPUs.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Config`] when any present worker-count
    /// environment variable cannot be parsed as a `usize`. No partial budget is
    /// returned.
    ///
    /// # Side Effects
    ///
    /// Reads `ZEPPELIN_QUERY_WORKERS`, `ZEPPELIN_COMPACTION_WORKERS`, and
    /// `ZEPPELIN_RAYON_THREADS`. It does not create either runtime or a Rayon
    /// pool.
    ///
    /// # Example
    ///
    /// On four CPUs, the computed values are eight query workers, one
    /// compaction worker, and four Rayon threads. Setting
    /// `ZEPPELIN_RAYON_THREADS=2` changes only the final value.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// [`std::thread::available_parallelism`] returns `NonZeroUsize`, so `.get()`
    /// converts a value already proven nonzero to `usize`. The `?` operator on
    /// each `env_override` call returns immediately on an invalid override while
    /// automatically converting through the crate's [`Result`] type. Java would
    /// usually propagate an exception; C would check and forward each error code.
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

        // Apply overrides only after computing a complete baseline so each
        // variable replaces exactly one worker pool.
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
    /// Loads, resolves, and validates the process configuration before startup.
    ///
    /// When `path` is present, Serde starts from that TOML document and uses
    /// each nested struct's defaults for omitted fields. With no path, loading
    /// starts from [`Config::default`]. Environment variables then replace file
    /// or default values, derived query choices are resolved, and cross-field
    /// validation runs last.
    ///
    /// # Parameters
    ///
    /// - `path`: Optional borrowed UTF-8 path to a TOML file. `None` means use
    ///   compiled defaults before applying environment overrides.
    ///
    /// # Returns
    ///
    /// A fully resolved, validated, owned configuration. Query code can assume
    /// [`QueryConfig::rerank_coalesce_gap_bytes`] has a concrete value.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Config`] if the file cannot be read, TOML is
    /// malformed or contains an unknown key, a present environment value cannot
    /// be parsed, query choices conflict after overrides, or any validation
    /// invariant fails. No service has started and no external state has been
    /// changed when this returns an error.
    ///
    /// # Side Effects
    ///
    /// Reads at most one local file and the supported process environment
    /// variables. It performs no object-store requests and writes no cache,
    /// manifest, WAL fragment, or segment.
    ///
    /// # Example
    ///
    /// ```text
    /// file:        server.port = 8080
    /// environment: ZEPPELIN_PORT=9090
    /// result:      config.server.port == 9090
    /// ```
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `Option<&str>` borrows a path when one exists; loading never takes
    /// ownership of the caller's string. The `?` operators propagate a typed
    /// error while dropping temporary owned values automatically. This resembles
    /// exceptions with deterministic resource cleanup in Java and explicit
    /// error forwarding plus cleanup in C.
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

    /// Validates all independent boot-time invariants and reports them together.
    ///
    /// Validation is separate from deserialization because important rules span
    /// fields: for example, default `top_k` must not exceed its maximum, default
    /// `nprobe` must not exceed its maximum, and the GC horizon must cover the
    /// sum of every reader-staleness interval.
    ///
    /// # Parameters
    ///
    /// - `self`: Configuration to inspect. It is borrowed immutably and is not
    ///   normalized or repaired.
    ///
    /// # Returns
    ///
    /// `Ok(())` when all invariants hold.
    ///
    /// # Errors
    ///
    /// Returns one [`ZeppelinError::Config`] containing a bullet for every
    /// detected violation. Validation does not stop after the first error.
    ///
    /// # Consistency
    ///
    /// The GC floor protects readers using a cached older manifest while a
    /// request is in flight and compaction uploads immutable artifacts before
    /// publication. Allowing a shorter horizon can delete an object that such a
    /// reader still legitimately needs; only the explicit unsafe override may
    /// bypass this check.
    ///
    /// # Example
    ///
    /// A config with `server.port = 0` and `indexing.default_nprobe` above
    /// `indexing.max_nprobe` returns one error that names both problems, allowing
    /// an operator to fix them in a single edit.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The function borrows `&self`, so it cannot modify the configuration.
    /// It accumulates owned diagnostic strings in a `Vec<String>` and moves them
    /// into the final joined message only on failure. Java would use a mutable
    /// list of strings; C would require explicit allocation and cleanup for each
    /// diagnostic.
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

    /// Calculates the minimum safe GC horizon in whole seconds.
    ///
    /// # Returns
    ///
    /// The sum of rounded-up manifest-cache TTL, request timeout, compaction
    /// upload window, and clock-skew allowance. Returns `None` if that sum would
    /// overflow `u64`; overflow is treated as a validation error by
    /// [`Config::validate`].
    ///
    /// # Example
    ///
    /// A 2,500 ms cache TTL, 30-second request timeout, 20-second upload window,
    /// and three-second skew allowance produce `Some(56)`.
    #[must_use]
    pub fn gc_horizon_floor_secs(&self) -> Option<u64> {
        self.checked_gc_horizon_floor_secs()
    }

    /// Reports whether the active config knowingly accepts an unsafe short GC horizon.
    ///
    /// # Returns
    ///
    /// `true` only when the override is enabled, the floor is representable,
    /// and [`GcConfig::horizon_secs`] is below it. An overflowing floor returns
    /// `false` here but is still rejected by [`Config::validate`].
    ///
    /// # Example
    ///
    /// With a 56-second floor, horizon 10, and
    /// `allow_unsafe_short_horizon = true`, this returns `true`.
    #[must_use]
    pub fn gc_horizon_is_unsafe_short(&self) -> bool {
        self.gc.allow_unsafe_short_horizon
            && self
                .checked_gc_horizon_floor_secs()
                .is_some_and(|floor_secs| self.gc.horizon_secs < floor_secs)
    }

    /// Emits the required structured warning for an active unsafe GC override.
    ///
    /// # Side Effects
    ///
    /// Writes one warning event through `tracing` when the explicit override is
    /// enabled and the configured horizon is below the computed floor. The event
    /// includes the floor and every contributing interval. Otherwise this is a
    /// no-op.
    ///
    /// # Example
    ///
    /// A deployment intentionally using a 10-second horizon against a 56-second
    /// floor logs both numbers and `allow_unsafe_short_horizon = true` during
    /// boot, making the risk visible to operators.
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

    /// Adds every GC reader-safety interval without allowing integer wraparound.
    ///
    /// # Returns
    ///
    /// `Some(total_seconds)` when every addition fits in `u64`, or `None` on
    /// overflow.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `checked_add` represents overflow as `Option::None`; `?` then propagates
    /// that absence through the remaining chain. Java's ordinary integer
    /// arithmetic and unsigned C arithmetic would require an explicit overflow
    /// check to avoid wrapping or losing the condition.
    fn checked_gc_horizon_floor_secs(&self) -> Option<u64> {
        self.manifest_cache_ttl_secs_for_gc_floor()
            .checked_add(self.server.request_timeout_secs)?
            .checked_add(self.gc.compaction_upload_window_secs)?
            .checked_add(self.gc.skew_slop_secs)
    }

    /// Rounds the manifest-cache TTL up from milliseconds to whole seconds.
    ///
    /// # Returns
    ///
    /// A ceiling conversion, so any partial second contributes a full second to
    /// the safety floor. For example, 2,500 ms becomes three seconds.
    fn manifest_cache_ttl_secs_for_gc_floor(&self) -> u64 {
        self.cache.manifest_cache_ttl_ms.div_ceil(1_000)
    }

    /// Replaces the two user-facing rerank choices with one concrete byte gap.
    ///
    /// # Parameters
    ///
    /// - `self`: Mutable configuration after environment overrides. On success,
    ///   the exact gap is `Some` and the profile is `None`.
    ///
    /// # Returns
    ///
    /// `Ok(())` after resolving an explicit byte value, a profile, or the
    /// compiled default in that order.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Config`] if both fields remain set after
    /// environment processing. The function returns before changing either
    /// field in that case.
    ///
    /// # Example
    ///
    /// `cost_latency_profile = "low_latency"` becomes a 128 KiB exact gap. An
    /// environment-provided exact gap has already cleared the file profile and
    /// therefore wins according to normal precedence.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The `Option` chain expresses precedence without nullable references:
    /// `.or_else(...)` evaluates the profile only when no exact value exists,
    /// and `.unwrap_or(...)` supplies the final compiled default. `Copy` scalar
    /// values move through this chain without allocation.
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

    /// Returns the effective rerank range-coalescing gap in bytes.
    ///
    /// # Returns
    ///
    /// The resolved exact gap. Fully loaded configurations always contain one;
    /// the compiled default is retained as a defensive convenience for tests or
    /// callers that construct [`Config`] directly.
    ///
    /// # Example
    ///
    /// A loaded low-latency profile returns 131,072. A direct
    /// `Config::default()` returns [`DEFAULT_RERANK_COALESCE_GAP_BYTES`].
    #[must_use]
    pub fn effective_rerank_coalesce_gap_bytes(&self) -> usize {
        self.query
            .rerank_coalesce_gap_bytes
            .unwrap_or(DEFAULT_RERANK_COALESCE_GAP_BYTES)
    }

    /// Applies every recognized environment override over file/default values.
    ///
    /// The method is intentionally centralized so precedence and accepted names
    /// are auditable. A present empty string is still a value: it is accepted
    /// for string fields, used to clear the optional S3 endpoint, and rejected
    /// when a numeric or boolean parser cannot interpret it.
    ///
    /// # Parameters
    ///
    /// - `self`: Mutable configuration built from TOML or compiled defaults.
    ///
    /// # Returns
    ///
    /// `Ok(())` after applying all present overrides.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Config`] on non-Unicode environment input,
    /// generic parse failure, or an unsupported named backend, hydration policy,
    /// or quantization value. Earlier fields may already have been mutated, but
    /// [`Config::load`] discards the in-progress configuration on error.
    ///
    /// # Side Effects
    ///
    /// Reads the process environment and mutates only `self`; it performs no
    /// network calls or persistent writes.
    ///
    /// # Example
    ///
    /// `ZEPPELIN_TRUSTED_PROXIES="10.0.0.0/8, 2001:db8::/32"` becomes two
    /// trimmed entries. `STORAGE_BACKEND=ftp` returns an error instead of
    /// choosing a storage fallback.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Each `if let Some(v)` unwraps only a present, successfully parsed value;
    /// absence leaves the previous layer untouched. The compiler infers each
    /// generic numeric or boolean type from the destination field, while string
    /// cases specify `::<String>` explicitly.
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
        if let Some(v) = env_override(GC_PITR_RETENTION_SECS_ENV)? {
            self.gc.pitr_retention_secs = v;
        }

        Ok(())
    }
}

/// Reads and parses one optional environment override without choosing a fallback.
///
/// # Parameters
///
/// - `name`: Static environment-variable name used in both the lookup and any
///   diagnostic. Callers pass string literals from the supported configuration
///   surface.
///
/// # Returns
///
/// `Ok(Some(value))` when the variable is present and parses as `T`, or
/// `Ok(None)` when it is absent. Absence is distinct from an empty string;
/// parsing decides whether an empty present value is valid.
///
/// # Errors
///
/// Returns [`ZeppelinError::Config`] when the value is not valid Unicode or
/// [`FromStr`] rejects it. The error names the variable, original value, target
/// Rust type, and parser diagnostic where available.
///
/// # Side Effects
///
/// Reads one value from the process environment. It does not remove or modify
/// the variable.
///
/// # Example
///
/// If `ZEPPELIN_PORT=9090`, `env_override::<u16>("ZEPPELIN_PORT")` returns
/// `Ok(Some(9090))`. If it is absent, the function returns `Ok(None)` so the
/// caller can preserve the TOML or default value.
///
/// # Rust Notes for Java/C Engineers
///
/// The `T: FromStr` bound accepts any destination type with a standard string
/// parser, and `T::Err: Display` guarantees its associated error can be shown.
/// This resembles a bounded generic parser in Java. C has no direct equivalent;
/// it would usually pass a conversion function pointer and untyped output
/// storage. Rust monomorphizes each used `T`, so this abstraction adds no
/// dynamic dispatch.
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
