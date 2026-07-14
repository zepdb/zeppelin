//! Prometheus instrumentation shared by Zeppelin's runtime subsystems.
//!
//! This module declares the process-local counters, gauges, and histograms used
//! by HTTP handling, object-store access, queries, caching, hydration,
//! compaction, indexing, full-text search, leases, and garbage collection.
//! Callers update these instruments after domain operations; metrics observe
//! behavior but never determine which S3 artifacts or manifests are valid.
//!
//! [`init`][crate::metrics::init] eagerly registers every instrument during startup so duplicate or
//! invalid Prometheus registrations fail before the server accepts traffic.
//! The instruments themselves live in a private module and are re-exported to
//! keep call sites concise.
//!
//! ## Metric lifecycle
//!
//! ```text
//! process startup
//!       |
//!       v
//! init() registers every metric
//!       |
//!       +---- request/query/storage code records observations
//!       |
//!       v
//! Prometheus scrape reads process-local values
//!
//! S3 and manifests remain authoritative; metrics are observational only.
//! ```
//!
//! ## Rust concepts used here
//!
//! `lazy_static!` provides one process-wide instance of each Prometheus
//! instrument. This resembles Java static initialization, but initialization is
//! deferred until first access unless [`init`][crate::metrics::init] forces it. In C, comparable code
//! would require global storage plus an explicit one-time initialization
//! protocol. [`GaugeGuard`][crate::metrics::GaugeGuard] demonstrates RAII: dropping an owned scope guard
//! automatically balances a previously incremented gauge.

#[allow(clippy::unwrap_used, missing_docs)]
mod inner {
    //! Lazy-initialized metric definitions grouped in one private registry.
    //!
    //! The public module re-exports these statics so instrumentation call sites
    //! do not depend on this organizational layer. Each definition includes its
    //! Prometheus name, help text, labels, and histogram buckets where needed.

    use prometheus::{
        register_gauge_vec, register_histogram_vec, register_int_counter, register_int_counter_vec,
        register_int_gauge, register_int_gauge_vec, GaugeVec, HistogramVec, IntCounter,
        IntCounterVec, IntGauge, IntGaugeVec,
    };

    lazy_static::lazy_static! {
        /// HTTP requests partitioned by method, route pattern, and response status.
        pub static ref HTTP_REQUESTS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_http_requests_total", "Total HTTP requests", &["method", "path", "status"]
        ).unwrap();
        /// End-to-end query latency in seconds for each namespace.
        pub static ref QUERY_DURATION: HistogramVec = register_histogram_vec!(
            "zeppelin_query_duration_seconds", "Query duration", &["namespace"],
            vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        ).unwrap();
        /// Queries accepted for execution, partitioned by namespace.
        pub static ref QUERIES_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_queries_total", "Total queries", &["namespace"]
        ).unwrap();
        /// WAL append operations partitioned by namespace.
        pub static ref WAL_APPENDS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_wal_appends_total", "WAL appends", &["namespace"]
        ).unwrap();
        /// Cache lookups partitioned by the resulting hit or miss classification.
        pub static ref CACHE_HITS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_cache_hits_total", "Cache hits", &["result"]
        ).unwrap();
        /// Completed compaction attempts partitioned by namespace and status.
        pub static ref COMPACTIONS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_compactions_total", "Compactions", &["namespace", "status"]
        ).unwrap();
        /// Bytes read by compaction, partitioned by namespace and artifact class.
        pub static ref COMPACTION_READ_BYTES_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_compaction_read_bytes_total",
            "Bytes read by compaction from immutable artifacts",
            &["namespace", "class"]
        ).unwrap();
        /// Object reads issued by compaction, partitioned by namespace and artifact class.
        pub static ref COMPACTION_READ_OPS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_compaction_read_ops_total",
            "Object read operations performed by compaction",
            &["namespace", "class"]
        ).unwrap();
        /// Compactions that chose full centroid retraining, partitioned by namespace.
        pub static ref COMPACTION_FULL_RETRAIN_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_compaction_full_retrain_total",
            "Compactions that retrained centroids instead of using the incremental path",
            &["namespace"]
        ).unwrap();
        /// Incremental attempts that fell back to full retraining, partitioned by reason.
        pub static ref COMPACTION_INCREMENTAL_FALLBACK_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_compaction_incremental_fallback_total",
            "Incremental compactions that fell back to a full retrain",
            &["namespace", "reason"]
        ).unwrap();
        /// Per-namespace flag indicating degradation after repeated compaction failure.
        pub static ref COMPACTION_NAMESPACE_DEGRADED: IntGaugeVec = register_int_gauge_vec!(
            "zeppelin_compaction_namespace_degraded",
            "Whether a namespace is degraded after repeated compaction failures",
            &["namespace"]
        ).unwrap();

        /// Object-store operation latency in seconds, partitioned by operation.
        pub static ref S3_OPERATION_DURATION: HistogramVec = register_histogram_vec!(
            "zeppelin_s3_operation_duration_seconds", "S3 operation latency",
            &["operation"],
            vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        ).unwrap();
        /// Failed object-store operations partitioned by operation.
        pub static ref S3_ERRORS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_s3_errors_total", "S3 operation errors", &["operation"]
        ).unwrap();
        /// End-to-end compaction latency in seconds for each namespace.
        pub static ref COMPACTION_DURATION: HistogramVec = register_histogram_vec!(
            "zeppelin_compaction_duration_seconds", "Compaction duration",
            &["namespace"],
            vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]
        ).unwrap();
        /// Current number of entries tracked by the disk cache.
        pub static ref CACHE_ENTRIES: IntGauge = register_int_gauge!(
            "zeppelin_cache_entries", "Number of entries in disk cache"
        ).unwrap();
        /// Disk-cache entries evicted since process start.
        pub static ref CACHE_EVICTIONS_TOTAL: IntCounter = register_int_counter!(
            "zeppelin_cache_evictions_total", "Total cache evictions"
        ).unwrap();
        /// Queries currently executing in this process.
        pub static ref ACTIVE_QUERIES: IntGauge = register_int_gauge!(
            "zeppelin_active_queries", "Number of in-flight queries"
        ).unwrap();
        /// Current effective byte gap used to coalesce rerank range reads.
        pub static ref RERANK_COALESCE_GAP_BYTES: IntGauge = register_int_gauge!(
            "zeppelin_rerank_coalesce_gap_bytes", "Effective rerank coalesce gap in bytes"
        ).unwrap();
        /// Range reads partitioned by query phase and serving source.
        pub static ref RANGE_SOURCE_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_range_source_total",
            "Range reads served by source",
            &["phase", "source"]
        ).unwrap();
        /// Hydration jobs admitted for execution, partitioned by trigger.
        pub static ref HYDRATION_JOBS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_hydration_jobs_total",
            "Hydration jobs accepted by trigger",
            &["trigger"]
        ).unwrap();
        /// Objects successfully hydrated, partitioned by artifact kind.
        pub static ref HYDRATION_OBJECTS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_hydration_objects_total",
            "Hydrated objects by kind",
            &["kind"]
        ).unwrap();
        /// Bytes successfully hydrated, partitioned by artifact kind.
        pub static ref HYDRATION_BYTES_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_hydration_bytes_total",
            "Hydrated bytes by kind",
            &["kind"]
        ).unwrap();
        /// Hydration jobs that failed after admission.
        pub static ref HYDRATION_FAILURES_TOTAL: IntCounter = register_int_counter!(
            "zeppelin_hydration_failures_total",
            "Hydration job failures"
        ).unwrap();
        /// Hydration jobs currently running in this process.
        pub static ref HYDRATION_INFLIGHT: IntGauge = register_int_gauge!(
            "zeppelin_hydration_inflight",
            "Hydration jobs currently running"
        ).unwrap();
        /// Hydration attempts skipped before fetching objects, partitioned by reason.
        pub static ref HYDRATION_SKIPPED_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_hydration_skipped_total",
            "Hydration jobs skipped before object fetch",
            &["reason"]
        ).unwrap();
        /// Latest hydration-refusal state for each namespace and reason.
        pub static ref HYDRATION_REFUSED: IntGaugeVec = register_int_gauge_vec!(
            "zeppelin_hydration_refused",
            "Whether a namespace's latest hydration attempt is currently refused",
            &["namespace", "reason"]
        ).unwrap();
        /// Estimated bytes required for each namespace's active warm set.
        pub static ref HYDRATION_REQUIRED_BYTES: GaugeVec = register_gauge_vec!(
            "zeppelin_hydration_required_bytes",
            "Bytes required to hydrate the active namespace warm set",
            &["namespace"]
        ).unwrap();
        /// Deduplicated hydration-refusal warnings partitioned by namespace and reason.
        pub static ref HYDRATION_REFUSAL_LOGS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_hydration_refusal_logs_total",
            "Capacity refusal warnings emitted after per-generation deduplication",
            &["namespace", "reason"]
        ).unwrap();
        /// Recent query heat observed for each namespace.
        pub static ref NAMESPACE_HEAT: IntGaugeVec = register_int_gauge_vec!(
            "zeppelin_namespace_heat",
            "Observed namespace query heat",
            &["namespace"]
        ).unwrap();
        /// Index-build latency in seconds, partitioned by namespace and index type.
        pub static ref INDEX_BUILD_DURATION: HistogramVec = register_histogram_vec!(
            "zeppelin_index_build_duration_seconds", "Index build duration",
            &["namespace", "index_type"],
            vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]
        ).unwrap();

        /// FTS inverted-index build latency in seconds for each namespace.
        pub static ref FTS_INDEX_BUILD_DURATION: HistogramVec = register_histogram_vec!(
            "zeppelin_fts_index_build_duration_seconds", "FTS inverted index build duration",
            &["namespace"],
            vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0]
        ).unwrap();
        /// Full-text queries accepted for execution, partitioned by namespace.
        pub static ref FTS_QUERIES_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_fts_queries_total", "Total FTS queries",
            &["namespace"]
        ).unwrap();

        /// Requests rejected by rate limiting, partitioned by rate-limit class.
        pub static ref RATE_LIMITED_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_rate_limited_total", "Requests rejected by rate limiter",
            &["class"]
        ).unwrap();

        /// Active process security posture, with exactly one mode label set to one.
        pub static ref SECURITY_MODE: IntGaugeVec = register_int_gauge_vec!(
            "zeppelin_security_mode",
            "Active Zeppelin process security mode",
            &["mode"]
        ).unwrap();

        /// Durable non-finite vectors skipped defensively during compaction.
        ///
        /// This records legacy or corrupt S3 data containing NaN or infinity;
        /// current validation should prevent new occurrences.
        pub static ref NON_FINITE_VECTORS_SKIPPED_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_non_finite_vectors_skipped_total",
            "Vectors with NaN/inf values skipped during compaction",
            &["namespace"]
        ).unwrap();

        /// Successful mid-compaction lease renewals for each namespace.
        pub static ref COMPACTION_LEASE_RENEWALS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_compaction_lease_renewals_total",
            "Successful mid-compaction lease renewals (heartbeat)",
            &["namespace"]
        ).unwrap();
        /// Compactions aborted after losing their lease, partitioned by namespace.
        pub static ref COMPACTION_LEASE_LOST_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_compaction_lease_lost_total",
            "Compactions aborted because the lease was lost mid-flight",
            &["namespace"]
        ).unwrap();

        /// WAL reads safely skipped after a fresh manifest proved compaction won a race.
        pub static ref WAL_FRAGMENT_GC_RACE_SKIPPED_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_wal_fragment_gc_race_skipped_total",
            "WAL fragment NotFound reads skipped after a fresh manifest confirmed compaction removed the fragment",
            &["namespace"]
        ).unwrap();

        /// Objects deleted by garbage collection for each namespace.
        pub static ref GC_OBJECTS_DELETED_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_gc_objects_deleted_total",
            "Objects deleted by storage garbage collection",
            &["namespace"]
        ).unwrap();
        /// Known bytes reclaimed by garbage collection for each namespace.
        pub static ref GC_BYTES_RECLAIMED_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_gc_bytes_reclaimed_total",
            "Known bytes reclaimed by storage garbage collection",
            &["namespace"]
        ).unwrap();
        /// Unreachable objects marked as garbage-collection candidates.
        pub static ref GC_CANDIDATES_MARKED_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_gc_candidates_marked_total",
            "Unreachable object candidates marked by storage garbage collection",
            &["namespace"]
        ).unwrap();
        /// Garbage-collection candidates retained instead of deleted, partitioned by reason.
        pub static ref GC_CANDIDATES_SKIPPED_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_gc_candidates_skipped_total",
            "Garbage-collection candidates skipped instead of deleted",
            &["namespace", "reason"]
        ).unwrap();
    }
}

// Keep instrumentation call sites independent of the private registration
// module; rustdoc still carries each metric's item documentation through the
// public re-export.
pub use inner::*;

use prometheus::IntGauge;

/// Scope guard that decrements an integer gauge when the guard is dropped.
///
/// Callers increment a gauge before constructing this guard. Every normal
/// return, `?` propagation, and panic unwind then runs [`Drop::drop`], balancing
/// the gauge without duplicating cleanup code.
///
/// # Examples
///
/// A query handler increments [`ACTIVE_QUERIES`] and creates a `GaugeGuard`.
/// When the handler finishes or returns an error, dropping the guard decrements
/// the gauge so it continues to represent in-flight queries rather than total
/// queries ever started.
///
/// # Rust Notes for Java/C Engineers
///
/// This is RAII. Java would commonly use `try`/`finally` or
/// `AutoCloseable`; C would use a single cleanup path or compiler extension.
/// Rust ties cleanup to ownership, so the compiler inserts `drop` wherever the
/// guard's scope ends.
pub struct GaugeGuard<'a>(
    /// Gauge borrowed for the entire lifetime of the guard.
    pub &'a IntGauge,
);

impl Drop for GaugeGuard<'_> {
    /// Balances the gauge increment associated with this guard.
    ///
    /// # Side Effects
    ///
    /// Decrements the referenced process-local Prometheus gauge exactly once
    /// when this guard is destroyed.
    fn drop(&mut self) {
        self.0.dec();
    }
}

/// Eagerly initializes and registers every Zeppelin Prometheus metric.
///
/// Startup calls this before serving requests so registration problems surface
/// immediately rather than on the first request that touches a particular
/// instrument.
///
/// # Returns
///
/// Returns unit after all lazy metric definitions have been initialized.
/// Repeated calls reuse the already initialized statics.
///
/// # Panics
///
/// Panics during a metric's first initialization if Prometheus rejects its
/// registration, including a duplicate name. The registration macros use
/// `unwrap` intentionally so invalid observability configuration fails loudly.
///
/// # Side Effects
///
/// Registers process-local collectors in the default Prometheus registry. It
/// does not contact a Prometheus server, write S3, or modify manifests.
///
/// # Performance
///
/// Performs one-time registration work during startup. Later calls only check
/// already initialized lazy statics.
///
/// # Examples
///
/// After `init()` succeeds, the metrics endpoint can expose every Zeppelin
/// metric even if no query, compaction, or hydration job has used it yet.
pub fn init() {
    lazy_static::initialize(&HTTP_REQUESTS_TOTAL);
    lazy_static::initialize(&QUERY_DURATION);
    lazy_static::initialize(&QUERIES_TOTAL);
    lazy_static::initialize(&WAL_APPENDS_TOTAL);
    lazy_static::initialize(&CACHE_HITS_TOTAL);
    lazy_static::initialize(&COMPACTIONS_TOTAL);
    lazy_static::initialize(&COMPACTION_READ_BYTES_TOTAL);
    lazy_static::initialize(&COMPACTION_READ_OPS_TOTAL);
    lazy_static::initialize(&COMPACTION_FULL_RETRAIN_TOTAL);
    lazy_static::initialize(&COMPACTION_INCREMENTAL_FALLBACK_TOTAL);
    lazy_static::initialize(&COMPACTION_NAMESPACE_DEGRADED);
    lazy_static::initialize(&S3_OPERATION_DURATION);
    lazy_static::initialize(&S3_ERRORS_TOTAL);
    lazy_static::initialize(&COMPACTION_DURATION);
    lazy_static::initialize(&CACHE_ENTRIES);
    lazy_static::initialize(&CACHE_EVICTIONS_TOTAL);
    lazy_static::initialize(&ACTIVE_QUERIES);
    lazy_static::initialize(&RERANK_COALESCE_GAP_BYTES);
    lazy_static::initialize(&RANGE_SOURCE_TOTAL);
    lazy_static::initialize(&HYDRATION_JOBS_TOTAL);
    lazy_static::initialize(&HYDRATION_OBJECTS_TOTAL);
    lazy_static::initialize(&HYDRATION_BYTES_TOTAL);
    lazy_static::initialize(&HYDRATION_FAILURES_TOTAL);
    lazy_static::initialize(&HYDRATION_INFLIGHT);
    lazy_static::initialize(&HYDRATION_SKIPPED_TOTAL);
    lazy_static::initialize(&HYDRATION_REFUSED);
    lazy_static::initialize(&HYDRATION_REQUIRED_BYTES);
    lazy_static::initialize(&HYDRATION_REFUSAL_LOGS_TOTAL);
    lazy_static::initialize(&NAMESPACE_HEAT);
    lazy_static::initialize(&INDEX_BUILD_DURATION);
    lazy_static::initialize(&FTS_INDEX_BUILD_DURATION);
    lazy_static::initialize(&FTS_QUERIES_TOTAL);
    lazy_static::initialize(&RATE_LIMITED_TOTAL);
    lazy_static::initialize(&SECURITY_MODE);
    lazy_static::initialize(&NON_FINITE_VECTORS_SKIPPED_TOTAL);
    lazy_static::initialize(&COMPACTION_LEASE_RENEWALS_TOTAL);
    lazy_static::initialize(&COMPACTION_LEASE_LOST_TOTAL);
    lazy_static::initialize(&WAL_FRAGMENT_GC_RACE_SKIPPED_TOTAL);
    lazy_static::initialize(&GC_OBJECTS_DELETED_TOTAL);
    lazy_static::initialize(&GC_BYTES_RECLAIMED_TOTAL);
    lazy_static::initialize(&GC_CANDIDATES_MARKED_TOTAL);
    lazy_static::initialize(&GC_CANDIDATES_SKIPPED_TOTAL);
}
