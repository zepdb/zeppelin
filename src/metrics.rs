/// Lazy-initialized Prometheus metric definitions.
#[allow(clippy::unwrap_used, missing_docs)]
mod inner {
    use prometheus::{
        register_gauge_vec, register_histogram_vec, register_int_counter, register_int_counter_vec,
        register_int_gauge, register_int_gauge_vec, GaugeVec, HistogramVec, IntCounter,
        IntCounterVec, IntGauge, IntGaugeVec,
    };

    lazy_static::lazy_static! {
        pub static ref HTTP_REQUESTS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_http_requests_total", "Total HTTP requests", &["method", "path", "status"]
        ).unwrap();
        pub static ref QUERY_DURATION: HistogramVec = register_histogram_vec!(
            "zeppelin_query_duration_seconds", "Query duration", &["namespace"],
            vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        ).unwrap();
        pub static ref QUERIES_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_queries_total", "Total queries", &["namespace"]
        ).unwrap();
        pub static ref WAL_APPENDS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_wal_appends_total", "WAL appends", &["namespace"]
        ).unwrap();
        pub static ref CACHE_HITS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_cache_hits_total", "Cache hits", &["result"]
        ).unwrap();
        pub static ref COMPACTIONS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_compactions_total", "Compactions", &["namespace", "status"]
        ).unwrap();
        pub static ref COMPACTION_READ_BYTES_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_compaction_read_bytes_total",
            "Bytes read by compaction from immutable artifacts",
            &["namespace", "class"]
        ).unwrap();
        pub static ref COMPACTION_READ_OPS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_compaction_read_ops_total",
            "Object read operations performed by compaction",
            &["namespace", "class"]
        ).unwrap();
        pub static ref COMPACTION_FULL_RETRAIN_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_compaction_full_retrain_total",
            "Compactions that retrained centroids instead of using the incremental path",
            &["namespace"]
        ).unwrap();
        pub static ref COMPACTION_INCREMENTAL_FALLBACK_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_compaction_incremental_fallback_total",
            "Incremental compactions that fell back to a full retrain",
            &["namespace", "reason"]
        ).unwrap();
        pub static ref COMPACTION_NAMESPACE_DEGRADED: IntGaugeVec = register_int_gauge_vec!(
            "zeppelin_compaction_namespace_degraded",
            "Whether a namespace is degraded after repeated compaction failures",
            &["namespace"]
        ).unwrap();

        // New metrics — Phase 6
        pub static ref S3_OPERATION_DURATION: HistogramVec = register_histogram_vec!(
            "zeppelin_s3_operation_duration_seconds", "S3 operation latency",
            &["operation"],
            vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        ).unwrap();
        pub static ref S3_ERRORS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_s3_errors_total", "S3 operation errors", &["operation"]
        ).unwrap();
        pub static ref COMPACTION_DURATION: HistogramVec = register_histogram_vec!(
            "zeppelin_compaction_duration_seconds", "Compaction duration",
            &["namespace"],
            vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]
        ).unwrap();
        pub static ref CACHE_ENTRIES: IntGauge = register_int_gauge!(
            "zeppelin_cache_entries", "Number of entries in disk cache"
        ).unwrap();
        pub static ref CACHE_EVICTIONS_TOTAL: IntCounter = register_int_counter!(
            "zeppelin_cache_evictions_total", "Total cache evictions"
        ).unwrap();
        pub static ref ACTIVE_QUERIES: IntGauge = register_int_gauge!(
            "zeppelin_active_queries", "Number of in-flight queries"
        ).unwrap();
        pub static ref RERANK_COALESCE_GAP_BYTES: IntGauge = register_int_gauge!(
            "zeppelin_rerank_coalesce_gap_bytes", "Effective rerank coalesce gap in bytes"
        ).unwrap();
        pub static ref RANGE_SOURCE_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_range_source_total",
            "Range reads served by source",
            &["phase", "source"]
        ).unwrap();
        pub static ref HYDRATION_JOBS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_hydration_jobs_total",
            "Hydration jobs accepted by trigger",
            &["trigger"]
        ).unwrap();
        pub static ref HYDRATION_OBJECTS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_hydration_objects_total",
            "Hydrated objects by kind",
            &["kind"]
        ).unwrap();
        pub static ref HYDRATION_BYTES_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_hydration_bytes_total",
            "Hydrated bytes by kind",
            &["kind"]
        ).unwrap();
        pub static ref HYDRATION_FAILURES_TOTAL: IntCounter = register_int_counter!(
            "zeppelin_hydration_failures_total",
            "Hydration job failures"
        ).unwrap();
        pub static ref HYDRATION_INFLIGHT: IntGauge = register_int_gauge!(
            "zeppelin_hydration_inflight",
            "Hydration jobs currently running"
        ).unwrap();
        pub static ref HYDRATION_SKIPPED_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_hydration_skipped_total",
            "Hydration jobs skipped before object fetch",
            &["reason"]
        ).unwrap();
        pub static ref HYDRATION_REFUSED: IntGaugeVec = register_int_gauge_vec!(
            "zeppelin_hydration_refused",
            "Whether a namespace's latest hydration attempt is currently refused",
            &["namespace", "reason"]
        ).unwrap();
        pub static ref HYDRATION_REQUIRED_BYTES: GaugeVec = register_gauge_vec!(
            "zeppelin_hydration_required_bytes",
            "Bytes required to hydrate the active namespace warm set",
            &["namespace"]
        ).unwrap();
        pub static ref HYDRATION_REFUSAL_LOGS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_hydration_refusal_logs_total",
            "Capacity refusal warnings emitted after per-generation deduplication",
            &["namespace", "reason"]
        ).unwrap();
        pub static ref NAMESPACE_HEAT: IntGaugeVec = register_int_gauge_vec!(
            "zeppelin_namespace_heat",
            "Observed namespace query heat",
            &["namespace"]
        ).unwrap();
        pub static ref INDEX_BUILD_DURATION: HistogramVec = register_histogram_vec!(
            "zeppelin_index_build_duration_seconds", "Index build duration",
            &["namespace", "index_type"],
            vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]
        ).unwrap();

        // Full-text search metrics
        pub static ref FTS_INDEX_BUILD_DURATION: HistogramVec = register_histogram_vec!(
            "zeppelin_fts_index_build_duration_seconds", "FTS inverted index build duration",
            &["namespace"],
            vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0]
        ).unwrap();
        pub static ref FTS_QUERIES_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_fts_queries_total", "Total FTS queries",
            &["namespace"]
        ).unwrap();

        // Rate limiting metrics
        pub static ref RATE_LIMITED_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_rate_limited_total", "Requests rejected by rate limiter",
            &["ip"]
        ).unwrap();

        // Per-IP request tracking (for Grafana live table)
        pub static ref REQUESTS_BY_IP_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_requests_by_ip_total", "Requests by source IP",
            &["ip", "method", "path", "status"]
        ).unwrap();

        // Data-quality defense in depth: pre-fix non-finite vectors found
        // durable on S3 and skipped during compaction (Task 10 I4).
        pub static ref NON_FINITE_VECTORS_SKIPPED_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_non_finite_vectors_skipped_total",
            "Vectors with NaN/inf values skipped during compaction",
            &["namespace"]
        ).unwrap();

        // Mid-compaction lease heartbeat (Task 2 Phase A).
        pub static ref COMPACTION_LEASE_RENEWALS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_compaction_lease_renewals_total",
            "Successful mid-compaction lease renewals (heartbeat)",
            &["namespace"]
        ).unwrap();
        pub static ref COMPACTION_LEASE_LOST_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_compaction_lease_lost_total",
            "Compactions aborted because the lease was lost mid-flight",
            &["namespace"]
        ).unwrap();

        pub static ref WAL_FRAGMENT_GC_RACE_SKIPPED_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_wal_fragment_gc_race_skipped_total",
            "WAL fragment NotFound reads skipped after a fresh manifest confirmed compaction removed the fragment",
            &["namespace"]
        ).unwrap();

        pub static ref GC_OBJECTS_DELETED_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_gc_objects_deleted_total",
            "Objects deleted by storage garbage collection",
            &["namespace"]
        ).unwrap();
        pub static ref GC_BYTES_RECLAIMED_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_gc_bytes_reclaimed_total",
            "Known bytes reclaimed by storage garbage collection",
            &["namespace"]
        ).unwrap();
        pub static ref GC_CANDIDATES_MARKED_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_gc_candidates_marked_total",
            "Unreachable object candidates marked by storage garbage collection",
            &["namespace"]
        ).unwrap();
        pub static ref GC_CANDIDATES_SKIPPED_TOTAL: IntCounterVec = register_int_counter_vec!(
            "zeppelin_gc_candidates_skipped_total",
            "Garbage-collection candidates skipped instead of deleted",
            &["namespace", "reason"]
        ).unwrap();
    }
}

// Re-export all metrics at the module level
pub use inner::*;

use prometheus::IntGauge;

/// RAII guard that decrements an IntGauge on drop.
pub struct GaugeGuard<'a>(pub &'a IntGauge);

impl Drop for GaugeGuard<'_> {
    fn drop(&mut self) {
        self.0.dec();
    }
}

/// Initialize all Prometheus metrics eagerly.
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
    lazy_static::initialize(&REQUESTS_BY_IP_TOTAL);
    lazy_static::initialize(&NON_FINITE_VECTORS_SKIPPED_TOTAL);
    lazy_static::initialize(&COMPACTION_LEASE_RENEWALS_TOTAL);
    lazy_static::initialize(&COMPACTION_LEASE_LOST_TOTAL);
    lazy_static::initialize(&WAL_FRAGMENT_GC_RACE_SKIPPED_TOTAL);
    lazy_static::initialize(&GC_OBJECTS_DELETED_TOTAL);
    lazy_static::initialize(&GC_BYTES_RECLAIMED_TOTAL);
    lazy_static::initialize(&GC_CANDIDATES_MARKED_TOTAL);
    lazy_static::initialize(&GC_CANDIDATES_SKIPPED_TOTAL);
}
