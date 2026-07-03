/// Lazy-initialized Prometheus metric definitions.
#[allow(clippy::unwrap_used, missing_docs)]
mod inner {
    use prometheus::{
        register_histogram_vec, register_int_counter, register_int_counter_vec, register_int_gauge,
        HistogramVec, IntCounter, IntCounterVec, IntGauge,
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
    lazy_static::initialize(&S3_OPERATION_DURATION);
    lazy_static::initialize(&S3_ERRORS_TOTAL);
    lazy_static::initialize(&COMPACTION_DURATION);
    lazy_static::initialize(&CACHE_ENTRIES);
    lazy_static::initialize(&CACHE_EVICTIONS_TOTAL);
    lazy_static::initialize(&ACTIVE_QUERIES);
    lazy_static::initialize(&INDEX_BUILD_DURATION);
    lazy_static::initialize(&FTS_INDEX_BUILD_DURATION);
    lazy_static::initialize(&FTS_QUERIES_TOTAL);
    lazy_static::initialize(&RATE_LIMITED_TOTAL);
    lazy_static::initialize(&REQUESTS_BY_IP_TOTAL);
    lazy_static::initialize(&NON_FINITE_VECTORS_SKIPPED_TOTAL);
    lazy_static::initialize(&COMPACTION_LEASE_RENEWALS_TOTAL);
    lazy_static::initialize(&COMPACTION_LEASE_LOST_TOTAL);
    lazy_static::initialize(&WAL_FRAGMENT_GC_RACE_SKIPPED_TOTAL);
}
