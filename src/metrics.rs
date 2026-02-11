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
}

/// RAII guard that decrements an IntGauge on drop.
pub struct GaugeGuard<'a>(pub &'a IntGauge);

impl Drop for GaugeGuard<'_> {
    fn drop(&mut self) {
        self.0.dec();
    }
}

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
}
