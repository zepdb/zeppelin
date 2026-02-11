use prometheus::{register_histogram_vec, register_int_counter_vec, HistogramVec, IntCounterVec};

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
}

pub fn init() {
    lazy_static::initialize(&HTTP_REQUESTS_TOTAL);
    lazy_static::initialize(&QUERY_DURATION);
    lazy_static::initialize(&QUERIES_TOTAL);
    lazy_static::initialize(&WAL_APPENDS_TOTAL);
    lazy_static::initialize(&CACHE_HITS_TOTAL);
    lazy_static::initialize(&COMPACTIONS_TOTAL);
}
