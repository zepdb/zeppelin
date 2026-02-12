//! Structured JSON output for benchmark results.

use serde::Serialize;

/// Top-level benchmark result.
#[derive(Debug, Serialize)]
pub struct BenchResult {
    pub timestamp: String,
    pub target: String,
    pub scenario: String,
    pub config: BenchConfig,
    pub results: serde_json::Value,
}

/// Configuration snapshot for reproducibility.
#[derive(Debug, Serialize)]
pub struct BenchConfig {
    pub vectors: usize,
    pub dimensions: usize,
    pub concurrency: usize,
    pub batch_size: usize,
    pub duration_secs: u64,
    pub top_k: usize,
}

impl BenchResult {
    pub fn new(
        target: &str,
        scenario: &str,
        args: &super::Args,
        results: serde_json::Value,
    ) -> Self {
        Self {
            timestamp: chrono::Utc::now().to_rfc3339(),
            target: target.to_string(),
            scenario: scenario.to_string(),
            config: BenchConfig {
                vectors: args.vectors,
                dimensions: args.dimensions,
                concurrency: args.concurrency,
                batch_size: args.batch_size,
                duration_secs: args.duration,
                top_k: args.top_k,
            },
            results,
        }
    }
}

/// Build a latency stats JSON object from an HDR histogram.
pub fn latency_stats(hist: &hdrhistogram::Histogram<u64>) -> serde_json::Value {
    serde_json::json!({
        "p50_ms": hist.value_at_quantile(0.50) as f64 / 1000.0,
        "p95_ms": hist.value_at_quantile(0.95) as f64 / 1000.0,
        "p99_ms": hist.value_at_quantile(0.99) as f64 / 1000.0,
        "max_ms": hist.max() as f64 / 1000.0,
        "mean_ms": hist.mean() / 1000.0,
        "count": hist.len(),
    })
}
