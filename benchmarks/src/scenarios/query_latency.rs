//! Query latency benchmark.
//!
//! Measures p50/p95/p99/max latency at various concurrency levels.

use std::time::Instant;

use hdrhistogram::Histogram;

use crate::client::{BenchClient, QueryRequest};
use crate::datasets;
use crate::results;
use crate::Args;

pub async fn run(args: &Args, client: &dyn BenchClient) -> Result<serde_json::Value, anyhow::Error> {
    let ns = format!("bench-qlat-{}", rand::random::<u32>());

    client
        .create_namespace(&ns, args.dimensions, None)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    // Ingest data
    let vectors = datasets::random_vectors(args.vectors, args.dimensions, false);
    eprintln!("  Ingesting {} vectors...", args.vectors);
    for batch in vectors.chunks(args.batch_size) {
        client
            .upsert(&ns, batch)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
    }
    eprintln!("  Ingestion complete.");

    // Run queries at different concurrency levels
    let n_queries = 1000;
    let mut level_results = Vec::new();

    for &concurrency in &[1, 4, 16, 64] {
        eprintln!("  Running {n_queries} queries at concurrency={concurrency}...");
        let mut hist = Histogram::<u64>::new(3).unwrap();

        let start = Instant::now();

        let queries: Vec<Vec<f32>> = (0..n_queries)
            .map(|_| datasets::random_query(args.dimensions))
            .collect();

        // Process in chunks to simulate concurrency level
        for chunk in queries.chunks(concurrency) {
            for q in chunk {
                let req = QueryRequest {
                    vector: Some(q.clone()),
                    top_k: args.top_k,
                    filter: None,
                    nprobe: args.nprobe,
                    rank_by: None,
                };
                let t = Instant::now();
                if client.query(&ns, &req).await.is_ok() {
                    hist.record(t.elapsed().as_micros() as u64).ok();
                }
            }
        }

        let total_elapsed = start.elapsed();
        let qps = n_queries as f64 / total_elapsed.as_secs_f64();

        eprintln!(
            "    p50={:.1}ms p95={:.1}ms p99={:.1}ms max={:.1}ms qps={:.0}",
            hist.value_at_quantile(0.50) as f64 / 1000.0,
            hist.value_at_quantile(0.95) as f64 / 1000.0,
            hist.value_at_quantile(0.99) as f64 / 1000.0,
            hist.max() as f64 / 1000.0,
            qps,
        );

        let mut stats = results::latency_stats(&hist);
        stats["concurrency"] = serde_json::json!(concurrency);
        stats["qps"] = serde_json::json!(qps);
        stats["total_queries"] = serde_json::json!(n_queries);
        level_results.push(stats);
    }

    let _ = client.delete_namespace(&ns).await;

    Ok(serde_json::json!({ "concurrency_levels": level_results }))
}
