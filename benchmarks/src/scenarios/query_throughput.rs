//! Query throughput benchmark.
//!
//! Sustains maximum query load for a fixed duration and reports QPS.

use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

use crate::client::{BenchClient, QueryRequest};
use crate::datasets;
use crate::results;
use crate::Args;

pub async fn run(args: &Args, client: &dyn BenchClient) -> Result<serde_json::Value, anyhow::Error> {
    let ns = format!("bench-qthru-{}", rand::random::<u32>());

    client
        .create_namespace(&ns, args.dimensions, None)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    // Ingest
    let vectors = datasets::random_vectors(args.vectors, args.dimensions, false);
    eprintln!("  Ingesting {} vectors...", args.vectors);
    for batch in vectors.chunks(args.batch_size) {
        client
            .upsert(&ns, batch)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
    }
    eprintln!("  Ingestion complete. Running sustained load for {}s...", args.duration);

    let duration = Duration::from_secs(args.duration);
    let mut hist = Histogram::<u64>::new(3).unwrap();
    let mut total: u64 = 0;
    let mut err_count: u64 = 0;

    let start = Instant::now();

    while start.elapsed() < duration {
        let query_vec = datasets::random_query(args.dimensions);
        let req = QueryRequest {
            vector: Some(query_vec),
            top_k: args.top_k,
            filter: None,
            nprobe: args.nprobe,
            rank_by: None,
        };

        let t = Instant::now();
        match client.query(&ns, &req).await {
            Ok(_) => {
                hist.record(t.elapsed().as_micros() as u64).ok();
                total += 1;
            }
            Err(_) => {
                err_count += 1;
            }
        }
    }

    let elapsed = start.elapsed();
    let qps = total as f64 / elapsed.as_secs_f64();

    eprintln!(
        "  {total} queries in {:.1}s = {qps:.0} QPS ({err_count} errors)",
        elapsed.as_secs_f64()
    );

    let mut stats = results::latency_stats(&hist);
    stats["qps"] = serde_json::json!(qps);
    stats["total_queries"] = serde_json::json!(total);
    stats["errors"] = serde_json::json!(err_count);
    stats["duration_secs"] = serde_json::json!(elapsed.as_secs_f64());

    let _ = client.delete_namespace(&ns).await;

    Ok(stats)
}
