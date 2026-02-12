//! Mixed workload benchmark.
//!
//! Simulates 80% reads / 20% writes and measures both.

use std::time::Instant;

use hdrhistogram::Histogram;
use rand::Rng;

use crate::client::{BenchClient, QueryRequest};
use crate::datasets;
use crate::results;
use crate::Args;

pub async fn run(args: &Args, client: &dyn BenchClient) -> Result<serde_json::Value, anyhow::Error> {
    let ns = format!("bench-mixed-{}", rand::random::<u32>());

    client
        .create_namespace(&ns, args.dimensions, None)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    // Pre-load some data
    let initial_count = args.vectors / 2;
    let vectors = datasets::random_vectors(initial_count, args.dimensions, false);
    eprintln!("  Pre-loading {initial_count} vectors...");
    for batch in vectors.chunks(args.batch_size) {
        client
            .upsert(&ns, batch)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
    }

    // Run mixed workload
    let total_ops = 1000;
    let mut rng = rand::thread_rng();
    let mut read_hist = Histogram::<u64>::new(3).unwrap();
    let mut write_hist = Histogram::<u64>::new(3).unwrap();
    let mut reads = 0u64;
    let mut writes = 0u64;
    let mut _write_offset = initial_count;

    eprintln!("  Running {total_ops} mixed operations (80% read / 20% write)...");
    let start = Instant::now();

    for _ in 0..total_ops {
        if rng.gen_range(0..100) < 80 {
            // Read
            let query_vec = datasets::random_query(args.dimensions);
            let req = QueryRequest {
                vector: Some(query_vec),
                top_k: args.top_k,
                filter: None,
                nprobe: args.nprobe,
                rank_by: None,
            };
            let t = Instant::now();
            if client.query(&ns, &req).await.is_ok() {
                read_hist.record(t.elapsed().as_micros() as u64).ok();
                reads += 1;
            }
        } else {
            // Write (small batch)
            let batch_size = 10;
            let batch = datasets::random_vectors(batch_size, args.dimensions, false);
            let t = Instant::now();
            if client.upsert(&ns, &batch).await.is_ok() {
                write_hist.record(t.elapsed().as_micros() as u64).ok();
                writes += 1;
                _write_offset += batch_size;
            }
        }
    }

    let elapsed = start.elapsed();
    eprintln!(
        "  {reads} reads + {writes} writes in {:.1}s",
        elapsed.as_secs_f64()
    );

    let _ = client.delete_namespace(&ns).await;

    Ok(serde_json::json!({
        "total_ops": total_ops,
        "reads": reads,
        "writes": writes,
        "elapsed_secs": elapsed.as_secs_f64(),
        "ops_per_sec": total_ops as f64 / elapsed.as_secs_f64(),
        "read_latency": results::latency_stats(&read_hist),
        "write_latency": results::latency_stats(&write_hist),
    }))
}
