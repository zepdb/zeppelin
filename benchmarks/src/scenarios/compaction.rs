//! Compaction benchmark.
//!
//! Ingests data, triggers compaction, and measures post-compaction query latency.

use std::time::Instant;

use hdrhistogram::Histogram;

use crate::client::{BenchClient, QueryRequest};
use crate::datasets;
use crate::results;
use crate::Args;

pub async fn run(args: &Args, client: &dyn BenchClient) -> Result<serde_json::Value, anyhow::Error> {
    let ns = format!("bench-compact-{}", rand::random::<u32>());

    client
        .create_namespace(&ns, args.dimensions, None)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    // Ingest in small batches to create many WAL fragments
    let vectors = datasets::random_vectors(args.vectors, args.dimensions, false);
    let small_batch = 50;
    eprintln!(
        "  Ingesting {} vectors in batches of {small_batch} (to fragment WAL)...",
        args.vectors
    );

    let ingest_start = Instant::now();
    for batch in vectors.chunks(small_batch) {
        client
            .upsert(&ns, batch)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
    }
    let ingest_elapsed = ingest_start.elapsed();

    // Pre-compaction queries
    let n_queries = 200;
    eprintln!("  Running {n_queries} pre-compaction queries...");
    let mut pre_hist = Histogram::<u64>::new(3).unwrap();

    for _ in 0..n_queries {
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
            pre_hist.record(t.elapsed().as_micros() as u64).ok();
        }
    }

    // Wait for compaction (Zeppelin compacts in background)
    // For now, just sleep and hope compaction has run
    eprintln!("  Waiting 10s for background compaction...");
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    // Post-compaction queries
    eprintln!("  Running {n_queries} post-compaction queries...");
    let mut post_hist = Histogram::<u64>::new(3).unwrap();

    for _ in 0..n_queries {
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
            post_hist.record(t.elapsed().as_micros() as u64).ok();
        }
    }

    eprintln!(
        "  Pre-compaction p50={:.1}ms, Post-compaction p50={:.1}ms",
        pre_hist.value_at_quantile(0.50) as f64 / 1000.0,
        post_hist.value_at_quantile(0.50) as f64 / 1000.0,
    );

    let _ = client.delete_namespace(&ns).await;

    Ok(serde_json::json!({
        "ingest_secs": ingest_elapsed.as_secs_f64(),
        "pre_compaction": results::latency_stats(&pre_hist),
        "post_compaction": results::latency_stats(&post_hist),
    }))
}
