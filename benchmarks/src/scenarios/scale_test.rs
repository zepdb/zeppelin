//! Scale test benchmark — 6-phase deep benchmark.
//!
//! Ingests `args.vectors` then runs 6 phases to reveal scaling behavior:
//!   1. Ingest with progress windows (detect WAL growth slowdown)
//!   2. Pre-compaction queries (worst-case: scan all WAL fragments)
//!   3. Compaction wait with stabilization detection
//!   4. Post-compaction cold cache queries
//!   5. Post-compaction warm cache queries
//!   6. Sustained throughput (60s)

use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

use crate::client::{BenchClient, QueryRequest};
use crate::datasets;
use crate::results;
use crate::Args;

/// Check if latencies have stabilized: 3 consecutive polls within 15% of their mean.
fn is_stabilized(recent_p50s: &[f64]) -> bool {
    if recent_p50s.len() < 3 {
        return false;
    }
    let last3 = &recent_p50s[recent_p50s.len() - 3..];
    let mean = last3.iter().sum::<f64>() / 3.0;
    if mean == 0.0 {
        return true;
    }
    last3.iter().all(|v| (v - mean).abs() / mean < 0.15)
}

/// Run sustained queries for a fixed duration, returning (total_queries, histogram).
async fn run_sustained_queries(
    client: &dyn BenchClient,
    ns: &str,
    dims: usize,
    top_k: usize,
    nprobe: Option<usize>,
    duration: Duration,
) -> (u64, Histogram<u64>) {
    let mut hist = Histogram::<u64>::new(3).unwrap();
    let start = Instant::now();
    let mut total = 0u64;

    while start.elapsed() < duration {
        let query_vec = datasets::random_query(dims);
        let req = QueryRequest {
            vector: Some(query_vec),
            top_k,
            filter: None,
            nprobe,
            rank_by: None,
        };
        let t = Instant::now();
        if client.query(ns, &req).await.is_ok() {
            hist.record(t.elapsed().as_micros() as u64).ok();
            total += 1;
        }
    }

    (total, hist)
}

pub async fn run(args: &Args, client: &dyn BenchClient) -> Result<serde_json::Value, anyhow::Error> {
    let scale = args.vectors;
    let ns = format!("bench-scale-{scale}-{}", rand::random::<u32>());

    client
        .create_namespace(&ns, args.dimensions, None)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    // ── Phase 1: Ingest with progress windows ──────────────────────────────
    eprintln!("  Phase 1: Ingesting {scale} vectors in batches of {}...", args.batch_size);
    let window_size = 10_000.max(scale / 10); // snapshot every 10K or 10% of total
    let ingest_start = Instant::now();
    let mut window_start = Instant::now();
    let mut ingested = 0usize;
    let mut window_ingested = 0usize;
    let mut windows = Vec::new();

    while ingested < scale {
        let batch_count = args.batch_size.min(scale - ingested);
        let batch = datasets::random_vectors(batch_count, args.dimensions, false);
        client
            .upsert(&ns, &batch)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        ingested += batch_count;
        window_ingested += batch_count;

        if window_ingested >= window_size {
            let window_elapsed = window_start.elapsed().as_secs_f64();
            let window_vps = window_ingested as f64 / window_elapsed;
            let cumulative_vps = ingested as f64 / ingest_start.elapsed().as_secs_f64();
            eprintln!(
                "    {ingested}/{scale}: window={window_vps:.0} vps, cumulative={cumulative_vps:.0} vps"
            );
            windows.push(serde_json::json!({
                "vectors_so_far": ingested,
                "window_vps": window_vps,
                "cumulative_vps": cumulative_vps,
            }));
            window_ingested = 0;
            window_start = Instant::now();
        }
    }

    let ingest_elapsed = ingest_start.elapsed();
    let total_vps = scale as f64 / ingest_elapsed.as_secs_f64();
    eprintln!(
        "  Phase 1 done: {scale} vectors in {:.1}s ({total_vps:.0} vps)",
        ingest_elapsed.as_secs_f64()
    );

    // ── Phase 2: Pre-compaction queries ────────────────────────────────────
    let n_queries = 200;
    eprintln!("  Phase 2: Pre-compaction queries ({n_queries})...");
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

    eprintln!(
        "  Phase 2 done: p50={:.1}ms p99={:.1}ms",
        pre_hist.value_at_quantile(0.50) as f64 / 1000.0,
        pre_hist.value_at_quantile(0.99) as f64 / 1000.0,
    );

    // ── Phase 3: Compaction wait with stabilization ────────────────────────
    eprintln!("  Phase 3: Waiting for compaction to stabilize (up to 120s)...");
    let compact_start = Instant::now();
    let max_wait = Duration::from_secs(120);
    let poll_interval = Duration::from_secs(5);
    let queries_per_poll = 10;
    let mut poll_p50s: Vec<f64> = Vec::new();
    let mut stabilized = false;

    while compact_start.elapsed() < max_wait {
        tokio::time::sleep(poll_interval).await;

        let mut poll_hist = Histogram::<u64>::new(3).unwrap();
        for _ in 0..queries_per_poll {
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
                poll_hist.record(t.elapsed().as_micros() as u64).ok();
            }
        }

        let p50 = poll_hist.value_at_quantile(0.50) as f64 / 1000.0;
        poll_p50s.push(p50);
        eprintln!(
            "    poll {}: p50={p50:.1}ms (elapsed {:.0}s)",
            poll_p50s.len(),
            compact_start.elapsed().as_secs_f64()
        );

        if is_stabilized(&poll_p50s) {
            stabilized = true;
            eprintln!("    Stabilized after {:.1}s", compact_start.elapsed().as_secs_f64());
            break;
        }
    }

    let compaction_wait_secs = compact_start.elapsed().as_secs_f64();
    if !stabilized {
        eprintln!("    WARNING: Did not stabilize within {max_wait:?}");
    }

    // ── Phase 4: Post-compaction cold cache ────────────────────────────────
    eprintln!("  Phase 4: Post-compaction cold cache queries ({n_queries})...");
    let mut cold_hist = Histogram::<u64>::new(3).unwrap();

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
            cold_hist.record(t.elapsed().as_micros() as u64).ok();
        }
    }

    eprintln!(
        "  Phase 4 done: p50={:.1}ms p99={:.1}ms",
        cold_hist.value_at_quantile(0.50) as f64 / 1000.0,
        cold_hist.value_at_quantile(0.99) as f64 / 1000.0,
    );

    // ── Phase 5: Post-compaction warm cache ────────────────────────────────
    eprintln!("  Phase 5: Post-compaction warm cache queries ({n_queries})...");
    let mut warm_hist = Histogram::<u64>::new(3).unwrap();

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
            warm_hist.record(t.elapsed().as_micros() as u64).ok();
        }
    }

    eprintln!(
        "  Phase 5 done: p50={:.1}ms p99={:.1}ms",
        warm_hist.value_at_quantile(0.50) as f64 / 1000.0,
        warm_hist.value_at_quantile(0.99) as f64 / 1000.0,
    );

    // ── Phase 6: Sustained throughput (60s) ────────────────────────────────
    let sustained_duration = Duration::from_secs(60);
    eprintln!("  Phase 6: Sustained throughput for {}s...", sustained_duration.as_secs());

    let (total_queries, sustained_hist) = run_sustained_queries(
        client,
        &ns,
        args.dimensions,
        args.top_k,
        args.nprobe,
        sustained_duration,
    )
    .await;

    let qps = total_queries as f64 / sustained_duration.as_secs_f64();
    eprintln!(
        "  Phase 6 done: {qps:.1} QPS, {} queries, p50={:.1}ms",
        total_queries,
        sustained_hist.value_at_quantile(0.50) as f64 / 1000.0,
    );

    // Cleanup
    let _ = client.delete_namespace(&ns).await;

    // ── Build result JSON ──────────────────────────────────────────────────
    Ok(serde_json::json!({
        "scale": scale,
        "ingest": {
            "total_secs": ingest_elapsed.as_secs_f64(),
            "total_vps": total_vps,
            "windows": windows,
        },
        "pre_compaction": results::latency_stats(&pre_hist),
        "compaction_wait_secs": compaction_wait_secs,
        "compaction_stabilized": stabilized,
        "compaction_polls": poll_p50s,
        "post_compaction_cold": results::latency_stats(&cold_hist),
        "post_compaction_warm": results::latency_stats(&warm_hist),
        "sustained_throughput": {
            "qps": qps,
            "total_queries": total_queries,
            "latency": results::latency_stats(&sustained_hist),
        }
    }))
}
