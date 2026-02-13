//! Query latency benchmark.
//!
//! Measures p50/p95/p99/max latency at various concurrency levels using
//! FuturesUnordered for true concurrent query execution.

use std::pin::Pin;
use std::time::Instant;

use futures::stream::{FuturesUnordered, StreamExt};
use futures::Future;
use hdrhistogram::Histogram;

use crate::client::{BenchClient, QueryRequest, QueryResponse};
use crate::datasets;
use crate::results;
use crate::Args;

type QueryFuture<'a> =
    Pin<Box<dyn Future<Output = (Instant, Result<QueryResponse, String>)> + Send + 'a>>;

fn submit_query<'a>(
    client: &'a dyn BenchClient,
    ns: &'a str,
    dims: usize,
    top_k: usize,
    nprobe: Option<usize>,
) -> QueryFuture<'a> {
    let query_vec = datasets::random_query(dims);
    let req = QueryRequest {
        vector: Some(query_vec),
        top_k,
        filter: None,
        nprobe,
        rank_by: None,
    };
    let t = Instant::now();
    Box::pin(async move {
        let result = client.query(ns, &req).await;
        (t, result)
    })
}

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
        let mut completed = 0u64;

        let start = Instant::now();
        let mut pending: FuturesUnordered<QueryFuture<'_>> = FuturesUnordered::new();

        // Seed the pipeline up to concurrency level
        let mut submitted = 0usize;
        while submitted < concurrency.min(n_queries) {
            pending.push(submit_query(client, &ns, args.dimensions, args.top_k, args.nprobe));
            submitted += 1;
        }

        // Drive concurrent queries, submitting new ones as each completes
        while let Some((t, result)) = pending.next().await {
            if result.is_ok() {
                hist.record(t.elapsed().as_micros() as u64).ok();
                completed += 1;
            }

            // Submit more until we've queued all n_queries
            if submitted < n_queries {
                pending.push(submit_query(client, &ns, args.dimensions, args.top_k, args.nprobe));
                submitted += 1;
            }
        }

        let total_elapsed = start.elapsed();
        let qps = completed as f64 / total_elapsed.as_secs_f64();

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
        stats["total_queries"] = serde_json::json!(completed);
        level_results.push(stats);
    }

    let _ = client.delete_namespace(&ns).await;

    Ok(serde_json::json!({ "concurrency_levels": level_results }))
}
