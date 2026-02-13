//! Query throughput benchmark.
//!
//! Sustains maximum query load for a fixed duration with concurrent clients and reports QPS.

use std::pin::Pin;
use std::time::{Duration, Instant};

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
    eprintln!(
        "  Ingestion complete. Running sustained load for {}s at concurrency={}...",
        args.duration, args.concurrency
    );

    let duration = Duration::from_secs(args.duration);
    let concurrency = args.concurrency;
    let mut hist = Histogram::<u64>::new(3).unwrap();
    let mut total: u64 = 0;
    let mut err_count: u64 = 0;

    let start = Instant::now();
    let mut pending: FuturesUnordered<QueryFuture<'_>> = FuturesUnordered::new();

    // Seed pipeline
    for _ in 0..concurrency {
        if start.elapsed() >= duration {
            break;
        }
        pending.push(submit_query(client, &ns, args.dimensions, args.top_k, args.nprobe));
    }

    // Drive concurrent queries
    while let Some((t, result)) = pending.next().await {
        match result {
            Ok(_) => {
                hist.record(t.elapsed().as_micros() as u64).ok();
                total += 1;
            }
            Err(_) => {
                err_count += 1;
            }
        }

        if start.elapsed() < duration {
            pending.push(submit_query(client, &ns, args.dimensions, args.top_k, args.nprobe));
        }
    }

    let elapsed = start.elapsed();
    let qps = total as f64 / elapsed.as_secs_f64();

    eprintln!(
        "  {total} queries in {:.1}s = {qps:.0} QPS ({err_count} errors, concurrency={concurrency})",
        elapsed.as_secs_f64()
    );

    let mut stats = results::latency_stats(&hist);
    stats["qps"] = serde_json::json!(qps);
    stats["total_queries"] = serde_json::json!(total);
    stats["errors"] = serde_json::json!(err_count);
    stats["duration_secs"] = serde_json::json!(elapsed.as_secs_f64());
    stats["concurrency"] = serde_json::json!(concurrency);

    let _ = client.delete_namespace(&ns).await;

    Ok(stats)
}
