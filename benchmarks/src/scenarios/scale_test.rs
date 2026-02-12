//! Scale test benchmark.
//!
//! Ingests + queries at [10K, 100K, 1M] to reveal scaling behavior.

use std::time::Instant;

use hdrhistogram::Histogram;

use crate::client::{BenchClient, QueryRequest};
use crate::datasets;
use crate::results;
use crate::Args;

pub async fn run(args: &Args, client: &dyn BenchClient) -> Result<serde_json::Value, anyhow::Error> {
    let scales = [10_000usize, 100_000, 1_000_000];
    let n_queries = 200;
    let mut scale_results = Vec::new();

    for &scale in &scales {
        if scale > args.vectors {
            eprintln!("  Skipping scale={scale} (exceeds --vectors={})", args.vectors);
            continue;
        }

        let ns = format!("bench-scale-{scale}-{}", rand::random::<u32>());
        client
            .create_namespace(&ns, args.dimensions, None)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        // Ingest
        eprintln!("  Ingesting {scale} vectors...");
        let ingest_start = Instant::now();

        let mut ingested = 0;
        while ingested < scale {
            let batch_count = args.batch_size.min(scale - ingested);
            let batch = datasets::random_vectors(batch_count, args.dimensions, false);
            client
                .upsert(&ns, &batch)
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            ingested += batch_count;
        }

        let ingest_elapsed = ingest_start.elapsed();
        let ingest_vps = scale as f64 / ingest_elapsed.as_secs_f64();

        // Query
        eprintln!("  Querying {n_queries} times at scale={scale}...");
        let mut hist = Histogram::<u64>::new(3).unwrap();

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
                hist.record(t.elapsed().as_micros() as u64).ok();
            }
        }

        let qps = n_queries as f64 / hist.mean() * 1_000_000.0 / n_queries as f64;

        eprintln!(
            "    scale={scale}: ingest={ingest_vps:.0} vps, query p50={:.1}ms p99={:.1}ms",
            hist.value_at_quantile(0.50) as f64 / 1000.0,
            hist.value_at_quantile(0.99) as f64 / 1000.0,
        );

        let mut stats = results::latency_stats(&hist);
        stats["scale"] = serde_json::json!(scale);
        stats["ingest_vps"] = serde_json::json!(ingest_vps);
        stats["ingest_secs"] = serde_json::json!(ingest_elapsed.as_secs_f64());
        scale_results.push(stats);

        let _ = client.delete_namespace(&ns).await;
    }

    Ok(serde_json::json!({ "scale_results": scale_results }))
}
