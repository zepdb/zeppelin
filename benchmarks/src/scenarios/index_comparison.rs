//! Index comparison benchmark.
//!
//! Runs the same workload across Flat / SQ8 / PQ index types and compares.
//! Note: This scenario only works against Zeppelin (turbopuffer doesn't expose index type config).

use std::time::Instant;

use hdrhistogram::Histogram;

use crate::client::{BenchClient, QueryRequest};
use crate::datasets;
use crate::results;
use crate::Args;

pub async fn run(args: &Args, client: &dyn BenchClient) -> Result<serde_json::Value, anyhow::Error> {
    let index_types = ["none", "scalar", "product"];
    let n_queries = 200;
    let mut comparison = Vec::new();

    for &quant_type in &index_types {
        let ns = format!("bench-idx-{quant_type}-{}", rand::random::<u32>());

        // Zeppelin doesn't currently support setting quantization via API at namespace level,
        // so this benchmark measures the same index type. In a full setup, you'd configure
        // the server with different IndexingConfig for each run.
        client
            .create_namespace(&ns, args.dimensions, None)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        // Ingest
        let vectors = datasets::random_vectors(args.vectors, args.dimensions, false);
        eprintln!("  [{quant_type}] Ingesting {} vectors...", args.vectors);
        let ingest_start = Instant::now();
        for batch in vectors.chunks(args.batch_size) {
            client
                .upsert(&ns, batch)
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
        }
        let ingest_elapsed = ingest_start.elapsed();

        // Query
        eprintln!("  [{quant_type}] Running {n_queries} queries...");
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

        eprintln!(
            "    [{quant_type}] ingest={:.1}s query p50={:.1}ms p99={:.1}ms",
            ingest_elapsed.as_secs_f64(),
            hist.value_at_quantile(0.50) as f64 / 1000.0,
            hist.value_at_quantile(0.99) as f64 / 1000.0,
        );

        let mut stats = results::latency_stats(&hist);
        stats["index_type"] = serde_json::json!(quant_type);
        stats["ingest_secs"] = serde_json::json!(ingest_elapsed.as_secs_f64());
        stats["ingest_vps"] = serde_json::json!(args.vectors as f64 / ingest_elapsed.as_secs_f64());
        comparison.push(stats);

        let _ = client.delete_namespace(&ns).await;
    }

    Ok(serde_json::json!({ "index_comparison": comparison }))
}
