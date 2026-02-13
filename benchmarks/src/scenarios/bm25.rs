//! BM25 full-text search benchmark.
//!
//! Measures BM25 query latency and throughput.

use std::time::Instant;

use hdrhistogram::Histogram;

use crate::client::{BenchClient, QueryRequest};
use crate::datasets;
use crate::results;
use crate::Args;

pub async fn run(args: &Args, client: &dyn BenchClient) -> Result<serde_json::Value, anyhow::Error> {
    let ns = format!("bench-bm25-{}", rand::random::<u32>());

    // Create namespace with FTS config
    let fts_config = serde_json::json!({
        "full_text_search": {
            "content": {
                "language": "english",
                "stemming": true,
                "remove_stopwords": true,
            }
        }
    });

    client
        .create_namespace(&ns, args.dimensions, Some(fts_config))
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    // Ingest documents
    let n_docs = args.vectors.min(200_000); // Cap BM25 bench at 200K docs
    let documents = datasets::random_documents(n_docs, args.dimensions);
    eprintln!("  Ingesting {n_docs} documents...");
    for batch in documents.chunks(args.batch_size) {
        client
            .upsert(&ns, batch)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
    }

    let queries = [
        "vector search engine",
        "machine learning neural network",
        "fast performance benchmark",
        "distributed database system",
        "cache memory optimization",
        "real time streaming",
    ];

    let n_iters = 100;
    let mut query_results = Vec::new();

    for query_text in &queries {
        eprintln!("  Querying: \"{query_text}\" x{n_iters}...");
        let mut hist = Histogram::<u64>::new(3).unwrap();

        let rank_by = serde_json::json!(["content", "BM25", query_text]);

        for _ in 0..n_iters {
            let req = QueryRequest {
                vector: None,
                top_k: args.top_k,
                filter: None,
                nprobe: None,
                rank_by: Some(rank_by.clone()),
            };

            let t = Instant::now();
            if client.query(&ns, &req).await.is_ok() {
                hist.record(t.elapsed().as_micros() as u64).ok();
            }
        }

        let mut stats = results::latency_stats(&hist);
        stats["query"] = serde_json::json!(query_text);
        eprintln!(
            "    p50={:.1}ms p99={:.1}ms",
            hist.value_at_quantile(0.50) as f64 / 1000.0,
            hist.value_at_quantile(0.99) as f64 / 1000.0,
        );
        query_results.push(stats);
    }

    // Sustained throughput test
    eprintln!("  Running BM25 throughput test for 30s...");
    let duration = std::time::Duration::from_secs(30);
    let start = Instant::now();
    let mut total = 0u64;
    let mut thru_hist = Histogram::<u64>::new(3).unwrap();

    while start.elapsed() < duration {
        let query_text = queries[total as usize % queries.len()];
        let req = QueryRequest {
            vector: None,
            top_k: args.top_k,
            filter: None,
            nprobe: None,
            rank_by: Some(serde_json::json!(["content", "BM25", query_text])),
        };

        let t = Instant::now();
        if client.query(&ns, &req).await.is_ok() {
            thru_hist.record(t.elapsed().as_micros() as u64).ok();
            total += 1;
        }
    }

    let qps = total as f64 / start.elapsed().as_secs_f64();
    eprintln!("  BM25 throughput: {qps:.0} QPS");

    let _ = client.delete_namespace(&ns).await;

    Ok(serde_json::json!({
        "query_results": query_results,
        "throughput": {
            "qps": qps,
            "total_queries": total,
            "latency": results::latency_stats(&thru_hist),
        }
    }))
}
