//! Filtered query benchmark.
//!
//! Measures query latency with various filter types: Eq, Range, In, And, Or, Not.

use std::time::Instant;

use hdrhistogram::Histogram;

use crate::client::{BenchClient, QueryRequest};
use crate::datasets;
use crate::results;
use crate::Args;

pub async fn run(args: &Args, client: &dyn BenchClient) -> Result<serde_json::Value, anyhow::Error> {
    let ns = format!("bench-filt-{}", rand::random::<u32>());

    client
        .create_namespace(&ns, args.dimensions, None)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    // Ingest with attributes
    let vectors = datasets::random_vectors(args.vectors, args.dimensions, true);
    eprintln!("  Ingesting {} vectors with attributes...", args.vectors);
    for batch in vectors.chunks(args.batch_size) {
        client
            .upsert(&ns, batch)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
    }

    let n_queries = 200;
    let filter_types: Vec<(&str, serde_json::Value)> = vec![
        (
            "eq",
            serde_json::json!({"op": "eq", "field": "category", "value": "electronics"}),
        ),
        (
            "range",
            serde_json::json!({"op": "range", "field": "price", "gte": 100.0, "lte": 500.0}),
        ),
        (
            "in",
            serde_json::json!({"op": "in", "field": "category", "values": ["electronics", "books"]}),
        ),
        (
            "and",
            serde_json::json!({
                "op": "and",
                "filters": [
                    {"op": "eq", "field": "category", "value": "electronics"},
                    {"op": "range", "field": "price", "gte": 50.0, "lte": 300.0}
                ]
            }),
        ),
        (
            "or",
            serde_json::json!({
                "op": "or",
                "filters": [
                    {"op": "eq", "field": "category", "value": "electronics"},
                    {"op": "eq", "field": "category", "value": "books"}
                ]
            }),
        ),
        (
            "not",
            serde_json::json!({
                "op": "not",
                "filter": {"op": "eq", "field": "category", "value": "food"}
            }),
        ),
    ];

    let mut filter_results = Vec::new();

    for (name, filter) in &filter_types {
        eprintln!("  Running {n_queries} queries with filter: {name}...");
        let mut hist = Histogram::<u64>::new(3).unwrap();

        for _ in 0..n_queries {
            let query_vec = datasets::random_query(args.dimensions);
            let req = QueryRequest {
                vector: Some(query_vec),
                top_k: args.top_k,
                filter: Some(filter.clone()),
                nprobe: args.nprobe,
                rank_by: None,
            };

            let t = Instant::now();
            if client.query(&ns, &req).await.is_ok() {
                hist.record(t.elapsed().as_micros() as u64).ok();
            }
        }

        let mut stats = results::latency_stats(&hist);
        stats["filter_type"] = serde_json::json!(name);
        eprintln!(
            "    {name}: p50={:.1}ms p99={:.1}ms",
            hist.value_at_quantile(0.50) as f64 / 1000.0,
            hist.value_at_quantile(0.99) as f64 / 1000.0,
        );
        filter_results.push(stats);
    }

    let _ = client.delete_namespace(&ns).await;

    Ok(serde_json::json!({ "filter_results": filter_results }))
}
