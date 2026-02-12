//! Ingest throughput benchmark.
//!
//! Measures vectors/second at various batch sizes.

use std::time::Instant;

use crate::client::BenchClient;
use crate::datasets;
use crate::Args;

pub async fn run(args: &Args, client: &dyn BenchClient) -> Result<serde_json::Value, anyhow::Error> {
    let ns = format!("bench-ingest-{}", rand::random::<u32>());

    client
        .create_namespace(&ns, args.dimensions, None)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    let vectors = datasets::random_vectors(args.vectors, args.dimensions, false);
    let mut results = Vec::new();

    for &batch_size in &[100, 500, 1000] {
        let actual_batch = batch_size.min(args.vectors);
        let total = args.vectors;
        let batches: Vec<_> = vectors.chunks(actual_batch).collect();

        eprintln!("  Ingesting {total} vectors in batches of {actual_batch}...");
        let start = Instant::now();
        let mut ingested = 0;

        for batch in &batches {
            client
                .upsert(&ns, batch)
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            ingested += batch.len();
        }

        let elapsed = start.elapsed();
        let vectors_per_sec = ingested as f64 / elapsed.as_secs_f64();
        let batches_per_sec = batches.len() as f64 / elapsed.as_secs_f64();

        eprintln!(
            "  batch_size={actual_batch}: {vectors_per_sec:.0} vectors/s, {:.0} batches/s, {:.2}s total",
            batches_per_sec,
            elapsed.as_secs_f64()
        );

        results.push(serde_json::json!({
            "batch_size": actual_batch,
            "total_vectors": ingested,
            "elapsed_secs": elapsed.as_secs_f64(),
            "vectors_per_sec": vectors_per_sec,
            "batches_per_sec": batches_per_sec,
        }));
    }

    // Cleanup
    let _ = client.delete_namespace(&ns).await;

    Ok(serde_json::json!({ "batch_results": results }))
}
