//! Zeppelin system benchmark runner.
//!
//! Drives the Zeppelin HTTP API at scale and measures end-to-end performance.
//!
//! Usage:
//!   cargo run -p zeppelin-bench -- --target http://localhost:8080 --scenario ingest
//!   cargo run -p zeppelin-bench -- --target http://localhost:8080 --scenario query_latency
//!   cargo run -p zeppelin-bench -- --help

mod client;
mod datasets;
mod results;
mod scenarios;
mod turbopuffer;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "zeppelin-bench", about = "System benchmarks for Zeppelin")]
struct Args {
    /// Server URL
    #[arg(long, default_value = "http://localhost:8080")]
    target: String,

    /// Benchmark scenario to run
    #[arg(long)]
    scenario: String,

    /// Number of vectors to ingest
    #[arg(long, default_value_t = 100_000)]
    vectors: usize,

    /// Vector dimensions
    #[arg(long, default_value_t = 128)]
    dimensions: usize,

    /// Concurrent clients
    #[arg(long, default_value_t = 16)]
    concurrency: usize,

    /// Duration for sustained load tests (seconds)
    #[arg(long, default_value_t = 60)]
    duration: u64,

    /// Output file for JSON results (default: stdout)
    #[arg(long)]
    output: Option<String>,

    /// Dataset source: "random" or path to SIFT fvecs file
    #[arg(long, default_value = "random")]
    dataset: String,

    /// Batch size for ingestion
    #[arg(long, default_value_t = 500)]
    batch_size: usize,

    /// turbopuffer API key (enables turbopuffer target)
    #[arg(long)]
    turbopuffer_key: Option<String>,

    /// Number of query probes (nprobe for IVF)
    #[arg(long)]
    nprobe: Option<usize>,

    /// Top-k results to return per query
    #[arg(long, default_value_t = 10)]
    top_k: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let bench_client: Box<dyn client::BenchClient> = if let Some(ref key) = args.turbopuffer_key {
        Box::new(turbopuffer::TurbopufferClient::new(&args.target, key))
    } else {
        Box::new(client::ZeppelinClient::new(&args.target))
    };

    let target_name = if args.turbopuffer_key.is_some() {
        "turbopuffer"
    } else {
        "zeppelin"
    };

    eprintln!(
        "=== Zeppelin Bench: {} against {} ===",
        args.scenario, target_name
    );
    eprintln!(
        "vectors={} dims={} concurrency={} batch_size={}",
        args.vectors, args.dimensions, args.concurrency, args.batch_size
    );

    let result = match args.scenario.as_str() {
        "ingest" => scenarios::ingest::run(&args, bench_client.as_ref()).await?,
        "query_latency" => scenarios::query_latency::run(&args, bench_client.as_ref()).await?,
        "query_throughput" => scenarios::query_throughput::run(&args, bench_client.as_ref()).await?,
        "filtered_query" => scenarios::filtered_query::run(&args, bench_client.as_ref()).await?,
        "mixed_workload" => scenarios::mixed_workload::run(&args, bench_client.as_ref()).await?,
        "compaction" => scenarios::compaction::run(&args, bench_client.as_ref()).await?,
        "scale_test" => scenarios::scale_test::run(&args, bench_client.as_ref()).await?,
        "bm25" => scenarios::bm25::run(&args, bench_client.as_ref()).await?,
        "index_comparison" => scenarios::index_comparison::run(&args, bench_client.as_ref()).await?,
        other => {
            eprintln!("Unknown scenario: {other}");
            eprintln!("Available: ingest, query_latency, query_throughput, filtered_query,");
            eprintln!("           mixed_workload, compaction, scale_test, bm25, index_comparison");
            std::process::exit(1);
        }
    };

    let json = results::BenchResult::new(target_name, &args.scenario, &args, result);
    let output = serde_json::to_string_pretty(&json)?;

    if let Some(ref path) = args.output {
        std::fs::write(path, &output)?;
        eprintln!("Results written to {path}");
    } else {
        println!("{output}");
    }

    Ok(())
}
