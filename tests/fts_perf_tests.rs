mod common;

use common::server::{api_ns, start_test_server_with_compactor};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::time::Instant;
use zeppelin::config::{CompactionConfig, Config, IndexingConfig};
use zeppelin::fts::types::FtsFieldConfig;

fn fts_perf_config() -> Config {
    let mut config = Config::load(None).unwrap();
    config.compaction = CompactionConfig {
        max_wal_fragments_before_compact: 10,
        ..Default::default()
    };
    config.indexing = IndexingConfig {
        default_num_centroids: 8,
        kmeans_max_iterations: 20,
        fts_index: true,
        ..Default::default()
    };
    config
}

fn default_fts_config() -> HashMap<String, FtsFieldConfig> {
    let mut m = HashMap::new();
    m.insert("content".to_string(), FtsFieldConfig::default());
    m
}

fn random_text(rng: &mut StdRng, word_count: usize) -> String {
    let vocab = [
        "vector", "search", "engine", "index", "query", "database", "rust",
        "programming", "system", "memory", "safe", "fast", "concurrent",
        "storage", "object", "bucket", "cluster", "centroid", "quantization",
        "inverted", "token", "score", "relevance", "document", "namespace",
        "compaction", "segment", "fragment", "manifest", "cache", "performance",
        "algorithm", "approximate", "neighbor", "distance", "cosine", "euclidean",
        "bitmap", "filter", "attribute", "hierarchical", "beam", "tree",
        "machine", "learning", "neural", "network", "embedding", "transformer",
    ];
    (0..word_count)
        .map(|_| vocab[rng.gen_range(0..vocab.len())])
        .collect::<Vec<_>>()
        .join(" ")
}

fn make_doc(id: &str, text: &str, dims: usize, rng: &mut StdRng) -> serde_json::Value {
    let values: Vec<f32> = (0..dims).map(|_| rng.gen_range(-1.0..1.0)).collect();
    serde_json::json!({
        "id": id,
        "values": values,
        "attributes": { "content": text }
    })
}

// ---------------------------------------------------------------------------
// Perf Test 1: Index build time for 1000 docs
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fts_perf_index_build_time() {
    let config = fts_perf_config();
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-perf-build");
    let fts_config = default_fts_config();

    // Create namespace
    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 16,
            "distance_metric": "cosine",
            "full_text_search": fts_config,
        }))
        .send()
        .await
        .unwrap();

    // Generate 1000 docs with 30-word text fields
    let mut rng = StdRng::seed_from_u64(42);
    let docs: Vec<serde_json::Value> = (0..1000)
        .map(|i| {
            let text = random_text(&mut rng, 30);
            make_doc(&format!("perf_{i}"), &text, 16, &mut rng)
        })
        .collect();

    // Upsert in batches
    for chunk in docs.chunks(200) {
        let resp = client
            .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
            .json(&serde_json::json!({ "vectors": chunk }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    // Measure compaction time (includes FTS index build)
    let start = Instant::now();
    let result = compactor
        .compact_with_fts(&ns, None, &fts_config)
        .await
        .unwrap();
    let build_elapsed = start.elapsed();

    assert_eq!(result.vectors_compacted, 1000);
    eprintln!(
        "[perf] FTS index build for 1000 docs: {:.1}ms",
        build_elapsed.as_secs_f64() * 1000.0
    );

    // Sanity: build should complete within reasonable time (< 30s)
    assert!(
        build_elapsed.as_secs() < 30,
        "FTS index build took too long: {:?}",
        build_elapsed
    );
}

// ---------------------------------------------------------------------------
// Perf Test 2: Query latency distribution over 50 queries
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fts_perf_query_latency() {
    let config = fts_perf_config();
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-perf-lat");
    let fts_config = default_fts_config();

    // Create and populate
    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 16,
            "distance_metric": "cosine",
            "full_text_search": fts_config,
        }))
        .send()
        .await
        .unwrap();

    let mut rng = StdRng::seed_from_u64(99);
    let docs: Vec<serde_json::Value> = (0..500)
        .map(|i| {
            let text = random_text(&mut rng, 25);
            make_doc(&format!("lat_{i}"), &text, 16, &mut rng)
        })
        .collect();

    for chunk in docs.chunks(100) {
        client
            .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
            .json(&serde_json::json!({ "vectors": chunk }))
            .send()
            .await
            .unwrap();
    }

    // Compact to build FTS index
    compactor
        .compact_with_fts(&ns, None, &fts_config)
        .await
        .unwrap();

    // Measure 50 queries (eventual consistency — segment-only)
    let queries = [
        "vector search", "database index", "rust programming", "memory safe",
        "machine learning", "neural network", "approximate neighbor", "cosine distance",
        "bitmap filter", "compaction segment",
    ];

    let mut latencies = Vec::new();
    for _round in 0..5 {
        for q in &queries {
            let start = Instant::now();
            let resp = client
                .post(format!("{base_url}/v1/namespaces/{ns}/query"))
                .json(&serde_json::json!({
                    "rank_by": ["content", "BM25", q],
                    "top_k": 10,
                    "consistency": "eventual",
                }))
                .send()
                .await
                .unwrap();
            let elapsed = start.elapsed();
            assert_eq!(resp.status(), 200);
            latencies.push(elapsed.as_secs_f64() * 1000.0);
        }
    }

    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p50 = latencies[latencies.len() / 2];
    let p99 = latencies[(latencies.len() as f64 * 0.99) as usize];
    let avg = latencies.iter().sum::<f64>() / latencies.len() as f64;

    eprintln!(
        "[perf] BM25 query latency (50 queries, 500 docs): avg={avg:.1}ms, p50={p50:.1}ms, p99={p99:.1}ms"
    );

    // Sanity: p99 should be under 5s for 500 docs
    assert!(
        p99 < 5000.0,
        "p99 latency too high: {p99:.1}ms"
    );
}

// ---------------------------------------------------------------------------
// Perf Test 3: WAL brute-force vs inverted index comparison
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fts_perf_wal_vs_segment() {
    let config = fts_perf_config();
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-perf-compare");
    let fts_config = default_fts_config();

    // Create and populate
    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 8,
            "distance_metric": "cosine",
            "full_text_search": fts_config,
        }))
        .send()
        .await
        .unwrap();

    let mut rng = StdRng::seed_from_u64(123);
    let docs: Vec<serde_json::Value> = (0..200)
        .map(|i| {
            let text = random_text(&mut rng, 20);
            make_doc(&format!("cmp_{i}"), &text, 8, &mut rng)
        })
        .collect();

    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": docs }))
        .send()
        .await
        .unwrap();

    // Measure WAL brute-force (strong consistency, pre-compaction)
    let wal_latencies: Vec<f64> = {
        let mut lats = Vec::new();
        for _ in 0..10 {
            let start = Instant::now();
            client
                .post(format!("{base_url}/v1/namespaces/{ns}/query"))
                .json(&serde_json::json!({
                    "rank_by": ["content", "BM25", "vector search"],
                    "top_k": 10,
                    "consistency": "strong",
                }))
                .send()
                .await
                .unwrap();
            lats.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        lats
    };

    // Compact
    compactor
        .compact_with_fts(&ns, None, &fts_config)
        .await
        .unwrap();

    // Measure inverted index (eventual consistency, post-compaction)
    let idx_latencies: Vec<f64> = {
        let mut lats = Vec::new();
        for _ in 0..10 {
            let start = Instant::now();
            client
                .post(format!("{base_url}/v1/namespaces/{ns}/query"))
                .json(&serde_json::json!({
                    "rank_by": ["content", "BM25", "vector search"],
                    "top_k": 10,
                    "consistency": "eventual",
                }))
                .send()
                .await
                .unwrap();
            lats.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        lats
    };

    let wal_avg = wal_latencies.iter().sum::<f64>() / wal_latencies.len() as f64;
    let idx_avg = idx_latencies.iter().sum::<f64>() / idx_latencies.len() as f64;

    eprintln!(
        "[perf] WAL brute-force avg={wal_avg:.1}ms, inverted index avg={idx_avg:.1}ms (200 docs)"
    );

    // Both should complete successfully — we don't assert relative speed since
    // for small datasets WAL scan can actually be faster due to S3 overhead
}
