mod common;

use common::server::{api_ns, cleanup_ns, start_test_server_with_compactor};

use std::collections::HashMap;
use std::sync::Arc;
use zeppelin::config::{CompactionConfig, Config, IndexingConfig};

fn fts_stress_config() -> Config {
    let mut config = Config::load(None).unwrap();
    config.compaction = CompactionConfig {
        max_wal_fragments_before_compact: 5,
        ..Default::default()
    };
    config.indexing = IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 10,
        fts_index: true,
        bitmap_index: false,
        ..Default::default()
    };
    config
}

fn fts_configs() -> HashMap<String, zeppelin::fts::types::FtsFieldConfig> {
    let mut m = HashMap::new();
    m.insert(
        "content".to_string(),
        zeppelin::fts::types::FtsFieldConfig {
            stemming: false,
            remove_stopwords: false,
            ..Default::default()
        },
    );
    m
}

// ---------------------------------------------------------------------------
// Test 1: Concurrent writers + readers (all FTS)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_stress_concurrent_writers_readers() {
    let config = fts_stress_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = Arc::new(reqwest::Client::new());
    let ns = api_ns(&harness, "fts-stress-rw");

    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 4,
            "full_text_search": {
                "content": { "stemming": false, "remove_stopwords": false }
            }
        }))
        .send()
        .await
        .unwrap();

    // Spawn 5 writers (each writes 20 vectors)
    let mut handles = Vec::new();
    for w in 0..5 {
        let client = client.clone();
        let base_url = base_url.clone();
        let ns = ns.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..20 {
                let resp = client
                    .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
                    .json(&serde_json::json!({
                        "vectors": [{
                            "id": format!("{ns}_w{w}_d{i}"),
                            "values": [0.1, 0.2, 0.3, 0.4],
                            "attributes": { "content": format!("document {w} number {i} alpha beta") }
                        }]
                    }))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.status(), 200, "writer {w} failed on doc {i}");
            }
        }));
    }

    // Spawn 5 readers (each queries 10 times)
    for r in 0..5 {
        let client = client.clone();
        let base_url = base_url.clone();
        let ns = ns.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..10 {
                let resp = client
                    .post(format!("{base_url}/v1/namespaces/{ns}/query"))
                    .json(&serde_json::json!({
                        "rank_by": ["content", "BM25", "alpha"],
                        "top_k": 10,
                        "consistency": "strong"
                    }))
                    .send()
                    .await
                    .unwrap();
                assert!(
                    resp.status() == 200,
                    "reader {r} got non-200: {}",
                    resp.status()
                );
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Final query should find some results
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["content", "BM25", "alpha"],
            "top_k": 100,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    // All 100 docs contain "alpha", but top_k caps at 100
    assert!(
        !results.is_empty(),
        "should find results after concurrent writes"
    );

    cleanup_ns(&harness.store, &ns).await;
}

// ---------------------------------------------------------------------------
// Test 2: Large batch BM25 write + query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_stress_large_batch() {
    let config = fts_stress_config();
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-stress-batch");

    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 4,
            "full_text_search": {
                "content": { "stemming": false, "remove_stopwords": false }
            }
        }))
        .send()
        .await
        .unwrap();

    // Build 500 vectors with varied text content
    let words = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"];
    let vectors: Vec<serde_json::Value> = (0..500)
        .map(|i| {
            let content = format!(
                "{} {} document number {}",
                words[i % words.len()],
                words[(i + 3) % words.len()],
                i
            );
            serde_json::json!({
                "id": format!("{ns}_d{i}"),
                "values": [
                    (i as f32 / 500.0),
                    ((i * 7) as f32 % 1.0),
                    ((i * 13) as f32 % 1.0),
                    ((i * 17) as f32 % 1.0)
                ],
                "attributes": { "content": content }
            })
        })
        .collect();

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Compact with FTS
    let result = compactor
        .compact_with_fts(&ns, None, &fts_configs())
        .await
        .unwrap();
    assert!(result.segment_id.is_some());
    assert_eq!(result.vectors_compacted, 500);

    // Query for "alpha" — should appear in ~1/8 of docs
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["content", "BM25", "alpha"],
            "top_k": 100,
            "consistency": "eventual"
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    // alpha appears at indices 0, 8, 16, ... = 63 docs
    assert!(results.len() >= 50, "expected ~63 results, got {}", results.len());

    cleanup_ns(&harness.store, &ns).await;
}

// ---------------------------------------------------------------------------
// Test 3: Query concurrency under load
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_stress_query_concurrency() {
    let config = fts_stress_config();
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = Arc::new(reqwest::Client::new());
    let ns = api_ns(&harness, "fts-stress-qc");

    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 4,
            "full_text_search": {
                "content": { "stemming": false, "remove_stopwords": false }
            }
        }))
        .send()
        .await
        .unwrap();

    // Seed with 50 docs
    let vectors: Vec<serde_json::Value> = (0..50)
        .map(|i| {
            serde_json::json!({
                "id": format!("{ns}_d{i}"),
                "values": [0.1, 0.2, 0.3, 0.4],
                "attributes": { "content": format!("searchable term alpha doc {i}") }
            })
        })
        .collect();

    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();

    compactor
        .compact_with_fts(&ns, None, &fts_configs())
        .await
        .unwrap();

    // Fire 20 concurrent queries
    let mut handles = Vec::new();
    let queries = ["alpha", "term", "searchable", "doc"];
    for query_term in queries.iter() {
        for _ in 0..5 {
            let client = client.clone();
            let base_url = base_url.clone();
            let ns = ns.clone();
            let query_term = query_term.to_string();
            handles.push(tokio::spawn(async move {
                let resp = client
                    .post(format!("{base_url}/v1/namespaces/{ns}/query"))
                    .json(&serde_json::json!({
                        "rank_by": ["content", "BM25", query_term],
                        "top_k": 10,
                        "consistency": "eventual"
                    }))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.status(), 200);
                let body: serde_json::Value = resp.json().await.unwrap();
                let results = body["results"].as_array().unwrap();
                assert!(!results.is_empty(), "query '{query_term}' returned empty");
            }));
        }
    }

    for handle in handles {
        handle.await.unwrap();
    }

    cleanup_ns(&harness.store, &ns).await;
}
