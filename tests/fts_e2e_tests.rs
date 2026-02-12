mod common;

use common::server::{api_ns, cleanup_ns, start_test_server_with_compactor};

use std::collections::HashMap;
use zeppelin::config::{CompactionConfig, Config, IndexingConfig};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn fts_e2e_config() -> Config {
    let mut config = Config::load(None).unwrap();
    config.compaction = CompactionConfig {
        max_wal_fragments_before_compact: 1,
        ..Default::default()
    };
    config.indexing = IndexingConfig {
        default_num_centroids: 2,
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
// Test 1: Full lifecycle — create ns, upsert, query WAL, compact, query segment
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_e2e_full_lifecycle() {
    let config = fts_e2e_config();
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "e2e-lifecycle");

    // Create namespace with FTS
    let resp = client
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
    assert_eq!(resp.status(), 201);

    // Upsert documents
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [
                { "id": format!("{ns}_d1"), "values": [0.1, 0.2, 0.3, 0.4], "attributes": { "content": "rust programming language" } },
                { "id": format!("{ns}_d2"), "values": [0.5, 0.6, 0.7, 0.8], "attributes": { "content": "python programming language" } },
                { "id": format!("{ns}_d3"), "values": [0.2, 0.3, 0.4, 0.5], "attributes": { "content": "rust systems programming" } }
            ]
        }))
        .send()
        .await
        .unwrap();

    // Phase 1: WAL query (strong consistency)
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["content", "BM25", "rust programming"],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(results.len() >= 2, "d1 and d3 contain 'rust'");
    assert!(
        body["scanned_fragments"].as_u64().unwrap() > 0,
        "should scan WAL fragments"
    );

    // Phase 2: Compact
    let result = compactor
        .compact_with_fts(&ns, None, &fts_configs())
        .await
        .unwrap();
    assert!(result.segment_id.is_some());

    // Phase 3: Segment query (eventual consistency)
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["content", "BM25", "rust"],
            "top_k": 10,
            "consistency": "eventual"
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 2, "d1 and d3 contain 'rust'");
    assert_eq!(
        body["scanned_fragments"].as_u64().unwrap(),
        0,
        "eventual should not scan WAL"
    );
    assert_eq!(
        body["scanned_segments"].as_u64().unwrap(),
        1,
        "should scan 1 segment"
    );

    cleanup_ns(&harness.store, &ns).await;
}

// ---------------------------------------------------------------------------
// Test 2: Strong consistency sees WAL data during compaction window
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_e2e_strong_consistency_during_compaction() {
    let config = fts_e2e_config();
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "e2e-strong");

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

    // Batch 1: upsert + compact
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [
                { "id": format!("{ns}_d1"), "values": [0.1, 0.2, 0.3, 0.4], "attributes": { "content": "alpha beta" } }
            ]
        }))
        .send()
        .await
        .unwrap();

    compactor
        .compact_with_fts(&ns, None, &fts_configs())
        .await
        .unwrap();

    // Batch 2: upsert (in WAL, not yet compacted)
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [
                { "id": format!("{ns}_d2"), "values": [0.5, 0.6, 0.7, 0.8], "attributes": { "content": "alpha gamma" } }
            ]
        }))
        .send()
        .await
        .unwrap();

    // Strong query should find both d1 (segment) and d2 (WAL)
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
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 2, "strong should find WAL + segment docs");

    // Eventual should only find d1 (segment)
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["content", "BM25", "alpha"],
            "top_k": 10,
            "consistency": "eventual"
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 1, "eventual should only find segment docs");

    cleanup_ns(&harness.store, &ns).await;
}

// ---------------------------------------------------------------------------
// Test 3: Delete and re-query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_e2e_delete_and_requery() {
    let config = fts_e2e_config();
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "e2e-delete");

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

    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [
                { "id": format!("{ns}_d1"), "values": [0.1, 0.2, 0.3, 0.4], "attributes": { "content": "quantum computing" } },
                { "id": format!("{ns}_d2"), "values": [0.5, 0.6, 0.7, 0.8], "attributes": { "content": "quantum physics" } },
                { "id": format!("{ns}_d3"), "values": [0.2, 0.3, 0.4, 0.5], "attributes": { "content": "classical mechanics" } }
            ]
        }))
        .send()
        .await
        .unwrap();

    // Compact to create segment with FTS index
    compactor
        .compact_with_fts(&ns, None, &fts_configs())
        .await
        .unwrap();

    // Delete d1
    client
        .delete(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "ids": [format!("{ns}_d1")] }))
        .send()
        .await
        .unwrap();

    // Strong query should exclude d1
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["content", "BM25", "quantum"],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 1, "d1 deleted, only d2 should match quantum");
    assert!(results[0]["id"].as_str().unwrap().ends_with("_d2"));

    cleanup_ns(&harness.store, &ns).await;
}

// ---------------------------------------------------------------------------
// Test 4: Multi-field query with compaction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_e2e_multi_field_with_compaction() {
    let config = fts_e2e_config();
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "e2e-multi");

    let mut fts = HashMap::new();
    fts.insert(
        "title".to_string(),
        zeppelin::fts::types::FtsFieldConfig {
            stemming: false,
            remove_stopwords: false,
            ..Default::default()
        },
    );
    fts.insert(
        "body".to_string(),
        zeppelin::fts::types::FtsFieldConfig {
            stemming: false,
            remove_stopwords: false,
            ..Default::default()
        },
    );

    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 4,
            "full_text_search": {
                "title": { "stemming": false, "remove_stopwords": false },
                "body": { "stemming": false, "remove_stopwords": false }
            }
        }))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [
                { "id": format!("{ns}_d1"), "values": [0.1, 0.2, 0.3, 0.4], "attributes": { "title": "rust guide", "body": "learn rust programming" } },
                { "id": format!("{ns}_d2"), "values": [0.5, 0.6, 0.7, 0.8], "attributes": { "title": "python guide", "body": "learn rust basics" } }
            ]
        }))
        .send()
        .await
        .unwrap();

    compactor.compact_with_fts(&ns, None, &fts).await.unwrap();

    // Sum across title + body
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["Sum", [
                ["title", "BM25", "rust"],
                ["body", "BM25", "rust"]
            ]],
            "top_k": 10,
            "consistency": "eventual"
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 2, "both docs contain 'rust' somewhere");
    // d1 has rust in both title AND body, should rank higher
    assert!(results[0]["id"].as_str().unwrap().ends_with("_d1"));

    cleanup_ns(&harness.store, &ns).await;
}

// ---------------------------------------------------------------------------
// Test 5: Prefix search (last_as_prefix)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_e2e_prefix_search() {
    let config = fts_e2e_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "e2e-prefix");

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

    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [
                { "id": format!("{ns}_d1"), "values": [0.1, 0.2, 0.3, 0.4], "attributes": { "content": "programming language" } },
                { "id": format!("{ns}_d2"), "values": [0.5, 0.6, 0.7, 0.8], "attributes": { "content": "programmer tools" } },
                { "id": format!("{ns}_d3"), "values": [0.2, 0.3, 0.4, 0.5], "attributes": { "content": "database management" } }
            ]
        }))
        .send()
        .await
        .unwrap();

    // "program" as prefix should match "programming" and "programmer"
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["content", "BM25", "program"],
            "top_k": 10,
            "consistency": "strong",
            "last_as_prefix": true
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 2, "prefix 'program' matches d1 and d2");

    cleanup_ns(&harness.store, &ns).await;
}

// ---------------------------------------------------------------------------
// Test 6: Stemming produces better recall
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_e2e_stemming() {
    let config = fts_e2e_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "e2e-stem");

    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 4,
            "full_text_search": {
                "content": { "stemming": true, "remove_stopwords": false }
            }
        }))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [
                { "id": format!("{ns}_d1"), "values": [0.1, 0.2, 0.3, 0.4], "attributes": { "content": "running quickly" } },
                { "id": format!("{ns}_d2"), "values": [0.5, 0.6, 0.7, 0.8], "attributes": { "content": "runs fast" } },
                { "id": format!("{ns}_d3"), "values": [0.2, 0.3, 0.4, 0.5], "attributes": { "content": "swimming slowly" } }
            ]
        }))
        .send()
        .await
        .unwrap();

    // "running" stems to "run", "runs" stems to "run" — both match
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["content", "BM25", "running"],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert_eq!(
        results.len(),
        2,
        "stemming: 'running' matches 'running' and 'runs' (both stem to 'run')"
    );

    cleanup_ns(&harness.store, &ns).await;
}

// ---------------------------------------------------------------------------
// Test 7: Compaction incremental — new WAL + existing segment
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_e2e_incremental_compaction() {
    let config = fts_e2e_config();
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "e2e-incr");

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

    // Batch 1
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [
                { "id": format!("{ns}_d1"), "values": [0.1, 0.2, 0.3, 0.4], "attributes": { "content": "alpha beta" } },
                { "id": format!("{ns}_d2"), "values": [0.5, 0.6, 0.7, 0.8], "attributes": { "content": "gamma delta" } }
            ]
        }))
        .send()
        .await
        .unwrap();

    compactor
        .compact_with_fts(&ns, None, &fts_configs())
        .await
        .unwrap();

    // Batch 2
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [
                { "id": format!("{ns}_d3"), "values": [0.2, 0.3, 0.4, 0.5], "attributes": { "content": "alpha epsilon" } }
            ]
        }))
        .send()
        .await
        .unwrap();

    // Second compaction merges WAL + existing segment
    compactor
        .compact_with_fts(&ns, None, &fts_configs())
        .await
        .unwrap();

    // All three docs should be searchable
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["content", "BM25", "alpha"],
            "top_k": 10,
            "consistency": "eventual"
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 2, "d1 and d3 contain 'alpha'");

    cleanup_ns(&harness.store, &ns).await;
}

// ---------------------------------------------------------------------------
// Test 8: Eventual vs Strong consistency behavior
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_e2e_eventual_vs_strong() {
    let config = fts_e2e_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "e2e-cons");

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

    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [
                { "id": format!("{ns}_d1"), "values": [0.1, 0.2, 0.3, 0.4], "attributes": { "content": "hello world" } }
            ]
        }))
        .send()
        .await
        .unwrap();

    // Without compaction, eventual should return empty (no segment)
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["content", "BM25", "hello"],
            "top_k": 10,
            "consistency": "eventual"
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(
        results.is_empty(),
        "eventual without compaction should return empty"
    );

    // Strong should find it in WAL
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["content", "BM25", "hello"],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 1, "strong should find WAL data");

    cleanup_ns(&harness.store, &ns).await;
}
