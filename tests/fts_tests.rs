mod common;

use common::server::{api_ns, cleanup_ns, start_test_server_with_compactor};

use std::collections::HashMap;
use zeppelin::config::{CompactionConfig, Config, IndexingConfig};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn fts_test_config() -> Config {
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

fn make_text_vectors(prefix: &str, texts: &[(&str, &str)], dims: usize) -> Vec<serde_json::Value> {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    prefix.hash(&mut hasher);
    let seed = hasher.finish();
    let mut rng = StdRng::seed_from_u64(seed);

    texts
        .iter()
        .enumerate()
        .map(|(i, (id, content))| {
            let values: Vec<f32> = (0..dims).map(|_| rng.gen_range(-1.0..1.0)).collect();
            serde_json::json!({
                "id": format!("{prefix}_{id}"),
                "values": values,
                "attributes": {
                    "content": content,
                    "category": if i % 2 == 0 { "science" } else { "arts" }
                }
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Test 1: WAL BM25 scan — query uncompacted data
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_wal_scan_basic() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-wal-basic");

    // Create namespace with FTS config
    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 8,
            "full_text_search": {
                "content": {
                    "language": "english",
                    "stemming": false,
                    "remove_stopwords": false
                }
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create namespace failed");

    // Upsert vectors with text
    let vectors = make_text_vectors(
        &ns,
        &[
            ("d1", "machine learning algorithm"),
            ("d2", "deep learning neural network"),
            ("d3", "random forest classifier tree"),
        ],
        8,
    );

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "upsert failed");

    // BM25 query — should find documents via WAL scan (strong consistency)
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["content", "BM25", "learning"],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "query failed");

    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();

    // "learning" appears in d1 and d2
    assert_eq!(results.len(), 2, "expected 2 results, got {}", results.len());

    // Scores should be positive and descending
    let score0 = results[0]["score"].as_f64().unwrap();
    let score1 = results[1]["score"].as_f64().unwrap();
    assert!(score0 > 0.0);
    assert!(score1 > 0.0);
    assert!(score0 >= score1, "results should be sorted descending");

    cleanup_ns(&harness.store, &ns).await;
}

// ---------------------------------------------------------------------------
// Test 2: WAL scan with deletes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_wal_scan_with_deletes() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-wal-del");

    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 8,
            "full_text_search": {
                "content": { "stemming": false, "remove_stopwords": false }
            }
        }))
        .send()
        .await
        .unwrap();

    let vectors = make_text_vectors(
        &ns,
        &[
            ("d1", "cat dog animal"),
            ("d2", "cat bird animal"),
        ],
        8,
    );

    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();

    // Delete d1
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors/delete"))
        .json(&serde_json::json!({ "ids": [format!("{ns}_d1")] }))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["content", "BM25", "cat"],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 1, "d1 was deleted, only d2 should remain");

    cleanup_ns(&harness.store, &ns).await;
}

// ---------------------------------------------------------------------------
// Test 3: Segment search after compaction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_segment_search_after_compaction() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-seg");

    let fts_config: HashMap<String, zeppelin::fts::types::FtsFieldConfig> = {
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
    };

    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 8,
            "full_text_search": {
                "content": { "stemming": false, "remove_stopwords": false }
            }
        }))
        .send()
        .await
        .unwrap();

    let vectors = make_text_vectors(
        &ns,
        &[
            ("d1", "machine learning algorithm"),
            ("d2", "deep learning neural network"),
            ("d3", "random forest classifier"),
            ("d4", "support vector machine"),
        ],
        8,
    );

    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();

    // Trigger compaction with FTS configs
    let result = compactor.compact_with_fts(&ns, None, &fts_config).await;
    assert!(result.is_ok(), "compaction failed: {:?}", result.err());
    let result = result.unwrap();
    assert!(result.segment_id.is_some(), "should have created a segment");

    // Verify FTS index exists in manifest
    let manifest = zeppelin::wal::Manifest::read(&harness.store, &ns)
        .await
        .unwrap()
        .unwrap();
    let active_seg = manifest.active_segment.unwrap();
    let seg_ref = manifest.segments.iter().find(|s| s.id == active_seg).unwrap();
    assert!(
        !seg_ref.fts_fields.is_empty(),
        "fts_fields should be populated"
    );
    assert!(seg_ref.fts_fields.contains(&"content".to_string()));

    // Query via eventual consistency (segment only)
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["content", "BM25", "learning"],
            "top_k": 10,
            "consistency": "eventual"
        }))
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 2, "d1 and d2 contain 'learning'");

    cleanup_ns(&harness.store, &ns).await;
}

// ---------------------------------------------------------------------------
// Test 4: Multi-field Sum query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_multi_field_sum() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-sum");

    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 4,
            "full_text_search": {
                "title": { "stemming": false, "remove_stopwords": false },
                "content": { "stemming": false, "remove_stopwords": false }
            }
        }))
        .send()
        .await
        .unwrap();

    // Upsert vectors with both title and content
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [
                {
                    "id": format!("{ns}_d1"),
                    "values": [0.1, 0.2, 0.3, 0.4],
                    "attributes": {
                        "title": "cat",
                        "content": "the cat sat on a mat with another cat"
                    }
                },
                {
                    "id": format!("{ns}_d2"),
                    "values": [0.5, 0.6, 0.7, 0.8],
                    "attributes": {
                        "title": "dog training",
                        "content": "cat food recipe"
                    }
                }
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Sum query across title + content
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["Sum", [
                ["title", "BM25", "cat"],
                ["content", "BM25", "cat"]
            ]],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    // Both docs contain "cat" somewhere
    assert!(results.len() >= 1, "at least one result expected");
    // d1 has "cat" in both title AND content, should score higher
    if results.len() == 2 {
        let id0 = results[0]["id"].as_str().unwrap();
        assert!(id0.ends_with("_d1"), "d1 should rank first (cat in both fields)");
    }

    cleanup_ns(&harness.store, &ns).await;
}

// ---------------------------------------------------------------------------
// Test 5: Empty BM25 query returns empty results
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_empty_query_returns_empty() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-empty");

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

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [{
                "id": format!("{ns}_d1"),
                "values": [0.1, 0.2, 0.3, 0.4],
                "attributes": { "content": "machine learning" }
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Empty string query
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["content", "BM25", ""],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(results.is_empty(), "empty query should return no results");

    cleanup_ns(&harness.store, &ns).await;
}

// ---------------------------------------------------------------------------
// Test 6: FTS field not configured — returns error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_field_not_configured_error() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-nofield");

    // Create namespace WITHOUT FTS config
    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 4,
        }))
        .send()
        .await
        .unwrap();

    // Try BM25 query — should fail because "content" is not configured
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["content", "BM25", "test"],
            "top_k": 10,
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400, "should return 400 for unconfigured field");

    cleanup_ns(&harness.store, &ns).await;
}

// ---------------------------------------------------------------------------
// Test 7: Validation — must provide exactly one of vector or rank_by
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_validation_vector_or_rank_by() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-val");

    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 4,
            "full_text_search": {
                "content": {}
            }
        }))
        .send()
        .await
        .unwrap();

    // Neither vector nor rank_by — should fail
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({ "top_k": 10 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "missing both vector and rank_by");

    cleanup_ns(&harness.store, &ns).await;
}

// ---------------------------------------------------------------------------
// Test 8: BM25 + attribute filter combined
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_with_attribute_filter() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-filter");

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
                {
                    "id": format!("{ns}_d1"),
                    "values": [0.1, 0.2, 0.3, 0.4],
                    "attributes": { "content": "machine learning", "category": "science" }
                },
                {
                    "id": format!("{ns}_d2"),
                    "values": [0.5, 0.6, 0.7, 0.8],
                    "attributes": { "content": "machine learning", "category": "arts" }
                }
            ]
        }))
        .send()
        .await
        .unwrap();

    // BM25 query with attribute filter
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["content", "BM25", "machine"],
            "top_k": 10,
            "filter": { "op": "eq", "field": "category", "value": "science" },
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    // Both match BM25 but only d1 passes filter
    assert_eq!(results.len(), 1);
    assert!(results[0]["id"].as_str().unwrap().ends_with("_d1"));

    cleanup_ns(&harness.store, &ns).await;
}

// ---------------------------------------------------------------------------
// Test 9: Namespace creation returns FTS config in response
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_namespace_returns_config() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-nscfg");

    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 4,
            "full_text_search": {
                "content": { "language": "english", "k1": 1.5, "b": 0.5 }
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // GET namespace should include full_text_search
    let resp = client
        .get(format!("{base_url}/v1/namespaces/{ns}"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["full_text_search"].is_object());
    assert!(body["full_text_search"]["content"].is_object());

    cleanup_ns(&harness.store, &ns).await;
}

// ---------------------------------------------------------------------------
// Test 10: Product weighted query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_product_weighted() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-prod");

    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 4,
            "full_text_search": {
                "title": { "stemming": false, "remove_stopwords": false }
            }
        }))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [{
                "id": format!("{ns}_d1"),
                "values": [0.1, 0.2, 0.3, 0.4],
                "attributes": { "title": "machine learning" }
            }]
        }))
        .send()
        .await
        .unwrap();

    // Product(3.0, BM25)
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["Product", 3.0, ["title", "BM25", "machine"]],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 1);
    let score = results[0]["score"].as_f64().unwrap();
    assert!(score > 0.0, "weighted score should be positive");

    cleanup_ns(&harness.store, &ns).await;
}

// ---------------------------------------------------------------------------
// Test 11: Vector query still works (backward compat)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_vector_query_still_works() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-compat");

    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 4,
        }))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({
            "vectors": [{
                "id": format!("{ns}_v1"),
                "values": [1.0, 0.0, 0.0, 0.0],
            }]
        }))
        .send()
        .await
        .unwrap();

    // Standard vector query should still work
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 1);

    cleanup_ns(&harness.store, &ns).await;
}

// ---------------------------------------------------------------------------
// Test 12: No results for non-matching query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_no_matching_terms() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-nomatch");

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
            "vectors": [{
                "id": format!("{ns}_d1"),
                "values": [0.1, 0.2, 0.3, 0.4],
                "attributes": { "content": "apple banana cherry" }
            }]
        }))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["content", "BM25", "zeppelin"],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(results.is_empty(), "no documents match 'zeppelin'");

    cleanup_ns(&harness.store, &ns).await;
}
