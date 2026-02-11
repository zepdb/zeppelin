mod common;

use common::server::{
    api_ns, cleanup_ns, start_test_server_with_compaction, start_test_server_with_compactor,
};
use common::vectors::{simple_attributes, with_attributes};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::time::Instant;
use zeppelin::config::{CompactionConfig, Config, IndexingConfig};
use zeppelin::types::VectorEntry;
use zeppelin::wal::Manifest;

/// Generate `n` random vectors with a given ID prefix and dimension.
/// Uses a hash of the prefix as the seed so different prefixes produce different vectors.
fn prefixed_vectors(prefix: &str, n: usize, dims: usize) -> Vec<VectorEntry> {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    prefix.hash(&mut hasher);
    let seed = hasher.finish();
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n)
        .map(|i| VectorEntry {
            id: format!("{prefix}_vec_{i}"),
            values: (0..dims).map(|_| rng.gen_range(-1.0..1.0)).collect(),
            attributes: None,
        })
        .collect()
}

/// Generate vectors with attributes using the simple_attributes generator.
fn prefixed_vectors_with_attrs(prefix: &str, n: usize, dims: usize) -> Vec<VectorEntry> {
    let vecs = prefixed_vectors(prefix, n, dims);
    with_attributes(vecs, simple_attributes)
}

/// Test config with small centroids for fast compaction.
fn e2e_test_config() -> Config {
    let mut config = Config::load(None).unwrap();
    config.compaction = CompactionConfig {
        max_wal_fragments_before_compact: 3,
        ..Default::default()
    };
    config.indexing = IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 10,
        ..Default::default()
    };
    config
}

// ---------------------------------------------------------------------------
// Test 1: Write -> Compact -> Query lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_e2e_write_compact_query_lifecycle() {
    let total_start = Instant::now();
    let config = e2e_test_config();
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "e2e-lifecycle");

    // Create namespace
    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 32,
            "distance_metric": "euclidean"
        }))
        .send()
        .await
        .unwrap();

    // Phase 1: Write 500 vectors with attributes
    let vectors = prefixed_vectors_with_attrs("life", 500, 32);
    let query_vec = vectors[0].values.clone();

    let write_start = Instant::now();
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    let write_elapsed = write_start.elapsed();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["upserted"], 500);

    // Phase 2: Compact
    let compact_start = Instant::now();
    let result = compactor.compact(&ns).await.unwrap();
    let compact_elapsed = compact_start.elapsed();
    assert_eq!(result.vectors_compacted, 500);

    // Phase 3: Strong query (should scan segment, not fragments)
    let strong_start = Instant::now();
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 10,
            "consistency": "strong",
        }))
        .send()
        .await
        .unwrap();
    let strong_elapsed = strong_start.elapsed();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(!results.is_empty());
    assert_eq!(
        body["scanned_fragments"].as_u64().unwrap(),
        0,
        "post-compaction strong query should scan 0 fragments"
    );
    assert!(body["scanned_segments"].as_u64().unwrap() >= 1);

    // Phase 4: Eventual query
    let eventual_start = Instant::now();
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 10,
            "consistency": "eventual",
        }))
        .send()
        .await
        .unwrap();
    let eventual_elapsed = eventual_start.elapsed();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(!body["results"].as_array().unwrap().is_empty());

    let total_elapsed = total_start.elapsed();
    eprintln!("[PERF] write_compact_query_lifecycle:");
    eprintln!("  write (500 vecs):   {:.3}s", write_elapsed.as_secs_f64());
    eprintln!("  compact:            {:.3}s", compact_elapsed.as_secs_f64());
    eprintln!(
        "  strong query:       {:.3}s",
        strong_elapsed.as_secs_f64()
    );
    eprintln!(
        "  eventual query:     {:.3}s",
        eventual_elapsed.as_secs_f64()
    );
    eprintln!("  total:              {:.3}s", total_elapsed.as_secs_f64());
    assert!(total_elapsed.as_secs() < 120, "test exceeded 120s timeout");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 2: Background compaction triggers automatically
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_e2e_background_compaction_triggers() {
    let total_start = Instant::now();

    // Config: fast compaction interval, low fragment threshold
    let mut config = Config::load(None).unwrap();
    config.compaction = CompactionConfig {
        interval_secs: 2,
        max_wal_fragments_before_compact: 3,
        ..Default::default()
    };
    config.indexing = IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 10,
        ..Default::default()
    };

    let (base_url, harness, _cache, _dir, shutdown_tx) =
        start_test_server_with_compaction(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "e2e-bgcompact");

    // Create namespace
    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 16,
            "distance_metric": "euclidean"
        }))
        .send()
        .await
        .unwrap();

    // Write 3 separate upsert calls (creates 3 WAL fragments, triggering threshold)
    for batch in 0..3 {
        let vectors = prefixed_vectors(&format!("bg_{batch}"), 20, 16);
        let resp = client
            .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
            .json(&serde_json::json!({ "vectors": vectors }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    // Poll manifest for compaction completion (up to 30s)
    let poll_start = Instant::now();
    let mut compaction_detected = false;
    for _ in 0..60 {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let manifest = Manifest::read(&harness.store, &ns).await.unwrap();
        if let Some(m) = manifest {
            if m.active_segment.is_some() && m.uncompacted_fragments().is_empty() {
                compaction_detected = true;
                break;
            }
        }
    }
    let poll_elapsed = poll_start.elapsed();

    assert!(
        compaction_detected,
        "background compaction did not trigger within 30s"
    );

    // Query to verify (eventual = segment only)
    let query_vec: Vec<f32> = (0..16).map(|_| 0.5).collect();
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 10,
            "consistency": "eventual",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["scanned_segments"].as_u64().unwrap() >= 1);
    assert_eq!(body["scanned_fragments"].as_u64().unwrap(), 0);

    let total_elapsed = total_start.elapsed();
    eprintln!("[PERF] background_compaction_triggers:");
    eprintln!(
        "  time-to-compaction: {:.3}s",
        poll_elapsed.as_secs_f64()
    );
    eprintln!("  total:              {:.3}s", total_elapsed.as_secs_f64());

    // Shutdown background loop
    let _ = shutdown_tx.send(true);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 3: Incremental compaction (multiple cycles)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_e2e_incremental_compaction() {
    let total_start = Instant::now();
    let config = e2e_test_config();
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "e2e-incremental");

    // Create namespace
    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 16,
            "distance_metric": "euclidean"
        }))
        .send()
        .await
        .unwrap();

    // Batch 1: Upsert 100 vectors, compact
    let batch1 = prefixed_vectors("batch1", 100, 16);
    let batch1_query = batch1[0].values.clone();
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": batch1 }))
        .send()
        .await
        .unwrap();

    let compact1_start = Instant::now();
    let result1 = compactor.compact(&ns).await.unwrap();
    let compact1_elapsed = compact1_start.elapsed();
    assert_eq!(result1.vectors_compacted, 100);

    // Batch 2: Upsert 100 more, compact again
    let batch2 = prefixed_vectors("batch2", 100, 16);
    let batch2_query = batch2[0].values.clone();
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": batch2 }))
        .send()
        .await
        .unwrap();

    let compact2_start = Instant::now();
    let result2 = compactor.compact(&ns).await.unwrap();
    let compact2_elapsed = compact2_start.elapsed();
    assert_eq!(result2.vectors_compacted, 200);
    assert!(
        result2.old_segment_removed.is_some(),
        "incremental compaction should replace old segment"
    );

    // Query batch1's first vector
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": batch1_query,
            "top_k": 1,
            "consistency": "eventual",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["results"][0]["id"], "batch1_vec_0");

    // Query batch2's first vector
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": batch2_query,
            "top_k": 1,
            "consistency": "eventual",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["results"][0]["id"], "batch2_vec_0");

    // Verify manifest: 1 active segment, no stale fragments
    let manifest = Manifest::read(&harness.store, &ns)
        .await
        .unwrap()
        .unwrap();
    assert!(manifest.active_segment.is_some());
    assert!(
        manifest.uncompacted_fragments().is_empty(),
        "should have no uncompacted fragments"
    );

    let total_elapsed = total_start.elapsed();
    eprintln!("[PERF] incremental_compaction:");
    eprintln!(
        "  compact1 (100 vecs): {:.3}s",
        compact1_elapsed.as_secs_f64()
    );
    eprintln!(
        "  compact2 (200 vecs): {:.3}s",
        compact2_elapsed.as_secs_f64()
    );
    eprintln!("  total:               {:.3}s", total_elapsed.as_secs_f64());
    assert!(total_elapsed.as_secs() < 120, "test exceeded 120s timeout");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 4: Filtered query after compaction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_e2e_filtered_query_after_compaction() {
    let total_start = Instant::now();
    let config = e2e_test_config();
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "e2e-filter");

    // Create namespace
    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 16,
            "distance_metric": "euclidean"
        }))
        .send()
        .await
        .unwrap();

    // Upsert 300 vectors with attributes (category a/b/c round-robin, score 0..299)
    let vectors = prefixed_vectors_with_attrs("filt", 300, 16);
    let query_vec = vectors[0].values.clone();
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();

    // Compact
    compactor.compact(&ns).await.unwrap();

    // Filter 1: Eq (category == "a")
    let eq_start = Instant::now();
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 300,
            "filter": {"op": "eq", "field": "category", "value": "a"},
            "consistency": "eventual",
        }))
        .send()
        .await
        .unwrap();
    let eq_elapsed = eq_start.elapsed();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(!results.is_empty());
    for r in results {
        let attrs = r["attributes"].as_object().unwrap();
        assert_eq!(attrs["category"], "a", "eq filter: expected category=a");
    }

    // Filter 2: Range (score in [0, 50])
    let range_start = Instant::now();
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 300,
            "filter": {"op": "range", "field": "score", "gte": 0.0, "lte": 50.0},
            "consistency": "eventual",
        }))
        .send()
        .await
        .unwrap();
    let range_elapsed = range_start.elapsed();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    for r in results {
        let score = r["attributes"]["score"].as_i64().unwrap();
        assert!(score <= 50, "range filter: score {score} should be <= 50");
    }

    // Filter 3: And (category == "b" AND score <= 30)
    let and_start = Instant::now();
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 300,
            "filter": {
                "op": "and",
                "filters": [
                    {"op": "eq", "field": "category", "value": "b"},
                    {"op": "range", "field": "score", "gte": 0.0, "lte": 30.0}
                ]
            },
            "consistency": "eventual",
        }))
        .send()
        .await
        .unwrap();
    let and_elapsed = and_start.elapsed();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    for r in results {
        let attrs = r["attributes"].as_object().unwrap();
        assert_eq!(attrs["category"], "b", "and filter: expected category=b");
        let score = attrs["score"].as_i64().unwrap();
        assert!(score <= 30, "and filter: score {score} should be <= 30");
    }

    // Filter 4: NotEq (category != "c")
    let noteq_start = Instant::now();
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 300,
            "filter": {"op": "not_eq", "field": "category", "value": "c"},
            "consistency": "eventual",
        }))
        .send()
        .await
        .unwrap();
    let noteq_elapsed = noteq_start.elapsed();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    for r in results {
        let attrs = r["attributes"].as_object().unwrap();
        assert_ne!(attrs["category"], "c", "not_eq filter: should exclude c");
    }

    let total_elapsed = total_start.elapsed();
    eprintln!("[PERF] filtered_query_after_compaction:");
    eprintln!("  eq filter:    {:.3}s", eq_elapsed.as_secs_f64());
    eprintln!("  range filter: {:.3}s", range_elapsed.as_secs_f64());
    eprintln!("  and filter:   {:.3}s", and_elapsed.as_secs_f64());
    eprintln!("  not_eq filter:{:.3}s", noteq_elapsed.as_secs_f64());
    eprintln!("  total:        {:.3}s", total_elapsed.as_secs_f64());
    assert!(total_elapsed.as_secs() < 120, "test exceeded 120s timeout");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 5: Delete -> Compact -> Verify gone
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_e2e_delete_compact_verify_gone() {
    let total_start = Instant::now();
    let config = e2e_test_config();
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "e2e-delete");

    // Create namespace
    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 8,
            "distance_metric": "euclidean"
        }))
        .send()
        .await
        .unwrap();

    // Upsert 50 vectors
    let vectors = prefixed_vectors("del", 50, 8);
    let query_vec = vectors[0].values.clone();
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();

    // Delete 5 vectors via HTTP
    let delete_ids: Vec<String> = (0..5).map(|i| format!("del_vec_{i}")).collect();
    let resp = client
        .delete(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "ids": delete_ids }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Pre-compaction strong query: deleted IDs absent
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 50,
            "consistency": "strong",
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    let result_ids: Vec<&str> = results.iter().map(|r| r["id"].as_str().unwrap()).collect();
    for del_id in &delete_ids {
        assert!(
            !result_ids.contains(&del_id.as_str()),
            "pre-compaction: deleted ID {del_id} should be absent"
        );
    }

    // Compact
    let compact_start = Instant::now();
    let result = compactor.compact(&ns).await.unwrap();
    let compact_elapsed = compact_start.elapsed();
    assert_eq!(result.vectors_compacted, 45);

    // Post-compaction strong query
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 50,
            "consistency": "strong",
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    let result_ids: Vec<&str> = results.iter().map(|r| r["id"].as_str().unwrap()).collect();
    for del_id in &delete_ids {
        assert!(
            !result_ids.contains(&del_id.as_str()),
            "post-compaction strong: deleted ID {del_id} should be absent"
        );
    }
    assert_eq!(body["scanned_fragments"].as_u64().unwrap(), 0);

    // Post-compaction eventual query
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 50,
            "consistency": "eventual",
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    let result_ids: Vec<&str> = results.iter().map(|r| r["id"].as_str().unwrap()).collect();
    for del_id in &delete_ids {
        assert!(
            !result_ids.contains(&del_id.as_str()),
            "post-compaction eventual: deleted ID {del_id} should be absent"
        );
    }
    assert!(body["scanned_segments"].as_u64().unwrap() >= 1);

    let total_elapsed = total_start.elapsed();
    eprintln!("[PERF] delete_compact_verify_gone:");
    eprintln!("  compact:  {:.3}s", compact_elapsed.as_secs_f64());
    eprintln!("  total:    {:.3}s", total_elapsed.as_secs_f64());
    assert!(total_elapsed.as_secs() < 120, "test exceeded 120s timeout");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 6: Strong vs Eventual consistency comparison
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_e2e_strong_vs_eventual_consistency() {
    let total_start = Instant::now();
    let config = e2e_test_config();
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "e2e-consistency");

    // Create namespace
    client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 16,
            "distance_metric": "euclidean"
        }))
        .send()
        .await
        .unwrap();

    // Upsert 200 vectors (will be compacted)
    let compacted_vecs = prefixed_vectors("compacted", 200, 16);
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": compacted_vecs }))
        .send()
        .await
        .unwrap();

    // Compact
    compactor.compact(&ns).await.unwrap();

    // Upsert 50 more (WAL only, NOT compacted)
    let wal_vecs = prefixed_vectors("wal", 50, 16);
    let wal_query_vec = wal_vecs[0].values.clone();
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": wal_vecs }))
        .send()
        .await
        .unwrap();

    // Strong query with a WAL vector: should find it
    let strong_start = Instant::now();
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": wal_query_vec,
            "top_k": 10,
            "consistency": "strong",
        }))
        .send()
        .await
        .unwrap();
    let strong_elapsed = strong_start.elapsed();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert_eq!(
        results[0]["id"], "wal_vec_0",
        "strong query should find wal vector as top result"
    );
    assert!(
        body["scanned_fragments"].as_u64().unwrap() > 0,
        "strong query should scan WAL fragments"
    );

    // Eventual query with same vector: should NOT find WAL vectors
    let eventual_start = Instant::now();
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": wal_query_vec,
            "top_k": 10,
            "consistency": "eventual",
        }))
        .send()
        .await
        .unwrap();
    let eventual_elapsed = eventual_start.elapsed();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    let result_ids: Vec<&str> = results.iter().map(|r| r["id"].as_str().unwrap()).collect();
    // No WAL vector IDs should appear in eventual results
    for id in &result_ids {
        assert!(
            !id.starts_with("wal_"),
            "eventual query should not return WAL vector {id}"
        );
    }
    assert_eq!(
        body["scanned_fragments"].as_u64().unwrap(),
        0,
        "eventual query should not scan fragments"
    );

    let total_elapsed = total_start.elapsed();
    eprintln!("[PERF] strong_vs_eventual_consistency:");
    eprintln!(
        "  strong query:   {:.3}s (scanned fragments + segments)",
        strong_elapsed.as_secs_f64()
    );
    eprintln!(
        "  eventual query: {:.3}s (segments only)",
        eventual_elapsed.as_secs_f64()
    );
    eprintln!("  total:          {:.3}s", total_elapsed.as_secs_f64());
    assert!(total_elapsed.as_secs() < 120, "test exceeded 120s timeout");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
