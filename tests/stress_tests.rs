mod common;

use common::harness::TestHarness;
use common::server::{
    cleanup_ns, create_ns_api, create_ns_api_with, start_test_server,
    start_test_server_with_compactor, start_test_server_with_config,
};
use common::vectors::random_vectors;

use futures::future::join_all;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use zeppelin::config::{CompactionConfig, Config, IndexingConfig};
use zeppelin::query::{execute_query, QueryParams};
use zeppelin::types::{ConsistencyLevel, DistanceMetric, VectorEntry};
use zeppelin::wal::{Manifest, WalReader, WalWriter};

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

/// Test config with small centroids for fast compaction.
fn stress_test_config() -> Config {
    Config {
        compaction: CompactionConfig {
            max_wal_fragments_before_compact: 3,
            ..Default::default()
        },
        indexing: IndexingConfig {
            default_num_centroids: 4,
            kmeans_max_iterations: 10,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn squared_l2_to_origin(values: &[f32]) -> f32 {
    values.iter().map(|value| value * value).sum()
}

// ---------------------------------------------------------------------------
// Task 28: Large uncompacted WAL backlog exact top-k
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_stress_large_uncompacted_wal_backlog_exact_topk() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("large-uncompacted-wal-backlog");
    Manifest::new()
        .write(&harness.store, &namespace)
        .await
        .unwrap();

    let writer = WalWriter::new(harness.store.clone());
    let mut latest: HashMap<String, Vec<f32>> = HashMap::new();
    let fragment_count = 30usize;
    let vectors_per_fragment = 200usize;

    for fragment_idx in 0..fragment_count {
        let mut vectors = Vec::with_capacity(vectors_per_fragment);
        for vector_idx in 0..vectors_per_fragment {
            let logical_id = (fragment_idx * 137 + vector_idx) % 1_000;
            let id = format!("backlog_{logical_id:04}");
            let sequence = (fragment_idx * vectors_per_fragment + vector_idx + 1) as f32;
            let values = vec![
                sequence / 10_000.0,
                (logical_id % 17) as f32 / 1_000.0,
                (fragment_idx % 11) as f32 / 1_000.0,
                (vector_idx % 13) as f32 / 1_000.0,
            ];
            latest.insert(id.clone(), values.clone());
            vectors.push(VectorEntry {
                id,
                values,
                attributes: None,
            });
        }
        writer.append(&namespace, vectors, vec![]).await.unwrap();
    }

    let manifest = Manifest::read(&harness.store, &namespace)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manifest.uncompacted_fragments().len(), fragment_count);

    let mut expected: Vec<(String, f32)> = latest
        .iter()
        .map(|(id, values)| (id.clone(), squared_l2_to_origin(values)))
        .collect();
    expected.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    expected.truncate(10);

    let wal_reader = WalReader::new(harness.store.clone());
    let response = execute_query(QueryParams {
        store: &harness.store,
        wal_reader: &wal_reader,
        namespace: &namespace,
        query: &[0.0, 0.0, 0.0, 0.0],
        top_k: 10,
        nprobe: 1,
        filter: None,
        consistency: ConsistencyLevel::Strong,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 1,
        rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: None,
        include_attributes: false,
    })
    .await
    .unwrap();

    assert_eq!(response.scanned_fragments, fragment_count);
    assert_eq!(response.scanned_segments, 0);

    let actual: Vec<(String, f32)> = response
        .results
        .iter()
        .map(|result| (result.id.clone(), result.score))
        .collect();
    assert_eq!(actual.len(), expected.len());
    for ((actual_id, actual_score), (expected_id, expected_score)) in
        actual.iter().zip(expected.iter())
    {
        assert_eq!(actual_id, expected_id);
        assert!(
            (actual_score - expected_score).abs() <= f32::EPSILON,
            "score mismatch for {actual_id}: actual={actual_score}, expected={expected_score}"
        );
    }

    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 1: Concurrent writers to the same namespace
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_stress_concurrent_writers_same_namespace() {
    let start = Instant::now();
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_with(
        &client,
        &base_url,
        serde_json::json!({
            "dimensions": 16,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    // Spawn 10 concurrent writers, each upserting 50 vectors
    let num_writers = 10;
    let vecs_per_writer = 50;

    let handles: Vec<_> = (0..num_writers)
        .map(|w| {
            let client = client.clone();
            let url = base_url.clone();
            let ns = ns.clone();
            tokio::spawn(async move {
                let vectors = prefixed_vectors(&format!("writer_{w}"), vecs_per_writer, 16);
                let resp = client
                    .post(format!("{url}/v1/namespaces/{ns}/vectors"))
                    .json(&serde_json::json!({ "vectors": vectors }))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(
                    resp.status(),
                    200,
                    "writer {w} failed with status {}",
                    resp.status()
                );
                let body: serde_json::Value = resp.json().await.unwrap();
                assert_eq!(body["upserted"], vecs_per_writer);
            })
        })
        .collect();

    join_all(handles).await.into_iter().for_each(|r| r.unwrap());

    // Query to verify vectors are queryable
    let query_vec: Vec<f32> = (0..16).map(|_| 0.5).collect();
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 500,
            "consistency": "strong",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(
        !results.is_empty(),
        "expected results after concurrent writes"
    );

    let elapsed = start.elapsed();
    eprintln!(
        "[STRESS] concurrent_writers: {} writers x {} vecs, query returned {} results in {:.2}s",
        num_writers,
        vecs_per_writer,
        results.len(),
        elapsed.as_secs_f64()
    );
    assert!(elapsed.as_secs() < 120, "test exceeded 120s timeout");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 2: Concurrent readers during writes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_stress_concurrent_readers_during_writes() {
    let start = Instant::now();
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_with(
        &client,
        &base_url,
        serde_json::json!({
            "dimensions": 8,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    // Upsert initial batch
    let initial = random_vectors(20, 8);
    let query_vec = initial[0].values.clone();
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": initial }))
        .send()
        .await
        .unwrap();

    // 1 writer: 20 sequential upserts of 10 vectors
    let writer_handle = {
        let client = client.clone();
        let url = base_url.clone();
        let ns = ns.clone();
        tokio::spawn(async move {
            let mut write_errors = 0u32;
            for batch in 0..20 {
                let vectors = prefixed_vectors(&format!("batch_{batch}"), 10, 8);
                let resp = client
                    .post(format!("{url}/v1/namespaces/{ns}/vectors"))
                    .json(&serde_json::json!({ "vectors": vectors }))
                    .send()
                    .await
                    .unwrap();
                if resp.status() != 200 {
                    write_errors += 1;
                }
            }
            write_errors
        })
    };

    // 5 readers: 20 sequential queries each
    let reader_handles: Vec<_> = (0..5)
        .map(|_| {
            let client = client.clone();
            let url = base_url.clone();
            let ns = ns.clone();
            let qv = query_vec.clone();
            tokio::spawn(async move {
                let mut query_errors = 0u32;
                for _ in 0..20 {
                    let resp = client
                        .post(format!("{url}/v1/namespaces/{ns}/query"))
                        .json(&serde_json::json!({
                            "vector": qv,
                            "top_k": 10,
                            "consistency": "strong",
                        }))
                        .send()
                        .await
                        .unwrap();
                    if resp.status() != 200 {
                        query_errors += 1;
                    }
                }
                query_errors
            })
        })
        .collect();

    let write_errors = writer_handle.await.unwrap();
    let read_errors: u32 = join_all(reader_handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .sum();

    assert_eq!(write_errors, 0, "expected zero write errors");
    assert_eq!(read_errors, 0, "expected zero read errors");

    let elapsed = start.elapsed();
    eprintln!(
        "[STRESS] concurrent_readers_during_writes: 1 writer (20x10), 5 readers (20 queries each) in {:.2}s",
        elapsed.as_secs_f64()
    );
    assert!(elapsed.as_secs() < 120, "test exceeded 120s timeout");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 3: Large batch upsert (10,000 vectors)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_stress_large_batch_upsert() {
    let start = Instant::now();
    // Use explicit config with large body limit for 10k vector payload
    let mut config = zeppelin::config::Config::default();
    config.server.max_request_body_mb = 100;
    let (base_url, harness, _cache, _dir, admin_bearer) =
        start_test_server_with_config(Some(config)).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_with(
        &client,
        &base_url,
        serde_json::json!({
            "dimensions": 32,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    // Generate 10,000 vectors
    let vectors = prefixed_vectors("bulk", 10_000, 32);
    let first_vec = vectors[0].values.clone();
    let last_vec = vectors[9_999].values.clone();

    // Single upsert of all 10,000
    let upsert_start = Instant::now();
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .timeout(std::time::Duration::from_secs(60))
        .send()
        .await
        .unwrap();
    let upsert_elapsed = upsert_start.elapsed();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["upserted"], 10_000);

    // Query first vector
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": first_vec,
            "top_k": 1,
            "consistency": "strong",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert_eq!(results[0]["id"], "bulk_vec_0");

    // Query last vector
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": last_vec,
            "top_k": 1,
            "consistency": "strong",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert_eq!(results[0]["id"], "bulk_vec_9999");

    let elapsed = start.elapsed();
    eprintln!(
        "[STRESS] large_batch_upsert: 10,000 vecs upserted in {:.2}s, total {:.2}s",
        upsert_elapsed.as_secs_f64(),
        elapsed.as_secs_f64()
    );
    assert!(elapsed.as_secs() < 120, "test exceeded 120s timeout");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 4: Rapid namespace create/delete
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_stress_rapid_namespace_create_delete() {
    let start = Instant::now();
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);

    let ns_count = 20;

    // Create 20 namespaces sequentially via API (server assigns UUID names)
    let mut ns_names: Vec<String> = Vec::with_capacity(ns_count);
    for _ in 0..ns_count {
        let ns = create_ns_api(&client, &base_url, 4).await;
        ns_names.push(ns);
    }

    // Verify each namespace is accessible via GET
    for ns in &ns_names {
        let resp = client
            .get(format!("{base_url}/v1/namespaces/{ns}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "namespace {ns} should be accessible");
    }

    // Delete all 20 concurrently
    let delete_handles: Vec<_> = ns_names
        .iter()
        .map(|ns| {
            let client = client.clone();
            let url = base_url.clone();
            let ns = ns.clone();
            tokio::spawn(async move {
                let resp = client
                    .delete(format!("{url}/v1/namespaces/{ns}"))
                    .send()
                    .await
                    .unwrap();
                (ns, resp.status())
            })
        })
        .collect();

    let results = join_all(delete_handles).await;
    for r in &results {
        let (ns, status) = r.as_ref().unwrap();
        assert_eq!(*status, 202, "failed to accept namespace delete {ns}");
    }

    // Verify each namespace is gone (GET returns 404)
    for ns in &ns_names {
        let mut deleted = false;
        for _ in 0..50 {
            let resp = client
                .get(format!("{base_url}/v1/namespaces/{ns}"))
                .send()
                .await
                .unwrap();
            if resp.status() == 404 {
                deleted = true;
                break;
            }
            assert_eq!(resp.status(), 200, "unexpected status for namespace {ns}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(deleted, "namespace {ns} should be deleted (404)");
    }

    // Re-create one to prove cleanup was complete
    let recreated_ns = create_ns_api(&client, &base_url, 4).await;

    let elapsed = start.elapsed();
    eprintln!(
        "[STRESS] rapid_namespace_create_delete: {ns_count} namespaces created+deleted in {:.2}s",
        elapsed.as_secs_f64()
    );
    assert!(elapsed.as_secs() < 120, "test exceeded 120s timeout");

    cleanup_ns(&harness.store, &recreated_ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 5: High query concurrency (WAL + segment merge path)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_stress_high_query_concurrency() {
    let start = Instant::now();
    let config = stress_test_config();
    let (base_url, harness, _cache, _dir, compactor, admin_bearer) =
        start_test_server_with_compactor(Some(config)).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_with(
        &client,
        &base_url,
        serde_json::json!({
            "dimensions": 16,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    // Upsert 200 vectors
    let vectors = prefixed_vectors("seg", 200, 16);
    let query_vec = vectors[0].values.clone();
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();

    // Compact (creates segment)
    let result = compactor.compact(&ns).await.unwrap();
    assert_eq!(result.vectors_compacted, 200);

    // Upsert 50 more (WAL fragments remain)
    let wal_vectors = prefixed_vectors("wal", 50, 16);
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": wal_vectors }))
        .send()
        .await
        .unwrap();

    // Spawn 50 concurrent queries
    let num_queries = 50;
    let query_handles: Vec<_> = (0..num_queries)
        .map(|_| {
            let client = client.clone();
            let url = base_url.clone();
            let ns = ns.clone();
            let qv = query_vec.clone();
            tokio::spawn(async move {
                let resp = client
                    .post(format!("{url}/v1/namespaces/{ns}/query"))
                    .json(&serde_json::json!({
                        "vector": qv,
                        "top_k": 10,
                        "consistency": "strong",
                    }))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.status(), 200);
                let body: serde_json::Value = resp.json().await.unwrap();
                body
            })
        })
        .collect();

    let query_results = join_all(query_handles).await;
    for r in &query_results {
        let body = r.as_ref().unwrap();
        let results = body["results"].as_array().unwrap();
        assert!(!results.is_empty(), "query should return results");
        // Verify merge path: both fragments and segments scanned
        let scanned_fragments = body["scanned_fragments"].as_u64().unwrap();
        let scanned_segments = body["scanned_segments"].as_u64().unwrap();
        assert!(
            scanned_fragments > 0,
            "expected scanned_fragments > 0 (merge path)"
        );
        assert!(
            scanned_segments > 0,
            "expected scanned_segments > 0 (merge path)"
        );
    }

    let elapsed = start.elapsed();
    eprintln!(
        "[STRESS] high_query_concurrency: {num_queries} concurrent queries over WAL+segment in {:.2}s",
        elapsed.as_secs_f64()
    );
    assert!(elapsed.as_secs() < 120, "test exceeded 120s timeout");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 6: Concurrent upsert and delete
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_stress_concurrent_upsert_and_delete() {
    let start = Instant::now();
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_with(
        &client,
        &base_url,
        serde_json::json!({
            "dimensions": 8,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    // Upsert 100 base vectors
    let base_vecs = prefixed_vectors("base", 100, 8);
    client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": base_vecs }))
        .send()
        .await
        .unwrap();

    // IDs to delete (first 50 base vectors, split across 5 deleters = 10 each)
    let delete_ids: Vec<String> = (0..50).map(|i| format!("base_vec_{i}")).collect();

    // 5 upserter tasks (20 new vectors each)
    let upserter_handles: Vec<_> = (0..5)
        .map(|u| {
            let client = client.clone();
            let url = base_url.clone();
            let ns = ns.clone();
            tokio::spawn(async move {
                let vectors = prefixed_vectors(&format!("new_{u}"), 20, 8);
                let resp = client
                    .post(format!("{url}/v1/namespaces/{ns}/vectors"))
                    .json(&serde_json::json!({ "vectors": vectors }))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.status(), 200);
            })
        })
        .collect();

    // 5 deleter tasks (10 base vectors each)
    let deleter_handles: Vec<_> = (0..5)
        .map(|d| {
            let client = client.clone();
            let url = base_url.clone();
            let ns = ns.clone();
            let ids: Vec<String> = delete_ids[d * 10..(d + 1) * 10].to_vec();
            tokio::spawn(async move {
                let resp = client
                    .delete(format!("{url}/v1/namespaces/{ns}/vectors"))
                    .json(&serde_json::json!({ "ids": ids }))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.status(), 204);
            })
        })
        .collect();

    join_all(upserter_handles)
        .await
        .into_iter()
        .for_each(|r| r.unwrap());
    join_all(deleter_handles)
        .await
        .into_iter()
        .for_each(|r| r.unwrap());

    // Query all vectors
    let query_vec: Vec<f32> = (0..8).map(|_| 0.5).collect();
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 300,
            "consistency": "strong",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    let result_ids: Vec<&str> = results.iter().map(|r| r["id"].as_str().unwrap()).collect();

    // Verify deleted IDs are not in results
    for del_id in &delete_ids {
        assert!(
            !result_ids.contains(&del_id.as_str()),
            "deleted ID {del_id} should not be in results"
        );
    }

    // Verify some new vectors are present
    let new_ids_present = result_ids.iter().any(|id| id.starts_with("new_"));
    assert!(new_ids_present, "expected some new vectors in results");

    let elapsed = start.elapsed();
    eprintln!(
        "[STRESS] concurrent_upsert_and_delete: {} results, {:.2}s",
        results.len(),
        elapsed.as_secs_f64()
    );
    assert!(elapsed.as_secs() < 120, "test exceeded 120s timeout");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
