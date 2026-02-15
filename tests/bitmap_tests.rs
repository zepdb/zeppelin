mod common;

use common::server::{cleanup_ns, create_ns_api_with, start_test_server_with_compactor};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use zeppelin::config::{CompactionConfig, Config, IndexingConfig};
use zeppelin::index::bitmap::{bitmap_key, ClusterBitmapIndex};
use zeppelin::types::{AttributeValue, VectorEntry};
use zeppelin::wal::Manifest;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn bitmap_test_config(bitmap_enabled: bool) -> Config {
    let mut config = Config::load(None).unwrap();
    config.compaction = CompactionConfig {
        max_wal_fragments_before_compact: 1,
        ..Default::default()
    };
    config.indexing = IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 10,
        bitmap_index: bitmap_enabled,
        ..Default::default()
    };
    config
}

fn bitmap_test_config_hierarchical(bitmap_enabled: bool) -> Config {
    let mut config = bitmap_test_config(bitmap_enabled);
    config.indexing.hierarchical = true;
    config.indexing.leaf_size = Some(20);
    config
}

/// Generate `n` vectors with "status" (active/inactive) and "priority" (0..n) attributes.
fn status_vectors(prefix: &str, n: usize, dims: usize) -> Vec<VectorEntry> {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    prefix.hash(&mut hasher);
    let seed = hasher.finish();
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n)
        .map(|i| VectorEntry {
            id: format!("{prefix}_vec_{i}"),
            values: (0..dims).map(|_| rng.gen_range(-1.0..1.0)).collect(),
            attributes: Some({
                let mut attrs = HashMap::new();
                attrs.insert(
                    "status".to_string(),
                    AttributeValue::String(if i % 2 == 0 {
                        "active".to_string()
                    } else {
                        "inactive".to_string()
                    }),
                );
                attrs.insert("priority".to_string(), AttributeValue::Integer(i as i64));
                attrs
            }),
        })
        .collect()
}

/// Generate vectors with list attributes for Contains filter testing.
fn tagged_vectors(prefix: &str, n: usize, dims: usize) -> Vec<VectorEntry> {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    prefix.hash(&mut hasher);
    let seed = hasher.finish();
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n)
        .map(|i| VectorEntry {
            id: format!("{prefix}_vec_{i}"),
            values: (0..dims).map(|_| rng.gen_range(-1.0..1.0)).collect(),
            attributes: Some({
                let mut attrs = HashMap::new();
                // Cycle through tags: first element rotates [alpha, beta, gamma]
                let tags = match i % 3 {
                    0 => vec!["alpha".to_string(), "common".to_string()],
                    1 => vec!["beta".to_string(), "common".to_string()],
                    _ => vec!["gamma".to_string()],
                };
                attrs.insert("tags".to_string(), AttributeValue::StringList(tags));
                attrs.insert("priority".to_string(), AttributeValue::Integer(i as i64));
                attrs
            }),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Test 1: IVF-Flat with Eq filter — verify bitmap files on S3 and correct results
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_bitmap_ivf_flat_eq_filter() {
    let config = bitmap_test_config(true);
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        serde_json::json!({
            "dimensions": 16,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    // Ingest 100 vectors with status=active/inactive
    let vectors = status_vectors("ivfeq", 100, 16);
    let query_vec = vectors[0].values.clone(); // status=active (index 0 is even)

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Compact
    let result = compactor.compact(&ns).await.unwrap();
    assert!(result.segment_id.is_some());
    let segment_id = result.segment_id.unwrap();

    // Verify bitmap files exist on S3
    let bm_key = bitmap_key(&ns, &segment_id, 0);
    let bm_data = harness.store.get(&bm_key).await.unwrap();
    let bm_index = ClusterBitmapIndex::from_bytes(&bm_data).unwrap();
    assert!(
        bm_index.fields.contains_key("status"),
        "bitmap index should contain 'status' field"
    );
    assert!(
        bm_index.fields.contains_key("priority"),
        "bitmap index should contain 'priority' field"
    );

    // Verify SegmentRef has bitmap_fields
    let manifest = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    let seg_ref = manifest
        .segments
        .iter()
        .find(|s| s.id == segment_id)
        .unwrap();
    assert!(
        !seg_ref.bitmap_fields.is_empty(),
        "SegmentRef should have bitmap_fields"
    );

    // Query with Eq filter — only active vectors
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 10,
            "consistency": "eventual",
            "filter": {
                "op": "eq",
                "field": "status",
                "value": "active"
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(!results.is_empty(), "should return results");

    // All returned results should have status=active
    for r in results {
        let status = r["attributes"]["status"].as_str().unwrap();
        assert_eq!(
            status, "active",
            "filtered result should have status=active"
        );
    }

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 2: Hierarchical with Eq filter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_bitmap_hierarchical_eq_filter() {
    let config = bitmap_test_config_hierarchical(true);
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        serde_json::json!({
            "dimensions": 16,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    let vectors = status_vectors("hanneq", 100, 16);
    let query_vec = vectors[0].values.clone();

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let result = compactor.compact(&ns).await.unwrap();
    assert!(result.segment_id.is_some());

    // Query with Eq filter
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 10,
            "consistency": "eventual",
            "filter": {
                "op": "eq",
                "field": "status",
                "value": "active"
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(!results.is_empty(), "should return results");

    for r in results {
        let status = r["attributes"]["status"].as_str().unwrap();
        assert_eq!(status, "active");
    }

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 3: Backward compatibility — bitmap_index=false still works via post-filter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_bitmap_backward_compat() {
    let config = bitmap_test_config(false); // bitmap disabled
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        serde_json::json!({
            "dimensions": 16,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    let vectors = status_vectors("compat", 50, 16);
    let query_vec = vectors[0].values.clone();

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let result = compactor.compact(&ns).await.unwrap();
    assert!(result.segment_id.is_some());
    let segment_id = result.segment_id.unwrap();

    // Verify NO bitmap files on S3
    let bm_key = bitmap_key(&ns, &segment_id, 0);
    assert!(
        harness.store.get(&bm_key).await.is_err(),
        "bitmap file should NOT exist when bitmap_index=false"
    );

    // SegmentRef should have empty bitmap_fields
    let manifest = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    let seg_ref = manifest
        .segments
        .iter()
        .find(|s| s.id == segment_id)
        .unwrap();
    assert!(
        seg_ref.bitmap_fields.is_empty(),
        "SegmentRef should have no bitmap_fields when disabled"
    );

    // Query with filter still works (post-filter only)
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 10,
            "consistency": "eventual",
            "filter": {
                "op": "eq",
                "field": "status",
                "value": "active"
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(
        !results.is_empty(),
        "post-filter should still return results"
    );

    for r in results {
        let status = r["attributes"]["status"].as_str().unwrap();
        assert_eq!(status, "active");
    }

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 4: Low selectivity — filter matches exactly 1 vector
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_bitmap_low_selectivity() {
    let config = bitmap_test_config(true);
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        serde_json::json!({
            "dimensions": 16,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    // 100 vectors, filter matches priority==42 (exactly 1)
    let vectors = status_vectors("lowsel", 100, 16);
    let query_vec = vectors[42].values.clone();

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let result = compactor.compact(&ns).await.unwrap();
    assert!(result.segment_id.is_some());

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 10,
            "consistency": "eventual",
            "filter": {
                "op": "eq",
                "field": "priority",
                "value": 42
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert_eq!(
        results.len(),
        1,
        "should return exactly 1 result for priority=42"
    );
    assert_eq!(results[0]["attributes"]["priority"].as_i64().unwrap(), 42);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 5: Range filter end-to-end
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_bitmap_range_filter() {
    let config = bitmap_test_config(true);
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        serde_json::json!({
            "dimensions": 16,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    let vectors = status_vectors("range", 100, 16);
    let query_vec = vectors[0].values.clone();

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let result = compactor.compact(&ns).await.unwrap();
    assert!(result.segment_id.is_some());

    // Range: priority >= 10 AND priority <= 20 (11 vectors: 10,11,...,20)
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 20,
            "consistency": "eventual",
            "filter": {
                "op": "range",
                "field": "priority",
                "gte": 10,
                "lte": 20
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(
        !results.is_empty(),
        "should return results for range [10, 20]"
    );

    for r in results {
        let priority = r["attributes"]["priority"].as_i64().unwrap();
        assert!(
            (10..=20).contains(&priority),
            "priority {} should be in range [10, 20]",
            priority
        );
    }

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 6: Compound filter (And/Or)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_bitmap_compound_filter() {
    let config = bitmap_test_config(true);
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        serde_json::json!({
            "dimensions": 16,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    let vectors = status_vectors("compound", 100, 16);
    let query_vec = vectors[0].values.clone();

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let result = compactor.compact(&ns).await.unwrap();
    assert!(result.segment_id.is_some());

    // And(status=active, range(priority >= 10, priority <= 50))
    // active = even indices, range = 10..=50 → even indices in [10,50] = {10,12,...,50} = 21 vectors
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 30,
            "consistency": "eventual",
            "filter": {
                "op": "and",
                "filters": [
                    {
                        "op": "eq",
                        "field": "status",
                        "value": "active"
                    },
                    {
                        "op": "range",
                        "field": "priority",
                        "gte": 10,
                        "lte": 50
                    }
                ]
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(!results.is_empty(), "compound filter should return results");

    for r in results {
        let status = r["attributes"]["status"].as_str().unwrap();
        let priority = r["attributes"]["priority"].as_i64().unwrap();
        assert_eq!(status, "active");
        assert!(
            (10..=50).contains(&priority),
            "priority {} out of range",
            priority
        );
    }

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 7: Contains filter on list attribute
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_bitmap_list_contains() {
    let config = bitmap_test_config(true);
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        serde_json::json!({
            "dimensions": 16,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    // Ingest vectors with StringList tags: [alpha, common], [beta, common], [gamma]
    let vectors = tagged_vectors("contains", 90, 16);
    let query_vec = vectors[0].values.clone();

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let result = compactor.compact(&ns).await.unwrap();
    assert!(result.segment_id.is_some());

    // Contains("tags", "alpha") — should match every 3rd vector (indices 0, 3, 6, ...)
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": query_vec,
            "top_k": 50,
            "consistency": "eventual",
            "filter": {
                "op": "contains",
                "field": "tags",
                "value": "alpha"
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(!results.is_empty(), "Contains filter should return results");

    for r in results {
        let tags = r["attributes"]["tags"].as_array().unwrap();
        let tag_strs: Vec<&str> = tags.iter().map(|t| t.as_str().unwrap()).collect();
        assert!(
            tag_strs.contains(&"alpha"),
            "result should contain 'alpha' tag, got: {:?}",
            tag_strs
        );
    }

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
