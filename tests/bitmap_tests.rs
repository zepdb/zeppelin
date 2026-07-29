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
    Config {
        compaction: CompactionConfig {
            max_wal_fragments_before_compact: 1,
            ..Default::default()
        },
        indexing: IndexingConfig {
            default_num_centroids: 4,
            kmeans_max_iterations: 10,
            bitmap_index: bitmap_enabled,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn bitmap_test_config_hierarchical(bitmap_enabled: bool) -> Config {
    let mut config = bitmap_test_config(bitmap_enabled);
    config.indexing.hierarchical = true;
    config.indexing.leaf_size = Some(20);
    // Two-bit requires a flat IVF index; hierarchical fixtures stay on SQ8.
    config.indexing.quantization = zeppelin::index::quantization::QuantizationType::Scalar;
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

// ---------------------------------------------------------------------------
// Test 8: the prefilter must actually engage, and its absence must be caching
//
// Regression guard for the 2026-07-24 perf-contract finding, where
// `filtered_query_bitmap` dropped from 7 bitmap GETs to 0 on every repeat.
// Fewer GETs on a bitmap-filtered query is only a win if the bitmap is still
// being consulted; the alternative is that the prefilter silently stopped
// engaging and the query is scanning clusters it used to skip. The tests above
// prove bitmap artifacts are *written* and that results are correct. Neither
// property distinguishes those two worlds. This one does.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bitmap_prefilter_engages_and_is_only_elided_by_cache() {
    use common::counting::{counting_store, ArtifactClass};
    use common::harness::TestHarness;
    use common::server::{client_with_bearer, start_test_server_on_store_with_config};

    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let (base_url, _cache, _dir, admin_bearer) = start_test_server_on_store_with_config(
        &harness,
        store.clone(),
        None,
        bitmap_test_config(true),
    )
    .await;
    let client = client_with_bearer(&admin_bearer);
    let ns = create_ns_api_with(
        &client,
        &base_url,
        serde_json::json!({ "dimensions": 16, "distance_metric": "euclidean" }),
    )
    .await;

    let vectors = status_vectors("bmengage", 120, 16);
    let query_vec = vectors[0].values.clone();
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Drive compaction directly rather than through the HTTP endpoint, which
    // is accepted-and-asynchronous. The compactor shares the counting store so
    // the bitmap sidecars it writes are the ones the query path later reads.
    let compactor = zeppelin::compaction::Compactor::new(
        store.clone(),
        zeppelin::wal::WalReader::new(store.clone()),
        CompactionConfig {
            max_wal_fragments_before_compact: 1,
            ..Default::default()
        },
        bitmap_test_config(true).indexing,
        common::default_gc_upload_window(),
    );
    let compacted = compactor.compact(&ns).await.unwrap();
    assert!(
        compacted.segment_id.is_some(),
        "compaction must publish a segment"
    );

    // The segment must actually advertise bitmaps, or every assertion below
    // would pass vacuously against a namespace that never had any.
    let manifest = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    let active = manifest.active_segment.clone().expect("active segment");
    let segment = manifest
        .segments
        .iter()
        .find(|segment| segment.id == active)
        .expect("active segment ref");
    assert!(
        !segment.bitmap_fields.is_empty(),
        "fixture must advertise bitmap fields, otherwise prefilter engagement is untestable"
    );

    let filtered = serde_json::json!({
        "vector": query_vec,
        "top_k": 10,
        "filter": { "op": "eq", "field": "status", "value": "active" }
    });

    // 1. First filtered query against a cold server cache: an engaged
    //    prefilter has nowhere to read a bitmap from except object storage.
    counter.reset();
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&filtered)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let cold_body: serde_json::Value = resp.json().await.unwrap();
    let cold_hits = cold_body["results"].as_array().unwrap().len();
    let cold_bitmap_gets = counter.gets_for(ArtifactClass::Bitmap);
    assert!(
        cold_bitmap_gets > 0,
        "bitmap prefilter did not engage: a filtered query over a segment advertising \
         bitmap_fields performed {cold_bitmap_gets} bitmap GETs against a cold cache. \
         Either has_bitmaps is false, try_bitmap_prefilter is being skipped, or the \
         bitmap key no longer resolves."
    );
    assert!(cold_hits > 0, "filtered query must return matching rows");

    // 2. The identical query, now warm. Dropping to zero bitmap GETs is
    //    benign *only* because the cache serves them. If this ever fails
    //    while (1) passes, the artifact cache key is unstable across queries.
    counter.reset();
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&filtered)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let warm_body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        counter.gets_for(ArtifactClass::Bitmap),
        0,
        "a warm cache must serve bitmaps without an object-store GET"
    );
    assert_eq!(
        warm_body["results"].as_array().unwrap().len(),
        cold_hits,
        "cached and uncached filtered queries must agree on result count"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
