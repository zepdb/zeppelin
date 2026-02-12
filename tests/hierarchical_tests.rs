mod common;

use common::assertions::{assert_recall_at_k, assert_s3_object_exists};
use common::harness::TestHarness;
use common::vectors::{clustered_vectors, simple_attributes, with_attributes};

use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig};
use zeppelin::error::ZeppelinError;
use zeppelin::index::distance::compute_distance;
use zeppelin::index::hierarchical::build::{build_hierarchical, load_hierarchical};
use zeppelin::index::hierarchical::tree_meta_key;
use zeppelin::index::traits::VectorIndex;
use zeppelin::index::HierarchicalIndex;
use zeppelin::query::execute_query;
use zeppelin::types::{AttributeValue, ConsistencyLevel, DistanceMetric, Filter, VectorEntry};
use zeppelin::wal::manifest::Manifest;
use zeppelin::wal::{WalReader, WalWriter};

/// Default config for hierarchical tests: small leaf_size to force multi-level trees.
fn hierarchical_test_config() -> IndexingConfig {
    IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 10,
        hierarchical: true,
        beam_width: 4,
        leaf_size: Some(10),
        ..Default::default()
    }
}

// ─── Test 1: Build hierarchical basic ───

#[tokio::test]
async fn test_build_hierarchical_basic() {
    let harness = TestHarness::new().await;
    let ns = harness.key("h-build-basic");

    let (vectors, _centroids) = clustered_vectors(4, 25, 32, 0.1);
    let config = hierarchical_test_config();

    let segment_id = "seg_h_basic";
    let index = HierarchicalIndex::build(&vectors, &config, &harness.store, &ns, segment_id)
        .await
        .unwrap();

    // Verify basic metadata.
    assert_eq!(index.vector_count(), 100);
    assert_eq!(index.dimension(), 32);

    // With leaf_size=10 (clamped to max(10, branching_factor*2) = 10 since bf=4),
    // 100 vectors should produce a multi-level tree.
    let meta_key = tree_meta_key(&ns, segment_id);
    assert_s3_object_exists(&harness.store, &meta_key).await;

    // Load the tree meta to verify num_levels > 1.
    let meta_data = harness.store.get(&meta_key).await.unwrap();
    let tree_meta: serde_json::Value = serde_json::from_slice(&meta_data).unwrap();
    let num_levels = tree_meta["num_levels"].as_u64().unwrap();
    assert!(
        num_levels > 1,
        "expected multi-level tree, got num_levels={num_levels}"
    );

    // Verify leaf clusters exist.
    let num_leaf_clusters = tree_meta["num_leaf_clusters"].as_u64().unwrap();
    assert!(
        num_leaf_clusters > 1,
        "expected multiple leaf clusters, got {num_leaf_clusters}"
    );

    harness.cleanup().await;
}

// ─── Test 2: Build hierarchical single leaf ───

#[tokio::test]
async fn test_build_hierarchical_single_leaf() {
    let harness = TestHarness::new().await;
    let ns = harness.key("h-build-single-leaf");

    // 8 vectors with large leaf_size → all fit in one leaf.
    let (vectors, _) = clustered_vectors(2, 4, 16, 0.1);
    let config = IndexingConfig {
        default_num_centroids: 2,
        kmeans_max_iterations: 10,
        hierarchical: true,
        beam_width: 2,
        leaf_size: Some(100), // large enough that all 8 vectors fit
        ..Default::default()
    };

    let segment_id = "seg_h_single";
    let index = HierarchicalIndex::build(&vectors, &config, &harness.store, &ns, segment_id)
        .await
        .unwrap();

    assert_eq!(index.vector_count(), 8);
    assert_eq!(index.dimension(), 16);

    // Verify tree_meta exists (single-leaf root wrapping).
    let meta_key = tree_meta_key(&ns, segment_id);
    assert_s3_object_exists(&harness.store, &meta_key).await;

    let meta_data = harness.store.get(&meta_key).await.unwrap();
    let tree_meta: serde_json::Value = serde_json::from_slice(&meta_data).unwrap();
    // Single leaf: num_levels should be 1.
    let num_levels = tree_meta["num_levels"].as_u64().unwrap();
    assert_eq!(
        num_levels, 1,
        "expected single-level tree for small dataset"
    );

    harness.cleanup().await;
}

// ─── Test 3: Build hierarchical errors ───

#[tokio::test]
async fn test_build_hierarchical_errors() {
    let harness = TestHarness::new().await;
    let ns = harness.key("h-build-errors");
    let config = hierarchical_test_config();

    // Empty vectors → error.
    let result = build_hierarchical(&[], &config, &harness.store, &ns, "seg_err1").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        ZeppelinError::Index(msg) => {
            assert!(msg.contains("empty"), "expected empty error, got: {msg}");
        }
        other => panic!("expected Index error, got: {other}"),
    }

    // Zero-dimension vectors → error.
    let zero_dim_vecs = vec![VectorEntry {
        id: "z0".into(),
        values: vec![],
        attributes: None,
    }];
    let result = build_hierarchical(&zero_dim_vecs, &config, &harness.store, &ns, "seg_err2").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        ZeppelinError::Index(msg) => {
            assert!(
                msg.contains("dimension"),
                "expected dimension error, got: {msg}"
            );
        }
        other => panic!("expected Index error, got: {other}"),
    }

    // Mismatched dimensions → error.
    let mixed_dim_vecs = vec![
        VectorEntry {
            id: "m0".into(),
            values: vec![1.0, 2.0, 3.0],
            attributes: None,
        },
        VectorEntry {
            id: "m1".into(),
            values: vec![1.0, 2.0],
            attributes: None,
        },
    ];
    let result =
        build_hierarchical(&mixed_dim_vecs, &config, &harness.store, &ns, "seg_err3").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        ZeppelinError::DimensionMismatch { expected, actual } => {
            assert_eq!(expected, 3);
            assert_eq!(actual, 2);
        }
        other => panic!("expected DimensionMismatch, got: {other}"),
    }

    harness.cleanup().await;
}

// ─── Test 4: Search hierarchical recall ───

#[tokio::test]
async fn test_search_hierarchical_recall() {
    let harness = TestHarness::new().await;
    let ns = harness.key("h-search-recall");

    let (vectors, centroids) = clustered_vectors(4, 50, 32, 0.05);
    let config = hierarchical_test_config();

    let segment_id = "seg_h_recall";
    let index = HierarchicalIndex::build(&vectors, &config, &harness.store, &ns, segment_id)
        .await
        .unwrap();

    // Search with a known centroid as query.
    let query = &centroids[0];
    let results = index
        .search(
            query,
            10,
            4, // beam_width
            None,
            DistanceMetric::Euclidean,
            &harness.store,
        )
        .await
        .unwrap();

    assert!(!results.is_empty(), "expected search results, got none");

    // Build brute-force ground truth.
    let mut distances: Vec<(&str, f32)> = vectors
        .iter()
        .map(|v| {
            (
                v.id.as_str(),
                compute_distance(query, &v.values, DistanceMetric::Euclidean),
            )
        })
        .collect();
    distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    let ground_truth: Vec<&str> = distances.iter().take(10).map(|(id, _)| *id).collect();

    assert_recall_at_k(&results, &ground_truth, 10, 0.5);

    harness.cleanup().await;
}

// ─── Test 5: Search hierarchical with filter ───

#[tokio::test]
async fn test_search_hierarchical_with_filter() {
    let harness = TestHarness::new().await;
    let ns = harness.key("h-search-filter");

    let (vectors, _) = clustered_vectors(4, 50, 32, 0.1);
    let vectors = with_attributes(vectors, simple_attributes);
    let config = hierarchical_test_config();

    let segment_id = "seg_h_filter";
    let index = HierarchicalIndex::build(&vectors, &config, &harness.store, &ns, segment_id)
        .await
        .unwrap();

    let query = vec![0.0f32; 32];
    let filter = Filter::Eq {
        field: "category".to_string(),
        value: AttributeValue::String("a".to_string()),
    };

    let results = index
        .search(
            &query,
            10,
            4,
            Some(&filter),
            DistanceMetric::Euclidean,
            &harness.store,
        )
        .await
        .unwrap();

    assert!(!results.is_empty(), "expected filtered results, got none");

    // Verify all results satisfy filter.
    for r in &results {
        let attrs = r
            .attributes
            .as_ref()
            .expect("result should have attributes");
        let cat = attrs.get("category").expect("result should have category");
        assert_eq!(
            cat,
            &AttributeValue::String("a".to_string()),
            "result {} has wrong category: {:?}",
            r.id,
            cat
        );
    }

    harness.cleanup().await;
}

// ─── Test 6: Search hierarchical SQ8 ───

#[tokio::test]
async fn test_search_hierarchical_sq8() {
    let harness = TestHarness::new().await;
    let ns = harness.key("h-search-sq8");

    let (vectors, centroids) = clustered_vectors(4, 50, 32, 0.05);
    let config = IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 10,
        hierarchical: true,
        beam_width: 4,
        leaf_size: Some(10),
        quantization: zeppelin::index::quantization::QuantizationType::Scalar,
        ..Default::default()
    };

    let segment_id = "seg_h_sq8";
    let index = HierarchicalIndex::build(&vectors, &config, &harness.store, &ns, segment_id)
        .await
        .unwrap();

    let query = &centroids[0];
    let results = index
        .search(
            query,
            10,
            4,
            None,
            DistanceMetric::Euclidean,
            &harness.store,
        )
        .await
        .unwrap();

    assert!(!results.is_empty(), "expected SQ8 search results, got none");

    // SQ8 should return reasonable results (at least some overlap with true neighbors).
    let mut distances: Vec<(&str, f32)> = vectors
        .iter()
        .map(|v| {
            (
                v.id.as_str(),
                compute_distance(query, &v.values, DistanceMetric::Euclidean),
            )
        })
        .collect();
    distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    let ground_truth: Vec<&str> = distances.iter().take(10).map(|(id, _)| *id).collect();

    // SQ8 may have lower recall than flat; just verify it's non-trivial.
    let recall = common::assertions::recall_at_k(&results, &ground_truth, 10);
    assert!(
        recall > 0.0,
        "SQ8 recall should be non-trivial, got {recall}"
    );

    harness.cleanup().await;
}

// ─── Test 7: Search hierarchical PQ ───

#[tokio::test]
async fn test_search_hierarchical_pq() {
    let harness = TestHarness::new().await;
    let ns = harness.key("h-search-pq");

    // PQ requires dim divisible by pq_m. Use dim=16, pq_m=4.
    let (vectors, centroids) = clustered_vectors(4, 50, 16, 0.05);
    let config = IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 10,
        hierarchical: true,
        beam_width: 4,
        leaf_size: Some(10),
        quantization: zeppelin::index::quantization::QuantizationType::Product,
        pq_m: 4,
        ..Default::default()
    };

    let segment_id = "seg_h_pq";
    let index = HierarchicalIndex::build(&vectors, &config, &harness.store, &ns, segment_id)
        .await
        .unwrap();

    let query = &centroids[0];
    let results = index
        .search(
            query,
            10,
            4,
            None,
            DistanceMetric::Euclidean,
            &harness.store,
        )
        .await
        .unwrap();

    assert!(!results.is_empty(), "expected PQ search results, got none");

    // Verify PQ produces at least some correct results.
    let mut distances: Vec<(&str, f32)> = vectors
        .iter()
        .map(|v| {
            (
                v.id.as_str(),
                compute_distance(query, &v.values, DistanceMetric::Euclidean),
            )
        })
        .collect();
    distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    let ground_truth: Vec<&str> = distances.iter().take(10).map(|(id, _)| *id).collect();

    let recall = common::assertions::recall_at_k(&results, &ground_truth, 10);
    assert!(
        recall > 0.0,
        "PQ recall should be non-trivial, got {recall}"
    );

    harness.cleanup().await;
}

// ─── Test 8: Compact hierarchical ───

#[tokio::test]
async fn test_compact_hierarchical() {
    let harness = TestHarness::new().await;
    let ns = harness.key("h-compact");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    // Create namespace manifest.
    let manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();

    // Append 2 WAL fragments with unique ID prefixes.
    // Use clustered_vectors for proper diversity (avoids degenerate k-means).
    let (all_vecs, _) = clustered_vectors(4, 25, 16, 0.1);
    let frag1_vecs: Vec<VectorEntry> = all_vecs[..50]
        .iter()
        .enumerate()
        .map(|(i, v)| VectorEntry {
            id: format!("frag1_{i}"),
            values: v.values.clone(),
            attributes: None,
        })
        .collect();
    let frag2_vecs: Vec<VectorEntry> = all_vecs[50..]
        .iter()
        .enumerate()
        .map(|(i, v)| VectorEntry {
            id: format!("frag2_{i}"),
            values: v.values.clone(),
            attributes: None,
        })
        .collect();

    writer.append(&ns, frag1_vecs, vec![]).await.unwrap();
    writer.append(&ns, frag2_vecs, vec![]).await.unwrap();

    // Create compactor with hierarchical config.
    let wal_reader = WalReader::new(store.clone());
    let compaction_config = CompactionConfig {
        max_wal_fragments_before_compact: 3,
        ..Default::default()
    };
    let indexing_config = IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 10,
        hierarchical: true,
        beam_width: 4,
        leaf_size: Some(10),
        ..Default::default()
    };
    let compactor = Compactor::new(
        store.clone(),
        wal_reader,
        compaction_config,
        indexing_config,
    );

    let result = compactor.compact(&ns).await.unwrap();
    assert_eq!(result.vectors_compacted, 100);

    // Verify manifest has a segment with hierarchical=true.
    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    assert_eq!(manifest.segments.len(), 1);
    let seg = &manifest.segments[0];
    assert!(seg.hierarchical, "segment should be hierarchical");

    // Verify tree_meta.json exists on S3.
    let meta_key = tree_meta_key(&ns, &seg.id);
    assert_s3_object_exists(store, &meta_key).await;

    harness.cleanup().await;
}

// ─── Test 9: Query hierarchical detection ───

#[tokio::test]
async fn test_query_hierarchical_detection() {
    let harness = TestHarness::new().await;
    let ns = harness.key("h-query-detect");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    // Create namespace manifest.
    let manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();

    // Append vectors.
    let (vectors, centroids) = clustered_vectors(4, 25, 16, 0.05);
    writer.append(&ns, vectors.clone(), vec![]).await.unwrap();

    // Compact with hierarchical.
    let wal_reader = WalReader::new(store.clone());
    let compaction_config = CompactionConfig {
        max_wal_fragments_before_compact: 3,
        ..Default::default()
    };
    let indexing_config = IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 10,
        hierarchical: true,
        beam_width: 4,
        leaf_size: Some(10),
        ..Default::default()
    };
    let compactor = Compactor::new(
        store.clone(),
        wal_reader,
        compaction_config,
        indexing_config,
    );
    compactor.compact(&ns).await.unwrap();

    // Execute query through the query.rs path (detects hierarchical via tree_meta.json probe).
    let query_reader = WalReader::new(store.clone());
    let query = &centroids[0];
    let response = execute_query(
        store,
        &query_reader,
        &ns,
        query,
        10,
        4,                          // nprobe / beam_width
        None,                       // no filter
        ConsistencyLevel::Eventual, // skip WAL scan, just segment search
        DistanceMetric::Euclidean,
        3,    // oversample_factor
        None, // no cache
    )
    .await
    .unwrap();

    assert!(
        !response.results.is_empty(),
        "query through hierarchical detection should return results"
    );
    assert_eq!(response.scanned_segments, 1);

    harness.cleanup().await;
}

// ─── Test 10: Load hierarchical index ───

#[tokio::test]
async fn test_load_hierarchical_index() {
    let harness = TestHarness::new().await;
    let ns = harness.key("h-load");

    let (vectors, centroids) = clustered_vectors(4, 25, 32, 0.1);
    let config = hierarchical_test_config();

    let segment_id = "seg_h_load";
    let built = HierarchicalIndex::build(&vectors, &config, &harness.store, &ns, segment_id)
        .await
        .unwrap();

    // Load from S3.
    let loaded = load_hierarchical(&harness.store, &ns, segment_id)
        .await
        .unwrap();

    assert_eq!(loaded.dimension(), built.dimension());
    assert_eq!(loaded.vector_count(), built.vector_count());
    assert_eq!(loaded.num_leaf_clusters(), built.num_leaf_clusters());

    // Search the loaded index to verify it works end-to-end.
    let query = &centroids[0];
    let results = loaded
        .search(
            query,
            10,
            4,
            None,
            DistanceMetric::Euclidean,
            &harness.store,
        )
        .await
        .unwrap();

    assert!(
        !results.is_empty(),
        "loaded index should return search results"
    );

    harness.cleanup().await;
}
