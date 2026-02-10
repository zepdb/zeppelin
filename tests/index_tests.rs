mod common;

use common::assertions::{assert_recall_at_k, assert_s3_object_exists};
use common::harness::TestHarness;
use common::vectors::{clustered_vectors, simple_attributes, with_attributes};

use zeppelin::config::IndexingConfig;
use zeppelin::error::ZeppelinError;
use zeppelin::index::distance::{
    compute_distance, cosine_distance, dot_product_distance, euclidean_distance,
};
use zeppelin::index::filter::evaluate_filter;
use zeppelin::index::ivf_flat::build::centroids_key;
use zeppelin::index::traits::VectorIndex;
use zeppelin::index::IvfFlatIndex;
use zeppelin::types::{AttributeValue, DistanceMetric, Filter};

use std::collections::HashMap;

// ─── Distance tests ───

#[test]
fn test_cosine_known_values() {
    let a = vec![1.0, 0.0, 0.0];
    let b = vec![0.0, 1.0, 0.0];
    let d = cosine_distance(&a, &b);
    assert!((d - 1.0).abs() < 1e-5);

    let d2 = cosine_distance(&a, &a);
    assert!(d2.abs() < 1e-5);
}

#[test]
fn test_euclidean_known_values() {
    let a = vec![1.0, 2.0, 3.0];
    let b = vec![4.0, 5.0, 6.0];
    let d = euclidean_distance(&a, &b);
    assert!((d - 27.0).abs() < 1e-4);
}

#[test]
fn test_dot_product_known_values() {
    let a = vec![1.0, 2.0, 3.0];
    let b = vec![4.0, 5.0, 6.0];
    let d = dot_product_distance(&a, &b);
    assert!((d - (-32.0)).abs() < 1e-4);
}

// ─── Filter tests ───

#[test]
fn test_filter_eq() {
    let mut attrs = HashMap::new();
    attrs.insert(
        "color".to_string(),
        AttributeValue::String("red".to_string()),
    );

    let filter = Filter::Eq {
        field: "color".to_string(),
        value: AttributeValue::String("red".to_string()),
    };
    assert!(evaluate_filter(&filter, &attrs));

    let filter_miss = Filter::Eq {
        field: "color".to_string(),
        value: AttributeValue::String("blue".to_string()),
    };
    assert!(!evaluate_filter(&filter_miss, &attrs));
}

#[test]
fn test_filter_range() {
    let mut attrs = HashMap::new();
    attrs.insert("score".to_string(), AttributeValue::Integer(50));

    let filter = Filter::Range {
        field: "score".to_string(),
        gte: Some(10.0),
        lte: Some(100.0),
        gt: None,
        lt: None,
    };
    assert!(evaluate_filter(&filter, &attrs));

    let filter_fail = Filter::Range {
        field: "score".to_string(),
        gte: Some(60.0),
        lte: None,
        gt: None,
        lt: None,
    };
    assert!(!evaluate_filter(&filter_fail, &attrs));
}

#[test]
fn test_filter_in() {
    let mut attrs = HashMap::new();
    attrs.insert(
        "category".to_string(),
        AttributeValue::String("a".to_string()),
    );

    let filter = Filter::In {
        field: "category".to_string(),
        values: vec![
            AttributeValue::String("a".to_string()),
            AttributeValue::String("b".to_string()),
        ],
    };
    assert!(evaluate_filter(&filter, &attrs));
}

#[test]
fn test_filter_and() {
    let mut attrs = HashMap::new();
    attrs.insert(
        "category".to_string(),
        AttributeValue::String("a".to_string()),
    );
    attrs.insert("score".to_string(), AttributeValue::Integer(50));

    let filter = Filter::And {
        filters: vec![
            Filter::Eq {
                field: "category".to_string(),
                value: AttributeValue::String("a".to_string()),
            },
            Filter::Range {
                field: "score".to_string(),
                gte: Some(40.0),
                lte: Some(60.0),
                gt: None,
                lt: None,
            },
        ],
    };
    assert!(evaluate_filter(&filter, &attrs));
}

// ─── IVF-Flat integration tests ───

#[tokio::test]
async fn test_ivf_flat_build() {
    let harness = TestHarness::new().await;
    let ns = harness.key("idx-build");

    let (vectors, _centroids) = clustered_vectors(4, 50, 32, 0.1);
    let vectors = with_attributes(vectors, simple_attributes);
    let config = IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 20,
        kmeans_convergence_epsilon: 1e-4,
        ..Default::default()
    };

    let segment_id = "seg_001";
    let index = IvfFlatIndex::build(&vectors, &config, &harness.store, &ns, segment_id)
        .await
        .unwrap();

    assert_eq!(index.vector_count(), 200);
    assert_eq!(index.dimension(), 32);
    assert_eq!(index.num_clusters(), 4);

    let ckey = centroids_key(&ns, segment_id);
    assert_s3_object_exists(&harness.store, &ckey).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn test_ivf_flat_search_recall() {
    let harness = TestHarness::new().await;
    let ns = harness.key("idx-recall");

    let (vectors, centroids) = clustered_vectors(4, 50, 32, 0.05);
    let config = IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 25,
        kmeans_convergence_epsilon: 1e-4,
        ..Default::default()
    };

    let segment_id = "seg_recall";
    let index = IvfFlatIndex::build(&vectors, &config, &harness.store, &ns, segment_id)
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

    assert!(!results.is_empty());

    // Build ground truth: brute-force nearest to query
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

    assert_recall_at_k(&results, &ground_truth, 10, 0.7);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_ivf_flat_load_from_s3() {
    let harness = TestHarness::new().await;
    let ns = harness.key("idx-load");

    let (vectors, _) = clustered_vectors(3, 30, 16, 0.1);
    let config = IndexingConfig {
        default_num_centroids: 3,
        kmeans_max_iterations: 20,
        kmeans_convergence_epsilon: 1e-4,
        ..Default::default()
    };

    let segment_id = "seg_load";
    let built = IvfFlatIndex::build(&vectors, &config, &harness.store, &ns, segment_id)
        .await
        .unwrap();

    let loaded = IvfFlatIndex::load(&harness.store, &ns, segment_id)
        .await
        .unwrap();

    assert_eq!(loaded.dimension(), built.dimension());
    assert_eq!(loaded.num_clusters(), built.num_clusters());
    assert_eq!(loaded.vector_count(), built.vector_count());

    harness.cleanup().await;
}

#[tokio::test]
async fn test_ivf_flat_search_with_filter() {
    let harness = TestHarness::new().await;
    let ns = harness.key("idx-filter");

    let (vectors, _) = clustered_vectors(3, 30, 16, 0.1);
    let vectors = with_attributes(vectors, simple_attributes);
    let config = IndexingConfig {
        default_num_centroids: 3,
        kmeans_max_iterations: 20,
        kmeans_convergence_epsilon: 1e-4,
        ..Default::default()
    };

    let segment_id = "seg_filter";
    let index = IvfFlatIndex::build(&vectors, &config, &harness.store, &ns, segment_id)
        .await
        .unwrap();

    let query = vec![0.0f32; 16];
    let filter = Filter::Eq {
        field: "category".to_string(),
        value: AttributeValue::String("a".to_string()),
    };

    let results = index
        .search(
            &query,
            10,
            3,
            Some(&filter),
            DistanceMetric::Euclidean,
            &harness.store,
        )
        .await
        .unwrap();

    assert!(!results.is_empty());

    harness.cleanup().await;
}

#[tokio::test]
async fn test_ivf_flat_dimension_mismatch() {
    let harness = TestHarness::new().await;
    let ns = harness.key("idx-dim");

    let (vectors, _) = clustered_vectors(2, 20, 16, 0.1);
    let config = IndexingConfig {
        default_num_centroids: 2,
        kmeans_max_iterations: 20,
        kmeans_convergence_epsilon: 1e-4,
        ..Default::default()
    };

    let segment_id = "seg_dim";
    let index = IvfFlatIndex::build(&vectors, &config, &harness.store, &ns, segment_id)
        .await
        .unwrap();

    let query = vec![0.0f32; 8]; // dim=8 vs index dim=16
    let result = index
        .search(
            &query,
            10,
            2,
            None,
            DistanceMetric::Euclidean,
            &harness.store,
        )
        .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        ZeppelinError::DimensionMismatch { expected, actual } => {
            assert_eq!(expected, 16);
            assert_eq!(actual, 8);
        }
        other => panic!("expected DimensionMismatch, got: {other}"),
    }

    harness.cleanup().await;
}
