mod common;

use common::harness::TestHarness;
use common::vectors::{random_vectors, simple_attributes, with_attributes};

use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig};
use zeppelin::index::ivf_flat::build::build_ivf_flat;
use zeppelin::query::execute_query;
use zeppelin::types::{AttributeValue, ConsistencyLevel, DistanceMetric, Filter, VectorEntry};
use zeppelin::wal::fragment::WalFragment;
use zeppelin::wal::manifest::{Manifest, SegmentRef};
use zeppelin::wal::{WalReader, WalWriter};

use common::assertions::*;

/// Create a Compactor with test-friendly settings.
fn test_compactor(store: &zeppelin::storage::ZeppelinStore) -> Compactor {
    let wal_reader = WalReader::new(store.clone());
    let compaction_config = CompactionConfig {
        max_wal_fragments_before_compact: 3,
        ..Default::default()
    };
    let indexing_config = IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 10,
        ..Default::default()
    };
    Compactor::new(
        store.clone(),
        wal_reader,
        compaction_config,
        indexing_config,
    )
}

#[tokio::test]
async fn test_compact_single_fragment() {
    let harness = TestHarness::new().await;
    let ns = harness.key("compact-single");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    // Create namespace manifest
    let manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();

    // Append 1 fragment with 50 vectors
    let vecs = random_vectors(50, 16);
    writer.append(&ns, vecs, vec![]).await.unwrap();

    // Compact
    let compactor = test_compactor(store);
    let result = compactor.compact(&ns).await.unwrap();

    assert_eq!(result.vectors_compacted, 50);
    assert_eq!(result.fragments_removed, 1);

    // Verify manifest state
    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    assert_eq!(manifest.segments.len(), 1);
    assert!(manifest.active_segment.is_some());
    assert!(manifest.compaction_watermark.is_some());
    assert!(manifest.fragments.is_empty());

    // Verify segment exists on S3
    let seg_id = &manifest.segments[0].id;
    let centroids_key = format!("{ns}/segments/{seg_id}/centroids.bin");
    assert_s3_object_exists(store, &centroids_key).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn test_compact_multiple_fragments() {
    let harness = TestHarness::new().await;
    let ns = harness.key("compact-multi");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    let manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();

    // Append 3 fragments with unique IDs
    let vecs1: Vec<VectorEntry> = (0..20)
        .map(|i| VectorEntry {
            id: format!("a_{i}"),
            values: random_vectors(1, 16)[0].values.clone(),
            attributes: None,
        })
        .collect();
    let vecs2: Vec<VectorEntry> = (0..30)
        .map(|i| VectorEntry {
            id: format!("b_{i}"),
            values: random_vectors(1, 16)[0].values.clone(),
            attributes: None,
        })
        .collect();
    let vecs3: Vec<VectorEntry> = (0..50)
        .map(|i| VectorEntry {
            id: format!("c_{i}"),
            values: random_vectors(1, 16)[0].values.clone(),
            attributes: None,
        })
        .collect();

    let f1 = writer.append(&ns, vecs1, vec![]).await.unwrap();
    let f2 = writer.append(&ns, vecs2, vec![]).await.unwrap();
    let f3 = writer.append(&ns, vecs3, vec![]).await.unwrap();

    // Record fragment keys
    let frag_keys: Vec<String> = [&f1, &f2, &f3]
        .iter()
        .map(|f| WalFragment::s3_key(&ns, &f.id))
        .collect();

    let compactor = test_compactor(store);
    let result = compactor.compact(&ns).await.unwrap();

    assert_eq!(result.vectors_compacted, 100);
    assert_eq!(result.fragments_removed, 3);

    // Deferred deletion: fragment keys should be in pending_deletes, not immediately removed
    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    for key in &frag_keys {
        assert!(
            manifest.pending_deletes.contains(key),
            "Fragment key {} should be in pending_deletes",
            key
        );
    }

    // Fragment files still exist on S3 until next compaction cycle
    for key in &frag_keys {
        assert_s3_object_exists(store, key).await;
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn test_compact_with_deletes() {
    let harness = TestHarness::new().await;
    let ns = harness.key("compact-del");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    let manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();

    // Append 10 vectors
    let vecs = random_vectors(10, 16);
    writer.append(&ns, vecs, vec![]).await.unwrap();

    // Delete 2 of them
    writer
        .append(&ns, vec![], vec!["vec_0".to_string(), "vec_1".to_string()])
        .await
        .unwrap();

    let compactor = test_compactor(store);
    let result = compactor.compact(&ns).await.unwrap();

    assert_eq!(result.vectors_compacted, 8);

    // Verify segment has 8 vectors
    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    assert_eq!(manifest.segments[0].vector_count, 8);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_compact_deduplication() {
    let harness = TestHarness::new().await;
    let ns = harness.key("compact-dedup");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    let manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();

    // Append vec id="dup" with values=[1,0,0,...,0]
    let mut v1 = vec![0.0f32; 16];
    v1[0] = 1.0;
    writer
        .append(
            &ns,
            vec![VectorEntry {
                id: "dup".to_string(),
                values: v1,
                attributes: None,
            }],
            vec![],
        )
        .await
        .unwrap();

    // Append vec id="dup" again with values=[0,1,0,...,0]
    let mut v2 = vec![0.0f32; 16];
    v2[1] = 1.0;
    writer
        .append(
            &ns,
            vec![VectorEntry {
                id: "dup".to_string(),
                values: v2.clone(),
                attributes: None,
            }],
            vec![],
        )
        .await
        .unwrap();

    let compactor = test_compactor(store);
    let result = compactor.compact(&ns).await.unwrap();

    // Should only have 1 vector (deduplicated)
    assert_eq!(result.vectors_compacted, 1);

    // Query with v2 should find "dup" at distance ~0
    let wal_reader = WalReader::new(store.clone());
    let query_result = execute_query(
        store,
        &wal_reader,
        &ns,
        &v2,
        1,
        4,
        None,
        ConsistencyLevel::Eventual,
        DistanceMetric::Euclidean,
        3,
        None,
    )
    .await
    .unwrap();

    assert!(!query_result.results.is_empty());
    assert_eq!(query_result.results[0].id, "dup");
    // Distance should be very small (close to 0)
    assert!(query_result.results[0].score < 0.01);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_compact_updates_manifest() {
    let harness = TestHarness::new().await;
    let ns = harness.key("compact-manifest");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    let manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();

    // Append 2 fragments
    let _f1 = writer
        .append(&ns, random_vectors(20, 16), vec![])
        .await
        .unwrap();
    let f2 = writer
        .append(&ns, random_vectors(30, 16), vec![])
        .await
        .unwrap();

    let compactor = test_compactor(store);
    compactor.compact(&ns).await.unwrap();

    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();

    assert_eq!(manifest.segments.len(), 1);
    assert!(manifest.active_segment.is_some());
    assert_eq!(
        manifest.compaction_watermark,
        Some(f2.id) // watermark should be the last fragment's ULID
    );
    assert!(manifest.fragments.is_empty());

    harness.cleanup().await;
}

#[tokio::test]
async fn test_compact_cleans_up_fragments() {
    let harness = TestHarness::new().await;
    let ns = harness.key("compact-cleanup");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    let manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();

    // Append 3 fragments, record their S3 keys
    let f1 = writer
        .append(&ns, random_vectors(10, 16), vec![])
        .await
        .unwrap();
    let f2 = writer
        .append(&ns, random_vectors(10, 16), vec![])
        .await
        .unwrap();
    let f3 = writer
        .append(&ns, random_vectors(10, 16), vec![])
        .await
        .unwrap();

    let keys: Vec<String> = [&f1, &f2, &f3]
        .iter()
        .map(|f| WalFragment::s3_key(&ns, &f.id))
        .collect();

    // Verify they exist before compaction
    for key in &keys {
        assert_s3_object_exists(store, key).await;
    }

    let compactor = test_compactor(store);
    compactor.compact(&ns).await.unwrap();

    // Deferred deletion: fragment keys should be in pending_deletes, not immediately removed
    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    for key in &keys {
        assert!(
            manifest.pending_deletes.contains(key),
            "Fragment key {} should be in pending_deletes",
            key
        );
    }

    // Fragment files still exist on S3 until next compaction cycle
    for key in &keys {
        assert_s3_object_exists(store, key).await;
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn test_compact_preserves_new_fragments() {
    let harness = TestHarness::new().await;
    let ns = harness.key("compact-preserve");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    let manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();

    // Append A, compact
    let frag_a = writer
        .append(&ns, random_vectors(20, 16), vec![])
        .await
        .unwrap();

    let compactor = test_compactor(store);
    compactor.compact(&ns).await.unwrap();

    // Append B (after compaction)
    let frag_b = writer
        .append(&ns, random_vectors(10, 16), vec![])
        .await
        .unwrap();

    // Fragment B should exist on S3
    let b_key = WalFragment::s3_key(&ns, &frag_b.id);
    assert_s3_object_exists(store, &b_key).await;

    // Manifest should have 1 fragment (B) and watermark == A.id
    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    assert_eq!(manifest.fragments.len(), 1);
    assert_eq!(manifest.fragments[0].id, frag_b.id);
    assert_eq!(manifest.compaction_watermark, Some(frag_a.id));

    harness.cleanup().await;
}

#[tokio::test]
async fn test_compact_with_existing_segment() {
    let harness = TestHarness::new().await;
    let ns = harness.key("compact-existing");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    // Build IVF-Flat with 50 vecs manually
    let initial_vecs = random_vectors(50, 16);
    let indexing_config = IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 10,
        ..Default::default()
    };
    let old_seg_id = "seg_old";
    build_ivf_flat(&initial_vecs, &indexing_config, store, &ns, old_seg_id)
        .await
        .unwrap();

    // Create manifest with that segment
    let mut manifest = Manifest::new();
    manifest.add_segment(SegmentRef {
        id: old_seg_id.to_string(),
        vector_count: 50,
        cluster_count: 4,
        quantization: Default::default(),
        hierarchical: false,
        bitmap_fields: Vec::new(),
    });
    manifest.write(store, &ns).await.unwrap();

    // Append 20 new vecs via WAL
    // Use different IDs to avoid collision with existing vec_0..vec_49
    let new_vecs: Vec<VectorEntry> = (0..20)
        .map(|i| VectorEntry {
            id: format!("new_vec_{i}"),
            values: random_vectors(1, 16)[0].values.clone(),
            attributes: None,
        })
        .collect();
    writer.append(&ns, new_vecs, vec![]).await.unwrap();

    let compactor = test_compactor(store);
    let result = compactor.compact(&ns).await.unwrap();

    // New segment should have 70 vectors (50 old + 20 new)
    assert_eq!(result.vectors_compacted, 70);

    // Deferred deletion: old segment keys should be in pending_deletes
    let old_centroids_key = format!("{ns}/segments/{old_seg_id}/centroids.bin");
    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    assert!(
        manifest
            .pending_deletes
            .iter()
            .any(|k| k.contains(old_seg_id)),
        "Old segment keys should be in pending_deletes"
    );
    // Old segment still exists on S3 until next compaction cycle
    assert_s3_object_exists(store, &old_centroids_key).await;

    // New segment should exist (reuse manifest from above)
    assert!(manifest.active_segment.is_some());
    let new_seg_id = manifest.active_segment.as_ref().unwrap();
    assert_ne!(new_seg_id, old_seg_id);
    let new_centroids_key = format!("{ns}/segments/{new_seg_id}/centroids.bin");
    assert_s3_object_exists(store, &new_centroids_key).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn test_query_after_compaction() {
    let harness = TestHarness::new().await;
    let ns = harness.key("compact-query");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());
    let wal_reader = WalReader::new(store.clone());

    let manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();

    // Append 100 vecs
    let vecs = random_vectors(100, 16);
    let query_vec = vecs[0].values.clone();
    writer.append(&ns, vecs, vec![]).await.unwrap();

    // Query before compaction (Strong)
    let pre_result = execute_query(
        store,
        &wal_reader,
        &ns,
        &query_vec,
        5,
        4,
        None,
        ConsistencyLevel::Strong,
        DistanceMetric::Cosine,
        3,
        None,
    )
    .await
    .unwrap();

    let pre_ids: Vec<String> = pre_result.results.iter().map(|r| r.id.clone()).collect();
    assert!(!pre_ids.is_empty());

    // Compact
    let compactor = test_compactor(store);
    compactor.compact(&ns).await.unwrap();

    // Query after compaction (Strong)
    let post_strong = execute_query(
        store,
        &wal_reader,
        &ns,
        &query_vec,
        5,
        4,
        None,
        ConsistencyLevel::Strong,
        DistanceMetric::Cosine,
        3,
        None,
    )
    .await
    .unwrap();

    // Query after compaction (Eventual)
    let post_eventual = execute_query(
        store,
        &wal_reader,
        &ns,
        &query_vec,
        5,
        4,
        None,
        ConsistencyLevel::Eventual,
        DistanceMetric::Cosine,
        3,
        None,
    )
    .await
    .unwrap();

    // Both should return results matching pre-compaction top results
    let post_strong_ids: Vec<String> = post_strong.results.iter().map(|r| r.id.clone()).collect();
    let post_eventual_ids: Vec<String> =
        post_eventual.results.iter().map(|r| r.id.clone()).collect();

    // The top result should be the same (vec_0, since we queried with its own vector)
    assert_eq!(post_strong_ids[0], pre_ids[0]);
    assert_eq!(post_eventual_ids[0], pre_ids[0]);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_compact_empty_namespace() {
    let harness = TestHarness::new().await;
    let ns = harness.key("compact-empty");
    let store = &harness.store;

    // Create manifest with no fragments
    let manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();

    let compactor = test_compactor(store);
    let result = compactor.compact(&ns).await.unwrap();

    assert_eq!(result.vectors_compacted, 0);
    assert_eq!(result.fragments_removed, 0);
    assert!(result.segment_id.is_none());

    // No segment should be created
    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    assert!(manifest.segments.is_empty());
    assert!(manifest.active_segment.is_none());

    harness.cleanup().await;
}

#[tokio::test]
async fn test_compact_trigger_by_fragment_count() {
    let harness = TestHarness::new().await;
    let ns = harness.key("compact-trigger");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    let manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();

    let compactor = test_compactor(store); // max_wal_fragments_before_compact: 3

    // Append 2 fragments
    writer
        .append(&ns, random_vectors(10, 16), vec![])
        .await
        .unwrap();
    writer
        .append(&ns, random_vectors(10, 16), vec![])
        .await
        .unwrap();

    // Should not compact yet (2 < 3)
    assert!(!compactor.should_compact(&ns).await.unwrap());

    // Append 1 more (now 3 >= 3)
    writer
        .append(&ns, random_vectors(10, 16), vec![])
        .await
        .unwrap();

    assert!(compactor.should_compact(&ns).await.unwrap());

    harness.cleanup().await;
}

#[tokio::test]
async fn test_compact_attributes_preserved() {
    let harness = TestHarness::new().await;
    let ns = harness.key("compact-attrs");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());
    let wal_reader = WalReader::new(store.clone());

    let manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();

    // Append vecs with simple_attributes
    let vecs = with_attributes(random_vectors(30, 16), simple_attributes);
    let query_vec = vecs[0].values.clone(); // category "a"
    writer.append(&ns, vecs, vec![]).await.unwrap();

    // Compact
    let compactor = test_compactor(store);
    compactor.compact(&ns).await.unwrap();

    // Query with filter for category=a (Eventual uses segment only)
    let filter = Filter::Eq {
        field: "category".to_string(),
        value: AttributeValue::String("a".to_string()),
    };
    let result = execute_query(
        store,
        &wal_reader,
        &ns,
        &query_vec,
        30,
        4,
        Some(&filter),
        ConsistencyLevel::Eventual,
        DistanceMetric::Cosine,
        3,
        None,
    )
    .await
    .unwrap();

    // All returned results should have category=a
    assert!(!result.results.is_empty());
    for r in &result.results {
        let attrs = r.attributes.as_ref().expect("attributes should be present");
        assert_eq!(
            attrs.get("category"),
            Some(&AttributeValue::String("a".to_string())),
            "vector {} should have category=a",
            r.id
        );
    }

    harness.cleanup().await;
}
