mod common;

use std::collections::HashMap;

use chrono::Utc;
use common::counting::{counting_store, ArtifactClass};
use common::fault_injection::fail_put_once_matching;
use common::harness::TestHarness;
use common::server::scoped_test_security_store;
use common::vectors::{random_vectors, simple_attributes, with_attributes};

use zeppelin::cache::manifest_cache::ManifestCache;
use zeppelin::cache::DiskCache;
use zeppelin::compaction::background::{
    compact_namespace_under_lease_with_lifecycle, evaluate_compaction_trigger, CompactionLifecycle,
    CompactionLoopOptions,
};
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig};
use zeppelin::index::ivf_flat::build::build_ivf_flat;
use zeppelin::namespace::manager::{CompactionHealth, NamespaceMetadata, NamespaceState};
use zeppelin::query::{execute_query, QueryParams};
use zeppelin::types::{
    AttributeValue, ConsistencyLevel, DistanceMetric, Filter, IndexType, VectorEntry,
};
use zeppelin::wal::fragment::WalFragment;
use zeppelin::wal::manifest::{Manifest, SegmentRef};
use zeppelin::wal::{FragmentCachePolicy, LeaseManager, WalReader, WalWriter};

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
        common::default_gc_upload_window(),
    )
}

fn normalize_segment_descriptor(mut segment: SegmentRef) -> SegmentRef {
    fn normalize_key(key: &mut String) {
        *key = key
            .rsplit('/')
            .next()
            .expect("segment artifact key must have a filename")
            .to_string();
    }

    let segment_id = std::mem::replace(&mut segment.id, "<segment>".to_string());
    for owner in &mut segment.cluster_owners {
        if owner == &segment_id {
            *owner = "<segment>".to_string();
        }
    }
    if let Some(sketch) = &mut segment.sketch {
        normalize_key(&mut sketch.key);
    }
    for object in &mut segment.cluster_objects {
        normalize_key(&mut object.key);
    }
    if let Some(bootstrap) = &mut segment.bootstrap {
        normalize_key(&mut bootstrap.key);
    }
    if let Some(membership) = &mut segment.membership {
        normalize_key(&mut membership.key);
    }
    segment
}

async fn cluster_checksums(
    store: &zeppelin::storage::ZeppelinStore,
    segment: &SegmentRef,
) -> Vec<(Vec<usize>, u64)> {
    let mut checksums = Vec::with_capacity(segment.cluster_objects.len());
    for object in &segment.cluster_objects {
        let bytes = store
            .get(&object.key)
            .await
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", object.key));
        checksums.push((object.clusters.clone(), xxhash_rust::xxh3::xxh3_64(&bytes)));
    }
    checksums.sort_by(|left, right| left.0.cmp(&right.0));
    checksums
}

#[tokio::test]
async fn test_compaction_fragment_cache_is_read_only_and_output_deterministic() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let compaction_lifecycle = CompactionLifecycle::new();
    let mut outputs = Vec::new();

    for (label, warm_fragments) in [("cold", 0usize), ("partial", 2), ("warm", 4)] {
        let namespace = harness.key(&format!("compaction-fragment-cache-{label}"));
        Manifest::new().write(&store, &namespace).await.unwrap();
        common::write_active_namespace_metadata(&store, &namespace, 16, DistanceMetric::Euclidean)
            .await;

        let writer = WalWriter::new(store.clone());
        let vectors = random_vectors(24, 16);
        for chunk in vectors.chunks(6) {
            writer
                .append(&namespace, chunk.to_vec(), Vec::new())
                .await
                .unwrap();
        }
        let manifest = Manifest::read(&store, &namespace).await.unwrap().unwrap();
        let fragment_refs = manifest.uncompacted_fragments().to_vec();
        assert_eq!(fragment_refs.len(), 4);

        let cache_dir = tempfile::TempDir::new().unwrap();
        let cache = Arc::new(
            DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap(),
        );
        if warm_fragments > 0 {
            WalReader::new(store.clone())
                .read_fragments_from_refs_unchecked(
                    &namespace,
                    &fragment_refs[..warm_fragments],
                    FragmentCachePolicy::ReadWrite(&cache),
                )
                .await
                .unwrap();
        }
        let cache_size_before = cache.total_size();
        counter.reset();

        let lease_manager = Arc::new(LeaseManager::new(
            store.clone(),
            format!("fragment-cache-{label}"),
            Duration::from_secs(30),
        ));
        let result = compact_namespace_under_lease_with_lifecycle(
            &test_compactor(&store),
            &lease_manager,
            &namespace,
            &HashMap::new(),
            FragmentCachePolicy::ReadOnly(&cache),
            &compaction_lifecycle,
        )
        .await
        .unwrap();
        assert_eq!(result.fragments_removed, 4);
        assert_eq!(
            counter.gets_for(ArtifactClass::Wal),
            (4 - warm_fragments) as u64,
            "{label} compaction fetched the wrong number of WAL fragments"
        );
        assert_eq!(
            cache.total_size(),
            cache_size_before,
            "{label} compaction populated its read-only cache on a miss"
        );

        let manifest = Manifest::read(&store, &namespace).await.unwrap().unwrap();
        let segment = manifest
            .segments
            .iter()
            .find(|segment| Some(&segment.id) == manifest.active_segment.as_ref())
            .expect("compaction must publish one active segment")
            .clone();
        assert!(!segment.cluster_objects.is_empty());
        let checksums = cluster_checksums(&store, &segment).await;
        outputs.push((label, normalize_segment_descriptor(segment), checksums));
    }

    let (_, expected_descriptor, expected_checksums) = &outputs[0];
    for (label, descriptor, checksums) in &outputs[1..] {
        assert_eq!(
            descriptor, expected_descriptor,
            "{label} cache state changed the published segment descriptor"
        );
        assert_eq!(
            checksums, expected_checksums,
            "{label} cache state changed compacted cluster bytes"
        );
    }

    compaction_lifecycle
        .close_and_abort_heartbeats()
        .await
        .unwrap();
    harness.cleanup().await;
}

fn trigger_metadata(namespace: &str, dimensions: usize) -> NamespaceMetadata {
    let now = Utc::now();
    NamespaceMetadata {
        name: namespace.to_string(),
        dimensions,
        distance_metric: DistanceMetric::Euclidean,
        index_type: IndexType::IvfFlat,
        vector_count: 0,
        created_at: now,
        updated_at: now,
        state: NamespaceState::Active,
        destruction_record_key: None,
        deletion_intent: None,
        full_text_search: HashMap::new(),
        index_config: None,
        compaction_health: CompactionHealth::default(),
        creation_kind: zeppelin::namespace::branching::NamespaceCreationKind::Root,
        branch_identity: None,
        branch_prepare: None,
        branch_activation: None,
        incarnation_id: None,
    }
}

#[tokio::test]
async fn test_compact_single_fragment() {
    let harness = TestHarness::new().await;
    let ns = harness.key("compact-single");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    // Create namespace manifest
    let mut manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();
    common::write_active_namespace_metadata(store, &ns, 16, DistanceMetric::Euclidean).await;

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
async fn test_failed_live_manifest_put_does_not_wedge_a_divergent_compaction_retry() {
    let harness = TestHarness::new().await;
    let ns = harness.key("compact-divergent-history-retry");
    Manifest::new().write(&harness.store, &ns).await.unwrap();
    common::write_active_namespace_metadata(&harness.store, &ns, 16, DistanceMetric::Euclidean)
        .await;
    WalWriter::new(harness.store.clone())
        .append(&ns, random_vectors(50, 16), vec![])
        .await
        .unwrap();

    let base = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    let candidate_version = base.version() + 1;
    let (faulted_store, failure) = fail_put_once_matching(&harness.store, Manifest::s3_key(&ns));
    let first = test_compactor(&faulted_store).compact(&ns).await;
    assert!(first.is_err(), "the injected live PUT must fail loudly");
    assert_eq!(failure.failures_injected(), 1);

    let after_failure = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(after_failure.to_bytes().unwrap(), base.to_bytes().unwrap());
    assert!(after_failure.active_segment.is_none());
    assert_eq!(after_failure.uncompacted_fragments().len(), 1);
    let retained_base = Manifest::read_history(&harness.store, &ns, base.version())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained_base.to_bytes().unwrap(), base.to_bytes().unwrap());
    assert!(!harness
        .store
        .exists(&Manifest::history_key(&ns, candidate_version))
        .await
        .unwrap());

    let result = test_compactor(&faulted_store)
        .compact(&ns)
        .await
        .expect("a rebuilt compaction candidate must publish at the unreserved generation");
    assert_eq!(result.vectors_compacted, 50);
    assert_eq!(result.fragments_removed, 1);
    let committed = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(committed.version(), candidate_version);
    assert!(committed.active_segment.is_some());
    assert!(committed.uncompacted_fragments().is_empty());

    harness.cleanup().await;
}

#[tokio::test]
async fn test_compact_multiple_fragments() {
    let harness = TestHarness::new().await;
    let ns = harness.key("compact-multi");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    let mut manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();
    common::write_active_namespace_metadata(store, &ns, 16, DistanceMetric::Euclidean).await;

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

    let (f1, _) = writer.append(&ns, vecs1, vec![]).await.unwrap();
    let (f2, _) = writer.append(&ns, vecs2, vec![]).await.unwrap();
    let (f3, _) = writer.append(&ns, vecs3, vec![]).await.unwrap();

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

    let mut manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();
    common::write_active_namespace_metadata(store, &ns, 16, DistanceMetric::Euclidean).await;

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

    let mut manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();
    common::write_active_namespace_metadata(store, &ns, 16, DistanceMetric::Euclidean).await;

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
    let query_result = execute_query(QueryParams {
        store,
        wal_reader: &wal_reader,
        namespace: &ns,
        query: &v2,
        top_k: 1,
        nprobe: 4,
        filter: None,
        consistency: ConsistencyLevel::Eventual,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 3,
        rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: None,
        include_attributes: true,
    })
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

    let mut manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();
    common::write_active_namespace_metadata(store, &ns, 16, DistanceMetric::Euclidean).await;

    // Append 2 fragments
    let (f1, _) = writer
        .append(&ns, random_vectors(20, 16), vec![])
        .await
        .unwrap();
    let (f2, _) = writer
        .append(&ns, random_vectors(30, 16), vec![])
        .await
        .unwrap();

    let compactor = test_compactor(store);
    compactor.compact(&ns).await.unwrap();

    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();

    assert_eq!(manifest.segments.len(), 1);
    assert!(manifest.active_segment.is_some());
    // Watermark records the max removed ULID. Use max(f1, f2), not f2:
    // same-millisecond ULIDs are not monotonic.
    assert_eq!(manifest.compaction_watermark, Some(f1.id.max(f2.id)));
    assert!(manifest.fragments.is_empty());

    harness.cleanup().await;
}

#[tokio::test]
async fn test_compact_cleans_up_fragments() {
    let harness = TestHarness::new().await;
    let ns = harness.key("compact-cleanup");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    let mut manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();
    common::write_active_namespace_metadata(store, &ns, 16, DistanceMetric::Euclidean).await;

    // Append 3 fragments, record their S3 keys
    let (f1, _) = writer
        .append(&ns, random_vectors(10, 16), vec![])
        .await
        .unwrap();
    let (f2, _) = writer
        .append(&ns, random_vectors(10, 16), vec![])
        .await
        .unwrap();
    let (f3, _) = writer
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

    let mut manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();
    common::write_active_namespace_metadata(store, &ns, 16, DistanceMetric::Euclidean).await;

    // Append A, compact
    let (frag_a, _) = writer
        .append(&ns, random_vectors(20, 16), vec![])
        .await
        .unwrap();

    let compactor = test_compactor(store);
    compactor.compact(&ns).await.unwrap();

    // Append B (after compaction)
    let (frag_b, _) = writer
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
    let old_index = build_ivf_flat(&initial_vecs, &indexing_config, store, &ns, old_seg_id)
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
        fts_fields: Vec::new(),
        has_global_fts: false,
        cluster_owners: Vec::new(),
        sketch: None,
        cluster_objects: old_index.cluster_objects().to_vec(),
        bootstrap: None,
        membership: None,
        artifact_origin: None,
    });
    manifest.write(store, &ns).await.unwrap();
    common::write_active_namespace_metadata(store, &ns, 16, DistanceMetric::Euclidean).await;

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

    let mut manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();
    common::write_active_namespace_metadata(store, &ns, 16, DistanceMetric::Cosine).await;

    // Append 100 vecs
    let vecs = random_vectors(100, 16);
    let query_vec = vecs[0].values.clone();
    writer.append(&ns, vecs, vec![]).await.unwrap();

    // Query before compaction (Strong)
    let pre_result = execute_query(QueryParams {
        store,
        wal_reader: &wal_reader,
        namespace: &ns,
        query: &query_vec,
        top_k: 5,
        nprobe: 4,
        filter: None,
        consistency: ConsistencyLevel::Strong,
        distance_metric: DistanceMetric::Cosine,
        oversample_factor: 3,
        rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: None,
        include_attributes: true,
    })
    .await
    .unwrap();

    let pre_ids: Vec<String> = pre_result.results.iter().map(|r| r.id.clone()).collect();
    assert!(!pre_ids.is_empty());

    // Compact
    let compactor = test_compactor(store);
    compactor.compact(&ns).await.unwrap();

    // Query after compaction (Strong)
    let post_strong = execute_query(QueryParams {
        store,
        wal_reader: &wal_reader,
        namespace: &ns,
        query: &query_vec,
        top_k: 5,
        nprobe: 4,
        filter: None,
        consistency: ConsistencyLevel::Strong,
        distance_metric: DistanceMetric::Cosine,
        oversample_factor: 3,
        rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: None,
        include_attributes: true,
    })
    .await
    .unwrap();

    // Query after compaction (Eventual)
    let post_eventual = execute_query(QueryParams {
        store,
        wal_reader: &wal_reader,
        namespace: &ns,
        query: &query_vec,
        top_k: 5,
        nprobe: 4,
        filter: None,
        consistency: ConsistencyLevel::Eventual,
        distance_metric: DistanceMetric::Cosine,
        oversample_factor: 3,
        rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: None,
        include_attributes: true,
    })
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

/// Regression: a delete of an already-compacted vector exists only as a WAL
/// tombstone (no live WAL entry). Strong queries must not resurrect the
/// vector from the segment before the next compaction.
#[tokio::test]
async fn test_delete_after_compaction_not_returned_strong() {
    let harness = TestHarness::new().await;
    let ns = harness.key("del-after-compact");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());
    let wal_reader = WalReader::new(store.clone());

    let mut manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();
    common::write_active_namespace_metadata(store, &ns, 16, DistanceMetric::Cosine).await;

    // Upsert 50 vectors and compact them into a segment
    let vecs = random_vectors(50, 16);
    let target_id = vecs[0].id.clone();
    let query_vec = vecs[0].values.clone();
    writer.append(&ns, vecs, vec![]).await.unwrap();

    let compactor = test_compactor(store);
    compactor.compact(&ns).await.unwrap();

    // Sanity: the vector is served from the segment
    let before = execute_query(QueryParams {
        store,
        wal_reader: &wal_reader,
        namespace: &ns,
        query: &query_vec,
        top_k: 5,
        nprobe: 4,
        filter: None,
        consistency: ConsistencyLevel::Strong,
        distance_metric: DistanceMetric::Cosine,
        oversample_factor: 3,
        rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: None,
        include_attributes: true,
    })
    .await
    .unwrap();
    assert_eq!(before.results[0].id, target_id);
    assert_eq!(before.scanned_fragments, 0);
    assert_eq!(before.scanned_segments, 1);

    // Delete the compacted vector — this lands as a tombstone-only WAL fragment
    writer
        .append(&ns, vec![], vec![target_id.clone()])
        .await
        .unwrap();

    // Strong query must not return the deleted vector
    let after = execute_query(QueryParams {
        store,
        wal_reader: &wal_reader,
        namespace: &ns,
        query: &query_vec,
        top_k: 5,
        nprobe: 4,
        filter: None,
        consistency: ConsistencyLevel::Strong,
        distance_metric: DistanceMetric::Cosine,
        oversample_factor: 3,
        rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: None,
        include_attributes: true,
    })
    .await
    .unwrap();
    assert!(
        after.results.iter().all(|r| r.id != target_id),
        "deleted vector {target_id} resurrected from segment in Strong query"
    );
    // Other vectors are still served
    assert!(!after.results.is_empty());

    // After the next compaction the tombstone is applied to the segment,
    // so the vector stays gone at both consistency levels.
    compactor.compact(&ns).await.unwrap();
    let post_compact = execute_query(QueryParams {
        store,
        wal_reader: &wal_reader,
        namespace: &ns,
        query: &query_vec,
        top_k: 5,
        nprobe: 4,
        filter: None,
        consistency: ConsistencyLevel::Eventual,
        distance_metric: DistanceMetric::Cosine,
        oversample_factor: 3,
        rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: None,
        include_attributes: true,
    })
    .await
    .unwrap();
    assert!(
        post_compact.results.iter().all(|r| r.id != target_id),
        "deleted vector {target_id} survived compaction"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn test_compact_empty_namespace() {
    let harness = TestHarness::new().await;
    let ns = harness.key("compact-empty");
    let store = &harness.store;

    // Create manifest with no fragments
    let mut manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();
    common::write_active_namespace_metadata(store, &ns, 16, DistanceMetric::Euclidean).await;

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

    let mut manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();
    common::write_active_namespace_metadata(store, &ns, 16, DistanceMetric::Euclidean).await;

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
    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    let metadata = trigger_metadata(&ns, 16);
    assert!(!compactor.should_compact(&ns, &manifest, &metadata).unwrap());

    // Append 1 more (now 3 >= 3)
    writer
        .append(&ns, random_vectors(10, 16), vec![])
        .await
        .unwrap();

    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    assert!(compactor.should_compact(&ns, &manifest, &metadata).unwrap());

    harness.cleanup().await;
}

#[tokio::test]
async fn test_compact_rejects_missing_manifest_for_active_namespace() {
    let harness = TestHarness::new().await;
    let ns = format!("{}-missing-transaction-manifest", harness.prefix);
    let store = &harness.store;
    let namespace_manager = zeppelin::namespace::NamespaceManager::new(store.clone());
    namespace_manager
        .create(&ns, 16, DistanceMetric::Euclidean)
        .await
        .unwrap();
    store.delete(&Manifest::s3_key(&ns)).await.unwrap();

    let error = test_compactor(store)
        .compact(&ns)
        .await
        .expect_err("compaction must not treat a missing active manifest as empty");
    assert!(
        matches!(
            error,
            zeppelin::error::ZeppelinError::ManifestNotFound { ref namespace }
                if namespace == &ns
        ),
        "missing active manifest must return ManifestNotFound, got {error:?}"
    );
    assert!(
        !store.exists(&Manifest::s3_key(&ns)).await.unwrap(),
        "failed compaction must not recreate the missing manifest"
    );

    store.delete_prefix(&format!("{ns}/")).await.unwrap();
    harness.cleanup().await;
}

#[tokio::test]
async fn test_trigger_refresh_rejects_missing_manifest_instead_of_using_cached_snapshot() {
    let harness = TestHarness::new().await;
    let ns = format!("{}-missing-trigger-manifest", harness.prefix);
    let manager = zeppelin::namespace::NamespaceManager::new(harness.store.clone());
    let metadata = manager
        .create(&ns, 16, DistanceMetric::Euclidean)
        .await
        .unwrap();
    let compactor = test_compactor(&harness.store);
    let manifest_cache = ManifestCache::new(std::time::Duration::from_secs(3_600));

    assert!(
        !evaluate_compaction_trigger(&compactor, &manifest_cache, &metadata)
            .await
            .unwrap()
    );
    harness.store.delete(&Manifest::s3_key(&ns)).await.unwrap();

    for _ in 0..2 {
        let error = evaluate_compaction_trigger(&compactor, &manifest_cache, &metadata)
            .await
            .expect_err("a required trigger refresh must reject a missing manifest");
        assert!(
            matches!(
                error,
                zeppelin::error::ZeppelinError::ManifestNotFound { ref namespace }
                    if namespace == &ns
            ),
            "missing trigger manifest must fail loudly, got {error:?}"
        );
    }

    harness
        .store
        .delete_prefix(&format!("{ns}/"))
        .await
        .unwrap();
    harness.cleanup().await;
}

#[tokio::test]
async fn test_trigger_refresh_rejects_corrupt_manifest_instead_of_using_cached_snapshot() {
    let harness = TestHarness::new().await;
    let ns = format!("{}-corrupt-trigger-manifest", harness.prefix);
    let manager = zeppelin::namespace::NamespaceManager::new(harness.store.clone());
    let metadata = manager
        .create(&ns, 16, DistanceMetric::Euclidean)
        .await
        .unwrap();
    let compactor = test_compactor(&harness.store);
    let manifest_cache = ManifestCache::new(std::time::Duration::from_secs(3_600));

    assert!(
        !evaluate_compaction_trigger(&compactor, &manifest_cache, &metadata)
            .await
            .unwrap()
    );
    harness
        .store
        .put(
            &Manifest::s3_key(&ns),
            bytes::Bytes::from_static(b"\x01not-a-valid-manifest"),
        )
        .await
        .unwrap();

    for _ in 0..2 {
        let error = evaluate_compaction_trigger(&compactor, &manifest_cache, &metadata)
            .await
            .expect_err("a required trigger refresh must reject a corrupt manifest");
        assert!(
            matches!(error, zeppelin::error::ZeppelinError::Serialization(_)),
            "corrupt trigger manifest must fail loudly, got {error:?}"
        );
    }

    harness
        .store
        .delete_prefix(&format!("{ns}/"))
        .await
        .unwrap();
    harness.cleanup().await;
}

#[tokio::test]
async fn test_leased_compaction_rejects_missing_authoritative_metadata() {
    let harness = TestHarness::new().await;
    let ns = format!("{}-missing-compaction-metadata", harness.prefix);
    let manager = zeppelin::namespace::NamespaceManager::new(harness.store.clone());
    manager
        .create(&ns, 16, DistanceMetric::Euclidean)
        .await
        .unwrap();
    WalWriter::new(harness.store.clone())
        .append(&ns, random_vectors(8, 16), Vec::new())
        .await
        .unwrap();
    let before = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    harness
        .store
        .delete(&NamespaceMetadata::s3_key(&ns))
        .await
        .unwrap();
    let lease_manager = Arc::new(LeaseManager::new(
        harness.store.clone(),
        format!("test-{}", uuid::Uuid::new_v4()),
        Duration::from_secs(30),
    ));
    let compaction_lifecycle = CompactionLifecycle::new();

    let error = compact_namespace_under_lease_with_lifecycle(
        &test_compactor(&harness.store),
        &lease_manager,
        &ns,
        &HashMap::new(),
        FragmentCachePolicy::Bypass,
        &compaction_lifecycle,
    )
    .await
    .expect_err("missing authoritative metadata must stop leased compaction");
    assert!(
        matches!(
            error,
            zeppelin::error::ZeppelinError::NamespaceNotFound { ref namespace }
                if namespace == &ns
        ),
        "missing metadata must remain a namespace error, got {error:?}"
    );
    compaction_lifecycle
        .close_and_abort_heartbeats()
        .await
        .unwrap();

    let after = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(after.version(), before.version());
    assert_eq!(after.active_segment, before.active_segment);
    assert_eq!(after.fragments, before.fragments);

    harness
        .store
        .delete_prefix(&format!("{ns}/"))
        .await
        .unwrap();
    harness.cleanup().await;
}

/// I1: a quiet namespace with a small number of uncompacted fragments must
/// converge to a compacted state within a bounded time window via the
/// age-based trigger — regardless of the fragment-count threshold.
///
/// Drives the same decision path as the background loop (`should_compact`
/// → `compact`) on a short interval, with `max_wal_age_before_compact_secs`
/// set to ~2s and the count threshold far out of reach.
#[tokio::test]
async fn test_age_trigger_compacts_quiet_namespace() {
    let harness = TestHarness::new().await;
    let ns = harness.key("age-trigger");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    let mut manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();
    common::write_active_namespace_metadata(store, &ns, 16, DistanceMetric::Euclidean).await;

    // 2 fragments — far below the count threshold of 100.
    writer
        .append(&ns, random_vectors(10, 16), vec![])
        .await
        .unwrap();
    writer
        .append(&ns, random_vectors(10, 16), vec![])
        .await
        .unwrap();

    let wal_reader = WalReader::new(store.clone());
    let compaction_config = CompactionConfig {
        interval_secs: 1,
        max_wal_fragments_before_compact: 100,
        max_wal_age_before_compact_secs: 2,
        max_wal_bytes_before_compact: u64::MAX,
        ..Default::default()
    };
    let indexing_config = IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 10,
        ..Default::default()
    };
    let compactor = Compactor::new(
        store.clone(),
        wal_reader,
        compaction_config,
        indexing_config,
        common::default_gc_upload_window(),
    );

    // Mirror the background loop: poll every interval, compact when the
    // trigger fires. The age trigger must fire within ~2s; give it a
    // bounded number of intervals before declaring failure.
    let mut compacted = false;
    let metadata = trigger_metadata(&ns, 16);
    for _ in 0..10 {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
        if compactor.should_compact(&ns, &manifest, &metadata).unwrap() {
            compactor.compact(&ns).await.unwrap();
            compacted = true;
            break;
        }
    }

    assert!(
        compacted,
        "namespace with 2 fragments older than max_wal_age_before_compact_secs \
         was never compacted — fragments stay uncompacted forever"
    );

    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    assert_eq!(manifest.segments.len(), 1, "expected a new segment");
    assert!(manifest.active_segment.is_some());
    assert!(
        manifest.uncompacted_fragments().is_empty(),
        "all fragments must be absorbed into the segment"
    );

    harness.cleanup().await;
}

/// I4: an idle namespace with ZERO uncompacted fragments is never touched,
/// across several compaction intervals, even with maximally aggressive
/// trigger thresholds — no busy work, no S3 churn on quiet namespaces.
#[tokio::test]
async fn test_idle_namespace_untouched_across_intervals() {
    let harness = TestHarness::new().await;
    let ns = harness.key("idle-untouched");
    let store = &harness.store;

    let mut manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();
    common::write_active_namespace_metadata(store, &ns, 16, DistanceMetric::Euclidean).await;

    let manifest_key = Manifest::s3_key(&ns);
    let etag_before = store.head(&manifest_key).await.unwrap().e_tag;

    let wal_reader = WalReader::new(store.clone());
    let compaction_config = CompactionConfig {
        interval_secs: 1,
        max_wal_fragments_before_compact: 0,
        max_wal_age_before_compact_secs: 0,
        max_wal_bytes_before_compact: 0,
        ..Default::default()
    };
    let compactor = Compactor::new(
        store.clone(),
        wal_reader,
        compaction_config,
        IndexingConfig {
            default_num_centroids: 4,
            kmeans_max_iterations: 10,
            ..Default::default()
        },
        common::default_gc_upload_window(),
    );

    // Several intervals: the trigger must never fire with 0 fragments.
    let metadata = trigger_metadata(&ns, 16);
    for _ in 0..3 {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
        assert!(
            !compactor.should_compact(&ns, &manifest, &metadata).unwrap(),
            "idle namespace (0 fragments) must never trigger compaction"
        );
    }

    let etag_after = store.head(&manifest_key).await.unwrap().e_tag;
    assert_eq!(
        etag_before, etag_after,
        "idle namespace manifest must not be rewritten across intervals"
    );

    harness.cleanup().await;
}

/// I2: the size trigger fires from real recorded fragment sizes — the
/// manifest carries `size_bytes` set at PUT time by the WAL writer.
#[tokio::test]
async fn test_bytes_trigger_uses_recorded_sizes() {
    let harness = TestHarness::new().await;
    let ns = harness.key("bytes-trigger");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    let mut manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();

    // One fragment with real payload — its serialized size is recorded in
    // the manifest at write time.
    writer
        .append(&ns, random_vectors(50, 16), vec![])
        .await
        .unwrap();

    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    assert_eq!(manifest.fragments.len(), 1);
    let recorded = manifest.fragments[0].size_bytes;
    assert!(
        recorded > 0,
        "WAL writer must record fragment size_bytes in the manifest"
    );

    // Threshold just below the recorded size → triggers.
    let make_compactor = |max_bytes: u64| {
        Compactor::new(
            store.clone(),
            WalReader::new(store.clone()),
            CompactionConfig {
                max_wal_fragments_before_compact: 100,
                max_wal_age_before_compact_secs: 3600,
                max_wal_bytes_before_compact: max_bytes,
                ..Default::default()
            },
            IndexingConfig::default(),
            common::default_gc_upload_window(),
        )
    };
    let metadata = trigger_metadata(&ns, 16);
    assert!(
        make_compactor(recorded)
            .should_compact(&ns, &manifest, &metadata)
            .unwrap(),
        "total WAL bytes >= threshold must trigger compaction"
    );
    // Threshold above the recorded size → does not trigger.
    assert!(
        !make_compactor(recorded + 1)
            .should_compact(&ns, &manifest, &metadata)
            .unwrap(),
        "total WAL bytes < threshold must not trigger compaction"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn test_compaction_populates_cluster_object_size_bytes() {
    let harness = TestHarness::new().await;
    let ns = harness.key("cluster-object-size-bytes");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    Manifest::new().write(store, &ns).await.unwrap();
    common::write_active_namespace_metadata(store, &ns, 32, DistanceMetric::Euclidean).await;
    writer
        .append(&ns, random_vectors(256, 32), vec![])
        .await
        .unwrap();

    let compactor = test_compactor(store);
    compactor.compact(&ns).await.unwrap();

    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    let segment = active_segment_ref(&manifest);
    assert!(
        !segment.cluster_objects.is_empty(),
        "compaction must write grouped cluster object refs"
    );

    for object in &segment.cluster_objects {
        let actual_len = store.get(&object.key).await.unwrap().len() as u64;
        assert_eq!(
            object.size_bytes, actual_len,
            "cluster object {} must record its exact S3 object length",
            object.key
        );
    }

    harness.cleanup().await;
}

// ─── Task 3: centroid caching invariants ───

use std::sync::Arc;
use std::time::Duration;
use zeppelin::index::ivf_flat::build::centroids_key;

/// Build a `QueryParams` for the Task 3 tests (Eventual → segment-only).
#[allow(clippy::too_many_arguments)]
fn segment_query_params<'a>(
    store: &'a zeppelin::storage::ZeppelinStore,
    wal_reader: &'a WalReader,
    ns: &'a str,
    query: &'a [f32],
    cache: Option<&'a Arc<DiskCache>>,
) -> QueryParams<'a> {
    QueryParams {
        store,
        wal_reader,
        namespace: ns,
        query,
        top_k: 5,
        nprobe: 4,
        filter: None,
        consistency: ConsistencyLevel::Eventual,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 3,
        rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache,
        manifest_cache: None,
        include_attributes: true,
    }
}

fn active_segment_ref(manifest: &Manifest) -> &SegmentRef {
    let active_segment = manifest
        .active_segment
        .as_ref()
        .expect("manifest must have an active segment");
    manifest
        .segments
        .iter()
        .find(|segment| segment.id == *active_segment)
        .expect("active segment must be present in manifest segments")
}

fn segment_index_meta_key(namespace: &str, segment: &SegmentRef) -> String {
    segment
        .bootstrap
        .as_ref()
        .map(|bootstrap| bootstrap.key.clone())
        .unwrap_or_else(|| centroids_key(namespace, &segment.id))
}

/// I1: a repeat query against an unchanged segment performs ZERO S3 GETs
/// for the centroids blob — it must be served from the cache.
#[tokio::test]
async fn test_repeat_query_zero_centroid_gets() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let ns = harness.key("centroid-zero-gets");
    let writer = WalWriter::new(store.clone());
    let wal_reader = WalReader::new(store.clone());

    Manifest::new().write(&store, &ns).await.unwrap();
    common::write_active_namespace_metadata(&store, &ns, 16, DistanceMetric::Euclidean).await;
    let vecs = random_vectors(100, 16);
    let query_vec = vecs[0].values.clone();
    writer.append(&ns, vecs, vec![]).await.unwrap();

    let compactor = test_compactor(&store);
    compactor.compact(&ns).await.unwrap();
    let manifest = Manifest::read(&store, &ns).await.unwrap().unwrap();
    let meta_key = segment_index_meta_key(&ns, active_segment_ref(&manifest));

    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    // Cold query: one centroid GET is allowed (and expected).
    execute_query(segment_query_params(
        &store,
        &wal_reader,
        &ns,
        &query_vec,
        Some(&cache),
    ))
    .await
    .unwrap();
    assert_eq!(
        counter.gets_matching(&meta_key),
        1,
        "cold query must fetch active segment metadata exactly once"
    );

    // Warm query: active segment metadata must come from cache — zero S3 GETs.
    counter.reset();
    execute_query(segment_query_params(
        &store,
        &wal_reader,
        &ns,
        &query_vec,
        Some(&cache),
    ))
    .await
    .unwrap();
    assert_eq!(
        counter.gets_matching(&meta_key),
        0,
        "repeat query against an unchanged segment must perform ZERO S3 GETs for active metadata"
    );

    harness.cleanup().await;
}

/// I2: cache keys embed segment identity — after a second compaction the
/// query reflects the new segment's data and the new segment's centroids
/// get their own cache entry (never served from the old segment's entry).
#[tokio::test]
async fn test_new_segment_never_serves_stale_centroids() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let ns = harness.key("centroid-seg-identity");
    let writer = WalWriter::new(store.clone());
    let wal_reader = WalReader::new(store.clone());

    Manifest::new().write(&store, &ns).await.unwrap();
    common::write_active_namespace_metadata(&store, &ns, 16, DistanceMetric::Euclidean).await;
    // random_vectors is fixed-seed: generate ONE pool and split it so the
    // two batches have disjoint values (identical values ⇒ distance ties ⇒
    // arbitrary winner).
    let pool = random_vectors(100, 16);
    let batch1: Vec<VectorEntry> = pool[..60]
        .iter()
        .enumerate()
        .map(|(i, v)| VectorEntry {
            id: format!("one_{i}"),
            values: v.values.clone(),
            attributes: None,
        })
        .collect();
    writer.append(&ns, batch1, vec![]).await.unwrap();

    let compactor = test_compactor(&store);
    compactor.compact(&ns).await.unwrap();
    let manifest = Manifest::read(&store, &ns).await.unwrap().unwrap();
    let seg1 = active_segment_ref(&manifest).id.clone();

    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    // Warm the cache against segment 1.
    let probe = random_vectors(1, 16)[0].values.clone();
    execute_query(segment_query_params(
        &store,
        &wal_reader,
        &ns,
        &probe,
        Some(&cache),
    ))
    .await
    .unwrap();

    // New data → new segment (disjoint values from batch1, see pool split).
    let batch2: Vec<VectorEntry> = pool[60..]
        .iter()
        .enumerate()
        .map(|(i, v)| VectorEntry {
            id: format!("two_{i}"),
            values: v.values.clone(),
            attributes: None,
        })
        .collect();
    let target = batch2[0].clone();
    writer.append(&ns, batch2, vec![]).await.unwrap();
    compactor.compact(&ns).await.unwrap();
    let manifest = Manifest::read(&store, &ns).await.unwrap().unwrap();
    let seg2_ref = active_segment_ref(&manifest);
    let seg2 = seg2_ref.id.clone();
    assert_ne!(seg1, seg2, "second compaction must produce a new segment");

    // Query for a batch-2 vector: results must reflect the NEW segment.
    let result = execute_query(segment_query_params(
        &store,
        &wal_reader,
        &ns,
        &target.values,
        Some(&cache),
    ))
    .await
    .unwrap();
    assert_eq!(
        result.results[0].id, target.id,
        "query after re-compaction must reflect the new segment's data"
    );

    // The new segment's centroids must have their OWN cache entry.
    let seg2_key = segment_index_meta_key(&ns, seg2_ref);
    assert!(
        cache.get(&seg2_key).await.is_some(),
        "new segment's index metadata must be cached under its own key ({seg2_key})"
    );

    harness.cleanup().await;
}

/// I3: the active segment's centroids are pinned — they survive LRU
/// eviction pressure and are still served without an S3 GET. On segment
/// rotation, the old segment's pin is released and the new one pinned.
#[tokio::test]
async fn test_pinned_centroids_survive_eviction_pressure() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let ns = harness.key("centroid-pin");
    let writer = WalWriter::new(store.clone());
    let wal_reader = WalReader::new(store.clone());

    Manifest::new().write(&store, &ns).await.unwrap();
    common::write_active_namespace_metadata(&store, &ns, 16, DistanceMetric::Euclidean).await;
    let vecs = random_vectors(100, 16);
    let query_vec = vecs[0].values.clone();
    writer.append(&ns, vecs, vec![]).await.unwrap();

    let compactor = test_compactor(&store);
    compactor.compact(&ns).await.unwrap();
    let manifest = Manifest::read(&store, &ns).await.unwrap().unwrap();
    let seg1_ref = active_segment_ref(&manifest);
    let seg1 = seg1_ref.id.clone();
    let seg1_ckey = segment_index_meta_key(&ns, seg1_ref);

    // Tiny cache: cluster-sized junk entries will force LRU eviction.
    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache =
        Arc::new(DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 32 * 1024).unwrap());

    // Populate + pin via a query.
    execute_query(segment_query_params(
        &store,
        &wal_reader,
        &ns,
        &query_vec,
        Some(&cache),
    ))
    .await
    .unwrap();
    assert!(
        cache.is_pinned(&seg1_ckey).await,
        "active segment's index metadata must be pinned after a query"
    );

    // Fill the cache well past capacity with cluster-sized entries.
    for i in 0..40 {
        cache
            .put(
                &format!("{ns}/junk/blob_{i}"),
                &bytes::Bytes::from(vec![0u8; 4 * 1024]),
            )
            .await
            .unwrap();
    }

    // Pinned centroids survive: still served with zero S3 GETs.
    assert!(
        cache.get(&seg1_ckey).await.is_some(),
        "pinned index metadata entry must survive LRU eviction pressure"
    );
    counter.reset();
    execute_query(segment_query_params(
        &store,
        &wal_reader,
        &ns,
        &query_vec,
        Some(&cache),
    ))
    .await
    .unwrap();
    assert_eq!(
        counter.gets_matching(&seg1_ckey),
        0,
        "query under eviction pressure must serve pinned index metadata without a GET"
    );

    // Segment rotation: new compaction → old pin released, new key pinned.
    writer
        .append(&ns, random_vectors(20, 16), vec![])
        .await
        .unwrap();
    compactor.compact(&ns).await.unwrap();
    let manifest = Manifest::read(&store, &ns).await.unwrap().unwrap();
    let seg2_ref = active_segment_ref(&manifest);
    let seg2 = seg2_ref.id.clone();
    assert_ne!(seg1, seg2);
    let seg2_ckey = segment_index_meta_key(&ns, seg2_ref);

    execute_query(segment_query_params(
        &store,
        &wal_reader,
        &ns,
        &query_vec,
        Some(&cache),
    ))
    .await
    .unwrap();
    assert!(
        cache.is_pinned(&seg2_ckey).await,
        "new active segment's index metadata must be pinned"
    );
    assert!(
        !cache.is_pinned(&seg1_ckey).await,
        "old segment's centroids must be unpinned after rotation"
    );

    harness.cleanup().await;
}

/// I4: after the background compaction loop produces a new segment, the
/// new segment's centroids are warmed into the cache eagerly — BEFORE any
/// query arrives.
#[tokio::test]
async fn test_compaction_warms_new_segment_centroids() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let namespace_manager = Arc::new(zeppelin::namespace::NamespaceManager::new(store.clone()));
    for meta in namespace_manager
        .list(Some(harness.prefix.as_str()))
        .await
        .unwrap()
    {
        if meta.name.ends_with("-warm") {
            namespace_manager.delete(&meta.name).await.unwrap();
        }
    }

    // Top-level namespace (no '/') so NamespaceManager can register it and
    // the compaction loop discovers it. Cleaned up manually below.
    let ns = format!("{}-warm", harness.prefix);
    let writer = WalWriter::new(store.clone());

    namespace_manager
        .create(&ns, 16, DistanceMetric::Euclidean)
        .await
        .unwrap();
    writer
        .append(&ns, random_vectors(50, 16), vec![])
        .await
        .unwrap();

    let compaction_config = CompactionConfig {
        interval_secs: 1,
        max_wal_fragments_before_compact: 100,
        max_wal_age_before_compact_secs: 1,
        max_wal_bytes_before_compact: u64::MAX,
        ..Default::default()
    };
    let indexing_config = IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 10,
        ..Default::default()
    };
    let compactor = Arc::new(Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        compaction_config.clone(),
        indexing_config,
        common::default_gc_upload_window(),
    ));
    let manifest_cache = Arc::new(zeppelin::cache::manifest_cache::ManifestCache::new(
        Duration::from_millis(500),
    ));
    let lease_manager = Arc::new(zeppelin::wal::LeaseManager::new(
        store.clone(),
        format!("test-{}", uuid::Uuid::new_v4()),
        Duration::from_secs(compaction_config.lease_duration_secs),
    ));
    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    {
        let compactor = compactor.clone();
        let namespace_manager = namespace_manager.clone();
        let manifest_cache = manifest_cache.clone();
        let cache = cache.clone();
        let namespace_prefix = Some(harness.prefix.clone());
        tokio::spawn(async move {
            zeppelin::compaction::background::compaction_loop(
                compactor,
                namespace_manager,
                shutdown_rx,
                manifest_cache,
                lease_manager,
                cache,
                CompactionLoopOptions {
                    gc_config: zeppelin::config::GcConfig::default(),
                    namespace_prefix,
                },
            )
            .await;
        });
    }

    // Wait for the loop to compact (age trigger ~1s, interval 1s), then
    // assert the NEW segment's centroids appear in the cache without any
    // query having been issued.
    let mut warmed_key: Option<String> = None;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let Some(manifest) = Manifest::read(&store, &ns).await.unwrap() else {
            continue;
        };
        if let Some(seg_id) = &manifest.active_segment {
            let ckey = centroids_key(&ns, seg_id);
            if cache.get(&ckey).await.is_some() {
                warmed_key = Some(ckey);
                break;
            }
        }
    }
    let _ = shutdown_tx.send(true);

    let warmed_key = warmed_key.expect(
        "post-compaction hook must warm the new segment's centroids into the cache \
         before any query arrives",
    );
    assert!(
        cache.is_pinned(&warmed_key).await,
        "warmed centroids must be pinned for the active segment"
    );

    let _ = store.delete_prefix(&format!("{ns}/")).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_background_compaction_records_missing_active_manifest_failure() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let ns = format!("{}-missing-background-manifest", harness.prefix);
    let namespace_manager = Arc::new(zeppelin::namespace::NamespaceManager::new(store.clone()));
    namespace_manager
        .create(&ns, 16, DistanceMetric::Euclidean)
        .await
        .unwrap();
    store.delete(&Manifest::s3_key(&ns)).await.unwrap();

    let compaction_config = CompactionConfig {
        interval_secs: 1,
        ..Default::default()
    };
    let compactor = Arc::new(Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        compaction_config.clone(),
        IndexingConfig::default(),
        common::default_gc_upload_window(),
    ));
    let manifest_cache = Arc::new(zeppelin::cache::manifest_cache::ManifestCache::new(
        Duration::from_millis(10),
    ));
    let lease_manager = Arc::new(zeppelin::wal::LeaseManager::new(
        store.clone(),
        format!("test-{}", uuid::Uuid::new_v4()),
        Duration::from_secs(compaction_config.lease_duration_secs),
    ));
    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );
    let failures =
        zeppelin::metrics::COMPACTIONS_TOTAL.with_label_values(&[ns.as_str(), "failure"]);
    assert_eq!(failures.get(), 0);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let loop_handle = {
        let compactor = compactor.clone();
        let namespace_manager = namespace_manager.clone();
        let namespace_prefix = Some(harness.prefix.clone());
        tokio::spawn(async move {
            zeppelin::compaction::background::compaction_loop(
                compactor,
                namespace_manager,
                shutdown_rx,
                manifest_cache,
                lease_manager,
                cache,
                CompactionLoopOptions {
                    gc_config: zeppelin::config::GcConfig::default(),
                    namespace_prefix,
                },
            )
            .await;
        })
    };

    let health = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let meta = namespace_manager.get(&ns).await.unwrap();
            if meta.compaction_health.consecutive_failures > 0 {
                break meta.compaction_health;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("background compaction must persist a missing-manifest failure");
    shutdown_tx.send(true).unwrap();
    loop_handle.await.unwrap();

    assert_eq!(failures.get(), 1);
    assert_eq!(
        health.last_compaction_status,
        zeppelin::namespace::manager::CompactionStatus::Failure
    );
    assert_eq!(health.consecutive_failures, 1);
    assert_eq!(
        health.last_compaction_error.as_deref(),
        Some(format!("manifest not found for namespace: {ns}").as_str())
    );
    assert!(
        !store.exists(&Manifest::s3_key(&ns)).await.unwrap(),
        "background failure handling must not recreate the manifest"
    );

    store.delete_prefix(&format!("{ns}/")).await.unwrap();
    harness.cleanup().await;
}

#[tokio::test]
async fn test_background_first_discovery_preserves_prewarmed_manifest_cache() {
    #[allow(clippy::too_many_arguments)]
    async fn run_one_tick(
        compactor: Arc<Compactor>,
        namespace_manager: Arc<zeppelin::namespace::NamespaceManager>,
        manifest_cache: Arc<ManifestCache>,
        lease_manager: Arc<LeaseManager>,
        cache: Arc<DiskCache>,
        namespace_prefix: String,
        counter: &common::counting::GetCounter,
        manifest_key: &str,
    ) -> (u64, u64) {
        counter.reset();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let loop_handle = tokio::spawn(async move {
            zeppelin::compaction::background::compaction_loop(
                compactor,
                namespace_manager,
                shutdown_rx,
                manifest_cache,
                lease_manager,
                cache,
                CompactionLoopOptions {
                    gc_config: zeppelin::config::GcConfig::default(),
                    namespace_prefix: Some(namespace_prefix),
                },
            )
            .await;
        });

        tokio::time::timeout(Duration::from_secs(3), async {
            while counter.delimiter_list_calls_for_prefix("") == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("tick one must begin fresh namespace discovery");
        shutdown_tx.send(true).unwrap();
        loop_handle.await.unwrap();

        (
            counter.gets_matching(manifest_key),
            counter.get_bytes_for(common::counting::ArtifactClass::Manifest),
        )
    }

    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let namespace_manager = Arc::new(zeppelin::namespace::NamespaceManager::new(store.clone()));
    let ns = format!("{}-warm-first-background-tick", harness.prefix);
    namespace_manager
        .create(&ns, 16, DistanceMetric::Euclidean)
        .await
        .unwrap();
    let manifest_key = Manifest::s3_key(&ns);
    let manifest_bytes = store.get(&manifest_key).await.unwrap().len() as u64;

    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_secs(3_600)));
    manifest_cache
        .get_strong_required(&store, &ns)
        .await
        .unwrap();

    let compaction_config = CompactionConfig {
        interval_secs: 1,
        ..Default::default()
    };
    let compactor = Arc::new(Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        compaction_config.clone(),
        IndexingConfig::default(),
        common::default_gc_upload_window(),
    ));
    let lease_manager = Arc::new(LeaseManager::new(
        store.clone(),
        format!("test-{}", uuid::Uuid::new_v4()),
        Duration::from_secs(compaction_config.lease_duration_secs),
    ));
    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    let (warm_gets, warm_bytes) = run_one_tick(
        compactor.clone(),
        namespace_manager.clone(),
        manifest_cache.clone(),
        lease_manager.clone(),
        cache.clone(),
        harness.prefix.clone(),
        &counter,
        &manifest_key,
    )
    .await;

    manifest_cache.invalidate_at(&ns, compactor.clock().now());
    let (cold_gets, cold_bytes) = run_one_tick(
        compactor,
        namespace_manager,
        manifest_cache,
        lease_manager,
        cache,
        harness.prefix.clone(),
        &counter,
        &manifest_key,
    )
    .await;

    assert_eq!(
        warm_gets, cold_gets,
        "warm and cold ticks must issue the same number of manifest requests"
    );
    assert_eq!(
        cold_bytes,
        warm_bytes + manifest_bytes,
        "tick one must preserve the resident ETag and receive NotModified; an explicitly cold tick must transfer exactly one additional manifest body"
    );

    store.delete_prefix(&format!("{ns}/")).await.unwrap();
    harness.cleanup().await;
}

#[tokio::test]
async fn test_background_discovery_resets_manifest_cache_across_remote_recreate_with_same_time() {
    #[derive(Debug)]
    struct FixedNamespaceTime(chrono::DateTime<Utc>);

    impl zeppelin::time::TimeSource for FixedNamespaceTime {
        fn now(&self) -> chrono::DateTime<Utc> {
            self.0
        }
    }

    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let ns = format!("{}-recreated-background-manifest", harness.prefix);
    let old_created_at = chrono::DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
    let new_created_at = old_created_at;
    let namespace_manager = Arc::new(zeppelin::namespace::NamespaceManager::with_clock(
        store.clone(),
        Duration::from_secs(3_600),
        zeppelin::time::Clock::from_source(Arc::new(FixedNamespaceTime(old_created_at))),
    ));
    let old_metadata = namespace_manager
        .create(&ns, 16, DistanceMetric::Euclidean)
        .await
        .unwrap();
    let writer = WalWriter::new(store.clone());
    writer
        .append(&ns, random_vectors(2, 16), Vec::new())
        .await
        .unwrap();
    writer
        .append(
            &ns,
            (0..2)
                .map(|index| VectorEntry {
                    id: format!("recreated-{index}"),
                    values: vec![index as f32; 16],
                    attributes: None,
                })
                .collect(),
            Vec::new(),
        )
        .await
        .unwrap();
    let old_manifest = Manifest::read(&store, &ns).await.unwrap().unwrap();
    assert!(old_manifest.version() > 1);

    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_secs(3_600)));
    let cached_old = manifest_cache
        .get_strong_required(&store, &ns)
        .await
        .unwrap();
    assert_eq!(cached_old.version(), old_manifest.version());

    let remote_manager = zeppelin::namespace::NamespaceManager::with_clock(
        store.clone(),
        Duration::from_secs(3_600),
        zeppelin::time::Clock::from_source(Arc::new(FixedNamespaceTime(new_created_at))),
    );
    remote_manager.delete(&ns).await.unwrap();
    let new_metadata = remote_manager
        .create(&ns, 16, DistanceMetric::Euclidean)
        .await
        .unwrap();
    assert_eq!(old_metadata.created_at, old_created_at);
    assert_eq!(new_metadata.created_at, old_metadata.created_at);
    assert!(old_metadata.incarnation_id.is_some());
    assert!(new_metadata.incarnation_id.is_some());
    assert_ne!(old_metadata.incarnation_id, new_metadata.incarnation_id);
    let recreated = Manifest::read(&store, &ns).await.unwrap().unwrap();
    assert_eq!(recreated.version(), 1);
    let stale_error = manifest_cache
        .get_strong_required(&store, &ns)
        .await
        .expect_err("the old lifecycle floor must initially reject generation one");
    assert!(matches!(
        stale_error,
        zeppelin::error::ZeppelinError::Serialization(ref message)
            if message.contains("generation regressed")
    ));

    let compaction_config = CompactionConfig {
        interval_secs: 1,
        ..Default::default()
    };
    let compactor = Arc::new(Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        compaction_config.clone(),
        IndexingConfig::default(),
        common::default_gc_upload_window(),
    ));
    let lease_manager = Arc::new(LeaseManager::new(
        store.clone(),
        format!("test-{}", uuid::Uuid::new_v4()),
        Duration::from_secs(compaction_config.lease_duration_secs),
    ));
    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let loop_handle = {
        let namespace_manager = namespace_manager.clone();
        let manifest_cache = manifest_cache.clone();
        let namespace_prefix = Some(harness.prefix.clone());
        tokio::spawn(async move {
            zeppelin::compaction::background::compaction_loop(
                compactor,
                namespace_manager,
                shutdown_rx,
                manifest_cache,
                lease_manager,
                cache,
                CompactionLoopOptions {
                    gc_config: zeppelin::config::GcConfig::default(),
                    namespace_prefix,
                },
            )
            .await;
        })
    };

    let observed = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match manifest_cache.get_strong_required(&store, &ns).await {
                Ok(manifest) if manifest.version() == recreated.version() => break manifest,
                Ok(_) | Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .expect("fresh discovery must reset the manifest cache lifecycle floor");
    shutdown_tx.send(true).unwrap();
    loop_handle.await.unwrap();
    assert_eq!(observed.version(), 1);

    store.delete_prefix(&format!("{ns}/")).await.unwrap();
    harness.cleanup().await;
}

#[tokio::test]
async fn test_background_compaction_accepts_missing_manifest_while_deleting() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let ns = format!("{}-deleting-without-manifest", harness.prefix);
    let namespace_manager = Arc::new(zeppelin::namespace::NamespaceManager::new(store.clone()));
    namespace_manager
        .create(&ns, 16, DistanceMetric::Euclidean)
        .await
        .unwrap();
    namespace_manager.start_delete(&ns).await.unwrap();
    assert!(!store.exists(&Manifest::s3_key(&ns)).await.unwrap());

    let compaction_config = CompactionConfig {
        interval_secs: 1,
        ..Default::default()
    };
    let compactor = Arc::new(Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        compaction_config.clone(),
        IndexingConfig::default(),
        common::default_gc_upload_window(),
    ));
    let manifest_cache = Arc::new(zeppelin::cache::manifest_cache::ManifestCache::new(
        Duration::from_millis(10),
    ));
    let lease_manager = Arc::new(zeppelin::wal::LeaseManager::new(
        store.clone(),
        format!("test-{}", uuid::Uuid::new_v4()),
        Duration::from_secs(compaction_config.lease_duration_secs),
    ));
    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );
    let mut config = zeppelin::config::Config::default();
    config.security.set_cursor_hmac_key_hex("42".repeat(32));
    let security = Arc::new(
        zeppelin::security::SecurityKernel::from_config(&config.security)
            .expect("open test security kernel must compose"),
    );
    let deletion_worker = zeppelin::compaction::background::GovernedDeletionWorker::new(
        store.clone(),
        namespace_manager.clone(),
        lease_manager.clone(),
        zeppelin::time::Clock::system(),
        manifest_cache.clone(),
        &config,
        security,
        zeppelin::namespace::BranchReadinessObserver::unscoped(),
    );
    let compaction_lifecycle = zeppelin::compaction::background::CompactionLifecycle::new();
    let failures =
        zeppelin::metrics::COMPACTIONS_TOTAL.with_label_values(&[ns.as_str(), "failure"]);
    assert_eq!(failures.get(), 0);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let loop_handle = tokio::spawn({
        let namespace_manager = namespace_manager.clone();
        let namespace_prefix = Some(harness.prefix.clone());
        async move {
            zeppelin::compaction::background::compaction_loop_with_governed_deletion(
                compactor,
                namespace_manager,
                shutdown_rx,
                manifest_cache,
                lease_manager,
                cache,
                CompactionLoopOptions {
                    gc_config: zeppelin::config::GcConfig::default(),
                    namespace_prefix,
                },
                deletion_worker,
                &compaction_lifecycle,
            )
            .await;
        }
    });

    tokio::time::timeout(Duration::from_secs(3), async {
        let metadata_key = zeppelin::namespace::manager::NamespaceMetadata::s3_key(&ns);
        while store.exists(&metadata_key).await.unwrap() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("background lifecycle handling must finish the deletion tombstone");
    shutdown_tx.send(true).unwrap();
    loop_handle.await.unwrap();

    assert_eq!(
        failures.get(),
        0,
        "a deleting namespace must bypass active compaction failure accounting"
    );

    harness.cleanup().await;
}

#[cfg(feature = "branching-test-support")]
#[tokio::test]
async fn test_background_compaction_resumes_active_governed_deletion_intent() {
    let harness = TestHarness::new().await;
    let isolated_store = scoped_test_security_store(&harness.store, &harness.prefix);
    let ns = format!("{}-active-governed-delete", harness.prefix);
    zeppelin::namespace::NamespaceManager::new(isolated_store.clone())
        .create(&ns, 16, DistanceMetric::Euclidean)
        .await
        .expect("governed deletion fixture namespace must be created");

    // Reject the deletion fence publication after the graph has durably
    // installed its metadata intent. This models a process crash in the exact
    // pre-tombstone state that a stateless maintenance worker must discover.
    let (crashed_store, fence_failure) =
        fail_put_once_matching(&isolated_store, Manifest::s3_key(&ns));
    let config = zeppelin::config::Config::default();
    zeppelin::namespace::branching::test_support::delete_namespace_for_test(
        crashed_store,
        zeppelin::namespace::NamespaceId::new(ns.clone()).expect("fixture namespace must be valid"),
        config.indexing.clone(),
        config.branching.clone(),
    )
    .await
    .expect_err("crash fixture must fail before publishing the deletion fence");
    assert_eq!(fence_failure.failures_injected(), 1);

    let metadata_key = NamespaceMetadata::s3_key(&ns);
    let interrupted = NamespaceMetadata::from_bytes(
        &isolated_store
            .get(&metadata_key)
            .await
            .expect("crash fixture must retain authoritative namespace metadata"),
    )
    .expect("crash fixture metadata must decode");
    assert_eq!(interrupted.state, NamespaceState::Active);
    let interrupted_intent = interrupted
        .deletion_intent
        .as_ref()
        .expect("crash fixture must retain its governed deletion intent");
    assert_eq!(
        interrupted_intent.fenced_generation, None,
        "crash fixture must stop before the destruction fence and tombstone"
    );

    let (store, counter) = counting_store(&isolated_store);
    let namespace_manager = Arc::new(zeppelin::namespace::NamespaceManager::new(store.clone()));
    let compaction_config = CompactionConfig {
        interval_secs: 1,
        ..Default::default()
    };
    let compactor = Arc::new(Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        compaction_config.clone(),
        IndexingConfig::default(),
        common::default_gc_upload_window(),
    ));
    let manifest_cache = Arc::new(zeppelin::cache::manifest_cache::ManifestCache::new(
        Duration::from_millis(10),
    ));
    let lease_manager = Arc::new(zeppelin::wal::LeaseManager::new(
        store.clone(),
        format!("test-{}", uuid::Uuid::new_v4()),
        Duration::from_secs(compaction_config.lease_duration_secs),
    ));
    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );
    let mut worker_config = config;
    worker_config
        .security
        .set_cursor_hmac_key_hex("42".repeat(32));
    let security = Arc::new(
        zeppelin::security::SecurityKernel::from_config(&worker_config.security)
            .expect("open test security kernel must compose"),
    );
    let deletion_worker = zeppelin::compaction::background::GovernedDeletionWorker::new(
        store.clone(),
        namespace_manager.clone(),
        lease_manager.clone(),
        zeppelin::time::Clock::system(),
        manifest_cache.clone(),
        &worker_config,
        security,
        zeppelin::namespace::BranchReadinessObserver::scoped(Some(harness.prefix.clone())),
    );
    let compaction_lifecycle = zeppelin::compaction::background::CompactionLifecycle::new();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let loop_handle = tokio::spawn({
        let namespace_prefix = Some(harness.prefix.clone());
        async move {
            zeppelin::compaction::background::compaction_loop_with_governed_deletion(
                compactor,
                namespace_manager,
                shutdown_rx,
                manifest_cache,
                lease_manager,
                cache,
                CompactionLoopOptions {
                    gc_config: zeppelin::config::GcConfig::default(),
                    namespace_prefix,
                },
                deletion_worker,
                &compaction_lifecycle,
            )
            .await;
        }
    });

    tokio::time::timeout(Duration::from_secs(3), async {
        while counter.delimiter_list_calls_for_prefix("") == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("background loop must complete authoritative namespace discovery");
    shutdown_tx.send(true).unwrap();
    loop_handle.await.unwrap();

    match store.get(&metadata_key).await {
        Err(zeppelin::error::ZeppelinError::NotFound { .. }) => {}
        Ok(bytes) => {
            let remaining = NamespaceMetadata::from_bytes(&bytes)
                .expect("remaining authoritative metadata must decode");
            panic!(
                "background governed deletion worker left crash metadata active: state={:?}, deletion_intent={:?}",
                remaining.state, remaining.deletion_intent
            );
        }
        Err(error) => panic!("authoritative metadata read failed unexpectedly: {error}"),
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn test_background_compaction_discovery_is_delimited_and_prefix_scoped() {
    let harness = TestHarness::new().await;
    let isolated_store =
        zeppelin::storage::ZeppelinStore::new(Arc::new(object_store::prefix::PrefixStore::new(
            harness.store.inner(),
            object_store::path::Path::from(harness.prefix.clone()),
        )));
    let (store, counter) = counting_store(&isolated_store);
    let namespace_manager = Arc::new(zeppelin::namespace::NamespaceManager::new(store.clone()));
    let ns = format!("{}-registered-loop", harness.prefix);
    let outside_ns = format!("outside-{}-registered-loop", uuid::Uuid::new_v4());

    namespace_manager
        .create(&ns, 16, DistanceMetric::Euclidean)
        .await
        .unwrap();
    namespace_manager
        .create(&outside_ns, 16, DistanceMetric::Euclidean)
        .await
        .unwrap();
    WalWriter::new(store.clone())
        .append(&ns, random_vectors(25, 16), vec![])
        .await
        .unwrap();
    WalWriter::new(store.clone())
        .append(&outside_ns, random_vectors(25, 16), vec![])
        .await
        .unwrap();

    let compaction_config = CompactionConfig {
        interval_secs: 1,
        max_wal_fragments_before_compact: 1,
        max_wal_age_before_compact_secs: u64::MAX,
        max_wal_bytes_before_compact: u64::MAX,
        ..Default::default()
    };
    let compactor = Arc::new(Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        compaction_config.clone(),
        IndexingConfig {
            default_num_centroids: 4,
            kmeans_max_iterations: 10,
            ..Default::default()
        },
        common::default_gc_upload_window(),
    ));
    let manifest_cache = Arc::new(zeppelin::cache::manifest_cache::ManifestCache::new(
        Duration::from_millis(500),
    ));
    let lease_manager = Arc::new(zeppelin::wal::LeaseManager::new(
        store.clone(),
        format!("test-{}", uuid::Uuid::new_v4()),
        Duration::from_secs(compaction_config.lease_duration_secs),
    ));
    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    counter.reset();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let scoped_handle = {
        let compactor = compactor.clone();
        let namespace_manager = namespace_manager.clone();
        let manifest_cache = manifest_cache.clone();
        let cache = cache.clone();
        let lease_manager = lease_manager.clone();
        let namespace_prefix = Some(harness.prefix.clone());
        tokio::spawn(async move {
            zeppelin::compaction::background::compaction_loop(
                compactor,
                namespace_manager,
                shutdown_rx,
                manifest_cache,
                lease_manager,
                cache,
                CompactionLoopOptions {
                    gc_config: zeppelin::config::GcConfig::default(),
                    namespace_prefix,
                },
            )
            .await;
        })
    };

    let mut compacted = false;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let Some(manifest) = Manifest::read(&store, &ns).await.unwrap() else {
            continue;
        };
        if manifest.active_segment.is_some() && manifest.uncompacted_fragments().is_empty() {
            compacted = true;
            break;
        }
    }
    let _ = shutdown_tx.send(true);
    scoped_handle.await.unwrap();

    assert!(
        compacted,
        "namespace under the supplied prefix should compact"
    );
    let outside_manifest = Manifest::read(&store, &outside_ns).await.unwrap().unwrap();
    assert!(
        outside_manifest.active_segment.is_none(),
        "prefix-scoped compaction must not discover outside namespaces"
    );
    assert_eq!(
        outside_manifest.uncompacted_fragments().len(),
        1,
        "outside namespace WAL must remain untouched by prefix-scoped compaction"
    );
    assert_eq!(
        counter.list_calls_for_prefix(""),
        0,
        "prefix-scoped background compaction must not recursively list the whole bucket"
    );
    assert!(
        counter.delimiter_list_calls_for_prefix("") > 0
            || counter.delimiter_list_calls_for_prefix(&harness.prefix) > 0,
        "prefix-scoped background compaction should use delimiter namespace discovery"
    );

    counter.reset();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let unscoped_handle = {
        let compactor = compactor.clone();
        let namespace_manager = namespace_manager.clone();
        let manifest_cache = manifest_cache.clone();
        let cache = cache.clone();
        let lease_manager = lease_manager.clone();
        tokio::spawn(async move {
            zeppelin::compaction::background::compaction_loop(
                compactor,
                namespace_manager,
                shutdown_rx,
                manifest_cache,
                lease_manager,
                cache,
                CompactionLoopOptions {
                    gc_config: zeppelin::config::GcConfig::default(),
                    namespace_prefix: None,
                },
            )
            .await;
        })
    };

    let mut outside_compacted = false;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let Some(manifest) = Manifest::read(&store, &outside_ns).await.unwrap() else {
            continue;
        };
        if manifest.active_segment.is_some() && manifest.uncompacted_fragments().is_empty() {
            outside_compacted = true;
            break;
        }
    }
    let _ = shutdown_tx.send(true);
    unscoped_handle.await.unwrap();

    assert!(
        outside_compacted,
        "unscoped compaction must still discover namespaces outside the harness prefix"
    );
    assert_eq!(
        counter.list_calls_for_prefix(""),
        0,
        "unscoped background compaction must still avoid recursive root listing"
    );
    assert!(
        counter.delimiter_list_calls_for_prefix("") > 0,
        "unscoped background compaction should delimiter-list the root"
    );

    let _ = store.delete_prefix(&format!("{ns}/")).await;
    let _ = store.delete_prefix(&format!("{outside_ns}/")).await;
    harness.cleanup().await;
}

/// I5 (fail-loud half): caching must never convert a query-path error into
/// silent degradation — a missing centroids blob during an actual query is
/// still an error, warm cache infrastructure or not.
#[tokio::test]
async fn test_query_path_stays_fail_loud_with_cache() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let ns = harness.key("centroid-fail-loud");
    let writer = WalWriter::new(store.clone());
    let wal_reader = WalReader::new(store.clone());

    Manifest::new().write(&store, &ns).await.unwrap();
    common::write_active_namespace_metadata(&store, &ns, 16, DistanceMetric::Euclidean).await;
    writer
        .append(&ns, random_vectors(50, 16), vec![])
        .await
        .unwrap();
    let compactor = test_compactor(&store);
    compactor.compact(&ns).await.unwrap();
    let manifest = Manifest::read(&store, &ns).await.unwrap().unwrap();
    let seg = active_segment_ref(&manifest);

    // Sabotage: remove the active index-metadata blob out from under the segment.
    store
        .delete(&segment_index_meta_key(&ns, seg))
        .await
        .unwrap();

    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );
    let probe = random_vectors(1, 16)[0].values.clone();
    let result = execute_query(segment_query_params(
        &store,
        &wal_reader,
        &ns,
        &probe,
        Some(&cache),
    ))
    .await;
    assert!(
        result.is_err(),
        "a failed centroid fetch during an actual query must remain an error"
    );

    harness.cleanup().await;
}

/// Task 10 I4 (defense in depth): non-finite vectors already durable on S3
/// (written before API validation existed, simulated here by writing directly
/// via WalWriter, bypassing the HTTP boundary) must not poison centroid
/// training. Compaction skips them with an ERROR log + metric and produces
/// finite centroids; it must NOT fail — crashing compaction forever on one
/// bad historical vector would brick the namespace.
#[tokio::test]
async fn test_compaction_skips_non_finite_prefix_data() {
    let harness = TestHarness::new().await;
    let ns = harness.key("compact-nonfinite");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    Manifest::new().write(store, &ns).await.unwrap();
    common::write_active_namespace_metadata(store, &ns, 16, DistanceMetric::Euclidean).await;

    // 50 good vectors + 1 NaN vector + 1 inf vector, direct to the WAL
    // (pre-fix data path; the API boundary now rejects these).
    let mut vecs = random_vectors(50, 16);
    let mut nan_values = vec![0.5f32; 16];
    nan_values[3] = f32::NAN;
    vecs.push(VectorEntry {
        id: "poison-nan".to_string(),
        values: nan_values,
        attributes: None,
    });
    let mut inf_values = vec![0.5f32; 16];
    inf_values[7] = f32::INFINITY;
    vecs.push(VectorEntry {
        id: "poison-inf".to_string(),
        values: inf_values,
        attributes: None,
    });
    writer.append(&ns, vecs, vec![]).await.unwrap();

    let metric_before = zeppelin::metrics::NON_FINITE_VECTORS_SKIPPED_TOTAL
        .with_label_values(&[&ns])
        .get();

    let compactor = test_compactor(store);
    let result = compactor
        .compact(&ns)
        .await
        .expect("compaction must succeed despite pre-existing non-finite vectors");

    // The two poisoned vectors are skipped, not compacted.
    assert_eq!(
        result.vectors_compacted, 50,
        "non-finite vectors must be excluded from the compacted segment"
    );

    // Centroids on S3 must be entirely finite.
    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    let seg = manifest.active_segment.clone().unwrap();
    let data = store
        .get(&format!("{ns}/segments/{seg}/centroids.bin"))
        .await
        .unwrap();
    // Layouts:
    // - legacy: [num_centroids: u32][dim: u32][f32 * num * dim]
    // - v2:     [b"ZCT2"][num_centroids: u32][dim: u32][f32 * num * dim]...
    let (num, dim, float_offset) = if data.starts_with(b"ZCT2") {
        (
            u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize,
            u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize,
            12usize,
        )
    } else {
        (
            u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize,
            u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize,
            8usize,
        )
    };
    assert!(num > 0 && dim == 16);
    for i in 0..num * dim {
        let off = float_offset + i * 4;
        let val = f32::from_le_bytes(data[off..off + 4].try_into().unwrap());
        assert!(
            val.is_finite(),
            "centroid float #{i} is non-finite ({val}) — NaN/inf poisoned k-means"
        );
    }

    // The sanctioned-degradation metric fired once per skipped vector.
    let metric_after = zeppelin::metrics::NON_FINITE_VECTORS_SKIPPED_TOTAL
        .with_label_values(&[&ns])
        .get();
    assert_eq!(
        metric_after - metric_before,
        2,
        "skipping non-finite vectors must be observable via metric"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn test_compact_attributes_preserved() {
    let harness = TestHarness::new().await;
    let ns = harness.key("compact-attrs");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());
    let wal_reader = WalReader::new(store.clone());

    let mut manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();
    common::write_active_namespace_metadata(store, &ns, 16, DistanceMetric::Cosine).await;

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
    let result = execute_query(QueryParams {
        store,
        wal_reader: &wal_reader,
        namespace: &ns,
        query: &query_vec,
        top_k: 30,
        nprobe: 4,
        filter: Some(&filter),
        consistency: ConsistencyLevel::Eventual,
        distance_metric: DistanceMetric::Cosine,
        oversample_factor: 3,
        rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: None,
        include_attributes: true,
    })
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
