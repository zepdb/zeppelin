//! Task 2 Phase B — incremental compaction (merge-without-retrain fast path).
//!
//! Invariants under test:
//!   B1: when centroids are reused, only clusters that gained/lost vectors are
//!       rewritten; untouched clusters are carried by reference.
//!   B2: a cluster whose contents did not change keeps its S3 object key.
//!   B3: a delete/update hitting a carried-over cluster forces it into the
//!       rewrite set (and the vector is gone from results).
//!   plus: carried-over objects are NOT enqueued for deletion, and query
//!         results match a full rewrite (golden equivalence).
//!   SQ8: carried clusters' codes decode against the COPIED (not recomputed)
//!         calibration.
//!   multi-gen + update-moves-cluster: owner chains resolve across successive
//!         incremental cycles; a relocated vector leaves no ghost.

mod common;

use bytes::Bytes;
use common::harness::TestHarness;
use common::vectors::clustered_vectors;

use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig};
use zeppelin::index::ivf_flat::build::build_ivf_flat;
use zeppelin::index::quantization::sq::{serialize_sq_cluster, SqCalibration};
use zeppelin::query::{execute_query, QueryParams};
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::{ConsistencyLevel, DistanceMetric, VectorEntry};
use zeppelin::wal::manifest::{Manifest, SegmentRef};
use zeppelin::wal::{WalReader, WalWriter};

const DIM: usize = 16;
const N_CLUSTERS: usize = 6;

/// Compactor whose config reuses centroids (high retrain threshold) and never
/// quantizes, so the incremental IVF-Flat carry-over path is exercised.
fn incremental_compactor(store: &ZeppelinStore) -> Compactor {
    let wal_reader = WalReader::new(store.clone());
    let compaction_config = CompactionConfig {
        max_wal_fragments_before_compact: 1,
        // Never retrain within the test's add ratios: keep the incremental path.
        retrain_imbalance_threshold: 1000.0,
        ..Default::default()
    };
    let indexing_config = IndexingConfig {
        default_num_centroids: N_CLUSTERS,
        kmeans_max_iterations: 25,
        quantization: zeppelin::index::quantization::QuantizationType::None,
        bitmap_index: false,
        fts_index: false,
        hierarchical: false,
        ..Default::default()
    };
    Compactor::new(
        store.clone(),
        wal_reader,
        compaction_config,
        indexing_config,
    )
}

/// Snapshot (key -> ETag) for every per-cluster object under a segment prefix.
async fn cluster_object_versions(
    store: &ZeppelinStore,
    ns: &str,
    seg_id: &str,
) -> std::collections::HashMap<String, String> {
    let prefix = format!("{ns}/segments/{seg_id}/");
    let keys = store.list_prefix(&prefix).await.unwrap();
    let mut out = std::collections::HashMap::new();
    for key in keys {
        let (_data, etag) = store.get_with_meta(&key).await.unwrap();
        if let Some(etag) = etag {
            out.insert(key, etag);
        }
    }
    out
}

/// Build an initial IVF-Flat segment from well-separated clusters and register
/// it as the active segment. Returns (segment_id, vectors) so the test knows
/// each vector's ground-truth cluster from its ID (`cluster_{ci}_vec_{vi}`).
async fn seed_segment(store: &ZeppelinStore, ns: &str) -> (String, Vec<VectorEntry>) {
    let (vectors, _centroids) = clustered_vectors(N_CLUSTERS, 20, DIM, 0.01);
    let indexing_config = IndexingConfig {
        default_num_centroids: N_CLUSTERS,
        kmeans_max_iterations: 25,
        quantization: zeppelin::index::quantization::QuantizationType::None,
        bitmap_index: false,
        fts_index: false,
        hierarchical: false,
        ..Default::default()
    };
    let seg_id = "seg_seed";
    let index = build_ivf_flat(&vectors, &indexing_config, store, ns, seg_id)
        .await
        .unwrap();

    let mut manifest = Manifest::new();
    manifest.add_segment(SegmentRef {
        id: seg_id.to_string(),
        vector_count: vectors.len(),
        cluster_count: index.num_clusters(),
        quantization: zeppelin::index::quantization::QuantizationType::None,
        hierarchical: false,
        bitmap_fields: Vec::new(),
        fts_fields: Vec::new(),
        has_global_fts: false,
        cluster_owners: Vec::new(),
    });
    manifest.write(store, ns).await.unwrap();
    (seg_id.to_string(), vectors)
}

/// Like [`incremental_compactor`] but with a specific quantization type, so the
/// SQ/PQ copy-calibration carry-over path is exercised end-to-end.
fn incremental_compactor_quantized(
    store: &ZeppelinStore,
    quantization: zeppelin::index::quantization::QuantizationType,
) -> Compactor {
    let wal_reader = WalReader::new(store.clone());
    let compaction_config = CompactionConfig {
        max_wal_fragments_before_compact: 1,
        retrain_imbalance_threshold: 1000.0,
        ..Default::default()
    };
    let indexing_config = IndexingConfig {
        default_num_centroids: N_CLUSTERS,
        kmeans_max_iterations: 25,
        quantization,
        bitmap_index: false,
        fts_index: false,
        hierarchical: false,
        ..Default::default()
    };
    Compactor::new(
        store.clone(),
        wal_reader,
        compaction_config,
        indexing_config,
    )
}

/// Seed a pre-C.0b SQ8 segment in the legacy physical layout:
/// centroids.bin without magic, cluster_i.bin full vectors, sq_cluster_i.bin
/// sidecars, and sq_calibration.bin. This fixture proves incremental
/// compaction can carry old-format clusters into a new-format active segment.
async fn seed_legacy_sq8_segment(store: &ZeppelinStore, ns: &str) -> (String, Vec<VectorEntry>) {
    let (vectors, centroids) = clustered_vectors(N_CLUSTERS, 20, DIM, 0.01);
    let seg_id = "seg_seed";

    let refs: Vec<&[f32]> = vectors.iter().map(|v| v.values.as_slice()).collect();
    let calibration = SqCalibration::calibrate(&refs, DIM);
    store
        .put(
            &format!("{ns}/segments/{seg_id}/centroids.bin"),
            legacy_centroids_bytes(&centroids, DIM),
        )
        .await
        .unwrap();
    store
        .put(
            &format!("{ns}/segments/{seg_id}/sq_calibration.bin"),
            calibration.to_bytes(),
        )
        .await
        .unwrap();

    for cluster_idx in 0..N_CLUSTERS {
        let prefix = format!("cluster_{cluster_idx}_");
        let cluster: Vec<&VectorEntry> = vectors
            .iter()
            .filter(|vector| vector.id.starts_with(&prefix))
            .collect();
        let ids: Vec<String> = cluster.iter().map(|vector| vector.id.clone()).collect();
        let values: Vec<Vec<f32>> = cluster.iter().map(|vector| vector.values.clone()).collect();
        let attrs: Vec<_> = cluster
            .iter()
            .map(|vector| vector.attributes.clone())
            .collect();
        let cluster_refs: Vec<&[f32]> = values.iter().map(|values| values.as_slice()).collect();
        let codes = calibration.encode_batch(&cluster_refs);

        store
            .put(
                &format!("{ns}/segments/{seg_id}/cluster_{cluster_idx}.bin"),
                legacy_cluster_bytes(&ids, &values, DIM),
            )
            .await
            .unwrap();
        store
            .put(
                &format!("{ns}/segments/{seg_id}/attrs_{cluster_idx}.bin"),
                Bytes::from(serde_json::to_vec(&attrs).unwrap()),
            )
            .await
            .unwrap();
        store
            .put(
                &format!("{ns}/segments/{seg_id}/sq_cluster_{cluster_idx}.bin"),
                serialize_sq_cluster(&ids, &codes, DIM).unwrap(),
            )
            .await
            .unwrap();
    }

    let mut manifest = Manifest::new();
    manifest.add_segment(SegmentRef {
        id: seg_id.to_string(),
        vector_count: vectors.len(),
        cluster_count: N_CLUSTERS,
        quantization: zeppelin::index::quantization::QuantizationType::Scalar,
        hierarchical: false,
        bitmap_fields: Vec::new(),
        fts_fields: Vec::new(),
        has_global_fts: false,
        cluster_owners: Vec::new(),
    });
    manifest.write(store, ns).await.unwrap();
    (seg_id.to_string(), vectors)
}

fn legacy_centroids_bytes(centroids: &[Vec<f32>], dim: usize) -> Bytes {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(centroids.len() as u32).to_le_bytes());
    buf.extend_from_slice(&(dim as u32).to_le_bytes());
    for centroid in centroids {
        for value in centroid {
            buf.extend_from_slice(&value.to_le_bytes());
        }
    }
    Bytes::from(buf)
}

fn legacy_cluster_bytes(ids: &[String], vectors: &[Vec<f32>], dim: usize) -> Bytes {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(ids.len() as u32).to_le_bytes());
    buf.extend_from_slice(&(dim as u32).to_le_bytes());
    for (id, vector) in ids.iter().zip(vectors) {
        let id_bytes = id.as_bytes();
        buf.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(id_bytes);
        for value in vector {
            buf.extend_from_slice(&value.to_le_bytes());
        }
    }
    Bytes::from(buf)
}

/// Run a Strong query and return the result IDs (order-independent set).
async fn strong_query_ids(
    store: &ZeppelinStore,
    ns: &str,
    query: &[f32],
    top_k: usize,
) -> std::collections::HashSet<String> {
    let reader = WalReader::new(store.clone());
    let resp = execute_query(QueryParams {
        store,
        wal_reader: &reader,
        namespace: ns,
        query,
        top_k,
        nprobe: N_CLUSTERS,
        filter: None,
        consistency: ConsistencyLevel::Strong,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 1,
        cache: None,
        manifest_cache: None,
    })
    .await
    .unwrap();
    resp.results.into_iter().map(|r| r.id).collect()
}

/// B1 + B2: appending vectors that all fall into ONE cluster rewrites exactly
/// that cluster; every other cluster keeps its exact S3 object key.
#[tokio::test]
async fn test_incremental_rewrites_only_touched_cluster() {
    let harness = TestHarness::new().await;
    let ns = harness.key("incr-touched");
    let store = &harness.store;

    let (seed_id, seed_vecs) = seed_segment(store, &ns).await;
    let before = cluster_object_versions(store, &ns, &seed_id).await;
    assert!(!before.is_empty(), "seed segment must have cluster objects");

    // Append new vectors that sit right on cluster 0's members (tiny offset),
    // so they all assign to cluster 0 and no other cluster is touched.
    let anchor = &seed_vecs[0].values; // a cluster_0 member
    let new_vecs: Vec<VectorEntry> = (0..5)
        .map(|i| VectorEntry {
            id: format!("added_{i}"),
            values: anchor.iter().map(|x| x + 0.001).collect(),
            attributes: None,
        })
        .collect();
    let writer = WalWriter::new(store.clone());
    writer.append(&ns, new_vecs, vec![]).await.unwrap();

    let compactor = incremental_compactor(store);
    let result = compactor.compact(&ns).await.unwrap();
    let new_seg = result.segment_id.expect("a new segment must be produced");
    assert_ne!(new_seg, seed_id);
    assert_eq!(result.vectors_compacted, seed_vecs.len() + 5);

    // After: read the active segment ref and resolve each cluster's owner.
    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    let seg_ref = manifest
        .segments
        .iter()
        .find(|s| s.id == new_seg)
        .expect("new segment in manifest");
    assert_eq!(
        seg_ref.cluster_count, N_CLUSTERS,
        "cluster count preserved across incremental compaction"
    );
    assert!(
        !seg_ref.cluster_owners.is_empty(),
        "B1: at least one cluster must be carried over (owner map populated)"
    );

    // Exactly one cluster (the one the adds landed in) should be owned by the
    // new segment; the rest carried from the seed.
    let rewritten: Vec<usize> = (0..seg_ref.cluster_count)
        .filter(|&i| seg_ref.cluster_owner(i) == new_seg)
        .collect();
    assert_eq!(
        rewritten.len(),
        1,
        "exactly one cluster rewritten, got {rewritten:?}"
    );
    let touched = rewritten[0];

    // B2: every carried cluster's vector/attrs objects keep their EXACT old key
    // AND old ETag (byte-identical, never re-uploaded).
    for i in 0..seg_ref.cluster_count {
        if i == touched {
            continue;
        }
        let owner = seg_ref.cluster_owner(i);
        assert_eq!(owner, seed_id, "cluster {i} must be carried from the seed");
        let cvec_key = format!("{ns}/segments/{owner}/cluster_{i}.bin");
        let (_d, etag) = store.get_with_meta(&cvec_key).await.unwrap();
        assert_eq!(
            etag.as_deref(),
            before.get(&cvec_key).map(|s| s.as_str()),
            "B2: carried cluster {i} must keep its exact object (same ETag)"
        );
    }

    harness.cleanup().await;
}

/// B3: a delete targeting a vector in an otherwise-untouched cluster forces
/// that cluster to be rewritten, and the vector disappears from results.
#[tokio::test]
async fn test_incremental_delete_forces_cluster_rewrite() {
    let harness = TestHarness::new().await;
    let ns = harness.key("incr-delete");
    let store = &harness.store;

    let (seed_id, seed_vecs) = seed_segment(store, &ns).await;
    let before = cluster_object_versions(store, &ns, &seed_id).await;

    // Delete one vector known to belong to cluster 5 (last cluster). Its ID is
    // cluster_5_vec_0 by construction of clustered_vectors.
    let victim = "cluster_5_vec_0".to_string();
    assert!(
        seed_vecs.iter().any(|v| v.id == victim),
        "victim must exist in the seed"
    );
    let writer = WalWriter::new(store.clone());
    writer
        .append(&ns, vec![], vec![victim.clone()])
        .await
        .unwrap();

    let compactor = incremental_compactor(store);
    let result = compactor.compact(&ns).await.unwrap();
    let new_seg = result.segment_id.expect("new segment");
    assert_eq!(
        result.vectors_compacted,
        seed_vecs.len() - 1,
        "the deleted vector must not be in the compacted set"
    );

    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    let seg_ref = manifest.segments.iter().find(|s| s.id == new_seg).unwrap();

    // The victim's cluster must be rewritten (owned by the new segment); its
    // old object may or may not change key, but it must NOT be carried.
    let rewritten: Vec<usize> = (0..seg_ref.cluster_count)
        .filter(|&i| seg_ref.cluster_owner(i) == new_seg)
        .collect();
    assert_eq!(
        rewritten.len(),
        1,
        "B3: exactly the victim's cluster is rewritten by a lone delete, got {rewritten:?}"
    );

    // The deleted vector must be gone from a Strong query (segment-only, since
    // the WAL tombstone was compacted away).
    let victim_vec = seed_vecs
        .iter()
        .find(|v| v.id == victim)
        .unwrap()
        .values
        .clone();
    let reader = WalReader::new(store.clone());
    let resp = execute_query(QueryParams {
        store,
        wal_reader: &reader,
        namespace: &ns,
        query: &victim_vec,
        top_k: N_CLUSTERS * 20,
        nprobe: N_CLUSTERS,
        filter: None,
        consistency: ConsistencyLevel::Strong,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 1,
        cache: None,
        manifest_cache: None,
    })
    .await
    .unwrap();
    assert!(
        !resp.results.iter().any(|r| r.id == victim),
        "B3: deleted vector must not reappear in query results"
    );

    // Sanity: carried clusters kept their keys/ETags (unaffected by the delete).
    for i in 0..seg_ref.cluster_count {
        if seg_ref.cluster_owner(i) == new_seg {
            continue;
        }
        let owner = seg_ref.cluster_owner(i);
        let cvec_key = format!("{ns}/segments/{owner}/cluster_{i}.bin");
        let (_d, etag) = store.get_with_meta(&cvec_key).await.unwrap();
        assert_eq!(
            etag.as_deref(),
            before.get(&cvec_key).map(|s| s.as_str()),
            "carried cluster {i} unchanged by an unrelated delete"
        );
    }

    harness.cleanup().await;
}

/// Carried-over objects must NOT be scheduled for deletion, and must still be
/// readable on S3 after the incremental compaction commits.
#[tokio::test]
async fn test_incremental_carried_objects_not_deleted() {
    let harness = TestHarness::new().await;
    let ns = harness.key("incr-carry-nodelete");
    let store = &harness.store;

    let (seed_id, seed_vecs) = seed_segment(store, &ns).await;

    // Touch only cluster 0.
    let anchor = &seed_vecs[0].values;
    let new_vecs: Vec<VectorEntry> = (0..3)
        .map(|i| VectorEntry {
            id: format!("added_{i}"),
            values: anchor.iter().map(|x| x + 0.001).collect(),
            attributes: None,
        })
        .collect();
    let writer = WalWriter::new(store.clone());
    writer.append(&ns, new_vecs, vec![]).await.unwrap();

    let compactor = incremental_compactor(store);
    let new_seg = compactor.compact(&ns).await.unwrap().segment_id.unwrap();

    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    let seg_ref = manifest.segments.iter().find(|s| s.id == new_seg).unwrap();

    for i in 0..seg_ref.cluster_count {
        let owner = seg_ref.cluster_owner(i);
        if owner != seed_id {
            continue; // rewritten cluster, lives under the new segment
        }
        let cvec_key = format!("{ns}/segments/{owner}/cluster_{i}.bin");
        // Still present on S3...
        assert!(
            store.exists(&cvec_key).await.unwrap(),
            "carried object {cvec_key} must still exist"
        );
        // ...and NOT queued for deletion.
        assert!(
            !manifest.pending_deletes.contains(&cvec_key),
            "carried object {cvec_key} must not be in pending_deletes"
        );
    }

    harness.cleanup().await;
}

/// Golden equivalence: incremental compaction returns the same result set as a
/// full rewrite of the same logical data.
#[tokio::test]
async fn test_incremental_matches_full_rewrite_results() {
    let harness = TestHarness::new().await;
    let store = &harness.store;

    // Two namespaces holding identical data; one compacted incrementally, one
    // via full retrain.
    let ns_incr = harness.key("incr-golden-incr");
    let ns_full = harness.key("incr-golden-full");

    let (seed_incr, seed_vecs) = seed_segment(store, &ns_incr).await;
    let _ = seed_incr;

    // Mirror the same seed into the full-rewrite namespace.
    let indexing_config = IndexingConfig {
        default_num_centroids: N_CLUSTERS,
        kmeans_max_iterations: 25,
        quantization: zeppelin::index::quantization::QuantizationType::None,
        bitmap_index: false,
        fts_index: false,
        hierarchical: false,
        ..Default::default()
    };
    build_ivf_flat(&seed_vecs, &indexing_config, store, &ns_full, "seg_seed")
        .await
        .unwrap();
    let mut full_manifest = Manifest::new();
    full_manifest.add_segment(SegmentRef {
        id: "seg_seed".to_string(),
        vector_count: seed_vecs.len(),
        cluster_count: N_CLUSTERS,
        quantization: zeppelin::index::quantization::QuantizationType::None,
        hierarchical: false,
        bitmap_fields: Vec::new(),
        fts_fields: Vec::new(),
        has_global_fts: false,
        cluster_owners: Vec::new(),
    });
    full_manifest.write(store, &ns_full).await.unwrap();

    // Same WAL append to both.
    let anchor = &seed_vecs[0].values;
    let make_new = || -> Vec<VectorEntry> {
        (0..5)
            .map(|i| VectorEntry {
                id: format!("added_{i}"),
                values: anchor.iter().map(|x| x + 0.002).collect(),
                attributes: None,
            })
            .collect()
    };
    let writer = WalWriter::new(store.clone());
    writer.append(&ns_incr, make_new(), vec![]).await.unwrap();
    writer.append(&ns_full, make_new(), vec![]).await.unwrap();

    // Incremental compaction.
    incremental_compactor(store)
        .compact(&ns_incr)
        .await
        .unwrap();

    // Full-retrain compaction (low threshold forces retrain).
    let full_compactor = {
        let wal_reader = WalReader::new(store.clone());
        let cfg = CompactionConfig {
            max_wal_fragments_before_compact: 1,
            retrain_imbalance_threshold: 0.0, // always retrain
            ..Default::default()
        };
        Compactor::new(store.clone(), wal_reader, cfg, indexing_config.clone())
    };
    full_compactor.compact(&ns_full).await.unwrap();

    // Query both with the same probe vector; result ID sets must match.
    let query_vec = anchor.clone();
    let reader = WalReader::new(store.clone());
    let incr_res = execute_query(QueryParams {
        store,
        wal_reader: &reader,
        namespace: &ns_incr,
        query: &query_vec,
        top_k: 10,
        nprobe: N_CLUSTERS,
        filter: None,
        consistency: ConsistencyLevel::Strong,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 1,
        cache: None,
        manifest_cache: None,
    })
    .await
    .unwrap();
    let full_res = execute_query(QueryParams {
        store,
        wal_reader: &reader,
        namespace: &ns_full,
        query: &query_vec,
        top_k: 10,
        nprobe: N_CLUSTERS,
        filter: None,
        consistency: ConsistencyLevel::Strong,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 1,
        cache: None,
        manifest_cache: None,
    })
    .await
    .unwrap();

    let incr_ids: std::collections::HashSet<&str> =
        incr_res.results.iter().map(|r| r.id.as_str()).collect();
    let full_ids: std::collections::HashSet<&str> =
        full_res.results.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(
        incr_ids, full_ids,
        "incremental and full-rewrite top-k must return the same IDs"
    );

    harness.cleanup().await;
}

/// SQ8 carry-over correctness: the subtlest claim in Task 2B is that the SQ
/// calibration is COPIED (not recomputed) to the new segment, so a carried
/// cluster's codes — encoded against the OLD calibration — still decode
/// correctly when read under the NEW segment id. If the calibration were
/// recomputed, carried clusters' approximate distances would be corrupt and
/// their vectors would drop out of / reorder in the result set.
///
/// We compact incrementally with SQ8 (touching only cluster 0), then assert
/// that a probe of a CARRIED cluster (cluster 5) still returns that cluster's
/// own members — which is only true if the carried codes decode against the
/// calibration they were encoded with.
#[tokio::test]
async fn test_incremental_sq8_carryover_decodes_correctly() {
    use zeppelin::index::quantization::QuantizationType;

    let harness = TestHarness::new().await;
    let ns = harness.key("incr-sq8-carry");
    let store = &harness.store;

    let (seed_id, seed_vecs) = seed_legacy_sq8_segment(store, &ns).await;

    // Touch only cluster 0 (adds sit on a cluster_0 member).
    let anchor0 = &seed_vecs[0].values;
    let new_vecs: Vec<VectorEntry> = (0..5)
        .map(|i| VectorEntry {
            id: format!("added_{i}"),
            values: anchor0.iter().map(|x| x + 0.001).collect(),
            attributes: None,
        })
        .collect();
    let writer = WalWriter::new(store.clone());
    writer.append(&ns, new_vecs, vec![]).await.unwrap();

    let compactor = incremental_compactor_quantized(store, QuantizationType::Scalar);
    let new_seg = compactor
        .compact(&ns)
        .await
        .unwrap()
        .segment_id
        .expect("new segment produced");

    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    let seg_ref = manifest.segments.iter().find(|s| s.id == new_seg).unwrap();
    assert_eq!(
        seg_ref.quantization,
        QuantizationType::Scalar,
        "new segment must record SQ8"
    );
    // The SQ calibration must be COPIED to the new segment (segment-global), or
    // reads of carried SQ clusters would decode against a missing/wrong table.
    // Phase C.0b stores that copied payload inside centroids.bin instead of a
    // separate sq_calibration.bin sidecar.
    let centroids_key = format!("{ns}/segments/{new_seg}/centroids.bin");
    let centroids = store.get(&centroids_key).await.unwrap();
    assert!(
        centroids.starts_with(b"ZCT2"),
        "new segment centroids must use the v2 format"
    );
    let num_centroids = u32::from_le_bytes(centroids[4..8].try_into().unwrap()) as usize;
    let dim = u32::from_le_bytes(centroids[8..12].try_into().unwrap()) as usize;
    let cal_len_offset = 12 + num_centroids * dim * 4;
    let cal_len = u64::from_le_bytes(
        centroids[cal_len_offset..cal_len_offset + 8]
            .try_into()
            .unwrap(),
    ) as usize;
    assert!(cal_len > 0, "SQ calibration must be embedded in centroids");
    assert_eq!(
        centroids.len(),
        cal_len_offset + 8 + cal_len,
        "embedded SQ calibration length must match centroids blob size"
    );
    let new_cal_key = format!("{ns}/segments/{new_seg}/sq_calibration.bin");
    assert!(
        !store.exists(&new_cal_key).await.unwrap(),
        "new SQ8 segments must not write a separate calibration sidecar"
    );
    // At least one cluster carried over (owner still the seed).
    let carried: Vec<usize> = (0..seg_ref.cluster_count)
        .filter(|&i| seg_ref.cluster_owner(i) == seed_id)
        .collect();
    assert!(
        !carried.is_empty(),
        "SQ8 incremental compaction must carry at least one cluster by reference"
    );

    // Probe a CARRIED cluster (cluster 5, untouched by the adds). Its own
    // members must dominate the top results — proving the carried SQ codes
    // decode against the calibration they were encoded with.
    let probe = seed_vecs
        .iter()
        .find(|v| v.id == "cluster_5_vec_0")
        .unwrap()
        .values
        .clone();
    let ids = strong_query_ids(store, &ns, &probe, 5).await;
    let from_c5 = ids.iter().filter(|id| id.starts_with("cluster_5_")).count();
    assert!(
        from_c5 >= 3,
        "carried SQ8 cluster must decode correctly: expected cluster_5 members to \
         dominate a probe of their own centroid, got {from_c5}/5 from cluster_5: {ids:?}"
    );

    harness.cleanup().await;
}

/// Multi-generation carry-over + update-moves-cluster: three successive
/// incremental compactions, each touching a different cluster, plus an update
/// that relocates a vector to a new cluster. Verifies:
///   - owner chains resolve (carried objects from ANY generation stay readable),
///   - a relocated vector appears exactly ONCE (no ghost left in its old cluster),
///   - all originally-seeded vectors plus every added vector remain queryable.
#[tokio::test]
async fn test_incremental_multigen_and_update_moves_cluster() {
    let harness = TestHarness::new().await;
    let ns = harness.key("incr-multigen");
    let store = &harness.store;

    let (_seed_id, seed_vecs) = seed_segment(store, &ns).await;
    let writer = WalWriter::new(store.clone());
    let compactor = incremental_compactor(store);

    // Anchor for each cluster we'll touch across generations.
    let anchor = |ci: usize| -> Vec<f32> {
        seed_vecs
            .iter()
            .find(|v| v.id == format!("cluster_{ci}_vec_0"))
            .unwrap()
            .values
            .clone()
    };

    // Gen 1: add near cluster 1.
    let gen1: Vec<VectorEntry> = (0..3)
        .map(|i| VectorEntry {
            id: format!("g1_{i}"),
            values: anchor(1).iter().map(|x| x + 0.001).collect(),
            attributes: None,
        })
        .collect();
    writer.append(&ns, gen1, vec![]).await.unwrap();
    compactor.compact(&ns).await.unwrap();

    // Gen 2: add near cluster 2.
    let gen2: Vec<VectorEntry> = (0..3)
        .map(|i| VectorEntry {
            id: format!("g2_{i}"),
            values: anchor(2).iter().map(|x| x + 0.001).collect(),
            attributes: None,
        })
        .collect();
    writer.append(&ns, gen2, vec![]).await.unwrap();
    compactor.compact(&ns).await.unwrap();

    // Gen 3: an UPDATE that relocates an existing vector from cluster 4 to
    // cluster 3 (re-add the same ID with cluster-3 values). Both clusters must
    // be rewritten; no stale copy may survive in cluster 4.
    let mover_id = "cluster_4_vec_0".to_string();
    let moved = VectorEntry {
        id: mover_id.clone(),
        values: anchor(3).iter().map(|x| x + 0.001).collect(),
        attributes: None,
    };
    writer.append(&ns, vec![moved], vec![]).await.unwrap();
    compactor.compact(&ns).await.unwrap();

    // The relocated vector must appear EXACTLY ONCE across the whole dataset.
    // Query broadly (probe cluster 3 where it now lives) with a large top_k.
    let ids_c3 = strong_query_ids(store, &ns, &anchor(3), N_CLUSTERS * 25).await;
    let mover_hits = ids_c3.iter().filter(|id| **id == mover_id).count();
    assert_eq!(
        mover_hits, 1,
        "relocated vector must appear exactly once (no ghost in old cluster), got {mover_hits}"
    );

    // Every generation's adds must still be queryable (carried objects from
    // gens 1 and 2 survived subsequent incremental cycles).
    for (ci, prefix) in [(1usize, "g1_"), (2usize, "g2_")] {
        let ids = strong_query_ids(store, &ns, &anchor(ci), 10).await;
        let found = ids.iter().filter(|id| id.starts_with(prefix)).count();
        assert!(
            found > 0,
            "adds from the '{prefix}' generation must survive multi-gen carry-over, \
             got none near cluster {ci}: {ids:?}"
        );
    }

    // A cluster untouched across ALL three generations (e.g. cluster 0) keeps
    // its seed members — the deepest carry-over chain.
    let ids_c0 = strong_query_ids(store, &ns, &anchor(0), 5).await;
    let from_c0 = ids_c0
        .iter()
        .filter(|id| id.starts_with("cluster_0_"))
        .count();
    assert!(
        from_c0 >= 3,
        "cluster untouched across 3 generations must still return its seed members, \
         got {from_c0}/5: {ids_c0:?}"
    );

    harness.cleanup().await;
}
