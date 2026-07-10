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
use common::counting::{counting_store, ArtifactClass};
use common::harness::TestHarness;
use common::vectors::clustered_vectors;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Range;

use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig};
use zeppelin::error::ZeppelinError;
use zeppelin::index::ivf_flat::build::{attrs_key, build_ivf_flat};
use zeppelin::index::ivf_flat::membership::{deserialize_membership, MembershipData};
use zeppelin::index::quantization::sq::{serialize_sq_cluster, SqCalibration};
use zeppelin::query::{execute_query, QueryParams};
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::{AttributeValue, ConsistencyLevel, DistanceMetric, VectorEntry};
use zeppelin::wal::manifest::{
    ClusterDataObjectRef, Manifest, MembershipRef, SegmentRef, SketchRef,
};
use zeppelin::wal::{WalReader, WalWriter};

const DIM: usize = 16;
const N_CLUSTERS: usize = 6;
const BASELINE_CLUSTERS: usize = 16;
const BASELINE_VECTORS_PER_CLUSTER: usize = 4;
const SKETCH_V4_HEADER_LEN: usize = 44;
const SKETCH_V4_ROTATION_SCHEME: u32 = 1;
const SKETCH_V4_BIT_WIDTH: u32 = 2;

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
        common::default_gc_upload_window(),
    )
}

fn baseline_compactor(store: &ZeppelinStore) -> Compactor {
    let wal_reader = WalReader::new(store.clone());
    let compaction_config = CompactionConfig {
        max_wal_fragments_before_compact: 1,
        retrain_imbalance_threshold: 1000.0,
        ..Default::default()
    };
    let indexing_config = IndexingConfig {
        default_num_centroids: BASELINE_CLUSTERS,
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
        common::default_gc_upload_window(),
    )
}

/// Snapshot (key -> ETag) for every object under a segment prefix.
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

fn cluster_data_key(ns: &str, segment: &SegmentRef, cluster_idx: usize) -> String {
    if let Some(object_ref) = segment
        .cluster_objects
        .iter()
        .find(|object_ref| object_ref.clusters.contains(&cluster_idx))
    {
        return object_ref.key.clone();
    }

    let owner = segment.cluster_owner(cluster_idx);
    format!("{ns}/segments/{owner}/cluster_{cluster_idx}.bin")
}

async fn decoded_membership(
    store: &ZeppelinStore,
    membership_ref: &MembershipRef,
) -> (MembershipData, BTreeMap<String, u32>) {
    let bytes = store.get(&membership_ref.key).await.unwrap();
    assert_eq!(membership_ref.size_bytes, bytes.len() as u64);
    let decoded = deserialize_membership(&bytes).unwrap();
    assert_eq!(membership_ref.entry_count, decoded.entries.len() as u64);
    let map = decoded.entries.iter().cloned().collect();
    (decoded, map)
}

async fn active_segment_ref(store: &ZeppelinStore, ns: &str) -> SegmentRef {
    let manifest = Manifest::read(store, ns).await.unwrap().unwrap();
    let active = manifest.active_segment.as_ref().unwrap();
    manifest
        .segments
        .iter()
        .find(|segment| &segment.id == active)
        .unwrap()
        .clone()
}

async fn sketch_bytes(store: &ZeppelinStore, segment: &SegmentRef) -> Bytes {
    let sketch_ref = segment
        .sketch
        .as_ref()
        .expect("segment must have a sketch ref");
    let bytes = store.get(&sketch_ref.key).await.unwrap();
    assert_eq!(sketch_ref.size_bytes, bytes.len() as u64);
    bytes
}

async fn actual_cluster_membership(
    store: &ZeppelinStore,
    ns: &str,
    segment: &SegmentRef,
) -> BTreeMap<String, u32> {
    let mut membership = BTreeMap::new();
    if segment.cluster_objects.is_empty() {
        for cluster_idx in 0..segment.cluster_count {
            let key = cluster_data_key(ns, segment, cluster_idx);
            let bytes = store.get(&key).await.unwrap();
            for id in decode_cluster_ids(&bytes) {
                assert!(
                    membership.insert(id, cluster_idx as u32).is_none(),
                    "each vector id appears in exactly one cluster"
                );
            }
        }
        return membership;
    }

    for object_ref in &segment.cluster_objects {
        let bytes = store.get(&object_ref.key).await.unwrap();
        for cluster_idx in &object_ref.clusters {
            let section = grouped_full_section(&bytes, *cluster_idx, object_ref);
            for id in decode_cluster_ids(section) {
                assert!(
                    membership.insert(id, *cluster_idx as u32).is_none(),
                    "each vector id appears in exactly one cluster"
                );
            }
        }
    }
    membership
}

fn grouped_full_section<'a>(
    data: &'a [u8],
    cluster_idx: usize,
    object_ref: &ClusterDataObjectRef,
) -> &'a [u8] {
    let ranges = grouped_full_ranges(data).unwrap_or_else(|| {
        panic!(
            "manifest cluster object {} must contain grouped data",
            object_ref.key
        )
    });
    let range = ranges
        .into_iter()
        .find(|(idx, _)| *idx == cluster_idx)
        .unwrap_or_else(|| {
            panic!(
                "manifest cluster object {} missing cluster {cluster_idx}",
                object_ref.key
            )
        })
        .1;
    &data[range]
}

fn grouped_full_ranges(data: &[u8]) -> Option<Vec<(usize, Range<usize>)>> {
    if data.starts_with(b"ZBP1") {
        let entry_count = read_u32(data, 4) as usize;
        let mut sections = Vec::with_capacity(entry_count);
        for entry_idx in 0..entry_count {
            let base = 8 + entry_idx * 20;
            let cluster_idx = read_u32(data, base) as usize;
            let offset = read_u64(data, base + 4) as usize;
            let len = read_u64(data, base + 12) as usize;
            sections.push((cluster_idx, offset..offset + len));
        }
        return Some(sections);
    }
    if data.len() >= 4 && &data[0..3] == b"ZBP" && data[3] == 4 {
        let entry_count = read_u32(data, 4) as usize;
        let mut sections = Vec::with_capacity(entry_count);
        for entry_idx in 0..entry_count {
            let base = 8 + entry_idx * 36;
            let cluster_idx = read_u32(data, base) as usize;
            let offset = read_u64(data, base + 20) as usize;
            let len = read_u64(data, base + 28) as usize;
            sections.push((cluster_idx, offset..offset + len));
        }
        return Some(sections);
    }
    None
}

fn decode_cluster_ids(data: &[u8]) -> Vec<String> {
    let data = full_cluster_section(data);
    let n = read_u32(data, 0) as usize;
    let dim = read_u32(data, 4) as usize;
    let mut offset = 8usize;
    let mut ids = Vec::with_capacity(n);
    for _ in 0..n {
        let id_len = read_u32(data, offset) as usize;
        offset += 4;
        let id = std::str::from_utf8(&data[offset..offset + id_len])
            .unwrap()
            .to_string();
        offset += id_len;
        offset += dim * 4;
        ids.push(id);
    }
    ids
}

fn full_cluster_section(data: &[u8]) -> &[u8] {
    if data.starts_with(b"ZCL2") {
        let offset = read_u64(data, 20) as usize;
        let len = read_u64(data, 28) as usize;
        &data[offset..offset + len]
    } else {
        data
    }
}

fn read_u32(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
}

fn read_u64(data: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap())
}

fn read_f32(data: &[u8], offset: usize) -> f32 {
    f32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
}

#[derive(Clone, Debug)]
struct RawSketchLayout {
    dim: usize,
    code_dims: usize,
    cluster_count: usize,
    vector_count: usize,
    rotation_seed: u64,
    rotation_scheme: u32,
    bit_width: u32,
    attr_range: Range<usize>,
    counts: Vec<usize>,
    rows_range: Range<usize>,
    row_bytes: usize,
}

fn raw_sketch_layout(data: &[u8]) -> RawSketchLayout {
    assert!(data.starts_with(b"ZSK1"), "sketch magic must match");
    assert_eq!(read_u32(data, 4), 4, "tests cover the v4 sketch format");
    let dim = read_u32(data, 8) as usize;
    let code_dims = read_u32(data, 12) as usize;
    let cluster_count = read_u32(data, 16) as usize;
    let vector_count = read_u64(data, 20) as usize;
    let rotation_seed = read_u64(data, 28);
    let rotation_scheme = read_u32(data, 36);
    let bit_width = read_u32(data, 40);
    assert!(code_dims >= dim, "v4 code_dims must cover the logical dim");
    assert_eq!(
        code_dims % 256,
        0,
        "v4 code_dims must use 256-dimension blocks"
    );
    assert_eq!(
        rotation_scheme, SKETCH_V4_ROTATION_SCHEME,
        "v4 rotation scheme must remain pinned"
    );
    assert_eq!(
        bit_width, SKETCH_V4_BIT_WIDTH,
        "v4 bit width must remain pinned"
    );
    let attr_start = SKETCH_V4_HEADER_LEN;
    let attr_len = cluster_count.div_ceil(8);
    let counts_start = attr_start + attr_len;
    let mut counts = Vec::with_capacity(cluster_count);
    for cluster_idx in 0..cluster_count {
        counts.push(read_u32(data, counts_start + cluster_idx * 4) as usize);
    }
    let rows_start = counts_start + cluster_count * 4;
    let row_bytes = code_dims / 8 * bit_width as usize + 2 * std::mem::size_of::<f32>();
    let rows_len = vector_count * row_bytes;
    assert_eq!(
        data.len(),
        rows_start + rows_len,
        "sketch length must match header/counts"
    );
    RawSketchLayout {
        dim,
        code_dims,
        cluster_count,
        vector_count,
        rotation_seed,
        rotation_scheme,
        bit_width,
        attr_range: attr_start..attr_start + attr_len,
        counts,
        rows_range: rows_start..rows_start + rows_len,
        row_bytes,
    }
}

fn raw_sketch_row_section<'a>(
    data: &'a [u8],
    layout: &RawSketchLayout,
    cluster_idx: usize,
) -> &'a [u8] {
    let row_start: usize = layout.counts[..cluster_idx].iter().sum();
    let row_end = row_start + layout.counts[cluster_idx];
    let start = layout.rows_range.start + row_start * layout.row_bytes;
    let end = layout.rows_range.start + row_end * layout.row_bytes;
    &data[start..end]
}

fn raw_attr_bit(data: &[u8], layout: &RawSketchLayout, cluster_idx: usize) -> bool {
    let bitset = &data[layout.attr_range.clone()];
    bitset[cluster_idx / 8] & (1 << (cluster_idx % 8)) != 0
}

/// Builds a frozen, structurally valid ZSK1 v3 object for migration coverage.
fn frozen_v3_sketch(dim: usize, cluster_counts: &[usize]) -> Bytes {
    let subquantizers = dim;
    let vector_count: usize = cluster_counts.iter().sum();
    let mut out = Vec::new();
    out.extend_from_slice(b"ZSK1");
    out.extend_from_slice(&3u32.to_le_bytes());
    out.extend_from_slice(&(dim as u32).to_le_bytes());
    out.extend_from_slice(&(subquantizers as u32).to_le_bytes());
    out.extend_from_slice(&(cluster_counts.len() as u32).to_le_bytes());
    out.extend_from_slice(&(vector_count as u64).to_le_bytes());
    out.resize(out.len() + dim * 256 * std::mem::size_of::<f32>(), 0);
    out.resize(out.len() + cluster_counts.len().div_ceil(8), 0);
    for &count in cluster_counts {
        out.extend_from_slice(&(count as u32).to_le_bytes());
    }
    out.resize(out.len() + vector_count * subquantizers, 0);
    Bytes::from(out)
}

/// Replaces only the active segment's sketch/bootstrap metadata with a frozen
/// v3 sketch while keeping its authoritative centroids and membership intact.
async fn install_frozen_v3_sketch(store: &ZeppelinStore, ns: &str) -> SketchRef {
    let segment = active_segment_ref(store, ns).await;
    let membership_ref = segment
        .membership
        .as_ref()
        .expect("modern fixture must have membership");
    let (membership, _) = decoded_membership(store, membership_ref).await;
    let mut cluster_counts = vec![0usize; segment.cluster_count];
    for (_, cluster_idx) in membership.entries {
        cluster_counts[cluster_idx as usize] += 1;
    }

    let bytes = frozen_v3_sketch(DIM, &cluster_counts);
    let key = format!("{ns}/segments/{}/coarse_sketch_v3_fixture.bin", segment.id);
    store.put(&key, bytes.clone()).await.unwrap();
    let sketch_ref = SketchRef {
        key,
        version: 3,
        code_dims: DIM,
        bytes_per_vector: DIM,
        size_bytes: bytes.len() as u64,
        rotation_seed: None,
    };

    let mut manifest = Manifest::read(store, ns).await.unwrap().unwrap();
    let active = manifest
        .segments
        .iter_mut()
        .find(|candidate| candidate.id == segment.id)
        .unwrap();
    active.sketch = Some(sketch_ref.clone());
    active.bootstrap = None;
    manifest.write(store, ns).await.unwrap();
    sketch_ref
}

/// Publishes a new immutable copy of the active v4 sketch with an unsupported
/// bit-width header, then points the active manifest at that corrupt object.
async fn install_v4_sketch_with_unsupported_width(store: &ZeppelinStore, ns: &str) -> SegmentRef {
    let segment = active_segment_ref(store, ns).await;
    let old_sketch_ref = segment
        .sketch
        .as_ref()
        .expect("modern fixture must have a v4 sketch");
    let old_bytes = store.get(&old_sketch_ref.key).await.unwrap();
    assert_eq!(read_u32(&old_bytes, 4), 4);
    assert_eq!(read_u32(&old_bytes, 40), SKETCH_V4_BIT_WIDTH);

    let mut corrupt = old_bytes.to_vec();
    corrupt[40..44].copy_from_slice(&1u32.to_le_bytes());
    let corrupt = Bytes::from(corrupt);
    let key = format!(
        "{ns}/segments/{}/coarse_sketch_v4_unsupported_width.bin",
        segment.id
    );
    store.put(&key, corrupt.clone()).await.unwrap();

    let mut corrupt_ref = old_sketch_ref.clone();
    corrupt_ref.key = key;
    corrupt_ref.size_bytes = corrupt.len() as u64;
    let mut manifest = Manifest::read(store, ns).await.unwrap().unwrap();
    let active = manifest
        .segments
        .iter_mut()
        .find(|candidate| candidate.id == segment.id)
        .unwrap();
    active.sketch = Some(corrupt_ref);
    // Avoid a stale embedded v4 copy masking the manifest-selected corrupt
    // sketch; legacy separate metadata objects remain authoritative here.
    active.bootstrap = None;
    manifest.write(store, ns).await.unwrap();
    segment
}

/// Makes the manifest's v4 rotation seed disagree with the immutable object.
async fn install_v4_manifest_seed_mismatch(store: &ZeppelinStore, ns: &str) {
    let segment = active_segment_ref(store, ns).await;
    let mut manifest = Manifest::read(store, ns).await.unwrap().unwrap();
    let active = manifest
        .segments
        .iter_mut()
        .find(|candidate| candidate.id == segment.id)
        .unwrap();
    let sketch_ref = active
        .sketch
        .as_mut()
        .expect("modern fixture must have a v4 sketch");
    sketch_ref.rotation_seed = Some(
        sketch_ref
            .rotation_seed
            .expect("v4 sketch ref must carry a rotation seed")
            ^ 1,
    );
    manifest.write(store, ns).await.unwrap();
}

fn decode_cluster_vectors(data: &[u8]) -> Vec<(String, Vec<f32>)> {
    let data = full_cluster_section(data);
    let n = read_u32(data, 0) as usize;
    let dim = read_u32(data, 4) as usize;
    let mut offset = 8usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let id_len = read_u32(data, offset) as usize;
        offset += 4;
        let id = std::str::from_utf8(&data[offset..offset + id_len])
            .unwrap()
            .to_string();
        offset += id_len;
        let mut values = Vec::with_capacity(dim);
        for _ in 0..dim {
            values.push(read_f32(data, offset));
            offset += 4;
        }
        out.push((id, values));
    }
    out
}

async fn persisted_cluster_vectors_and_attrs(
    store: &ZeppelinStore,
    ns: &str,
    segment: &SegmentRef,
) -> (
    Vec<Vec<Vec<f32>>>,
    Vec<Vec<Option<HashMap<String, AttributeValue>>>>,
) {
    let mut cluster_vecs = Vec::with_capacity(segment.cluster_count);
    let mut cluster_attrs = Vec::with_capacity(segment.cluster_count);
    for cluster_idx in 0..segment.cluster_count {
        let cvec_key = cluster_data_key(ns, segment, cluster_idx);
        let cvec_bytes = store.get(&cvec_key).await.unwrap();
        let cluster_bytes = if segment.cluster_objects.is_empty() {
            cvec_bytes.as_ref()
        } else {
            let object_ref = segment
                .cluster_objects
                .iter()
                .find(|object_ref| object_ref.clusters.contains(&cluster_idx))
                .unwrap();
            grouped_full_section(&cvec_bytes, cluster_idx, object_ref)
        };
        let decoded = decode_cluster_vectors(cluster_bytes);
        cluster_vecs.push(decoded.into_iter().map(|(_, values)| values).collect());

        let owner = segment.cluster_owner(cluster_idx);
        let attrs_bytes = store.get(&attrs_key(ns, owner, cluster_idx)).await.unwrap();
        let attrs: Vec<Option<HashMap<String, AttributeValue>>> =
            serde_json::from_slice(&attrs_bytes).unwrap();
        cluster_attrs.push(attrs);
    }
    (cluster_vecs, cluster_attrs)
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
        sketch: None,
        cluster_objects: index.cluster_objects().to_vec(),
        bootstrap: None,
        membership: None,
    });
    manifest.write(store, ns).await.unwrap();
    (seg_id.to_string(), vectors)
}

/// Seed through the compactor so the active SegmentRef contains the modern
/// sketch/bootstrap/membership refs that incremental stitching consumes.
async fn seed_modern_segment(store: &ZeppelinStore, ns: &str) -> (String, Vec<VectorEntry>) {
    Manifest::new().write(store, ns).await.unwrap();
    let (vectors, _centroids) = clustered_vectors(N_CLUSTERS, 20, DIM, 0.01);
    seed_modern_vectors(store, ns, vectors, N_CLUSTERS).await
}

async fn seed_modern_vectors(
    store: &ZeppelinStore,
    ns: &str,
    vectors: Vec<VectorEntry>,
    n_clusters: usize,
) -> (String, Vec<VectorEntry>) {
    Manifest::new().write(store, ns).await.unwrap();
    WalWriter::new(store.clone())
        .append(ns, vectors.clone(), vec![])
        .await
        .unwrap();

    let result = compactor_with_clusters(store, n_clusters)
        .compact(ns)
        .await
        .unwrap();
    let segment_id = result.segment_id.unwrap();
    let manifest = Manifest::read(store, ns).await.unwrap().unwrap();
    let segment = manifest
        .segments
        .iter()
        .find(|segment| segment.id == segment_id)
        .unwrap();
    assert!(
        segment.sketch.is_some(),
        "modern seed must expose a sketch ref for incremental stitching"
    );
    assert!(
        segment.bootstrap.is_some(),
        "modern seed must expose a bootstrap ref"
    );
    assert!(
        segment.membership.is_some(),
        "modern seed must expose a membership ref"
    );
    (segment_id, vectors)
}

fn compactor_with_clusters(store: &ZeppelinStore, n_clusters: usize) -> Compactor {
    let wal_reader = WalReader::new(store.clone());
    let compaction_config = CompactionConfig {
        max_wal_fragments_before_compact: 1,
        retrain_imbalance_threshold: 1000.0,
        ..Default::default()
    };
    let indexing_config = IndexingConfig {
        default_num_centroids: n_clusters,
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
        common::default_gc_upload_window(),
    )
}

async fn seed_modern_flat_segment(
    store: &ZeppelinStore,
    ns: &str,
    n_clusters: usize,
    vectors_per_cluster: usize,
) -> (SegmentRef, Vec<VectorEntry>) {
    let (_seed_id, seed_vecs, _cluster_bytes, _attrs_bytes) =
        seed_legacy_flat_segment(store, ns, n_clusters, vectors_per_cluster).await;
    let anchor = seed_vecs
        .iter()
        .find(|vector| vector.id == "cluster_0_vec_0")
        .unwrap()
        .values
        .clone();
    WalWriter::new(store.clone())
        .append(
            ns,
            vec![VectorEntry {
                id: "self_heal_seed_added".to_string(),
                values: anchor.iter().map(|x| x + 0.001).collect(),
                attributes: None,
            }],
            vec![],
        )
        .await
        .unwrap();
    compactor_with_clusters(store, n_clusters)
        .compact(ns)
        .await
        .unwrap();

    let segment = active_segment_ref(store, ns).await;
    assert!(
        segment.cluster_objects.is_empty(),
        "modern flat fixture must keep the legacy per-cluster layout"
    );
    assert!(
        segment.membership.is_some(),
        "modern flat fixture must have membership for bounded reads"
    );
    assert!(
        segment.sketch.is_some(),
        "modern flat fixture must have a sketch for stitching"
    );
    (segment, seed_vecs)
}

async fn seed_modern_grouped_segment(
    store: &ZeppelinStore,
    ns: &str,
    n_clusters: usize,
    vectors_per_cluster: usize,
) -> (SegmentRef, Vec<VectorEntry>) {
    let (vectors, _centroids) = clustered_vectors(n_clusters, vectors_per_cluster, DIM, 0.01);
    let (_segment_id, vectors) = seed_modern_vectors(store, ns, vectors, n_clusters).await;
    let segment = active_segment_ref(store, ns).await;
    assert!(
        !segment.cluster_objects.is_empty(),
        "modern grouped fixture must use cluster_objects"
    );
    assert!(
        segment.membership.is_some(),
        "modern grouped fixture must have membership for bounded reads"
    );
    (segment, vectors)
}

/// Seed a legacy per-cluster IVF-Flat segment so the compaction read baseline
/// observes one cluster GET per logical cluster. Grouped cluster objects are
/// covered elsewhere; this test freezes the pre-2C.3 O(dataset) profile.
async fn seed_legacy_flat_segment(
    store: &ZeppelinStore,
    ns: &str,
    n_clusters: usize,
    vectors_per_cluster: usize,
) -> (String, Vec<VectorEntry>, u64, u64) {
    let (vectors, centroids) = clustered_vectors(n_clusters, vectors_per_cluster, DIM, 0.01);
    let seg_id = "seg_seed";
    store
        .put(
            &format!("{ns}/segments/{seg_id}/centroids.bin"),
            legacy_centroids_bytes(&centroids, DIM),
        )
        .await
        .unwrap();

    let mut cluster_bytes_total = 0u64;
    let mut attrs_bytes_total = 0u64;
    for cluster_idx in 0..n_clusters {
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

        let cluster_bytes = legacy_cluster_bytes(&ids, &values, DIM);
        cluster_bytes_total += cluster_bytes.len() as u64;
        store
            .put(
                &format!("{ns}/segments/{seg_id}/cluster_{cluster_idx}.bin"),
                cluster_bytes,
            )
            .await
            .unwrap();

        let attrs_bytes = Bytes::from(serde_json::to_vec(&attrs).unwrap());
        attrs_bytes_total += attrs_bytes.len() as u64;
        store
            .put(
                &format!("{ns}/segments/{seg_id}/attrs_{cluster_idx}.bin"),
                attrs_bytes,
            )
            .await
            .unwrap();
    }

    let mut manifest = Manifest::new();
    manifest.add_segment(SegmentRef {
        id: seg_id.to_string(),
        vector_count: vectors.len(),
        cluster_count: n_clusters,
        quantization: zeppelin::index::quantization::QuantizationType::None,
        hierarchical: false,
        bitmap_fields: Vec::new(),
        fts_fields: Vec::new(),
        has_global_fts: false,
        cluster_owners: Vec::new(),
        sketch: None,
        cluster_objects: Vec::new(),
        bootstrap: None,
        membership: None,
    });
    manifest.write(store, ns).await.unwrap();

    (
        seg_id.to_string(),
        vectors,
        cluster_bytes_total,
        attrs_bytes_total,
    )
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
        common::default_gc_upload_window(),
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
        sketch: None,
        cluster_objects: Vec::new(),
        bootstrap: None,
        membership: None,
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
        rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: None,
        include_attributes: true,
    })
    .await
    .unwrap();
    resp.results.into_iter().map(|r| r.id).collect()
}

async fn ordered_query_ids(
    store: &ZeppelinStore,
    ns: &str,
    query: &[f32],
    top_k: usize,
    nprobe: usize,
) -> Vec<String> {
    let reader = WalReader::new(store.clone());
    let resp = execute_query(QueryParams {
        store,
        wal_reader: &reader,
        namespace: ns,
        query,
        top_k,
        nprobe,
        filter: None,
        consistency: ConsistencyLevel::Strong,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 1,
        rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: None,
        include_attributes: true,
    })
    .await
    .unwrap();
    resp.results.into_iter().map(|r| r.id).collect()
}

fn cluster_for_id(membership: &BTreeMap<String, u32>, id: &str) -> usize {
    *membership
        .get(id)
        .unwrap_or_else(|| panic!("membership missing id {id}")) as usize
}

fn cluster_object_for(segment: &SegmentRef, cluster_idx: usize) -> Option<&ClusterDataObjectRef> {
    segment
        .cluster_objects
        .iter()
        .find(|object_ref| object_ref.clusters.contains(&cluster_idx))
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
        let cvec_key = cluster_data_key(&ns, seg_ref, i);
        let (_d, etag) = store.get_with_meta(&cvec_key).await.unwrap();
        assert_eq!(
            etag.as_deref(),
            before.get(&cvec_key).map(|s| s.as_str()),
            "B2: carried cluster {i} must keep its exact object (same ETag)"
        );
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn test_full_compaction_writes_segment_membership_artifact() {
    let harness = TestHarness::new().await;
    let ns = harness.key("full-membership");
    let store = &harness.store;

    Manifest::new().write(store, &ns).await.unwrap();
    let (vectors, _centroids) = clustered_vectors(N_CLUSTERS, 10, DIM, 0.01);
    WalWriter::new(store.clone())
        .append(&ns, vectors.clone(), vec![])
        .await
        .unwrap();

    let result = incremental_compactor(store).compact(&ns).await.unwrap();
    let new_seg = result
        .segment_id
        .expect("full compaction produces a segment");
    assert_eq!(result.vectors_compacted, vectors.len());

    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    let seg_ref = manifest
        .segments
        .iter()
        .find(|segment| segment.id == new_seg)
        .unwrap();
    let membership_ref = seg_ref
        .membership
        .as_ref()
        .expect("new IVF-Flat full compaction segment must record membership");
    assert!(
        store.exists(&membership_ref.key).await.unwrap(),
        "manifest membership object must exist on S3"
    );

    let (decoded, decoded_map) = decoded_membership(store, membership_ref).await;
    assert_eq!(decoded.cluster_count, seg_ref.cluster_count as u32);
    assert_eq!(membership_ref.entry_count, seg_ref.vector_count as u64);
    assert_eq!(decoded.entries.len(), vectors.len());
    assert_eq!(
        decoded_map,
        actual_cluster_membership(store, &ns, seg_ref).await,
        "membership must exactly match the segment's persisted id-to-cluster assignment"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn test_incremental_compaction_writes_segment_global_membership_artifact() {
    let harness = TestHarness::new().await;
    let ns = harness.key("incr-membership");
    let store = &harness.store;

    let (_seed_id, seed_vecs) = seed_segment(store, &ns).await;
    let anchor = &seed_vecs[0].values;
    let new_vecs: Vec<VectorEntry> = (0..5)
        .map(|i| VectorEntry {
            id: format!("membership_added_{i}"),
            values: anchor.iter().map(|x| x + 0.001).collect(),
            attributes: None,
        })
        .collect();
    WalWriter::new(store.clone())
        .append(&ns, new_vecs, vec![])
        .await
        .unwrap();

    let result = incremental_compactor(store).compact(&ns).await.unwrap();
    let new_seg = result
        .segment_id
        .expect("incremental compaction produces a segment");

    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    let seg_ref = manifest
        .segments
        .iter()
        .find(|segment| segment.id == new_seg)
        .unwrap();
    assert!(
        !seg_ref.cluster_owners.is_empty(),
        "fixture must exercise carried clusters and rewritten clusters"
    );
    let membership_ref = seg_ref
        .membership
        .as_ref()
        .expect("new IVF-Flat incremental segment must record membership");
    let (decoded, decoded_map) = decoded_membership(store, membership_ref).await;
    let actual_map = actual_cluster_membership(store, &ns, seg_ref).await;
    assert_eq!(decoded.cluster_count, seg_ref.cluster_count as u32);
    assert_eq!(membership_ref.entry_count, seg_ref.vector_count as u64);
    assert_eq!(decoded.entries.len(), seg_ref.vector_count);
    assert_eq!(
        decoded_map, actual_map,
        "incremental membership must cover carried and rewritten clusters under the new segment id"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn test_incremental_stitched_v4_sketch_preserves_full_carried_rows() {
    let harness = TestHarness::new().await;
    let ns = harness.key("incr-sketch-v4-full-rows");
    let store = &harness.store;

    let (old_seg, seed_vecs) = seed_modern_segment(store, &ns).await;
    let old_manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    let old_seg_ref = old_manifest
        .segments
        .iter()
        .find(|segment| segment.id == old_seg)
        .unwrap()
        .clone();
    let old_sketch_ref = old_seg_ref
        .sketch
        .as_ref()
        .expect("modern seed must have an old sketch ref");
    let old_sketch = store.get(&old_sketch_ref.key).await.unwrap();
    let old_layout = raw_sketch_layout(&old_sketch);

    let anchor = seed_vecs
        .iter()
        .find(|vector| vector.id == "cluster_0_vec_0")
        .unwrap()
        .values
        .clone();
    let new_vecs: Vec<VectorEntry> = (0..5)
        .map(|i| VectorEntry {
            id: format!("stitched_added_{i}"),
            values: anchor.iter().map(|x| x + 0.001).collect(),
            attributes: if i == 0 {
                Some(HashMap::from([(
                    "kind".to_string(),
                    AttributeValue::String("stitched".to_string()),
                )]))
            } else {
                None
            },
        })
        .collect();
    let added_ids: Vec<String> = new_vecs.iter().map(|vector| vector.id.clone()).collect();
    WalWriter::new(store.clone())
        .append(&ns, new_vecs, vec![])
        .await
        .unwrap();

    let result = incremental_compactor(store).compact(&ns).await.unwrap();
    let new_seg = result
        .segment_id
        .expect("incremental compaction must produce a new segment");
    assert_ne!(new_seg, old_seg);

    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    let new_seg_ref = manifest
        .segments
        .iter()
        .find(|segment| segment.id == new_seg)
        .unwrap();
    assert!(
        !new_seg_ref.cluster_owners.is_empty(),
        "fixture must carry at least one untouched cluster"
    );
    let new_sketch_ref = new_seg_ref
        .sketch
        .as_ref()
        .expect("incremental segment must write a sketch ref");
    let new_sketch = store.get(&new_sketch_ref.key).await.unwrap();
    assert_eq!(new_sketch_ref.size_bytes, new_sketch.len() as u64);

    let (cluster_vecs, cluster_attrs) =
        persisted_cluster_vectors_and_attrs(store, &ns, new_seg_ref).await;
    let new_layout = raw_sketch_layout(&new_sketch);
    assert_eq!(new_layout.dim, old_layout.dim);
    assert_eq!(new_layout.cluster_count, new_seg_ref.cluster_count);
    assert_eq!(
        new_layout.code_dims, old_layout.code_dims,
        "incremental stitching must keep the padded rotation dimension"
    );
    assert_eq!(
        new_layout.rotation_seed, old_layout.rotation_seed,
        "incremental stitching must keep the persisted rotation seed"
    );
    assert_eq!(
        new_layout.rotation_scheme, old_layout.rotation_scheme,
        "incremental stitching must keep the rotation scheme"
    );
    assert_eq!(
        new_layout.bit_width, old_layout.bit_width,
        "incremental stitching must keep the two-bit row format"
    );
    assert_eq!(
        new_layout.row_bytes, old_layout.row_bytes,
        "incremental stitching must keep the full row stride"
    );
    assert_eq!(
        new_layout.vector_count,
        new_layout.counts.iter().sum::<usize>(),
        "header vector_count must equal the sum of per-cluster counts"
    );
    assert_eq!(
        new_layout.counts,
        cluster_vecs.iter().map(Vec::len).collect::<Vec<_>>(),
        "sketch counts must match the persisted cluster vectors"
    );
    for (cluster_idx, attrs) in cluster_attrs.iter().enumerate() {
        assert_eq!(
            raw_attr_bit(&new_sketch, &new_layout, cluster_idx),
            attrs.iter().any(Option::is_some),
            "cluster {cluster_idx} attr bit must match persisted row attributes"
        );
    }

    let membership_ref = new_seg_ref
        .membership
        .as_ref()
        .expect("incremental segment must write membership");
    let (membership, _membership_map) = decoded_membership(store, membership_ref).await;
    let mut membership_counts = vec![0usize; new_seg_ref.cluster_count];
    for (_id, cluster_idx) in membership.entries {
        membership_counts[cluster_idx as usize] += 1;
    }
    assert_eq!(
        new_layout.counts, membership_counts,
        "sketch counts must match membership cluster_ids"
    );

    let mut carried_with_rows = None;
    let mut rewritten_with_more_rows = None;
    for cluster_idx in 0..new_seg_ref.cluster_count {
        let is_carried = new_seg_ref.cluster_owner(cluster_idx) != new_seg;
        if !is_carried {
            if new_layout.counts[cluster_idx] > old_layout.counts[cluster_idx] {
                rewritten_with_more_rows = Some(cluster_idx);
            }
            continue;
        }
        assert_eq!(
            raw_sketch_row_section(&new_sketch, &new_layout, cluster_idx),
            raw_sketch_row_section(&old_sketch, &old_layout, cluster_idx),
            "carried cluster {cluster_idx} must copy both bit planes and both correction factors byte-for-byte"
        );
        assert_eq!(
            new_layout.counts[cluster_idx], old_layout.counts[cluster_idx],
            "carried cluster {cluster_idx} must keep its old count"
        );
        assert_eq!(
            raw_attr_bit(&new_sketch, &new_layout, cluster_idx),
            raw_attr_bit(&old_sketch, &old_layout, cluster_idx),
            "carried cluster {cluster_idx} must keep its old attr bit"
        );
        if new_layout.counts[cluster_idx] > 0 {
            carried_with_rows = Some(cluster_idx);
        }
    }
    carried_with_rows.expect("fixture must carry a non-empty cluster");
    let rewritten_idx =
        rewritten_with_more_rows.expect("fixture must append rows to one rewritten cluster");
    assert_eq!(
        raw_sketch_row_section(&new_sketch, &new_layout, rewritten_idx).len(),
        new_layout.counts[rewritten_idx] * new_layout.row_bytes,
        "rewritten cluster must contain complete two-plane rows with both factors"
    );

    let query_ids = strong_query_ids(store, &ns, &anchor, N_CLUSTERS * 25).await;
    for added_id in added_ids {
        assert!(
            query_ids.contains(&added_id),
            "rewritten v4 rows must keep added vector {added_id} queryable"
        );
    }

    harness.cleanup().await;
}

/// A legacy v3 sketch is readable input, but a new compaction must publish v4.
#[tokio::test]
async fn test_incremental_compaction_rebuilds_v3_sketch_as_v4() {
    let harness = TestHarness::new().await;
    let ns = harness.key("incr-sketch-v3-to-v4");
    let store = &harness.store;

    let (_old_seg, seed_vecs) = seed_modern_segment(store, &ns).await;
    let old_sketch_ref = install_frozen_v3_sketch(store, &ns).await;
    assert_eq!(old_sketch_ref.version, 3);
    assert_eq!(old_sketch_ref.rotation_seed, None);

    let seed_vector = seed_vecs
        .iter()
        .find(|vector| vector.id == "cluster_0_vec_0")
        .unwrap();
    let anchor = seed_vector.values.clone();
    let v3_query_ids = strong_query_ids(store, &ns, &anchor, N_CLUSTERS * 25).await;
    assert!(
        v3_query_ids.contains(&seed_vector.id),
        "valid v3 sketch must load and serve a known seed vector before migration"
    );
    let added_id = "v3_migration_added".to_string();
    WalWriter::new(store.clone())
        .append(
            &ns,
            vec![VectorEntry {
                id: added_id.clone(),
                values: anchor.iter().map(|value| value + 0.001).collect(),
                attributes: None,
            }],
            vec![],
        )
        .await
        .unwrap();

    let result = incremental_compactor(store).compact(&ns).await.unwrap();
    let new_seg = result
        .segment_id
        .expect("v3 migration compaction must produce a segment");
    let new_segment = active_segment_ref(store, &ns).await;
    assert_eq!(new_segment.id, new_seg);
    let new_sketch_ref = new_segment
        .sketch
        .as_ref()
        .expect("v3 migration must publish a new sketch");
    assert_eq!(new_sketch_ref.version, 4);
    assert!(new_sketch_ref.rotation_seed.is_some());
    let new_sketch = sketch_bytes(store, &new_segment).await;
    let new_layout = raw_sketch_layout(&new_sketch);
    assert_eq!(new_layout.vector_count, new_segment.vector_count);

    let query_ids = strong_query_ids(store, &ns, &anchor, N_CLUSTERS * 25).await;
    assert!(
        query_ids.contains(&added_id),
        "v3-to-v4 rebuild must keep the new vector queryable"
    );

    harness.cleanup().await;
}

/// Corrupt active v4 format metadata must fail query load and compaction loud.
#[tokio::test]
async fn test_corrupt_v4_width_does_not_scan_or_publish_replacement_segment() {
    let harness = TestHarness::new().await;
    let ns = harness.key("corrupt-v4-width-fails-loud");
    let store = &harness.store;

    let (_seed_segment, seed_vecs) = seed_modern_segment(store, &ns).await;
    let old_active = install_v4_sketch_with_unsupported_width(store, &ns).await;
    let anchor = seed_vecs
        .iter()
        .find(|vector| vector.id == "cluster_0_vec_0")
        .unwrap()
        .values
        .clone();

    let reader = WalReader::new(store.clone());
    let query_result = execute_query(QueryParams {
        store,
        wal_reader: &reader,
        namespace: &ns,
        query: &anchor,
        top_k: 10,
        nprobe: N_CLUSTERS,
        filter: None,
        consistency: ConsistencyLevel::Strong,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 1,
        rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: None,
        include_attributes: false,
    })
    .await;
    let query_error = match query_result {
        Ok(_) => panic!("corrupt v4 query load must not fall through to scanning"),
        Err(error) => error,
    };
    assert!(
        query_error.to_string().contains("bit width"),
        "query must report the corrupt v4 width: {query_error}"
    );

    WalWriter::new(store.clone())
        .append(
            &ns,
            vec![VectorEntry {
                id: "corrupt_v4_incremental_added".to_string(),
                values: anchor.iter().map(|value| value + 0.001).collect(),
                attributes: None,
            }],
            vec![],
        )
        .await
        .unwrap();
    let compaction_result = incremental_compactor(store).compact(&ns).await;
    let compaction_error = match compaction_result {
        Ok(_) => panic!("corrupt active v4 must not be replaced through a rebuild fallback"),
        Err(error) => error,
    };
    assert!(
        compaction_error.to_string().contains("bit width"),
        "compaction must report the corrupt v4 width: {compaction_error}"
    );

    let active_after_failure = active_segment_ref(store, &ns).await;
    assert_eq!(
        active_after_failure.id, old_active.id,
        "failed compaction must not publish a replacement active segment"
    );

    harness.cleanup().await;
}

/// A v4 manifest seed that disagrees with its object must stop query loading.
#[tokio::test]
async fn test_v4_manifest_seed_mismatch_fails_query_instead_of_scanning() {
    let harness = TestHarness::new().await;
    let ns = harness.key("v4-seed-ref-mismatch-fails-query");
    let store = &harness.store;

    let (_seed_segment, seed_vecs) = seed_modern_segment(store, &ns).await;
    install_v4_manifest_seed_mismatch(store, &ns).await;
    let anchor = seed_vecs
        .iter()
        .find(|vector| vector.id == "cluster_0_vec_0")
        .unwrap()
        .values
        .clone();
    let reader = WalReader::new(store.clone());
    let query_result = execute_query(QueryParams {
        store,
        wal_reader: &reader,
        namespace: &ns,
        query: &anchor,
        top_k: 10,
        nprobe: N_CLUSTERS,
        filter: None,
        consistency: ConsistencyLevel::Strong,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 1,
        rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: None,
        include_attributes: false,
    })
    .await;
    let error = match query_result {
        Ok(_) => panic!("v4 seed/reference mismatch must not fall through to scanning"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("reference mismatch"),
        "query must report the v4 seed/reference mismatch: {error}"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn test_incremental_compaction_fails_when_referenced_sketch_is_missing() {
    let harness = TestHarness::new().await;
    let ns = harness.key("incr-sketch-missing-fails-loud");
    let store = &harness.store;

    let (old_segment_id, seed_vecs) = seed_modern_segment(store, &ns).await;
    let old_active = active_segment_ref(store, &ns).await;
    assert_eq!(old_active.id, old_segment_id);
    let old_sketch_ref = old_active.sketch.as_ref().unwrap().clone();
    store.delete(&old_sketch_ref.key).await.unwrap();

    let anchor = seed_vecs
        .iter()
        .find(|vector| vector.id == "cluster_0_vec_0")
        .unwrap()
        .values
        .clone();
    WalWriter::new(store.clone())
        .append(
            &ns,
            vec![VectorEntry {
                id: "missing_sketch_incremental_added".to_string(),
                values: anchor.iter().map(|value| value + 0.001).collect(),
                attributes: None,
            }],
            vec![],
        )
        .await
        .unwrap();

    let result = incremental_compactor(store).compact(&ns).await;
    let error = match result {
        Ok(_) => panic!("manifest-referenced missing sketch must not trigger a rebuild fallback"),
        Err(error) => error,
    };
    match error {
        ZeppelinError::CoarseSketch(message) => {
            assert!(
                message.contains("referenced resident sketch is missing"),
                "missing sketch diagnostic must identify the broken reference: {message}"
            );
            assert!(
                message.contains(&old_sketch_ref.key),
                "missing sketch diagnostic must name the immutable object key: {message}"
            );
        }
        other => panic!("missing referenced sketch must use CoarseSketch semantics, got {other}"),
    }

    let active_after_failure = active_segment_ref(store, &ns).await;
    assert_eq!(
        active_after_failure.id, old_active.id,
        "failed compaction must not publish a replacement active segment"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn test_incremental_stitched_sketch_multicycle_carries_sections_stably() {
    let harness = TestHarness::new().await;
    let ns = harness.key("incr-sketch-multicycle");
    let store = &harness.store;

    let (_seed_seg, seed_vecs) = seed_modern_segment(store, &ns).await;
    let writer = WalWriter::new(store.clone());
    let compactor = incremental_compactor(store);
    let anchor = |ci: usize| -> Vec<f32> {
        seed_vecs
            .iter()
            .find(|vector| vector.id == format!("cluster_{ci}_vec_0"))
            .unwrap()
            .values
            .clone()
    };

    let mut previous_segment = active_segment_ref(store, &ns).await;
    let mut previous_sketch = sketch_bytes(store, &previous_segment).await;
    let mut rewritten_clusters = Vec::new();

    for (cycle, cluster_hint) in [1usize, 2, 3].into_iter().enumerate() {
        let cycle_no = cycle + 1;
        let anchor_values = anchor(cluster_hint);
        let added_id = format!("stitch_cycle_{cycle_no}_added");
        writer
            .append(
                &ns,
                vec![VectorEntry {
                    id: added_id.clone(),
                    values: anchor_values.iter().map(|x| x + 0.001).collect(),
                    attributes: None,
                }],
                vec![],
            )
            .await
            .unwrap();

        let result = compactor.compact(&ns).await.unwrap();
        let new_seg = result
            .segment_id
            .expect("each incremental cycle must produce a segment");
        let new_segment = active_segment_ref(store, &ns).await;
        assert_eq!(new_segment.id, new_seg);
        let new_sketch = sketch_bytes(store, &new_segment).await;
        let old_layout = raw_sketch_layout(&previous_sketch);
        let new_layout = raw_sketch_layout(&new_sketch);

        let rewritten: Vec<usize> = (0..new_segment.cluster_count)
            .filter(|&cluster_idx| new_segment.cluster_owner(cluster_idx) == new_seg)
            .collect();
        assert_eq!(
            rewritten.len(),
            1,
            "cycle {cycle_no} must rewrite only the newly touched cluster, got {rewritten:?}"
        );
        rewritten_clusters.push(rewritten[0]);

        let mut carried = 0usize;
        for cluster_idx in 0..new_segment.cluster_count {
            if new_segment.cluster_owner(cluster_idx) == new_seg {
                continue;
            }
            carried += 1;
            assert_eq!(
                raw_sketch_row_section(&new_sketch, &new_layout, cluster_idx),
                raw_sketch_row_section(&previous_sketch, &old_layout, cluster_idx),
                "cycle {cycle_no} carried cluster {cluster_idx} must keep both bit planes and both correction factors stable"
            );
            assert_eq!(
                new_layout.counts[cluster_idx], old_layout.counts[cluster_idx],
                "cycle {cycle_no} carried cluster {cluster_idx} must keep its old count"
            );
        }
        assert!(
            carried > 0,
            "cycle {cycle_no} must carry at least one untouched cluster"
        );

        let membership_ref = new_segment
            .membership
            .as_ref()
            .expect("incremental segment must write membership");
        let (membership, _membership_map) = decoded_membership(store, membership_ref).await;
        let mut membership_counts = vec![0usize; new_segment.cluster_count];
        for (_id, cluster_idx) in membership.entries {
            membership_counts[cluster_idx as usize] += 1;
        }
        assert_eq!(
            new_layout.counts, membership_counts,
            "cycle {cycle_no} stitched counts must match membership"
        );
        assert_eq!(
            new_layout.vector_count,
            membership_counts.iter().sum::<usize>(),
            "cycle {cycle_no} header vector_count must be exact"
        );

        let ids = strong_query_ids(store, &ns, &anchor_values, N_CLUSTERS * 25).await;
        assert!(
            ids.contains(&added_id),
            "cycle {cycle_no} query results must include the newly added vector"
        );

        previous_segment = new_segment;
        previous_sketch = new_sketch;
    }

    rewritten_clusters.sort_unstable();
    rewritten_clusters.dedup();
    assert_eq!(
        rewritten_clusters.len(),
        3,
        "the fixture must touch different clusters across the three cycles"
    );
    assert_eq!(
        previous_segment.vector_count,
        seed_vecs.len() + 3,
        "final segment vector count must include all cycle additions"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn test_incremental_multicycle_bounded_reads_and_carried_object_fences() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let ns = harness.key("incr-bounded-multicycle-fences");

    let (_seed_segment, seed_vecs) = seed_modern_grouped_segment(&store, &ns, N_CLUSTERS, 20).await;
    let writer = WalWriter::new(store.clone());
    let compactor = compactor_with_clusters(&store, N_CLUSTERS);
    let anchor = |ci: usize| -> (&'static str, Vec<f32>) {
        let id = match ci {
            1 => "cluster_1_vec_0",
            2 => "cluster_2_vec_0",
            3 => "cluster_3_vec_0",
            _ => unreachable!("fixture touches clusters 1, 2, and 3"),
        };
        (
            id,
            seed_vecs
                .iter()
                .find(|vector| vector.id == id)
                .unwrap()
                .values
                .clone(),
        )
    };

    for (cycle, cluster_hint) in [1usize, 2, 3].into_iter().enumerate() {
        let cycle_no = cycle + 1;
        let old_segment = active_segment_ref(&store, &ns).await;
        let (_membership, membership_map) =
            decoded_membership(&harness.store, old_segment.membership.as_ref().unwrap()).await;
        let (anchor_id, anchor_values) = anchor(cluster_hint);
        let touched_cluster = cluster_for_id(&membership_map, anchor_id);
        let touched_object_key = cluster_object_for(&old_segment, touched_cluster)
            .map(|object_ref| object_ref.key.clone())
            .unwrap_or_else(|| cluster_data_key(&ns, &old_segment, touched_cluster));

        let mut old_object_etags = HashMap::new();
        for object_ref in &old_segment.cluster_objects {
            let (_bytes, etag) = harness.store.get_with_meta(&object_ref.key).await.unwrap();
            if let Some(etag) = etag {
                old_object_etags.insert(object_ref.key.clone(), etag);
            }
        }

        writer
            .append(
                &ns,
                vec![VectorEntry {
                    id: format!("bounded_cycle_{cycle_no}_added"),
                    values: anchor_values.iter().map(|x| x + 0.001).collect(),
                    attributes: None,
                }],
                vec![],
            )
            .await
            .unwrap();

        counter.reset();
        let result = compactor.compact(&ns).await.unwrap();
        let new_seg = result
            .segment_id
            .expect("each bounded cycle must produce a segment");
        let new_segment = active_segment_ref(&store, &ns).await;
        assert_eq!(new_segment.id, new_seg);

        let rewritten: Vec<usize> = (0..new_segment.cluster_count)
            .filter(|&cluster_idx| new_segment.cluster_owner(cluster_idx) == new_seg)
            .collect();
        assert_eq!(
            rewritten,
            vec![touched_cluster],
            "cycle {cycle_no} must rewrite exactly the newly touched cluster"
        );
        assert_eq!(
            counter.gets_for(ArtifactClass::Cluster),
            1,
            "cycle {cycle_no} must read only the touched physical cluster object"
        );
        assert_eq!(
            counter.gets_matching(&touched_object_key),
            1,
            "cycle {cycle_no} must read the touched object once"
        );
        for object_ref in &old_segment.cluster_objects {
            if object_ref.key == touched_object_key {
                continue;
            }
            assert_eq!(
                counter.gets_matching(&object_ref.key),
                0,
                "cycle {cycle_no} must not read untouched object {}",
                object_ref.key
            );
        }
        assert_eq!(
            counter.gets_for(ArtifactClass::Attrs),
            1,
            "cycle {cycle_no} must read attrs only for the touched cluster"
        );

        let manifest = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
        let live_cluster_object_keys: HashSet<String> = new_segment
            .cluster_objects
            .iter()
            .map(|object_ref| object_ref.key.clone())
            .collect();
        for key in &live_cluster_object_keys {
            assert!(
                !manifest.pending_deletes.contains(key),
                "cycle {cycle_no}: live carried object {key} must not be pending deletion"
            );
        }
        for (key, old_etag) in old_object_etags {
            if !live_cluster_object_keys.contains(&key) {
                continue;
            }
            let (_bytes, new_etag) = harness.store.get_with_meta(&key).await.unwrap();
            assert_eq!(
                new_etag.as_deref(),
                Some(old_etag.as_str()),
                "cycle {cycle_no}: carried object {key} must keep a stable ETag"
            );
        }

        let ids = ordered_query_ids(&store, &ns, &anchor_values, N_CLUSTERS * 25, N_CLUSTERS).await;
        assert!(
            ids.iter()
                .any(|id| id == &format!("bounded_cycle_{cycle_no}_added")),
            "cycle {cycle_no} exact query must include the new vector"
        );
    }

    harness.cleanup().await;
}

/// 2C.3 achieved bound: flat incremental compaction reads membership plus only
/// the touched cluster's vector and attrs objects, not every old cluster.
#[tokio::test]
async fn test_incremental_read_io_baseline_reads_all_clusters() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let ns = harness.key("incr-read-baseline");

    let (old_segment, seed_vecs) =
        seed_modern_flat_segment(&store, &ns, BASELINE_CLUSTERS, BASELINE_VECTORS_PER_CLUSTER)
            .await;
    let (_membership, membership_map) =
        decoded_membership(&store, old_segment.membership.as_ref().unwrap()).await;

    let anchor = seed_vecs
        .iter()
        .find(|vector| vector.id == "cluster_0_vec_0")
        .unwrap()
        .values
        .clone();
    let touched_cluster = cluster_for_id(&membership_map, "cluster_0_vec_0");
    let expected_cluster_key = cluster_data_key(&ns, &old_segment, touched_cluster);
    let expected_attrs_key = attrs_key(
        &ns,
        old_segment.cluster_owner(touched_cluster),
        touched_cluster,
    );
    let expected_cluster_bytes = harness
        .store
        .head(&expected_cluster_key)
        .await
        .unwrap()
        .size as u64;
    let expected_attrs_bytes = harness.store.head(&expected_attrs_key).await.unwrap().size as u64;
    let new_vec = VectorEntry {
        id: "baseline_added_one_cluster".to_string(),
        values: anchor.iter().map(|x| x + 0.001).collect(),
        attributes: None,
    };
    WalWriter::new(store.clone())
        .append(&ns, vec![new_vec], vec![])
        .await
        .unwrap();

    counter.reset();
    let result = baseline_compactor(&store).compact(&ns).await.unwrap();
    assert!(
        result.segment_id.is_some(),
        "baseline compaction must produce a new segment"
    );

    println!("2C.3 bounded flat incremental compaction read profile");
    println!("before: cluster GETs=16 attrs GETs=16");
    println!("{}", counter.report());

    assert_eq!(
        counter.gets_for(ArtifactClass::Cluster),
        1,
        "2C.3: one WAL vector touches one flat cluster, so compaction reads only \
         that cluster blob"
    );
    assert_eq!(
        counter.gets_for(ArtifactClass::Attrs),
        1,
        "2C.3: attrs are read only for the rewritten cluster"
    );
    assert_eq!(
        counter.gets_matching("membership.bin"),
        1,
        "2C.3: old membership replaces deriving id->cluster from a full vector load"
    );
    assert_eq!(
        counter.gets_for(ArtifactClass::Centroids),
        1,
        "2C.3: reused centroids are loaded once"
    );
    assert_eq!(
        counter.gets_for(ArtifactClass::Sketch),
        1,
        "2C.3: old sketch is loaded once for stitching"
    );
    assert_eq!(
        counter.get_bytes_for(ArtifactClass::Cluster),
        expected_cluster_bytes,
        "2C.3: cluster bytes equal the one touched cluster"
    );
    assert_eq!(
        counter.get_bytes_for(ArtifactClass::Attrs),
        expected_attrs_bytes,
        "2C.3: attrs bytes equal the one touched cluster's attrs"
    );

    harness.cleanup().await;
}

/// 2C.3 achieved bound: grouped incremental compaction reads only the physical
/// grouped object that contains a touched cluster, plus touched attrs.
#[tokio::test]
async fn test_incremental_grouped_read_io_baseline_reads_all_cluster_objects() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let ns = harness.key("incr-grouped-read-baseline");

    let (old_segment, seed_vecs) =
        seed_modern_grouped_segment(&store, &ns, BASELINE_CLUSTERS, BASELINE_VECTORS_PER_CLUSTER)
            .await;
    let (_membership, membership_map) =
        decoded_membership(&store, old_segment.membership.as_ref().unwrap()).await;

    let anchor = seed_vecs
        .iter()
        .find(|vector| vector.id == "cluster_0_vec_0")
        .unwrap()
        .values
        .clone();
    let touched_cluster = cluster_for_id(&membership_map, "cluster_0_vec_0");
    let touched_object = cluster_object_for(&old_segment, touched_cluster)
        .expect("grouped fixture must have an object for the touched cluster");
    let expected_attrs_key = attrs_key(
        &ns,
        old_segment.cluster_owner(touched_cluster),
        touched_cluster,
    );
    let expected_attrs_bytes = harness.store.head(&expected_attrs_key).await.unwrap().size as u64;
    let new_vec = VectorEntry {
        id: "grouped_baseline_added_one_cluster".to_string(),
        values: anchor.iter().map(|x| x + 0.001).collect(),
        attributes: None,
    };
    WalWriter::new(store.clone())
        .append(&ns, vec![new_vec], vec![])
        .await
        .unwrap();

    counter.reset();
    let result = baseline_compactor(&store).compact(&ns).await.unwrap();
    assert!(
        result.segment_id.is_some(),
        "grouped baseline compaction must produce a new segment"
    );

    println!("2C.3 bounded grouped incremental compaction read profile");
    println!("before: cluster GETs=9 attrs GETs=16");
    println!("{}", counter.report());

    assert_eq!(
        counter.gets_for(ArtifactClass::Cluster),
        1,
        "2C.3: grouped layout reads only the object containing the touched cluster"
    );
    assert_eq!(
        counter.gets_for(ArtifactClass::Attrs),
        1,
        "2C.3: grouped layout reads attrs only for the touched logical cluster"
    );
    assert_eq!(
        counter.gets_matching("membership.bin"),
        1,
        "2C.3: grouped bounded reads use membership"
    );
    assert_eq!(
        counter.gets_for(ArtifactClass::Centroids),
        1,
        "2C.3: reused centroids are loaded once"
    );
    assert_eq!(
        counter.gets_for(ArtifactClass::Sketch),
        1,
        "2C.3: old sketch is loaded once for stitching"
    );
    assert_eq!(
        counter.get_bytes_for(ArtifactClass::Cluster),
        touched_object.size_bytes,
        "2C.3: grouped bytes equal the one touched physical object"
    );
    assert_eq!(
        counter.get_bytes_for(ArtifactClass::Attrs),
        expected_attrs_bytes,
        "2C.3: grouped attrs bytes equal only the touched attrs blob"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn test_incremental_membership_delete_reads_only_touched_object() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let ns = harness.key("incr-membership-delete-bounded");

    let (old_segment, seed_vecs) = seed_modern_grouped_segment(&store, &ns, N_CLUSTERS, 20).await;
    let (_membership, membership_map) =
        decoded_membership(&store, old_segment.membership.as_ref().unwrap()).await;
    let victim = "cluster_5_vec_0".to_string();
    let victim_cluster = cluster_for_id(&membership_map, &victim);
    let touched_object = cluster_object_for(&old_segment, victim_cluster)
        .expect("victim cluster must live in one grouped object")
        .clone();
    let untouched_objects: Vec<String> = old_segment
        .cluster_objects
        .iter()
        .filter(|object_ref| object_ref.key != touched_object.key)
        .map(|object_ref| object_ref.key.clone())
        .collect();

    WalWriter::new(store.clone())
        .append(&ns, vec![], vec![victim.clone()])
        .await
        .unwrap();

    counter.reset();
    let result = compactor_with_clusters(&store, N_CLUSTERS)
        .compact(&ns)
        .await
        .unwrap();
    let new_seg = result
        .segment_id
        .expect("delete compaction must produce a segment");
    let new_segment = active_segment_ref(&store, &ns).await;
    assert_eq!(new_segment.id, new_seg);

    let rewritten: Vec<usize> = (0..new_segment.cluster_count)
        .filter(|&cluster_idx| new_segment.cluster_owner(cluster_idx) == new_seg)
        .collect();
    assert_eq!(
        rewritten,
        vec![victim_cluster],
        "membership must force exactly the deleted vector's old cluster to rewrite"
    );
    assert_eq!(
        counter.gets_matching(&touched_object.key),
        1,
        "the physical object containing the touched cluster must be read once"
    );
    for object_key in untouched_objects {
        assert_eq!(
            counter.gets_matching(&object_key),
            0,
            "untouched grouped object {object_key} must not be read"
        );
    }
    assert_eq!(
        counter.gets_for(ArtifactClass::Cluster),
        1,
        "a delete in one old cluster reads exactly one physical cluster object"
    );
    assert_eq!(
        counter.gets_for(ArtifactClass::Attrs),
        1,
        "a delete in one old cluster reads exactly that cluster's attrs"
    );
    assert_eq!(counter.gets_matching("membership.bin"), 1);

    let victim_vec = seed_vecs
        .iter()
        .find(|vector| vector.id == victim)
        .unwrap()
        .values
        .clone();
    let ids = ordered_query_ids(&store, &ns, &victim_vec, N_CLUSTERS * 20, N_CLUSTERS).await;
    assert!(
        !ids.iter().any(|id| id == &victim),
        "deleted vector must not survive the membership-driven rewrite"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn test_incremental_nonexistent_tombstone_reads_no_cluster_objects() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let ns = harness.key("incr-nonexistent-tombstone-bounded");

    seed_modern_grouped_segment(&store, &ns, N_CLUSTERS, 20).await;
    WalWriter::new(store.clone())
        .append(&ns, vec![], vec!["never_existed_id".to_string()])
        .await
        .unwrap();

    counter.reset();
    let result = compactor_with_clusters(&store, N_CLUSTERS)
        .compact(&ns)
        .await
        .unwrap();
    let new_seg = result
        .segment_id
        .expect("tombstone compaction must still advance the segment");
    let new_segment = active_segment_ref(&store, &ns).await;
    let rewritten: Vec<usize> = (0..new_segment.cluster_count)
        .filter(|&cluster_idx| new_segment.cluster_owner(cluster_idx) == new_seg)
        .collect();
    assert!(
        rewritten.is_empty(),
        "a tombstone for a nonexistent id must not rewrite any cluster, got {rewritten:?}"
    );
    assert_eq!(
        counter.gets_for(ArtifactClass::Cluster),
        0,
        "a nonexistent tombstone must not cause any cluster-object read"
    );
    assert_eq!(
        counter.gets_for(ArtifactClass::Attrs),
        0,
        "a nonexistent tombstone must not cause any attrs read"
    );
    assert_eq!(counter.gets_matching("membership.bin"), 1);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_incremental_membership_absent_self_heals_then_bounds_reads() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let ns = harness.key("incr-membership-self-heal");

    let (_seed_id, seed_vecs, _cluster_bytes, _attrs_bytes) =
        seed_legacy_flat_segment(&store, &ns, BASELINE_CLUSTERS, BASELINE_VECTORS_PER_CLUSTER)
            .await;
    let anchor0 = seed_vecs
        .iter()
        .find(|vector| vector.id == "cluster_0_vec_0")
        .unwrap()
        .values
        .clone();
    let anchor1 = seed_vecs
        .iter()
        .find(|vector| vector.id == "cluster_1_vec_0")
        .unwrap()
        .values
        .clone();

    let fallback_counter = zeppelin::metrics::COMPACTION_INCREMENTAL_FALLBACK_TOTAL
        .with_label_values(&[ns.as_str(), "membership_absent"]);
    let fallback_before = fallback_counter.get();

    WalWriter::new(store.clone())
        .append(
            &ns,
            vec![VectorEntry {
                id: "self_heal_cycle_1".to_string(),
                values: anchor0.iter().map(|x| x + 0.001).collect(),
                attributes: None,
            }],
            vec![],
        )
        .await
        .unwrap();
    counter.reset();
    compactor_with_clusters(&store, BASELINE_CLUSTERS)
        .compact(&ns)
        .await
        .unwrap();
    assert_eq!(
        fallback_counter.get(),
        fallback_before + 1,
        "pre-2C.1 segments must take the membership_absent fallback once"
    );
    assert_eq!(
        counter.gets_for(ArtifactClass::Cluster),
        BASELINE_CLUSTERS as u64,
        "membership_absent fallback uses the legacy full-read path for this cycle"
    );
    assert_eq!(
        counter.gets_for(ArtifactClass::Attrs),
        BASELINE_CLUSTERS as u64,
        "membership_absent fallback reads all attrs for this cycle"
    );
    let healed_segment = active_segment_ref(&store, &ns).await;
    assert!(
        healed_segment.membership.is_some(),
        "fallback compaction must write membership so the next cycle is bounded"
    );

    WalWriter::new(store.clone())
        .append(
            &ns,
            vec![VectorEntry {
                id: "self_heal_cycle_2".to_string(),
                values: anchor1.iter().map(|x| x + 0.001).collect(),
                attributes: None,
            }],
            vec![],
        )
        .await
        .unwrap();
    counter.reset();
    compactor_with_clusters(&store, BASELINE_CLUSTERS)
        .compact(&ns)
        .await
        .unwrap();
    assert_eq!(
        fallback_counter.get(),
        fallback_before + 1,
        "healed segment must not take membership_absent again"
    );
    assert_eq!(
        counter.gets_for(ArtifactClass::Cluster),
        1,
        "cycle 2 must be bounded to the newly touched cluster"
    );
    assert_eq!(
        counter.gets_for(ArtifactClass::Attrs),
        1,
        "cycle 2 must read attrs only for the newly touched cluster"
    );
    assert_eq!(counter.gets_matching("membership.bin"), 1);

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
        rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: None,
        include_attributes: true,
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
        let cvec_key = cluster_data_key(&ns, seg_ref, i);
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
        let cvec_key = cluster_data_key(&ns, seg_ref, i);
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

/// Golden equivalence: bounded incremental compaction returns the same ordered
/// nprobe=all results as a forced full rewrite of the same logical data.
#[tokio::test]
async fn test_incremental_matches_full_rewrite_results() {
    let harness = TestHarness::new().await;
    let store = &harness.store;

    // Two namespaces holding identical data; one compacted incrementally, one
    // via full retrain.
    let ns_incr = harness.key("incr-golden-incr");
    let ns_full = harness.key("incr-golden-full");

    let indexing_config = IndexingConfig {
        default_num_centroids: N_CLUSTERS,
        kmeans_max_iterations: 25,
        quantization: zeppelin::index::quantization::QuantizationType::None,
        bitmap_index: false,
        fts_index: false,
        hierarchical: false,
        ..Default::default()
    };
    let (seed_vecs, _centroids) = clustered_vectors(N_CLUSTERS, 20, DIM, 0.01);
    let (_seed_incr, seed_vecs) = seed_modern_vectors(store, &ns_incr, seed_vecs, N_CLUSTERS).await;
    seed_modern_vectors(store, &ns_full, seed_vecs.clone(), N_CLUSTERS).await;

    // Same WAL append to both.
    let anchor0 = seed_vecs
        .iter()
        .find(|vector| vector.id == "cluster_0_vec_0")
        .unwrap()
        .values
        .clone();
    let anchor3 = seed_vecs
        .iter()
        .find(|vector| vector.id == "cluster_3_vec_0")
        .unwrap()
        .values
        .clone();
    let make_new = || -> Vec<VectorEntry> {
        (0..5)
            .map(|i| VectorEntry {
                id: format!("added_{i}"),
                values: anchor0
                    .iter()
                    .map(|x| x + 0.002 + i as f32 * 0.0001)
                    .collect(),
                attributes: None,
            })
            .chain(std::iter::once(VectorEntry {
                id: "cluster_4_vec_0".to_string(),
                values: anchor3.iter().map(|x| x + 0.001).collect(),
                attributes: None,
            }))
            .collect()
    };
    let writer = WalWriter::new(store.clone());
    writer
        .append(&ns_incr, make_new(), vec!["cluster_5_vec_0".to_string()])
        .await
        .unwrap();
    writer
        .append(&ns_full, make_new(), vec!["cluster_5_vec_0".to_string()])
        .await
        .unwrap();

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
        Compactor::new(
            store.clone(),
            wal_reader,
            cfg,
            indexing_config.clone(),
            common::default_gc_upload_window(),
        )
    };
    full_compactor.compact(&ns_full).await.unwrap();

    let incr_segment = active_segment_ref(store, &ns_incr).await;
    assert!(
        incr_segment.membership.is_some(),
        "oracle must exercise the bounded membership path"
    );
    assert!(
        !incr_segment.cluster_owners.is_empty(),
        "oracle must carry untouched clusters by reference"
    );

    for query_vec in [&anchor0, &anchor3] {
        let incr_ids = ordered_query_ids(store, &ns_incr, query_vec, 40, N_CLUSTERS).await;
        let full_ids = ordered_query_ids(store, &ns_full, query_vec, 40, N_CLUSTERS).await;
        assert_eq!(
            incr_ids, full_ids,
            "bounded incremental and forced full rewrite must return identical \
             ordered IDs at nprobe=all"
        );
    }

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
