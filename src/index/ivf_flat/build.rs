//! Build phase for IVF-Flat index.
//!
//! Pipeline: train centroids -> assign vectors to clusters -> serialize and
//! write artifacts (centroids, cluster vectors, cluster attributes) to S3.
//!
//! ## Phase C.0b storage-format design
//!
//! This change is intentionally per-object versioned; the manifest remains
//! unchanged so old immutable segments and new segments can coexist in one
//! namespace. The compatibility boundary is:
//!
//! - Old `centroids.bin`: `[num_centroids:u32][dim:u32][f32...]`.
//! - New `centroids.bin`: `[b"ZCT2"][num_centroids:u32][dim:u32][f32...]
//!   [sq_calibration_len:u64][sq_calibration bytes]`. The calibration bytes are
//!   exactly the existing SQ calibration payload. `sq_calibration_len == 0`
//!   means the segment has no embedded SQ calibration. Readers detect `b"ZCT2"`
//!   and otherwise parse the legacy bytes. SQ readers first ask the loaded
//!   centroids blob for embedded calibration; when absent they read the legacy
//!   `sq_calibration.bin` key.
//! - Old `cluster_i.bin`: `[num_vectors:u32][dim:u32] ... full-precision rows`.
//! - New scalar cluster sections: `[b"ZCL2"][sq_offset:u64][sq_len:u64]
//!   [full_offset:u64][full_len:u64][sq_cluster bytes][full cluster bytes]`.
//!   Section offsets are absolute byte offsets from the beginning of the
//!   object. The coarse SQ path fetches this whole object through the normal
//!   cache and parses only the SQ section; the rerank path later asks for the
//!   same `cluster_i.bin` key and gets a cache hit, then parses the full section.
//!   The offset table is present now so a later range-GET implementation can
//!   fetch only `[sq_offset, sq_offset + sq_len)` without changing the format.
//! - New grouped `cluster_group_i.bin`: `[b"ZBP1"][entry_count:u32]`
//!   followed by `[cluster_idx:u32][offset:u64][len:u64] * entry_count` and
//!   then one or more cluster sections. Group membership is stored in the
//!   manifest's `cluster_objects` field; an empty field is an explicit legacy
//!   per-cluster layout. Cycle 7 `cluster_pair_i.bin` objects use the same
//!   per-object directory and remain readable.
//! - `attrs_i.bin` is unchanged and remains lazy-loaded.
//! - `sq_cluster_i.bin` and `sq_calibration.bin` are legacy read-only keys.
//!   New SQ segments do not write them. Old-format carried clusters continue
//!   routing through `cluster_owner()` to their original segment; if that
//!   owner's `cluster_i.bin` is legacy, SQ coarse reads fall back to
//!   `sq_cluster_i.bin` under the same owner and calibration falls back to the
//!   active segment's legacy `sq_calibration.bin`.
//! - Hierarchical indexes do not have an IVF `centroids.bin`; their
//!   read-once-up-front metadata is `tree_meta.json`. New hierarchical SQ
//!   segments store the same legacy SQ calibration bytes in an optional
//!   `sq_calibration` JSON field there. Old tree metadata lacks the field and
//!   therefore falls back to legacy `sq_calibration.bin`.
//! - Compaction migrates only rewritten clusters by writing the new co-located
//!   object under the new segment. Task 2B carried clusters keep their old owner
//!   string in `cluster_owners`, so their old physical keys stay authoritative.
//! - PQ is deliberately deferred. The current requirement and GET-count target
//!   are specific to SQ calibration and SQ cluster/full-vector co-location; PQ
//!   keeps `pq_codebook.bin` and `pq_cluster_i.bin` unchanged for this phase.

use bytes::Bytes;
use std::collections::{BTreeSet, HashMap};
use tracing::{debug, info};

use crate::config::IndexingConfig;
use crate::error::{Result, ZeppelinError};
use crate::index::quantization::QuantizationType;
use crate::storage::ZeppelinStore;
use crate::types::{AttributeValue, VectorEntry};
use crate::wal::manifest::{BootstrapRef, ClusterDataObjectRef};

use super::kmeans::train_kmeans;
use super::sketch::{build_resident_sketch, ResidentSketch};
use super::IvfFlatIndex;
use crate::index::distance;

const CENTROIDS_V2_MAGIC: &[u8; 4] = b"ZCT2";
const CLUSTER_V2_MAGIC: &[u8; 4] = b"ZCL2";
const CLUSTER_V2_HEADER_LEN: usize = 4 + 8 * 4;
const CLUSTER_DATA_OBJECT_MAGIC: &[u8; 4] = b"ZBP1";
const CLUSTER_DATA_OBJECT_HEADER_LEN: usize = 8;
const CLUSTER_DATA_OBJECT_DIR_ENTRY_LEN: usize = 4 + 8 + 8;
const BOOTSTRAP_MAGIC: &[u8; 4] = b"ZBS1";
const BOOTSTRAP_VERSION: u32 = 1;
const BOOTSTRAP_SECTION_COUNT: usize = 2;
const BOOTSTRAP_HEADER_LEN: usize = 4 + 4 + BOOTSTRAP_SECTION_COUNT * 16;
const DEFAULT_MAX_CLUSTERS_PER_OBJECT: usize = 3;
const MAX_CLUSTERS_PER_OBJECT_ENV: &str = "ZEPPELIN_MAX_CLUSTERS_PER_OBJECT";
const CLUSTER_GROUP_STATS_ENV: &str = "ZEPPELIN_CLUSTER_GROUP_STATS";

// ---------------------------------------------------------------------------
// Artifact paths
// ---------------------------------------------------------------------------

/// S3 key for the centroids blob.
pub fn centroids_key(namespace: &str, segment_id: &str) -> String {
    format!("{namespace}/segments/{segment_id}/centroids.bin")
}

/// S3 key for the segment bootstrap blob.
#[must_use]
pub fn bootstrap_key(namespace: &str, segment_id: &str) -> String {
    format!("{namespace}/segments/{segment_id}/bootstrap.bin")
}

/// S3 key for the vector data of cluster `i`.
pub(crate) fn cluster_key(namespace: &str, segment_id: &str, cluster_idx: usize) -> String {
    format!("{namespace}/segments/{segment_id}/cluster_{cluster_idx}.bin")
}

/// S3 key for grouped cluster data object `group_idx`.
pub(crate) fn cluster_group_key(namespace: &str, segment_id: &str, group_idx: usize) -> String {
    format!("{namespace}/segments/{segment_id}/cluster_group_{group_idx}.bin")
}

/// S3 key for the attribute data of cluster `i`.
pub(crate) fn attrs_key(namespace: &str, segment_id: &str, cluster_idx: usize) -> String {
    format!("{namespace}/segments/{segment_id}/attrs_{cluster_idx}.bin")
}

fn configured_max_clusters_per_object() -> Result<usize> {
    match std::env::var(MAX_CLUSTERS_PER_OBJECT_ENV) {
        Ok(value) => {
            let parsed = value.parse::<usize>().map_err(|e| {
                ZeppelinError::Index(format!(
                    "{MAX_CLUSTERS_PER_OBJECT_ENV} must be a positive integer: {e}"
                ))
            })?;
            if parsed == 0 {
                return Err(ZeppelinError::Index(format!(
                    "{MAX_CLUSTERS_PER_OBJECT_ENV} must be greater than zero"
                )));
            }
            Ok(parsed)
        }
        Err(std::env::VarError::NotPresent) => Ok(DEFAULT_MAX_CLUSTERS_PER_OBJECT),
        Err(e) => Err(ZeppelinError::Index(format!(
            "failed to read {MAX_CLUSTERS_PER_OBJECT_ENV}: {e}"
        ))),
    }
}

/// Capped density-adaptive centroid grouping used for cluster-data objects.
///
/// The only external bound is `max_clusters_per_object`. The merge cutoff is
/// derived from the segment's own cap-neighbor centroid distance distribution,
/// so it scales with the embedding space rather than baking in an absolute
/// radius.
pub(crate) fn density_cluster_groups(
    centroids: &[Vec<f32>],
    affinity: &[Vec<u32>],
) -> Result<Vec<Vec<usize>>> {
    let max_clusters_per_object = configured_max_clusters_per_object()?;
    density_cluster_groups_with_cap(centroids, affinity, max_clusters_per_object)
}

pub(crate) fn density_cluster_groups_with_cap(
    centroids: &[Vec<f32>],
    affinity: &[Vec<u32>],
    max_clusters_per_object: usize,
) -> Result<Vec<Vec<usize>>> {
    if max_clusters_per_object == 0 {
        return Err(ZeppelinError::Index(
            "max_clusters_per_object must be greater than zero".into(),
        ));
    }
    if centroids.is_empty() {
        return Ok(Vec::new());
    }
    if max_clusters_per_object == 1 || centroids.len() == 1 {
        return Ok((0..centroids.len()).map(|idx| vec![idx]).collect());
    }

    let distances = centroid_distances(centroids)?;
    let cutoff =
        cap_neighbor_distance_upper_quartile(&distances, centroids.len(), max_clusters_per_object)?;
    let mut edges = Vec::new();
    for left in 0..centroids.len() {
        for right in (left + 1)..centroids.len() {
            let weight = affinity
                .get(left)
                .and_then(|row| row.get(right))
                .copied()
                .unwrap_or(0);
            let dist = centroid_distance(&distances, centroids.len(), left, right);
            if dist <= cutoff {
                edges.push((dist, weight, left, right));
            }
        }
    }
    edges.sort_by(
        |(a_dist, a_weight, a_left, a_right), (b_dist, b_weight, b_left, b_right)| {
            a_dist
                .partial_cmp(b_dist)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b_weight.cmp(a_weight))
                .then_with(|| a_left.cmp(b_left))
                .then_with(|| a_right.cmp(b_right))
        },
    );

    let mut cluster_to_group: Vec<usize> = (0..centroids.len()).collect();
    let mut groups: Vec<Vec<usize>> = (0..centroids.len()).map(|idx| vec![idx]).collect();
    for (_, _, left, right) in edges {
        let left_group = cluster_to_group[left];
        let right_group = cluster_to_group[right];
        if left_group == right_group {
            continue;
        }
        let merged_len = groups[left_group].len() + groups[right_group].len();
        if merged_len > max_clusters_per_object {
            continue;
        }
        if !groups_within_cutoff(
            &groups[left_group],
            &groups[right_group],
            &distances,
            centroids.len(),
            cutoff,
        ) {
            continue;
        }

        let moved = std::mem::take(&mut groups[right_group]);
        let mut merged = std::mem::take(&mut groups[left_group]);
        merged.extend(moved);
        merged.sort_unstable();
        for &cluster_idx in &merged {
            cluster_to_group[cluster_idx] = left_group;
        }
        groups[left_group] = merged;
    }

    let mut groups: Vec<Vec<usize>> = groups
        .into_iter()
        .filter(|group| !group.is_empty())
        .collect();
    groups.sort_by_key(|group| group[0]);
    emit_cluster_group_stats(max_clusters_per_object, cutoff, &groups);
    Ok(groups)
}

fn centroid_distances(centroids: &[Vec<f32>]) -> Result<Vec<f32>> {
    let n = centroids.len();
    let total = n
        .checked_mul(n)
        .ok_or_else(|| ZeppelinError::Index("centroid distance matrix overflows".into()))?;
    let mut distances = vec![0.0; total];
    for left in 0..n {
        for right in (left + 1)..n {
            let dist = distance::euclidean_distance(&centroids[left], &centroids[right]);
            distances[left * n + right] = dist;
            distances[right * n + left] = dist;
        }
    }
    Ok(distances)
}

fn centroid_distance(distances: &[f32], n: usize, left: usize, right: usize) -> f32 {
    distances[left * n + right]
}

fn cap_neighbor_distance_upper_quartile(
    distances: &[f32],
    n: usize,
    max_clusters_per_object: usize,
) -> Result<f32> {
    if n < 2 {
        return Err(ZeppelinError::Index(
            "cannot derive centroid merge cutoff from fewer than two centroids".into(),
        ));
    }
    let neighbor_rank = max_clusters_per_object.saturating_sub(1).min(n - 1).max(1);
    let mut cap_neighbor_distances = Vec::with_capacity(n);
    for left in 0..n {
        let mut row = Vec::with_capacity(n - 1);
        for right in 0..n {
            if left == right {
                continue;
            }
            row.push(centroid_distance(distances, n, left, right));
        }
        row.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let dist = row[neighbor_rank - 1];
        if !dist.is_finite() {
            return Err(ZeppelinError::Index(format!(
                "non-finite cap-neighbor centroid distance for cluster {left}"
            )));
        }
        cap_neighbor_distances.push(dist);
    }
    cap_neighbor_distances.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = (cap_neighbor_distances.len() * 3) / 4;
    Ok(cap_neighbor_distances[idx.min(cap_neighbor_distances.len() - 1)])
}

fn groups_within_cutoff(
    left_group: &[usize],
    right_group: &[usize],
    distances: &[f32],
    n: usize,
    cutoff: f32,
) -> bool {
    left_group.iter().all(|&left| {
        right_group
            .iter()
            .all(|&right| centroid_distance(distances, n, left, right) <= cutoff)
    })
}

fn emit_cluster_group_stats(max_clusters_per_object: usize, cutoff: f32, groups: &[Vec<usize>]) {
    if std::env::var_os(CLUSTER_GROUP_STATS_ENV).is_none() {
        return;
    }
    let mut hist = std::collections::BTreeMap::new();
    for group in groups {
        *hist.entry(group.len()).or_insert(0usize) += 1;
    }
    let histogram = hist
        .into_iter()
        .map(|(size, count)| format!("{size}:{count}"))
        .collect::<Vec<_>>()
        .join(",");
    eprintln!(
        "zeppelin_cluster_group_stats max_clusters_per_object={max_clusters_per_object} objects={} cutoff={cutoff:.6} group_size_histogram={histogram}",
        groups.len()
    );
}

pub(crate) fn build_cluster_object_lookup(
    cluster_count: usize,
    cluster_objects: &[ClusterDataObjectRef],
) -> Result<Vec<usize>> {
    if cluster_objects.is_empty() {
        return Ok(Vec::new());
    }

    let mut lookup = vec![usize::MAX; cluster_count];
    for (object_idx, object_ref) in cluster_objects.iter().enumerate() {
        if object_ref.key.is_empty() {
            return Err(ZeppelinError::Index(format!(
                "cluster object {object_idx} has empty key"
            )));
        }
        if object_ref.clusters.is_empty() {
            return Err(ZeppelinError::Index(format!(
                "cluster object {} has no clusters",
                object_ref.key
            )));
        }
        let mut seen_in_object = BTreeSet::new();
        for &cluster_idx in &object_ref.clusters {
            if cluster_idx >= cluster_count {
                return Err(ZeppelinError::Index(format!(
                    "cluster object {} references out-of-range cluster {cluster_idx} for count {cluster_count}",
                    object_ref.key
                )));
            }
            if !seen_in_object.insert(cluster_idx) {
                return Err(ZeppelinError::Index(format!(
                    "cluster object {} lists cluster {cluster_idx} twice",
                    object_ref.key
                )));
            }
            if lookup[cluster_idx] != usize::MAX {
                return Err(ZeppelinError::Index(format!(
                    "cluster {cluster_idx} appears in multiple cluster objects"
                )));
            }
            lookup[cluster_idx] = object_idx;
        }
    }

    for (cluster_idx, object_idx) in lookup.iter().enumerate() {
        if *object_idx == usize::MAX {
            return Err(ZeppelinError::Index(format!(
                "cluster {cluster_idx} missing from cluster object layout"
            )));
        }
    }

    Ok(lookup)
}

// ---------------------------------------------------------------------------
// Serialization helpers
// ---------------------------------------------------------------------------

/// Borrowed sections from a segment bootstrap artifact.
pub(crate) struct BootstrapSections<'a> {
    pub centroids: &'a [u8],
    pub sketch: &'a [u8],
}

/// Serialize a segment bootstrap artifact from existing artifact bytes.
///
/// The centroid and sketch payloads are embedded verbatim. Their internal
/// formats remain independently versioned by their existing decoders.
pub(crate) fn serialize_bootstrap(centroids: &[u8], sketch: &[u8]) -> Result<Bytes> {
    if centroids.is_empty() {
        return Err(ZeppelinError::Index(
            "bootstrap centroids section cannot be empty".into(),
        ));
    }
    if sketch.is_empty() {
        return Err(ZeppelinError::Index(
            "bootstrap sketch section cannot be empty".into(),
        ));
    }

    let centroids_offset = BOOTSTRAP_HEADER_LEN;
    let sketch_offset = centroids_offset
        .checked_add(centroids.len())
        .ok_or_else(|| ZeppelinError::Index("bootstrap centroids section overflows".into()))?;
    let total = sketch_offset
        .checked_add(sketch.len())
        .ok_or_else(|| ZeppelinError::Index("bootstrap sketch section overflows".into()))?;

    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(BOOTSTRAP_MAGIC);
    buf.extend_from_slice(&BOOTSTRAP_VERSION.to_le_bytes());
    buf.extend_from_slice(&(centroids_offset as u64).to_le_bytes());
    buf.extend_from_slice(&(centroids.len() as u64).to_le_bytes());
    buf.extend_from_slice(&(sketch_offset as u64).to_le_bytes());
    buf.extend_from_slice(&(sketch.len() as u64).to_le_bytes());
    buf.extend_from_slice(centroids);
    buf.extend_from_slice(sketch);
    debug_assert_eq!(buf.len(), total);

    Ok(Bytes::from(buf))
}

/// Build a manifest ref and bytes for the segment bootstrap artifact.
pub(crate) fn build_bootstrap_artifact(
    namespace: &str,
    segment_id: &str,
    centroids: &[u8],
    sketch: &[u8],
) -> Result<(BootstrapRef, Bytes)> {
    let bytes = serialize_bootstrap(centroids, sketch)?;
    let bootstrap_ref = BootstrapRef {
        key: bootstrap_key(namespace, segment_id),
        size_bytes: bytes.len() as u64,
    };
    Ok((bootstrap_ref, bytes))
}

/// Deserialize and validate a segment bootstrap artifact.
pub(crate) fn deserialize_bootstrap(data: &[u8]) -> Result<BootstrapSections<'_>> {
    if data.len() < BOOTSTRAP_HEADER_LEN {
        return Err(ZeppelinError::Index(
            "bootstrap blob too small for header".into(),
        ));
    }
    if !data.starts_with(BOOTSTRAP_MAGIC) {
        return Err(ZeppelinError::Index("bootstrap magic mismatch".into()));
    }

    let version = u32::from_le_bytes(
        data[4..8]
            .try_into()
            .map_err(|_| ZeppelinError::Index("bootstrap version parse error".into()))?,
    );
    if version != BOOTSTRAP_VERSION {
        return Err(ZeppelinError::Index(format!(
            "unsupported bootstrap version: {version}"
        )));
    }

    let centroids_offset = read_u64_usize(data, 8, "bootstrap centroids offset")?;
    let centroids_len = read_u64_usize(data, 16, "bootstrap centroids length")?;
    let sketch_offset = read_u64_usize(data, 24, "bootstrap sketch offset")?;
    let sketch_len = read_u64_usize(data, 32, "bootstrap sketch length")?;
    validate_bootstrap_section(
        "centroids",
        centroids_offset,
        centroids_len,
        BOOTSTRAP_HEADER_LEN,
        data.len(),
    )?;
    let centroids_end = centroids_offset.checked_add(centroids_len).ok_or_else(|| {
        ZeppelinError::Index(format!(
            "bootstrap centroids section overflows: offset={centroids_offset}, len={centroids_len}"
        ))
    })?;
    if centroids_offset != BOOTSTRAP_HEADER_LEN {
        return Err(ZeppelinError::Index(format!(
            "bootstrap centroids offset mismatch: expected {BOOTSTRAP_HEADER_LEN}, got {centroids_offset}"
        )));
    }
    if sketch_offset != centroids_end {
        return Err(ZeppelinError::Index(format!(
            "bootstrap sketch offset mismatch: expected {centroids_end}, got {sketch_offset}"
        )));
    }
    validate_bootstrap_section(
        "sketch",
        sketch_offset,
        sketch_len,
        BOOTSTRAP_HEADER_LEN,
        data.len(),
    )?;
    let sketch_end = sketch_offset.checked_add(sketch_len).ok_or_else(|| {
        ZeppelinError::Index(format!(
            "bootstrap sketch section overflows: offset={sketch_offset}, len={sketch_len}"
        ))
    })?;
    if sketch_end != data.len() {
        return Err(ZeppelinError::Index(format!(
            "bootstrap blob size mismatch: expected {sketch_end}, got {}",
            data.len()
        )));
    }

    Ok(BootstrapSections {
        centroids: &data[centroids_offset..centroids_end],
        sketch: &data[sketch_offset..sketch_end],
    })
}

fn validate_bootstrap_section(
    label: &str,
    offset: usize,
    len: usize,
    min_offset: usize,
    data_len: usize,
) -> Result<()> {
    if len == 0 {
        return Err(ZeppelinError::Index(format!(
            "bootstrap {label} section is empty"
        )));
    }
    if offset < min_offset {
        return Err(ZeppelinError::Index(format!(
            "bootstrap {label} section starts inside header: offset={offset}, header={min_offset}"
        )));
    }
    let end = offset.checked_add(len).ok_or_else(|| {
        ZeppelinError::Index(format!(
            "bootstrap {label} section overflows: offset={offset}, len={len}"
        ))
    })?;
    if end > data_len {
        return Err(ZeppelinError::Index(format!(
            "bootstrap {label} section out of bounds: end={end}, len={data_len}"
        )));
    }
    Ok(())
}

/// Header written before the centroid float array.
///
/// Layout:
/// `[b"ZCT2"][num_centroids: u32][dimension: u32]`
/// `[f32 * num_centroids * dimension][sq_calibration_len:u64][sq_calibration bytes]`
pub(crate) fn serialize_centroids(centroids: &[Vec<f32>], dim: usize) -> Result<Bytes> {
    serialize_centroids_with_sq_calibration(centroids, dim, None)
}

/// Serialize centroids with an optional embedded SQ calibration payload.
pub(crate) fn serialize_centroids_with_sq_calibration(
    centroids: &[Vec<f32>],
    dim: usize,
    sq_calibration: Option<&[u8]>,
) -> Result<Bytes> {
    let num_centroids = centroids.len() as u32;
    let dimension = dim as u32;
    let sq_calibration_len = sq_calibration.map_or(0, |bytes| bytes.len());

    let float_bytes = centroids.len() * dim * std::mem::size_of::<f32>();
    let total = 4 + 8 + float_bytes + 8 + sq_calibration_len;
    let mut buf = Vec::with_capacity(total);

    buf.extend_from_slice(CENTROIDS_V2_MAGIC);
    buf.extend_from_slice(&num_centroids.to_le_bytes());
    buf.extend_from_slice(&dimension.to_le_bytes());

    for centroid in centroids {
        for &val in centroid {
            buf.extend_from_slice(&val.to_le_bytes());
        }
    }
    buf.extend_from_slice(&(sq_calibration_len as u64).to_le_bytes());
    if let Some(calibration) = sq_calibration {
        buf.extend_from_slice(calibration);
    }

    debug_assert_eq!(buf.len(), total);
    Ok(Bytes::from(buf))
}

/// Parsed centroid blob with optional embedded SQ calibration.
#[derive(Debug)]
pub(crate) struct CentroidsData {
    /// IVF centroids.
    pub centroids: Vec<Vec<f32>>,
    /// Vector dimensionality.
    pub dim: usize,
    /// Embedded legacy SQ calibration payload, present for new SQ segments.
    pub sq_calibration: Option<Bytes>,
}

/// Deserialize centroids from the binary format produced by `serialize_centroids`.
pub(crate) fn deserialize_centroids(data: &[u8]) -> Result<(Vec<Vec<f32>>, usize)> {
    let decoded = deserialize_centroids_data(data)?;
    Ok((decoded.centroids, decoded.dim))
}

/// Deserialize centroids, auto-detecting legacy and v2 object formats.
pub(crate) fn deserialize_centroids_data(data: &[u8]) -> Result<CentroidsData> {
    if data.starts_with(CENTROIDS_V2_MAGIC) {
        return deserialize_centroids_v2(data);
    }
    deserialize_centroids_legacy(data)
}

fn deserialize_centroids_legacy(data: &[u8]) -> Result<CentroidsData> {
    if data.len() < 8 {
        return Err(ZeppelinError::Index(
            "centroids blob too small for header".into(),
        ));
    }

    let num_centroids = u32::from_le_bytes(
        data[0..4]
            .try_into()
            .map_err(|_| ZeppelinError::Index("centroids header parse error".into()))?,
    ) as usize;
    let dim = u32::from_le_bytes(
        data[4..8]
            .try_into()
            .map_err(|_| ZeppelinError::Index("centroids header parse error".into()))?,
    ) as usize;

    let expected = 8 + num_centroids * dim * 4;
    if data.len() < expected {
        return Err(ZeppelinError::Index(format!(
            "centroids blob size mismatch: expected {expected}, got {}",
            data.len()
        )));
    }

    let mut centroids = Vec::with_capacity(num_centroids);
    let mut offset = 8;
    for _ in 0..num_centroids {
        let mut c = Vec::with_capacity(dim);
        for _ in 0..dim {
            let val = f32::from_le_bytes(
                data[offset..offset + 4]
                    .try_into()
                    .map_err(|_| ZeppelinError::Index("centroids float parse error".into()))?,
            );
            c.push(val);
            offset += 4;
        }
        centroids.push(c);
    }

    Ok(CentroidsData {
        centroids,
        dim,
        sq_calibration: None,
    })
}

fn deserialize_centroids_v2(data: &[u8]) -> Result<CentroidsData> {
    if data.len() < 12 {
        return Err(ZeppelinError::Index(
            "v2 centroids blob too small for header".into(),
        ));
    }

    let num_centroids = u32::from_le_bytes(
        data[4..8]
            .try_into()
            .map_err(|_| ZeppelinError::Index("v2 centroids header parse error".into()))?,
    ) as usize;
    let dim = u32::from_le_bytes(
        data[8..12]
            .try_into()
            .map_err(|_| ZeppelinError::Index("v2 centroids header parse error".into()))?,
    ) as usize;

    let float_bytes = num_centroids
        .checked_mul(dim)
        .and_then(|v| v.checked_mul(4))
        .ok_or_else(|| {
            ZeppelinError::Index(format!(
                "v2 centroids size overflows: num_centroids={num_centroids}, dim={dim}"
            ))
        })?;
    let calibration_len_offset = 12usize.checked_add(float_bytes).ok_or_else(|| {
        ZeppelinError::Index(format!(
            "v2 centroids offset overflows: num_centroids={num_centroids}, dim={dim}"
        ))
    })?;
    if data.len() < calibration_len_offset + 8 {
        return Err(ZeppelinError::Index(format!(
            "v2 centroids blob size mismatch: expected at least {}, got {}",
            calibration_len_offset + 8,
            data.len()
        )));
    }

    let mut centroids = Vec::with_capacity(num_centroids);
    let mut offset = 12;
    for _ in 0..num_centroids {
        let mut c = Vec::with_capacity(dim);
        for _ in 0..dim {
            let val = f32::from_le_bytes(
                data[offset..offset + 4]
                    .try_into()
                    .map_err(|_| ZeppelinError::Index("v2 centroids float parse error".into()))?,
            );
            c.push(val);
            offset += 4;
        }
        centroids.push(c);
    }

    let sq_len = u64::from_le_bytes(
        data[offset..offset + 8]
            .try_into()
            .map_err(|_| ZeppelinError::Index("v2 centroids SQ length parse error".into()))?,
    ) as usize;
    offset += 8;
    let expected = offset.checked_add(sq_len).ok_or_else(|| {
        ZeppelinError::Index(format!(
            "v2 centroids SQ section size overflows: sq_len={sq_len}"
        ))
    })?;
    if data.len() != expected {
        return Err(ZeppelinError::Index(format!(
            "v2 centroids blob size mismatch: expected {expected}, got {}",
            data.len()
        )));
    }
    let sq_calibration = if sq_len == 0 {
        None
    } else {
        Some(Bytes::copy_from_slice(&data[offset..expected]))
    };

    Ok(CentroidsData {
        centroids,
        dim,
        sq_calibration,
    })
}

/// Cluster blob layout:
/// `[num_vectors: u32][dimension: u32]`
/// then for each vector: `[id_len: u32][id_bytes...][f32 * dim]`
pub(crate) fn serialize_cluster(ids: &[String], vectors: &[Vec<f32>], dim: usize) -> Result<Bytes> {
    let n = ids.len() as u32;
    let dimension = dim as u32;

    let mut buf = Vec::new();
    buf.extend_from_slice(&n.to_le_bytes());
    buf.extend_from_slice(&dimension.to_le_bytes());

    for (id, vec) in ids.iter().zip(vectors.iter()) {
        let id_bytes = id.as_bytes();
        let id_len = id_bytes.len() as u32;
        buf.extend_from_slice(&id_len.to_le_bytes());
        buf.extend_from_slice(id_bytes);
        for &val in vec {
            buf.extend_from_slice(&val.to_le_bytes());
        }
    }

    Ok(Bytes::from(buf))
}

/// Serialize a v2 per-cluster object containing SQ codes and full vectors.
pub(crate) fn serialize_colocated_sq_cluster(
    ids: &[String],
    vectors: &[Vec<f32>],
    sq_codes: &[Vec<u8>],
    dim: usize,
) -> Result<Bytes> {
    let sq_data = crate::index::quantization::sq::serialize_sq_cluster(ids, sq_codes, dim)?;
    let full_data = serialize_cluster(ids, vectors, dim)?;
    let sq_offset = CLUSTER_V2_HEADER_LEN as u64;
    let sq_len = sq_data.len() as u64;
    let full_offset = sq_offset + sq_len;
    let full_len = full_data.len() as u64;

    let total = CLUSTER_V2_HEADER_LEN + sq_data.len() + full_data.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(CLUSTER_V2_MAGIC);
    buf.extend_from_slice(&sq_offset.to_le_bytes());
    buf.extend_from_slice(&sq_len.to_le_bytes());
    buf.extend_from_slice(&full_offset.to_le_bytes());
    buf.extend_from_slice(&full_len.to_le_bytes());
    buf.extend_from_slice(&sq_data);
    buf.extend_from_slice(&full_data);
    debug_assert_eq!(buf.len(), total);

    Ok(Bytes::from(buf))
}

/// Serialize one immutable object containing one or more cluster payloads.
///
/// Each payload is a complete cluster section: either the legacy full-vector
/// cluster format or the v2 SQ+full co-located format.
pub(crate) fn serialize_cluster_data_object(entries: &[(usize, Bytes)]) -> Result<Bytes> {
    if entries.is_empty() {
        return Err(ZeppelinError::Index(
            "cluster data object cannot be empty".into(),
        ));
    }

    let mut seen = BTreeSet::new();
    for (cluster_idx, _) in entries {
        if *cluster_idx > u32::MAX as usize {
            return Err(ZeppelinError::Index(format!(
                "cluster index does not fit in u32: {cluster_idx}"
            )));
        }
        if !seen.insert(*cluster_idx) {
            return Err(ZeppelinError::Index(format!(
                "duplicate cluster {cluster_idx} in cluster data object"
            )));
        }
    }

    let directory_len = entries
        .len()
        .checked_mul(CLUSTER_DATA_OBJECT_DIR_ENTRY_LEN)
        .ok_or_else(|| ZeppelinError::Index("cluster object directory overflows".into()))?;
    let payload_offset = CLUSTER_DATA_OBJECT_HEADER_LEN
        .checked_add(directory_len)
        .ok_or_else(|| ZeppelinError::Index("cluster object header overflows".into()))?;
    let payload_len: usize =
        entries
            .iter()
            .map(|(_, bytes)| bytes.len())
            .try_fold(0usize, |acc, len| {
                acc.checked_add(len)
                    .ok_or_else(|| ZeppelinError::Index("cluster object payload overflows".into()))
            })?;
    let total = payload_offset
        .checked_add(payload_len)
        .ok_or_else(|| ZeppelinError::Index("cluster object size overflows".into()))?;

    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(CLUSTER_DATA_OBJECT_MAGIC);
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());

    let mut offset = payload_offset;
    for (cluster_idx, bytes) in entries {
        buf.extend_from_slice(&(*cluster_idx as u32).to_le_bytes());
        buf.extend_from_slice(&(offset as u64).to_le_bytes());
        buf.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        offset = offset
            .checked_add(bytes.len())
            .ok_or_else(|| ZeppelinError::Index("cluster object section overflows".into()))?;
    }

    for (_, bytes) in entries {
        buf.extend_from_slice(bytes);
    }
    debug_assert_eq!(buf.len(), total);

    Ok(Bytes::from(buf))
}

/// Cluster data for a single cluster.
#[derive(Debug)]
pub(crate) struct ClusterData {
    pub ids: Vec<String>,
    pub vectors: Vec<Vec<f32>>,
}

/// Deserialize a cluster blob.
pub(crate) fn deserialize_cluster(data: &[u8]) -> Result<ClusterData> {
    let data = full_cluster_section(data)?;
    deserialize_legacy_cluster(data)
}

/// Deserialize one cluster section from either a legacy per-cluster object or
/// a grouped cluster-data object.
pub(crate) fn deserialize_cluster_from_object(
    data: &[u8],
    cluster_idx: usize,
) -> Result<ClusterData> {
    let data = cluster_section_from_object(data, cluster_idx)?;
    deserialize_cluster(data)
}

fn deserialize_legacy_cluster(data: &[u8]) -> Result<ClusterData> {
    if data.len() < 8 {
        return Err(ZeppelinError::Index(
            "cluster blob too small for header".into(),
        ));
    }

    let n = u32::from_le_bytes(
        data[0..4]
            .try_into()
            .map_err(|_| ZeppelinError::Index("cluster header parse error".into()))?,
    ) as usize;
    let dim = u32::from_le_bytes(
        data[4..8]
            .try_into()
            .map_err(|_| ZeppelinError::Index("cluster header parse error".into()))?,
    ) as usize;

    let mut ids = Vec::with_capacity(n);
    let mut vectors = Vec::with_capacity(n);
    let mut offset = 8;

    for _ in 0..n {
        if offset + 4 > data.len() {
            return Err(ZeppelinError::Index(
                "cluster blob truncated at id_len".into(),
            ));
        }
        let id_len = u32::from_le_bytes(
            data[offset..offset + 4]
                .try_into()
                .map_err(|_| ZeppelinError::Index("cluster id_len parse error".into()))?,
        ) as usize;
        offset += 4;

        if offset + id_len > data.len() {
            return Err(ZeppelinError::Index("cluster blob truncated at id".into()));
        }
        let id = String::from_utf8_lossy(&data[offset..offset + id_len]).into_owned();
        offset += id_len;

        let float_bytes = dim * 4;
        if offset + float_bytes > data.len() {
            return Err(ZeppelinError::Index(
                "cluster blob truncated at vector data".into(),
            ));
        }
        // Parse f32 slice using chunks_exact — enables compiler auto-vectorization
        // and removes per-element try_into/map_err overhead.
        let vec: Vec<f32> = data[offset..offset + float_bytes]
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        offset += float_bytes;

        ids.push(id);
        vectors.push(vec);
    }

    Ok(ClusterData { ids, vectors })
}

/// Deserialize the SQ section of a v2 co-located cluster object.
pub(crate) fn deserialize_colocated_sq_cluster(
    data: &[u8],
) -> Result<Option<crate::index::quantization::sq::SqClusterData>> {
    if !data.starts_with(CLUSTER_V2_MAGIC) {
        return Ok(None);
    }

    let sections = colocated_cluster_sections(data)?;
    let sq_cluster = crate::index::quantization::sq::deserialize_sq_cluster(sections.sq)?;
    Ok(Some(sq_cluster))
}

/// Deserialize the SQ section for one cluster in either a legacy per-cluster
/// object or a grouped cluster-data object.
pub(crate) fn deserialize_colocated_sq_cluster_from_object(
    data: &[u8],
    cluster_idx: usize,
) -> Result<Option<crate::index::quantization::sq::SqClusterData>> {
    let data = cluster_section_from_object(data, cluster_idx)?;
    deserialize_colocated_sq_cluster(data)
}

struct ColocatedClusterSections<'a> {
    sq: &'a [u8],
    full: &'a [u8],
}

pub(crate) struct ClusterObjectSection<'a> {
    pub cluster_idx: usize,
    pub data: &'a [u8],
}

/// Return all sections in a grouped cluster-data object.
///
/// `Ok(None)` means the bytes are a legacy per-cluster object, not a grouped
/// object. Callers that already know they fetched a grouped key should treat
/// `None` as an error at that boundary.
pub(crate) fn cluster_object_sections(
    data: &[u8],
) -> Result<Option<Vec<ClusterObjectSection<'_>>>> {
    if !data.starts_with(CLUSTER_DATA_OBJECT_MAGIC) {
        return Ok(None);
    }
    if data.len() < CLUSTER_DATA_OBJECT_HEADER_LEN {
        return Err(ZeppelinError::Index(
            "cluster data object too small for header".into(),
        ));
    }

    let entry_count = u32::from_le_bytes(
        data[4..8]
            .try_into()
            .map_err(|_| ZeppelinError::Index("cluster data object count parse error".into()))?,
    ) as usize;
    if entry_count == 0 {
        return Err(ZeppelinError::Index(
            "cluster data object has zero entries".into(),
        ));
    }

    let directory_len = entry_count
        .checked_mul(CLUSTER_DATA_OBJECT_DIR_ENTRY_LEN)
        .ok_or_else(|| ZeppelinError::Index("cluster data object directory overflows".into()))?;
    let payload_start = CLUSTER_DATA_OBJECT_HEADER_LEN
        .checked_add(directory_len)
        .ok_or_else(|| ZeppelinError::Index("cluster data object header overflows".into()))?;
    if data.len() < payload_start {
        return Err(ZeppelinError::Index(format!(
            "cluster data object truncated directory: expected at least {payload_start}, got {}",
            data.len()
        )));
    }

    let mut sections = Vec::with_capacity(entry_count);
    let mut seen = BTreeSet::new();
    for entry_idx in 0..entry_count {
        let base = CLUSTER_DATA_OBJECT_HEADER_LEN + entry_idx * CLUSTER_DATA_OBJECT_DIR_ENTRY_LEN;
        let cluster_idx =
            u32::from_le_bytes(data[base..base + 4].try_into().map_err(|_| {
                ZeppelinError::Index("cluster data object index parse error".into())
            })?) as usize;
        if !seen.insert(cluster_idx) {
            return Err(ZeppelinError::Index(format!(
                "duplicate cluster {cluster_idx} in cluster data object"
            )));
        }

        let offset = read_u64_usize(data, base + 4, "cluster data object section offset")?;
        let len = read_u64_usize(data, base + 12, "cluster data object section length")?;
        if offset < payload_start {
            return Err(ZeppelinError::Index(format!(
                "cluster data object section starts inside directory: offset={offset}, payload_start={payload_start}"
            )));
        }
        let end = offset.checked_add(len).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "cluster data object section overflows: offset={offset}, len={len}"
            ))
        })?;
        if end > data.len() {
            return Err(ZeppelinError::Index(format!(
                "cluster data object section out of bounds: end={end}, len={}",
                data.len()
            )));
        }
        sections.push(ClusterObjectSection {
            cluster_idx,
            data: &data[offset..end],
        });
    }

    Ok(Some(sections))
}

fn cluster_section_from_object(data: &[u8], cluster_idx: usize) -> Result<&[u8]> {
    let Some(sections) = cluster_object_sections(data)? else {
        return Ok(data);
    };
    sections
        .into_iter()
        .find(|section| section.cluster_idx == cluster_idx)
        .map(|section| section.data)
        .ok_or_else(|| {
            ZeppelinError::Index(format!(
                "cluster {cluster_idx} missing from cluster data object"
            ))
        })
}

fn full_cluster_section(data: &[u8]) -> Result<&[u8]> {
    if !data.starts_with(CLUSTER_V2_MAGIC) {
        return Ok(data);
    }
    Ok(colocated_cluster_sections(data)?.full)
}

fn colocated_cluster_sections(data: &[u8]) -> Result<ColocatedClusterSections<'_>> {
    if data.len() < CLUSTER_V2_HEADER_LEN {
        return Err(ZeppelinError::Index(
            "v2 cluster blob too small for header".into(),
        ));
    }

    let sq_offset = read_u64_usize(data, 4, "v2 cluster SQ offset")?;
    let sq_len = read_u64_usize(data, 12, "v2 cluster SQ length")?;
    let full_offset = read_u64_usize(data, 20, "v2 cluster full offset")?;
    let full_len = read_u64_usize(data, 28, "v2 cluster full length")?;

    if sq_offset != CLUSTER_V2_HEADER_LEN {
        return Err(ZeppelinError::Index(format!(
            "v2 cluster SQ offset mismatch: expected {CLUSTER_V2_HEADER_LEN}, got {sq_offset}"
        )));
    }
    let expected_full_offset = sq_offset.checked_add(sq_len).ok_or_else(|| {
        ZeppelinError::Index(format!(
            "v2 cluster SQ section overflows: offset={sq_offset}, len={sq_len}"
        ))
    })?;
    if full_offset != expected_full_offset {
        return Err(ZeppelinError::Index(format!(
            "v2 cluster full offset mismatch: expected {expected_full_offset}, got {full_offset}"
        )));
    }
    let expected_len = full_offset.checked_add(full_len).ok_or_else(|| {
        ZeppelinError::Index(format!(
            "v2 cluster full section overflows: offset={full_offset}, len={full_len}"
        ))
    })?;
    if data.len() != expected_len {
        return Err(ZeppelinError::Index(format!(
            "v2 cluster blob size mismatch: expected {expected_len}, got {}",
            data.len()
        )));
    }

    Ok(ColocatedClusterSections {
        sq: &data[sq_offset..full_offset],
        full: &data[full_offset..expected_len],
    })
}

fn read_u64_usize(data: &[u8], offset: usize, label: &str) -> Result<usize> {
    let value = u64::from_le_bytes(
        data[offset..offset + 8]
            .try_into()
            .map_err(|_| ZeppelinError::Index(format!("{label} parse error")))?,
    );
    usize::try_from(value)
        .map_err(|_| ZeppelinError::Index(format!("{label} does not fit in usize: {value}")))
}

/// Attributes blob: JSON-serialized `Vec<Option<HashMap<String, AttributeValue>>>`.
///
/// We use JSON rather than bincode because `AttributeValue` uses
/// `#[serde(untagged)]`, which requires `deserialize_any` -- a method
/// that bincode does not support.
pub(crate) fn serialize_attrs(attrs: &[Option<HashMap<String, AttributeValue>>]) -> Result<Bytes> {
    let encoded = serde_json::to_vec(attrs)?;
    Ok(Bytes::from(encoded))
}

/// Deserialize attributes blob.
pub(crate) fn deserialize_attrs(
    data: &[u8],
) -> Result<Vec<Option<HashMap<String, AttributeValue>>>> {
    Ok(serde_json::from_slice(data)?)
}

// ---------------------------------------------------------------------------
// Build pipeline
// ---------------------------------------------------------------------------

/// Build an IVF-Flat index from the given vectors.
///
/// 1. Train centroids via k-means++.
/// 2. Assign every vector to its nearest centroid.
/// 3. Serialize and write all artifacts to S3.
/// 4. Return an `IvfFlatIndex` handle with the metadata needed for search.
pub async fn build_ivf_flat(
    vectors: &[VectorEntry],
    config: &IndexingConfig,
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
) -> Result<IvfFlatIndex> {
    if vectors.is_empty() {
        return Err(ZeppelinError::Index(
            "cannot build index from empty vector set".into(),
        ));
    }

    let dim = vectors[0].values.len();
    if dim == 0 {
        return Err(ZeppelinError::Index("vector dimension must be > 0".into()));
    }

    // Validate all dimensions match.
    for v in vectors.iter() {
        if v.values.len() != dim {
            return Err(ZeppelinError::DimensionMismatch {
                expected: dim,
                actual: v.values.len(),
            });
        }
    }

    let k = config.default_num_centroids.min(vectors.len());

    info!(
        n = vectors.len(),
        dim = dim,
        k = k,
        namespace = namespace,
        segment_id = segment_id,
        "building IVF-Flat index"
    );

    // --- Step 1: Train centroids ---
    let vec_refs: Vec<&[f32]> = vectors.iter().map(|v| v.values.as_slice()).collect();
    let centroids = train_kmeans(
        &vec_refs,
        dim,
        k,
        config.kmeans_max_iterations,
        config.kmeans_convergence_epsilon,
    )?;

    let num_clusters = centroids.len();
    info!(num_clusters = num_clusters, "k-means training complete");

    // --- Step 2: Assign vectors to clusters ---
    let mut cluster_ids: Vec<Vec<String>> = vec![Vec::new(); num_clusters];
    let mut cluster_vecs: Vec<Vec<Vec<f32>>> = vec![Vec::new(); num_clusters];
    let mut cluster_attrs: Vec<Vec<Option<HashMap<String, AttributeValue>>>> =
        vec![Vec::new(); num_clusters];
    let mut buddy_affinity: Vec<Vec<u32>> = vec![vec![0; num_clusters]; num_clusters];

    for entry in vectors {
        let mut best_dist = f32::MAX;
        let mut second_dist = f32::MAX;
        let mut best_cluster = 0usize;
        let mut second_cluster = 0usize;
        for (c, centroid) in centroids.iter().enumerate() {
            let d = distance::euclidean_distance(&entry.values, centroid);
            if d < best_dist {
                second_dist = best_dist;
                second_cluster = best_cluster;
                best_dist = d;
                best_cluster = c;
            } else if d < second_dist {
                second_dist = d;
                second_cluster = c;
            }
        }
        if num_clusters > 1 && second_cluster != best_cluster {
            buddy_affinity[best_cluster][second_cluster] =
                buddy_affinity[best_cluster][second_cluster].saturating_add(1);
            buddy_affinity[second_cluster][best_cluster] =
                buddy_affinity[second_cluster][best_cluster].saturating_add(1);
        }
        cluster_ids[best_cluster].push(entry.id.clone());
        cluster_vecs[best_cluster].push(entry.values.clone());
        cluster_attrs[best_cluster].push(entry.attributes.clone());
    }

    for (i, ids) in cluster_ids.iter().enumerate() {
        debug!(cluster = i, count = ids.len(), "cluster assignment");
    }

    // --- Step 3: Write artifacts to S3 ---
    let quantization = config.quantization;
    let sq_calibration = if matches!(quantization, QuantizationType::Scalar) {
        Some(crate::index::quantization::sq::SqCalibration::calibrate(
            &vec_refs, dim,
        ))
    } else {
        None
    };
    let sq_calibration_bytes = sq_calibration.as_ref().map(|cal| cal.to_bytes());

    // Write centroids.
    let centroids_data = if let Some(bytes) = sq_calibration_bytes.as_ref() {
        serialize_centroids_with_sq_calibration(&centroids, dim, Some(bytes.as_ref()))?
    } else {
        serialize_centroids(&centroids, dim)?
    };
    let ckey = centroids_key(namespace, segment_id);
    store.put(&ckey, centroids_data.clone()).await?;
    debug!(key = %ckey, "wrote centroids");

    // CPU phase: pre-serialize all cluster sections and sidecars.
    let mut bitmap_fields_set = std::collections::HashSet::new();
    let mut cluster_sections: Vec<Bytes> = Vec::with_capacity(num_clusters);
    let mut sidecar_payloads: Vec<(String, Bytes)> = Vec::new();
    for i in 0..num_clusters {
        let cvec_data = if let Some(cal) = &sq_calibration {
            let cluster_refs: Vec<&[f32]> = cluster_vecs[i].iter().map(|v| v.as_slice()).collect();
            let codes = cal.encode_batch(&cluster_refs);
            serialize_colocated_sq_cluster(&cluster_ids[i], &cluster_vecs[i], &codes, dim)?
        } else {
            serialize_cluster(&cluster_ids[i], &cluster_vecs[i], dim)?
        };
        cluster_sections.push(cvec_data);

        let cattr_data = serialize_attrs(&cluster_attrs[i])?;
        let cattr_key = attrs_key(namespace, segment_id, i);
        sidecar_payloads.push((cattr_key, cattr_data));

        if config.bitmap_index {
            let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
                cluster_attrs[i].iter().map(|a| a.as_ref()).collect();
            let bitmap_index = crate::index::bitmap::build::build_cluster_bitmaps(&attr_refs);
            for field_name in bitmap_index.fields.keys() {
                bitmap_fields_set.insert(field_name.clone());
            }
            let bitmap_data = bitmap_index.to_bytes()?;
            let bkey = crate::index::bitmap::bitmap_key(namespace, segment_id, i);
            sidecar_payloads.push((bkey, bitmap_data));
        }
    }
    let bitmap_fields: Vec<String> = bitmap_fields_set.into_iter().collect();

    let mut cluster_objects = Vec::new();
    let mut cluster_object_payloads = Vec::new();
    for (group_idx, group) in density_cluster_groups(&centroids, &buddy_affinity)?
        .into_iter()
        .enumerate()
    {
        let entries: Vec<(usize, Bytes)> = group
            .iter()
            .map(|&cluster_idx| (cluster_idx, cluster_sections[cluster_idx].clone()))
            .collect();
        let key = cluster_group_key(namespace, segment_id, group_idx);
        let data = serialize_cluster_data_object(&entries)?;
        cluster_objects.push(ClusterDataObjectRef {
            key: key.clone(),
            clusters: group,
        });
        cluster_object_payloads.push((key, data));
    }

    // I/O phase: write all cluster data and sidecars in parallel.
    let mut write_futs = Vec::new();
    for (key, data) in &cluster_object_payloads {
        write_futs.push(store.put(key, data.clone()));
    }
    for (key, data) in &sidecar_payloads {
        write_futs.push(store.put(key, data.clone()));
    }
    let results = futures::future::join_all(write_futs).await;
    for result in results {
        result?;
    }
    debug!(
        num_clusters,
        cluster_objects = cluster_objects.len(),
        "wrote all grouped cluster data"
    );

    // --- Step 4: Write resident coarse sketch ---
    let (sketch_ref, sketch_data, resident_sketch) =
        build_resident_sketch(namespace, segment_id, dim, &cluster_vecs, &cluster_attrs)?;
    store.put(&sketch_ref.key, sketch_data.clone()).await?;
    info!(
        key = %sketch_ref.key,
        code_dims = sketch_ref.code_dims,
        bytes_per_vector = sketch_ref.bytes_per_vector,
        size_bytes = sketch_ref.size_bytes,
        "wrote resident coarse sketch"
    );

    // --- Step 5: Write segment bootstrap artifact ---
    let (bootstrap_ref, bootstrap_data) =
        build_bootstrap_artifact(namespace, segment_id, &centroids_data, &sketch_data)?;
    store.put(&bootstrap_ref.key, bootstrap_data).await?;
    info!(
        key = %bootstrap_ref.key,
        size_bytes = bootstrap_ref.size_bytes,
        "wrote segment bootstrap"
    );

    // --- Step 6: Write quantized artifacts (if configured) ---
    match quantization {
        QuantizationType::Scalar => {
            info!("wrote SQ8 co-located clusters and embedded calibration");
        }
        QuantizationType::Product => {
            use crate::index::quantization::pq::{
                pq_cluster_key, pq_codebook_key, serialize_pq_cluster, PqCodebook,
            };

            let pq_m = config.pq_m;
            // Train PQ codebook on all vectors.
            let codebook = PqCodebook::train(&vec_refs, dim, pq_m, config.kmeans_max_iterations)?;
            let cb_bytes = codebook.to_bytes();
            store
                .put(&pq_codebook_key(namespace, segment_id), cb_bytes)
                .await?;
            debug!(m = pq_m, "wrote PQ codebook");

            // CPU phase: encode all clusters.
            let mut pq_payloads: Vec<(String, Bytes)> = Vec::with_capacity(num_clusters);
            for i in 0..num_clusters {
                let cluster_refs: Vec<&[f32]> =
                    cluster_vecs[i].iter().map(|v| v.as_slice()).collect();
                let codes = codebook.encode_batch(&cluster_refs);
                let pq_data = serialize_pq_cluster(&cluster_ids[i], &codes, pq_m)?;
                pq_payloads.push((pq_cluster_key(namespace, segment_id, i), pq_data));
            }

            // I/O phase: write all PQ clusters in parallel.
            let write_futs: Vec<_> = pq_payloads
                .iter()
                .map(|(key, data)| store.put(key, data.clone()))
                .collect();
            let results = futures::future::join_all(write_futs).await;
            for result in results {
                result?;
            }
            info!(m = pq_m, "wrote PQ-encoded clusters");
        }
        QuantizationType::None => {}
    }

    info!(
        namespace = namespace,
        segment_id = segment_id,
        num_vectors = vectors.len(),
        num_clusters = num_clusters,
        dim = dim,
        quantization = ?quantization,
        "IVF-Flat index build complete"
    );

    let cluster_object_by_cluster = build_cluster_object_lookup(num_clusters, &cluster_objects)?;
    Ok(IvfFlatIndex {
        centroids,
        num_vectors: vectors.len(),
        dim,
        namespace: namespace.to_string(),
        segment_id: segment_id.to_string(),
        quantization,
        sq_calibration,
        bitmap_fields,
        // Freshly built segment: every cluster owned by this segment.
        cluster_owners: Vec::new(),
        cluster_objects,
        cluster_object_by_cluster,
        resident_sketch: Some(resident_sketch),
        sketch_ref: Some(sketch_ref),
        bootstrap_ref: Some(bootstrap_ref),
    })
}

/// Load an IVF-Flat index using pre-known metadata from the manifest.
///
/// Fetches the bootstrap object when present, otherwise fetches legacy
/// centroids plus resident sketch artifacts. It skips the cluster-count probe
/// loop and quantization-type detection that `load_ivf_flat` performs, saving
/// ~18 S3 GETs per query.
///
/// When `cache` is provided, the bootstrap or legacy metadata blobs are served
/// through the tiered cache (memory → disk → S3) and pinned for the
/// namespace's active segment: `pin_scoped` keeps them safe from LRU eviction
/// and automatically unpins the previous segment's key on rotation.
/// Cache errors are NOT swallowed — a failed fetch fails the load.
#[allow(clippy::too_many_arguments)]
pub async fn load_ivf_flat_from_manifest(
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
    num_vectors: usize,
    quantization: QuantizationType,
    cluster_owners: Vec<String>,
    cluster_objects: Vec<ClusterDataObjectRef>,
    sketch_ref: Option<crate::wal::manifest::SketchRef>,
    bootstrap_ref: Option<BootstrapRef>,
    cache: Option<&std::sync::Arc<crate::cache::DiskCache>>,
) -> Result<IvfFlatIndex> {
    let (centroids_data, resident_sketch) = match bootstrap_ref.as_ref() {
        Some(bootstrap_ref) => {
            load_bootstrap_artifacts(store, namespace, bootstrap_ref, sketch_ref.as_ref(), cache)
                .await?
        }
        None => {
            let ckey = centroids_key(namespace, segment_id);
            let data = match cache {
                Some(c) => {
                    let data = c.get_or_fetch(&ckey, || store.get(&ckey)).await?;
                    // This load path is only used for the manifest's active segment;
                    // pin its centroids (unpinning the previous segment's).
                    c.pin_scoped(&format!("{namespace}:centroids"), &ckey).await;
                    c.unpin_scoped(&format!("{namespace}:bootstrap")).await;
                    data
                }
                None => store.get(&ckey).await?,
            };
            let centroids_data = deserialize_centroids_data(&data)?;
            let resident_sketch =
                load_resident_sketch(store, namespace, sketch_ref.as_ref(), cache).await?;
            (centroids_data, resident_sketch)
        }
    };
    let sq_calibration = centroids_data
        .sq_calibration
        .as_ref()
        .map(|bytes| crate::index::quantization::sq::SqCalibration::from_bytes(bytes))
        .transpose()?;
    let centroids = centroids_data.centroids;
    let dim = centroids_data.dim;
    let cluster_object_by_cluster = build_cluster_object_lookup(centroids.len(), &cluster_objects)?;

    info!(
        namespace = namespace,
        segment_id = segment_id,
        num_vectors = num_vectors,
        num_clusters = centroids.len(),
        dim = dim,
        quantization = ?quantization,
        "loaded IVF-Flat index from manifest metadata"
    );

    Ok(IvfFlatIndex {
        centroids,
        num_vectors,
        dim,
        namespace: namespace.to_string(),
        segment_id: segment_id.to_string(),
        quantization,
        sq_calibration,
        bitmap_fields: Vec::new(), // Populated from SegmentRef at search time
        cluster_owners,
        cluster_objects,
        cluster_object_by_cluster,
        resident_sketch,
        sketch_ref,
        bootstrap_ref,
    })
}

async fn load_bootstrap_artifacts(
    store: &ZeppelinStore,
    namespace: &str,
    bootstrap_ref: &BootstrapRef,
    sketch_ref: Option<&crate::wal::manifest::SketchRef>,
    cache: Option<&std::sync::Arc<crate::cache::DiskCache>>,
) -> Result<(CentroidsData, Option<ResidentSketch>)> {
    let Some(sketch_ref) = sketch_ref else {
        return Err(ZeppelinError::Index(format!(
            "bootstrap {} present but segment is missing sketch ref",
            bootstrap_ref.key
        )));
    };

    let data = match cache {
        Some(c) => {
            let data = c
                .get_or_fetch(&bootstrap_ref.key, || store.get(&bootstrap_ref.key))
                .await?;
            c.unpin_scoped(&format!("{namespace}:centroids")).await;
            c.unpin_scoped(&format!("{namespace}:coarse_sketch")).await;
            c.pin_scoped(&format!("{namespace}:bootstrap"), &bootstrap_ref.key)
                .await;
            data
        }
        None => store.get(&bootstrap_ref.key).await?,
    };
    if bootstrap_ref.size_bytes != data.len() as u64 {
        return Err(ZeppelinError::Index(format!(
            "bootstrap size mismatch for {}: manifest={}, object={}",
            bootstrap_ref.key,
            bootstrap_ref.size_bytes,
            data.len()
        )));
    }

    let sections = deserialize_bootstrap(&data)?;
    if sketch_ref.size_bytes != sections.sketch.len() as u64 {
        return Err(ZeppelinError::Index(format!(
            "coarse sketch size mismatch inside bootstrap {}: manifest={}, section={}",
            bootstrap_ref.key,
            sketch_ref.size_bytes,
            sections.sketch.len()
        )));
    }
    let centroids_data = deserialize_centroids_data(sections.centroids)?;
    let sketch = ResidentSketch::from_bytes(sections.sketch)?;
    Ok((centroids_data, Some(sketch)))
}

async fn load_resident_sketch(
    store: &ZeppelinStore,
    namespace: &str,
    sketch_ref: Option<&crate::wal::manifest::SketchRef>,
    cache: Option<&std::sync::Arc<crate::cache::DiskCache>>,
) -> Result<Option<ResidentSketch>> {
    let Some(sketch_ref) = sketch_ref else {
        return Ok(None);
    };

    let data = match cache {
        Some(c) => {
            let data = c
                .get_or_fetch(&sketch_ref.key, || store.get(&sketch_ref.key))
                .await?;
            c.pin_scoped(&format!("{namespace}:coarse_sketch"), &sketch_ref.key)
                .await;
            data
        }
        None => store.get(&sketch_ref.key).await?,
    };
    let sketch = ResidentSketch::from_bytes(&data)?;
    if sketch_ref.size_bytes != data.len() as u64 {
        return Err(ZeppelinError::Index(format!(
            "coarse sketch size mismatch for {}: manifest={}, object={}",
            sketch_ref.key,
            sketch_ref.size_bytes,
            data.len()
        )));
    }
    Ok(Some(sketch))
}

/// Load an existing IVF-Flat index from S3 artifacts.
///
/// Only the centroids are loaded into memory; cluster data is fetched
/// on demand during search. Detects available quantization by probing
/// for calibration/codebook artifacts.
pub async fn load_ivf_flat(
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
) -> Result<IvfFlatIndex> {
    let ckey = centroids_key(namespace, segment_id);
    let data = store.get(&ckey).await?;
    let centroids_data = deserialize_centroids_data(&data)?;
    let sq_calibration = centroids_data
        .sq_calibration
        .as_ref()
        .map(|bytes| crate::index::quantization::sq::SqCalibration::from_bytes(bytes))
        .transpose()?;
    let has_embedded_sq_calibration = sq_calibration.is_some();
    let centroids = centroids_data.centroids;
    let dim = centroids_data.dim;

    // Count total vectors by summing cluster sizes.
    let num_clusters = centroids.len();
    let mut num_vectors = 0usize;
    let segment_prefix = format!("{namespace}/segments/{segment_id}/");
    let segment_keys = store.list_prefix(&segment_prefix).await?;
    let mut cluster_objects: Vec<ClusterDataObjectRef> = Vec::new();
    let mut cluster_object_keys: Vec<String> = segment_keys
        .iter()
        .filter(|key| {
            key.rsplit('/')
                .next()
                .map(|filename| {
                    filename.starts_with("cluster_pair_") || filename.starts_with("cluster_group_")
                })
                .unwrap_or(false)
        })
        .cloned()
        .collect();
    cluster_object_keys.sort();
    for key in cluster_object_keys {
        let data = store.get(&key).await?;
        let sections = cluster_object_sections(&data)?.ok_or_else(|| {
            ZeppelinError::Index(format!(
                "cluster object key {key} did not contain grouped data"
            ))
        })?;
        for section in &sections {
            let cluster = deserialize_cluster(section.data)?;
            num_vectors += cluster.ids.len();
        }
        let clusters = sections
            .into_iter()
            .map(|section| section.cluster_idx)
            .collect::<Vec<_>>();
        cluster_objects.push(ClusterDataObjectRef { key, clusters });
    }

    if cluster_objects.is_empty() {
        for i in 0..num_clusters {
            let cvec_key = cluster_key(namespace, segment_id, i);
            let cluster_data = store.get(&cvec_key).await?;
            let cluster = deserialize_cluster(&cluster_data)?;
            num_vectors += cluster.ids.len();
        }
    }
    let cluster_object_by_cluster = build_cluster_object_lookup(num_clusters, &cluster_objects)?;

    // Detect quantization: check for PQ codebook first, then embedded or
    // legacy SQ calibration.
    let quantization = {
        use crate::index::quantization::pq::pq_codebook_key;
        use crate::index::quantization::sq::sq_calibration_key;

        let pq_key = pq_codebook_key(namespace, segment_id);
        if store.get(&pq_key).await.is_ok() {
            QuantizationType::Product
        } else if has_embedded_sq_calibration {
            QuantizationType::Scalar
        } else {
            let sq_key = sq_calibration_key(namespace, segment_id);
            if store.get(&sq_key).await.is_ok() {
                QuantizationType::Scalar
            } else {
                QuantizationType::None
            }
        }
    };

    info!(
        namespace = namespace,
        segment_id = segment_id,
        num_vectors = num_vectors,
        num_clusters = num_clusters,
        dim = dim,
        quantization = ?quantization,
        "loaded IVF-Flat index"
    );

    Ok(IvfFlatIndex {
        centroids,
        num_vectors,
        dim,
        namespace: namespace.to_string(),
        segment_id: segment_id.to_string(),
        quantization,
        sq_calibration,
        bitmap_fields: Vec::new(), // Populated from SegmentRef at search time
        // Probing loader is used by compaction to read a segment it will fully
        // rewrite, and by tests — legacy single-segment layout.
        cluster_owners: Vec::new(),
        cluster_objects,
        cluster_object_by_cluster,
        resident_sketch: None,
        sketch_ref: None,
        bootstrap_ref: None,
    })
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize_centroids() {
        let centroids = vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]];
        let data = serialize_centroids(&centroids, 3).unwrap();
        let (decoded, dim) = deserialize_centroids(&data).unwrap();
        assert_eq!(dim, 3);
        assert_eq!(decoded, centroids);
    }

    #[test]
    fn test_serialize_deserialize_bootstrap_sections() {
        let centroids = b"centroid-bytes";
        let sketch = b"sketch-bytes";
        let data = serialize_bootstrap(centroids, sketch).unwrap();
        let sections = deserialize_bootstrap(&data).unwrap();
        assert_eq!(sections.centroids, centroids);
        assert_eq!(sections.sketch, sketch);
    }

    #[test]
    fn test_deserialize_bootstrap_rejects_malformed_header() {
        let data = serialize_bootstrap(b"centroids", b"sketch").unwrap();

        let mut bad_magic = data.to_vec();
        bad_magic[0] = b'X';
        assert!(deserialize_bootstrap(&bad_magic).is_err());

        let mut bad_version = data.to_vec();
        bad_version[4..8].copy_from_slice(&99u32.to_le_bytes());
        assert!(deserialize_bootstrap(&bad_version).is_err());

        let mut bad_bounds = data.to_vec();
        bad_bounds[32..40].copy_from_slice(&999u64.to_le_bytes());
        assert!(deserialize_bootstrap(&bad_bounds).is_err());
    }

    #[test]
    fn test_serialize_deserialize_cluster() {
        let ids = vec!["vec_1".to_string(), "vec_2".to_string()];
        let vecs = vec![vec![1.0, 2.0], vec![3.0, 4.0]];
        let data = serialize_cluster(&ids, &vecs, 2).unwrap();
        let cluster = deserialize_cluster(&data).unwrap();
        assert_eq!(cluster.ids, ids);
        assert_eq!(cluster.vectors, vecs);
    }

    #[test]
    fn test_serialize_deserialize_attrs() {
        let mut attrs_map = HashMap::new();
        attrs_map.insert(
            "color".to_string(),
            AttributeValue::String("red".to_string()),
        );
        let attrs = vec![Some(attrs_map), None];

        let data = serialize_attrs(&attrs).unwrap();
        let decoded = deserialize_attrs(&data).unwrap();
        assert_eq!(decoded.len(), 2);
        assert!(decoded[0].is_some());
        assert!(decoded[1].is_none());
    }

    #[test]
    fn test_centroids_header_too_small() {
        let data = vec![0u8; 4]; // less than 8 bytes
        assert!(deserialize_centroids(&data).is_err());
    }

    #[test]
    fn test_cluster_header_too_small() {
        let data = vec![0u8; 4];
        assert!(deserialize_cluster(&data).is_err());
    }

    #[test]
    fn density_grouping_respects_cap_and_leaves_sparse_tail() {
        let centroids = vec![
            vec![0.0, 0.0],
            vec![0.1, 0.0],
            vec![0.2, 0.0],
            vec![0.3, 0.0],
            vec![10.0, 0.0],
            vec![20.0, 0.0],
        ];
        let affinity = vec![vec![0; centroids.len()]; centroids.len()];

        let groups = density_cluster_groups_with_cap(&centroids, &affinity, 4).unwrap();

        assert!(groups.iter().all(|group| group.len() <= 4));
        assert!(groups.iter().any(|group| group.as_slice() == &[0, 1, 2, 3]));
        assert!(groups.iter().any(|group| group.as_slice() == &[4]));
        assert!(groups.iter().any(|group| group.as_slice() == &[5]));
    }

    #[test]
    fn test_deserialize_centroids_truncated_floats() {
        // Header says 2 centroids dim=3 but only provide 1 centroid of float data
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_le_bytes()); // num_centroids = 2
        buf.extend_from_slice(&3u32.to_le_bytes()); // dim = 3
                                                    // Only provide 1 centroid worth of floats (3 floats = 12 bytes) instead of 2 (24 bytes)
        for _ in 0..3 {
            buf.extend_from_slice(&1.0f32.to_le_bytes());
        }
        let result = deserialize_centroids(&buf);
        assert!(result.is_err());
        match result.unwrap_err() {
            ZeppelinError::Index(msg) => assert!(msg.contains("mismatch"), "got: {msg}"),
            other => panic!("expected Index error, got: {other}"),
        }
    }

    #[test]
    fn test_deserialize_cluster_truncated_vector() {
        // Header says 1 vector dim=4 but truncate after 2 floats
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes()); // n = 1
        buf.extend_from_slice(&4u32.to_le_bytes()); // dim = 4
                                                    // id
        let id = b"vec_0";
        buf.extend_from_slice(&(id.len() as u32).to_le_bytes());
        buf.extend_from_slice(id);
        // Only 2 floats instead of 4
        buf.extend_from_slice(&1.0f32.to_le_bytes());
        buf.extend_from_slice(&2.0f32.to_le_bytes());
        let result = deserialize_cluster(&buf);
        assert!(result.is_err());
        match result.unwrap_err() {
            ZeppelinError::Index(msg) => assert!(msg.contains("truncated"), "got: {msg}"),
            other => panic!("expected Index error, got: {other}"),
        }
    }

    #[tokio::test]
    async fn test_load_from_manifest_uses_bootstrap_when_present() {
        let store = ZeppelinStore::new(std::sync::Arc::new(object_store::memory::InMemory::new()));
        let namespace = "ns_bootstrap";
        let segment_id = "seg_bootstrap";
        let centroids = vec![vec![0.0, 0.0], vec![10.0, 10.0]];
        let centroids_data = serialize_centroids(&centroids, 2).unwrap();
        let cluster_vecs = vec![vec![vec![0.1, 0.0]], vec![vec![9.9, 10.1]]];
        let cluster_attrs = vec![vec![None], vec![None]];
        let (sketch_ref, sketch_data, _) =
            build_resident_sketch(namespace, segment_id, 2, &cluster_vecs, &cluster_attrs).unwrap();
        let (bootstrap_ref, bootstrap_data) =
            build_bootstrap_artifact(namespace, segment_id, &centroids_data, &sketch_data).unwrap();
        store.put(&bootstrap_ref.key, bootstrap_data).await.unwrap();

        let index = load_ivf_flat_from_manifest(
            &store,
            namespace,
            segment_id,
            2,
            QuantizationType::None,
            Vec::new(),
            Vec::new(),
            Some(sketch_ref),
            Some(bootstrap_ref),
            None,
        )
        .await
        .unwrap();

        assert_eq!(index.num_clusters(), 2);
        assert!(index.resident_sketch.is_some());
    }

    #[tokio::test]
    async fn test_load_from_manifest_without_bootstrap_uses_legacy_artifacts() {
        let store = ZeppelinStore::new(std::sync::Arc::new(object_store::memory::InMemory::new()));
        let namespace = "ns_legacy_bootstrap";
        let segment_id = "seg_legacy_bootstrap";
        let centroids = vec![vec![0.0, 0.0], vec![10.0, 10.0]];
        let centroids_data = serialize_centroids(&centroids, 2).unwrap();
        store
            .put(&centroids_key(namespace, segment_id), centroids_data)
            .await
            .unwrap();
        let cluster_vecs = vec![vec![vec![0.1, 0.0]], vec![vec![9.9, 10.1]]];
        let cluster_attrs = vec![vec![None], vec![None]];
        let (sketch_ref, sketch_data, _) =
            build_resident_sketch(namespace, segment_id, 2, &cluster_vecs, &cluster_attrs).unwrap();
        store.put(&sketch_ref.key, sketch_data).await.unwrap();

        let index = load_ivf_flat_from_manifest(
            &store,
            namespace,
            segment_id,
            2,
            QuantizationType::None,
            Vec::new(),
            Vec::new(),
            Some(sketch_ref),
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(index.num_clusters(), 2);
        assert!(index.resident_sketch.is_some());
        assert!(index.bootstrap_ref.is_none());
    }
}
