//! Builds, encodes, uploads, and reloads immutable IVF-Flat segment artifacts.
//!
//! This file is the storage-facing half of IVF-Flat. Compaction enters through
//! [`build_ivf_flat`] with a complete vector snapshot, and query planning
//! normally enters through [`load_ivf_flat_from_manifest`] with artifact
//! metadata copied from the authoritative manifest. [`load_ivf_flat`] is the
//! slower probing loader used when manifest metadata is not available. Search
//! itself lives in `search.rs`; centroid training, membership, and the resident
//! coarse sketch live in sibling modules.
//!
//! An inverted-file (IVF) index partitions vectors around learned centroids.
//! At query time, `nprobe` controls how many nearby clusters are considered:
//! probing more clusters usually improves recall but reads and scores more
//! data. "Flat" means the final rerank still uses the stored full-precision
//! vectors; scalar or product quantization may supply an earlier coarse pass.
//!
//! The build path deliberately separates CPU work from object-store I/O. It
//! trains centroids, assigns rows, builds optional bitmap and quantization
//! sidecars, groups nearby clusters into bounded objects, and uploads those
//! immutable objects through [`ZeppelinStore`]. Uploading does **not** make a
//! segment visible. The compactor later places the returned references in a
//! [`SegmentRef`][crate::wal::manifest::SegmentRef] and publishes the manifest
//! with compare-and-swap. If construction fails after some PUTs, those objects
//! may remain unreferenced; readers must never discover them by listing alone.
//!
//! ```text
//! validated VectorEntry snapshot
//!              |
//!              v
//! train centroids -> assign rows -> group nearby clusters
//!              |                         |
//!              |                         +--> attributes / bitmaps
//!              |                         +--> SQ or PQ coarse data
//!              v
//! serialize immutable cluster, membership, sketch, and bootstrap bytes
//!              |
//!              v
//! parallel/ordered PUTs to S3 or MinIO   (objects exist, not visible)
//!              |
//!              v
//! return IvfFlatIndex + artifact refs
//!              |
//!              v
//! compaction publishes SegmentRef by manifest CAS   (now visible)
//! ```
//!
//! ## Reading map
//!
//! 1. Start with [`build_ivf_flat`] for the seven construction phases and the
//!    artifact-visibility boundary.
//! 2. Read `density_cluster_groups` and `build_cluster_object_lookup` for the
//!    logical-cluster-to-physical-object layout.
//! 3. Read the bootstrap, centroid, cluster-object, and attribute serializers
//!    as independent persisted-format contracts.
//! 4. Read [`load_ivf_flat_from_manifest`] and `load_bootstrap_artifacts` for
//!    the normal cached query-loading path.
//! 5. Finish with [`load_ivf_flat`] to understand the more expensive legacy
//!    path that reconstructs metadata by listing and probing objects.
//!
//! ## Invariants
//!
//! - Segment artifacts are write-once. Rebuilds use a new `segment_id`; this
//!   module never mutates a published object in place.
//! - The manifest, not object existence or a local cache entry, defines which
//!   segment and object layout readers may use.
//! - Every vector in one build has the same non-zero dimension, and IDs,
//!   vectors, attributes, and quantized codes retain the same row order.
//! - Every logical cluster appears exactly once in a non-legacy grouped-object
//!   layout. Malformed, duplicate, missing, truncated, or overflowing metadata
//!   fails loudly instead of selecting a fallback value.
//! - Persisted little-endian formats remain independently versioned. Legacy
//!   centroid and per-cluster objects remain readable alongside current ones.
//! - Cache hits may avoid object-store GETs, but cached metadata is accepted
//!   only for the manifest-provided key and declared sizes.
//!
//! ## Rust concepts used here
//!
//! Build inputs are borrowed slices, roughly like read-only Java views or C
//! `const` pointer/length pairs, while returned [`IvfFlatIndex`]
//! owns its strings and metadata. Temporary `Vec<&[f32]>` values borrow vector
//! payloads without duplicating floats; the compiler prevents those views from
//! outliving the input snapshot. [`Bytes`] moves encoded buffers into cheap,
//! reference-counted immutable handles, so clones used by parallel PUT futures
//! share payload memory rather than copying whole artifacts.
//!
//! [`Arc`] shares decoded centroids and sketches among index handles and cache
//! entries. [`OnceLock`] initializes the process-wide decoded-bootstrap map on
//! first use, while [`DashMap`] permits concurrent lookup without one global
//! mutex. This resembles Java concurrent-map references; unlike manual C
//! ownership, Rust's types arrange destruction only after the last owner goes
//! away. `Result` and `?` make every validation or I/O failure explicit and
//! prevent later phases from running after an error.
//!
//! ## Persisted-format compatibility
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
//! - Quantized cluster sections: `[magic][coarse_offset:u64][coarse_len:u64]
//!   [full_offset:u64][full_len:u64][coarse bytes][full cluster bytes]`.
//!   `ZCL2` stores SQ8 coarse bytes and `ZCL3` stores two-bit RaBitQ bytes.
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
//! - `ZBP5` grouped objects hoist IDs out of the vector block:
//!   `[b"ZBP", version=5][entry_count:u32]` followed by directory entries of
//!   `cluster_idx:u32, row_count:u32` and absolute `coarse`, `ids`, and
//!   `vectors` ranges, then all coarse blocks, all ID blocks, and all
//!   fixed-stride f32 blocks in entry order. A coarse block carries codes and
//!   factors only; an ID block is `row_count:u32` then repeated
//!   `[id_len:u32][UTF-8 id]`; a vector block is exactly `row_count × dim × 4`
//!   bytes with no per-row header or ID, so row `r` starts at
//!   `vectors_offset + r × dim × 4`. Regions tile the object exactly; any gap,
//!   overlap, or trailing byte is an error.
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
//! - Incremental compaction migrates only rewritten clusters by writing the new co-located
//!   object under the new segment. Task 2B carried clusters keep their old owner
//!   string in `cluster_owners`, so their old physical keys stay authoritative.
//! - PQ is deliberately deferred. The current requirement and GET-count target
//!   are specific to SQ calibration and SQ cluster/full-vector co-location; PQ
//!   keeps `pq_codebook.bin` and `pq_cluster_i.bin` unchanged for this phase.

use bytes::Bytes;
use dashmap::DashMap;
use std::collections::{BTreeSet, HashMap};
use std::ops::Range;
use std::sync::{Arc, OnceLock, Weak};
use tracing::{debug, info};

use crate::config::IndexingConfig;
use crate::error::{Result, ZeppelinError};
use crate::index::quantization::QuantizationType;
use crate::storage::ZeppelinStore;
use crate::types::{AttributeValue, VectorEntry};
use crate::wal::manifest::{
    BootstrapRef, ClusterDataObjectRef, ClusterRowLayoutRef, CoarsePayloadEncoding,
    CLUSTER_LAYOUT_VERSION_ZBP5,
};

use super::kmeans::{repair_cluster_balance, train_kmeans};
use super::membership::build_membership_artifact;
use super::sketch::{build_resident_sketch, decode_resident_sketch, ResidentSketch};
use super::IvfFlatIndex;
use crate::index::distance;

/// Four-byte signature for current centroid objects.
const CENTROIDS_V2_MAGIC: &[u8; 4] = b"ZCT2";
/// Four-byte signature for one SQ-and-full-vector cluster section.
const CLUSTER_V2_MAGIC: &[u8; 4] = b"ZCL2";
/// Four-byte signature for one RQ-and-full-vector cluster section.
const CLUSTER_V3_MAGIC: &[u8; 4] = b"ZCL3";
/// Fixed bytes preceding the SQ and full-vector payloads in a v2 section.
const CLUSTER_V2_HEADER_LEN: usize = 4 + 8 * 4;
/// Four-byte signature for grouped objects containing unsplit cluster sections.
const CLUSTER_DATA_OBJECT_V1_MAGIC: &[u8; 4] = b"ZBP1";
/// Shared prefix used to recognize versioned grouped cluster-data objects.
const CLUSTER_DATA_OBJECT_MAGIC_PREFIX: &[u8; 3] = b"ZBP";
/// Version byte for grouped objects whose SQ and full blocks are separated.
const CLUSTER_DATA_OBJECT_V4_VERSION: u8 = 4;
/// Magic/version plus entry-count bytes shared by grouped-object formats.
const CLUSTER_DATA_OBJECT_HEADER_LEN: usize = 8;
/// Bytes in one v1 directory tuple: cluster index, offset, and length.
const CLUSTER_DATA_OBJECT_DIR_ENTRY_LEN: usize = 4 + 8 + 8;
/// Bytes in one v4 tuple containing separate SQ and full-vector ranges.
const CLUSTER_DATA_OBJECT_V4_DIR_ENTRY_LEN: usize = 4 + 8 + 8 + 8 + 8;
/// Version byte for grouped objects with hoisted ID blocks and fixed-stride
/// f32 vector blocks.
const CLUSTER_DATA_OBJECT_V5_VERSION: u8 = 5;
/// Bytes in one v5 tuple: cluster index, row count, and coarse/IDs/vectors
/// absolute ranges.
const CLUSTER_DATA_OBJECT_V5_DIR_ENTRY_LEN: usize = 4 + 4 + 8 * 6;
/// Four-byte signature for a combined segment-bootstrap object.
const BOOTSTRAP_MAGIC: &[u8; 4] = b"ZBS1";
/// Legacy bootstrap version containing centroids and resident sketch only.
const BOOTSTRAP_VERSION_V1: u32 = 1;
/// Bootstrap version adding segment-wide complete bitmap fields.
const BOOTSTRAP_VERSION_V2: u32 = 2;
/// Current bootstrap version adding the filter-cardinality summary section.
const BOOTSTRAP_VERSION: u32 = 3;
/// Fixed v1 header size before the first embedded artifact.
const BOOTSTRAP_V1_HEADER_LEN: usize = 4 + 4 + 2 * 16;
/// Fixed v2 header size before the first embedded artifact.
const BOOTSTRAP_V2_HEADER_LEN: usize = 4 + 4 + 3 * 16;
/// Fixed current header size before the first embedded artifact.
const BOOTSTRAP_HEADER_LEN: usize = 4 + 4 + 4 * 16;
/// Object-count compromise used when no grouping cap is configured.
const DEFAULT_MAX_CLUSTERS_PER_OBJECT: usize = 3;
/// Environment variable overriding the maximum clusters in a grouped object.
const MAX_CLUSTERS_PER_OBJECT_ENV: &str = "ZEPPELIN_MAX_CLUSTERS_PER_OBJECT";
/// Presence-only switch that emits grouping diagnostics to standard error.
const CLUSTER_GROUP_STATS_ENV: &str = "ZEPPELIN_CLUSTER_GROUP_STATS";

/// Process-wide weak reuse of validated bootstrap metadata by immutable key.
///
/// Entries are safe to reuse because segment keys identify write-once objects.
/// The manifest-provided sizes are still compared before a cached value is
/// accepted, so incompatible metadata fails loudly. Weak values let the owning
/// [`DiskCache`] evict a multi-gigabyte resident sketch without this lookup map
/// retaining it for the process lifetime.
static BOOTSTRAP_DECODED_CACHE: OnceLock<DashMap<String, Weak<DecodedBootstrap>>> = OnceLock::new();

/// Returns the lazily initialized process-wide decoded-bootstrap map.
///
/// # Returns
///
/// A shared map that lives for the process lifetime. Calling this function does
/// not fetch or decode an object.
///
/// # Rust Notes for Java/C Engineers
///
/// `OnceLock` performs thread-safe one-time initialization without requiring
/// callers to coordinate. The `'static` reference is valid for the rest of the
/// process; in C this lifetime would be a convention around global storage.
fn bootstrap_decoded_cache() -> &'static DashMap<String, Weak<DecodedBootstrap>> {
    BOOTSTRAP_DECODED_CACHE.get_or_init(DashMap::new)
}

// ---------------------------------------------------------------------------
// Artifact paths
// ---------------------------------------------------------------------------

/// Constructs the immutable centroid-object key for a segment.
///
/// # Parameters
///
/// - `namespace`: Validated object-store prefix for the namespace.
/// - `segment_id`: Unique identifier of the segment that owns the centroids.
///
/// # Returns
///
/// An owned key ending in `segments/{segment_id}/centroids.bin`; no I/O occurs.
///
/// # Examples
///
/// Namespace `catalog` and segment `seg-7` produce
/// `catalog/segments/seg-7/centroids.bin`.
pub fn centroids_key(namespace: &str, segment_id: &str) -> String {
    format!("{namespace}/segments/{segment_id}/centroids.bin")
}

/// Constructs the immutable combined-bootstrap key for a segment.
///
/// # Parameters
///
/// - `namespace`: Validated object-store prefix for the namespace.
/// - `segment_id`: Unique identifier of the segment that owns the bootstrap.
///
/// # Returns
///
/// An owned key ending in `segments/{segment_id}/bootstrap.bin`; no I/O occurs.
#[must_use]
pub fn bootstrap_key(namespace: &str, segment_id: &str) -> String {
    format!("{namespace}/segments/{segment_id}/bootstrap.bin")
}

/// Constructs a legacy per-cluster full-vector object key.
///
/// # Parameters
///
/// - `namespace`: Validated namespace object-store prefix.
/// - `segment_id`: Physical owner of the cluster object.
/// - `cluster_idx`: Zero-based logical cluster index.
///
/// # Returns
///
/// A key ending in `cluster_{cluster_idx}.bin`. Current full builds use grouped
/// objects; this shape remains necessary for legacy and incremental layouts.
pub(crate) fn cluster_key(namespace: &str, segment_id: &str, cluster_idx: usize) -> String {
    format!("{namespace}/segments/{segment_id}/cluster_{cluster_idx}.bin")
}

/// Constructs the key for one bounded group of logical cluster payloads.
///
/// # Parameters
///
/// - `namespace`: Validated namespace object-store prefix.
/// - `segment_id`: Segment that owns the newly written immutable object.
/// - `group_idx`: Stable zero-based group number produced by build traversal.
///
/// # Returns
///
/// A key ending in `cluster_group_{group_idx}.bin`; no I/O occurs.
pub(crate) fn cluster_group_key(namespace: &str, segment_id: &str, group_idx: usize) -> String {
    format!("{namespace}/segments/{segment_id}/cluster_group_{group_idx}.bin")
}

/// Constructs the sidecar key for one cluster's row-aligned attributes.
///
/// # Parameters
///
/// - `namespace`: Validated namespace object-store prefix.
/// - `segment_id`: Physical owner of the attribute sidecar.
/// - `cluster_idx`: Logical cluster whose row order the sidecar mirrors.
///
/// # Returns
///
/// A key ending in `attrs_{cluster_idx}.bin`; no I/O occurs.
pub fn attrs_key(namespace: &str, segment_id: &str, cluster_idx: usize) -> String {
    format!("{namespace}/segments/{segment_id}/attrs_{cluster_idx}.bin")
}

/// Reads and validates the process environment's cluster-grouping cap.
///
/// # Returns
///
/// The configured positive cap, or [`DEFAULT_MAX_CLUSTERS_PER_OBJECT`] when the
/// variable is absent.
///
/// # Errors
///
/// Returns an index error when the variable is non-Unicode, cannot be parsed as
/// `usize`, or is zero. The function does not silently substitute the default
/// for malformed operator input.
///
/// # Examples
///
/// An absent variable permits up to three clusters per object. A value of `1`
/// forces one object per logical cluster; `0` fails the build.
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

/// Groups nearby logical clusters into capped physical data objects.
///
/// The only external bound is `max_clusters_per_object`. The merge cutoff is
/// derived from the segment's own cap-neighbor centroid distance distribution,
/// so it scales with the embedding space rather than baking in an absolute
/// radius.
///
/// # Parameters
///
/// - `centroids`: One owned centroid vector per logical cluster.
/// - `affinity`: Symmetric boundary-frequency matrix built while assigning
///   vectors. It breaks distance ties in favor of clusters often seen as first
///   and second choices; missing cells behave as zero affinity.
///
/// # Returns
///
/// Deterministic groups sorted by their lowest cluster index. Every input
/// cluster appears once, and each group respects the environment-selected cap.
/// Empty centroid input returns an empty layout.
///
/// # Errors
///
/// Returns an index error for invalid environment configuration, size overflow,
/// fewer than two centroids where a cutoff is required, or a non-finite
/// derived neighbor distance.
///
/// # Performance
///
/// Materializes an `n x n` centroid-distance matrix and examines all centroid
/// pairs, using `O(n^2 * dim)` CPU and `O(n^2)` float memory. Grouping reduces
/// object GET count later but can increase bytes fetched for one hot cluster.
///
/// # Examples
///
/// Four centroids packed near one another and one distant centroid may become
/// groups `[0, 1, 2]`, `[3]`, and `[4]` under a cap of three. Dense clusters are
/// never merged past the configured cap.
pub(crate) fn density_cluster_groups(
    centroids: &[Vec<f32>],
    affinity: &[Vec<u32>],
) -> Result<Vec<Vec<usize>>> {
    let max_clusters_per_object = configured_max_clusters_per_object()?;
    density_cluster_groups_with_cap(centroids, affinity, max_clusters_per_object)
}

/// Implements density grouping with an explicit cap for callers and tests.
///
/// Candidate edges are ordered by increasing centroid distance, then decreasing
/// assignment affinity, then cluster index. Two groups merge only when their
/// combined size fits the cap and every cross-group pair is within the derived
/// cutoff, which is complete-linkage behavior.
///
/// # Parameters
///
/// - `centroids`: One centroid per logical cluster; dimensions must agree.
/// - `affinity`: Optional square boundary-frequency matrix used for tie breaks.
/// - `max_clusters_per_object`: Strict positive upper bound on group size.
///
/// # Returns
///
/// A deterministic partition of cluster indexes. A cap of one returns singleton
/// groups without constructing the quadratic distance matrix.
///
/// # Errors
///
/// Returns an index error when the cap is zero, distance-matrix sizing
/// overflows, or cutoff derivation encounters invalid/non-finite data.
///
/// # Examples
///
/// For centroids at `0.0`, `0.1`, and `10.0` with cap two, the nearby first two
/// can share one object while the distant third remains alone.
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

/// Computes a symmetric row-major matrix of all centroid L2 distances.
///
/// # Parameters
///
/// - `centroids`: Equal-dimensional centroid vectors.
///
/// # Returns
///
/// `n * n` distances with zeroes on the diagonal and mirrored off-diagonal
/// entries.
///
/// # Errors
///
/// Returns an index error if `n * n` overflows `usize`.
///
/// # Panics
///
/// Panics when centroid dimensions differ because the distance primitive
/// requires equal-length slices.
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

/// Reads one cell from a row-major centroid-distance matrix.
///
/// # Parameters
///
/// - `distances`: Flat matrix produced by `centroid_distances`.
/// - `n`: Matrix width in centroids.
/// - `left`: Row index.
/// - `right`: Column index.
///
/// # Returns
///
/// The stored L2 distance between the two logical clusters.
///
/// # Panics
///
/// Panics if either index is outside the declared matrix or if `n` does not
/// match the buffer layout. Callers use indexes derived from the same centroids.
fn centroid_distance(distances: &[f32], n: usize, left: usize, right: usize) -> f32 {
    distances[left * n + right]
}

/// Derives a scale-free merge cutoff from cap-neighbor distances.
///
/// For each centroid, this helper selects the distance to the neighbor rank
/// implied by the group cap, then returns the upper quartile of those values.
/// Dense regions can therefore merge while sparse tails remain separate.
///
/// # Parameters
///
/// - `distances`: Complete `n x n` row-major distance matrix.
/// - `n`: Number of centroids represented by the matrix.
/// - `max_clusters_per_object`: Group cap used to select the neighbor rank.
///
/// # Returns
///
/// A finite L2-distance cutoff at the upper-quartile position.
///
/// # Errors
///
/// Returns an index error when fewer than two centroids are supplied or any
/// selected cap-neighbor distance is NaN or infinite.
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

/// Tests whether every cross-pair between two groups satisfies a cutoff.
///
/// # Parameters
///
/// - `left_group`: Cluster indexes already joined in the first group.
/// - `right_group`: Cluster indexes in the candidate partner group.
/// - `distances`: Complete row-major centroid-distance matrix.
/// - `n`: Matrix width.
/// - `cutoff`: Inclusive maximum permitted cross-group distance.
///
/// # Returns
///
/// `true` only when complete-linkage distance is within the cutoff. Empty input
/// groups vacuously return `true`, although production grouping never passes
/// empty groups.
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

/// Optionally prints a compact cluster-group histogram for operators.
///
/// # Parameters
///
/// - `max_clusters_per_object`: Effective group cap included in the record.
/// - `cutoff`: Derived centroid-distance cutoff.
/// - `groups`: Final non-empty groups whose sizes form the histogram.
///
/// # Side Effects
///
/// Writes one line to standard error only when
/// `ZEPPELIN_CLUSTER_GROUP_STATS` is present. It never changes grouping.
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

/// Validates manifest cluster objects and builds a constant-time owner lookup.
///
/// # Parameters
///
/// - `cluster_count`: Number of logical clusters in the loaded centroid set.
/// - `cluster_objects`: Manifest-defined immutable objects and their logical
///   cluster membership.
/// - `dim`: Segment vector dimension used to check declared row layouts.
///
/// # Returns
///
/// An array where position `i` contains the index of the sole object that owns
/// cluster `i`. An empty manifest layout returns an empty array and signals the
/// legacy per-cluster key convention.
///
/// # Errors
///
/// Returns an index error for an empty object key, an object with no clusters,
/// an out-of-range or duplicate cluster, a cluster listed in two objects, a
/// logical cluster missing from the layout, or an inconsistent declared row
/// layout. No object-store I/O occurs. Validating layouts here is what lets the
/// query path trust manifest ranges without re-reading object headers.
///
/// # Examples
///
/// Objects `A -> [0, 2]` and `B -> [1]` produce lookup `[0, 1, 0]`. Omitting
/// cluster 1 fails rather than silently routing it to a guessed key.
///
/// # Rust Notes for Java/C Engineers
///
/// `usize::MAX` is an internal sentinel only during validation; the function
/// never returns a lookup containing it. `BTreeSet` checks duplicates with
/// deterministic behavior, and `Result` forces callers to handle malformed
/// persisted state before indexing the vector.
pub(crate) fn build_cluster_object_lookup(
    cluster_count: usize,
    cluster_objects: &[ClusterDataObjectRef],
    dim: usize,
) -> Result<Vec<usize>> {
    if cluster_objects.is_empty() {
        return Ok(Vec::new());
    }

    let mut lookup = vec![usize::MAX; cluster_count];
    for (object_idx, object_ref) in cluster_objects.iter().enumerate() {
        object_ref.validate_row_layouts(dim)?;
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

/// Borrowed, validated views into a combined segment bootstrap artifact.
///
/// The views avoid copying the embedded versioned artifact bytes. Their
/// lifetime cannot exceed the input buffer passed to `deserialize_bootstrap`.
#[derive(Debug)]
pub(crate) struct BootstrapSections<'a> {
    /// Complete encoded centroid artifact, including its own version header.
    pub centroids: &'a [u8],
    /// Complete encoded resident-sketch artifact, including its own header.
    pub sketch: &'a [u8],
    /// Absolute range of the sketch section in the source bootstrap bytes.
    pub sketch_range: Range<usize>,
    /// Fields guaranteed to have a bitmap index in every logical cluster.
    pub bitmap_complete_fields: BTreeSet<String>,
    /// Versioned filter-cardinality summary bytes, absent from v1/v2 objects.
    pub filter_summary: Option<&'a [u8]>,
}

/// Serialize a segment bootstrap artifact from existing artifact bytes.
///
/// The centroid, sketch, and filter-summary payloads are embedded verbatim.
/// Their internal formats remain independently versioned by their decoders.
///
/// # Parameters
///
/// - `centroids`: Complete encoded centroid artifact to place first.
/// - `sketch`: Complete encoded resident-sketch artifact to place second.
/// - `bitmap_complete_fields`: Complete-field capability set to place third.
/// - `filter_summary`: Complete encoded cardinality summary to place fourth.
///
/// # Returns
///
/// One owned immutable buffer with a versioned offset/length directory and the
/// four payloads in that order.
///
/// # Errors
///
/// Returns an index error when a required payload is empty or size arithmetic
/// overflows. No object-store write occurs.
///
/// # Performance
///
/// Allocates one buffer and copies each input payload once. The combined object
/// later replaces two cold object-store GETs with one GET.
///
/// # Examples
///
/// A 12 KiB centroid blob and 200 KiB sketch become one bootstrap object. Their
/// internal bytes are unchanged, so existing decoders can consume each slice.
///
/// # Rust Notes for Java/C Engineers
///
/// The parameters are temporary borrows; [`Bytes::from`] takes ownership of the
/// completed `Vec<u8>` without copying it again. The result can outlive both
/// input slices because their bytes were copied into the owned buffer.
pub(crate) fn serialize_bootstrap(
    centroids: &[u8],
    sketch: &[u8],
    bitmap_complete_fields: &BTreeSet<String>,
    filter_summary: &[u8],
) -> Result<Bytes> {
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
    if filter_summary.is_empty() {
        return Err(ZeppelinError::Index(
            "bootstrap filter-summary section cannot be empty".into(),
        ));
    }

    let bitmap_complete_fields = serialize_bitmap_complete_fields(bitmap_complete_fields)?;
    let centroids_offset = BOOTSTRAP_HEADER_LEN;
    let sketch_offset = centroids_offset
        .checked_add(centroids.len())
        .ok_or_else(|| ZeppelinError::Index("bootstrap centroids section overflows".into()))?;
    let bitmap_complete_fields_offset = sketch_offset
        .checked_add(sketch.len())
        .ok_or_else(|| ZeppelinError::Index("bootstrap sketch section overflows".into()))?;
    let filter_summary_offset = bitmap_complete_fields_offset
        .checked_add(bitmap_complete_fields.len())
        .ok_or_else(|| {
            ZeppelinError::Index("bootstrap bitmap complete-fields section overflows".into())
        })?;
    let total = filter_summary_offset
        .checked_add(filter_summary.len())
        .ok_or_else(|| ZeppelinError::Index("bootstrap filter-summary section overflows".into()))?;

    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(BOOTSTRAP_MAGIC);
    buf.extend_from_slice(&BOOTSTRAP_VERSION.to_le_bytes());
    buf.extend_from_slice(&(centroids_offset as u64).to_le_bytes());
    buf.extend_from_slice(&(centroids.len() as u64).to_le_bytes());
    buf.extend_from_slice(&(sketch_offset as u64).to_le_bytes());
    buf.extend_from_slice(&(sketch.len() as u64).to_le_bytes());
    buf.extend_from_slice(&(bitmap_complete_fields_offset as u64).to_le_bytes());
    buf.extend_from_slice(&(bitmap_complete_fields.len() as u64).to_le_bytes());
    buf.extend_from_slice(&(filter_summary_offset as u64).to_le_bytes());
    buf.extend_from_slice(&(filter_summary.len() as u64).to_le_bytes());
    buf.extend_from_slice(centroids);
    buf.extend_from_slice(sketch);
    buf.extend_from_slice(&bitmap_complete_fields);
    buf.extend_from_slice(filter_summary);
    debug_assert_eq!(buf.len(), total);

    Ok(Bytes::from(buf))
}

/// Builds a bootstrap object's future manifest reference and complete bytes.
///
/// # Parameters
///
/// - `namespace`: Namespace prefix used to derive the immutable key.
/// - `segment_id`: New segment identifier that will own the object.
/// - `centroids`: Complete encoded centroid artifact.
/// - `sketch`: Complete encoded resident-sketch artifact.
///
/// # Returns
///
/// A [`BootstrapRef`] carrying the key and exact byte length, paired with the
/// bytes the caller must PUT at that key.
///
/// # Errors
///
/// Propagates bootstrap serialization errors. It does not perform I/O and
/// therefore cannot leave a partial remote artifact.
///
/// # Consistency
///
/// Constructing a reference does not publish it. Readers use the object only
/// after its reference appears in the authoritative manifest.
///
/// # Examples
///
/// Building segment `seg-7` returns a ref to `.../seg-7/bootstrap.bin`; after a
/// successful PUT the object still remains invisible until manifest CAS.
pub(crate) fn build_bootstrap_artifact(
    namespace: &str,
    segment_id: &str,
    centroids: &[u8],
    sketch: &[u8],
    bitmap_complete_fields: &BTreeSet<String>,
    filter_summary: &[u8],
) -> Result<(BootstrapRef, Bytes)> {
    let bytes = serialize_bootstrap(centroids, sketch, bitmap_complete_fields, filter_summary)?;
    let bootstrap_ref = BootstrapRef {
        key: bootstrap_key(namespace, segment_id),
        size_bytes: bytes.len() as u64,
    };
    Ok((bootstrap_ref, bytes))
}

/// Validates a bootstrap object and borrows its embedded artifact sections.
///
/// Validation requires the current magic/version, exact contiguous section
/// ordering, non-empty payloads, in-bounds checked ranges, and no trailing
/// bytes. It does not decode the nested formats themselves.
///
/// # Parameters
///
/// - `data`: Complete bootstrap-object bytes loaded from storage or cache.
///
/// # Returns
///
/// Borrowed artifact slices and decoded complete fields tied to `data`'s
/// lifetime.
///
/// # Errors
///
/// Returns an index error for a short header, wrong magic, unsupported version,
/// malformed integer, empty/overlapping/out-of-bounds section, overflow, or an
/// exact-size mismatch. No partially validated sections are returned.
///
/// # Examples
///
/// Bytes emitted by `serialize_bootstrap` yield the exact original payloads.
/// Changing the sketch length to extend past the object returns an error before
/// any decoder sees the slice.
///
/// # Rust Notes for Java/C Engineers
///
/// `BootstrapSections<'_>` is a zero-copy view. It resembles Java
/// `ByteBuffer.slice()` or C pointer/length pairs, but the borrow checker proves
/// the views cannot survive after `data` is released.
pub(crate) fn deserialize_bootstrap(data: &[u8]) -> Result<BootstrapSections<'_>> {
    if data.len() < 8 {
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
    if version != BOOTSTRAP_VERSION_V1
        && version != BOOTSTRAP_VERSION_V2
        && version != BOOTSTRAP_VERSION
    {
        return Err(ZeppelinError::Index(format!(
            "unsupported bootstrap version: {version}"
        )));
    }
    let header_len = match version {
        BOOTSTRAP_VERSION_V1 => BOOTSTRAP_V1_HEADER_LEN,
        BOOTSTRAP_VERSION_V2 => BOOTSTRAP_V2_HEADER_LEN,
        BOOTSTRAP_VERSION => BOOTSTRAP_HEADER_LEN,
        _ => unreachable!("bootstrap version was validated above"),
    };
    if data.len() < header_len {
        return Err(ZeppelinError::Index(
            "bootstrap blob too small for header".into(),
        ));
    }

    let centroids_offset = read_u64_usize(data, 8, "bootstrap centroids offset")?;
    let centroids_len = read_u64_usize(data, 16, "bootstrap centroids length")?;
    let sketch_offset = read_u64_usize(data, 24, "bootstrap sketch offset")?;
    let sketch_len = read_u64_usize(data, 32, "bootstrap sketch length")?;
    validate_bootstrap_section(
        "centroids",
        centroids_offset,
        centroids_len,
        header_len,
        data.len(),
    )?;
    let centroids_end = centroids_offset.checked_add(centroids_len).ok_or_else(|| {
        ZeppelinError::Index(format!(
            "bootstrap centroids section overflows: offset={centroids_offset}, len={centroids_len}"
        ))
    })?;
    if centroids_offset != header_len {
        return Err(ZeppelinError::Index(format!(
            "bootstrap centroids offset mismatch: expected {header_len}, got {centroids_offset}"
        )));
    }
    if sketch_offset != centroids_end {
        return Err(ZeppelinError::Index(format!(
            "bootstrap sketch offset mismatch: expected {centroids_end}, got {sketch_offset}"
        )));
    }
    validate_bootstrap_section("sketch", sketch_offset, sketch_len, header_len, data.len())?;
    let sketch_end = sketch_offset.checked_add(sketch_len).ok_or_else(|| {
        ZeppelinError::Index(format!(
            "bootstrap sketch section overflows: offset={sketch_offset}, len={sketch_len}"
        ))
    })?;
    let (bitmap_complete_fields, fields_end) = if version == BOOTSTRAP_VERSION_V1 {
        (BTreeSet::new(), sketch_end)
    } else {
        let fields_offset = read_u64_usize(data, 40, "bootstrap bitmap complete-fields offset")?;
        let fields_len = read_u64_usize(data, 48, "bootstrap bitmap complete-fields length")?;
        if fields_offset != sketch_end {
            return Err(ZeppelinError::Index(format!(
                "bootstrap bitmap complete-fields offset mismatch: expected {sketch_end}, got {fields_offset}"
            )));
        }
        validate_bootstrap_section(
            "bitmap complete-fields",
            fields_offset,
            fields_len,
            header_len,
            data.len(),
        )?;
        let fields_end = fields_offset.checked_add(fields_len).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "bootstrap bitmap complete-fields section overflows: offset={fields_offset}, len={fields_len}"
            ))
        })?;
        (
            deserialize_bitmap_complete_fields(&data[fields_offset..fields_end])?,
            fields_end,
        )
    };
    let (filter_summary, expected_end) = if version == BOOTSTRAP_VERSION {
        let summary_offset = read_u64_usize(data, 56, "bootstrap filter-summary offset")?;
        let summary_len = read_u64_usize(data, 64, "bootstrap filter-summary length")?;
        if summary_offset != fields_end {
            return Err(ZeppelinError::Index(format!(
                "bootstrap filter-summary offset mismatch: expected {fields_end}, got {summary_offset}"
            )));
        }
        validate_bootstrap_section(
            "filter-summary",
            summary_offset,
            summary_len,
            header_len,
            data.len(),
        )?;
        let summary_end = summary_offset.checked_add(summary_len).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "bootstrap filter-summary section overflows: offset={summary_offset}, len={summary_len}"
            ))
        })?;
        (Some(&data[summary_offset..summary_end]), summary_end)
    } else {
        (None, fields_end)
    };
    if expected_end != data.len() {
        return Err(ZeppelinError::Index(format!(
            "bootstrap blob size mismatch: expected {expected_end}, got {}",
            data.len()
        )));
    }

    Ok(BootstrapSections {
        centroids: &data[centroids_offset..centroids_end],
        sketch: &data[sketch_offset..sketch_end],
        sketch_range: sketch_offset..sketch_end,
        bitmap_complete_fields,
        filter_summary,
    })
}

fn serialize_bitmap_complete_fields(fields: &BTreeSet<String>) -> Result<Vec<u8>> {
    let field_count = u32::try_from(fields.len()).map_err(|_| {
        ZeppelinError::Index("bootstrap bitmap complete-field count exceeds u32".into())
    })?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&field_count.to_le_bytes());
    for field in fields {
        let field_len = u32::try_from(field.len()).map_err(|_| {
            ZeppelinError::Index(format!(
                "bootstrap bitmap complete-field length exceeds u32: {field}"
            ))
        })?;
        bytes.extend_from_slice(&field_len.to_le_bytes());
        bytes.extend_from_slice(field.as_bytes());
    }
    Ok(bytes)
}

fn deserialize_bitmap_complete_fields(data: &[u8]) -> Result<BTreeSet<String>> {
    if data.len() < 4 {
        return Err(ZeppelinError::Index(
            "bootstrap bitmap complete-fields section is truncated".into(),
        ));
    }
    let field_count = read_u32_usize(data, 0, "bootstrap bitmap complete-field count")?;
    let mut offset = 4usize;
    let mut fields = BTreeSet::new();
    let mut previous: Option<String> = None;
    for _ in 0..field_count {
        let field_len = read_u32_usize(data, offset, "bootstrap bitmap complete-field length")?;
        offset = offset.checked_add(4).ok_or_else(|| {
            ZeppelinError::Index("bootstrap bitmap complete-field offset overflows".into())
        })?;
        let end = offset.checked_add(field_len).ok_or_else(|| {
            ZeppelinError::Index("bootstrap bitmap complete-field bytes overflow".into())
        })?;
        let field_bytes = data.get(offset..end).ok_or_else(|| {
            ZeppelinError::Index("bootstrap bitmap complete-field is truncated".into())
        })?;
        let field = std::str::from_utf8(field_bytes)
            .map_err(|_| {
                ZeppelinError::Index("bootstrap bitmap complete-field is not UTF-8".into())
            })?
            .to_string();
        if field.is_empty() {
            return Err(ZeppelinError::Index(
                "bootstrap bitmap complete-field cannot be empty".into(),
            ));
        }
        if previous.as_ref().is_some_and(|prior| prior >= &field) {
            return Err(ZeppelinError::Index(
                "bootstrap bitmap complete-fields are not sorted and unique".into(),
            ));
        }
        previous = Some(field.clone());
        fields.insert(field);
        offset = end;
    }
    if offset != data.len() {
        return Err(ZeppelinError::Index(format!(
            "bootstrap bitmap complete-fields size mismatch: decoded={offset}, section={}",
            data.len()
        )));
    }
    Ok(fields)
}

/// Checks one bootstrap directory entry against header and object bounds.
///
/// # Parameters
///
/// - `label`: Human-readable section name included in failures.
/// - `offset`: Absolute start byte declared by the artifact.
/// - `len`: Declared section length in bytes.
/// - `min_offset`: First byte after the fixed header.
/// - `data_len`: Complete object length.
///
/// # Returns
///
/// `Ok(())` only for a non-empty range wholly outside the header and within the
/// object.
///
/// # Errors
///
/// Returns an index error for an empty section, a start inside the header,
/// arithmetic overflow, or an end beyond the object.
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

/// Serializes centroids without an embedded scalar-quantization calibration.
///
/// The current versioned layout is:
/// `[b"ZCT2"][num_centroids: u32][dimension: u32]`
/// `[f32 * num_centroids * dimension][sq_calibration_len:u64][sq_calibration bytes]`
/// with a zero calibration length.
///
/// # Parameters
///
/// - `centroids`: Cluster representatives in logical cluster order.
/// - `dim`: Persisted dimension for each centroid row.
///
/// # Returns
///
/// Complete current-format centroid bytes.
///
/// # Errors
///
/// Propagates serialization failures from the general calibrated form. The
/// current implementation does not validate row lengths or integer narrowing.
///
/// # Panics
///
/// In debug builds, panics when the supplied shapes make the written length
/// differ from `centroids.len() * dim` floats.
///
/// # Examples
///
/// Two three-dimensional centroids produce a header followed by six little-
/// endian floats and a zero `u64` calibration length.
pub(crate) fn serialize_centroids(centroids: &[Vec<f32>], dim: usize) -> Result<Bytes> {
    serialize_centroids_with_sq_calibration(centroids, dim, None)
}

/// Serializes centroids with an optional embedded SQ calibration payload.
///
/// # Parameters
///
/// - `centroids`: Cluster representatives in logical cluster order.
/// - `dim`: Number of floats expected in each row and persisted in the header.
/// - `sq_calibration`: Already encoded scalar-quantization calibration bytes,
///   or `None` to persist a zero-length section.
///
/// # Returns
///
/// One `ZCT2` buffer containing centroid floats followed by the optional
/// calibration bytes.
///
/// # Errors
///
/// The signature reserves `Result` for format validation; current code emits a
/// buffer directly. No I/O occurs.
///
/// # Panics
///
/// Integer multiplication/addition may panic in overflow-checking builds for
/// impossible in-memory sizes. Debug builds also assert exact output length.
/// Callers must supply rows whose lengths equal `dim`.
///
/// # Performance
///
/// Allocates and writes `O(cluster_count * dim + calibration_bytes)` bytes.
/// Embedding calibration lets the normal metadata load avoid a separate GET.
///
/// # Examples
///
/// An SQ segment stores its calibration after the centroids. A non-SQ or PQ
/// segment passes `None`; the same decoder then reports no embedded calibration.
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

/// Owned logical contents of either a legacy or current centroid artifact.
#[derive(Debug)]
pub(crate) struct CentroidsData {
    /// IVF centroids in logical cluster order.
    pub centroids: Vec<Vec<f32>>,
    /// Persisted vector dimensionality shared by centroid rows.
    pub dim: usize,
    /// Embedded SQ calibration payload, present for current SQ segments.
    pub sq_calibration: Option<Bytes>,
}

/// Metadata needed to construct an in-memory index handle after loading.
///
/// This private transfer type unifies bootstrap and legacy-object load paths.
#[derive(Debug)]
struct LoadedIndexMetadata {
    /// Shared, owned centroids in logical cluster order.
    centroids: Arc<Vec<Vec<f32>>>,
    /// Vector dimension decoded from the centroid artifact.
    dim: usize,
    /// Decoded SQ calibration when the centroid artifact embeds one.
    sq_calibration: Option<crate::index::quantization::sq::SqCalibration>,
    /// Decoded resident sketch, absent for segments without a sketch ref.
    resident_sketch: Option<Arc<ResidentSketch>>,
    /// Fields with bitmap coverage in every logical cluster.
    bitmap_complete_fields: BTreeSet<String>,
    /// Decoded exact filter cardinalities, absent for pre-v3 bootstrap objects.
    filter_summary: Option<Arc<super::filter_summary::FilterCardinalitySummary>>,
}

/// Cacheable decoded contents of one immutable bootstrap object.
///
/// Stored sizes are rechecked against each manifest reference before reuse so
/// key reuse or inconsistent metadata fails loudly.
#[derive(Debug)]
struct DecodedBootstrap {
    /// Complete bootstrap-object size validated during the initial decode.
    bootstrap_size_bytes: u64,
    /// Embedded sketch-section size validated during the initial decode.
    sketch_size_bytes: u64,
    /// Shared decoded centroid rows.
    centroids: Arc<Vec<Vec<f32>>>,
    /// Vector dimension decoded from the centroid section.
    dim: usize,
    /// Decoded embedded SQ calibration, if present.
    sq_calibration: Option<crate::index::quantization::sq::SqCalibration>,
    /// Shared decoded resident sketch.
    resident_sketch: Arc<ResidentSketch>,
    /// Fields with bitmap coverage in every logical cluster.
    bitmap_complete_fields: BTreeSet<String>,
    /// Decoded exact filter cardinalities from the v3 bootstrap section.
    filter_summary: Option<Arc<super::filter_summary::FilterCardinalitySummary>>,
}

/// Decodes centroid rows while discarding optional embedded calibration bytes.
///
/// # Parameters
///
/// - `data`: Complete legacy or current centroid-object bytes.
///
/// # Returns
///
/// Owned centroid rows and their persisted dimension.
///
/// # Errors
///
/// Propagates all format, bounds, size, and overflow errors from
/// `deserialize_centroids_data`.
///
/// # Examples
///
/// A `ZCT2` SQ artifact still returns only its centroids and dimension here;
/// callers needing calibration use `deserialize_centroids_data` instead.
pub(crate) fn deserialize_centroids(data: &[u8]) -> Result<(Vec<Vec<f32>>, usize)> {
    let decoded = deserialize_centroids_data(data)?;
    Ok((decoded.centroids, decoded.dim))
}

/// Decodes centroids while auto-detecting legacy and current object formats.
///
/// # Parameters
///
/// - `data`: Complete centroid object loaded from the manifest-selected key.
///
/// # Returns
///
/// Owned centroid rows, dimension, and optional embedded SQ calibration.
/// Legacy bytes return `None` for calibration.
///
/// # Errors
///
/// Returns an index error for malformed/truncated headers, size arithmetic
/// overflow, incomplete floats or calibration, or extra current-format bytes.
///
/// # Examples
///
/// Bytes beginning `ZCT2` use the current decoder. Any other prefix is treated
/// as the historical count-and-dimension header and must satisfy that layout.
pub(crate) fn deserialize_centroids_data(data: &[u8]) -> Result<CentroidsData> {
    if data.starts_with(CENTROIDS_V2_MAGIC) {
        return deserialize_centroids_v2(data);
    }
    deserialize_centroids_legacy(data)
}

/// Decodes the historical unversioned centroid layout.
///
/// # Parameters
///
/// - `data`: Bytes beginning with `num_centroids:u32, dim:u32` followed by
///   little-endian floats.
///
/// # Returns
///
/// Owned centroids with no embedded SQ calibration.
///
/// # Errors
///
/// Returns an index error for a short/malformed header, insufficient float
/// bytes, or a malformed float slice conversion.
///
/// # Compatibility
///
/// Extra trailing bytes are currently ignored by this legacy decoder; current
/// `ZCT2` objects require an exact length.
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

/// Decodes and exactly validates the current `ZCT2` centroid layout.
///
/// # Parameters
///
/// - `data`: Complete bytes beginning with [`CENTROIDS_V2_MAGIC`].
///
/// # Returns
///
/// Owned centroid rows, dimension, and copied calibration bytes when the
/// declared calibration length is non-zero.
///
/// # Errors
///
/// Returns an index error for a short header, malformed integers/floats,
/// checked-size overflow, truncated data, or any trailing bytes.
///
/// # Rust Notes for Java/C Engineers
///
/// `checked_mul`/`checked_add` turn attacker- or corruption-controlled sizes
/// into `Option`; `ok_or_else` converts absence into a domain error. C pointer
/// arithmetic and Java primitive arithmetic would need equivalent explicit
/// overflow checks to avoid accepting a wrapped length.
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

/// Serializes one legacy-format cluster's IDs and full-precision vectors.
///
/// The row-aligned layout is:
/// `[num_vectors: u32][dimension: u32]`
/// then for each vector: `[id_len: u32][id_bytes...][f32 * dim]`
///
/// # Parameters
///
/// - `ids`: Vector IDs in cluster row order.
/// - `vectors`: Full-precision vectors in the same row order.
/// - `dim`: Persisted component count per vector.
///
/// # Returns
///
/// Complete legacy cluster-section bytes. The same section may stand alone or
/// be placed inside a grouped object.
///
/// # Errors
///
/// The signature reserves `Result` for format evolution; current code emits a
/// buffer directly and performs no I/O.
///
/// # Panics
///
/// Allocation or integer arithmetic can panic for impossible in-memory sizes.
/// The function also assumes `ids.len() == vectors.len()` and every vector has
/// `dim` values; `zip` would otherwise serialize only the shorter row set.
///
/// # Examples
///
/// IDs `[a, b]` and two two-dimensional vectors become two row records whose
/// ID and vector remain adjacent and in the caller's order.
pub(crate) fn serialize_cluster(ids: &[String], vectors: &[Vec<f32>], dim: usize) -> Result<Bytes> {
    // A silent `as u32` truncation here would write a header that disagrees
    // with the payload, and the decoder would then drop rows without any
    // error. Fail loud instead; the Result channel already exists.
    let n = u32::try_from(ids.len())
        .map_err(|_| ZeppelinError::Index("cluster row count exceeds u32".into()))?;
    let dimension = u32::try_from(dim)
        .map_err(|_| ZeppelinError::Index("cluster dimension exceeds u32".into()))?;

    let mut buf = Vec::new();
    buf.extend_from_slice(&n.to_le_bytes());
    buf.extend_from_slice(&dimension.to_le_bytes());

    for (id, vec) in ids.iter().zip(vectors.iter()) {
        let id_bytes = id.as_bytes();
        let id_len = u32::try_from(id_bytes.len())
            .map_err(|_| ZeppelinError::Index("vector id length exceeds u32".into()))?;
        buf.extend_from_slice(&id_len.to_le_bytes());
        buf.extend_from_slice(id_bytes);
        for &val in vec {
            buf.extend_from_slice(&val.to_le_bytes());
        }
    }

    Ok(Bytes::from(buf))
}

/// Serializes one cluster section containing SQ codes and exact vectors.
///
/// # Parameters
///
/// - `ids`: Row IDs shared by both representations.
/// - `vectors`: Full-precision rerank vectors in row order.
/// - `sq_codes`: One scalar-quantized code row per vector in the same order.
/// - `dim`: Vector and code dimension persisted by both child formats.
///
/// # Returns
///
/// A `ZCL2` section with absolute offsets to the SQ and full-vector payloads.
///
/// # Errors
///
/// Propagates child-format serialization errors. No remote object is written.
///
/// # Panics
///
/// Assumes row counts and dimensions agree; malformed internal inputs may be
/// truncated by child `zip` operations or panic during size arithmetic.
///
/// # Performance
///
/// Allocates one combined buffer after separately encoding the SQ and full
/// payloads. Co-location allows coarse scoring and exact rerank to share one
/// cached object, while offsets permit future range reads.
///
/// # Examples
///
/// A two-row SQ cluster stores compact codes first and exact floats second. The
/// query path can decode only codes, then later slice exact rows for reranking.
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

/// Serializes one `ZCL3` section containing RQ codes and exact vectors.
///
/// The coarse and full offsets use the same field order and widths as `ZCL2`.
/// This function only builds immutable bytes; compaction does not select or
/// write this format until the later configuration slice.
pub(crate) fn serialize_colocated_rq_cluster(
    vectors: &[Vec<f32>],
    rq_codes: &crate::index::quantization::rq::RqClusterCodes,
    dim: usize,
) -> Result<Bytes> {
    if rq_codes.dim() != dim {
        return Err(ZeppelinError::Index(format!(
            "RQ cluster dimension mismatch: expected {dim}, got {}",
            rq_codes.dim()
        )));
    }
    if rq_codes.row_count() != vectors.len() {
        return Err(ZeppelinError::Index(format!(
            "RQ cluster row count mismatch: {} codes, {} vectors",
            rq_codes.row_count(),
            vectors.len()
        )));
    }

    let coarse_data = rq_codes.to_bytes();
    let full_data = serialize_cluster(rq_codes.ids(), vectors, dim)?;
    let coarse_offset = CLUSTER_V2_HEADER_LEN as u64;
    let coarse_len = coarse_data.len() as u64;
    let full_offset = coarse_offset + coarse_len;
    let full_len = full_data.len() as u64;

    let total = CLUSTER_V2_HEADER_LEN + coarse_data.len() + full_data.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(CLUSTER_V3_MAGIC);
    buf.extend_from_slice(&coarse_offset.to_le_bytes());
    buf.extend_from_slice(&coarse_len.to_le_bytes());
    buf.extend_from_slice(&full_offset.to_le_bytes());
    buf.extend_from_slice(&full_len.to_le_bytes());
    buf.extend_from_slice(&coarse_data);
    buf.extend_from_slice(&full_data);
    debug_assert_eq!(buf.len(), total);

    Ok(Bytes::from(buf))
}

/// Serialize one immutable object containing one or more cluster payloads.
///
/// Each payload is a complete cluster section: either the legacy full-vector
/// cluster format or the v2 SQ+full co-located format.
///
/// # Parameters
///
/// - `entries`: `(logical cluster index, complete cluster section)` pairs. The
///   order becomes directory and payload order; indexes must be unique and fit
///   in `u32`.
///
/// # Returns
///
/// A v4 grouped object when every section is `ZCL2` or every section is
/// `ZCL3`; otherwise a v1 object for homogeneous legacy sections.
///
/// # Errors
///
/// Returns an index error for no entries, a duplicate/oversized cluster index,
/// mixed quantized section magics, malformed quantized sections, or any checked
/// size/offset overflow.
///
/// # Examples
///
/// Entries for clusters 2 and 5 produce one directory naming both. A reader can
/// locate cluster 5 without interpreting cluster 2's payload.
///
/// # Rust Notes for Java/C Engineers
///
/// [`Bytes`] clones share immutable buffers. Passing entries by borrowed slice
/// lets serialization inspect them without taking ownership; the returned
/// buffer owns its complete encoded copy.
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

    let all_zcl2 = entries
        .iter()
        .all(|(_, bytes)| bytes.starts_with(CLUSTER_V2_MAGIC));
    let all_zcl3 = entries
        .iter()
        .all(|(_, bytes)| bytes.starts_with(CLUSTER_V3_MAGIC));
    if all_zcl2 || all_zcl3 {
        return serialize_cluster_data_object_v4(entries);
    }
    if entries.iter().any(|(_, bytes)| {
        bytes.starts_with(CLUSTER_V2_MAGIC) || bytes.starts_with(CLUSTER_V3_MAGIC)
    }) {
        return Err(ZeppelinError::Index(
            "cluster data object sections must use one homogeneous encoding".into(),
        ));
    }

    serialize_cluster_data_object_v1(entries)
}

/// Writes the v1 grouped-object directory followed by contiguous sections.
///
/// # Parameters
///
/// - `entries`: Prevalidated unique cluster indexes and complete sections.
///
/// # Returns
///
/// One `ZBP1` object whose directory records an absolute offset and length for
/// each section.
///
/// # Errors
///
/// Returns an index error for directory, payload, section, or total-size
/// overflow.
fn serialize_cluster_data_object_v1(entries: &[(usize, Bytes)]) -> Result<Bytes> {
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
    buf.extend_from_slice(CLUSTER_DATA_OBJECT_V1_MAGIC);
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

/// Writes a v4 grouped object with all SQ ranges before all full-vector ranges.
///
/// Separating blocks lets a coarse query range-read compact SQ data without
/// pulling the usually larger exact-vector block. Homogeneous `ZCL2` or `ZCL3`
/// sections are parsed and copied into the corresponding blocks.
///
/// # Parameters
///
/// - `entries`: Prevalidated cluster indexes paired with homogeneous quantized
///   sections.
///
/// # Returns
///
/// One `ZBP4` buffer with per-cluster SQ and full absolute ranges.
///
/// # Errors
///
/// Returns an index error for malformed input sections or checked directory,
/// block, range, and total-size overflow.
///
/// # Examples
///
/// Two scalar-quantized clusters become `directory | SQ0 | SQ1 | full0 |
/// full1`; the largest SQ end is therefore no later than the first full start.
fn serialize_cluster_data_object_v4(entries: &[(usize, Bytes)]) -> Result<Bytes> {
    /// Borrowed child payloads extracted from one quantized section.
    struct SplitSection<'a> {
        /// Logical cluster named in the grouped directory.
        cluster_idx: usize,
        /// Compact SQ8 or two-bit child artifact.
        sq: &'a [u8],
        /// Exact full-vector child artifact.
        full: &'a [u8],
    }

    let sections: Vec<SplitSection<'_>> = entries
        .iter()
        .map(|(cluster_idx, bytes)| {
            let sections = colocated_cluster_sections(bytes)?;
            Ok(SplitSection {
                cluster_idx: *cluster_idx,
                sq: sections.coarse,
                full: sections.full,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let directory_len = entries
        .len()
        .checked_mul(CLUSTER_DATA_OBJECT_V4_DIR_ENTRY_LEN)
        .ok_or_else(|| ZeppelinError::Index("v4 cluster object directory overflows".into()))?;
    let payload_offset = CLUSTER_DATA_OBJECT_HEADER_LEN
        .checked_add(directory_len)
        .ok_or_else(|| ZeppelinError::Index("v4 cluster object header overflows".into()))?;
    let sq_total: usize =
        sections
            .iter()
            .map(|section| section.sq.len())
            .try_fold(0usize, |acc, len| {
                acc.checked_add(len).ok_or_else(|| {
                    ZeppelinError::Index("v4 cluster object SQ block overflows".into())
                })
            })?;
    let full_total: usize =
        sections
            .iter()
            .map(|section| section.full.len())
            .try_fold(0usize, |acc, len| {
                acc.checked_add(len).ok_or_else(|| {
                    ZeppelinError::Index("v4 cluster object full block overflows".into())
                })
            })?;
    let full_block_offset = payload_offset
        .checked_add(sq_total)
        .ok_or_else(|| ZeppelinError::Index("v4 cluster object SQ block end overflows".into()))?;
    let total = full_block_offset
        .checked_add(full_total)
        .ok_or_else(|| ZeppelinError::Index("v4 cluster object size overflows".into()))?;

    let mut sq_offsets = Vec::with_capacity(sections.len());
    let mut offset = payload_offset;
    for section in &sections {
        sq_offsets.push(offset);
        offset = offset
            .checked_add(section.sq.len())
            .ok_or_else(|| ZeppelinError::Index("v4 cluster object SQ section overflows".into()))?;
    }
    debug_assert_eq!(offset, full_block_offset);

    let mut full_offsets = Vec::with_capacity(sections.len());
    offset = full_block_offset;
    for section in &sections {
        full_offsets.push(offset);
        offset = offset.checked_add(section.full.len()).ok_or_else(|| {
            ZeppelinError::Index("v4 cluster object full section overflows".into())
        })?;
    }
    debug_assert_eq!(offset, total);

    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(CLUSTER_DATA_OBJECT_MAGIC_PREFIX);
    buf.push(CLUSTER_DATA_OBJECT_V4_VERSION);
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());

    for (idx, section) in sections.iter().enumerate() {
        buf.extend_from_slice(&(section.cluster_idx as u32).to_le_bytes());
        buf.extend_from_slice(&(sq_offsets[idx] as u64).to_le_bytes());
        buf.extend_from_slice(&(section.sq.len() as u64).to_le_bytes());
        buf.extend_from_slice(&(full_offsets[idx] as u64).to_le_bytes());
        buf.extend_from_slice(&(section.full.len() as u64).to_le_bytes());
    }

    for section in &sections {
        buf.extend_from_slice(section.sq);
    }
    for section in &sections {
        buf.extend_from_slice(section.full);
    }
    debug_assert_eq!(buf.len(), total);

    Ok(Bytes::from(buf))
}

/// Owned IDs and exact vectors decoded for one logical IVF cluster.
///
/// Both vectors retain artifact row order; `ids[i]` identifies `vectors[i]`.
#[derive(Debug)]
pub(crate) struct ClusterData {
    /// Vector IDs in stored cluster row order.
    pub ids: Vec<String>,
    /// Full-precision vectors aligned one-to-one with [`Self::ids`].
    pub vectors: Vec<Vec<f32>>,
}

/// Decodes exact vectors from a legacy, `ZCL2`, or `ZCL3` cluster section.
///
/// # Parameters
///
/// - `data`: One complete cluster section, not an entire grouped object.
///
/// # Returns
///
/// Owned IDs and full-precision vectors. Coarse bytes in a quantized section
/// are ignored by this exact-data path.
///
/// # Errors
///
/// Returns an index error for malformed co-located offsets, a short legacy
/// header, truncated IDs/vectors, or malformed integer fields.
///
/// # Examples
///
/// Passing a scalar-quantized co-located section returns the same IDs and exact
/// vectors as its full child payload, not the compact codes.
pub(crate) fn deserialize_cluster(data: &[u8]) -> Result<ClusterData> {
    let data = full_cluster_section(data)?;
    deserialize_legacy_cluster(data)
}

/// Deserialize one cluster section from either a legacy per-cluster object or
/// a grouped cluster-data object.
///
/// # Parameters
///
/// - `data`: Complete standalone or grouped object bytes.
/// - `cluster_idx`: Logical cluster requested by the manifest layout.
///
/// # Returns
///
/// Owned exact cluster data. Standalone legacy bytes are decoded directly;
/// grouped bytes select the matching directory range first.
///
/// # Errors
///
/// Returns an index error for malformed grouped metadata, a missing requested
/// cluster, an out-of-bounds range, or malformed cluster payload.
///
/// # Examples
///
/// A grouped object containing clusters 2 and 5 returns only cluster 5 when
/// called with index 5. Requesting cluster 3 fails loudly.
pub(crate) fn deserialize_cluster_from_object(
    data: &[u8],
    cluster_idx: usize,
) -> Result<ClusterData> {
    let Some(sections) = cluster_object_sections(data)? else {
        return deserialize_cluster(data);
    };
    sections
        .iter()
        .find(|section| section.cluster_idx == cluster_idx)
        .ok_or_else(|| {
            ZeppelinError::Index(format!(
                "cluster {cluster_idx} missing from cluster data object"
            ))
        })?
        .decode()
}

/// Decodes the count/dimension legacy full-vector cluster representation.
///
/// # Parameters
///
/// - `data`: Complete child section beginning with row count and dimension.
///
/// # Returns
///
/// Owned row IDs and full-precision vector buffers in serialized order.
///
/// # Errors
///
/// Returns an index error for a short header, truncated ID length/bytes or
/// vector bytes, and malformed numeric fields.
///
/// # Compatibility
///
/// Invalid UTF-8 IDs are decoded lossily with replacement characters, and
/// trailing bytes after the declared rows are currently ignored.
///
/// # Rust Notes for Java/C Engineers
///
/// `chunks_exact(4)` creates checked four-byte borrowed views and the iterator
/// compiles to a tight loop. Unlike manual C pointer stepping, slice bounds are
/// checked before iteration; the collected `Vec<f32>` owns its decoded values.
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

    // Cap the reservation by what the payload could possibly hold: each row
    // carries at least a 4-byte id_len prefix, so a valid n never exceeds
    // data.len() / 4. A hostile or corrupt header otherwise requests gigabytes
    // before any per-row validation runs.
    let cap = n.min(data.len() / 4);
    let mut ids = Vec::with_capacity(cap);
    let mut vectors = Vec::with_capacity(cap);
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

/// Decoded coarse rows selected by manifest metadata.
#[derive(Debug)]
pub(crate) enum CoarseClusterData {
    /// Scalar-quantized IDs and codes from a `ZCL2` section.
    Sq8(crate::index::quantization::sq::SqClusterData),
    /// Two-bit RaBitQ IDs and codes from a `ZCL3` section.
    // The payload is read only under cfg(test); production SQ8 accessors
    // match this variant solely to reject a mis-encoded section.
    #[allow(dead_code)]
    TwoBit(crate::index::quantization::rq::RqClusterCodes),
}

/// Decodes the coarse child selected by manifest metadata.
///
/// A recognized section magic that disagrees with `encoding` is an error. Raw
/// legacy full-vector sections are accepted only for SQ8, whose historical
/// sidecar reader may supply the coarse data separately.
pub(crate) fn deserialize_colocated_coarse_cluster(
    data: &[u8],
    encoding: CoarsePayloadEncoding,
) -> Result<Option<CoarseClusterData>> {
    let section_encoding = cluster_section_encoding(data)?;
    match (encoding, section_encoding) {
        (CoarsePayloadEncoding::Sq8, Some(CoarsePayloadEncoding::Sq8)) => {
            let sections = colocated_cluster_sections(data)?;
            let codes = crate::index::quantization::sq::deserialize_sq_cluster(sections.coarse)?;
            Ok(Some(CoarseClusterData::Sq8(codes)))
        }
        (CoarsePayloadEncoding::TwoBit, Some(CoarsePayloadEncoding::TwoBit)) => {
            let sections = colocated_cluster_sections(data)?;
            let codes =
                crate::index::quantization::rq::RqClusterCodes::from_bytes(sections.coarse)?;
            Ok(Some(CoarseClusterData::TwoBit(codes)))
        }
        (expected, Some(actual)) => Err(ZeppelinError::Index(format!(
            "cluster coarse encoding mismatch: manifest={expected:?}, section={actual:?}"
        ))),
        (CoarsePayloadEncoding::Sq8, None) => Ok(None),
        (CoarsePayloadEncoding::TwoBit, None) => Err(ZeppelinError::Index(
            "two-bit manifest tag requires a ZCL3 cluster section".into(),
        )),
    }
}

/// Decodes a raw coarse range whose encoding came from manifest metadata.
fn decode_coarse_payload(
    data: &[u8],
    encoding: CoarsePayloadEncoding,
) -> Result<CoarseClusterData> {
    match encoding {
        CoarsePayloadEncoding::Sq8 => {
            let codes = crate::index::quantization::sq::deserialize_sq_cluster(data)?;
            Ok(CoarseClusterData::Sq8(codes))
        }
        CoarsePayloadEncoding::TwoBit => {
            let codes = crate::index::quantization::rq::RqClusterCodes::from_bytes(data)?;
            Ok(CoarseClusterData::TwoBit(codes))
        }
    }
}

/// Decodes the SQ child of a co-located cluster section when present.
///
/// # Parameters
///
/// - `data`: One standalone cluster section.
///
/// # Returns
///
/// `Some` decoded SQ IDs/codes for `ZCL2`; `None` for a legacy full-only
/// section.
///
/// # Errors
///
/// Returns an index error for malformed co-located offsets or SQ payload bytes.
///
/// # Examples
///
/// A legacy `cluster_i.bin` yields `None`, telling the caller to consult the
/// historical separate `sq_cluster_i.bin` key when SQ is required.
pub(crate) fn deserialize_colocated_sq_cluster(
    data: &[u8],
) -> Result<Option<crate::index::quantization::sq::SqClusterData>> {
    match deserialize_colocated_coarse_cluster(data, CoarsePayloadEncoding::Sq8)? {
        Some(CoarseClusterData::Sq8(codes)) => Ok(Some(codes)),
        Some(CoarseClusterData::TwoBit(_)) => Err(ZeppelinError::Index(
            "SQ8 decoder received two-bit cluster data".into(),
        )),
        None => Ok(None),
    }
}

/// Deserialize the manifest-selected coarse section from one cluster object.
pub(crate) fn deserialize_colocated_coarse_cluster_from_object(
    data: &[u8],
    cluster_idx: usize,
    encoding: CoarsePayloadEncoding,
) -> Result<Option<CoarseClusterData>> {
    if is_cluster_data_object_v4(data) {
        let layout = cluster_object_layout_v4(data)?;
        let section = layout.section(cluster_idx).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "cluster {cluster_idx} missing from v4 cluster data object"
            ))
        })?;
        let coarse = section.sq.as_ref().ok_or_else(|| {
            ZeppelinError::Index(format!(
                "cluster {cluster_idx} missing coarse section in v4 cluster data object"
            ))
        })?;
        validate_range_in_object(coarse, data.len(), "v4 cluster coarse section")?;
        return decode_coarse_payload(&data[coarse.clone()], encoding).map(Some);
    }

    if is_cluster_data_object_v5(data) {
        // v5 coarse blocks carry no IDs, so they are only meaningful alongside
        // their sibling ID block. Callers must use the manifest row layout.
        return Err(ZeppelinError::Index(
            "v5 cluster data object coarse blocks require manifest row layouts".into(),
        ));
    }

    let Some(layout) = cluster_object_layout(data)? else {
        return deserialize_colocated_coarse_cluster(data, encoding);
    };
    let section = layout.section(cluster_idx).ok_or_else(|| {
        ZeppelinError::Index(format!(
            "cluster {cluster_idx} missing from cluster data object"
        ))
    })?;
    validate_range_in_object(
        &section.full,
        data.len(),
        "cluster data object full section",
    )?;
    deserialize_colocated_coarse_cluster(&data[section.full.clone()], encoding)
}

/// Deserialize the SQ section for one cluster in either a legacy per-cluster
/// object or a grouped cluster-data object.
///
/// # Parameters
///
/// - `data`: Complete standalone or grouped object bytes.
/// - `cluster_idx`: Logical cluster to locate in a grouped object.
///
/// # Returns
///
/// `Some` decoded SQ data for v4 or `ZCL2` input, or `None` when the selected
/// legacy section contains only full vectors.
///
/// # Errors
///
/// Returns an index error for malformed directories/ranges, a missing cluster
/// or SQ range in v4, or invalid SQ bytes.
pub(crate) fn deserialize_colocated_sq_cluster_from_object(
    data: &[u8],
    cluster_idx: usize,
) -> Result<Option<crate::index::quantization::sq::SqClusterData>> {
    match deserialize_colocated_coarse_cluster_from_object(
        data,
        cluster_idx,
        CoarsePayloadEncoding::Sq8,
    )? {
        Some(CoarseClusterData::Sq8(codes)) => Ok(Some(codes)),
        Some(CoarseClusterData::TwoBit(_)) => Err(ZeppelinError::Index(
            "SQ8 decoder received two-bit cluster data".into(),
        )),
        None => Ok(None),
    }
}

/// Borrowed child payloads from one validated quantized cluster section.
struct ColocatedClusterSections<'a> {
    /// Scalar-quantized or two-bit cluster artifact bytes.
    coarse: &'a [u8],
    /// Legacy-format exact-vector cluster artifact bytes.
    full: &'a [u8],
}

/// Borrowed exact-row payload for one entry in a grouped object.
///
/// A `ZBP1`/`ZBP4` entry owns one contiguous legacy row section; a `ZBP5` entry
/// instead owns a hoisted ID block and a fixed-stride f32 block that must be
/// joined by row position. [`Self::decode`] hides that difference from callers
/// that only want the cluster's IDs and exact vectors.
pub(crate) struct ClusterObjectSection<'a> {
    /// Logical IVF cluster represented by this range.
    pub cluster_idx: usize,
    /// Borrowed rows in whichever grouped layout the object uses.
    rows: ClusterObjectRows<'a>,
}

/// The two persisted shapes an exact-row section can take.
enum ClusterObjectRows<'a> {
    /// One `[row_count][dim][(id_len, id, f32[dim])...]` child section.
    Legacy(&'a [u8]),
    /// Separated `ZBP5` ID and fixed-stride vector blocks for one cluster.
    RowLayout {
        /// Rows shared by both blocks, from the object directory.
        row_count: usize,
        /// Deterministic ID block bytes.
        ids: &'a [u8],
        /// Exactly `row_count × dim × 4` vector bytes.
        vectors: &'a [u8],
    },
}

impl ClusterObjectSection<'_> {
    /// Decodes this section's IDs and exact vectors in persisted row order.
    ///
    /// # Errors
    ///
    /// Returns an index error for a malformed legacy section, a malformed
    /// `ZBP5` ID block, a vector block whose length is not a whole number of
    /// fixed-stride rows, a zero-width row, or an ID/vector row-count
    /// disagreement.
    ///
    /// A `ZBP5` object stores no dimension, so this recovers it by dividing the
    /// vector block by the row count. That division is exact for anything
    /// [`serialize_cluster_data_object_v5`] wrote, because it validates the
    /// stride before the object becomes immutable; a remainder or a zero width
    /// here is corruption, not a differently sized segment.
    pub(crate) fn decode(&self) -> Result<ClusterData> {
        match &self.rows {
            ClusterObjectRows::Legacy(data) => deserialize_cluster(data),
            ClusterObjectRows::RowLayout {
                row_count,
                ids,
                vectors,
            } => {
                let ids = deserialize_id_block(ids)?;
                if ids.len() != *row_count {
                    return Err(ZeppelinError::Index(format!(
                        "v5 cluster {} declares {row_count} rows but its ID block has {}",
                        self.cluster_idx,
                        ids.len()
                    )));
                }
                let dim = if *row_count == 0 {
                    if !vectors.is_empty() {
                        return Err(ZeppelinError::Index(format!(
                            "v5 cluster {} declares no rows but carries {} vector bytes",
                            self.cluster_idx,
                            vectors.len()
                        )));
                    }
                    0
                } else {
                    let row_bytes = vectors.len() / row_count;
                    if row_bytes == 0
                        || row_bytes % 4 != 0
                        || row_bytes * row_count != vectors.len()
                    {
                        return Err(ZeppelinError::Index(format!(
                            "v5 cluster {} vector block of {} bytes is not {row_count} whole f32 rows",
                            self.cluster_idx,
                            vectors.len()
                        )));
                    }
                    row_bytes / 4
                };
                let vectors = deserialize_fixed_stride_f32_block(vectors, *row_count, dim)?;
                Ok(ClusterData { ids, vectors })
            }
        }
    }
}

/// Absolute byte ranges for one cluster inside a grouped cluster-data object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClusterObjectRange {
    /// Logical IVF cluster named by the directory entry.
    pub cluster_idx: usize,
    /// Compact SQ range for v4 objects; absent for v1 full-only objects.
    pub sq: Option<Range<usize>>,
    /// Exact-vector child range in either grouped format.
    pub full: Range<usize>,
}

/// Validated directory layout for a grouped cluster-data object.
///
/// Ranges are absolute object offsets. Parsing the directory proves structural
/// relationships but full object bounds are checked when a range is consumed,
/// allowing callers to parse a header-only range GET first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClusterObjectLayout {
    /// One unique range descriptor per directory entry, in stored order.
    pub sections: Vec<ClusterObjectRange>,
}

impl ClusterObjectLayout {
    /// Finds the range descriptor for a logical cluster.
    ///
    /// # Parameters
    ///
    /// - `cluster_idx`: Logical cluster index requested by query planning.
    ///
    /// # Returns
    ///
    /// A borrowed descriptor, or `None` when the directory does not name that
    /// cluster. Stored section order does not affect lookup semantics.
    ///
    /// # Examples
    ///
    /// A layout for clusters `[2, 5]` returns the second descriptor for 5 and
    /// `None` for 3.
    pub(crate) fn section(&self, cluster_idx: usize) -> Option<&ClusterObjectRange> {
        self.sections
            .iter()
            .find(|section| section.cluster_idx == cluster_idx)
    }
}

/// Number of leading bytes needed to parse either supported grouped-object
/// directory for `entry_count` manifest entries. The returned range may include
/// a few payload bytes for v1 objects; parsers ignore bytes beyond the actual
/// directory.
///
/// # Parameters
///
/// - `entry_count`: Number of cluster entries declared by manifest metadata.
///
/// # Returns
///
/// A conservative header range length large enough for the wider v4 directory.
/// Callers can range-read `0..len` and pass those bytes to
/// `cluster_object_layout` without fetching payload bodies.
///
/// # Errors
///
/// Returns an index error if directory or header-size arithmetic overflows.
///
/// # Examples
///
/// For two entries, the returned length covers an eight-byte header and two v4
/// directory records. A v1 parser ignores the harmless extra payload prefix.
pub(crate) fn cluster_object_header_range_len(entry_count: usize) -> Result<usize> {
    let directory_len = entry_count
        .checked_mul(CLUSTER_DATA_OBJECT_V4_DIR_ENTRY_LEN)
        .ok_or_else(|| ZeppelinError::Index("cluster object header range overflows".into()))?;
    CLUSTER_DATA_OBJECT_HEADER_LEN
        .checked_add(directory_len)
        .ok_or_else(|| ZeppelinError::Index("cluster object header range overflows".into()))
}

/// Return the directory layout in a grouped cluster-data object. `data` only
/// needs to contain the object header and directory.
///
/// # Parameters
///
/// - `data`: Header-plus-directory bytes, optionally followed by payload data.
///
/// # Returns
///
/// `Some` parsed v1/v4 layout or `None` when the bytes carry no plausible
/// grouped-object signature. `None` identifies a legacy standalone cluster,
/// not a malformed or unsupported recognized grouped object.
///
/// # Errors
///
/// Returns an index error for an unsupported grouped-object version, a
/// recognized but truncated/malformed directory, duplicate cluster indexes,
/// invalid relationships, or size overflow.
///
/// A `ZBP5` object is an error, never `None`. Its coarse/ID/vector regions are
/// not the coarse/full section pair this directory describes, and returning
/// `None` would tell the caller it was holding a legacy standalone cluster.
/// Callers that can read v5 must dispatch on the manifest row layout — or on
/// [`cluster_object_sections`] — before reaching here.
///
/// # Examples
///
/// A `ZBP4` header range can expose SQ and full ranges before the payload is
/// fetched. Raw legacy cluster bytes return `None`.
pub(crate) fn cluster_object_layout(data: &[u8]) -> Result<Option<ClusterObjectLayout>> {
    if data.starts_with(CLUSTER_DATA_OBJECT_V1_MAGIC) {
        return cluster_object_layout_v1(data).map(Some);
    }
    if is_cluster_data_object_v4(data) {
        return cluster_object_layout_v4(data).map(Some);
    }
    if is_cluster_data_object_v5(data) {
        return Err(ZeppelinError::Index(
            "v5 cluster data object has no coarse/full section directory; \
             read its regions from the manifest row layout"
                .into(),
        ));
    }
    if let Some(version) = plausible_cluster_data_object_version(data) {
        return Err(ZeppelinError::Index(format!(
            "unsupported cluster data object version {version}; this binary reads ZBP1, ZBP4, ZBP5, and legacy standalone clusters"
        )));
    }
    Ok(None)
}

/// Returns an unrecognized `ZBP` version while preserving true legacy rows.
///
/// Legacy row counts are little-endian and can begin with the three bytes
/// `ZBP`. A fourth zero byte still denotes an ordinary row count, not a
/// versioned object. Every real or plausible grouped-object version is
/// nonzero; ASCII digits are normalized for a readable diagnostic.
fn plausible_cluster_data_object_version(data: &[u8]) -> Option<u8> {
    if data.len() < 4 || &data[..3] != CLUSTER_DATA_OBJECT_MAGIC_PREFIX || data[3] == 0 {
        return None;
    }
    Some(if data[3].is_ascii_digit() {
        data[3] - b'0'
    } else {
        data[3]
    })
}

/// Return all full-vector sections in a grouped cluster-data object.
///
/// `Ok(None)` means the bytes are a legacy per-cluster object, not a grouped
/// object. Callers that already know they fetched a grouped key should treat
/// `None` as an error at that boundary.
///
/// # Parameters
///
/// - `data`: Complete object bytes. Unlike `cluster_object_layout`, this helper
///   must be able to slice every declared full-vector range.
///
/// # Returns
///
/// Borrowed full-vector sections in directory order, or `None` for a standalone
/// legacy object. Each returned slice remains tied to `data`.
///
/// # Errors
///
/// Returns an index error for malformed directory metadata or any full-vector
/// range that extends outside the supplied complete object.
///
/// # Examples
///
/// A grouped object naming clusters 2 and 5 yields two views. Passing legacy
/// `cluster_2.bin` bytes yields `None`, not a fabricated one-entry directory.
pub(crate) fn cluster_object_sections(
    data: &[u8],
) -> Result<Option<Vec<ClusterObjectSection<'_>>>> {
    if is_cluster_data_object_v5(data) {
        let layouts = parse_cluster_data_object_v5(data)?;
        let mut sections = Vec::with_capacity(layouts.len());
        for layout in layouts {
            let ids = usize_range(&layout.ids, "v5 cluster data object ID block")?;
            let vectors = usize_range(&layout.vectors, "v5 cluster data object vector block")?;
            validate_range_in_object(&ids, data.len(), "v5 cluster data object ID block")?;
            validate_range_in_object(&vectors, data.len(), "v5 cluster data object vector block")?;
            sections.push(ClusterObjectSection {
                cluster_idx: layout.cluster_idx,
                rows: ClusterObjectRows::RowLayout {
                    row_count: layout.row_count,
                    ids: &data[ids],
                    vectors: &data[vectors],
                },
            });
        }
        return Ok(Some(sections));
    }

    let Some(layout) = cluster_object_layout(data)? else {
        return Ok(None);
    };
    let mut sections = Vec::with_capacity(layout.sections.len());
    for section in layout.sections {
        validate_range_in_object(
            &section.full,
            data.len(),
            "cluster data object full section",
        )?;
        sections.push(ClusterObjectSection {
            cluster_idx: section.cluster_idx,
            rows: ClusterObjectRows::Legacy(&data[section.full]),
        });
    }

    Ok(Some(sections))
}

/// Parses the v1 directory into full-vector ranges.
///
/// # Parameters
///
/// - `data`: Bytes beginning with `ZBP1` and containing the full directory.
///
/// # Returns
///
/// Unique logical cluster descriptors whose `full` ranges use absolute object
/// offsets and whose `sq` fields are `None`.
///
/// # Errors
///
/// Returns an index error for zero entries, truncation, duplicate indexes,
/// offsets inside the directory, malformed integers, or arithmetic overflow.
fn cluster_object_layout_v1(data: &[u8]) -> Result<ClusterObjectLayout> {
    let entry_count = cluster_object_entry_count(data)?;
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
        sections.push(ClusterObjectRange {
            cluster_idx,
            sq: None,
            full: offset..end,
        });
    }

    Ok(ClusterObjectLayout { sections })
}

/// Parses the v4 directory into separate SQ and exact-vector ranges.
///
/// # Parameters
///
/// - `data`: Bytes beginning with `ZBP` plus version 4 and containing the full
///   directory.
///
/// # Returns
///
/// Unique logical cluster descriptors with both SQ and full absolute ranges.
///
/// # Errors
///
/// Returns an index error for zero entries, a truncated directory, duplicate
/// indexes, SQ data starting inside the directory, overlapping SQ/full ranges,
/// malformed integers, or arithmetic overflow.
fn cluster_object_layout_v4(data: &[u8]) -> Result<ClusterObjectLayout> {
    let entry_count = cluster_object_entry_count(data)?;
    let directory_len = entry_count
        .checked_mul(CLUSTER_DATA_OBJECT_V4_DIR_ENTRY_LEN)
        .ok_or_else(|| ZeppelinError::Index("v4 cluster object directory overflows".into()))?;
    let payload_start = CLUSTER_DATA_OBJECT_HEADER_LEN
        .checked_add(directory_len)
        .ok_or_else(|| ZeppelinError::Index("v4 cluster object header overflows".into()))?;
    if data.len() < payload_start {
        return Err(ZeppelinError::Index(format!(
            "v4 cluster data object truncated directory: expected at least {payload_start}, got {}",
            data.len()
        )));
    }

    let mut sections = Vec::with_capacity(entry_count);
    let mut seen = BTreeSet::new();
    for entry_idx in 0..entry_count {
        let base =
            CLUSTER_DATA_OBJECT_HEADER_LEN + entry_idx * CLUSTER_DATA_OBJECT_V4_DIR_ENTRY_LEN;
        let cluster_idx = u32::from_le_bytes(
            data[base..base + 4]
                .try_into()
                .map_err(|_| ZeppelinError::Index("v4 cluster object index parse error".into()))?,
        ) as usize;
        if !seen.insert(cluster_idx) {
            return Err(ZeppelinError::Index(format!(
                "duplicate cluster {cluster_idx} in v4 cluster data object"
            )));
        }

        let sq_offset = read_u64_usize(data, base + 4, "v4 cluster object SQ offset")?;
        let sq_len = read_u64_usize(data, base + 12, "v4 cluster object SQ length")?;
        let full_offset = read_u64_usize(data, base + 20, "v4 cluster object full offset")?;
        let full_len = read_u64_usize(data, base + 28, "v4 cluster object full length")?;
        if sq_offset < payload_start {
            return Err(ZeppelinError::Index(format!(
                "v4 cluster SQ section starts inside directory: offset={sq_offset}, payload_start={payload_start}"
            )));
        }
        let sq_end = sq_offset.checked_add(sq_len).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "v4 cluster SQ section overflows: offset={sq_offset}, len={sq_len}"
            ))
        })?;
        if full_offset < sq_end {
            return Err(ZeppelinError::Index(format!(
                "v4 cluster full section overlaps SQ section: full_offset={full_offset}, sq_end={sq_end}"
            )));
        }
        let full_end = full_offset.checked_add(full_len).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "v4 cluster full section overflows: offset={full_offset}, len={full_len}"
            ))
        })?;
        sections.push(ClusterObjectRange {
            cluster_idx,
            sq: Some(sq_offset..sq_end),
            full: full_offset..full_end,
        });
    }

    Ok(ClusterObjectLayout { sections })
}

/// Reads and validates the entry count shared by grouped-object headers.
///
/// # Parameters
///
/// - `data`: Bytes containing at least the shared eight-byte header.
///
/// # Returns
///
/// A strictly positive platform-sized entry count.
///
/// # Errors
///
/// Returns an index error for a short header, malformed count field, or zero
/// entries.
fn cluster_object_entry_count(data: &[u8]) -> Result<usize> {
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
    Ok(entry_count)
}

/// Recognizes the v4 grouped-object signature without parsing its directory.
///
/// # Parameters
///
/// - `data`: Any byte slice, including one shorter than a header.
///
/// # Returns
///
/// `true` only when the first four bytes are `ZBP` followed by version 4.
fn is_cluster_data_object_v4(data: &[u8]) -> bool {
    data.len() >= 4
        && &data[0..3] == CLUSTER_DATA_OBJECT_MAGIC_PREFIX
        && data[3] == CLUSTER_DATA_OBJECT_V4_VERSION
}

// ---------------------------------------------------------------------------
// ZBP5: hoisted IDs and fixed-stride f32 rows
// ---------------------------------------------------------------------------

/// Recognizes the v5 grouped-object signature without parsing its directory.
///
/// # Parameters
///
/// - `data`: Any byte slice, including one shorter than a header.
///
/// # Returns
///
/// `true` only when the first four bytes are `ZBP` followed by version 5.
fn is_cluster_data_object_v5(data: &[u8]) -> bool {
    data.len() >= 4
        && &data[0..3] == CLUSTER_DATA_OBJECT_MAGIC_PREFIX
        && data[3] == CLUSTER_DATA_OBJECT_V5_VERSION
}

/// Serializes one cluster's row IDs into a deterministic `ZBP5` ID block.
///
/// The block is `row_count:u32` followed by repeated `[id_len:u32][UTF-8 id]`
/// in row order. Row position joins each ID to the sibling coarse and
/// fixed-stride f32 blocks of the same cluster.
///
/// # Errors
///
/// Returns an index error when the row count or an ID length does not fit the
/// format's `u32` fields.
pub(crate) fn serialize_id_block(ids: &[String]) -> Result<Bytes> {
    let row_count = u32::try_from(ids.len()).map_err(|_| {
        ZeppelinError::Index(format!(
            "v5 ID block row count does not fit in u32: {}",
            ids.len()
        ))
    })?;
    let mut buf = Vec::new();
    buf.extend_from_slice(&row_count.to_le_bytes());
    for id in ids {
        let id_bytes = id.as_bytes();
        let id_len = u32::try_from(id_bytes.len()).map_err(|_| {
            ZeppelinError::Index(format!(
                "v5 ID block ID length does not fit in u32: {}",
                id_bytes.len()
            ))
        })?;
        buf.extend_from_slice(&id_len.to_le_bytes());
        buf.extend_from_slice(id_bytes);
    }
    Ok(Bytes::from(buf))
}

/// Parses one `ZBP5` ID block into owned row IDs.
///
/// # Returns
///
/// Exactly the declared number of IDs in persisted row order.
///
/// # Errors
///
/// Returns an index error for a truncated header, ID length, or ID payload,
/// for non-UTF-8 IDs, or for trailing bytes after the declared rows. The block
/// has an exact length; malformed input is never partially returned.
/// Reads only the row count an ID block declares in its header.
///
/// Lets the `ZBP5` serializer cross-check a caller's row count against the ID
/// block it supplied without decoding every ID.
///
/// # Errors
///
/// Returns an index error when the block is too small to hold its header.
fn id_block_row_count(data: &[u8]) -> Result<usize> {
    if data.len() < 4 {
        return Err(ZeppelinError::Index(
            "v5 ID block too small for header".into(),
        ));
    }
    Ok(u32::from_le_bytes(
        data[0..4]
            .try_into()
            .map_err(|_| ZeppelinError::Index("v5 ID block header parse error".into()))?,
    ) as usize)
}

pub(crate) fn deserialize_id_block(data: &[u8]) -> Result<Vec<String>> {
    let row_count = id_block_row_count(data)?;

    // Cap the reservation by what the payload could possibly hold: each row
    // carries at least a 4-byte id_len prefix, so a valid row_count never
    // exceeds data.len() / 4. A hostile or corrupt header otherwise requests
    // gigabytes before any per-row validation runs.
    let mut ids = Vec::with_capacity(row_count.min(data.len() / 4));
    let mut offset = 4;
    for row in 0..row_count {
        if offset + 4 > data.len() {
            return Err(ZeppelinError::Index(
                "v5 ID block truncated at id_len".into(),
            ));
        }
        let id_len = u32::from_le_bytes(
            data[offset..offset + 4]
                .try_into()
                .map_err(|_| ZeppelinError::Index("v5 ID block id_len parse error".into()))?,
        ) as usize;
        offset += 4;
        let end = offset
            .checked_add(id_len)
            .ok_or_else(|| ZeppelinError::Index("v5 ID block ID range overflows".into()))?;
        if end > data.len() {
            return Err(ZeppelinError::Index("v5 ID block truncated at id".into()));
        }
        let id = std::str::from_utf8(&data[offset..end])
            .map_err(|_| ZeppelinError::Index(format!("v5 ID block row {row} is not valid UTF-8")))?
            .to_owned();
        offset = end;
        ids.push(id);
    }
    if offset != data.len() {
        return Err(ZeppelinError::Index(format!(
            "v5 ID block has {} trailing bytes",
            data.len() - offset
        )));
    }
    Ok(ids)
}

/// Computes the exact byte length of a fixed-stride f32 block.
///
/// # Returns
///
/// `row_count × dim × 4`, the only valid length for a `ZBP5` vector block.
///
/// # Errors
///
/// Returns an index error when the multiplication overflows.
pub(crate) fn fixed_stride_f32_block_len(row_count: usize, dim: usize) -> Result<usize> {
    row_count
        .checked_mul(dim)
        .and_then(|floats| floats.checked_mul(4))
        .ok_or_else(|| ZeppelinError::Index("fixed-stride f32 block size overflows".into()))
}

/// Serializes exact vectors into a fixed-stride `ZBP5` f32 block.
///
/// The block is exactly `row_count × dim × 4` little-endian bytes with no
/// per-row header and no per-row ID, so row `r` occupies
/// `r × dim × 4 .. (r + 1) × dim × 4` within the block.
///
/// # Errors
///
/// Returns an index error when a row's width differs from `dim` or the size
/// arithmetic overflows. A wrong width would silently shift every following
/// row, so it is rejected rather than zipped short.
pub(crate) fn serialize_fixed_stride_f32_block(vectors: &[Vec<f32>], dim: usize) -> Result<Bytes> {
    let total = fixed_stride_f32_block_len(vectors.len(), dim)?;
    let mut buf = Vec::with_capacity(total);
    for (row, vector) in vectors.iter().enumerate() {
        if vector.len() != dim {
            return Err(ZeppelinError::Index(format!(
                "fixed-stride f32 row {row} width mismatch: expected {dim}, got {}",
                vector.len()
            )));
        }
        for &val in vector {
            buf.extend_from_slice(&val.to_le_bytes());
        }
    }
    debug_assert_eq!(buf.len(), total);
    Ok(Bytes::from(buf))
}

/// Parses a fixed-stride `ZBP5` f32 block into owned vectors.
///
/// # Parameters
///
/// - `data`: Complete vector-block bytes for one cluster.
/// - `row_count`: Declared row count from the directory or manifest layout.
/// - `dim`: Vector dimension shared by every row.
///
/// # Errors
///
/// Returns an index error when `data.len()` differs from exactly
/// `row_count × dim × 4` or the size arithmetic overflows.
pub(crate) fn deserialize_fixed_stride_f32_block(
    data: &[u8],
    row_count: usize,
    dim: usize,
) -> Result<Vec<Vec<f32>>> {
    let expected = fixed_stride_f32_block_len(row_count, dim)?;
    if data.len() != expected {
        return Err(ZeppelinError::Index(format!(
            "fixed-stride f32 block size mismatch: expected {expected}, got {}",
            data.len()
        )));
    }
    let row_bytes = dim * 4;
    let mut vectors = Vec::with_capacity(row_count);
    let mut offset = 0;
    for _ in 0..row_count {
        let end = offset + row_bytes;
        vectors.push(
            data[offset..end]
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect(),
        );
        offset = end;
    }
    Ok(vectors)
}

/// Typed per-cluster layout metadata for one `ZBP5` directory entry.
///
/// Ranges are absolute byte offsets within the grouped object. Serialization
/// returns these values so the publisher records exactly what was written; the
/// full-object parser derives the same values as the object's self-describing
/// corruption boundary. Row `r`'s exact-vector range is pure arithmetic via
/// [`Self::vector_row_range`]; no ID-length walk is ever required.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Zbp5ClusterLayout {
    /// Logical IVF cluster named by the directory entry.
    pub cluster_idx: usize,
    /// Number of rows shared by the coarse, ID, and vector blocks.
    pub row_count: usize,
    /// Absolute range of the codes-and-factors coarse block.
    pub coarse: Range<u64>,
    /// Absolute range of the deterministic ID block.
    pub ids: Range<u64>,
    /// Absolute range of the fixed-stride f32 vector block.
    pub vectors: Range<u64>,
}

impl Zbp5ClusterLayout {
    /// Rebuilds the typed layout from its manifest projection.
    ///
    /// The manifest is authoritative for query ranges, so this is the seam that
    /// lets the hot path plan reads without a grouped-object header GET. The
    /// values are validated once by
    /// [`ClusterDataObjectRef::validate_row_layouts`] when the index handle is
    /// constructed.
    #[must_use]
    pub(crate) fn from_manifest(layout: &ClusterRowLayoutRef) -> Self {
        Self {
            cluster_idx: layout.cluster_idx,
            row_count: layout.row_count as usize,
            coarse: layout.coarse_offset..layout.coarse_offset + layout.coarse_len,
            ids: layout.ids_offset..layout.ids_offset + layout.ids_len,
            vectors: layout.vectors_offset..layout.vectors_offset + layout.vectors_len,
        }
    }

    /// Returns the coarse block as a platform-sized range.
    ///
    /// # Errors
    ///
    /// Returns an index error when an endpoint does not fit in `usize`.
    pub(crate) fn coarse_range(&self) -> Result<Range<usize>> {
        usize_range(&self.coarse, "v5 coarse block")
    }

    /// Returns the ID block as a platform-sized range.
    ///
    /// # Errors
    ///
    /// Returns an index error when an endpoint does not fit in `usize`.
    pub(crate) fn ids_range(&self) -> Result<Range<usize>> {
        usize_range(&self.ids, "v5 ID block")
    }

    /// Returns row `r`'s exact-vector span as a platform-sized range.
    ///
    /// # Errors
    ///
    /// Propagates [`Self::vector_row_range`] and narrowing failures.
    pub(crate) fn vector_row_range_usize(&self, row: usize, dim: usize) -> Result<Range<usize>> {
        usize_range(&self.vector_row_range(row, dim)?, "v5 vector row")
    }

    /// Computes row `r`'s exact-vector range with checked arithmetic.
    ///
    /// The range is `vectors_offset + r × dim × 4 .. + dim × 4`, clamped to the
    /// declared vector block.
    ///
    /// # Errors
    ///
    /// Returns an index error when `row` is outside the declared row count,
    /// when the arithmetic overflows, or when the computed range escapes the
    /// vector block. There is no fallback estimate.
    pub(crate) fn vector_row_range(&self, row: usize, dim: usize) -> Result<Range<u64>> {
        if row >= self.row_count {
            return Err(ZeppelinError::Index(format!(
                "v5 vector row {row} out of bounds for {} rows",
                self.row_count
            )));
        }
        let stride = u64::try_from(dim)
            .ok()
            .and_then(|dim| dim.checked_mul(4))
            .ok_or_else(|| ZeppelinError::Index("v5 vector row stride overflows".into()))?;
        let row_offset = stride
            .checked_mul(row as u64)
            .ok_or_else(|| ZeppelinError::Index("v5 vector row offset overflows".into()))?;
        let start = self
            .vectors
            .start
            .checked_add(row_offset)
            .ok_or_else(|| ZeppelinError::Index("v5 vector row start overflows".into()))?;
        let end = start
            .checked_add(stride)
            .ok_or_else(|| ZeppelinError::Index("v5 vector row end overflows".into()))?;
        if end > self.vectors.end {
            return Err(ZeppelinError::Index(format!(
                "v5 vector row {row} escapes vectors block: end={end}, vectors_end={}",
                self.vectors.end
            )));
        }
        Ok(start..end)
    }
}

/// Borrowed per-cluster blocks that become one `ZBP5` directory entry.
pub(crate) struct Zbp5ClusterBlocks<'a> {
    /// Logical IVF cluster named in the grouped directory.
    pub cluster_idx: usize,
    /// Row count shared by all three blocks.
    pub row_count: usize,
    /// Segment vector dimension. The object stores no dimension of its own, so
    /// readers recover it as `vectors.len() / row_count / 4`. Carrying it here
    /// lets the serializer prove that division is exact before the bytes become
    /// immutable.
    pub dim: usize,
    /// Codes-and-factors coarse block (SQ8 or two-bit), without IDs.
    pub coarse: &'a [u8],
    /// Deterministic ID block from [`serialize_id_block`].
    pub ids: &'a [u8],
    /// Fixed-stride f32 block from [`serialize_fixed_stride_f32_block`].
    pub vectors: &'a [u8],
}

/// A serialized `ZBP5` object plus the typed layout that was written.
///
/// The layout is returned, never re-derived, so the publisher can record the
/// exact ranges in manifest metadata without re-parsing the just-built bytes.
pub(crate) struct SerializedClusterDataObjectV5 {
    /// Complete immutable object bytes.
    pub bytes: Bytes,
    /// One layout descriptor per directory entry, in stored order.
    pub layout: Vec<Zbp5ClusterLayout>,
}

impl From<&Zbp5ClusterLayout> for ClusterRowLayoutRef {
    fn from(layout: &Zbp5ClusterLayout) -> Self {
        Self {
            cluster_idx: layout.cluster_idx,
            row_count: layout.row_count as u64,
            coarse_offset: layout.coarse.start,
            coarse_len: layout.coarse.end - layout.coarse.start,
            ids_offset: layout.ids.start,
            ids_len: layout.ids.end - layout.ids.start,
            vectors_offset: layout.vectors.start,
            vectors_len: layout.vectors.end - layout.vectors.start,
        }
    }
}

/// One cluster's serialized payload before it is placed in a grouped object.
///
/// The variant decides the grouped-object layout: quantized builds emit the
/// three `ZBP5` blocks, while unquantized and product-quantized builds keep the
/// legacy row section. A group is never mixed.
pub(crate) enum ClusterPayload {
    /// One complete legacy or co-located cluster section.
    Legacy(Bytes),
    /// Hoisted-ID `ZBP5` blocks for one cluster, in row order.
    RowLayout {
        /// Rows shared by all three blocks.
        row_count: usize,
        /// Segment vector dimension, carried so the serializer can verify the
        /// vector block's fixed stride before the object becomes immutable.
        dim: usize,
        /// Codes-and-factors coarse block without IDs.
        coarse: Bytes,
        /// Deterministic ID block.
        ids: Bytes,
        /// Fixed-stride f32 block of exactly `row_count × dim × 4` bytes.
        vectors: Bytes,
    },
}

/// A serialized grouped object plus the manifest metadata describing it.
pub(crate) struct SerializedClusterGroup {
    /// Complete immutable object bytes.
    pub bytes: Bytes,
    /// [`CLUSTER_LAYOUT_VERSION_ZBP5`] for a row-layout object, else `0`.
    pub layout_version: u32,
    /// Exactly the ranges written, in entry order; empty for earlier layouts.
    pub row_layouts: Vec<ClusterRowLayoutRef>,
}

/// Serializes one group of clusters into a single immutable object.
///
/// # Parameters
///
/// - `entries`: `(logical cluster index, payload)` pairs. Entry order becomes
///   directory and payload order.
///
/// # Returns
///
/// The object bytes plus the manifest layout metadata the publisher must record
/// verbatim. Row layouts come from the serializer, never from re-parsing the
/// bytes that were just built.
///
/// # Errors
///
/// Returns an index error for an empty group, a group mixing row-layout and
/// legacy payloads, or any child serializer failure. Mixing is rejected rather
/// than downgraded: one object has exactly one layout.
pub(crate) fn serialize_cluster_group(
    entries: &[(usize, &ClusterPayload)],
) -> Result<SerializedClusterGroup> {
    if entries.is_empty() {
        return Err(ZeppelinError::Index("cluster group cannot be empty".into()));
    }
    let row_layout_entries = entries
        .iter()
        .filter(|(_, payload)| matches!(payload, ClusterPayload::RowLayout { .. }))
        .count();
    if row_layout_entries != 0 && row_layout_entries != entries.len() {
        return Err(ZeppelinError::Index(format!(
            "cluster group mixes v5 row-layout and legacy payloads: {row_layout_entries} of {}",
            entries.len()
        )));
    }

    if row_layout_entries == 0 {
        let legacy: Vec<(usize, Bytes)> = entries
            .iter()
            .map(|(cluster_idx, payload)| match payload {
                ClusterPayload::Legacy(bytes) => (*cluster_idx, bytes.clone()),
                ClusterPayload::RowLayout { .. } => unreachable!("checked above"),
            })
            .collect();
        return Ok(SerializedClusterGroup {
            bytes: serialize_cluster_data_object(&legacy)?,
            layout_version: 0,
            row_layouts: Vec::new(),
        });
    }

    let blocks: Vec<Zbp5ClusterBlocks<'_>> = entries
        .iter()
        .map(|(cluster_idx, payload)| match payload {
            ClusterPayload::RowLayout {
                row_count,
                dim,
                coarse,
                ids,
                vectors,
            } => Zbp5ClusterBlocks {
                cluster_idx: *cluster_idx,
                row_count: *row_count,
                dim: *dim,
                coarse,
                ids,
                vectors,
            },
            ClusterPayload::Legacy(_) => unreachable!("checked above"),
        })
        .collect();
    let serialized = serialize_cluster_data_object_v5(&blocks)?;
    Ok(SerializedClusterGroup {
        row_layouts: serialized.layout.iter().map(Into::into).collect(),
        bytes: serialized.bytes,
        layout_version: CLUSTER_LAYOUT_VERSION_ZBP5,
    })
}

/// Writes a v5 grouped object: directory, coarse blocks, ID blocks, then
/// fixed-stride f32 blocks, each region set in entry order.
///
/// # Parameters
///
/// - `entries`: Prevalidated cluster indexes and their three block payloads.
///   Entry order becomes directory and block order; indexes must be unique and
///   fit in `u32`.
///
/// # Returns
///
/// The object bytes together with the typed per-cluster layout exactly as
/// written.
///
/// # Errors
///
/// Returns an index error for no entries, a duplicate/oversized cluster index
/// or row count, an ID block that does not declare exactly `row_count` rows, a
/// vector block that is not exactly `row_count × dim × 4` bytes, or any checked
/// directory, block, range, or total-size overflow.
///
/// The row-count and stride checks are what make the object self-consistent:
/// the format stores no dimension, so a reader recovers it by dividing the
/// vector block by the row count. Rejecting a mismatch here means that division
/// is exact for every object this writer has ever produced, and a reader that
/// finds otherwise is looking at corruption.
///
/// # Examples
///
/// Two clusters become `directory | coarse0 | coarse1 | ids0 | ids1 |
/// vectors0 | vectors1`; the first ID block therefore starts exactly where the
/// last coarse block ends.
pub(crate) fn serialize_cluster_data_object_v5(
    entries: &[Zbp5ClusterBlocks<'_>],
) -> Result<SerializedClusterDataObjectV5> {
    if entries.is_empty() {
        return Err(ZeppelinError::Index(
            "v5 cluster data object cannot be empty".into(),
        ));
    }
    let mut seen = BTreeSet::new();
    for entry in entries {
        if entry.cluster_idx > u32::MAX as usize {
            return Err(ZeppelinError::Index(format!(
                "cluster index does not fit in u32: {}",
                entry.cluster_idx
            )));
        }
        if entry.row_count > u32::MAX as usize {
            return Err(ZeppelinError::Index(format!(
                "v5 cluster row count does not fit in u32: {}",
                entry.row_count
            )));
        }
        if !seen.insert(entry.cluster_idx) {
            return Err(ZeppelinError::Index(format!(
                "duplicate cluster {} in v5 cluster data object",
                entry.cluster_idx
            )));
        }
        let declared_ids = id_block_row_count(entry.ids)?;
        if declared_ids != entry.row_count {
            return Err(ZeppelinError::Index(format!(
                "v5 cluster {} declares {} rows but its ID block declares {declared_ids}",
                entry.cluster_idx, entry.row_count
            )));
        }
        let expected_vectors = fixed_stride_f32_block_len(entry.row_count, entry.dim)?;
        if entry.vectors.len() != expected_vectors {
            return Err(ZeppelinError::Index(format!(
                "v5 cluster {} vector block is {} bytes, expected {expected_vectors} for {} rows at dim {}",
                entry.cluster_idx,
                entry.vectors.len(),
                entry.row_count,
                entry.dim
            )));
        }
    }

    let directory_len = entries
        .len()
        .checked_mul(CLUSTER_DATA_OBJECT_V5_DIR_ENTRY_LEN)
        .ok_or_else(|| ZeppelinError::Index("v5 cluster object directory overflows".into()))?;
    let payload_offset = CLUSTER_DATA_OBJECT_HEADER_LEN
        .checked_add(directory_len)
        .ok_or_else(|| ZeppelinError::Index("v5 cluster object header overflows".into()))?;

    let mut coarse_ranges = Vec::with_capacity(entries.len());
    let mut cursor = payload_offset;
    for entry in entries {
        let end = cursor.checked_add(entry.coarse.len()).ok_or_else(|| {
            ZeppelinError::Index("v5 cluster object coarse block overflows".into())
        })?;
        coarse_ranges.push(cursor..end);
        cursor = end;
    }
    let mut id_ranges = Vec::with_capacity(entries.len());
    for entry in entries {
        let end = cursor
            .checked_add(entry.ids.len())
            .ok_or_else(|| ZeppelinError::Index("v5 cluster object ID block overflows".into()))?;
        id_ranges.push(cursor..end);
        cursor = end;
    }
    let mut vector_ranges = Vec::with_capacity(entries.len());
    for entry in entries {
        let end = cursor.checked_add(entry.vectors.len()).ok_or_else(|| {
            ZeppelinError::Index("v5 cluster object vectors block overflows".into())
        })?;
        vector_ranges.push(cursor..end);
        cursor = end;
    }
    let total = cursor;

    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(CLUSTER_DATA_OBJECT_MAGIC_PREFIX);
    buf.push(CLUSTER_DATA_OBJECT_V5_VERSION);
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (idx, entry) in entries.iter().enumerate() {
        buf.extend_from_slice(&(entry.cluster_idx as u32).to_le_bytes());
        buf.extend_from_slice(&(entry.row_count as u32).to_le_bytes());
        buf.extend_from_slice(&(coarse_ranges[idx].start as u64).to_le_bytes());
        buf.extend_from_slice(&(entry.coarse.len() as u64).to_le_bytes());
        buf.extend_from_slice(&(id_ranges[idx].start as u64).to_le_bytes());
        buf.extend_from_slice(&(entry.ids.len() as u64).to_le_bytes());
        buf.extend_from_slice(&(vector_ranges[idx].start as u64).to_le_bytes());
        buf.extend_from_slice(&(entry.vectors.len() as u64).to_le_bytes());
    }
    for entry in entries {
        buf.extend_from_slice(entry.coarse);
    }
    for entry in entries {
        buf.extend_from_slice(entry.ids);
    }
    for entry in entries {
        buf.extend_from_slice(entry.vectors);
    }
    debug_assert_eq!(buf.len(), total);

    let layout = entries
        .iter()
        .enumerate()
        .map(|(idx, entry)| Zbp5ClusterLayout {
            cluster_idx: entry.cluster_idx,
            row_count: entry.row_count,
            coarse: coarse_ranges[idx].start as u64..coarse_ranges[idx].end as u64,
            ids: id_ranges[idx].start as u64..id_ranges[idx].end as u64,
            vectors: vector_ranges[idx].start as u64..vector_ranges[idx].end as u64,
        })
        .collect();

    Ok(SerializedClusterDataObjectV5 {
        bytes: Bytes::from(buf),
        layout,
    })
}

/// Parses and validates a complete `ZBP5` grouped object's directory.
///
/// Beyond field decoding, every region must tile the object exactly: coarse
/// blocks are contiguous from the end of the directory in entry order, ID
/// blocks follow contiguously, vector blocks follow contiguously, and the last
/// vector block ends exactly at the object end. Any gap, overlap, truncation,
/// or trailing byte is an error — the parser never returns a partial layout.
///
/// # Parameters
///
/// - `data`: Complete `ZBP5` object bytes.
///
/// # Returns
///
/// One validated layout descriptor per directory entry, in stored order.
///
/// # Errors
///
/// Returns an index error for a non-v5 signature, a truncated header or
/// directory, zero entries, duplicate cluster indexes, non-tiling ranges, an
/// inexact object end, or arithmetic overflow.
pub(crate) fn parse_cluster_data_object_v5(data: &[u8]) -> Result<Vec<Zbp5ClusterLayout>> {
    if !is_cluster_data_object_v5(data) {
        return Err(ZeppelinError::Index("not a v5 cluster data object".into()));
    }
    let entry_count = cluster_object_entry_count(data)?;
    let directory_len = entry_count
        .checked_mul(CLUSTER_DATA_OBJECT_V5_DIR_ENTRY_LEN)
        .ok_or_else(|| ZeppelinError::Index("v5 cluster object directory overflows".into()))?;
    let payload_start = CLUSTER_DATA_OBJECT_HEADER_LEN
        .checked_add(directory_len)
        .ok_or_else(|| ZeppelinError::Index("v5 cluster object header overflows".into()))?;
    if data.len() < payload_start {
        return Err(ZeppelinError::Index(format!(
            "v5 cluster data object truncated directory: expected at least {payload_start}, got {}",
            data.len()
        )));
    }

    let mut layouts = Vec::with_capacity(entry_count);
    let mut seen = BTreeSet::new();
    for entry_idx in 0..entry_count {
        let base =
            CLUSTER_DATA_OBJECT_HEADER_LEN + entry_idx * CLUSTER_DATA_OBJECT_V5_DIR_ENTRY_LEN;
        let cluster_idx = u32::from_le_bytes(
            data[base..base + 4]
                .try_into()
                .map_err(|_| ZeppelinError::Index("v5 cluster object index parse error".into()))?,
        ) as usize;
        if !seen.insert(cluster_idx) {
            return Err(ZeppelinError::Index(format!(
                "duplicate cluster {cluster_idx} in v5 cluster data object"
            )));
        }
        let row_count =
            u32::from_le_bytes(data[base + 4..base + 8].try_into().map_err(|_| {
                ZeppelinError::Index("v5 cluster object row count parse error".into())
            })?) as usize;
        let coarse_offset = read_u64_usize(data, base + 8, "v5 cluster object coarse offset")?;
        let coarse_len = read_u64_usize(data, base + 16, "v5 cluster object coarse length")?;
        let ids_offset = read_u64_usize(data, base + 24, "v5 cluster object IDs offset")?;
        let ids_len = read_u64_usize(data, base + 32, "v5 cluster object IDs length")?;
        let vectors_offset = read_u64_usize(data, base + 40, "v5 cluster object vectors offset")?;
        let vectors_len = read_u64_usize(data, base + 48, "v5 cluster object vectors length")?;
        layouts.push((
            cluster_idx,
            row_count,
            coarse_offset,
            coarse_len,
            ids_offset,
            ids_len,
            vectors_offset,
            vectors_len,
        ));
    }

    // Exact tiling: all coarse blocks tile from payload_start in entry order,
    // then all ID blocks, then all vector blocks; the object must end exactly
    // at the last vector block's end.
    let mut cursor = payload_start;
    for region in 0..3 {
        for (_, _, coarse_offset, coarse_len, ids_offset, ids_len, vectors_offset, vectors_len) in
            &layouts
        {
            let (offset, len, label) = match region {
                0 => (*coarse_offset, *coarse_len, "coarse"),
                1 => (*ids_offset, *ids_len, "ID"),
                _ => (*vectors_offset, *vectors_len, "vectors"),
            };
            let end = offset.checked_add(len).ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "v5 cluster {label} block overflows: offset={offset}, len={len}"
                ))
            })?;
            if offset != cursor {
                return Err(ZeppelinError::Index(format!(
                    "v5 cluster {label} block does not tile: offset={offset}, expected={cursor}"
                )));
            }
            cursor = end;
        }
    }
    if cursor != data.len() {
        return Err(ZeppelinError::Index(format!(
            "v5 cluster data object size mismatch: expected exact end {cursor}, got {}",
            data.len()
        )));
    }

    let layouts_typed = layouts
        .into_iter()
        .map(
            |(
                cluster_idx,
                row_count,
                coarse_offset,
                coarse_len,
                ids_offset,
                ids_len,
                vectors_offset,
                vectors_len,
            )| {
                Zbp5ClusterLayout {
                    cluster_idx,
                    row_count,
                    coarse: coarse_offset as u64..(coarse_offset + coarse_len) as u64,
                    ids: ids_offset as u64..(ids_offset + ids_len) as u64,
                    vectors: vectors_offset as u64..(vectors_offset + vectors_len) as u64,
                }
            },
        )
        .collect();

    Ok(layouts_typed)
}

/// Validates a half-open persisted range against complete object bytes.
///
/// # Parameters
///
/// - `range`: Absolute `start..end` byte interval.
/// - `object_len`: Complete object size available to slice.
/// - `label`: Context included in error messages.
///
/// # Returns
///
/// `Ok(())` when `start <= end <= object_len`.
///
/// # Errors
///
/// Returns an index error for a reversed or out-of-bounds range.
/// Narrows a persisted `u64` range to this platform's `usize`.
///
/// # Errors
///
/// Returns an index error when either endpoint does not fit in `usize`.
fn usize_range(range: &Range<u64>, label: &str) -> Result<Range<usize>> {
    let start = usize::try_from(range.start).map_err(|_| {
        ZeppelinError::Index(format!(
            "{label} start does not fit in usize: {}",
            range.start
        ))
    })?;
    let end = usize::try_from(range.end).map_err(|_| {
        ZeppelinError::Index(format!("{label} end does not fit in usize: {}", range.end))
    })?;
    Ok(start..end)
}

fn validate_range_in_object(range: &Range<usize>, object_len: usize, label: &str) -> Result<()> {
    if range.start > range.end || range.end > object_len {
        return Err(ZeppelinError::Index(format!(
            "{label} out of bounds: start={}, end={}, len={object_len}",
            range.start, range.end
        )));
    }
    Ok(())
}

/// Selects the exact-vector child from a co-located or legacy cluster section.
///
/// # Parameters
///
/// - `data`: One complete child cluster section.
///
/// # Returns
///
/// The quantized section's full-vector slice, or all legacy full-only bytes.
///
/// # Errors
///
/// Returns an index error when a recognized quantized header has invalid
/// offsets or size.
fn full_cluster_section(data: &[u8]) -> Result<&[u8]> {
    if cluster_section_encoding(data)?.is_none() {
        return Ok(data);
    }
    Ok(colocated_cluster_sections(data)?.full)
}

/// Returns the coarse encoding named by a recognized cluster-section magic.
///
/// A nonzero fourth byte after `ZCL` is a version discriminator. Unknown
/// versions fail before their payload can be mistaken for a legacy cluster;
/// `ZCL\0` remains eligible for legacy decoding because little-endian row
/// counts can collide with the three-byte prefix.
fn cluster_section_encoding(data: &[u8]) -> Result<Option<CoarsePayloadEncoding>> {
    if data.starts_with(CLUSTER_V2_MAGIC) {
        Ok(Some(CoarsePayloadEncoding::Sq8))
    } else if data.starts_with(CLUSTER_V3_MAGIC) {
        Ok(Some(CoarsePayloadEncoding::TwoBit))
    } else if data.len() >= 4 && &data[..3] == b"ZCL" && data[3] != 0 {
        let version = if data[3].is_ascii_digit() {
            data[3] - b'0'
        } else {
            data[3]
        };
        Err(ZeppelinError::Index(format!(
            "unsupported cluster section version ZCL{version}; this binary reads ZCL2, ZCL3, and legacy standalone clusters"
        )))
    } else {
        Ok(None)
    }
}

/// Merges one artifact's detected coarse encoding into a segment-wide result.
///
/// Artifacts without coarse evidence (legacy v1 objects, full-only standalone
/// clusters) contribute nothing. Two artifacts naming different encodings are
/// a loud error: one segment has exactly one coarse encoding, and a mixed
/// layout is corruption that a guessed label would only hide.
///
/// # Errors
///
/// Returns an index error when `new` conflicts with an encoding already
/// detected from another artifact of the same segment.
fn merge_detected_encoding(
    detected: &mut Option<CoarsePayloadEncoding>,
    new: Option<CoarsePayloadEncoding>,
) -> Result<()> {
    let Some(new) = new else {
        return Ok(());
    };
    match detected {
        Some(existing) if *existing != new => Err(ZeppelinError::Index(format!(
            "segment mixes coarse payload encodings: {existing:?} and {new:?}"
        ))),
        _ => {
            *detected = Some(new);
            Ok(())
        }
    }
}

/// Detects the encoding of one `ZBP5` codes-only coarse block.
///
/// Neither codes-only format carries a magic, but their headers overlap
/// deterministically: the SQ8 header's `dimension: u32` field (bytes 4..8,
/// always nonzero) occupies the high half of the two-bit header's
/// `row_count: u64` field (always zero, because row counts fit the `ZBP5`
/// directory's u32). One read therefore selects the arm without guessing,
/// and the strict production decoder for that arm validates the full block —
/// including its exact-length check — so a corrupt block is an error rather
/// than a mislabeled encoding.
///
/// # Errors
///
/// Propagates the selected decoder's validation error (index or RQ), and
/// returns an index error when the block is too small for either header or
/// decodes a row count that disagrees with the `ZBP5` directory entry.
fn detect_codes_only_encoding(block: &[u8], declared_rows: usize) -> Result<CoarsePayloadEncoding> {
    if block.len() < 8 {
        return Err(ZeppelinError::Index(format!(
            "v5 coarse block too small for a codes-only header: {} bytes",
            block.len()
        )));
    }
    // Header overlap invariant: SQ8 writes a nonzero dimension here; two-bit
    // writes the always-zero high half of its u64 row count. See the doc above.
    let header_overlap = u32::from_le_bytes([block[4], block[5], block[6], block[7]]);
    if header_overlap == 0 {
        let codes = crate::index::quantization::rq::RqClusterCodesOnly::from_bytes(block)?;
        if codes.row_count() != declared_rows {
            return Err(ZeppelinError::Index(format!(
                "v5 two-bit coarse block row count mismatch: directory declares {declared_rows}, block holds {}",
                codes.row_count()
            )));
        }
        Ok(CoarsePayloadEncoding::TwoBit)
    } else {
        let codes = crate::index::quantization::sq::deserialize_sq_codes_only(block)?;
        if codes.codes.len() != declared_rows {
            return Err(ZeppelinError::Index(format!(
                "v5 SQ8 coarse block row count mismatch: directory declares {declared_rows}, block holds {}",
                codes.codes.len()
            )));
        }
        Ok(CoarsePayloadEncoding::Sq8)
    }
}

/// Detects the coarse payload encoding persisted in one grouped cluster
/// object.
///
/// `ZBP5` objects are probed per coarse block (see
/// [`detect_codes_only_encoding`]). `ZBP4` objects store each cluster's
/// coarse child contiguously, and the two-bit child keeps its `ZRQ1`
/// container signature, which the magic-less SQ8 payload never carries; both
/// v4 arms are validated with their strict production decoders. Returns
/// `None` for v1 legacy objects, which hold exact vectors only.
///
/// # Errors
///
/// Returns an index error for a malformed directory, an unrecognized or
/// corrupt coarse block, or mixed encodings inside one object.
fn detect_cluster_object_encoding(data: &[u8]) -> Result<Option<CoarsePayloadEncoding>> {
    if is_cluster_data_object_v5(data) {
        let mut detected = None;
        for layout in parse_cluster_data_object_v5(data)? {
            let coarse = usize_range(&layout.coarse, "v5 coarse block")?;
            validate_range_in_object(&coarse, data.len(), "v5 coarse block")?;
            merge_detected_encoding(
                &mut detected,
                Some(detect_codes_only_encoding(&data[coarse], layout.row_count)?),
            )?;
        }
        return Ok(detected);
    }
    if is_cluster_data_object_v4(data) {
        let mut detected = None;
        for section in cluster_object_layout_v4(data)?.sections {
            let sq = section.sq.as_ref().ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "v4 cluster {} is missing its coarse section",
                    section.cluster_idx
                ))
            })?;
            validate_range_in_object(sq, data.len(), "v4 cluster coarse section")?;
            let child = &data[sq.start..sq.end];
            let encoding = if crate::index::quantization::rq::is_rq_container(child) {
                // Decode only to validate; the probing loader retains no rows.
                let _codes = crate::index::quantization::rq::RqClusterCodes::from_bytes(child)?;
                CoarsePayloadEncoding::TwoBit
            } else {
                crate::index::quantization::sq::deserialize_sq_cluster(child)?;
                CoarsePayloadEncoding::Sq8
            };
            merge_detected_encoding(&mut detected, Some(encoding))?;
        }
        return Ok(detected);
    }
    Ok(None)
}

/// Validates quantized offsets and borrows coarse and exact-vector children.
///
/// # Parameters
///
/// - `data`: Complete co-located cluster-section bytes.
///
/// # Returns
///
/// Non-overlapping contiguous child slices, with SQ first and full data second.
///
/// # Errors
///
/// Returns an index error for a short header, malformed integer, unexpected
/// coarse start, non-contiguous full start, arithmetic overflow, or exact-size
/// mismatch.
///
/// # Examples
///
/// A valid section laid out as `header | SQ | full` returns the two payloads.
/// Appending a byte is rejected because immutable artifact lengths are exact.
fn colocated_cluster_sections(data: &[u8]) -> Result<ColocatedClusterSections<'_>> {
    if data.len() < CLUSTER_V2_HEADER_LEN {
        return Err(ZeppelinError::Index(
            "quantized cluster blob too small for header".into(),
        ));
    }
    if cluster_section_encoding(data)?.is_none() {
        return Err(ZeppelinError::Index(
            "unrecognized quantized cluster magic".into(),
        ));
    }

    let coarse_offset = read_u64_usize(data, 4, "cluster coarse offset")?;
    let coarse_len = read_u64_usize(data, 12, "cluster coarse length")?;
    let full_offset = read_u64_usize(data, 20, "cluster full offset")?;
    let full_len = read_u64_usize(data, 28, "cluster full length")?;

    if coarse_offset != CLUSTER_V2_HEADER_LEN {
        return Err(ZeppelinError::Index(format!(
            "cluster coarse offset mismatch: expected {CLUSTER_V2_HEADER_LEN}, got {coarse_offset}"
        )));
    }
    let expected_full_offset = coarse_offset.checked_add(coarse_len).ok_or_else(|| {
        ZeppelinError::Index(format!(
            "cluster coarse section overflows: offset={coarse_offset}, len={coarse_len}"
        ))
    })?;
    if full_offset != expected_full_offset {
        return Err(ZeppelinError::Index(format!(
            "cluster full offset mismatch: expected {expected_full_offset}, got {full_offset}"
        )));
    }
    let expected_len = full_offset.checked_add(full_len).ok_or_else(|| {
        ZeppelinError::Index(format!(
            "cluster full section overflows: offset={full_offset}, len={full_len}"
        ))
    })?;
    if data.len() != expected_len {
        return Err(ZeppelinError::Index(format!(
            "quantized cluster blob size mismatch: expected {expected_len}, got {}",
            data.len()
        )));
    }

    Ok(ColocatedClusterSections {
        coarse: &data[coarse_offset..full_offset],
        full: &data[full_offset..expected_len],
    })
}

/// Reads one little-endian `u64` field and converts it safely to `usize`.
///
/// # Parameters
///
/// - `data`: Buffer containing the complete eight-byte field.
/// - `offset`: Start of the field.
/// - `label`: Context included in parse/conversion errors.
///
/// # Returns
///
/// The platform-sized value.
///
/// # Errors
///
/// Returns an index error when the eight-byte slice is unavailable/malformed or
/// the value cannot fit this platform's `usize`.
fn read_u64_usize(data: &[u8], offset: usize, label: &str) -> Result<usize> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| ZeppelinError::Index(format!("{label} offset overflows")))?;
    let bytes = data
        .get(offset..end)
        .ok_or_else(|| ZeppelinError::Index(format!("{label} is truncated")))?;
    let value = u64::from_le_bytes(
        bytes
            .try_into()
            .map_err(|_| ZeppelinError::Index(format!("{label} parse error")))?,
    );
    usize::try_from(value)
        .map_err(|_| ZeppelinError::Index(format!("{label} does not fit in usize: {value}")))
}

fn read_u32_usize(data: &[u8], offset: usize, label: &str) -> Result<usize> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| ZeppelinError::Index(format!("{label} offset overflows")))?;
    let bytes = data
        .get(offset..end)
        .ok_or_else(|| ZeppelinError::Index(format!("{label} is truncated")))?;
    Ok(u32::from_le_bytes(
        bytes
            .try_into()
            .map_err(|_| ZeppelinError::Index(format!("{label} parse error")))?,
    ) as usize)
}

/// Attributes blob: JSON-serialized `Vec<Option<HashMap<String, AttributeValue>>>`.
///
/// We use JSON rather than bincode because `AttributeValue` uses
/// `#[serde(untagged)]`, which requires `deserialize_any` -- a method
/// that bincode does not support.
///
/// # Parameters
///
/// - `attrs`: Row-aligned optional attribute maps for one logical cluster.
///
/// # Returns
///
/// UTF-8 JSON bytes preserving vector-row order and explicit null rows.
///
/// # Errors
///
/// Returns a serialization error if serde cannot encode the values. No object
/// is written.
///
/// # Examples
///
/// `[Some({"color": "red"}), None]` remains two rows, so the first attribute
/// still belongs to the first vector after decode.
///
/// # Rust Notes for Java/C Engineers
///
/// `Option<HashMap<...>>` makes absence explicit rather than using a nullable
/// pointer. The self-describing JSON format is required because serde's
/// untagged enum must inspect value shape during decoding.
pub(crate) fn serialize_attrs(attrs: &[Option<HashMap<String, AttributeValue>>]) -> Result<Bytes> {
    let encoded = serde_json::to_vec(attrs)?;
    Ok(Bytes::from(encoded))
}

/// Decodes one cluster's row-aligned JSON attribute sidecar.
///
/// # Parameters
///
/// - `data`: Complete JSON sidecar bytes loaded from the manifest-selected
///   cluster owner.
///
/// # Returns
///
/// Owned optional maps in stored row order.
///
/// # Errors
///
/// Returns a serde error for invalid JSON, unsupported value shapes, or values
/// that cannot be represented by [`AttributeValue`].
///
/// # Examples
///
/// A two-row sidecar containing one object and one `null` yields `Some(map)`
/// followed by `None`.
pub(crate) fn deserialize_attrs(
    data: &[u8],
) -> Result<Vec<Option<HashMap<String, AttributeValue>>>> {
    Ok(serde_json::from_slice(data)?)
}

// ---------------------------------------------------------------------------
// Build pipeline
// ---------------------------------------------------------------------------

/// Deterministic CPU-only IVF partition used by production segment builds.
///
/// Row indexes refer to the input slice. Each logical row has exactly one
/// canonical primary cluster; the no-spill policy keeps `spilled` at zero and
/// stores each row exactly once. The private affinity matrix preserves the
/// existing second-nearest-centroid signal used to group physical objects.
#[derive(Debug, Clone, PartialEq)]
#[must_use]
pub struct IvfPartition {
    /// Trained centroids in stable training order.
    pub centroids: Vec<Vec<f32>>,
    /// Input row indexes stored by logical cluster.
    pub clusters: Vec<Vec<u32>>,
    /// Canonical primary cluster for every input row.
    pub primary: Vec<u32>,
    /// Number of additional stored row copies. Always zero without spill.
    pub spilled: usize,
    /// Symmetric second-nearest affinity used only by the artifact builder.
    pub(crate) buddy_affinity: Vec<Vec<u32>>,
}

/// Trains centroids and assigns every row without performing object-store I/O.
///
/// This is the production partition seam shared by the immutable segment
/// builder and the real-dataset recall gate. Keeping training and assignment
/// here prevents an offline evaluator from drifting away from shipped
/// behavior.
///
/// # Errors
///
/// Returns an index error for empty input, zero dimension, or an input too
/// large for persisted `u32` row indexes. Returns a dimension mismatch when a
/// row length differs from `dim`, and propagates k-means training failures.
#[must_use = "the IVF partition result must be handled"]
pub fn partition_vectors(
    vectors: &[&[f32]],
    dim: usize,
    config: &IndexingConfig,
) -> Result<IvfPartition> {
    if vectors.is_empty() {
        return Err(ZeppelinError::Index(
            "cannot build index from empty vector set".into(),
        ));
    }
    if dim == 0 {
        return Err(ZeppelinError::Index("vector dimension must be > 0".into()));
    }
    if vectors.len() > u32::MAX as usize {
        return Err(ZeppelinError::Index(format!(
            "IVF partition row count exceeds u32: {}",
            vectors.len()
        )));
    }
    for vector in vectors {
        if vector.len() != dim {
            return Err(ZeppelinError::DimensionMismatch {
                expected: dim,
                actual: vector.len(),
            });
        }
    }

    let k = config.effective_num_centroids(vectors.len());
    let mut centroids = train_kmeans(
        vectors,
        dim,
        k,
        config.kmeans_max_iterations,
        config.kmeans_convergence_epsilon,
    )?;
    repair_cluster_balance(
        vectors,
        dim,
        &mut centroids,
        config.balance_max_ratio,
        config.balance_repair_rounds,
    );
    let num_clusters = centroids.len();
    let mut clusters = vec![Vec::new(); num_clusters];
    let mut primary = Vec::with_capacity(vectors.len());
    let mut buddy_affinity = vec![vec![0u32; num_clusters]; num_clusters];

    for (row_idx, vector) in vectors.iter().enumerate() {
        let mut best_dist = f32::MAX;
        let mut second_dist = f32::MAX;
        let mut best_cluster = 0usize;
        let mut second_cluster = 0usize;
        for (cluster_idx, centroid) in centroids.iter().enumerate() {
            let candidate = distance::euclidean_distance(vector, centroid);
            if candidate < best_dist {
                second_dist = best_dist;
                second_cluster = best_cluster;
                best_dist = candidate;
                best_cluster = cluster_idx;
            } else if candidate < second_dist {
                second_dist = candidate;
                second_cluster = cluster_idx;
            }
        }
        if num_clusters > 1 && second_cluster != best_cluster {
            buddy_affinity[best_cluster][second_cluster] =
                buddy_affinity[best_cluster][second_cluster].saturating_add(1);
            buddy_affinity[second_cluster][best_cluster] =
                buddy_affinity[second_cluster][best_cluster].saturating_add(1);
        }

        let row_idx = row_idx as u32;
        clusters[best_cluster].push(row_idx);
        primary.push(best_cluster as u32);
    }

    let stored_rows = clusters.iter().map(Vec::len).sum::<usize>();
    assert_eq!(
        stored_rows,
        vectors.len(),
        "no-spill IVF partition must store every logical row exactly once"
    );

    Ok(IvfPartition {
        centroids,
        clusters,
        primary,
        spilled: 0,
        buddy_affinity,
    })
}

/// Builds and uploads a complete immutable IVF-Flat segment candidate.
///
/// The operation validates one same-dimensional vector snapshot, trains
/// k-means centroids, assigns each row to its nearest centroid, records the
/// runner-up affinity used for physical object grouping, and produces all
/// cluster, attribute, bitmap, membership, sketch, bootstrap, and configured
/// quantization artifacts. It returns an index handle and manifest references
/// but deliberately does not publish a manifest.
///
/// # Parameters
///
/// - `vectors`: Complete borrowed vector snapshot for the new segment. It must
///   be non-empty; every row must have the same non-zero dimension. IDs and
///   attributes are cloned into cluster order during construction.
/// - `config`: Validated indexing configuration controlling centroid count,
///   k-means convergence, quantization, PQ subdivision, and bitmap sidecars.
/// - `store`: Zeppelin object-store boundary used for every immutable PUT.
/// - `namespace`: Validated namespace key prefix. This is an object-store key
///   component, not an HTTP path segment.
/// - `segment_id`: Fresh segment identifier. Reusing an ID would overwrite
///   write-once keys and violates the caller contract.
///
/// # Returns
///
/// An owned [`IvfFlatIndex`] with resident centroids/sketch and references to
/// the uploaded cluster objects, bootstrap, membership, and sketch. Callers use
/// those references when constructing the later manifest [`SegmentRef`][crate::wal::manifest::SegmentRef].
///
/// # Errors
///
/// Returns an index error for empty input, zero dimension, invalid grouping or
/// persisted-format limits, and centroid/quantization/sketch training failures;
/// returns [`ZeppelinError::DimensionMismatch`] for inconsistent row shape;
/// and propagates serialization and object-store PUT failures.
///
/// Failure is not transactional. Because objects are uploaded in phases, an
/// error may leave immutable but unreferenced objects under `segment_id`. No
/// manifest is changed here, so those objects are not visible to readers and
/// may later be removed as orphans.
///
/// # Side Effects
///
/// Writes the centroid object first; grouped cluster data and row sidecars in
/// parallel; then membership, sketch, and bootstrap objects; and finally PQ
/// codebook/cluster objects when product quantization is configured. It emits
/// structured progress logs and may print grouping statistics when enabled.
///
/// # Consistency
///
/// Every output key is segment-scoped and intended to be immutable. Successful
/// PUTs establish object existence only. A subsequent compaction fencing check
/// and manifest compare-and-swap make the segment authoritative and visible.
/// This function neither acquires a lease nor writes the manifest.
///
/// # Performance
///
/// Centroid training dominates CPU for many builds. Assignment costs
/// `O(vector_count * centroid_count * dim)`; grouping additionally costs
/// `O(centroid_count^2 * dim)` time and `O(centroid_count^2)` memory. The build
/// clones vector values into cluster buffers and temporarily retains encoded
/// artifacts until their PUT futures complete. It writes one cluster-data
/// object per density group rather than one per cluster; attributes and
/// optional bitmaps remain one sidecar per cluster.
///
/// # Examples
///
/// Suppose compaction has 50,000 surviving 768-dimensional product vectors and
/// a fresh ID `seg-42`. This function trains the configured centroids, uploads
/// immutable `seg-42` artifacts, and returns their refs. Queries still see the
/// previous segment until compaction successfully publishes a manifest naming
/// `seg-42`. If the sketch PUT fails, earlier centroid and cluster objects may
/// remain in S3 but are not queryable through the manifest.
///
/// # Rust Notes for Java/C Engineers
///
/// `vectors: &[VectorEntry]` and `config: &IndexingConfig` are shared borrows;
/// the builder cannot consume or mutate caller state. It intentionally clones
/// IDs, values, and attributes into a new row order because the async PUT
/// futures must own stable data after the assignment loop. [`Bytes`] clones in
/// `write_futs` share immutable payload allocations, unlike cloning a `Vec`,
/// which would copy elements. `join_all` drives independent PUT futures
/// concurrently and returns every result; the later loop uses `?` to surface
/// failures rather than discard them.
pub async fn build_ivf_flat(
    vectors: &[VectorEntry],
    config: &IndexingConfig,
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
) -> Result<IvfFlatIndex> {
    let dim = vectors.first().map_or(0, |vector| vector.values.len());
    let vec_refs: Vec<&[f32]> = vectors.iter().map(|v| v.values.as_slice()).collect();
    let partition = partition_vectors(&vec_refs, dim, config)?;
    let num_clusters = partition.centroids.len();

    info!(
        n = vectors.len(),
        dim = dim,
        k = num_clusters,
        namespace = namespace,
        segment_id = segment_id,
        "building IVF-Flat index"
    );

    // --- Steps 1-2: Train centroids and assign rows through the shared seam. ---
    let IvfPartition {
        centroids,
        clusters,
        buddy_affinity,
        ..
    } = partition;
    info!(num_clusters = num_clusters, "k-means training complete");

    let mut cluster_ids: Vec<Vec<String>> = vec![Vec::new(); num_clusters];
    let mut cluster_vecs: Vec<Vec<Vec<f32>>> = vec![Vec::new(); num_clusters];
    let mut cluster_attrs: Vec<Vec<Option<HashMap<String, AttributeValue>>>> =
        vec![Vec::new(); num_clusters];

    for (cluster_idx, rows) in clusters.iter().enumerate() {
        for &row_idx in rows {
            let entry = &vectors[row_idx as usize];
            cluster_ids[cluster_idx].push(entry.id.clone());
            cluster_vecs[cluster_idx].push(entry.values.clone());
            cluster_attrs[cluster_idx].push(entry.attributes.clone());
        }
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
    let (sketch_ref, sketch_data, resident_sketch) = build_resident_sketch(
        namespace,
        segment_id,
        dim,
        &centroids,
        &cluster_vecs,
        &cluster_attrs,
    )?;
    let rq_rotation = if quantization == QuantizationType::TwoBit {
        let rotation_seed = sketch_ref.rotation_seed.ok_or_else(|| {
            ZeppelinError::Config(format!(
                "namespace {namespace} configured quantization=two_bit but its resident sketch has no resolvable rotation seed"
            ))
        })?;
        Some(crate::index::quantization::rabitq::StructuredRotation::new(
            sketch_ref.code_dims,
            rotation_seed,
        )?)
    } else {
        None
    };

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
    let mut bitmap_complete_fields: Option<BTreeSet<String>> = None;
    let mut cluster_bitmap_indexes = Vec::new();
    let mut cluster_sections: Vec<ClusterPayload> = Vec::with_capacity(num_clusters);
    let mut sidecar_payloads: Vec<(String, Bytes)> = Vec::new();
    for i in 0..num_clusters {
        let cvec_data = match quantization {
            QuantizationType::Scalar => {
                let calibration = sq_calibration.as_ref().ok_or_else(|| {
                    ZeppelinError::Index("SQ8 build omitted its calibration".into())
                })?;
                let cluster_refs: Vec<&[f32]> =
                    cluster_vecs[i].iter().map(|v| v.as_slice()).collect();
                let codes = calibration.encode_batch(&cluster_refs);
                ClusterPayload::RowLayout {
                    row_count: cluster_ids[i].len(),
                    dim,
                    coarse: crate::index::quantization::sq::serialize_sq_codes_only(&codes, dim)?,
                    ids: serialize_id_block(&cluster_ids[i])?,
                    vectors: serialize_fixed_stride_f32_block(&cluster_vecs[i], dim)?,
                }
            }
            QuantizationType::TwoBit => {
                let rotation = rq_rotation.as_ref().ok_or_else(|| {
                    ZeppelinError::Config(format!(
                        "namespace {namespace} configured quantization=two_bit but no rotation was resolved"
                    ))
                })?;
                let cluster_refs: Vec<&[f32]> =
                    cluster_vecs[i].iter().map(|v| v.as_slice()).collect();
                let codes = crate::index::quantization::rq::RqClusterCodes::encode(
                    &cluster_ids[i],
                    &cluster_refs,
                    &centroids[i],
                    rotation,
                )?;
                ClusterPayload::RowLayout {
                    row_count: cluster_ids[i].len(),
                    dim,
                    coarse: codes.to_codes_only_bytes(),
                    ids: serialize_id_block(&cluster_ids[i])?,
                    vectors: serialize_fixed_stride_f32_block(&cluster_vecs[i], dim)?,
                }
            }
            QuantizationType::None | QuantizationType::Product => {
                ClusterPayload::Legacy(serialize_cluster(&cluster_ids[i], &cluster_vecs[i], dim)?)
            }
        };
        cluster_sections.push(cvec_data);

        let cattr_data = serialize_attrs(&cluster_attrs[i])?;
        let cattr_key = attrs_key(namespace, segment_id, i);
        sidecar_payloads.push((cattr_key, cattr_data));

        if config.bitmap_index {
            let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
                cluster_attrs[i].iter().map(|a| a.as_ref()).collect();
            let bitmap_index = crate::index::bitmap::build::build_cluster_bitmaps(&attr_refs);
            let cluster_bitmap_fields: BTreeSet<String> =
                bitmap_index.fields.keys().cloned().collect();
            bitmap_fields_set.extend(cluster_bitmap_fields.iter().cloned());
            bitmap_complete_fields = Some(match bitmap_complete_fields.take() {
                Some(complete) => complete
                    .intersection(&cluster_bitmap_fields)
                    .cloned()
                    .collect(),
                None => cluster_bitmap_fields,
            });
            let bitmap_data = bitmap_index.to_bytes()?;
            let bkey = crate::index::bitmap::bitmap_key(namespace, segment_id, i);
            sidecar_payloads.push((bkey, bitmap_data));
            cluster_bitmap_indexes.push(bitmap_index);
        }
    }
    let bitmap_fields: Vec<String> = bitmap_fields_set.into_iter().collect();
    let bitmap_complete_fields = bitmap_complete_fields.unwrap_or_default();
    let filter_summary = super::filter_summary::build_filter_cardinality_summary(
        &cluster_bitmap_indexes,
        &bitmap_complete_fields,
        config.filter_summary_max_values_per_field,
        config.filter_summary_max_bytes,
    )?;
    if filter_summary.eligible_fields != bitmap_complete_fields {
        return Err(ZeppelinError::Index(
            "filter summary eligible fields disagree with complete bitmap fields".into(),
        ));
    }
    info!(
        segment_id,
        encoded_size_bytes = filter_summary.bytes.len(),
        covered_field_count = filter_summary.summary.covered_fields.len(),
        skipped_high_cardinality_fields = ?filter_summary.skipped_high_cardinality_fields,
        dropped_for_size_fields = ?filter_summary.dropped_for_size_fields,
        "built filter cardinality summary"
    );

    let mut cluster_objects = Vec::new();
    let mut cluster_object_payloads = Vec::new();
    for (group_idx, group) in density_cluster_groups(&centroids, &buddy_affinity)?
        .into_iter()
        .enumerate()
    {
        let entries: Vec<(usize, &ClusterPayload)> = group
            .iter()
            .map(|&cluster_idx| (cluster_idx, &cluster_sections[cluster_idx]))
            .collect();
        let key = cluster_group_key(namespace, segment_id, group_idx);
        let serialized = serialize_cluster_group(&entries)?;
        let size_bytes = serialized.bytes.len() as u64;
        cluster_objects.push(ClusterDataObjectRef {
            key: key.clone(),
            clusters: group,
            live_offset: 0,
            live_len: 0,
            size_bytes,
            cluster_layout_version: serialized.layout_version,
            row_layouts: serialized.row_layouts,
        });
        cluster_object_payloads.push((key, serialized.bytes));
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

    // --- Step 4: Write segment membership artifact ---
    let (membership_ref, membership_data) =
        build_membership_artifact(namespace, segment_id, &cluster_ids)?;
    store.put(&membership_ref.key, membership_data).await?;
    info!(
        key = %membership_ref.key,
        entry_count = membership_ref.entry_count,
        size_bytes = membership_ref.size_bytes,
        "wrote segment membership"
    );

    // --- Step 5: Write resident coarse sketch ---
    store.put(&sketch_ref.key, sketch_data.clone()).await?;
    info!(
        key = %sketch_ref.key,
        code_dims = sketch_ref.code_dims,
        bytes_per_vector = sketch_ref.bytes_per_vector,
        size_bytes = sketch_ref.size_bytes,
        "wrote resident coarse sketch"
    );

    // --- Step 6: Write segment bootstrap artifact ---
    let (bootstrap_ref, bootstrap_data) = build_bootstrap_artifact(
        namespace,
        segment_id,
        &centroids_data,
        &sketch_data,
        &bitmap_complete_fields,
        &filter_summary.bytes,
    )?;
    store.put(&bootstrap_ref.key, bootstrap_data).await?;
    info!(
        key = %bootstrap_ref.key,
        size_bytes = bootstrap_ref.size_bytes,
        "wrote segment bootstrap"
    );

    // --- Step 7: Write quantized artifacts (if configured) ---
    match quantization {
        QuantizationType::Scalar => {
            info!("wrote SQ8 co-located clusters and embedded calibration");
        }
        QuantizationType::TwoBit => {
            info!("wrote two-bit RaBitQ co-located clusters");
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

    let cluster_object_by_cluster =
        build_cluster_object_lookup(num_clusters, &cluster_objects, dim)?;
    Ok(IvfFlatIndex {
        centroids: Arc::new(centroids),
        num_vectors: vectors.len(),
        dim,
        namespace: namespace.to_string(),
        physical_namespace: namespace.to_string(),
        physical_origin: None,
        segment_id: segment_id.to_string(),
        quantization,
        sq_calibration,
        bitmap_fields,
        bitmap_complete_fields,
        filter_summary: Some(Arc::new(filter_summary.summary)),
        // Freshly built segment: every cluster owned by this segment.
        cluster_owners: Vec::new(),
        cluster_objects,
        cluster_object_by_cluster,
        resident_sketch: Some(Arc::new(resident_sketch)),
        sketch_ref: Some(sketch_ref),
        bootstrap_ref: Some(bootstrap_ref),
        membership_ref: Some(membership_ref),
    })
}

/// Loads an IVF-Flat handle from manifest-provided metadata.
///
/// This is the normal query-planning loader. New segments provide one bootstrap
/// object containing centroids and the resident sketch. Older segments load
/// those artifacts separately. Cluster vector objects remain lazy and are read
/// only if search chooses them.
///
/// # Parameters
///
/// - `store`: Object-store boundary used after cache misses.
/// - `namespace`: Namespace whose active segment metadata is being loaded.
/// - `segment_id`: Manifest-selected logical segment identifier.
/// - `num_vectors`: Manifest-declared segment cardinality; no cluster scan is
///   performed to recompute it.
/// - `quantization`: Manifest-declared representation strategy.
/// - `cluster_owners`: Per-cluster physical owner IDs for incremental
///   carry-over, or empty for the segment's own legacy layout.
/// - `cluster_objects`: Manifest-defined grouped-object layout, or empty for
///   one legacy object per cluster.
/// - `sketch_ref`: Manifest reference to the resident coarse sketch. A
///   bootstrap requires this ref so embedded sketch size can be validated.
/// - `bootstrap_ref`: Manifest reference to the combined metadata object, or
///   `None` for older independently stored metadata.
/// - `cache`: Optional tiered disk/memory cache for active-segment metadata.
///
/// # Returns
///
/// An owned [`IvfFlatIndex`] that shares decoded centroid/sketch allocations
/// through [`Arc`] and retains the supplied physical layout for lazy search.
///
/// # Errors
///
/// Propagates cache and object-store failures, malformed or size-mismatched
/// centroid/bootstrap/sketch bytes, missing sketch metadata for a bootstrap,
/// SQ calibration decode failures, and invalid cluster-object coverage.
/// Cache failures are not treated as misses unless the cache API itself reports
/// a normal miss.
///
/// # Side Effects
///
/// On a cold path, performs object-store GETs through the cache. With a cache,
/// it inserts decoded metadata and pins the current bootstrap or legacy
/// centroids/sketch under namespace-scoped roles, unpinning the previous active
/// keys.
///
/// # Consistency
///
/// The caller must supply fields from one authoritative manifest snapshot.
/// Cache keys are exactly those refs, and declared sizes are revalidated before
/// decoded bootstrap reuse. Cache residency cannot select a segment or make an
/// unpublished artifact visible.
///
/// # Performance
///
/// A cold current-format load needs one bootstrap GET. A cold legacy load needs
/// one centroid GET plus one sketch GET when a sketch ref exists. Decoded-cache
/// hits need no GET. Cluster-object lookup construction is linear in logical
/// cluster count and manifest entries, avoiding the probing loader's LIST and
/// per-cluster reads.
///
/// # Examples
///
/// A manifest naming `seg-42`, its bootstrap, two grouped cluster objects, and
/// 50,000 rows yields a handle after one cold bootstrap GET. Rotating the
/// manifest to `seg-43` causes its metadata to be pinned and the prior segment's
/// pins to be released; the manifest remains the authority for that rotation.
///
/// # Rust Notes for Java/C Engineers
///
/// Ownership of the supplied `Vec<String>` and `Vec<ClusterDataObjectRef>` is
/// moved into the returned handle, avoiding deep copies. The optional cache is
/// merely borrowed for the async call and cannot be stored accidentally.
/// Pattern matching on `Option` makes the bootstrap and legacy paths explicit.
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
    load_ivf_flat_from_manifest_routed(
        store,
        namespace,
        namespace,
        None,
        None,
        segment_id,
        num_vectors,
        quantization,
        cluster_owners,
        cluster_objects,
        sketch_ref,
        bootstrap_ref,
        cache,
    )
    .await
}

/// Loads one descriptor using the manifest-resolved physical artifact owner.
pub(crate) async fn load_ivf_flat_from_located_manifest(
    store: &ZeppelinStore,
    located: crate::wal::manifest::LocatedSegmentRef<'_>,
    cache: Option<&std::sync::Arc<crate::cache::DiskCache>>,
) -> Result<IvfFlatIndex> {
    let segment = located.segment;
    load_ivf_flat_from_manifest_routed(
        store,
        located.logical_namespace,
        located.physical_namespace(),
        Some(located.logical_origin.as_origin()),
        Some(located.physical_origin.as_origin()),
        &segment.id,
        segment.vector_count,
        segment.quantization,
        segment.cluster_owners.clone(),
        segment.cluster_objects.clone(),
        segment.sketch.clone(),
        segment.bootstrap.clone(),
        cache,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn load_ivf_flat_from_manifest_routed(
    store: &ZeppelinStore,
    logical_namespace: &str,
    physical_namespace: &str,
    logical_origin: Option<&crate::namespace::branching::ArtifactOrigin>,
    physical_origin: Option<&crate::namespace::branching::ArtifactOrigin>,
    segment_id: &str,
    num_vectors: usize,
    quantization: QuantizationType,
    cluster_owners: Vec<String>,
    cluster_objects: Vec<ClusterDataObjectRef>,
    sketch_ref: Option<crate::wal::manifest::SketchRef>,
    bootstrap_ref: Option<BootstrapRef>,
    cache: Option<&std::sync::Arc<crate::cache::DiskCache>>,
) -> Result<IvfFlatIndex> {
    let metadata = match bootstrap_ref.as_ref() {
        Some(bootstrap_ref) => {
            load_bootstrap_artifacts(
                store,
                logical_namespace,
                physical_origin,
                logical_origin,
                bootstrap_ref,
                sketch_ref.as_ref(),
                num_vectors,
                cache,
            )
            .await?
        }
        None => {
            let ckey = centroids_key(physical_namespace, segment_id);
            let cache_key = super::artifact_cache_key(physical_origin, &ckey);
            let data = match cache {
                Some(c) => {
                    let data = c.get_or_fetch(&cache_key, || store.get(&ckey)).await?;
                    // This load path is only used for the manifest's active segment;
                    // pin its centroids (unpinning the previous segment's).
                    c.pin_scoped(
                        &super::cache_pin_scope(logical_origin, logical_namespace, "centroids"),
                        &cache_key,
                    )
                    .await;
                    c.unpin_scoped(&super::cache_pin_scope(
                        logical_origin,
                        logical_namespace,
                        "bootstrap",
                    ))
                    .await;
                    data
                }
                None => store.get(&ckey).await?,
            };
            let centroids_data = deserialize_centroids_data(&data)?;
            let sq_calibration = centroids_data
                .sq_calibration
                .as_ref()
                .map(|bytes| crate::index::quantization::sq::SqCalibration::from_bytes(bytes))
                .transpose()?;
            let resident_sketch = load_resident_sketch(
                store,
                logical_namespace,
                physical_origin,
                logical_origin,
                sketch_ref.as_ref(),
                &centroids_data.centroids,
                num_vectors,
                cache,
            )
            .await?;
            LoadedIndexMetadata {
                centroids: Arc::new(centroids_data.centroids),
                dim: centroids_data.dim,
                sq_calibration,
                resident_sketch,
                bitmap_complete_fields: BTreeSet::new(),
                filter_summary: None,
            }
        }
    };
    let cluster_object_by_cluster =
        build_cluster_object_lookup(metadata.centroids.len(), &cluster_objects, metadata.dim)?;

    info!(
        namespace = logical_namespace,
        physical_namespace,
        segment_id = segment_id,
        num_vectors = num_vectors,
        num_clusters = metadata.centroids.len(),
        dim = metadata.dim,
        quantization = ?quantization,
        "loaded IVF-Flat index from manifest metadata"
    );

    Ok(IvfFlatIndex {
        centroids: metadata.centroids,
        num_vectors,
        dim: metadata.dim,
        namespace: logical_namespace.to_string(),
        physical_namespace: physical_namespace.to_string(),
        physical_origin: physical_origin.cloned(),
        segment_id: segment_id.to_string(),
        quantization,
        sq_calibration: metadata.sq_calibration,
        bitmap_fields: Vec::new(), // Populated from SegmentRef at search time
        bitmap_complete_fields: metadata.bitmap_complete_fields,
        filter_summary: metadata.filter_summary,
        cluster_owners,
        cluster_objects,
        cluster_object_by_cluster,
        resident_sketch: metadata.resident_sketch,
        sketch_ref,
        bootstrap_ref,
        membership_ref: None,
    })
}

/// Loads, validates, decodes, and caches one manifest-selected bootstrap.
///
/// Lookup order is decoded disk-cache metadata, process-wide decoded reuse,
/// tiered raw-byte cache, then S3/MinIO. The process-wide path is used only when
/// a disk cache is present; cache-less callers are intentionally cold.
///
/// # Parameters
///
/// - `store`: Authoritative object-store boundary used after raw cache misses.
/// - `namespace`: Namespace scope used for pin rotation.
/// - `bootstrap_ref`: Manifest key and exact object size.
/// - `sketch_ref`: Required manifest metadata for the embedded sketch section.
/// - `cache`: Optional cache that can hold both raw bytes and decoded values.
///
/// # Returns
///
/// Shared decoded centroids/sketch, dimension, and optional SQ calibration.
///
/// # Errors
///
/// Returns an index error when the sketch ref is absent, manifest/cached/object
/// sizes disagree, or embedded bytes fail bootstrap, centroid, calibration, or
/// sketch validation. Propagates cache and object-store failures.
///
/// # Side Effects
///
/// May GET the bootstrap, insert raw/decoded cache entries, add a process-wide
/// decoded entry, and rotate namespace metadata pins. Pinning occurs after raw
/// fetch and before full decode, so a later format error can leave the bad key
/// pinned until a subsequent rotation.
///
/// # Consistency
///
/// Reuse is keyed by the manifest-selected immutable object key and guarded by
/// both bootstrap and embedded-sketch size checks. The cache cannot override a
/// different manifest ref.
///
/// # Performance
///
/// The cold path performs one GET and decodes both child artifacts. Decoded hits
/// clone only [`Arc`] handles and the small SQ calibration value.
#[allow(clippy::too_many_arguments)]
async fn load_bootstrap_artifacts(
    store: &ZeppelinStore,
    namespace: &str,
    physical_origin: Option<&crate::namespace::branching::ArtifactOrigin>,
    logical_origin: Option<&crate::namespace::branching::ArtifactOrigin>,
    bootstrap_ref: &BootstrapRef,
    sketch_ref: Option<&crate::wal::manifest::SketchRef>,
    expected_vector_count: usize,
    cache: Option<&std::sync::Arc<crate::cache::DiskCache>>,
) -> Result<LoadedIndexMetadata> {
    let Some(sketch_ref) = sketch_ref else {
        return Err(ZeppelinError::Index(format!(
            "bootstrap {} present but segment is missing sketch ref",
            bootstrap_ref.key
        )));
    };

    if let Some(c) = cache {
        let cache_key = super::artifact_cache_key(physical_origin, &bootstrap_ref.key);
        if let Some(decoded) = c.get_decoded::<DecodedBootstrap>(&cache_key)? {
            pin_bootstrap_metadata(c, logical_origin, namespace, &cache_key).await;
            return metadata_from_decoded_bootstrap(
                &bootstrap_ref.key,
                decoded,
                bootstrap_ref,
                sketch_ref,
                expected_vector_count,
            );
        }
    }
    if let Some(c) = cache {
        let cache_key = super::artifact_cache_key(physical_origin, &bootstrap_ref.key);
        // Process-wide decoded reuse is only for disk-cache-backed query paths;
        // cache-less callers are cold by construction and fetch S3 bytes.
        if let Some(decoded) = bootstrap_decoded_cache()
            .get(&cache_key)
            .and_then(|entry| entry.value().upgrade())
        {
            c.insert_decoded(&cache_key, Arc::clone(&decoded));
            pin_bootstrap_metadata(c, logical_origin, namespace, &cache_key).await;
            return metadata_from_decoded_bootstrap(
                &bootstrap_ref.key,
                decoded,
                bootstrap_ref,
                sketch_ref,
                expected_vector_count,
            );
        }
    }

    let data = match cache {
        Some(c) => {
            let cache_key = super::artifact_cache_key(physical_origin, &bootstrap_ref.key);
            let data = c
                .get_or_fetch(&cache_key, || store.get(&bootstrap_ref.key))
                .await?;
            pin_bootstrap_metadata(c, logical_origin, namespace, &cache_key).await;
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
    let bitmap_complete_fields = sections.bitmap_complete_fields.clone();
    let filter_summary = sections
        .filter_summary
        .map(super::filter_summary::FilterCardinalitySummary::from_bytes)
        .transpose()?
        .map(Arc::new);
    if sketch_ref.size_bytes != sections.sketch.len() as u64 {
        return Err(ZeppelinError::Index(format!(
            "coarse sketch size mismatch inside bootstrap {}: manifest={}, section={}",
            bootstrap_ref.key,
            sketch_ref.size_bytes,
            sections.sketch.len()
        )));
    }
    let centroids_data = deserialize_centroids_data(sections.centroids)?;
    let sq_calibration = centroids_data
        .sq_calibration
        .as_ref()
        .map(|bytes| crate::index::quantization::sq::SqCalibration::from_bytes(bytes))
        .transpose()?;
    let centroids = Arc::new(centroids_data.centroids);
    let sketch_bytes = data.slice(sections.sketch_range.clone());
    let sketch = Arc::new(decode_resident_sketch(
        sketch_bytes,
        sketch_ref,
        &centroids,
        expected_vector_count,
    )?);

    let decoded = Arc::new(DecodedBootstrap {
        bootstrap_size_bytes: bootstrap_ref.size_bytes,
        sketch_size_bytes: sketch_ref.size_bytes,
        centroids: Arc::clone(&centroids),
        dim: centroids_data.dim,
        sq_calibration: sq_calibration.clone(),
        resident_sketch: Arc::clone(&sketch),
        bitmap_complete_fields: bitmap_complete_fields.clone(),
        filter_summary: filter_summary.clone(),
    });
    if let Some(c) = cache {
        let cache_key = super::artifact_cache_key(physical_origin, &bootstrap_ref.key);
        bootstrap_decoded_cache().insert(cache_key.clone(), Arc::downgrade(&decoded));
        c.insert_decoded(&cache_key, decoded);
    }

    Ok(LoadedIndexMetadata {
        centroids,
        dim: centroids_data.dim,
        sq_calibration,
        resident_sketch: Some(sketch),
        bitmap_complete_fields,
        filter_summary,
    })
}

/// Revalidates cached bootstrap sizes and builds loader metadata.
///
/// # Parameters
///
/// - `key`: Bootstrap key used in diagnostics.
/// - `decoded`: Shared previously validated bootstrap contents.
/// - `bootstrap_ref`: Current manifest's complete-object size.
/// - `sketch_ref`: Current manifest's embedded-sketch size.
///
/// # Returns
///
/// A metadata view sharing centroid and sketch allocations with the cache.
///
/// # Errors
///
/// Returns an index error if either current manifest size differs from the size
/// stored at decode time.
///
/// # Rust Notes for Java/C Engineers
///
/// `Arc::clone` increments a reference count; it does not clone the centroid or
/// sketch payload. The explicit spelling distinguishes shared ownership from a
/// deep `Vec::clone`.
fn metadata_from_decoded_bootstrap(
    key: &str,
    decoded: Arc<DecodedBootstrap>,
    bootstrap_ref: &BootstrapRef,
    sketch_ref: &crate::wal::manifest::SketchRef,
    expected_vector_count: usize,
) -> Result<LoadedIndexMetadata> {
    if decoded.bootstrap_size_bytes != bootstrap_ref.size_bytes {
        return Err(ZeppelinError::Index(format!(
            "decoded bootstrap size mismatch for {key}: manifest={}, cached={}",
            bootstrap_ref.size_bytes, decoded.bootstrap_size_bytes
        )));
    }
    if decoded.sketch_size_bytes != sketch_ref.size_bytes {
        return Err(ZeppelinError::Index(format!(
            "decoded coarse sketch size mismatch inside bootstrap {key}: manifest={}, cached={}",
            sketch_ref.size_bytes, decoded.sketch_size_bytes
        )));
    }
    decoded.resident_sketch.validate_reference(sketch_ref)?;
    decoded
        .resident_sketch
        .validate_vector_count(expected_vector_count)?;
    decoded
        .resident_sketch
        .validate_centroid_shape(&decoded.centroids)?;
    Ok(LoadedIndexMetadata {
        centroids: Arc::clone(&decoded.centroids),
        dim: decoded.dim,
        sq_calibration: decoded.sq_calibration.clone(),
        resident_sketch: Some(Arc::clone(&decoded.resident_sketch)),
        bitmap_complete_fields: decoded.bitmap_complete_fields.clone(),
        filter_summary: decoded.filter_summary.clone(),
    })
}

/// Rotates cache pins from separate metadata objects to one bootstrap object.
///
/// # Parameters
///
/// - `cache`: Namespace-aware cache whose LRU pins are updated.
/// - `namespace`: Scope name used to replace only this namespace's roles.
/// - `bootstrap_key`: Manifest-selected active bootstrap key to protect.
///
/// # Side Effects
///
/// Removes legacy centroid and sketch role pins, then pins the bootstrap role.
/// The operations are awaited in that order and perform no object-store I/O.
///
/// # Examples
///
/// When `catalog` moves from legacy metadata to `seg-42/bootstrap.bin`, the two
/// old role pins are released before the combined object is pinned.
async fn pin_bootstrap_metadata(
    cache: &crate::cache::DiskCache,
    logical_origin: Option<&crate::namespace::branching::ArtifactOrigin>,
    namespace: &str,
    bootstrap_key: &str,
) {
    cache
        .unpin_scoped(&super::cache_pin_scope(
            logical_origin,
            namespace,
            "centroids",
        ))
        .await;
    cache
        .unpin_scoped(&super::cache_pin_scope(
            logical_origin,
            namespace,
            "coarse_sketch",
        ))
        .await;
    cache
        .pin_scoped(
            &super::cache_pin_scope(logical_origin, namespace, "bootstrap"),
            bootstrap_key,
        )
        .await;
}

/// Loads and optionally caches a legacy separately stored resident sketch.
///
/// # Parameters
///
/// - `store`: Object-store boundary used after cache misses.
/// - `namespace`: Scope for the active sketch pin.
/// - `sketch_ref`: Manifest key and expected size, or `None` for a segment that
///   predates resident sketches.
/// - `cache`: Optional raw and decoded cache.
///
/// # Returns
///
/// `Some` shared decoded sketch when a ref exists, or `None` without any I/O
/// when it does not.
///
/// # Errors
///
/// Propagates cache/object-store failures and returns an index error for invalid
/// sketch bytes or a manifest/object size mismatch.
///
/// # Side Effects
///
/// May fetch and cache raw bytes, pin the active sketch key, and insert the
/// decoded value. Pinning precedes size validation on the raw path.
///
/// # Performance
///
/// A decoded hit performs no GET and shares the allocation. A cold path
/// performs one GET and one complete sketch decode.
#[allow(clippy::too_many_arguments)]
async fn load_resident_sketch(
    store: &ZeppelinStore,
    namespace: &str,
    physical_origin: Option<&crate::namespace::branching::ArtifactOrigin>,
    logical_origin: Option<&crate::namespace::branching::ArtifactOrigin>,
    sketch_ref: Option<&crate::wal::manifest::SketchRef>,
    centroids: &[Vec<f32>],
    expected_vector_count: usize,
    cache: Option<&std::sync::Arc<crate::cache::DiskCache>>,
) -> Result<Option<Arc<ResidentSketch>>> {
    let Some(sketch_ref) = sketch_ref else {
        return Ok(None);
    };

    if let Some(c) = cache {
        let cache_key = super::artifact_cache_key(physical_origin, &sketch_ref.key);
        if let Some(sketch) = c.get_decoded::<ResidentSketch>(&cache_key)? {
            sketch.validate_reference(sketch_ref)?;
            sketch.validate_vector_count(expected_vector_count)?;
            sketch.validate_centroid_shape(centroids)?;
            c.pin_scoped(
                &super::cache_pin_scope(logical_origin, namespace, "coarse_sketch"),
                &cache_key,
            )
            .await;
            return Ok(Some(sketch));
        }
    }

    let data = match cache {
        Some(c) => {
            let cache_key = super::artifact_cache_key(physical_origin, &sketch_ref.key);
            let data = c
                .get_or_fetch(&cache_key, || store.get(&sketch_ref.key))
                .await?;
            c.pin_scoped(
                &super::cache_pin_scope(logical_origin, namespace, "coarse_sketch"),
                &cache_key,
            )
            .await;
            data
        }
        None => store.get(&sketch_ref.key).await?,
    };
    let sketch = Arc::new(decode_resident_sketch(
        data,
        sketch_ref,
        centroids,
        expected_vector_count,
    )?);
    if let Some(c) = cache {
        let cache_key = super::artifact_cache_key(physical_origin, &sketch_ref.key);
        c.insert_decoded(&cache_key, Arc::clone(&sketch));
    }
    Ok(Some(sketch))
}

/// Reconstructs an IVF-Flat handle by listing and probing segment artifacts.
///
/// This compatibility loader is used by compaction and tests when a
/// [`SegmentRef`][crate::wal::manifest::SegmentRef] is not supplied. It loads
/// centroids, lists grouped keys, reads every cluster to count rows, and probes
/// quantization sidecars plus the coarse encoding persisted in the cluster
/// artifacts. Normal query planning should use
/// [`load_ivf_flat_from_manifest`] so the manifest determines layout without
/// expensive discovery.
///
/// # Parameters
///
/// - `store`: Object-store boundary used for all GET and LIST requests.
/// - `namespace`: Namespace key prefix containing the segment directory.
/// - `segment_id`: Existing segment directory to inspect.
///
/// # Returns
///
/// An [`IvfFlatIndex`] with owned centroids, reconstructed row count and grouped
/// layout, and detected quantization. Cluster payloads are decoded for counting
/// but are not retained in the returned handle.
///
/// # Errors
///
/// Propagates centroid, list, grouped/per-cluster GET, layout, and cluster decode
/// errors. A key with a grouped filename but non-grouped bytes is an error.
/// The PQ and legacy SQ sidecar probes are intentionally heuristic: any PQ
/// probe error is treated as "not PQ," and any legacy SQ probe error as
/// "not SQ." Coarse-encoding detection is strict instead — a corrupt or
/// unrecognized coarse block, or mixed encodings across a segment's objects,
/// is an index error, because a wrong silent quantization label is worse
/// than a failure.
///
/// # Side Effects
///
/// Performs object-store reads and emits a structured load log. It does not
/// write artifacts, cache data, or publish a manifest.
///
/// # Consistency
///
/// This function discovers physical objects by prefix and therefore is not an
/// authority boundary. Callers must already know the segment is appropriate to
/// inspect; query visibility still comes exclusively from the manifest. It does
/// not load bootstrap, sketch, membership, cluster-owner carry-over, or bitmap
/// field metadata.
///
/// # Performance
///
/// Performs one centroid GET, one prefix LIST, one GET per grouped object (or
/// per logical cluster for legacy layout), then one PQ probe and possibly one SQ
/// probe. It decodes all full cluster vectors solely to sum row counts. The
/// manifest-aware loader avoids this work.
///
/// # Examples
///
/// Compaction inspecting a legacy four-cluster segment GETs all four cluster
/// objects to derive cardinality. The returned handle can read/rewrite that
/// segment, but its existence does not itself prove the segment is active.
///
/// # Rust Notes for Java/C Engineers
///
/// The iterator pipeline filters listed keys and owns the selected `String`s;
/// `collect` makes that ownership explicit before asynchronous GETs begin.
/// `Option` is not used for probe errors here—the code calls `is_ok`, so all
/// error detail is deliberately discarded by this legacy heuristic.
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
    // Coarse-encoding evidence gathered from the cluster artifacts themselves;
    // the quantization probe below consumes it after the calibration checks.
    let mut detected_encoding: Option<CoarsePayloadEncoding> = None;
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
        merge_detected_encoding(
            &mut detected_encoding,
            detect_cluster_object_encoding(&data)?,
        )?;
        let sections = cluster_object_sections(&data)?.ok_or_else(|| {
            ZeppelinError::Index(format!(
                "cluster object key {key} did not contain grouped data"
            ))
        })?;
        for section in &sections {
            let cluster = section.decode()?;
            num_vectors += cluster.ids.len();
        }
        let clusters = sections
            .into_iter()
            .map(|section| section.cluster_idx)
            .collect::<Vec<_>>();
        // The probing loader recovers the same declared layout a manifest would
        // publish, so a handle built here reads a v5 object exactly like one
        // loaded from the manifest instead of silently degrading to full GETs.
        let (cluster_layout_version, row_layouts) = if is_cluster_data_object_v5(&data) {
            (
                CLUSTER_LAYOUT_VERSION_ZBP5,
                parse_cluster_data_object_v5(&data)?
                    .iter()
                    .map(Into::into)
                    .collect(),
            )
        } else {
            (0, Vec::new())
        };
        cluster_objects.push(ClusterDataObjectRef {
            key,
            clusters,
            live_offset: 0,
            live_len: 0,
            size_bytes: data.len() as u64,
            cluster_layout_version,
            row_layouts,
        });
    }

    if cluster_objects.is_empty() {
        for i in 0..num_clusters {
            let cvec_key = cluster_key(namespace, segment_id, i);
            let cluster_data = store.get(&cvec_key).await?;
            merge_detected_encoding(
                &mut detected_encoding,
                cluster_section_encoding(&cluster_data)?,
            )?;
            let cluster = deserialize_cluster(&cluster_data)?;
            num_vectors += cluster.ids.len();
        }
    }
    let cluster_object_by_cluster =
        build_cluster_object_lookup(num_clusters, &cluster_objects, dim)?;

    // Detect quantization: check for PQ codebook first, then embedded or
    // legacy SQ calibration, then the coarse encoding persisted in the
    // cluster artifacts. Two-bit segments carry no calibration sidecar, so
    // only the artifact evidence distinguishes them from an unquantized
    // segment; labeling one `None` would silently route it to the flat scan.
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
            } else if matches!(detected_encoding, Some(CoarsePayloadEncoding::TwoBit)) {
                QuantizationType::TwoBit
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
        centroids: Arc::new(centroids),
        num_vectors,
        dim,
        namespace: namespace.to_string(),
        physical_namespace: namespace.to_string(),
        physical_origin: None,
        segment_id: segment_id.to_string(),
        quantization,
        sq_calibration,
        bitmap_fields: Vec::new(), // Populated from SegmentRef at search time
        bitmap_complete_fields: BTreeSet::new(),
        filter_summary: None,
        // Probing loader is used by compaction to read a segment it will fully
        // rewrite, and by tests — legacy single-segment layout.
        cluster_owners: Vec::new(),
        cluster_objects,
        cluster_object_by_cluster,
        resident_sketch: None,
        sketch_ref: None,
        bootstrap_ref: None,
        membership_ref: None,
    })
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Protects persisted-format round trips, corruption rejection, grouping
    //! limits, and normal/legacy metadata-loading paths.
    //!
    //! These unit tests use an in-memory object-store implementation so they
    //! can isolate encoding and cache contracts without external S3 setup. They
    //! do not prove manifest publication; production compaction tests cover the
    //! later visibility boundary. `unwrap` and `expect` are allowed here so a
    //! failed prerequisite stops the test at the exact setup operation.

    use super::*;

    fn empty_filter_summary_bytes() -> Bytes {
        super::super::filter_summary::FilterCardinalitySummary::default()
            .to_bytes()
            .unwrap()
    }

    /// Verifies hostile row counts in tiny payloads are rejected without
    /// reserving row-count-proportional memory.
    ///
    /// Before the reservation cap, an 8-byte blob claiming `u32::MAX` rows
    /// requested a multi-gigabyte allocation (aborting the process) before any
    /// per-row validation ran. Covers the legacy cluster decoder and the v5 ID
    /// block decoder.
    #[test]
    fn test_decoders_reject_hostile_row_counts() {
        let mut cluster = Vec::new();
        cluster.extend_from_slice(&u32::MAX.to_le_bytes());
        cluster.extend_from_slice(&8u32.to_le_bytes());
        assert!(deserialize_legacy_cluster(&cluster).is_err());

        let id_block = u32::MAX.to_le_bytes().to_vec();
        assert!(deserialize_id_block(&id_block).is_err());
    }

    /// Pins the extracted partition seam to the pre-refactor assignment shape.
    ///
    /// Two duplicate rows on each side of the origin must remain canonical
    /// single assignments in input order. This fixture also catches centroid
    /// reordering because `primary` stores the exact deterministic labels.
    #[test]
    fn partition_vectors_preserves_pinned_small_assignments() {
        let values = [
            vec![-1.0, 0.0],
            vec![-1.0, 0.0],
            vec![1.0, 0.0],
            vec![1.0, 0.0],
        ];
        let refs: Vec<&[f32]> = values.iter().map(Vec::as_slice).collect();
        let config = IndexingConfig {
            default_num_centroids: 2,
            ..IndexingConfig::default()
        };

        let partition = partition_vectors(&refs, 2, &config).unwrap();

        assert_eq!(partition.primary, vec![1, 1, 0, 0]);
        assert_eq!(partition.clusters, vec![vec![2, 3], vec![0, 1]]);
        assert_eq!(partition.spilled, 0);
        assert_eq!(partition.buddy_affinity, vec![vec![0, 4], vec![4, 0]]);
    }

    /// Every logical row has exactly one stored location under no-spill policy.
    #[test]
    fn partition_vectors_stores_each_row_exactly_once() {
        let values: Vec<Vec<f32>> = (0..64)
            .map(|row| vec![row as f32, (row % 7) as f32])
            .collect();
        let refs: Vec<&[f32]> = values.iter().map(Vec::as_slice).collect();
        let config = IndexingConfig {
            default_num_centroids: 8,
            max_num_centroids: 8,
            target_rows_per_cluster: usize::MAX,
            ..IndexingConfig::default()
        };

        let partition = partition_vectors(&refs, 2, &config).unwrap();
        let mut occurrences = vec![0usize; values.len()];
        for (cluster, rows) in partition.clusters.iter().enumerate() {
            for &row in rows {
                occurrences[row as usize] += 1;
                assert_eq!(partition.primary[row as usize], cluster as u32);
            }
        }

        assert_eq!(partition.spilled, 0);
        assert_eq!(partition.clusters.iter().map(Vec::len).sum::<usize>(), 64);
        assert!(occurrences.into_iter().all(|count| count == 1));
    }

    /// Proves current centroid bytes preserve row order, values, and dimension.
    ///
    /// A regression in little-endian encoding, header offsets, or row traversal
    /// would change the decoded values and fail this round trip.
    #[test]
    fn test_serialize_deserialize_centroids() {
        let centroids = vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]];
        let data = serialize_centroids(&centroids, 3).unwrap();
        let (decoded, dim) = deserialize_centroids(&data).unwrap();
        assert_eq!(dim, 3);
        assert_eq!(decoded, centroids);
    }

    /// Proves a bootstrap preserves its embedded artifacts byte-for-byte.
    ///
    /// This catches directory offset changes that would make the combined GET
    /// incompatible with the independent centroid or sketch decoders.
    #[test]
    fn test_serialize_deserialize_bootstrap_sections() {
        let centroids = b"centroid-bytes";
        let sketch = b"sketch-bytes";
        let complete_fields = BTreeSet::from(["color".to_string(), "tenant".to_string()]);
        let filter_summary = empty_filter_summary_bytes();
        let data =
            serialize_bootstrap(centroids, sketch, &complete_fields, &filter_summary).unwrap();
        assert_eq!(&data[..8], b"ZBS1\x03\x00\x00\x00");
        let sections = deserialize_bootstrap(&data).unwrap();
        assert_eq!(sections.centroids, centroids);
        assert_eq!(sections.sketch, sketch);
        assert_eq!(sections.bitmap_complete_fields, complete_fields);
        assert_eq!(sections.filter_summary, Some(filter_summary.as_ref()));

        let mut v1 = Vec::new();
        let sketch_offset = BOOTSTRAP_V1_HEADER_LEN + centroids.len();
        v1.extend_from_slice(BOOTSTRAP_MAGIC);
        v1.extend_from_slice(&BOOTSTRAP_VERSION_V1.to_le_bytes());
        v1.extend_from_slice(&(BOOTSTRAP_V1_HEADER_LEN as u64).to_le_bytes());
        v1.extend_from_slice(&(centroids.len() as u64).to_le_bytes());
        v1.extend_from_slice(&(sketch_offset as u64).to_le_bytes());
        v1.extend_from_slice(&(sketch.len() as u64).to_le_bytes());
        v1.extend_from_slice(centroids);
        v1.extend_from_slice(sketch);
        let v1_sections = deserialize_bootstrap(&v1).unwrap();
        assert!(v1_sections.bitmap_complete_fields.is_empty());
        assert!(v1_sections.filter_summary.is_none());

        let encoded_fields = serialize_bitmap_complete_fields(&complete_fields).unwrap();
        let v2_sketch_offset = BOOTSTRAP_V2_HEADER_LEN + centroids.len();
        let v2_fields_offset = v2_sketch_offset + sketch.len();
        let mut v2 = Vec::new();
        v2.extend_from_slice(BOOTSTRAP_MAGIC);
        v2.extend_from_slice(&BOOTSTRAP_VERSION_V2.to_le_bytes());
        v2.extend_from_slice(&(BOOTSTRAP_V2_HEADER_LEN as u64).to_le_bytes());
        v2.extend_from_slice(&(centroids.len() as u64).to_le_bytes());
        v2.extend_from_slice(&(v2_sketch_offset as u64).to_le_bytes());
        v2.extend_from_slice(&(sketch.len() as u64).to_le_bytes());
        v2.extend_from_slice(&(v2_fields_offset as u64).to_le_bytes());
        v2.extend_from_slice(&(encoded_fields.len() as u64).to_le_bytes());
        v2.extend_from_slice(centroids);
        v2.extend_from_slice(sketch);
        v2.extend_from_slice(&encoded_fields);
        let v2_sections = deserialize_bootstrap(&v2).unwrap();
        assert_eq!(v2_sections.bitmap_complete_fields, complete_fields);
        assert!(v2_sections.filter_summary.is_none());
    }

    /// Proves malformed bootstrap identity, version, and bounds fail loudly.
    ///
    /// Accepting any case would allow corrupt remote bytes to reach a child
    /// decoder or permit an out-of-bounds slice.
    #[test]
    fn test_deserialize_bootstrap_rejects_malformed_header() {
        let data = serialize_bootstrap(
            b"centroids",
            b"sketch",
            &BTreeSet::new(),
            &empty_filter_summary_bytes(),
        )
        .unwrap();

        let mut bad_magic = data.to_vec();
        bad_magic[0] = b'X';
        assert!(deserialize_bootstrap(&bad_magic).is_err());

        let mut bad_version = data.to_vec();
        bad_version[4..8].copy_from_slice(&99u32.to_le_bytes());
        assert_eq!(
            deserialize_bootstrap(&bad_version).unwrap_err().to_string(),
            "index error: unsupported bootstrap version: 99"
        );

        let mut bad_bounds = data.to_vec();
        bad_bounds[32..40].copy_from_slice(&999u64.to_le_bytes());
        assert!(deserialize_bootstrap(&bad_bounds).is_err());
    }

    /// Proves legacy cluster encoding keeps IDs aligned with exact vectors.
    ///
    /// Swapping row order or consuming the wrong number of floats would break
    /// query-result identity and fail the equality checks.
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
    fn legacy_cluster_row_count_prefix_collision_remains_legacy() {
        let ids = (0..usize::from(b'Z'))
            .map(|row| format!("legacy-{row}"))
            .collect::<Vec<_>>();
        let vectors = (0..ids.len())
            .map(|row| vec![row as f32])
            .collect::<Vec<_>>();
        let legacy = serialize_cluster(&ids, &vectors, 1).unwrap();
        assert_eq!(&legacy[..4], &[b'Z', 0, 0, 0]);
        assert!(cluster_object_layout(&legacy).unwrap().is_none());
        assert_eq!(deserialize_cluster(&legacy).unwrap().ids, ids);

        // A larger little-endian row count can spell the complete three-byte
        // `ZBP` prefix. Its zero high byte is not a plausible format version,
        // so the grouped-object dispatcher must still leave it to the legacy
        // decoder instead of claiming it as an unsupported version.
        let zbp_collision_header = [b'Z', b'B', b'P', 0, 1, 0, 0, 0];
        assert!(cluster_object_layout(&zbp_collision_header)
            .unwrap()
            .is_none());

        let zcl_collision_header = [b'Z', b'C', b'L', 0, 1, 0, 0, 0];
        assert!(cluster_section_encoding(&zcl_collision_header)
            .unwrap()
            .is_none());
    }

    #[test]
    fn cluster_object_layout_rejects_unknown_version() {
        let future = b"ZBP\x06current-shaped-payload";
        let error = cluster_object_layout(future).unwrap_err();
        assert!(
            matches!(&error, ZeppelinError::Index(message) if
                message == "unsupported cluster data object version 6; this binary reads ZBP1, ZBP4, ZBP5, and legacy standalone clusters"),
            "future cluster object version must fail with the exact compatibility error, got {error:?}"
        );
    }

    #[test]
    fn cluster_section_dispatch_rejects_unknown_version() {
        let ids = vec!["future".to_string()];
        let vectors = vec![vec![1.0, 2.0]];
        let codes = vec![vec![1, 2]];
        let mut future = serialize_colocated_sq_cluster(&ids, &vectors, &codes, 2)
            .unwrap()
            .to_vec();
        future[3] = b'4';

        let error = deserialize_cluster(&future).unwrap_err();
        assert!(
            matches!(&error, ZeppelinError::Index(message) if
                message == "unsupported cluster section version ZCL4; this binary reads ZCL2, ZCL3, and legacy standalone clusters"),
            "future cluster section version must fail with the exact compatibility error, got {error:?}"
        );
    }

    /// Proves a ZCL3 section preserves its two-bit rows and exact vectors.
    #[test]
    fn test_zcl3_section_round_trip() {
        use crate::index::quantization::rabitq::StructuredRotation;
        use crate::index::quantization::rq::RqClusterCodes;

        const DIM: usize = 256;
        const SEED: u64 = 0x5a43_4c33;

        let ids = vec!["rq-0".to_string(), "rq-1".to_string()];
        let vectors = vec![vec![0.5_f32; DIM], vec![-0.25_f32; DIM]];
        let rows: Vec<&[f32]> = vectors.iter().map(Vec::as_slice).collect();
        let centroid = vec![0.125_f32; DIM];
        let rotation = StructuredRotation::new(DIM, SEED).unwrap();
        let codes = RqClusterCodes::encode(&ids, &rows, &centroid, &rotation).unwrap();

        let section = serialize_colocated_rq_cluster(&vectors, &codes, DIM).unwrap();
        let Some(CoarseClusterData::TwoBit(decoded_codes)) =
            deserialize_colocated_coarse_cluster(&section, CoarsePayloadEncoding::TwoBit).unwrap()
        else {
            panic!("ZCL3 section did not decode as two-bit");
        };
        let decoded_full = deserialize_cluster(&section).unwrap();

        assert_eq!(decoded_codes.ids(), codes.ids());
        assert_eq!(decoded_codes.packed_planes(), codes.packed_planes());
        assert_eq!(decoded_codes.to_bytes(), codes.to_bytes());
        assert_eq!(decoded_full.ids, ids);
        assert_eq!(decoded_full.vectors, vectors);
    }

    /// Proves ZCL3 grouped ranges reuse the checked ZCL2 directory arithmetic.
    #[test]
    fn test_zcl3_grouped_offsets_and_overflow_rejection() {
        use crate::index::quantization::rabitq::StructuredRotation;
        use crate::index::quantization::rq::RqClusterCodes;

        const DIM: usize = 256;
        const SEED: u64 = 0x5a43_4c33;

        let ids = vec!["rq-range".to_string()];
        let vectors = vec![vec![0.75_f32; DIM]];
        let rows: Vec<&[f32]> = vectors.iter().map(Vec::as_slice).collect();
        let centroid = vec![0.0_f32; DIM];
        let rotation = StructuredRotation::new(DIM, SEED).unwrap();
        let codes = RqClusterCodes::encode(&ids, &rows, &centroid, &rotation).unwrap();
        let section = serialize_colocated_rq_cluster(&vectors, &codes, DIM).unwrap();
        let object = serialize_cluster_data_object(&[(7, section)]).unwrap();

        let layout = cluster_object_layout(&object).unwrap().unwrap();
        let range = layout.section(7).unwrap();
        let coarse = range.sq.clone().unwrap();
        let Some(CoarseClusterData::TwoBit(decoded_codes)) =
            deserialize_colocated_coarse_cluster_from_object(
                &object,
                7,
                CoarsePayloadEncoding::TwoBit,
            )
            .unwrap()
        else {
            panic!("ZCL3 grouped range did not decode as two-bit");
        };
        assert_eq!(decoded_codes.to_bytes(), codes.to_bytes());
        assert_eq!(&object[coarse], &codes.to_bytes()[..]);
        assert_eq!(
            deserialize_legacy_cluster(&object[range.full.clone()])
                .unwrap()
                .vectors,
            vectors
        );

        let payload_start = CLUSTER_DATA_OBJECT_HEADER_LEN + CLUSTER_DATA_OBJECT_V4_DIR_ENTRY_LEN;
        let mut inside_directory = object.to_vec();
        inside_directory[12..20].copy_from_slice(&((payload_start - 1) as u64).to_le_bytes());
        assert!(cluster_object_layout(&inside_directory).is_err());

        let mut coarse_overflow = object.to_vec();
        coarse_overflow[12..20].copy_from_slice(&u64::MAX.to_le_bytes());
        coarse_overflow[20..28].copy_from_slice(&1_u64.to_le_bytes());
        assert!(cluster_object_layout(&coarse_overflow).is_err());

        let mut full_overflow = object.to_vec();
        full_overflow[36..44].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(cluster_object_layout(&full_overflow).is_err());
    }

    /// Proves v4 grouped SQ objects expose valid, separable coarse/exact ranges.
    ///
    /// The test parses both a full object and a header-only prefix, checks that
    /// all SQ bytes precede full bytes, and decodes both logical clusters. It
    /// protects the range-GET directory contract as well as row alignment.
    #[test]
    fn test_cluster_data_object_v4_exposes_sq_and_full_ranges() {
        let ids0 = vec!["a".to_string(), "b".to_string()];
        let vecs0 = vec![vec![1.0, 2.0], vec![3.0, 4.0]];
        let codes0 = vec![vec![10, 20], vec![30, 40]];
        let ids1 = vec!["c".to_string()];
        let vecs1 = vec![vec![5.0, 6.0]];
        let codes1 = vec![vec![50, 60]];

        let section0 = serialize_colocated_sq_cluster(&ids0, &vecs0, &codes0, 2).unwrap();
        let section1 = serialize_colocated_sq_cluster(&ids1, &vecs1, &codes1, 2).unwrap();
        let object = serialize_cluster_data_object(&[(0, section0), (1, section1)]).unwrap();

        assert_eq!(&object[0..3], b"ZBP");
        assert_eq!(object[3], CLUSTER_DATA_OBJECT_V4_VERSION);

        let layout = cluster_object_layout(&object).unwrap().unwrap();
        assert_eq!(layout.sections.len(), 2);
        assert!(layout.sections.iter().all(|section| section.sq.is_some()));

        let header_len = cluster_object_header_range_len(2).unwrap();
        let header_layout = cluster_object_layout(&object[..header_len])
            .unwrap()
            .unwrap();
        assert_eq!(header_layout, layout);

        let max_sq_end = layout
            .sections
            .iter()
            .map(|section| section.sq.as_ref().unwrap().end)
            .max()
            .unwrap();
        let min_full_start = layout
            .sections
            .iter()
            .map(|section| section.full.start)
            .min()
            .unwrap();
        assert!(max_sq_end <= min_full_start);

        let sq0 = deserialize_colocated_sq_cluster_from_object(&object, 0)
            .unwrap()
            .unwrap();
        assert_eq!(sq0.ids, ids0);
        assert_eq!(sq0.codes, codes0);

        let sections = cluster_object_sections(&object).unwrap().unwrap();
        let cluster0 = sections[0].decode().unwrap();
        let cluster1 = sections[1].decode().unwrap();
        assert_eq!(cluster0.ids, ids0);
        assert_eq!(cluster0.vectors, vecs0);
        assert_eq!(cluster1.ids, ids1);
        assert_eq!(cluster1.vectors, vecs1);
    }

    /// Proves JSON attribute sidecars preserve present and absent row values.
    ///
    /// This guards the self-describing serde format required by untagged
    /// `AttributeValue` variants and the one-entry-per-vector alignment.
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

    /// Proves a centroid artifact shorter than the legacy header is rejected.
    ///
    /// The failure prevents decoder indexing from treating arbitrary short
    /// storage data as a valid zero-shaped index.
    #[test]
    fn test_centroids_header_too_small() {
        let data = vec![0u8; 4]; // less than 8 bytes
        assert!(deserialize_centroids(&data).is_err());
    }

    /// Proves a cluster artifact shorter than its fixed header is rejected.
    ///
    /// This protects all callers because legacy and current exact-data paths
    /// ultimately pass through the same decoder.
    #[test]
    fn test_cluster_header_too_small() {
        let data = vec![0u8; 4];
        assert!(deserialize_cluster(&data).is_err());
    }

    /// Proves density grouping obeys its cap without absorbing sparse tails.
    ///
    /// Four nearby centroids may share one object, while two distant centroids
    /// must remain singleton groups. This catches both over-grouping and a cap
    /// violation.
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
        assert!(groups.iter().any(|group| group.as_slice() == [0, 1, 2, 3]));
        assert!(groups.iter().any(|group| group.as_slice() == [4]));
        assert!(groups.iter().any(|group| group.as_slice() == [5]));
    }

    /// Proves the legacy centroid decoder rejects a truncated float payload.
    ///
    /// A header declaring two three-dimensional centroids receives only one
    /// row; the error must identify a size mismatch rather than return partial
    /// training metadata.
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

    /// Proves a declared vector row cannot be decoded from partial float bytes.
    ///
    /// Returning the ID with a short vector would violate the index dimension
    /// invariant and could later panic distance calculation.
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

    /// Proves manifest-aware loading prefers a present combined bootstrap.
    ///
    /// Two loads must recover the same centroids and a resident sketch from the
    /// single uploaded object, protecting the current-format path independently
    /// of the optional disk cache.
    #[tokio::test]
    async fn test_load_from_manifest_uses_bootstrap_when_present() {
        let store = ZeppelinStore::new(std::sync::Arc::new(object_store::memory::InMemory::new()));
        let namespace = "ns_bootstrap";
        let segment_id = "seg_bootstrap";
        let centroids = vec![vec![0.0, 0.0], vec![10.0, 10.0]];
        let centroids_data = serialize_centroids(&centroids, 2).unwrap();
        let cluster_vecs = vec![vec![vec![0.1, 0.0]], vec![vec![9.9, 10.1]]];
        let cluster_attrs = vec![vec![None], vec![None]];
        let (sketch_ref, sketch_data, _) = build_resident_sketch(
            namespace,
            segment_id,
            2,
            &centroids,
            &cluster_vecs,
            &cluster_attrs,
        )
        .unwrap();
        let (bootstrap_ref, bootstrap_data) = build_bootstrap_artifact(
            namespace,
            segment_id,
            &centroids_data,
            &sketch_data,
            &BTreeSet::new(),
            &empty_filter_summary_bytes(),
        )
        .unwrap();
        store.put(&bootstrap_ref.key, bootstrap_data).await.unwrap();

        let index = load_ivf_flat_from_manifest(
            &store,
            namespace,
            segment_id,
            2,
            QuantizationType::None,
            Vec::new(),
            Vec::new(),
            Some(sketch_ref.clone()),
            Some(bootstrap_ref.clone()),
            None,
        )
        .await
        .unwrap();

        assert_eq!(index.num_clusters(), 2);
        assert!(index.resident_sketch.is_some());
        assert!(index.filter_summary.is_some());

        let second = load_ivf_flat_from_manifest(
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
        assert_eq!(*index.centroids, *second.centroids);
        assert_eq!(index.dim, second.dim);
        assert!(second.resident_sketch.is_some());
    }

    /// Proves cached bootstrap loads reuse the exact decoded allocations.
    ///
    /// Keeping the temporary directory alive preserves the disk cache during
    /// both loads. `Arc::ptr_eq` catches an accidental second decode that value
    /// equality alone would miss.
    #[tokio::test]
    async fn test_load_from_manifest_reuses_decoded_bootstrap() {
        let store = ZeppelinStore::new(std::sync::Arc::new(object_store::memory::InMemory::new()));
        let namespace = "ns_bootstrap_decoded";
        let segment_id = "seg_bootstrap_decoded";
        let centroids = vec![vec![0.0, 0.0], vec![10.0, 10.0]];
        let centroids_data = serialize_centroids(&centroids, 2).unwrap();
        let cluster_vecs = vec![vec![vec![0.1, 0.0]], vec![vec![9.9, 10.1]]];
        let cluster_attrs = vec![vec![None], vec![None]];
        let (sketch_ref, sketch_data, _) = build_resident_sketch(
            namespace,
            segment_id,
            2,
            &centroids,
            &cluster_vecs,
            &cluster_attrs,
        )
        .unwrap();
        let (bootstrap_ref, bootstrap_data) = build_bootstrap_artifact(
            namespace,
            segment_id,
            &centroids_data,
            &sketch_data,
            &BTreeSet::new(),
            &empty_filter_summary_bytes(),
        )
        .unwrap();
        store.put(&bootstrap_ref.key, bootstrap_data).await.unwrap();

        let cache_dir = tempfile::TempDir::new().unwrap();
        let cache = std::sync::Arc::new(
            crate::cache::DiskCache::new_with_max_bytes(
                cache_dir.path().to_path_buf(),
                100 * 1024 * 1024,
            )
            .unwrap(),
        );

        let first = load_ivf_flat_from_manifest(
            &store,
            namespace,
            segment_id,
            2,
            QuantizationType::None,
            Vec::new(),
            Vec::new(),
            Some(sketch_ref.clone()),
            Some(bootstrap_ref.clone()),
            Some(&cache),
        )
        .await
        .unwrap();
        assert!(cache.has_decoded(&bootstrap_ref.key));

        let second = load_ivf_flat_from_manifest(
            &store,
            namespace,
            segment_id,
            2,
            QuantizationType::None,
            Vec::new(),
            Vec::new(),
            Some(sketch_ref),
            Some(bootstrap_ref),
            Some(&cache),
        )
        .await
        .unwrap();

        assert!(std::sync::Arc::ptr_eq(&first.centroids, &second.centroids));
        assert!(std::sync::Arc::ptr_eq(
            first.resident_sketch.as_ref().unwrap(),
            second.resident_sketch.as_ref().unwrap()
        ));
        assert!(std::sync::Arc::ptr_eq(
            first.filter_summary.as_ref().unwrap(),
            second.filter_summary.as_ref().unwrap()
        ));
    }

    /// Proves old segments without a bootstrap load separate legacy artifacts.
    ///
    /// The handle must recover centroids and the resident sketch while leaving
    /// its bootstrap ref absent, preserving compatibility with published
    /// segments created before combined metadata objects existed.
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
        let (sketch_ref, sketch_data, _) = build_resident_sketch(
            namespace,
            segment_id,
            2,
            &centroids,
            &cluster_vecs,
            &cluster_attrs,
        )
        .unwrap();
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

    /// Codes-only parity: SQ8 and two-bit codes-only payloads round-trip the
    /// same code/factor rows as the existing ID-carrying codecs, with IDs
    /// absent from the new payload bytes.
    #[test]
    fn codes_only_parity_matches_existing_codecs() {
        use crate::index::quantization::rabitq::{StructuredRotation, BLOCK_DIM};
        use crate::index::quantization::rq::{RqClusterCodes, RqClusterCodesOnly};
        use crate::index::quantization::sq::{
            deserialize_sq_cluster, deserialize_sq_codes_only, serialize_sq_cluster,
            serialize_sq_codes_only, SqCalibration,
        };

        // SQ8: decoded code rows equal the existing codec's rows.
        let sq_dim = 8;
        let sq_vectors = [
            vec![0.0_f32; 8],
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
            vec![-3.0, 0.5, 9.25, 2.0, -1.0, 4.5, 0.0, 6.75],
        ];
        let sq_refs: Vec<&[f32]> = sq_vectors.iter().map(Vec::as_slice).collect();
        let calibration = SqCalibration::calibrate(&sq_refs, sq_dim);
        let sq_codes: Vec<Vec<u8>> = sq_refs.iter().map(|v| calibration.encode(v)).collect();
        let sq_ids = vec![
            "sq-row-alpha".to_string(),
            "sq-row-beta".to_string(),
            "sq-row-gamma-longer".to_string(),
        ];

        let existing = serialize_sq_cluster(&sq_ids, &sq_codes, sq_dim).unwrap();
        let existing_decoded = deserialize_sq_cluster(&existing).unwrap();
        let codes_only = serialize_sq_codes_only(&sq_codes, sq_dim).unwrap();
        let decoded = deserialize_sq_codes_only(&codes_only).unwrap();
        assert_eq!(decoded.dim, sq_dim);
        assert_eq!(decoded.codes.len(), sq_ids.len());
        assert_eq!(decoded.codes, existing_decoded.codes);
        for id in &sq_ids {
            assert!(
                !codes_only
                    .windows(id.len())
                    .any(|window| window == id.as_bytes()),
                "SQ8 codes-only payload must not contain ID {id}"
            );
        }

        // Two-bit: decoded planes/factors equal the existing codec's rows.
        let rq_dim = BLOCK_DIM;
        let rotation = StructuredRotation::new(rq_dim, 0x5A50_4352_5354_5431).unwrap();
        let centroid = vec![0.25_f32; rq_dim];
        let rq_rows = [
            vec![0.5_f32; rq_dim],
            vec![-0.125_f32; rq_dim],
            vec![0.0625_f32; rq_dim],
        ];
        let rq_refs: Vec<&[f32]> = rq_rows.iter().map(Vec::as_slice).collect();
        let rq_ids = vec![
            "rq-row-zero".to_string(),
            "rq-row-one".to_string(),
            "rq-row-two-with-a-longer-id".to_string(),
        ];
        let encoded = RqClusterCodes::encode(&rq_ids, &rq_refs, &centroid, &rotation).unwrap();
        let existing_decoded = RqClusterCodes::from_bytes(&encoded.to_bytes()).unwrap();

        let codes_only = encoded.to_codes_only_bytes();
        let decoded = RqClusterCodesOnly::from_bytes(&codes_only).unwrap();
        assert_eq!(decoded.dim(), existing_decoded.dim());
        assert_eq!(decoded.row_count(), existing_decoded.row_count());
        assert_eq!(decoded.packed_planes(), existing_decoded.packed_planes());
        assert_eq!(decoded.factors().len(), existing_decoded.factors().len());
        for (actual, expected) in decoded.factors().iter().zip(existing_decoded.factors()) {
            assert_eq!(
                actual.residual_norm.to_bits(),
                expected.residual_norm.to_bits()
            );
            assert_eq!(
                actual.bar_dot_residual.to_bits(),
                expected.bar_dot_residual.to_bits()
            );
        }
        for id in &rq_ids {
            assert!(
                !codes_only
                    .windows(id.len())
                    .any(|window| window == id.as_bytes()),
                "two-bit codes-only payload must not contain ID {id}"
            );
        }
    }

    /// ZBP5 round trip: two clusters with mixed ID lengths prove directory →
    /// IDs → fixed-stride f32 row alignment and the exact arithmetic ranges of
    /// the first, middle, and last rows.
    #[test]
    fn zbp5_round_trip_two_clusters_mixed_id_lengths() {
        let dim = 3;
        let ids_a = vec![
            "a".to_string(),
            "medium-length-id".to_string(),
            "x".to_string(),
        ];
        let vectors_a = vec![
            vec![1.0, 2.0, 3.0],
            vec![4.0, 5.0, 6.0],
            vec![7.0, 8.0, 9.0],
        ];
        let ids_b = vec!["cluster-b-row-zero".to_string(), "bb".to_string()];
        let vectors_b = vec![vec![-1.5, 0.25, 3.5], vec![9.0, -8.0, 7.0]];

        let id_block_a = serialize_id_block(&ids_a).unwrap();
        let id_block_b = serialize_id_block(&ids_b).unwrap();
        let f32_block_a = serialize_fixed_stride_f32_block(&vectors_a, dim).unwrap();
        let f32_block_b = serialize_fixed_stride_f32_block(&vectors_b, dim).unwrap();

        let entries = [
            Zbp5ClusterBlocks {
                cluster_idx: 7,
                row_count: ids_a.len(),
                dim,
                coarse: b"sq8-codes-a",
                ids: &id_block_a,
                vectors: &f32_block_a,
            },
            Zbp5ClusterBlocks {
                cluster_idx: 12,
                row_count: ids_b.len(),
                dim,
                coarse: b"sq8-codes-b-longer",
                ids: &id_block_b,
                vectors: &f32_block_b,
            },
        ];
        let object = serialize_cluster_data_object_v5(&entries).unwrap();

        // The parser derives exactly the layout the serializer returned.
        let parsed = parse_cluster_data_object_v5(&object.bytes).unwrap();
        assert_eq!(parsed, object.layout);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].cluster_idx, 7);
        assert_eq!(parsed[0].row_count, 3);
        assert_eq!(parsed[1].cluster_idx, 12);
        assert_eq!(parsed[1].row_count, 2);

        // Directory → coarse and ID blocks slice back to the written payloads.
        assert_eq!(
            &object.bytes[parsed[0].coarse.start as usize..parsed[0].coarse.end as usize],
            b"sq8-codes-a"
        );
        assert_eq!(
            deserialize_id_block(
                &object.bytes[parsed[0].ids.start as usize..parsed[0].ids.end as usize]
            )
            .unwrap(),
            ids_a
        );
        assert_eq!(
            deserialize_id_block(
                &object.bytes[parsed[1].ids.start as usize..parsed[1].ids.end as usize]
            )
            .unwrap(),
            ids_b
        );

        // Regions tile in order: coarse blocks, then ID blocks, then f32 blocks.
        assert_eq!(parsed[0].coarse.end, parsed[1].coarse.start);
        assert_eq!(parsed[1].coarse.end, parsed[0].ids.start);
        assert_eq!(parsed[1].ids.end, parsed[0].vectors.start);
        assert_eq!(parsed[0].vectors.end, parsed[1].vectors.start);
        assert_eq!(parsed[1].vectors.end, object.bytes.len() as u64);

        // Exact fixed-stride arithmetic for first, middle, and last rows of
        // both clusters; decoded row bytes match the input vectors.
        let stride = (dim * 4) as u64;
        for (layout, vectors) in [(&parsed[0], &vectors_a), (&parsed[1], &vectors_b)] {
            let row_count = layout.row_count;
            assert_eq!(
                layout.vectors.end - layout.vectors.start,
                fixed_stride_f32_block_len(row_count, dim).unwrap() as u64
            );
            for row in [0, row_count / 2, row_count - 1] {
                let range = layout.vector_row_range(row, dim).unwrap();
                let expected_start = layout.vectors.start + row as u64 * stride;
                assert_eq!(range, expected_start..expected_start + stride);
                let decoded = deserialize_fixed_stride_f32_block(
                    &object.bytes[range.start as usize..range.end as usize],
                    1,
                    dim,
                )
                .unwrap();
                assert_eq!(decoded, vec![vectors[row].clone()]);
            }
        }

        // Whole-block decode also aligns with the input rows.
        let decoded_b = deserialize_fixed_stride_f32_block(
            &object.bytes[parsed[1].vectors.start as usize..parsed[1].vectors.end as usize],
            ids_b.len(),
            dim,
        )
        .unwrap();
        assert_eq!(decoded_b, vectors_b);
    }

    /// Fail-loud bounds: a corrupted range length or row count makes the v5
    /// parser/block decoders reject the object before any partial layout or
    /// vectors are returned.
    #[test]
    fn zbp5_parser_rejects_corrupt_bounds() {
        let dim = 2;
        let ids = vec!["id-zero".to_string(), "id-one".to_string()];
        let vectors = vec![vec![1.0, 2.0], vec![3.0, 4.0]];
        let id_block = serialize_id_block(&ids).unwrap();
        let f32_block = serialize_fixed_stride_f32_block(&vectors, dim).unwrap();
        let entries = [Zbp5ClusterBlocks {
            cluster_idx: 3,
            row_count: ids.len(),
            dim,
            coarse: b"coarse",
            ids: &id_block,
            vectors: &f32_block,
        }];
        let object = serialize_cluster_data_object_v5(&entries).unwrap();
        assert!(parse_cluster_data_object_v5(&object.bytes).is_ok());

        // Corrupt the directory vectors_len: the regions no longer tile to the
        // exact object end.
        let entry_base = CLUSTER_DATA_OBJECT_HEADER_LEN;
        let vectors_len_at = entry_base + 48;
        let mut corrupted = object.bytes.to_vec();
        corrupted[vectors_len_at..vectors_len_at + 8]
            .copy_from_slice(&(f32_block.len() as u64 - 4).to_le_bytes());
        assert!(parse_cluster_data_object_v5(&corrupted).is_err());

        // Corrupt the directory row count: the declared layout no longer
        // matches the fixed-stride block, so row access fails loudly.
        let row_count_at = entry_base + 4;
        let mut corrupted = object.bytes.to_vec();
        corrupted[row_count_at..row_count_at + 4].copy_from_slice(&1_u32.to_le_bytes());
        let layout = parse_cluster_data_object_v5(&corrupted).unwrap();
        assert!(layout[0].vector_row_range(1, dim).is_err());
        assert!(deserialize_fixed_stride_f32_block(
            &object.bytes[layout[0].vectors.start as usize..layout[0].vectors.end as usize],
            layout[0].row_count,
            dim,
        )
        .is_err());

        // Corrupt the ID block row count: the block decoder rejects it.
        let mut corrupted_ids = id_block.to_vec();
        corrupted_ids[0..4].copy_from_slice(&99_u32.to_le_bytes());
        assert!(deserialize_id_block(&corrupted_ids).is_err());

        // Truncate the object: exact-end validation rejects it.
        let truncated = &object.bytes[..object.bytes.len() - 1];
        assert!(parse_cluster_data_object_v5(truncated).is_err());

        // The object stores no dimension, so a reader recovers it by dividing
        // the vector block by the row count. The serializer refuses to write
        // anything that would make that division wrong: a row count the ID
        // block disagrees with, or a vector block off the fixed stride.
        assert!(serialize_cluster_data_object_v5(&[Zbp5ClusterBlocks {
            cluster_idx: 3,
            row_count: ids.len() + 1,
            dim,
            coarse: b"coarse",
            ids: &id_block,
            vectors: &f32_block,
        }])
        .is_err());
        assert!(serialize_cluster_data_object_v5(&[Zbp5ClusterBlocks {
            cluster_idx: 3,
            row_count: ids.len(),
            dim: dim + 1,
            coarse: b"coarse",
            ids: &id_block,
            vectors: &f32_block,
        }])
        .is_err());

        // A v5 object has no coarse/full section directory. Asking for one is
        // an error, never `Ok(None)` — that value means "legacy standalone
        // cluster bytes" and would silently mis-type the object.
        assert!(cluster_object_layout(&object.bytes).is_err());
    }

    /// Two well-separated clusters of small vectors for loader probing tests.
    fn probing_test_vectors() -> Vec<VectorEntry> {
        (0..8)
            .map(|i| {
                let base = if i < 4 { 0.0_f32 } else { 100.0 };
                VectorEntry {
                    id: format!("probe-row-{i}"),
                    values: vec![base + i as f32; 4],
                    attributes: None,
                }
            })
            .collect()
    }

    fn probing_test_config(quantization: QuantizationType) -> IndexingConfig {
        IndexingConfig {
            default_num_centroids: 2,
            kmeans_max_iterations: 10,
            quantization,
            ..IndexingConfig::default()
        }
    }

    /// Proves the probing loader labels a two-bit segment `TwoBit`.
    ///
    /// `load_ivf_flat` has no manifest tag to read, so the label must come
    /// from the persisted coarse blocks. Before this probe existed, a
    /// two-bit segment fell through to `None`: the handle took the flat
    /// scan and lied about its quantization.
    #[tokio::test]
    async fn test_load_ivf_flat_detects_two_bit_encoding() {
        let store = ZeppelinStore::new(std::sync::Arc::new(object_store::memory::InMemory::new()));
        let namespace = "ns_probe_two_bit";
        let segment_id = "seg_probe_two_bit";
        let vectors = probing_test_vectors();
        let config = probing_test_config(QuantizationType::TwoBit);
        build_ivf_flat(&vectors, &config, &store, namespace, segment_id)
            .await
            .unwrap();

        let loaded = load_ivf_flat(&store, namespace, segment_id).await.unwrap();

        assert_eq!(loaded.quantization, QuantizationType::TwoBit);
        assert!(loaded.sq_calibration.is_none());
    }

    /// Proves the coarse-encoding probe leaves the existing Scalar and
    /// unquantized labels unchanged: embedded calibration still decides
    /// Scalar, and legacy payloads carry no coarse evidence.
    #[tokio::test]
    async fn test_load_ivf_flat_keeps_scalar_and_unquantized_labels() {
        let store = ZeppelinStore::new(std::sync::Arc::new(object_store::memory::InMemory::new()));
        let vectors = probing_test_vectors();

        let scalar_config = probing_test_config(QuantizationType::Scalar);
        build_ivf_flat(
            &vectors,
            &scalar_config,
            &store,
            "ns_probe_scalar",
            "seg_probe_scalar",
        )
        .await
        .unwrap();
        let loaded = load_ivf_flat(&store, "ns_probe_scalar", "seg_probe_scalar")
            .await
            .unwrap();
        assert_eq!(loaded.quantization, QuantizationType::Scalar);
        assert!(loaded.sq_calibration.is_some());

        let none_config = probing_test_config(QuantizationType::None);
        build_ivf_flat(
            &vectors,
            &none_config,
            &store,
            "ns_probe_none",
            "seg_probe_none",
        )
        .await
        .unwrap();
        let loaded = load_ivf_flat(&store, "ns_probe_none", "seg_probe_none")
            .await
            .unwrap();
        assert_eq!(loaded.quantization, QuantizationType::None);
    }

    /// Proves a corrupt two-bit coarse block fails the load loudly instead
    /// of being mislabeled or silently scanned flat.
    #[tokio::test]
    async fn test_load_ivf_flat_rejects_corrupt_two_bit_coarse_block() {
        let store = ZeppelinStore::new(std::sync::Arc::new(object_store::memory::InMemory::new()));
        let namespace = "ns_probe_corrupt";
        let segment_id = "seg_probe_corrupt";
        let vectors = probing_test_vectors();
        let config = probing_test_config(QuantizationType::TwoBit);
        let built = build_ivf_flat(&vectors, &config, &store, namespace, segment_id)
            .await
            .unwrap();

        let object_key = built.cluster_objects()[0].key.clone();
        let mut data = store.get(&object_key).await.unwrap().to_vec();
        let layouts = parse_cluster_data_object_v5(&data).unwrap();
        // The two-bit codes-only dimension must be a BLOCK_DIM multiple; a
        // low byte of 3 can never be one.
        let dim_byte = layouts[0].coarse.start as usize + 8;
        data[dim_byte] = 3;
        store.put(&object_key, Bytes::from(data)).await.unwrap();

        match load_ivf_flat(&store, namespace, segment_id).await {
            Ok(_) => panic!("corrupt two-bit coarse block must fail the load loudly"),
            Err(err) => assert!(
                matches!(err, ZeppelinError::Rq(_) | ZeppelinError::Index(_)),
                "expected a typed RQ or index error, got: {err}"
            ),
        }
    }

    /// Probes the object-level detector directly: v5 and v4 objects report
    /// their persisted arm, legacy v1 objects report nothing, and one object
    /// mixing coarse encodings is a loud error.
    #[test]
    fn detect_cluster_object_encoding_reads_persisted_arms() {
        use crate::index::quantization::rabitq::{StructuredRotation, BLOCK_DIM};
        use crate::index::quantization::rq::RqClusterCodes;
        use crate::index::quantization::sq::{serialize_sq_codes_only, SqCalibration};

        let ids = vec!["row-zero".to_string(), "row-one".to_string()];
        let vectors = vec![vec![1.0, 0.0], vec![0.0, 1.0]];
        let dim = 2;

        // v5 SQ8 object.
        let vec_refs: Vec<&[f32]> = vectors.iter().map(Vec::as_slice).collect();
        let calibration = SqCalibration::calibrate(&vec_refs, dim);
        let sq_codes: Vec<Vec<u8>> = vec_refs.iter().map(|v| calibration.encode(v)).collect();
        let sq_coarse = serialize_sq_codes_only(&sq_codes, dim).unwrap();
        let id_block = serialize_id_block(&ids).unwrap();
        let f32_block = serialize_fixed_stride_f32_block(&vectors, dim).unwrap();
        let v5_sq8 = serialize_cluster_data_object_v5(&[Zbp5ClusterBlocks {
            cluster_idx: 0,
            row_count: ids.len(),
            dim,
            coarse: &sq_coarse,
            ids: &id_block,
            vectors: &f32_block,
        }])
        .unwrap();
        assert_eq!(
            detect_cluster_object_encoding(&v5_sq8.bytes).unwrap(),
            Some(CoarsePayloadEncoding::Sq8)
        );

        // v5 two-bit object.
        let rq_dim = BLOCK_DIM;
        let rotation = StructuredRotation::new(rq_dim, 0x5A50_4352_5354_5431).unwrap();
        let rq_rows = [vec![0.5_f32; rq_dim], vec![-0.125_f32; rq_dim]];
        let rq_refs: Vec<&[f32]> = rq_rows.iter().map(Vec::as_slice).collect();
        let rq_codes =
            RqClusterCodes::encode(&ids, &rq_refs, &vec![0.25_f32; rq_dim], &rotation).unwrap();
        let rq_coarse = rq_codes.to_codes_only_bytes();
        let rq_id_block = serialize_id_block(&ids).unwrap();
        let rq_f32_block = serialize_fixed_stride_f32_block(&rq_rows, rq_dim).unwrap();
        let v5_two_bit = serialize_cluster_data_object_v5(&[Zbp5ClusterBlocks {
            cluster_idx: 0,
            row_count: ids.len(),
            dim: rq_dim,
            coarse: &rq_coarse,
            ids: &rq_id_block,
            vectors: &rq_f32_block,
        }])
        .unwrap();
        assert_eq!(
            detect_cluster_object_encoding(&v5_two_bit.bytes).unwrap(),
            Some(CoarsePayloadEncoding::TwoBit)
        );

        // One v5 object mixing the two arms is corruption, not a coin flip.
        let mixed = serialize_cluster_data_object_v5(&[
            Zbp5ClusterBlocks {
                cluster_idx: 0,
                row_count: ids.len(),
                dim,
                coarse: &sq_coarse,
                ids: &id_block,
                vectors: &f32_block,
            },
            Zbp5ClusterBlocks {
                cluster_idx: 1,
                row_count: ids.len(),
                dim: rq_dim,
                coarse: &rq_coarse,
                ids: &rq_id_block,
                vectors: &rq_f32_block,
            },
        ])
        .unwrap();
        match detect_cluster_object_encoding(&mixed.bytes) {
            Err(ZeppelinError::Index(msg)) => {
                assert!(msg.contains("mixes coarse payload encodings"), "got: {msg}")
            }
            other => panic!("mixed coarse encodings must be a loud error, got: {other:?}"),
        }

        // v4 objects: the two-bit coarse child keeps its ZRQ1 signature.
        let zcl3 = serialize_colocated_rq_cluster(&rq_rows, &rq_codes, rq_dim).unwrap();
        let v4_two_bit = serialize_cluster_data_object(&[(0, zcl3)]).unwrap();
        assert_eq!(
            detect_cluster_object_encoding(&v4_two_bit).unwrap(),
            Some(CoarsePayloadEncoding::TwoBit)
        );
        let zcl2 = serialize_colocated_sq_cluster(&ids, &vectors, &sq_codes, dim).unwrap();
        let v4_sq8 = serialize_cluster_data_object(&[(0, zcl2)]).unwrap();
        assert_eq!(
            detect_cluster_object_encoding(&v4_sq8).unwrap(),
            Some(CoarsePayloadEncoding::Sq8)
        );

        // v1 legacy objects hold exact vectors only: no encoding evidence.
        let legacy = serialize_cluster(&ids, &vectors, dim).unwrap();
        let v1 = serialize_cluster_data_object(&[(0, legacy)]).unwrap();
        assert_eq!(detect_cluster_object_encoding(&v1).unwrap(), None);
    }
}
