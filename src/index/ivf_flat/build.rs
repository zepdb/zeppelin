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
use std::sync::{Arc, OnceLock};
use tracing::{debug, info};

use crate::config::IndexingConfig;
use crate::error::{Result, ZeppelinError};
use crate::index::quantization::QuantizationType;
use crate::storage::ZeppelinStore;
use crate::types::{AttributeValue, VectorEntry};
use crate::wal::manifest::{BootstrapRef, ClusterDataObjectRef};

use super::kmeans::train_kmeans;
use super::membership::build_membership_artifact;
use super::sketch::{build_resident_sketch, ResidentSketch};
use super::IvfFlatIndex;
use crate::index::distance;

/// Four-byte signature for current centroid objects.
const CENTROIDS_V2_MAGIC: &[u8; 4] = b"ZCT2";
/// Four-byte signature for one SQ-and-full-vector cluster section.
const CLUSTER_V2_MAGIC: &[u8; 4] = b"ZCL2";
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
/// Four-byte signature for a combined centroid-and-sketch bootstrap object.
const BOOTSTRAP_MAGIC: &[u8; 4] = b"ZBS1";
/// Only bootstrap format version currently accepted by the decoder.
const BOOTSTRAP_VERSION: u32 = 1;
/// Number of offset/length entries in a bootstrap header.
const BOOTSTRAP_SECTION_COUNT: usize = 2;
/// Fixed bootstrap header size before the first embedded artifact.
const BOOTSTRAP_HEADER_LEN: usize = 4 + 4 + BOOTSTRAP_SECTION_COUNT * 16;
/// Object-count compromise used when no grouping cap is configured.
const DEFAULT_MAX_CLUSTERS_PER_OBJECT: usize = 3;
/// Environment variable overriding the maximum clusters in a grouped object.
const MAX_CLUSTERS_PER_OBJECT_ENV: &str = "ZEPPELIN_MAX_CLUSTERS_PER_OBJECT";
/// Presence-only switch that emits grouping diagnostics to standard error.
const CLUSTER_GROUP_STATS_ENV: &str = "ZEPPELIN_CLUSTER_GROUP_STATS";

/// Process-wide reuse of validated bootstrap metadata, keyed by immutable key.
///
/// Entries are safe to reuse because segment keys identify write-once objects.
/// The manifest-provided sizes are still compared before a cached value is
/// accepted, so incompatible metadata fails loudly.
static BOOTSTRAP_DECODED_CACHE: OnceLock<DashMap<String, Arc<DecodedBootstrap>>> = OnceLock::new();

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
fn bootstrap_decoded_cache() -> &'static DashMap<String, Arc<DecodedBootstrap>> {
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
/// an out-of-range or duplicate cluster, a cluster listed in two objects, or a
/// logical cluster missing from the layout. No object-store I/O occurs.
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

/// Borrowed, validated views into a combined segment bootstrap artifact.
///
/// The views avoid copying the embedded centroid and sketch bytes. Their
/// lifetime cannot exceed the input buffer passed to `deserialize_bootstrap`.
pub(crate) struct BootstrapSections<'a> {
    /// Complete encoded centroid artifact, including its own version header.
    pub centroids: &'a [u8],
    /// Complete encoded resident-sketch artifact, including its own header.
    pub sketch: &'a [u8],
}

/// Serialize a segment bootstrap artifact from existing artifact bytes.
///
/// The centroid and sketch payloads are embedded verbatim. Their internal
/// formats remain independently versioned by their existing decoders.
///
/// # Parameters
///
/// - `centroids`: Complete encoded centroid artifact to place first.
/// - `sketch`: Complete encoded resident-sketch artifact to place second.
///
/// # Returns
///
/// One owned immutable buffer with a versioned offset/length directory and the
/// two payloads in that order.
///
/// # Errors
///
/// Returns an index error when either payload is empty or size arithmetic
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
) -> Result<(BootstrapRef, Bytes)> {
    let bytes = serialize_bootstrap(centroids, sketch)?;
    let bootstrap_ref = BootstrapRef {
        key: bootstrap_key(namespace, segment_id),
        size_bytes: bytes.len() as u64,
    };
    Ok((bootstrap_ref, bytes))
}

/// Validates a bootstrap object and borrows its two embedded artifact sections.
///
/// Validation requires the current magic/version, exact contiguous section
/// ordering, non-empty payloads, in-bounds checked ranges, and no trailing
/// bytes. It does not decode the centroid or sketch formats themselves.
///
/// # Parameters
///
/// - `data`: Complete bootstrap-object bytes loaded from storage or cache.
///
/// # Returns
///
/// Borrowed centroid and sketch slices tied to `data`'s lifetime.
///
/// # Errors
///
/// Returns an index error for a short header, wrong magic, unsupported version,
/// malformed integer, empty/overlapping/out-of-bounds section, overflow, or an
/// exact-size mismatch. No partially validated sections are returned.
///
/// # Examples
///
/// Bytes emitted by `serialize_bootstrap` yield the exact two original
/// payloads. Changing the sketch length to extend past the object returns an
/// error before any decoder sees the slice.
///
/// # Rust Notes for Java/C Engineers
///
/// `BootstrapSections<'_>` is a zero-copy view. It resembles Java
/// `ByteBuffer.slice()` or C pointer/length pairs, but the borrow checker proves
/// the views cannot survive after `data` is released.
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
/// A v4 grouped object when every section is `ZCL2`; otherwise a v1 object that
/// keeps each section contiguous.
///
/// # Errors
///
/// Returns an index error for no entries, a duplicate/oversized cluster index,
/// malformed `ZCL2` sections, or any checked size/offset overflow.
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

    if entries
        .iter()
        .all(|(_, bytes)| bytes.starts_with(CLUSTER_V2_MAGIC))
    {
        return serialize_cluster_data_object_v4(entries);
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
/// pulling the usually larger exact-vector block. Each input `ZCL2` section is
/// parsed and its child payloads are copied into the corresponding block.
///
/// # Parameters
///
/// - `entries`: Prevalidated cluster indexes paired with valid `ZCL2` sections.
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
    /// Borrowed child payloads extracted from one input `ZCL2` section.
    struct SplitSection<'a> {
        /// Logical cluster named in the grouped directory.
        cluster_idx: usize,
        /// Compact scalar-quantized child artifact.
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
                sq: sections.sq,
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

/// Decodes exact vectors from either a legacy or `ZCL2` cluster section.
///
/// # Parameters
///
/// - `data`: One complete cluster section, not an entire grouped object.
///
/// # Returns
///
/// Owned IDs and full-precision vectors. SQ bytes in a `ZCL2` section are
/// ignored by this exact-data path.
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
    let data = cluster_section_from_object(data, cluster_idx)?;
    deserialize_cluster(data)
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
    if !data.starts_with(CLUSTER_V2_MAGIC) {
        return Ok(None);
    }

    let sections = colocated_cluster_sections(data)?;
    let sq_cluster = crate::index::quantization::sq::deserialize_sq_cluster(sections.sq)?;
    Ok(Some(sq_cluster))
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
    if is_cluster_data_object_v4(data) {
        let layout = cluster_object_layout_v4(data)?;
        let section = layout.section(cluster_idx).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "cluster {cluster_idx} missing from v4 cluster data object"
            ))
        })?;
        let sq = section.sq.as_ref().ok_or_else(|| {
            ZeppelinError::Index(format!(
                "cluster {cluster_idx} missing SQ section in v4 cluster data object"
            ))
        })?;
        validate_range_in_object(sq, data.len(), "v4 cluster SQ section")?;
        let sq_cluster = crate::index::quantization::sq::deserialize_sq_cluster(&data[sq.clone()])?;
        return Ok(Some(sq_cluster));
    }

    let data = cluster_section_from_object(data, cluster_idx)?;
    deserialize_colocated_sq_cluster(data)
}

/// Borrowed child payloads from one validated `ZCL2` cluster section.
struct ColocatedClusterSections<'a> {
    /// Scalar-quantized cluster artifact bytes.
    sq: &'a [u8],
    /// Legacy-format exact-vector cluster artifact bytes.
    full: &'a [u8],
}

/// Borrowed full-vector payload for one entry in a grouped object.
pub(crate) struct ClusterObjectSection<'a> {
    /// Logical IVF cluster represented by this range.
    pub cluster_idx: usize,
    /// Full-vector child section borrowed from the grouped object.
    pub data: &'a [u8],
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
/// `Some` parsed v1/v4 layout or `None` when the bytes do not carry a supported
/// grouped-object signature. `None` identifies a legacy standalone cluster,
/// not a malformed recognized grouped object.
///
/// # Errors
///
/// Returns an index error for a recognized but truncated/malformed directory,
/// duplicate cluster indexes, invalid relationships, or size overflow.
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
    Ok(None)
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
            data: &data[section.full],
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
fn validate_range_in_object(range: &Range<usize>, object_len: usize, label: &str) -> Result<()> {
    if range.start > range.end || range.end > object_len {
        return Err(ZeppelinError::Index(format!(
            "{label} out of bounds: start={}, end={}, len={object_len}",
            range.start, range.end
        )));
    }
    Ok(())
}

/// Borrows one full-vector child section from standalone or grouped bytes.
///
/// # Parameters
///
/// - `data`: Complete standalone cluster section or grouped object.
/// - `cluster_idx`: Logical cluster to select when grouped.
///
/// # Returns
///
/// A slice tied to `data`. Legacy standalone bytes are returned whole because
/// their key already identifies the cluster.
///
/// # Errors
///
/// Returns an index error for malformed grouped metadata/ranges or a missing
/// requested cluster.
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

/// Selects the exact-vector child from a co-located or legacy cluster section.
///
/// # Parameters
///
/// - `data`: One complete child cluster section.
///
/// # Returns
///
/// The `ZCL2` full-vector slice, or all input bytes for legacy full-only data.
///
/// # Errors
///
/// Returns an index error when a recognized `ZCL2` header has invalid offsets
/// or size.
fn full_cluster_section(data: &[u8]) -> Result<&[u8]> {
    if !data.starts_with(CLUSTER_V2_MAGIC) {
        return Ok(data);
    }
    Ok(colocated_cluster_sections(data)?.full)
}

/// Validates `ZCL2` offsets and borrows its SQ and exact-vector children.
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
/// Returns an index error for a short header, malformed integer, unexpected SQ
/// start, non-contiguous full start, arithmetic overflow, or exact-size
/// mismatch.
///
/// # Examples
///
/// A valid section laid out as `header | SQ | full` returns the two payloads.
/// Appending a byte is rejected because immutable artifact lengths are exact.
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
///
/// # Panics
///
/// Slicing panics if `offset + 8` exceeds `data.len()`. Callers first validate
/// the fixed header/directory length that contains each requested field.
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
        let size_bytes = data.len() as u64;
        cluster_objects.push(ClusterDataObjectRef {
            key: key.clone(),
            clusters: group,
            live_offset: 0,
            live_len: 0,
            size_bytes,
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

    // --- Step 6: Write segment bootstrap artifact ---
    let (bootstrap_ref, bootstrap_data) =
        build_bootstrap_artifact(namespace, segment_id, &centroids_data, &sketch_data)?;
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
        centroids: Arc::new(centroids),
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
    let metadata = match bootstrap_ref.as_ref() {
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
            let sq_calibration = centroids_data
                .sq_calibration
                .as_ref()
                .map(|bytes| crate::index::quantization::sq::SqCalibration::from_bytes(bytes))
                .transpose()?;
            let resident_sketch =
                load_resident_sketch(store, namespace, sketch_ref.as_ref(), cache).await?;
            LoadedIndexMetadata {
                centroids: Arc::new(centroids_data.centroids),
                dim: centroids_data.dim,
                sq_calibration,
                resident_sketch,
            }
        }
    };
    let cluster_object_by_cluster =
        build_cluster_object_lookup(metadata.centroids.len(), &cluster_objects)?;

    info!(
        namespace = namespace,
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
        namespace: namespace.to_string(),
        segment_id: segment_id.to_string(),
        quantization,
        sq_calibration: metadata.sq_calibration,
        bitmap_fields: Vec::new(), // Populated from SegmentRef at search time
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
async fn load_bootstrap_artifacts(
    store: &ZeppelinStore,
    namespace: &str,
    bootstrap_ref: &BootstrapRef,
    sketch_ref: Option<&crate::wal::manifest::SketchRef>,
    cache: Option<&std::sync::Arc<crate::cache::DiskCache>>,
) -> Result<LoadedIndexMetadata> {
    let Some(sketch_ref) = sketch_ref else {
        return Err(ZeppelinError::Index(format!(
            "bootstrap {} present but segment is missing sketch ref",
            bootstrap_ref.key
        )));
    };

    if let Some(c) = cache {
        if let Some(decoded) = c.get_decoded::<DecodedBootstrap>(&bootstrap_ref.key)? {
            pin_bootstrap_metadata(c, namespace, &bootstrap_ref.key).await;
            return metadata_from_decoded_bootstrap(
                &bootstrap_ref.key,
                decoded,
                bootstrap_ref,
                sketch_ref,
            );
        }
    }
    if let Some(c) = cache {
        // Process-wide decoded reuse is only for disk-cache-backed query paths;
        // cache-less callers are cold by construction and fetch S3 bytes.
        if let Some(decoded) = bootstrap_decoded_cache()
            .get(&bootstrap_ref.key)
            .map(|entry| Arc::clone(entry.value()))
        {
            c.insert_decoded(&bootstrap_ref.key, Arc::clone(&decoded));
            pin_bootstrap_metadata(c, namespace, &bootstrap_ref.key).await;
            return metadata_from_decoded_bootstrap(
                &bootstrap_ref.key,
                decoded,
                bootstrap_ref,
                sketch_ref,
            );
        }
    }

    let data = match cache {
        Some(c) => {
            let data = c
                .get_or_fetch(&bootstrap_ref.key, || store.get(&bootstrap_ref.key))
                .await?;
            pin_bootstrap_metadata(c, namespace, &bootstrap_ref.key).await;
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
    let sq_calibration = centroids_data
        .sq_calibration
        .as_ref()
        .map(|bytes| crate::index::quantization::sq::SqCalibration::from_bytes(bytes))
        .transpose()?;
    let centroids = Arc::new(centroids_data.centroids);
    let sketch = Arc::new(ResidentSketch::from_bytes(sections.sketch)?);

    let decoded = Arc::new(DecodedBootstrap {
        bootstrap_size_bytes: bootstrap_ref.size_bytes,
        sketch_size_bytes: sketch_ref.size_bytes,
        centroids: Arc::clone(&centroids),
        dim: centroids_data.dim,
        sq_calibration: sq_calibration.clone(),
        resident_sketch: Arc::clone(&sketch),
    });
    bootstrap_decoded_cache().insert(bootstrap_ref.key.clone(), Arc::clone(&decoded));
    if let Some(c) = cache {
        c.insert_decoded(&bootstrap_ref.key, decoded);
    }

    Ok(LoadedIndexMetadata {
        centroids,
        dim: centroids_data.dim,
        sq_calibration,
        resident_sketch: Some(sketch),
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
    Ok(LoadedIndexMetadata {
        centroids: Arc::clone(&decoded.centroids),
        dim: decoded.dim,
        sq_calibration: decoded.sq_calibration.clone(),
        resident_sketch: Some(Arc::clone(&decoded.resident_sketch)),
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
    namespace: &str,
    bootstrap_key: &str,
) {
    cache.unpin_scoped(&format!("{namespace}:centroids")).await;
    cache
        .unpin_scoped(&format!("{namespace}:coarse_sketch"))
        .await;
    cache
        .pin_scoped(&format!("{namespace}:bootstrap"), bootstrap_key)
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
async fn load_resident_sketch(
    store: &ZeppelinStore,
    namespace: &str,
    sketch_ref: Option<&crate::wal::manifest::SketchRef>,
    cache: Option<&std::sync::Arc<crate::cache::DiskCache>>,
) -> Result<Option<Arc<ResidentSketch>>> {
    let Some(sketch_ref) = sketch_ref else {
        return Ok(None);
    };

    if let Some(c) = cache {
        if let Some(sketch) = c.get_decoded::<ResidentSketch>(&sketch_ref.key)? {
            c.pin_scoped(&format!("{namespace}:coarse_sketch"), &sketch_ref.key)
                .await;
            return Ok(Some(sketch));
        }
    }

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
    let sketch = Arc::new(ResidentSketch::from_bytes(&data)?);
    if sketch_ref.size_bytes != data.len() as u64 {
        return Err(ZeppelinError::Index(format!(
            "coarse sketch size mismatch for {}: manifest={}, object={}",
            sketch_ref.key,
            sketch_ref.size_bytes,
            data.len()
        )));
    }
    if let Some(c) = cache {
        c.insert_decoded(&sketch_ref.key, Arc::clone(&sketch));
    }
    Ok(Some(sketch))
}

/// Reconstructs an IVF-Flat handle by listing and probing segment artifacts.
///
/// This compatibility loader is used by compaction and tests when a
/// [`SegmentRef`][crate::wal::manifest::SegmentRef] is not supplied. It loads
/// centroids, lists grouped keys, reads every cluster to count rows, and probes
/// quantization sidecars. Normal query planning should use
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
/// Quantization probes are intentionally heuristic: any PQ probe error is
/// treated as "not PQ," and any legacy SQ probe error as "not SQ."
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
        cluster_objects.push(ClusterDataObjectRef {
            key,
            clusters,
            live_offset: 0,
            live_len: 0,
            size_bytes: data.len() as u64,
        });
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
        centroids: Arc::new(centroids),
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
        let data = serialize_bootstrap(centroids, sketch).unwrap();
        let sections = deserialize_bootstrap(&data).unwrap();
        assert_eq!(sections.centroids, centroids);
        assert_eq!(sections.sketch, sketch);
    }

    /// Proves malformed bootstrap identity, version, and bounds fail loudly.
    ///
    /// Accepting any case would allow corrupt remote bytes to reach a child
    /// decoder or permit an out-of-bounds slice.
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
        let cluster0 = deserialize_cluster(sections[0].data).unwrap();
        let cluster1 = deserialize_cluster(sections[1].data).unwrap();
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
            Some(sketch_ref.clone()),
            Some(bootstrap_ref.clone()),
            None,
        )
        .await
        .unwrap();

        assert_eq!(index.num_clusters(), 2);
        assert!(index.resident_sketch.is_some());

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
        let (sketch_ref, sketch_data, _) =
            build_resident_sketch(namespace, segment_id, 2, &cluster_vecs, &cluster_attrs).unwrap();
        let (bootstrap_ref, bootstrap_data) =
            build_bootstrap_artifact(namespace, segment_id, &centroids_data, &sketch_data).unwrap();
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
