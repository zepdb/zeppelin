//! Executes the object-store read path for one immutable IVF-Flat segment.
//!
//! An [`IvfFlatIndex`] keeps centroids and small bootstrap metadata in memory,
//! while the vector rows, attributes, bitmap indexes, and quantized codes remain
//! in immutable segment objects. [`search_ivf_flat`] first chooses the nearest
//! centroid clusters (`nprobe`), may narrow that set with a resident coarse
//! sketch, and then uses the segment's [`QuantizationType`] to choose one of
//! three scan paths. The caller in the query layer merges these segment results
//! with newer WAL results; this file neither reads the manifest nor scans the
//! WAL and never publishes or mutates authoritative data.
//!
//! `nprobe` is the number of centroid regions considered. Raising it normally
//! improves recall—the chance that the true nearest rows are examined—but adds
//! object-store bytes and distance calculations. A resident sketch can reduce
//! physical reads within that probe set for unfiltered queries. Grouped object
//! metadata can make one GET cover several logical clusters, so the number of
//! scanned clusters and the number of object requests are deliberately distinct.
//!
//! ```text
//! query + loaded segment handle
//!              |
//!              v
//! rank all centroids; retain min(nprobe, cluster count)
//!              |
//!              v
//! optional resident-sketch selection (unfiltered queries only)
//!              |
//!       +------+------+----------------+
//!       |             |                |
//!       v             v                v
//!  full precision    SQ8              PQ
//!  fetch + score     approximate      approximate
//!  every row         coarse score     coarse score
//!       |             |                |
//!       |             +--------+-------+
//!       |                      v
//!       |             exact full-vector rerank
//!       +----------------------+
//!                              v
//! exact filter/order, optional attribute enrichment, top-k
//! ```
//!
//! Bitmap filtering is an optimization, not a separate source of truth. If a
//! bitmap object is absent, unreadable, or cannot express a filter, quantized
//! scans fetch row attributes and evaluate the same filter exactly before
//! approximate truncation. Immutable object keys make cached bytes reusable;
//! object storage remains the source of truth, and malformed cached full objects
//! are evicted rather than accepted.
//!
//! ## Reading map
//!
//! 1. Start with [`search_ivf_flat`] for validation, centroid probing, scan-path
//!    dispatch, final filtering, and result shaping.
//! 2. Read `select_scan_clusters` and the grouped-object scoring helpers for the
//!    resident-sketch read budget.
//! 3. Compare `scan_clusters_flat`, `scan_clusters_sq`, and `scan_clusters_pq`.
//! 4. Follow `load_sq_object_for_coarse` through the range-read and rerank
//!    helpers for current grouped SQ objects and legacy compatibility.
//! 5. Finish with `try_bitmap_prefilter`, `coarse_row_passes`, and
//!    `enrich_unfiltered_results` for metadata semantics.
//!
//! ## Invariants
//!
//! - Lower [`SearchResult::score`] values rank first for every supported metric.
//! - Logical row positions keep IDs, vectors, attributes, bitmap positions, and
//!   quantized codes aligned; a mismatch is an index error, never skipped data.
//! - Quantized scores may choose rerank candidates, but returned distances are
//!   always recomputed from full-precision vectors.
//! - A filter is applied before quantized candidate truncation and again at the
//!   final boundary, so selective filters cannot be discarded as coarse noise.
//! - Manifest-provided grouped-object membership must match the object's decoded
//!   directory before range offsets are trusted.
//! - This read path creates no artifacts and changes no manifest visibility.
//!
//! ## Rust concepts used here
//!
//! Borrowed slices such as `&[f32]` let all phases inspect one caller-owned
//! query without copying it. `Arc<DiskCache>` is shared ownership similar to a
//! Java reference-counted service handle; unlike a raw C pointer, Rust proves
//! the cache remains alive while async reads use it. `join_all` and
//! `tokio::join!` poll independent I/O concurrently without spawning detached
//! tasks or sharing mutable candidate buffers. After those futures finish, the
//! code performs CPU scoring sequentially on owned `Vec` and `HashMap` values.
//! `Bytes::slice` creates a reference-counted view into immutable bytes rather
//! than allocating or copying the selected range.

use dashmap::DashMap;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tracing::{debug, error};

use crate::cache::DiskCache;
use crate::error::{Result, ZeppelinError};
use crate::index::distance::compute_distance;
use crate::index::filter::{evaluate_filter, oversampled_k};
use crate::index::quantization::QuantizationType;
use crate::index::topk::partial_topk_by;
use crate::storage::ZeppelinStore;
use crate::types::{AttributeValue, DistanceMetric, Filter, SearchResult};

use super::build::{
    attrs_key, cluster_key, cluster_object_header_range_len, cluster_object_layout,
    cluster_object_sections, deserialize_attrs, deserialize_cluster_from_object,
    deserialize_colocated_sq_cluster_from_object, ClusterObjectLayout, ClusterObjectRange,
};
use super::sketch::{AdaptiveClusterBudget, ClusterScore};
use super::IvfFlatIndex;

use crate::index::bitmap::evaluate::evaluate_filter_bitmap;
use crate::index::bitmap::{bitmap_key, ClusterBitmapIndex};

/// Row-aligned optional attribute maps for one logical cluster.
///
/// Position `i` belongs to the same row as vector and ID position `i`; `None`
/// means that row stored no attributes. The outer vector is owned after decode.
type ClusterAttrs = Vec<Option<HashMap<String, AttributeValue>>>;

/// Minimum size of the historical smooth cluster budget before adaptation.
///
/// Keeping this floor at six avoids overly aggressive sketch pruning for small
/// probe sets; the final cap still never exceeds the effective `nprobe`.
const SKETCH_BASE_MIN_CLUSTERS: usize = 6;
/// Linear coefficient in the historical sketch cluster-budget curve.
const SKETCH_CLUSTER_LINEAR_FRACTION: f32 = 0.3125;
/// Denominator controlling the curve's quadratic growth at large `nprobe`.
const SKETCH_CLUSTER_QUADRATIC_SCALE: f32 = 150.0;
/// Minimum sketch-ranked cluster core for an adaptively pruned query.
const SKETCH_ADAPTIVE_FLOOR_CLUSTERS: usize = 4;
/// Maximum multiple of the historical cluster budget available to hard queries.
const SKETCH_ADAPTIVE_MAX_MULTIPLIER: usize = 2;
/// Relative approximate-distance margin for admitting extra sketch clusters.
const SKETCH_ADAPTIVE_RELATIVE_SCORE_MARGIN: f32 = 0.13;
/// Minimum physical grouped objects fetched by the normal adaptive policy.
const SKETCH_ADAPTIVE_FLOOR_OBJECTS: usize = 3;
/// Larger object floor used when objects contain at most a few clusters.
const SKETCH_ADAPTIVE_THIN_OBJECT_FLOOR: usize = 5;
/// Maximum clusters per object still classified as a thin grouped layout.
const SKETCH_THIN_OBJECT_MAX_ARITY: usize = 3;
/// Tighter relative-distance margin used for thin grouped objects.
const SKETCH_ADAPTIVE_THIN_RELATIVE_SCORE_MARGIN: f32 = 0.12;
/// Extra physical objects allowed beyond the arity-scaled cluster budget.
const SKETCH_ADAPTIVE_OBJECT_CAP_EXTRA: usize = 2;
/// Environment variable enabling one-line sketch object-scan diagnostics.
const SKETCH_SCAN_STATS_ENV: &str = "ZEPPELIN_SKETCH_SCAN_STATS";
/// Environment variable enabling query-local SQ byte diagnostics.
const SQ_BYTE_STATS_ENV: &str = "ZEPPELIN_SQ_BYTE_STATS";

/// One exact-distance segment candidate before final filtering and projection.
///
/// The cluster and row coordinates are retained so attributes can be loaded
/// only for unfiltered winners. `score` is always a full-precision distance at
/// this stage, even when SQ or PQ chose the row during coarse ranking.
struct Candidate {
    /// Owned vector identifier returned to the caller if this candidate wins.
    id: String,
    /// Exact lower-is-better distance from the query to the stored vector.
    score: f32,
    /// Decoded row attributes when filtering already required them.
    attributes: Option<HashMap<String, AttributeValue>>,
    /// Logical cluster containing this row.
    cluster_idx: usize,
    /// Zero-based position inside that cluster's aligned row arrays.
    row_idx: usize,
}

/// Orders exact candidates by ascending distance and then identifier.
///
/// # Parameters
///
/// - `a`: First borrowed candidate.
/// - `b`: Second borrowed candidate.
///
/// # Returns
///
/// A total ordering suitable for sorting or partial top-k selection. Ties are
/// deterministic by ID, and IEEE-754 special values are ordered by `total_cmp`.
///
/// # Examples
///
/// A candidate at distance `0.2` precedes one at `0.5`; equal-distance IDs
/// `item-a` and `item-b` retain the lexical order `item-a`, then `item-b`.
fn candidate_distance_cmp(a: &Candidate, b: &Candidate) -> CmpOrdering {
    a.score.total_cmp(&b.score).then_with(|| a.id.cmp(&b.id))
}

/// Orders SQ coarse tuples by approximate distance and then identifier.
///
/// # Parameters
///
/// - `a`: `(id, approximate_score, cluster, row)` tuple to compare.
/// - `b`: Other tuple in the same representation.
///
/// # Returns
///
/// A deterministic lower-score-first ordering; cluster and row are payload,
/// not tie breakers.
///
/// # Examples
///
/// Two SQ rows with equal approximate scores are ordered by ID before exact
/// reranking, which makes truncation reproducible.
fn coarse_sq_candidate_cmp(
    a: &(String, f32, usize, usize),
    b: &(String, f32, usize, usize),
) -> CmpOrdering {
    a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0))
}

/// Orders PQ coarse tuples by approximate distance and then identifier.
///
/// # Parameters
///
/// - `a`: `(id, approximate_score, cluster)` tuple to compare.
/// - `b`: Other tuple in the same representation.
///
/// # Returns
///
/// A deterministic lower-score-first ordering used by partial top-k selection.
///
/// # Examples
///
/// If two product-quantized codes score `4.0`, their IDs decide which appears
/// first in the rerank window.
fn coarse_pq_candidate_cmp(a: &(String, f32, usize), b: &(String, f32, usize)) -> CmpOrdering {
    a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0))
}

#[derive(Debug, Clone)]
/// Manifest-resolved physical object covering one or more logical clusters.
///
/// Cloning this value allocates clones of the key and cluster vector. It does
/// not clone object bytes or perform I/O.
struct ClusterFetchObject {
    /// Immutable object-store key.
    key: String,
    /// Logical cluster indexes whose full sections live in the object.
    clusters: Vec<usize>,
    /// Optional self-contained prefix containing live full-vector data.
    live_range: Option<std::ops::Range<usize>>,
    /// Manifest-declared complete object length, or zero for legacy metadata.
    size_bytes: u64,
}

/// Outcome of consulting the local full-object cache for a range request.
enum RangeCacheLookup {
    /// A validated local full object can satisfy the requested range.
    Local(bytes::Bytes),
    /// No usable full object was present; the caller should read object storage.
    Miss,
    /// A wrong-length cached object was evicted before the caller reads S3.
    CorruptEvicted,
}

/// Process-wide decoded directory cache keyed by immutable cluster-object key.
///
/// [`OnceLock`] initializes the concurrent map on first use. Each [`Arc`] shares
/// an immutable decoded layout across queries without copying its section list.
static CLUSTER_OBJECT_LAYOUT_CACHE: OnceLock<DashMap<String, Arc<ClusterObjectLayout>>> =
    OnceLock::new();
/// Monotonic process-local identifier used only to correlate SQ diagnostics.
static SQ_BYTE_STATS_QUERY_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy)]
/// Logical I/O phase to which an SQ query attributes a physical GET.
enum SqBytePhase {
    /// Quantized coarse-scoring bytes.
    Sq,
    /// Full-precision exact-rerank bytes.
    Rerank,
    /// Calibration, attributes, or other supporting bytes.
    Other,
}

/// Lock-free, query-local counters emitted when SQ byte diagnostics are enabled.
///
/// Independent async fetches share this value through [`Arc`]. Relaxed atomics
/// are sufficient because the counters collect statistics only; they do not
/// synchronize correctness state.
struct SqSearchByteStats {
    /// Process-local diagnostic query identifier.
    query_id: u64,
    /// Physical GET count during coarse SQ reads.
    sq_gets: AtomicU64,
    /// Physical bytes returned by coarse SQ reads.
    sq_bytes: AtomicU64,
    /// Payload bytes actually needed from those coarse reads.
    sq_logical_bytes: AtomicU64,
    /// Physical GET count during exact rerank.
    rerank_gets: AtomicU64,
    /// Physical bytes returned during exact rerank.
    rerank_bytes: AtomicU64,
    /// Exact vector payload bytes requested by rerank.
    rerank_logical_bytes: AtomicU64,
    /// Physical supporting-object GET count.
    other_gets: AtomicU64,
    /// Physical supporting-object bytes.
    other_bytes: AtomicU64,
    /// Bytes served from a complete locally cached object.
    local_bytes: AtomicU64,
    /// Logical clusters selected for SQ scanning.
    selected_clusters: AtomicU64,
    /// Distinct physical objects containing those clusters.
    sq_objects: AtomicU64,
    /// Coarse candidates retained after approximate selection.
    coarse_candidates: AtomicU64,
    /// Rows requested for exact rerank.
    rerank_candidates: AtomicU64,
    /// Logical clusters containing rerank rows.
    rerank_clusters: AtomicU64,
    /// Physical objects containing rerank rows.
    rerank_objects: AtomicU64,
    /// Results ultimately returned to the segment caller.
    final_results: AtomicU64,
}

impl SqSearchByteStats {
    /// Creates diagnostics only for an active SQ path with the environment flag.
    ///
    /// # Parameters
    ///
    /// - `enabled`: Whether the selected quantization mode is scalar.
    ///
    /// # Returns
    ///
    /// A shared zeroed counter set when both `enabled` and
    /// `ZEPPELIN_SQ_BYTE_STATS` are present; otherwise `None` with no allocation.
    ///
    /// # Examples
    ///
    /// An unquantized query passes `false` and never emits SQ statistics even if
    /// the environment flag is set. An SQ query with the flag receives a unique
    /// process-local diagnostic ID.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `Option<Arc<Self>>` encodes disabled versus enabled diagnostics without a
    /// null pointer. Cloning the returned `Arc` increments a reference count; it
    /// does not duplicate the atomics or create a second set of counters.
    fn new_if_enabled(enabled: bool) -> Option<Arc<Self>> {
        if !enabled || std::env::var_os(SQ_BYTE_STATS_ENV).is_none() {
            return None;
        }
        Some(Arc::new(Self {
            query_id: SQ_BYTE_STATS_QUERY_ID.fetch_add(1, Ordering::Relaxed),
            sq_gets: AtomicU64::new(0),
            sq_bytes: AtomicU64::new(0),
            sq_logical_bytes: AtomicU64::new(0),
            rerank_gets: AtomicU64::new(0),
            rerank_bytes: AtomicU64::new(0),
            rerank_logical_bytes: AtomicU64::new(0),
            other_gets: AtomicU64::new(0),
            other_bytes: AtomicU64::new(0),
            local_bytes: AtomicU64::new(0),
            selected_clusters: AtomicU64::new(0),
            sq_objects: AtomicU64::new(0),
            coarse_candidates: AtomicU64::new(0),
            rerank_candidates: AtomicU64::new(0),
            rerank_clusters: AtomicU64::new(0),
            rerank_objects: AtomicU64::new(0),
            final_results: AtomicU64::new(0),
        }))
    }

    /// Adds one physical GET and its returned byte count to a phase.
    ///
    /// # Parameters
    ///
    /// - `phase`: Query phase charged for the read.
    /// - `bytes`: Physical bytes returned by the store.
    ///
    /// # Side Effects
    ///
    /// Mutates diagnostic atomics only; it does not issue I/O.
    ///
    /// # Examples
    ///
    /// Reading a 4 KiB SQ range records one SQ GET and 4096 physical bytes.
    fn record_get(&self, phase: SqBytePhase, bytes: usize) {
        self.record_gets(phase, 1, bytes);
    }

    /// Adds an arbitrary number of physical reads and bytes to one phase.
    ///
    /// # Parameters
    ///
    /// - `phase`: Coarse, rerank, or supporting-I/O bucket.
    /// - `gets`: Number of completed object-store requests.
    /// - `bytes`: Sum of bytes returned by those requests.
    ///
    /// # Side Effects
    ///
    /// Uses relaxed atomic additions so concurrently polled range reads can
    /// contribute without a mutex.
    ///
    /// # Examples
    ///
    /// Three coalesced rerank ranges totaling 12 KiB add `(3, 12288)` to the
    /// rerank counters.
    fn record_gets(&self, phase: SqBytePhase, gets: usize, bytes: usize) {
        let gets = gets as u64;
        let bytes = bytes as u64;
        match phase {
            SqBytePhase::Sq => {
                self.sq_gets.fetch_add(gets, Ordering::Relaxed);
                self.sq_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            SqBytePhase::Rerank => {
                self.rerank_gets.fetch_add(gets, Ordering::Relaxed);
                self.rerank_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            SqBytePhase::Other => {
                self.other_gets.fetch_add(gets, Ordering::Relaxed);
                self.other_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
        }
    }

    /// Records useful SQ payload bytes independently of range slack.
    ///
    /// # Parameters
    ///
    /// - `bytes`: Bytes occupied by requested SQ sections.
    ///
    /// # Examples
    ///
    /// A 6 KiB physical span containing 5 KiB of selected code sections records
    /// 5 KiB here; `emit` reports the remaining 1 KiB as slack.
    fn record_logical_sq_bytes(&self, bytes: usize) {
        self.sq_logical_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Records useful full-vector payload bytes independently of coalesced gaps.
    ///
    /// # Parameters
    ///
    /// - `bytes`: Sum of exact vector byte widths requested by rerank.
    ///
    /// # Examples
    ///
    /// Reranking ten 128-dimensional vectors records `10 * 128 * 4` logical
    /// bytes even if fewer, larger physical ranges include gaps between rows.
    fn record_logical_rerank_bytes(&self, bytes: usize) {
        self.rerank_logical_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Records bytes read from a validated complete local object.
    ///
    /// # Parameters
    ///
    /// - `bytes`: Size of the cached object consulted.
    ///
    /// # Examples
    ///
    /// Slicing one vector from a 1 MiB cached object records 1 MiB of local
    /// bytes because the full cache value was read.
    fn record_local_bytes(&self, bytes: usize) {
        self.local_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Stores a query cardinality in an atomic diagnostic field.
    ///
    /// # Parameters
    ///
    /// - `field`: Counter to replace.
    /// - `value`: Current cluster, object, candidate, or result count.
    ///
    /// # Examples
    ///
    /// After selecting eight clusters, the caller stores `8` in
    /// `selected_clusters` before concurrent fetches begin.
    fn set_usize(field: &AtomicU64, value: usize) {
        field.store(value as u64, Ordering::Relaxed);
    }

    /// Emits one complete SQ byte-accounting line to standard error.
    ///
    /// # Parameters
    ///
    /// - `effective_nprobe`: Probe count after clamping to existing centroids.
    /// - `fetch_k`: Candidate target after filter oversampling.
    ///
    /// # Side Effects
    ///
    /// Writes a diagnostic line to standard error. It does not affect query
    /// results, metrics, cache contents, or object-store state.
    ///
    /// # Examples
    ///
    /// A query may report eight selected clusters but four physical SQ objects,
    /// making grouping efficiency and range slack visible in one line.
    fn emit(&self, effective_nprobe: usize, fetch_k: usize) {
        let sq_gets = self.sq_gets.load(Ordering::Relaxed);
        let sq_bytes = self.sq_bytes.load(Ordering::Relaxed);
        let sq_logical_bytes = self.sq_logical_bytes.load(Ordering::Relaxed);
        let rerank_gets = self.rerank_gets.load(Ordering::Relaxed);
        let rerank_bytes = self.rerank_bytes.load(Ordering::Relaxed);
        let rerank_logical_bytes = self.rerank_logical_bytes.load(Ordering::Relaxed);
        let other_gets = self.other_gets.load(Ordering::Relaxed);
        let other_bytes = self.other_bytes.load(Ordering::Relaxed);
        let local_bytes = self.local_bytes.load(Ordering::Relaxed);
        let total_gets = sq_gets + rerank_gets + other_gets;
        let total_bytes = sq_bytes + rerank_bytes + other_bytes;
        eprintln!(
            "zeppelin_sq_byte_stats query={} nprobe={effective_nprobe} fetch_k={fetch_k} \
selected_clusters={} sq_objects={} coarse_candidates={} rerank_candidates={} rerank_clusters={} \
rerank_objects={} final_results={} sq_gets={sq_gets} sq_bytes={sq_bytes} \
sq_logical_bytes={sq_logical_bytes} sq_slack_bytes={} rerank_gets={rerank_gets} \
rerank_bytes={rerank_bytes} rerank_logical_bytes={rerank_logical_bytes} rerank_slack_bytes={} \
other_gets={other_gets} other_bytes={other_bytes} local_bytes={local_bytes} \
total_gets={total_gets} total_bytes={total_bytes}",
            self.query_id,
            self.selected_clusters.load(Ordering::Relaxed),
            self.sq_objects.load(Ordering::Relaxed),
            self.coarse_candidates.load(Ordering::Relaxed),
            self.rerank_candidates.load(Ordering::Relaxed),
            self.rerank_clusters.load(Ordering::Relaxed),
            self.rerank_objects.load(Ordering::Relaxed),
            self.final_results.load(Ordering::Relaxed),
            sq_bytes.saturating_sub(sq_logical_bytes),
            rerank_bytes.saturating_sub(rerank_logical_bytes),
        );
    }
}

/// Returns the lazily initialized process-wide decoded-layout cache.
///
/// # Returns
///
/// A `'static` concurrent map. Values are immutable and shared through [`Arc`].
///
/// # Examples
///
/// Two queries for the same grouped object can reuse one decoded directory even
/// when they use different per-query disk-cache handles.
///
/// # Rust Notes for Java/C Engineers
///
/// [`OnceLock`] is comparable to a Java initialization-on-demand holder or
/// `pthread_once`, while [`DashMap`] provides sharded concurrent access. The
/// returned reference is valid for the process lifetime; callers do not free it.
fn cluster_object_layout_cache() -> &'static DashMap<String, Arc<ClusterObjectLayout>> {
    CLUSTER_OBJECT_LAYOUT_CACHE.get_or_init(DashMap::new)
}

/// Loads one complete immutable object through the optional cache.
///
/// # Parameters
///
/// - `cache`: Optional shared local disk cache. A miss is delegated to `store`.
/// - `store`: Zeppelin's object-store abstraction and source of truth.
/// - `key`: Complete immutable object key.
///
/// # Returns
///
/// Shared immutable bytes from cache or object storage.
///
/// # Errors
///
/// Propagates cache and object-store failures. This helper does not replace a
/// missing or unreadable authoritative object with empty bytes.
///
/// # Side Effects
///
/// A miss may perform one GET and populate the cache. The cache may coalesce
/// concurrent misses for the same key.
///
/// # Consistency
///
/// Segment objects are immutable, so a cached value for the exact key is safe
/// to reuse. The helper never changes manifest visibility.
///
/// # Performance
///
/// A hit avoids network I/O. A miss downloads the complete object rather than a
/// range.
///
/// # Examples
///
/// Sixteen concurrent cold queries for one cluster can share the cache's single
/// in-flight GET and then receive cheap [`bytes::Bytes`] handles.
async fn fetch_with_cache(
    cache: Option<&Arc<DiskCache>>,
    store: &ZeppelinStore,
    key: &str,
) -> Result<bytes::Bytes> {
    if let Some(c) = cache {
        c.get_or_fetch(key, || store.get(key)).await
    } else {
        store.get(key).await
    }
}

/// Loads one complete object while attributing physical bytes to an SQ phase.
///
/// # Parameters
///
/// - `cache`: Optional shared disk cache.
/// - `store`: Authoritative object-store reader used after a miss.
/// - `key`: Immutable object key.
/// - `stats`: Optional query-local counters.
/// - `phase`: Bucket charged when a network GET occurs.
///
/// # Returns
///
/// Shared immutable complete-object bytes.
///
/// # Errors
///
/// Propagates cache or object-store failures. Failed reads are not counted as
/// successful GET bytes.
///
/// # Side Effects
///
/// May fill the cache and update diagnostic atomics. A cache hit records local
/// bytes; only the closure that actually fetches S3 records a physical GET.
///
/// # Performance
///
/// The explicit `get` makes local-byte accounting possible. On a race after a
/// miss, `get_or_fetch` still single-flights the authoritative read.
///
/// # Examples
///
/// Loading an SQ calibration sidecar after a cold miss records one `Other` GET;
/// the next query's cache hit records local bytes instead.
async fn fetch_with_cache_counted(
    cache: Option<&Arc<DiskCache>>,
    store: &ZeppelinStore,
    key: &str,
    stats: Option<&SqSearchByteStats>,
    phase: SqBytePhase,
) -> Result<bytes::Bytes> {
    if let Some(c) = cache {
        if let Some(data) = c.get(key).await {
            if let Some(stats) = stats {
                stats.record_local_bytes(data.len());
            }
            return Ok(data);
        }
        c.get_or_fetch(key, || async {
            let data = store.get(key).await?;
            if let Some(stats) = stats {
                stats.record_get(phase, data.len());
            }
            Ok(data)
        })
        .await
    } else {
        let data = store.get(key).await?;
        if let Some(stats) = stats {
            stats.record_get(phase, data.len());
        }
        Ok(data)
    }
}

/// Checks whether a validated local full object can satisfy a range request.
///
/// # Parameters
///
/// - `cache`: Optional complete-object cache.
/// - `key`: Immutable object key.
/// - `size_bytes`: Manifest-declared full length, or zero for legacy metadata
///   that cannot validate exact length.
/// - `needed_end`: Exclusive absolute end offset the cached value must contain.
/// - `phase`: Metrics label such as `flat`, `sq`, `rerank`, or `header`.
/// - `range_count`: Logical physical-range count credited to the selected source.
///
/// # Returns
///
/// [`RangeCacheLookup::Local`] with the complete cached value when usable,
/// [`RangeCacheLookup::Miss`] when S3 should be consulted, or
/// [`RangeCacheLookup::CorruptEvicted`] after removing a wrong-length value.
///
/// # Errors
///
/// Returns an error if cache lookup or invalidation fails. A corrupt value is
/// never returned to the parser.
///
/// # Side Effects
///
/// May evict the cache key, log a corruption error, and increment range-source
/// metrics. It performs no object-store request itself.
///
/// # Consistency
///
/// When a manifest size is available, exact length validation protects range
/// offsets from a truncated or unrelated cache value. Object storage remains
/// the recovery source after eviction.
///
/// # Examples
///
/// A manifest says an object is 64 KiB but the cache contains 40 KiB. This
/// helper evicts it and returns `CorruptEvicted`; the caller then reads S3.
async fn cached_full_object_for_range(
    cache: Option<&Arc<DiskCache>>,
    key: &str,
    size_bytes: u64,
    needed_end: usize,
    phase: &'static str,
    range_count: u64,
) -> Result<RangeCacheLookup> {
    let Some(c) = cache else {
        return Ok(RangeCacheLookup::Miss);
    };
    let Some(data) = c.get(key).await else {
        return Ok(RangeCacheLookup::Miss);
    };

    if size_bytes == 0 {
        if data.len() >= needed_end {
            crate::metrics::RANGE_SOURCE_TOTAL
                .with_label_values(&[phase, "local"])
                .inc_by(range_count);
            return Ok(RangeCacheLookup::Local(data));
        }
        return Ok(RangeCacheLookup::Miss);
    }

    let actual = data.len() as u64;
    if actual != size_bytes {
        c.invalidate(key).await?;
        error!(
            key,
            expected = size_bytes,
            actual,
            "cached object length mismatch; evicting"
        );
        crate::metrics::RANGE_SOURCE_TOTAL
            .with_label_values(&[phase, "s3_after_corrupt_evict"])
            .inc_by(range_count);
        return Ok(RangeCacheLookup::CorruptEvicted);
    }

    if data.len() >= needed_end {
        crate::metrics::RANGE_SOURCE_TOTAL
            .with_label_values(&[phase, "local"])
            .inc_by(range_count);
        return Ok(RangeCacheLookup::Local(data));
    }

    Ok(RangeCacheLookup::Miss)
}

/// Resolves one logical cluster to its physical immutable data object.
///
/// # Parameters
///
/// - `index`: Loaded segment metadata, including carried-over owners and any
///   manifest-defined grouped objects.
/// - `cluster_idx`: Logical centroid/cluster index.
///
/// # Returns
///
/// An owned fetch descriptor. Legacy layouts synthesize a one-cluster key;
/// grouped layouts clone the manifest key, membership, size, and live range.
///
/// # Errors
///
/// Returns an index error for an invalid manifest lookup or live range. A live
/// range beginning after byte zero is rejected because flat parsing expects a
/// self-contained object prefix.
///
/// # Consistency
///
/// Per-cluster keys always use [`IvfFlatIndex::cluster_owner`], preserving
/// incremental-compaction ownership. Manifest object membership is not guessed.
///
/// # Examples
///
/// Logical clusters 4 and 5 may both resolve to
/// `segments/seg-9/cluster_group_2.bin`; a legacy cluster 4 resolves to its own
/// key under the segment that owns that carried-over cluster.
fn cluster_fetch_object(index: &IvfFlatIndex, cluster_idx: usize) -> Result<ClusterFetchObject> {
    if let Some(object_ref) = index.cluster_object(cluster_idx)? {
        let live_range = object_ref.live_range()?;
        if live_range.as_ref().is_some_and(|range| range.start != 0) {
            return Err(ZeppelinError::Index(format!(
                "cluster object {} advertised nonzero live offset; flat-scan range must be self-contained",
                object_ref.key
            )));
        }
        return Ok(ClusterFetchObject {
            key: object_ref.key.clone(),
            clusters: object_ref.clusters.clone(),
            live_range,
            size_bytes: object_ref.size_bytes,
        });
    }

    Ok(ClusterFetchObject {
        key: cluster_key(
            &index.namespace,
            index.cluster_owner(cluster_idx),
            cluster_idx,
        ),
        clusters: vec![cluster_idx],
        live_range: None,
        size_bytes: 0,
    })
}

/// Fetches bytes needed to flat-scan one physical cluster object.
///
/// # Parameters
///
/// - `cache`: Optional full-object cache.
/// - `store`: Authoritative object-store reader.
/// - `object`: Resolved object descriptor.
/// - `use_live_range`: Whether an advertised self-contained live prefix may be
///   read instead of the complete object.
///
/// # Returns
///
/// Bytes parseable as the complete legacy/grouped object view expected by the
/// flat scanner. A cached full object may be sliced to its live prefix.
///
/// # Errors
///
/// Propagates cache validation and S3 full/range GET failures.
///
/// # Side Effects
///
/// May issue one range GET or one complete-object GET, populate the cache on the
/// full path, and increment range-source metrics.
///
/// # Performance
///
/// A live-range read avoids trailing dead bytes for sketch-selected,
/// unfiltered flat scans. Filtered queries and unsupported layouts read the
/// complete object.
///
/// # Examples
///
/// If a 20 MiB immutable object advertises a 12 MiB live prefix, an unfiltered
/// sketch query can request `0..12 MiB`; a valid cached 20 MiB object supplies
/// the same bytes without S3.
async fn fetch_cluster_object_for_flat_scan(
    cache: Option<&Arc<DiskCache>>,
    store: &ZeppelinStore,
    object: &ClusterFetchObject,
    use_live_range: bool,
) -> Result<bytes::Bytes> {
    if use_live_range {
        if let Some(range) = object.live_range.clone() {
            // A locally cached full object supersedes the ranged fetch; the
            // live span is a prefix, so slicing preserves parse semantics.
            match cached_full_object_for_range(
                cache,
                &object.key,
                object.size_bytes,
                range.end,
                "flat",
                1,
            )
            .await?
            {
                RangeCacheLookup::Local(data) => return Ok(data.slice(range)),
                RangeCacheLookup::Miss => {
                    crate::metrics::RANGE_SOURCE_TOTAL
                        .with_label_values(&["flat", "s3"])
                        .inc();
                }
                RangeCacheLookup::CorruptEvicted => {}
            }
            return store.get_range(&object.key, range).await;
        }
    }
    fetch_with_cache(cache, store, &object.key).await
}

/// Deduplicates physical objects for a list of logical clusters.
///
/// # Parameters
///
/// - `index`: Loaded segment layout.
/// - `clusters`: Logical cluster indexes in desired traversal order.
///
/// # Returns
///
/// Owned object descriptors ordered by the first cluster that references each
/// key. Multiple clusters in one grouped object produce one descriptor.
///
/// # Errors
///
/// Propagates invalid manifest lookups or live-range metadata.
///
/// # Examples
///
/// Clusters `[0, 1, 3]` where 0 and 1 share object A and 3 uses object B return
/// `[A, B]`, predicting two object reads rather than three.
fn cluster_fetch_objects(
    index: &IvfFlatIndex,
    clusters: &[usize],
) -> Result<Vec<ClusterFetchObject>> {
    let mut objects = Vec::new();
    let mut seen_keys = HashSet::new();
    for &cluster_idx in clusters {
        let object = cluster_fetch_object(index, cluster_idx)?;
        if seen_keys.insert(object.key.clone()) {
            objects.push(object);
        }
    }
    Ok(objects)
}

/// Expands selected logical clusters to every cluster in each touched object.
///
/// # Parameters
///
/// - `index`: Loaded segment layout.
/// - `clusters`: Initially selected logical clusters.
///
/// # Returns
///
/// Deduplicated logical indexes in physical-object order. This ensures a
/// fetched grouped object is parsed consistently as a whole.
///
/// # Errors
///
/// Propagates malformed object mappings.
///
/// # Examples
///
/// Selecting cluster 0 from an object containing `[0, 1]` expands the scan set
/// to `[0, 1]`; no second GET is needed to include cluster 1.
fn expand_clusters_to_objects(index: &IvfFlatIndex, clusters: &[usize]) -> Result<Vec<usize>> {
    let mut expanded = Vec::new();
    let mut seen_clusters = HashSet::new();
    for object in cluster_fetch_objects(index, clusters)? {
        for cluster_idx in object.clusters {
            if seen_clusters.insert(cluster_idx) {
                expanded.push(cluster_idx);
            }
        }
    }
    Ok(expanded)
}

/// Optionally emits sketch scan cardinalities for one query.
///
/// # Parameters
///
/// - `effective_nprobe`: Probe count after centroid-count clamping.
/// - `objects`: Distinct physical objects selected.
/// - `clusters`: Logical clusters covered by those objects.
/// - `grouped`: Whether manifest-defined grouped layout was active.
///
/// # Side Effects
///
/// Writes one line to standard error only when
/// `ZEPPELIN_SKETCH_SCAN_STATS` is present.
///
/// # Examples
///
/// `nprobe=16`, `objects=5`, and `clusters=12` makes the sketch and grouping
/// reduction visible without changing the result path.
fn emit_scan_stats(effective_nprobe: usize, objects: usize, clusters: usize, grouped: bool) {
    if std::env::var_os(SKETCH_SCAN_STATS_ENV).is_some() {
        eprintln!(
            "zeppelin_scan_stats nprobe={effective_nprobe} object_gets={objects} clusters_covered={clusters} grouped={grouped}"
        );
    }
}

/// Searches one loaded immutable IVF-Flat segment and returns exact-distance winners.
///
/// Centroids choose at most `nprobe` nearby logical clusters. An optional
/// resident sketch may reduce the unfiltered physical scan, after which the
/// selected quantization strategy either scores full vectors directly or uses
/// compact codes to choose a larger exact-rerank frontier. Filtering and result
/// attribute projection are kept separate: filtered queries load attributes for
/// correctness, while unfiltered queries load them only for final winners when
/// requested.
///
/// # Parameters
///
/// - `index`: Loaded segment handle containing trusted centroid, object-layout,
///   quantization, and optional resident-sketch metadata.
/// - `query`: Borrowed full-precision query. Its length must equal `index.dim`.
/// - `top_k`: Maximum results requested. Zero returns an empty result after
///   dimension validation.
/// - `nprobe`: Maximum nearest centroid clusters considered. Values above the
///   cluster count are clamped; zero examines no cluster.
/// - `filter`: Optional metadata predicate. A matching row must have suitable
///   attributes unless the filter's exact semantics allow a missing field.
/// - `distance_metric`: Metric used for centroid ranking and exact row scores.
/// - `store`: Zeppelin's object-store abstraction for immutable segment reads.
/// - `oversample_factor`: Multiplier for the candidate target when a filter is
///   present; factor zero still preserves at least `top_k`.
/// - `cache`: Optional shared disk cache for immutable objects and layouts.
/// - `include_attributes`: Whether returned results retain attribute maps.
/// - `rerank_coalesce_gap_bytes`: Exclusive maximum gap that SQ exact-vector
///   ranges may bridge to reduce request count. A gap equal to this value stays
///   separate.
///
/// # Returns
///
/// Up to `top_k` owned [`SearchResult`] values ordered by ascending exact
/// distance and deterministic ID tie breaking. Fewer results mean the segment
/// has fewer surviving candidates; an empty vector is valid for zero probes,
/// zero `top_k`, an empty segment, or a filter with no matches.
///
/// # Errors
///
/// Returns [`ZeppelinError::DimensionMismatch`] when the query width differs
/// from the index. Returns storage, cache, or index errors for missing immutable
/// objects, failed GETs, malformed quantization/calibration/layout/cluster data,
/// manifest-to-object layout disagreement, invalid range arithmetic, or row
/// metadata missing during enrichment. Completed reads and cache fills may have
/// occurred before a later parallel phase fails; this read-only operation never
/// publishes partial segment state.
///
/// # Panics
///
/// The loaded index must preserve its build-time invariants: every centroid and
/// decoded full vector has `index.dim` components, and aligned ID/code/vector
/// arrays have matching row counts. Violating those internal invariants can
/// panic in distance or indexed row access. In debug builds, an extremely large
/// filtered `top_k` can also panic when the quantized rerank factor multiplies
/// the saturated fetch target by four.
///
/// # Side Effects
///
/// Performs cache lookups and object-store full/range GETs, may populate or
/// invalidate local cache entries, updates range-source metrics, and writes
/// tracing or opt-in diagnostic events. It does not upload, delete, or mutate
/// any segment, WAL, or manifest object.
///
/// # Consistency
///
/// The caller constructs `index` from an authoritative manifest snapshot.
/// This function reads only objects named by that snapshot or deterministic
/// legacy keys. Cached segment objects are safe because the keys are immutable;
/// cache state cannot add a cluster or change visibility. Exact full vectors,
/// not SQ/PQ/sketch scores, determine returned scores.
///
/// # Performance
///
/// Centroid ranking costs `O(cluster_count * dim)` CPU and a full centroid sort.
/// The remaining cost depends on `nprobe`, resident-sketch selection, grouping,
/// filter metadata, and quantization. Independent object reads are polled in
/// parallel. Flat scan computes one exact distance per visited row. SQ/PQ scan
/// scores all visited compact codes, retains at most roughly `4 * fetch_k`, and
/// rereads only those full vectors when current layout metadata permits ranges.
/// Request coalescing trades extra gap bytes for fewer range GETs.
///
/// # Examples
///
/// ```text
/// loaded segment: 64 clusters, grouped two per object, SQ8 enabled
/// request: top_k=10, nprobe=8, filter=color == "red", oversample=3
///
/// 1. rank 64 resident centroids and choose 8 clusters
/// 2. keep all touched grouped clusters because filters bypass sketch pruning
/// 3. load SQ codes plus bitmap/attributes and discard non-red rows
/// 4. retain an approximate frontier for fetch_k=30, then exact-rerank it
/// 5. return at most 10 red rows ordered by full-vector distance
/// ```
///
/// With `include_attributes=false`, the IDs and distances are unchanged, but
/// unfiltered winners avoid the final attribute-object reads.
///
/// # Rust Notes for Java/C Engineers
///
/// The function borrows `index`, `query`, `store`, `filter`, and `cache`; it
/// cannot free or retain those values beyond the returned future's lifetime.
/// This resembles Java object references or C `const` pointers, but Rust also
/// proves non-null validity and prevents mutation through these shared borrows.
/// The `match` on [`QuantizationType`] is exhaustive, so adding a strategy forces
/// this dispatch to be updated. `?` returns the first error while RAII drops all
/// partially built vectors and maps; it is ordinary typed control flow, not a
/// Java exception or C `goto cleanup` path.
#[allow(clippy::too_many_arguments)]
pub async fn search_ivf_flat(
    index: &IvfFlatIndex,
    query: &[f32],
    top_k: usize,
    nprobe: usize,
    filter: Option<&Filter>,
    distance_metric: DistanceMetric,
    store: &ZeppelinStore,
    oversample_factor: usize,
    cache: Option<&Arc<DiskCache>>,
    include_attributes: bool,
    rerank_coalesce_gap_bytes: usize,
) -> Result<Vec<SearchResult>> {
    // Validate query dimension.
    if query.len() != index.dim {
        return Err(ZeppelinError::DimensionMismatch {
            expected: index.dim,
            actual: query.len(),
        });
    }

    if top_k == 0 {
        return Ok(Vec::new());
    }

    let num_clusters = index.centroids.len();
    let effective_nprobe = nprobe.min(num_clusters);

    // --- Step 1: Rank centroids by distance to query ---
    let mut centroid_dists: Vec<(usize, f32)> = index
        .centroids
        .iter()
        .enumerate()
        .map(|(i, c)| (i, compute_distance(query, c, distance_metric)))
        .collect();

    // Sort ascending (lower distance = closer).
    centroid_dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    let probe_clusters: Vec<usize> = centroid_dists
        .iter()
        .take(effective_nprobe)
        .map(|(idx, _)| *idx)
        .collect();
    // --- Step 2: Determine fetch size (oversample if filtering) ---
    let fetch_k = if filter.is_some() {
        oversampled_k(top_k, oversample_factor)
    } else {
        top_k
    };
    let sq_byte_stats =
        SqSearchByteStats::new_if_enabled(matches!(index.quantization, QuantizationType::Scalar));

    let scan_clusters = select_scan_clusters(
        index,
        query,
        distance_metric,
        filter,
        &probe_clusters,
        effective_nprobe,
        fetch_k,
    )?;

    debug!(
        nprobe = effective_nprobe,
        probe_clusters = ?probe_clusters,
        scan_clusters = ?scan_clusters,
        "probing clusters"
    );

    // --- Step 3: Scan selected clusters ---
    // The resident sketch is only allowed to decide which cluster objects to
    // fetch. Scalar-quantized indexes still score every vector in those
    // selected clusters with SQ8 before exact rerank.
    let candidates = match index.quantization {
        QuantizationType::Scalar => {
            scan_clusters_sq(
                index,
                &scan_clusters,
                query,
                distance_metric,
                filter,
                fetch_k,
                store,
                cache,
                sq_byte_stats.clone(),
                rerank_coalesce_gap_bytes,
            )
            .await?
        }
        QuantizationType::Product => {
            scan_clusters_pq(
                index,
                &scan_clusters,
                query,
                distance_metric,
                filter,
                fetch_k,
                store,
                cache,
            )
            .await?
        }
        QuantizationType::None => {
            scan_clusters_flat(
                index,
                &scan_clusters,
                query,
                distance_metric,
                filter,
                store,
                cache,
                index.resident_sketch.is_some() && filter.is_none(),
            )
            .await?
        }
    };

    debug!(
        total_candidates = candidates.len(),
        fetch_k = fetch_k,
        "scanned clusters"
    );

    // --- Step 4: Retain candidates by distance ---
    let mut sorted = candidates;
    if filter.is_some() {
        sorted.sort_by(candidate_distance_cmp);
    } else {
        partial_topk_by(&mut sorted, top_k, candidate_distance_cmp);
    }

    // --- Step 5: Apply post-filter if present ---
    let results: Vec<SearchResult> = if let Some(f) = filter {
        sorted
            .into_iter()
            .filter(|c| {
                match &c.attributes {
                    Some(attrs) => evaluate_filter(f, attrs),
                    None => false, // No attributes means filter cannot match.
                }
            })
            .take(top_k)
            .map(|c| SearchResult {
                id: c.id,
                score: c.score,
                attributes: if include_attributes {
                    c.attributes
                } else {
                    None
                },
            })
            .collect()
    } else {
        let top_candidates: Vec<Candidate> = sorted.into_iter().take(top_k).collect();
        if include_attributes {
            enrich_unfiltered_results(
                index,
                top_candidates,
                store,
                cache,
                sq_byte_stats.as_deref(),
            )
            .await?
        } else {
            top_candidates
                .into_iter()
                .map(|candidate| SearchResult {
                    id: candidate.id,
                    score: candidate.score,
                    attributes: None,
                })
                .collect()
        }
    };

    debug!(returned = results.len(), top_k = top_k, "search complete");
    if let Some(stats) = &sq_byte_stats {
        SqSearchByteStats::set_usize(&stats.final_results, results.len());
        stats.emit(effective_nprobe, fetch_k);
    }

    Ok(results)
}

/// Chooses logical clusters whose physical objects proceed to vector scanning.
///
/// The centroid probe set decides which physical objects are eligible to be
/// touched. Without a resident sketch—or whenever a metadata filter is
/// present—the function keeps every touched object and expands grouped
/// membership, so a physical sibling outside the original centroid set may also
/// be scanned. Unfiltered queries may use resident PQ evidence to spend fewer
/// object reads. Grouped layouts select whole objects rather than pretending a
/// partial object saves a GET.
///
/// # Parameters
///
/// - `index`: Loaded segment and optional resident sketch.
/// - `query`: Full-precision query already checked against `index.dim`.
/// - `distance_metric`: Metric used to score resident sketch codes.
/// - `filter`: Present when exact attribute semantics must preserve the full
///   centroid probe set.
/// - `probe_clusters`: Nearest centroid indexes in centroid-distance order.
/// - `effective_nprobe`: Probe count after clamping; used to derive budgets.
/// - `retrieval_top_k`: Candidate-window size used to estimate sketch mass.
///
/// # Returns
///
/// Logical cluster indexes to scan. The result can include siblings from a
/// grouped object that were not individually in `probe_clusters`, because the
/// physical GET already covers them.
///
/// # Errors
///
/// Returns index errors for malformed object mappings, invalid sketch inputs or
/// budgets, and empty grouped-object selections. No object-store I/O occurs.
///
/// # Performance
///
/// The no-sketch/filter path performs metadata lookups only. Sketch paths build
/// ADC tables and scan resident compact codes for the probed clusters, trading
/// CPU for fewer downstream object reads.
///
/// # Examples
///
/// If centroid probing selects clusters `[2, 7, 8, 9]` and the sketch finds
/// most approximate top-row mass in 7 and 8, an ungrouped segment may scan
/// `[7, 8]`. If 7 shares an object with 6, the grouped result includes both 7
/// and 6 because fetching only 7 would not save bytes or requests.
fn select_scan_clusters(
    index: &IvfFlatIndex,
    query: &[f32],
    distance_metric: DistanceMetric,
    filter: Option<&Filter>,
    probe_clusters: &[usize],
    effective_nprobe: usize,
    retrieval_top_k: usize,
) -> Result<Vec<usize>> {
    let Some(sketch) = &index.resident_sketch else {
        return expand_clusters_to_objects(index, probe_clusters);
    };

    // Attribute filters require exact per-row attrs during coarse pruning.
    // The current resident sketch intentionally stores no attribute values, so
    // filtered queries keep the legacy cluster set and preserve existing
    // semantics. Unfiltered benchmark/query traffic uses the sketch path.
    if filter.is_some() {
        return expand_clusters_to_objects(index, probe_clusters);
    }

    let cluster_budget = adaptive_sketch_budget(effective_nprobe);
    if cluster_budget.max_clusters() >= probe_clusters.len() {
        let clusters = expand_clusters_to_objects(index, probe_clusters)?;
        let objects = cluster_fetch_objects(index, probe_clusters)?.len();
        emit_scan_stats(
            effective_nprobe,
            objects,
            clusters.len(),
            !index.cluster_objects.is_empty(),
        );
        return Ok(clusters);
    }

    if !index.cluster_objects.is_empty() {
        let ranked_clusters =
            sketch.rank_clusters(query, distance_metric, probe_clusters, retrieval_top_k)?;
        let selected = select_grouped_object_clusters(index, &ranked_clusters, effective_nprobe)?;
        emit_scan_stats(
            effective_nprobe,
            selected.object_count,
            selected.clusters.len(),
            true,
        );
        return Ok(selected.clusters);
    }

    sketch.select_clusters(
        query,
        distance_metric,
        probe_clusters,
        cluster_budget,
        retrieval_top_k,
    )
}

/// Object-aware sketch selection returned to the scan dispatcher.
struct SelectedObjectClusters {
    /// Logical clusters covered by selected whole physical objects.
    clusters: Vec<usize>,
    /// Distinct physical objects represented by `clusters`.
    object_count: usize,
}

#[derive(Debug, Clone, Copy)]
/// Adaptive physical-object limits derived from grouping arity and query shape.
///
/// The value is `Copy`: passing it duplicates three scalar fields and allocates
/// nothing, similar to a small C struct passed by value.
struct AdaptiveObjectBudget {
    /// Minimum selected objects before distance margin may stop expansion.
    floor_objects: usize,
    /// Hard upper bound on selected physical objects.
    max_objects: usize,
    /// Fractional distance slack relative to the best sketch score.
    relative_score_margin: f32,
}

/// Selects whole grouped objects from resident-sketch cluster rankings.
///
/// Selection first establishes a close-distance core, then favors objects that
/// cover more global approximate top-row mass while remaining within a
/// query-relative score cutoff. Every chosen object's complete manifest cluster
/// membership enters the scan set.
///
/// # Parameters
///
/// - `index`: Segment with manifest-defined grouped-object mappings.
/// - `ranked_clusters`: Non-empty sketch scores ordered best first.
/// - `effective_nprobe`: Clamped centroid probe count used to derive read caps.
///
/// # Returns
///
/// Selected logical clusters and the corresponding physical object count.
/// Cluster order follows object selection and each object's manifest membership.
///
/// # Errors
///
/// Returns an index error when rankings are empty, mappings are invalid, or the
/// policy cannot produce any cluster. It fails loudly rather than reverting to
/// an unbounded scan.
///
/// # Performance
///
/// Builds cluster-score/rank maps and repeatedly scores unselected candidate
/// objects. With `o` objects and budget `b`, selection is approximately
/// `O(o * b * average_clusters_per_object)` and performs no I/O.
///
/// # Examples
///
/// Suppose object A contains two close clusters with moderate mass and object B
/// contains four slightly farther clusters holding most approximate top rows.
/// The core can select A first, then mass expansion can admit B if its best
/// score remains inside the relative cutoff.
fn select_grouped_object_clusters(
    index: &IvfFlatIndex,
    ranked_clusters: &[ClusterScore],
    effective_nprobe: usize,
) -> Result<SelectedObjectClusters> {
    if ranked_clusters.is_empty() {
        return Err(ZeppelinError::Index(
            "grouped object selection received no ranked clusters".into(),
        ));
    }
    let budget = adaptive_object_budget(index, effective_nprobe);
    let mut object_candidates = Vec::new();
    let mut candidate_keys = HashSet::new();
    for score in ranked_clusters {
        let object = cluster_fetch_object(index, score.cluster_idx)?;
        if candidate_keys.insert(object.key.clone()) {
            object_candidates.push(object);
        }
    }

    let score_by_cluster: HashMap<usize, ClusterScore> = ranked_clusters
        .iter()
        .copied()
        .map(|score| (score.cluster_idx, score))
        .collect();
    let rank_by_cluster: HashMap<usize, usize> = ranked_clusters
        .iter()
        .enumerate()
        .map(|(rank, score)| (score.cluster_idx, rank))
        .collect();
    let mut selected_object_idxs = HashSet::new();
    let mut covered_clusters = HashSet::new();
    let mut clusters = Vec::new();
    let best_distance_score = ranked_clusters
        .iter()
        .map(|score| score.aggregate_score)
        .fold(f32::INFINITY, f32::min);
    let distance_cutoff =
        best_distance_score + best_distance_score.abs() * budget.relative_score_margin;

    while selected_object_idxs.len() < budget.max_objects {
        let ranking = if selected_object_idxs.len() <= budget.floor_objects {
            ObjectCandidateRanking::DistanceCore
        } else {
            ObjectCandidateRanking::MassExpansion
        };
        let Some(candidate) = best_object_candidate(
            &object_candidates,
            &score_by_cluster,
            &rank_by_cluster,
            &covered_clusters,
            &selected_object_idxs,
            ranking,
            distance_cutoff,
        ) else {
            break;
        };
        let within_floor = selected_object_idxs.len() < budget.floor_objects;
        if !within_floor && candidate.aggregate_score > distance_cutoff {
            break;
        }

        selected_object_idxs.insert(candidate.object_idx);
        for &cluster_idx in &object_candidates[candidate.object_idx].clusters {
            if covered_clusters.insert(cluster_idx) {
                clusters.push(cluster_idx);
            }
        }
    }

    if clusters.is_empty() {
        return Err(ZeppelinError::Index(
            "grouped object selection produced no clusters".into(),
        ));
    }

    Ok(SelectedObjectClusters {
        clusters,
        object_count: selected_object_idxs.len(),
    })
}

#[derive(Debug, Clone, Copy)]
/// Query-local score assigned to one candidate physical object.
struct ObjectCandidateScore {
    /// Position in the temporary `object_candidates` vector.
    object_idx: usize,
    /// Global approximate top-row mass not already covered by selected objects.
    mass_count: usize,
    /// Best lower-is-better sketch distance among uncovered member clusters.
    aggregate_score: f32,
    /// Best original sketch rank among uncovered member clusters.
    best_rank: usize,
}

#[derive(Debug, Clone, Copy)]
/// Tie-breaking policy for the two grouped-object selection phases.
enum ObjectCandidateRanking {
    /// Prefer the closest object, then uncovered mass, then stable object order.
    DistanceCore,
    /// Prefer uncovered mass, then distance, sketch rank, and stable order.
    MassExpansion,
}

/// Finds the best remaining physical object under one selection policy.
///
/// # Parameters
///
/// - `object_candidates`: Manifest-resolved objects under consideration.
/// - `score_by_cluster`: Resident-sketch evidence keyed by logical cluster.
/// - `rank_by_cluster`: Original sketch ordering used for deterministic ties.
/// - `covered_clusters`: Clusters already obtained through selected objects.
/// - `selected_object_idxs`: Candidate positions already chosen.
/// - `ranking`: Distance-core or mass-expansion ordering.
/// - `distance_cutoff`: Largest acceptable best-cluster score during expansion.
///
/// # Returns
///
/// The best unselected object score, or `None` when none remains or all
/// mass-expansion candidates exceed the cutoff.
///
/// # Examples
///
/// After object A covers clusters 0 and 1, scoring object B ignores any already
/// covered members and can return B if it contributes the best remaining mass.
fn best_object_candidate(
    object_candidates: &[ClusterFetchObject],
    score_by_cluster: &HashMap<usize, ClusterScore>,
    rank_by_cluster: &HashMap<usize, usize>,
    covered_clusters: &HashSet<usize>,
    selected_object_idxs: &HashSet<usize>,
    ranking: ObjectCandidateRanking,
    distance_cutoff: f32,
) -> Option<ObjectCandidateScore> {
    let mut best = None;
    for (object_idx, object) in object_candidates.iter().enumerate() {
        if selected_object_idxs.contains(&object_idx) {
            continue;
        }
        let candidate = score_object_candidate(
            object_idx,
            object,
            score_by_cluster,
            rank_by_cluster,
            covered_clusters,
        );
        if matches!(ranking, ObjectCandidateRanking::MassExpansion)
            && candidate.aggregate_score > distance_cutoff
        {
            continue;
        }
        if best
            .as_ref()
            .is_none_or(|best| object_candidate_better(&candidate, best, ranking))
        {
            best = Some(candidate);
        }
    }
    best
}

/// Aggregates uncovered sketch evidence for one physical object.
///
/// # Parameters
///
/// - `object_idx`: Stable temporary object position.
/// - `object`: Physical object and its logical memberships.
/// - `score_by_cluster`: Scores for clusters participating in this query.
/// - `rank_by_cluster`: Query-local sketch ranks.
/// - `covered_clusters`: Membership already supplied by selected objects.
///
/// # Returns
///
/// A score containing summed uncovered mass, best uncovered distance, and best
/// rank. Clusters absent from this query's ranking contribute nothing.
///
/// # Examples
///
/// An object with uncovered cluster masses 3 and 5 receives mass 8 and keeps
/// the smaller of their two aggregate distances.
fn score_object_candidate(
    object_idx: usize,
    object: &ClusterFetchObject,
    score_by_cluster: &HashMap<usize, ClusterScore>,
    rank_by_cluster: &HashMap<usize, usize>,
    covered_clusters: &HashSet<usize>,
) -> ObjectCandidateScore {
    let mut mass_count = 0usize;
    let mut aggregate_score = f32::INFINITY;
    let mut best_rank = usize::MAX;

    for cluster_idx in &object.clusters {
        if covered_clusters.contains(cluster_idx) {
            continue;
        }
        let Some(score) = score_by_cluster.get(cluster_idx) else {
            continue;
        };
        mass_count += score.mass_count;
        if score.aggregate_score < aggregate_score {
            aggregate_score = score.aggregate_score;
        }
        best_rank = best_rank.min(
            rank_by_cluster
                .get(cluster_idx)
                .copied()
                .unwrap_or(usize::MAX),
        );
    }

    ObjectCandidateScore {
        object_idx,
        mass_count,
        aggregate_score,
        best_rank,
    }
}

/// Applies deterministic ordering between two grouped-object scores.
///
/// # Parameters
///
/// - `candidate`: Newly scored object.
/// - `best`: Current winner.
/// - `ranking`: Phase-specific precedence rules.
///
/// # Returns
///
/// `true` when `candidate` should replace `best`. Stable object position is the
/// final tie breaker.
///
/// # Examples
///
/// In the core phase, distance `0.2` beats `0.3` even with less mass. In mass
/// expansion, mass 10 beats mass 8 as long as the caller already enforced the
/// distance cutoff.
fn object_candidate_better(
    candidate: &ObjectCandidateScore,
    best: &ObjectCandidateScore,
    ranking: ObjectCandidateRanking,
) -> bool {
    match ranking {
        ObjectCandidateRanking::DistanceCore => best
            .aggregate_score
            .partial_cmp(&candidate.aggregate_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| candidate.mass_count.cmp(&best.mass_count))
            .then_with(|| best.object_idx.cmp(&candidate.object_idx))
            .is_gt(),
        ObjectCandidateRanking::MassExpansion => candidate
            .mass_count
            .cmp(&best.mass_count)
            .then_with(|| {
                best.aggregate_score
                    .partial_cmp(&candidate.aggregate_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| best.best_rank.cmp(&candidate.best_rank))
            .is_gt(),
    }
}

/// Derives grouped-object floors, caps, and score margin for one query.
///
/// # Parameters
///
/// - `index`: Segment whose largest grouped-object arity shapes the policy.
/// - `effective_nprobe`: Clamped centroid probe count.
///
/// # Returns
///
/// A scalar budget. Thin objects use a larger floor and tighter margin; dense
/// grouping uses the normal floor because each GET already covers more clusters.
///
/// # Examples
///
/// A layout with at most two clusters per object is “thin” and retains up to a
/// five-object floor when the computed cap permits it.
fn adaptive_object_budget(index: &IvfFlatIndex, effective_nprobe: usize) -> AdaptiveObjectBudget {
    let max_arity = max_cluster_object_arity(index);
    let max_objects = grouped_object_cap_for_arity(max_arity, effective_nprobe);
    let is_thin = max_arity <= SKETCH_THIN_OBJECT_MAX_ARITY;
    let floor = if is_thin {
        SKETCH_ADAPTIVE_THIN_OBJECT_FLOOR
    } else {
        SKETCH_ADAPTIVE_FLOOR_OBJECTS
    };
    let relative_score_margin = if is_thin {
        SKETCH_ADAPTIVE_THIN_RELATIVE_SCORE_MARGIN
    } else {
        SKETCH_ADAPTIVE_RELATIVE_SCORE_MARGIN
    };
    AdaptiveObjectBudget {
        floor_objects: floor.min(max_objects),
        max_objects,
        relative_score_margin,
    }
}

/// Reports the largest number of logical clusters stored in one physical object.
///
/// # Parameters
///
/// - `index`: Loaded segment layout.
///
/// # Returns
///
/// At least one. Legacy and empty mappings are treated as one cluster per object.
///
/// # Examples
///
/// Object memberships of `[0, 1]` and `[2, 3, 4, 5]` yield arity four.
fn max_cluster_object_arity(index: &IvfFlatIndex) -> usize {
    index
        .cluster_objects
        .iter()
        .map(|object_ref| object_ref.clusters.len().max(1))
        .max()
        .unwrap_or(1)
}

/// Converts a cluster scan cap into a physical-object cap for grouping arity.
///
/// # Parameters
///
/// - `max_arity`: Maximum clusters carried by one object; callers pass at least
///   one.
/// - `effective_nprobe`: Clamped centroid probe count.
///
/// # Returns
///
/// A cap bounded by the historical pair-object budget and a small floor. When
/// the adaptive cluster cap already covers every probe, this returns the full
/// probe count so sketch pruning becomes a structural no-op.
///
/// # Panics
///
/// Panics if `max_arity` is zero because integer ceiling division requires a
/// nonzero divisor. [`max_cluster_object_arity`] guarantees at least one.
///
/// # Examples
///
/// With arity four and `nprobe=16`, the current policy caps selection at six
/// objects; at `nprobe=128`, the no-pruning sentinel allows all 128.
fn grouped_object_cap_for_arity(max_arity: usize, effective_nprobe: usize) -> usize {
    let cluster_cap = sketch_adaptive_cluster_cap(effective_nprobe);
    if cluster_cap >= effective_nprobe {
        effective_nprobe
    } else {
        let legacy_pair_cap = cluster_cap.div_ceil(2).max(1);
        cluster_cap
            .div_ceil(max_arity)
            .saturating_add(SKETCH_ADAPTIVE_OBJECT_CAP_EXTRA)
            .min(legacy_pair_cap)
            .max(SKETCH_ADAPTIVE_FLOOR_OBJECTS.min(effective_nprobe))
    }
}

/// Builds the cluster budget used by resident-sketch selection.
///
/// # Parameters
///
/// - `effective_nprobe`: Clamped centroid probe count.
///
/// # Returns
///
/// A budget whose floor and cap never exceed the available probes, with a 13%
/// relative score margin. Zero probes produce a zero budget, but the caller's
/// no-scan branch returns before asking the sketch to validate or use it.
///
/// # Examples
///
/// `nprobe=16` produces a cap of 14, while `nprobe=8` keeps all eight probes.
fn adaptive_sketch_budget(effective_nprobe: usize) -> AdaptiveClusterBudget {
    let max_clusters = sketch_adaptive_cluster_cap(effective_nprobe);
    AdaptiveClusterBudget::new(
        SKETCH_ADAPTIVE_FLOOR_CLUSTERS.min(max_clusters),
        max_clusters,
        SKETCH_ADAPTIVE_RELATIVE_SCORE_MARGIN,
    )
}

/// Computes the hard adaptive cluster cap for a probe count.
///
/// # Parameters
///
/// - `effective_nprobe`: Available centroid probes.
///
/// # Returns
///
/// Twice the historical smooth budget, clamped to `effective_nprobe`.
///
/// # Examples
///
/// A small probe set can return its full size, deliberately disabling pruning.
fn sketch_adaptive_cluster_cap(effective_nprobe: usize) -> usize {
    sketch_base_cluster_budget(effective_nprobe)
        .saturating_mul(SKETCH_ADAPTIVE_MAX_MULTIPLIER)
        .min(effective_nprobe)
}

/// Evaluates the monotonic historical sketch budget curve.
///
/// # Parameters
///
/// - `effective_nprobe`: Available centroid probes.
///
/// # Returns
///
/// The ceiling of a linear-plus-quadratic curve, floored at six and clamped to
/// `effective_nprobe`.
///
/// # Examples
///
/// At high probe counts the quadratic term grows the budget until the final
/// clamp makes a sentinel run scan every probe.
fn sketch_base_cluster_budget(effective_nprobe: usize) -> usize {
    let nprobe = effective_nprobe as f32;
    ((nprobe * SKETCH_CLUSTER_LINEAR_FRACTION + (nprobe * nprobe / SKETCH_CLUSTER_QUADRATIC_SCALE))
        .ceil() as usize)
        .max(SKETCH_BASE_MIN_CLUSTERS)
        .min(effective_nprobe)
}

/// Scans selected cluster objects and scores every surviving full vector exactly.
///
/// This is the unquantized path. Physical object fetches are concurrent across
/// selected objects. For each fetched object, bitmap and optional attribute
/// metadata for its member clusters are then fetched concurrently before a
/// sequential CPU pass parses rows, applies bitmap membership, and computes
/// exact distances.
///
/// # Parameters
///
/// - `index`: Loaded segment layout and bitmap-field metadata.
/// - `probe_clusters`: Logical clusters selected for this physical scan.
/// - `query`: Validated full-precision query.
/// - `distance_metric`: Exact row scoring policy.
/// - `filter`: Optional predicate. Its attributes are loaded for final exact
///   evaluation by the dispatcher; a bitmap can skip impossible rows early.
/// - `store`: Authoritative object-store reader.
/// - `cache`: Optional shared complete-object cache.
/// - `use_live_range`: Whether a self-contained live object prefix may replace
///   a complete GET.
///
/// # Returns
///
/// One owned [`Candidate`] per visited row allowed by the bitmap prefilter.
/// Ordering follows selected physical objects, manifest cluster membership, and
/// persisted row order; the caller performs top-k ordering.
///
/// # Errors
///
/// Propagates layout resolution, cache, object-store, bitmap-independent
/// attribute, and cluster decode errors. It also rejects grouped bytes under a
/// legacy key or cluster metadata inconsistent with the object membership.
///
/// # Side Effects
///
/// Performs object, bitmap, and possibly attribute GETs, may fill the cache, and
/// updates range-source metrics. No artifact is changed.
///
/// # Performance
///
/// Reads each distinct grouped object once and computes `O(rows * dim)` exact
/// distance work. It retains all scanned candidates until the dispatcher applies
/// top-k, so memory is proportional to surviving rows. Bitmap membership can
/// avoid vector scoring but not the full cluster-object read.
///
/// # Examples
///
/// Two selected logical clusters sharing one grouped object cause one data GET.
/// A bitmap that admits rows 2 and 9 makes only those rows candidates, but their
/// exact attributes are still available for final filter verification.
///
/// # Rust Notes for Java/C Engineers
///
/// `join_all` owns a collection of async blocks that borrow the immutable index,
/// store, and cache. It provides concurrency without OS threads or detached
/// tasks. Once I/O finishes, the loop owns decoded clusters and moves owned IDs
/// into candidates while cloning only attributes that may outlive the decode.
#[allow(clippy::too_many_arguments)]
async fn scan_clusters_flat(
    index: &IvfFlatIndex,
    probe_clusters: &[usize],
    query: &[f32],
    distance_metric: DistanceMetric,
    filter: Option<&Filter>,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
    use_live_range: bool,
) -> Result<Vec<Candidate>> {
    let has_bitmaps = !index.bitmap_fields.is_empty();

    // Phase 1: Parallel prefetch — all S3 I/O fires concurrently.
    let want_attrs = filter.is_some();
    let fetch_objects = cluster_fetch_objects(index, probe_clusters)?;
    let prefetched = futures::future::join_all(fetch_objects.iter().map(|object| {
        let object_key = object.key.clone();
        let object_clusters = object.clusters.clone();
        async move {
            let object_res =
                fetch_cluster_object_for_flat_scan(cache, store, object, use_live_range).await;
            let cluster_meta =
                futures::future::join_all(object_clusters.iter().map(|&cluster_idx| async move {
                    let owner = index.cluster_owner(cluster_idx);
                    let (prefilter, attrs) = tokio::join!(
                        try_bitmap_prefilter(
                            &index.namespace,
                            owner,
                            cluster_idx,
                            filter,
                            has_bitmaps,
                            store,
                            cache,
                        ),
                        async {
                            if want_attrs {
                                load_attrs(index, cluster_idx, filter, store, cache, None).await
                            } else {
                                Ok(None)
                            }
                        },
                    );
                    (cluster_idx, prefilter, attrs)
                }))
                .await;
            (object_key, object_clusters, object_res, cluster_meta)
        }
    }))
    .await;

    // Phase 2: Sequential compute — CPU-bound, no I/O.
    let mut candidates = Vec::new();
    for (object_key, object_clusters, object_res, cluster_meta) in prefetched {
        let object_data = object_res?;
        let grouped_sections = cluster_object_sections(&object_data)?;
        if grouped_sections.is_some() && index.cluster_objects.is_empty() {
            return Err(ZeppelinError::Index(format!(
                "legacy cluster key {object_key} contained grouped cluster data"
            )));
        }

        for (cluster_idx, prefilter, attrs) in cluster_meta {
            if !object_clusters.contains(&cluster_idx) {
                return Err(ZeppelinError::Index(format!(
                    "cluster metadata mismatch for object {object_key}: cluster {cluster_idx}"
                )));
            }
            let attrs = attrs?;
            let cluster = deserialize_cluster_from_object(&object_data, cluster_idx)?;

            for (j, vec) in cluster.vectors.iter().enumerate() {
                if let Some(ref bm) = prefilter {
                    if !bm.contains(j as u32) {
                        continue;
                    }
                }

                let score = compute_distance(query, vec, distance_metric);
                let vector_attrs = attrs.as_ref().and_then(|a| a.get(j)).cloned().flatten();

                candidates.push(Candidate {
                    id: cluster.ids[j].clone(),
                    score,
                    attributes: vector_attrs,
                    cluster_idx,
                    row_idx: j,
                });
            }
        }
    }

    Ok(candidates)
}

/// Uses SQ8 for coarse row ranking and full vectors for exact reranking.
///
/// Scalar quantization stores one calibrated byte per vector component. The
/// query remains full precision while
/// [`SqCalibration::asymmetric_distance`](crate::index::quantization::sq::SqCalibration::asymmetric_distance)
/// scores compact rows. Only a bounded frontier is then read and rescored from
/// full-precision vector bytes, so approximation affects recall and I/O but
/// never the returned distance value.
///
/// ```text
/// embedded calibration or legacy sidecar
///                  |
///                  v
/// fetch SQ blocks for selected objects
///                  |
///                  v
/// bitmap / exact attribute filter before truncation
///                  |
///                  v
/// retain at most 4 * fetch_k approximate rows
///                  |
///          +-------+------------------+
///          | current ranged layout    | legacy / cached full object
///          v                          v
/// coalesce full-vector ranges       parse full clusters
///          +-------------+------------+
///                        v
///                 exact distance
/// ```
///
/// # Parameters
///
/// - `index`: Loaded SQ segment, calibration metadata, and object layout.
/// - `probe_clusters`: Logical clusters selected for SQ scanning.
/// - `query`: Validated full-precision query.
/// - `distance_metric`: Metric used by both approximate and exact phases.
/// - `filter`: Optional predicate applied before coarse truncation.
/// - `fetch_k`: Candidate target after filter oversampling.
/// - `store`: Authoritative object-store reader.
/// - `cache`: Optional full-object and decoded-layout cache.
/// - `byte_stats`: Optional shared query-local diagnostic counters.
/// - `rerank_coalesce_gap_bytes`: Exclusive gap threshold for merging adjacent
///   exact-vector range requests.
///
/// # Returns
///
/// Exact-distance [`Candidate`] values for the retained rerank frontier. The
/// caller sorts, rechecks a filter when present, and truncates to `top_k`.
///
/// # Errors
///
/// Returns storage, cache, calibration, layout, range, SQ decode, attribute, or
/// full-vector decode errors. Duplicate/missing cluster payload or metadata is
/// treated as corruption. Parallel reads may already have filled cache entries
/// before a sibling error is observed.
///
/// # Panics
///
/// Internal artifacts must keep SQ IDs, codes, calibration dimensions, and row
/// metadata aligned. Debug builds also panic if `fetch_k * 4` overflows.
///
/// # Side Effects
///
/// Issues supporting, SQ, bitmap, attribute, header, and exact-vector GETs; may
/// populate or invalidate caches; updates metrics and optional byte counters.
/// It does not modify the immutable segment.
///
/// # Performance
///
/// Coarse scoring costs `O(selected_rows * dim)` over one-byte codes. Current v4
/// grouped objects can use one contiguous SQ range per object and coalesced
/// exact-vector ranges; legacy objects may require full-object or sidecar GETs.
/// The rerank frontier is at most four times `fetch_k`, subject to available
/// filter matches.
///
/// # Examples
///
/// For `top_k=10` with a filter and oversampled `fetch_k=30`, a 2,000-row SQ
/// scan first removes nonmatching rows, retains at most 120 approximate matches,
/// fetches their full vectors, and returns exact candidates for final top ten.
/// Filtering before the 120-row cutoff prevents selective matches from being
/// displaced by better approximate scores that would later fail the predicate.
///
/// # Rust Notes for Java/C Engineers
///
/// Each async fetch receives a cheap clone of `Arc<SqSearchByteStats>`, so all
/// futures update one counter set. Candidate maps own IDs and ranges across the
/// `.await` boundary; borrowed references into temporary decoded SQ buffers
/// could not legally survive there. Rust enforces that lifetime distinction at
/// compile time, whereas Java relies on heap reachability and C on manual buffer
/// lifetime discipline.
#[allow(clippy::too_many_arguments)]
async fn scan_clusters_sq(
    index: &IvfFlatIndex,
    probe_clusters: &[usize],
    query: &[f32],
    distance_metric: DistanceMetric,
    filter: Option<&Filter>,
    fetch_k: usize,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
    byte_stats: Option<Arc<SqSearchByteStats>>,
    rerank_coalesce_gap_bytes: usize,
) -> Result<Vec<Candidate>> {
    use crate::index::quantization::sq::{sq_calibration_key, SqCalibration};

    // Load SQ calibration. New segments embed it in centroids; legacy segments
    // keep the old sidecar.
    let calibration = if let Some(calibration) = &index.sq_calibration {
        calibration.clone()
    } else {
        let cal_key = sq_calibration_key(&index.namespace, &index.segment_id);
        let cal_data = fetch_with_cache_counted(
            cache,
            store,
            &cal_key,
            byte_stats.as_deref(),
            SqBytePhase::Other,
        )
        .await?;
        SqCalibration::from_bytes(&cal_data)?
    };
    let prefer_colocated_clusters = index.sq_calibration.is_some();

    // Phase 1: Coarse ranking with quantized distances — parallel prefetch.
    let has_bitmaps = !index.bitmap_fields.is_empty();

    // When a filter is present but NOT resolved by a bitmap index, we must
    // apply it DURING the coarse scan — before truncating to `rerank_count`.
    // Otherwise a selective filter's matches get truncated away by
    // approximate-distance ranking and the query silently under-fills top_k
    // (Task 6). That needs the per-cluster attrs in the coarse phase, so fetch
    // them alongside the SQ codes whenever a filter is active. Bitmap-resolved
    // clusters (prefilter Some) keep their fast path and ignore attrs.
    let want_attr_filter = filter.is_some();

    let fetch_objects = cluster_fetch_objects(index, probe_clusters)?;
    if let Some(stats) = &byte_stats {
        SqSearchByteStats::set_usize(&stats.selected_clusters, probe_clusters.len());
        SqSearchByteStats::set_usize(&stats.sq_objects, fetch_objects.len());
    }
    let sq_prefetched = futures::future::join_all(fetch_objects.iter().map(|object| {
        let stats = byte_stats.clone();
        async move {
            load_sq_object_for_coarse(
                index,
                object,
                prefer_colocated_clusters,
                store,
                cache,
                stats.as_deref(),
            )
            .await
        }
    }))
    .await;

    let meta_prefetched = futures::future::join_all(probe_clusters.iter().map(|&cluster_idx| {
        let owner = index.cluster_owner(cluster_idx);
        let stats = byte_stats.clone();
        async move {
            let (prefilter, attrs) = tokio::join!(
                try_bitmap_prefilter(
                    &index.namespace,
                    owner,
                    cluster_idx,
                    filter,
                    has_bitmaps,
                    store,
                    cache,
                ),
                async {
                    if want_attr_filter {
                        load_attrs(index, cluster_idx, filter, store, cache, stats.as_deref()).await
                    } else {
                        Ok(None)
                    }
                },
            );
            (cluster_idx, prefilter, attrs)
        }
    }))
    .await;

    let mut sq_by_cluster = HashMap::new();
    let mut vector_ranges_by_cluster: HashMap<usize, Vec<Range<usize>>> = HashMap::new();
    let mut prefetched_objects: HashMap<String, bytes::Bytes> = HashMap::new();
    for object_res in sq_prefetched {
        let fetched = object_res?;
        if let Some(full_object) = fetched.full_object {
            prefetched_objects.insert(fetched.object_key.clone(), full_object);
        }
        for (cluster_idx, ranges) in fetched.vector_ranges {
            if vector_ranges_by_cluster
                .insert(cluster_idx, ranges)
                .is_some()
            {
                return Err(ZeppelinError::Index(format!(
                    "SQ coarse fetched duplicate vector ranges for cluster {cluster_idx}"
                )));
            }
        }
        for (cluster_idx, sq_cluster) in fetched.sq_clusters {
            if sq_by_cluster.insert(cluster_idx, sq_cluster).is_some() {
                return Err(ZeppelinError::Index(format!(
                    "SQ coarse fetched duplicate cluster {cluster_idx}"
                )));
            }
        }
    }

    let mut meta_by_cluster = HashMap::new();
    for (cluster_idx, prefilter, attrs) in meta_prefetched {
        let attrs = attrs?;
        if meta_by_cluster
            .insert(cluster_idx, (prefilter, attrs))
            .is_some()
        {
            return Err(ZeppelinError::Index(format!(
                "SQ coarse fetched duplicate metadata for cluster {cluster_idx}"
            )));
        }
    }

    let mut coarse_candidates: Vec<(String, f32, usize, usize)> = Vec::new();
    for &cluster_idx in probe_clusters {
        let sq_cluster = sq_by_cluster.remove(&cluster_idx).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "SQ coarse missing cluster data for cluster {cluster_idx}"
            ))
        })?;
        let (prefilter, attrs) = meta_by_cluster.remove(&cluster_idx).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "SQ coarse missing metadata for cluster {cluster_idx}"
            ))
        })?;

        for (j, codes) in sq_cluster.codes.iter().enumerate() {
            if !coarse_row_passes(filter, &prefilter, &attrs, j) {
                continue;
            }
            let approx_score = calibration.asymmetric_distance(query, codes, distance_metric);
            coarse_candidates.push((sq_cluster.ids[j].clone(), approx_score, cluster_idx, j));
        }
    }

    // Rerank factor: take more candidates than needed for full-precision
    // reranking. Truncation now happens AFTER filtering, so the survivors are
    // real matches, not filtered-away noise (Task 6).
    let rerank_count = fetch_k * 4;
    partial_topk_by(
        &mut coarse_candidates,
        rerank_count,
        coarse_sq_candidate_cmp,
    );
    if let Some(stats) = &byte_stats {
        SqSearchByteStats::set_usize(&stats.coarse_candidates, coarse_candidates.len());
        SqSearchByteStats::set_usize(&stats.rerank_candidates, coarse_candidates.len());
    }

    debug!(
        coarse_candidates = coarse_candidates.len(),
        rerank_count = rerank_count,
        "SQ8 coarse ranking complete, starting rerank"
    );

    // Phase 2: Rerank with full-precision vectors — parallel prefetch.
    let mut cluster_candidates: HashMap<usize, Vec<RerankNeed>> = HashMap::new();
    for (id, _, cluster_idx, row_idx) in &coarse_candidates {
        let vector_range = vector_ranges_by_cluster
            .get(cluster_idx)
            .and_then(|ranges| ranges.get(*row_idx))
            .cloned();
        cluster_candidates
            .entry(*cluster_idx)
            .or_default()
            .push(RerankNeed {
                id: id.clone(),
                row_idx: *row_idx,
                vector_range,
            });
    }

    let want_rerank_attrs = filter.is_some();
    let rerank_cluster_ids: Vec<usize> = cluster_candidates.keys().copied().collect();
    let rerank_objects = cluster_fetch_objects(index, &rerank_cluster_ids)?;
    if let Some(stats) = &byte_stats {
        SqSearchByteStats::set_usize(&stats.rerank_clusters, rerank_cluster_ids.len());
        SqSearchByteStats::set_usize(&stats.rerank_objects, rerank_objects.len());
    }
    let attrs_future = futures::future::join_all(rerank_cluster_ids.iter().map(|&cluster_idx| {
        let stats = byte_stats.clone();
        async move {
            let attrs = if want_rerank_attrs {
                load_attrs(index, cluster_idx, filter, store, cache, stats.as_deref()).await
            } else {
                Ok(None)
            };
            (cluster_idx, attrs)
        }
    }));
    let clusters_future = futures::future::join_all(rerank_objects.iter().map(|object| {
        let stats = byte_stats.clone();
        let cluster_candidates = &cluster_candidates;
        let prefetched_objects = &prefetched_objects;
        async move {
            load_full_clusters_for_rerank(
                index,
                object,
                cluster_candidates,
                prefetched_objects,
                store,
                cache,
                stats.as_deref(),
                rerank_coalesce_gap_bytes,
            )
            .await
        }
    }));
    let (attrs_prefetched, rerank_prefetched) = tokio::join!(attrs_future, clusters_future);

    let mut attrs_by_cluster = HashMap::new();
    for (cluster_idx, attrs) in attrs_prefetched {
        attrs_by_cluster.insert(cluster_idx, attrs?);
    }

    let mut candidates = Vec::new();
    for object_res in rerank_prefetched {
        let vectors = object_res?;
        for fetched in vectors {
            let cluster_idx = fetched.cluster_idx;
            let attrs = attrs_by_cluster.get(&cluster_idx).ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "missing rerank attrs entry for cluster {cluster_idx}"
                ))
            })?;
            let score = compute_distance(query, &fetched.vector, distance_metric);
            let vector_attrs = attrs
                .as_ref()
                .and_then(|a| a.get(fetched.row_idx))
                .cloned()
                .flatten();
            candidates.push(Candidate {
                id: fetched.id,
                score,
                attributes: vector_attrs,
                cluster_idx: fetched.cluster_idx,
                row_idx: fetched.row_idx,
            });
        }
    }

    Ok(candidates)
}

/// SQ coarse payloads and rerank metadata recovered from one physical object.
struct CoarseObjectSqFetch {
    /// Immutable object key used to associate a prefetched full object.
    object_key: String,
    /// Decoded compact rows keyed by logical cluster.
    sq_clusters: Vec<(usize, crate::index::quantization::sq::SqClusterData)>,
    /// Absolute full-vector byte ranges aligned with each cluster's SQ IDs.
    vector_ranges: Vec<(usize, Vec<Range<usize>>)>,
    /// Complete bytes retained when a compatibility path had to download them.
    full_object: Option<bytes::Bytes>,
}

#[derive(Clone)]
/// One SQ coarse winner that must be recovered at full precision.
///
/// Cloning duplicates the ID string and optional range; it is used to build
/// owned requests that safely cross async range fetches.
struct RerankNeed {
    /// Vector identifier recorded by the SQ payload.
    id: String,
    /// Original row position used for attribute alignment.
    row_idx: usize,
    /// Absolute full-vector byte range when current layout metadata provides it.
    vector_range: Option<Range<usize>>,
}

/// One logical exact-vector range request before physical coalescing.
struct RerankRangeRequest {
    /// Logical cluster owning the row.
    cluster_idx: usize,
    /// Owned identity and row metadata.
    need: RerankNeed,
    /// Absolute vector-only byte range in the grouped object.
    range: Range<usize>,
}

#[derive(Debug, PartialEq, Eq)]
/// One physical range GET that covers one or more logical rerank requests.
struct CoalescedRerankRange {
    /// Absolute merged byte span.
    range: Range<usize>,
    /// Positions in the original request vector recovered from this span.
    request_indices: Vec<usize>,
}

/// Full-precision row reconstructed for exact distance calculation.
struct RerankFetchedVector {
    /// Logical cluster owning the row.
    cluster_idx: usize,
    /// Owned vector identifier.
    id: String,
    /// Row position retained for attribute lookup.
    row_idx: usize,
    /// Owned full-precision components parsed or cloned from the object.
    vector: Vec<f32>,
}

/// Fetches SQ coarse data for every selected cluster in one physical object.
///
/// v4 grouped objects expose a contiguous SQ block, so the hot path reads one
/// range per selected object and never downloads full vectors during coarse
/// scoring. Legacy grouped/per-cluster objects keep the old full-object or
/// sidecar behavior so existing immutable data remains readable.
///
/// # Parameters
///
/// - `index`: Loaded segment metadata and vector dimension.
/// - `object`: Physical object and logical clusters selected from it.
/// - `prefer_colocated`: Whether embedded calibration indicates the newer
///   colocated SQ layout should be attempted.
/// - `store`: Authoritative object-store reader.
/// - `cache`: Optional full-object and decoded-layout cache.
/// - `stats`: Optional query-local byte counters.
///
/// # Returns
///
/// Decoded SQ rows for every object cluster, plus absolute full-vector ranges
/// when a v4 directory describes them. A compatibility full-object download is
/// retained so exact rerank can reuse it without a second GET.
///
/// # Errors
///
/// Propagates storage, cache, header, layout, range, and SQ decode failures.
/// Current layouts with an SQ directory must describe every selected cluster;
/// inconsistent sections fail rather than silently reading a sidecar.
///
/// # Side Effects
///
/// May perform one SQ range GET, one full-object GET, or one sidecar GET per
/// legacy cluster, and may populate caches and diagnostics.
///
/// # Consistency
///
/// Compatibility branches read immutable formats already named by the loaded
/// segment. They do not infer new visibility or rewrite old artifacts.
///
/// # Performance
///
/// The current v4 path reads one contiguous range per grouped object. A legacy
/// object can require the complete object plus sidecars for member clusters that
/// do not contain colocated codes.
///
/// # Examples
///
/// A v4 object containing clusters 4 and 5 returns two decoded code arrays and
/// two row-aligned vector-range tables from one SQ span. A legacy object with
/// external codes performs separate sidecar reads but returns the same logical
/// cluster payloads.
async fn load_sq_object_for_coarse(
    index: &IvfFlatIndex,
    object: &ClusterFetchObject,
    prefer_colocated: bool,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
    stats: Option<&SqSearchByteStats>,
) -> Result<CoarseObjectSqFetch> {
    use crate::index::quantization::sq::{deserialize_sq_cluster, sq_cluster_key};

    if !prefer_colocated {
        let mut sq_clusters = Vec::with_capacity(object.clusters.len());
        for &cluster_idx in &object.clusters {
            let sq_key = sq_cluster_key(
                &index.namespace,
                index.cluster_owner(cluster_idx),
                cluster_idx,
            );
            let sq_data =
                fetch_with_cache_counted(cache, store, &sq_key, stats, SqBytePhase::Sq).await?;
            if let Some(stats) = stats {
                stats.record_logical_sq_bytes(sq_data.len());
            }
            sq_clusters.push((cluster_idx, deserialize_sq_cluster(&sq_data)?));
        }
        return Ok(CoarseObjectSqFetch {
            object_key: object.key.clone(),
            sq_clusters,
            vector_ranges: Vec::new(),
            full_object: None,
        });
    }

    if let Some(layout) = load_cluster_object_layout(index, object, store, cache, stats).await? {
        if layout.sections.iter().all(|section| section.sq.is_some()) {
            let sq_bytes = fetch_object_sq_range(
                &object.key,
                object.size_bytes,
                &layout,
                &object.clusters,
                store,
                cache,
                stats,
            )
            .await?;
            let mut sq_clusters = Vec::with_capacity(object.clusters.len());
            let mut vector_ranges = Vec::with_capacity(object.clusters.len());
            for &cluster_idx in &object.clusters {
                let section = layout.section(cluster_idx).ok_or_else(|| {
                    ZeppelinError::Index(format!(
                        "cluster {cluster_idx} missing from layout for {}",
                        object.key
                    ))
                })?;
                let sq_range = section.sq.as_ref().ok_or_else(|| {
                    ZeppelinError::Index(format!(
                        "cluster {cluster_idx} missing SQ range in {}",
                        object.key
                    ))
                })?;
                let sq_slice = slice_relative_range(
                    &sq_bytes.bytes,
                    &sq_range.clone(),
                    sq_bytes.base_offset,
                    "SQ range",
                    &object.key,
                )?;
                let sq_cluster = deserialize_sq_cluster(sq_slice)?;
                let ranges = full_vector_ranges_from_sq_ids(section, &sq_cluster.ids, index.dim)?;
                sq_clusters.push((cluster_idx, sq_cluster));
                vector_ranges.push((cluster_idx, ranges));
            }
            return Ok(CoarseObjectSqFetch {
                object_key: object.key.clone(),
                sq_clusters,
                vector_ranges,
                full_object: None,
            });
        }
    }

    let object_data =
        fetch_with_cache_counted(cache, store, &object.key, stats, SqBytePhase::Sq).await?;
    let mut sq_clusters = Vec::with_capacity(object.clusters.len());
    for &cluster_idx in &object.clusters {
        if let Some(sq_cluster) =
            deserialize_colocated_sq_cluster_from_object(&object_data, cluster_idx)?
        {
            if let Some(stats) = stats {
                let sq_bytes: usize = sq_cluster.codes.iter().map(Vec::len).sum::<usize>()
                    + sq_cluster.ids.iter().map(|id| 4 + id.len()).sum::<usize>()
                    + 8;
                stats.record_logical_sq_bytes(sq_bytes);
            }
            sq_clusters.push((cluster_idx, sq_cluster));
            continue;
        }

        let sq_key = sq_cluster_key(
            &index.namespace,
            index.cluster_owner(cluster_idx),
            cluster_idx,
        );
        let sq_data =
            fetch_with_cache_counted(cache, store, &sq_key, stats, SqBytePhase::Sq).await?;
        if let Some(stats) = stats {
            stats.record_logical_sq_bytes(sq_data.len());
        }
        sq_clusters.push((cluster_idx, deserialize_sq_cluster(&sq_data)?));
    }

    Ok(CoarseObjectSqFetch {
        object_key: object.key.clone(),
        sq_clusters,
        vector_ranges: Vec::new(),
        full_object: Some(object_data),
    })
}

/// Bytes fetched for one absolute object span plus their origin offset.
struct RangeBytes {
    /// Absolute object offset corresponding to `bytes[0]`.
    base_offset: usize,
    /// Immutable range payload, possibly sliced from a complete cached object.
    bytes: bytes::Bytes,
}

/// Fetches the smallest contiguous span covering selected SQ sections.
///
/// # Parameters
///
/// - `object_key`: Immutable grouped-object key.
/// - `object_size_bytes`: Manifest-declared full length used to validate cache.
/// - `layout`: Validated decoded object directory.
/// - `clusters`: Logical cluster sections to include.
/// - `store`: Authoritative range reader.
/// - `cache`: Optional complete-object cache.
/// - `stats`: Optional byte counters.
///
/// # Returns
///
/// The contiguous bytes from the minimum SQ start through maximum SQ end, plus
/// that absolute starting offset. Gaps between requested sections are included
/// physically but excluded from logical-byte accounting.
///
/// # Errors
///
/// Returns an index error for a missing SQ section, arithmetic overflow, or an
/// empty/invalid combined span, and propagates cache or range-GET failures.
///
/// # Side Effects
///
/// May evict a wrong-length cached object, perform one S3/MinIO range GET, and
/// update metrics and diagnostics.
///
/// # Performance
///
/// Always uses at most one physical range read for the selected SQ sections in
/// an object. This deliberately accepts intervening bytes to avoid extra
/// roundtrips.
///
/// # Examples
///
/// SQ sections `100..180` and `200..260` produce one physical `100..260` read,
/// 140 logical bytes, and 20 slack bytes.
async fn fetch_object_sq_range(
    object_key: &str,
    object_size_bytes: u64,
    layout: &ClusterObjectLayout,
    clusters: &[usize],
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
    stats: Option<&SqSearchByteStats>,
) -> Result<RangeBytes> {
    let mut start = usize::MAX;
    let mut end = 0usize;
    let mut logical_bytes = 0usize;
    for &cluster_idx in clusters {
        let section = layout.section(cluster_idx).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "cluster {cluster_idx} missing from layout for {object_key}"
            ))
        })?;
        let sq = section.sq.as_ref().ok_or_else(|| {
            ZeppelinError::Index(format!(
                "cluster {cluster_idx} missing SQ range in {object_key}"
            ))
        })?;
        logical_bytes = logical_bytes
            .checked_add(sq.end - sq.start)
            .ok_or_else(|| ZeppelinError::Index("SQ logical byte count overflows".into()))?;
        start = start.min(sq.start);
        end = end.max(sq.end);
    }
    if start == usize::MAX || start >= end {
        return Err(ZeppelinError::Index(format!(
            "empty SQ range for object {object_key}"
        )));
    }
    match cached_full_object_for_range(cache, object_key, object_size_bytes, end, "sq", 1).await? {
        RangeCacheLookup::Local(data) => {
            if let Some(stats) = stats {
                stats.record_local_bytes(data.len());
                stats.record_logical_sq_bytes(logical_bytes);
            }
            return Ok(RangeBytes {
                base_offset: start,
                bytes: data.slice(start..end),
            });
        }
        RangeCacheLookup::Miss => {
            crate::metrics::RANGE_SOURCE_TOTAL
                .with_label_values(&["sq", "s3"])
                .inc();
        }
        RangeCacheLookup::CorruptEvicted => {}
    }
    let bytes = store.get_range(object_key, start..end).await?;
    if let Some(stats) = stats {
        stats.record_get(SqBytePhase::Sq, bytes.len());
        stats.record_logical_sq_bytes(logical_bytes);
    }
    Ok(RangeBytes {
        base_offset: start,
        bytes,
    })
}

/// Derives each full-vector byte span from SQ row IDs and the full section range.
///
/// The full section encodes an eight-byte `(row_count, dim)` header followed by
/// repeated `(id_length, id_bytes, vector_bytes)` rows. SQ IDs preserve the same
/// order, so their encoded lengths advance directly to each vector payload.
///
/// # Parameters
///
/// - `section`: Directory entry containing the absolute full-section range.
/// - `ids`: SQ identifiers in persisted row order.
/// - `dim`: Full-vector component count.
///
/// # Returns
///
/// One absolute vector-only range per ID, aligned to `ids` order.
///
/// # Errors
///
/// Returns an index error on byte-width or offset overflow, or when derived rows
/// do not end exactly at the declared full section boundary. Exact coverage is
/// the cross-format proof that SQ IDs and full rows remain aligned.
///
/// # Performance
///
/// Allocates one range per row and performs no byte reads or vector decoding.
///
/// # Examples
///
/// For dimension 128, each derived span is 512 bytes. The preceding ID length
/// and bytes are skipped, so rerank can fetch only the floating-point payload.
fn full_vector_ranges_from_sq_ids(
    section: &ClusterObjectRange,
    ids: &[String],
    dim: usize,
) -> Result<Vec<Range<usize>>> {
    let vector_bytes = dim
        .checked_mul(4)
        .ok_or_else(|| ZeppelinError::Index("full-vector byte width overflows".into()))?;
    let mut offset = section
        .full
        .start
        .checked_add(8)
        .ok_or_else(|| ZeppelinError::Index("full-vector section header overflows".into()))?;
    let mut ranges = Vec::with_capacity(ids.len());
    for id in ids {
        let id_len = id.len();
        let vector_start = offset
            .checked_add(4)
            .and_then(|offset| offset.checked_add(id_len))
            .ok_or_else(|| ZeppelinError::Index("full-vector row offset overflows".into()))?;
        let vector_end = vector_start
            .checked_add(vector_bytes)
            .ok_or_else(|| ZeppelinError::Index("full-vector row range overflows".into()))?;
        ranges.push(vector_start..vector_end);
        offset = vector_end;
    }
    if offset != section.full.end {
        return Err(ZeppelinError::Index(format!(
            "derived full-vector ranges do not cover cluster {}: derived_end={}, section_end={}",
            section.cluster_idx, offset, section.full.end
        )));
    }
    Ok(ranges)
}

#[allow(clippy::too_many_arguments)]
/// Loads exact full vectors for all coarse winners contained in one object.
///
/// Reuses a full object already downloaded during SQ coarse compatibility work;
/// otherwise it prefers layout-backed range reads when every candidate has a
/// derived span, then falls back to one complete-object GET for legacy formats.
///
/// # Parameters
///
/// - `index`: Segment dimension and layout metadata.
/// - `object`: Physical object being reranked.
/// - `cluster_candidates`: Rerank needs keyed by logical cluster.
/// - `prefetched_objects`: Complete bytes retained from the coarse phase.
/// - `store`: Authoritative object-store reader.
/// - `cache`: Optional full-object/layout cache.
/// - `stats`: Optional SQ byte diagnostics.
/// - `rerank_coalesce_gap_bytes`: Exclusive physical range merge threshold.
///
/// # Returns
///
/// Owned full-precision rows for candidate clusters in this object. Returns an
/// empty vector without I/O when the object contains no requested cluster.
///
/// # Errors
///
/// Propagates layout, cache, object-store, coalescing, parsing, and candidate-ID
/// consistency errors.
///
/// # Side Effects
///
/// May fetch a directory header, exact ranges, or the complete object and update
/// caches, metrics, and diagnostics.
///
/// # Examples
///
/// If the coarse phase retained a legacy full object, rerank parses winners from
/// those bytes immediately. For a v4 object with known row ranges, it requests
/// only the winner vectors, optionally bridging small gaps.
async fn load_full_clusters_for_rerank(
    index: &IvfFlatIndex,
    object: &ClusterFetchObject,
    cluster_candidates: &HashMap<usize, Vec<RerankNeed>>,
    prefetched_objects: &HashMap<String, bytes::Bytes>,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
    stats: Option<&SqSearchByteStats>,
    rerank_coalesce_gap_bytes: usize,
) -> Result<Vec<RerankFetchedVector>> {
    let needed_clusters: Vec<usize> = object
        .clusters
        .iter()
        .copied()
        .filter(|cluster_idx| cluster_candidates.contains_key(cluster_idx))
        .collect();
    if needed_clusters.is_empty() {
        return Ok(Vec::new());
    }

    if let Some(object_data) = prefetched_objects.get(&object.key) {
        return rerank_vectors_from_object(
            &object.key,
            object_data,
            &needed_clusters,
            cluster_candidates,
        );
    }

    if load_cluster_object_layout(index, object, store, cache, stats)
        .await?
        .is_some()
        && all_needed_vectors_have_ranges(&needed_clusters, cluster_candidates)
    {
        return fetch_rerank_vectors_by_range(
            &object.key,
            object.size_bytes,
            &needed_clusters,
            cluster_candidates,
            index.dim,
            store,
            cache,
            stats,
            rerank_coalesce_gap_bytes,
        )
        .await;
    }

    let object_data =
        fetch_with_cache_counted(cache, store, &object.key, stats, SqBytePhase::Rerank).await?;
    rerank_vectors_from_object(
        &object.key,
        &object_data,
        &needed_clusters,
        cluster_candidates,
    )
}

/// Checks whether every requested rerank row has an absolute vector span.
///
/// # Parameters
///
/// - `needed_clusters`: Object member clusters participating in rerank.
/// - `cluster_candidates`: Candidate metadata keyed by cluster.
///
/// # Returns
///
/// `true` only when every cluster has a candidate list and every need contains
/// `Some(range)`. An empty `needed_clusters` slice is vacuously true, although
/// the caller handles that case before this helper.
///
/// # Examples
///
/// Two candidates with ranges and one legacy candidate without a range return
/// `false`, selecting the full-object compatibility path.
fn all_needed_vectors_have_ranges(
    needed_clusters: &[usize],
    cluster_candidates: &HashMap<usize, Vec<RerankNeed>>,
) -> bool {
    needed_clusters.iter().all(|cluster_idx| {
        cluster_candidates
            .get(cluster_idx)
            .map(|needs| needs.iter().all(|need| need.vector_range.is_some()))
            .unwrap_or(false)
    })
}

#[allow(clippy::too_many_arguments)]
/// Fetches and reconstructs exact rerank vectors using coalesced range reads.
///
/// Logical row ranges are sorted and merged into physical spans. Fetches for
/// distinct spans are concurrent, but the result is reconstructed into the
/// original logical request order so cluster/ID/attribute alignment survives
/// the I/O optimization.
///
/// # Parameters
///
/// - `object_key`: Immutable grouped-object key.
/// - `object_size_bytes`: Manifest-declared length for cache validation.
/// - `clusters`: Logical clusters in desired result grouping order.
/// - `cluster_candidates`: Candidate rows and absolute vector ranges.
/// - `dim`: Components per full vector.
/// - `store`: Authoritative object-store range reader.
/// - `cache`: Optional complete-object cache.
/// - `stats`: Optional byte counters.
/// - `rerank_coalesce_gap_bytes`: Exclusive maximum gap bridged between ranges.
///
/// # Returns
///
/// Owned full-precision vectors in the same order the logical requests were
/// collected: cluster order, then candidate order within each cluster.
///
/// # Errors
///
/// Returns errors for missing candidate/range metadata, invalid ranges, byte
/// arithmetic overflow, failed cache/S3 reads, mismatched physical response
/// counts, out-of-bounds slices, wrong vector widths, or a logical request not
/// reconstructed from its coalesced span.
///
/// # Side Effects
///
/// May validate or evict a cached full object, issue concurrent range GETs, and
/// update range-source metrics and query diagnostics.
///
/// # Performance
///
/// Coalescing reduces GET count at the cost of downloading bytes between nearby
/// vectors. A valid complete-object cache satisfies every span locally. Network
/// range futures are all polled together; parsed vectors allocate `dim` floats
/// per candidate.
///
/// # Examples
///
/// Logical ranges `100..116`, `120..136`, and `900..916` with threshold 8
/// become physical reads `100..136` and `900..916`. The four-byte first gap is
/// downloaded as slack, then the first two vectors are sliced back out.
///
/// # Rust Notes for Java/C Engineers
///
/// The temporary `Vec<Option<RerankFetchedVector>>` is a checked assembly table:
/// each original request slot starts empty and must become `Some`. Rust's
/// exhaustive conversion to `Result<Vec<_>>` prevents a null-filled result from
/// escaping, while ownership moves each completed vector out exactly once.
async fn fetch_rerank_vectors_by_range(
    object_key: &str,
    object_size_bytes: u64,
    clusters: &[usize],
    cluster_candidates: &HashMap<usize, Vec<RerankNeed>>,
    dim: usize,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
    stats: Option<&SqSearchByteStats>,
    rerank_coalesce_gap_bytes: usize,
) -> Result<Vec<RerankFetchedVector>> {
    let mut requested = Vec::new();
    for &cluster_idx in clusters {
        let needs = cluster_candidates.get(&cluster_idx).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "missing rerank candidates for cluster {cluster_idx}"
            ))
        })?;
        for need in needs {
            let range = need.vector_range.clone().ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "missing vector range for cluster {cluster_idx} row {}",
                    need.row_idx
                ))
            })?;
            requested.push(RerankRangeRequest {
                cluster_idx,
                need: need.clone(),
                range,
            });
        }
    }
    let logical_bytes = requested.iter().try_fold(0usize, |acc, request| {
        let len = request.range.end - request.range.start;
        acc.checked_add(len)
            .ok_or_else(|| ZeppelinError::Index("rerank logical byte count overflows".into()))
    })?;
    let coalesced = coalesce_rerank_ranges(&requested, rerank_coalesce_gap_bytes)?;
    let ranges: Vec<Range<usize>> = coalesced
        .iter()
        .map(|coalesced| coalesced.range.clone())
        .collect();
    debug!(
        object_key,
        logical_ranges = requested.len(),
        physical_ranges = ranges.len(),
        gap_bytes = rerank_coalesce_gap_bytes,
        "coalesced rerank vector ranges"
    );
    let needed_end = ranges.iter().map(|range| range.end).max().unwrap_or(0);
    let vector_bytes =
        match cached_full_object_for_range(
            cache,
            object_key,
            object_size_bytes,
            needed_end,
            "rerank",
            ranges.len() as u64,
        )
        .await?
        {
            RangeCacheLookup::Local(data) => {
                if let Some(stats) = stats {
                    stats.record_local_bytes(data.len());
                    stats.record_logical_rerank_bytes(logical_bytes);
                }
                ranges
                    .iter()
                    .cloned()
                    .map(|range| data.slice(range))
                    .collect()
            }
            RangeCacheLookup::Miss => {
                crate::metrics::RANGE_SOURCE_TOTAL
                    .with_label_values(&["rerank", "s3"])
                    .inc_by(ranges.len() as u64);
                let vector_bytes = futures::future::join_all(
                    ranges
                        .iter()
                        .cloned()
                        .map(|range| store.get_range(object_key, range)),
                )
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?;
                if let Some(stats) = stats {
                    let physical_bytes = vector_bytes.iter().map(bytes::Bytes::len).try_fold(
                        0usize,
                        |acc, len| {
                            acc.checked_add(len).ok_or_else(|| {
                                ZeppelinError::Index("rerank physical byte count overflows".into())
                            })
                        },
                    )?;
                    stats.record_gets(SqBytePhase::Rerank, ranges.len(), physical_bytes);
                    stats.record_logical_rerank_bytes(logical_bytes);
                }
                vector_bytes
            }
            RangeCacheLookup::CorruptEvicted => {
                let vector_bytes = futures::future::join_all(
                    ranges
                        .iter()
                        .cloned()
                        .map(|range| store.get_range(object_key, range)),
                )
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?;
                if let Some(stats) = stats {
                    let physical_bytes = vector_bytes.iter().map(bytes::Bytes::len).try_fold(
                        0usize,
                        |acc, len| {
                            acc.checked_add(len).ok_or_else(|| {
                                ZeppelinError::Index("rerank physical byte count overflows".into())
                            })
                        },
                    )?;
                    stats.record_gets(SqBytePhase::Rerank, ranges.len(), physical_bytes);
                    stats.record_logical_rerank_bytes(logical_bytes);
                }
                vector_bytes
            }
        };
    if vector_bytes.len() != coalesced.len() {
        return Err(ZeppelinError::Index(format!(
            "range fetch count mismatch for {object_key}: requested={}, got={}",
            coalesced.len(),
            vector_bytes.len()
        )));
    }

    let mut fetched: Vec<Option<RerankFetchedVector>> = std::iter::repeat_with(|| None)
        .take(requested.len())
        .collect();
    for (coalesced_range, bytes) in coalesced.iter().zip(vector_bytes) {
        for &request_idx in &coalesced_range.request_indices {
            let request = requested.get(request_idx).ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "coalesced range for {object_key} references missing request {request_idx}"
                ))
            })?;
            let vector_bytes = slice_relative_range(
                &bytes,
                &request.range,
                coalesced_range.range.start,
                "rerank vector range",
                object_key,
            )?;
            let vector = parse_f32_vector(vector_bytes, dim)?;
            fetched[request_idx] = Some(RerankFetchedVector {
                cluster_idx: request.cluster_idx,
                id: request.need.id.clone(),
                row_idx: request.need.row_idx,
                vector,
            });
        }
    }

    fetched
        .into_iter()
        .enumerate()
        .map(|(idx, fetched)| {
            fetched.ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "coalesced rerank fetch for {object_key} missing request {idx}"
                ))
            })
        })
        .collect()
}

/// Merges sorted overlapping or sufficiently close rerank ranges.
///
/// # Parameters
///
/// - `requests`: Logical absolute ranges in any order.
/// - `max_gap_bytes`: Exclusive gap limit. Overlaps always merge; a nonoverlap
///   gap merges only when it is strictly smaller than this value.
///
/// # Returns
///
/// Physical spans sorted by byte offset. Each span retains original request
/// indexes so the caller can restore logical order after concurrent GETs. Empty
/// input returns an empty vector.
///
/// # Errors
///
/// Returns an index error if any request is empty or reversed. No partial set of
/// ranges is returned.
///
/// # Performance
///
/// Sorting costs `O(n log n)` and the merge pass is linear. Range values and
/// request indexes are copied; vector payload bytes are not involved.
///
/// # Examples
///
/// With threshold 11, `100..110` and `120..130` merge because their gap is 10.
/// With threshold 10 they remain separate. Overlapping ranges merge even when
/// the threshold is zero.
fn coalesce_rerank_ranges(
    requests: &[RerankRangeRequest],
    max_gap_bytes: usize,
) -> Result<Vec<CoalescedRerankRange>> {
    let mut sorted = Vec::with_capacity(requests.len());
    for (idx, request) in requests.iter().enumerate() {
        if request.range.start >= request.range.end {
            return Err(ZeppelinError::Index(format!(
                "invalid rerank vector range: request={idx}, start={}, end={}",
                request.range.start, request.range.end
            )));
        }
        sorted.push((idx, request.range.clone()));
    }
    sorted.sort_by(|a, b| {
        a.1.start
            .cmp(&b.1.start)
            .then_with(|| a.1.end.cmp(&b.1.end))
            .then_with(|| a.0.cmp(&b.0))
    });

    let mut coalesced: Vec<CoalescedRerankRange> = Vec::new();
    for (request_idx, range) in sorted {
        let Some(last) = coalesced.last_mut() else {
            coalesced.push(CoalescedRerankRange {
                range,
                request_indices: vec![request_idx],
            });
            continue;
        };

        let overlaps = range.start < last.range.end;
        let mergeable_gap = if overlaps {
            true
        } else {
            range.start - last.range.end < max_gap_bytes
        };
        if mergeable_gap {
            last.range.end = last.range.end.max(range.end);
            last.request_indices.push(request_idx);
        } else {
            coalesced.push(CoalescedRerankRange {
                range,
                request_indices: vec![request_idx],
            });
        }
    }

    Ok(coalesced)
}

/// Recovers candidate full vectors by ID from a downloaded complete object.
///
/// # Parameters
///
/// - `object_key`: Key included in corruption diagnostics.
/// - `object_data`: Complete legacy or grouped object bytes.
/// - `needed_clusters`: Logical member clusters to parse.
/// - `cluster_candidates`: Coarse winners keyed by cluster.
///
/// # Returns
///
/// Owned vectors in `needed_clusters` order and candidate order. The returned
/// row index is the row found by ID in the full object, preserving attribute
/// alignment even though the coarse request also carried its earlier position.
///
/// # Errors
///
/// Returns an index error when candidate metadata is missing, a cluster cannot
/// be decoded, or a coarse winner ID is absent from the corresponding full
/// cluster. No incomplete result escapes.
///
/// # Performance
///
/// Parses every requested full cluster, builds one borrowed ID-to-row map, and
/// clones only winner vectors. Cost is linear in parsed cluster bytes plus
/// `winner_count * dim` copied floats.
///
/// # Examples
///
/// If cluster 3 contains 1,000 rows but coarse ranking retained IDs `a` and `z`,
/// the helper parses the cluster once and clones only those two vectors.
///
/// # Rust Notes for Java/C Engineers
///
/// The hash map stores `&str` slices borrowed from the decoded cluster's owned
/// strings. Rust prevents the map from outliving that cluster. Returned vectors
/// are cloned into owned allocations, so they remain valid after both the map
/// and decoded object are dropped.
fn rerank_vectors_from_object(
    object_key: &str,
    object_data: &[u8],
    needed_clusters: &[usize],
    cluster_candidates: &HashMap<usize, Vec<RerankNeed>>,
) -> Result<Vec<RerankFetchedVector>> {
    let mut vectors = Vec::new();
    for &cluster_idx in needed_clusters {
        let needs = cluster_candidates
            .get(&cluster_idx)
            .ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "missing rerank candidate IDs for cluster {cluster_idx}"
                ))
            })?
            .clone();
        let cluster = deserialize_cluster_from_object(object_data, cluster_idx).map_err(|e| {
            ZeppelinError::Index(format!(
                "failed to deserialize cluster {cluster_idx} from {object_key}: {e}"
            ))
        })?;
        let row_by_id: HashMap<&str, usize> = cluster
            .ids
            .iter()
            .enumerate()
            .map(|(row_idx, id)| (id.as_str(), row_idx))
            .collect();
        for need in needs {
            let row_idx = row_by_id.get(need.id.as_str()).copied().ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "candidate {} missing from cluster {cluster_idx} in {object_key}",
                    need.id
                ))
            })?;
            vectors.push(RerankFetchedVector {
                cluster_idx,
                id: need.id,
                row_idx,
                vector: cluster.vectors[row_idx].clone(),
            });
        }
    }
    Ok(vectors)
}

/// Decodes one exact little-endian vector from a vector-only byte range.
///
/// # Parameters
///
/// - `data`: Borrowed bytes expected to contain exactly `dim` IEEE-754 values.
/// - `dim`: Required component count.
///
/// # Returns
///
/// An owned `Vec<f32>` in persisted component order.
///
/// # Errors
///
/// Returns an index error if `dim * 4` overflows or the byte range has any other
/// length. Exact size rejects both truncation and accidental adjacent bytes.
///
/// # Performance
///
/// Allocates `dim` floats and performs one linear endian conversion.
///
/// # Examples
///
/// Eight bytes representing `1.0f32` and `-2.0f32` decode with `dim=2`; the
/// same bytes with `dim=3` return a length mismatch.
fn parse_f32_vector(data: &[u8], dim: usize) -> Result<Vec<f32>> {
    let expected_len = dim
        .checked_mul(4)
        .ok_or_else(|| ZeppelinError::Index("vector byte length overflows".into()))?;
    if data.len() != expected_len {
        return Err(ZeppelinError::Index(format!(
            "vector range length mismatch: expected {expected_len}, got {}",
            data.len()
        )));
    }
    Ok(data
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect())
}

/// Loads and validates the directory of a possibly grouped cluster object.
///
/// Decoded layouts are checked first in the supplied cache, then in a
/// process-wide concurrent cache. A cold lookup reads only the fixed header plus
/// manifest-implied directory length, validates cluster membership, and stores
/// the immutable decoded result in both cache layers.
///
/// # Parameters
///
/// - `_index`: Loaded index handle retained for a uniform helper signature; the
///   current implementation needs no additional fields from it.
/// - `object`: Physical key, manifest clusters, and declared size.
/// - `store`: Authoritative object-store range reader.
/// - `cache`: Optional local byte/decoded-object cache.
/// - `stats`: Optional byte diagnostics charged to supporting I/O.
///
/// # Returns
///
/// `Some(shared_layout)` for a recognized grouped format. `None` means the key
/// cannot have a directory or the fetched header is a legacy per-cluster format.
///
/// # Errors
///
/// Propagates decoded-cache, invalidation, range-GET, header parse, arithmetic,
/// and manifest/header cluster-membership errors.
///
/// # Side Effects
///
/// May evict corrupt full bytes, issue one header range GET, update metrics and
/// diagnostics, and insert an immutable layout into process and local caches.
///
/// # Consistency
///
/// Cache reuse is keyed by an immutable object key. Before first insertion, the
/// decoded directory's cluster set must equal the manifest descriptor's set;
/// object bytes cannot silently redefine logical membership.
///
/// # Performance
///
/// Cache hits avoid I/O and parsing. A cold current-format lookup reads only the
/// directory prefix, not vector or SQ payloads.
///
/// # Examples
///
/// A manifest object listing clusters `{4, 5}` whose header lists `{4, 6}`
/// returns an error and is not cached. A matching header is decoded once and
/// shared by subsequent queries through [`Arc`].
async fn load_cluster_object_layout(
    _index: &IvfFlatIndex,
    object: &ClusterFetchObject,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
    stats: Option<&SqSearchByteStats>,
) -> Result<Option<Arc<ClusterObjectLayout>>> {
    if !cluster_object_key_may_have_layout(&object.key, object.clusters.len()) {
        return Ok(None);
    }

    if let Some(c) = cache {
        if let Some(layout) = c.get_decoded::<ClusterObjectLayout>(&object.key)? {
            return Ok(Some(layout));
        }
    }
    if let Some(layout) = cluster_object_layout_cache()
        .get(&object.key)
        .map(|entry| Arc::clone(entry.value()))
    {
        if let Some(c) = cache {
            c.insert_decoded(&object.key, Arc::clone(&layout));
        }
        return Ok(Some(layout));
    }

    let header_len = cluster_object_header_range_len(object.clusters.len())?;
    let header = match cached_full_object_for_range(
        cache,
        &object.key,
        object.size_bytes,
        header_len,
        "header",
        1,
    )
    .await?
    {
        RangeCacheLookup::Local(data) => data.slice(0..header_len),
        RangeCacheLookup::Miss => {
            crate::metrics::RANGE_SOURCE_TOTAL
                .with_label_values(&["header", "s3"])
                .inc();
            let header = store.get_range(&object.key, 0..header_len).await?;
            if let Some(stats) = stats {
                stats.record_get(SqBytePhase::Other, header.len());
            }
            header
        }
        RangeCacheLookup::CorruptEvicted => {
            let header = store.get_range(&object.key, 0..header_len).await?;
            if let Some(stats) = stats {
                stats.record_get(SqBytePhase::Other, header.len());
            }
            header
        }
    };
    let Some(layout) = cluster_object_layout(&header)? else {
        return Ok(None);
    };
    validate_layout_matches_manifest(&object.key, &layout, &object.clusters)?;

    let layout = Arc::new(layout);
    cluster_object_layout_cache().insert(object.key.clone(), Arc::clone(&layout));
    if let Some(c) = cache {
        c.insert_decoded(&object.key, Arc::clone(&layout));
    }
    Ok(Some(layout))
}

/// Cheaply identifies keys that may carry a grouped-object directory.
///
/// # Parameters
///
/// - `key`: Object-store key.
/// - `cluster_count`: Manifest membership count.
///
/// # Returns
///
/// `true` for multi-cluster objects or filenames beginning with
/// `cluster_group_`/`cluster_pair_`; otherwise `false` for legacy per-cluster
/// objects.
///
/// # Examples
///
/// A two-cluster object always returns `true`. A one-cluster key ending in
/// `cluster_7.bin` returns `false` and avoids an unnecessary header GET.
fn cluster_object_key_may_have_layout(key: &str, cluster_count: usize) -> bool {
    cluster_count > 1
        || key
            .rsplit('/')
            .next()
            .map(|filename| {
                filename.starts_with("cluster_group_") || filename.starts_with("cluster_pair_")
            })
            .unwrap_or(false)
}

/// Verifies that decoded object membership equals manifest membership.
///
/// # Parameters
///
/// - `object_key`: Key included in any diagnostic.
/// - `layout`: Parsed grouped-object directory.
/// - `manifest_clusters`: Logical clusters advertised by the segment manifest.
///
/// # Returns
///
/// `Ok(())` when both sets are equal; ordering does not matter.
///
/// # Errors
///
/// Returns an index error showing both sets when any cluster is missing or
/// unexpected. The caller must not trust range offsets after this error.
///
/// # Examples
///
/// Header `[5, 4]` matches manifest `[4, 5]`; header `[4, 6]` does not.
fn validate_layout_matches_manifest(
    object_key: &str,
    layout: &ClusterObjectLayout,
    manifest_clusters: &[usize],
) -> Result<()> {
    let layout_clusters: BTreeSet<usize> = layout
        .sections
        .iter()
        .map(|section| section.cluster_idx)
        .collect();
    let manifest_clusters: BTreeSet<usize> = manifest_clusters.iter().copied().collect();
    if layout_clusters != manifest_clusters {
        return Err(ZeppelinError::Index(format!(
            "cluster object {object_key} layout mismatch: manifest={manifest_clusters:?}, header={layout_clusters:?}"
        )));
    }
    Ok(())
}

/// Converts an absolute object range into a checked borrow of fetched bytes.
///
/// # Parameters
///
/// - `bytes`: Borrowed fetched span.
/// - `absolute`: Desired half-open offsets in complete-object coordinates.
/// - `base_offset`: Complete-object offset corresponding to `bytes[0]`.
/// - `label`: Human-readable range kind for errors.
/// - `object_key`: Object key for diagnostics.
///
/// # Returns
///
/// A borrowed subslice whose lifetime is tied to `bytes`; no allocation or copy
/// occurs.
///
/// # Errors
///
/// Returns an index error when the range begins before the fetched base, is
/// reversed, or extends past the fetched bytes.
///
/// # Examples
///
/// Fetched bytes for absolute `100..200` and desired range `120..140` return
/// `&bytes[20..40]`. Desired `90..110` is rejected.
///
/// # Rust Notes for Java/C Engineers
///
/// The explicit lifetime `'a` states that the returned slice cannot outlive the
/// input buffer. A Java `ByteBuffer.slice()` relies on garbage collection to
/// keep backing storage alive; a C pointer requires manual lifetime tracking.
/// Rust proves this relationship statically and keeps bounds checks explicit.
fn slice_relative_range<'a>(
    bytes: &'a [u8],
    absolute: &Range<usize>,
    base_offset: usize,
    label: &str,
    object_key: &str,
) -> Result<&'a [u8]> {
    if absolute.start < base_offset || absolute.end < absolute.start {
        return Err(ZeppelinError::Index(format!(
            "{label} for {object_key} is outside fetched base: start={}, end={}, base={base_offset}",
            absolute.start, absolute.end
        )));
    }
    let start = absolute.start - base_offset;
    let end = absolute.end - base_offset;
    if end > bytes.len() {
        return Err(ZeppelinError::Index(format!(
            "{label} for {object_key} exceeds fetched bytes: relative_end={end}, len={}",
            bytes.len()
        )));
    }
    Ok(&bytes[start..end])
}

/// Uses product-quantized ADC scores for coarse ranking, then reranks exactly.
///
/// Product quantization divides each stored vector into chunks and replaces each
/// chunk with a codebook index. One query-to-codeword lookup table turns each
/// approximate row score into cheap table additions. As with SQ, filtering is
/// resolved before coarse truncation and full vectors determine returned scores.
///
/// # Parameters
///
/// - `index`: Loaded PQ segment and cluster ownership metadata.
/// - `probe_clusters`: Logical clusters selected for coarse scan.
/// - `query`: Validated full-precision query.
/// - `distance_metric`: Metric used to build ADC and compute exact rerank scores.
/// - `filter`: Optional metadata predicate applied before truncation.
/// - `fetch_k`: Candidate target after filter oversampling.
/// - `store`: Authoritative object-store reader.
/// - `cache`: Optional complete-object cache.
///
/// # Returns
///
/// Exact-distance candidates for at most the retained approximate rerank
/// frontier. The dispatcher performs final order, filter verification, and
/// result shaping.
///
/// # Errors
///
/// Propagates codebook, PQ sidecar, bitmap/attribute, full-object, cache, and
/// decode failures, plus invalid physical cluster mappings. Some parallel reads
/// may complete before another read's error is returned.
///
/// # Panics
///
/// Build-time invariants must keep code widths, IDs, full vectors, and
/// attributes row-aligned. Debug builds also panic if `fetch_k * 4` overflows.
///
/// # Side Effects
///
/// Reads the segment-global codebook, per-cluster PQ payloads, optional bitmap
/// and attribute objects, and full cluster objects. Cache misses may populate
/// the local cache; no immutable artifact or manifest is changed.
///
/// # Performance
///
/// ADC-table construction is proportional to codebook size. Coarse work is
/// `O(scanned_rows * subquantizers)`. At most roughly `4 * fetch_k` rows survive
/// to full-object rerank. Unlike the current SQ v4 path, this implementation
/// loads per-cluster PQ sidecars and parses complete full-vector cluster objects.
///
/// # Examples
///
/// A 16-subquantizer index scores each coarse row with 16 lookup additions. If
/// `fetch_k=25`, at most 100 matching rows are retained, grouped by cluster, and
/// recomputed from exact vectors before the final top 25 are chosen.
///
/// # Rust Notes for Java/C Engineers
///
/// The ADC table and decoded codebook are owned local values, while each async
/// block borrows the store and cache. `HashSet<&str>` borrows candidate ID
/// strings only for the duration of full-cluster matching; Rust prevents those
/// string views from escaping after `needed_ids` is dropped.
#[allow(clippy::too_many_arguments)]
async fn scan_clusters_pq(
    index: &IvfFlatIndex,
    probe_clusters: &[usize],
    query: &[f32],
    distance_metric: DistanceMetric,
    filter: Option<&Filter>,
    fetch_k: usize,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
) -> Result<Vec<Candidate>> {
    use crate::index::quantization::pq::{
        deserialize_pq_cluster, pq_cluster_key, pq_codebook_key, PqCodebook,
    };

    // Load PQ codebook.
    let cb_key = pq_codebook_key(&index.namespace, &index.segment_id);
    let cb_data = fetch_with_cache(cache, store, &cb_key).await?;
    let codebook = PqCodebook::from_bytes(&cb_data)?;

    // Precompute ADC lookup table.
    let adc_table = codebook.build_adc_table(query, distance_metric);

    // Phase 1: Coarse ranking with PQ distances — parallel prefetch.
    let has_bitmaps = !index.bitmap_fields.is_empty();

    // Apply a non-bitmap attribute filter DURING the coarse scan so a selective
    // filter's matches survive truncation (Task 6). Fetch attrs alongside the
    // PQ codes whenever a filter is active.
    let want_attr_filter = filter.is_some();

    let coarse_prefetched = futures::future::join_all(probe_clusters.iter().map(|&cluster_idx| {
        let owner = index.cluster_owner(cluster_idx);
        let pq_key = pq_cluster_key(&index.namespace, owner, cluster_idx);
        async move {
            let (prefilter, pq_res, attrs) = tokio::join!(
                try_bitmap_prefilter(
                    &index.namespace,
                    owner,
                    cluster_idx,
                    filter,
                    has_bitmaps,
                    store,
                    cache,
                ),
                fetch_with_cache(cache, store, &pq_key),
                async {
                    if want_attr_filter {
                        load_attrs(index, cluster_idx, filter, store, cache, None).await
                    } else {
                        Ok(None)
                    }
                },
            );
            (cluster_idx, prefilter, pq_res, attrs)
        }
    }))
    .await;

    let mut coarse_candidates: Vec<(String, f32, usize)> = Vec::new();
    for (cluster_idx, prefilter, pq_res, attrs) in coarse_prefetched {
        let pq_data = pq_res?;
        let attrs = attrs?;
        let pq_cluster = deserialize_pq_cluster(&pq_data)?;

        for (j, codes) in pq_cluster.codes.iter().enumerate() {
            if !coarse_row_passes(filter, &prefilter, &attrs, j) {
                continue;
            }
            let approx_score = codebook.adc_distance(&adc_table, codes);
            coarse_candidates.push((pq_cluster.ids[j].clone(), approx_score, cluster_idx));
        }
    }

    let rerank_count = fetch_k * 4;
    partial_topk_by(
        &mut coarse_candidates,
        rerank_count,
        coarse_pq_candidate_cmp,
    );

    debug!(
        coarse_candidates = coarse_candidates.len(),
        "PQ coarse ranking complete, starting rerank"
    );

    // Phase 2: Rerank with full-precision vectors — parallel prefetch.
    let mut cluster_candidates: HashMap<usize, Vec<String>> = HashMap::new();
    for (id, _, cluster_idx) in &coarse_candidates {
        cluster_candidates
            .entry(*cluster_idx)
            .or_default()
            .push(id.clone());
    }

    let want_rerank_attrs = filter.is_some();
    let rerank_prefetched =
        futures::future::join_all(cluster_candidates.iter().map(|(&cluster_idx, needed_ids)| {
            let cluster_object = cluster_fetch_object(index, cluster_idx);
            let needed_ids = needed_ids.clone();
            async move {
                let cluster_fetch = async {
                    let object = cluster_object?;
                    fetch_with_cache(cache, store, &object.key).await
                };
                let (cluster_res, attrs) = tokio::join!(cluster_fetch, async {
                    if want_rerank_attrs {
                        load_attrs(index, cluster_idx, filter, store, cache, None).await
                    } else {
                        Ok(None)
                    }
                },);
                (cluster_idx, needed_ids, cluster_res, attrs)
            }
        }))
        .await;

    let mut candidates = Vec::new();
    for (cluster_idx, needed_ids, cluster_res, attrs) in rerank_prefetched {
        let cluster_data = cluster_res?;
        let attrs = attrs?;
        let cluster = deserialize_cluster_from_object(&cluster_data, cluster_idx)?;

        let needed_set: HashSet<&str> = needed_ids.iter().map(|s| s.as_str()).collect();

        for (j, id) in cluster.ids.iter().enumerate() {
            if needed_set.contains(id.as_str()) {
                let score = compute_distance(query, &cluster.vectors[j], distance_metric);
                let vector_attrs = attrs.as_ref().and_then(|a| a.get(j)).cloned().flatten();
                candidates.push(Candidate {
                    id: id.clone(),
                    score,
                    attributes: vector_attrs,
                    cluster_idx,
                    row_idx: j,
                });
            }
        }
    }

    Ok(candidates)
}

/// Tries to resolve a metadata filter into allowed row positions for one cluster.
///
/// Bitmap indexes accelerate supported predicates but are optional. Any absent,
/// unreadable, undecodable, or inexpressible bitmap path returns `None`, causing
/// the caller to load attributes and use exact filter evaluation. That fallback
/// preserves semantics rather than silently accepting or rejecting rows.
///
/// # Parameters
///
/// - `namespace`: Namespace prefix for the immutable bitmap key.
/// - `segment_id`: Physical owner of this cluster's per-cluster objects.
/// - `cluster_idx`: Logical cluster index and bitmap row coordinate space.
/// - `filter`: Optional predicate to compile against bitmap postings.
/// - `has_bitmaps`: Whether manifest metadata advertises any bitmap fields.
/// - `store`: Authoritative object-store reader.
/// - `cache`: Optional complete-object cache.
///
/// # Returns
///
/// `Some(bitmap)` when the complete filter is resolved, with set bits naming
/// allowed row positions. `None` requests exact attribute evaluation.
///
/// # Side Effects
///
/// May perform and cache one bitmap-object GET. Decode failures emit a debug
/// event; storage/cache failures are intentionally converted to `None`.
///
/// # Performance
///
/// A successful bitmap can avoid per-row distance and filter work, but loading
/// it adds one cache lookup or object GET per cluster. Unsupported filters skip
/// only after loading the advertised cluster bitmap.
///
/// # Examples
///
/// A bitmap-resolvable `color == "red"` predicate may return positions
/// `{2, 9, 11}`. A token predicate absent from the bitmap index returns `None`,
/// so exact row attributes decide matches.
async fn try_bitmap_prefilter(
    namespace: &str,
    segment_id: &str,
    cluster_idx: usize,
    filter: Option<&Filter>,
    has_bitmaps: bool,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
) -> Option<roaring::RoaringBitmap> {
    let filter = filter?;
    if !has_bitmaps {
        return None;
    }

    let bkey = bitmap_key(namespace, segment_id, cluster_idx);
    let data = match fetch_with_cache(cache, store, &bkey).await {
        Ok(d) => d,
        Err(_) => return None,
    };
    let bitmap_index = match ClusterBitmapIndex::from_bytes(&data) {
        Ok(idx) => idx,
        Err(e) => {
            tracing::debug!(cluster = cluster_idx, error = %e, "failed to load bitmap index");
            return None;
        }
    };

    evaluate_filter_bitmap(filter, &bitmap_index)
}

/// Decides whether one row may enter a quantized coarse-ranking frontier.
///
/// A resolved bitmap is the fast path. Otherwise the function evaluates a
/// present filter against that row's attributes before approximate top-k
/// truncation; missing attributes reject the row. With neither bitmap nor
/// filter, every row passes. Hierarchical leaf search shares this helper because
/// it has the same two-phase truncation hazard.
///
/// # Parameters
///
/// - `filter`: Optional exact predicate.
/// - `prefilter`: Optional complete bitmap result in cluster row coordinates.
/// - `attrs`: Optional row-aligned cluster attributes.
/// - `j`: Zero-based row position.
///
/// # Returns
///
/// `true` when the row may participate in approximate ranking. A bitmap result
/// takes precedence; callers later perform final exact filter verification.
///
/// # Examples
///
/// If the bitmap contains row 7, row 7 passes without consulting attributes.
/// Without a bitmap, row 7 passes only when its attribute map satisfies the
/// filter. An unfiltered row always passes.
///
/// # Rust Notes for Java/C Engineers
///
/// Nested `Option` values distinguish “no cluster attribute object,” “no row at
/// this position,” and “row has no map” without sentinel pointers. Chained
/// `and_then` borrows through each layer and produces a non-null map only when
/// every condition succeeds.
pub(crate) fn coarse_row_passes(
    filter: Option<&Filter>,
    prefilter: &Option<roaring::RoaringBitmap>,
    attrs: &Option<Vec<Option<HashMap<String, AttributeValue>>>>,
    j: usize,
) -> bool {
    if let Some(bm) = prefilter {
        return bm.contains(j as u32);
    }
    match filter {
        None => true,
        Some(f) => match attrs
            .as_ref()
            .and_then(|a| a.get(j))
            .and_then(|a| a.as_ref())
        {
            Some(a) => evaluate_filter(f, a),
            None => false,
        },
    }
}

/// Loads attributes only for final unfiltered winners and builds API results.
///
/// Unfiltered scans avoid attribute I/O during ranking. When the caller requests
/// attributes, this helper deduplicates winner clusters, loads their immutable
/// attribute objects concurrently, and joins maps back by retained cluster/row
/// coordinates without changing candidate order.
///
/// # Parameters
///
/// - `index`: Loaded segment ownership and attribute-presence metadata.
/// - `candidates`: Ordered final candidate frontier, consumed by this helper.
/// - `store`: Authoritative object-store reader.
/// - `cache`: Optional complete-object cache.
/// - `stats`: Optional SQ diagnostics charged for attribute reads.
///
/// # Returns
///
/// Owned [`SearchResult`] values in the same order as `candidates`, each with
/// its row's optional attribute map. Empty candidates return immediately.
///
/// # Errors
///
/// Propagates attribute read/decode failures and returns index errors if a
/// completed cluster lookup or row coordinate is unexpectedly absent.
///
/// # Side Effects
///
/// May perform one attribute GET per distinct winner cluster, populate the
/// cache, and update SQ supporting-byte diagnostics.
///
/// # Performance
///
/// Attribute I/O is proportional to distinct winner clusters rather than all
/// scanned clusters. The function clones only winning row maps into results.
///
/// # Examples
///
/// Ten winners spread across three clusters cause three concurrent attribute
/// loads, not ten. A sketch proving one cluster has no non-null maps avoids that
/// cluster's GET and returns `attributes: None` for its winners.
async fn enrich_unfiltered_results(
    index: &IvfFlatIndex,
    candidates: Vec<Candidate>,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
    stats: Option<&SqSearchByteStats>,
) -> Result<Vec<SearchResult>> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    let mut seen = HashSet::new();
    let mut cluster_indices = Vec::new();
    for candidate in &candidates {
        if seen.insert(candidate.cluster_idx) {
            cluster_indices.push(candidate.cluster_idx);
        }
    }

    let attrs_fetches =
        futures::future::join_all(cluster_indices.iter().map(|&cluster_idx| async move {
            (
                cluster_idx,
                load_attrs(index, cluster_idx, None, store, cache, stats).await,
            )
        }))
        .await;

    let mut attrs_by_cluster: HashMap<usize, Option<ClusterAttrs>> = HashMap::new();
    for (cluster_idx, attrs) in attrs_fetches {
        attrs_by_cluster.insert(cluster_idx, attrs?);
    }

    let mut results = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let cluster_attrs = attrs_by_cluster
            .get(&candidate.cluster_idx)
            .ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "missing attrs for final result cluster {}",
                    candidate.cluster_idx
                ))
            })?;
        let attributes = match cluster_attrs {
            Some(cluster_attrs) => cluster_attrs
                .get(candidate.row_idx)
                .ok_or_else(|| {
                    ZeppelinError::Index(format!(
                        "attrs row {} missing in cluster {}",
                        candidate.row_idx, candidate.cluster_idx
                    ))
                })?
                .clone(),
            None => None,
        };
        results.push(SearchResult {
            id: candidate.id,
            score: candidate.score,
            attributes,
        });
    }

    Ok(results)
}

/// Loads and decodes row-aligned attributes for one logical cluster when needed.
///
/// The resident sketch carries a conservative “cluster may have attributes”
/// bit. A proven-empty cluster returns `None` without a GET; otherwise the key
/// is built under the cluster's physical owner so carried-over incremental
/// compaction data remains addressable.
///
/// # Parameters
///
/// - `index`: Loaded segment and ownership/sketch metadata.
/// - `cluster_idx`: Logical cluster whose row attributes are requested.
/// - `_filter`: Present for call-site symmetry; current loading behavior does
///   not inspect the predicate.
/// - `store`: Authoritative object-store reader.
/// - `cache`: Optional complete-object cache.
/// - `stats`: Optional SQ diagnostics charged to supporting I/O.
///
/// # Returns
///
/// `Some(rows)` when an attribute object is loaded, preserving one optional map
/// per cluster row. `None` means the resident sketch proves the cluster contains
/// no non-null attributes.
///
/// # Errors
///
/// Propagates cache, object-store, and attribute decode errors. A cluster that
/// may have attributes but lacks its immutable object fails loudly.
///
/// # Side Effects
///
/// May fetch and cache one complete attribute object and update diagnostics.
///
/// # Consistency
///
/// Keys use [`IvfFlatIndex::cluster_owner`]; the active segment ID must not
/// override the manifest's older owner for a carried-over cluster.
///
/// # Examples
///
/// Cluster 6 carried from `seg-3` while the active index is `seg-4` loads
/// `seg-3`'s attribute object. If the resident sketch's bit is clear, it performs
/// no read and returns `None`.
async fn load_attrs(
    index: &IvfFlatIndex,
    cluster_idx: usize,
    _filter: Option<&Filter>,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
    stats: Option<&SqSearchByteStats>,
) -> Result<Option<ClusterAttrs>> {
    if !index.cluster_may_have_attrs(cluster_idx) {
        return Ok(None);
    }
    let akey = attrs_key(
        &index.namespace,
        index.cluster_owner(cluster_idx),
        cluster_idx,
    );
    let data = fetch_with_cache_counted(cache, store, &akey, stats, SqBytePhase::Other).await?;
    Ok(Some(deserialize_attrs(&data)?))
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Unit tests for validation short circuits, adaptive read budgets, grouped
    //! object expansion, and deterministic exact-vector range coalescing.
    //!
    //! Tests that call the async entry point use an in-memory object store and
    //! deliberately stop before artifact reads. They isolate pure search
    //! contracts without pretending the in-memory backend validates S3 behavior;
    //! integration coverage exercises real cache/object-store interaction.

    use super::*;

    /// Builds a minimal two-centroid handle for validation-only search tests.
    ///
    /// # Returns
    ///
    /// An unquantized, ungrouped index with dimension two and no resident
    /// artifacts. Tests must not attempt a nonempty scan because no cluster
    /// objects are installed in the backing store.
    ///
    /// # Examples
    ///
    /// A three-component query fails before this fixture needs object data, and
    /// `top_k=0` returns before cluster reads.
    fn make_index() -> IvfFlatIndex {
        IvfFlatIndex {
            centroids: std::sync::Arc::new(vec![vec![0.0, 0.0], vec![10.0, 10.0]]),
            num_vectors: 4,
            dim: 2,
            namespace: "test_ns".to_string(),
            segment_id: "seg_001".to_string(),
            quantization: QuantizationType::None,
            sq_calibration: None,
            bitmap_fields: Vec::new(),
            cluster_owners: Vec::new(),
            cluster_objects: Vec::new(),
            cluster_object_by_cluster: Vec::new(),
            resident_sketch: None,
            sketch_ref: None,
            bootstrap_ref: None,
            membership_ref: None,
        }
    }

    /// Builds a production-v4 sketch over one deterministic row per cluster.
    ///
    /// When `attach_centroids` is false, the returned sketch is decoded from
    /// its immutable bytes but intentionally not prepared for query scoring.
    /// That fail-loud state lets bypass tests prove that ADC was never entered.
    fn make_v4_test_index(cluster_count: usize, attach_centroids: bool) -> IvfFlatIndex {
        assert!(cluster_count > 0);
        let centroids: Vec<Vec<f32>> = (0..cluster_count)
            .map(|cluster_idx| vec![cluster_idx as f32, 0.0])
            .collect();
        let cluster_vecs: Vec<Vec<Vec<f32>>> = centroids
            .iter()
            .map(|centroid| vec![centroid.clone()])
            .collect();
        let cluster_attrs = vec![vec![None]; cluster_count];
        let (sketch_ref, bytes, attached_sketch) =
            crate::index::ivf_flat::sketch::build_resident_sketch(
                "test_ns",
                "seg_001",
                2,
                &centroids,
                &cluster_vecs,
                &cluster_attrs,
            )
            .unwrap();
        let resident_sketch = if attach_centroids {
            attached_sketch
        } else {
            crate::index::ivf_flat::sketch::ResidentSketch::from_bytes(&bytes).unwrap()
        };

        let mut index = make_index();
        index.centroids = std::sync::Arc::new(centroids);
        index.num_vectors = cluster_count;
        index.resident_sketch = Some(std::sync::Arc::new(resident_sketch));
        index.sketch_ref = Some(sketch_ref);
        index
    }

    #[test]
    /// Protects the public query-width validation and structured error payload.
    ///
    /// A regression that deferred validation until distance calculation could
    /// panic or perform needless I/O instead of returning expected/actual widths.
    fn test_dimension_mismatch() {
        let index = make_index();
        let query = vec![1.0, 2.0, 3.0]; // dim=3 vs index dim=2

        let rt = tokio::runtime::Runtime::new().unwrap();
        let store = rt.block_on(async {
            let mem = std::sync::Arc::new(object_store::memory::InMemory::new());
            ZeppelinStore::new(mem)
        });

        let result = rt.block_on(search_ivf_flat(
            &index,
            &query,
            10,
            2,
            None,
            DistanceMetric::Euclidean,
            &store,
            3,
            None,
            true,
            crate::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        ));
        assert!(result.is_err());
        match result.unwrap_err() {
            ZeppelinError::DimensionMismatch { expected, actual } => {
                assert_eq!(expected, 2);
                assert_eq!(actual, 3);
            }
            other => panic!("expected DimensionMismatch, got: {other}"),
        }
    }

    #[test]
    /// Proves zero requested results short-circuit before immutable object reads.
    ///
    /// The query still has the correct dimension, distinguishing this path from
    /// dimension validation and ensuring the empty result is successful.
    fn test_top_k_zero() {
        let index = make_index();
        let query = vec![1.0, 2.0];

        let rt = tokio::runtime::Runtime::new().unwrap();
        let store = rt.block_on(async {
            let mem = std::sync::Arc::new(object_store::memory::InMemory::new());
            ZeppelinStore::new(mem)
        });

        let results = rt
            .block_on(search_ivf_flat(
                &index,
                &query,
                0,
                2,
                None,
                DistanceMetric::Euclidean,
                &store,
                3,
                None,
                true,
                crate::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
            ))
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    /// A metadata filter preserves the complete centroid probe set for v4.
    ///
    /// The fixture deliberately leaves the resident sketch unattached from its
    /// authoritative centroids. Any attempted sketch scoring would therefore
    /// fail loudly, so success also proves the filtered path bypasses ADC.
    fn v4_filtered_query_preserves_every_probe_without_scoring() {
        let index = make_v4_test_index(16, false);
        let probe_clusters = vec![15, 3, 7, 0];
        let filter = Filter::Eq {
            field: "color".to_string(),
            value: AttributeValue::String("blue".to_string()),
        };

        let scan_clusters = select_scan_clusters(
            &index,
            &[15.0, 0.0],
            DistanceMetric::Euclidean,
            Some(&filter),
            &probe_clusters,
            probe_clusters.len(),
            4,
        )
        .unwrap();

        assert_eq!(scan_clusters, probe_clusters);
    }

    #[test]
    /// Scale-aware full-retention probe points bypass v4 sketch scoring.
    ///
    /// The sketch is valid but unattached, so each successful call proves the
    /// no-prune decision happens structurally before ADC preparation.
    fn v4_scale_aware_sentinels_are_structural_noops() {
        let index = make_v4_test_index(126, false);

        for nprobe in [32usize, 48, 63, 126] {
            let probe_clusters: Vec<usize> = (0..nprobe).collect();
            let scan_clusters = select_scan_clusters(
                &index,
                &[125.0, 0.0],
                DistanceMetric::Euclidean,
                None,
                &probe_clusters,
                nprobe,
                4,
            )
            .unwrap();

            assert_eq!(scan_clusters, probe_clusters, "nprobe={nprobe}");
        }
    }

    #[test]
    /// The diagnostic nprobe-16 point still uses v4 adaptive ranking.
    ///
    /// One row sits exactly on each centroid. A query on centroid 15 makes the
    /// four nearest rows an independent expected prefix, while the nprobe-16
    /// policy's four-cluster floor stops once the remaining mass drops to zero.
    fn v4_nprobe16_uses_adaptive_budget_and_ranking() {
        let index = make_v4_test_index(16, true);
        let probe_clusters: Vec<usize> = (0..16).collect();

        let scan_clusters = select_scan_clusters(
            &index,
            &[15.0, 0.0],
            DistanceMetric::Euclidean,
            None,
            &probe_clusters,
            16,
            4,
        )
        .unwrap();

        assert_eq!(adaptive_sketch_budget(16).max_clusters(), 14);
        assert_eq!(scan_clusters, vec![15, 14, 13, 12]);
    }

    #[test]
    /// Pins monotonic adaptive caps and sentinel points used by read budgeting.
    ///
    /// This catches a tuning-formula change that would reduce the cap as
    /// `nprobe` grows, exceed the probe set, or alter calibrated small/full-scan
    /// checkpoints and grouped arity conversion.
    fn adaptive_sketch_cap_scales_monotonically() {
        let mut prev = 0usize;
        for nprobe in 1..=128 {
            let budget = adaptive_sketch_budget(nprobe);
            let cap = budget.max_clusters();
            assert!(
                cap >= prev,
                "budget cap must be monotonic: nprobe={nprobe} cap={cap} prev={prev}"
            );
            assert!(cap <= nprobe);
            prev = cap;
        }

        assert_eq!(adaptive_sketch_budget(8).max_clusters(), 8);
        assert_eq!(adaptive_sketch_budget(16).max_clusters(), 14);
        // Scale-aware IVF defaults begin at 32 probes. These measured policy
        // points deliberately disable sketch pruning so the coarse sketch
        // cannot erase recall bought by the larger probe set; grouped objects
        // still coalesce their physical GETs.
        assert_eq!(adaptive_sketch_budget(32).max_clusters(), 32);
        assert_eq!(adaptive_sketch_budget(48).max_clusters(), 48);
        assert_eq!(adaptive_sketch_budget(63).max_clusters(), 63);
        assert_eq!(adaptive_sketch_budget(126).max_clusters(), 126);
        assert_eq!(adaptive_sketch_budget(128).max_clusters(), 128);
        assert_eq!(grouped_object_cap_for_arity(4, 16), 6);
        assert_eq!(grouped_object_cap_for_arity(4, 48), 48);
        assert_eq!(grouped_object_cap_for_arity(4, 63), 63);
        assert_eq!(grouped_object_cap_for_arity(4, 126), 126);
        assert_eq!(grouped_object_cap_for_arity(4, 128), 128);
    }

    #[test]
    /// Ensures a touched grouped object expands to all of its logical clusters.
    ///
    /// Without a resident sketch, selecting cluster 0 must include sibling 1
    /// because both arrive in the same physical bytes. A regression returning
    /// only 0 would make parsing/scan accounting inconsistent with the GET.
    fn select_scan_clusters_expands_touched_grouped_object_without_sketch() {
        let mut index = make_index();
        index.centroids = std::sync::Arc::new(vec![
            vec![0.0, 0.0],
            vec![1.0, 1.0],
            vec![10.0, 10.0],
            vec![11.0, 11.0],
        ]);
        index.num_vectors = 4;
        index.cluster_objects = vec![
            crate::wal::manifest::ClusterDataObjectRef {
                key: "test_ns/segments/seg_001/cluster_group_0.bin".to_string(),
                clusters: vec![0, 1],
                live_offset: 0,
                live_len: 0,
                size_bytes: 0,
            },
            crate::wal::manifest::ClusterDataObjectRef {
                key: "test_ns/segments/seg_001/cluster_group_1.bin".to_string(),
                clusters: vec![2, 3],
                live_offset: 0,
                live_len: 0,
                size_bytes: 0,
            },
        ];
        index.cluster_object_by_cluster = vec![0, 0, 1, 1];

        let scan_clusters = select_scan_clusters(
            &index,
            &[0.0, 0.0],
            DistanceMetric::Euclidean,
            None,
            &[0],
            1,
            1,
        )
        .unwrap();

        assert_eq!(scan_clusters, vec![0, 1]);
    }

    /// Constructs one minimal logical rerank request for coalescing tests.
    ///
    /// # Parameters
    ///
    /// - `range`: Absolute vector byte span, cloned into both need metadata and
    ///   the physical request field.
    ///
    /// # Returns
    ///
    /// A request for cluster and row zero with a stable placeholder ID.
    ///
    /// # Examples
    ///
    /// `rerank_request(100..116)` models one four-float vector at that object
    /// offset.
    fn rerank_request(range: Range<usize>) -> RerankRangeRequest {
        RerankRangeRequest {
            cluster_idx: 0,
            need: RerankNeed {
                id: "id".to_string(),
                row_idx: 0,
                vector_range: Some(range.clone()),
            },
            range,
        }
    }

    #[test]
    /// Verifies unsorted logical requests are sorted and small gaps are merged.
    ///
    /// It also pins original request indexes, which are required to restore
    /// candidate order after physical range responses arrive.
    fn coalesce_rerank_ranges_sorts_and_merges_small_gaps() {
        let requests = vec![
            rerank_request(300..310),
            rerank_request(100..110),
            rerank_request(120..130),
        ];

        let coalesced = coalesce_rerank_ranges(&requests, 11).unwrap();

        assert_eq!(
            coalesced,
            vec![
                CoalescedRerankRange {
                    range: 100..130,
                    request_indices: vec![1, 2],
                },
                CoalescedRerankRange {
                    range: 300..310,
                    request_indices: vec![0],
                },
            ]
        );
    }

    #[test]
    /// Pins the coalescing threshold as exclusive rather than inclusive.
    ///
    /// A ten-byte gap with threshold ten must remain two GETs; changing this
    /// boundary would silently alter the configured byte-versus-request tradeoff.
    fn coalesce_rerank_ranges_does_not_merge_gap_equal_to_threshold() {
        let requests = vec![rerank_request(100..110), rerank_request(120..130)];

        let coalesced = coalesce_rerank_ranges(&requests, 10).unwrap();

        assert_eq!(
            coalesced,
            vec![
                CoalescedRerankRange {
                    range: 100..110,
                    request_indices: vec![0],
                },
                CoalescedRerankRange {
                    range: 120..130,
                    request_indices: vec![1],
                },
            ]
        );
    }

    #[test]
    /// Rejects empty logical vector spans before any object-store request.
    ///
    /// Accepting `start == end` would later yield a zero-length vector or make a
    /// coalesced request impossible to reconstruct exactly.
    fn coalesce_rerank_ranges_rejects_empty_ranges() {
        let requests = vec![rerank_request(100..100)];

        let err = coalesce_rerank_ranges(&requests, 1024).unwrap_err();

        assert!(err.to_string().contains("invalid rerank vector range"));
    }
}
