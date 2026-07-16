//! Query execution for immutable hierarchical IVF segments.
//!
//! A beam search keeps the nearest routing choices at each tree level instead
//! of following only one centroid. `beam_width` is therefore the hierarchical
//! equivalent of IVF-Flat's `nprobe`: larger values inspect more branches and
//! usually improve recall, but increase tree-node reads, leaf reads, bytes, and
//! distance calculations. Search ranks lower distance scores first.
//!
//! Tree depth is not necessarily uniform. K-means can produce a small group
//! beside a group that needs more partitioning, so one routing node may contain
//! numeric leaf IDs and non-numeric node IDs together. Traversal scans leaves as
//! they appear, continues down internal children, and merges exact candidates at
//! the end.
//!
//! ```text
//! query + manifest-selected HierarchicalIndex
//!                    |
//!                    v
//!          fetch and score root node
//!                    |
//!             keep nearest beam
//!                    |
//!         +----------+-----------+
//!         |                      |
//!         v                      v
//! numeric child             node child
//! scan leaf now       parallel fetch next level
//!         |                      |
//!         +----------+-----------+
//!                    |
//!                    v
//!      flat scan OR SQ8/PQ coarse ranking
//!                    |
//!       optional bitmap/attribute filtering
//!                    |
//!                    v
//!         full-precision final distances
//!                    |
//!                    v
//!       best top_k + lazy attribute enrichment
//! ```
//!
//! The namespace manifest remains authoritative for segment selection and for
//! declaring bitmap fields. Node, cluster, attribute, and quantization objects
//! are immutable. The optional [`DiskCache`](crate::cache::DiskCache)
//! accelerates memory → disk → S3
//! lookup but cannot make an unreferenced segment visible or replace a missing
//! required artifact.
//!
//! ## Reading map
//!
//! 1. Start with
//!    [`search_hierarchical`](crate::index::hierarchical::search::search_hierarchical)
//!    for validation and mixed-depth routing.
//! 2. Follow `scan_leaf_clusters` for quantization dispatch and exact filtering.
//! 3. Compare `scan_clusters_flat`, `scan_clusters_sq`, and `scan_clusters_pq`.
//! 4. Read `finalize_candidates` and `enrich_unfiltered_results` for top-k and
//!    lazy response attributes.
//! 5. Read `try_bitmap_prefilter` beside `load_attrs` to understand why a bitmap
//!    miss affects performance but not exact filter semantics.
//!
//! ## Invariants
//!
//! - The query dimension equals the segment dimension before any object read.
//! - IDs, vector rows, quantized codes, attributes, and bitmap positions remain
//!   row-aligned within each leaf cluster.
//! - Approximate SQ8/PQ scores choose rerank candidates only; returned scores are
//!   recomputed from full-precision vectors.
//! - Filters are applied before quantized truncation and again to selected
//!   candidates, preventing selective predicates from being silently discarded.
//! - Bitmap evaluation is an optional prefilter. Unsupported, missing, or corrupt
//!   bitmap data falls back to exact attributes rather than excluding rows.
//! - Required tree, vector, quantization, or attribute artifacts fail the query
//!   loudly when they cannot be loaded or decoded.
//!
//! ## Rust concepts used here
//!
//! [`futures::future::join_all`] and [`tokio::join!`] overlap independent reads
//! within one level or phase, while `.await` between levels preserves the tree's
//! data dependency. No detached task outlives a query. Java might compose
//! `CompletableFuture` values; C would need an event loop or explicit threads.
//!
//! `Option<&Arc<DiskCache>>` borrows an optional shared cache without increasing
//! its reference count. Owned candidate strings and attribute maps may outlive
//! fetched byte buffers, while borrowed query and filter values are guaranteed
//! valid for every awaited operation. Exhaustive matching on
//! [`QuantizationType`] forces each encoding to choose a scan path.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use tracing::debug;

use crate::cache::DiskCache;
use crate::error::{Result, ZeppelinError};
use crate::index::distance::compute_distance;
use crate::index::filter::{evaluate_filter, oversampled_k};
use crate::index::ivf_flat::build::{
    attrs_key, cluster_key, deserialize_attrs, deserialize_cluster,
    deserialize_colocated_sq_cluster,
};
use crate::index::ivf_flat::search::coarse_row_passes;
use crate::index::quantization::QuantizationType;
use crate::index::topk::partial_topk_by;
use crate::storage::ZeppelinStore;
use crate::types::{AttributeValue, DistanceMetric, Filter, SearchResult};

use super::{deserialize_tree_node, tree_node_key, HierarchicalIndex};

use crate::index::bitmap::evaluate::evaluate_filter_bitmap;
use crate::index::bitmap::{bitmap_key, ClusterBitmapIndex};

/// Row-aligned optional attribute maps decoded for one leaf cluster.
///
/// The outer vector position matches vector, ID, code, and bitmap row indexes;
/// an inner `None` means that row has no attributes.
type ClusterAttrs = Vec<Option<HashMap<String, AttributeValue>>>;

#[derive(Clone, Default)]
struct ArtifactReadTrace(Arc<Mutex<BTreeSet<String>>>);

impl ArtifactReadTrace {
    fn record(&self, key: &str) {
        self.0
            .lock()
            .unwrap_or_else(|_| panic!("hierarchical artifact-read trace lock poisoned"))
            .insert(key.to_string());
    }

    fn snapshot(&self) -> BTreeSet<String> {
        self.0
            .lock()
            .unwrap_or_else(|_| panic!("hierarchical artifact-read trace lock poisoned"))
            .clone()
    }
}

tokio::task_local! {
    static ARTIFACT_READ_TRACE: ArtifactReadTrace;
}

fn record_artifact_read(key: &str) {
    let _outside_receipt_traced_search = ARTIFACT_READ_TRACE.try_with(|trace| trace.record(key));
}

/// Owned internal result retained between leaf scanning and final projection.
///
/// `cluster_idx` and `row_idx` preserve the location needed for lazy attribute
/// enrichment when an unfiltered scan intentionally avoids reading attrs.
struct Candidate {
    /// Stable vector identifier used as the deterministic score tie-breaker.
    id: String,
    /// Full-precision distance; lower values rank ahead of higher values.
    score: f32,
    /// Attributes loaded during filtered scans, or deferred for unfiltered ones.
    attributes: Option<HashMap<String, AttributeValue>>,
    /// Leaf cluster containing the vector and its attribute row.
    cluster_idx: usize,
    /// Zero-based row shared by vector and attribute artifacts.
    row_idx: usize,
}

/// Orders final candidates by ascending distance and then vector ID.
///
/// # Parameters
///
/// - `a`: First borrowed candidate.
/// - `b`: Second borrowed candidate.
///
/// # Returns
///
/// A total ordering suitable for deterministic top-k selection, including NaN
/// scores through [`f32::total_cmp`].
///
/// # Examples
///
/// Distance `0.2` precedes `0.5`; equal distances order ID `a` before `b`.
fn candidate_distance_cmp(a: &Candidate, b: &Candidate) -> Ordering {
    a.score.total_cmp(&b.score).then_with(|| a.id.cmp(&b.id))
}

/// Orders quantized coarse tuples by approximate distance and then vector ID.
///
/// # Parameters
///
/// - `a`: First `(id, approximate_distance, cluster_index)` tuple.
/// - `b`: Second tuple.
///
/// # Returns
///
/// A deterministic best-first order. Cluster index does not break ties because
/// vector IDs are expected to identify rows across the segment.
fn coarse_candidate_cmp(a: &(String, f32, usize), b: &(String, f32, usize)) -> Ordering {
    a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0))
}

/// Fetches immutable artifact bytes through the cache when one is available.
///
/// # Parameters
///
/// - `cache`: Optional shared memory/disk cache.
/// - `store`: Authoritative object-store abstraction used on a cache miss.
/// - `key`: Complete immutable segment-artifact key.
///
/// # Returns
///
/// Shared immutable bytes from memory, disk, or object storage.
///
/// # Errors
///
/// Propagates cache read/write or object-store failures. Required artifacts do
/// not silently become empty bytes.
///
/// # Side Effects
///
/// A cold cached lookup may perform one GET and populate disk and memory tiers.
///
/// # Consistency
///
/// Cache entries are copies of immutable segment objects. The manifest-selected
/// key, not cache contents, determines which segment is queried.
///
/// # Performance
///
/// Warm memory is cheapest, followed by local disk; a cold miss incurs an
/// object-store GET and cache population.
///
/// # Examples
///
/// The first query may fetch `node_root.bin` from S3; a later query for the same
/// immutable segment can receive the same bytes from memory or disk.
async fn fetch_with_cache(
    cache: Option<&Arc<DiskCache>>,
    store: &ZeppelinStore,
    key: &str,
) -> Result<bytes::Bytes> {
    let bytes = if let Some(c) = cache {
        c.get_or_fetch(key, || store.get(key)).await
    } else {
        store.get(key).await
    }?;
    record_artifact_read(key);
    Ok(bytes)
}

/// Searches one hierarchical segment with mixed-depth beam traversal.
///
/// The root is loaded first. At every subsequent depth, all currently selected
/// node objects are fetched concurrently, all their child centroids compete in
/// one best-first beam, numeric children are scanned, and non-numeric children
/// continue downward. Candidates from leaves encountered at different depths
/// are merged by exact distance before return.
///
/// # Parameters
///
/// - `index`: Metadata-only handle for the manifest-selected immutable segment.
/// - `query`: Borrowed query vector whose length must equal the segment dimension.
/// - `top_k`: Maximum results to return. Zero returns an empty vector after
///   dimension validation and performs no object read.
/// - `beam_width`: Maximum routing children retained at each level. Zero is
///   normalized to one rather than disabling traversal.
/// - `filter`: Optional exact metadata predicate. Bitmap sidecars may prune rows
///   first when the manifest declares indexed fields.
/// - `distance_metric`: Metric used both for centroid routing and exact ranking.
/// - `store`: Object-store abstraction for cold artifact reads.
/// - `oversample_factor`: Multiplier used to retain extra filtered candidates
///   before exact predicate application.
/// - `cache`: Optional shared tiered cache for immutable artifacts.
/// - `include_attributes`: Whether final [`SearchResult`] values include attrs.
///
/// # Returns
///
/// Up to `top_k` results sorted by ascending exact distance and vector ID.
/// Fewer results can be returned when the approximate beam does not visit enough
/// matching rows. Attribute maps are omitted when `include_attributes` is false.
///
/// # Errors
///
/// Returns [`ZeppelinError::DimensionMismatch`] before I/O for a wrong query
/// length. Propagates cache, object-store, node/cluster/codebook/calibration,
/// serialization, and required attribute errors. Search performs no remote
/// writes, so a failure leaves persisted state unchanged.
///
/// # Side Effects
///
/// Performs cache lookups and object GETs and emits structured debug events. A
/// cold cached read can populate local cache tiers.
///
/// # Consistency
///
/// `index` must come from the current manifest-selected segment. The function
/// trusts immutable artifacts under that prefix and never discovers or publishes
/// a segment independently.
///
/// # Performance
///
/// Tree depths are sequential roundtrip stages, while node GETs within a depth
/// are parallel. Leaf GET count depends on beam width, mixed-depth branches,
/// filters, quantization, cache hits, and final attrs. SQ8/PQ add coarse and exact
/// rerank phases. Wider beams increase recall and cost.
///
/// # Examples
///
/// A beam of four may select one numeric leaf and three internal nodes at the
/// root. Search scans that leaf, fetches the three nodes in parallel, continues
/// their nearest children, then merges all full-precision candidates into one
/// top ten.
///
/// A 127-component query against a 128-dimensional segment fails before loading
/// the root. A missing root object also fails the query rather than scanning an
/// arbitrary leaf or returning an apparently valid empty result.
///
/// # Rust Notes for Java/C Engineers
///
/// The function borrows every external dependency across `.await`; the compiler
/// ensures they remain valid for the future's lifetime. Values moved into each
/// `async move` block are owned by that level's read future, avoiding dangling C
/// pointers or Java-style accidental mutation of loop variables. Awaiting
/// `join_all` joins every read before decoded nodes are consumed.
#[allow(clippy::too_many_arguments)]
pub async fn search_hierarchical(
    index: &HierarchicalIndex,
    query: &[f32],
    top_k: usize,
    beam_width: usize,
    filter: Option<&Filter>,
    distance_metric: DistanceMetric,
    store: &ZeppelinStore,
    oversample_factor: usize,
    cache: Option<&Arc<DiskCache>>,
    include_attributes: bool,
) -> Result<Vec<SearchResult>> {
    Ok(search_hierarchical_with_trace(
        index,
        query,
        top_k,
        beam_width,
        filter,
        distance_metric,
        store,
        oversample_factor,
        cache,
        include_attributes,
    )
    .await?
    .results)
}

/// Results plus the exact leaf-cluster traversal selected by beam search.
pub(crate) struct HierarchicalSearchOutput {
    pub(crate) results: Vec<SearchResult>,
    pub(crate) probed_centroids: Vec<usize>,
    pub(crate) probed_routing_nodes: Vec<String>,
    pub(crate) touched_artifacts: BTreeSet<String>,
}

fn require_inventoried_routing_node(
    routing_inventory: Option<&HashSet<&str>>,
    node_id: &str,
) -> Result<()> {
    if routing_inventory.is_some_and(|inventory| !inventory.contains(node_id)) {
        return Err(ZeppelinError::Index(format!(
            "hierarchical traversal discovered routing node {node_id} outside the authoritative inventory"
        )));
    }
    Ok(())
}

/// Execute hierarchical search while retaining production leaf selections.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn search_hierarchical_with_trace(
    index: &HierarchicalIndex,
    query: &[f32],
    top_k: usize,
    beam_width: usize,
    filter: Option<&Filter>,
    distance_metric: DistanceMetric,
    store: &ZeppelinStore,
    oversample_factor: usize,
    cache: Option<&Arc<DiskCache>>,
    include_attributes: bool,
) -> Result<HierarchicalSearchOutput> {
    let trace = ArtifactReadTrace::default();
    let mut output = ARTIFACT_READ_TRACE
        .scope(
            trace.clone(),
            search_hierarchical_with_trace_inner(
                index,
                query,
                top_k,
                beam_width,
                filter,
                distance_metric,
                store,
                oversample_factor,
                cache,
                include_attributes,
            ),
        )
        .await?;
    output.touched_artifacts = trace.snapshot();
    Ok(output)
}

#[allow(clippy::too_many_arguments)]
async fn search_hierarchical_with_trace_inner(
    index: &HierarchicalIndex,
    query: &[f32],
    top_k: usize,
    beam_width: usize,
    filter: Option<&Filter>,
    distance_metric: DistanceMetric,
    store: &ZeppelinStore,
    oversample_factor: usize,
    cache: Option<&Arc<DiskCache>>,
    include_attributes: bool,
) -> Result<HierarchicalSearchOutput> {
    if query.len() != index.meta.dim {
        return Err(ZeppelinError::DimensionMismatch {
            expected: index.meta.dim,
            actual: query.len(),
        });
    }

    if top_k == 0 {
        return Ok(HierarchicalSearchOutput {
            results: Vec::new(),
            probed_centroids: Vec::new(),
            probed_routing_nodes: Vec::new(),
            touched_artifacts: BTreeSet::new(),
        });
    }

    let ns = &index.namespace;
    let seg = &index.segment_id;
    let effective_beam = beam_width.max(1);
    let routing_inventory = (!index.routing_node_ids.is_empty()).then(|| {
        index
            .routing_node_ids
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>()
    });

    // --- Navigate the tree with beam search ---
    // Start at root.
    require_inventoried_routing_node(routing_inventory.as_ref(), &index.meta.root_node_id)?;
    let root_key = tree_node_key(ns, seg, &index.meta.root_node_id);
    let root_data = fetch_with_cache(cache, store, &root_key).await?;
    let root_node = deserialize_tree_node(&root_data)?;

    // Rank root centroids.
    let mut beam: Vec<(String, f32)> = root_node
        .centroids
        .iter()
        .zip(root_node.children.iter())
        .map(|(c, child_id)| {
            let dist = compute_distance(query, c, distance_metric);
            (child_id.clone(), dist)
        })
        .collect();

    beam.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    beam.truncate(effective_beam);

    debug!(
        root_children = root_node.children.len(),
        beam_size = beam.len(),
        is_leaf = root_node.is_leaf,
        "beam search: root level"
    );

    if root_node.is_leaf {
        // Root is a leaf — scan the selected clusters directly.
        let cluster_indices: Vec<usize> = beam
            .iter()
            .filter_map(|(id, _)| id.parse::<usize>().ok())
            .collect();
        let candidates = scan_leaf_clusters(
            index,
            &cluster_indices,
            query,
            top_k,
            filter,
            distance_metric,
            store,
            oversample_factor,
            cache,
        )
        .await?;
        let results = finalize_candidates(
            ns,
            seg,
            candidates,
            top_k,
            filter,
            store,
            cache,
            include_attributes,
        )
        .await?;
        return Ok(HierarchicalSearchOutput {
            results,
            probed_centroids: cluster_indices,
            probed_routing_nodes: vec![index.meta.root_node_id.clone()],
            touched_artifacts: BTreeSet::new(),
        });
    }

    // Partition root beam into leaf clusters vs internal nodes.
    // Root `is_leaf == false` does NOT mean all children are internal nodes —
    // hybrid nodes have a mix of leaf cluster IDs (numeric, e.g. "42") and
    // internal node IDs (e.g. "n_2_ULID"). Without this partition, leaf cluster
    // IDs enter the descent loop, fail to load as tree nodes, and get silently
    // skipped — causing 0 results for namespaces with mostly-leaf root children.
    let mut root_leaf_clusters: Vec<usize> = Vec::new();
    let mut current_ids: Vec<String> = Vec::new();
    for (id, _) in beam {
        if let Ok(idx) = id.parse::<usize>() {
            root_leaf_clusters.push(idx);
        } else {
            current_ids.push(id);
        }
    }

    let mut accumulated: Vec<Candidate> = Vec::new();
    let mut probed_centroids = root_leaf_clusters.clone();
    let mut probed_routing_nodes = vec![index.meta.root_node_id.clone()];

    // Scan any leaf clusters found at root level.
    if !root_leaf_clusters.is_empty() {
        debug!(
            leaf_count = root_leaf_clusters.len(),
            internal_count = current_ids.len(),
            "root beam: partitioned into leaf clusters and internal nodes"
        );
        let leaf_candidates = scan_leaf_clusters(
            index,
            &root_leaf_clusters,
            query,
            top_k,
            filter,
            distance_metric,
            store,
            oversample_factor,
            cache,
        )
        .await?;
        accumulated.extend(leaf_candidates);
    }

    if current_ids.is_empty() {
        // All root beam entries were leaf clusters — return results.
        let results = finalize_candidates(
            ns,
            seg,
            accumulated,
            top_k,
            filter,
            store,
            cache,
            include_attributes,
        )
        .await?;
        return Ok(HierarchicalSearchOutput {
            results,
            probed_centroids,
            probed_routing_nodes,
            touched_artifacts: BTreeSet::new(),
        });
    }

    loop {
        let mut next_beam: Vec<(String, f32, bool)> = Vec::new(); // (child_id, dist, is_leaf)

        // Parallel prefetch all beam nodes at this level.
        for node_id in &current_ids {
            require_inventoried_routing_node(routing_inventory.as_ref(), node_id)?;
        }
        probed_routing_nodes.extend(current_ids.iter().cloned());
        let node_results = futures::future::join_all(current_ids.iter().map(|node_id| {
            let nkey = tree_node_key(ns, seg, node_id);
            async move { (node_id.clone(), fetch_with_cache(cache, store, &nkey).await) }
        }))
        .await;

        for (_, node_res) in node_results {
            let node_data = node_res?;
            let node = deserialize_tree_node(&node_data)?;

            for (c, child_id) in node.children.iter().enumerate() {
                let dist = compute_distance(query, &node.centroids[c], distance_metric);
                // Classify per-child: leaf cluster indices parse as usize,
                // internal node IDs have format "n_{depth}_{ulid}" and never do.
                let child_is_leaf = node.is_leaf || child_id.parse::<usize>().is_ok();
                next_beam.push((child_id.clone(), dist, child_is_leaf));
            }
        }

        // Sort by distance and keep top beam_width.
        next_beam.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        next_beam.truncate(effective_beam);

        let any_internal = next_beam.iter().any(|(_, _, is_leaf)| !*is_leaf);

        debug!(
            candidates = next_beam.len(),
            any_internal, "beam search: descending level"
        );

        // Separate leaf cluster entries from internal node entries.
        let mut leaf_clusters: Vec<usize> = Vec::new();
        let mut internal_ids: Vec<String> = Vec::new();

        for (id, _, is_leaf) in &next_beam {
            if *is_leaf {
                if let Ok(idx) = id.parse::<usize>() {
                    leaf_clusters.push(idx);
                }
            } else {
                internal_ids.push(id.clone());
            }
        }

        // Scan any leaf clusters found at this level.
        if !leaf_clusters.is_empty() {
            probed_centroids.extend(leaf_clusters.iter().copied());
            let leaf_candidates = scan_leaf_clusters(
                index,
                &leaf_clusters,
                query,
                top_k,
                filter,
                distance_metric,
                store,
                oversample_factor,
                cache,
            )
            .await?;
            accumulated.extend(leaf_candidates);
        }

        if internal_ids.is_empty() {
            // No more internal nodes to descend — return merged results.
            let results = finalize_candidates(
                ns,
                seg,
                accumulated,
                top_k,
                filter,
                store,
                cache,
                include_attributes,
            )
            .await?;
            let mut seen = HashSet::new();
            probed_centroids.retain(|cluster| seen.insert(*cluster));
            let mut seen_nodes = HashSet::new();
            probed_routing_nodes.retain(|node| seen_nodes.insert(node.clone()));
            return Ok(HierarchicalSearchOutput {
                results,
                probed_centroids,
                probed_routing_nodes,
                touched_artifacts: BTreeSet::new(),
            });
        }

        current_ids = internal_ids;
    }
}

/// Scans selected leaves through the segment's configured encoding path.
///
/// Filtered queries expand the intermediate target with `oversample_factor`.
/// The encoding-specific scanner returns full-precision candidates, after which
/// this function applies the exact predicate and final per-batch top-k. An
/// available bitmap is only a prefilter; exact attributes remain the final
/// filtered-result contract.
///
/// # Parameters
///
/// - `index`: Segment metadata and manifest-provided bitmap field declaration.
/// - `cluster_indices`: Borrowed leaf IDs selected by beam traversal.
/// - `query`: Dimension-validated query vector.
/// - `top_k`: Maximum candidates this leaf batch should return.
/// - `filter`: Optional exact attribute predicate.
/// - `distance_metric`: Metric used for coarse and exact distances as applicable.
/// - `store`: Object store for leaf artifacts.
/// - `oversample_factor`: Filtered-query candidate multiplier.
/// - `cache`: Optional shared immutable-artifact cache.
///
/// # Returns
///
/// Up to `top_k` owned candidates in best-first exact-distance order. Empty
/// selected leaves or no matching rows produce an empty vector.
///
/// # Errors
///
/// Propagates required artifact fetch and decoding failures from the flat, SQ8,
/// or PQ path. Bitmap unavailability alone is not an error because the exact
/// attribute path remains available.
///
/// # Performance
///
/// Cost scales with selected leaves and their rows. Filtering may retain more
/// candidates; quantized modes reduce coarse distance CPU but add sidecar and
/// exact-rerank reads.
///
/// # Examples
///
/// With `top_k = 10`, factor three, and a filter, a quantized scan keeps a wider
/// coarse pool before exact evaluation so selective matches are less likely to
/// be truncated by approximate distance.
#[allow(clippy::too_many_arguments)]
async fn scan_leaf_clusters(
    index: &HierarchicalIndex,
    cluster_indices: &[usize],
    query: &[f32],
    top_k: usize,
    filter: Option<&Filter>,
    distance_metric: DistanceMetric,
    store: &ZeppelinStore,
    oversample_factor: usize,
    cache: Option<&Arc<DiskCache>>,
) -> Result<Vec<Candidate>> {
    let fetch_k = if filter.is_some() {
        oversampled_k(top_k, oversample_factor)
    } else {
        top_k
    };

    debug!(nprobe = cluster_indices.len(), clusters = ?cluster_indices, "probing leaf clusters");

    let ns = &index.namespace;
    let seg = &index.segment_id;
    let has_bitmaps = !index.bitmap_fields.is_empty();

    let candidates = match index.meta.quantization {
        QuantizationType::Scalar => {
            scan_clusters_sq(
                ns,
                seg,
                cluster_indices,
                query,
                distance_metric,
                filter,
                fetch_k,
                index.meta.sq_calibration.as_deref(),
                has_bitmaps,
                store,
                cache,
            )
            .await?
        }
        QuantizationType::Product => {
            scan_clusters_pq(
                ns,
                seg,
                cluster_indices,
                query,
                distance_metric,
                filter,
                fetch_k,
                has_bitmaps,
                store,
                cache,
            )
            .await?
        }
        QuantizationType::None => {
            scan_clusters_flat(
                ns,
                seg,
                cluster_indices,
                query,
                distance_metric,
                filter,
                has_bitmaps,
                store,
                cache,
            )
            .await?
        }
    };

    debug!(
        total_candidates = candidates.len(),
        fetch_k, "scanned leaf clusters"
    );

    // Retain candidates and apply filter.
    let mut sorted = candidates;
    if filter.is_some() {
        sorted.sort_by(candidate_distance_cmp);
    } else {
        partial_topk_by(&mut sorted, top_k, candidate_distance_cmp);
    }

    let results: Vec<Candidate> = if let Some(f) = filter {
        sorted
            .into_iter()
            .filter(|c| match &c.attributes {
                Some(attrs) => evaluate_filter(f, attrs),
                None => false,
            })
            .take(top_k)
            .collect()
    } else {
        sorted.into_iter().take(top_k).collect()
    };

    debug!(
        returned = results.len(),
        top_k, "hierarchical search complete"
    );
    Ok(results)
}

/// Computes exact distances for every surviving row in selected flat leaves.
///
/// Each cluster's vector object, optional bitmap, and required filter attributes
/// are fetched concurrently. After all selected clusters finish I/O, rows are
/// decoded and scored sequentially. Unfiltered scans deliberately defer attrs
/// until final top-k enrichment.
///
/// # Parameters
///
/// - `namespace`: Namespace component of leaf keys.
/// - `segment_id`: Immutable segment component of leaf keys.
/// - `cluster_indices`: Selected numbered leaves.
/// - `query`: Validated query vector.
/// - `distance_metric`: Exact metric for result scores.
/// - `filter`: Optional predicate requiring attribute rows.
/// - `has_bitmaps`: Whether manifest metadata says bitmap sidecars may exist.
/// - `store`: Object store for vectors, attrs, and bitmap bytes.
/// - `cache`: Optional tiered cache.
///
/// # Returns
///
/// One owned candidate for every row not rejected by an available bitmap. When
/// a filter is active, each candidate also carries its row's cloned attributes
/// for exact evaluation by `scan_leaf_clusters`.
///
/// # Errors
///
/// Propagates vector or required attribute fetch/decoding failures. Bitmap
/// failures yield no prefilter and therefore do not cause false negatives.
///
/// # Performance
///
/// Performs parallel per-cluster I/O, then `O(selected_rows * dim)` exact
/// distance work. Attribute GETs occur only for filtered queries at this stage.
///
/// # Examples
///
/// An unfiltered two-leaf search fetches both vector objects together, scores
/// all rows, and carries only cluster/row locations for later attrs. A filtered
/// search also loads attrs and can skip bitmap-rejected rows before distance CPU.
///
/// # Rust Notes for Java/C Engineers
///
/// The iterator creates one future per borrowed cluster ID; `async move` copies
/// that `usize` into its future. `tokio::join!` gives each cluster three typed
/// results, eliminating a shared mutable completion structure or manual thread
/// synchronization.
#[allow(clippy::too_many_arguments)]
async fn scan_clusters_flat(
    namespace: &str,
    segment_id: &str,
    cluster_indices: &[usize],
    query: &[f32],
    distance_metric: DistanceMetric,
    filter: Option<&Filter>,
    has_bitmaps: bool,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
) -> Result<Vec<Candidate>> {
    // Phase 1: Parallel prefetch — all S3 I/O fires concurrently.
    let want_attrs = filter.is_some();
    let prefetched = futures::future::join_all(cluster_indices.iter().map(|&cluster_idx| {
        let cvec_key = cluster_key(namespace, segment_id, cluster_idx);
        async move {
            let (cluster_res, prefilter, attrs) = tokio::join!(
                fetch_with_cache(cache, store, &cvec_key),
                try_bitmap_prefilter(
                    namespace,
                    segment_id,
                    cluster_idx,
                    filter,
                    has_bitmaps,
                    store,
                    cache,
                ),
                async {
                    if want_attrs {
                        load_attrs(namespace, segment_id, cluster_idx, filter, store, cache).await
                    } else {
                        Ok(None)
                    }
                },
            );
            (cluster_idx, cluster_res, prefilter, attrs)
        }
    }))
    .await;

    // Phase 2: Sequential compute — CPU-bound, no I/O.
    let mut candidates = Vec::new();
    for (cluster_idx, cluster_res, prefilter, attrs) in prefetched {
        let cluster_data = cluster_res?;
        let attrs = attrs?;
        let cluster = deserialize_cluster(&cluster_data)?;

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

    Ok(candidates)
}

/// Uses SQ8 codes for coarse ranking, then reranks selected rows exactly.
///
/// New segments embed calibration in [`super::TreeMeta`] and co-locate SQ8 codes
/// with full vectors. Older segments load a separate calibration and SQ cluster
/// sidecar. Filters are evaluated during coarse ranking before the pool is cut
/// to four times `fetch_k`; otherwise an approximate nearest set could discard
/// every row satisfying a selective predicate.
///
/// # Parameters
///
/// - `namespace`: Namespace component of artifact keys.
/// - `segment_id`: Immutable segment identifier.
/// - `cluster_indices`: Selected leaf indexes.
/// - `query`: Validated full-precision query.
/// - `distance_metric`: Metric used by SQ8 asymmetric distance and exact rerank.
/// - `filter`: Optional metadata predicate applied before coarse truncation.
/// - `fetch_k`: Expanded result target used to size the rerank pool.
/// - `sq_calibration`: Embedded calibration bytes for new segments, or `None`
///   for the legacy sidecar format.
/// - `has_bitmaps`: Whether bitmap prefilter objects may be available.
/// - `store`: Object store for calibration, codes, full vectors, and attrs.
/// - `cache`: Optional tiered cache.
///
/// # Returns
///
/// Owned candidates with scores recomputed from full-precision vectors. The
/// caller performs final exact filtering and top-k selection.
///
/// # Errors
///
/// Propagates invalid calibration or SQ8 data, required object/attribute reads,
/// cluster decoding, and cache failures. A missing bitmap alone uses attrs.
///
/// # Performance
///
/// Coarse work is `O(selected_rows * dim)` over byte codes with parallel leaf
/// reads. At most roughly `fetch_k * 4` coarse tuples proceed to full-precision
/// reranking. Co-located clusters can reuse bytes fetched during the coarse
/// phase; legacy layouts require separate SQ and full-vector GETs.
///
/// # Examples
///
/// For `fetch_k = 30`, at most the best 120 filter-surviving SQ8 rows proceed to
/// exact distance. If a category predicate cannot use a bitmap, attrs are still
/// checked before those 120 are selected, preserving matching rows.
///
/// # Rust Notes for Java/C Engineers
///
/// Calibration is represented as `Option<&[u8]>`, a borrowed tagged state rather
/// than a nullable pointer. Pattern matching forces both the embedded and legacy
/// formats to be handled. `HashMap<usize, Bytes>` retains co-located buffers;
/// cloning `Bytes` shares an immutable reference-counted allocation rather than
/// deep-copying its contents.
#[allow(clippy::too_many_arguments)]
async fn scan_clusters_sq(
    namespace: &str,
    segment_id: &str,
    cluster_indices: &[usize],
    query: &[f32],
    distance_metric: DistanceMetric,
    filter: Option<&Filter>,
    fetch_k: usize,
    sq_calibration: Option<&[u8]>,
    has_bitmaps: bool,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
) -> Result<Vec<Candidate>> {
    use crate::index::quantization::sq::{sq_calibration_key, SqCalibration};

    let calibration = if let Some(calibration) = sq_calibration {
        SqCalibration::from_bytes(calibration)?
    } else {
        let cal_key = sq_calibration_key(namespace, segment_id);
        let cal_data = fetch_with_cache(cache, store, &cal_key).await?;
        SqCalibration::from_bytes(&cal_data)?
    };
    let prefer_colocated_clusters = sq_calibration.is_some();

    // Phase 1: coarse ranking — parallel prefetch. A non-bitmap attribute
    // filter must be applied DURING this scan, before truncating to
    // `rerank_count` — otherwise a selective filter's matches get truncated
    // away by approximate-distance ranking and the query silently under-fills
    // top_k (Task 6). Fetch attrs alongside the SQ codes whenever a filter is
    // active; bitmap-resolved clusters keep their fast path and ignore attrs.
    let want_attr_filter = filter.is_some();

    let coarse_prefetched =
        futures::future::join_all(cluster_indices.iter().map(|&cluster_idx| async move {
            let (prefilter, sq_res, attrs) = tokio::join!(
                try_bitmap_prefilter(
                    namespace,
                    segment_id,
                    cluster_idx,
                    filter,
                    has_bitmaps,
                    store,
                    cache,
                ),
                load_sq_cluster_for_coarse(
                    namespace,
                    segment_id,
                    cluster_idx,
                    prefer_colocated_clusters,
                    store,
                    cache,
                ),
                async {
                    if want_attr_filter {
                        load_attrs(namespace, segment_id, cluster_idx, filter, store, cache).await
                    } else {
                        Ok(None)
                    }
                },
            );
            (cluster_idx, prefilter, sq_res, attrs)
        }))
        .await;

    let mut coarse: Vec<(String, f32, usize)> = Vec::new();
    let mut prefetched_clusters: HashMap<usize, bytes::Bytes> = HashMap::new();
    for (cluster_idx, prefilter, sq_res, attrs) in coarse_prefetched {
        let (sq_cluster, cluster_data) = sq_res?;
        if let Some(cluster_data) = cluster_data {
            prefetched_clusters.insert(cluster_idx, cluster_data);
        }
        let attrs = attrs?;
        for (j, codes) in sq_cluster.codes.iter().enumerate() {
            if !coarse_row_passes(filter, &prefilter, &attrs, j) {
                continue;
            }
            let approx = calibration.asymmetric_distance(query, codes, distance_metric);
            coarse.push((sq_cluster.ids[j].clone(), approx, cluster_idx));
        }
    }

    let rerank_count = fetch_k * 4;
    partial_topk_by(&mut coarse, rerank_count, coarse_candidate_cmp);

    debug!(
        coarse_candidates = coarse.len(),
        rerank_count, "SQ8 coarse ranking complete, starting rerank"
    );

    // Phase 2: rerank with full-precision — parallel prefetch.
    let mut by_cluster: HashMap<usize, Vec<String>> = HashMap::new();
    for (id, _, cidx) in &coarse {
        by_cluster.entry(*cidx).or_default().push(id.clone());
    }

    let want_rerank_attrs = filter.is_some();
    let rerank_prefetched =
        futures::future::join_all(by_cluster.iter().map(|(&cluster_idx, needed_ids)| {
            let prefetched_cluster = prefetched_clusters.get(&cluster_idx).cloned();
            let cvec_key = cluster_key(namespace, segment_id, cluster_idx);
            let needed_ids = needed_ids.clone();
            async move {
                let cluster_fetch = async {
                    if let Some(cluster_data) = prefetched_cluster {
                        Ok(cluster_data)
                    } else {
                        fetch_with_cache(cache, store, &cvec_key).await
                    }
                };
                let (cluster_res, attrs) = tokio::join!(cluster_fetch, async {
                    if want_rerank_attrs {
                        load_attrs(namespace, segment_id, cluster_idx, filter, store, cache).await
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
        let cluster = deserialize_cluster(&cluster_data)?;
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

/// Loads SQ8 codes and optionally retains their co-located full-vector bytes.
///
/// New-format segments first inspect `cluster_N.bin` for embedded codes. If that
/// object lacks the embedded section, the helper loads the legacy SQ sidecar but
/// still returns the already-fetched full cluster for reranking. Legacy metadata
/// skips the cluster read during coarse ranking and returns no retained bytes.
///
/// # Parameters
///
/// - `namespace`: Namespace component of artifact keys.
/// - `segment_id`: Immutable segment identifier.
/// - `cluster_idx`: Numbered leaf to load.
/// - `prefer_colocated`: Whether metadata indicates the new embedded layout.
/// - `store`: Object store for required bytes.
/// - `cache`: Optional tiered cache.
///
/// # Returns
///
/// Decoded SQ8 IDs/codes plus `Some(cluster_bytes)` when the full cluster was
/// already fetched, even if codes ultimately came from a legacy sidecar.
///
/// # Errors
///
/// Propagates cache, object-store, co-located-format, or legacy SQ decoding
/// errors. It does not continue without valid codes.
///
/// # Performance
///
/// Uses one cluster GET for a valid co-located object, two GETs when preferred
/// co-location is absent, or one SQ-sidecar GET for legacy metadata.
///
/// # Examples
///
/// A new leaf with embedded codes returns both decoded codes and the shared
/// cluster bytes, allowing exact rerank without another vector GET.
async fn load_sq_cluster_for_coarse(
    namespace: &str,
    segment_id: &str,
    cluster_idx: usize,
    prefer_colocated: bool,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
) -> Result<(
    crate::index::quantization::sq::SqClusterData,
    Option<bytes::Bytes>,
)> {
    use crate::index::quantization::sq::{deserialize_sq_cluster, sq_cluster_key};

    if prefer_colocated {
        let cvec_key = cluster_key(namespace, segment_id, cluster_idx);
        let cluster_data = fetch_with_cache(cache, store, &cvec_key).await?;
        if let Some(sq_cluster) = deserialize_colocated_sq_cluster(&cluster_data)? {
            return Ok((sq_cluster, Some(cluster_data)));
        }

        let sq_key = sq_cluster_key(namespace, segment_id, cluster_idx);
        let sq_data = fetch_with_cache(cache, store, &sq_key).await?;
        let sq_cluster = deserialize_sq_cluster(&sq_data)?;
        return Ok((sq_cluster, Some(cluster_data)));
    }

    let sq_key = sq_cluster_key(namespace, segment_id, cluster_idx);
    let sq_data = fetch_with_cache(cache, store, &sq_key).await?;
    let sq_cluster = deserialize_sq_cluster(&sq_data)?;
    Ok((sq_cluster, None))
}

/// Uses product-quantized codes for coarse ranking, then reranks exactly.
///
/// The segment-wide codebook converts the query into an asymmetric-distance
/// lookup table. Every selected PQ row is scored from compact subquantizer codes,
/// with filters applied before truncation. Only the best coarse pool is grouped
/// by leaf and fetched from full-precision cluster objects for final distances.
///
/// # Parameters
///
/// - `namespace`: Namespace component of artifact keys.
/// - `segment_id`: Immutable segment identifier.
/// - `cluster_indices`: Selected numbered leaves.
/// - `query`: Validated full-precision query vector.
/// - `distance_metric`: Metric used to construct the ADC table and exact scores.
/// - `filter`: Optional predicate applied before coarse truncation.
/// - `fetch_k`: Expanded result target; four times this many coarse rows rerank.
/// - `has_bitmaps`: Whether bitmap prefilter sidecars may be present.
/// - `store`: Object store for codebook, PQ codes, vectors, and attrs.
/// - `cache`: Optional shared tiered cache.
///
/// # Returns
///
/// Owned candidates scored from full-precision vectors. Final filtering and
/// top-k projection remain with the caller.
///
/// # Errors
///
/// Propagates codebook, PQ, vector, or attribute fetch/decoding errors and cache
/// failures. Bitmap inability alone falls back to exact attributes.
///
/// # Performance
///
/// Loads one codebook, then selected PQ and optional attr/bitmap objects in
/// parallel. ADC scoring is proportional to rows times subquantizers rather than
/// full dimension. Exact rerank fetches only leaves containing the best roughly
/// `fetch_k * 4` rows, but unlike co-located SQ8 it requires vector objects.
///
/// # Examples
///
/// Ten selected leaves may contribute thousands of PQ rows; a request with
/// `fetch_k = 25` retains at most 100 coarse matches, groups them by leaf, and
/// recomputes exact distances only for those IDs.
///
/// # Rust Notes for Java/C Engineers
///
/// The ADC table and codebook are owned local values, while the query remains a
/// borrow. Grouping IDs in `HashMap<usize, Vec<String>>` transfers cloned IDs
/// into per-leaf async reads, so no future retains an iterator reference after
/// its source collection changes.
#[allow(clippy::too_many_arguments)]
async fn scan_clusters_pq(
    namespace: &str,
    segment_id: &str,
    cluster_indices: &[usize],
    query: &[f32],
    distance_metric: DistanceMetric,
    filter: Option<&Filter>,
    fetch_k: usize,
    has_bitmaps: bool,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
) -> Result<Vec<Candidate>> {
    use crate::index::quantization::pq::{
        deserialize_pq_cluster, pq_cluster_key, pq_codebook_key, PqCodebook,
    };

    let cb_key = pq_codebook_key(namespace, segment_id);
    let cb_data = fetch_with_cache(cache, store, &cb_key).await?;
    let codebook = PqCodebook::from_bytes(&cb_data)?;
    let adc_table = codebook.build_adc_table(query, distance_metric);

    // Phase 1: coarse ranking — parallel prefetch. Apply a non-bitmap
    // attribute filter DURING the coarse scan so a selective filter's matches
    // survive truncation (Task 6). Fetch attrs alongside the PQ codes whenever
    // a filter is active.
    let want_attr_filter = filter.is_some();

    let coarse_prefetched = futures::future::join_all(cluster_indices.iter().map(|&cluster_idx| {
        let pq_key = pq_cluster_key(namespace, segment_id, cluster_idx);
        async move {
            let (prefilter, pq_res, attrs) = tokio::join!(
                try_bitmap_prefilter(
                    namespace,
                    segment_id,
                    cluster_idx,
                    filter,
                    has_bitmaps,
                    store,
                    cache,
                ),
                fetch_with_cache(cache, store, &pq_key),
                async {
                    if want_attr_filter {
                        load_attrs(namespace, segment_id, cluster_idx, filter, store, cache).await
                    } else {
                        Ok(None)
                    }
                },
            );
            (cluster_idx, prefilter, pq_res, attrs)
        }
    }))
    .await;

    let mut coarse: Vec<(String, f32, usize)> = Vec::new();
    for (cluster_idx, prefilter, pq_res, attrs) in coarse_prefetched {
        let pq_data = pq_res?;
        let attrs = attrs?;
        let pq_cluster = deserialize_pq_cluster(&pq_data)?;
        for (j, codes) in pq_cluster.codes.iter().enumerate() {
            if !coarse_row_passes(filter, &prefilter, &attrs, j) {
                continue;
            }
            let approx = codebook.adc_distance(&adc_table, codes);
            coarse.push((pq_cluster.ids[j].clone(), approx, cluster_idx));
        }
    }

    let rerank_count = fetch_k * 4;
    partial_topk_by(&mut coarse, rerank_count, coarse_candidate_cmp);

    debug!(
        coarse_candidates = coarse.len(),
        "PQ coarse ranking complete, starting rerank"
    );

    // Phase 2: rerank — parallel prefetch.
    let mut by_cluster: HashMap<usize, Vec<String>> = HashMap::new();
    for (id, _, cidx) in &coarse {
        by_cluster.entry(*cidx).or_default().push(id.clone());
    }

    let want_rerank_attrs = filter.is_some();
    let rerank_prefetched =
        futures::future::join_all(by_cluster.iter().map(|(&cluster_idx, needed_ids)| {
            let cvec_key = cluster_key(namespace, segment_id, cluster_idx);
            let needed_ids = needed_ids.clone();
            async move {
                let (cluster_res, attrs) =
                    tokio::join!(fetch_with_cache(cache, store, &cvec_key), async {
                        if want_rerank_attrs {
                            load_attrs(namespace, segment_id, cluster_idx, filter, store, cache)
                                .await
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
        let cluster = deserialize_cluster(&cluster_data)?;
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

/// Selects the segment-level top-k and projects internal candidates to results.
///
/// Filtered candidates already carry exact attributes, so projection is local.
/// Unfiltered candidates deferred attrs; when requested, only clusters
/// represented in the final top-k are fetched and joined by saved row index.
///
/// # Parameters
///
/// - `namespace`: Namespace component of attribute keys.
/// - `segment_id`: Immutable segment identifier.
/// - `candidates`: Owned exact candidates accumulated across tree depths.
/// - `top_k`: Maximum results to retain.
/// - `filter`: Presence indicates attrs were loaded during scanning.
/// - `store`: Object store for deferred attribute reads.
/// - `cache`: Optional tiered cache.
/// - `include_attributes`: Whether response results should carry attrs.
///
/// # Returns
///
/// Up to `top_k` best-first [`SearchResult`] values. Attribute fields are `None`
/// when projection is disabled; individual vectors may also legitimately have
/// no attributes.
///
/// # Errors
///
/// Filtered or attribute-free projection is infallible here. Unfiltered
/// enrichment propagates required attr fetch/decoding and row-alignment errors.
///
/// # Performance
///
/// Uses expected-linear partial selection plus sorting the retained top-k.
/// Deferred enrichment performs one parallel attrs GET per distinct final
/// cluster, not per visited leaf.
///
/// # Examples
///
/// If 500 candidates from six leaves compete for ten results and the winners
/// occupy two leaves, an unfiltered attributes-included query fetches two attrs
/// objects after top-k rather than all six during scanning.
#[allow(clippy::too_many_arguments)]
async fn finalize_candidates(
    namespace: &str,
    segment_id: &str,
    mut candidates: Vec<Candidate>,
    top_k: usize,
    filter: Option<&Filter>,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
    include_attributes: bool,
) -> Result<Vec<SearchResult>> {
    partial_topk_by(&mut candidates, top_k, candidate_distance_cmp);

    if filter.is_some() {
        return Ok(candidates
            .into_iter()
            .map(|candidate| SearchResult {
                id: candidate.id,
                score: candidate.score,
                attributes: if include_attributes {
                    candidate.attributes
                } else {
                    None
                },
            })
            .collect());
    }

    if include_attributes {
        enrich_unfiltered_results(namespace, segment_id, candidates, store, cache).await
    } else {
        Ok(candidates
            .into_iter()
            .map(|candidate| SearchResult {
                id: candidate.id,
                score: candidate.score,
                attributes: None,
            })
            .collect())
    }
}

/// Loads attributes only for final unfiltered candidates and restores row joins.
///
/// # Parameters
///
/// - `namespace`: Namespace component of attrs keys.
/// - `segment_id`: Immutable segment identifier.
/// - `candidates`: Already-ranked owned candidates with cluster and row locations.
/// - `store`: Object store for attrs objects.
/// - `cache`: Optional tiered cache.
///
/// # Returns
///
/// Results in the same order as `candidates`, with cloned optional attribute maps.
/// An empty input performs no reads and returns an empty vector.
///
/// # Errors
///
/// Propagates attr fetch or decoding failures. Also returns an index error if a
/// fetched cluster entry, attr object, or row is unexpectedly absent. These are
/// immutable-artifact alignment violations and are not hidden.
///
/// # Performance
///
/// Deduplicates leaf IDs, fetches distinct attrs objects concurrently, and
/// allocates a lookup map plus the result vector. Attribute maps for result rows
/// are cloned into owned response values.
///
/// # Examples
///
/// Three winners at rows 2 and 8 of leaf 4 and row 1 of leaf 9 cause two attrs
/// reads. Their returned order still follows exact distance, not cluster order.
///
/// # Rust Notes for Java/C Engineers
///
/// Consuming `Vec<Candidate>` moves each candidate into projection after all
/// borrowed inspection is complete. Rust prevents mutation while the earlier
/// `for candidate in &candidates` borrow is active and automatically drops the
/// location-only candidates after their fields move into results.
async fn enrich_unfiltered_results(
    namespace: &str,
    segment_id: &str,
    candidates: Vec<Candidate>,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
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
                load_attrs(namespace, segment_id, cluster_idx, None, store, cache).await,
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
            })?
            .as_ref()
            .ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "attrs absent for final result cluster {}",
                    candidate.cluster_idx
                ))
            })?;
        let attributes = cluster_attrs
            .get(candidate.row_idx)
            .ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "attrs row {} missing in cluster {}",
                    candidate.row_idx, candidate.cluster_idx
                ))
            })?
            .clone();
        results.push(SearchResult {
            id: candidate.id,
            score: candidate.score,
            attributes,
        });
    }

    Ok(results)
}

/// Attempts to produce an exact bitmap row set for one cluster and filter.
///
/// Bitmap data is a performance sidecar, not authoritative query state. This
/// helper returns `None` for no filter, no manifest-declared bitmap fields, a
/// missing/corrupt sidecar, or a predicate the bitmap representation cannot
/// answer. Callers then evaluate original attributes. `Some(empty)` is distinct:
/// it proves that the indexed predicate matches no rows in this cluster.
///
/// # Parameters
///
/// - `namespace`: Namespace component of the bitmap key.
/// - `segment_id`: Immutable segment identifier.
/// - `cluster_idx`: Leaf whose row bitmap should be evaluated.
/// - `filter`: Optional predicate to pre-evaluate.
/// - `has_bitmaps`: Whether manifest metadata declares any bitmap fields.
/// - `store`: Object store for a cold sidecar read.
/// - `cache`: Optional tiered cache.
///
/// # Returns
///
/// `Some(rows)` only when the bitmap index can answer the complete predicate;
/// otherwise `None`, directing the caller to exact attrs.
///
/// # Side Effects
///
/// May fetch and cache one bitmap object. Decode failures emit a debug event;
/// fetch failures are treated as optimization misses.
///
/// # Consistency
///
/// The manifest supplies `has_bitmaps`, and immutable row positions must align
/// with vector and attribute artifacts. An unavailable sidecar never excludes a
/// row, so it cannot silently change exact filter meaning.
///
/// # Performance
///
/// A usable bitmap costs one cached read and compressed set evaluation but can
/// avoid distance CPU for rejected rows. Unsupported predicates still incur
/// exact attribute work.
///
/// # Examples
///
/// If `category = "books"` maps to rows `{1, 8}`, the helper returns that set and
/// leaf scoring skips all other rows. If the field was too high-cardinality to
/// index, it returns `None`; the scanner checks every row's attrs instead.
///
/// # Rust Notes for Java/C Engineers
///
/// The `?` operator is used on `Option` to return early when no filter exists.
/// Unlike a Java null or C null pointer, `Option<RoaringBitmap>` is a tagged type
/// that requires callers to distinguish unavailable evaluation from an owned,
/// valid empty set.
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

/// Loads and decodes the row-aligned attribute object for one leaf.
///
/// The `_filter` parameter is intentionally unused by the current loader; it
/// keeps call sites parallel with other scan helpers. Filtering occurs after
/// decoding, not inside this function.
///
/// # Parameters
///
/// - `namespace`: Namespace component of the attrs key.
/// - `segment_id`: Immutable segment identifier.
/// - `cluster_idx`: Numbered leaf whose attrs are required.
/// - `_filter`: Optional caller predicate, currently not inspected.
/// - `store`: Object store used on a cache miss.
/// - `cache`: Optional tiered cache.
///
/// # Returns
///
/// `Some` row-aligned attrs on success. The current implementation does not
/// return `None`; the option shape is consumed by shared filtering code.
///
/// # Errors
///
/// Propagates cache, object-store, missing-object, and attribute decoding errors.
/// Required attrs are not replaced by an empty collection.
///
/// # Performance
///
/// Performs one cached artifact lookup and decodes the complete cluster attrs
/// object. Callers avoid this on unfiltered scans until final result enrichment.
///
/// # Examples
///
/// Loading leaf 3 returns a vector whose row 7 corresponds to vector row 7 in
/// `cluster_3.bin`; a truncated attrs object fails the query.
async fn load_attrs(
    namespace: &str,
    segment_id: &str,
    cluster_idx: usize,
    _filter: Option<&Filter>,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
) -> Result<Option<ClusterAttrs>> {
    let akey = attrs_key(namespace, segment_id, cluster_idx);
    let data = fetch_with_cache(cache, store, &akey).await?;
    Ok(Some(deserialize_attrs(&data)?))
}
