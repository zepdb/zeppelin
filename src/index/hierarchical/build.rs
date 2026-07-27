//! Constructs immutable hierarchical IVF artifacts from a validated vector set.
//!
//! Compaction enters through
//! [`build_hierarchical`](crate::index::hierarchical::build::build_hierarchical).
//! The builder recursively
//! partitions vectors with k-means until each group is small enough to become a
//! leaf cluster. Tree nodes are written bottom-up, while leaves reuse IVF-Flat's
//! full-precision vector and attribute formats. Optional roaring bitmaps, SQ8
//! codes, or PQ codes are additional immutable artifacts under the same segment
//! prefix.
//!
//! Hierarchical IVF trades construction work and several small routing objects
//! for query-time pruning. The branching factor controls how many centroids a
//! node targets; `leaf_size` controls when recursion stops. A high branching
//! factor does more distance work per visited node, while a small leaf size
//! produces a deeper tree and more objects.
//!
//! ```text
//! validated vectors + indexing configuration
//!                    |
//!                    v
//!        recursive k-means partitioning
//!          /         |          \
//!         v          v           v
//!   small group   large group   uneven group
//!      |              |             |
//!      v              v             v
//! leaf cluster    recurse       mixed-depth parent
//!      |              |             |
//!      +------ write child artifacts first ------+
//!                            |
//!                            v
//!                 write routing-node objects
//!                            |
//!                            v
//!            write PQ sidecars when configured
//!                            |
//!                            v
//!                   write tree_meta.json
//!                            |
//!                            v
//!          return handle; compaction later publishes
//!          the segment through the namespace manifest
//! ```
//!
//! `tree_meta.json` is the builder's last planned metadata write, but it is not
//! the namespace visibility boundary. If any PUT or encoding step fails, already
//! written objects can remain unreferenced. The caller must not publish the
//! segment in the authoritative manifest unless this function succeeds.
//!
//! ## Reading map
//!
//! 1. Start with
//!    [`build_hierarchical`](crate::index::hierarchical::build::build_hierarchical)
//!    for validation and phase ordering.
//! 2. Follow `build_subtree` for recursive partitioning and mixed-depth nodes.
//! 3. Read `write_leaf_cluster` for IVF-compatible leaf artifacts.
//! 4. Read `write_quantized_artifacts` for the PQ post-pass; SQ8 is already
//!    co-located by `write_leaf_cluster`.
//! 5. Finish with
//!    [`load_hierarchical`](crate::index::hierarchical::build::load_hierarchical)
//!    for metadata-only reopening.
//!
//! ## Invariants
//!
//! - Input vectors are non-empty, non-zero-dimensional, and uniformly sized.
//! - Leaf cluster indexes are unique and contiguous within the segment.
//! - A parent stores the centroid associated with each child in the same slot.
//! - Tree nodes and leaf artifacts are immutable after upload.
//! - Only successful manifest publication outside this module makes the segment
//!   visible; partial build objects are not queryable state.
//!
//! ## Rust concepts used here
//!
//! Recursive `async fn` calls are wrapped in [`Box::pin`]. Without indirection,
//! the compiler would need to construct a future type containing itself with
//! infinite size. Java futures are already heap objects; C typically builds an
//! explicit stack or state machine. Rust keeps the recursion memory-safe while
//! making the allocation visible in the code.
//!
//! Shared slices borrow the source batch, while each recursive child currently
//! owns cloned [`VectorEntry`] values. That is closer to copying a Java list's
//! contents than sharing references, and to deep-copying C structs with owned
//! buffers. Mutable references to the cluster and depth counters guarantee that
//! only the active recursive call can assign IDs. [`tokio::join!`] runs
//! independent object PUT futures concurrently without spawning detached tasks.

use bytes::Bytes;
use std::collections::{BTreeSet, HashMap, VecDeque};
use tracing::{debug, info};
use ulid::Ulid;

use crate::config::IndexingConfig;
use crate::error::{Result, ZeppelinError};
use crate::index::distance;
use crate::index::ivf_flat::build::{
    attrs_key, cluster_key, serialize_attrs, serialize_cluster, serialize_colocated_sq_cluster,
};
use crate::index::ivf_flat::kmeans::train_kmeans;
use crate::index::quantization::sq::SqCalibration;
use crate::index::quantization::QuantizationType;
use crate::storage::ZeppelinStore;
use crate::types::{AttributeValue, VectorEntry};

use super::{
    serialize_tree_node, tree_meta_key, tree_node_key, HierarchicalIndex, TreeMeta, TreeNode,
};

/// Default recursion cutoff when configuration does not provide a leaf size.
///
/// The effective cutoff is also raised to at least twice the branching factor,
/// preventing a requested fanout from immediately producing implausibly tiny
/// leaves.
const DEFAULT_LEAF_SIZE: usize = 1000;

/// Identifies the immutable artifact produced for one recursive group.
///
/// The enum prevents callers from confusing an object-backed routing-node ID
/// with a numeric leaf-cluster index. A parent converts either form to its
/// persisted child string only after matching the variant.
enum BuildResult {
    /// A tree-node object was written under this generated non-numeric ID.
    InternalNode(String),
    /// IVF-compatible leaf artifacts were written under this global index.
    LeafCluster(usize),
}

/// Builds and uploads one complete hierarchical index segment.
///
/// The function validates vector shape, calibrates SQ8 when selected, builds
/// the tree and leaves, writes any remaining quantization artifacts, and writes
/// [`TreeMeta`] last. If the whole dataset fits in one leaf, it creates a
/// one-child root so every index still has a root-node object.
///
/// # Parameters
///
/// - `vectors`: Borrowed complete segment input. Every entry must have the same
///   non-zero dimension, and the slice must not be empty.
/// - `config`: Borrowed indexing parameters controlling branching, k-means,
///   leaf cutoff, quantization, and bitmap creation.
/// - `store`: Object-store abstraction receiving immutable segment artifacts.
/// - `namespace`: Namespace prefix used to construct object keys. This function
///   does not validate the namespace or publish its manifest.
/// - `segment_id`: Unique immutable segment identifier selected by compaction.
///
/// # Returns
///
/// A metadata handle for the newly built artifacts. For bitmap-enabled builds,
/// the handle discovers field names from cluster zero when that sidecar can be
/// read and decoded; otherwise the list is empty and filtering remains exact
/// but does not use bitmap pruning through this handle.
///
/// # Errors
///
/// Returns an index error for an empty batch or zero-dimensional vectors, a
/// dimension mismatch for inconsistent entries, and propagates k-means,
/// quantization, serialization, cache-independent storage, and PUT failures.
/// Some leaf, node, codebook, or sidecar objects may already exist when a later
/// phase fails; no cleanup or manifest publication occurs here.
///
/// # Side Effects
///
/// Writes multiple immutable objects below the segment prefix and emits
/// structured build logs. The optional bitmap-field discovery performs one GET
/// of cluster zero and treats a missing or undecodable bitmap only as absence of
/// the optimization in the returned in-memory handle.
///
/// # Consistency
///
/// Successful return means the builder completed its planned artifacts, not
/// that readers may use them. Compaction must publish a segment reference in the
/// authoritative namespace manifest. A failed build must never be published.
///
/// # Performance
///
/// K-means and vector assignment dominate CPU. Recursive child construction is
/// sequential, and child vector lists are cloned. Leaf payload PUTs are parallel
/// per cluster. PQ adds a full-segment training pass, parallel GETs of every
/// leaf, encoding, and parallel PQ PUTs; SQ8 encodes while writing each leaf.
///
/// # Examples
///
/// With 20,000 vectors, branching factor 16, and leaf size 1,000, the builder
/// repeatedly partitions oversized groups, writes numbered leaves and their
/// parent nodes, then records the generated root in `tree_meta.json`. Only after
/// compaction publishes that segment does query planning open it.
///
/// If a PQ sidecar PUT fails after leaves were uploaded, the function returns an
/// error and those objects remain unreferenced; callers must not expose a
/// partially encoded segment through the manifest.
///
/// # Rust Notes for Java/C Engineers
///
/// The source slice and configuration are borrowed for the build, so ownership
/// stays with compaction. `?` returns immediately on the first required failure
/// and preserves the concrete [`ZeppelinError`] variant. The returned handle
/// owns cloned namespace, segment, metadata, and bitmap-field strings, so it can
/// outlive all input borrows.
pub async fn build_hierarchical(
    vectors: &[VectorEntry],
    config: &IndexingConfig,
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
) -> Result<HierarchicalIndex> {
    if vectors.is_empty() {
        return Err(ZeppelinError::Index(
            "cannot build index from empty vector set".into(),
        ));
    }

    let dim = vectors[0].values.len();
    if dim == 0 {
        return Err(ZeppelinError::Index("vector dimension must be > 0".into()));
    }

    for v in vectors.iter() {
        if v.values.len() != dim {
            return Err(ZeppelinError::DimensionMismatch {
                expected: dim,
                actual: v.values.len(),
            });
        }
    }

    let branching_factor = config.default_num_centroids.min(vectors.len());
    let leaf_size = config
        .leaf_size
        .unwrap_or(DEFAULT_LEAF_SIZE)
        .max(branching_factor * 2);
    let vec_refs: Vec<&[f32]> = vectors.iter().map(|v| v.values.as_slice()).collect();
    let sq_calibration = if matches!(config.quantization, QuantizationType::Scalar) {
        Some(SqCalibration::calibrate(&vec_refs, dim))
    } else {
        None
    };

    info!(
        n = vectors.len(),
        dim, branching_factor, leaf_size, namespace, segment_id, "building hierarchical ANN index"
    );

    // Mutable counter for assigning global leaf cluster indexes.
    let mut next_cluster_idx: usize = 0;
    let mut num_levels: usize = 0;
    let mut routing_node_ids = Vec::new();

    // Build the tree recursively.
    let root_result = build_subtree(
        vectors,
        dim,
        branching_factor,
        leaf_size,
        config,
        store,
        namespace,
        segment_id,
        sq_calibration.as_ref(),
        &mut next_cluster_idx,
        1, // current depth
        &mut num_levels,
        &mut routing_node_ids,
    )
    .await?;

    let root_node_id = match root_result {
        BuildResult::InternalNode(id) => id,
        BuildResult::LeafCluster(cluster_idx) => {
            // Edge case: entire dataset fits in one leaf.
            // Create a single-child root node wrapping it.
            let centroid = compute_centroid(vectors, dim);
            let root_id = format!("root_{}", Ulid::new());
            let root_node = TreeNode {
                centroids: vec![centroid],
                children: vec![cluster_idx.to_string()],
                is_leaf: true,
            };
            let node_data = serialize_tree_node(&root_node, dim);
            store
                .put(&tree_node_key(namespace, segment_id, &root_id), node_data)
                .await?;
            routing_node_ids.push(root_id.clone());
            num_levels = 1;
            root_id
        }
    };
    routing_node_ids.sort();

    info!(
        num_leaf_clusters = next_cluster_idx,
        "hierarchical tree partitioning complete"
    );

    // Write quantized artifacts if configured.
    write_quantized_artifacts(
        vectors,
        dim,
        next_cluster_idx,
        config,
        store,
        namespace,
        segment_id,
    )
    .await?;

    let meta = TreeMeta {
        num_levels,
        branching_factor,
        total_vectors: vectors.len(),
        dim,
        root_node_id: root_node_id.clone(),
        num_leaf_clusters: next_cluster_idx,
        quantization: config.quantization,
        sq_calibration: sq_calibration
            .as_ref()
            .map(|calibration| calibration.to_bytes().to_vec()),
    };

    // Write tree metadata.
    let meta_json = serde_json::to_vec_pretty(&meta)?;
    store
        .put(
            &tree_meta_key(namespace, segment_id),
            Bytes::from(meta_json),
        )
        .await?;

    info!(
        num_levels,
        num_leaf_clusters = next_cluster_idx,
        total_vectors = vectors.len(),
        root_node_id = %root_node_id,
        quantization = ?config.quantization,
        "hierarchical index build complete"
    );

    // Collect bitmap field names from the built bitmaps.
    let bitmap_fields = if config.bitmap_index && next_cluster_idx > 0 {
        // Read back the first cluster's bitmap to get field names.
        let bkey = crate::index::bitmap::bitmap_key(namespace, segment_id, 0);
        match store.get(&bkey).await {
            Ok(data) => match crate::index::bitmap::ClusterBitmapIndex::from_bytes(&data) {
                Ok(idx) => idx.fields.keys().cloned().collect(),
                Err(_) => Vec::new(),
            },
            Err(_) => Vec::new(),
        }
    } else {
        Vec::new()
    };

    Ok(HierarchicalIndex {
        meta,
        namespace: namespace.to_string(),
        physical_namespace: namespace.to_string(),
        physical_origin: None,
        segment_id: segment_id.to_string(),
        bitmap_fields,
        routing_node_ids,
    })
}

/// Recursively materializes one vector group as a leaf or routing subtree.
///
/// Groups at or below `leaf_size` receive the next global cluster index.
/// Oversized groups are partitioned by k-means, empty partitions are skipped,
/// and each non-empty child is built before its parent node is uploaded. Because
/// child sizes can differ, a parent may point to both leaves and deeper nodes.
///
/// # Parameters
///
/// - `vectors`: Borrowed non-empty, dimensionally validated group for this call.
/// - `dim`: Common vector and centroid dimension.
/// - `branching_factor`: Target maximum partitions for an oversized group.
/// - `leaf_size`: Inclusive vector-count cutoff for leaf creation.
/// - `config`: K-means, bitmap, and quantization settings shared by the build.
/// - `store`: Object store receiving child and node artifacts.
/// - `namespace`: Namespace component of every artifact key.
/// - `segment_id`: Segment component of every artifact key.
/// - `sq_calibration`: Segment-wide SQ8 calibration when scalar quantization is
///   active; absent for unquantized and PQ builds.
/// - `next_cluster_idx`: Exclusive mutable counter assigning contiguous leaf IDs.
/// - `depth`: One-based depth of this group.
/// - `max_depth`: Exclusive mutable accumulator for the greatest built depth.
///
/// # Returns
///
/// [`BuildResult::LeafCluster`] after leaf artifacts are written, or
/// [`BuildResult::InternalNode`] after all descendants and this node exist.
///
/// # Errors
///
/// Propagates k-means, encoding, bitmap, quantization, or object-store failures.
/// Descendants written before a later error remain under the unpublished segment
/// prefix, and the mutable counters may already have advanced.
///
/// # Side Effects
///
/// Assigns leaf IDs, updates maximum depth, uploads immutable leaf or node
/// objects, and emits structured debug events.
///
/// # Performance
///
/// Each internal group trains k-means and compares every vector with every
/// resulting centroid. Child groups currently deep-clone their vector entries
/// and recurse sequentially. The async recursion allocates one boxed future per
/// descended edge.
///
/// # Examples
///
/// If a root split produces groups of 600 and 4,000 vectors with a 1,000-row
/// cutoff, the first becomes a numbered leaf immediately. The second recurses.
/// Their parent is mixed-depth and stores one numeric child ID plus one node ID.
///
/// # Rust Notes for Java/C Engineers
///
/// `&mut usize` parameters are exclusive borrows: while this call assigns IDs,
/// no sibling can concurrently mutate the same counters. `Box::pin` supplies the
/// indirection needed for recursive async futures. Matching [`BuildResult`]
/// exhaustively forces both artifact kinds to be handled before a child ID is
/// persisted.
#[allow(clippy::too_many_arguments)]
async fn build_subtree(
    vectors: &[VectorEntry],
    dim: usize,
    branching_factor: usize,
    leaf_size: usize,
    config: &IndexingConfig,
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
    sq_calibration: Option<&SqCalibration>,
    next_cluster_idx: &mut usize,
    depth: usize,
    max_depth: &mut usize,
    routing_node_ids: &mut Vec<String>,
) -> Result<BuildResult> {
    // Base case: small enough to be a leaf cluster.
    if vectors.len() <= leaf_size {
        let cluster_idx = *next_cluster_idx;
        *next_cluster_idx += 1;
        if depth > *max_depth {
            *max_depth = depth;
        }

        write_leaf_cluster(
            vectors,
            dim,
            cluster_idx,
            store,
            namespace,
            segment_id,
            sq_calibration,
            config.bitmap_index,
        )
        .await?;

        debug!(
            cluster_idx,
            vectors = vectors.len(),
            depth,
            "wrote leaf cluster"
        );
        return Ok(BuildResult::LeafCluster(cluster_idx));
    }

    // Recursive case: partition with k-means and build child subtrees.
    let k = branching_factor.min(vectors.len());

    let vec_refs: Vec<&[f32]> = vectors.iter().map(|v| v.values.as_slice()).collect();
    let centroids = train_kmeans(
        &vec_refs,
        dim,
        k,
        config.kmeans_max_iterations,
        config.kmeans_convergence_epsilon,
    )?;

    // Assign vectors to nearest centroid.
    let mut assignments: Vec<Vec<usize>> = vec![Vec::new(); centroids.len()];
    for (i, entry) in vectors.iter().enumerate() {
        let mut best_dist = f32::MAX;
        let mut best_c = 0;
        for (c, centroid) in centroids.iter().enumerate() {
            let d = distance::euclidean_distance(&entry.values, centroid);
            if d < best_dist {
                best_dist = d;
                best_c = c;
            }
        }
        assignments[best_c].push(i);
    }

    for (i, a) in assignments.iter().enumerate() {
        debug!(cluster = i, count = a.len(), depth, "cluster assignment");
    }

    // Build child subtrees.
    let mut child_centroids = Vec::with_capacity(centroids.len());
    let mut child_ids = Vec::with_capacity(centroids.len());
    let mut all_leaves = true;

    for (c, centroid) in centroids.into_iter().enumerate() {
        if assignments[c].is_empty() {
            continue; // Skip empty clusters.
        }

        let child_vectors: Vec<VectorEntry> =
            assignments[c].iter().map(|&i| vectors[i].clone()).collect();

        let result = Box::pin(build_subtree(
            &child_vectors,
            dim,
            branching_factor,
            leaf_size,
            config,
            store,
            namespace,
            segment_id,
            sq_calibration,
            next_cluster_idx,
            depth + 1,
            max_depth,
            routing_node_ids,
        ))
        .await?;

        child_centroids.push(centroid);
        match result {
            BuildResult::InternalNode(node_id) => {
                child_ids.push(node_id);
                all_leaves = false;
            }
            BuildResult::LeafCluster(idx) => {
                child_ids.push(idx.to_string());
            }
        }
    }

    // Create this internal node.
    let node_id = format!("n_{}_{}", depth, Ulid::new());
    let node = TreeNode {
        centroids: child_centroids,
        children: child_ids,
        is_leaf: all_leaves,
    };

    let node_data = serialize_tree_node(&node, dim);
    store
        .put(&tree_node_key(namespace, segment_id, &node_id), node_data)
        .await?;
    routing_node_ids.push(node_id.clone());

    if depth > *max_depth {
        *max_depth = depth;
    }

    debug!(
        node_id = %node_id,
        children = node.children.len(),
        depth,
        is_leaf = all_leaves,
        "wrote internal node"
    );

    Ok(BuildResult::InternalNode(node_id))
}

/// Serializes and uploads one IVF-compatible leaf cluster.
///
/// Every leaf always stores IDs, full-precision vectors, and attributes. Scalar
/// quantization co-locates SQ8 codes inside the cluster object while retaining
/// full precision for reranking. Optional bitmap data is derived from the exact
/// row-aligned attributes.
///
/// # Parameters
///
/// - `vectors`: Borrowed entries assigned to this non-empty leaf.
/// - `dim`: Validated common vector dimension.
/// - `cluster_idx`: Globally unique leaf index within the segment.
/// - `store`: Object store receiving the immutable payloads.
/// - `namespace`: Namespace component of artifact keys.
/// - `segment_id`: Segment component of artifact keys.
/// - `sq_calibration`: Segment-wide calibration used to encode co-located SQ8
///   codes, or `None` for full-precision and PQ builds.
/// - `bitmap_index_enabled`: Whether to build a roaring attribute sidecar.
///
/// # Returns
///
/// Returns unit after all required leaf PUTs succeed.
///
/// # Errors
///
/// Propagates vector, attribute, bitmap, SQ8, or object-store errors. The three
/// PUTs run concurrently, so one artifact can exist even when another PUT fails.
///
/// # Side Effects
///
/// Writes `cluster_N.bin`, `attrs_N.bin`, and, when enabled, the cluster's
/// bitmap sidecar. It does not publish a manifest.
///
/// # Consistency
///
/// Vector IDs, vector rows, attributes, SQ8 codes, and bitmap row numbers must
/// retain identical ordering. Search relies on row indexes to join them.
///
/// # Performance
///
/// Clones IDs, vectors, and attributes into serialization-friendly buffers,
/// performs optional SQ8/bitmap CPU work, then overlaps up to three object PUTs.
///
/// # Examples
///
/// Leaf 7 containing 800 product vectors writes full-precision cluster and
/// attribute objects now; the PQ post-pass later reads that cluster and writes
/// `pq_cluster_7.bin`. An SQ8 build instead embeds codes in the cluster PUT.
///
/// # Rust Notes for Java/C Engineers
///
/// [`tokio::join!`] polls all three borrowed futures together and returns each
/// result; unlike detached Java executor tasks or C threads, no child outlives
/// this function. The function checks every result after all joins complete.
#[allow(clippy::too_many_arguments)]
async fn write_leaf_cluster(
    vectors: &[VectorEntry],
    dim: usize,
    cluster_idx: usize,
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
    sq_calibration: Option<&SqCalibration>,
    bitmap_index_enabled: bool,
) -> Result<()> {
    let ids: Vec<String> = vectors.iter().map(|v| v.id.clone()).collect();
    let vecs: Vec<Vec<f32>> = vectors.iter().map(|v| v.values.clone()).collect();
    let attrs: Vec<Option<HashMap<String, AttributeValue>>> =
        vectors.iter().map(|v| v.attributes.clone()).collect();

    // CPU phase: serialize all payloads.
    let cvec_data = if let Some(calibration) = sq_calibration {
        let vec_refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codes = calibration.encode_batch(&vec_refs);
        serialize_colocated_sq_cluster(&ids, &vecs, &codes, dim)?
    } else {
        serialize_cluster(&ids, &vecs, dim)?
    };
    let cvec_key = cluster_key(namespace, segment_id, cluster_idx);

    let cattr_data = serialize_attrs(&attrs)?;
    let cattr_key = attrs_key(namespace, segment_id, cluster_idx);

    let bitmap_payload = if bitmap_index_enabled {
        let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
            attrs.iter().map(|a| a.as_ref()).collect();
        let bitmap_idx = crate::index::bitmap::build::build_cluster_bitmaps(&attr_refs);
        let bitmap_data = bitmap_idx.to_bytes()?;
        let bkey = crate::index::bitmap::bitmap_key(namespace, segment_id, cluster_idx);
        Some((bkey, bitmap_data))
    } else {
        None
    };

    // I/O phase: write all artifacts in parallel.
    let bitmap_fut = async {
        if let Some((bkey, bitmap_data)) = bitmap_payload {
            store.put(&bkey, bitmap_data).await
        } else {
            Ok(None)
        }
    };
    let (r1, r2, r3) = tokio::join!(
        store.put(&cvec_key, cvec_data),
        store.put(&cattr_key, cattr_data),
        bitmap_fut,
    );
    r1?;
    r2?;
    r3?;

    Ok(())
}

/// Completes the configured quantized representation for all leaves.
///
/// SQ8 needs no post-pass because `write_leaf_cluster` already co-locates its
/// codes. PQ trains and uploads one segment-wide codebook, fetches every
/// full-precision leaf in parallel, encodes each leaf, and uploads all PQ
/// sidecars in parallel. Unquantized builds do nothing.
///
/// # Parameters
///
/// - `vectors`: Complete validated segment batch used to train a PQ codebook.
/// - `dim`: Common vector dimension.
/// - `num_clusters`: Number of numbered leaves created by recursion.
/// - `config`: Quantization type, PQ subquantizer count, and training iterations.
/// - `store`: Object store used for codebook and cluster GETs/PUTs.
/// - `namespace`: Namespace component of artifact keys.
/// - `segment_id`: Segment component of artifact keys.
///
/// # Returns
///
/// Returns unit after every artifact required by the selected encoding exists.
///
/// # Errors
///
/// Propagates invalid PQ configuration, training, cluster decoding, GET, and PUT
/// failures. The codebook or some PQ sidecars may already have been written; the
/// caller must leave the segment unpublished on any error.
///
/// # Side Effects
///
/// PQ performs one codebook PUT, one GET and one sidecar PUT per leaf. SQ8 and
/// unquantized variants perform no storage request here. Structured logs record
/// the selected completion path.
///
/// # Performance
///
/// PQ retains all parallel GET results, then all encoded payloads, so peak memory
/// scales with full-precision and compressed leaf data for the segment. GETs and
/// PUTs are parallel within their phases, but the three phases are sequential.
///
/// # Examples
///
/// For 32 leaves and PQ with eight subquantizers, this function trains one
/// codebook, reads 32 cluster objects concurrently, encodes their rows, and
/// writes 32 PQ objects. A failure on the final PUT leaves an incomplete,
/// unpublished prefix and returns an error.
///
/// # Rust Notes for Java/C Engineers
///
/// The exhaustive `match` makes adding a new [`QuantizationType`] a compiler
/// error until its artifact path is defined. `join_all` owns a collection of
/// futures and preserves result order, allowing the enumeration index to remain
/// the leaf index without shared mutable synchronization.
#[allow(clippy::too_many_arguments)]
async fn write_quantized_artifacts(
    vectors: &[VectorEntry],
    dim: usize,
    num_clusters: usize,
    config: &IndexingConfig,
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
) -> Result<()> {
    match config.quantization {
        QuantizationType::Scalar => {
            info!("wrote SQ8 co-located leaf clusters for hierarchical index");
        }
        QuantizationType::TwoBit => {
            return Err(ZeppelinError::Config(
                "two-bit quantization requires a flat IVF index".into(),
            ));
        }
        QuantizationType::Product => {
            use crate::index::quantization::pq::{
                pq_cluster_key, pq_codebook_key, serialize_pq_cluster, PqCodebook,
            };

            let vec_refs: Vec<&[f32]> = vectors.iter().map(|v| v.values.as_slice()).collect();
            let codebook =
                PqCodebook::train(&vec_refs, dim, config.pq_m, config.kmeans_max_iterations)?;
            store
                .put(&pq_codebook_key(namespace, segment_id), codebook.to_bytes())
                .await?;
            debug!(m = config.pq_m, "wrote PQ codebook");

            // Phase 1: Parallel reads of all leaf clusters.
            let read_keys: Vec<String> = (0..num_clusters)
                .map(|i| cluster_key(namespace, segment_id, i))
                .collect();
            let read_futs: Vec<_> = read_keys.iter().map(|k| store.get(k)).collect();
            let read_results = futures::future::join_all(read_futs).await;

            // Phase 2: CPU — deserialize, encode, serialize.
            let mut pq_payloads: Vec<(String, Bytes)> = Vec::with_capacity(num_clusters);
            for (i, result) in read_results.into_iter().enumerate() {
                let cluster_data = result?;
                let cluster = crate::index::ivf_flat::build::deserialize_cluster(&cluster_data)?;
                let cluster_refs: Vec<&[f32]> =
                    cluster.vectors.iter().map(|v| v.as_slice()).collect();
                let codes = codebook.encode_batch(&cluster_refs);
                let pq_data = serialize_pq_cluster(&cluster.ids, &codes, config.pq_m)?;
                pq_payloads.push((pq_cluster_key(namespace, segment_id, i), pq_data));
            }

            // Phase 3: Parallel writes of all PQ clusters.
            let write_futs: Vec<_> = pq_payloads
                .iter()
                .map(|(key, data)| store.put(key, data.clone()))
                .collect();
            let write_results = futures::future::join_all(write_futs).await;
            for result in write_results {
                result?;
            }
            info!(
                m = config.pq_m,
                "wrote PQ-encoded clusters for hierarchical index"
            );
        }
        QuantizationType::None => {}
    }
    Ok(())
}

/// Computes the component-wise arithmetic mean for a validated vector group.
///
/// # Parameters
///
/// - `vectors`: Borrowed non-empty entries whose values are all `dim` wide.
/// - `dim`: Number of output components.
///
/// # Returns
///
/// An owned `dim`-element centroid. This helper is used for the synthetic root
/// that wraps a single leaf.
///
/// # Panics
///
/// Panics if any input vector has more than `dim` components. The public builder
/// validates dimensions before this private helper is called. An empty slice is
/// outside the caller contract and would produce non-finite values.
///
/// # Performance
///
/// Runs in `O(vector_count * dim)` time and allocates only the result.
///
/// # Examples
///
/// Vectors `[0, 2]` and `[2, 4]` produce centroid `[1, 3]`.
fn compute_centroid(vectors: &[VectorEntry], dim: usize) -> Vec<f32> {
    let mut centroid = vec![0.0f32; dim];
    for v in vectors {
        for (d, val) in v.values.iter().enumerate() {
            centroid[d] += val;
        }
    }
    let inv = 1.0 / vectors.len() as f32;
    for val in &mut centroid {
        *val *= inv;
    }
    centroid
}

/// Opens an existing hierarchical segment by loading only `tree_meta.json`.
///
/// Tree nodes and leaf artifacts remain lazy. When a cache is supplied, metadata
/// follows memory → disk → S3 lookup and is scoped-pinned for the namespace;
/// a later segment pin replaces it. Cache and storage failures are not hidden.
///
/// # Parameters
///
/// - `store`: Object-store abstraction used on a cache miss.
/// - `namespace`: Namespace containing the manifest-selected segment.
/// - `segment_id`: Immutable segment whose metadata should be decoded.
/// - `cache`: Optional shared tiered cache borrowed for lookup and pinning.
///
/// # Returns
///
/// An owned [`HierarchicalIndex`] containing metadata and key context. Its
/// bitmap-field list is empty; manifest-aware query planning populates that list
/// before search, while direct callers still receive correct exact filtering.
///
/// # Errors
///
/// Propagates cache, object-store, missing-object, and JSON decoding errors. It
/// never substitutes default metadata for a corrupt or absent object.
///
/// # Side Effects
///
/// A cold load may populate memory and disk cache tiers. A successful cached
/// load updates the scoped pin for this namespace.
///
/// # Consistency
///
/// `tree_meta.json` is immutable segment data, but the namespace manifest is
/// still authoritative for whether this segment is visible. Callers must not use
/// arbitrary object-prefix discovery as a replacement for manifest selection.
///
/// # Performance
///
/// Performs one small object GET on a cache miss and no tree-node or leaf GETs.
///
/// # Examples
///
/// A second query for the same active segment can load metadata from the pinned
/// cache entry, while beam traversal separately fetches only its visited nodes.
/// If the JSON is truncated, loading fails before any search begins.
///
/// # Rust Notes for Java/C Engineers
///
/// `Option<&Arc<DiskCache>>` represents either no cache or a borrowed shared
/// cache owner. Borrowing the `Arc` avoids even a reference-count increment. The
/// returned handle owns its metadata and strings, so no cache or input reference
/// is embedded in it.
pub async fn load_hierarchical(
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
    cache: Option<&std::sync::Arc<crate::cache::DiskCache>>,
) -> Result<HierarchicalIndex> {
    load_hierarchical_routed(store, namespace, namespace, None, None, segment_id, cache).await
}

/// Loads one manifest-selected hierarchical descriptor through its resolved
/// physical owner while retaining the logical namespace for pin scope.
pub(crate) async fn load_hierarchical_from_located_manifest(
    store: &ZeppelinStore,
    located: crate::wal::manifest::LocatedSegmentRef<'_>,
    cache: Option<&std::sync::Arc<crate::cache::DiskCache>>,
) -> Result<HierarchicalIndex> {
    load_hierarchical_routed(
        store,
        located.logical_namespace,
        located.physical_namespace(),
        Some(located.logical_origin.as_origin()),
        Some(located.physical_origin.as_origin()),
        &located.segment.id,
        cache,
    )
    .await
}

async fn load_hierarchical_routed(
    store: &ZeppelinStore,
    logical_namespace: &str,
    physical_namespace: &str,
    logical_origin: Option<&crate::namespace::branching::ArtifactOrigin>,
    physical_origin: Option<&crate::namespace::branching::ArtifactOrigin>,
    segment_id: &str,
    cache: Option<&std::sync::Arc<crate::cache::DiskCache>>,
) -> Result<HierarchicalIndex> {
    let key = tree_meta_key(physical_namespace, segment_id);
    let cache_key = physical_origin.map_or_else(
        || key.clone(),
        |origin| crate::wal::manifest::immutable_artifact_cache_key(origin, &key),
    );
    let data = match cache {
        Some(c) => {
            let data = c.get_or_fetch(&cache_key, || store.get(&key)).await?;
            let pin_scope = logical_origin.map_or_else(
                || logical_namespace.to_string(),
                |origin| {
                    format!(
                        "artifact-origin/{}/pin/hierarchical",
                        origin.incarnation.as_uuid().simple()
                    )
                },
            );
            c.pin_scoped(&pin_scope, &cache_key).await;
            data
        }
        None => store.get(&key).await?,
    };
    let meta: TreeMeta = serde_json::from_slice(&data)?;

    info!(
        namespace = logical_namespace,
        physical_namespace,
        segment_id,
        num_levels = meta.num_levels,
        num_leaf_clusters = meta.num_leaf_clusters,
        total_vectors = meta.total_vectors,
        "loaded hierarchical index metadata"
    );

    Ok(HierarchicalIndex {
        meta,
        namespace: logical_namespace.to_string(),
        physical_namespace: physical_namespace.to_string(),
        physical_origin: physical_origin.cloned(),
        segment_id: segment_id.to_string(),
        bitmap_fields: Vec::new(), // Populated from SegmentRef at search time
        routing_node_ids: Vec::new(),
    })
}

/// Discover every routing node reachable from a legacy tree root.
///
/// Current builders return this inventory directly. Explicit receipt upgrade
/// uses this bounded traversal only for older manifests that predate the
/// persisted inventory; query execution never substitutes prefix listing for
/// manifest authority.
pub(crate) async fn discover_hierarchical_routing_nodes(
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
) -> Result<Vec<String>> {
    let index = load_hierarchical(store, namespace, segment_id, None).await?;
    let mut pending = VecDeque::from([index.meta.root_node_id.clone()]);
    let mut seen = BTreeSet::new();

    while let Some(node_id) = pending.pop_front() {
        if !seen.insert(node_id.clone()) {
            return Err(ZeppelinError::Index(format!(
                "hierarchical routing graph repeats node {node_id}"
            )));
        }
        let key = tree_node_key(namespace, segment_id, &node_id);
        let bytes = store.get(&key).await?;
        let node = super::deserialize_tree_node(&bytes)?;
        for child in node.children {
            if child.parse::<usize>().is_err() {
                pending.push_back(child);
            }
        }
    }

    Ok(seen.into_iter().collect())
}
