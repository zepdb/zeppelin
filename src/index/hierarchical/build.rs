//! Build phase for the hierarchical ANN index.
//!
//! Constructs a multi-level centroid tree via recursive k-means partitioning.
//! At each level, vectors are partitioned into B groups. If a group exceeds
//! the leaf threshold, it becomes an internal node and recurses. Otherwise
//! it becomes a leaf cluster with data written in IVF-Flat format.

use bytes::Bytes;
use std::collections::HashMap;
use tracing::{debug, info};
use ulid::Ulid;

use crate::config::IndexingConfig;
use crate::error::{Result, ZeppelinError};
use crate::index::distance;
use crate::index::ivf_flat::build::{attrs_key, cluster_key, serialize_attrs, serialize_cluster};
use crate::index::ivf_flat::kmeans::train_kmeans;
use crate::index::quantization::QuantizationType;
use crate::storage::ZeppelinStore;
use crate::types::{AttributeValue, VectorEntry};

use super::{
    serialize_tree_node, tree_meta_key, tree_node_key, HierarchicalIndex, TreeMeta, TreeNode,
};

/// Maximum vectors per leaf cluster. Groups smaller than this become leaves.
const DEFAULT_LEAF_SIZE: usize = 1000;

/// Result of building a subtree — either an internal node or a leaf cluster.
enum BuildResult {
    /// An internal node was created with the given node ID.
    InternalNode(String),
    /// A leaf cluster was created with the given cluster index.
    LeafCluster(usize),
}

/// Build a hierarchical ANN index from the given vectors.
///
/// 1. Recursively partition via k-means into a centroid tree.
/// 2. Write tree nodes and leaf cluster data to S3.
/// 3. Write tree metadata.
/// 4. Optionally write quantized artifacts at the leaf level.
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

    info!(
        n = vectors.len(),
        dim, branching_factor, leaf_size, namespace, segment_id, "building hierarchical ANN index"
    );

    // Mutable counter for assigning global leaf cluster indexes.
    let mut next_cluster_idx: usize = 0;
    let mut num_levels: usize = 0;

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
        &mut next_cluster_idx,
        1, // current depth
        &mut num_levels,
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
            num_levels = 1;
            root_id
        }
    };

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
        segment_id: segment_id.to_string(),
        bitmap_fields,
    })
}

/// Recursively build a subtree from a subset of vectors.
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
    next_cluster_idx: &mut usize,
    depth: usize,
    max_depth: &mut usize,
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
            next_cluster_idx,
            depth + 1,
            max_depth,
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

/// Write a leaf cluster's data (vectors + attributes) in IVF-Flat format.
async fn write_leaf_cluster(
    vectors: &[VectorEntry],
    dim: usize,
    cluster_idx: usize,
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
    bitmap_index_enabled: bool,
) -> Result<()> {
    let ids: Vec<String> = vectors.iter().map(|v| v.id.clone()).collect();
    let vecs: Vec<Vec<f32>> = vectors.iter().map(|v| v.values.clone()).collect();
    let attrs: Vec<Option<HashMap<String, AttributeValue>>> =
        vectors.iter().map(|v| v.attributes.clone()).collect();

    let cvec_data = serialize_cluster(&ids, &vecs, dim)?;
    store
        .put(&cluster_key(namespace, segment_id, cluster_idx), cvec_data)
        .await?;

    let cattr_data = serialize_attrs(&attrs)?;
    store
        .put(&attrs_key(namespace, segment_id, cluster_idx), cattr_data)
        .await?;

    // Build and write bitmap index for this leaf cluster.
    if bitmap_index_enabled {
        let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
            attrs.iter().map(|a| a.as_ref()).collect();
        let bitmap_idx = crate::index::bitmap::build::build_cluster_bitmaps(&attr_refs);
        let bitmap_data = bitmap_idx.to_bytes()?;
        let bkey = crate::index::bitmap::bitmap_key(namespace, segment_id, cluster_idx);
        store.put(&bkey, bitmap_data).await?;
    }

    Ok(())
}

/// Write quantized artifacts for all leaf clusters (SQ8 or PQ).
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
            use crate::index::quantization::sq::{
                serialize_sq_cluster, sq_calibration_key, sq_cluster_key, SqCalibration,
            };

            // Calibrate on all vectors globally.
            let vec_refs: Vec<&[f32]> = vectors.iter().map(|v| v.values.as_slice()).collect();
            let cal = SqCalibration::calibrate(&vec_refs, dim);
            store
                .put(&sq_calibration_key(namespace, segment_id), cal.to_bytes())
                .await?;
            debug!("wrote SQ8 calibration");

            // Re-read each leaf cluster and write quantized version.
            for i in 0..num_clusters {
                let cluster_data = store.get(&cluster_key(namespace, segment_id, i)).await?;
                let cluster = crate::index::ivf_flat::build::deserialize_cluster(&cluster_data)?;
                let cluster_refs: Vec<&[f32]> =
                    cluster.vectors.iter().map(|v| v.as_slice()).collect();
                let codes = cal.encode_batch(&cluster_refs);
                let sq_data = serialize_sq_cluster(&cluster.ids, &codes, dim)?;
                store
                    .put(&sq_cluster_key(namespace, segment_id, i), sq_data)
                    .await?;
            }
            info!("wrote SQ8 quantized clusters for hierarchical index");
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

            for i in 0..num_clusters {
                let cluster_data = store.get(&cluster_key(namespace, segment_id, i)).await?;
                let cluster = crate::index::ivf_flat::build::deserialize_cluster(&cluster_data)?;
                let cluster_refs: Vec<&[f32]> =
                    cluster.vectors.iter().map(|v| v.as_slice()).collect();
                let codes = codebook.encode_batch(&cluster_refs);
                let pq_data = serialize_pq_cluster(&cluster.ids, &codes, config.pq_m)?;
                store
                    .put(&pq_cluster_key(namespace, segment_id, i), pq_data)
                    .await?;
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

/// Compute the centroid (mean) of a set of vectors.
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

/// Load an existing hierarchical index from S3 (metadata only).
pub async fn load_hierarchical(
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
) -> Result<HierarchicalIndex> {
    let key = tree_meta_key(namespace, segment_id);
    let data = store.get(&key).await?;
    let meta: TreeMeta = serde_json::from_slice(&data)?;

    info!(
        namespace,
        segment_id,
        num_levels = meta.num_levels,
        num_leaf_clusters = meta.num_leaf_clusters,
        total_vectors = meta.total_vectors,
        "loaded hierarchical index metadata"
    );

    Ok(HierarchicalIndex {
        meta,
        namespace: namespace.to_string(),
        segment_id: segment_id.to_string(),
        bitmap_fields: Vec::new(), // Populated from SegmentRef at search time
    })
}
