//! Hierarchical ANN index implementation.
//!
//! A multi-level centroid tree that enables O(log n) search with 2-3 S3
//! roundtrips per cold query, regardless of dataset size.
//!
//! - Internal nodes contain B centroids, each pointing to a child node.
//! - Leaf nodes contain B centroids, each pointing to a data cluster.
//! - Data clusters use the same format as IVF-Flat (reuses serialization).
//! - Beam search maintains `beam_width` candidates per level for recall.

pub mod build;
pub mod search;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::config::IndexingConfig;
use crate::error::Result;
use crate::index::quantization::QuantizationType;
use crate::index::traits::VectorIndex;
use crate::storage::ZeppelinStore;
use crate::types::{DistanceMetric, Filter, SearchResult, VectorEntry};

/// Metadata describing the tree structure, stored as JSON on S3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeMeta {
    /// Number of levels in the tree (1 = flat IVF, 2+ = hierarchical).
    pub num_levels: usize,
    /// Target branching factor (centroids per node).
    pub branching_factor: usize,
    /// Total vectors indexed.
    pub total_vectors: usize,
    /// Vector dimensionality.
    pub dim: usize,
    /// ID of the root node.
    pub root_node_id: String,
    /// Total number of leaf clusters.
    pub num_leaf_clusters: usize,
    /// Quantization type used at the leaf level.
    pub quantization: QuantizationType,
}

/// A single node in the centroid tree, stored as a binary blob on S3.
///
/// Each node has B centroids and B child references. At the leaf level,
/// children are cluster IDs (indexes into the flat cluster array).
/// At internal levels, children are node IDs.
#[derive(Debug, Clone)]
pub struct TreeNode {
    /// Centroid vectors for this node.
    pub centroids: Vec<Vec<f32>>,
    /// Child identifiers — either node IDs (internal) or cluster indexes (leaf).
    pub children: Vec<String>,
    /// Whether this is a leaf node (children point to clusters, not nodes).
    pub is_leaf: bool,
}

/// In-memory handle for a hierarchical ANN index.
///
/// Only the tree metadata is loaded eagerly. Node centroids and cluster data
/// are fetched from S3 on demand during search (beam search navigates the tree).
#[derive(Debug, Clone)]
pub struct HierarchicalIndex {
    /// Tree structure metadata.
    pub(crate) meta: TreeMeta,
    /// Namespace (for S3 key construction).
    pub(crate) namespace: String,
    /// Segment ID (for S3 key construction).
    pub(crate) segment_id: String,
    /// Fields that have bitmap indexes.
    pub(crate) bitmap_fields: Vec<String>,
}

// ---------------------------------------------------------------------------
// S3 key helpers
// ---------------------------------------------------------------------------

/// S3 key for tree metadata JSON.
pub fn tree_meta_key(namespace: &str, segment_id: &str) -> String {
    format!("{namespace}/segments/{segment_id}/tree_meta.json")
}

/// S3 key for a tree node binary blob.
pub fn tree_node_key(namespace: &str, segment_id: &str, node_id: &str) -> String {
    format!("{namespace}/segments/{segment_id}/node_{node_id}.bin")
}

// Leaf-level cluster data reuses IVF-Flat keys:
//   cluster_{i}.bin, attrs_{i}.bin, sq_cluster_{i}.bin, etc.

// ---------------------------------------------------------------------------
// Node serialization
// ---------------------------------------------------------------------------

/// Serialize a tree node to binary.
///
/// Layout:
/// ```text
/// [num_centroids: u32 LE]
/// [dim: u32 LE]
/// [is_leaf: u8]
/// For each centroid: [f32 LE] * dim
/// For each child:    [id_len: u32 LE][id_bytes]
/// ```
pub fn serialize_tree_node(node: &TreeNode, dim: usize) -> bytes::Bytes {
    let n = node.centroids.len();
    let mut buf = Vec::new();

    buf.extend_from_slice(&(n as u32).to_le_bytes());
    buf.extend_from_slice(&(dim as u32).to_le_bytes());
    buf.push(if node.is_leaf { 1 } else { 0 });

    for centroid in &node.centroids {
        for &val in centroid {
            buf.extend_from_slice(&val.to_le_bytes());
        }
    }

    for child_id in &node.children {
        let id_bytes = child_id.as_bytes();
        buf.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(id_bytes);
    }

    bytes::Bytes::from(buf)
}

/// Deserialize a tree node from binary.
pub fn deserialize_tree_node(data: &[u8]) -> Result<TreeNode> {
    use crate::error::ZeppelinError;

    if data.len() < 9 {
        return Err(ZeppelinError::Index(
            "tree node blob too small for header".into(),
        ));
    }

    let n = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let dim = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
    let is_leaf = data[8] != 0;

    let mut offset = 9;

    // Read centroids.
    let mut centroids = Vec::with_capacity(n);
    for _ in 0..n {
        let end = offset + dim * 4;
        if end > data.len() {
            return Err(ZeppelinError::Index(
                "tree node blob truncated at centroid data".into(),
            ));
        }
        let mut c = Vec::with_capacity(dim);
        for _ in 0..dim {
            let val = f32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            c.push(val);
            offset += 4;
        }
        centroids.push(c);
    }

    // Read child IDs.
    let mut children = Vec::with_capacity(n);
    for _ in 0..n {
        if offset + 4 > data.len() {
            return Err(ZeppelinError::Index(
                "tree node blob truncated at child id".into(),
            ));
        }
        let id_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if offset + id_len > data.len() {
            return Err(ZeppelinError::Index(
                "tree node blob truncated at child id bytes".into(),
            ));
        }
        let id = String::from_utf8_lossy(&data[offset..offset + id_len]).into_owned();
        offset += id_len;
        children.push(id);
    }

    Ok(TreeNode {
        centroids,
        children,
        is_leaf,
    })
}

impl HierarchicalIndex {
    pub fn num_leaf_clusters(&self) -> usize {
        self.meta.num_leaf_clusters
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn segment_id(&self) -> &str {
        &self.segment_id
    }

    /// Load an existing hierarchical index from S3 (metadata only).
    pub async fn load(store: &ZeppelinStore, namespace: &str, segment_id: &str) -> Result<Self> {
        build::load_hierarchical(store, namespace, segment_id).await
    }
}

#[async_trait]
impl VectorIndex for HierarchicalIndex {
    async fn build(
        vectors: &[VectorEntry],
        config: &IndexingConfig,
        store: &ZeppelinStore,
        namespace: &str,
        segment_id: &str,
    ) -> Result<Self> {
        build::build_hierarchical(vectors, config, store, namespace, segment_id).await
    }

    async fn search(
        &self,
        query: &[f32],
        top_k: usize,
        nprobe: usize,
        filter: Option<&Filter>,
        distance_metric: DistanceMetric,
        store: &ZeppelinStore,
    ) -> Result<Vec<SearchResult>> {
        let oversample_factor = if filter.is_some() { 3 } else { 1 };
        search::search_hierarchical(
            self,
            query,
            top_k,
            nprobe,
            filter,
            distance_metric,
            store,
            oversample_factor,
            None,
        )
        .await
    }

    fn vector_count(&self) -> usize {
        self.meta.total_vectors
    }

    fn dimension(&self) -> usize {
        self.meta.dim
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tree_node_serde_roundtrip_internal() {
        let node = TreeNode {
            centroids: vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]],
            children: vec!["node_a".to_string(), "node_b".to_string()],
            is_leaf: false,
        };
        let data = serialize_tree_node(&node, 3);
        let decoded = deserialize_tree_node(&data).unwrap();
        assert_eq!(decoded.centroids, node.centroids);
        assert_eq!(decoded.children, node.children);
        assert!(!decoded.is_leaf);
    }

    #[test]
    fn test_tree_node_serde_roundtrip_leaf() {
        let node = TreeNode {
            centroids: vec![vec![0.5, 0.5]],
            children: vec!["42".to_string()],
            is_leaf: true,
        };
        let data = serialize_tree_node(&node, 2);
        let decoded = deserialize_tree_node(&data).unwrap();
        assert_eq!(decoded.centroids, node.centroids);
        assert_eq!(decoded.children, node.children);
        assert!(decoded.is_leaf);
    }

    #[test]
    fn test_tree_node_deserialize_truncated() {
        let data = vec![0u8; 5]; // too small
        assert!(deserialize_tree_node(&data).is_err());
    }

    #[test]
    fn test_tree_meta_serde_roundtrip() {
        let meta = TreeMeta {
            num_levels: 3,
            branching_factor: 100,
            total_vectors: 1_000_000,
            dim: 128,
            root_node_id: "root".to_string(),
            num_leaf_clusters: 10_000,
            quantization: QuantizationType::None,
        };
        let json = serde_json::to_string(&meta).unwrap();
        let back: TreeMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(back.num_levels, 3);
        assert_eq!(back.branching_factor, 100);
        assert_eq!(back.total_vectors, 1_000_000);
        assert_eq!(back.root_node_id, "root");
        assert_eq!(back.num_leaf_clusters, 10_000);
    }

    #[test]
    fn test_s3_key_helpers() {
        assert_eq!(
            tree_meta_key("ns1", "seg_001"),
            "ns1/segments/seg_001/tree_meta.json"
        );
        assert_eq!(
            tree_node_key("ns1", "seg_001", "root"),
            "ns1/segments/seg_001/node_root.bin"
        );
    }
}
