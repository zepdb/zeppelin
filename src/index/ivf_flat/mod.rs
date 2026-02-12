//! IVF-Flat index implementation.
//!
//! An Inverted File index with flat (uncompressed) vector storage.
//! Vectors are partitioned into clusters via k-means, and at search time
//! only the `nprobe` closest clusters are scanned.

pub mod build;
pub mod kmeans;
pub mod search;

use async_trait::async_trait;

use crate::config::IndexingConfig;
use crate::error::Result;
use crate::index::traits::VectorIndex;
use crate::storage::ZeppelinStore;
use crate::types::{DistanceMetric, Filter, SearchResult, VectorEntry};

/// In-memory handle for a built IVF-Flat index.
///
/// Only the centroids live in memory; cluster vector data and attributes
/// are fetched from S3 on demand during search.
#[derive(Debug, Clone)]
pub struct IvfFlatIndex {
    /// Centroid vectors, one per cluster.  `centroids[i].len() == dim`.
    pub(crate) centroids: Vec<Vec<f32>>,
    /// Total number of vectors across all clusters.
    pub(crate) num_vectors: usize,
    /// Dimensionality of the vectors.
    pub(crate) dim: usize,
    /// Namespace this index belongs to (for constructing S3 keys).
    pub(crate) namespace: String,
    /// Segment identifier (for constructing S3 keys).
    pub(crate) segment_id: String,
    /// Quantization type used by this index.
    pub(crate) quantization: crate::index::quantization::QuantizationType,
    /// Fields that have bitmap indexes.
    pub(crate) bitmap_fields: Vec<String>,
}

impl IvfFlatIndex {
    /// Number of clusters (centroids) in this index.
    pub fn num_clusters(&self) -> usize {
        self.centroids.len()
    }

    /// The namespace this index is associated with.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The segment ID this index was built under.
    pub fn segment_id(&self) -> &str {
        &self.segment_id
    }

    /// Load an existing IVF-Flat index from S3 artifacts.
    pub async fn load(store: &ZeppelinStore, namespace: &str, segment_id: &str) -> Result<Self> {
        build::load_ivf_flat(store, namespace, segment_id).await
    }

    /// Load an IVF-Flat index using pre-known metadata from the manifest.
    ///
    /// Only fetches centroids — skips cluster-count probing and quantization
    /// detection, saving ~18 S3 GETs per query.
    pub async fn load_from_manifest(
        store: &ZeppelinStore,
        namespace: &str,
        segment_id: &str,
        num_vectors: usize,
        quantization: crate::index::quantization::QuantizationType,
    ) -> Result<Self> {
        build::load_ivf_flat_from_manifest(store, namespace, segment_id, num_vectors, quantization)
            .await
    }
}

#[async_trait]
impl VectorIndex for IvfFlatIndex {
    async fn build(
        vectors: &[VectorEntry],
        config: &IndexingConfig,
        store: &ZeppelinStore,
        namespace: &str,
        segment_id: &str,
    ) -> Result<Self> {
        build::build_ivf_flat(vectors, config, store, namespace, segment_id).await
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
        // Use a default oversample factor; callers that need a custom one
        // can invoke search::search_ivf_flat directly.
        let oversample_factor = if filter.is_some() { 3 } else { 1 };
        search::search_ivf_flat(
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
        self.num_vectors
    }

    fn dimension(&self) -> usize {
        self.dim
    }
}
