//! Core trait definition for vector index implementations.
//!
//! Every index type (IVF-Flat, IVF-PQ, HNSW, etc.) implements `VectorIndex`
//! so the query engine can treat them uniformly.

use async_trait::async_trait;

use crate::config::IndexingConfig;
use crate::error::Result;
use crate::storage::ZeppelinStore;
use crate::types::{DistanceMetric, Filter, SearchResult, VectorEntry};

/// Trait that all Zeppelin index implementations must satisfy.
///
/// The trait is object-safe (via `async_trait`) so that the query layer can
/// hold a `Box<dyn VectorIndex>` when the concrete type is chosen at runtime.
#[async_trait]
pub trait VectorIndex: Send + Sync {
    /// Build the index from a batch of vectors and persist artifacts to S3.
    ///
    /// # Arguments
    /// * `vectors`  - The full set of vectors to index.
    /// * `config`   - Indexing hyper-parameters (centroids, iterations, ...).
    /// * `store`    - S3-compatible object store for writing artifacts.
    /// * `namespace` - Namespace prefix for S3 keys.
    /// * `segment_id` - Unique segment identifier for this build.
    ///
    /// # Errors
    /// Returns `ZeppelinError::Index` or `ZeppelinError::KMeansConvergence`
    /// if training fails.
    async fn build(
        vectors: &[VectorEntry],
        config: &IndexingConfig,
        store: &ZeppelinStore,
        namespace: &str,
        segment_id: &str,
    ) -> Result<Self>
    where
        Self: Sized;

    /// Search the index for the `top_k` nearest neighbors of `query`.
    ///
    /// # Arguments
    /// * `query`    - The query vector (must match index dimensionality).
    /// * `top_k`    - Number of results to return.
    /// * `nprobe`   - Number of clusters to scan (IVF-style indexes).
    /// * `filter`   - Optional post-filter to apply to candidates.
    /// * `distance_metric` - The distance metric to use for ranking.
    /// * `store`    - S3-compatible object store for reading cluster data.
    ///
    /// # Errors
    /// Returns `ZeppelinError::DimensionMismatch` if the query dimension
    /// does not match the index, or storage errors if cluster data cannot
    /// be read.
    async fn search(
        &self,
        query: &[f32],
        top_k: usize,
        nprobe: usize,
        filter: Option<&Filter>,
        distance_metric: DistanceMetric,
        store: &ZeppelinStore,
    ) -> Result<Vec<SearchResult>>;

    /// Return the total number of vectors in this index.
    fn vector_count(&self) -> usize;

    /// Return the dimensionality of vectors in this index.
    fn dimension(&self) -> usize;
}
