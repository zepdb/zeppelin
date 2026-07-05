//! IVF-Flat index implementation.
//!
//! An Inverted File index with flat (uncompressed) vector storage.
//! Vectors are partitioned into clusters via k-means, and at search time
//! only the `nprobe` closest clusters are scanned.

pub mod build;
pub mod kmeans;
pub mod search;
pub mod sketch;

use async_trait::async_trait;

use crate::config::IndexingConfig;
use crate::error::Result;
use crate::index::VectorIndex;
use crate::storage::ZeppelinStore;
use crate::types::{DistanceMetric, Filter, SearchResult, VectorEntry};
use crate::wal::manifest::ClusterDataObjectRef;

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
    /// SQ calibration loaded from the v2 centroids blob, when embedded.
    pub(crate) sq_calibration: Option<crate::index::quantization::sq::SqCalibration>,
    /// Fields that have bitmap indexes.
    pub(crate) bitmap_fields: Vec<String>,
    /// Per-cluster owning segment IDs (incremental compaction carry-over).
    ///
    /// Mirrors `SegmentRef.cluster_owners`: `cluster_owners[i]` is the segment
    /// ID under which cluster `i`'s per-cluster S3 objects live. EMPTY means
    /// every cluster is owned by `segment_id` (legacy / full-rebuild layout).
    /// Segment-global artifacts (centroids, SQ calibration, PQ codebook)
    /// always live under `segment_id` and never consult this map.
    pub(crate) cluster_owners: Vec<String>,
    /// Manifest-defined cluster-data objects. EMPTY means legacy
    /// one-object-per-cluster layout through `cluster_owner()`.
    pub(crate) cluster_objects: Vec<ClusterDataObjectRef>,
    /// Lookup from logical cluster index to `cluster_objects` index.
    pub(crate) cluster_object_by_cluster: Vec<usize>,
    /// Resident coarse sketch loaded from the segment artifact, when present.
    pub(crate) resident_sketch: Option<sketch::ResidentSketch>,
    /// Manifest reference for the resident sketch artifact, when this handle
    /// came from a build path that created one.
    pub(crate) sketch_ref: Option<crate::wal::manifest::SketchRef>,
}

impl IvfFlatIndex {
    /// Number of clusters (centroids) in this index.
    pub fn num_clusters(&self) -> usize {
        self.centroids.len()
    }

    /// Segment ID that owns cluster `cluster_idx`'s per-cluster S3 objects.
    ///
    /// Resolves through `cluster_owners` (carried-over clusters keep an older
    /// segment's keys); falls back to `segment_id` when the map is empty or
    /// the index is out of range. Every per-cluster key builder in the search
    /// path MUST route through this — a carried-over cluster's data lives
    /// under a different segment ID than `self.segment_id`.
    pub(crate) fn cluster_owner(&self, cluster_idx: usize) -> &str {
        self.cluster_owners
            .get(cluster_idx)
            .map(String::as_str)
            .unwrap_or(&self.segment_id)
    }

    /// Paired cluster-data object for a logical cluster, when this segment uses
    /// the manifest-defined object layout.
    pub(crate) fn cluster_object(
        &self,
        cluster_idx: usize,
    ) -> Result<Option<&ClusterDataObjectRef>> {
        if self.cluster_objects.is_empty() {
            return Ok(None);
        }
        let object_idx = self
            .cluster_object_by_cluster
            .get(cluster_idx)
            .copied()
            .ok_or_else(|| {
                crate::error::ZeppelinError::Index(format!(
                    "cluster {cluster_idx} outside cluster object lookup"
                ))
            })?;
        self.cluster_objects.get(object_idx).map(Some).ok_or_else(|| {
            crate::error::ZeppelinError::Index(format!(
                "cluster object lookup for cluster {cluster_idx} points to missing object {object_idx}"
            ))
        })
    }

    /// Whether the cluster may contain non-null attributes.
    #[must_use]
    pub(crate) fn cluster_may_have_attrs(&self, cluster_idx: usize) -> bool {
        self.resident_sketch
            .as_ref()
            .map(|sketch| sketch.cluster_has_attrs(cluster_idx))
            .unwrap_or(true)
    }

    /// The namespace this index is associated with.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The segment ID this index was built under.
    pub fn segment_id(&self) -> &str {
        &self.segment_id
    }

    /// Manifest cluster-data object refs for this built/loaded index.
    pub fn cluster_objects(&self) -> &[ClusterDataObjectRef] {
        &self.cluster_objects
    }

    /// Load an existing IVF-Flat index from S3 artifacts.
    pub async fn load(store: &ZeppelinStore, namespace: &str, segment_id: &str) -> Result<Self> {
        build::load_ivf_flat(store, namespace, segment_id).await
    }

    /// Load an IVF-Flat index using pre-known metadata from the manifest.
    ///
    /// Only fetches centroids — skips cluster-count probing and quantization
    /// detection, saving ~18 S3 GETs per query. When `cache` is provided,
    /// the centroids are served through the tiered cache and pinned for the
    /// namespace's active segment.
    #[allow(clippy::too_many_arguments)]
    pub async fn load_from_manifest(
        store: &ZeppelinStore,
        namespace: &str,
        segment_id: &str,
        num_vectors: usize,
        quantization: crate::index::quantization::QuantizationType,
        cluster_owners: Vec<String>,
        cluster_objects: Vec<ClusterDataObjectRef>,
        sketch_ref: Option<crate::wal::manifest::SketchRef>,
        cache: Option<&std::sync::Arc<crate::cache::DiskCache>>,
    ) -> Result<Self> {
        build::load_ivf_flat_from_manifest(
            store,
            namespace,
            segment_id,
            num_vectors,
            quantization,
            cluster_owners,
            cluster_objects,
            sketch_ref,
            cache,
        )
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
