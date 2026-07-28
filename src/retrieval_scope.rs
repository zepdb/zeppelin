//! Policy-scope-derived ANN and BM25 artifacts.
//!
//! The authoritative manifest still chooses every source row. Stable compacted
//! snapshots may create-publish immutable policy-slice artifacts beneath the
//! source segment's lifecycle prefix; mutable WAL-frontier variants remain in
//! the bounded decoded cache. Cache keys bind the source descriptor, mandatory
//! filter, and build configuration, so eviction or restart can increase work
//! but cannot widen visibility.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::cache::DiskCache;
use crate::config::IndexingConfig;
use crate::error::{Result, ZeppelinError};
use crate::fts::bm25::Bm25Params;
use crate::fts::inverted_index::InvertedIndex;
use crate::fts::rank_by::{evaluate_rank_by, RankBy};
use crate::fts::tokenizer::tokenize_text;
use crate::fts::FtsFieldConfig;
use crate::index::filter::evaluate_filter;
use crate::index::hierarchical::build::build_hierarchical;
use crate::index::hierarchical::search::search_hierarchical_with_trace;
use crate::index::ivf_flat::build::build_ivf_flat;
use crate::index::ivf_flat::search::search_ivf_flat_with_trace;
use crate::index::topk::partial_topk_by;
use crate::index::{HierarchicalIndex, IvfFlatIndex};
use crate::namespace::branching::ArtifactOrigin;
use crate::storage::{CreateOnlyOutcome, ZeppelinStore};
use crate::types::{
    AttributeValue, ConsistencyLevel, DistanceMetric, Filter, SearchResult, VectorEntry,
};
use crate::wal::manifest::{CoarsePayloadEncoding, LocatedSegmentRef};
use crate::wal::Manifest;

const CACHE_KEY_VERSION: &str = "v2";
const SCOPED_ANN_DESCRIPTOR_VERSION: u32 = 4;
const SCOPED_FTS_ARTIFACT_VERSION: u32 = 1;
const SCOPED_FTS_MAGIC: &[u8; 5] = b"ZSFT1";

/// Failures specific to policy-scope retrieval artifact construction and use.
#[derive(Debug, Error)]
pub enum RetrievalScopeError {
    /// A persisted or derived scope artifact violates its binding invariants.
    #[error("artifact integrity failure: {0}")]
    Integrity(String),
    /// JSON at the scope-artifact boundary could not be encoded or decoded.
    #[error("artifact serialization failure: {0}")]
    Serialization(#[source] serde_json::Error),
    /// A dedicated CPU/build task panicked or was cancelled.
    #[error("artifact worker failed: {0}")]
    Worker(String),
}

fn scope_integrity(message: impl Into<String>) -> ZeppelinError {
    RetrievalScopeError::Integrity(message.into()).into()
}

/// One source segment's typed policy-artifact lifecycle context.
#[derive(Debug, Clone, Copy)]
struct ScopedArtifactLocation<'a> {
    namespace: &'a str,
    source_segment_id: &'a str,
}

impl<'a> ScopedArtifactLocation<'a> {
    fn new(namespace: &'a str, source_segment_id: &'a str) -> Self {
        Self {
            namespace,
            source_segment_id,
        }
    }

    fn artifact_namespace(self) -> String {
        format!(
            "{}/segments/{}/security_scopes",
            self.namespace, self.source_segment_id
        )
    }

    fn ann_descriptor_key(self, scope_cache_key: &str) -> Result<String> {
        Ok(format!(
            "{}/ann/{}.json",
            self.artifact_namespace(),
            scoped_object_digest(scope_cache_key)?
        ))
    }

    fn fts_object_key(self, scope_cache_key: &str) -> Result<String> {
        Ok(format!(
            "{}/fts/{}.bin",
            self.artifact_namespace(),
            scoped_object_digest(scope_cache_key)?
        ))
    }
}

/// Complete logical rows decoded from one manifest-selected segment or snapshot.
#[derive(Debug)]
pub(crate) struct ScopedSegmentCorpus {
    rows: Vec<VectorEntry>,
    dimensions: usize,
}

impl ScopedSegmentCorpus {
    /// Validates, ID-sorts, and owns one authoritative logical row snapshot.
    pub(crate) fn new(mut rows: Vec<VectorEntry>, dimensions: usize) -> Result<Self> {
        let dimensions = if dimensions == 0 {
            rows.first().map_or(0, |row| row.values.len())
        } else {
            dimensions
        };
        if dimensions == 0 && !rows.is_empty() {
            return Err(scope_integrity(
                "scoped retrieval corpus requires positive dimensions",
            ));
        }
        for row in &rows {
            if row.values.len() != dimensions {
                return Err(ZeppelinError::DimensionMismatch {
                    expected: dimensions,
                    actual: row.values.len(),
                });
            }
        }
        rows.sort_by(|left, right| left.id.cmp(&right.id));
        if let Some(duplicate) = rows
            .windows(2)
            .find(|pair| pair[0].id == pair[1].id)
            .map(|pair| pair[0].id.clone())
        {
            return Err(scope_integrity(format!(
                "scoped retrieval corpus contains duplicate logical id {duplicate}"
            )));
        }
        Ok(Self { rows, dimensions })
    }

    /// Borrows the stable ID-sorted logical rows.
    pub(crate) fn rows(&self) -> &[VectorEntry] {
        &self.rows
    }

    /// Returns the validated vector dimension.
    pub(crate) fn dimensions(&self) -> usize {
        self.dimensions
    }

    /// Approximates retained heap bytes for bounded-cache accounting.
    pub(crate) fn estimated_size_bytes(&self) -> usize {
        size_of::<Self>().saturating_add(self.rows.iter().map(estimated_vector_entry_bytes).sum())
    }
}

/// Loaded immutable ANN artifact trained only on one mandatory row slice.
#[derive(Debug)]
pub(crate) struct ScopedAnnIndex {
    artifact: ScopedAnnArtifact,
    dimensions: usize,
}

#[derive(Debug)]
enum ScopedAnnArtifact {
    Empty,
    Flat(Box<IvfFlatIndex>),
    Hierarchical(Box<HierarchicalIndex>),
}

/// Immutable inputs that identify and configure one policy-owned ANN artifact.
pub(crate) struct ScopedAnnBuildRequest<'a> {
    pub(crate) store: &'a ZeppelinStore,
    pub(crate) logical_origin: &'a ArtifactOrigin,
    pub(crate) source_segment_id: &'a str,
    pub(crate) scope_cache_key: &'a str,
    pub(crate) mandatory_filter: &'a Filter,
    pub(crate) config: &'a IndexingConfig,
    pub(crate) cache: Option<&'a Arc<DiskCache>>,
}

/// ANN results plus the scope-local cluster count actually probed.
pub(crate) struct ScopedAnnSearch {
    pub(crate) results: Vec<SearchResult>,
    pub(crate) clusters_probed: usize,
}

impl ScopedAnnIndex {
    /// Loads a published descriptor or builds and create-publishes one winner.
    pub(crate) async fn load_or_build<F, Fut>(
        request: ScopedAnnBuildRequest<'_>,
        build_corpus: F,
    ) -> Result<Self>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Arc<ScopedSegmentCorpus>>>,
    {
        let ScopedAnnBuildRequest {
            store,
            logical_origin,
            source_segment_id,
            scope_cache_key,
            mandatory_filter,
            config,
            cache,
        } = request;
        let namespace = logical_origin.namespace.as_str();
        let location = ScopedArtifactLocation::new(namespace, source_segment_id);
        let artifact_namespace = location.artifact_namespace();
        let descriptor_key = location.ann_descriptor_key(scope_cache_key)?;
        if let Some(bytes) = read_optional_immutable(store, cache, &descriptor_key).await? {
            let descriptor =
                ScopedAnnDescriptor::from_bytes(&bytes, scope_cache_key, &artifact_namespace)?;
            return descriptor.load(store, cache).await;
        }

        let corpus = build_corpus().await?;
        let dimensions = corpus.dimensions();
        let artifact_id = format!("ann_{}", ulid::Ulid::new());
        let built_artifact = build_scoped_ann(
            corpus,
            mandatory_filter.clone(),
            config.clone(),
            store.clone(),
            artifact_namespace.clone(),
            artifact_id.clone(),
        )
        .await?;
        let mut descriptor = match &built_artifact {
            ScopedAnnArtifact::Empty => {
                ScopedAnnDescriptor::empty(scope_cache_key, artifact_namespace.clone(), dimensions)
            }
            ScopedAnnArtifact::Flat(index) => ScopedAnnDescriptor::from_flat(
                scope_cache_key,
                artifact_namespace.clone(),
                artifact_id.clone(),
                index,
            ),
            ScopedAnnArtifact::Hierarchical(index) => ScopedAnnDescriptor::from_hierarchical(
                scope_cache_key,
                artifact_namespace.clone(),
                artifact_id,
                index,
            ),
        };
        descriptor.capture_artifact_hashes(store).await?;
        let descriptor_bytes = descriptor.to_bytes()?;
        match store
            .put_create_outcome(&descriptor_key, descriptor_bytes.clone())
            .await?
        {
            CreateOnlyOutcome::Created { .. } => {
                if let Some(cache) = cache {
                    cache.put(&descriptor_key, &descriptor_bytes).await?;
                    descriptor.warm_bootstrap(store, cache).await?;
                }
                store.forget_known_content_hashes(descriptor.artifact_hashes.keys());
                Ok(Self {
                    artifact: built_artifact,
                    dimensions: descriptor.dimensions,
                })
            }
            CreateOnlyOutcome::AlreadyExists => {
                store.forget_known_content_hashes(descriptor.artifact_hashes.keys());
                let bytes = store.get(&descriptor_key).await?;
                if let Some(cache) = cache {
                    cache.put(&descriptor_key, &bytes).await?;
                }
                let descriptor =
                    ScopedAnnDescriptor::from_bytes(&bytes, scope_cache_key, &artifact_namespace)?;
                descriptor.load(store, cache).await
            }
        }
    }

    /// Searches the published scope artifact without a whole-segment scan.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn search(
        &self,
        query: &[f32],
        top_k: usize,
        nprobe: usize,
        filter: Option<&Filter>,
        distance_metric: DistanceMetric,
        store: &ZeppelinStore,
        cache: Option<&Arc<DiskCache>>,
        oversample_factor: usize,
        rerank_coalesce_gap_bytes: usize,
        include_attributes: bool,
    ) -> Result<ScopedAnnSearch> {
        if query.len() != self.dimensions {
            return Err(ZeppelinError::DimensionMismatch {
                expected: self.dimensions,
                actual: query.len(),
            });
        }
        let (results, clusters_probed) = match &self.artifact {
            ScopedAnnArtifact::Empty => (Vec::new(), 0),
            ScopedAnnArtifact::Flat(index) => {
                let output = search_ivf_flat_with_trace(
                    index,
                    CoarsePayloadEncoding::Sq8,
                    query,
                    top_k,
                    nprobe,
                    filter,
                    distance_metric,
                    store,
                    oversample_factor,
                    cache,
                    include_attributes,
                    rerank_coalesce_gap_bytes,
                )
                .await?;
                let clusters_probed = output.probed_centroids.len();
                (output.results, clusters_probed)
            }
            ScopedAnnArtifact::Hierarchical(index) => {
                let output = search_hierarchical_with_trace(
                    index,
                    query,
                    top_k,
                    nprobe,
                    filter,
                    distance_metric,
                    store,
                    oversample_factor,
                    cache,
                    include_attributes,
                )
                .await?;
                let clusters_probed = output.probed_centroids.len();
                (output.results, clusters_probed)
            }
        };
        Ok(ScopedAnnSearch {
            results,
            clusters_probed,
        })
    }

    /// Approximates resident routing metadata for bounded-cache accounting.
    pub(crate) fn estimated_size_bytes(&self) -> usize {
        let artifact_bytes = match &self.artifact {
            ScopedAnnArtifact::Empty => 0,
            ScopedAnnArtifact::Flat(index) => {
                let bootstrap_bytes = index.bootstrap_ref.as_ref().map_or(0, |artifact| {
                    usize::try_from(artifact.size_bytes).map_or(usize::MAX, |size| size)
                });
                size_of::<IvfFlatIndex>()
                    .saturating_add(bootstrap_bytes)
                    .saturating_add(index.namespace.capacity())
                    .saturating_add(index.segment_id.capacity())
                    .saturating_add(
                        index
                            .centroids
                            .iter()
                            .map(|centroid| centroid.capacity().saturating_mul(size_of::<f32>()))
                            .sum::<usize>(),
                    )
                    .saturating_add(
                        index
                            .bitmap_fields
                            .iter()
                            .map(String::capacity)
                            .sum::<usize>(),
                    )
                    .saturating_add(
                        index
                            .cluster_owners
                            .iter()
                            .map(String::capacity)
                            .sum::<usize>(),
                    )
                    .saturating_add(
                        index
                            .cluster_objects
                            .iter()
                            .map(|object| {
                                object.key.capacity().saturating_add(
                                    object
                                        .clusters
                                        .capacity()
                                        .saturating_mul(size_of::<usize>()),
                                )
                            })
                            .sum::<usize>(),
                    )
                    .saturating_add(
                        index
                            .cluster_object_by_cluster
                            .capacity()
                            .saturating_mul(size_of::<usize>()),
                    )
            }
            ScopedAnnArtifact::Hierarchical(index) => size_of::<HierarchicalIndex>()
                .saturating_add(index.namespace.capacity())
                .saturating_add(index.segment_id.capacity())
                .saturating_add(index.meta.root_node_id.capacity())
                .saturating_add(index.meta.sq_calibration.as_ref().map_or(0, Vec::capacity))
                .saturating_add(
                    index
                        .bitmap_fields
                        .iter()
                        .map(String::capacity)
                        .sum::<usize>(),
                ),
        };
        size_of::<Self>().saturating_add(artifact_bytes)
    }
}

async fn build_scoped_ann(
    corpus: Arc<ScopedSegmentCorpus>,
    mandatory_filter: Filter,
    config: IndexingConfig,
    store: ZeppelinStore,
    artifact_namespace: String,
    artifact_id: String,
) -> Result<ScopedAnnArtifact> {
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let rows = mandatory_visible_rows(&corpus, &mandatory_filter);
        if rows.is_empty() {
            return Ok(ScopedAnnArtifact::Empty);
        }
        if config.hierarchical {
            runtime
                .block_on(build_hierarchical(
                    &rows,
                    &config,
                    &store,
                    &artifact_namespace,
                    &artifact_id,
                ))
                .map(Box::new)
                .map(ScopedAnnArtifact::Hierarchical)
        } else {
            runtime
                .block_on(build_ivf_flat(
                    &rows,
                    &config,
                    &store,
                    &artifact_namespace,
                    &artifact_id,
                ))
                .map(Box::new)
                .map(ScopedAnnArtifact::Flat)
        }
    })
    .await
    .map_err(|error| RetrievalScopeError::Worker(error.to_string()))?
}

/// Persisted ANN topology selected from the namespace indexing configuration.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ScopedAnnKind {
    Empty,
    Flat,
    Hierarchical,
}

/// Create-only publication record for one multi-object scoped ANN artifact.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopedAnnDescriptor {
    version: u32,
    scope_cache_key: String,
    artifact_namespace: String,
    artifact_id: Option<String>,
    kind: ScopedAnnKind,
    dimensions: usize,
    vector_count: usize,
    quantization: crate::index::quantization::QuantizationType,
    bitmap_fields: Vec<String>,
    num_clusters: usize,
    cluster_objects: Vec<crate::wal::manifest::ClusterDataObjectRef>,
    sketch: Option<crate::wal::manifest::SketchRef>,
    bootstrap: Option<crate::wal::manifest::BootstrapRef>,
    membership: Option<crate::wal::manifest::MembershipRef>,
    routing_node_ids: Vec<String>,
    artifact_hashes: BTreeMap<String, [u8; 32]>,
}

impl ScopedAnnDescriptor {
    fn empty(scope_cache_key: &str, artifact_namespace: String, dimensions: usize) -> Self {
        Self {
            version: SCOPED_ANN_DESCRIPTOR_VERSION,
            scope_cache_key: scope_cache_key.to_string(),
            artifact_namespace,
            artifact_id: None,
            kind: ScopedAnnKind::Empty,
            dimensions,
            vector_count: 0,
            quantization: crate::index::quantization::QuantizationType::None,
            bitmap_fields: Vec::new(),
            num_clusters: 0,
            cluster_objects: Vec::new(),
            sketch: None,
            bootstrap: None,
            membership: None,
            routing_node_ids: Vec::new(),
            artifact_hashes: BTreeMap::new(),
        }
    }

    fn from_flat(
        scope_cache_key: &str,
        artifact_namespace: String,
        artifact_id: String,
        index: &IvfFlatIndex,
    ) -> Self {
        Self {
            version: SCOPED_ANN_DESCRIPTOR_VERSION,
            scope_cache_key: scope_cache_key.to_string(),
            artifact_namespace,
            artifact_id: Some(artifact_id),
            kind: ScopedAnnKind::Flat,
            dimensions: index.dim,
            vector_count: index.num_vectors,
            quantization: index.quantization,
            bitmap_fields: index.bitmap_fields.clone(),
            num_clusters: index.centroids.len(),
            cluster_objects: index.cluster_objects.clone(),
            sketch: index.sketch_ref.clone(),
            bootstrap: index.bootstrap_ref.clone(),
            membership: index.membership_ref.clone(),
            routing_node_ids: Vec::new(),
            artifact_hashes: BTreeMap::new(),
        }
    }

    fn from_hierarchical(
        scope_cache_key: &str,
        artifact_namespace: String,
        artifact_id: String,
        index: &HierarchicalIndex,
    ) -> Self {
        Self {
            version: SCOPED_ANN_DESCRIPTOR_VERSION,
            scope_cache_key: scope_cache_key.to_string(),
            artifact_namespace,
            artifact_id: Some(artifact_id),
            kind: ScopedAnnKind::Hierarchical,
            dimensions: index.meta.dim,
            vector_count: index.meta.total_vectors,
            quantization: index.meta.quantization,
            bitmap_fields: index.bitmap_fields.clone(),
            num_clusters: index.meta.num_leaf_clusters,
            cluster_objects: Vec::new(),
            sketch: None,
            bootstrap: None,
            membership: None,
            routing_node_ids: index.routing_node_ids().to_vec(),
            artifact_hashes: BTreeMap::new(),
        }
    }

    async fn capture_artifact_hashes(&mut self, store: &ZeppelinStore) -> Result<()> {
        let Some(artifact_id) = self.artifact_id.as_ref() else {
            return Ok(());
        };
        let prefix = format!("{}/segments/{artifact_id}/", self.artifact_namespace);
        let keys = store.list_prefix(&prefix).await?;
        if keys.is_empty() {
            return Err(scope_integrity(format!(
                "scoped ANN artifact {artifact_id} published no objects"
            )));
        }
        for key in keys {
            let content_hash = store.known_content_hash(&key).ok_or_else(|| {
                scope_integrity(format!(
                    "scoped ANN artifact {key} lacks its publication content hash"
                ))
            })?;
            self.artifact_hashes.insert(key, content_hash);
        }
        self.validate_artifact_hashes()?;
        Ok(())
    }

    fn to_bytes(&self) -> Result<Bytes> {
        serde_json::to_vec(self)
            .map(Bytes::from)
            .map_err(|error| RetrievalScopeError::Serialization(error).into())
    }

    fn from_bytes(
        bytes: &[u8],
        expected_scope_cache_key: &str,
        expected_artifact_namespace: &str,
    ) -> Result<Self> {
        let descriptor: Self =
            serde_json::from_slice(bytes).map_err(RetrievalScopeError::Serialization)?;
        if descriptor.version != SCOPED_ANN_DESCRIPTOR_VERSION {
            return Err(scope_integrity(format!(
                "unsupported scoped ANN descriptor version {}",
                descriptor.version
            )));
        }
        if descriptor.scope_cache_key != expected_scope_cache_key {
            return Err(scope_integrity(
                "scoped ANN descriptor cache-key binding mismatch",
            ));
        }
        if descriptor.artifact_namespace != expected_artifact_namespace {
            return Err(scope_integrity(
                "scoped ANN descriptor lifecycle-prefix binding mismatch",
            ));
        }
        if descriptor.dimensions == 0 {
            return Err(scope_integrity("scoped ANN descriptor has zero dimensions"));
        }
        match (descriptor.kind, descriptor.artifact_id.as_ref()) {
            (ScopedAnnKind::Empty, None)
                if descriptor.vector_count == 0
                    && descriptor.cluster_objects.is_empty()
                    && descriptor.sketch.is_none()
                    && descriptor.bootstrap.is_none()
                    && descriptor.membership.is_none()
                    && descriptor.routing_node_ids.is_empty()
                    && descriptor.num_clusters == 0
                    && descriptor.artifact_hashes.is_empty() => {}
            (ScopedAnnKind::Flat, Some(_)) if descriptor.vector_count > 0 => {
                if descriptor.num_clusters == 0
                    || descriptor.sketch.is_none()
                    || descriptor.bootstrap.is_none()
                    || descriptor.membership.is_none()
                    || !descriptor.routing_node_ids.is_empty()
                {
                    return Err(scope_integrity(
                        "nonempty scoped flat ANN descriptor is missing or mixing routing artifacts",
                    ));
                }
                descriptor.validate_artifact_keys()?;
                descriptor.validate_artifact_hashes()?;
            }
            (ScopedAnnKind::Hierarchical, Some(artifact_id))
                if descriptor.vector_count > 0
                    && descriptor.num_clusters > 0
                    && descriptor.cluster_objects.is_empty()
                    && descriptor.sketch.is_none()
                    && descriptor.bootstrap.is_none() =>
            {
                validate_scoped_artifact_id(artifact_id)?;
                if descriptor.routing_node_ids.is_empty() || descriptor.membership.is_some() {
                    return Err(scope_integrity(
                        "nonempty scoped hierarchical ANN descriptor has no exact routing-node inventory",
                    ));
                }
                descriptor.validate_artifact_hashes()?;
            }
            _ => {
                return Err(scope_integrity(
                    "scoped ANN descriptor has an invalid topology or empty/nonempty shape",
                ));
            }
        }
        Ok(descriptor)
    }

    fn validate_artifact_keys(&self) -> Result<()> {
        let artifact_id = self
            .artifact_id
            .as_ref()
            .ok_or_else(|| scope_integrity("nonempty scoped ANN descriptor has no artifact id"))?;
        validate_scoped_artifact_id(artifact_id)?;
        let prefix = format!("{}/segments/{artifact_id}/", self.artifact_namespace);
        let keys_are_scoped = self
            .cluster_objects
            .iter()
            .all(|object| object.key.starts_with(&prefix))
            && self
                .sketch
                .as_ref()
                .is_some_and(|artifact| artifact.key.starts_with(&prefix))
            && self
                .bootstrap
                .as_ref()
                .is_some_and(|artifact| artifact.key.starts_with(&prefix));
        if !keys_are_scoped {
            return Err(scope_integrity(
                "scoped ANN descriptor references an object outside its lifecycle prefix",
            ));
        }
        Ok(())
    }

    fn validate_artifact_hashes(&self) -> Result<()> {
        let artifact_id = self
            .artifact_id
            .as_ref()
            .ok_or_else(|| scope_integrity("nonempty scoped ANN descriptor has no artifact id"))?;
        let prefix = format!("{}/segments/{artifact_id}/", self.artifact_namespace);
        if self.artifact_hashes.is_empty()
            || self
                .artifact_hashes
                .keys()
                .any(|key| !key.starts_with(&prefix))
        {
            return Err(scope_integrity(
                "scoped ANN descriptor has an incomplete or out-of-prefix hash inventory",
            ));
        }
        let required = self.required_artifact_keys(artifact_id);
        if let Some(missing) = required
            .iter()
            .find(|key| !self.artifact_hashes.contains_key(*key))
        {
            return Err(scope_integrity(format!(
                "scoped ANN descriptor hash inventory omits referenced artifact {missing}"
            )));
        }
        Ok(())
    }

    fn required_artifact_keys(&self, artifact_id: &str) -> BTreeSet<String> {
        let mut required = BTreeSet::new();
        match self.kind {
            ScopedAnnKind::Empty => {}
            ScopedAnnKind::Flat => {
                required.extend(self.cluster_objects.iter().map(|object| object.key.clone()));
                required.extend(self.sketch.iter().map(|artifact| artifact.key.clone()));
                required.extend(self.bootstrap.iter().map(|artifact| artifact.key.clone()));
                required.extend(self.membership.iter().map(|artifact| artifact.key.clone()));
                for cluster_idx in 0..self.num_clusters {
                    required.insert(crate::index::ivf_flat::build::attrs_key(
                        &self.artifact_namespace,
                        artifact_id,
                        cluster_idx,
                    ));
                    if !self.bitmap_fields.is_empty() {
                        required.insert(crate::index::bitmap::bitmap_key(
                            &self.artifact_namespace,
                            artifact_id,
                            cluster_idx,
                        ));
                    }
                }
            }
            ScopedAnnKind::Hierarchical => {
                required.insert(crate::index::hierarchical::tree_meta_key(
                    &self.artifact_namespace,
                    artifact_id,
                ));
                required.extend(self.routing_node_ids.iter().map(|node_id| {
                    crate::index::hierarchical::tree_node_key(
                        &self.artifact_namespace,
                        artifact_id,
                        node_id,
                    )
                }));
                for cluster_idx in 0..self.num_clusters {
                    required.insert(crate::index::ivf_flat::build::cluster_key(
                        &self.artifact_namespace,
                        artifact_id,
                        cluster_idx,
                    ));
                    required.insert(crate::index::ivf_flat::build::attrs_key(
                        &self.artifact_namespace,
                        artifact_id,
                        cluster_idx,
                    ));
                    if !self.bitmap_fields.is_empty() {
                        required.insert(crate::index::bitmap::bitmap_key(
                            &self.artifact_namespace,
                            artifact_id,
                            cluster_idx,
                        ));
                    }
                }
            }
        }
        if self.quantization == crate::index::quantization::QuantizationType::Product {
            required.insert(crate::index::quantization::pq::pq_codebook_key(
                &self.artifact_namespace,
                artifact_id,
            ));
            for cluster_idx in 0..self.num_clusters {
                required.insert(crate::index::quantization::pq::pq_cluster_key(
                    &self.artifact_namespace,
                    artifact_id,
                    cluster_idx,
                ));
            }
        }
        required
    }

    async fn load(
        self,
        store: &ZeppelinStore,
        cache: Option<&Arc<DiskCache>>,
    ) -> Result<ScopedAnnIndex> {
        let artifact = match (self.kind, self.artifact_id.as_ref()) {
            (ScopedAnnKind::Empty, None) => ScopedAnnArtifact::Empty,
            (ScopedAnnKind::Flat, Some(artifact_id)) => {
                let mut index = IvfFlatIndex::load_from_manifest(
                    store,
                    &self.artifact_namespace,
                    artifact_id,
                    self.vector_count,
                    self.quantization,
                    Vec::new(),
                    self.cluster_objects,
                    self.sketch,
                    self.bootstrap,
                    cache,
                )
                .await?;
                if index.dim != self.dimensions {
                    return Err(scope_integrity(format!(
                        "scoped flat ANN descriptor dimension {} disagrees with artifact {}",
                        self.dimensions, index.dim
                    )));
                }
                index.bitmap_fields = self.bitmap_fields;
                ScopedAnnArtifact::Flat(Box::new(index))
            }
            (ScopedAnnKind::Hierarchical, Some(artifact_id)) => {
                let mut index =
                    HierarchicalIndex::load(store, &self.artifact_namespace, artifact_id, cache)
                        .await?;
                if index.meta.dim != self.dimensions
                    || index.meta.total_vectors != self.vector_count
                    || index.meta.num_leaf_clusters != self.num_clusters
                    || index.meta.quantization != self.quantization
                {
                    return Err(scope_integrity(
                        "scoped hierarchical ANN descriptor disagrees with tree metadata",
                    ));
                }
                index.bitmap_fields = self.bitmap_fields;
                index.routing_node_ids = self.routing_node_ids;
                ScopedAnnArtifact::Hierarchical(Box::new(index))
            }
            _ => {
                return Err(scope_integrity(
                    "validated scoped ANN descriptor changed topology before load",
                ));
            }
        };
        Ok(ScopedAnnIndex {
            artifact,
            dimensions: self.dimensions,
        })
    }

    async fn warm_bootstrap(&self, store: &ZeppelinStore, cache: &Arc<DiskCache>) -> Result<()> {
        if let Some(bootstrap) = self.bootstrap.as_ref() {
            let _ = cache
                .get_or_fetch(&bootstrap.key, || async { store.get(&bootstrap.key).await })
                .await?;
        }
        Ok(())
    }
}

/// Policy-corpus BM25 index whose caller filter only narrows scored candidates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ScopedFtsIndex {
    rows: Vec<ScopedFtsRow>,
    index: InvertedIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScopedFtsRow {
    id: String,
    attributes: HashMap<String, AttributeValue>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopedFtsArtifact {
    version: u32,
    scope_cache_key: String,
    index: ScopedFtsIndex,
}

impl ScopedFtsIndex {
    /// Builds corpus statistics from mandatory-visible rows only.
    pub(crate) fn build(
        corpus: &ScopedSegmentCorpus,
        mandatory_filter: &Filter,
        fts_configs: &HashMap<String, FtsFieldConfig>,
    ) -> Self {
        let rows: Vec<ScopedFtsRow> = corpus
            .rows()
            .iter()
            .filter_map(|row| {
                row.attributes
                    .as_ref()
                    .filter(|attrs| evaluate_filter(mandatory_filter, attrs))
                    .map(|attributes| ScopedFtsRow {
                        id: row.id.clone(),
                        attributes: attributes.clone(),
                    })
            })
            .collect();
        let attribute_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
            rows.iter().map(|row| Some(&row.attributes)).collect();
        let index = InvertedIndex::build(&attribute_refs, fts_configs);
        Self { rows, index }
    }

    /// Loads or create-publishes a stable segment snapshot.
    ///
    /// `source_segment_id = None` denotes a mutable WAL frontier. That variant
    /// is built for the bounded decoded cache but is never durably published.
    pub(crate) async fn load_or_build<F, Fut>(
        store: &ZeppelinStore,
        namespace: &str,
        source_segment_id: Option<&str>,
        scope_cache_key: &str,
        cache: Option<&Arc<DiskCache>>,
        build: F,
    ) -> Result<Self>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Self>>,
    {
        let Some(source_segment_id) = source_segment_id else {
            return build().await;
        };
        let object_key = ScopedArtifactLocation::new(namespace, source_segment_id)
            .fts_object_key(scope_cache_key)?;
        if let Some(bytes) = read_optional_immutable(store, cache, &object_key).await? {
            return decode_scoped_fts(bytes, scope_cache_key.to_string()).await;
        }

        let built = build().await?;
        let (built, bytes) = encode_scoped_fts(built, scope_cache_key.to_string()).await?;
        match store.put_create_outcome(&object_key, bytes.clone()).await? {
            CreateOnlyOutcome::Created { .. } => {
                if let Some(cache) = cache {
                    cache.put(&object_key, &bytes).await?;
                }
                Ok(built)
            }
            CreateOnlyOutcome::AlreadyExists => {
                let winner = store.get(&object_key).await?;
                if let Some(cache) = cache {
                    cache.put(&object_key, &winner).await?;
                }
                decode_scoped_fts(winner, scope_cache_key.to_string()).await
            }
        }
    }

    fn to_bytes(&self, scope_cache_key: &str) -> Result<Bytes> {
        let artifact = ScopedFtsArtifact {
            version: SCOPED_FTS_ARTIFACT_VERSION,
            scope_cache_key: scope_cache_key.to_string(),
            index: self.clone(),
        };
        let json = serde_json::to_vec(&artifact).map_err(RetrievalScopeError::Serialization)?;
        let mut bytes = Vec::with_capacity(SCOPED_FTS_MAGIC.len() + json.len());
        bytes.extend_from_slice(SCOPED_FTS_MAGIC);
        bytes.extend_from_slice(&json);
        Ok(Bytes::from(bytes))
    }

    fn from_bytes(bytes: &[u8], expected_scope_cache_key: &str) -> Result<Self> {
        if !bytes.starts_with(SCOPED_FTS_MAGIC) {
            return Err(scope_integrity("scoped FTS artifact magic mismatch"));
        }
        let artifact: ScopedFtsArtifact = serde_json::from_slice(&bytes[SCOPED_FTS_MAGIC.len()..])
            .map_err(RetrievalScopeError::Serialization)?;
        if artifact.version != SCOPED_FTS_ARTIFACT_VERSION {
            return Err(scope_integrity(format!(
                "unsupported scoped FTS artifact version {}",
                artifact.version
            )));
        }
        if artifact.scope_cache_key != expected_scope_cache_key {
            return Err(scope_integrity(
                "scoped FTS artifact cache-key binding mismatch",
            ));
        }
        Ok(artifact.index)
    }

    /// Scores the policy corpus and applies the effective caller predicate last.
    pub(crate) fn search(
        &self,
        rank_by: &RankBy,
        fts_configs: &HashMap<String, FtsFieldConfig>,
        candidate_filter: &Filter,
        last_as_prefix: bool,
        top_k: usize,
        include_attributes: bool,
    ) -> Result<Vec<SearchResult>> {
        let mut position_field_scores: HashMap<u32, HashMap<String, f32>> = HashMap::new();
        for (field, query) in rank_by.extract_field_queries() {
            let Some(config) = fts_configs.get(field.as_str()) else {
                continue;
            };
            let query_tokens = tokenize_text(&query, config, last_as_prefix);
            let params = Bm25Params {
                k1: config.k1,
                b: config.b,
            };
            let field_results = if last_as_prefix {
                self.index.search_prefix(&field, &query_tokens, &params)
            } else {
                self.index.search(&field, &query_tokens, &params)
            };
            for (position, score) in field_results {
                *position_field_scores
                    .entry(position)
                    .or_default()
                    .entry(field.clone())
                    .or_insert(0.0) += score;
            }
        }

        let mut results = Vec::new();
        for (position, field_scores) in position_field_scores {
            let final_score = evaluate_rank_by(rank_by, &field_scores);
            if final_score <= 0.0 {
                continue;
            }
            let row = self.rows.get(position as usize).ok_or_else(|| {
                scope_integrity(format!(
                    "scoped BM25 position {position} is outside its policy corpus"
                ))
            })?;
            if !evaluate_filter(candidate_filter, &row.attributes) {
                continue;
            }
            results.push(SearchResult {
                id: row.id.clone(),
                score: final_score,
                attributes: include_attributes.then(|| row.attributes.clone()),
            });
        }
        partial_topk_by(&mut results, top_k, bm25_result_cmp);
        Ok(results)
    }

    /// Approximates retained heap bytes for bounded-cache accounting.
    pub(crate) fn estimated_size_bytes(&self) -> usize {
        let rows = self
            .rows
            .capacity()
            .saturating_mul(size_of::<ScopedFtsRow>())
            .saturating_add(
                self.rows
                    .iter()
                    .map(|row| {
                        row.id
                            .capacity()
                            .saturating_add(estimated_attributes_bytes(&row.attributes))
                    })
                    .sum::<usize>(),
            );
        let postings = self
            .index
            .fields
            .iter()
            .map(|(field, index)| {
                field
                    .capacity()
                    .saturating_add(size_of_val(index))
                    .saturating_add(
                        index
                            .postings
                            .iter()
                            .map(|(token, posting)| {
                                token
                                    .capacity()
                                    .saturating_add(size_of_val(posting))
                                    .saturating_add(posting.entries.capacity().saturating_mul(
                                        size_of::<crate::fts::inverted_index::Posting>(),
                                    ))
                            })
                            .sum::<usize>(),
                    )
            })
            .sum::<usize>();
        size_of::<Self>()
            .saturating_add(rows)
            .saturating_add(postings)
    }
}

async fn encode_scoped_fts(
    index: ScopedFtsIndex,
    scope_cache_key: String,
) -> Result<(ScopedFtsIndex, Bytes)> {
    tokio::task::spawn_blocking(move || {
        let bytes = index.to_bytes(&scope_cache_key)?;
        Ok((index, bytes))
    })
    .await
    .map_err(|error| RetrievalScopeError::Worker(error.to_string()))?
}

async fn decode_scoped_fts(
    bytes: Bytes,
    expected_scope_cache_key: String,
) -> Result<ScopedFtsIndex> {
    tokio::task::spawn_blocking(move || {
        ScopedFtsIndex::from_bytes(&bytes, &expected_scope_cache_key)
    })
    .await
    .map_err(|error| RetrievalScopeError::Worker(error.to_string()))?
}

/// Cache identity for one fully decoded immutable segment corpus.
pub(crate) fn segment_corpus_cache_key(located: LocatedSegmentRef<'_>) -> Result<String> {
    cache_key(
        "segment-corpus",
        located.logical_namespace,
        &[
            serde_json::to_vec(located.logical_origin.as_origin())?,
            serde_json::to_vec(located.physical_origin.as_origin())?,
            serde_json::to_vec(located.segment)?,
        ],
    )
}

/// Cache identity for one policy-slice IVF artifact.
pub(crate) fn scoped_ann_cache_key(
    located: LocatedSegmentRef<'_>,
    mandatory_filter: &Filter,
    config: &IndexingConfig,
) -> Result<String> {
    cache_key(
        "scoped-ann",
        located.logical_namespace,
        &[
            serde_json::to_vec(located.logical_origin.as_origin())?,
            serde_json::to_vec(located.physical_origin.as_origin())?,
            serde_json::to_vec(located.segment)?,
            serde_json::to_vec(mandatory_filter)?,
            serde_json::to_vec(config)?,
        ],
    )
}

/// Cache identity for one policy-corpus BM25 artifact at a fixed snapshot.
pub(crate) fn scoped_fts_cache_key(
    logical_origin: &ArtifactOrigin,
    manifest: &Manifest,
    consistency: ConsistencyLevel,
    mandatory_filter: &Filter,
    fts_configs: &HashMap<String, FtsFieldConfig>,
) -> Result<String> {
    let canonical_configs: BTreeMap<&String, &FtsFieldConfig> = fts_configs.iter().collect();
    cache_key(
        "scoped-fts",
        logical_origin.namespace.as_str(),
        &[
            serde_json::to_vec(logical_origin)?,
            serde_json::to_vec(manifest)?,
            serde_json::to_vec(&consistency)?,
            serde_json::to_vec(mandatory_filter)?,
            serde_json::to_vec(&canonical_configs)?,
        ],
    )
}

fn mandatory_visible_rows(
    corpus: &ScopedSegmentCorpus,
    mandatory_filter: &Filter,
) -> Vec<VectorEntry> {
    corpus
        .rows()
        .iter()
        .filter(|row| {
            row.attributes
                .as_ref()
                .is_some_and(|attrs| evaluate_filter(mandatory_filter, attrs))
        })
        .cloned()
        .collect()
}

fn scoped_object_digest(scope_cache_key: &str) -> Result<String> {
    if scope_cache_key.is_empty() {
        return Err(scope_integrity("scoped artifact cache key cannot be empty"));
    }
    Ok(Sha256::digest(scope_cache_key.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn validate_scoped_artifact_id(artifact_id: &str) -> Result<()> {
    let ulid = artifact_id
        .strip_prefix("ann_")
        .ok_or_else(|| scope_integrity("scoped ANN artifact id lacks ann_ prefix"))?;
    ulid::Ulid::from_string(ulid)
        .map(|_| ())
        .map_err(|_| scope_integrity("scoped ANN artifact id has an invalid ULID"))
}

async fn read_optional_immutable(
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
    key: &str,
) -> Result<Option<Bytes>> {
    let result = match cache {
        Some(cache) => {
            cache
                .get_or_fetch(key, || async { store.get(key).await })
                .await
        }
        None => store.get(key).await,
    };
    match result {
        Ok(bytes) => Ok(Some(bytes)),
        Err(ZeppelinError::NotFound { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

fn cache_key(kind: &str, namespace: &str, parts: &[Vec<u8>]) -> Result<String> {
    let mut digest = Sha256::new();
    for part in parts {
        let len = u64::try_from(part.len())
            .map_err(|_| scope_integrity("scope cache-key part exceeds u64"))?;
        digest.update(len.to_le_bytes());
        digest.update(part);
    }
    let hex = digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(format!("{kind}:{CACHE_KEY_VERSION}:{namespace}:{hex}"))
}

fn bm25_result_cmp(left: &SearchResult, right: &SearchResult) -> std::cmp::Ordering {
    right
        .score
        .total_cmp(&left.score)
        .then_with(|| left.id.cmp(&right.id))
}

fn estimated_vector_entry_bytes(row: &VectorEntry) -> usize {
    size_of::<VectorEntry>()
        .saturating_add(row.id.capacity())
        .saturating_add(row.values.capacity().saturating_mul(size_of::<f32>()))
        .saturating_add(
            row.attributes
                .as_ref()
                .map_or(0, estimated_attributes_bytes),
        )
}

fn estimated_attributes_bytes(attributes: &HashMap<String, AttributeValue>) -> usize {
    attributes
        .iter()
        .map(|(field, value)| {
            field
                .capacity()
                .saturating_add(estimated_attribute_value_bytes(value))
        })
        .sum()
}

fn estimated_attribute_value_bytes(value: &AttributeValue) -> usize {
    match value {
        AttributeValue::String(value) => value.capacity(),
        AttributeValue::StringList(values) => values
            .iter()
            .map(String::capacity)
            .sum::<usize>()
            .saturating_add(values.capacity().saturating_mul(size_of::<String>())),
        AttributeValue::IntegerList(values) => values.capacity().saturating_mul(size_of::<i64>()),
        AttributeValue::FloatList(values) => values.capacity().saturating_mul(size_of::<f64>()),
        AttributeValue::Integer(_) | AttributeValue::Float(_) | AttributeValue::Bool(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::ivf_flat::build::partition_vectors;

    fn row(id: &str, x: f32, tenant: &str, content: &str) -> VectorEntry {
        VectorEntry {
            id: id.to_string(),
            values: vec![x, 0.0],
            attributes: Some(HashMap::from([
                (
                    "tenant_id".to_string(),
                    AttributeValue::String(tenant.to_string()),
                ),
                (
                    "content".to_string(),
                    AttributeValue::String(content.to_string()),
                ),
            ])),
        }
    }

    fn tenant_filter(tenant: &str) -> Filter {
        Filter::Eq {
            field: "tenant_id".to_string(),
            value: AttributeValue::String(tenant.to_string()),
        }
    }

    #[test]
    fn scoped_ann_training_ignores_hidden_rows() -> Result<()> {
        let visible = vec![
            row("a0", 0.0, "a", "shared"),
            row("a1", 10.0, "a", "shared"),
            row("a2", 20.0, "a", "shared"),
            row("a3", 30.0, "a", "shared"),
        ];
        let mut with_hidden = visible.clone();
        with_hidden.extend((0..32).map(|idx| row(&format!("b{idx}"), -1000.0, "b", "hidden")));
        let first = ScopedSegmentCorpus::new(visible, 2)?;
        let second = ScopedSegmentCorpus::new(with_hidden, 2)?;
        let config = IndexingConfig {
            default_num_centroids: 4,
            max_num_centroids: 4,
            target_rows_per_cluster: 1,
            ..IndexingConfig::default()
        };
        let filter = tenant_filter("a");
        let partition = |corpus: &ScopedSegmentCorpus| {
            let rows = mandatory_visible_rows(corpus, &filter);
            let vectors: Vec<&[f32]> = rows.iter().map(|row| row.values.as_slice()).collect();
            partition_vectors(&vectors, 2, &config)
        };
        assert_eq!(partition(&first)?, partition(&second)?);
        Ok(())
    }

    #[test]
    fn persisted_scope_artifacts_reject_cross_key_or_prefix_rebinding() -> Result<()> {
        let ann = ScopedAnnDescriptor::empty(
            "scope-a",
            "ns/segments/source/security_scopes".to_string(),
            2,
        );
        let ann_bytes = ann.to_bytes()?;
        assert!(ScopedAnnDescriptor::from_bytes(
            &ann_bytes,
            "scope-a",
            "other/segments/source/security_scopes"
        )
        .is_err());

        let corpus = ScopedSegmentCorpus::new(vec![row("a0", 0.0, "a", "shared")], 2)?;
        let fts = ScopedFtsIndex::build(&corpus, &tenant_filter("a"), &HashMap::new());
        let fts_bytes = fts.to_bytes("scope-a")?;
        assert!(ScopedFtsIndex::from_bytes(&fts_bytes, "scope-b").is_err());
        Ok(())
    }

    #[test]
    fn scoped_ann_hash_inventory_cannot_omit_flat_children_or_hierarchical_nodes() {
        let artifact_namespace = "ns/segments/source/security_scopes".to_string();

        let flat_id = format!("ann_{}", ulid::Ulid::new());
        let flat_prefix = format!("{artifact_namespace}/segments/{flat_id}");
        let cluster_key = format!("{flat_prefix}/cluster_group_0.bin");
        let mut flat = ScopedAnnDescriptor {
            version: SCOPED_ANN_DESCRIPTOR_VERSION,
            scope_cache_key: "flat-scope".to_string(),
            artifact_namespace: artifact_namespace.clone(),
            artifact_id: Some(flat_id.clone()),
            kind: ScopedAnnKind::Flat,
            dimensions: 2,
            vector_count: 1,
            quantization: crate::index::quantization::QuantizationType::None,
            bitmap_fields: Vec::new(),
            num_clusters: 1,
            cluster_objects: vec![crate::wal::manifest::ClusterDataObjectRef {
                key: cluster_key.clone(),
                clusters: vec![0],
                live_offset: 0,
                live_len: 0,
                size_bytes: 1,
                cluster_layout_version: 0,
                row_layouts: Vec::new(),
            }],
            sketch: Some(crate::wal::manifest::SketchRef {
                key: format!("{flat_prefix}/coarse_sketch.bin"),
                version: 1,
                code_dims: 1,
                bytes_per_vector: 1,
                size_bytes: 1,
                rotation_seed: None,
            }),
            bootstrap: Some(crate::wal::manifest::BootstrapRef {
                key: format!("{flat_prefix}/bootstrap.bin"),
                size_bytes: 1,
            }),
            membership: Some(crate::wal::manifest::MembershipRef {
                key: format!("{flat_prefix}/membership.bin"),
                size_bytes: 1,
                entry_count: 1,
            }),
            routing_node_ids: Vec::new(),
            artifact_hashes: BTreeMap::new(),
        };
        for key in flat.required_artifact_keys(&flat_id) {
            flat.artifact_hashes.insert(key, [1; 32]);
        }
        assert!(flat.validate_artifact_hashes().is_ok());
        flat.artifact_hashes.remove(&cluster_key);
        assert!(flat.validate_artifact_hashes().is_err());

        let hierarchical_id = format!("ann_{}", ulid::Ulid::new());
        let hierarchical_prefix = format!("{artifact_namespace}/segments/{hierarchical_id}");
        let node_id = format!("root_{}", ulid::Ulid::new());
        let node_key = crate::index::hierarchical::tree_node_key(
            &artifact_namespace,
            &hierarchical_id,
            &node_id,
        );
        let mut hierarchical = ScopedAnnDescriptor {
            version: SCOPED_ANN_DESCRIPTOR_VERSION,
            scope_cache_key: "hierarchical-scope".to_string(),
            artifact_namespace,
            artifact_id: Some(hierarchical_id.clone()),
            kind: ScopedAnnKind::Hierarchical,
            dimensions: 2,
            vector_count: 1,
            quantization: crate::index::quantization::QuantizationType::None,
            bitmap_fields: Vec::new(),
            num_clusters: 1,
            cluster_objects: Vec::new(),
            sketch: None,
            bootstrap: None,
            membership: None,
            routing_node_ids: vec![node_id],
            artifact_hashes: BTreeMap::new(),
        };
        for key in hierarchical.required_artifact_keys(&hierarchical_id) {
            hierarchical.artifact_hashes.insert(key, [2; 32]);
        }
        assert!(hierarchical.validate_artifact_hashes().is_ok());
        assert!(hierarchical
            .artifact_hashes
            .contains_key(&format!("{hierarchical_prefix}/tree_meta.json")));
        hierarchical.artifact_hashes.remove(&node_key);
        assert!(hierarchical.validate_artifact_hashes().is_err());
    }

    #[test]
    fn boxed_scoped_ann_counts_heap_allocated_index_handle() {
        let index = ScopedAnnIndex {
            artifact: ScopedAnnArtifact::Hierarchical(Box::new(HierarchicalIndex {
                meta: crate::index::hierarchical::TreeMeta {
                    num_levels: 1,
                    branching_factor: 1,
                    total_vectors: 0,
                    dim: 2,
                    root_node_id: String::new(),
                    num_leaf_clusters: 0,
                    quantization: crate::index::quantization::QuantizationType::None,
                    sq_calibration: None,
                },
                namespace: String::new(),
                physical_namespace: String::new(),
                physical_origin: None,
                segment_id: String::new(),
                bitmap_fields: Vec::new(),
                routing_node_ids: Vec::new(),
            })),
            dimensions: 2,
        };

        assert!(
            index.estimated_size_bytes()
                >= size_of::<ScopedAnnIndex>().saturating_add(size_of::<HierarchicalIndex>())
        );
    }
}
