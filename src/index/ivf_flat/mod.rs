//! Public integration point for Zeppelin's IVF-Flat vector index.
//!
//! IVF means "inverted file." A build trains representative vectors called
//! centroids, assigns each input vector to its nearest centroid, and stores the
//! resulting clusters as immutable segment artifacts. A query first compares
//! itself with the small centroid set, then reads only the most promising
//! clusters. `nprobe` controls how many centroid regions are considered: a
//! larger value normally spends more I/O and CPU to improve recall.
//!
//! This file assembles the IVF-Flat submodules and defines [`IvfFlatIndex`],
//! the lightweight in-memory handle used by compaction and query execution. It
//! does not hold every indexed vector. Centroids, optional coarse sketches,
//! and routing metadata stay resident; cluster vectors and attributes remain
//! in object storage until [`search`] needs them.
//!
//! Building an index writes immutable objects but does **not** publish a
//! manifest. The compaction layer makes those artifacts visible by publishing
//! the segment reference separately. Conversely,
//! [`IvfFlatIndex::load_from_manifest`] assumes its metadata came from an
//! authoritative manifest and must not be used to make an unreferenced object
//! visible.
//!
//! ## Reading map
//!
//! 1. Start with [`IvfFlatIndex`] for the resident state shared by all paths.
//! 2. Read [`build`] for artifact formats and the build/load pipelines.
//! 3. Read [`kmeans`] and [`membership`] for training and row membership.
//! 4. Read [`sketch`] for the optional resident coarse-selection artifact.
//! 5. Finish with [`search`] for cluster selection, object reads, filtering,
//!    quantized scoring, and exact result construction.
//! 6. See [`crate::index::VectorIndex`] for the common interface exposed to
//!    callers that do not need IVF-specific controls.
//!
//! ## Handle and query flow
//!
//! ```text
//! authoritative manifest
//!          |
//!          | segment metadata and immutable object references
//!          v
//! load_from_manifest ----> resident centroids + optional coarse sketch
//!                                  |
//! query + nprobe ------------------+
//!                                  v
//!                       choose promising clusters
//!                                  |
//!                                  | range/whole-object GETs
//!                                  v
//!                  immutable cluster data in S3/MinIO
//!                                  |
//!                                  v
//!                     filter, rerank, top-k results
//! ```
//!
//! ## Invariants
//!
//! - The manifest, not this in-memory handle, determines segment visibility.
//! - Segment artifacts are immutable; a rebuild uses a new segment identity.
//! - Every centroid and cluster uses the same vector dimension.
//! - Carried-over clusters route through their recorded owning segment.
//! - Manifest-defined grouped-object lookup must cover every logical cluster.
//! - Missing or malformed artifacts fail the operation rather than producing
//!   an empty or silently degraded index.
//!
//! ## Rust concepts used here
//!
//! [`Arc`] lets cloned handles share the potentially large centroid and sketch
//! allocations. This resembles sharing an immutable object reference in Java,
//! but Rust tracks thread safety and reference counts in the type system. In C,
//! it replaces a manually reference-counted shared allocation. The owned
//! `String` and `Vec` metadata still clone normally, while each `Arc` clone only
//! increments a reference count.
//!
//! [`async_trait::async_trait`] adapts async build and search methods to the
//! shared [`crate::index::VectorIndex`] trait. Borrowed slices such as `&[f32]`
//! and `&[VectorEntry]` avoid copying caller-owned vectors, while owned values
//! in an [`IvfFlatIndex`] remain valid after those temporary borrows end.

pub mod build;
pub mod filter_summary;
pub mod kmeans;
pub mod membership;
pub mod search;
pub mod sketch;

use async_trait::async_trait;
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::config::IndexingConfig;
use crate::error::Result;
use crate::index::VectorIndex;
use crate::namespace::branching::ArtifactOrigin;
use crate::storage::ZeppelinStore;
use crate::types::{DistanceMetric, Filter, SearchResult, VectorEntry};
use crate::wal::manifest::{
    immutable_artifact_cache_key, BootstrapRef, ClusterDataObjectRef, LocatedSegmentRef,
    MembershipRef,
};

/// Resident routing metadata for one immutable IVF-Flat segment.
///
/// This is a search handle, not a full in-memory copy of the segment. It keeps
/// centroids, optional scalar-quantization calibration, optional coarse sketch,
/// and manifest-derived object routing in memory. Full cluster vectors,
/// attributes, bitmap sidecars, and quantized codes are loaded on demand.
///
/// Cloning the handle shares centroid and sketch allocations through [`Arc`].
/// Namespace, segment, owner, and object-reference collections are owned and
/// therefore cloned normally.
///
/// # Consistency
///
/// Constructing this value does not publish a segment. Callers on the query
/// path must derive it from the current authoritative manifest. A handle can
/// continue reading its immutable segment after a newer manifest is published,
/// but it cannot make that older segment current again.
///
/// # Examples
///
/// A manifest may describe segment `seg-42` with 256 centroids and grouped
/// cluster objects. Loading that entry creates one handle with resident routing
/// data; searching it fetches only the selected immutable object ranges.
#[derive(Debug, Clone)]
pub struct IvfFlatIndex {
    /// Shared centroid vectors, one per logical cluster.
    ///
    /// Every inner vector has exactly `dim` components. The outer index
    /// is also the logical cluster index used by routing metadata.
    pub(crate) centroids: Arc<Vec<Vec<f32>>>,
    /// Total number of vectors recorded for this segment.
    ///
    /// Manifest-aware loading trusts the published count rather than reading
    /// every cluster merely to recount it.
    pub(crate) num_vectors: usize,
    /// Number of `f32` components expected in every stored and query vector.
    pub(crate) dim: usize,
    /// Namespace prefix used to construct legacy and sidecar object keys.
    pub(crate) namespace: String,
    /// Physical namespace prefix that owns every immutable artifact selected
    /// through this handle.  This differs from `namespace` when a branch reads
    /// a source segment without copying it.
    pub(crate) physical_namespace: String,
    /// Incarnation-qualified physical owner used for disposable cache keys.
    /// Keeping this alongside the physical prefix prevents a recreated
    /// namespace from reusing stale immutable bytes with the same S3 key.
    pub(crate) physical_origin: Option<ArtifactOrigin>,
    /// Identity of the segment that owns segment-global artifacts.
    ///
    /// Centroids, calibration, codebooks, sketches, and bootstrap metadata live
    /// under this identity even when individual clusters were carried forward
    /// from older segments.
    pub(crate) segment_id: String,
    /// Encoding used for the segment's approximate candidate-scoring data.
    pub(crate) quantization: crate::index::quantization::QuantizationType,
    /// Scalar-quantization calibration embedded in current centroid artifacts.
    ///
    /// `None` means scalar quantization is not active or the legacy artifact
    /// path must obtain calibration elsewhere.
    pub(crate) sq_calibration: Option<crate::index::quantization::sq::SqCalibration>,
    /// Attribute field names with per-cluster bitmap sidecars.
    ///
    /// Manifest-aware query loading fills this after construction from the
    /// segment reference; an empty collection means no bitmap may be assumed.
    pub(crate) bitmap_fields: Vec<String>,
    /// Fields with bitmap coverage in every logical cluster.
    ///
    /// This is decoded from the resident bootstrap before query I/O. An empty
    /// set disables coarse attribute elision.
    pub(crate) bitmap_complete_fields: BTreeSet<String>,
    /// Exact bounded segment-wide cardinalities for equality and `IN` filters.
    ///
    /// Bootstrap v1/v2 and legacy separately stored metadata leave this absent;
    /// bootstrap v3 decodes it once with the other resident segment metadata.
    pub(crate) filter_summary: Option<Arc<filter_summary::FilterCardinalitySummary>>,
    /// Per-cluster owning segment identities for incremental carry-over.
    ///
    /// Entry `i` names the segment under which cluster `i`'s sidecar objects
    /// live. Empty means every cluster is owned by `segment_id`, as in
    /// legacy or full-rebuild layouts. Segment-global artifacts never consult
    /// this map.
    pub(crate) cluster_owners: Vec<String>,
    /// Manifest-defined immutable objects that contain one or more clusters.
    ///
    /// Empty selects the legacy one-object-per-cluster layout routed through
    /// `cluster_owner`.
    pub(crate) cluster_objects: Vec<ClusterDataObjectRef>,
    /// Dense map from logical cluster index to `cluster_objects` index.
    ///
    /// This is derived and validated while building or loading the handle so a
    /// query does not linearly search object references for every cluster.
    pub(crate) cluster_object_by_cluster: Vec<usize>,
    /// Shared coarse sketch used to narrow cluster/object reads when present.
    pub(crate) resident_sketch: Option<Arc<sketch::ResidentSketch>>,
    /// Reference for the resident-sketch artifact created or supplied at load.
    ///
    /// The reference describes an object; it does not by itself prove that a
    /// manifest has published that object.
    pub(crate) sketch_ref: Option<crate::wal::manifest::SketchRef>,
    /// Reference for the combined bootstrap artifact, when available.
    pub(crate) bootstrap_ref: Option<BootstrapRef>,
    /// Reference for vector-to-cluster membership emitted by a fresh build.
    ///
    /// Manifest-based query loading does not currently retain this reference,
    /// because membership is consumed by compaction and mutation workflows.
    pub(crate) membership_ref: Option<MembershipRef>,
}

impl IvfFlatIndex {
    /// Returns the decoded exact filter-cardinality summary when bootstrap v3
    /// supplied one.
    ///
    /// Legacy metadata and bootstrap v1/v2 return `None`, which means summary
    /// knowledge is absent. The returned value is resident metadata and this
    /// accessor performs no object-store I/O.
    #[must_use]
    pub fn filter_summary(&self) -> Option<&filter_summary::FilterCardinalitySummary> {
        self.filter_summary.as_deref()
    }

    /// Return an incarnation-qualified cache identity while retaining the exact
    /// manifest-selected S3 key for the actual object-store GET.
    #[must_use]
    pub(crate) fn artifact_cache_key(&self, store_key: &str) -> String {
        artifact_cache_key(self.physical_origin.as_ref(), store_key)
    }
    /// Reports the number of logical centroid clusters in this segment.
    ///
    /// # Returns
    ///
    /// The length of the resident centroid table. Cluster indexes range from
    /// zero up to, but not including, this value.
    ///
    /// # Examples
    ///
    /// A handle with 256 centroids returns `256`, so an `nprobe` above 256 can
    /// be clamped without referring to object storage.
    pub fn num_clusters(&self) -> usize {
        self.centroids.len()
    }

    /// Resolves the segment that owns one logical cluster's sidecar objects.
    ///
    /// Incremental compaction can carry a cluster forward without rewriting
    /// all of its per-cluster artifacts. Such a cluster keeps the older segment
    /// identity recorded in `cluster_owners`. A missing mapping falls back to
    /// this handle's segment identity, which is the full-rebuild and legacy
    /// layout.
    ///
    /// # Parameters
    ///
    /// - `cluster_idx`: Zero-based logical cluster index used by the centroid
    ///   table and per-cluster key builders.
    ///
    /// # Returns
    ///
    /// A string slice borrowed from either the owner table or `segment_id`.
    /// No allocation is performed.
    ///
    /// # Consistency
    ///
    /// Search code that constructs a per-cluster key must use this resolver.
    /// Reading under the current segment unconditionally would miss immutable
    /// artifacts carried over from an older segment.
    ///
    /// # Examples
    ///
    /// If cluster 7 was carried from `seg-40` into the metadata for `seg-42`,
    /// this method returns `seg-40`. A newly rebuilt cluster with no explicit
    /// owner returns `seg-42`.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The returned `&str` borrows storage already owned by `self`. Java would
    /// return another reference to a `String`; C might return a `const char *`.
    /// Rust additionally prevents the caller from using the slice after the
    /// index handle is gone, and the immutable borrow prevents mutation of the
    /// source string during use.
    pub(crate) fn cluster_owner(&self, cluster_idx: usize) -> &str {
        self.cluster_owners
            .get(cluster_idx)
            .map(String::as_str)
            .unwrap_or(&self.segment_id)
    }

    /// Finds the manifest-defined grouped object containing a logical cluster.
    ///
    /// An empty object table denotes the legacy layout and returns `None`.
    /// Otherwise the prevalidated dense lookup selects exactly one object
    /// reference without scanning the manifest collection.
    ///
    /// # Parameters
    ///
    /// - `cluster_idx`: Zero-based logical cluster index to resolve.
    ///
    /// # Returns
    ///
    /// `Ok(Some(reference))` for a grouped-object layout, or `Ok(None)` for the
    /// legacy one-object-per-cluster layout. The reference borrows this handle.
    ///
    /// # Errors
    ///
    /// Returns an index error if the logical cluster has no lookup entry or if
    /// that entry names a missing object. Such a mismatch means resident
    /// routing metadata is inconsistent; the method never guesses a key.
    ///
    /// # Examples
    ///
    /// If object 3 stores clusters 8 and 9, resolving cluster 9 returns object
    /// 3. A legacy segment with no grouped references returns `None`, directing
    /// the caller to `cluster_owner` and the legacy key format.
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

    /// Conservatively reports whether a cluster may contain any attributes.
    ///
    /// The resident sketch can prove that every row in a cluster has null
    /// attributes. Without a sketch, this method returns `true` so query code
    /// does not incorrectly skip an attribute sidecar.
    ///
    /// # Parameters
    ///
    /// - `cluster_idx`: Zero-based logical cluster index to inspect.
    ///
    /// # Returns
    ///
    /// `false` only when the resident sketch proves the cluster has no
    /// attributes; otherwise `true`.
    ///
    /// # Examples
    ///
    /// A sketch marking cluster 5 as all-null returns `false`. The same handle
    /// loaded from a legacy segment without a sketch returns `true`, preserving
    /// correctness at the cost of a possible extra object read.
    #[must_use]
    pub(crate) fn cluster_may_have_attrs(&self, cluster_idx: usize) -> bool {
        self.resident_sketch
            .as_ref()
            .map(|sketch| sketch.cluster_has_attrs(cluster_idx))
            .unwrap_or(true)
    }

    /// Returns the namespace used to address this segment's artifacts.
    ///
    /// # Returns
    ///
    /// A borrowed namespace string; no allocation or object-store access occurs.
    ///
    /// # Examples
    ///
    /// A handle loaded for namespace `catalog` returns `"catalog"` for key
    /// construction and structured diagnostics.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns the identity owning this index's segment-global artifacts.
    ///
    /// # Returns
    ///
    /// A borrowed segment identifier. Individual carried-over clusters may
    /// still resolve to older owners through `cluster_owner`.
    ///
    /// # Examples
    ///
    /// A handle for manifest segment `seg-42` returns `"seg-42"` even when one
    /// cluster's sidecar data remains under `seg-40`.
    pub fn segment_id(&self) -> &str {
        &self.segment_id
    }

    /// Returns the manifest-defined grouped cluster-data objects.
    ///
    /// # Returns
    ///
    /// A borrowed, manifest-order slice. An empty slice denotes the legacy
    /// one-object-per-cluster layout rather than an index with no clusters.
    ///
    /// # Examples
    ///
    /// A freshly built segment may return 64 references covering 128 logical
    /// clusters, while a legacy segment returns an empty slice and uses legacy
    /// per-cluster keys.
    pub fn cluster_objects(&self) -> &[ClusterDataObjectRef] {
        &self.cluster_objects
    }

    /// Probes immutable object-store artifacts to reconstruct an index handle.
    ///
    /// This compatibility-oriented loader reads centroids, lists the segment
    /// prefix, examines cluster objects to count vectors, and probes
    /// quantization artifacts. The production query path normally prefers
    /// [`Self::load_from_manifest`], which already has authoritative counts and
    /// references.
    ///
    /// # Parameters
    ///
    /// - `store`: Zeppelin's object-store boundary used for all GET and list
    ///   requests.
    /// - `namespace`: Namespace prefix containing the segment artifacts.
    /// - `segment_id`: Segment whose immutable artifacts should be inspected.
    ///
    /// # Returns
    ///
    /// A resident handle containing decoded centroids and discovered routing
    /// metadata. Cluster vectors remain in object storage until search.
    ///
    /// # Errors
    ///
    /// Returns a storage error for failed GET/list operations and an index or
    /// serialization error for missing, malformed, dimensionally inconsistent,
    /// or unsupported artifacts. The method does not substitute empty data.
    ///
    /// # Side Effects
    ///
    /// Performs read-only object-store requests. It does not write artifacts,
    /// update a cache, or publish a manifest.
    ///
    /// # Consistency
    ///
    /// This method does not verify that the named segment is visible in the
    /// current manifest. Its caller must already have selected an appropriate
    /// immutable segment, typically for tests, compatibility, or compaction.
    ///
    /// # Performance
    ///
    /// Performs a centroid GET and prefix list, then reads grouped objects or
    /// every legacy cluster to count vectors, plus quantization probes. This is
    /// intentionally more expensive than manifest-aware loading.
    ///
    /// # Examples
    ///
    /// A compaction test that has already built `seg-17` can call this method to
    /// recover its dimension, vector count, and quantization mode directly from
    /// stored artifacts. A missing cluster object returns an error instead of a
    /// partial handle.
    pub async fn load(store: &ZeppelinStore, namespace: &str, segment_id: &str) -> Result<Self> {
        build::load_ivf_flat(store, namespace, segment_id).await
    }

    /// Loads an IVF-Flat handle from authoritative manifest metadata.
    ///
    /// A current segment normally has a bootstrap object that co-locates
    /// centroids and the resident coarse sketch. Legacy segments instead load
    /// centroids and an optional sketch separately. Counts, quantization, owner
    /// routing, and grouped-object references come from the manifest, avoiding
    /// artifact probes and a full recount.
    ///
    /// # Parameters
    ///
    /// - `store`: Zeppelin's object-store boundary for cache misses and direct
    ///   metadata reads.
    /// - `namespace`: Namespace whose manifest supplied the segment metadata.
    /// - `segment_id`: Identity owning segment-global artifacts.
    /// - `num_vectors`: Published total vector count for the segment.
    /// - `quantization`: Published encoding used by candidate-scoring artifacts.
    /// - `cluster_owners`: Optional owner identity for each carried-over cluster.
    /// - `cluster_objects`: Published objects mapping logical clusters to stored
    ///   byte ranges or whole objects.
    /// - `sketch_ref`: Published reference to the resident coarse sketch, when
    ///   the segment has one.
    /// - `bootstrap_ref`: Published reference to combined bootstrap metadata,
    ///   when the segment uses the current layout.
    /// - `cache`: Optional shared tiered cache. The active segment's bootstrap
    ///   or legacy metadata is pinned by namespace scope.
    ///
    /// # Returns
    ///
    /// A handle with resident centroids, optional calibration/sketch data, and
    /// validated logical-cluster routing. Cluster payloads remain remote.
    ///
    /// # Errors
    ///
    /// Returns an error when cache or object-store reads fail, referenced
    /// objects have unexpected sizes or invalid encodings, a bootstrap lacks
    /// its required sketch reference, or grouped-object metadata does not cover
    /// the centroid table consistently. No probing fallback hides corruption.
    ///
    /// # Side Effects
    ///
    /// May populate memory and disk cache tiers, update decoded-object reuse,
    /// and change which metadata key is pinned for the namespace. A cache-less
    /// call performs direct object-store GETs only.
    ///
    /// # Consistency
    ///
    /// The supplied metadata must come from one manifest snapshot. Cache bytes
    /// are an optimization and, where the manifest carries a size, are checked
    /// against that published metadata. They never override the manifest's
    /// segment description.
    ///
    /// # Performance
    ///
    /// A bootstrap cache hit can avoid object storage entirely. A cold current
    /// segment usually needs one bootstrap GET; a legacy segment may need
    /// separate centroid and sketch GETs. The method avoids per-cluster counting
    /// and quantization-detection probes.
    ///
    /// # Examples
    ///
    /// Suppose manifest version 42 names `seg-42`, 1,000,000 vectors, grouped
    /// cluster objects, and one bootstrap reference. This method loads the
    /// bootstrap, validates its sections, and returns a routing handle without
    /// reading the million vectors. If the bootstrap size differs from the
    /// manifest, loading fails loudly.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Owner and object vectors are moved into the returned handle instead of
    /// copied. Java passes collection references and relies on convention to
    /// avoid later mutation; C often transfers pointers with an ownership
    /// comment. Rust makes the move explicit: after the call, the caller cannot
    /// reuse those `Vec` values unless it cloned them first. The optional cache
    /// is only borrowed, while its [`Arc`] allows the underlying cache to remain
    /// shared safely across async query tasks.
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
        bootstrap_ref: Option<BootstrapRef>,
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
            bootstrap_ref,
            cache,
        )
        .await
    }

    /// Loads a manifest-selected segment through its resolved physical owner.
    ///
    /// The logical namespace remains attached to the returned handle for
    /// accounting and cache-pin scope; every immutable object GET derives from
    /// `located.physical_namespace()` instead.
    pub(crate) async fn load_from_located_manifest(
        store: &ZeppelinStore,
        located: LocatedSegmentRef<'_>,
        cache: Option<&std::sync::Arc<crate::cache::DiskCache>>,
    ) -> Result<Self> {
        let segment = located.segment;
        build::load_ivf_flat_from_located_manifest(store, located, cache)
            .await
            .map(|mut index| {
                // Preserve the manifest's logical target for caller-visible
                // accounting while build routing has already used the source.
                index.namespace = located.logical_namespace.to_string();
                index.segment_id = segment.id.clone();
                index
            })
    }
}

pub(crate) fn artifact_cache_key(origin: Option<&ArtifactOrigin>, store_key: &str) -> String {
    origin.map_or_else(
        || store_key.to_string(),
        |origin| immutable_artifact_cache_key(origin, store_key),
    )
}

pub(crate) fn cache_pin_scope(
    logical_origin: Option<&ArtifactOrigin>,
    logical_namespace: &str,
    role: &str,
) -> String {
    logical_origin.map_or_else(
        || format!("{logical_namespace}:{role}"),
        |origin| {
            format!(
                "artifact-origin/{}/pin/{role}",
                origin.incarnation.as_uuid().simple()
            )
        },
    )
}

#[async_trait]
impl VectorIndex for IvfFlatIndex {
    /// Trains an IVF-Flat segment and writes its immutable index artifacts.
    ///
    /// The build pipeline validates dimensions, trains centroids, assigns rows,
    /// creates cluster/attribute/bitmap/quantization sidecars, writes membership
    /// and coarse metadata, and returns a resident handle for the new segment.
    /// Manifest publication belongs to the compaction workflow and is not part
    /// of this method.
    ///
    /// # Parameters
    ///
    /// - `vectors`: Complete borrowed row set for the segment. It must be
    ///   non-empty, every vector must have a positive identical dimension, and
    ///   identifiers/attributes are cloned into artifact-building buffers.
    /// - `config`: Borrowed centroid-training, quantization, and bitmap choices.
    /// - `store`: Object-store boundary receiving immutable segment objects.
    /// - `namespace`: Namespace prefix under which artifacts are written.
    /// - `segment_id`: New segment identity used in every artifact key.
    ///
    /// # Returns
    ///
    /// A handle for the newly built artifacts, including references that a
    /// higher layer can place in a future manifest segment entry.
    ///
    /// # Errors
    ///
    /// Returns an index or dimension error for invalid input, a training error
    /// when centroids or quantization cannot be learned, a serialization error
    /// for an invalid artifact, or a storage error when any PUT fails.
    /// Previously written immutable objects may remain after a later failure;
    /// they are not visible unless a manifest subsequently references them.
    ///
    /// # Side Effects
    ///
    /// Performs CPU-intensive training and writes centroids, grouped cluster
    /// data, attributes, optional bitmap/quantization data, membership, sketch,
    /// and bootstrap objects to S3/MinIO through [`ZeppelinStore`].
    ///
    /// # Consistency
    ///
    /// Object creation precedes manifest publication. Callers must publish all
    /// returned references atomically through the manifest and must never
    /// overwrite the segment's immutable objects in place.
    ///
    /// # Performance
    ///
    /// Training and assignment scan the input vectors in memory. Artifact size
    /// is proportional to the row count and dimension; independent object PUTs
    /// are issued concurrently after CPU serialization.
    ///
    /// # Examples
    ///
    /// ```text
    /// 100,000 visible WAL rows
    ///          |
    ///          v
    /// build new immutable seg-42 objects
    ///          |
    ///          | success: objects exist but are not visible
    ///          v
    /// compaction publishes a manifest referencing seg-42
    /// ```
    ///
    /// A failed bitmap-object PUT leaves no partially published segment: any
    /// already uploaded objects remain unreferenced and therefore invisible.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `vectors: &[VectorEntry]` and `config: &IndexingConfig` are temporary
    /// shared borrows. They resemble read-only Java references or `const *`
    /// parameters in C, but Rust proves they stay valid across each use. The
    /// returned handle owns its metadata, so it can outlive both input borrows.
    async fn build(
        vectors: &[VectorEntry],
        config: &IndexingConfig,
        store: &ZeppelinStore,
        namespace: &str,
        segment_id: &str,
    ) -> Result<Self> {
        build::build_ivf_flat(vectors, config, store, namespace, segment_id).await
    }

    /// Searches this segment through the common index interface.
    ///
    /// This convenience path selects a filter oversampling factor of three,
    /// disables the optional disk cache, includes result attributes, and uses
    /// the configured default gap for coalescing exact-rerank byte ranges.
    /// Callers needing those controls invoke `search::search_ivf_flat` directly.
    ///
    /// # Parameters
    ///
    /// - `query`: Borrowed query vector whose dimension must equal
    ///   [`VectorIndex::dimension`].
    /// - `top_k`: Maximum number of nearest results to return. Zero returns an
    ///   empty collection without reading cluster data.
    /// - `nprobe`: Maximum number of nearest centroid regions to consider. A
    ///   value above the cluster count is clamped.
    /// - `filter`: Optional borrowed attribute predicate. Rows without
    ///   attributes cannot satisfy it.
    /// - `distance_metric`: Ranking function used for centroid and row scores.
    /// - `store`: Object-store boundary used to read immutable cluster data and
    ///   sidecars.
    ///
    /// # Returns
    ///
    /// Up to `top_k` nearest matching rows, ordered from best to worst score.
    /// Fewer results means the probed clusters did not contain more matching
    /// rows; IVF search is approximate when not every cluster is probed.
    ///
    /// # Errors
    ///
    /// Returns a dimension mismatch for an incompatible query, an index error
    /// for inconsistent routing or corrupt data, or a storage/serialization
    /// error when a required immutable artifact cannot be read or decoded.
    ///
    /// # Side Effects
    ///
    /// Performs read-only object-store requests and emits structured tracing
    /// and search metrics. This wrapper does not populate the optional disk
    /// cache.
    ///
    /// # Consistency
    ///
    /// The search reads the immutable segment named by this handle. It neither
    /// reloads nor publishes a manifest; the query coordinator is responsible
    /// for choosing handles from one authoritative manifest snapshot.
    ///
    /// # Performance
    ///
    /// Scores all resident centroids, then reads only the selected cluster
    /// objects. Larger `nprobe` values generally increase object bytes and CPU
    /// while improving recall. A filter raises the candidate target to three
    /// times `top_k` before exact predicate evaluation.
    ///
    /// # Examples
    ///
    /// Searching a 256-cluster segment with `top_k = 10` and `nprobe = 8`
    /// scores 256 resident centroids and considers up to eight nearby regions.
    /// Adding a category filter raises the candidate target to 30 before the
    /// method returns at most ten exact matches.
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
            true,
            crate::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        )
        .await
    }

    /// Reports the published number of vectors represented by this handle.
    ///
    /// # Returns
    ///
    /// The resident count supplied by a build, artifact probe, or manifest.
    /// No cluster object is read to answer this call.
    ///
    /// # Examples
    ///
    /// A manifest-loaded million-row segment returns `1_000_000` in constant
    /// time even though its vector payloads remain in object storage.
    fn vector_count(&self) -> usize {
        self.num_vectors
    }

    /// Reports the component count required for stored and query vectors.
    ///
    /// # Returns
    ///
    /// The positive vector dimension decoded from segment metadata.
    ///
    /// # Examples
    ///
    /// A 768-component embedding segment returns `768`; a 1,024-component
    /// query is rejected before cluster data is scanned.
    fn dimension(&self) -> usize {
        self.dim
    }
}
