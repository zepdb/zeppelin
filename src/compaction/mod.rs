//! WAL-to-segment compaction and authoritative manifest publication.
//!
//! This module turns the immutable WAL fragments referenced by a namespace's
//! live [`Manifest`][crate::wal::manifest::Manifest] into immutable vector,
//! attribute, bitmap, quantization, sketch, membership, and optional full-text
//! index artifacts. The object-store manifest remains the visibility boundary:
//! uploading those artifacts does not make them queryable. Only a successful
//! ETag compare-and-swap (CAS) that installs their
//! [`SegmentRef`][crate::wal::manifest::SegmentRef] does.
//!
//! [`Compactor`][crate::compaction::Compactor] is the domain coordinator.
//! Production normally enters it through
//! [`background::compact_namespace_under_lease_with_lifecycle`][crate::compaction::background::compact_namespace_under_lease_with_lifecycle], which acquires and
//! renews a per-namespace lease, supplies its fencing token, and runs on the
//! dedicated compaction runtime. Direct entry points remain useful to tests and
//! administrative tools. This file calls the WAL reader, IVF-Flat or
//! hierarchical builders, full-text builders, the storage abstraction, and the
//! manifest API; it does not serve queries or physically delete retired
//! objects. [`gc`][crate::compaction::gc] owns that later reclamation step.
//!
//! ## WAL-to-segment lifecycle
//!
//! ```text
//! authoritative Manifest snapshot
//!        | exact uncompacted FragmentRef set
//!        v
//! read immutable WAL objects -> merge add/update/delete by manifest order
//!        |
//!        +--------------------+--------------------------+
//!        |                    |                          |
//!        v                    v                          v
//! full retrain        reuse centroids, read all   bounded membership path
//! all clusters new    survivors, rewrite touched  read touched clusters only
//!        |                    |                          |
//!        +--------------------+--------------------------+
//!                             |
//!                             v
//! upload immutable segment/index artifacts
//!      (objects exist, but are not yet visible)
//!                             |
//!                 optional fenced staging root for GC
//!                             |
//!                             v
//! re-read Manifest + ETag -> fencing/lease checks -> conditional PUT
//!                             |                         |
//!                          success                  CAS miss
//!                             |                         |
//!                             v                         v
//!                  new segment is visible       reload and retry only
//!                  old keys are deferred        manifest publication
//! ```
//!
//! The expensive WAL merge and index build happen once. A CAS conflict retries
//! only the small manifest mutation, preserving fragments that arrived after
//! the original snapshot. If publication ultimately fails, newly uploaded
//! objects can remain unreferenced; fenced production compactions also publish
//! a staging side object so storage GC does not race an upload still in flight.
//!
//! ## Incremental ownership and deletion safety
//!
//! ```text
//! old logical segment S1
//!   cluster 0 -> object under S1       touched by WAL
//!   cluster 1 -> object under S0       unchanged carry-over
//!                     |
//!                     v
//! new logical segment S2
//!   cluster 0 owner = S2 -> new immutable object
//!   cluster 1 owner = S0 -> same immutable object
//!                     |
//!                     v
//! defer old S1 objects EXCEPT every key still referenced by S2
//! ```
//!
//! A logical segment can therefore reference physical cluster data beneath an
//! older segment prefix. Prefix age is never proof that an object is dead.
//! `cluster_owners`, explicit grouped-object references, retained manifest
//! history, and active staging roots all participate in reachability. The
//! corresponding formal-model work is described in
//! `tasks/FormalVerification/04-tla-incremental-artifact-closure.md`; staging
//! and two-pass GC safety are described in
//! `tasks/FormalVerification/02-tla-storage-gc-safety.md`.
//!
//! ## Formal models
//!
//! - [CompactionSafety](https://github.com/Ghatage/zeppelin/blob/main/formal-verifications/tla/CompactionSafety.tla)
//!   demonstrates why a stale unconditional manifest write loses a concurrent
//!   WAL append and motivates the CAS loop.
//! - [CompactionRetryConvergence](https://github.com/Ghatage/zeppelin/blob/main/formal-verifications/tla/CompactionRetryConvergence.tla)
//!   separates retry exhaustion as a liveness failure from data-loss safety.
//! - [MultiWriterLease](https://github.com/Ghatage/zeppelin/blob/main/formal-verifications/tla/MultiWriterLease.tla)
//!   models lease acquisition, fencing, CAS, expiry, and stale-writer rejection.
//! - [IndexAtomicity](https://github.com/Ghatage/zeppelin/blob/main/formal-verifications/tla/IndexAtomicity.tla)
//!   models upload-all-before-manifest publication.
//! - [IncrementalArtifactClosure](https://github.com/Ghatage/zeppelin/blob/main/formal-verifications/tla/IncrementalArtifactClosure.tla)
//!   models carried cluster ownership and exact-key GC reachability.
//! - [TwoPassGcSafety](https://github.com/Ghatage/zeppelin/blob/main/formal-verifications/tla/TwoPassGcSafety.tla)
//!   models live/history/staging roots and fresh reachability at sweep time.
//!
//! ## CPU, I/O, and async ownership
//!
//! ```text
//! caller owns Arc<Compactor>
//!          |
//!          | compact(&self): temporary shared borrow
//!          v
//! async future owns per-run Manifest, maps, vectors, and artifact Bytes
//!          |
//!          +-- async GET futures borrow &ZeppelinStore
//!          |      `-- join_all overlaps network waits; no detached task
//!          |
//!          +-- index CPU runs on dedicated compaction runtime
//!          |
//!          +-- FTS CPU moves owned attrs/config into spawn_blocking tasks
//!          |      `-- JoinHandle returns owned serialized indexes
//!          |
//!          `-- PUT futures borrow store and share cloned Bytes buffers
//!                 |
//!                 v
//!       all borrows end when the compaction future completes
//! ```
//!
//! The index build and serialization phases are CPU work; object GETs, PUTs,
//! LISTs, staging, and manifest publication are I/O phases. Full-text building
//! explicitly crosses from async into blocking workers. The other heavy index
//! work stays isolated from query-serving threads because
//! [`background`][crate::compaction::background] owns a dedicated Tokio runtime.
//!
//! ## Reading map
//!
//! 1. Start with [`CompactionResult`][crate::compaction::CompactionResult] and
//!    [`Compactor`][crate::compaction::Compactor] for the caller-facing API and
//!    retained dependencies.
//! 2. Read
//!    [`Compactor::should_compact`][crate::compaction::Compactor::should_compact]
//!    for count, age, byte, and index-layout trigger decisions.
//! 3. Read
//!    [`Compactor::compact_with_fts_signaled`][crate::compaction::Compactor::compact_with_fts_signaled]
//!    for the complete snapshot, build, staging, fencing, CAS, and
//!    deferred-deletion transaction.
//! 4. Follow `incremental_build_bounded`, `incremental_build`, and
//!    `write_incremental_segment` for the two centroid-reuse paths.
//! 5. Finish with `load_touched_segment_vectors`, `load_segment_vectors`, and
//!    `incremental_cluster_objects` to understand physical object ownership.
//! 6. Continue into [`background`][crate::compaction::background] for lease
//!    renewal, CPU isolation, cache warming, and periodic scheduling, then
//!    [`gc`][crate::compaction::gc] for physical reclamation.
//!
//! ## Invariants
//!
//! - S3 or MinIO is authoritative. Memory holds a candidate snapshot only.
//! - WAL fragments and segment artifacts are immutable; compaction creates new
//!   objects and never edits the old objects in place.
//! - The manifest CAS is the visibility commit. A stale ETag never overwrites a
//!   newer fragment or segment inventory.
//! - Lease fencing and CAS are separate defenses. The heartbeat abort flag
//!   closes most of the interval between a fencing check and publication.
//! - Production permits one lease-owning compactor per namespace. CAS retry
//!   rebases the manifest edit over WAL appends; it does not rebuild a candidate
//!   over a segment independently published by a competing compactor.
//! - Fragment removal uses the exact IDs read by this run, never a ULID
//!   watermark that could swallow a concurrent same-millisecond fragment.
//! - Every carried cluster object and global sidecar remains reachable from the
//!   new manifest; deletion favors leaks over deleting possibly live data.
//! - Quantization calibration or codebooks are reused with carried codes.
//!   Recalibrating only part of a segment would silently corrupt distances.
//! - Missing or corrupt required artifacts fail the cycle or trigger an
//!   explicitly metered correctness-preserving full rebuild; they are never
//!   replaced with empty data.
//!
//! ## Rust concepts used here
//!
//! The coordinator borrows shared clients as `&self` across async operations,
//! while owned manifests, vectors, and descriptors make each candidate build
//! independent. In Java, this resembles a service with immutable request-local
//! state; in C it would require an explicit ownership and cleanup convention.
//! Rust prevents a borrowed value from outliving its owner and makes moved
//! artifact descriptions unavailable at the old binding.
//!
//! [`bytes::Bytes`] clones share immutable buffers by reference count, so the
//! parallel PUT phase does not deep-copy every payload. Iterator futures are
//! collected with [`futures::future::join_all`] to overlap object-store I/O;
//! CPU-heavy full-text construction is moved to [`tokio::task::spawn_blocking`]
//! so it does not block an async worker. The production scheduler additionally
//! places compaction on a dedicated Tokio runtime.
//!
//! Optional lease-loss state is shared as
//! [`Arc<AtomicBool>`][std::sync::Arc] rather than a nullable raw pointer or a
//! mutable global. Java would commonly use an `AtomicBoolean`; C would require
//! explicit shared lifetime and atomic-ordering discipline. Rust's `Arc` keeps
//! the flag alive until both heartbeat and compactor release it.

/// Runs lease-protected background compaction and cache warming.
///
/// See
/// [`background::compact_namespace_under_lease_with_lifecycle`]
/// for the production entry point that supplies the fencing token and
/// lease-loss signal used here.
pub mod background;
/// Computes exact-key reachability and reclaims artifacts after a safety horizon.
///
/// Compaction records retired keys in the manifest; this module deliberately
/// leaves the physical DELETE operations to [`gc`][crate::compaction::gc].
pub mod gc;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use tracing::{debug, error, info, instrument, warn};
use ulid::Ulid;

use crate::config::{CompactionConfig, IndexingConfig};
use crate::error::{Result, ZeppelinError};
use crate::fts::inverted_index::{fts_index_key, InvertedIndex};
use crate::fts::FtsFieldConfig;
use crate::index::hierarchical::build::build_hierarchical;
use crate::index::hierarchical::HierarchicalIndex;
use crate::index::ivf_flat::build::{
    attrs_key, build_ivf_flat, cluster_group_key, cluster_key, cluster_object_sections,
    deserialize_attrs, deserialize_cluster,
};
use crate::index::ivf_flat::membership::{
    build_membership_artifact, deserialize_membership, MembershipData,
};
use crate::namespace::manager::{NamespaceMetadata, NamespaceState};
use crate::security::{NamespaceId, PreservationService};
use crate::storage::ZeppelinStore;
use crate::time::Clock;
use crate::types::VectorEntry;
use crate::wal::fragment::WalFragment;
use crate::wal::manifest::{
    BootstrapRef, ClusterDataObjectRef, LocatedFragmentIdentity, LocatedFragmentRef,
    LocatedSegmentRef, Manifest, MembershipRef, SegmentRef,
};
use crate::wal::{FragmentCachePolicy, WalReader};

/// Maximum number of fresh-read/CAS publication attempts in one compaction.
///
/// Index artifacts are built once before this loop. Exhausting the attempts
/// returns [`ZeppelinError::ManifestConflict`] and leaves the uploaded objects
/// unreferenced rather than overwriting a newer manifest.
const MAX_CAS_RETRIES: u32 = 10;
/// Metrics label for bytes read from vector cluster objects.
const COMPACTION_READ_CLASS_CLUSTER: &str = "cluster";
/// Metrics label for bytes read from per-vector attribute sidecars.
const COMPACTION_READ_CLASS_ATTRS: &str = "attrs";
/// Metrics label for bytes read from the segment-global centroid artifact.
const COMPACTION_READ_CLASS_CENTROIDS: &str = "centroids";
/// Metrics label for scalar or product quantization calibration data.
const COMPACTION_READ_CLASS_SQ: &str = "sq";
/// Metrics label for bytes read from the resident coarse-search sketch.
const COMPACTION_READ_CLASS_SKETCH: &str = "sketch";
/// Metrics label for bytes read from the vector-to-cluster membership map.
const COMPACTION_READ_CLASS_MEMBERSHIP: &str = "membership";

/// Fetches one compaction artifact and attributes the operation and bytes read.
///
/// # Parameters
///
/// - `store`: Borrowed object-store boundary used for the GET.
/// - `namespace`: Namespace label used for metrics; it does not alter `key`.
/// - `key`: Complete immutable object key to fetch.
/// - `class`: Stable low-cardinality artifact class used as a metric label.
///
/// # Returns
///
/// Shared immutable bytes containing the complete object.
///
/// # Errors
///
/// Propagates missing-object and storage failures. The operation counter has
/// already advanced, but the byte counter advances only after a successful GET.
///
/// # Side Effects
///
/// Performs one object-store GET and updates compaction read metrics.
///
/// # Performance
///
/// Allocates no second payload copy beyond the store implementation's returned
/// [`bytes::Bytes`]. Callers often run several of these futures concurrently.
///
/// # Examples
///
/// Loading a 4 KiB membership artifact increments the `membership` operation
/// counter once and its byte counter by 4 KiB. A missing object increments only
/// the attempted-operation counter and fails the compaction.
async fn get_compaction_read(
    store: &ZeppelinStore,
    namespace: &str,
    key: &str,
    class: &str,
) -> Result<bytes::Bytes> {
    crate::metrics::COMPACTION_READ_OPS_TOTAL
        .with_label_values(&[namespace, class])
        .inc();
    let data = store.get(key).await?;
    crate::metrics::COMPACTION_READ_BYTES_TOTAL
        .with_label_values(&[namespace, class])
        .inc_by(data.len() as u64);
    Ok(data)
}

/// Resolves the manifest's active segment ID to its borrowed descriptor.
///
/// # Parameters
///
/// - `manifest`: Candidate or authoritative manifest view to inspect.
///
/// # Returns
///
/// The matching [`SegmentRef`] when both the active ID and its descriptor are
/// present; `None` for a new manifest or internally inconsistent reference.
///
/// # Examples
///
/// A manifest whose `active_segment` is `seg_2` and whose segment list contains
/// `seg_2` yields that descriptor without cloning it.
///
/// # Rust Notes for Java/C Engineers
///
/// The returned reference borrows storage inside `manifest`. It resembles a
/// Java object reference or C `const SegmentRef *`, but it is non-null in the
/// `Some` branch and cannot outlive the manifest borrow.
fn active_segment_ref(manifest: &Manifest) -> Option<&SegmentRef> {
    let active_segment = manifest.active_segment.as_ref()?;
    manifest
        .segments
        .iter()
        .find(|segment| segment.id == *active_segment)
}

/// Returns the only prefix this logical target is allowed to sweep for an old segment.
///
/// A branch may compact a segment physically owned by another namespace lifetime.
/// Such an object remains reachable to the logical target but is never a target-owned
/// deletion candidate. Comparing the complete origins, rather than namespace text,
/// also protects objects from an earlier incarnation of a recreated namespace.
#[must_use]
fn target_owned_old_segment_prefix(located: LocatedSegmentRef<'_>) -> Option<String> {
    (located.physical_origin.as_origin() == located.logical_origin.as_origin()).then(|| {
        format!(
            "{}/segments/{}/",
            located.physical_namespace(),
            located.segment.id
        )
    })
}

/// Classifies one retired WAL fragment as a target-owned deletion candidate.
#[must_use]
fn target_owned_fragment_deletion_key(located: LocatedFragmentRef<'_>) -> Option<String> {
    (located.physical_origin.as_origin() == located.logical_origin.as_origin())
        .then(|| WalFragment::s3_key(located.physical_origin.namespace(), &located.fragment.id))
}

/// Reports whether the active segment's physical layout differs from config.
///
/// # Parameters
///
/// - `manifest`: Manifest whose active segment is the current compacted view.
/// - `config`: Effective namespace indexing configuration desired now.
///
/// # Returns
///
/// `true` only when an active descriptor exists and
/// `segment_matches_index_config` rejects it. A namespace with no active segment
/// has nothing to rewrite and returns `false`.
///
/// # Examples
///
/// Switching an existing flat segment to hierarchical indexing returns `true`
/// even with no pending WAL fragments, allowing compaction to rebuild layout.
fn manifest_needs_index_rewrite(manifest: &Manifest, config: &IndexingConfig) -> bool {
    active_segment_ref(manifest)
        .is_some_and(|segment| !segment_matches_index_config(segment, config))
}

/// Resolves process defaults with one already-observed namespace overlay.
///
/// The metadata snapshot may be stale when it is used only to decide whether
/// background compaction should wake up. The compaction transaction performs a
/// separate authoritative metadata GET under the lease before it changes any
/// visible state.
fn resolve_indexing_config(
    namespace: &str,
    metadata: &NamespaceMetadata,
    defaults: &IndexingConfig,
) -> Result<IndexingConfig> {
    match metadata.state {
        NamespaceState::Active => {}
        NamespaceState::Creating => {
            return Err(ZeppelinError::ManifestConflict {
                namespace: namespace.to_string(),
            });
        }
        NamespaceState::Deleting => {
            return Err(ZeppelinError::NamespaceDeleting {
                namespace: namespace.to_string(),
            });
        }
    }

    if let Some(namespace_config) = metadata.index_config.as_ref() {
        namespace_config.validate(metadata.dimensions)?;
        return Ok(namespace_config.apply_to_indexing_config(defaults));
    }
    Ok(defaults.clone())
}

/// Compares manifest-visible layout choices with the effective index config.
///
/// # Parameters
///
/// - `segment`: Active immutable segment descriptor.
/// - `config`: Desired quantization, hierarchy, and centroid-count settings.
///
/// # Returns
///
/// `false` when quantization or hierarchy differ. Hierarchical layouts otherwise
/// match without comparing leaf count. Flat layouts also require the cluster
/// count to equal the scale-aware centroid target for `vector_count`.
///
/// # Examples
///
/// A ten-vector flat segment with ten clusters matches a configured 64-centroid
/// target because a build cannot create more non-empty training clusters than
/// vectors. The same descriptor does not match if scalar quantization is newly
/// enabled.
fn segment_matches_index_config(segment: &SegmentRef, config: &IndexingConfig) -> bool {
    if segment.quantization != config.quantization || segment.hierarchical != config.hierarchical {
        return false;
    }
    if segment.hierarchical {
        return true;
    }
    let expected_clusters = config.effective_num_centroids(segment.vector_count);
    segment.cluster_count == expected_clusters
}

/// Caller-visible outcome of one complete compaction attempt.
///
/// This value describes the manifest change that successfully became visible.
/// It does not claim that deferred old objects have already been deleted.
///
/// # Examples
///
/// Replacing `seg_old` with a 50,000-vector segment built from four fragments
/// reports the new ID, `vectors_compacted = 50_000`,
/// `fragments_removed = 4`, and `old_segment_removed = Some("seg_old")`.
/// A namespace with no work returns all counts at zero and no segment IDs.
#[derive(Debug)]
pub struct CompactionResult {
    /// ID of the newly published segment, or `None` for a no-op or all-deleted run.
    pub segment_id: Option<String>,
    /// Number of surviving vectors represented by the published segment.
    ///
    /// This is zero when all visible data was deleted or no work was needed.
    pub vectors_compacted: usize,
    /// Number of exact snapshot WAL fragment descriptors removed by the CAS.
    pub fragments_removed: usize,
    /// ID of the previously active segment removed from the live view, if any.
    ///
    /// Its physical objects may remain until deferred deletion and exact-key GC
    /// prove them unreachable.
    pub old_segment_removed: Option<String>,
}

/// Coordinates immutable WAL compaction, index construction, and manifest CAS.
///
/// A compactor owns cheap-to-clone storage and WAL clients plus process-wide
/// defaults. Per-namespace metadata can override indexing choices at run time.
/// The type contains no authoritative namespace state between calls; every run
/// starts by reading S3 or MinIO.
pub struct Compactor {
    /// Object-store abstraction used for all metadata and artifact I/O.
    store: ZeppelinStore,
    /// Reader that decodes the exact manifest-referenced WAL snapshot.
    wal_reader: WalReader,
    /// Trigger, retry-adjacent, and retention limits for compaction.
    config: CompactionConfig,
    /// Process defaults overlaid by namespace-specific indexing metadata.
    indexing_config: IndexingConfig,
    /// Maximum artifact-upload age allowed before publication.
    ///
    /// This is derived from GC configuration so in-flight objects cannot age
    /// past the horizon that staging is intended to protect.
    upload_window: Duration,
    /// Explicit wall clock used for manifest stamps and GC orchestration.
    clock: Clock,
    /// Fresh fail-closed preservation authority, when licensed at boot.
    preservation: Option<Arc<PreservationService>>,
    /// Test-only hook: artificial delay injected after index build and
    /// before the final manifest CAS loop, simulating a compaction whose
    /// build phase outlasts the lease duration. Always `None` in production
    /// (`Compactor::new` never sets it); only `set_test_pre_cas_delay` does.
    test_pre_cas_delay: Option<Duration>,
}

/// Segment-global training state reused by an incremental IVF-Flat build.
///
/// Calibration bytes are retained as well as their decoded form because the
/// new segment must publish byte-compatible global metadata while encoding
/// rewritten clusters against the same numeric scale as carried clusters.
struct IncrementalCentroidState {
    /// Borrow-independent owned centroid vectors, one per logical cluster.
    centroids: Vec<Vec<f32>>,
    /// Vector dimensionality encoded with the centroid artifact.
    dim: usize,
    /// Original scalar-quantization calibration bytes, when SQ is active.
    sq_calibration_bytes: Option<bytes::Bytes>,
    /// Decoded calibration used to encode rewritten cluster vectors.
    sq_calibration: Option<crate::index::quantization::sq::SqCalibration>,
}

/// Per-cluster rows and rewrite decisions for an incremental segment.
///
/// All four vectors have one entry per logical cluster. Untouched bounded-path
/// clusters can carry IDs only: their vectors and attributes remain in the old
/// immutable objects and are represented by placeholders here for cardinality.
struct IncrementalClusterState {
    /// Ordered vector IDs to record in the new membership artifact.
    cluster_ids: Vec<Vec<String>>,
    /// Full values for rewritten clusters, or empty placeholders when carried.
    cluster_vecs: Vec<Vec<Vec<f32>>>,
    /// Attribute rows aligned with `cluster_ids` for rewritten clusters.
    cluster_attrs: Vec<Vec<Option<HashMap<String, crate::types::AttributeValue>>>>,
    /// Per-cluster flag selecting rewrite (`true`) or immutable carry-over.
    touched: Vec<bool>,
}

impl IncrementalClusterState {
    /// Counts membership rows across all rewritten and carried clusters.
    ///
    /// # Returns
    ///
    /// Total logical vector count for the candidate segment.
    ///
    /// # Performance
    ///
    /// Runs in `O(number of clusters)` and does not inspect or clone vector data.
    ///
    /// # Examples
    ///
    /// Cluster ID lists of lengths 4, 0, and 7 report 11 even when the seven
    /// rows belong to a carried cluster whose values are not resident.
    fn vector_count(&self) -> usize {
        self.cluster_ids.iter().map(Vec::len).sum()
    }
}

impl Compactor {
    /// Creates a stateless compaction coordinator from shared infrastructure.
    ///
    /// # Parameters
    ///
    /// - `store`: Cloneable object-store boundary for authoritative metadata and
    ///   immutable artifact I/O.
    /// - `wal_reader`: Reader configured for the same store and namespace key
    ///   conventions.
    /// - `config`: Compaction trigger and manifest-retention limits.
    /// - `indexing_config`: Process-wide indexing defaults; namespace metadata
    ///   may override supported fields for each run.
    /// - `upload_window`: Maximum interval from segment-ID allocation to final
    ///   publication, normally derived from the GC safety configuration.
    ///
    /// # Returns
    ///
    /// An owned compactor with no namespace state loaded and no background task
    /// started. The production scheduler wraps it in [`std::sync::Arc`].
    ///
    /// # Examples
    ///
    /// Startup constructs one compactor and shares it with the HTTP state and
    /// background loop. Each later `compact` call still reloads the namespace's
    /// manifest rather than trusting state retained in this value.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Parameters are moved into `Self`; callers cannot use non-`Copy` inputs
    /// afterward unless they cloned them first. This resembles constructor
    /// ownership in Java, but Java leaves aliases usable. In C, the equivalent
    /// ownership transfer is only a convention; Rust enforces it at compile
    /// time.
    pub fn new(
        store: ZeppelinStore,
        wal_reader: WalReader,
        config: CompactionConfig,
        indexing_config: IndexingConfig,
        upload_window: Duration,
    ) -> Self {
        Self::with_clock(
            store,
            wal_reader,
            config,
            indexing_config,
            upload_window,
            Clock::system(),
        )
    }

    /// Creates a compactor with an explicitly selected wall-clock source.
    #[must_use]
    pub fn with_clock(
        store: ZeppelinStore,
        wal_reader: WalReader,
        config: CompactionConfig,
        indexing_config: IndexingConfig,
        upload_window: Duration,
        clock: Clock,
    ) -> Self {
        Self {
            store,
            wal_reader,
            config,
            indexing_config,
            upload_window,
            clock,
            preservation: None,
            test_pre_cas_delay: None,
        }
    }

    /// Attach the boot-composed preservation authority to destructive work.
    #[must_use]
    pub fn with_preservation_service(
        mut self,
        preservation: Option<Arc<PreservationService>>,
    ) -> Self {
        self.preservation = preservation;
        self
    }

    /// Borrow the preservation authority shared with background GC.
    #[must_use]
    pub(crate) fn preservation_service(&self) -> Option<&Arc<PreservationService>> {
        self.preservation.as_ref()
    }

    /// Injects a test-only delay between artifact construction and manifest CAS.
    ///
    /// # Parameters
    ///
    /// - `delay`: Time to sleep before the publication loop. Tests choose a
    ///   duration longer than the lease to exercise renewal and takeover paths.
    ///
    /// # Side Effects
    ///
    /// Mutates only this compactor's test hook. Production construction always
    /// leaves it disabled.
    ///
    /// # Examples
    ///
    /// A test with a two-second lease can inject five seconds, observe heartbeat
    /// renewal during the pause, and then verify that the same fencing token
    /// publishes safely.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Requiring `&mut self` gives the caller exclusive access for the update.
    /// The compiler rejects concurrent use through ordinary references without
    /// an explicit synchronization wrapper.
    #[doc(hidden)]
    pub fn set_test_pre_cas_delay(&mut self, delay: Duration) {
        self.test_pre_cas_delay = Some(delay);
    }

    /// Borrows the trigger and retention configuration used by this compactor.
    ///
    /// # Returns
    ///
    /// A read-only reference tied to the lifetime of `self`; no configuration
    /// is cloned or reloaded from object storage.
    ///
    /// # Examples
    ///
    /// The background loop reads `interval_secs` through this accessor to
    /// schedule its next namespace scan.
    pub fn config(&self) -> &CompactionConfig {
        &self.config
    }

    /// Returns the GC-owned maximum duration of an unpublished artifact upload.
    ///
    /// # Returns
    ///
    /// A copied [`Duration`] used by the final publication guard.
    ///
    /// # Examples
    ///
    /// If GC supplies 42 seconds, a build whose upload phase exceeds 42 seconds
    /// aborts before CAS instead of publishing after its protection window.
    #[must_use]
    pub fn compaction_upload_window(&self) -> Duration {
        self.upload_window
    }

    /// Borrows the object-store client used by compaction.
    ///
    /// # Returns
    ///
    /// A shared reference to [`ZeppelinStore`], primarily for orchestration and
    /// tests. The caller does not receive a manifest snapshot or cache.
    ///
    /// # Examples
    ///
    /// A test can inspect immutable artifacts through this store after a
    /// successful compaction without transferring ownership out of the
    /// compactor.
    pub fn store(&self) -> &ZeppelinStore {
        &self.store
    }

    /// Borrows the wall clock shared with compaction and GC paths.
    #[must_use]
    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    /// Resolves process defaults with the namespace's current indexing overlay.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace whose metadata object should be read.
    ///
    /// # Returns
    ///
    /// An owned, validated [`IndexingConfig`]. If metadata has no index overlay,
    /// the process defaults are cloned.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::NamespaceNotFound`] when the authoritative
    /// metadata object is absent and [`ZeppelinError::NamespaceDeleting`] when
    /// metadata marks the namespace as deleting. Storage, decoding, and
    /// per-dimension validation failures also propagate; no malformed overlay
    /// is silently ignored.
    ///
    /// # Consistency
    ///
    /// Namespace metadata controls the desired layout, while the manifest
    /// controls which already-built layout is visible. This read occurs near the
    /// beginning of each run so a config transition can request a rewrite.
    ///
    /// # Performance
    ///
    /// Performs one complete metadata-object GET and clones a small config.
    ///
    /// # Examples
    ///
    /// If process defaults request unquantized flat IVF but namespace metadata
    /// enables scalar quantization, the returned config requests SQ and
    /// `should_compact` can trigger a segment rewrite even without new WAL.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Exhaustive `match` converts a missing authoritative metadata object into
    /// the namespace-domain error while preserving every other failure. No
    /// process default can authorize a build after `meta.json` disappears.
    async fn effective_indexing_config(&self, namespace: &str) -> Result<IndexingConfig> {
        let key = NamespaceMetadata::s3_key(namespace);
        match self.store.get(&key).await {
            Ok(data) => {
                let meta = NamespaceMetadata::from_bytes(&data)?;
                resolve_indexing_config(namespace, &meta, &self.indexing_config)
            }
            Err(ZeppelinError::NotFound { .. }) => Err(ZeppelinError::NamespaceNotFound {
                namespace: namespace.to_string(),
            }),
            Err(e) => Err(e),
        }
    }

    /// Determines whether a namespace currently meets any compaction trigger.
    ///
    /// Four independent triggers:
    /// - **count**: uncompacted fragments >= `max_wal_fragments_before_compact`
    /// - **age**: oldest uncompacted fragment (from its ULID timestamp) is
    ///   >= `max_wal_age_before_compact_secs` old — guarantees any namespace
    ///   with pending WAL data converges within a bounded window
    /// - **bytes**: total uncompacted WAL bytes (recorded at write time in
    ///   `FragmentRef.size_bytes`) >= `max_wal_bytes_before_compact`
    /// - **index config**: the active segment's manifest-visible layout no
    ///   longer matches the namespace's desired index config.
    ///
    /// A namespace with zero uncompacted fragments only triggers when an
    /// active-segment rewrite is needed for a staged index config change.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace label used for diagnostics and metrics.
    /// - `manifest`: Caller-supplied, strongly revalidated visibility snapshot.
    /// - `metadata`: Caller-supplied discovery snapshot used only by the
    ///   advisory idle-layout trigger.
    ///
    /// # Returns
    ///
    /// `true` when any threshold is met or the active layout must be rewritten;
    /// otherwise `false`. The method does not start compaction.
    ///
    /// # Errors
    ///
    /// Propagates metadata/config and pre-Unix-epoch clock errors. Fetching and
    /// decoding the authoritative snapshots is the caller's responsibility.
    ///
    /// # Consistency
    ///
    /// The decision is advisory. The background caller strongly revalidates the
    /// manifest and may use a bounded-staleness discovery metadata snapshot; a
    /// later lease-protected compaction reads both objects afresh and remains
    /// correct if either changes between trigger and execution.
    ///
    /// # Performance
    ///
    /// Performs no object-store I/O. Threshold checks scan the in-memory
    /// fragment descriptors in linear time without reading WAL payloads.
    ///
    /// # Examples
    ///
    /// Three small fresh fragments trigger when the count threshold is three.
    /// One hour-old fragment can trigger by age even below count and byte
    /// limits. An empty namespace remains idle unless its active segment no
    /// longer matches the effective quantization or hierarchy configuration.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `manifest.uncompacted_fragments()` returns a borrowed slice; the method
    /// can aggregate its descriptors without copying them. Iterator chains are
    /// statically specialized like hand-written C loops, rather than allocating
    /// Java stream objects for each element.
    #[instrument(skip(self, manifest, metadata), fields(namespace = namespace))]
    pub fn should_compact(
        &self,
        namespace: &str,
        manifest: &Manifest,
        metadata: &NamespaceMetadata,
    ) -> Result<bool> {
        let fragments = manifest.uncompacted_fragments();

        // Idle namespace: nothing to compact, never trigger (no busy work,
        // no S3 churn on quiet namespaces).
        if fragments.is_empty() {
            let indexing_config =
                resolve_indexing_config(namespace, metadata, &self.indexing_config)?;
            if manifest_needs_index_rewrite(manifest, &indexing_config) {
                info!("compaction triggered by index config layout change");
                return Ok(true);
            }
            debug!("no uncompacted fragments, compaction not needed");
            return Ok(false);
        }

        let count = fragments.len();
        let total_bytes: u64 = fragments.iter().map(|f| f.size_bytes).sum();
        let now = self.clock.now();
        let now_ms = u64::try_from(now.timestamp_millis()).map_err(|_| {
            ZeppelinError::Index(format!("compactor clock before Unix epoch: {now}"))
        })?;
        let oldest_age_secs = fragments
            .iter()
            .map(|f| fragment_age_secs(&f.id, now_ms))
            .max()
            .unwrap_or(0);

        let count_exceeded = count >= self.config.max_wal_fragments_before_compact;
        let age_exceeded = oldest_age_secs >= self.config.max_wal_age_before_compact_secs;
        let bytes_exceeded = total_bytes >= self.config.max_wal_bytes_before_compact;

        if count_exceeded || age_exceeded || bytes_exceeded {
            info!(
                fragment_count = count,
                total_wal_bytes = total_bytes,
                oldest_fragment_age_secs = oldest_age_secs,
                count_trigger = count_exceeded,
                age_trigger = age_exceeded,
                bytes_trigger = bytes_exceeded,
                count_threshold = self.config.max_wal_fragments_before_compact,
                age_threshold_secs = self.config.max_wal_age_before_compact_secs,
                bytes_threshold = self.config.max_wal_bytes_before_compact,
                "compaction triggered"
            );
            return Ok(true);
        }

        debug!(
            fragment_count = count,
            total_wal_bytes = total_bytes,
            oldest_fragment_age_secs = oldest_age_secs,
            "compaction not needed"
        );
        Ok(false)
    }

    /// Compacts all currently visible WAL fragments without lease fencing.
    ///
    /// Uses CAS (compare-and-swap) for manifest updates to prevent concurrent
    /// overwrites. Fragment deletion is deferred: keys are added to
    /// `pending_deletes` in the manifest and reclaimed by storage GC after the
    /// configured horizon. Production background work normally uses the leased
    /// entry point instead.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace whose manifest-visible WAL should be compacted.
    ///
    /// # Returns
    ///
    /// A [`CompactionResult`] describing the committed change, including a
    /// no-op result when no fragments or layout rewrite are pending.
    ///
    /// # Errors
    ///
    /// Propagates every read, decode, build, upload, and publication failure.
    /// Uploaded artifacts can remain invisible if a later step fails.
    ///
    /// # Side Effects
    ///
    /// May read WAL and segment objects, upload new immutable artifacts, and
    /// conditionally replace the live manifest. It never deletes retired
    /// artifacts synchronously.
    ///
    /// # Consistency
    ///
    /// ETag CAS preserves concurrent manifest changes, but this wrapper supplies
    /// no lease fencing token. Use
    /// [`background::compact_namespace_under_lease_with_lifecycle`] for production multi-node
    /// coordination.
    ///
    /// # Examples
    ///
    /// A deterministic test may append two fragments, call `compact`, and find
    /// one active segment plus both fragment keys in `pending_deletes`. Query
    /// readers discover the segment only after the conditional manifest PUT.
    #[instrument(skip(self), fields(namespace = namespace))]
    pub async fn compact(&self, namespace: &str) -> Result<CompactionResult> {
        self.compact_with_lease(namespace, None).await
    }

    /// Compacts with an optional lease fencing token and no FTS fields.
    ///
    /// When `fencing_token` is `Some(token)`:
    /// - **Layer 1 (CheckFencing)**: Before each CAS write, checks
    ///   `manifest.fencing_token <= token`. If false → `FencingTokenStale`.
    /// - **Layer 2 (CAS)**: If the ETag changed, retries with re-check.
    ///
    /// When `fencing_token` is `None`: behaves identically to [`Self::compact`].
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace to compact.
    /// - `fencing_token`: Token from the currently acquired namespace lease, or
    ///   `None` only for an explicitly unfenced caller.
    ///
    /// # Returns
    ///
    /// The committed [`CompactionResult`].
    ///
    /// # Errors
    ///
    /// In addition to normal compaction failures, returns
    /// [`ZeppelinError::FencingTokenStale`] if a fresh manifest contains a newer
    /// token. Because this wrapper has no heartbeat signal, it cannot detect a
    /// lost lease until another writer publishes the newer token.
    ///
    /// # Consistency
    ///
    /// Fencing rejects a known-old lease holder; CAS rejects an old manifest
    /// base. Both checks run on every publication retry.
    ///
    /// # Examples
    ///
    /// A holder with token 8 may publish over a manifest token 8. If a takeover
    /// already published token 9, token 8 fails rather than making its segment
    /// visible.
    #[instrument(skip(self), fields(namespace = namespace))]
    pub async fn compact_with_lease(
        &self,
        namespace: &str,
        fencing_token: Option<u64>,
    ) -> Result<CompactionResult> {
        self.compact_with_fts(namespace, fencing_token, &HashMap::new())
            .await
    }

    /// Compacts with optional fencing and caller-supplied FTS field definitions.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace to compact.
    /// - `fencing_token`: Current lease token, or `None` for unfenced use.
    /// - `fts_configs`: Fields and tokenization settings for new inverted
    ///   indexes. An empty map omits FTS construction.
    ///
    /// # Returns
    ///
    /// The committed [`CompactionResult`].
    ///
    /// # Errors
    ///
    /// Propagates WAL, index, FTS, storage, fencing, and CAS errors. A failure
    /// after some PUTs can leave invisible immutable objects for GC.
    ///
    /// # Side Effects
    ///
    /// May upload per-cluster and global full-text indexes in addition to the
    /// vector segment.
    ///
    /// # Performance
    ///
    /// Non-empty FTS configuration requires every cluster's attributes, so the
    /// membership-bounded read path is disabled. Centroids may still be reused,
    /// but all clusters are rewritten before FTS indexing.
    ///
    /// # Examples
    ///
    /// Configuring a `title` text field creates per-cluster inverted indexes and
    /// one global index, then records `title` and `has_global_fts` in the new
    /// segment descriptor.
    #[instrument(skip(self, fts_configs), fields(namespace = namespace))]
    pub async fn compact_with_fts(
        &self,
        namespace: &str,
        fencing_token: Option<u64>,
        fts_configs: &HashMap<String, FtsFieldConfig>,
    ) -> Result<CompactionResult> {
        self.compact_with_fts_signaled(
            namespace,
            fencing_token,
            fts_configs,
            None,
            FragmentCachePolicy::Bypass,
        )
        .await
    }

    /// Executes the complete compaction transaction with an optional abort signal.
    ///
    /// `lease_lost` is set by the lease-renewal heartbeat
    /// (`background::LeaseHeartbeat`) when a mid-compaction renewal fails —
    /// i.e. the lease was stolen or expired-and-taken. The flag is checked
    /// before EVERY manifest CAS attempt: a compaction whose lease is gone
    /// aborts with `LeaseExpired` instead of committing (invariant A2 —
    /// this closes the TOCTOU window between the fencing check and the CAS
    /// down to one heartbeat interval; the fencing+CAS layers remain the
    /// backstop for the residual race).
    ///
    /// ```text
    /// initial Manifest M7
    ///       |
    ///       | snapshot fragment IDs {A, C}
    ///       v
    /// build/upload candidate segment S8 once
    ///       |
    ///       v
    /// CAS attempt: read M8 + ETag E8
    ///       |
    ///       +-- lease_lost=true --------------------> LeaseExpired
    ///       +-- manifest token > our token ---------> FencingTokenStale
    ///       +-- conditional PUT loses --------------> backoff; read again
    ///       `-- succeeds ---------------------------> S8 visible;
    ///                                                  remove only A and C
    /// ```
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace whose live WAL snapshot should become a segment.
    /// - `fencing_token`: Token obtained with the compaction lease, or `None`
    ///   for an explicitly unfenced direct caller.
    /// - `fts_configs`: Full-text fields to materialize when FTS indexing is
    ///   enabled in the effective index config.
    /// - `lease_lost`: Optional atomically shared heartbeat signal. `true`
    ///   forbids every subsequent manifest commit attempt.
    /// - `fragment_cache`: Immutable WAL-byte cache behavior. Production
    ///   background compaction uses read-only hits; direct callers bypass it.
    ///
    /// # Returns
    ///
    /// The result of the manifest change that became authoritative. `segment_id`
    /// is `None` for a no-op or when all surviving vectors were deleted.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::ManifestNotFound`] when the namespace's
    /// authoritative manifest is missing. Also fails on namespace metadata or
    /// manifest decoding errors, WAL corruption, missing required segment
    /// artifacts, dimension mismatch, index/FTS serialization, any required
    /// object-store operation, lease loss, stale fencing, upload window expiry,
    /// or exhausted CAS retries. The function intentionally does not roll back
    /// immutable PUTs; failure after upload can leave orphaned objects, and
    /// fenced staging may remain for GC to expire.
    ///
    /// Incremental build errors are logged and metered before a full rebuild is
    /// attempted. That is a correctness-preserving cost fallback, not silent
    /// data loss. An error from the full rebuild still reaches the caller.
    ///
    /// # Side Effects
    ///
    /// Reads authoritative metadata, the manifest, WAL objects, and some or all
    /// old segment artifacts. It uploads immutable candidate artifacts, updates
    /// metrics, may write/clear a compaction staging root, sleeps during CAS
    /// backoff, and conditionally publishes a new live manifest whose
    /// `pending_deletes` records retired keys.
    ///
    /// # Consistency
    ///
    /// The first manifest is only the build snapshot. Every CAS attempt reloads
    /// the authoritative manifest and ETag, checks lease state and fencing, adds
    /// the candidate segment, removes the exact snapshot fragment IDs, merges
    /// concurrent pending-deletion entries, and writes conditionally. Fragments
    /// appended after the snapshot remain visible. A missing manifest at commit
    /// time fails with `ManifestNotFound` so namespace deletion cannot be
    /// reversed by compaction.
    ///
    /// The production lease must serialize segment builds for a namespace. CAS
    /// retries merge concurrent WAL-manifest updates but do not re-run the
    /// expensive index build against a different concurrently published active
    /// segment.
    ///
    /// # Performance
    ///
    /// The full path reads every surviving old vector and trains an index. WAL
    /// fragment cache hits avoid object-store GETs; read-only misses fetch once
    /// without filling soon-dead data into the cache. The centroid-reuse path
    /// avoids k-means but can still read all clusters. The bounded path reads
    /// membership, the resident sketch, and touched clusters only, then writes
    /// touched clusters plus segment-global sidecars. Full-text indexing reads
    /// all attribute blobs, builds clusters on blocking workers, and uploads
    /// per-cluster plus global artifacts. CAS retries do not repeat the
    /// expensive build.
    ///
    /// # Examples
    ///
    /// With 100,000 vectors in 100 clusters and a WAL update touching clusters
    /// 4 and 71, the bounded path reuses centroids and 98 physical clusters,
    /// writes new objects for two clusters, publishes a new sketch/membership
    /// closure, and keeps the carried old keys out of deferred deletion.
    ///
    /// If another writer appends fragment D during the build, the final CAS
    /// removes only the snapshotted fragments and leaves D in the fresh
    /// manifest. If the heartbeat reports takeover first, the candidate segment
    /// remains invisible and the function returns `LeaseExpired`.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The optional signal is an owned [`std::sync::Arc`] shared with the
    /// heartbeat, then temporarily borrowed as `Option<&AtomicBool>` for each
    /// check. Cloning `Arc` increments a reference count; it does not clone the
    /// boolean. `Result` plus `?` makes early exit explicit, but it is not a
    /// database rollback mechanism: remote PUTs completed before an error remain.
    ///
    /// Local maps own IDs and vectors so they survive across `.await` points.
    /// Cloning a [`VectorEntry`] deep-clones its strings, vector values, and
    /// attributes; cloning [`bytes::Bytes`] for concurrent PUTs shares its
    /// immutable allocation instead.
    #[instrument(skip(self, fts_configs, lease_lost, fragment_cache), fields(namespace = namespace))]
    pub async fn compact_with_fts_signaled(
        &self,
        namespace: &str,
        fencing_token: Option<u64>,
        fts_configs: &HashMap<String, FtsFieldConfig>,
        lease_lost: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
        fragment_cache: FragmentCachePolicy<'_>,
    ) -> Result<CompactionResult> {
        let start = std::time::Instant::now();

        if let Some(preservation) = &self.preservation {
            let namespace_id = NamespaceId::new(namespace.to_string())?;
            let guard = preservation.guard_namespace(&namespace_id)?;
            if guard.is_locked() {
                preservation
                    .record_maintenance_deferral(true, &namespace_id, &guard)
                    .await?;
                info!(
                    lock_count = guard.lock_ids().len(),
                    "compaction_deferred_preservation"
                );
                return Ok(CompactionResult {
                    segment_id: None,
                    vectors_compacted: 0,
                    fragments_removed: 0,
                    old_segment_removed: None,
                });
            }
        }

        let processed_deletes: HashSet<String> = HashSet::new();

        // 1. Read manifest to get fragment list (snapshot for segment building)
        let (mut manifest, manifest_version) =
            Manifest::read_versioned_required(&self.store, namespace)
                .await
                .map_err(|error| match error {
                    ZeppelinError::NotFound { .. } => ZeppelinError::ManifestNotFound {
                        namespace: namespace.to_string(),
                    },
                    error => error,
                })?;
        let indexing_config = self.effective_indexing_config(namespace).await?;
        let rewrite_for_index_config = manifest_needs_index_rewrite(&manifest, &indexing_config);
        let authoritative_origin = manifest.local_origin()?;

        // 2. If no uncompacted fragments → no-op
        if manifest.uncompacted_fragments().is_empty() && !rewrite_for_index_config {
            if self.store.object_signer_node()?.is_some()
                && manifest.receipt_upgrade_needed(namespace)
            {
                check_lease_lost(namespace, lease_lost.as_deref())?;
                if let Some(token) = fencing_token {
                    if manifest.fencing_token > token {
                        return Err(ZeppelinError::FencingTokenStale {
                            namespace: namespace.to_string(),
                            our_token: token,
                            manifest_token: manifest.fencing_token,
                        });
                    }
                    manifest.fencing_token = token;
                }
                manifest
                    .hydrate_receipt_artifacts(&self.store, namespace)
                    .await?;
                // The legacy inventory may require arbitrarily many S3 GETs.
                // Reuse the publication-delay hook here so takeover tests can
                // hold this exact hydration-to-CAS window open.
                if let Some(delay) = self.test_pre_cas_delay {
                    warn!(
                        delay_ms = delay.as_millis() as u64,
                        "test hook: delaying legacy receipt upgrade before final CAS"
                    );
                    tokio::time::sleep(delay).await;
                }
                // Hydration is not lease-protected work by itself. A heartbeat
                // can observe takeover while those reads are in flight, so the
                // stale writer must recheck immediately before its CAS.
                check_lease_lost(namespace, lease_lost.as_deref())?;
                manifest
                    .write_conditional(&self.store, namespace, &manifest_version)
                    .await?;
                info!("upgraded legacy manifest receipt inventory");
            }
            debug!("no uncompacted fragments, skipping");
            return Ok(CompactionResult {
                segment_id: None,
                vectors_compacted: 0,
                fragments_removed: 0,
                old_segment_removed: None,
            });
        }

        let artifact_origins = manifest.artifact_origin_resolver(&authoritative_origin)?;
        let located_fragment_refs = artifact_origins.uncompacted_located_fragments()?;
        let old_segment = artifact_origins.active_located_segment()?;
        let fragments_removed = located_fragment_refs.len();
        // Exact set of fragment IDs in this compaction's snapshot. Manifest
        // removal must use this set, not a max-ULID watermark: a fragment
        // appended concurrently can sort <= the snapshot max (same-ms ULIDs
        // or clock skew) and a watermark comparison would silently drop it.
        let compacted_fragments: HashSet<LocatedFragmentIdentity> = located_fragment_refs
            .iter()
            .copied()
            .map(|located| located.identity())
            .collect();
        if compacted_fragments.is_empty() && !rewrite_for_index_config {
            return Err(ZeppelinError::Index("no fragments to compact".into()));
        }

        info!(
            fragment_count = fragments_removed,
            rewrite_for_index_config, "starting compaction"
        );

        // 3. Read fragments using snapshot refs (not re-reading manifest).
        // Uses unchecked read — fragments were validated on write.
        let fragments = self
            .wal_reader
            .read_located_fragments_unchecked(&located_fragment_refs, fragment_cache)
            .await?;

        // 4. Merge vectors: process in manifest order (sequence number), latest wins.
        //
        // `wal_touched_ids` is every vector ID that appeared in the WAL this
        // cycle — as an add/update (`fragment.vectors`) OR a delete
        // (`fragment.deletes`). An existing cluster is UNCHANGED iff none of
        // its members (old or newly-assigned) is in this set, which is what
        // the incremental fast path uses to decide carry-over vs rewrite.
        let mut latest_vectors: HashMap<String, VectorEntry> = HashMap::new();
        let mut deleted_ids: HashSet<String> = HashSet::new();
        let mut wal_touched_ids: HashSet<String> = HashSet::new();

        for fragment in &fragments {
            for del_id in &fragment.deletes {
                deleted_ids.insert(del_id.clone());
                wal_touched_ids.insert(del_id.clone());
                latest_vectors.remove(del_id);
            }
            for vec in &fragment.vectors {
                deleted_ids.remove(&vec.id);
                wal_touched_ids.insert(vec.id.clone());
                latest_vectors.insert(vec.id.clone(), vec.clone());
            }
        }

        // 5. Snapshot the old active segment, if any. S3 manifest state is the
        // source of truth for retrain decisions; do not load old vectors just to
        // count them.
        let old_segment_id = old_segment.map(|located| located.segment.id.clone());
        // Snapshot of the old active segment's full SegmentRef — needed both
        // to resolve its per-cluster owners (carried-over clusters live under
        // an even-older segment's keys) and to enumerate the exact S3 objects
        // it referenced when computing what is safe to delete.
        let old_segment_ref = old_segment.map(|located| located.segment);
        let old_cluster_owners: Vec<String> = old_segment_ref
            .as_ref()
            .map(|s| s.cluster_owners.clone())
            .unwrap_or_default();

        // 5b. Incremental compaction decision: skip k-means retraining when
        // new vectors are a small fraction of the manifest-carried old count.
        // This must happen before any old cluster data is read so the bounded
        // incremental path stays O(WAL + touched clusters).
        let new_from_wal = fragments.iter().map(|f| f.vectors.len()).sum::<usize>();
        let existing_count = old_segment_ref
            .as_ref()
            .map(|segment| segment.vector_count)
            .unwrap_or(0);
        let retrain_ratio = if existing_count == 0 {
            f64::INFINITY
        } else {
            new_from_wal as f64 / existing_count as f64
        };
        let old_segment_is_target_owned = old_segment
            .is_none_or(|located| located.physical_origin.as_origin() == &authoritative_origin);
        let should_retrain = rewrite_for_index_config
            || existing_count == 0
            || retrain_ratio > self.config.retrain_imbalance_threshold
            // A target-local candidate cannot encode a foreign namespace in
            // legacy `cluster_owners`. Fully materialize the foreign view under
            // the target instead of silently turning source owner IDs into
            // target keys during incremental carry-over.
            || !old_segment_is_target_owned;
        if should_retrain {
            crate::metrics::COMPACTION_FULL_RETRAIN_TOTAL
                .with_label_values(&[namespace])
                .inc();
            info!(
                new_from_wal,
                existing_count,
                retrain_ratio,
                old_segment_is_target_owned,
                retrain_imbalance_threshold = self.config.retrain_imbalance_threshold,
                "compaction full retrain selected"
            );
        }

        let incremental_candidate = !should_retrain
            && old_segment_ref
                .as_ref()
                .is_some_and(|segment| !segment.hierarchical)
            && !indexing_config.hierarchical;
        let bounded_incremental = if incremental_candidate {
            if !fts_configs.is_empty() {
                crate::metrics::COMPACTION_INCREMENTAL_FALLBACK_TOTAL
                    .with_label_values(&[namespace, "fts_configured"])
                    .inc();
                warn!(
                    "bounded incremental compaction disabled because FTS rebuild \
                     requires all vectors"
                );
                None
            } else if let Some(membership_ref) = old_segment_ref
                .as_ref()
                .and_then(|segment| segment.membership.as_ref())
            {
                let membership_bytes = get_compaction_read(
                    &self.store,
                    namespace,
                    &membership_ref.key,
                    COMPACTION_READ_CLASS_MEMBERSHIP,
                )
                .await?;
                let membership = deserialize_membership(&membership_bytes)?;
                Some(membership)
            } else {
                crate::metrics::COMPACTION_INCREMENTAL_FALLBACK_TOTAL
                    .with_label_values(&[namespace, "membership_absent"])
                    .inc();
                warn!(
                    "bounded incremental compaction disabled because old segment \
                     has no membership artifact; falling back to full old-segment read"
                );
                None
            }
        } else {
            None
        };

        let mut old_id_to_cluster: HashMap<String, usize> = HashMap::new();
        let mut vectors: Vec<VectorEntry> = Vec::new();
        let mut vectors_compacted = if let Some(membership) = bounded_incremental.as_ref() {
            bounded_survivor_count(membership, &latest_vectors, &deleted_ids)
        } else {
            // Full-read path: materialize old vectors, merge WAL overrides, and
            // skip non-finite historical data loudly. On the bounded path,
            // untouched clusters are no longer scanned; any pre-Task-10 poison
            // in an untouched carried cluster remains exactly as it was served
            // before until that cluster is rewritten or a retrain fires.
            if let Some(located) = old_segment {
                let (existing_vecs, id_to_cluster) =
                    load_segment_vectors(&self.store, located).await?;
                old_id_to_cluster = id_to_cluster;
                for vec in existing_vecs {
                    // WAL overrides: only insert if not already in latest_vectors and not deleted
                    if !latest_vectors.contains_key(&vec.id) && !deleted_ids.contains(&vec.id) {
                        latest_vectors.insert(vec.id.clone(), vec);
                    }
                }
            }

            vectors = latest_vectors
                .values()
                .filter(|v| keep_finite_compaction_vector(namespace, v))
                .cloned()
                .collect();
            vectors.sort_by(|a, b| a.id.cmp(&b.id));
            vectors.len()
        };

        // Collect keys for deferred deletion. The old segment's own S3
        // objects are added PER BRANCH below: the incremental fast path must
        // EXCLUDE carried-over cluster objects (still referenced by the new
        // segment) from deletion, whereas the all-deleted and full-rebuild
        // branches delete every old object.
        let mut deferred_deletes: Vec<String> = Vec::new();
        for located in &located_fragment_refs {
            if let Some(key) = target_owned_fragment_deletion_key(*located) {
                deferred_deletes.push(key);
            }
        }

        if vectors_compacted == 0 {
            // No new segment is produced, so nothing is carried over — every
            // old-segment object is safe to delete.
            if let Some(prefix) = old_segment.and_then(target_owned_old_segment_prefix) {
                deferred_deletes.extend(self.store.list_prefix(&prefix).await?);
            }
            // Edge case: all vectors were deleted
            // CAS loop to update manifest
            for attempt in 0..MAX_CAS_RETRIES {
                // Invariant A2: never commit after the heartbeat reported
                // the lease lost.
                check_lease_lost(namespace, lease_lost.as_deref())?;
                // A missing manifest means the namespace was deleted mid-
                // compaction. Recreating it from Manifest::default() would
                // resurrect the namespace (unconditional PUT) — abort instead
                // (NoZombieNamespace, same rule as WalWriter).
                let (mut fresh_manifest, version) =
                    match Manifest::read_versioned(&self.store, namespace).await? {
                        Some(pair) => pair,
                        None => {
                            return Err(ZeppelinError::ManifestNotFound {
                                namespace: namespace.to_string(),
                            });
                        }
                    };

                // Layer 1: Fencing check.
                if let Some(token) = fencing_token {
                    if fresh_manifest.fencing_token > token {
                        return Err(ZeppelinError::FencingTokenStale {
                            namespace: namespace.to_string(),
                            our_token: token,
                            manifest_token: fresh_manifest.fencing_token,
                        });
                    }
                    fresh_manifest.fencing_token = token;
                }

                let manifest_stamp = self.clock.now();
                if let Some(seg_id) = old_segment_id.as_deref() {
                    fresh_manifest.remove_segment_at(seg_id, manifest_stamp);
                }
                fresh_manifest.remove_compacted_located_fragments_at(
                    &authoritative_origin,
                    &compacted_fragments,
                    manifest_stamp,
                )?;
                merge_pending_deletes(&mut fresh_manifest, &deferred_deletes, &processed_deletes);

                // Layer 2: CAS.
                match fresh_manifest
                    .write_conditional(&self.store, namespace, &version)
                    .await
                {
                    Ok(_) => {
                        let elapsed = start.elapsed();
                        crate::metrics::COMPACTION_DURATION
                            .with_label_values(&[namespace])
                            .observe(elapsed.as_secs_f64());

                        info!(
                            elapsed_ms = elapsed.as_millis(),
                            attempt, "compaction complete (all vectors deleted)"
                        );
                        return Ok(CompactionResult {
                            segment_id: None,
                            vectors_compacted: 0,
                            fragments_removed,
                            old_segment_removed: old_segment_id,
                        });
                    }
                    Err(ZeppelinError::ManifestConflict { .. }) => {
                        warn!(
                            attempt,
                            "manifest CAS conflict in compactor (empty), retrying with backoff"
                        );
                        let backoff_ms = (50u64 * (1 << attempt.min(5))).min(2000);
                        let jitter_ms = rand::thread_rng().gen_range(0..50);
                        tokio::time::sleep(Duration::from_millis(backoff_ms + jitter_ms)).await;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
            return Err(ZeppelinError::ManifestConflict {
                namespace: namespace.to_string(),
            });
        }

        // 7. Generate new segment ID
        let segment_id = format!("seg_{}", Ulid::new());
        let upload_phase_start = std::time::Instant::now();

        // 8. Build index (expensive, done once — NOT retried)
        // Choose hierarchical or flat based on config.
        //
        // `cluster_owners` maps each cluster index to the segment ID that owns
        // its S3 objects. It is EMPTY for every full-rebuild path (all clusters
        // owned by `segment_id`); only the incremental fast path populates it,
        // carrying untouched clusters forward under the old segment's keys.
        let build_start = std::time::Instant::now();
        let (
            cluster_count,
            is_hierarchical,
            bitmap_fields,
            cluster_owners,
            sketch_ref,
            bootstrap_ref,
            membership_ref,
            cluster_objects,
            routing_node_ids,
        ) = if let Some(old_membership) = bounded_incremental.as_ref() {
            match self
                .incremental_build_bounded(
                    namespace,
                    old_segment.ok_or_else(|| {
                        ZeppelinError::Index("no old segment for bounded incremental build".into())
                    })?,
                    &indexing_config,
                    &segment_id,
                    &latest_vectors,
                    &deleted_ids,
                    &wal_touched_ids,
                    old_membership,
                    &old_cluster_owners,
                    old_segment_ref
                        .as_ref()
                        .map(|s| s.cluster_objects.as_slice())
                        .unwrap_or(&[]),
                    old_segment_ref
                        .as_ref()
                        .map(|s| s.bitmap_fields.as_slice())
                        .unwrap_or(&[]),
                    old_segment_ref.as_ref().and_then(|s| s.sketch.as_ref()),
                )
                .await
            {
                Ok((
                    count,
                    bf,
                    owners,
                    sketch_ref,
                    bootstrap_ref,
                    membership_ref,
                    cluster_objects,
                )) => {
                    vectors_compacted =
                        usize::try_from(membership_ref.entry_count).map_err(|_| {
                            ZeppelinError::Index(format!(
                                "membership entry_count does not fit in usize: {}",
                                membership_ref.entry_count
                            ))
                        })?;
                    info!(
                        new_from_wal,
                        existing_count,
                        "bounded incremental compaction: reusing centroids and membership"
                    );
                    (
                        count,
                        false,
                        bf,
                        owners,
                        Some(sketch_ref),
                        Some(bootstrap_ref),
                        Some(membership_ref),
                        cluster_objects,
                        Vec::new(),
                    )
                }
                Err(error @ ZeppelinError::CoarseSketch(_)) => {
                    error!(error = %error, "bounded incremental rejected corrupt resident sketch");
                    return Err(error);
                }
                Err(e) => {
                    crate::metrics::COMPACTION_INCREMENTAL_FALLBACK_TOTAL
                        .with_label_values(&[namespace, "build_failed"])
                        .inc();
                    warn!(error = %e, "bounded incremental build failed, falling back to full retrain");
                    let full_vectors = load_full_surviving_vectors_for_fallback(
                        &self.store,
                        namespace,
                        old_segment,
                        latest_vectors.clone(),
                        &deleted_ids,
                    )
                    .await?;
                    vectors_compacted = full_vectors.len();
                    let index = build_ivf_flat(
                        &full_vectors,
                        &indexing_config,
                        &self.store,
                        namespace,
                        &segment_id,
                    )
                    .await?;
                    let bf = index.bitmap_fields.clone();
                    (
                        index.num_clusters(),
                        false,
                        bf,
                        Vec::new(),
                        index.sketch_ref.clone(),
                        index.bootstrap_ref.clone(),
                        index.membership_ref.clone(),
                        index.cluster_objects.clone(),
                        Vec::new(),
                    )
                }
            }
        } else if incremental_candidate {
            // Incremental path: reuse existing centroids, just reassign vectors.
            match self
                .incremental_build(
                    namespace,
                    old_segment.ok_or_else(|| {
                        ZeppelinError::Index("no old segment for incremental build".into())
                    })?,
                    &indexing_config,
                    &segment_id,
                    &vectors,
                    &wal_touched_ids,
                    &old_id_to_cluster,
                    &old_cluster_owners,
                    old_segment_ref
                        .as_ref()
                        .map(|s| s.cluster_objects.as_slice())
                        .unwrap_or(&[]),
                    old_segment_ref
                        .as_ref()
                        .map(|s| s.bitmap_fields.as_slice())
                        .unwrap_or(&[]),
                    old_segment_ref.as_ref().and_then(|s| s.sketch.as_ref()),
                    // Carry-over is unsafe when FTS is configured: the FTS
                    // pass below reads every cluster's attrs under the NEW
                    // segment ID and rebuilds a per-segment global index,
                    // which a carried cluster (attrs under an OLD ID) would
                    // break. Centroid reuse still applies — only the
                    // per-cluster carry-over is disabled. (Correctness over
                    // cleverness; revisit when FTS learns carry-over.)
                    fts_configs.is_empty(),
                )
                .await
            {
                Ok((
                    count,
                    bf,
                    owners,
                    sketch_ref,
                    bootstrap_ref,
                    membership_ref,
                    cluster_objects,
                )) => {
                    info!(
                        new_from_wal,
                        existing_count, "incremental compaction: reusing centroids"
                    );
                    (
                        count,
                        false,
                        bf,
                        owners,
                        Some(sketch_ref),
                        Some(bootstrap_ref),
                        Some(membership_ref),
                        cluster_objects,
                        Vec::new(),
                    )
                }
                Err(error @ ZeppelinError::CoarseSketch(_)) => {
                    error!(error = %error, "incremental build rejected corrupt resident sketch");
                    return Err(error);
                }
                Err(e) => {
                    crate::metrics::COMPACTION_INCREMENTAL_FALLBACK_TOTAL
                        .with_label_values(&[namespace, "build_failed"])
                        .inc();
                    warn!(error = %e, "incremental build failed, falling back to full retrain");
                    let index = build_ivf_flat(
                        &vectors,
                        &indexing_config,
                        &self.store,
                        namespace,
                        &segment_id,
                    )
                    .await?;
                    let bf = index.bitmap_fields.clone();
                    (
                        index.num_clusters(),
                        false,
                        bf,
                        Vec::new(),
                        index.sketch_ref.clone(),
                        index.bootstrap_ref.clone(),
                        index.membership_ref.clone(),
                        index.cluster_objects.clone(),
                        Vec::new(),
                    )
                }
            }
        } else if indexing_config.hierarchical {
            let h_index = build_hierarchical(
                &vectors,
                &indexing_config,
                &self.store,
                namespace,
                &segment_id,
            )
            .await?;
            let bf = h_index.bitmap_fields.clone();
            (
                h_index.num_leaf_clusters(),
                true,
                bf,
                Vec::new(),
                None,
                None,
                None,
                Vec::new(),
                h_index.routing_node_ids().to_vec(),
            )
        } else {
            let index = build_ivf_flat(
                &vectors,
                &indexing_config,
                &self.store,
                namespace,
                &segment_id,
            )
            .await?;
            let bf = index.bitmap_fields.clone();
            (
                index.num_clusters(),
                false,
                bf,
                Vec::new(),
                index.sketch_ref.clone(),
                index.bootstrap_ref.clone(),
                index.membership_ref.clone(),
                index.cluster_objects.clone(),
                Vec::new(),
            )
        };
        let build_elapsed = build_start.elapsed();
        let index_type_label = if is_hierarchical {
            "hierarchical"
        } else {
            "ivf_flat"
        };
        crate::metrics::INDEX_BUILD_DURATION
            .with_label_values(&[namespace, index_type_label])
            .observe(build_elapsed.as_secs_f64());
        debug!(
            index_type = index_type_label,
            build_duration_ms = build_elapsed.as_millis() as u64,
            "index build phase complete"
        );

        if let Some(token) = fencing_token {
            publish_compaction_staging(&self.store, namespace, &segment_id, token).await?;
        }

        // 8a. Enqueue the old segment's now-superseded S3 objects for deletion.
        //
        // DATA-LOSS CLIFF: with incremental carry-over, some of the old
        // segment's per-cluster objects are STILL REFERENCED by the new
        // segment (`cluster_owners[i] == old_segment_id`). Those must NOT be
        // deleted. We list everything under `{old_seg}/` and subtract the exact
        // per-cluster keys the new segment still points at.
        //
        // Safety direction: we protect EVERY possible per-cluster key shape for
        // each carried cluster owned directly by the old segment, whether or not
        // that object happens to exist. Protecting a non-existent key is a
        // harmless no-op (it won't be in the listing); failing to protect a
        // referenced key destroys live data. Carried clusters owned by an even-
        // older segment are not under `{old_seg}/` at all, so the listing never
        // surfaces them. Under-deletion leaks objects (Task 19 GC), which is the
        // safe failure mode.
        if let Some((located, prefix)) = old_segment.and_then(|located| {
            target_owned_old_segment_prefix(located).map(|prefix| (located, prefix))
        }) {
            use crate::index::quantization::pq::pq_cluster_key;
            use crate::index::quantization::sq::sq_cluster_key;

            let seg_id = &located.segment.id;
            let physical_namespace = located.physical_namespace();
            let mut referenced: HashSet<String> = HashSet::new();
            for (i, owner) in cluster_owners.iter().enumerate() {
                if owner == seg_id {
                    referenced.insert(cluster_key(physical_namespace, seg_id, i));
                    referenced.insert(attrs_key(physical_namespace, seg_id, i));
                    referenced.insert(sq_cluster_key(physical_namespace, seg_id, i));
                    referenced.insert(pq_cluster_key(physical_namespace, seg_id, i));
                    referenced.insert(crate::index::bitmap::bitmap_key(
                        physical_namespace,
                        seg_id,
                        i,
                    ));
                    referenced.insert(fts_index_key(physical_namespace, seg_id, i));
                }
            }
            for object_ref in &cluster_objects {
                if object_ref.key.starts_with(&prefix) {
                    referenced.insert(object_ref.key.clone());
                }
            }

            let old_keys = self.store.list_prefix(&prefix).await?;
            let carried = old_keys.iter().filter(|k| referenced.contains(*k)).count();
            deferred_deletes.extend(old_keys.into_iter().filter(|k| !referenced.contains(k)));
            debug!(
                old_segment = %seg_id,
                carried_objects = carried,
                "computed old-segment deletion set (carried objects retained)"
            );
        }

        // 8b. Build FTS inverted indexes (if FTS fields configured)
        let mut has_global_fts = false;
        let fts_fields: Vec<String> = if !fts_configs.is_empty() && indexing_config.fts_index {
            let fts_start = std::time::Instant::now();
            let mut fts_field_names = Vec::new();

            // Phase 1: Parallel reads of cluster attributes.
            let attr_keys: Vec<String> = (0..cluster_count)
                .map(|i| attrs_key(namespace, &segment_id, i))
                .collect();
            let read_futs: Vec<_> = attr_keys
                .iter()
                .map(|k| {
                    get_compaction_read(&self.store, namespace, k, COMPACTION_READ_CLASS_ATTRS)
                })
                .collect();
            let read_results = futures::future::join_all(read_futs).await;

            // Phase 2: CPU — build inverted indexes (parallelized via spawn_blocking).
            let fts_configs_clone = fts_configs.clone();
            let segment_id_clone = segment_id.clone();
            let namespace_clone = namespace.to_string();

            // These attrs blobs were written by this compaction moments ago —
            // a read failure is transient storage trouble. Skipping a cluster
            // would permanently drop its documents from the FTS index, so
            // fail the cycle and let the next one rebuild.
            let mut cluster_data: Vec<(usize, bytes::Bytes)> = Vec::new();
            for (cluster_idx, result) in read_results.into_iter().enumerate() {
                cluster_data.push((cluster_idx, result?));
            }

            // Build inverted indexes in parallel using spawn_blocking.
            // Also collect InvertedIndex objects for global index construction.
            let build_futs: Vec<_> = cluster_data
                .into_iter()
                .map(|(cluster_idx, data)| {
                    let configs = fts_configs_clone.clone();
                    let ns = namespace_clone.clone();
                    let seg = segment_id_clone.clone();
                    tokio::task::spawn_blocking(move || {
                        let cluster_attrs = deserialize_attrs(&data)?;
                        let attr_refs: Vec<Option<&HashMap<String, crate::types::AttributeValue>>> =
                            cluster_attrs.iter().map(|a| a.as_ref()).collect();
                        let inv_index = InvertedIndex::build(&attr_refs, &configs);
                        let field_names: Vec<String> = inv_index.fields.keys().cloned().collect();
                        let fts_data = inv_index.to_bytes()?;
                        let fts_key = fts_index_key(&ns, &seg, cluster_idx);
                        Ok::<_, ZeppelinError>((
                            cluster_idx,
                            fts_key,
                            fts_data,
                            field_names,
                            inv_index,
                        ))
                    })
                })
                .collect();

            let build_results = futures::future::join_all(build_futs).await;
            let mut write_payloads = Vec::new();
            let mut cluster_inv_indexes: Vec<(usize, InvertedIndex)> = Vec::new();
            for result in build_results {
                let (cluster_idx, fts_key, fts_data, field_names, inv_index) = result
                    .map_err(|e| ZeppelinError::Index(format!("FTS build task failed: {e}")))??;
                for name in field_names {
                    if !fts_field_names.contains(&name) {
                        fts_field_names.push(name);
                    }
                }
                write_payloads.push((fts_key, fts_data));
                cluster_inv_indexes.push((cluster_idx, inv_index));
            }

            // Phase 2b: Build global FTS index from per-cluster indexes.
            has_global_fts = if !cluster_inv_indexes.is_empty() {
                use crate::fts::global_index::{global_fts_key, GlobalInvertedIndex};
                let refs: Vec<(usize, &InvertedIndex)> = cluster_inv_indexes
                    .iter()
                    .map(|(idx, inv)| (*idx, inv))
                    .collect();
                let global_index = GlobalInvertedIndex::build(&refs);
                let global_data = global_index.to_bytes()?;
                let gkey = global_fts_key(namespace, &segment_id);
                write_payloads.push((gkey, global_data));
                true
            } else {
                false
            };

            // Phase 3: Parallel writes of FTS indexes (per-cluster + global).
            let write_futs: Vec<_> = write_payloads
                .iter()
                .map(|(key, data)| self.store.put(key, data.clone()))
                .collect();
            let write_results = futures::future::join_all(write_futs).await;
            for result in write_results {
                result?;
            }

            let fts_elapsed = fts_start.elapsed();
            crate::metrics::FTS_INDEX_BUILD_DURATION
                .with_label_values(&[namespace])
                .observe(fts_elapsed.as_secs_f64());
            debug!(
                fts_fields = ?fts_field_names,
                fts_build_duration_ms = fts_elapsed.as_millis() as u64,
                clusters = cluster_count,
                "FTS inverted index build complete"
            );

            fts_field_names
        } else {
            Vec::new()
        };

        if let Some(token) = fencing_token {
            publish_compaction_staging(&self.store, namespace, &segment_id, token).await?;
        }

        // Test-only slow-step injection (see `set_test_pre_cas_delay`):
        // simulates an index-build phase that outlasts the lease duration.
        if let Some(delay) = self.test_pre_cas_delay {
            warn!(
                delay_ms = delay.as_millis() as u64,
                "test hook: delaying compaction before final CAS"
            );
            tokio::time::sleep(delay).await;
        }

        // 9. CAS loop: re-read manifest, apply changes, write conditionally
        for attempt in 0..MAX_CAS_RETRIES {
            if let Err(e) = check_upload_window(
                namespace,
                upload_phase_start,
                self.compaction_upload_window(),
            ) {
                if let Some(token) = fencing_token {
                    drop_compaction_staging(&self.store, namespace, token).await;
                }
                return Err(e);
            }
            // Invariant A2: a compaction whose lease-renewal heartbeat
            // failed must abort BEFORE committing its manifest — the
            // in-flight segment becomes an orphan (GC's problem), never
            // a zombie commit.
            check_lease_lost(namespace, lease_lost.as_deref())?;
            // A missing manifest means the namespace was deleted mid-
            // compaction. Recreating it from Manifest::default() would
            // resurrect the namespace (unconditional PUT) — abort instead
            // (NoZombieNamespace, same rule as WalWriter). The freshly built
            // segment objects become orphans; namespace delete_prefix or a
            // future GC pass removes them.
            let (mut fresh_manifest, version) =
                match Manifest::read_versioned(&self.store, namespace).await? {
                    Some(pair) => pair,
                    None => {
                        return Err(ZeppelinError::ManifestNotFound {
                            namespace: namespace.to_string(),
                        });
                    }
                };

            // Layer 1: Fencing check.
            if let Some(token) = fencing_token {
                if fresh_manifest.fencing_token > token {
                    return Err(ZeppelinError::FencingTokenStale {
                        namespace: namespace.to_string(),
                        our_token: token,
                        manifest_token: fresh_manifest.fencing_token,
                    });
                }
                fresh_manifest.fencing_token = token;
            }

            let manifest_stamp = self.clock.now();
            fresh_manifest.add_segment_with_limits_at(
                SegmentRef {
                    id: segment_id.clone(),
                    vector_count: vectors_compacted,
                    cluster_count,
                    quantization: indexing_config.quantization,
                    hierarchical: is_hierarchical,
                    bitmap_fields: bitmap_fields.clone(),
                    fts_fields: fts_fields.clone(),
                    has_global_fts,
                    // Explicit per rule 15: a defaulted layout field has
                    // mis-dispatched the query path before. `cluster_owners`
                    // carries the incremental carry-over map; it is empty for
                    // full rebuilds (every cluster owned by `segment_id`) and
                    // populated only on the incremental fast path below.
                    cluster_owners: cluster_owners.clone(),
                    sketch: sketch_ref.clone(),
                    cluster_objects: cluster_objects.clone(),
                    bootstrap: bootstrap_ref.clone(),
                    membership: membership_ref.clone(),
                    artifact_origin: None,
                },
                self.config.max_pending_deletes,
                self.config.max_old_segments,
                manifest_stamp,
            );
            if is_hierarchical {
                fresh_manifest
                    .set_hierarchical_routing_nodes(&segment_id, routing_node_ids.clone());
            }
            fresh_manifest.remove_compacted_located_fragments_at(
                &authoritative_origin,
                &compacted_fragments,
                manifest_stamp,
            )?;
            merge_pending_deletes(&mut fresh_manifest, &deferred_deletes, &processed_deletes);

            // A legacy generation may retain old immutable segments alongside
            // the new segment. A restarted process has no local hash knowledge
            // for those retained objects, so complete the exact post-compaction
            // inventory now. This makes one explicit compaction sufficient for
            // upgrade instead of requiring a second no-WAL pass.
            if self.store.object_signer_node()?.is_some()
                && fresh_manifest.receipt_upgrade_needed(namespace)
            {
                fresh_manifest
                    .hydrate_receipt_artifacts(&self.store, namespace)
                    .await?;
            }

            // Manifest reads, inventory hydration, and candidate mutation can
            // all outlive the lease observation at the top of this attempt.
            // Recheck at the final publication boundary; ETag CAS remains the
            // independent second layer against a concurrent manifest writer.
            check_lease_lost(namespace, lease_lost.as_deref())?;

            // Layer 2: CAS.
            match fresh_manifest
                .write_conditional(&self.store, namespace, &version)
                .await
            {
                Ok(_) => {
                    let elapsed = start.elapsed();
                    crate::metrics::COMPACTION_DURATION
                        .with_label_values(&[namespace])
                        .observe(elapsed.as_secs_f64());

                    info!(
                        segment_id = %segment_id,
                        vectors_compacted,
                        fragments_removed,
                        elapsed_ms = elapsed.as_millis(),
                        attempt,
                        "compaction complete"
                    );

                    if let Some(token) = fencing_token {
                        drop_compaction_staging(&self.store, namespace, token).await;
                    }

                    return Ok(CompactionResult {
                        segment_id: Some(segment_id),
                        vectors_compacted,
                        fragments_removed,
                        old_segment_removed: old_segment_id,
                    });
                }
                Err(ZeppelinError::ManifestConflict { .. }) => {
                    warn!(
                        attempt,
                        "manifest CAS conflict in compactor, retrying with backoff"
                    );
                    let backoff_ms = (50u64 * (1 << attempt.min(5))).min(2000);
                    let jitter_ms = rand::thread_rng().gen_range(0..50);
                    tokio::time::sleep(Duration::from_millis(backoff_ms + jitter_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(ZeppelinError::ManifestConflict {
            namespace: namespace.to_string(),
        })
    }
}

impl Compactor {
    /// Reuses old centroids while assigning a fully materialized survivor set.
    ///
    /// Every surviving vector is assigned to its nearest old centroid. Only
    /// clusters affected by a WAL add, update, delete, or cross-cluster move need
    /// new physical objects; untouched clusters can remain under their resolved
    /// older owner keys. When carry-over is disabled, all clusters are rewritten
    /// while still avoiding k-means retraining.
    ///
    /// Returns `(cluster_count, bitmap_fields, cluster_owners, sketch_ref,
    /// bootstrap_ref, membership_ref, cluster_objects)`.
    /// `cluster_owners[i]` is the segment ID that owns cluster `i`'s per-cluster sidecars:
    /// `new_segment_id` for rewritten clusters, the old segment's resolved
    /// owner for carried-over ones. An empty vec would mean "all owned by
    /// `new_segment_id`", but this fn always returns a full-length map when it
    /// carries anything; it returns empty only if every cluster is rewritten.
    ///
    /// # Touched-cluster detection (correctness)
    /// A cluster `i` must be rewritten if EITHER:
    /// - a WAL-touched ID is newly assigned to it (add/update landing in `i`), OR
    /// - a WAL-touched ID was an OLD member of it (`old_id_to_cluster[id] == i`) —
    ///   this catches deletes (member removed) and updates that move a vector to
    ///   a different cluster (stale copy must be dropped from `i`). B3.
    ///
    /// Everything not in the touched set is byte-identical to the old segment,
    /// so it is carried by reference.
    ///
    /// `bitmap_fields` reflects the UNION across rewritten clusters only; when
    /// nothing forces a field's bitmap into a rewritten cluster it can be
    /// dropped from the reported set. To keep the segment's advertised bitmap
    /// fields stable (a carried cluster may still have bitmaps under its old
    /// key), we seed the set with the old segment's fields — see the caller,
    /// which is why `old_bitmap_fields` is passed in.
    ///
    /// ```text
    /// all surviving VectorEntry values
    ///           |
    ///           v
    /// assign each to nearest reused centroid
    ///           |
    ///           +-- WAL ID lands here ----------+
    ///           +-- WAL ID used to live here ---+--> mark cluster touched
    ///           |
    ///           v
    /// touched: rewrite under new segment ID
    /// untouched: preserve resolved old owner and immutable objects
    /// ```
    ///
    /// # Parameters
    ///
    /// - `namespace`: Logical target used to derive new keys and metric labels.
    /// - `old_segment`: Manifest-resolved active segment whose physical
    ///   centroids and global quantization artifacts are reused.
    /// - `indexing_config`: Effective layout and quantization configuration.
    /// - `new_segment_id`: Unique owner for newly written artifacts.
    /// - `vectors`: Complete sorted survivor set, including old and WAL rows.
    /// - `wal_touched_ids`: IDs added, updated, or deleted in this snapshot.
    /// - `old_id_to_cluster`: Old physical membership used to mark removals and
    ///   cross-cluster moves.
    /// - `old_cluster_owners`: Resolved owner overrides for carried clusters.
    /// - `old_cluster_objects`: Explicit grouped-object layout, if present.
    /// - `old_bitmap_fields`: Fields advertised by carried bitmap sidecars.
    /// - `old_sketch_ref`: Existing resident sketch used for stitching.
    /// - `allow_carryover`: Whether untouched clusters may keep old objects;
    ///   FTS callers pass `false` because FTS rebuild expects new attr keys.
    ///
    /// # Returns
    ///
    /// Cluster count, advertised bitmap fields, optional owner overrides,
    /// sketch/bootstrap/membership references, and the candidate's exact cluster
    /// object descriptors.
    ///
    /// # Errors
    ///
    /// Propagates old artifact reads and decoding, dimension/index calculations,
    /// serialization, sketch stitching/rebuild, quantization, and any candidate
    /// upload failure. Some new immutable objects may already exist.
    ///
    /// # Side Effects
    ///
    /// Reads old segment-global artifacts, computes assignments in memory, and
    /// uploads new globals plus rewritten per-cluster objects.
    ///
    /// # Consistency
    ///
    /// This function does not publish a manifest. Its returned ownership closure
    /// is safe to expose only through the caller's later fenced CAS.
    ///
    /// # Performance
    ///
    /// Assignment costs `O(vectors * clusters * dimensions)`. K-means is avoided;
    /// upload volume is proportional to touched clusters when carry-over is
    /// allowed, but this variant has already read the complete old segment.
    ///
    /// # Examples
    ///
    /// Updating an ID from old cluster 2 to nearest cluster 5 marks both 2 and
    /// 5. Rewriting only cluster 5 would leave a ghost copy in cluster 2; the old
    /// membership map prevents that error.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Borrowed slices and maps express read-only views without copying. The
    /// method constructs owned nested vectors for the new layout, then moves
    /// them into `IncrementalClusterState`; the compiler rejects later use of
    /// those moved containers. Iterator collection is monomorphized without
    /// virtual stream dispatch.
    #[allow(clippy::too_many_arguments)]
    async fn incremental_build(
        &self,
        namespace: &str,
        old_segment: LocatedSegmentRef<'_>,
        indexing_config: &IndexingConfig,
        new_segment_id: &str,
        vectors: &[VectorEntry],
        wal_touched_ids: &HashSet<String>,
        old_id_to_cluster: &HashMap<String, usize>,
        old_cluster_owners: &[String],
        old_cluster_objects: &[ClusterDataObjectRef],
        old_bitmap_fields: &[String],
        old_sketch_ref: Option<&crate::wal::manifest::SketchRef>,
        allow_carryover: bool,
    ) -> Result<(
        usize,
        Vec<String>,
        Vec<String>,
        crate::wal::manifest::SketchRef,
        BootstrapRef,
        MembershipRef,
        Vec<ClusterDataObjectRef>,
    )> {
        use crate::index::distance::euclidean_distance;
        let centroid_state = self
            .load_incremental_centroid_state(namespace, old_segment, indexing_config)
            .await?;
        let IncrementalCentroidState {
            centroids,
            dim,
            sq_calibration_bytes,
            sq_calibration,
        } = centroid_state;
        let num_clusters = centroids.len();

        // Assign ALL surviving vectors to nearest centroid.
        let mut cluster_ids: Vec<Vec<String>> = vec![Vec::new(); num_clusters];
        let mut cluster_vecs: Vec<Vec<Vec<f32>>> = vec![Vec::new(); num_clusters];
        let mut cluster_attrs: Vec<Vec<Option<HashMap<String, crate::types::AttributeValue>>>> =
            vec![Vec::new(); num_clusters];

        // A cluster is "touched" (must be rewritten) if a WAL-touched vector is
        // assigned to it now, or was an old member of it.
        let mut touched: Vec<bool> = vec![false; num_clusters];

        for entry in vectors {
            let mut best_dist = f32::MAX;
            let mut best_cluster = 0usize;
            for (c, centroid) in centroids.iter().enumerate() {
                let d = euclidean_distance(&entry.values, centroid);
                if d < best_dist {
                    best_dist = d;
                    best_cluster = c;
                }
            }
            if wal_touched_ids.contains(&entry.id) {
                touched[best_cluster] = true;
            }
            cluster_ids[best_cluster].push(entry.id.clone());
            cluster_vecs[best_cluster].push(entry.values.clone());
            cluster_attrs[best_cluster].push(entry.attributes.clone());
        }
        // Deletes and cross-cluster moves: any WAL-touched ID that used to live
        // in cluster `i` forces `i` to be rewritten even if nothing landed in
        // it this cycle (B3).
        for id in wal_touched_ids {
            if let Some(&old_cluster) = old_id_to_cluster.get(id) {
                if old_cluster < num_clusters {
                    touched[old_cluster] = true;
                }
            }
        }

        // If carry-over is disabled (FTS configured, or no usable old owner
        // map), fall back to rewriting everything — still cheaper than a full
        // retrain because centroids are reused, matching prior behavior.
        let carry_over = allow_carryover;
        if !carry_over {
            for t in touched.iter_mut() {
                *t = true;
            }
        }

        let centroid_state = IncrementalCentroidState {
            centroids,
            dim,
            sq_calibration_bytes,
            sq_calibration,
        };
        let cluster_state = IncrementalClusterState {
            cluster_ids,
            cluster_vecs,
            cluster_attrs,
            touched,
        };
        self.write_incremental_segment(
            namespace,
            old_segment,
            new_segment_id,
            centroid_state,
            cluster_state,
            old_cluster_owners,
            old_cluster_objects,
            old_bitmap_fields,
            old_sketch_ref,
            None,
            true,
            indexing_config,
        )
        .await
    }

    /// Loads reusable centroid and scalar-calibration state from the old segment.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Logical target used for metrics.
    /// - `old_segment`: Located segment whose physical centroid coordinate
    ///   system must remain stable for carried clusters.
    /// - `indexing_config`: Effective quantization choice for the new segment.
    ///
    /// # Returns
    ///
    /// Owned centroids and dimension plus both encoded and decoded SQ
    /// calibration when scalar quantization is active.
    ///
    /// # Errors
    ///
    /// Propagates missing/corrupt centroid data, a required legacy calibration
    /// GET, or calibration decoding failure.
    ///
    /// # Side Effects
    ///
    /// Performs one centroid GET and, for legacy scalar segments whose centroid
    /// blob does not embed calibration, one additional calibration GET.
    ///
    /// # Consistency
    ///
    /// Reusing these exact values is required: carried quantized rows were
    /// encoded against the old coordinate system. This function never trains or
    /// silently substitutes calibration.
    ///
    /// # Examples
    ///
    /// A current SQ segment returns embedded calibration with its centroids. An
    /// older SQ segment triggers a read of its separate calibration artifact;
    /// absence fails incremental construction so the caller can full-retrain.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `Option::map(...).transpose()` converts
    /// `Option<Result<Calibration>>` into `Result<Option<Calibration>>`. It is an
    /// exhaustive, allocation-free way to say "decode only when present, but do
    /// not hide a decode error," replacing nested null/error checks in Java or C.
    async fn load_incremental_centroid_state(
        &self,
        namespace: &str,
        old_segment: LocatedSegmentRef<'_>,
        indexing_config: &IndexingConfig,
    ) -> Result<IncrementalCentroidState> {
        use crate::index::ivf_flat::build::{centroids_key, deserialize_centroids_data};

        let physical_namespace = old_segment.physical_namespace();
        let old_segment_id = &old_segment.segment.id;
        let ckey = centroids_key(physical_namespace, old_segment_id);
        let centroids_data = get_compaction_read(
            &self.store,
            namespace,
            &ckey,
            COMPACTION_READ_CLASS_CENTROIDS,
        )
        .await?;
        let decoded_centroids = deserialize_centroids_data(&centroids_data)?;
        let centroids = decoded_centroids.centroids;
        let dim = decoded_centroids.dim;
        let mut sq_calibration_bytes = decoded_centroids.sq_calibration;
        if matches!(
            indexing_config.quantization,
            crate::index::quantization::QuantizationType::Scalar
        ) && sq_calibration_bytes.is_none()
        {
            use crate::index::quantization::sq::sq_calibration_key;
            sq_calibration_bytes = Some(
                get_compaction_read(
                    &self.store,
                    namespace,
                    &sq_calibration_key(physical_namespace, old_segment_id),
                    COMPACTION_READ_CLASS_SQ,
                )
                .await?,
            );
        }
        let sq_calibration = sq_calibration_bytes
            .as_ref()
            .map(|bytes| crate::index::quantization::sq::SqCalibration::from_bytes(bytes))
            .transpose()?;

        Ok(IncrementalCentroidState {
            centroids,
            dim,
            sq_calibration_bytes,
            sq_calibration,
        })
    }

    /// Builds an incremental candidate while reading only touched old clusters.
    ///
    /// The membership artifact supplies old ID-to-cluster placement and the
    /// resident sketch supplies coarse data for untouched clusters. WAL rows are
    /// assigned against reused centroids; only old clusters marked by an add,
    /// update, delete, or move are fetched. Untouched rows are represented by
    /// their IDs until the new membership and stitched sketch are written.
    ///
    /// ```text
    /// membership IDs + WAL touched IDs + reused centroids
    ///                   |
    ///                   v
    ///          compute touched cluster bitset
    ///                   |
    ///       +-----------+------------+
    ///       |                        |
    ///       v                        v
    /// touched old cluster GETs   untouched IDs only
    /// merge old survivors + WAL  carry old objects/sketch rows
    ///       |                        |
    ///       +-----------+------------+
    ///                   v
    ///       write one coherent candidate closure
    /// ```
    ///
    /// # Parameters
    ///
    /// - `namespace`: Logical target used for new keys and metrics.
    /// - `old_segment`: Manifest-resolved previously active physical segment.
    /// - `indexing_config`: Effective flat-IVF and quantization configuration.
    /// - `new_segment_id`: Unique ID for newly written artifacts.
    /// - `latest_vectors`: Last WAL upsert per ID after snapshot-order merge.
    /// - `deleted_ids`: IDs whose last WAL operation is a delete.
    /// - `wal_touched_ids`: Union of all add/update/delete IDs in the snapshot.
    /// - `old_membership`: Decoded complete membership of the old segment.
    /// - `old_cluster_owners`: Per-cluster physical owner overrides.
    /// - `old_cluster_objects`: Explicit grouped cluster-object descriptors.
    /// - `old_bitmap_fields`: Bitmap fields that carried clusters may retain.
    /// - `old_sketch_ref`: Required old sketch descriptor for row stitching.
    ///
    /// # Returns
    ///
    /// The same complete manifest-facing artifact tuple as `incremental_build`.
    ///
    /// # Errors
    ///
    /// Rejects membership/centroid count mismatch, out-of-range or missing
    /// membership entries, unavailable/corrupt sketches, inconsistent touched
    /// cluster contents, an empty resulting data set, and all downstream
    /// read/build/upload failures. The caller meters this as an incremental
    /// failure and attempts a full rebuild.
    ///
    /// # Side Effects
    ///
    /// Reads centroid, sketch, and touched cluster artifacts, records fallback
    /// metrics for unavailable sketches, and uploads candidate artifacts.
    ///
    /// # Consistency
    ///
    /// Old membership is cross-checked against each loaded touched row before it
    /// is trusted. Untouched clusters require a valid old sketch because this
    /// path lacks their full vectors and cannot reconstruct coarse rows safely.
    ///
    /// # Performance
    ///
    /// Storage reads and per-vector merge work are bounded by WAL rows plus
    /// touched clusters, apart from the compact membership and resident sketch
    /// globals. This is the main large-namespace compaction fast path.
    ///
    /// # Examples
    ///
    /// Deleting one vector in cluster 12 loads cluster 12 and its attributes,
    /// keeps 99 other clusters by reference, removes the ID from membership, and
    /// stitches the old sketch with a rebuilt row for cluster 12.
    #[allow(clippy::too_many_arguments)]
    async fn incremental_build_bounded(
        &self,
        namespace: &str,
        old_segment: LocatedSegmentRef<'_>,
        indexing_config: &IndexingConfig,
        new_segment_id: &str,
        latest_vectors: &HashMap<String, VectorEntry>,
        deleted_ids: &HashSet<String>,
        wal_touched_ids: &HashSet<String>,
        old_membership: &MembershipData,
        old_cluster_owners: &[String],
        old_cluster_objects: &[ClusterDataObjectRef],
        old_bitmap_fields: &[String],
        old_sketch_ref: Option<&crate::wal::manifest::SketchRef>,
    ) -> Result<(
        usize,
        Vec<String>,
        Vec<String>,
        crate::wal::manifest::SketchRef,
        BootstrapRef,
        MembershipRef,
        Vec<ClusterDataObjectRef>,
    )> {
        use crate::index::ivf_flat::sketch::decode_resident_sketch;

        let centroid_state = self
            .load_incremental_centroid_state(namespace, old_segment, indexing_config)
            .await?;
        let num_clusters = centroid_state.centroids.len();
        if old_membership.cluster_count as usize != num_clusters {
            return Err(ZeppelinError::Membership(format!(
                "membership cluster_count {} does not match centroid count {num_clusters}",
                old_membership.cluster_count
            )));
        }

        let old_sketch_ref = old_sketch_ref.ok_or_else(|| {
            meter_sketch_unavailable(namespace, "old_sketch_ref_missing");
            ZeppelinError::Index(
                "bounded incremental requires an old resident sketch for carried clusters".into(),
            )
        })?;
        let old_sketch_data = match get_compaction_read(
            &self.store,
            namespace,
            &old_sketch_ref.key,
            COMPACTION_READ_CLASS_SKETCH,
        )
        .await
        {
            Ok(data) => data,
            Err(ZeppelinError::NotFound { key }) => {
                warn!(
                    key = %key,
                    "old resident sketch missing for bounded incremental stitching"
                );
                meter_sketch_unavailable(namespace, "old_sketch_missing");
                return Err(ZeppelinError::CoarseSketch(format!(
                    "bounded incremental referenced resident sketch is missing: {key}"
                )));
            }
            Err(error) => return Err(error),
        };
        let old_sketch = decode_resident_sketch(
            old_sketch_data,
            old_sketch_ref,
            &centroid_state.centroids,
            old_membership.entries.len(),
        )
        .map_err(|error| {
            warn!(
                error = %error,
                key = %old_sketch_ref.key,
                "old resident sketch could not be decoded for bounded incremental stitching"
            );
            meter_sketch_unavailable(namespace, "old_sketch_decode_failed");
            ZeppelinError::CoarseSketch(error.to_string())
        })?;

        let mut old_id_to_cluster: HashMap<String, usize> =
            HashMap::with_capacity(old_membership.entries.len());
        let mut carried_ids: Vec<Vec<String>> = vec![Vec::new(); num_clusters];
        for (id, cluster_idx) in &old_membership.entries {
            let cluster_idx = *cluster_idx as usize;
            if cluster_idx >= num_clusters {
                return Err(ZeppelinError::Membership(format!(
                    "membership id {id} references out-of-range cluster {cluster_idx}"
                )));
            }
            old_id_to_cluster.insert(id.clone(), cluster_idx);
            if !deleted_ids.contains(id) && !latest_vectors.contains_key(id) {
                carried_ids[cluster_idx].push(id.clone());
            }
        }

        let mut wal_entries: Vec<VectorEntry> = latest_vectors
            .values()
            .filter(|entry| keep_finite_compaction_vector(namespace, entry))
            .cloned()
            .collect();
        wal_entries.sort_by(|a, b| a.id.cmp(&b.id));

        let mut wal_by_cluster: Vec<Vec<VectorEntry>> = vec![Vec::new(); num_clusters];
        let mut touched = vec![false; num_clusters];
        for entry in wal_entries {
            let cluster_idx = nearest_cluster(&centroid_state.centroids, &entry.values)?;
            touched[cluster_idx] = true;
            wal_by_cluster[cluster_idx].push(entry);
        }
        for id in wal_touched_ids {
            if let Some(&old_cluster) = old_id_to_cluster.get(id) {
                touched[old_cluster] = true;
            }
        }

        let loaded_touched = load_touched_segment_vectors(
            &self.store,
            old_segment,
            old_cluster_owners,
            old_cluster_objects,
            &touched,
        )
        .await?;

        let mut cluster_ids: Vec<Vec<String>> = vec![Vec::new(); num_clusters];
        let mut cluster_vecs: Vec<Vec<Vec<f32>>> = vec![Vec::new(); num_clusters];
        let mut cluster_attrs: Vec<Vec<Option<HashMap<String, crate::types::AttributeValue>>>> =
            vec![Vec::new(); num_clusters];

        for cluster_idx in 0..num_clusters {
            if !touched[cluster_idx] {
                cluster_ids[cluster_idx] = carried_ids[cluster_idx].clone();
                cluster_vecs[cluster_idx] = vec![Vec::new(); carried_ids[cluster_idx].len()];
                cluster_attrs[cluster_idx] = vec![None; carried_ids[cluster_idx].len()];
                continue;
            }

            let mut entries = Vec::new();
            for entry in &loaded_touched[cluster_idx] {
                match old_id_to_cluster.get(&entry.id).copied() {
                    Some(old_cluster) if old_cluster == cluster_idx => {}
                    Some(old_cluster) => {
                        return Err(ZeppelinError::Membership(format!(
                            "membership says id {} belongs to cluster {old_cluster}, \
                             but loaded it from cluster {cluster_idx}",
                            entry.id
                        )));
                    }
                    None => {
                        return Err(ZeppelinError::Membership(format!(
                            "loaded id {} from touched cluster {cluster_idx}, \
                             but membership has no entry for it",
                            entry.id
                        )));
                    }
                }
                if !deleted_ids.contains(&entry.id) && !latest_vectors.contains_key(&entry.id) {
                    entries.push(entry.clone());
                }
            }
            entries.extend(wal_by_cluster[cluster_idx].iter().cloned());
            entries.sort_by(|a, b| a.id.cmp(&b.id));

            for entry in entries {
                cluster_ids[cluster_idx].push(entry.id);
                cluster_vecs[cluster_idx].push(entry.values);
                cluster_attrs[cluster_idx].push(entry.attributes);
            }
        }

        let cluster_state = IncrementalClusterState {
            cluster_ids,
            cluster_vecs,
            cluster_attrs,
            touched,
        };
        if cluster_state.vector_count() == 0 {
            return Err(ZeppelinError::Index(
                "bounded incremental produced an empty vector set".into(),
            ));
        }

        self.write_incremental_segment(
            namespace,
            old_segment,
            new_segment_id,
            centroid_state,
            cluster_state,
            old_cluster_owners,
            old_cluster_objects,
            old_bitmap_fields,
            Some(old_sketch_ref),
            Some(old_sketch),
            false,
            indexing_config,
        )
        .await
    }

    /// Serializes and uploads a complete incremental segment ownership closure.
    ///
    /// This shared finalizer writes new segment-global centroids, sketch,
    /// bootstrap, and membership artifacts; serializes only touched cluster
    /// vector/attribute/bitmap/quantization data; and returns explicit references
    /// for everything carried or rewritten. It never publishes the manifest.
    ///
    /// ```text
    /// IncrementalCentroidState + IncrementalClusterState
    ///                         |
    ///               resolve old owner chains
    ///                         |
    ///          +--------------+----------------+
    ///          |                               |
    /// touched cluster                     carried cluster
    /// serialize under new ID              keep old exact key
    ///          |                               |
    ///          +--------------+----------------+
    ///                         |
    ///         rebuild/stitch globals using one calibration
    ///                         |
    ///                         v
    ///              parallel immutable PUTs
    ///                         |
    ///                         v
    ///       return SegmentRef components to CAS caller
    /// ```
    ///
    /// # Parameters
    ///
    /// - `namespace`: Logical target used to construct every new object key.
    /// - `old_segment`: Located prior segment used as the default carried owner.
    /// - `new_segment_id`: Candidate segment ID owning all new objects.
    /// - `centroid_state`: Reused segment-global coordinate and SQ state.
    /// - `cluster_state`: IDs, rewritten values/attributes, and touched flags.
    /// - `old_cluster_owners`: Owner overrides to chain-compress when carrying.
    /// - `old_cluster_objects`: Grouped-object descriptors for carried clusters.
    /// - `old_bitmap_fields`: Advertised fields that carried bitmaps still serve.
    /// - `old_sketch_ref`: Optional old sketch to fetch and stitch.
    /// - `preloaded_old_sketch`: Already decoded sketch for the bounded path.
    /// - `allow_sketch_rebuild`: Whether complete resident vectors permit a
    ///   missing sketch to be rebuilt rather than rejecting incremental work.
    /// - `indexing_config`: Bitmap and quantization format choices.
    ///
    /// # Returns
    ///
    /// Cluster count, bitmap fields, compressed owner map, sketch/bootstrap/
    /// membership references, and exact cluster object descriptors.
    ///
    /// # Errors
    ///
    /// Rejects mismatched per-cluster state, unavailable sketch when rebuilding
    /// is unsafe, malformed old quantization data, missing grouped ownership,
    /// serialization failures, and any required GET or PUT failure. Because PUTs
    /// are parallel, a returned error can follow successful sibling uploads.
    ///
    /// # Side Effects
    ///
    /// Reads an old sketch or PQ codebook when required, computes artifacts, and
    /// uploads all new payloads concurrently through the store abstraction.
    ///
    /// # Consistency
    ///
    /// Carried SQ codes retain the exact old calibration, and carried PQ codes
    /// retain the exact old codebook copied under the new segment ID. Recomputing
    /// either global would make old codes numerically incompatible. Every owner
    /// is resolved to a physical segment rather than another forwarding logical
    /// segment, preventing unbounded owner chains.
    ///
    /// # Performance
    ///
    /// CPU and upload volume scale with rewritten clusters plus the mandatory
    /// global artifacts. [`bytes::Bytes`] payload clones are shallow, and
    /// `join_all` overlaps PUT latency. The function holds no synchronous lock
    /// across `.await`.
    ///
    /// # Examples
    ///
    /// If clusters 0 and 3 are touched in a ten-cluster SQ segment, both are
    /// encoded with the reused calibration and written under the new ID. Eight
    /// owner entries still point at their resolved old objects; the new centroid
    /// artifact embeds the same calibration for query decoding.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Destructuring moves owned fields out of the two state structs, making the
    /// transition from "planning" to "serialization" explicit. The local
    /// `resolve_old_owner` closure borrows the old owner slice and returns owned
    /// `String` values, avoiding dangling references in the manifest candidate.
    /// A `BTreeMap` later provides deterministic key order where a `HashMap`
    /// deliberately would not.
    #[allow(clippy::too_many_arguments)]
    async fn write_incremental_segment(
        &self,
        namespace: &str,
        old_segment: LocatedSegmentRef<'_>,
        new_segment_id: &str,
        centroid_state: IncrementalCentroidState,
        cluster_state: IncrementalClusterState,
        old_cluster_owners: &[String],
        old_cluster_objects: &[ClusterDataObjectRef],
        old_bitmap_fields: &[String],
        old_sketch_ref: Option<&crate::wal::manifest::SketchRef>,
        preloaded_old_sketch: Option<crate::index::ivf_flat::sketch::ResidentSketch>,
        allow_sketch_rebuild: bool,
        indexing_config: &IndexingConfig,
    ) -> Result<(
        usize,
        Vec<String>,
        Vec<String>,
        crate::wal::manifest::SketchRef,
        BootstrapRef,
        MembershipRef,
        Vec<ClusterDataObjectRef>,
    )> {
        use crate::index::ivf_flat::build::{
            build_bootstrap_artifact, centroids_key, serialize_attrs,
            serialize_centroids_with_sq_calibration, serialize_cluster,
            serialize_cluster_data_object, serialize_colocated_sq_cluster,
        };
        use crate::index::ivf_flat::sketch::{
            build_resident_sketch, stitch_resident_sketch, ResidentSketchStitch,
        };
        use bytes::Bytes;

        let old_segment_id = old_segment.segment.id.as_str();
        let old_physical_namespace = old_segment.physical_namespace();

        let IncrementalCentroidState {
            centroids,
            dim,
            sq_calibration_bytes,
            sq_calibration,
        } = centroid_state;
        let IncrementalClusterState {
            cluster_ids,
            cluster_vecs,
            cluster_attrs,
            touched,
        } = cluster_state;
        let num_clusters = centroids.len();
        if touched.len() != num_clusters
            || cluster_ids.len() != num_clusters
            || cluster_vecs.len() != num_clusters
            || cluster_attrs.len() != num_clusters
        {
            return Err(ZeppelinError::Index(
                "incremental cluster state length mismatch".into(),
            ));
        }

        // Resolve the owning segment ID for cluster `i` when carried over: the
        // old segment's own resolved owner (chain-compresses across
        // generations so we never point at a segment that itself only holds a
        // reference).
        let resolve_old_owner = |i: usize| -> String {
            old_cluster_owners
                .get(i)
                .cloned()
                .unwrap_or_else(|| old_segment_id.to_string())
        };

        // Build cluster_owners: rewritten -> new_segment_id, carried -> old owner.
        let mut cluster_owners: Vec<String> = Vec::with_capacity(num_clusters);
        for (i, is_touched) in touched.iter().enumerate() {
            if *is_touched {
                cluster_owners.push(new_segment_id.to_string());
            } else {
                cluster_owners.push(resolve_old_owner(i));
            }
        }
        let rewritten_count = touched.iter().filter(|t| **t).count();
        let carries_clusters = rewritten_count < num_clusters;

        let new_ckey = centroids_key(namespace, new_segment_id);
        let new_centroids_data = serialize_centroids_with_sq_calibration(
            &centroids,
            dim,
            sq_calibration_bytes.as_ref().map(|bytes| bytes.as_ref()),
        )?;

        // CPU phase: pre-serialize payloads for REWRITTEN clusters only.
        //
        // Seed the reported bitmap fields with the OLD segment's set: a field
        // whose bitmaps live only in carried clusters (under old keys, still
        // owner-resolved at query time) must stay advertised, or the query
        // path would stop offering its prefilter. When carry-over is disabled
        // every cluster is rewritten, so this seed is a harmless superset that
        // the rewritten-cluster scan reproduces anyway.
        let mut bitmap_fields_set: std::collections::HashSet<String> =
            if carries_clusters && indexing_config.bitmap_index {
                old_bitmap_fields.iter().cloned().collect()
            } else {
                std::collections::HashSet::new()
            };
        let mut payloads: Vec<(String, Bytes)> = vec![(new_ckey, new_centroids_data.clone())];
        let mut cluster_object_sizes: HashMap<String, u64> = HashMap::new();

        let mut sketch_unavailable_reason = None;
        let stitched_sketch = if let Some(old_sketch) = preloaded_old_sketch.as_ref() {
            match stitch_resident_sketch(
                namespace,
                new_segment_id,
                dim,
                &centroids,
                old_sketch,
                &touched,
                &cluster_vecs,
                &cluster_attrs,
            )? {
                ResidentSketchStitch::Stitched(sketch_ref, sketch_data, resident) => {
                    Some((sketch_ref, sketch_data, *resident))
                }
                ResidentSketchStitch::Unavailable(reason) => {
                    sketch_unavailable_reason = Some(reason);
                    None
                }
            }
        } else if let Some(old_sketch_ref) = old_sketch_ref {
            match get_compaction_read(
                &self.store,
                namespace,
                &old_sketch_ref.key,
                COMPACTION_READ_CLASS_SKETCH,
            )
            .await
            {
                Ok(old_sketch_data) => {
                    match crate::index::ivf_flat::sketch::ResidentSketch::from_owned_bytes(
                        old_sketch_data,
                    )
                    .and_then(|sketch| {
                        sketch.validate_reference(old_sketch_ref)?;
                        sketch.validate_centroid_shape(&centroids)?;
                        sketch.with_centroids(&centroids)
                    }) {
                        Ok(old_sketch) => match stitch_resident_sketch(
                            namespace,
                            new_segment_id,
                            dim,
                            &centroids,
                            &old_sketch,
                            &touched,
                            &cluster_vecs,
                            &cluster_attrs,
                        )? {
                            ResidentSketchStitch::Stitched(sketch_ref, sketch_data, resident) => {
                                Some((sketch_ref, sketch_data, *resident))
                            }
                            ResidentSketchStitch::Unavailable(reason) => {
                                sketch_unavailable_reason = Some(reason);
                                None
                            }
                        },
                        Err(error) => {
                            warn!(
                                error = %error,
                                key = %old_sketch_ref.key,
                                "old resident sketch could not be decoded for stitching"
                            );
                            return Err(ZeppelinError::CoarseSketch(error.to_string()));
                        }
                    }
                }
                Err(ZeppelinError::NotFound { key }) => {
                    warn!(
                        key = %key,
                        "old resident sketch missing for incremental stitching"
                    );
                    return Err(ZeppelinError::CoarseSketch(format!(
                        "incremental referenced resident sketch is missing: {key}"
                    )));
                }
                Err(error) => return Err(error),
            }
        } else {
            sketch_unavailable_reason = Some("old_sketch_ref_missing");
            None
        };
        // The stitched rows own their new immutable allocation. Release the
        // potentially multi-gigabyte old resident artifact before assembling a
        // second copy inside the bootstrap object.
        drop(preloaded_old_sketch);
        let (sketch_ref, sketch_data, _resident_sketch) =
            if let Some(stitched_sketch) = stitched_sketch {
                stitched_sketch
            } else {
                let reason = sketch_unavailable_reason.unwrap_or("unknown");
                meter_sketch_unavailable(namespace, reason);
                if !allow_sketch_rebuild {
                    warn!(
                        reason,
                        "bounded incremental resident sketch stitch unavailable"
                    );
                    return Err(ZeppelinError::Index(format!(
                        "bounded incremental resident sketch stitch unavailable: {reason}"
                    )));
                }
                warn!(
                    reason,
                    "incremental resident sketch stitch unavailable, rebuilding sketch"
                );
                build_resident_sketch(
                    namespace,
                    new_segment_id,
                    dim,
                    &centroids,
                    &cluster_vecs,
                    &cluster_attrs,
                )?
            };
        let (bootstrap_ref, bootstrap_data) =
            build_bootstrap_artifact(namespace, new_segment_id, &new_centroids_data, &sketch_data)?;
        let (membership_ref, membership_data) =
            build_membership_artifact(namespace, new_segment_id, &cluster_ids)?;
        payloads.push((sketch_ref.key.clone(), sketch_data));
        payloads.push((bootstrap_ref.key.clone(), bootstrap_data));
        payloads.push((membership_ref.key.clone(), membership_data));

        for i in 0..num_clusters {
            if !touched[i] {
                continue; // carried over by reference
            }
            let cvec_data = if let Some(calibration) = &sq_calibration {
                let cluster_refs: Vec<&[f32]> =
                    cluster_vecs[i].iter().map(|v| v.as_slice()).collect();
                let codes = calibration.encode_batch(&cluster_refs);
                serialize_colocated_sq_cluster(&cluster_ids[i], &cluster_vecs[i], &codes, dim)?
            } else {
                serialize_cluster(&cluster_ids[i], &cluster_vecs[i], dim)?
            };
            let (cvec_key, cvec_payload) = if old_cluster_objects.is_empty() {
                (cluster_key(namespace, new_segment_id, i), cvec_data)
            } else {
                let key = cluster_group_key(namespace, new_segment_id, i);
                let data = serialize_cluster_data_object(&[(i, cvec_data)])?;
                (key, data)
            };
            cluster_object_sizes.insert(cvec_key.clone(), cvec_payload.len() as u64);

            let cattr_data = serialize_attrs(&cluster_attrs[i])?;
            let cattr_key = attrs_key(namespace, new_segment_id, i);

            payloads.push((cvec_key, cvec_payload));
            payloads.push((cattr_key, cattr_data));

            if indexing_config.bitmap_index {
                let attr_refs: Vec<Option<&HashMap<String, crate::types::AttributeValue>>> =
                    cluster_attrs[i].iter().map(|a| a.as_ref()).collect();
                let bitmap_index = crate::index::bitmap::build::build_cluster_bitmaps(&attr_refs);
                for field_name in bitmap_index.fields.keys() {
                    bitmap_fields_set.insert(field_name.clone());
                }
                let bitmap_data = bitmap_index.to_bytes()?;
                let bkey = crate::index::bitmap::bitmap_key(namespace, new_segment_id, i);
                payloads.push((bkey, bitmap_data));
            }
        }

        // Quantized artifacts. The calibration/codebook is SEGMENT-GLOBAL and
        // MUST be reused (not recomputed): carried clusters' codes were encoded
        // against the old calibration, and the search path reads the
        // calibration under the NEW segment ID. Recomputing it would silently
        // corrupt every carried cluster's approximate distances. So we COPY the
        // old calibration/codebook to the new segment and re-encode only the
        // rewritten clusters against it.
        match indexing_config.quantization {
            crate::index::quantization::QuantizationType::Scalar => {
                debug!(
                    "incremental SQ8 build embedded calibration and co-located rewritten clusters"
                );
            }
            crate::index::quantization::QuantizationType::Product => {
                use crate::index::quantization::pq::{
                    pq_cluster_key, pq_codebook_key, serialize_pq_cluster, PqCodebook,
                };

                // Reuse the old segment's codebook — retraining defeats the
                // point of the incremental path. If it's missing, fail so the
                // caller falls back to a full retrain.
                let cb_data = get_compaction_read(
                    &self.store,
                    namespace,
                    &pq_codebook_key(old_physical_namespace, old_segment_id),
                    COMPACTION_READ_CLASS_SQ,
                )
                .await?;
                let codebook = PqCodebook::from_bytes(&cb_data)?;
                payloads.push((
                    pq_codebook_key(namespace, new_segment_id),
                    codebook.to_bytes(),
                ));

                for i in 0..num_clusters {
                    if !touched[i] {
                        continue;
                    }
                    let cluster_refs: Vec<&[f32]> =
                        cluster_vecs[i].iter().map(|v| v.as_slice()).collect();
                    let codes = codebook.encode_batch(&cluster_refs);
                    let pq_data = serialize_pq_cluster(&cluster_ids[i], &codes, codebook.m)?;
                    payloads.push((pq_cluster_key(namespace, new_segment_id, i), pq_data));
                }
            }
            crate::index::quantization::QuantizationType::None => {}
        }

        // I/O phase: write all new segment-global and rewritten-cluster payloads in parallel.
        let write_futs: Vec<_> = payloads
            .iter()
            .map(|(key, data)| self.store.put(key, data.clone()))
            .collect();
        let results = futures::future::join_all(write_futs).await;
        for result in results {
            result?;
        }

        info!(
            num_clusters,
            rewritten_clusters = rewritten_count,
            carried_clusters = num_clusters - rewritten_count,
            "incremental build: rewrote touched clusters, carried the rest by reference"
        );

        let bitmap_fields: Vec<String> = bitmap_fields_set.into_iter().collect();
        // If every cluster was rewritten, return an empty owner map (equivalent
        // to "all owned by new_segment_id") so the common full-rewrite case
        // carries zero extra bytes in the manifest.
        let owners_out = if rewritten_count == num_clusters {
            Vec::new()
        } else {
            cluster_owners
        };
        let cluster_objects_out = incremental_cluster_objects(
            namespace,
            new_segment_id,
            num_clusters,
            &touched,
            old_cluster_objects,
            &cluster_object_sizes,
        )?;
        Ok((
            num_clusters,
            bitmap_fields,
            owners_out,
            sketch_ref,
            bootstrap_ref,
            membership_ref,
            cluster_objects_out,
        ))
    }
}

/// Records that incremental sketch stitching could not be used.
///
/// # Parameters
///
/// - `namespace`: Namespace metric label.
/// - `reason`: Structured log field describing the specific unavailable path.
///
/// # Side Effects
///
/// Increments the stable `sketch_stitch_unavailable` fallback metric and emits a
/// debug record. The reason remains in logs rather than becoming an unbounded
/// metric label.
///
/// # Examples
///
/// A missing old sketch increments the fallback family once with the stable
/// category and logs `old_sketch_missing` for diagnosis.
fn meter_sketch_unavailable(namespace: &str, reason: &str) {
    crate::metrics::COMPACTION_INCREMENTAL_FALLBACK_TOTAL
        .with_label_values(&[namespace, "sketch_stitch_unavailable"])
        .inc();
    debug!(
        reason,
        "metered incremental resident sketch stitch unavailable"
    );
}

/// Keeps finite vectors and loudly drops legacy rows containing NaN or infinity.
///
/// New API writes reject non-finite coordinates, but durable data from before
/// that validation may still exist. Failing every future compaction would trap
/// the namespace permanently, so this is the one deliberate data-quality
/// degradation: each rejected row is observable through an error log and
/// metric and is omitted from the next segment.
///
/// # Parameters
///
/// - `namespace`: Namespace label for the rejection metric and log.
/// - `vector`: Borrowed candidate row to validate without cloning it.
///
/// # Returns
///
/// `true` when every coordinate is finite; `false` after reporting the first
/// non-finite coordinate.
///
/// # Side Effects
///
/// On rejection, emits an error with ID, coordinate index, and value and
/// increments `NON_FINITE_VECTORS_SKIPPED_TOTAL`.
///
/// # Examples
///
/// `[0.2, NaN, 0.8]` returns `false`, identifies dimension 1, and will not enter
/// the new segment. `[0.2, -1.0, 0.8]` returns `true` without logging.
///
/// # Performance
///
/// Scans coordinates until the first invalid value and allocates nothing.
fn keep_finite_compaction_vector(namespace: &str, vector: &VectorEntry) -> bool {
    // Defense in depth (Task 10 I4): the API boundary now rejects NaN/inf, but
    // data written BEFORE that fix may already be durable on S3. This skip is
    // the ONE sanctioned degradation in the compactor: crashing here would
    // fail every future compaction cycle for the namespace. Skipping is loud:
    // ERROR-level structured log + metric per dropped vector. In the bounded
    // incremental path, untouched carried clusters are no longer scanned, so
    // any pre-Task-10 poison there remains served exactly as before until that
    // cluster is rewritten or a retrain fires.
    if let Some(bad_idx) = vector.values.iter().position(|x| !x.is_finite()) {
        error!(
            vector_id = %vector.id,
            dimension = bad_idx,
            value = %vector.values[bad_idx],
            "skipping vector with non-finite value during compaction; \
             it will be dropped from the compacted segment"
        );
        crate::metrics::NON_FINITE_VECTORS_SKIPPED_TOTAL
            .with_label_values(&[namespace])
            .inc();
        false
    } else {
        true
    }
}

/// Counts the logical survivor set without loading untouched cluster vectors.
///
/// # Parameters
///
/// - `old_membership`: Complete old ID-to-cluster inventory.
/// - `latest_vectors`: Last WAL upsert per touched ID.
/// - `deleted_ids`: IDs whose last WAL operation is deletion.
///
/// # Returns
///
/// Old IDs neither deleted nor replaced, plus finite latest WAL vectors. IDs in
/// `latest_vectors` are excluded from the old count to avoid double counting.
///
/// # Performance
///
/// Runs in `O(old membership + latest WAL entries)` using expected constant-time
/// hash lookups and performs no object-store I/O.
///
/// # Examples
///
/// With 100 old IDs, one delete, one updated ID, and two finite new IDs, the old
/// side contributes 98 and the WAL side contributes three, for 101 survivors.
fn bounded_survivor_count(
    old_membership: &MembershipData,
    latest_vectors: &HashMap<String, VectorEntry>,
    deleted_ids: &HashSet<String>,
) -> usize {
    let old_survivors = old_membership
        .entries
        .iter()
        .filter(|(id, _)| !deleted_ids.contains(id) && !latest_vectors.contains_key(id))
        .count();
    let wal_survivors = latest_vectors
        .values()
        .filter(|entry| entry.values.iter().all(|value| value.is_finite()))
        .count();
    old_survivors + wal_survivors
}

/// Selects the nearest centroid after validating the candidate dimensions.
///
/// # Parameters
///
/// - `centroids`: Non-empty centroid vectors expected to share one dimension.
/// - `values`: Vector coordinates to assign.
///
/// # Returns
///
/// Zero-based index of the centroid with smallest Euclidean distance. Equal
/// distances keep the earliest centroid because replacement uses strict `<`.
///
/// # Errors
///
/// Returns an index error for zero centroids or inconsistent centroid lengths,
/// and [`ZeppelinError::DimensionMismatch`] when `values` has the wrong length.
///
/// # Performance
///
/// Computes every distance in `O(clusters * dimensions)` with no allocation.
///
/// # Examples
///
/// Centroids `[0, 0]` and `[10, 10]` assign `[9, 9]` to index 1. A three-value
/// input returns a dimension error rather than reading mismatched slices.
fn nearest_cluster(centroids: &[Vec<f32>], values: &[f32]) -> Result<usize> {
    use crate::index::distance::euclidean_distance;

    if centroids.is_empty() {
        return Err(ZeppelinError::Index(
            "cannot assign vector with zero centroids".into(),
        ));
    }
    let expected = centroids[0].len();
    if values.len() != expected {
        return Err(ZeppelinError::DimensionMismatch {
            expected,
            actual: values.len(),
        });
    }
    let mut best_dist = f32::MAX;
    let mut best_cluster = 0usize;
    for (cluster_idx, centroid) in centroids.iter().enumerate() {
        if centroid.len() != expected {
            return Err(ZeppelinError::Index(format!(
                "centroid {cluster_idx} has dim {}, expected {expected}",
                centroid.len()
            )));
        }
        let dist = euclidean_distance(values, centroid);
        if dist < best_dist {
            best_dist = dist;
            best_cluster = cluster_idx;
        }
    }
    Ok(best_cluster)
}

/// Materializes a complete survivor set for a correctness-preserving full rebuild.
///
/// This helper is used after the membership-bounded incremental path fails. It
/// reloads the old segment, overlays already-merged WAL upserts and deletes, and
/// returns deterministic ID order for normal index construction.
///
/// # Parameters
///
/// - `store`: Object-store boundary used to load the prior segment.
/// - `namespace`: Logical target used for filtering metrics.
/// - `old_segment`: Manifest-resolved previous active descriptor, if present.
/// - `latest_vectors`: Owned latest WAL upserts; the helper extends this map.
/// - `deleted_ids`: IDs that must not be restored from the old segment.
///
/// # Returns
///
/// Owned finite survivors sorted lexicographically by vector ID.
///
/// # Errors
///
/// Propagates every required old segment read or decode failure. Partial reads
/// do not produce a shortened result.
///
/// # Side Effects
///
/// May GET every vector and attribute object in the old segment. Legacy
/// non-finite rows are loudly filtered through `keep_finite_compaction_vector`.
///
/// # Performance
///
/// Reads and materializes the complete old dataset, then sorts survivors in
/// `O(n log n)`. This intentionally sacrifices the bounded fast path to retain
/// correctness after an incremental artifact problem.
///
/// # Examples
///
/// If the old sketch is missing, a caller can pass two WAL updates and one
/// delete; this helper reloads all old rows, applies those changes, and supplies
/// a complete input for a new full IVF build.
async fn load_full_surviving_vectors_for_fallback(
    store: &ZeppelinStore,
    namespace: &str,
    old_segment: Option<LocatedSegmentRef<'_>>,
    mut latest_vectors: HashMap<String, VectorEntry>,
    deleted_ids: &HashSet<String>,
) -> Result<Vec<VectorEntry>> {
    if let Some(located) = old_segment {
        let (existing_vecs, _id_to_cluster) = load_segment_vectors(store, located).await?;
        for vector in existing_vecs {
            if !latest_vectors.contains_key(&vector.id) && !deleted_ids.contains(&vector.id) {
                latest_vectors.insert(vector.id.clone(), vector);
            }
        }
    }

    let mut vectors: Vec<VectorEntry> = latest_vectors
        .values()
        .filter(|vector| keep_finite_compaction_vector(namespace, vector))
        .cloned()
        .collect();
    vectors.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(vectors)
}

/// Loads and validates only old clusters marked for incremental rewrite.
///
/// ```text
/// touched bitset
///      |
///      +-- legacy layout -> vector GET || attrs GET per touched cluster
///      |
///      `-- grouped layout -> GET each intersecting object
///                              + attrs GET per touched section
///                                      |
///                                      v
///                          aligned Vec<VectorEntry> per cluster
/// ```
///
/// # Parameters
///
/// - `store`: Object-store boundary for immutable GETs.
/// - `old_segment`: Located prior segment supplying the logical metric scope and
///   physical namespace for computed keys.
/// - `cluster_owners`: Resolved per-cluster physical segment owners.
/// - `cluster_objects`: Explicit grouped-object layout, or empty for legacy
///   one-object-per-cluster data.
/// - `touched`: Rewrite selector indexed by logical cluster.
///
/// # Returns
///
/// One vector list per logical cluster. Untouched entries remain empty; touched
/// entries contain finite owned rows in stored order.
///
/// # Errors
///
/// Propagates missing/corrupt vector or attribute data, rejects grouped objects
/// that lack advertised sections or reference out-of-range clusters, and rejects
/// attribute arrays shorter than their cluster row count.
///
/// # Side Effects
///
/// Performs parallel object-store reads and updates compaction read metrics.
/// Legacy non-finite rows are logged and omitted.
///
/// # Consistency
///
/// Owner overrides and explicit object refs come from the manifest snapshot;
/// prefix inference is used only for the legacy layout. Attributes are required
/// because silently substituting `None` would change metadata filtering.
///
/// # Performance
///
/// Reads only objects intersecting `touched`. Legacy vector and attribute GETs
/// for one cluster run concurrently; grouped vector objects are fetched once
/// even when they contain multiple touched sections.
///
/// # Examples
///
/// For 100 clusters with bits 2 and 80 set, the legacy layout issues four GETs
/// instead of 200. A grouped object containing both clusters is fetched once,
/// followed by their two attribute GETs.
async fn load_touched_segment_vectors(
    store: &ZeppelinStore,
    old_segment: LocatedSegmentRef<'_>,
    cluster_owners: &[String],
    cluster_objects: &[ClusterDataObjectRef],
    touched: &[bool],
) -> Result<Vec<Vec<VectorEntry>>> {
    let namespace = old_segment.logical_namespace;
    let physical_namespace = old_segment.physical_namespace();
    let segment_id = old_segment.segment.id.as_str();
    let owner = |i: usize| -> &str {
        cluster_owners
            .get(i)
            .map(String::as_str)
            .unwrap_or(segment_id)
    };
    let num_clusters = touched.len();
    let mut cluster_results = Vec::new();

    if cluster_objects.is_empty() {
        cluster_results =
            futures::future::join_all((0..num_clusters).filter(|&i| touched[i]).map(|i| {
                let cvec_key = cluster_key(physical_namespace, owner(i), i);
                let cattr_key = attrs_key(physical_namespace, owner(i), i);
                async move {
                    let (cluster_res, attrs_res) = tokio::join!(
                        get_compaction_read(
                            store,
                            namespace,
                            &cvec_key,
                            COMPACTION_READ_CLASS_CLUSTER,
                        ),
                        get_compaction_read(
                            store,
                            namespace,
                            &cattr_key,
                            COMPACTION_READ_CLASS_ATTRS,
                        ),
                    );
                    (i, cluster_res, attrs_res)
                }
            }))
            .await;
    } else {
        let object_results = futures::future::join_all(
            cluster_objects
                .iter()
                .filter(|object_ref| {
                    object_ref
                        .clusters
                        .iter()
                        .any(|&cluster_idx| touched.get(cluster_idx).copied().unwrap_or(false))
                })
                .map(|object_ref| async move {
                    let object_res = get_compaction_read(
                        store,
                        namespace,
                        &object_ref.key,
                        COMPACTION_READ_CLASS_CLUSTER,
                    )
                    .await;
                    (object_ref, object_res)
                }),
        )
        .await;

        for (object_ref, object_res) in object_results {
            let object_data = object_res?;
            let Some(sections) = cluster_object_sections(&object_data)? else {
                return Err(ZeppelinError::Index(format!(
                    "manifest cluster object {} did not contain grouped cluster data",
                    object_ref.key
                )));
            };
            for &cluster_idx in &object_ref.clusters {
                if cluster_idx >= num_clusters {
                    return Err(ZeppelinError::Index(format!(
                        "cluster object {} references out-of-range cluster {cluster_idx}",
                        object_ref.key
                    )));
                }
                if !touched[cluster_idx] {
                    continue;
                }
                let section = sections
                    .iter()
                    .find(|section| section.cluster_idx == cluster_idx)
                    .ok_or_else(|| {
                        ZeppelinError::Index(format!(
                            "cluster object {} missing cluster {cluster_idx}",
                            object_ref.key
                        ))
                    })?;
                let cattr_key = attrs_key(physical_namespace, owner(cluster_idx), cluster_idx);
                let attrs_res =
                    get_compaction_read(store, namespace, &cattr_key, COMPACTION_READ_CLASS_ATTRS)
                        .await;
                cluster_results.push((
                    cluster_idx,
                    Ok(bytes::Bytes::copy_from_slice(section.data)),
                    attrs_res,
                ));
            }
        }
    }

    let mut clusters = vec![Vec::new(); num_clusters];
    for (cluster_idx, cluster_res, attrs_res) in cluster_results {
        let cluster = deserialize_cluster(&cluster_res?)?;
        let attrs = deserialize_attrs(&attrs_res?)?;
        if attrs.len() < cluster.ids.len() {
            return Err(ZeppelinError::Index(format!(
                "attrs length {} shorter than cluster {cluster_idx} vector count {}",
                attrs.len(),
                cluster.ids.len()
            )));
        }
        for (row_idx, id) in cluster.ids.into_iter().enumerate() {
            let entry = VectorEntry {
                id,
                values: cluster.vectors[row_idx].clone(),
                attributes: attrs.get(row_idx).cloned().flatten(),
            };
            if keep_finite_compaction_vector(namespace, &entry) {
                clusters[cluster_idx].push(entry);
            }
        }
    }
    Ok(clusters)
}

/// Builds exact grouped-object references for a mixed carried/rewritten segment.
///
/// # Parameters
///
/// - `namespace`: Namespace used to derive keys for rewritten singleton groups.
/// - `new_segment_id`: Physical owner of rewritten cluster objects.
/// - `num_clusters`: Expected logical cluster count.
/// - `touched`: Per-cluster rewrite selector of exactly `num_clusters` entries.
/// - `old_cluster_objects`: Complete old grouped layout.
/// - `new_object_sizes`: Serialized size by rewritten object key.
///
/// # Returns
///
/// Deterministically key-ordered descriptors. Carried clusters sharing an old
/// object remain grouped under that key; each rewritten cluster points at its
/// newly serialized object. Returns an empty vector for a legacy old layout.
///
/// # Errors
///
/// Rejects touched-length mismatch, out-of-range or duplicate old cluster
/// ownership, missing old ownership for a carried cluster, and missing size
/// metadata for any emitted key.
///
/// # Consistency
///
/// The result is an exact manifest reachability closure. A carried key must not
/// later be inferred dead from its older segment prefix.
///
/// # Performance
///
/// Runs in linear time over old references plus clusters. [`BTreeMap`] produces
/// stable descriptor order independent of randomized hash iteration.
///
/// # Examples
///
/// If old object `group_A` owns clusters 0 and 1 and only cluster 1 is touched,
/// the result retains `group_A -> [0]` and adds the new singleton key for
/// cluster 1.
fn incremental_cluster_objects(
    namespace: &str,
    new_segment_id: &str,
    num_clusters: usize,
    touched: &[bool],
    old_cluster_objects: &[ClusterDataObjectRef],
    new_object_sizes: &HashMap<String, u64>,
) -> Result<Vec<ClusterDataObjectRef>> {
    if old_cluster_objects.is_empty() {
        return Ok(Vec::new());
    }
    if touched.len() != num_clusters {
        return Err(ZeppelinError::Index(format!(
            "touched length mismatch: expected {num_clusters}, got {}",
            touched.len()
        )));
    }

    let mut old_object_by_cluster: Vec<Option<&ClusterDataObjectRef>> = vec![None; num_clusters];
    for object_ref in old_cluster_objects {
        for &cluster_idx in &object_ref.clusters {
            if cluster_idx >= num_clusters {
                return Err(ZeppelinError::Index(format!(
                    "old cluster object {} references out-of-range cluster {cluster_idx}",
                    object_ref.key
                )));
            }
            if old_object_by_cluster[cluster_idx].is_some() {
                return Err(ZeppelinError::Index(format!(
                    "old cluster {cluster_idx} appears in multiple cluster objects"
                )));
            }
            old_object_by_cluster[cluster_idx] = Some(object_ref);
        }
    }

    let mut grouped: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut size_by_key: HashMap<String, u64> = HashMap::new();
    for cluster_idx in 0..num_clusters {
        let key = if touched[cluster_idx] {
            let key = cluster_group_key(namespace, new_segment_id, cluster_idx);
            let size = new_object_sizes.get(&key).copied().ok_or_else(|| {
                ZeppelinError::Index(format!("missing size for rewritten cluster object {key}"))
            })?;
            size_by_key.insert(key.clone(), size);
            key
        } else {
            let object_ref = old_object_by_cluster[cluster_idx].ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "grouped old segment missing object for carried cluster {cluster_idx}"
                ))
            })?;
            size_by_key.insert(object_ref.key.clone(), object_ref.size_bytes);
            object_ref.key.clone()
        };
        grouped.entry(key).or_default().push(cluster_idx);
    }

    let mut object_refs = Vec::with_capacity(grouped.len());
    for (key, clusters) in grouped {
        let size_bytes = size_by_key.remove(&key).ok_or_else(|| {
            ZeppelinError::Index(format!("missing size for cluster object {key}"))
        })?;
        object_refs.push(ClusterDataObjectRef {
            key,
            clusters,
            live_offset: 0,
            live_len: 0,
            size_bytes,
        });
    }
    Ok(object_refs)
}

/// Aborts publication after the lease heartbeat reports loss (invariant A2).
///
/// The lease-renewal heartbeat flips `lease_lost` to `true` when a renewal
/// fails because another node took the lease over. Called before every
/// manifest CAS attempt so a fenced-out compaction aborts with a clean
/// `LeaseExpired` error instead of racing its stale commit.
///
/// # Parameters
///
/// - `namespace`: Namespace included in the error and structured log.
/// - `lease_lost`: Optional shared atomic flag. `None` means the caller chose an
///   unfenced/no-heartbeat path; it does not mean a lease was checked and held.
///
/// # Returns
///
/// `Ok(())` while no supplied heartbeat has reported loss.
///
/// # Errors
///
/// Returns [`ZeppelinError::LeaseExpired`] when the flag is true. The candidate
/// artifacts may already be uploaded but no manifest CAS follows this check.
///
/// # Consistency
///
/// This is one layer, not the whole protocol. The caller must still compare the
/// fencing token in the fresh manifest and use ETag CAS because takeover can
/// race immediately after the atomic load.
///
/// # Examples
///
/// A renewal task detects that token 12 was taken over, stores `true`, and the
/// compactor returns `LeaseExpired` before publishing its candidate.
///
/// # Rust Notes for Java/C Engineers
///
/// [`AtomicBool::load`][std::sync::atomic::AtomicBool::load] with sequential
/// consistency provides one global ordering model for the heartbeat and
/// compactor. It resembles Java's `AtomicBoolean.get()`. In C, the corresponding
/// `_Atomic bool` load must use compatible memory ordering and independently
/// managed shared lifetime.
fn check_lease_lost(
    namespace: &str,
    lease_lost: Option<&std::sync::atomic::AtomicBool>,
) -> Result<()> {
    if let Some(flag) = lease_lost {
        if flag.load(std::sync::atomic::Ordering::SeqCst) {
            error!(
                namespace = namespace,
                "aborting compaction before manifest CAS: lease lost mid-compaction \
                 (renewal failed — another node holds the lease)"
            );
            return Err(ZeppelinError::LeaseExpired {
                namespace: namespace.to_string(),
            });
        }
    }
    Ok(())
}

/// Age of a fragment in seconds, derived from its ULID timestamp.
///
/// ULIDs encode their creation time in the top 48 bits (milliseconds since
/// the Unix epoch), so no extra timestamp field is needed on `FragmentRef`.
/// Clock skew can make a fragment's ULID timestamp sit slightly in the
/// future relative to this node; saturate to 0 rather than underflow.
///
/// # Parameters
///
/// - `id`: Fragment ULID whose embedded millisecond timestamp is inspected.
/// - `now_ms`: Current Unix time in milliseconds from the evaluating node.
///
/// # Returns
///
/// Whole elapsed seconds, truncating sub-second age. Future timestamps return
/// zero.
///
/// # Examples
///
/// A ULID created 90,500 ms ago reports 90 seconds; one timestamped five seconds
/// in the future reports zero rather than wrapping to a huge age.
fn fragment_age_secs(id: &Ulid, now_ms: u64) -> u64 {
    now_ms.saturating_sub(id.timestamp_ms()) / 1000
}

/// Merge this cycle's deferred-delete keys into the manifest.
///
/// The fresh manifest re-read inside the CAS loop may contain
/// `pending_deletes` entries added by a concurrent writer since our step-0
/// GC ran. Wholesale replacement would drop those keys and leak the objects.
/// Keep any entry we did not process in step 0, then append this cycle's
/// keys (deduplicated).
///
/// # Parameters
///
/// - `manifest`: Fresh CAS candidate whose deletion queue is updated in place.
/// - `deferred_deletes`: Retired WAL and segment keys discovered by this build.
/// - `processed_deletes`: Keys confirmed processed earlier in the cycle.
///
/// # Side Effects
///
/// Removes only explicitly processed keys, preserves concurrent entries, and
/// appends this cycle's keys once. It performs no physical object deletion.
///
/// # Consistency
///
/// The fresh manifest may contain queue entries absent from the original build
/// snapshot. Merging rather than replacing prevents those concurrent keys from
/// being forgotten and leaked.
///
/// # Performance
///
/// Builds a temporary hash set of existing borrowed strings and clones only new
/// keys. Expected time is linear in the three collections.
///
/// # Examples
///
/// If the fresh queue contains `concurrent` and this run proposes
/// `[concurrent, wal_A]`, the result keeps one `concurrent` followed by `wal_A`.
fn merge_pending_deletes(
    manifest: &mut Manifest,
    deferred_deletes: &[String],
    processed_deletes: &HashSet<String>,
) {
    manifest
        .pending_deletes
        .retain(|k| !processed_deletes.contains(k));
    let existing: HashSet<&String> = manifest.pending_deletes.iter().collect();
    let new_keys: Vec<String> = deferred_deletes
        .iter()
        .filter(|k| !existing.contains(k))
        .cloned()
        .collect();
    manifest.pending_deletes.extend(new_keys);
}

/// Publishes the current candidate artifact keys as a fenced GC staging root.
///
/// # Parameters
///
/// - `store`: Object-store boundary used to list and write staging metadata.
/// - `namespace`: Namespace containing the unpublished segment prefix.
/// - `segment_id`: Candidate segment whose already-uploaded keys are protected.
/// - `fencing_token`: Current lease generation used to name/validate staging.
///
/// # Returns
///
/// `Ok(())` after the exact listed key set is stored in the compaction staging
/// side object.
///
/// # Errors
///
/// Propagates prefix listing and staging serialization/write failures. The
/// candidate objects already uploaded remain invisible.
///
/// # Side Effects
///
/// Performs one namespace-prefix LIST and writes a GC staging object.
///
/// # Consistency
///
/// Staging is a temporary GC root, not the visibility commit. The function is
/// called after vector artifacts and again after optional FTS uploads so the
/// root set reflects each completed upload phase.
///
/// # Examples
///
/// A fenced build uploads centroids and four cluster objects, lists those five
/// keys, and stages them under its token before beginning manifest publication.
async fn publish_compaction_staging(
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
    fencing_token: u64,
) -> Result<()> {
    let prefix = format!("{namespace}/segments/{segment_id}/");
    let keys = store.list_prefix(&prefix).await?.into_iter().collect();
    gc::write_compaction_staging(store, namespace, fencing_token, keys).await
}

/// Best-effort clears a compaction staging root after success or safe abort.
///
/// # Parameters
///
/// - `store`: Object-store boundary containing the side object.
/// - `namespace`: Namespace whose staging record should be cleared.
/// - `fencing_token`: Token identifying this compaction's staging generation.
///
/// # Side Effects
///
/// Attempts a staging delete. Failure is warned and deliberately swallowed;
/// leaving extra GC protection leaks temporary space but cannot hide a committed
/// error or delete live data.
///
/// # Examples
///
/// After CAS success, clearing the token-7 staging record lets normal exact-key
/// reachability govern the now-manifest-visible segment. A failed clear remains
/// observable in logs and expires through the staging protocol.
async fn drop_compaction_staging(store: &ZeppelinStore, namespace: &str, fencing_token: u64) {
    if let Err(e) = gc::clear_compaction_staging(store, namespace, fencing_token).await {
        warn!(
            namespace = %namespace,
            fencing_token,
            error = %e,
            "failed to clear compaction staging side object"
        );
    }
}

/// Rejects a candidate whose unpublished upload phase exceeded its GC window.
///
/// # Parameters
///
/// - `namespace`: Namespace included in diagnostics.
/// - `upload_phase_start`: Monotonic instant captured when the segment ID and
///   upload phase began.
/// - `upload_window`: Maximum permitted unpublished duration from GC config.
///
/// # Returns
///
/// `Ok(())` when elapsed time is at most the window.
///
/// # Errors
///
/// Returns an index error after the window is exceeded. Candidate artifacts are
/// left unpublished; the caller clears fenced staging best-effort before
/// returning.
///
/// # Consistency
///
/// This guard prevents publication after objects could have crossed the safety
/// horizon assumed by the GC/staging design. It uses a monotonic
/// [`std::time::Instant`] rather than wall time and therefore is immune to clock
/// adjustments.
///
/// # Examples
///
/// A 45-second upload with a 42-second window logs both durations and aborts
/// before CAS. An exactly 42-second upload remains permitted.
fn check_upload_window(
    namespace: &str,
    upload_phase_start: std::time::Instant,
    upload_window: Duration,
) -> Result<()> {
    let elapsed = upload_phase_start.elapsed();
    if elapsed > upload_window {
        error!(
            namespace = %namespace,
            elapsed_ms = elapsed.as_millis() as u64,
            upload_window_ms = upload_window.as_millis() as u64,
            "aborting compaction before manifest CAS: upload phase exceeded GC horizon window"
        );
        return Err(ZeppelinError::Index(format!(
            "compaction upload phase exceeded GC horizon window for namespace {namespace}: \
             elapsed_ms={}, window_ms={}",
            elapsed.as_millis(),
            upload_window.as_millis()
        )));
    }
    Ok(())
}

/// Load all vectors from an existing IVF-Flat segment on S3.
///
/// Fetches all clusters in parallel (2 S3 GETs per cluster) for ~15%
/// compaction speedup vs sequential loading.
///
/// Returns the flattened vectors AND a map from vector ID to its cluster
/// index in this segment. The map drives incremental compaction's
/// touched-cluster detection (a WAL delete/update of an old member must
/// mark that member's cluster for rewrite) — it includes IDs that a later
/// merge step will drop, since the segment on S3 still contains them. The map
/// is populated for either layout, although current incremental carry-over uses
/// it only for IVF-Flat segments.
///
/// ```text
/// SegmentRef layout
///      |
///      +-- hierarchical -> load tree metadata for leaf count
///      `-- IVF-Flat ----> read segment-global centroids for cluster count
///                             |
///              +--------------+----------------+
///              |                               |
///       legacy cluster keys              explicit grouped objects
///       vector GET || attrs GET          object GET + attrs GETs
///              |                               |
///              +--------------+----------------+
///                             v
///              flatten aligned rows + old membership map
/// ```
///
/// # Parameters
///
/// - `store`: Object-store abstraction for all immutable artifact reads.
/// - `located`: Manifest-resolved descriptor supplying the logical metric scope,
///   physical namespace, layout, owner overrides, and exact grouped refs.
///
/// # Returns
///
/// All owned vector entries in cluster traversal order plus old ID-to-cluster
/// membership. Current callers ignore the membership for hierarchical input
/// because hierarchical segments are never incrementally carried.
///
/// # Errors
///
/// Propagates missing/corrupt tree metadata, centroids, cluster data, or invalid
/// attribute JSON. Rejects explicit grouped objects that do not decode or omit
/// an advertised section.
///
/// TODO(doc): Verify whether a decoded attribute array shorter than the cluster
/// row count should be rejected here, as `load_touched_segment_vectors` does.
/// The current full-load path uses `attrs.get(row).flatten()`, so a missing
/// trailing attribute row currently becomes `None` rather than an error.
///
/// # Side Effects
///
/// Performs object-store GETs and records compaction read metrics. No cache or
/// manifest state is mutated.
///
/// # Consistency
///
/// Global metadata always lives beneath `segment_id`; per-cluster keys resolve
/// through owner overrides or exact object refs. The function reads one immutable
/// segment closure from the caller's manifest snapshot and does not discover
/// objects by treating a prefix listing as authority. Invalid attribute JSON
/// fails, while the shorter-array behavior noted above currently synthesizes
/// absent trailing attributes.
///
/// # Performance
///
/// Loads every cluster into memory. Legacy vector and attribute pairs run in
/// parallel across clusters; grouped vector objects are each fetched once, then
/// decoded sequentially. Cost is proportional to the entire old segment.
///
/// # Examples
///
/// A flat segment with clusters 0 and 2 carried from older owners reads its own
/// centroids but resolves those cluster payloads under the owner IDs. The
/// returned membership still maps each ID to its logical cluster number.
///
/// # Rust Notes for Java/C Engineers
///
/// The `owner` closure returns a borrowed `&str` from either the owner slice or
/// `segment_id`; Rust infers that both sources live long enough for each key
/// construction. `tokio::join!` polls vector and attribute reads concurrently
/// without spawning detached tasks, while `join_all` scales that pattern across
/// clusters.
async fn load_segment_vectors(
    store: &ZeppelinStore,
    located: LocatedSegmentRef<'_>,
) -> Result<(Vec<VectorEntry>, HashMap<String, usize>)> {
    let namespace = located.logical_namespace;
    let physical_namespace = located.physical_namespace();
    let segment_id = located.segment.id.as_str();
    let cluster_owners = located.segment.cluster_owners.as_slice();
    let cluster_objects = located.segment.cluster_objects.as_slice();
    // Resolve cluster `i`'s owning segment ID (carried-over clusters live
    // under an older segment's keys; empty map ⇒ this segment owns all).
    let owner = |i: usize| -> &str {
        cluster_owners
            .get(i)
            .map(String::as_str)
            .unwrap_or(segment_id)
    };
    // Determine cluster count: hierarchical segments store it in tree_meta.json,
    // IVF-Flat segments store it in centroids.bin.
    //
    // For IVF-Flat we read the centroids blob DIRECTLY rather than via
    // `IvfFlatIndex::load` — the probing loader issues a per-cluster GET under
    // `segment_id` to sum vector counts, which would 404 on a segment whose
    // clusters were carried over to other keys. Centroids are segment-global
    // and always live under `segment_id`.
    let num_clusters = if located.segment.hierarchical {
        // Compaction reads the segment once; no query cache involved here.
        let h_index = HierarchicalIndex::load_from_located_manifest(store, located, None).await?;
        h_index.num_leaf_clusters()
    } else {
        use crate::index::ivf_flat::build::{centroids_key, deserialize_centroids};
        let centroids_data = get_compaction_read(
            store,
            namespace,
            &centroids_key(physical_namespace, segment_id),
            COMPACTION_READ_CLASS_CENTROIDS,
        )
        .await?;
        let (centroids, _dim) = deserialize_centroids(&centroids_data)?;
        centroids.len()
    };

    let mut cluster_results = Vec::new();
    if cluster_objects.is_empty() {
        // Parallel fetch: 2 GETs per cluster via tokio::join!
        cluster_results =
            futures::future::join_all((0..num_clusters).map(|i| {
                let cvec_key = cluster_key(physical_namespace, owner(i), i);
                let cattr_key = attrs_key(physical_namespace, owner(i), i);
                async move {
                    let (cluster_res, attrs_res) = tokio::join!(
                        get_compaction_read(
                            store,
                            namespace,
                            &cvec_key,
                            COMPACTION_READ_CLASS_CLUSTER,
                        ),
                        get_compaction_read(
                            store,
                            namespace,
                            &cattr_key,
                            COMPACTION_READ_CLASS_ATTRS,
                        ),
                    );
                    (i, cluster_res, attrs_res)
                }
            }))
            .await;
    } else {
        let object_results =
            futures::future::join_all(cluster_objects.iter().map(|object_ref| async move {
                let object_res = get_compaction_read(
                    store,
                    namespace,
                    &object_ref.key,
                    COMPACTION_READ_CLASS_CLUSTER,
                )
                .await;
                (object_ref, object_res)
            }))
            .await;

        for (object_ref, object_res) in object_results {
            let object_data = object_res?;
            let Some(sections) = cluster_object_sections(&object_data)? else {
                return Err(ZeppelinError::Index(format!(
                    "manifest cluster object {} did not contain grouped cluster data",
                    object_ref.key
                )));
            };
            for &cluster_idx in &object_ref.clusters {
                let section = sections
                    .iter()
                    .find(|section| section.cluster_idx == cluster_idx)
                    .ok_or_else(|| {
                        ZeppelinError::Index(format!(
                            "cluster object {} missing cluster {cluster_idx}",
                            object_ref.key
                        ))
                    })?;
                let cattr_key = attrs_key(physical_namespace, owner(cluster_idx), cluster_idx);
                let attrs_res =
                    get_compaction_read(store, namespace, &cattr_key, COMPACTION_READ_CLASS_ATTRS)
                        .await;
                cluster_results.push((
                    cluster_idx,
                    Ok(bytes::Bytes::copy_from_slice(section.data)),
                    attrs_res,
                ));
            }
        }
    }

    // Sequential deserialization (CPU-bound, no I/O)
    //
    // Attrs errors must propagate: every segment build writes an attrs blob
    // per cluster, so a failed read is transient storage trouble, not a
    // missing artifact. Substituting None here would commit a segment with
    // the cluster's attributes stripped — filtered queries would then
    // permanently exclude those vectors.
    let mut vectors = Vec::new();
    let mut id_to_cluster: HashMap<String, usize> = HashMap::new();
    for (i, cluster_res, attrs_res) in cluster_results {
        let cluster = deserialize_cluster(&cluster_res?)?;
        let attrs = deserialize_attrs(&attrs_res?)?;

        for (j, id) in cluster.ids.into_iter().enumerate() {
            id_to_cluster.insert(id.clone(), i);
            vectors.push(VectorEntry {
                id,
                values: cluster.vectors[j].clone(),
                attributes: attrs.get(j).cloned().flatten(),
            });
        }
    }

    debug!(
        segment_id = segment_id,
        vectors_loaded = vectors.len(),
        "loaded vectors from existing segment"
    );

    Ok((vectors, id_to_cluster))
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Focused unit coverage for trigger arithmetic and manifest-queue merging.
    //!
    //! These tests use the in-memory object-store backend because they exercise
    //! local trigger and candidate-manifest logic, not S3 semantics. Broader
    //! integration suites cover full compaction, incremental ownership,
    //! lease/fencing races, FTS, grouped objects, and storage GC against the
    //! repository's storage harness.
    //!
    //! ## Reading map
    //!
    //! 1. The `mem_compactor` helpers construct isolated coordinators.
    //! 2. The `should_compact` tests prove the independent count, age, and byte
    //!    triggers plus the idle case.
    //! 3. `test_fragment_age_from_ulid_timestamp` protects skew-safe arithmetic.
    //! 4. `test_merge_pending_deletes_keeps_concurrent_keys` protects a CAS
    //!    retry from dropping deletion work added by another writer.

    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Arc;

    use chrono::{DateTime, Utc};

    use super::*;
    use crate::config::{Config, GcConfig, SecurityMode};
    use crate::namespace::branching::{ArtifactOrigin, ArtifactOriginIndex};
    use crate::namespace::manager::{CompactionHealth, NamespaceIndexConfig};
    use crate::namespace::{NamespaceId, NamespaceIncarnationId};
    use crate::time::TimeSource;
    use crate::types::{DistanceMetric, IndexType};
    use crate::wal::manifest::FragmentRef;

    #[derive(Debug)]
    struct AdjustableTimeSource {
        now_ms: AtomicI64,
    }

    impl AdjustableTimeSource {
        fn new(now: DateTime<Utc>) -> Self {
            Self {
                now_ms: AtomicI64::new(now.timestamp_millis()),
            }
        }

        fn jump(&self, delta: chrono::Duration) {
            self.now_ms
                .fetch_add(delta.num_milliseconds(), Ordering::SeqCst);
        }
    }

    impl TimeSource for AdjustableTimeSource {
        fn now(&self) -> DateTime<Utc> {
            DateTime::from_timestamp_millis(self.now_ms.load(Ordering::SeqCst))
                .expect("adjustable compactor-test timestamp must be representable")
        }
    }

    /// Creates an isolated in-memory compactor with the default GC upload window.
    ///
    /// # Parameters
    ///
    /// - `config`: Trigger policy under test.
    ///
    /// # Returns
    ///
    /// A compactor and WAL reader sharing one new in-memory object store.
    fn mem_compactor(config: CompactionConfig) -> Compactor {
        let mem = std::sync::Arc::new(object_store::memory::InMemory::new());
        let store = ZeppelinStore::new(mem);
        let wal_reader = WalReader::new(store.clone());
        Compactor::new(
            store,
            wal_reader,
            config,
            IndexingConfig::default(),
            Duration::from_secs(GcConfig::default().compaction_upload_window_secs),
        )
    }

    /// Creates an isolated compactor driven by an explicitly injected clock.
    fn mem_compactor_with_clock(config: CompactionConfig, clock: Clock) -> Compactor {
        let mem = Arc::new(object_store::memory::InMemory::new());
        let store = ZeppelinStore::new(mem);
        let wal_reader = WalReader::new(store.clone());
        Compactor::with_clock(
            store,
            wal_reader,
            config,
            IndexingConfig::default(),
            Duration::from_secs(GcConfig::default().compaction_upload_window_secs),
            clock,
        )
    }

    /// Scale-aware flat segments match only their row-derived centroid count.
    #[test]
    fn segment_layout_match_uses_scale_aware_centroid_count() {
        let config = IndexingConfig::default();
        let mut segment = SegmentRef {
            id: "seg_scale_aware".to_string(),
            vector_count: 1_000_000,
            cluster_count: 334,
            quantization: config.quantization,
            hierarchical: false,
            bitmap_fields: Vec::new(),
            fts_fields: Vec::new(),
            has_global_fts: false,
            cluster_owners: Vec::new(),
            sketch: None,
            cluster_objects: Vec::new(),
            bootstrap: None,
            membership: None,
            artifact_origin: None,
        };

        assert!(segment_matches_index_config(&segment, &config));
        segment.cluster_count = 256;
        assert!(!segment_matches_index_config(&segment, &config));
    }

    /// Returns current Unix wall time in milliseconds for ULID test fixtures.
    ///
    /// # Returns
    ///
    /// Milliseconds since the Unix epoch; tests panic if the host clock predates
    /// the epoch because such a host cannot construct meaningful fixture ages.
    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    /// Builds a one-vector fragment descriptor for trigger tests.
    ///
    /// # Parameters
    ///
    /// - `id`: ULID carrying the desired test timestamp.
    /// - `size_bytes`: Recorded serialized size used by the byte trigger.
    ///
    /// # Returns
    ///
    /// A descriptor with one vector, no deletes, and sequence number zero.
    fn fragment_ref(id: Ulid, size_bytes: u64) -> FragmentRef {
        FragmentRef {
            id,
            vector_count: 1,
            delete_count: 0,
            sequence_number: 0,
            size_bytes,
            artifact_origin: None,
        }
    }

    /// Proves validated GC timing is threaded into the compactor unchanged.
    ///
    /// This catches configuration drift where horizon validation and the final
    /// upload-window publication guard might use different values.
    #[test]
    fn gc_upload_window_drives_horizon_floor_and_compactor_abort_window() {
        let mut config = Config::default();
        config.security.mode = SecurityMode::OpenUnsafe;
        config.cache.manifest_cache_ttl_ms = 2_500;
        config.server.request_timeout_secs = 30;
        config.gc.compaction_upload_window_secs = 42;
        config.gc.skew_slop_secs = 3;
        config.gc.horizon_secs = 78;

        config.validate().unwrap();
        assert_eq!(config.gc_horizon_floor_secs(), Some(78));

        let compactor = mem_compactor_with_upload_window(
            config.compaction.clone(),
            Duration::from_secs(config.gc.compaction_upload_window_secs),
        );
        assert_eq!(
            compactor.compaction_upload_window(),
            Duration::from_secs(42)
        );
    }

    /// Creates an isolated compactor with an explicit publication time window.
    ///
    /// # Parameters
    ///
    /// - `config`: Compaction trigger and retention settings.
    /// - `upload_window`: GC-derived duration to retain in the compactor.
    ///
    /// # Returns
    ///
    /// A compactor backed by a new in-memory object store.
    fn mem_compactor_with_upload_window(
        config: CompactionConfig,
        upload_window: Duration,
    ) -> Compactor {
        let mem = std::sync::Arc::new(object_store::memory::InMemory::new());
        let store = ZeppelinStore::new(mem);
        let wal_reader = WalReader::new(store.clone());
        Compactor::new(
            store,
            wal_reader,
            config,
            IndexingConfig::default(),
            upload_window,
        )
    }

    /// Builds a test manifest containing the supplied fragment descriptors.
    ///
    /// # Parameters
    ///
    /// - `fragments`: Visible descriptors added in the provided order.
    ///
    /// # Returns
    ///
    /// An owned in-memory snapshot suitable for the pure trigger seam.
    fn manifest_with_fragments(fragments: Vec<FragmentRef>) -> Manifest {
        let mut manifest = Manifest::new();
        for f in fragments {
            manifest.add_fragment(f);
        }
        manifest
    }

    fn active_metadata(namespace: &str) -> NamespaceMetadata {
        let now = Utc::now();
        NamespaceMetadata {
            name: namespace.to_string(),
            dimensions: 4,
            distance_metric: DistanceMetric::Euclidean,
            index_type: IndexType::IvfFlat,
            vector_count: 0,
            created_at: now,
            updated_at: now,
            state: NamespaceState::Active,
            destruction_record_key: None,
            full_text_search: HashMap::new(),
            index_config: None,
            compaction_health: CompactionHealth::default(),
            incarnation_id: None,
        }
    }

    fn segment_for_config(id: &str, vector_count: usize, config: &IndexingConfig) -> SegmentRef {
        SegmentRef {
            id: id.to_string(),
            vector_count,
            cluster_count: config.effective_num_centroids(vector_count),
            quantization: config.quantization,
            hierarchical: config.hierarchical,
            bitmap_fields: Vec::new(),
            fts_fields: Vec::new(),
            has_global_fts: false,
            cluster_owners: Vec::new(),
            sketch: None,
            cluster_objects: Vec::new(),
            bootstrap: None,
            membership: None,
            artifact_origin: None,
        }
    }

    fn artifact_origin(namespace: &str, incarnation: u128) -> ArtifactOrigin {
        ArtifactOrigin {
            namespace: NamespaceId::parse(namespace).expect("test namespace must be valid"),
            incarnation: NamespaceIncarnationId::from_uuid(uuid::Uuid::from_u128(incarnation)),
        }
    }

    fn manifest_with_foreign_active_segment() -> (Manifest, ArtifactOrigin) {
        let target_origin = artifact_origin("branch-target", 1);
        let source_origin = artifact_origin("branch-source", 2);
        let mut manifest = Manifest::new();
        manifest
            .bind_namespace_incarnation(target_origin.incarnation.as_uuid())
            .expect("target fixture must bind its incarnation");
        manifest.artifact_origins = vec![source_origin];
        let mut segment = segment_for_config("seg_foreign", 1, &IndexingConfig::default());
        segment.cluster_count = 1;
        segment.artifact_origin = Some(ArtifactOriginIndex::new(0));
        manifest.add_segment(segment);
        (manifest, target_origin)
    }

    #[tokio::test]
    async fn foreign_old_segment_reads_source_physical_keys() {
        use crate::index::ivf_flat::build::{
            attrs_key, centroids_key, cluster_key, serialize_attrs, serialize_centroids,
            serialize_cluster,
        };

        let mem = Arc::new(object_store::memory::InMemory::new());
        let store = ZeppelinStore::new(mem);
        let source_namespace = "branch-source";
        let segment_id = "seg_foreign";
        store
            .put(
                &centroids_key(source_namespace, segment_id),
                serialize_centroids(&[vec![0.0, 0.0]], 2).unwrap(),
            )
            .await
            .unwrap();
        store
            .put(
                &cluster_key(source_namespace, segment_id, 0),
                serialize_cluster(&["source-row".to_string()], &[vec![1.0, 2.0]], 2).unwrap(),
            )
            .await
            .unwrap();
        store
            .put(
                &attrs_key(source_namespace, segment_id, 0),
                serialize_attrs(&[None]).unwrap(),
            )
            .await
            .unwrap();

        let (manifest, target_origin) = manifest_with_foreign_active_segment();
        let located = manifest
            .artifact_origin_resolver(&target_origin)
            .unwrap()
            .active_located_segment()
            .unwrap()
            .expect("fixture has one active segment");

        let (vectors, membership) = load_segment_vectors(&store, located).await.unwrap();
        assert_eq!(vectors.len(), 1);
        assert_eq!(vectors[0].id, "source-row");
        assert_eq!(membership.get("source-row"), Some(&0));
    }

    #[test]
    fn foreign_old_artifacts_never_become_deferred_deletion_candidates() {
        let (mut manifest, target_origin) = manifest_with_foreign_active_segment();
        manifest.add_fragment(FragmentRef {
            id: Ulid::from_parts(1, 1),
            vector_count: 0,
            delete_count: 1,
            sequence_number: 0,
            size_bytes: 1,
            artifact_origin: Some(ArtifactOriginIndex::new(0)),
        });
        let resolver = manifest.artifact_origin_resolver(&target_origin).unwrap();
        let located = resolver
            .active_located_segment()
            .unwrap()
            .expect("fixture has one active segment");
        let located_fragment = resolver.locate_fragment(&manifest.fragments[0]).unwrap();

        assert_eq!(target_owned_old_segment_prefix(located), None);
        assert_eq!(target_owned_fragment_deletion_key(located_fragment), None);
    }

    /// Trigger evaluation consumes caller-supplied snapshots and therefore
    /// performs no storage reads of its own.
    #[test]
    fn should_compact_uses_supplied_snapshots_without_store_io() {
        let compactor = mem_compactor(CompactionConfig::default());
        let manifest = Manifest::new();
        let metadata = active_metadata("ns-pure-trigger");

        assert!(!compactor
            .should_compact("ns-pure-trigger", &manifest, &metadata)
            .unwrap());
    }

    #[test]
    fn idle_trigger_preserves_requested_namespace_in_lifecycle_errors() {
        let compactor = mem_compactor(CompactionConfig::default());
        let manifest = Manifest::new();
        let mut metadata = active_metadata("embedded-name");
        metadata.state = NamespaceState::Deleting;

        let error = compactor
            .should_compact("requested-name", &manifest, &metadata)
            .unwrap_err();
        assert!(matches!(
            error,
            ZeppelinError::NamespaceDeleting { namespace }
                if namespace == "requested-name"
        ));
    }

    #[test]
    fn idle_trigger_rejects_creating_metadata_as_manifest_conflict() {
        let compactor = mem_compactor(CompactionConfig::default());
        let manifest = Manifest::new();
        let mut metadata = active_metadata("embedded-name");
        metadata.state = NamespaceState::Creating;

        let error = compactor
            .should_compact("requested-name", &manifest, &metadata)
            .unwrap_err();
        assert!(matches!(
            error,
            ZeppelinError::ManifestConflict { namespace }
                if namespace == "requested-name"
        ));
    }

    #[test]
    fn idle_trigger_without_overlay_uses_process_defaults() {
        let defaults = IndexingConfig::default();
        let compactor = mem_compactor(CompactionConfig::default());
        let mut manifest = Manifest::new();
        manifest.add_segment(segment_for_config("seg-defaults", 10, &defaults));
        let metadata = active_metadata("ns-defaults");

        assert!(!compactor
            .should_compact("ns-defaults", &manifest, &metadata)
            .unwrap());
    }

    #[test]
    fn idle_trigger_applies_valid_namespace_overlay() {
        let defaults = IndexingConfig::default();
        let compactor = mem_compactor(CompactionConfig::default());
        let mut manifest = Manifest::new();
        manifest.add_segment(segment_for_config("seg-overlay", 10, &defaults));
        let mut metadata = active_metadata("ns-overlay");
        let mut override_config = NamespaceIndexConfig::from_indexing_config(&defaults);
        override_config.nlist = 2;
        metadata.index_config = Some(override_config);

        assert!(compactor
            .should_compact("ns-overlay", &manifest, &metadata)
            .unwrap());
    }

    #[test]
    fn idle_trigger_rejects_invalid_namespace_overlay() {
        let defaults = IndexingConfig::default();
        let compactor = mem_compactor(CompactionConfig::default());
        let manifest = Manifest::new();
        let mut metadata = active_metadata("ns-invalid-overlay");
        let mut override_config = NamespaceIndexConfig::from_indexing_config(&defaults);
        override_config.quantization = crate::index::quantization::QuantizationType::Product;
        override_config.pq_m = 3;
        metadata.index_config = Some(override_config);

        let error = compactor
            .should_compact("ns-invalid-overlay", &manifest, &metadata)
            .unwrap_err();
        assert!(matches!(error, ZeppelinError::Validation(_)));
    }

    #[test]
    fn nonempty_trigger_does_not_resolve_advisory_metadata() {
        let compactor = mem_compactor(CompactionConfig {
            max_wal_fragments_before_compact: 1,
            ..Default::default()
        });
        let manifest =
            manifest_with_fragments(vec![fragment_ref(Ulid::from_parts(now_ms(), 1), 1)]);
        let mut metadata = active_metadata("ns-nonempty");
        metadata.state = NamespaceState::Deleting;

        assert!(compactor
            .should_compact("ns-nonempty", &manifest, &metadata)
            .unwrap());
    }

    /// I1: a single old fragment must trigger compaction via the age
    /// trigger even when the count threshold is far away.
    #[test]
    fn test_should_compact_age_exceeded_single_fragment() {
        let compactor = mem_compactor(CompactionConfig {
            max_wal_fragments_before_compact: 1000,
            max_wal_age_before_compact_secs: 60, // 1 minute
            max_wal_bytes_before_compact: u64::MAX,
            ..Default::default()
        });
        // Fragment written 1 hour ago (ULID encodes the timestamp).
        let old_id = Ulid::from_parts(now_ms() - 3_600_000, 42);
        let manifest = manifest_with_fragments(vec![fragment_ref(old_id, 100)]);
        let metadata = active_metadata("ns-age");

        assert!(
            compactor
                .should_compact("ns-age", &manifest, &metadata)
                .unwrap(),
            "1 fragment older than max_wal_age_before_compact_secs must trigger compaction"
        );
    }

    /// Advancing only the injected clock must make a fresh fragment age into
    /// compaction eligibility without changing count or byte thresholds.
    #[test]
    fn test_should_compact_age_uses_injected_clock() {
        let source = Arc::new(AdjustableTimeSource::new(Utc::now()));
        let compactor = mem_compactor_with_clock(
            CompactionConfig {
                max_wal_fragments_before_compact: 1000,
                max_wal_age_before_compact_secs: 300,
                max_wal_bytes_before_compact: u64::MAX,
                ..Default::default()
            },
            Clock::from_source(source.clone()),
        );
        let fragment_timestamp = u64::try_from(source.now().timestamp_millis())
            .expect("compactor-test clock must be after the Unix epoch");
        let fragment_id = Ulid::from_parts(fragment_timestamp, 42);
        let manifest = manifest_with_fragments(vec![fragment_ref(fragment_id, 100)]);
        let metadata = active_metadata("ns-injected-age");

        assert!(
            !compactor
                .should_compact("ns-injected-age", &manifest, &metadata)
                .unwrap(),
            "fragment at the injected current time must remain below all thresholds"
        );

        source.jump(chrono::Duration::seconds(301));

        assert!(
            compactor
                .should_compact("ns-injected-age", &manifest, &metadata)
                .unwrap(),
            "advancing the injected clock past the age threshold must trigger compaction"
        );
    }

    /// A pre-epoch injected clock is invalid configuration, not a young
    /// fragment; reject it instead of saturating or consulting host time.
    #[test]
    fn test_should_compact_rejects_pre_epoch_injected_clock() {
        let before_epoch = DateTime::from_timestamp_millis(-1)
            .expect("one millisecond before the Unix epoch must be representable");
        let compactor = mem_compactor_with_clock(
            CompactionConfig {
                max_wal_fragments_before_compact: 1000,
                max_wal_age_before_compact_secs: 300,
                max_wal_bytes_before_compact: u64::MAX,
                ..Default::default()
            },
            Clock::from_source(Arc::new(AdjustableTimeSource::new(before_epoch))),
        );
        let manifest = manifest_with_fragments(vec![fragment_ref(Ulid::from_parts(0, 42), 100)]);
        let metadata = active_metadata("ns-pre-epoch-age");

        let error = compactor
            .should_compact("ns-pre-epoch-age", &manifest, &metadata)
            .unwrap_err();
        assert!(
            matches!(error, ZeppelinError::Index(ref message) if message.contains("compactor clock before Unix epoch")),
            "pre-epoch injected time must fail loudly: {error:?}"
        );
    }

    /// A fresh fragment below all thresholds must NOT trigger.
    #[test]
    fn test_should_compact_fresh_fragment_below_thresholds() {
        let compactor = mem_compactor(CompactionConfig {
            max_wal_fragments_before_compact: 1000,
            max_wal_age_before_compact_secs: 3600,
            max_wal_bytes_before_compact: u64::MAX,
            ..Default::default()
        });
        let fresh_id = Ulid::from_parts(now_ms(), 42);
        let manifest = manifest_with_fragments(vec![fragment_ref(fresh_id, 100)]);
        let metadata = active_metadata("ns-fresh");

        assert!(
            !compactor
                .should_compact("ns-fresh", &manifest, &metadata)
                .unwrap(),
            "fresh fragment below all thresholds must not trigger compaction"
        );
    }

    /// I2: total uncompacted WAL bytes over the threshold must trigger,
    /// so few-but-huge fragments don't linger under the count trigger.
    #[test]
    fn test_should_compact_bytes_exceeded() {
        let compactor = mem_compactor(CompactionConfig {
            max_wal_fragments_before_compact: 1000,
            max_wal_age_before_compact_secs: u64::MAX / 1000,
            max_wal_bytes_before_compact: 64 * 1024 * 1024, // 64 MB
            ..Default::default()
        });
        let now = now_ms();
        let manifest = manifest_with_fragments(vec![
            fragment_ref(Ulid::from_parts(now, 1), 40 * 1024 * 1024),
            fragment_ref(Ulid::from_parts(now, 2), 40 * 1024 * 1024),
        ]);
        let metadata = active_metadata("ns-bytes");

        assert!(
            compactor
                .should_compact("ns-bytes", &manifest, &metadata)
                .unwrap(),
            "80MB of WAL over a 64MB threshold must trigger compaction"
        );
    }

    /// I4: zero uncompacted fragments and no active layout mismatch stays idle,
    /// no matter how aggressive the WAL thresholds are.
    #[test]
    fn test_should_compact_zero_fragments_never_triggers() {
        let compactor = mem_compactor(CompactionConfig {
            max_wal_fragments_before_compact: 0,
            max_wal_age_before_compact_secs: 0,
            max_wal_bytes_before_compact: 0,
            ..Default::default()
        });
        let manifest = manifest_with_fragments(vec![]);
        let metadata = active_metadata("ns-idle");

        assert!(
            !compactor
                .should_compact("ns-idle", &manifest, &metadata)
                .unwrap(),
            "an idle namespace (0 fragments) must never be compacted"
        );
    }

    /// I3: the count trigger keeps working (backward compat).
    #[test]
    fn test_should_compact_count_trigger_preserved() {
        let compactor = mem_compactor(CompactionConfig {
            max_wal_fragments_before_compact: 3,
            max_wal_age_before_compact_secs: u64::MAX / 1000,
            max_wal_bytes_before_compact: u64::MAX,
            ..Default::default()
        });
        let now = now_ms();
        let manifest = manifest_with_fragments(
            (0..3)
                .map(|i| fragment_ref(Ulid::from_parts(now, i as u128), 10))
                .collect(),
        );
        let metadata = active_metadata("ns-count");

        assert!(
            compactor
                .should_compact("ns-count", &manifest, &metadata)
                .unwrap(),
            "reaching the fragment-count threshold must trigger compaction"
        );
    }

    /// Fragment age is derived from the ULID's embedded millisecond
    /// timestamp — no separate timestamp field needed.
    #[test]
    fn test_fragment_age_from_ulid_timestamp() {
        let now = now_ms();
        // Created 90 seconds ago.
        let id = Ulid::from_parts(now - 90_000, 7);
        assert_eq!(fragment_age_secs(&id, now), 90);

        // Created "in the future" (clock skew): age saturates to 0.
        let future = Ulid::from_parts(now + 5_000, 7);
        assert_eq!(fragment_age_secs(&future, now), 0);

        // Same millisecond: age 0.
        let same = Ulid::from_parts(now, 7);
        assert_eq!(fragment_age_secs(&same, now), 0);
    }

    /// Proves the pending-delete merge preserves unprocessed concurrent work.
    ///
    /// The scenario removes a key processed earlier, retains a key appended by
    /// another writer, and deduplicates that key against this cycle's additions.
    #[test]
    fn test_merge_pending_deletes_keeps_concurrent_keys() {
        // Keys added to pending_deletes by another writer after our step-0
        // GC must survive the merge; keys we processed must be removed;
        // this cycle's keys are appended without duplicates.
        let mut manifest = Manifest::new();
        manifest.pending_deletes = vec![
            "ns/wal/old_a.wal".to_string(),      // processed in step 0
            "ns/wal/concurrent.wal".to_string(), // added by concurrent writer
        ];

        let processed: HashSet<String> = ["ns/wal/old_a.wal".to_string()].into_iter().collect();
        let deferred = vec![
            "ns/wal/new_1.wal".to_string(),
            "ns/wal/concurrent.wal".to_string(), // dedup against existing
        ];

        merge_pending_deletes(&mut manifest, &deferred, &processed);

        assert_eq!(
            manifest.pending_deletes,
            vec![
                "ns/wal/concurrent.wal".to_string(),
                "ns/wal/new_1.wal".to_string(),
            ]
        );
    }
}
