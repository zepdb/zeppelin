//! Authoritative namespace manifests, retained history, and point-in-time pins.
//!
//! This module defines the object-store metadata that tells Zeppelin which
//! immutable WAL fragments and indexed segments belong to a namespace. The live
//! [`Manifest`][crate::wal::manifest::Manifest] at
//! `<namespace>/manifest.json` is the visibility boundary: uploading a fragment
//! or segment object does not make that data queryable until a manifest that
//! references it is successfully published. Despite the historical `.json`
//! suffix, new live manifests use versioned MessagePack; readers also accept
//! legacy JSON.
//!
//! The namespace manager creates the first manifest. WAL writers, compaction,
//! and garbage collection load and publish later generations. Query planning
//! and the manifest cache consume the published view. All remote access goes
//! through [`ZeppelinStore`][crate::storage::ZeppelinStore]; this module neither
//! treats a process-local value as authoritative nor edits an artifact in place.
//!
//! ## Artifact visibility and commit order
//!
//! ```text
//! upload immutable WAL fragment or segment
//!                  |
//!                  | object exists, but readers cannot discover it
//!                  v
//! build next Manifest in memory
//!                  |
//!                  v
//! retain authoritative live generation N as immutable history N
//!                  |
//!                  v
//! publish candidate N+1 ------------ live PUT fails
//!                  |                       |
//!                  | success               v
//!                  v                 no speculative N+1 history exists;
//! retain CAS winner as history N+1     a divergent retry remains possible
//! ```
//!
//! The authoritative predecessor is written first so it is retained before a
//! successful replacement makes it historical. Competing writers with the same
//! live ETag create the same predecessor bytes; their divergent candidates are
//! never written to history before CAS. The CAS winner is snapshotted only
//! after its live publication succeeds. History is never overwritten, and a
//! failed live PUT cannot reserve or wedge the next generation. A writer that
//! observes a live generation whose history snapshot is missing repairs those
//! exact ETag-bound bytes before it may advance authority again.
//!
//! ## Compare-and-swap publication
//!
//! ```text
//! read_versioned() -> Manifest + ETag E1
//!                         |
//!                         | mutate owned Manifest
//!                         v
//!              write_conditional(..., E1)
//!                   /                 \
//!          E1 still current        ETag changed
//!                |                     |
//!                v                     v
//!       publish next generation   ManifestConflict;
//!                                reload, never overwrite
//! ```
//!
//! [`Manifest::write_conditional`][crate::wal::manifest::Manifest::write_conditional]
//! protects updates to an existing manifest.
//! [`Manifest::write`][crate::wal::manifest::Manifest::write] safely chooses
//! create-only publication for absent state or ETag CAS for existing state
//! without overwriting a concurrent writer. A missing ETag in
//! [`ManifestVersion`][crate::wal::manifest::ManifestVersion] is never treated
//! as permission to overwrite an existing namespace.
//!
//! ## Reading map
//!
//! 1. Start with [`Manifest`][crate::wal::manifest::Manifest] and its
//!    [`FragmentRef`][crate::wal::manifest::FragmentRef] and
//!    [`SegmentRef`][crate::wal::manifest::SegmentRef] fields to understand the
//!    authoritative data model.
//! 2. Read
//!    [`Manifest::add_fragment`][crate::wal::manifest::Manifest::add_fragment],
//!    [`Manifest::remove_compacted_fragments`][crate::wal::manifest::Manifest::remove_compacted_fragments],
//!    and
//!    [`Manifest::add_segment_with_limits`][crate::wal::manifest::Manifest::add_segment_with_limits]
//!    for in-memory state changes.
//! 3. Read [`Manifest::to_bytes`][crate::wal::manifest::Manifest::to_bytes] and
//!    [`Manifest::from_bytes`][crate::wal::manifest::Manifest::from_bytes] for
//!    the persisted compatibility contract.
//! 4. Follow
//!    [`Manifest::read_versioned`][crate::wal::manifest::Manifest::read_versioned]
//!    and
//!    [`Manifest::write_conditional`][crate::wal::manifest::Manifest::write_conditional]
//!    for normal optimistic publication.
//! 5. Follow
//!    [`Manifest::list_history`][crate::wal::manifest::Manifest::list_history],
//!    [`Manifest::read_history`][crate::wal::manifest::Manifest::read_history],
//!    and
//!    [`Manifest::prune_history_with_retention`][crate::wal::manifest::Manifest::prune_history_with_retention]
//!    for point-in-time recovery.
//! 6. Finish with [`NamedSnapshot`][crate::wal::manifest::NamedSnapshot] to see
//!    how users pin retained generations.
//!
//! ## Invariants
//!
//! - The live object-store manifest, never a local copy, defines visibility.
//! - Existing manifests are updated with the ETag CAS implemented here.
//!   Lease-aware WAL and compaction callers must also validate fencing; this
//!   module does not turn an ETag into proof of lease ownership.
//! - Compaction removes the exact fragment-ID snapshot it processed. ULID order
//!   is observability metadata, not proof that a fragment was compacted.
//! - History generations are nonzero and monotonically increasing.
//! - MessagePack encodes these structs positionally. Persisted fields may only
//!   be appended at the end with a serde default; reordering fields is a wire
//!   format break.
//! - Pending deletion keys remain recorded until deletion succeeds. Dropping a
//!   key merely to bound metadata would leak its object permanently.
//!
//! ## Rust concepts used here
//!
//! The persisted structs derive [`Serialize`][serde::Serialize] and
//! [`Deserialize`][serde::Deserialize]. This is
//! roughly analogous to Java data-transfer objects or C wire structs, but serde
//! generates the encoder/decoder while the Rust types still enforce ownership
//! and nullability. Optional legacy fields use [`Option`][std::option::Option]
//! or default values instead of nullable references.
//!
//! Methods that inspect state borrow `&self`; mutation methods require
//! `&mut self`, so the compiler prevents another ordinary reference from
//! concurrently observing a half-updated in-memory manifest. Async publication
//! borrows the store across `.await`, while owned [`Bytes`][bytes::Bytes] values
//! can be moved into storage calls without retaining pointers into a temporary
//! buffer.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::ops::Range;
use ulid::Ulid;

use crate::error::{Result, ZeppelinError};
use crate::namespace::branching::{
    ArtifactOrigin, ArtifactOriginIndex, ArtifactOriginSetBuilder, BranchError, BranchLineage,
};
use crate::namespace::{
    BranchId, BranchRoot, ForkViewDigest, ManifestDigest, ManifestGeneration, NamespaceId,
    NamespaceIncarnationId, SourceDataPlaneConfigDigest,
};
use crate::storage::store::DELETE_MANY_MAX_KEYS;
use crate::storage::{
    CreateOnlyOutcome, ListedObject, NamespaceObjectKey, StorageVersion, ZeppelinStore,
};

/// Prefix byte identifying Zeppelin's current MessagePack manifest encoding.
///
/// The byte precedes both live manifests and named snapshot pins. Legacy JSON
/// objects have no prefix and begin with `{`.
const MANIFEST_FORMAT_MSGPACK: u8 = 0x01;

/// Manifest metadata for one immutable WAL fragment in object storage.
///
/// Presence in [`Manifest::fragments`] makes the fragment visible to readers;
/// the fragment object may have existed before publication. Sequence numbers,
/// rather than ULID ordering, establish replay order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FragmentRef {
    /// Stable ULID used to derive the fragment's immutable object key.
    pub id: Ulid,
    /// Number of vector upsert entries encoded in the fragment.
    pub vector_count: usize,
    /// Number of deletion tombstones encoded in the fragment.
    pub delete_count: usize,
    /// Namespace-local replay order assigned by [`Manifest::add_fragment`].
    ///
    /// This counter is immune to clock skew and same-millisecond ULID randomness.
    #[serde(default)]
    pub sequence_number: u64,
    /// Serialized fragment size in bytes, recorded at PUT time.
    ///
    /// Used by the size-based compaction trigger so trigger evaluation
    /// needs no S3 reads beyond the manifest itself. `0` on refs written
    /// before this field existed (decoded via serde default) — those
    /// fragments simply don't contribute to the bytes trigger; the age
    /// and count triggers still cover them.
    ///
    /// NOTE: manifest schema additions must remain trailing in the struct.
    /// MessagePack encodes structs as arrays, so old manifests decode only if
    /// new fields are trailing and `#[serde(default)]`.
    #[serde(default)]
    pub size_bytes: u64,
    /// Physical owner of this fragment, or local ownership when absent.
    ///
    /// NOTE: this field must remain last for positional MessagePack decoding.
    #[serde(default)]
    pub artifact_origin: Option<ArtifactOriginIndex>,
}

/// Location and shape of a segment's immutable coarse-search sketch.
///
/// Query planning can keep this compact representation resident and use it to
/// select candidate clusters before reading their full vector payloads.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SketchRef {
    /// S3 key for the immutable sketch artifact.
    pub key: String,
    /// Sketch format version.
    pub version: u32,
    /// Number of projection/code dimensions stored per vector.
    pub code_dims: usize,
    /// Resident bytes per vector, excluding fixed segment metadata.
    pub bytes_per_vector: usize,
    /// Serialized artifact size in bytes.
    pub size_bytes: u64,
    /// Rotation seed for ZSK1 v4; legacy PQ sketches have no rotation.
    ///
    /// NOTE: this field must stay LAST. MessagePack encodes structs as arrays,
    /// so the default preserves manifests written before RaBitQ sketches.
    #[serde(default)]
    pub rotation_seed: Option<u64>,
}

/// Location of an immutable segment bootstrap object.
///
/// A bootstrap object groups centroids and resident sketch bytes into one
/// object-store read for newer segments.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BootstrapRef {
    /// S3 key for the immutable bootstrap artifact.
    pub key: String,
    /// Serialized artifact size in bytes.
    pub size_bytes: u64,
}

/// Location and cardinality of an immutable IVF-Flat membership map.
///
/// The map records vector-ID-to-cluster membership for future incremental
/// compaction work; current readers do not consult it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MembershipRef {
    /// S3 key for the immutable membership artifact.
    pub key: String,
    /// Serialized artifact size in bytes.
    pub size_bytes: u64,
    /// Number of vector-id entries in the artifact.
    pub entry_count: u64,
}

/// Encoding stored in each cluster section's independently ranged coarse region.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CoarsePayloadEncoding {
    /// Scalar-quantized codes stored by `ZCL2` sections.
    #[default]
    Sq8,
    /// Two-bit RaBitQ codes stored by `ZCL3` sections.
    TwoBit,
}

/// Manifest metadata for one immutable object containing one or more IVF
/// cluster payloads.
///
/// Grouping multiple clusters caps object counts and permits a range read of a
/// self-contained live span. Legacy objects advertise no span and require a
/// full GET.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterDataObjectRef {
    /// S3 key for the immutable cluster-data object.
    pub key: String,
    /// Logical cluster indexes whose current data lives in this object.
    pub clusters: Vec<usize>,
    /// Byte offset of the self-contained flat-scan live span.
    ///
    /// `0` with `live_len == 0` means no ranged flat-scan span is advertised
    /// and readers must fetch the full object. This is the default for old
    /// ZBP1 grouped objects and incremental singleton refs.
    #[serde(default)]
    pub live_offset: u64,
    /// Byte length of the self-contained flat-scan live span.
    ///
    /// NOTE: manifest schema additions must remain trailing in the struct.
    /// MessagePack encodes structs as arrays, so old manifests decode only if
    /// new fields are trailing and `#[serde(default)]`.
    #[serde(default)]
    pub live_len: u64,
    /// Total serialized object size in bytes.
    ///
    /// `0` means unknown for legacy manifests. Known sizes let warm range
    /// serving distinguish a short cache entry from a complete cached object.
    ///
    /// NOTE: manifest schema additions must remain trailing in the struct.
    /// MessagePack encodes structs as arrays, so old manifests decode only if
    /// new fields are trailing and `#[serde(default)]`.
    #[serde(default)]
    pub size_bytes: u64,
}

impl ClusterDataObjectRef {
    /// Converts the persisted live-span metadata into a platform-sized byte range.
    ///
    /// # Returns
    ///
    /// `Ok(Some(start..end))` when `live_len` advertises a range, or `Ok(None)`
    /// for legacy/full-object layouts where `live_len` is zero.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Index`] if either persisted `u64` value cannot
    /// fit in this platform's `usize`, or if `offset + length` overflows. No
    /// object-store request occurs before an error.
    ///
    /// # Examples
    ///
    /// An object with `live_offset = 64` and `live_len = 128` yields
    /// `Some(64..192)`. An old object with `live_len = 0` yields `None`, telling
    /// the caller to fetch the complete object.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Rust makes both narrowing conversions and integer addition explicit.
    /// Java's cast to `int` and ordinary C casts can truncate, while
    /// [`usize::try_from`] rejects values that do not fit. [`usize::checked_add`]
    /// returns [`Option`] instead of wrapping, so malformed remote metadata
    /// becomes a typed error rather than an unsafe range.
    pub fn live_range(&self) -> Result<Option<Range<usize>>> {
        if self.live_len == 0 {
            return Ok(None);
        }
        let start = usize::try_from(self.live_offset).map_err(|_| {
            ZeppelinError::Index(format!(
                "cluster object {} live offset does not fit in usize: {}",
                self.key, self.live_offset
            ))
        })?;
        let len = usize::try_from(self.live_len).map_err(|_| {
            ZeppelinError::Index(format!(
                "cluster object {} live length does not fit in usize: {}",
                self.key, self.live_len
            ))
        })?;
        let end = start.checked_add(len).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "cluster object {} live range overflows: offset={}, len={}",
                self.key, self.live_offset, self.live_len
            ))
        })?;
        Ok(Some(start..end))
    }
}

/// Manifest metadata for one immutable IVF segment stored in object storage.
///
/// The descriptor captures the segment's search capabilities and the keys or
/// owners needed to locate its artifacts. Incremental compaction may create a
/// logical segment that still references unchanged cluster objects owned by an
/// older segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentRef {
    /// Unique segment identifier (e.g., `seg_<ULID>`).
    pub id: String,
    /// Number of vectors in the segment.
    pub vector_count: usize,
    /// Number of IVF clusters in the segment.
    pub cluster_count: usize,
    /// Quantization method used for this segment.
    #[serde(default)]
    pub quantization: crate::index::quantization::QuantizationType,
    /// Whether this segment uses a hierarchical index.
    #[serde(default)]
    pub hierarchical: bool,
    /// Fields that have bitmap indexes in this segment.
    /// Empty if bitmap indexing was not enabled when the segment was built.
    #[serde(default)]
    pub bitmap_fields: Vec<String>,
    /// Fields that have FTS inverted indexes in this segment.
    #[serde(default)]
    pub fts_fields: Vec<String>,
    /// Whether this segment has a global FTS index.
    #[serde(default)]
    pub has_global_fts: bool,
    /// Per-cluster owning segment IDs for incremental compaction.
    ///
    /// `cluster_owners[i]` is the segment ID under which cluster `i`'s S3
    /// objects (`cluster_i.bin`, `attrs_i.bin`, `sq_cluster_i.bin`,
    /// `pq_cluster_i.bin`, `bitmap_i.bin`, `fts_i.bin`) actually live.
    /// Incremental compaction (Task 2 Phase B) carries UNTOUCHED clusters
    /// forward by reference — they keep the object keys of an older segment
    /// rather than being re-uploaded under this segment's ID. Only clusters
    /// that gained/lost vectors are rewritten under `self.id`.
    ///
    /// EMPTY means the legacy layout: every cluster is owned by `self.id`
    /// (`{self.id}/cluster_{i}.bin`). All full-retrain builds leave this
    /// empty; `cluster_owner()` resolves an empty vec to `self.id`. Keeping
    /// it empty for full rebuilds means the common path carries zero extra
    /// bytes in the manifest and old manifests decode unchanged.
    ///
    #[serde(default)]
    pub cluster_owners: Vec<String>,
    /// Resident coarse sketch artifact for this segment.
    ///
    /// When present, the query path loads this immutable segment-global
    /// artifact and uses it to choose a smaller set of clusters for exact
    /// rerank. `None` means the segment predates sketches and must use the
    /// legacy IVF path.
    ///
    /// NOTE: manifest schema additions must remain trailing in the struct.
    /// MessagePack encodes structs as arrays, so old manifests decode only if
    /// new fields are trailing and `#[serde(default)]`.
    #[serde(default)]
    pub sketch: Option<SketchRef>,
    /// Immutable cluster-data object layout for grouped cluster objects.
    ///
    /// EMPTY means the legacy one-object-per-cluster layout:
    /// `{cluster_owner(i)}/cluster_i.bin`. New full IVF-Flat compactions write
    /// capped grouped cluster data and populate this list. Incremental
    /// compactions may contain mixed references: rewritten singleton objects
    /// under the new segment and carried objects under older segment keys.
    ///
    /// NOTE: manifest schema additions must remain trailing in the struct.
    /// structs as arrays, so old manifests decode only if new fields are
    /// trailing and `#[serde(default)]`.
    #[serde(default)]
    pub cluster_objects: Vec<ClusterDataObjectRef>,
    /// Immutable bootstrap artifact containing centroids and resident sketch
    /// bytes for this segment.
    ///
    /// When present, the vector query load path fetches this single object and
    /// slices its sections into the existing centroids and sketch decoders.
    /// `None` means the segment predates bootstrap artifacts and must load the
    /// legacy `centroids.bin` and `coarse_sketch.bin` objects explicitly.
    ///
    #[serde(default)]
    pub bootstrap: Option<BootstrapRef>,
    /// Immutable IVF-Flat segment membership artifact.
    ///
    /// When present, this segment has a compact id → cluster-index map. Stage
    /// 2C.1 writes it for new IVF-Flat segments only; current readers do not
    /// consult it yet.
    ///
    /// NOTE: newer persisted fields must remain trailing. MessagePack encodes
    /// structs as arrays, so old manifests decode only if new fields are
    /// trailing and `#[serde(default)]`.
    #[serde(default)]
    pub membership: Option<MembershipRef>,
    /// Physical owner of this segment, or local ownership when absent.
    ///
    /// NOTE: this field must remain last for positional MessagePack decoding.
    #[serde(default)]
    pub artifact_origin: Option<ArtifactOriginIndex>,
}

impl SegmentRef {
    /// Returns the segment ID that owns a cluster's immutable objects.
    ///
    /// Returns the entry in `cluster_owners` when present (incremental
    /// carry-over), otherwise falls back to `self.id` (legacy full-rewrite
    /// layout, and any cluster written by this compaction). Every reader of
    /// a per-cluster S3 key MUST resolve the owner through this method
    /// rather than assuming `self.id` — carried-over clusters live under an
    /// older segment's keys.
    ///
    /// # Parameters
    ///
    /// - `cluster_idx`: Zero-based logical cluster index. An index beyond the
    ///   explicit owner map uses this segment's own ID, matching the legacy
    ///   layout; this method does not validate against `cluster_count`.
    ///
    /// # Returns
    ///
    /// A borrowed owner ID whose lifetime is tied to this descriptor.
    ///
    /// # Examples
    ///
    /// If cluster 2 was carried from `seg_old`, this returns `seg_old`; a cluster
    /// without an override returns `self.id`.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The returned `&str` borrows existing string storage and performs no
    /// allocation. It resembles a Java view of an existing `String` or a C
    /// `const char *`, but Rust proves the text cannot outlive `self` and is
    /// valid UTF-8.
    #[must_use]
    pub fn cluster_owner(&self, cluster_idx: usize) -> &str {
        self.cluster_owners
            .get(cluster_idx)
            .map(String::as_str)
            .unwrap_or(&self.id)
    }

    /// Sums the sizes of segment artifacts recorded directly in this descriptor.
    ///
    /// # Returns
    ///
    /// The known bytes for grouped cluster objects, sketch, bootstrap, and
    /// membership artifacts. Legacy or separately addressed artifacts whose
    /// sizes are absent contribute zero, so this is a lower-bound estimate.
    ///
    /// # Performance
    ///
    /// Runs in `O(cluster_objects.len())`, allocates nothing, and performs no
    /// object-store requests.
    ///
    /// # Examples
    ///
    /// A segment with a 1 KiB cluster object and a 256-byte bootstrap reports
    /// 1,280 bytes even if legacy per-cluster sidecars also exist.
    #[must_use]
    pub fn approximate_storage_bytes(&self) -> u64 {
        let cluster_bytes: u64 = self
            .cluster_objects
            .iter()
            .map(|object| object.size_bytes)
            .sum();
        let sketch_bytes = self.sketch.as_ref().map_or(0, |sketch| sketch.size_bytes);
        let bootstrap_bytes = self
            .bootstrap
            .as_ref()
            .map_or(0, |bootstrap| bootstrap.size_bytes);
        let membership_bytes = self
            .membership
            .as_ref()
            .map_or(0, |membership| membership.size_bytes);

        cluster_bytes + sketch_bytes + bootstrap_bytes + membership_bytes
    }
}

/// Borrowed, validated physical owner for one immutable artifact read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResolvedArtifactOrigin<'a> {
    origin: &'a ArtifactOrigin,
}

impl<'a> ResolvedArtifactOrigin<'a> {
    /// Return the strong namespace-lifetime identity selected by the manifest.
    #[must_use]
    pub(crate) const fn as_origin(self) -> &'a ArtifactOrigin {
        self.origin
    }

    /// Return the physical namespace prefix used by immutable key builders.
    #[must_use]
    pub(crate) fn namespace(self) -> &'a str {
        self.origin.namespace.as_str()
    }
}

/// Globally unique identity of one immutable WAL fragment.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct LocatedFragmentIdentity {
    /// Namespace lifetime that owns the physical object.
    pub(crate) physical_origin: ArtifactOrigin,
    /// ULID embedded in the immutable fragment key and body.
    pub(crate) id: Ulid,
}

/// Manifest fragment descriptor paired with its logical and physical identities.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LocatedFragmentRef<'a> {
    /// Namespace whose manifest authorizes visibility and query accounting.
    pub(crate) logical_namespace: &'a str,
    /// Exact target lifetime whose manifest authorizes this read.
    pub(crate) logical_origin: ResolvedArtifactOrigin<'a>,
    /// Namespace lifetime whose prefix contains the immutable object.
    pub(crate) physical_origin: ResolvedArtifactOrigin<'a>,
    /// Exact descriptor selected from the authoritative manifest.
    pub(crate) fragment: &'a FragmentRef,
}

impl LocatedFragmentRef<'_> {
    /// Build the global cache/dedup identity for this descriptor.
    #[must_use]
    pub(crate) fn identity(self) -> LocatedFragmentIdentity {
        LocatedFragmentIdentity {
            physical_origin: self.physical_origin.as_origin().clone(),
            id: self.fragment.id,
        }
    }

    /// Build the incarnation-qualified disposable-cache key for this object.
    #[must_use]
    pub(crate) fn cache_key(self, store_key: &str) -> String {
        immutable_artifact_cache_key(self.physical_origin.as_origin(), store_key)
    }
}

/// Globally unique identity of one immutable segment descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct LocatedSegmentIdentity {
    /// Namespace lifetime that owns the physical segment objects.
    pub(crate) physical_origin: ArtifactOrigin,
    /// Segment identifier used beneath that physical namespace.
    pub(crate) id: String,
}

/// Manifest segment descriptor paired with its logical and physical identities.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LocatedSegmentRef<'a> {
    /// Namespace whose manifest authorizes visibility and query accounting.
    pub(crate) logical_namespace: &'a str,
    /// Exact target lifetime whose manifest authorizes this read.
    pub(crate) logical_origin: ResolvedArtifactOrigin<'a>,
    /// Namespace lifetime whose prefix contains the immutable objects.
    pub(crate) physical_origin: ResolvedArtifactOrigin<'a>,
    /// Exact descriptor selected from the authoritative manifest.
    pub(crate) segment: &'a SegmentRef,
}

impl<'a> LocatedSegmentRef<'a> {
    /// Build the global cache/dedup identity for this descriptor.
    #[must_use]
    pub(crate) fn identity(self) -> LocatedSegmentIdentity {
        LocatedSegmentIdentity {
            physical_origin: self.physical_origin.as_origin().clone(),
            id: self.segment.id.clone(),
        }
    }

    /// Return the namespace prefix used by every computed segment key.
    #[must_use]
    pub(crate) fn physical_namespace(self) -> &'a str {
        self.physical_origin.namespace()
    }

    /// Build the incarnation-qualified disposable-cache key for an artifact.
    #[must_use]
    pub(crate) fn cache_key(self, store_key: &str) -> String {
        immutable_artifact_cache_key(self.physical_origin.as_origin(), store_key)
    }
}

/// Build a cache-only identity that cannot alias across namespace recreation.
#[must_use]
pub(crate) fn immutable_artifact_cache_key(origin: &ArtifactOrigin, store_key: &str) -> String {
    format!(
        "artifact-origin/{}/{store_key}",
        origin.incarnation.as_uuid().simple()
    )
}

/// Resolves every descriptor in one manifest against one authoritative target lifetime.
///
/// The resolver is a read-only context. It never fills legacy manifest fields in
/// memory, which preserves the signed bytes and receipt projection of retained
/// pre-incarnation history.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ArtifactOriginResolver<'a> {
    manifest: &'a Manifest,
    authoritative_local: &'a ArtifactOrigin,
}

impl<'a> ArtifactOriginResolver<'a> {
    /// Resolve one fragment descriptor without exposing its persisted table index.
    pub(crate) fn locate_fragment(
        &self,
        fragment: &'a FragmentRef,
    ) -> Result<LocatedFragmentRef<'a>> {
        let physical_origin = match fragment.artifact_origin {
            Some(index) => ResolvedArtifactOrigin {
                origin: self.manifest.indexed_artifact_origin_ref(
                    "fragment",
                    &fragment.id.to_string(),
                    index,
                )?,
            },
            None => ResolvedArtifactOrigin {
                origin: self.authoritative_local,
            },
        };
        Ok(LocatedFragmentRef {
            logical_namespace: self.authoritative_local.namespace.as_str(),
            logical_origin: ResolvedArtifactOrigin {
                origin: self.authoritative_local,
            },
            physical_origin,
            fragment,
        })
    }

    /// Resolve all retained fragment descriptors in manifest replay order.
    pub(crate) fn located_fragments(&self) -> Result<Vec<LocatedFragmentRef<'a>>> {
        self.manifest
            .fragments
            .iter()
            .map(|fragment| self.locate_fragment(fragment))
            .collect()
    }

    /// Resolve only the currently uncompacted refs in replay order.
    pub(crate) fn uncompacted_located_fragments(&self) -> Result<Vec<LocatedFragmentRef<'a>>> {
        self.manifest
            .uncompacted_fragments()
            .iter()
            .map(|fragment| self.locate_fragment(fragment))
            .collect()
    }

    /// Resolve one segment descriptor without exposing its persisted table index.
    pub(crate) fn locate_segment(&self, segment: &'a SegmentRef) -> Result<LocatedSegmentRef<'a>> {
        let physical_origin = match segment.artifact_origin {
            Some(index) => ResolvedArtifactOrigin {
                origin: self
                    .manifest
                    .indexed_artifact_origin_ref("segment", &segment.id, index)?,
            },
            None => ResolvedArtifactOrigin {
                origin: self.authoritative_local,
            },
        };
        Ok(LocatedSegmentRef {
            logical_namespace: self.authoritative_local.namespace.as_str(),
            logical_origin: ResolvedArtifactOrigin {
                origin: self.authoritative_local,
            },
            physical_origin,
            segment,
        })
    }

    /// Resolve all retained segment descriptors in manifest order.
    pub(crate) fn located_segments(&self) -> Result<Vec<LocatedSegmentRef<'a>>> {
        self.manifest
            .segments
            .iter()
            .map(|segment| self.locate_segment(segment))
            .collect()
    }

    /// Resolve the unique descriptor named by the manifest's active pointer.
    pub(crate) fn active_located_segment(&self) -> Result<Option<LocatedSegmentRef<'a>>> {
        let Some(active_id) = self.manifest.active_segment.as_deref() else {
            return Ok(None);
        };
        let mut matches = self
            .manifest
            .segments
            .iter()
            .filter(|segment| segment.id == active_id);
        let Some(segment) = matches.next() else {
            return Err(ZeppelinError::Index(format!(
                "active segment {active_id} is missing from manifest segments"
            )));
        };
        if matches.next().is_some() {
            return Err(ZeppelinError::Index(format!(
                "active segment {active_id} is ambiguous across artifact origins"
            )));
        }
        self.locate_segment(segment).map(Some)
    }
}

/// Persisted strong type for one namespace lifetime identity.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
struct ManifestNamespaceIncarnation([u8; 16]);

impl ManifestNamespaceIncarnation {
    fn from_uuid(value: uuid::Uuid) -> Self {
        Self(*value.as_bytes())
    }

    fn as_uuid(self) -> uuid::Uuid {
        uuid::Uuid::from_bytes(self.0)
    }
}

/// Authoritative inventory of the data visible in one namespace.
///
/// A value in memory is only a candidate view. It becomes authoritative when
/// its encoded bytes are published at [`Manifest::s3_key`]. The manifest tracks
/// uncompacted WAL fragments, immutable search segments, deferred deletion
/// work, the writer's fencing token, and its persisted generation.
///
/// # Persisted format
///
/// MessagePack encodes this struct as a positional array. New fields must be
/// appended at the end and carry `#[serde(default)]`; moving or inserting a
/// field can make old object-store manifests decode into the wrong fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Visible, uncompacted WAL fragments in sequence-number order.
    pub fragments: Vec<FragmentRef>,
    /// Visible immutable segments, appended as compactions publish them.
    pub segments: Vec<SegmentRef>,
    /// ULID of the last fragment that was compacted.
    /// Fragments with IDs <= this have been incorporated into segments.
    #[serde(default)]
    pub compaction_watermark: Option<Ulid>,
    /// ID of the segment that currently serves as the active compacted view.
    ///
    /// `None` means no segment is active, as in a new namespace or after the
    /// active descriptor was explicitly removed.
    #[serde(default)]
    pub active_segment: Option<String>,
    /// Next namespace-local replay sequence assigned to a new fragment.
    #[serde(default)]
    pub next_sequence: u64,
    /// Object-store keys awaiting successful deferred deletion.
    #[serde(default)]
    pub pending_deletes: Vec<String>,
    /// Fencing token set by the lease holder during manifest writes.
    /// Prevents zombie writers (expired lease holders) from overwriting
    /// a manifest that a newer lease holder has already written.
    #[serde(default)]
    pub fencing_token: u64,
    /// Wall-clock time of the last in-memory domain update represented here.
    ///
    /// Retention uses this timestamp with an explicit skew allowance; ordering
    /// and publication correctness never rely on it.
    pub updated_at: DateTime<Utc>,
    /// Monotonic manifest generation persisted with each manifest commit.
    ///
    /// Legacy manifests decode as `0`; each successful manifest write stores
    /// the next generation.
    #[serde(default)]
    version: u64,
    /// Namespace whose live pointer owns these bytes.
    ///
    /// Old manifests predate this binding and decode as `None`. Every new live
    /// or history write sets it before serialization so a valid manifest
    /// returned for another namespace fails loud instead of becoming state.
    ///
    /// NOTE: persisted fields added after this one must remain trailing because
    /// MessagePack encodes structs as positional arrays.
    #[serde(default)]
    namespace: Option<String>,
    /// Collision-resistant identity of the namespace lifetime owning these bytes.
    ///
    /// Guarded writes compare this value from the same manifest GET that
    /// supplied their CAS ETag. A delete/recreate therefore cannot reuse a
    /// byte-identical generation/ETag as authority for work derived from the old
    /// namespace lifetime.
    ///
    /// NOTE: retain this field's existing position; MessagePack encodes structs
    /// as positional arrays, so new persisted fields append after existing ones.
    #[serde(default)]
    namespace_incarnation: Option<ManifestNamespaceIncarnation>,
    /// Immutable governed-destruction fence bound to this namespace lifetime.
    ///
    /// The fence is CAS-published before destruction evidence is finalized.
    /// Normal manifest writers reject a fenced base, while writers holding an
    /// older ETag lose to the fence CAS. This field retains its historical
    /// position; newer persisted fields append after it because MessagePack
    /// encodes structs as positional arrays.
    #[serde(default)]
    deletion_fence: Option<ManifestDeletionFence>,
    /// SHA-256 inventory for every immutable artifact visible through this generation.
    ///
    /// Old manifests decode as empty and remain queryable, but receipt issuance
    /// fails loudly until compaction replaces their reachable artifact set.
    #[serde(default)]
    artifact_hashes: BTreeMap<String, [u8; 32]>,
    /// Canonical Merkle root over `artifact_hashes` in sorted-key order.
    #[serde(default)]
    merkle_root: Option<[u8; 32]>,
    /// Ed25519 signature over root, execution-state digest, generation, and fencing token.
    #[serde(default)]
    root_signature: Option<Vec<u8>>,
    /// Published signer identity used by `root_signature`.
    #[serde(default)]
    root_signer_node: Option<String>,
    /// Exact hierarchical routing-node IDs keyed by owning segment ID.
    ///
    /// Routing nodes are fetched lazily by production search and therefore
    /// must be explicit manifest-rooted artifacts. Legacy manifests decode
    /// empty and are populated only by an explicit compaction upgrade.
    #[serde(default)]
    hierarchical_routing_nodes: BTreeMap<String, Vec<String>>,
    /// Canonical digest of the query-routing manifest projection.
    ///
    /// This field retains its persisted position because MessagePack encodes
    /// structs as positional arrays. It excludes the artifact hashes and
    /// signature envelope, which are bound separately by the Merkle root
    /// signature.
    #[serde(default)]
    receipt_state_digest: Option<[u8; 32]>,
    /// Version of the stable projection encoded by `receipt_state_digest`.
    ///
    /// A new query-relevant manifest field requires a new binding version
    /// rather than changing the v1 projection in place.
    #[serde(default)]
    receipt_binding_version: Option<ReceiptBindingVersion>,
    /// Canonical physical owners referenced by fragment and segment descriptors.
    #[serde(default)]
    pub artifact_origins: Vec<ArtifactOrigin>,
    /// Versioned digest of retention and lineage control state.
    ///
    /// Phase 02 reserves this trailing seam. V1 and V2 require it to be absent.
    #[serde(default)]
    control_state_digest: Option<[u8; 32]>,
    /// Exact source-generation retention roots for direct child namespaces.
    ///
    /// Old manifests decode with no roots. The ordered map participates in the
    /// V3 control projection, so its serialization is deterministic. New
    /// persisted fields must remain after this frozen position.
    #[serde(default)]
    branch_roots: BTreeMap<BranchId, BranchRoot>,
    /// Immutable direct-parent ancestry retained for this namespace lifetime.
    ///
    /// This field is appended after the frozen V3 root map. A branch remains
    /// V4-bound even after all inherited artifacts have been materialized.
    #[serde(default)]
    branch_lineage: Option<BranchLineage>,
    /// Coarse-region encoding keyed by segment ID.
    ///
    /// Missing entries identify SQ8, including manifests written before this
    /// field existed. New persisted fields must remain after this position.
    #[serde(default)]
    coarse_payload_encodings: BTreeMap<String, CoarsePayloadEncoding>,
}

/// Stable manifest execution projection version used by signed receipts.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReceiptBindingVersion {
    /// Original field-by-field query-routing projection.
    #[serde(rename = "v1")]
    V1,
    /// Origin-aware execution projection and v2 root envelope.
    #[serde(rename = "v2_origins")]
    V2Origins,
    /// Reserved root-control projection owned by phase 04.
    #[serde(rename = "v3_roots")]
    V3Roots,
    /// Origin-aware execution plus immutable lineage/roots control projection.
    #[serde(rename = "v4_lineage")]
    V4Lineage,
}

#[derive(Serialize)]
struct FragmentExecutionBindingV1 {
    id: String,
    vector_count: usize,
    delete_count: usize,
    sequence_number: u64,
    size_bytes: u64,
}

#[derive(Serialize)]
struct SketchExecutionBindingV1<'a> {
    key: &'a str,
    version: u32,
    code_dims: usize,
    bytes_per_vector: usize,
    size_bytes: u64,
    rotation_seed: Option<u64>,
}

#[derive(Serialize)]
struct ClusterObjectExecutionBindingV1<'a> {
    key: &'a str,
    clusters: &'a [usize],
    live_offset: u64,
    live_len: u64,
    size_bytes: u64,
}

#[derive(Serialize)]
struct BootstrapExecutionBindingV1<'a> {
    key: &'a str,
    size_bytes: u64,
}

#[derive(Serialize)]
struct SegmentExecutionBindingV1<'a> {
    id: &'a str,
    vector_count: usize,
    cluster_count: usize,
    quantization: &'static str,
    hierarchical: bool,
    bitmap_fields: &'a [String],
    fts_fields: &'a [String],
    has_global_fts: bool,
    cluster_owners: &'a [String],
    sketch: Option<SketchExecutionBindingV1<'a>>,
    cluster_objects: Vec<ClusterObjectExecutionBindingV1<'a>>,
    bootstrap: Option<BootstrapExecutionBindingV1<'a>>,
}

#[derive(Serialize)]
struct HierarchicalRoutingExecutionBindingV1<'a> {
    segment_id: &'a str,
    node_ids: &'a [String],
}

#[derive(Serialize)]
struct ManifestExecutionBindingV1<'a> {
    format: &'static str,
    namespace: &'a str,
    namespace_incarnation: Option<[u8; 16]>,
    fragments: Vec<FragmentExecutionBindingV1>,
    segments: Vec<SegmentExecutionBindingV1<'a>>,
    active_segment: Option<&'a str>,
    hierarchical_routing_nodes: Vec<HierarchicalRoutingExecutionBindingV1<'a>>,
}

#[derive(Serialize)]
struct FragmentExecutionBindingV2 {
    id: String,
    vector_count: usize,
    delete_count: usize,
    sequence_number: u64,
    size_bytes: u64,
    artifact_origin: Option<u32>,
}

#[derive(Serialize)]
struct SegmentExecutionBindingV2<'a> {
    id: &'a str,
    vector_count: usize,
    cluster_count: usize,
    quantization: &'static str,
    hierarchical: bool,
    bitmap_fields: &'a [String],
    fts_fields: &'a [String],
    has_global_fts: bool,
    cluster_owners: &'a [String],
    sketch: Option<SketchExecutionBindingV1<'a>>,
    cluster_objects: Vec<ClusterObjectExecutionBindingV1<'a>>,
    bootstrap: Option<BootstrapExecutionBindingV1<'a>>,
    artifact_origin: Option<u32>,
}

#[derive(Serialize)]
struct ArtifactOriginExecutionBindingV2<'a> {
    namespace: &'a str,
    incarnation: [u8; 16],
}

#[derive(Serialize)]
struct ManifestExecutionBindingV2<'a> {
    format: &'static str,
    namespace: &'a str,
    namespace_incarnation: Option<[u8; 16]>,
    fragments: Vec<FragmentExecutionBindingV2>,
    segments: Vec<SegmentExecutionBindingV2<'a>>,
    active_segment: Option<&'a str>,
    hierarchical_routing_nodes: Vec<HierarchicalRoutingExecutionBindingV1<'a>>,
    artifact_origins: Vec<ArtifactOriginExecutionBindingV2<'a>>,
}

/// Exact retention/control projection introduced by receipt binding V3.
#[derive(Serialize)]
struct ControlRootsV1<'a> {
    namespace: &'a str,
    incarnation: Option<[u8; 16]>,
    deletion_fence: Option<&'a ManifestDeletionFence>,
    branch_roots: &'a BTreeMap<BranchId, BranchRoot>,
}

/// Exact lineage/control projection introduced by receipt binding V4.
#[derive(Serialize)]
struct ControlBranchV2<'a> {
    namespace: &'a str,
    incarnation: Option<[u8; 16]>,
    deletion_fence: Option<&'a ManifestDeletionFence>,
    branch_roots: &'a BTreeMap<BranchId, BranchRoot>,
    branch_lineage: &'a BranchLineage,
}

/// Non-circular immutable inputs used to construct a target branch lineage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BranchLineageSeed {
    pub(crate) branch_id: BranchId,
    pub(crate) parent_namespace: NamespaceId,
    pub(crate) parent_incarnation: NamespaceIncarnationId,
    pub(crate) fork_generation: ManifestGeneration,
    pub(crate) fork_manifest_sha256: ManifestDigest,
    pub(crate) source_config_sha256: SourceDataPlaneConfigDigest,
    pub(crate) depth: u16,
    pub(crate) created_at: DateTime<Utc>,
}

impl BranchLineageSeed {
    fn with_fork_view(&self, fork_view_sha256: ForkViewDigest) -> BranchLineage {
        BranchLineage {
            branch_id: self.branch_id,
            parent_namespace: self.parent_namespace.clone(),
            parent_incarnation: self.parent_incarnation.clone(),
            fork_generation: self.fork_generation,
            fork_manifest_sha256: self.fork_manifest_sha256,
            fork_view_sha256,
            source_config_sha256: self.source_config_sha256,
            depth: self.depth,
            created_at: self.created_at,
        }
    }
}

impl From<&BranchLineage> for BranchLineageSeed {
    fn from(lineage: &BranchLineage) -> Self {
        Self {
            branch_id: lineage.branch_id,
            parent_namespace: lineage.parent_namespace.clone(),
            parent_incarnation: lineage.parent_incarnation.clone(),
            fork_generation: lineage.fork_generation,
            fork_manifest_sha256: lineage.fork_manifest_sha256,
            source_config_sha256: lineage.source_config_sha256,
            depth: lineage.depth,
            created_at: lineage.created_at,
        }
    }
}

/// Canonical normalized target view and its computed immutable lineage.
#[derive(Debug, Clone)]
pub(crate) struct PreparedZeroCopyFork {
    pub(crate) manifest: Manifest,
    pub(crate) lineage: BranchLineage,
}

/// One finalized generation-one manifest and the only bytes it may publish.
#[derive(Debug, Clone)]
pub(crate) struct PreparedManifestPublication {
    manifest: Manifest,
    bytes: Bytes,
    digest: ManifestDigest,
}

impl PreparedManifestPublication {
    #[must_use]
    pub(crate) fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    #[must_use]
    pub(crate) fn exact_bytes(&self) -> &Bytes {
        &self.bytes
    }

    #[must_use]
    pub(crate) const fn digest(&self) -> ManifestDigest {
        self.digest
    }
}

/// Frozen canonical initial-view projection. The lineage digest itself,
/// generation/timestamps, signatures, roots, and pending deletes are excluded.
#[derive(Serialize)]
struct ForkViewProjectionV1<'a> {
    domain: &'static str,
    target_namespace: &'a str,
    target_incarnation: [u8; 16],
    source_namespace: &'a str,
    source_incarnation: [u8; 16],
    branch_id: BranchId,
    source_generation: ManifestGeneration,
    source_manifest_sha256: ManifestDigest,
    source_config_sha256: SourceDataPlaneConfigDigest,
    depth: u16,
    execution: ManifestExecutionBindingV2<'a>,
    artifact_hashes: &'a BTreeMap<String, [u8; 32]>,
}

/// Fixed root-signing envelope shared by V2 origins and later control bindings.
///
/// Field order and the domain string are frozen by the V2 byte fixture. Moving
/// this type out of the match arm lets V3 add a control digest without changing
/// one byte of existing V2 signatures.
#[derive(Serialize)]
struct ManifestRootEnvelopeV2 {
    domain: &'static str,
    merkle_root: [u8; 32],
    manifest_generation: u64,
    fencing_token: u64,
    binding_version: ReceiptBindingVersion,
    execution_digest: [u8; 32],
    control_digest: Option<[u8; 32]>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ManifestDeletionFence {
    destruction_record_key: String,
}

/// Location of one immutable, addressable historical manifest generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestHistoryRef {
    /// Persisted manifest generation.
    pub version: u64,
    /// Immutable S3 key containing the serialized manifest snapshot.
    pub key: String,
}

/// One fresh object-store observation of an immutable history generation.
///
/// The history reference is parsed from the reserved key grammar while the
/// optional storage version is preserved exactly as LIST reported it. Absence
/// never authorizes reuse: callers must read and validate the body whenever
/// `storage_version` is `None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManifestHistoryObservation {
    /// Parsed generation and exact immutable object key.
    pub(crate) history: ManifestHistoryRef,
    /// Opaque backend identity observed by the same recursive LIST.
    pub(crate) storage_version: Option<StorageVersion>,
}

/// Persisted point-in-time-recovery pin for one manifest generation.
///
/// The object lives at `<namespace>/snapshots/<name>.msgpack`. Its presence
/// prevents history pruning from deleting the referenced generation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamedSnapshot {
    /// Manifest generation pinned by this snapshot.
    pub generation: u64,
    /// Snapshot creation timestamp.
    pub created_at: DateTime<Utc>,
}

/// Caller-facing named snapshot metadata plus its object-store location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedSnapshotRef {
    /// Caller-supplied snapshot name.
    pub name: String,
    /// S3 key containing the snapshot pin.
    pub key: String,
    /// Manifest generation pinned by this snapshot.
    pub generation: u64,
    /// Snapshot creation timestamp.
    pub created_at: DateTime<Utc>,
}

/// One named-snapshot body paired with the identity observed by its LIST.
///
/// Garbage collection keeps this crate-private observation until its later
/// namespace inventory is available. A different key, size, or opaque version
/// means the retention decision and the inventory did not describe one stable
/// snapshot-pin set, so that cycle cannot authorize a subsequent idle skip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NamedSnapshotObservation {
    /// Decoded, caller-facing pin metadata.
    pub(crate) snapshot: NamedSnapshotRef,
    /// Exact object metadata returned by the same prefix LIST.
    pub(crate) object: ListedObject,
}

/// Observable result of a manifest-history pruning pass.
#[derive(Debug, Clone)]
pub struct ManifestHistoryPruneResult {
    /// Number of history snapshots deleted.
    pub pruned: usize,
    /// Decoded manifests kept by count, time window, named pin, or current live
    /// branch root, in ascending generation order.
    pub retained_manifests: Vec<Manifest>,
}

/// Union-of-rules policy controlling manifest-history retention.
///
/// A generation is retained when *any* configured rule keeps it: recent count,
/// PITR age window (including skew slop), a [`NamedSnapshot`] pin, or a root in
/// the current authoritative live manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestHistoryRetention {
    /// Number of newest generations to retain; must be greater than zero.
    pub keep_count: usize,
    /// Time-based PITR retention window in seconds. `0` disables time retention.
    pub pitr_retention_secs: u64,
    /// Additional seconds allowed for writer/read-side clock skew.
    pub skew_slop_secs: u64,
}

/// Outcome of idempotently creating an immutable history object.
enum HistorySnapshotWrite {
    /// The desired bytes were created or already existed byte-for-byte.
    Stored,
    /// The generation key exists, but contains different candidate bytes.
    AlreadyExistsWithDifferentBytes {
        /// Conflicting history key that the caller must classify.
        key: String,
    },
}

/// Canonical bytes signed by each Merkle-rooted manifest generation.
pub(crate) fn manifest_root_signing_bytes(
    merkle_root: [u8; 32],
    manifest_version: u64,
    fencing_token: u64,
    binding_version: ReceiptBindingVersion,
    state_digest: [u8; 32],
    control_state_digest: Option<[u8; 32]>,
) -> Result<Vec<u8>> {
    match binding_version {
        ReceiptBindingVersion::V1 => {
            if control_state_digest.is_some() {
                return Err(ZeppelinError::Serialization(
                    "receipt binding v1 forbids a control digest".to_string(),
                ));
            }

            #[derive(Serialize)]
            struct RootBinding {
                merkle_root: [u8; 32],
                manifest_version: u64,
                fencing_token: u64,
                binding_version: ReceiptBindingVersion,
                state_digest: [u8; 32],
            }

            serde_json::to_vec(&RootBinding {
                merkle_root,
                manifest_version,
                fencing_token,
                binding_version,
                state_digest,
            })
            .map_err(|error| {
                ZeppelinError::Serialization(format!("manifest root signing failed: {error}"))
            })
        }
        ReceiptBindingVersion::V2Origins => {
            if control_state_digest.is_some() {
                return Err(ZeppelinError::Serialization(
                    "receipt binding v2_origins forbids a control digest".to_string(),
                ));
            }

            serde_json::to_vec(&ManifestRootEnvelopeV2 {
                domain: "zeppelin-manifest-root-envelope-v2",
                merkle_root,
                manifest_generation: manifest_version,
                fencing_token,
                binding_version,
                execution_digest: state_digest,
                control_digest: control_state_digest,
            })
            .map_err(|error| {
                ZeppelinError::Serialization(format!("manifest root signing failed: {error}"))
            })
        }
        ReceiptBindingVersion::V3Roots | ReceiptBindingVersion::V4Lineage => {
            let control_digest = control_state_digest.ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "receipt binding {binding_version:?} requires a control digest"
                ))
            })?;
            serde_json::to_vec(&ManifestRootEnvelopeV2 {
                domain: "zeppelin-manifest-root-envelope-v2",
                merkle_root,
                manifest_generation: manifest_version,
                fencing_token,
                binding_version,
                execution_digest: state_digest,
                control_digest: Some(control_digest),
            })
            .map_err(|error| {
                ZeppelinError::Serialization(format!("manifest root signing failed: {error}"))
            })
        }
    }
}

impl Manifest {
    /// Creates an unpublished, empty namespace manifest at generation zero.
    ///
    /// # Returns
    ///
    /// An owned manifest with no visible artifacts, no active segment, and no
    /// deferred deletions. [`Manifest::write`] assigns the first committed
    /// generation.
    ///
    /// # Examples
    ///
    /// Namespace creation starts with this value, writes generation 1, and only
    /// then exposes the namespace to write and query paths.
    pub fn new() -> Self {
        Self::new_at(Utc::now())
    }

    /// Creates an unpublished empty manifest stamped at an explicit time.
    #[must_use]
    pub fn new_at(now: DateTime<Utc>) -> Self {
        Self {
            fragments: Vec::new(),
            segments: Vec::new(),
            compaction_watermark: None,
            active_segment: None,
            next_sequence: 0,
            pending_deletes: Vec::new(),
            fencing_token: 0,
            updated_at: now,
            version: 0,
            namespace: None,
            namespace_incarnation: None,
            deletion_fence: None,
            artifact_hashes: BTreeMap::new(),
            merkle_root: None,
            root_signature: None,
            root_signer_node: None,
            hierarchical_routing_nodes: BTreeMap::new(),
            receipt_state_digest: None,
            receipt_binding_version: None,
            artifact_origins: Vec::new(),
            control_state_digest: None,
            branch_roots: BTreeMap::new(),
            branch_lineage: None,
            coarse_payload_encodings: BTreeMap::new(),
        }
    }

    /// Returns the manifest-selected coarse decoder for one segment.
    #[must_use]
    pub fn coarse_payload_encoding(&self, segment_id: &str) -> CoarsePayloadEncoding {
        self.coarse_payload_encodings
            .get(segment_id)
            .copied()
            .unwrap_or_default()
    }

    /// Records the coarse decoder for one immutable segment.
    pub fn set_coarse_payload_encoding(
        &mut self,
        segment_id: impl Into<String>,
        encoding: CoarsePayloadEncoding,
    ) {
        let segment_id = segment_id.into();
        if encoding == CoarsePayloadEncoding::Sq8 {
            self.coarse_payload_encodings.remove(&segment_id);
        } else {
            self.coarse_payload_encodings.insert(segment_id, encoding);
        }
    }

    /// Binds this manifest to one authoritative namespace lifetime.
    ///
    /// Rebinding an already-bound manifest fails loudly. Clone preparation must
    /// first call [`Manifest::reset_version_for_clone`], which clears source
    /// namespace identity before the target incarnation is attached.
    pub fn bind_namespace_incarnation(&mut self, incarnation: uuid::Uuid) -> Result<()> {
        let incarnation = ManifestNamespaceIncarnation::from_uuid(incarnation);
        match self.namespace_incarnation {
            Some(existing) if existing != incarnation => Err(ZeppelinError::Serialization(
                "manifest namespace incarnation cannot be rebound".to_string(),
            )),
            Some(_) => Ok(()),
            None => {
                self.namespace_incarnation = Some(incarnation);
                Ok(())
            }
        }
    }

    /// Returns the namespace lifetime identity carried by these manifest bytes.
    #[must_use]
    pub(crate) fn namespace_incarnation(&self) -> Option<uuid::Uuid> {
        self.namespace_incarnation
            .map(ManifestNamespaceIncarnation::as_uuid)
    }

    fn artifact_origin_error(
        &self,
        descriptor_kind: &'static str,
        descriptor_id: impl Into<String>,
        offending_index: Option<ArtifactOriginIndex>,
        offending_key: Option<String>,
        expected_origin: Option<ArtifactOrigin>,
        reason: impl Into<String>,
    ) -> ZeppelinError {
        BranchError::ArtifactOriginInvalid {
            manifest_namespace: self
                .namespace
                .clone()
                .unwrap_or_else(|| "<unbound>".to_string()),
            manifest_incarnation: self
                .namespace_incarnation()
                .map(NamespaceIncarnationId::from_uuid),
            descriptor_kind,
            descriptor_id: descriptor_id.into(),
            offending_index,
            offending_key,
            expected_origin,
            reason: reason.into(),
        }
        .into()
    }

    /// Return the local cache key an immutable artifact of `segment` is stored under.
    ///
    /// Immutable artifacts are cached by **physical incarnation**, not by the
    /// logical namespace, so two branches sharing a source segment share one
    /// entry and neither can reconstruct a key from its own name. That makes
    /// the raw object key the wrong thing to look up.
    ///
    /// This is the seam for tests and tooling that need to assert what the
    /// cache should contain. It deliberately routes through the same origin
    /// resolver the hydration and read paths use, so a caller can never carry
    /// a divergent copy of the key format — the copy is what silently rots
    /// when the derivation changes.
    ///
    /// # Errors
    ///
    /// Returns an error when the manifest has no namespace or incarnation
    /// binding, or when `segment`'s origin index does not resolve.
    pub fn segment_artifact_cache_key(
        &self,
        segment: &SegmentRef,
        store_key: &str,
    ) -> Result<String> {
        let local = self.local_origin()?;
        let resolver = self.artifact_origin_resolver(&local)?;
        let located = resolver.locate_segment(segment)?;
        Ok(immutable_artifact_cache_key(
            located.physical_origin.as_origin(),
            store_key,
        ))
    }

    /// Return the local cache key a WAL fragment's bytes are stored under.
    ///
    /// The fragment counterpart of [`Self::segment_artifact_cache_key`], and
    /// it exists for the same reason. Every reader that consults the byte
    /// cache for a fragment — the query path, fetch-by-id, compaction — keys
    /// by physical incarnation. A caller that rebuilds
    /// `wal_fragments/{ulid}.wal` by hand addresses a namespace nothing else
    /// reads, so its lookups and invalidations silently do nothing.
    ///
    /// Three separate harnesses have made exactly that mistake, each time
    /// producing a measurement that looked like a product change. Route
    /// through this seam rather than formatting the key.
    ///
    /// # Errors
    ///
    /// Returns an error when the manifest has no namespace or incarnation
    /// binding, or when `fragment`'s origin index does not resolve.
    pub fn fragment_artifact_cache_key(
        &self,
        fragment: &FragmentRef,
        store_key: &str,
    ) -> Result<String> {
        let local = self.local_origin()?;
        let resolver = self.artifact_origin_resolver(&local)?;
        let located = resolver.locate_fragment(fragment)?;
        Ok(immutable_artifact_cache_key(
            located.physical_origin.as_origin(),
            store_key,
        ))
    }

    /// Resolve the physical owner encoded by an absent origin index.
    pub(crate) fn local_origin(&self) -> Result<ArtifactOrigin> {
        let namespace = self.namespace.as_ref().ok_or_else(|| {
            self.artifact_origin_error(
                "manifest",
                "namespace",
                None,
                None,
                None,
                "local artifact origin requires a namespace binding",
            )
        })?;
        let namespace = NamespaceId::parse(namespace.clone()).map_err(|_| {
            self.artifact_origin_error(
                "manifest",
                "namespace",
                None,
                None,
                None,
                "manifest namespace violates the namespace grammar",
            )
        })?;
        let incarnation = self
            .namespace_incarnation()
            .map(NamespaceIncarnationId::from_uuid)
            .ok_or_else(|| {
                self.artifact_origin_error(
                    "manifest",
                    "namespace_incarnation",
                    None,
                    None,
                    None,
                    "local artifact origin requires an incarnation binding",
                )
            })?;
        if incarnation.is_nil() {
            return Err(self.artifact_origin_error(
                "manifest",
                "namespace_incarnation",
                None,
                None,
                None,
                "manifest namespace incarnation is nil",
            ));
        }
        Ok(ArtifactOrigin {
            namespace,
            incarnation,
        })
    }

    /// Resolve the physical owner of this manifest's active immutable segment.
    ///
    /// This is the narrow public seam used by offline production evaluators
    /// that validate exact segment objects without constructing the internal
    /// query index. Resolution still runs through the same manifest validator
    /// as server reads, including namespace-incarnation binding, origin-table
    /// bounds, duplicate identities, and active-segment ambiguity checks.
    ///
    /// # Parameters
    ///
    /// - `authoritative_local`: Namespace metadata identity for the logical
    ///   manifest owner. It supplies read-time identity for legacy unbound
    ///   manifests and must match bindings carried by current manifests.
    ///
    /// # Returns
    ///
    /// `Ok(None)` when the manifest has no active segment, otherwise the exact
    /// namespace lifetime that owns the active segment's immutable objects.
    ///
    /// # Errors
    ///
    /// Returns the normal typed manifest/origin error when the supplied owner
    /// conflicts with a binding, an origin is corrupt, or the active descriptor
    /// cannot be resolved uniquely.
    pub fn active_segment_artifact_origin(
        &self,
        authoritative_local: &ArtifactOrigin,
    ) -> Result<Option<ArtifactOrigin>> {
        self.artifact_origin_resolver(authoritative_local)?
            .active_located_segment()
            .map(|located| located.map(|located| located.physical_origin.as_origin().clone()))
    }

    /// Bind read routing to the authoritative local namespace lifetime.
    ///
    /// Persisted bindings, when present, must match the supplied context. When
    /// legacy fields are absent the context supplies only read-time location;
    /// the manifest remains byte-for-byte unchanged.
    pub(crate) fn artifact_origin_resolver<'a>(
        &'a self,
        authoritative_local: &'a ArtifactOrigin,
    ) -> Result<ArtifactOriginResolver<'a>> {
        if authoritative_local.incarnation.is_nil() {
            return Err(self.artifact_origin_error(
                "manifest",
                "authoritative_local_origin",
                None,
                None,
                Some(authoritative_local.clone()),
                "authoritative namespace incarnation is nil",
            ));
        }
        if let Some(namespace) = self.namespace.as_deref() {
            if namespace != authoritative_local.namespace.as_str() {
                return Err(self.artifact_origin_error(
                    "manifest",
                    "namespace",
                    None,
                    None,
                    Some(authoritative_local.clone()),
                    "persisted namespace does not match authoritative local origin",
                ));
            }
        }
        if let Some(incarnation) = self.namespace_incarnation() {
            if incarnation != authoritative_local.incarnation.as_uuid() {
                return Err(self.artifact_origin_error(
                    "manifest",
                    "namespace_incarnation",
                    None,
                    None,
                    Some(authoritative_local.clone()),
                    "persisted incarnation does not match authoritative local origin",
                ));
            }
        }

        self.validate_artifact_origins_structural(true)?;
        let resolver = ArtifactOriginResolver {
            manifest: self,
            authoritative_local,
        };

        let mut fragment_identities = HashSet::with_capacity(self.fragments.len());
        for fragment in &self.fragments {
            let located = resolver.locate_fragment(fragment)?;
            if !fragment_identities.insert(located.identity()) {
                return Err(self.artifact_origin_error(
                    "fragment",
                    fragment.id.to_string(),
                    fragment.artifact_origin,
                    None,
                    Some(located.physical_origin.as_origin().clone()),
                    "duplicate full located fragment identity",
                ));
            }
        }

        let mut segment_identities = HashSet::with_capacity(self.segments.len());
        for segment in &self.segments {
            let located = resolver.locate_segment(segment)?;
            let origin = located.physical_origin.as_origin();
            if let Some(sketch) = &segment.sketch {
                self.validate_explicit_origin_key(segment, origin, &sketch.key)?;
            }
            if let Some(bootstrap) = &segment.bootstrap {
                self.validate_explicit_origin_key(segment, origin, &bootstrap.key)?;
            }
            if let Some(membership) = &segment.membership {
                self.validate_explicit_origin_key(segment, origin, &membership.key)?;
            }
            for object in &segment.cluster_objects {
                self.validate_explicit_origin_key(segment, origin, &object.key)?;
            }
            if !segment_identities.insert(located.identity()) {
                return Err(self.artifact_origin_error(
                    "segment",
                    &segment.id,
                    segment.artifact_origin,
                    None,
                    Some(origin.clone()),
                    "duplicate full located segment identity",
                ));
            }
        }

        Ok(resolver)
    }

    fn indexed_artifact_origin_ref(
        &self,
        descriptor_kind: &'static str,
        descriptor_id: &str,
        index: ArtifactOriginIndex,
    ) -> Result<&ArtifactOrigin> {
        let index_usize = usize::try_from(index.get()).map_err(|_| {
            self.artifact_origin_error(
                descriptor_kind,
                descriptor_id,
                Some(index),
                None,
                None,
                "artifact origin index does not fit this platform",
            )
        })?;
        self.artifact_origins.get(index_usize).ok_or_else(|| {
            self.artifact_origin_error(
                descriptor_kind,
                descriptor_id,
                Some(index),
                None,
                None,
                format!(
                    "artifact origin index is out of bounds for table length {}",
                    self.artifact_origins.len()
                ),
            )
        })
    }

    fn indexed_artifact_origin(
        &self,
        descriptor_kind: &'static str,
        descriptor_id: &str,
        index: ArtifactOriginIndex,
    ) -> Result<ArtifactOrigin> {
        self.indexed_artifact_origin_ref(descriptor_kind, descriptor_id, index)
            .cloned()
    }

    /// Resolve one WAL fragment's exact physical namespace lifetime.
    pub(crate) fn fragment_origin(&self, fragment: &FragmentRef) -> Result<ArtifactOrigin> {
        match fragment.artifact_origin {
            Some(index) => {
                self.indexed_artifact_origin("fragment", &fragment.id.to_string(), index)
            }
            None => self.local_origin(),
        }
    }

    /// Resolve one immutable segment's exact physical namespace lifetime.
    pub(crate) fn segment_origin(&self, segment: &SegmentRef) -> Result<ArtifactOrigin> {
        match segment.artifact_origin {
            Some(index) => self.indexed_artifact_origin("segment", &segment.id, index),
            None => self.local_origin(),
        }
    }

    fn validate_origin_entry(&self, origin: &ArtifactOrigin, index: usize) -> Result<()> {
        NamespaceId::parse(origin.namespace.as_str().to_string()).map_err(|_| {
            self.artifact_origin_error(
                "manifest",
                "artifact_origins",
                u32::try_from(index).ok().map(ArtifactOriginIndex::new),
                None,
                Some(origin.clone()),
                "origin namespace violates the namespace grammar",
            )
        })?;
        if origin.incarnation.is_nil() {
            return Err(self.artifact_origin_error(
                "manifest",
                "artifact_origins",
                u32::try_from(index).ok().map(ArtifactOriginIndex::new),
                None,
                Some(origin.clone()),
                "origin namespace incarnation is nil",
            ));
        }
        Ok(())
    }

    fn validate_artifact_origin_table_len(&self, count: u64) -> Result<()> {
        if count <= u64::from(u32::MAX) {
            return Ok(());
        }
        Err(self.artifact_origin_error(
            "manifest",
            "artifact_origins",
            None,
            None,
            None,
            "origin table exceeds u32 address space",
        ))
    }

    fn validate_explicit_origin_key(
        &self,
        segment: &SegmentRef,
        origin: &ArtifactOrigin,
        key: &str,
    ) -> Result<()> {
        let prefix = format!("{}/", origin.namespace.as_str());
        if key.starts_with(&prefix) {
            return Ok(());
        }
        Err(self.artifact_origin_error(
            "segment",
            &segment.id,
            segment.artifact_origin,
            Some(key.to_string()),
            Some(origin.clone()),
            format!("explicit artifact key is outside expected prefix {prefix:?}"),
        ))
    }

    fn validate_artifact_origins_structural(&self, require_canonical_order: bool) -> Result<()> {
        let origin_count = u64::try_from(self.artifact_origins.len()).map_err(|_| {
            self.artifact_origin_error(
                "manifest",
                "artifact_origins",
                None,
                None,
                None,
                "origin table exceeds u32 address space",
            )
        })?;
        self.validate_artifact_origin_table_len(origin_count)?;

        let mut unique = BTreeSet::new();
        let mut previous = None;
        for (index, origin) in self.artifact_origins.iter().enumerate() {
            self.validate_origin_entry(origin, index)?;
            if require_canonical_order && !unique.insert(origin) {
                return Err(self.artifact_origin_error(
                    "manifest",
                    "artifact_origins",
                    u32::try_from(index).ok().map(ArtifactOriginIndex::new),
                    None,
                    Some(origin.clone()),
                    "duplicate artifact origin makes persisted indices ambiguous",
                ));
            }
            if require_canonical_order && previous.is_some_and(|previous| previous > origin) {
                return Err(self.artifact_origin_error(
                    "manifest",
                    "artifact_origins",
                    u32::try_from(index).ok().map(ArtifactOriginIndex::new),
                    None,
                    Some(origin.clone()),
                    "artifact origin table is not in canonical sorted order",
                ));
            }
            previous = Some(origin);
        }

        for fragment in &self.fragments {
            if fragment.artifact_origin.is_some() {
                self.fragment_origin(fragment)?;
            }
        }
        for segment in &self.segments {
            let origin = match segment.artifact_origin {
                Some(_) => self.segment_origin(segment)?,
                None if self.namespace.is_some() && self.namespace_incarnation.is_some() => {
                    self.local_origin()?
                }
                None => continue,
            };
            if let Some(sketch) = &segment.sketch {
                self.validate_explicit_origin_key(segment, &origin, &sketch.key)?;
            }
            if let Some(bootstrap) = &segment.bootstrap {
                self.validate_explicit_origin_key(segment, &origin, &bootstrap.key)?;
            }
            if let Some(membership) = &segment.membership {
                self.validate_explicit_origin_key(segment, &origin, &membership.key)?;
            }
            for object in &segment.cluster_objects {
                self.validate_explicit_origin_key(segment, &origin, &object.key)?;
            }
        }
        Ok(())
    }

    /// Validate the complete persisted origin table and every descriptor index.
    pub(crate) fn validate_artifact_origins(&self) -> Result<()> {
        self.validate_artifact_origins_structural(true)
    }

    fn validate_branch_lineage_state(&self, namespace: &str) -> Result<()> {
        let Some(lineage) = &self.branch_lineage else {
            return Ok(());
        };
        if self.namespace.as_deref() != Some(namespace) {
            return Err(BranchError::BranchRootInvalid {
                branch_id: Some(lineage.branch_id),
                reason: "lineage-bearing manifest is not bound to its target namespace".to_string(),
            }
            .into());
        }
        let Some(target_incarnation) = self.namespace_incarnation() else {
            return Err(BranchError::BranchRootInvalid {
                branch_id: Some(lineage.branch_id),
                reason: "lineage-bearing manifest has no target incarnation".to_string(),
            }
            .into());
        };
        if target_incarnation.is_nil() || lineage.parent_incarnation.is_nil() {
            return Err(BranchError::BranchRootInvalid {
                branch_id: Some(lineage.branch_id),
                reason: "branch lineage contains a nil namespace incarnation".to_string(),
            }
            .into());
        }
        if lineage.parent_namespace.as_str() == namespace {
            return Err(BranchError::BranchRootInvalid {
                branch_id: Some(lineage.branch_id),
                reason: "branch parent and target namespaces must be distinct".to_string(),
            }
            .into());
        }
        if lineage.depth == 0 {
            return Err(BranchError::BranchRootInvalid {
                branch_id: Some(lineage.branch_id),
                reason: "branch lineage depth must be greater than zero".to_string(),
            }
            .into());
        }

        let mut referenced = BTreeSet::new();
        for fragment in &self.fragments {
            if let Some(index) = fragment.artifact_origin {
                referenced.insert(index);
            }
        }
        for segment in &self.segments {
            if let Some(index) = segment.artifact_origin {
                referenced.insert(index);
            }
        }
        if self.artifact_origins.iter().enumerate().any(|(index, _)| {
            u32::try_from(index)
                .ok()
                .map(ArtifactOriginIndex::new)
                .is_none_or(|index| !referenced.contains(&index))
        }) {
            return Err(self.artifact_origin_error(
                "manifest",
                "artifact_origins",
                None,
                None,
                None,
                "branch origin table contains an owner unused by the visible view",
            ));
        }
        Ok(())
    }

    fn validate_foreign_origin_admission(&self) -> Result<()> {
        if !self
            .fragments
            .iter()
            .any(|fragment| fragment.artifact_origin.is_some())
            && !self
                .segments
                .iter()
                .any(|segment| segment.artifact_origin.is_some())
        {
            return Ok(());
        }
        let local = self.local_origin()?;
        let foreign_fragment = self.fragments.iter().find_map(|fragment| {
            fragment
                .artifact_origin
                .map(|_| fragment)
                .filter(|fragment| {
                    self.fragment_origin(fragment)
                        .is_ok_and(|origin| origin != local)
                })
        });
        let foreign_segment = self.segments.iter().find_map(|segment| {
            segment.artifact_origin.map(|_| segment).filter(|segment| {
                self.segment_origin(segment)
                    .is_ok_and(|origin| origin != local)
            })
        });
        if (foreign_fragment.is_some() || foreign_segment.is_some())
            && self.branch_lineage.is_none()
        {
            return Err(BranchError::BranchingNotReady {
                feature: "foreign artifact origin admission",
            }
            .into());
        }
        Ok(())
    }

    /// Canonicalize all resolved owners and remap descriptors in a second pass.
    #[allow(dead_code)] // Phase 05 calls this after collecting a fork's ultimate owners.
    pub(crate) fn canonicalize_artifact_origins(&mut self) -> Result<()> {
        self.validate_artifact_origins_structural(false)?;
        let fragment_origins = self
            .fragments
            .iter()
            .map(|fragment| self.fragment_origin(fragment))
            .collect::<Result<Vec<_>>>()?;
        let segment_origins = self
            .segments
            .iter()
            .map(|segment| self.segment_origin(segment))
            .collect::<Result<Vec<_>>>()?;

        let mut builder = ArtifactOriginSetBuilder::default();
        for origin in fragment_origins.iter().chain(&segment_origins) {
            builder.collect(origin.clone())?;
        }
        let canonical = builder.finish()?;

        let fragment_indices = self
            .fragments
            .iter()
            .zip(&fragment_origins)
            .map(|(fragment, origin)| {
                self.canonical_origin_index(
                    &canonical.indices,
                    "fragment",
                    fragment.id.to_string(),
                    origin,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let segment_indices = self
            .segments
            .iter()
            .zip(&segment_origins)
            .map(|(segment, origin)| {
                self.canonical_origin_index(
                    &canonical.indices,
                    "segment",
                    segment.id.clone(),
                    origin,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        for (fragment, index) in self.fragments.iter_mut().zip(fragment_indices) {
            fragment.artifact_origin = Some(index);
        }
        for (segment, index) in self.segments.iter_mut().zip(segment_indices) {
            segment.artifact_origin = Some(index);
        }
        self.artifact_origins = canonical.table;
        Ok(())
    }

    fn canonical_origin_index(
        &self,
        indices: &BTreeMap<ArtifactOrigin, ArtifactOriginIndex>,
        descriptor_kind: &'static str,
        descriptor_id: String,
        origin: &ArtifactOrigin,
    ) -> Result<ArtifactOriginIndex> {
        indices.get(origin).copied().ok_or_else(|| {
            self.artifact_origin_error(
                descriptor_kind,
                descriptor_id,
                None,
                None,
                Some(origin.clone()),
                "canonical origin table omitted a referenced owner",
            )
        })
    }

    /// Canonicalize only persisted explicit origins while preserving legacy
    /// local ownership as `None`.
    fn canonicalize_explicit_artifact_origins(&mut self) -> Result<()> {
        self.validate_artifact_origins_structural(false)?;
        let fragment_origins = self
            .fragments
            .iter()
            .map(|fragment| match fragment.artifact_origin {
                Some(_) => self.fragment_origin(fragment).map(Some),
                None => Ok(None),
            })
            .collect::<Result<Vec<_>>>()?;
        let segment_origins = self
            .segments
            .iter()
            .map(|segment| match segment.artifact_origin {
                Some(_) => self.segment_origin(segment).map(Some),
                None => Ok(None),
            })
            .collect::<Result<Vec<_>>>()?;

        let mut builder = ArtifactOriginSetBuilder::default();
        for origin in fragment_origins.iter().chain(&segment_origins).flatten() {
            builder.collect(origin.clone())?;
        }
        let canonical = builder.finish()?;

        let fragment_indices = self
            .fragments
            .iter()
            .zip(&fragment_origins)
            .map(|(fragment, origin)| {
                origin
                    .as_ref()
                    .map(|origin| {
                        self.canonical_origin_index(
                            &canonical.indices,
                            "fragment",
                            fragment.id.to_string(),
                            origin,
                        )
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        let segment_indices = self
            .segments
            .iter()
            .zip(&segment_origins)
            .map(|(segment, origin)| {
                origin
                    .as_ref()
                    .map(|origin| {
                        self.canonical_origin_index(
                            &canonical.indices,
                            "segment",
                            segment.id.clone(),
                            origin,
                        )
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;

        for (fragment, index) in self.fragments.iter_mut().zip(fragment_indices) {
            fragment.artifact_origin = index;
        }
        for (segment, index) in self.segments.iter_mut().zip(segment_indices) {
            segment.artifact_origin = index;
        }
        self.artifact_origins = canonical.table;
        Ok(())
    }

    /// Builds the live manifest key for a namespace.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Validated namespace path component used as the key prefix.
    ///
    /// # Returns
    ///
    /// `<namespace>/manifest.json`. The suffix is legacy naming; newly written
    /// contents are version-prefixed MessagePack, not JSON.
    pub fn s3_key(namespace: &str) -> String {
        format!("{namespace}/manifest.json")
    }

    /// Builds the object-store prefix for immutable manifest history.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace whose retained generations are addressed.
    ///
    /// # Returns
    ///
    /// `<namespace>/manifests/`, including the trailing slash.
    #[must_use]
    pub fn history_prefix(namespace: &str) -> String {
        format!("{namespace}/manifests/")
    }

    /// Builds the immutable key for a retained manifest generation.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace that owns the history object.
    /// - `version`: Persisted manifest generation.
    ///
    /// # Returns
    ///
    /// A `.msgpack` key with the generation zero-padded to 20 decimal digits so
    /// lexical key order matches numeric generation order.
    #[must_use]
    pub fn history_key(namespace: &str, version: u64) -> String {
        format!("{}{version:020}.msgpack", Self::history_prefix(namespace))
    }

    /// Returns this value's persisted manifest generation.
    ///
    /// Generation zero means the value has not yet been committed in its
    /// current namespace; committed manifests begin at generation one.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Borrow the authoritative direct-child root map.
    #[must_use]
    pub(crate) fn branch_roots(&self) -> &BTreeMap<BranchId, BranchRoot> {
        &self.branch_roots
    }

    /// Borrow the immutable direct-parent lineage carried by this namespace.
    #[must_use]
    pub(crate) fn branch_lineage(&self) -> Option<&BranchLineage> {
        self.branch_lineage.as_ref()
    }

    /// Return the distinct generations pinned by current live roots.
    ///
    /// Multiple children may pin one generation. Their exact source-manifest
    /// digest must agree; disagreement is persisted corruption, not a choice of
    /// one child over another.
    pub(crate) fn rooted_generations(
        &self,
    ) -> Result<BTreeMap<ManifestGeneration, ManifestDigest>> {
        let namespace = self
            .namespace
            .clone()
            .unwrap_or_else(|| "<unbound>".to_string());
        self.validate_branch_root_state(&namespace)?;
        let mut rooted = BTreeMap::new();
        for root in self.branch_roots.values() {
            match rooted.insert(root.source_generation, root.source_manifest_sha256) {
                Some(existing) if existing != root.source_manifest_sha256 => {
                    return Err(BranchError::ManifestDigestMismatch {
                        generation: root.source_generation,
                    }
                    .into());
                }
                Some(_) | None => {}
            }
        }
        Ok(rooted)
    }

    /// Verify one rooted history object against its exact stored bytes.
    ///
    /// This validates namespace, incarnation, persisted generation, and the
    /// SHA-256 named by every current root for that generation. It never hashes
    /// a decoded-and-reserialized manifest.
    pub(crate) fn validate_rooted_history_bytes(
        &self,
        generation: ManifestGeneration,
        bytes: &[u8],
    ) -> Result<()> {
        let expected = self
            .rooted_generations()?
            .get(&generation)
            .copied()
            .ok_or_else(|| BranchError::BranchRootInvalid {
                branch_id: None,
                reason: format!(
                    "generation {} is not named by the current live root map",
                    generation.get()
                ),
            })?;
        let namespace =
            self.namespace
                .as_deref()
                .ok_or_else(|| BranchError::BranchRootInvalid {
                    branch_id: None,
                    reason: "root-bearing manifest has no namespace binding".to_string(),
                })?;
        let history = ManifestHistoryRef {
            version: generation.get(),
            key: Self::history_key(namespace, generation.get()),
        };
        let decoded = Self::decode_history_body(bytes, namespace, &history)?;
        if decoded.namespace_incarnation != self.namespace_incarnation {
            return Err(BranchError::BranchRootInvalid {
                branch_id: None,
                reason: format!(
                    "rooted history generation {} belongs to a different namespace incarnation",
                    generation.get()
                ),
            }
            .into());
        }
        let actual = ManifestDigest::new(Sha256::digest(bytes).into());
        if actual != expected {
            return Err(BranchError::ManifestDigestMismatch { generation }.into());
        }
        Ok(())
    }

    /// Insert one exact live-head root candidate.
    ///
    /// Returns `false` for an exact idempotent retry and `true` when the map was
    /// changed. A caller must still publish this candidate with the ETag and
    /// fencing token bound to the same source-head observation.
    #[allow(dead_code)] // Phase 04 root primitive is the first production caller.
    pub(crate) fn insert_branch_root_candidate(
        &mut self,
        root: BranchRoot,
        max_children: usize,
    ) -> Result<bool> {
        let namespace = self
            .namespace
            .clone()
            .unwrap_or_else(|| "<unbound>".to_string());
        self.validate_branch_root_state(&namespace)?;
        if let Some(existing) = self.branch_roots.get(&root.branch_id) {
            if existing == &root {
                return Ok(false);
            }
            return Err(BranchError::BranchRootConflict {
                branch_id: root.branch_id,
            }
            .into());
        }
        if self.version == 0 || root.source_generation.get() != self.version {
            return Err(BranchError::BranchRootInvalid {
                branch_id: Some(root.branch_id),
                reason: format!(
                    "source generation {} does not equal live generation {}",
                    root.source_generation.get(),
                    self.version
                ),
            }
            .into());
        }
        if self.branch_roots.len() >= max_children {
            return Err(BranchError::BranchRootLimitExceeded {
                limit: max_children,
            }
            .into());
        }

        let branch_id = root.branch_id;
        self.branch_roots.insert(branch_id, root);
        if let Err(error) = self.validate_branch_root_state(&namespace) {
            self.branch_roots.remove(&branch_id);
            return Err(error);
        }
        Ok(true)
    }

    /// Remove only one exact root body from this candidate.
    #[allow(dead_code)] // Phase 07 lifecycle removal is the first production caller.
    pub(crate) fn remove_branch_root_candidate(&mut self, expected: &BranchRoot) -> Result<()> {
        let namespace = self
            .namespace
            .clone()
            .unwrap_or_else(|| "<unbound>".to_string());
        self.validate_branch_root_state(&namespace)?;
        match self.branch_roots.get(&expected.branch_id) {
            Some(actual) if actual == expected => {}
            Some(_) => {
                return Err(BranchError::BranchRootConflict {
                    branch_id: expected.branch_id,
                }
                .into());
            }
            None => {
                return Err(BranchError::BranchRootMissing {
                    branch_id: expected.branch_id,
                }
                .into());
            }
        }
        self.branch_roots.remove(&expected.branch_id);
        self.validate_branch_root_state(&namespace)
    }

    /// Return the writer-fencing generation bound into the signed root.
    #[must_use]
    pub const fn fencing_token(&self) -> u64 {
        self.fencing_token
    }

    /// Return the fully hashed immutable inventory used for receipt proofs.
    pub fn receipt_artifacts(
        &self,
        namespace: &str,
    ) -> std::result::Result<&BTreeMap<String, [u8; 32]>, crate::security::SecurityError> {
        if self.segments.iter().any(|segment| {
            segment.hierarchical && self.hierarchical_routing_nodes(&segment.id).is_empty()
        }) {
            return Err(crate::security::SecurityError::ReceiptsUnavailableUnhashed);
        }
        let reachable = self
            .receipt_reachable_keys(namespace)
            .map_err(|error| crate::security::SecurityError::InvalidReceipt(error.to_string()))?;
        if reachable.len() != self.artifact_hashes.len()
            || reachable
                .iter()
                .any(|key| !self.artifact_hashes.contains_key(key))
        {
            return Err(crate::security::SecurityError::ReceiptsUnavailableUnhashed);
        }
        Ok(&self.artifact_hashes)
    }

    /// Return whether an explicit compaction must upgrade receipt metadata.
    #[must_use]
    pub(crate) fn receipt_upgrade_needed(&self, namespace: &str) -> bool {
        self.receipt_artifacts(namespace).is_err()
            || self.merkle_root.is_none()
            || self.root_signature.is_none()
            || self.root_signer_node.is_none()
            || self.receipt_binding_version.is_none()
            || self.recompute_receipt_state_digest(namespace).ok() != self.receipt_state_digest
            || (matches!(
                self.receipt_binding_version,
                Some(ReceiptBindingVersion::V3Roots | ReceiptBindingVersion::V4Lineage)
            ) && self.recompute_control_state_digest(namespace).ok()
                != self.control_state_digest)
    }

    /// Read and hash every currently reachable immutable artifact missing from
    /// a legacy manifest's receipt inventory.
    ///
    /// This is called only by an explicit compaction upgrade. Query execution
    /// never performs backfill I/O and therefore continues to fail closed until
    /// the upgraded generation is CAS-published.
    pub(crate) async fn hydrate_receipt_artifacts(
        &mut self,
        store: &ZeppelinStore,
        namespace: &str,
    ) -> Result<()> {
        let incarnation = self.namespace_incarnation().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "manifest for namespace {namespace} has no incarnation for receipt hydration"
            ))
        })?;
        let local_origin = ArtifactOrigin {
            namespace: NamespaceId::parse(namespace.to_string()).map_err(|_| {
                ZeppelinError::Validation(format!(
                    "namespace violates artifact-origin grammar: {namespace}"
                ))
            })?,
            incarnation: NamespaceIncarnationId::from_uuid(incarnation),
        };
        let hierarchical_segments = self
            .artifact_origin_resolver(&local_origin)?
            .located_segments()?
            .into_iter()
            .filter(|located| {
                located.segment.hierarchical
                    && self
                        .hierarchical_routing_nodes(&located.segment.id)
                        .is_empty()
            })
            .map(|located| {
                (
                    located.segment.id.clone(),
                    located.physical_namespace().to_string(),
                )
            })
            .collect::<Vec<_>>();
        for (segment_id, physical_namespace) in hierarchical_segments {
            let node_ids = crate::index::hierarchical::build::discover_hierarchical_routing_nodes(
                store,
                &physical_namespace,
                &segment_id,
            )
            .await?;
            if node_ids.is_empty() {
                return Err(ZeppelinError::Index(format!(
                    "hierarchical segment {} has no routing-node inventory",
                    segment_id
                )));
            }
            self.set_hierarchical_routing_nodes(&segment_id, node_ids);
        }
        let reachable = self.receipt_reachable_keys(namespace)?;
        self.artifact_hashes
            .retain(|key, _| reachable.contains(key));
        for key in reachable {
            if let std::collections::btree_map::Entry::Vacant(entry) =
                self.artifact_hashes.entry(key)
            {
                // Fresh artifacts already have an exact hash computed beside
                // their successful PUT. Reuse it so ordinary compaction keeps
                // the Phase 10 zero-extra-I/O contract. Only retained legacy
                // artifacts unknown to this process require a storage read.
                if let Some(content_hash) = store.known_content_hash(entry.key()) {
                    entry.insert(content_hash);
                } else {
                    let body = store.get(entry.key()).await?;
                    entry.insert(<[u8; 32]>::from(Sha256::digest(&body)));
                }
            }
        }
        self.merkle_root = None;
        self.root_signature = None;
        self.root_signer_node = None;
        self.receipt_state_digest = None;
        Ok(())
    }

    /// Rewrite exact receipt inventory keys after byte-identical clone copies.
    pub(crate) fn rewrite_receipt_artifacts_for_clone(
        &mut self,
        source: &str,
        target: &str,
    ) -> Result<()> {
        let source_prefix = format!("{source}/");
        let mut rewritten = BTreeMap::new();
        for (key, content_hash) in std::mem::take(&mut self.artifact_hashes) {
            let suffix = key.strip_prefix(&source_prefix).ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "clone receipt artifact key {key:?} is outside source prefix {source_prefix:?}"
                ))
            })?;
            rewritten.insert(format!("{target}/{suffix}"), content_hash);
        }
        self.artifact_hashes = rewritten;
        self.merkle_root = None;
        self.root_signature = None;
        self.root_signer_node = None;
        self.receipt_state_digest = None;
        self.receipt_binding_version = None;
        Ok(())
    }

    /// Return the canonical root carried by this manifest generation.
    #[must_use]
    pub const fn merkle_root(&self) -> Option<[u8; 32]> {
        self.merkle_root
    }

    /// Return the canonical query-routing state digest carried by this generation.
    #[must_use]
    pub const fn receipt_state_digest(&self) -> Option<[u8; 32]> {
        self.receipt_state_digest
    }

    /// Return the stable projection version carried by this generation.
    #[must_use]
    pub const fn receipt_binding_version(&self) -> Option<ReceiptBindingVersion> {
        self.receipt_binding_version
    }

    /// Return the versioned retention/lineage control digest, when published.
    #[must_use]
    pub const fn control_state_digest(&self) -> Option<[u8; 32]> {
        self.control_state_digest
    }

    /// Recompute the domain-separated query-routing projection digest.
    pub(crate) fn recompute_receipt_state_digest(&self, namespace: &str) -> Result<[u8; 32]> {
        let binding_version = self.receipt_binding_version.ok_or_else(|| {
            ZeppelinError::Serialization(
                "manifest receipt binding version is unavailable".to_string(),
            )
        })?;
        self.compute_receipt_state_digest(namespace, binding_version)
    }

    /// Recompute the exact V3 roots/fence control digest.
    pub(crate) fn recompute_control_state_digest(&self, namespace: &str) -> Result<[u8; 32]> {
        match self.receipt_binding_version {
            Some(ReceiptBindingVersion::V3Roots) => self.compute_control_roots_digest(namespace),
            Some(ReceiptBindingVersion::V4Lineage) => self.compute_control_branch_digest(namespace),
            _ => Err(ZeppelinError::Serialization(
                "manifest control digest requires receipt binding v3_roots or v4_lineage"
                    .to_string(),
            )),
        }
    }

    /// Seal a feature-only synthetic foreign view with a valid v2 projection.
    ///
    /// This only makes integration-fixture bytes structurally well-formed so
    /// normal persisted decoding reaches the independent foreign-origin
    /// admission gate. It does not publish or authorize the manifest.
    #[cfg(feature = "branching-test-support")]
    pub(crate) fn bind_synthetic_origin_receipt_for_test_support(
        &mut self,
        namespace: &str,
    ) -> Result<()> {
        self.receipt_binding_version = Some(ReceiptBindingVersion::V2Origins);
        self.receipt_state_digest =
            Some(self.compute_receipt_state_digest(namespace, ReceiptBindingVersion::V2Origins)?);
        Ok(())
    }

    fn compute_receipt_state_digest(
        &self,
        namespace: &str,
        binding_version: ReceiptBindingVersion,
    ) -> Result<[u8; 32]> {
        self.validate_namespace_binding(namespace)?;
        let bytes = match binding_version {
            ReceiptBindingVersion::V1 => serde_json::to_vec(&self.execution_binding_v1(namespace)),
            ReceiptBindingVersion::V2Origins
            | ReceiptBindingVersion::V3Roots
            | ReceiptBindingVersion::V4Lineage => {
                self.validate_artifact_origins()?;
                serde_json::to_vec(&self.execution_binding_v2(namespace))
            }
        };
        let bytes = bytes.map_err(|error| {
            ZeppelinError::Serialization(format!(
                "manifest execution binding serialization failed: {error}"
            ))
        })?;
        Ok(Sha256::digest(bytes).into())
    }

    fn compute_control_roots_digest(&self, namespace: &str) -> Result<[u8; 32]> {
        self.validate_namespace_binding(namespace)?;
        self.validate_branch_root_state(namespace)?;
        let bytes = serde_json::to_vec(&ControlRootsV1 {
            namespace,
            incarnation: self.namespace_incarnation.map(|incarnation| incarnation.0),
            deletion_fence: self.deletion_fence.as_ref(),
            branch_roots: &self.branch_roots,
        })
        .map_err(|error| {
            ZeppelinError::Serialization(format!(
                "manifest roots control binding serialization failed: {error}"
            ))
        })?;
        Ok(Sha256::digest(bytes).into())
    }

    fn compute_control_branch_digest(&self, namespace: &str) -> Result<[u8; 32]> {
        self.validate_namespace_binding(namespace)?;
        self.validate_branch_root_state(namespace)?;
        self.validate_branch_lineage_state(namespace)?;
        let lineage = self.branch_lineage.as_ref().ok_or_else(|| {
            ZeppelinError::Serialization(
                "receipt binding v4_lineage requires immutable branch lineage".to_string(),
            )
        })?;
        let bytes = serde_json::to_vec(&ControlBranchV2 {
            namespace,
            incarnation: self.namespace_incarnation.map(|incarnation| incarnation.0),
            deletion_fence: self.deletion_fence.as_ref(),
            branch_roots: &self.branch_roots,
            branch_lineage: lineage,
        })
        .map_err(|error| {
            ZeppelinError::Serialization(format!(
                "manifest branch control binding serialization failed: {error}"
            ))
        })?;
        Ok(Sha256::digest(bytes).into())
    }

    fn execution_binding_v2<'a>(&'a self, namespace: &'a str) -> ManifestExecutionBindingV2<'a> {
        let fragments = self
            .fragments
            .iter()
            .map(|fragment| FragmentExecutionBindingV2 {
                id: fragment.id.to_string(),
                vector_count: fragment.vector_count,
                delete_count: fragment.delete_count,
                sequence_number: fragment.sequence_number,
                size_bytes: fragment.size_bytes,
                artifact_origin: fragment.artifact_origin.map(ArtifactOriginIndex::get),
            })
            .collect();
        let segments = self
            .segments
            .iter()
            .map(|segment| SegmentExecutionBindingV2 {
                id: &segment.id,
                vector_count: segment.vector_count,
                cluster_count: segment.cluster_count,
                quantization: match segment.quantization {
                    crate::index::quantization::QuantizationType::None => "none",
                    crate::index::quantization::QuantizationType::Scalar => "scalar",
                    crate::index::quantization::QuantizationType::TwoBit => "two_bit",
                    crate::index::quantization::QuantizationType::Product => "product",
                },
                hierarchical: segment.hierarchical,
                bitmap_fields: &segment.bitmap_fields,
                fts_fields: &segment.fts_fields,
                has_global_fts: segment.has_global_fts,
                cluster_owners: &segment.cluster_owners,
                sketch: segment
                    .sketch
                    .as_ref()
                    .map(|sketch| SketchExecutionBindingV1 {
                        key: &sketch.key,
                        version: sketch.version,
                        code_dims: sketch.code_dims,
                        bytes_per_vector: sketch.bytes_per_vector,
                        size_bytes: sketch.size_bytes,
                        rotation_seed: sketch.rotation_seed,
                    }),
                cluster_objects: segment
                    .cluster_objects
                    .iter()
                    .map(|object| ClusterObjectExecutionBindingV1 {
                        key: &object.key,
                        clusters: &object.clusters,
                        live_offset: object.live_offset,
                        live_len: object.live_len,
                        size_bytes: object.size_bytes,
                    })
                    .collect(),
                bootstrap: segment.bootstrap.as_ref().map(|bootstrap| {
                    BootstrapExecutionBindingV1 {
                        key: &bootstrap.key,
                        size_bytes: bootstrap.size_bytes,
                    }
                }),
                artifact_origin: segment.artifact_origin.map(ArtifactOriginIndex::get),
            })
            .collect();
        let hierarchical_routing_nodes = self
            .hierarchical_routing_nodes
            .iter()
            .map(
                |(segment_id, node_ids)| HierarchicalRoutingExecutionBindingV1 {
                    segment_id,
                    node_ids,
                },
            )
            .collect();
        let artifact_origins = self
            .artifact_origins
            .iter()
            .map(|origin| ArtifactOriginExecutionBindingV2 {
                namespace: origin.namespace.as_str(),
                incarnation: *origin.incarnation.as_uuid().as_bytes(),
            })
            .collect();

        ManifestExecutionBindingV2 {
            format: "zeppelin-manifest-execution-v2-origins",
            namespace,
            namespace_incarnation: self.namespace_incarnation.map(|incarnation| incarnation.0),
            fragments,
            segments,
            active_segment: self.active_segment.as_deref(),
            hierarchical_routing_nodes,
            artifact_origins,
        }
    }

    fn execution_binding_v1<'a>(&'a self, namespace: &'a str) -> ManifestExecutionBindingV1<'a> {
        let fragments = self
            .fragments
            .iter()
            .map(|fragment| FragmentExecutionBindingV1 {
                id: fragment.id.to_string(),
                vector_count: fragment.vector_count,
                delete_count: fragment.delete_count,
                sequence_number: fragment.sequence_number,
                size_bytes: fragment.size_bytes,
            })
            .collect();
        let segments = self
            .segments
            .iter()
            .map(|segment| SegmentExecutionBindingV1 {
                id: &segment.id,
                vector_count: segment.vector_count,
                cluster_count: segment.cluster_count,
                quantization: match segment.quantization {
                    crate::index::quantization::QuantizationType::None => "none",
                    crate::index::quantization::QuantizationType::Scalar => "scalar",
                    crate::index::quantization::QuantizationType::TwoBit => "two_bit",
                    crate::index::quantization::QuantizationType::Product => "product",
                },
                hierarchical: segment.hierarchical,
                bitmap_fields: &segment.bitmap_fields,
                fts_fields: &segment.fts_fields,
                has_global_fts: segment.has_global_fts,
                cluster_owners: &segment.cluster_owners,
                sketch: segment
                    .sketch
                    .as_ref()
                    .map(|sketch| SketchExecutionBindingV1 {
                        key: &sketch.key,
                        version: sketch.version,
                        code_dims: sketch.code_dims,
                        bytes_per_vector: sketch.bytes_per_vector,
                        size_bytes: sketch.size_bytes,
                        rotation_seed: sketch.rotation_seed,
                    }),
                cluster_objects: segment
                    .cluster_objects
                    .iter()
                    .map(|object| ClusterObjectExecutionBindingV1 {
                        key: &object.key,
                        clusters: &object.clusters,
                        live_offset: object.live_offset,
                        live_len: object.live_len,
                        size_bytes: object.size_bytes,
                    })
                    .collect(),
                bootstrap: segment.bootstrap.as_ref().map(|bootstrap| {
                    BootstrapExecutionBindingV1 {
                        key: &bootstrap.key,
                        size_bytes: bootstrap.size_bytes,
                    }
                }),
            })
            .collect();
        let hierarchical_routing_nodes = self
            .hierarchical_routing_nodes
            .iter()
            .map(
                |(segment_id, node_ids)| HierarchicalRoutingExecutionBindingV1 {
                    segment_id,
                    node_ids,
                },
            )
            .collect();

        ManifestExecutionBindingV1 {
            format: "zeppelin-manifest-execution-v1",
            namespace,
            namespace_incarnation: self.namespace_incarnation.map(|incarnation| incarnation.0),
            fragments,
            segments,
            active_segment: self.active_segment.as_deref(),
            hierarchical_routing_nodes,
        }
    }

    /// Borrow the node signature carried by this manifest generation.
    #[must_use]
    pub fn root_signature(&self) -> Option<&[u8]> {
        self.root_signature.as_deref()
    }

    /// Borrow the published signer identity carried by this manifest generation.
    #[must_use]
    pub fn root_signer_node(&self) -> Option<&str> {
        self.root_signer_node.as_deref()
    }

    /// Record the exact routing-node inventory produced for one segment.
    pub(crate) fn set_hierarchical_routing_nodes(
        &mut self,
        segment_id: &str,
        mut node_ids: Vec<String>,
    ) {
        node_ids.sort();
        node_ids.dedup();
        self.hierarchical_routing_nodes
            .insert(segment_id.to_string(), node_ids);
    }

    /// Borrow the manifest-owned routing-node IDs for one hierarchical segment.
    #[must_use]
    pub(crate) fn hierarchical_routing_nodes(&self, segment_id: &str) -> &[String] {
        self.hierarchical_routing_nodes
            .get(segment_id)
            .map_or(&[], Vec::as_slice)
    }

    fn receipt_reachable_keys(&self, namespace: &str) -> Result<BTreeSet<String>> {
        let mut reachable = crate::compaction::gc::reachable_keys(namespace, self)?;
        let located_segments = match self.namespace_incarnation() {
            Some(incarnation) => {
                let local_origin = ArtifactOrigin {
                    namespace: NamespaceId::parse(namespace.to_string()).map_err(|_| {
                        ZeppelinError::Validation(format!(
                            "namespace violates artifact-origin grammar: {namespace}"
                        ))
                    })?,
                    incarnation: NamespaceIncarnationId::from_uuid(incarnation),
                };
                self.artifact_origin_resolver(&local_origin)?
                    .located_segments()?
                    .into_iter()
                    .map(|located| {
                        (
                            located.segment.clone(),
                            located.physical_namespace().to_string(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
            None if self.has_explicit_artifact_origins() => {
                return Err(self.artifact_origin_error(
                    "manifest",
                    "namespace_incarnation",
                    None,
                    None,
                    None,
                    "explicit artifact origins require a local namespace incarnation",
                ));
            }
            // Pre-incarnation manifests cannot contain an origin table or
            // descriptor indices. Their immutable layout is therefore
            // unambiguously namespace-local. Preserve that wire-compatible
            // receipt inventory without fabricating a namespace-lifetime ID.
            None => self
                .segments
                .iter()
                .map(|segment| (segment.clone(), namespace.to_string()))
                .collect::<Vec<_>>(),
        };
        for (segment, physical_namespace) in located_segments {
            let physical_namespace = physical_namespace.as_str();
            if segment.hierarchical {
                for node_id in self.hierarchical_routing_nodes(&segment.id) {
                    reachable.insert(crate::index::hierarchical::tree_node_key(
                        physical_namespace,
                        &segment.id,
                        node_id,
                    ));
                }
            }
            if segment.has_global_fts {
                for cluster_idx in 0..segment.cluster_count {
                    reachable.remove(&crate::fts::inverted_index::fts_index_key(
                        physical_namespace,
                        segment.cluster_owner(cluster_idx),
                        cluster_idx,
                    ));
                }
            }
            if segment.quantization != crate::index::quantization::QuantizationType::Scalar {
                continue;
            }

            // Hierarchical SQ embeds calibration in tree_meta.json and codes
            // in each ordinary cluster object. The conservative GC inventory
            // protects legacy sidecar names too, but receipts bind only real
            // published objects.
            if segment.hierarchical {
                reachable.remove(&crate::index::quantization::sq::sq_calibration_key(
                    physical_namespace,
                    &segment.id,
                ));
                for cluster_idx in 0..segment.cluster_count {
                    reachable.remove(&crate::index::quantization::sq::sq_cluster_key(
                        physical_namespace,
                        segment.cluster_owner(cluster_idx),
                        cluster_idx,
                    ));
                }
                continue;
            }

            // GC deliberately protects every legacy SQ sidecar that might
            // exist, even when a newer manifest proves that the equivalent SQ
            // bytes are embedded in a bootstrap or co-located cluster object.
            // Receipts need the exact published artifact inventory instead of
            // that conservative sweep superset, otherwise they would commit
            // nonexistent keys. A carried cluster from a pre-co-location
            // segment keeps its old standalone sidecar; rewritten and grouped
            // clusters bind only the object that actually contains the codes.
            if segment.bootstrap.is_some() {
                reachable.remove(&crate::index::quantization::sq::sq_calibration_key(
                    physical_namespace,
                    &segment.id,
                ));
            }
            for cluster_idx in 0..segment.cluster_count {
                let owner = segment.cluster_owner(cluster_idx);
                let codes_are_colocated = !segment.cluster_objects.is_empty()
                    || (segment.bootstrap.is_some() && owner == segment.id);
                if codes_are_colocated {
                    reachable.remove(&crate::index::quantization::sq::sq_cluster_key(
                        physical_namespace,
                        owner,
                        cluster_idx,
                    ));
                }
            }
        }
        for pending in &self.pending_deletes {
            reachable.remove(pending);
        }
        Ok(reachable)
    }

    fn finalize_receipt_root(&mut self, store: &ZeppelinStore, namespace: &str) -> Result<()> {
        self.validate_branch_root_state(namespace)?;
        self.canonicalize_explicit_artifact_origins()?;
        self.validate_artifact_origins()?;
        self.validate_branch_lineage_state(namespace)?;
        self.validate_foreign_origin_admission()?;
        let reachable = self.receipt_reachable_keys(namespace)?;
        self.artifact_hashes
            .retain(|key, _| reachable.contains(key));
        for key in &reachable {
            if !self.artifact_hashes.contains_key(key) {
                if let Some(content_hash) = store.known_content_hash(key) {
                    self.artifact_hashes.insert(key.clone(), content_hash);
                }
            }
        }

        let has_roots_control = self.deletion_fence.is_some() || !self.branch_roots.is_empty();
        let has_lineage_control = self.branch_lineage.is_some();
        let binding_version = match self.receipt_binding_version {
            Some(ReceiptBindingVersion::V4Lineage) => ReceiptBindingVersion::V4Lineage,
            _ if has_lineage_control => ReceiptBindingVersion::V4Lineage,
            Some(ReceiptBindingVersion::V3Roots) => ReceiptBindingVersion::V3Roots,
            _ if has_roots_control => ReceiptBindingVersion::V3Roots,
            Some(ReceiptBindingVersion::V2Origins) => ReceiptBindingVersion::V2Origins,
            Some(ReceiptBindingVersion::V1) | None if self.has_explicit_artifact_origins() => {
                ReceiptBindingVersion::V2Origins
            }
            Some(ReceiptBindingVersion::V1) | None => ReceiptBindingVersion::V1,
        };
        let state_digest = self.compute_receipt_state_digest(namespace, binding_version)?;
        self.receipt_state_digest = Some(state_digest);
        self.receipt_binding_version = Some(binding_version);
        self.control_state_digest = match binding_version {
            ReceiptBindingVersion::V3Roots => Some(self.compute_control_roots_digest(namespace)?),
            ReceiptBindingVersion::V4Lineage => {
                Some(self.compute_control_branch_digest(namespace)?)
            }
            ReceiptBindingVersion::V1 | ReceiptBindingVersion::V2Origins => None,
        };

        if reachable
            .iter()
            .any(|key| !self.artifact_hashes.contains_key(key))
        {
            self.merkle_root = None;
            self.root_signature = None;
            self.root_signer_node = None;
            return Ok(());
        }

        let root = crate::security::MerkleTree::build(&self.artifact_hashes)?.root();
        self.merkle_root = Some(root);
        let payload = manifest_root_signing_bytes(
            root,
            self.version,
            self.fencing_token,
            binding_version,
            state_digest,
            self.control_state_digest,
        )?;
        if let Some((signer_node, signature)) = store.sign_object(&payload)? {
            self.root_signer_node = Some(signer_node);
            self.root_signature = Some(signature);
        } else {
            self.root_signer_node = None;
            self.root_signature = None;
        }
        Ok(())
    }

    /// Resets the persisted generation before cloning into another namespace.
    ///
    /// The content remains unchanged, but the destination must establish its
    /// own generation history instead of inheriting the source namespace's
    /// counter.
    ///
    /// # Side Effects
    ///
    /// Mutates only this in-memory value; no object-store request is made.
    ///
    /// # Examples
    ///
    /// Cloning source generation 42 resets it to zero, then
    /// [`Manifest::write`] publishes destination generation 1.
    ///
    pub fn reset_version_for_clone(&mut self) {
        self.version = 0;
        self.namespace = None;
        self.namespace_incarnation = None;
        self.clear_branch_control_for_new_namespace();
    }

    /// Rebinds byte-copied source artifacts to implicit target-local ownership.
    ///
    /// Raw clone copies every retained immutable object beneath the target
    /// namespace prefix. Explicit origin indices that named the source's own
    /// incarnation must therefore not survive into the target manifest: after
    /// the copy, `None` is the canonical representation of target-local
    /// ownership. Foreign descriptors are rejected because their bytes were
    /// not proven to have been copied from the source namespace prefix.
    pub(crate) fn normalize_copy_clone_artifact_ownership(&mut self) -> Result<()> {
        self.validate_artifact_origins()?;
        let local = self.local_origin()?;
        for fragment in &self.fragments {
            if self.fragment_origin(fragment)? != local {
                return Err(ZeppelinError::Validation(format!(
                    "copy clone fragment {} is not owned by the source namespace incarnation",
                    fragment.id
                )));
            }
        }
        for segment in &self.segments {
            if self.segment_origin(segment)? != local {
                return Err(ZeppelinError::Validation(format!(
                    "copy clone segment {} is not owned by the source namespace incarnation",
                    segment.id
                )));
            }
        }

        for fragment in &mut self.fragments {
            fragment.artifact_origin = None;
        }
        for segment in &mut self.segments {
            segment.artifact_origin = None;
        }
        self.artifact_origins.clear();
        Ok(())
    }

    /// Clear source-owned branch control before binding a different namespace.
    ///
    /// This is the only intentional root-map clearing path outside exact root
    /// removal. A clone starts a fresh namespace lifetime, so retaining V3's
    /// control binding would incorrectly pin or sign the source's child graph.
    pub(crate) fn clear_branch_control_for_new_namespace(&mut self) {
        self.deletion_fence = None;
        self.branch_roots.clear();
        self.branch_lineage = None;
        self.control_state_digest = None;
        if matches!(
            self.receipt_binding_version,
            Some(ReceiptBindingVersion::V3Roots | ReceiptBindingVersion::V4Lineage)
        ) {
            self.receipt_binding_version = None;
            self.receipt_state_digest = None;
            self.root_signature = None;
            self.root_signer_node = None;
        }
    }

    /// Normalize one exact source head into an unpublished zero-copy target.
    pub(crate) fn prepare_zero_copy_fork(
        source: &Manifest,
        source_identity: &ArtifactOrigin,
        target_identity: &ArtifactOrigin,
        lineage_seed: BranchLineageSeed,
        now: DateTime<Utc>,
    ) -> Result<PreparedZeroCopyFork> {
        source.validate_namespace_binding(source_identity.namespace.as_str())?;
        source.validate_branch_root_state(source_identity.namespace.as_str())?;
        source.validate_branch_lineage_state(source_identity.namespace.as_str())?;
        source.validate_artifact_origins()?;
        source.validate_receipt_binding_state(source_identity.namespace.as_str())?;
        source.validate_foreign_origin_admission()?;
        if source.namespace.as_deref() != Some(source_identity.namespace.as_str())
            || source.namespace_incarnation() != Some(source_identity.incarnation.as_uuid())
        {
            return Err(BranchError::ArtifactOriginInvalid {
                manifest_namespace: source_identity.namespace.to_string(),
                manifest_incarnation: source
                    .namespace_incarnation()
                    .map(NamespaceIncarnationId::from_uuid),
                descriptor_kind: "manifest",
                descriptor_id: "source_identity".to_string(),
                offending_index: None,
                offending_key: None,
                expected_origin: Some(source_identity.clone()),
                reason: "fork source manifest is not bound to the authoritative source identity"
                    .to_string(),
            }
            .into());
        }
        if source.version == 0
            || source.version != lineage_seed.fork_generation.get()
            || lineage_seed.parent_namespace != source_identity.namespace
            || lineage_seed.parent_incarnation != source_identity.incarnation
        {
            return Err(BranchError::BranchRootInvalid {
                branch_id: Some(lineage_seed.branch_id),
                reason: "fork lineage seed does not name the exact source manifest head"
                    .to_string(),
            }
            .into());
        }
        if source.deletion_fence.is_some() {
            return Err(ZeppelinError::NamespaceDeleting {
                namespace: source_identity.namespace.to_string(),
            });
        }
        if source_identity.namespace == target_identity.namespace
            || source_identity.incarnation.is_nil()
            || target_identity.incarnation.is_nil()
            || lineage_seed.depth == 0
        {
            return Err(BranchError::BranchRootInvalid {
                branch_id: Some(lineage_seed.branch_id),
                reason: "fork source, target, depth, or incarnation is invalid".to_string(),
            }
            .into());
        }

        let resolver = source.artifact_origin_resolver(source_identity)?;
        let mut fragments = resolver
            .located_fragments()?
            .into_iter()
            .map(|located| {
                (
                    located.fragment.clone(),
                    located.physical_origin.as_origin().clone(),
                )
            })
            .collect::<Vec<_>>();
        fragments.sort_by_key(|(fragment, _)| fragment.sequence_number);

        let active = resolver.active_located_segment()?.map(|located| {
            (
                located.segment.clone(),
                located.physical_origin.as_origin().clone(),
            )
        });
        let mut origins = ArtifactOriginSetBuilder::default();
        for (_, origin) in &fragments {
            origins.collect(origin.clone())?;
        }
        if let Some((_, origin)) = &active {
            origins.collect(origin.clone())?;
        }
        let origins = origins.finish()?;

        let mut target = Manifest::new_at(now);
        target.fragments = fragments
            .into_iter()
            .map(|(mut fragment, origin)| {
                fragment.artifact_origin =
                    Some(origins.indices.get(&origin).copied().ok_or_else(|| {
                        BranchError::ArtifactOriginInvalid {
                            manifest_namespace: target_identity.namespace.to_string(),
                            manifest_incarnation: Some(target_identity.incarnation.clone()),
                            descriptor_kind: "fragment",
                            descriptor_id: fragment.id.to_string(),
                            offending_index: None,
                            offending_key: None,
                            expected_origin: Some(origin),
                            reason: "canonical fork origin table omitted a visible fragment owner"
                                .to_string(),
                        }
                    })?);
                Ok(fragment)
            })
            .collect::<std::result::Result<Vec<_>, BranchError>>()?;
        if let Some((mut segment, origin)) = active {
            segment.artifact_origin =
                Some(origins.indices.get(&origin).copied().ok_or_else(|| {
                    BranchError::ArtifactOriginInvalid {
                        manifest_namespace: target_identity.namespace.to_string(),
                        manifest_incarnation: Some(target_identity.incarnation.clone()),
                        descriptor_kind: "segment",
                        descriptor_id: segment.id.clone(),
                        offending_index: None,
                        offending_key: None,
                        expected_origin: Some(origin),
                        reason: "canonical fork origin table omitted the active segment owner"
                            .to_string(),
                    }
                })?);
            target.active_segment = Some(segment.id.clone());
            if let Some(nodes) = source.hierarchical_routing_nodes.get(&segment.id) {
                target
                    .hierarchical_routing_nodes
                    .insert(segment.id.clone(), nodes.clone());
            }
            target.segments.push(segment);
        }
        target.next_sequence = source.next_sequence;
        target.namespace = Some(target_identity.namespace.to_string());
        target.bind_namespace_incarnation(target_identity.incarnation.as_uuid())?;
        target.artifact_origins = origins.table;
        target.artifact_hashes = source.artifact_hashes.clone();
        target.validate_artifact_origins()?;
        let reachable = target.receipt_reachable_keys(target_identity.namespace.as_str())?;
        target
            .artifact_hashes
            .retain(|key, _| reachable.contains(key));
        if reachable
            .iter()
            .any(|key| !target.artifact_hashes.contains_key(key))
        {
            return Err(ZeppelinError::Serialization(
                "zero-copy fork source is missing a reachable artifact hash".to_string(),
            ));
        }

        let fork_view_sha256 = target.compute_initial_fork_view_digest(
            source_identity,
            target_identity,
            &lineage_seed,
        )?;
        let lineage = lineage_seed.with_fork_view(fork_view_sha256);
        target.branch_lineage = Some(lineage.clone());
        target.validate_branch_lineage_state(target_identity.namespace.as_str())?;
        target.validate_foreign_origin_admission()?;
        target.validate_initial_fork_view()?;
        Ok(PreparedZeroCopyFork {
            manifest: target,
            lineage,
        })
    }

    fn compute_initial_fork_view_digest(
        &self,
        source_identity: &ArtifactOrigin,
        target_identity: &ArtifactOrigin,
        lineage_seed: &BranchLineageSeed,
    ) -> Result<ForkViewDigest> {
        self.validate_namespace_binding(target_identity.namespace.as_str())?;
        if self.namespace_incarnation() != Some(target_identity.incarnation.as_uuid())
            || lineage_seed.parent_namespace != source_identity.namespace
            || lineage_seed.parent_incarnation != source_identity.incarnation
        {
            return Err(BranchError::BranchRootInvalid {
                branch_id: Some(lineage_seed.branch_id),
                reason: "fork-view projection identities do not match manifest bindings"
                    .to_string(),
            }
            .into());
        }
        self.validate_artifact_origins()?;
        let bytes = serde_json::to_vec(&ForkViewProjectionV1 {
            domain: "zeppelin-fork-view-projection-v1",
            target_namespace: target_identity.namespace.as_str(),
            target_incarnation: *target_identity.incarnation.as_uuid().as_bytes(),
            source_namespace: source_identity.namespace.as_str(),
            source_incarnation: *source_identity.incarnation.as_uuid().as_bytes(),
            branch_id: lineage_seed.branch_id,
            source_generation: lineage_seed.fork_generation,
            source_manifest_sha256: lineage_seed.fork_manifest_sha256,
            source_config_sha256: lineage_seed.source_config_sha256,
            depth: lineage_seed.depth,
            execution: self.execution_binding_v2(target_identity.namespace.as_str()),
            artifact_hashes: &self.artifact_hashes,
        })
        .map_err(|error| {
            ZeppelinError::Serialization(format!(
                "fork-view projection serialization failed: {error}"
            ))
        })?;
        Ok(ForkViewDigest::new(Sha256::digest(bytes).into()))
    }

    /// Verify the canonical initial view against the immutable lineage digest.
    pub(crate) fn validate_initial_fork_view(&self) -> Result<()> {
        let lineage = self.branch_lineage.as_ref().ok_or_else(|| {
            ZeppelinError::Serialization(
                "initial fork-view validation requires branch lineage".to_string(),
            )
        })?;
        let target_identity = self.local_origin()?;
        let source_identity = ArtifactOrigin {
            namespace: lineage.parent_namespace.clone(),
            incarnation: lineage.parent_incarnation.clone(),
        };
        let actual = self.compute_initial_fork_view_digest(
            &source_identity,
            &target_identity,
            &BranchLineageSeed::from(lineage),
        )?;
        if actual != lineage.fork_view_sha256 {
            return Err(BranchError::BranchRootInvalid {
                branch_id: Some(lineage.branch_id),
                reason: "canonical initial fork-view digest does not match branch lineage"
                    .to_string(),
            }
            .into());
        }
        Ok(())
    }

    /// Finalize an unpublished normalized fork exactly once as generation one.
    pub(crate) fn preseal_generation_one(
        &self,
        store: &ZeppelinStore,
        target_identity: &ArtifactOrigin,
    ) -> Result<PreparedManifestPublication> {
        if self.version != 0
            || self.namespace.as_deref() != Some(target_identity.namespace.as_str())
            || self.namespace_incarnation() != Some(target_identity.incarnation.as_uuid())
            || self.fencing_token != 0
            || self.deletion_fence.is_some()
            || !self.branch_roots.is_empty()
        {
            return Err(ZeppelinError::Serialization(
                "fork generation-one preseal requires one unpublished target-bound manifest"
                    .to_string(),
            ));
        }
        self.validate_initial_fork_view()?;
        let mut manifest = self.clone();
        manifest.version = 1;
        manifest.finalize_receipt_root(store, target_identity.namespace.as_str())?;
        if manifest.receipt_binding_version != Some(ReceiptBindingVersion::V4Lineage) {
            return Err(ZeppelinError::Serialization(
                "fork generation one did not select receipt binding v4_lineage".to_string(),
            ));
        }
        let bytes = manifest.to_bytes()?;
        let digest = ManifestDigest::new(Sha256::digest(&bytes).into());
        Ok(PreparedManifestPublication {
            manifest,
            bytes,
            digest,
        })
    }

    /// Create or byte-verify the exact presealed live and history generation one.
    pub(crate) async fn create_or_verify_generation_one(
        store: &ZeppelinStore,
        target_identity: &ArtifactOrigin,
        prepared: &PreparedManifestPublication,
    ) -> Result<Manifest> {
        let generation_one = ManifestGeneration::new(1)?;
        if prepared.manifest.version != 1
            || prepared.manifest.namespace.as_deref() != Some(target_identity.namespace.as_str())
            || prepared.manifest.namespace_incarnation()
                != Some(target_identity.incarnation.as_uuid())
            || prepared.manifest.to_bytes()? != prepared.bytes
            || ManifestDigest::new(Sha256::digest(&prepared.bytes).into()) != prepared.digest
        {
            return Err(BranchError::ManifestDigestMismatch {
                generation: generation_one,
            }
            .into());
        }
        let decoded =
            Self::from_bytes_for_namespace(&prepared.bytes, target_identity.namespace.as_str())?;
        if decoded.namespace_incarnation() != Some(target_identity.incarnation.as_uuid()) {
            return Err(BranchError::ManifestDigestMismatch {
                generation: generation_one,
            }
            .into());
        }
        decoded.validate_initial_fork_view()?;

        let live_key = Self::s3_key(target_identity.namespace.as_str());
        match store
            .put_create_outcome(&live_key, prepared.bytes.clone())
            .await?
        {
            CreateOnlyOutcome::Created { .. } => {}
            CreateOnlyOutcome::AlreadyExists => {
                let existing = store.get(&live_key).await?;
                if existing != prepared.bytes {
                    return Err(BranchError::ManifestDigestMismatch {
                        generation: generation_one,
                    }
                    .into());
                }
            }
        }
        Self::write_immutable_history_snapshot(
            store,
            target_identity.namespace.as_str(),
            1,
            prepared.bytes.clone(),
        )
        .await?;
        Ok(prepared.manifest.clone())
    }

    /// Adopts the exact empty target generation used as a clone-publication CAS base.
    ///
    /// Clone materialization first clears all source namespace identity. Before
    /// publication, this method verifies that the freshly created target still
    /// names the expected incarnation and contains no data, then advances the
    /// candidate from that target generation. The subsequent
    /// [`Manifest::write_conditional`] therefore cannot overwrite a concurrent
    /// target write, delete/recreate, or another clone attempt.
    pub(crate) fn prepare_clone_publication(
        &mut self,
        target_namespace: &str,
        target_incarnation: uuid::Uuid,
        target_base: &Manifest,
    ) -> Result<()> {
        if self.version != 0
            || self.namespace.is_some()
            || self.namespace_incarnation.is_some()
            || self.deletion_fence.is_some()
        {
            return Err(ZeppelinError::Serialization(
                "clone candidate must clear source namespace identity before publication"
                    .to_string(),
            ));
        }
        target_base.require_empty_clone_state(target_namespace, target_incarnation)?;

        self.version = target_base.version;
        self.namespace = Some(target_namespace.to_string());
        self.bind_namespace_incarnation(target_incarnation)
    }

    /// Validates the exact empty target generation used as a clone CAS base.
    ///
    /// Clone targets are active before potentially expensive artifact work. A
    /// target write can therefore win before the clone captures its ETag. That
    /// acknowledged state must make clone fail rather than become part of the
    /// clone candidate. The final conditional manifest write independently
    /// rejects every mutation that occurs after this exact base is captured.
    pub(crate) fn require_empty_clone_base(
        &self,
        target_namespace: &str,
        target_incarnation: uuid::Uuid,
    ) -> Result<()> {
        if self.version != 1 {
            return Err(ZeppelinError::ManifestConflict {
                namespace: target_namespace.to_string(),
            });
        }
        self.require_empty_clone_state(target_namespace, target_incarnation)
    }

    /// Validates empty clone state without imposing the production bootstrap generation.
    ///
    /// Synthetic branching fixtures reuse clone normalization after explicitly
    /// removing fixture WAL from an advanced manifest. Production HTTP callers
    /// must use [`Self::require_empty_clone_base`] first so this compatibility
    /// seam cannot weaken the fresh-target contract.
    fn require_empty_clone_state(
        &self,
        target_namespace: &str,
        target_incarnation: uuid::Uuid,
    ) -> Result<()> {
        self.validate_namespace_binding(target_namespace)?;
        if self.namespace_incarnation() != Some(target_incarnation)
            || !self.fragments.is_empty()
            || !self.segments.is_empty()
            || self.active_segment.is_some()
            || !self.pending_deletes.is_empty()
            || self.deletion_fence.is_some()
            || !self.branch_roots.is_empty()
        {
            return Err(ZeppelinError::ManifestConflict {
                namespace: target_namespace.to_string(),
            });
        }
        Ok(())
    }

    /// Computes the successor of a persisted generation without wrapping.
    ///
    /// # Parameters
    ///
    /// - `version`: Current generation, including zero for an unpublished value.
    ///
    /// # Returns
    ///
    /// The next generation.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Serialization`] at `u64::MAX`; wrapping to zero
    /// would violate the persisted monotonic-generation invariant.
    fn checked_next_version(version: u64) -> Result<u64> {
        version
            .checked_add(1)
            .ok_or_else(|| ZeppelinError::Serialization("manifest version overflow".to_string()))
    }

    /// Computes the generation that would be assigned to this candidate commit.
    ///
    /// # Returns
    ///
    /// `self.version + 1` when representable.
    ///
    /// # Errors
    ///
    /// Propagates the generation-overflow error from
    /// [`Manifest::checked_next_version`].
    fn next_committed_version(&self) -> Result<u64> {
        Self::checked_next_version(self.version)
    }

    /// Appends a visible fragment descriptor with the next replay sequence.
    ///
    /// # Parameters
    ///
    /// - `fref`: Owned descriptor for an already-uploaded immutable fragment.
    ///   Any sequence supplied by the caller is replaced.
    ///
    /// # Side Effects
    ///
    /// Mutates this in-memory candidate by assigning `next_sequence`, advancing
    /// that counter, appending the descriptor, and refreshing `updated_at`.
    /// Publication is separate; no object-store request occurs here.
    ///
    /// # Examples
    ///
    /// Adding the first fragment to an empty manifest assigns sequence 0. After
    /// the candidate is published, readers replay that fragment before the next
    /// fragment assigned sequence 1.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `fref` is moved into the method and then into the vector. Unlike passing
    /// a Java object reference or copying a C pointer, Rust makes the caller's
    /// binding unusable after the call unless it explicitly cloned the value.
    /// The method therefore gains unique ownership without allocating another
    /// `FragmentRef`.
    pub fn add_fragment(&mut self, fref: FragmentRef) {
        self.add_fragment_at(fref, Utc::now());
    }

    /// Appends a fragment descriptor using an explicit manifest stamp.
    pub fn add_fragment_at(&mut self, mut fref: FragmentRef, now: DateTime<Utc>) {
        fref.sequence_number = self.next_sequence;
        self.next_sequence += 1;
        self.fragments.push(fref);
        self.updated_at = now;
    }

    /// Remove exactly the fragments that were compacted (by ID).
    ///
    /// Removal must use the exact snapshot set, not a ULID watermark
    /// inequality: ULIDs are not monotonic within the same millisecond (and
    /// not across nodes with clock skew), so a fragment appended concurrently
    /// with compaction can sort <= the snapshot's max ULID. A watermark
    /// comparison would drop it from the manifest without its vectors being
    /// in the segment — silent data loss (see UpsertDeleteCompactQuery.tla).
    ///
    /// `compaction_watermark` is still recorded (max removed ID) for
    /// observability, but is never used to decide removal.
    ///
    /// # Parameters
    ///
    /// - `compacted_ids`: Borrowed exact set captured by the compaction input
    ///   snapshot. IDs absent from this set always survive.
    ///
    /// # Side Effects
    ///
    /// Removes matching in-memory fragment descriptors, advances the
    /// observability watermark monotonically, and refreshes `updated_at`. The
    /// fragment objects are not deleted and the change is not visible until a
    /// later manifest publication succeeds.
    ///
    /// # Performance
    ///
    /// Scans all visible fragments and performs expected constant-time hash-set
    /// membership checks; no object-store requests occur.
    ///
    /// # Examples
    ///
    /// If compaction read fragments A and C while B arrived concurrently, a set
    /// `{A, C}` removes only A and C even when B's ULID sorts below C.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The borrowed `&HashSet<Ulid>` cannot be mutated through this reference.
    /// The closure passed to [`Vec::retain`] borrows it while mutating a separate
    /// vector, a separation the borrow checker verifies without a garbage
    /// collector or manual alias analysis.
    pub fn remove_compacted_fragments(&mut self, compacted_ids: &HashSet<Ulid>) {
        self.remove_compacted_fragments_at(compacted_ids, Utc::now());
    }

    /// Removes exact compacted fragments using an explicit manifest stamp.
    pub fn remove_compacted_fragments_at(
        &mut self,
        compacted_ids: &HashSet<Ulid>,
        now: DateTime<Utc>,
    ) {
        self.fragments.retain(|f| !compacted_ids.contains(&f.id));
        if let Some(max_id) = compacted_ids.iter().max() {
            let watermark = match self.compaction_watermark {
                Some(prev) => prev.max(*max_id),
                None => *max_id,
            };
            self.compaction_watermark = Some(watermark);
        }
        self.updated_at = now;
    }

    /// Removes an exact compaction snapshot using origin-qualified identities.
    ///
    /// Unlike the legacy ULID-only helper, this keeps a same-ULID fragment from
    /// another physical namespace lifetime visible. The authoritative logical
    /// origin is revalidated against this fresh CAS candidate before mutation.
    pub(crate) fn remove_compacted_located_fragments_at(
        &mut self,
        authoritative_local: &ArtifactOrigin,
        compacted: &HashSet<LocatedFragmentIdentity>,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let remove = {
            let resolver = self.artifact_origin_resolver(authoritative_local)?;
            self.fragments
                .iter()
                .map(|fragment| {
                    resolver
                        .locate_fragment(fragment)
                        .map(|located| compacted.contains(&located.identity()))
                })
                .collect::<Result<Vec<_>>>()?
        };
        self.fragments = std::mem::take(&mut self.fragments)
            .into_iter()
            .zip(remove)
            .filter_map(|(fragment, remove)| (!remove).then_some(fragment))
            .collect();
        if let Some(max_id) = compacted.iter().map(|identity| identity.id).max() {
            self.compaction_watermark = Some(
                self.compaction_watermark
                    .map_or(max_id, |previous| previous.max(max_id)),
            );
        }
        self.updated_at = now;
        Ok(())
    }

    /// Add a segment reference and prune old segments using the provided limit.
    ///
    /// NOTE: `max_pending_deletes` is currently unused — `pending_deletes` is
    /// deliberately not capped (see `prune()`). The parameter is retained for
    /// call-site compatibility; capping it would leak S3 objects.
    ///
    /// # Parameters
    ///
    /// - `sref`: Owned descriptor for the newly published segment artifacts.
    /// - `max_pending_deletes`: Compatibility parameter; intentionally ignored.
    /// - `max_old_segments`: Maximum non-active segment descriptors to retain.
    ///
    /// # Side Effects
    ///
    /// Makes `sref` active in this in-memory candidate, appends it, updates the
    /// timestamp, and prunes old segment metadata. It neither uploads the
    /// segment nor publishes this manifest.
    ///
    /// # Examples
    ///
    /// With two retained old segments, adding `seg_4` makes it active and keeps
    /// at most `seg_4` plus the two most recent older descriptors.
    pub fn add_segment_with_limits(
        &mut self,
        sref: SegmentRef,
        max_pending_deletes: usize,
        max_old_segments: usize,
    ) {
        self.add_segment_with_limits_at(sref, max_pending_deletes, max_old_segments, Utc::now());
    }

    /// Adds a segment with retention limits and an explicit manifest stamp.
    pub fn add_segment_with_limits_at(
        &mut self,
        sref: SegmentRef,
        max_pending_deletes: usize,
        max_old_segments: usize,
        now: DateTime<Utc>,
    ) {
        self.active_segment = Some(sref.id.clone());
        self.segments.push(sref);
        self.updated_at = now;
        self.prune(max_pending_deletes, max_old_segments);
    }

    /// Adds a segment using the legacy default retention limits.
    ///
    /// # Parameters
    ///
    /// - `sref`: Owned descriptor for a completed immutable segment.
    ///
    /// # Side Effects
    ///
    /// Delegates to [`Manifest::add_segment_with_limits`] with 1,000 as the
    /// ignored deletion parameter and 10 retained old segments.
    ///
    /// # Examples
    ///
    /// Tests and setup utilities use this convenience method when production
    /// configuration is irrelevant to the scenario.
    pub fn add_segment(&mut self, sref: SegmentRef) {
        self.add_segment_with_limits(sref, 1000, 10);
    }

    /// Removes a segment descriptor and clears the active pointer when needed.
    ///
    /// # Parameters
    ///
    /// - `segment_id`: Borrowed ID to remove. A missing ID is a metadata no-op
    ///   apart from refreshing `updated_at`.
    ///
    /// # Side Effects
    ///
    /// Mutates only the in-memory manifest. It does not delete immutable segment
    /// objects or publish the result.
    ///
    /// # Examples
    ///
    /// Removing active `seg_live` leaves older descriptors intact and sets
    /// `active_segment` to `None`, so callers cannot accidentally keep routing
    /// through the removed descriptor.
    pub fn remove_segment(&mut self, segment_id: &str) {
        self.remove_segment_at(segment_id, Utc::now());
    }

    /// Removes a segment descriptor using an explicit manifest stamp.
    pub fn remove_segment_at(&mut self, segment_id: &str, now: DateTime<Utc>) {
        if self.active_segment.as_deref() == Some(segment_id) {
            self.active_segment = None;
        }
        self.segments.retain(|segment| segment.id != segment_id);
        self.coarse_payload_encodings.remove(segment_id);
        self.prune_hierarchical_routing_nodes();
        self.updated_at = now;
    }

    /// Prune the manifest to prevent unbounded growth at 1M+ scale.
    ///
    /// Retains only the most recent `max_old_segments` non-active segments.
    /// Segment refs are safe to drop: a replaced segment's S3 files were
    /// queued into `pending_deletes` when it was replaced, so pruning the
    /// ref is metadata-only.
    ///
    /// `pending_deletes` is deliberately NOT capped here: every entry is an
    /// S3 key that still needs deletion, and draining entries without
    /// deleting the objects leaks them permanently. The list is bounded in
    /// practice — it is rewritten each compaction cycle and cleared (or
    /// carried over on failure) at the start of the next.
    ///
    /// # Parameters
    ///
    /// - `_max_pending_deletes`: Retained for API compatibility and deliberately
    ///   ignored because forgetting deletion work would leak objects.
    /// - `max_old_segments`: Maximum non-active descriptors to retain.
    ///
    /// # Side Effects
    ///
    /// Rewrites the in-memory `segments` vector when it exceeds the configured
    /// bound. It never edits `pending_deletes` and performs no remote deletes.
    ///
    /// # Performance
    ///
    /// At most linear in the number of segment descriptors and may allocate a
    /// replacement vector for the retained tail.
    ///
    /// # Examples
    ///
    /// Six descriptors with `max_old_segments = 2` become the active descriptor
    /// plus two recent older descriptors. Every pending deletion key survives.
    pub fn prune(&mut self, _max_pending_deletes: usize, max_old_segments: usize) {
        // Prune old segments: keep active + most recent max_old_segments
        if self.segments.len() > max_old_segments + 1 {
            let active_id = self.active_segment.as_deref();
            // Partition: keep active segment and the newest max_old_segments others.
            // Segments are appended in order, so newest are at the end.
            let keep_from = self.segments.len() - (max_old_segments + 1);
            let mut pruned: Vec<SegmentRef> = self.segments.drain(keep_from..).collect();
            // Ensure active segment is retained even if it wasn't in the tail
            if let Some(aid) = active_id {
                if !pruned.iter().any(|s| s.id == aid) {
                    if let Some(active) = self.segments.iter().find(|s| s.id == aid).cloned() {
                        pruned.insert(0, active);
                    }
                }
            }
            self.segments = pruned;
        }
        let retained = self
            .segments
            .iter()
            .map(|segment| segment.id.as_str())
            .collect::<HashSet<_>>();
        self.coarse_payload_encodings
            .retain(|segment_id, _| retained.contains(segment_id.as_str()));
        self.prune_hierarchical_routing_nodes();
    }

    fn prune_hierarchical_routing_nodes(&mut self) {
        let retained = self
            .segments
            .iter()
            .filter(|segment| segment.hierarchical)
            .map(|segment| segment.id.as_str())
            .collect::<HashSet<_>>();
        self.hierarchical_routing_nodes
            .retain(|segment_id, _| retained.contains(segment_id.as_str()));
    }

    /// Borrows all fragment descriptors currently visible in this manifest.
    ///
    /// The name is historical: membership in `fragments`, not comparison with
    /// `compaction_watermark`, defines whether a fragment is uncompacted.
    ///
    /// # Returns
    ///
    /// A read-only slice in replay order. No clone or allocation occurs.
    pub fn uncompacted_fragments(&self) -> &[FragmentRef] {
        &self.fragments
    }

    /// Returns whether any currently visible segment or WAL fragment is
    /// owned by another namespace incarnation.
    pub fn has_foreign_visible_artifacts(&self) -> Result<bool> {
        let local = self.local_origin()?;
        let resolver = self.artifact_origin_resolver(&local)?;
        let foreign_segment = resolver
            .active_located_segment()?
            .is_some_and(|segment| segment.physical_origin.as_origin() != &local);
        let foreign_fragment = resolver
            .uncompacted_located_fragments()?
            .iter()
            .any(|fragment| fragment.physical_origin.as_origin() != &local);
        Ok(foreign_segment || foreign_fragment)
    }

    /// Returns true when every visible artifact is target-local.
    pub fn visible_refs_are_local(&self) -> Result<bool> {
        Ok(!self.has_foreign_visible_artifacts()?)
    }

    /// Rejects deferred-delete entries that are not owned by `namespace`.
    pub fn validate_pending_deletes_are_local(&self, namespace: &str) -> Result<()> {
        for key in &self.pending_deletes {
            let owned = NamespaceObjectKey::classify(namespace, key.clone()).map_err(|error| {
                ZeppelinError::Validation(format!(
                    "pending delete is not local to namespace {namespace}: {key}: {error}"
                ))
            })?;
            if !owned.allows_deferred_delete() {
                return Err(ZeppelinError::Validation(format!(
                    "pending delete is not a local immutable artifact for namespace {namespace}: {key}"
                )));
            }
        }
        Ok(())
    }

    /// Sums vector entries recorded across all retained segment descriptors.
    ///
    /// # Returns
    ///
    /// The descriptor total as `usize`. This is not the namespace's deduplicated
    /// live-vector count and includes every retained segment reference.
    ///
    /// # Performance
    ///
    /// Linear in `segments.len()` with no allocation or object-store I/O.
    pub fn segment_vector_count(&self) -> usize {
        self.segments.iter().map(|s| s.vector_count).sum()
    }

    /// Manifest-derived vector count exposed by namespace metadata.
    ///
    /// This is exact for the manifest's aggregate entries:
    /// compacted segment vectors plus uncompacted WAL vector entries minus
    /// uncompacted WAL tombstones, lower-bounded at zero. Until compaction
    /// resolves duplicate upserts by ID, it is an upper bound on unique live
    /// vector IDs.
    ///
    /// # Returns
    ///
    /// Segment entries plus WAL upserts minus WAL tombstones, saturating at zero.
    ///
    /// # Examples
    ///
    /// Segments containing 125 entries plus 15 WAL upserts and four tombstones
    /// report 136. Three tombstones against one entry report zero, not an
    /// unsigned underflow.
    #[must_use]
    pub fn vector_count(&self) -> u64 {
        let entries = self
            .segments
            .iter()
            .map(|segment| segment.vector_count as u64)
            .sum::<u64>()
            + self
                .fragments
                .iter()
                .map(|fragment| fragment.vector_count as u64)
                .sum::<u64>();
        let tombstones = self
            .fragments
            .iter()
            .map(|fragment| fragment.delete_count as u64)
            .sum::<u64>();

        entries.saturating_sub(tombstones)
    }

    /// Approximate live storage bytes from sizes recorded in manifest refs.
    ///
    /// This never lists or HEADs S3 objects. Legacy refs whose sizes were not
    /// recorded contribute zero for unknown artifacts, so the value is a known
    /// lower-bound approximation rather than an object-store inventory.
    ///
    /// # Returns
    ///
    /// Known bytes for visible fragment refs and the artifact sizes represented
    /// by each segment descriptor.
    ///
    /// # Performance
    ///
    /// Linear in the number of fragment and grouped-cluster references. It
    /// performs no GET, HEAD, or LIST request.
    ///
    /// # Examples
    ///
    /// A legacy fragment with unknown size contributes zero; a newer 2 KiB
    /// fragment contributes 2 KiB even if its object is not locally cached.
    #[must_use]
    pub fn approximate_storage_bytes(&self) -> u64 {
        let fragment_bytes: u64 = self
            .fragments
            .iter()
            .map(|fragment| fragment.size_bytes)
            .sum();
        let segment_bytes: u64 = self
            .segments
            .iter()
            .map(SegmentRef::approximate_storage_bytes)
            .sum();

        fragment_bytes + segment_bytes
    }

    /// Serializes this manifest as version-prefixed MessagePack bytes.
    ///
    /// # Returns
    ///
    /// Owned shared bytes in the format `[0x01][MessagePack payload]`.
    /// Serialization does not mutate the manifest.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Serialization`] if serde cannot encode the
    /// manifest. No object-store write has occurred when this fails.
    ///
    /// # Examples
    ///
    /// An empty generation-zero manifest encodes with `0x01` as its first byte;
    /// [`Manifest::from_bytes`] can decode the result back into an owned value.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// [`Bytes`] is an immutable, reference-counted byte buffer. Returning it is
    /// closer to sharing a read-only Java `ByteBuffer` than returning a fresh
    /// `byte[]`; in C it replaces a pointer/length pair with owned lifetime
    /// tracking. The intermediate `Vec<u8>` is moved into `Bytes` without
    /// exposing manual allocation or free operations.
    pub fn to_bytes(&self) -> Result<Bytes> {
        let msgpack = rmp_serde::to_vec(self).map_err(|e| {
            ZeppelinError::Serialization(format!("manifest msgpack serialize: {e}"))
        })?;
        let mut data = Vec::with_capacity(1 + msgpack.len());
        data.push(MANIFEST_FORMAT_MSGPACK);
        data.extend_from_slice(&msgpack);
        Ok(Bytes::from(data))
    }

    /// Decodes a current MessagePack or legacy JSON manifest.
    ///
    /// # Parameters
    ///
    /// - `data`: Borrowed complete object bytes. The decoder does not retain the
    ///   slice after returning.
    ///
    /// # Returns
    ///
    /// An owned manifest. An empty object decodes as [`Manifest::new`] for
    /// compatibility. `0x01` selects the current prefixed MessagePack format;
    /// `{` selects legacy JSON. Other leading bytes are tried as an unknown
    /// one-byte prefix and then as unprefixed MessagePack.
    ///
    /// # Errors
    ///
    /// Returns a serialization error when the selected format is malformed or
    /// incompatible. It does not silently substitute a default for non-empty
    /// corrupt data.
    ///
    /// # Examples
    ///
    /// A pre-MessagePack JSON object beginning with `{` remains readable. A
    /// current object beginning with `0x01` decodes its remaining bytes as
    /// MessagePack.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The exhaustive `match` makes every recognized prefix explicit. `?`
    /// propagates a typed error like a checked exception without Java's runtime
    /// exception machinery; unlike a C status code, the caller cannot access a
    /// success value unless the [`Result`] is `Ok`.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Ok(Self::new());
        }
        match data[0] {
            MANIFEST_FORMAT_MSGPACK => rmp_serde::from_slice(&data[1..]).map_err(|e| {
                ZeppelinError::Serialization(format!("manifest msgpack deserialize: {e}"))
            }),
            // Legacy JSON: starts with '{' (0x7B)
            b'{' => Ok(serde_json::from_slice(data)?),
            _ => {
                // Try msgpack (skip version byte), fall back to JSON
                rmp_serde::from_slice(&data[1..])
                    .or_else(|_| rmp_serde::from_slice(data))
                    .map_err(|e| {
                        ZeppelinError::Serialization(format!("manifest msgpack deserialize: {e}"))
                    })
            }
        }
    }

    /// Decodes manifest bytes and validates any persisted namespace binding.
    ///
    /// Legacy manifests without a binding remain readable. Newly written
    /// manifests must match the namespace whose object key supplied the bytes.
    pub(crate) fn from_bytes_for_namespace(data: &[u8], namespace: &str) -> Result<Self> {
        let manifest = Self::from_bytes(data)?;
        manifest.validate_namespace_binding(namespace)?;
        manifest.validate_branch_root_state(namespace)?;
        manifest.validate_artifact_origins()?;
        manifest.validate_branch_lineage_state(namespace)?;
        manifest.validate_receipt_binding_state(namespace)?;
        manifest.validate_foreign_origin_admission()?;
        manifest.validate_pending_deletes_are_local(namespace)?;
        Ok(manifest)
    }

    fn has_explicit_artifact_origins(&self) -> bool {
        !self.artifact_origins.is_empty()
            || self
                .fragments
                .iter()
                .any(|fragment| fragment.artifact_origin.is_some())
            || self
                .segments
                .iter()
                .any(|segment| segment.artifact_origin.is_some())
    }

    fn validate_receipt_binding_state(&self, namespace: &str) -> Result<()> {
        match self.receipt_binding_version {
            None => {
                if self.receipt_state_digest.is_some() || self.control_state_digest.is_some() {
                    return Err(ZeppelinError::Serialization(
                        "manifest digest fields require a receipt binding version".to_string(),
                    ));
                }
                if self.has_explicit_artifact_origins() {
                    return Err(ZeppelinError::Serialization(
                        "explicit artifact origins require receipt binding v2_origins".to_string(),
                    ));
                }
                if !self.branch_roots.is_empty() {
                    return Err(ZeppelinError::Serialization(
                        "branch roots require receipt binding v3_roots".to_string(),
                    ));
                }
                if self.branch_lineage.is_some() {
                    return Err(ZeppelinError::Serialization(
                        "branch lineage requires receipt binding v4_lineage".to_string(),
                    ));
                }
            }
            Some(ReceiptBindingVersion::V1) => {
                if self.receipt_state_digest.is_none() {
                    return Err(ZeppelinError::Serialization(
                        "receipt binding v1 requires an execution digest".to_string(),
                    ));
                }
                if self.control_state_digest.is_some() {
                    return Err(ZeppelinError::Serialization(
                        "receipt binding v1 forbids a control digest".to_string(),
                    ));
                }
                if self.has_explicit_artifact_origins() {
                    return Err(ZeppelinError::Serialization(
                        "explicit artifact origins require receipt binding v2_origins".to_string(),
                    ));
                }
                if !self.branch_roots.is_empty() {
                    return Err(ZeppelinError::Serialization(
                        "branch roots require receipt binding v3_roots".to_string(),
                    ));
                }
                if self.branch_lineage.is_some() {
                    return Err(ZeppelinError::Serialization(
                        "branch lineage requires receipt binding v4_lineage".to_string(),
                    ));
                }
            }
            Some(ReceiptBindingVersion::V2Origins) => {
                if self.receipt_state_digest.is_none() {
                    return Err(ZeppelinError::Serialization(
                        "receipt binding v2_origins requires an execution digest".to_string(),
                    ));
                }
                if self.control_state_digest.is_some() {
                    return Err(ZeppelinError::Serialization(
                        "receipt binding v2_origins forbids a control digest".to_string(),
                    ));
                }
                if !self.branch_roots.is_empty() {
                    return Err(ZeppelinError::Serialization(
                        "branch roots require receipt binding v3_roots".to_string(),
                    ));
                }
                if self.branch_lineage.is_some() {
                    return Err(ZeppelinError::Serialization(
                        "branch lineage requires receipt binding v4_lineage".to_string(),
                    ));
                }
            }
            Some(ReceiptBindingVersion::V3Roots) => {
                if self.branch_lineage.is_some() {
                    return Err(ZeppelinError::Serialization(
                        "branch lineage requires receipt binding v4_lineage".to_string(),
                    ));
                }
                let execution = self.receipt_state_digest.ok_or_else(|| {
                    ZeppelinError::Serialization(
                        "receipt binding v3_roots requires an execution digest".to_string(),
                    )
                })?;
                let control = self.control_state_digest.ok_or_else(|| {
                    ZeppelinError::Serialization(
                        "receipt binding v3_roots requires a control digest".to_string(),
                    )
                })?;
                if self.compute_receipt_state_digest(namespace, ReceiptBindingVersion::V3Roots)?
                    != execution
                {
                    return Err(ZeppelinError::Serialization(
                        "receipt binding v3_roots execution digest mismatch".to_string(),
                    ));
                }
                if self.compute_control_roots_digest(namespace)? != control {
                    return Err(ZeppelinError::Serialization(
                        "receipt binding v3_roots control digest mismatch".to_string(),
                    ));
                }
            }
            Some(ReceiptBindingVersion::V4Lineage) => {
                let execution = self.receipt_state_digest.ok_or_else(|| {
                    ZeppelinError::Serialization(
                        "receipt binding v4_lineage requires an execution digest".to_string(),
                    )
                })?;
                let control = self.control_state_digest.ok_or_else(|| {
                    ZeppelinError::Serialization(
                        "receipt binding v4_lineage requires a control digest".to_string(),
                    )
                })?;
                if self.branch_lineage.is_none() {
                    return Err(ZeppelinError::Serialization(
                        "receipt binding v4_lineage requires immutable branch lineage".to_string(),
                    ));
                }
                if self.compute_receipt_state_digest(namespace, ReceiptBindingVersion::V4Lineage)?
                    != execution
                {
                    return Err(ZeppelinError::Serialization(
                        "receipt binding v4_lineage execution digest mismatch".to_string(),
                    ));
                }
                if self.compute_control_branch_digest(namespace)? != control {
                    return Err(ZeppelinError::Serialization(
                        "receipt binding v4_lineage control digest mismatch".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_namespace_binding(&self, namespace: &str) -> Result<()> {
        if let Some(bound) = &self.namespace {
            if bound != namespace {
                return Err(ZeppelinError::Serialization(format!(
                    "manifest namespace binding mismatch: expected {namespace}, got {bound}"
                )));
            }
        }
        if let Some(fence) = &self.deletion_fence {
            validate_destruction_record_key(&fence.destruction_record_key).map_err(|error| {
                ZeppelinError::Serialization(format!("manifest deletion fence is invalid: {error}"))
            })?;
        }
        Ok(())
    }

    /// Validate the complete live branch-root/control state as one domain.
    fn validate_branch_root_state(&self, namespace: &str) -> Result<()> {
        if self.deletion_fence.is_some() && !self.branch_roots.is_empty() {
            return Err(BranchError::NamespaceHasLiveBranches {
                namespace: namespace.to_string(),
                visible_children: Vec::new(),
                has_additional_children: true,
            }
            .into());
        }
        if !self.branch_roots.is_empty() {
            if self.namespace.as_deref() != Some(namespace) {
                return Err(BranchError::BranchRootInvalid {
                    branch_id: None,
                    reason: "root-bearing manifest is not bound to its authoritative namespace"
                        .to_string(),
                }
                .into());
            }
            if self
                .namespace_incarnation
                .is_none_or(|incarnation| incarnation.as_uuid().is_nil())
            {
                return Err(BranchError::BranchRootInvalid {
                    branch_id: None,
                    reason: "root-bearing manifest has no non-nil source incarnation".to_string(),
                }
                .into());
            }
        }

        let mut target_incarnations = BTreeSet::new();
        for (branch_id, root) in &self.branch_roots {
            if branch_id != &root.branch_id {
                return Err(BranchError::BranchRootInvalid {
                    branch_id: Some(*branch_id),
                    reason: "branch root map key does not match its body".to_string(),
                }
                .into());
            }
            if root.source_generation.get() == 0 {
                return Err(BranchError::BranchRootInvalid {
                    branch_id: Some(*branch_id),
                    reason: "source generation must be greater than zero".to_string(),
                }
                .into());
            }
            if !crate::namespace::types::is_valid_namespace_name(root.target_namespace.as_str()) {
                return Err(BranchError::BranchRootInvalid {
                    branch_id: Some(*branch_id),
                    reason: "target namespace violates the namespace grammar".to_string(),
                }
                .into());
            }
            if root.target_incarnation.is_nil() {
                return Err(BranchError::BranchRootInvalid {
                    branch_id: Some(*branch_id),
                    reason: "target namespace incarnation is nil".to_string(),
                }
                .into());
            }
            if !target_incarnations.insert((
                root.target_namespace.clone(),
                root.target_incarnation.clone(),
            )) {
                return Err(BranchError::BranchRootInvalid {
                    branch_id: Some(*branch_id),
                    reason: "target namespace incarnation has more than one direct-parent root"
                        .to_string(),
                }
                .into());
            }
        }
        Ok(())
    }

    /// Reads and decodes the authoritative live manifest from object storage.
    ///
    /// # Parameters
    ///
    /// - `store`: Borrowed storage abstraction used for one complete-object GET.
    /// - `namespace`: Namespace whose live manifest should be read.
    ///
    /// # Returns
    ///
    /// `Ok(Some(manifest))` when the live object exists, or `Ok(None)` only when
    /// storage reports that key as not found.
    ///
    /// # Errors
    ///
    /// Propagates storage failures and manifest decoding errors. A corrupt live
    /// object is not treated as an absent or empty namespace.
    ///
    /// # Consistency
    ///
    /// This reads the object-store source of truth directly. A caller that also
    /// needs an ETag for publication must use [`Manifest::read_versioned`].
    ///
    /// # Performance
    ///
    /// Performs one full object-store GET and allocates the decoded collections.
    ///
    /// # Examples
    ///
    /// Reading a newly created namespace returns generation 1. Reading a deleted
    /// namespace returns `None`; it does not recreate the manifest.
    pub async fn read(store: &ZeppelinStore, namespace: &str) -> Result<Option<Self>> {
        let key = Self::s3_key(namespace);
        match store.get(&key).await {
            Ok(data) => Ok(Some(Self::from_bytes_for_namespace(&data, namespace)?)),
            Err(crate::error::ZeppelinError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Publishes this candidate without overwriting a concurrent live manifest.
    ///
    /// The method first reads the current live generation, retains that exact
    /// predecessor, chooses a generation greater than both the live value and
    /// `self.version`, and finally publishes the replacement. Existing manifests
    /// use the ETag from the discovery read; absent manifests use create-only PUT.
    /// Use [`Manifest::write_conditional`] when the caller already holds a
    /// versioned read capability.
    ///
    /// # Parameters
    ///
    /// - `store`: Borrowed storage abstraction for the read and writes.
    /// - `namespace`: Destination namespace. Internal clone paths reset the
    ///   candidate generation before calling this method.
    ///
    /// # Returns
    ///
    /// `Ok(())` after predecessor retention, replacement live publication, and
    /// immutable history for the CAS winner. Only then is `self.version`
    /// advanced to the committed generation.
    ///
    /// # Errors
    ///
    /// Returns on read, missing ETag, generation overflow, serialization,
    /// predecessor-history, live PUT, concurrent-publication conflict, or a
    /// candidate/live branch-control mismatch. A failed live PUT cannot reserve
    /// the speculative generation; `self.version` remains unchanged and a
    /// divergent retry stays possible.
    ///
    /// # Side Effects
    ///
    /// Performs one generation-discovery GET, a predecessor-history operation
    /// when needed, one conditional or create-only live-manifest PUT, and one
    /// immutable winner-history operation on the success path.
    ///
    /// # Consistency
    ///
    /// A concurrent fence, deletion, or publication after the discovery GET
    /// makes the final PUT fail instead of overwriting or resurrecting state.
    /// Candidates crossing a V3 branch-control epoch fail before predecessor
    /// history or live publication, even when the discovery read obtained a
    /// fresh ETag.
    ///
    /// # Examples
    ///
    /// Namespace bootstrap writes generation 1 directly. Replacing it first
    /// retains authoritative history 1. If the candidate-2 live PUT fails, a
    /// different candidate 2 can reuse the same history 1 and retry safely.
    pub async fn write(&mut self, store: &ZeppelinStore, namespace: &str) -> Result<()> {
        if self.deletion_fence.is_some() {
            return Err(ZeppelinError::NamespaceDeleting {
                namespace: namespace.to_string(),
            });
        }
        let key = Self::s3_key(namespace);
        let current = Self::read_versioned(store, namespace).await?;
        if current
            .as_ref()
            .is_some_and(|(manifest, _)| manifest.deletion_fence.is_some())
        {
            return Err(ZeppelinError::NamespaceDeleting {
                namespace: namespace.to_string(),
            });
        }
        if let Some((live, _)) = &current {
            self.require_matching_branch_control_for_recovery_write(live, namespace)?;
            self.require_valid_branch_successor(live, namespace)?;
        }
        let current_version = current
            .as_ref()
            .map_or(0, |(manifest, _)| manifest.version());
        if let Some((live, version)) = &current {
            if !version.history_confirmed {
                let data = version.history_snapshot_bytes(namespace, live.version())?;
                Self::write_immutable_history_snapshot(store, namespace, live.version(), data)
                    .await?;
            }
        }
        let base_version = self.version.max(current_version);
        let mut committed = self.clone();
        committed.version = Self::checked_next_version(base_version)?;
        committed.namespace = Some(namespace.to_string());
        committed.finalize_receipt_root(store, namespace)?;
        let data = committed.to_bytes()?;
        match current {
            Some((_, version)) => {
                let observed = version.require_version(namespace, "manifest recovery write")?;
                store
                    .put_if_match(&key, data.clone(), observed, namespace)
                    .await?;
            }
            None => match store.put_create_outcome(&key, data.clone()).await? {
                CreateOnlyOutcome::Created { .. } => {}
                CreateOnlyOutcome::AlreadyExists => {
                    return Err(ZeppelinError::ManifestConflict {
                        namespace: namespace.to_string(),
                    });
                }
            },
        }
        Self::write_immutable_history_snapshot(store, namespace, committed.version(), data).await?;
        store.forget_known_content_hashes(committed.artifact_hashes.keys());
        *self = committed;
        Ok(())
    }

    /// Reject a generic recovery write that would cross a branch-control epoch.
    ///
    /// `write` deliberately discovers a fresh ETag for recovery-style callers,
    /// so its candidate may predate the authoritative manifest observation.
    /// Once either side has entered the V3 branch-control domain, rebasing is
    /// safe only when the candidate carries the exact current root map,
    /// incarnation, binding version, and control digest. Root insertion and
    /// removal use their narrower versioned CAS primitives instead.
    fn require_matching_branch_control_for_recovery_write(
        &self,
        live: &Self,
        namespace: &str,
    ) -> Result<()> {
        let is_branch_bound = |manifest: &Self| {
            !manifest.branch_roots.is_empty()
                || manifest.branch_lineage.is_some()
                || manifest.control_state_digest.is_some()
                || matches!(
                    manifest.receipt_binding_version,
                    Some(ReceiptBindingVersion::V3Roots | ReceiptBindingVersion::V4Lineage)
                )
        };
        if (is_branch_bound(self) || is_branch_bound(live))
            && (self.branch_roots != live.branch_roots
                || self.branch_lineage != live.branch_lineage
                || self.namespace_incarnation != live.namespace_incarnation
                || self.receipt_binding_version != live.receipt_binding_version
                || self.control_state_digest != live.control_state_digest)
        {
            return Err(ZeppelinError::ManifestConflict {
                namespace: namespace.to_string(),
            });
        }
        Ok(())
    }

    fn foreign_descriptor_closure(&self) -> Result<BTreeSet<Vec<u8>>> {
        #[derive(Serialize)]
        struct ForeignFragment<'a> {
            kind: &'static str,
            origin: &'a ArtifactOrigin,
            descriptor: FragmentRef,
        }

        #[derive(Serialize)]
        struct ForeignSegment<'a> {
            kind: &'static str,
            origin: &'a ArtifactOrigin,
            descriptor: SegmentRef,
        }

        let local = self.local_origin()?;
        let resolver = self.artifact_origin_resolver(&local)?;
        let mut closure = BTreeSet::new();
        for located in resolver.located_fragments()? {
            if located.physical_origin.as_origin() == &local {
                continue;
            }
            let mut descriptor = located.fragment.clone();
            descriptor.artifact_origin = None;
            let bytes = serde_json::to_vec(&ForeignFragment {
                kind: "fragment",
                origin: located.physical_origin.as_origin(),
                descriptor,
            })
            .map_err(|error| {
                ZeppelinError::Serialization(format!(
                    "foreign fragment closure serialization failed: {error}"
                ))
            })?;
            closure.insert(bytes);
        }
        for located in resolver.located_segments()? {
            if located.physical_origin.as_origin() == &local {
                continue;
            }
            let mut descriptor = located.segment.clone();
            descriptor.artifact_origin = None;
            let bytes = serde_json::to_vec(&ForeignSegment {
                kind: "segment",
                origin: located.physical_origin.as_origin(),
                descriptor,
            })
            .map_err(|error| {
                ZeppelinError::Serialization(format!(
                    "foreign segment closure serialization failed: {error}"
                ))
            })?;
            closure.insert(bytes);
        }
        Ok(closure)
    }

    /// A branch successor may retain or remove inherited descriptors, but it
    /// cannot mutate lineage or introduce a new foreign descriptor.
    fn require_valid_branch_successor(&self, predecessor: &Self, namespace: &str) -> Result<()> {
        if self.branch_lineage != predecessor.branch_lineage {
            if self.branch_lineage.is_some() || predecessor.branch_lineage.is_some() {
                return Err(ZeppelinError::ManifestConflict {
                    namespace: namespace.to_string(),
                });
            }
            return Ok(());
        }
        if predecessor.branch_lineage.is_none() {
            return Ok(());
        }
        predecessor.validate_branch_lineage_state(namespace)?;
        self.validate_branch_lineage_state(namespace)?;
        let predecessor_foreign = predecessor.foreign_descriptor_closure()?;
        let successor_foreign = self.foreign_descriptor_closure()?;
        if !successor_foreign.is_subset(&predecessor_foreign) {
            return Err(ZeppelinError::ManifestConflict {
                namespace: namespace.to_string(),
            });
        }
        Ok(())
    }

    /// Reads the live manifest together with the ETag needed for CAS publication.
    ///
    /// # Parameters
    ///
    /// - `store`: Borrowed storage abstraction used for a metadata-bearing GET.
    /// - `namespace`: Namespace whose current candidate base should be loaded.
    ///
    /// # Returns
    ///
    /// `Some((manifest, version))` when present, binding the backend ETag and
    /// deletion-fence state from that same read, or `None` for not found.
    ///
    /// # Errors
    ///
    /// Propagates storage and decoding failures. Corrupt bytes never become an
    /// empty manifest.
    ///
    /// # Consistency
    ///
    /// The returned manifest and ETag come from the same object-store read. The
    /// ETag is an opaque capability for [`Manifest::write_conditional`], not a
    /// manifest generation.
    ///
    /// # Performance
    ///
    /// Performs one full object-store GET with metadata.
    ///
    /// # Examples
    ///
    /// A writer reads generation 12 with ETag `E12`, mutates the owned manifest,
    /// and later presents `E12`. If another writer publishes first, `E12` no
    /// longer matches and the update is rejected.
    pub async fn read_versioned(
        store: &ZeppelinStore,
        namespace: &str,
    ) -> Result<Option<(Self, ManifestVersion)>> {
        let key = Self::s3_key(namespace);
        match store.get_with_meta(&key).await {
            Ok((data, observed)) => {
                let manifest = Self::from_bytes_for_namespace(&data, namespace)?;
                let version = ManifestVersion::for_manifest(observed, &manifest, data, false);
                Ok(Some((manifest, version)))
            }
            Err(crate::error::ZeppelinError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Reads a published live manifest that must exist.
    ///
    /// Active namespace paths use this form after metadata has established that
    /// a live manifest must exist. A missing object is therefore an integrity
    /// failure rather than empty namespace state. Published legacy manifests
    /// that predate generation tracking remain valid at generation zero.
    pub(crate) async fn read_versioned_required(
        store: &ZeppelinStore,
        namespace: &str,
    ) -> Result<(Self, ManifestVersion)> {
        let key = Self::s3_key(namespace);
        let (data, observed) = store.get_with_meta(&key).await?;
        let manifest = Self::from_bytes_for_namespace(&data, namespace)?;
        let version = ManifestVersion::for_manifest(observed, &manifest, data, false);
        Ok((manifest, version))
    }

    /// Reads one authoritative manifest and binds a legacy generation to its
    /// namespace incarnation with CAS before returning it as write authority.
    ///
    /// Manifests created before incarnation binding decode with `None`. When
    /// namespace metadata already carries the durable incarnation, this method
    /// publishes a data-identical successor generation containing that identity.
    /// A concurrent writer or delete/recreate changes the live ETag; the method
    /// then reloads and either observes the expected binding or fails on a
    /// different incarnation. No cross-object check is used after migration:
    /// all later guards bind generation, incarnation, and ETag from one GET.
    pub async fn read_versioned_required_for_incarnation(
        store: &ZeppelinStore,
        namespace: &str,
        expected_incarnation: uuid::Uuid,
    ) -> Result<(Self, ManifestVersion)> {
        const MAX_MIGRATION_ATTEMPTS: usize = 8;

        for _ in 0..MAX_MIGRATION_ATTEMPTS {
            let (mut manifest, version) = Self::read_versioned_required(store, namespace).await?;
            version.require_version(namespace, "incarnation-bound manifest read")?;
            match manifest.namespace_incarnation() {
                Some(actual) if actual == expected_incarnation => {
                    return Ok((manifest, version));
                }
                Some(_) => {
                    return Err(ZeppelinError::ManifestConflict {
                        namespace: namespace.to_string(),
                    });
                }
                None => {
                    manifest.bind_namespace_incarnation(expected_incarnation)?;
                    match manifest.write_conditional(store, namespace, &version).await {
                        Ok(new_version) => return Ok((manifest, new_version)),
                        Err(ZeppelinError::ManifestConflict { .. }) => continue,
                        Err(error) => return Err(error),
                    }
                }
            }
        }

        Err(ZeppelinError::ManifestConflict {
            namespace: namespace.to_string(),
        })
    }

    /// Reads a required published manifest without exposing its object ETag.
    pub(crate) async fn read_required(store: &ZeppelinStore, namespace: &str) -> Result<Self> {
        Self::read_versioned_required(store, namespace)
            .await
            .map(|(manifest, _)| manifest)
    }

    /// Publishes the next generation using ETag compare-and-swap when available.
    ///
    /// ```text
    /// authoritative live N
    ///         |
    ///         v
    /// create immutable history N
    ///         |
    ///         v
    /// PUT candidate N+1 if ETag matches ---- mismatch
    ///         |                                |
    ///         v                                v
    /// candidate becomes N+1            reload authoritative state
    /// ```
    ///
    /// # Parameters
    ///
    /// - `store`: Borrowed storage abstraction configured for conditional PUT.
    /// - `namespace`: Namespace whose live manifest is being replaced.
    /// - `version`: ETag returned with the base manifest. `None` permits only a
    ///   create-only first write and cannot overwrite or resurrect a namespace.
    ///
    /// # Returns
    ///
    /// The new live-object ETag after the conditional PUT succeeds;
    /// `self.version` then advances by exactly one. Backends that omit an ETag
    /// produce an unversioned result without fabricating a CAS capability.
    ///
    /// # Errors
    ///
    /// Returns on generation overflow, serialization, predecessor-history I/O,
    /// live PUT, or ETag conflict. A failed live PUT never creates history for
    /// the speculative generation and does not advance `self.version`. Uploaded
    /// data artifacts referenced only by this candidate also remain invisible.
    ///
    /// # Side Effects
    ///
    /// Creates or validates immutable history for an unconfirmed authoritative
    /// predecessor, attempts one ETag-conditional or create-only live PUT, then
    /// creates or validates immutable history for the committed winner.
    ///
    /// # Consistency
    ///
    /// CAS prevents a stale base from overwriting a newer live manifest. Writer
    /// call sites must also validate lease fencing before this operation; the
    /// ETag alone does not identify a stale lease holder before its write.
    ///
    /// # Examples
    ///
    /// Two writers read ETag `E7`. The first publishes generation 8. The second
    /// receives [`ZeppelinError::ManifestConflict`] and must reload rather than
    /// overwrite generation 8.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The method clones `self` into an owned candidate so failure leaves the
    /// caller's generation unchanged. In Java, code would rely on discipline or
    /// another object instance to avoid partially mutating shared state. In C,
    /// it would require explicit copy and cleanup paths. Rust's ownership and
    /// [`Result`] flow make the commit point visible in the final assignment.
    pub async fn write_conditional(
        &mut self,
        store: &ZeppelinStore,
        namespace: &str,
        version: &ManifestVersion,
    ) -> Result<ManifestVersion> {
        if self.deletion_fence.is_some() || version.deletion_fenced {
            return Err(ZeppelinError::NamespaceDeleting {
                namespace: namespace.to_string(),
            });
        }
        self.write_conditional_candidate(store, namespace, version)
            .await
    }

    /// Publishes the generation-one namespace bootstrap without discovery or rebasing.
    ///
    /// Namespace creation must never rewrite a newer live manifest with its
    /// original empty candidate. This seam uses the ordinary conditional writer
    /// with an absent-version capability, so the live PUT is create-only while
    /// receipt finalization and immutable generation-one history remain shared
    /// with every other manifest publication path.
    ///
    /// # Errors
    ///
    /// Returns a serialization error when the candidate is not generation zero,
    /// a manifest conflict when the live key already exists, or the underlying
    /// storage/serialization error. A conflict leaves `self` unchanged.
    pub(crate) async fn publish_initial_create_only(
        &mut self,
        store: &ZeppelinStore,
        namespace: &str,
    ) -> Result<ManifestVersion> {
        if self.version != 0 {
            return Err(ZeppelinError::Serialization(format!(
                "initial manifest publication for namespace {namespace} requires generation zero, got {}",
                self.version
            )));
        }
        self.write_conditional(store, namespace, &ManifestVersion::unversioned())
            .await
    }

    async fn write_conditional_candidate(
        &mut self,
        store: &ZeppelinStore,
        namespace: &str,
        version: &ManifestVersion,
    ) -> Result<ManifestVersion> {
        let key = Self::s3_key(namespace);
        if version.has_version() {
            let predecessor_bytes = version.exact_manifest_bytes()?;
            let predecessor = Self::from_bytes_for_namespace(&predecessor_bytes, namespace)?;
            if predecessor.version != self.version {
                return Err(ZeppelinError::ManifestConflict {
                    namespace: namespace.to_string(),
                });
            }
            self.require_valid_branch_successor(&predecessor, namespace)?;
        }
        if self.version() > 0 && !version.history_confirmed {
            let data = version.history_snapshot_bytes(namespace, self.version())?;
            Self::write_immutable_history_snapshot(store, namespace, self.version(), data).await?;
        }
        let next_version = self.next_committed_version()?;
        let mut committed = self.clone();
        committed.version = next_version;
        committed.namespace = Some(namespace.to_string());
        committed.finalize_receipt_root(store, namespace)?;
        let data = committed.to_bytes()?;
        let published = match &version.version {
            Some(observed) => {
                store
                    .put_if_match(&key, data.clone(), observed, namespace)
                    .await?
            }
            None => match store.put_create_outcome(&key, data.clone()).await? {
                CreateOnlyOutcome::Created { version } => version,
                CreateOnlyOutcome::AlreadyExists => {
                    return Err(ZeppelinError::ManifestConflict {
                        namespace: namespace.to_string(),
                    });
                }
            },
        };
        Self::write_immutable_history_snapshot(store, namespace, committed.version(), data.clone())
            .await?;
        let new_version = ManifestVersion::for_manifest(published, &committed, data, true);
        store.forget_known_content_hashes(committed.artifact_hashes.keys());
        *self = committed;
        Ok(new_version)
    }

    /// CAS-publish the governed-destruction fence and return its exact manifest.
    #[cfg_attr(not(feature = "branching-test-support"), allow(dead_code))]
    pub(crate) async fn fence_for_destruction(
        store: &ZeppelinStore,
        namespace: &str,
        destruction_record_key: &str,
    ) -> Result<Self> {
        const MAX_FENCE_ATTEMPTS: usize = 8;
        validate_destruction_record_key(destruction_record_key)?;
        for _ in 0..MAX_FENCE_ATTEMPTS {
            let (mut manifest, version) = Self::read_versioned_required(store, namespace).await?;
            version.require_version(namespace, "governed destruction fence")?;
            if !manifest.branch_roots.is_empty() {
                return Err(BranchError::NamespaceHasLiveBranches {
                    namespace: namespace.to_string(),
                    visible_children: Vec::new(),
                    has_additional_children: true,
                }
                .into());
            }
            match &manifest.deletion_fence {
                Some(existing) if existing.destruction_record_key == destruction_record_key => {
                    return Ok(manifest);
                }
                Some(_) => {
                    return Err(ZeppelinError::Validation(format!(
                        "namespace {namespace} manifest is fenced by different destruction evidence"
                    )));
                }
                None => {}
            }
            manifest.deletion_fence = Some(ManifestDeletionFence {
                destruction_record_key: destruction_record_key.to_string(),
            });
            match manifest
                .write_conditional_candidate(store, namespace, &version)
                .await
            {
                Ok(_) => return Ok(manifest),
                Err(ZeppelinError::ManifestConflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(ZeppelinError::ManifestConflict {
            namespace: namespace.to_string(),
        })
    }

    /// Lease-fence and CAS-publish the governed destruction marker.
    pub(crate) async fn fence_for_destruction_with_lease(
        store: &ZeppelinStore,
        lease_manager: &crate::wal::LeaseManager,
        lease: &crate::wal::Lease,
        namespace: &str,
        destruction_record_key: &str,
    ) -> Result<(Self, crate::wal::Lease)> {
        const MAX_FENCE_ATTEMPTS: usize = 8;
        validate_destruction_record_key(destruction_record_key)?;
        let mut current_lease = lease.clone();
        for _ in 0..MAX_FENCE_ATTEMPTS {
            let (mut manifest, version) = Self::read_versioned_required(store, namespace).await?;
            version.require_version(namespace, "governed destruction fence")?;
            if !manifest.branch_roots.is_empty() {
                return Err(BranchError::NamespaceHasLiveBranches {
                    namespace: namespace.to_string(),
                    visible_children: Vec::new(),
                    has_additional_children: true,
                }
                .into());
            }
            match &manifest.deletion_fence {
                Some(existing) if existing.destruction_record_key == destruction_record_key => {
                    return Ok((manifest, current_lease));
                }
                Some(_) => {
                    return Err(ZeppelinError::Validation(format!(
                        "namespace {namespace} manifest is fenced by different destruction evidence"
                    )));
                }
                None => {}
            }
            let renewed = lease_manager.renew(namespace, &current_lease).await?;
            if !lease_manager.validate(&renewed) {
                return Err(ZeppelinError::LeaseExpired {
                    namespace: namespace.to_string(),
                });
            }
            if manifest.fencing_token() > renewed.fencing_token {
                return Err(ZeppelinError::FencingTokenStale {
                    namespace: namespace.to_string(),
                    our_token: renewed.fencing_token,
                    manifest_token: manifest.fencing_token(),
                });
            }
            manifest.fencing_token = renewed.fencing_token;
            manifest.deletion_fence = Some(ManifestDeletionFence {
                destruction_record_key: destruction_record_key.to_string(),
            });
            match manifest
                .write_conditional_candidate(store, namespace, &version)
                .await
            {
                Ok(_) => return Ok((manifest, renewed)),
                Err(ZeppelinError::ManifestConflict { .. }) => current_lease = renewed,
                Err(error) => return Err(error),
            }
        }
        Err(ZeppelinError::ManifestConflict {
            namespace: namespace.to_string(),
        })
    }

    /// Verify that this exact manifest is governed by the expected evidence key.
    pub(crate) fn require_destruction_fence(
        &self,
        namespace: &str,
        destruction_record_key: &str,
        expected_version: u64,
    ) -> Result<()> {
        if self.version != expected_version
            || self
                .deletion_fence
                .as_ref()
                .map(|fence| fence.destruction_record_key.as_str())
                != Some(destruction_record_key)
        {
            return Err(ZeppelinError::ManifestConflict {
                namespace: namespace.to_string(),
            });
        }
        Ok(())
    }

    /// Lists retained manifest history descriptors in ascending generation order.
    ///
    /// # Parameters
    ///
    /// - `store`: Borrowed storage abstraction used to list history keys.
    /// - `namespace`: Namespace whose retained generations should be listed.
    ///
    /// # Returns
    ///
    /// Descriptors sorted by numeric generation. An empty vector means no
    /// history objects are retained. A crash between live CAS and the matching
    /// history PUT can transiently omit the current generation until the next
    /// writer repairs it from its version-bound live bytes.
    ///
    /// # Errors
    ///
    /// Propagates LIST failures and rejects any key under the history prefix that
    /// does not have the exact 20-digit `.msgpack` generation shape.
    ///
    /// # Performance
    ///
    /// Performs one prefix LIST, allocates one descriptor per returned key, and
    /// sorts them in `O(n log n)` time.
    ///
    /// # Examples
    ///
    /// Keys for generations 10, 2, and 3 are returned as versions 2, 3, 10 even
    /// if the backend's listing order differs.
    pub async fn list_history(
        store: &ZeppelinStore,
        namespace: &str,
    ) -> Result<Vec<ManifestHistoryRef>> {
        Ok(Self::list_history_observations(store, namespace)
            .await?
            .into_iter()
            .map(|observation| observation.history)
            .collect())
    }

    /// Lists history refs together with the opaque version observed by LIST.
    ///
    /// Every invocation performs a fresh metadata-preserving prefix LIST. The
    /// result is sorted by parsed generation, matching [`Self::list_history`],
    /// and a missing backend version remains `None` so disposable callers cannot
    /// mistake two unversioned observations for proof that an object is unchanged.
    pub(crate) async fn list_history_observations(
        store: &ZeppelinStore,
        namespace: &str,
    ) -> Result<Vec<ManifestHistoryObservation>> {
        let prefix = Self::history_prefix(namespace);
        Self::history_observations_from_listed(namespace, store.list_prefix_meta(&prefix).await?)
    }

    /// Parses one metadata-preserving LIST result using the canonical key grammar.
    pub(crate) fn history_observations_from_listed(
        namespace: &str,
        listed: Vec<ListedObject>,
    ) -> Result<Vec<ManifestHistoryObservation>> {
        let mut observations = listed
            .into_iter()
            .map(|object| {
                let key = object.key.clone();
                let version = Self::history_version_from_key(namespace, &key).map_err(|error| {
                    ZeppelinError::MalformedControlKey {
                        family: "manifest-history",
                        key: key.clone(),
                        reason: error.to_string(),
                    }
                })?;
                Ok(ManifestHistoryObservation {
                    history: ManifestHistoryRef {
                        version,
                        key: object.key,
                    },
                    storage_version: object.version,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        observations.sort_by(|left, right| {
            left.history
                .version
                .cmp(&right.history.version)
                .then_with(|| left.history.key.cmp(&right.history.key))
        });
        Ok(observations)
    }

    /// Reads and validates a retained manifest by persisted generation.
    ///
    /// # Parameters
    ///
    /// - `store`: Borrowed storage abstraction used for one full GET.
    /// - `namespace`: Namespace owning the history object.
    /// - `version`: Exact generation encoded in the history key.
    ///
    /// # Returns
    ///
    /// `Some(manifest)` when the physical immutable history object is retained,
    /// or `None` when that generation key is absent.
    ///
    /// # Errors
    ///
    /// Propagates storage and decoding failures. It also returns a serialization
    /// error if the manifest payload's generation differs from the key, because
    /// accepting that mismatch would make PITR address the wrong state.
    ///
    /// # Consistency
    ///
    /// History objects are immutable once created. Reading history never
    /// substitutes the mutable live-manifest key for a missing physical object.
    ///
    /// # Examples
    ///
    /// Reading generation 4 may return a view with fewer fragments than the live
    /// generation 9. A missing generation returns `None`, never the live or
    /// nearest retained generation.
    pub async fn read_history(
        store: &ZeppelinStore,
        namespace: &str,
        version: u64,
    ) -> Result<Option<Self>> {
        let key = Self::history_key(namespace, version);
        let history = ManifestHistoryRef { version, key };
        match store.get(&history.key).await {
            Ok(data) => Ok(Some(Self::decode_history_body(&data, namespace, &history)?)),
            Err(crate::error::ZeppelinError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Decodes one history body against its observed namespace and generation.
    ///
    /// This crate-visible seam is shared by direct reads and disposable history
    /// memos. It deliberately delegates namespace binding to
    /// [`Self::from_bytes_for_namespace`] and performs the same persisted-version
    /// check as [`Self::read_history`], keeping one validation grammar for every
    /// body consumer.
    pub(crate) fn decode_history_body(
        data: &[u8],
        namespace: &str,
        history: &ManifestHistoryRef,
    ) -> Result<Self> {
        let manifest = Self::from_bytes_for_namespace(data, namespace)?;
        if manifest.version() != history.version {
            return Err(ZeppelinError::Serialization(format!(
                "manifest history key {} contains version {}, expected {}",
                history.key,
                manifest.version(),
                history.version
            )));
        }
        Ok(manifest)
    }

    /// Deletes old history using only a most-recent-count retention rule.
    ///
    /// # Parameters
    ///
    /// - `store`: Borrowed storage abstraction used to list, read, and delete.
    /// - `namespace`: Namespace whose history should be pruned.
    /// - `keep_count`: Number of newest generations to retain; must be nonzero.
    ///
    /// # Returns
    ///
    /// Number of deleted history objects.
    ///
    /// # Errors
    ///
    /// Propagates validation, list, read, decode, snapshot-list, and delete
    /// failures from [`Manifest::prune_history_with_retention`]. Earlier batches
    /// may already have succeeded if a later batch fails.
    ///
    /// # Examples
    ///
    /// With generations 1 through 4 and `keep_count = 2`, this deletes 1 and 2,
    /// unless a named snapshot or current live branch root protects either
    /// generation.
    pub async fn prune_history(
        store: &ZeppelinStore,
        namespace: &str,
        keep_count: usize,
    ) -> Result<usize> {
        Ok(Self::prune_history_with_retention(
            store,
            namespace,
            ManifestHistoryRetention {
                keep_count,
                pitr_retention_secs: 0,
                skew_slop_secs: 0,
            },
        )
        .await?
        .pruned)
    }

    /// Prunes history while retaining count, time, named-pin, and live-root union.
    ///
    /// ```text
    /// history generation
    ///       |
    ///       +-- among newest keep_count? -------- keep
    ///       +-- inside PITR window + skew? ------ keep
    ///       +-- named snapshot pins it? --------- keep
    ///       +-- current live branch root? ------- keep
    ///       `-- none apply ---------------------- delete
    /// ```
    ///
    /// # Parameters
    ///
    /// - `store`: Borrowed storage abstraction for history and snapshot objects.
    /// - `namespace`: Namespace whose history should be evaluated.
    /// - `retention`: Copyable policy; `keep_count` must be greater than zero.
    ///
    /// # Returns
    ///
    /// The number deleted plus decoded retained manifests in ascending
    /// generation order. Garbage collection uses the retained manifests to
    /// preserve every artifact still reachable through PITR.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for a zero count. Propagates listing,
    /// decoding, malformed-key, missing-between-list-and-read, and deletion
    /// failures. All retention bodies validate before deletion starts. Deletion
    /// is not transactional across 1,000-key batches: earlier batches may
    /// already be gone when a later batch fails, and a failed batch is
    /// conservatively treated as uncertain.
    ///
    /// # Side Effects
    ///
    /// GETs the live manifest, lists history and named pins, GETs every history
    /// manifest, revalidates the live manifest before destructive work, and
    /// DELETEs generations kept by no rule. It does not modify the live manifest.
    ///
    /// # Consistency
    ///
    /// Retention is an OR, not an AND. `skew_slop_secs` extends only an enabled
    /// PITR time window. Named pins are read before pruning so a generation
    /// observed as pinned in this pass is not deleted. Current roots and their
    /// ETag-bound manifest identity are revalidated before a history DELETE.
    /// The pin LIST and history DELETEs are not one object-store transaction; a
    /// pin created concurrently after the LIST can race this pass, so higher
    /// layers must serialize those operations when they require a stronger
    /// creation-versus-prune guarantee.
    ///
    /// # Performance
    ///
    /// Performs two live-manifest GETs when deletion is possible, one history
    /// LIST, one snapshot LIST plus a GET per pin, one GET per history entry,
    /// and at most one DELETE request per 1,000 pruned generations.
    ///
    /// # Examples
    ///
    /// If generation 2 is pinned, generation 3 is within the time window,
    /// generation 4 is rooted by a current child, and generation 5 is newest,
    /// all four survive while an unprotected generation 1 is deleted.
    pub async fn prune_history_with_retention(
        store: &ZeppelinStore,
        namespace: &str,
        retention: ManifestHistoryRetention,
    ) -> Result<ManifestHistoryPruneResult> {
        Self::prune_history_with_retention_at(store, namespace, retention, Utc::now()).await
    }

    /// Prunes manifest history using one explicit wall-clock timestamp.
    pub async fn prune_history_with_retention_at(
        store: &ZeppelinStore,
        namespace: &str,
        retention: ManifestHistoryRetention,
        now: DateTime<Utc>,
    ) -> Result<ManifestHistoryPruneResult> {
        if retention.keep_count == 0 {
            return Err(ZeppelinError::Config(
                "gc.manifest_history_keep_count must be greater than zero".to_string(),
            ));
        }
        let (live, live_version) = Self::read_versioned_required(store, namespace).await?;
        let observed_version = live_version
            .require_version(namespace, "branch-root history retention")?
            .clone();
        let rooted_generations = live.rooted_generations()?;
        let history = Self::list_history(store, namespace).await?;
        let keep_from = history.len().saturating_sub(retention.keep_count);
        let pinned_generations = NamedSnapshot::pinned_generations(store, namespace).await?;
        let mut retained_manifests = Vec::new();
        let mut prunable = Vec::new();
        let mut observed_rooted_generations = BTreeSet::new();
        for (index, entry) in history.iter().enumerate() {
            let bytes = store.get(&entry.key).await?;
            let manifest = Self::decode_history_body(&bytes, namespace, entry)?;
            let keep_by_count = index >= keep_from;
            let keep_by_pin = pinned_generations.contains(&entry.version);
            let generation = ManifestGeneration::new(entry.version)?;
            let keep_by_root = rooted_generations.contains_key(&generation);
            if keep_by_root {
                live.validate_rooted_history_bytes(generation, &bytes)?;
                observed_rooted_generations.insert(generation);
            }
            let retention_window = retention
                .pitr_retention_secs
                .saturating_add(retention.skew_slop_secs);
            let keep_by_time = retention.pitr_retention_secs > 0
                && now.signed_duration_since(manifest.updated_at).num_seconds()
                    <= retention_window as i64;
            if keep_by_count || keep_by_time || keep_by_pin || keep_by_root {
                retained_manifests.push(manifest);
            } else {
                prunable.push(entry.key.clone());
            }
        }
        if let Some(missing) = rooted_generations
            .keys()
            .find(|generation| !observed_rooted_generations.contains(generation))
        {
            return Err(ZeppelinError::NotFound {
                key: Self::history_key(namespace, missing.get()),
            });
        }
        if !prunable.is_empty() {
            let (revalidated, revalidated_version) =
                Self::read_versioned_required(store, namespace).await?;
            let revalidated_identity = revalidated_version
                .require_version(namespace, "branch-root history retention revalidation")?;
            if *revalidated_identity != observed_version
                || revalidated.namespace_incarnation != live.namespace_incarnation
                || revalidated.branch_roots != live.branch_roots
            {
                return Err(ZeppelinError::ManifestConflict {
                    namespace: namespace.to_string(),
                });
            }
        }
        for batch in prunable.chunks(DELETE_MANY_MAX_KEYS) {
            store.delete_many(batch.to_vec()).await?;
        }
        Ok(ManifestHistoryPruneResult {
            pruned: prunable.len(),
            retained_manifests,
        })
    }

    /// Retains the authoritative live generation before it is superseded.
    ///
    /// Competing writers that share one live ETag also share these exact bytes,
    /// so they can idempotently create the same immutable history object while
    /// keeping their divergent next-generation candidates out of history. A
    /// failed live PUT therefore cannot reserve or wedge the next generation.
    ///
    /// # Parameters
    ///
    /// - `store`: Borrowed storage abstraction.
    /// - `namespace`: Namespace whose live generation is about to be replaced.
    /// - `version`: Nonzero predecessor or committed-winner generation.
    /// - `data`: Exact ETag-bound predecessor bytes or exact CAS-winning bytes.
    ///
    /// # Errors
    ///
    /// Propagates storage and serialization failures. Different bytes already
    /// stored at the selected generation are an immutable-history invariant
    /// failure, never an optimistic-concurrency retry.
    ///
    /// # Side Effects
    ///
    /// May create or read one history object. It never overwrites history and
    /// does not publish the replacement live manifest.
    ///
    /// # Consistency
    ///
    /// The source bytes are either an authoritative ETag-bound predecessor or
    /// an already published CAS winner, never a speculative successor. Every
    /// retained history object is write-once.
    ///
    /// # Examples
    ///
    /// Writers A and B both read live generation 7 and may create identical
    /// history 7. If A's candidate-8 PUT fails, B may still publish a different
    /// candidate 8 because no speculative history-8 object was created.
    async fn write_immutable_history_snapshot(
        store: &ZeppelinStore,
        namespace: &str,
        version: u64,
        data: Bytes,
    ) -> Result<()> {
        match Self::try_write_history_snapshot(store, namespace, version, data).await? {
            HistorySnapshotWrite::Stored => Ok(()),
            HistorySnapshotWrite::AlreadyExistsWithDifferentBytes { key } => {
                Err(Self::history_snapshot_mismatch_error(&key))
            }
        }
    }

    /// Creates a history object only when its generation key is absent.
    ///
    /// # Parameters
    ///
    /// - `store`: Borrowed storage abstraction supporting create-if-absent.
    /// - `namespace`: Namespace owning the history generation.
    /// - `committed`: Candidate manifest with an already assigned nonzero
    ///   generation.
    ///
    /// # Returns
    ///
    /// [`HistorySnapshotWrite::Stored`] for a new or byte-identical object, or
    /// `AlreadyExistsWithDifferentBytes` for a generation collision.
    ///
    /// # Errors
    ///
    /// Rejects generation zero and propagates serialization, conditional PUT,
    /// and collision-read failures.
    ///
    /// # Side Effects
    ///
    /// Performs one create-if-absent PUT and, on key collision, one GET.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// [`HistorySnapshotWrite`] is an enum carrying collision data only in the
    /// relevant variant. This resembles a closed Java sealed hierarchy; C would
    /// commonly use a tag plus union whose pairing must be maintained manually.
    /// Rust's exhaustive `match` prevents callers from forgetting an outcome.
    ///
    /// # Examples
    ///
    /// Creating generation 6 for the first time yields `Stored`. Repeating the
    /// exact bytes also yields `Stored`; different bytes return the collision
    /// variant for the caller to classify against the live generation.
    async fn try_write_history_snapshot(
        store: &ZeppelinStore,
        namespace: &str,
        version: u64,
        data: Bytes,
    ) -> Result<HistorySnapshotWrite> {
        if version == 0 {
            return Err(ZeppelinError::Serialization(
                "manifest history requires a committed nonzero version".to_string(),
            ));
        }

        let key = Self::history_key(namespace, version);
        match store.put_if_not_exists(&key, data.clone(), namespace).await {
            Ok(()) => Ok(HistorySnapshotWrite::Stored),
            Err(ZeppelinError::NamespaceAlreadyExists { .. }) => {
                let existing = store.get(&key).await?;
                if existing == data {
                    Ok(HistorySnapshotWrite::Stored)
                } else {
                    Ok(HistorySnapshotWrite::AlreadyExistsWithDifferentBytes { key })
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Builds the invariant error for immutable history-byte disagreement.
    ///
    /// # Parameters
    ///
    /// - `key`: History object key that already contains different bytes.
    ///
    /// # Returns
    ///
    /// A serialization error identifying the conflicting key.
    fn history_snapshot_mismatch_error(key: &str) -> ZeppelinError {
        ZeppelinError::Serialization(format!(
            "manifest history key {key} already exists with different bytes"
        ))
    }

    /// Parses and validates a numeric generation from a history object key.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace whose exact history prefix is required.
    /// - `key`: Complete object-store key returned by prefix listing.
    ///
    /// # Returns
    ///
    /// The `u64` generation encoded as exactly 20 decimal digits before
    /// `.msgpack`.
    ///
    /// # Errors
    ///
    /// Returns a serialization error for an outside-prefix key, wrong suffix,
    /// wrong width, nondigit component, or unparseable number. Strict parsing
    /// fails loudly if unrelated objects appear under the reserved prefix.
    ///
    /// # Examples
    ///
    /// `ns/manifests/00000000000000000042.msgpack` parses as generation 42;
    /// `ns/manifests/latest.msgpack` is rejected.
    fn history_version_from_key(namespace: &str, key: &str) -> Result<u64> {
        let prefix = Self::history_prefix(namespace);
        let Some(name) = key.strip_prefix(&prefix) else {
            return Err(ZeppelinError::Serialization(format!(
                "manifest history key {key} is outside prefix {prefix}"
            )));
        };
        let Some(version_text) = name.strip_suffix(".msgpack") else {
            return Err(ZeppelinError::Serialization(format!(
                "manifest history key {key} must end with .msgpack"
            )));
        };
        if version_text.len() != 20 || !version_text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(ZeppelinError::Serialization(format!(
                "manifest history key {key} has invalid generation component {version_text}"
            )));
        }
        version_text.parse::<u64>().map_err(|e| {
            ZeppelinError::Serialization(format!(
                "manifest history key {key} has unparseable generation {version_text}: {e}"
            ))
        })
    }
}

impl NamedSnapshot {
    /// Builds the object-store prefix reserved for named snapshot pins.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace that owns the pins.
    ///
    /// # Returns
    ///
    /// `<namespace>/snapshots/`, including the trailing slash.
    #[must_use]
    pub fn prefix(namespace: &str) -> String {
        format!("{namespace}/snapshots/")
    }

    /// Validates a snapshot name and builds its immutable pin key.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace that owns the pin.
    /// - `name`: Caller-visible pin name.
    ///
    /// # Returns
    ///
    /// `<namespace>/snapshots/<name>.msgpack`.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Validation`] when the name is empty, longer
    /// than 255 bytes, or contains characters outside ASCII letters, digits,
    /// dash, underscore, and dot.
    ///
    /// # Examples
    ///
    /// `daily.2026-07-08` is accepted; `daily/2026-07-08` is rejected so a user
    /// cannot escape the reserved snapshot prefix.
    pub fn key(namespace: &str, name: &str) -> Result<String> {
        validate_snapshot_name(name)?;
        Ok(format!("{}{}.msgpack", Self::prefix(namespace), name))
    }

    /// Serializes this pin as version-prefixed MessagePack.
    ///
    /// # Returns
    ///
    /// Owned shared bytes in `[0x01][MessagePack payload]` format.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Serialization`] if the generation and timestamp
    /// cannot be encoded. No storage operation occurs here.
    ///
    /// # Examples
    ///
    /// A pin for generation 42 round-trips through
    /// [`NamedSnapshot::from_bytes`] with its creation timestamp unchanged.
    pub fn to_bytes(&self) -> Result<Bytes> {
        let msgpack = rmp_serde::to_vec(self).map_err(|e| {
            ZeppelinError::Serialization(format!("snapshot msgpack serialize: {e}"))
        })?;
        let mut data = Vec::with_capacity(1 + msgpack.len());
        data.push(MANIFEST_FORMAT_MSGPACK);
        data.extend_from_slice(&msgpack);
        Ok(Bytes::from(data))
    }

    /// Decodes a current MessagePack or legacy JSON snapshot pin.
    ///
    /// # Parameters
    ///
    /// - `data`: Borrowed complete pin-object bytes.
    ///
    /// # Returns
    ///
    /// An owned pin. `0x01` selects prefixed MessagePack, `{` selects legacy
    /// JSON, and other prefixes are tried as prefixed then unprefixed MessagePack.
    ///
    /// # Errors
    ///
    /// Rejects empty objects and malformed or incompatible encodings with a
    /// serialization error. Unlike [`Manifest::from_bytes`], an empty pin never
    /// means a valid default.
    ///
    /// # Examples
    ///
    /// Both a legacy JSON pin and bytes returned by [`NamedSnapshot::to_bytes`]
    /// decode to the same domain fields.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(ZeppelinError::Serialization(
                "snapshot pin object is empty".to_string(),
            ));
        }
        match data[0] {
            MANIFEST_FORMAT_MSGPACK => rmp_serde::from_slice(&data[1..]).map_err(|e| {
                ZeppelinError::Serialization(format!("snapshot msgpack deserialize: {e}"))
            }),
            b'{' => Ok(serde_json::from_slice(data)?),
            _ => rmp_serde::from_slice(&data[1..])
                .or_else(|_| rmp_serde::from_slice(data))
                .map_err(|e| {
                    ZeppelinError::Serialization(format!("snapshot msgpack deserialize: {e}"))
                }),
        }
    }

    /// Creates or idempotently confirms a named pin for a retained generation.
    ///
    /// ```text
    /// validate name and generation
    ///             |
    ///             v
    /// history generation exists? ---- no ---> Validation error
    ///             |
    ///             v
    /// create pin if absent
    ///       /             \
    ///  created       name already exists
    ///    |             /             \
    ///    v        same generation   different generation
    /// success         success        SnapshotAlreadyExists
    /// ```
    ///
    /// # Parameters
    ///
    /// - `store`: Borrowed storage abstraction.
    /// - `namespace`: Namespace that owns both history and the new pin.
    /// - `name`: Valid caller-facing pin name.
    /// - `generation`: Nonzero retained manifest generation to protect.
    ///
    /// # Returns
    ///
    /// The addressable pin. Repeating the same name and generation returns the
    /// original pin, including its original `created_at` value.
    ///
    /// # Errors
    ///
    /// Returns validation errors for bad names, zero generations, or missing
    /// history. Returns [`ZeppelinError::SnapshotAlreadyExists`] when the name
    /// already pins a different generation. Storage and decoding failures are
    /// propagated. A failed create-if-absent does not overwrite the old pin.
    ///
    /// # Side Effects
    ///
    /// GETs the referenced history, conditionally PUTs the pin, and may GET an
    /// existing pin to decide whether the request is an idempotent retry.
    ///
    /// # Consistency
    ///
    /// The history existence check rejects a generation already absent at that
    /// point. It is a separate GET from the pin PUT, so concurrent history
    /// pruning can race between them unless higher layers serialize those
    /// operations. Create-if-absent makes a snapshot name immutable: changing
    /// its target requires deletion followed by explicit recreation.
    ///
    /// # Examples
    ///
    /// Creating `before-migration` for retained generation 7 twice succeeds and
    /// returns the same timestamp. Requesting generation 8 under that name is a
    /// conflict rather than a silent retarget.
    pub async fn create(
        store: &ZeppelinStore,
        namespace: &str,
        name: &str,
        generation: u64,
    ) -> Result<NamedSnapshotRef> {
        Self::create_at(store, namespace, name, generation, Utc::now()).await
    }

    /// Creates a named snapshot pin with an explicit creation timestamp.
    pub async fn create_at(
        store: &ZeppelinStore,
        namespace: &str,
        name: &str,
        generation: u64,
        now: DateTime<Utc>,
    ) -> Result<NamedSnapshotRef> {
        if generation == 0 {
            return Err(ZeppelinError::Validation(
                "snapshot generation must be a committed nonzero manifest generation".into(),
            ));
        }
        let key = Self::key(namespace, name)?;
        let retained = Manifest::read_history(store, namespace, generation)
            .await?
            .is_some();
        let current = if retained {
            false
        } else {
            Manifest::read(store, namespace)
                .await?
                .is_some_and(|live| live.version() == generation)
        };
        if !retained && !current {
            return Err(ZeppelinError::Validation(format!(
                "snapshot generation {generation} is not retained for namespace {namespace}"
            )));
        }
        let snapshot = Self {
            generation,
            created_at: now,
        };
        match store
            .put_if_not_exists(&key, snapshot.to_bytes()?, namespace)
            .await
        {
            Ok(()) => Ok(NamedSnapshotRef {
                name: name.to_string(),
                key,
                generation,
                created_at: snapshot.created_at,
            }),
            Err(ZeppelinError::NamespaceAlreadyExists { .. }) => {
                let existing = Self::read(store, namespace, name)
                    .await?
                    .ok_or_else(|| ZeppelinError::NotFound { key: key.clone() })?;
                if existing.generation == generation {
                    Ok(existing)
                } else {
                    Err(ZeppelinError::SnapshotAlreadyExists {
                        namespace: namespace.to_string(),
                        name: name.to_string(),
                        existing_generation: existing.generation,
                        requested_generation: generation,
                    })
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Reads a named snapshot pin by exact name.
    ///
    /// # Parameters
    ///
    /// - `store`: Borrowed storage abstraction used for one GET.
    /// - `namespace`: Namespace owning the pin.
    /// - `name`: Valid exact pin name.
    ///
    /// # Returns
    ///
    /// `Some` addressable metadata when present or `None` only for a missing key.
    ///
    /// # Errors
    ///
    /// Propagates invalid-name, storage, and decoding errors. Corrupt bytes do
    /// not become a missing pin.
    ///
    /// # Examples
    ///
    /// Reading `before-migration` returns its pinned generation and timestamp;
    /// reading a valid but absent name returns `None`.
    pub async fn read(
        store: &ZeppelinStore,
        namespace: &str,
        name: &str,
    ) -> Result<Option<NamedSnapshotRef>> {
        let key = Self::key(namespace, name)?;
        match store.get(&key).await {
            Ok(data) => {
                let snapshot = Self::from_bytes(&data)?;
                Ok(Some(NamedSnapshotRef {
                    name: name.to_string(),
                    key,
                    generation: snapshot.generation,
                    created_at: snapshot.created_at,
                }))
            }
            Err(ZeppelinError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Lists and decodes all named pins in lexical name order.
    ///
    /// # Parameters
    ///
    /// - `store`: Borrowed storage abstraction used for LIST and GET operations.
    /// - `namespace`: Namespace whose pins should be enumerated.
    ///
    /// # Returns
    ///
    /// Addressable pins sorted by `name`; an empty vector means no pins exist.
    ///
    /// # Errors
    ///
    /// Propagates list/get/decode failures and rejects malformed keys under the
    /// reserved snapshot prefix. The operation fails as a whole rather than
    /// silently skipping a corrupt pin.
    ///
    /// # Performance
    ///
    /// Performs one prefix LIST and one full GET per pin, then sorts in
    /// `O(n log n)` time.
    ///
    /// # Examples
    ///
    /// Pins named `weekly` and `daily` are returned as `daily`, then `weekly`,
    /// regardless of object-store listing order.
    pub async fn list(store: &ZeppelinStore, namespace: &str) -> Result<Vec<NamedSnapshotRef>> {
        Ok(Self::list_observations(store, namespace)
            .await?
            .into_iter()
            .map(|observation| observation.snapshot)
            .collect())
    }

    /// Lists and decodes pins while preserving each LIST-observed identity.
    ///
    /// This is the metadata-preserving implementation behind [`Self::list`].
    /// It performs the same one LIST plus one GET per pin; callers that do not
    /// need the storage observation project only [`NamedSnapshotRef`].
    pub(crate) async fn list_observations(
        store: &ZeppelinStore,
        namespace: &str,
    ) -> Result<Vec<NamedSnapshotObservation>> {
        let prefix = Self::prefix(namespace);
        let mut snapshots = Vec::new();
        for object in
            Self::validate_listed_objects(namespace, store.list_prefix_meta(&prefix).await?)?
        {
            snapshots.push(Self::read_listed_observation(store, namespace, object).await?);
        }
        Ok(snapshots)
    }

    /// Validates snapshot objects supplied by an enclosing namespace LIST.
    ///
    /// This pure seam applies the same exact key grammar and lexical name order
    /// as [`Self::list_observations`] without performing another LIST or any
    /// body GETs. Garbage collection can therefore validate a namespace-wide
    /// inventory first, then retain control of bounded observation reads through
    /// [`Self::read_listed_observation`].
    pub(crate) fn validate_listed_objects(
        namespace: &str,
        listed: Vec<ListedObject>,
    ) -> Result<Vec<ListedObject>> {
        let mut objects = listed
            .into_iter()
            .map(|object| {
                let key = object.key.clone();
                let name = snapshot_name_from_key(namespace, &key).map_err(|error| {
                    ZeppelinError::MalformedControlKey {
                        family: "snapshot",
                        key: key.clone(),
                        reason: error.to_string(),
                    }
                })?;
                Ok((name, object))
            })
            .collect::<Result<Vec<_>>>()?;
        objects.sort_by(|(left_name, left), (right_name, right)| {
            left_name
                .cmp(right_name)
                .then_with(|| left.key.cmp(&right.key))
        });
        Ok(objects.into_iter().map(|(_, object)| object).collect())
    }

    /// Reads and decodes one pin discovered by a metadata-preserving LIST.
    pub(crate) async fn read_listed_observation(
        store: &ZeppelinStore,
        namespace: &str,
        object: ListedObject,
    ) -> Result<NamedSnapshotObservation> {
        let name = snapshot_name_from_key(namespace, &object.key)?;
        let (data, get_version) = store.get_with_meta(&object.key).await?;
        if let Some(list_etag) = object.version.as_ref().and_then(StorageVersion::etag) {
            let get_etag = get_version.as_ref().and_then(StorageVersion::etag);
            if get_etag != Some(list_etag) {
                return Err(ZeppelinError::Serialization(format!(
                    "snapshot pin {} changed between LIST ETag {:?} and GET ETag {:?}",
                    object.key, list_etag, get_etag
                )));
            }
        }
        let snapshot = Self::from_bytes(&data)?;
        Ok(NamedSnapshotObservation {
            snapshot: NamedSnapshotRef {
                name,
                key: object.key.clone(),
                generation: snapshot.generation,
                created_at: snapshot.created_at,
            },
            object,
        })
    }

    /// Deletes a named pin, allowing its generation to age out of retention.
    ///
    /// # Parameters
    ///
    /// - `store`: Borrowed storage abstraction used for the delete.
    /// - `namespace`: Namespace owning the pin.
    /// - `name`: Valid exact pin name.
    ///
    /// # Errors
    ///
    /// Propagates invalid-name and storage delete errors.
    ///
    /// # Side Effects
    ///
    /// Deletes only the small pin object. It does not immediately delete the
    /// history manifest or its referenced artifacts; a later retention/GC pass
    /// decides whether they remain reachable.
    ///
    /// # Examples
    ///
    /// Deleting `before-migration` removes its retention protection. The pinned
    /// generation remains readable until a later history-prune pass removes it.
    pub async fn delete(store: &ZeppelinStore, namespace: &str, name: &str) -> Result<()> {
        let key = Self::key(namespace, name)?;
        store.delete(&key).await
    }

    /// Collects the generations protected by all current named pins.
    ///
    /// # Parameters
    ///
    /// - `store`: Borrowed storage abstraction.
    /// - `namespace`: Namespace whose pins should be inspected.
    ///
    /// # Returns
    ///
    /// A set of unique generation numbers; multiple names may collapse to one
    /// entry when they pin the same generation.
    ///
    /// # Errors
    ///
    /// Propagates every failure from [`NamedSnapshot::list`].
    ///
    /// # Performance
    ///
    /// Performs the LIST/GET work of `list`, then allocates a hash set linear in
    /// the number of distinct pinned generations.
    ///
    /// # Examples
    ///
    /// Pins `daily` and `weekly` may both target generation 7 while `release`
    /// targets generation 9; the returned set is `{7, 9}`.
    pub(crate) async fn pinned_generations(
        store: &ZeppelinStore,
        namespace: &str,
    ) -> Result<HashSet<u64>> {
        Ok(Self::list(store, namespace)
            .await?
            .into_iter()
            .map(|snapshot| snapshot.generation)
            .collect())
    }
}

/// Validates that a snapshot name is one safe object-key component.
///
/// # Parameters
///
/// - `name`: Candidate caller-visible name, measured in bytes.
///
/// # Errors
///
/// Returns [`ZeppelinError::Validation`] unless the name is 1 through 255 bytes
/// and consists only of ASCII alphanumerics, dash, underscore, or dot.
///
/// # Examples
///
/// `release_7.2` is valid; an empty name, non-ASCII text, or `team/snapshot` is
/// rejected.
fn validate_destruction_record_key(key: &str) -> Result<()> {
    let record_id = key
        .strip_prefix("_audit/destruction/")
        .and_then(|suffix| suffix.strip_suffix(".json"))
        .filter(|record_id| {
            record_id.len() == 32
                && record_id
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        .ok_or_else(|| {
            ZeppelinError::Validation("invalid governed destruction record key".to_string())
        })?;
    if record_id.contains('/') {
        return Err(ZeppelinError::Validation(
            "invalid governed destruction record key".to_string(),
        ));
    }
    Ok(())
}

fn validate_snapshot_name(name: &str) -> Result<()> {
    let valid = !name.is_empty()
        && name.len() <= 255
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if valid {
        Ok(())
    } else {
        Err(ZeppelinError::Validation(format!(
            "invalid snapshot name '{name}': must be 1-255 chars and contain only alphanumeric, dash, underscore, or dot characters"
        )))
    }
}

/// Extracts and validates a snapshot name from a listed object-store key.
///
/// # Parameters
///
/// - `namespace`: Namespace whose exact snapshot prefix is required.
/// - `key`: Complete object-store key beneath that reserved prefix.
///
/// # Returns
///
/// An owned name with the prefix and `.msgpack` suffix removed.
///
/// # Errors
///
/// Returns a serialization error for the wrong prefix or suffix, and a
/// validation error if the embedded name violates [`validate_snapshot_name`].
///
/// # Examples
///
/// `ns/snapshots/daily.msgpack` yields `daily`; a nested key is rejected.
fn snapshot_name_from_key(namespace: &str, key: &str) -> Result<String> {
    let prefix = NamedSnapshot::prefix(namespace);
    let Some(name) = key.strip_prefix(&prefix) else {
        return Err(ZeppelinError::Serialization(format!(
            "snapshot key {key} is outside prefix {prefix}"
        )));
    };
    let Some(name) = name.strip_suffix(".msgpack") else {
        return Err(ZeppelinError::Serialization(format!(
            "snapshot key {key} must end with .msgpack"
        )));
    };
    validate_snapshot_name(name)?;
    Ok(name.to_string())
}

/// Opaque object-store ETag used for optimistic manifest publication.
///
/// `Some(etag)` instructs [`Manifest::write_conditional`] to replace only the
/// exact object read by [`Manifest::read_versioned`]. `None` permits only a
/// create-only first write and cannot overwrite an existing namespace.
///
/// # Rust Notes for Java/C Engineers
///
/// This struct gives an optional string a domain name, so APIs cannot as
/// easily confuse an ETag with an arbitrary `Option<String>`. Java would often
/// use a small wrapper class; C would use a struct plus a presence flag. Rust's
/// [`Option`] encodes absence without a null `String`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestVersion {
    /// Backend identity, or `None` only when no conditional version exists.
    version: Option<StorageVersion>,
    /// Whether the same authoritative read observed a governed-deletion fence.
    deletion_fenced: bool,
    /// Exact live bytes paired with this ETag for predecessor-history repair.
    history_snapshot: Option<Bytes>,
    /// Whether this process observed matching history publication succeed.
    history_confirmed: bool,
}

impl ManifestVersion {
    pub(crate) fn for_manifest(
        version: Option<StorageVersion>,
        manifest: &Manifest,
        history_snapshot: Bytes,
        history_confirmed: bool,
    ) -> Self {
        Self {
            version,
            deletion_fenced: manifest.deletion_fence.is_some(),
            history_snapshot: Some(history_snapshot),
            history_confirmed,
        }
    }

    pub(crate) fn unversioned() -> Self {
        Self {
            version: None,
            deletion_fenced: false,
            history_snapshot: None,
            history_confirmed: false,
        }
    }

    fn history_snapshot_bytes(&self, namespace: &str, version: u64) -> Result<Bytes> {
        let data = self.history_snapshot.clone().ok_or_else(|| {
            ZeppelinError::Index(format!(
                "manifest predecessor history for namespace {namespace} requires bytes bound to the authoritative version"
            ))
        })?;
        let history = ManifestHistoryRef {
            version,
            key: Manifest::history_key(namespace, version),
        };
        Manifest::decode_history_body(&data, namespace, &history)?;
        Ok(data)
    }

    /// Return the exact authoritative live bytes paired with this observation.
    #[allow(dead_code)] // Phase 04 root publication consumes the exact ETag-bound bytes.
    pub(crate) fn exact_manifest_bytes(&self) -> Result<Bytes> {
        self.history_snapshot.clone().ok_or_else(|| {
            ZeppelinError::Serialization(
                "manifest version has no exact authoritative bytes".to_string(),
            )
        })
    }

    /// Hash the exact authoritative live bytes paired with this observation.
    #[allow(dead_code)] // Phase 04 root publication consumes the exact ETag-bound digest.
    pub(crate) fn exact_manifest_digest(&self) -> Result<ManifestDigest> {
        Ok(ManifestDigest::new(
            Sha256::digest(self.exact_manifest_bytes()?).into(),
        ))
    }

    /// Borrow the backend ETag carried by this observation, when available.
    ///
    /// Narrower than [`Self::version`]: use this only to compare against a LIST
    /// observation, which reports no other identity form. A conditional write
    /// wants the whole token, because the backend chooses which form it needs.
    #[must_use]
    pub fn e_tag(&self) -> Option<&str> {
        self.version.as_ref().and_then(StorageVersion::etag)
    }

    /// Borrow the whole backend identity carried by this observation.
    ///
    /// `None` means the read observed no identity at all, which is not write
    /// authority; pass the result through [`StorageVersion::require`] before
    /// using it as a compare-and-swap precondition.
    #[must_use]
    pub fn version(&self) -> Option<&StorageVersion> {
        self.version.as_ref()
    }

    /// Consume this observation and yield the backend identity it carried.
    pub(crate) fn into_storage_version(self) -> Option<StorageVersion> {
        self.version
    }

    /// Returns whether this observation carries any backend identity.
    ///
    /// Deliberately not ETag-specific: a GCS generation is just as usable a
    /// precondition, so a caller asking "can I CAS on this?" must not be told
    /// no merely because the ETag form is absent.
    pub(crate) fn has_version(&self) -> bool {
        self.version.is_some()
    }

    /// Return whether the same authoritative read observed a deletion fence.
    #[must_use]
    #[allow(dead_code)] // Phase 04 root publication maps a fenced base before mutation.
    pub(crate) const fn is_deletion_fenced(&self) -> bool {
        self.deletion_fenced
    }

    /// Returns the backend identity required to replace an existing manifest.
    ///
    /// A missing identity is never converted into unconditional write authority.
    /// Callers that derive a mutation from an existing live object must stop
    /// before uploading history or replacing the live manifest.
    pub(crate) fn require_version(
        &self,
        namespace: &str,
        operation: &str,
    ) -> Result<&StorageVersion> {
        self.version.as_ref().ok_or_else(|| {
            ZeppelinError::Index(format!(
                "{operation} for namespace {namespace} requires an object-store version token"
            ))
        })
    }

    /// Returns the ETag specifically, for callers that compare against a LIST.
    ///
    /// Distinct from [`Self::require_version`]: a conditional write accepts any
    /// identity form the backend defines, but comparing a GET against a LIST
    /// observation is only meaningful on the ETag, which is the one form both
    /// responses report. Callers needing that comparison must fail rather than
    /// silently accept a token they cannot compare.
    pub(crate) fn require_etag(&self, namespace: &str, operation: &str) -> Result<&str> {
        self.version
            .as_ref()
            .and_then(StorageVersion::etag)
            .ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "{operation} for namespace {namespace} requires an object-store ETag"
                ))
            })
    }
}

impl Default for Manifest {
    /// Returns the same unpublished empty value as [`Manifest::new`].
    ///
    /// # Returns
    ///
    /// A generation-zero manifest with no visible artifacts.
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Wire-compatibility and manifest-state regression tests.
    //!
    //! These unit tests use the in-memory object-store backend to isolate the
    //! manifest state machine. Integration coverage elsewhere exercises the same
    //! contracts against S3/MinIO. Local replica structs intentionally preserve
    //! old positional MessagePack shapes so adding or moving a persisted field
    //! breaks a focused compatibility test.

    use super::*;
    use proptest::prelude::*;

    #[test]
    fn conditional_manifest_versions_reject_missing_or_empty_etags() {
        // An empty ETag used to need its own arm here. It is now unrepresentable:
        // `StorageVersion` has one constructor and it treats empty as absent, so
        // the "empty token" case can only exist as `None`.
        assert_eq!(StorageVersion::from_parts(Some(String::new()), None), None);
        assert_eq!(StorageVersion::from_parts(None, Some(String::new())), None);
        assert_eq!(StorageVersion::from_parts(None, None), None);

        for version in [
            ManifestVersion::unversioned(),
            ManifestVersion {
                version: None,
                deletion_fenced: false,
                history_snapshot: None,
                history_confirmed: false,
            },
        ] {
            let error = version
                .require_version("catalog", "legacy manifest incarnation migration")
                .expect_err("existing-manifest migration must never fall back to a plain PUT");
            assert!(matches!(error, ZeppelinError::Index(_)));
        }
    }

    use object_store::memory::InMemory;
    use std::sync::Arc;

    use crate::storage::ZeppelinStore;

    /// Builds a minimal legacy-layout segment descriptor for state-model tests.
    ///
    /// The returned segment owns every cluster itself and records no optional
    /// side artifacts, making individual tests explicit about fields they vary.
    ///
    /// # Parameters
    ///
    /// - `id`: Segment identifier copied into the owned descriptor.
    ///
    /// # Returns
    ///
    /// A four-cluster, 100-vector descriptor using no quantization.
    fn make_segment(id: &str) -> SegmentRef {
        SegmentRef {
            id: id.to_string(),
            vector_count: 100,
            cluster_count: 4,
            quantization: crate::index::quantization::QuantizationType::None,
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
        }
    }

    fn origin(namespace: &str, incarnation: u128) -> ArtifactOrigin {
        ArtifactOrigin {
            namespace: crate::namespace::NamespaceId::parse(namespace)
                .expect("origin fixture namespace must be valid"),
            incarnation: crate::namespace::NamespaceIncarnationId::from_uuid(
                uuid::Uuid::from_u128(incarnation),
            ),
        }
    }

    fn branch_root(
        branch_id: BranchId,
        generation: u64,
        source_manifest_sha256: ManifestDigest,
        target: &str,
        target_incarnation: u128,
    ) -> BranchRoot {
        BranchRoot {
            branch_id,
            source_generation: ManifestGeneration::new(generation)
                .expect("root generation must be nonzero"),
            source_manifest_sha256,
            fork_view_sha256: crate::namespace::ForkViewDigest::new([2; 32]),
            source_config_sha256: crate::namespace::SourceDataPlaneConfigDigest::new([3; 32]),
            target_namespace: NamespaceId::parse(target)
                .expect("branch target fixture must be valid"),
            target_incarnation: NamespaceIncarnationId::from_uuid(uuid::Uuid::from_u128(
                target_incarnation,
            )),
            created_at: Utc::now(),
        }
    }

    fn lineage_seed(
        source: &ArtifactOrigin,
        generation: u64,
        branch_id: u128,
        depth: u16,
    ) -> BranchLineageSeed {
        BranchLineageSeed {
            branch_id: BranchId::from_ulid(Ulid::from(branch_id)),
            parent_namespace: source.namespace.clone(),
            parent_incarnation: source.incarnation.clone(),
            fork_generation: ManifestGeneration::new(generation).unwrap(),
            fork_manifest_sha256: ManifestDigest::new([0x41; 32]),
            source_config_sha256: SourceDataPlaneConfigDigest::new([0x42; 32]),
            depth,
            created_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        }
    }

    fn bound_manifest(identity: &ArtifactOrigin, generation: u64) -> Manifest {
        let mut manifest = Manifest::new_at(DateTime::from_timestamp(1_700_000_000, 0).unwrap());
        manifest.namespace = Some(identity.namespace.to_string());
        manifest
            .bind_namespace_incarnation(identity.incarnation.as_uuid())
            .unwrap();
        manifest.version = generation;
        manifest
    }

    #[test]
    fn receipt_v2_digest_binds_segment_origin_index() {
        let mut manifest = Manifest::new();
        manifest.namespace = Some("target".to_string());
        manifest
            .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
            .expect("target manifest must bind one incarnation");
        manifest.artifact_origins = vec![origin("source-a", 2), origin("source-b", 3)];
        let mut segment = make_segment("segment-origin");
        segment.artifact_origin = Some(ArtifactOriginIndex::new(0));
        manifest.segments.push(segment);
        manifest.receipt_binding_version = Some(ReceiptBindingVersion::V2Origins);

        let baseline = manifest
            .recompute_receipt_state_digest("target")
            .expect("origin-aware receipt projection must encode");
        manifest.segments[0].artifact_origin = Some(ArtifactOriginIndex::new(1));

        assert_ne!(
            baseline,
            manifest
                .recompute_receipt_state_digest("target")
                .expect("mutated origin-aware receipt projection must encode"),
            "changing only an origin index must change the receipt digest"
        );
    }

    #[test]
    fn decode_rejects_out_of_bounds_artifact_origin_index() {
        let mut manifest = Manifest::new();
        manifest.namespace = Some("target".to_string());
        manifest
            .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
            .expect("target manifest must bind one incarnation");
        manifest.artifact_origins = vec![origin("source", 2)];
        let mut segment = make_segment("segment-oob");
        segment.artifact_origin = Some(ArtifactOriginIndex::new(1));
        manifest.segments.push(segment);

        let error = Manifest::from_bytes_for_namespace(
            &manifest.to_bytes().expect("fixture must encode"),
            "target",
        )
        .expect_err("out-of-bounds origin index must fail decode");
        assert!(matches!(
            error,
            ZeppelinError::Branch(error)
                if matches!(error.as_ref(), BranchError::ArtifactOriginInvalid { .. })
        ));
    }

    #[test]
    fn decode_rejects_explicit_key_outside_artifact_origin_prefix() {
        let mut manifest = Manifest::new();
        manifest.namespace = Some("target".to_string());
        manifest
            .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
            .expect("target manifest must bind one incarnation");
        manifest.artifact_origins = vec![origin("source", 2)];
        let mut segment = make_segment("segment-prefix");
        segment.artifact_origin = Some(ArtifactOriginIndex::new(0));
        segment.sketch = Some(SketchRef {
            key: "wrong/segments/segment-prefix/coarse_sketch.bin".to_string(),
            version: 4,
            code_dims: 1,
            bytes_per_vector: 1,
            size_bytes: 1,
            rotation_seed: None,
        });
        manifest.segments.push(segment);

        let error = Manifest::from_bytes_for_namespace(
            &manifest.to_bytes().expect("fixture must encode"),
            "target",
        )
        .expect_err("origin/key prefix mismatch must fail decode");
        assert!(matches!(
            error,
            ZeppelinError::Branch(error)
                if matches!(error.as_ref(), BranchError::ArtifactOriginInvalid { .. })
        ));
    }

    #[test]
    fn absent_and_indexed_origins_resolve_exact_namespace_lifetimes() {
        let mut manifest = Manifest::new();
        manifest.namespace = Some("target".to_string());
        manifest
            .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
            .expect("target manifest must bind one incarnation");
        let local = origin("target", 1);
        let source = origin("source", 2);
        manifest.artifact_origins.push(source.clone());

        let local_fragment = FragmentRef {
            id: Ulid::from(20_u128),
            vector_count: 1,
            delete_count: 0,
            sequence_number: 0,
            size_bytes: 1,
            artifact_origin: None,
        };
        let foreign_fragment = FragmentRef {
            artifact_origin: Some(ArtifactOriginIndex::new(0)),
            ..local_fragment.clone()
        };

        assert_eq!(manifest.local_origin().unwrap(), local);
        assert_eq!(manifest.fragment_origin(&local_fragment).unwrap(), local);
        assert_eq!(manifest.fragment_origin(&foreign_fragment).unwrap(), source);
    }

    #[test]
    fn pre_incarnation_manifest_uses_authoritative_local_origin_without_mutation() {
        let mut manifest = Manifest::new();
        manifest.fragments.push(FragmentRef {
            id: Ulid::from(21_u128),
            vector_count: 1,
            delete_count: 0,
            sequence_number: 0,
            size_bytes: 1,
            artifact_origin: None,
        });
        let original = manifest.to_bytes().unwrap();
        let authoritative = origin("legacy-target", 9);

        let origins = manifest
            .artifact_origin_resolver(&authoritative)
            .expect("authoritative metadata must resolve an unbound legacy manifest");
        let located = origins
            .locate_fragment(&manifest.fragments[0])
            .expect("implicit legacy fragment must resolve through the context");

        assert_eq!(located.logical_namespace, "legacy-target");
        assert_eq!(located.physical_origin.as_origin(), &authoritative);
        assert_eq!(
            manifest.to_bytes().unwrap(),
            original,
            "read routing must not rewrite history"
        );
    }

    #[test]
    fn located_identity_rejects_exact_duplicates_but_keeps_cross_origin_ulids() {
        let repeated_id = Ulid::from(22_u128);
        let local = FragmentRef {
            id: repeated_id,
            vector_count: 1,
            delete_count: 0,
            sequence_number: 0,
            size_bytes: 1,
            artifact_origin: None,
        };
        let foreign = FragmentRef {
            sequence_number: 1,
            artifact_origin: Some(ArtifactOriginIndex::new(0)),
            ..local.clone()
        };
        let authoritative = origin("identity-target", 10);
        let source = origin("identity-source", 11);

        let mut distinct = Manifest::new();
        distinct.namespace = Some("identity-target".to_string());
        distinct
            .bind_namespace_incarnation(uuid::Uuid::from_u128(10))
            .unwrap();
        distinct.artifact_origins = vec![source];
        distinct.fragments = vec![local.clone(), foreign];
        let origins = distinct.artifact_origin_resolver(&authoritative).unwrap();
        assert_eq!(origins.located_fragments().unwrap().len(), 2);

        let mut duplicate = distinct;
        duplicate.fragments = vec![local.clone(), local];
        let error = duplicate
            .artifact_origin_resolver(&authoritative)
            .and_then(|origins| origins.located_fragments().map(|_| ()))
            .expect_err("one full located fragment identity cannot occur twice");
        assert!(matches!(
            error,
            ZeppelinError::Branch(error)
                if matches!(error.as_ref(), BranchError::ArtifactOriginInvalid { .. })
        ));
    }

    #[test]
    fn decode_rejects_duplicate_nil_and_invalid_origin_entries() {
        let mut duplicate = Manifest::new();
        duplicate.namespace = Some("target".to_string());
        duplicate
            .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
            .unwrap();
        duplicate.artifact_origins = vec![origin("source", 2), origin("source", 2)];

        let mut nil = duplicate.clone();
        nil.artifact_origins = vec![origin("source", 0)];

        let mut invalid = duplicate.clone();
        invalid.artifact_origins = vec![serde_json::from_value(serde_json::json!({
            "namespace": "../source",
            "incarnation": "00000000-0000-0000-0000-000000000002"
        }))
        .expect("wire fixture intentionally bypasses the NamespaceId constructor")];

        for (fixture, expected_reason) in [
            (duplicate, "duplicate artifact origin"),
            (nil, "incarnation is nil"),
            (invalid, "namespace violates"),
        ] {
            let error = Manifest::from_bytes_for_namespace(
                &fixture.to_bytes().expect("fixture must encode"),
                "target",
            )
            .expect_err("invalid origin table must fail decode");
            assert!(
                matches!(&error, ZeppelinError::Branch(branch_error)
                    if matches!(branch_error.as_ref(), BranchError::ArtifactOriginInvalid { reason, .. }
                        if reason.contains(expected_reason))),
                "expected {expected_reason:?}, got {error:?}"
            );
        }
    }

    #[test]
    fn persisted_origin_table_rejects_count_beyond_u32_address_space() {
        let mut manifest = Manifest::new();
        manifest.namespace = Some("origin-capacity".to_string());
        manifest
            .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
            .unwrap();

        manifest
            .validate_artifact_origin_table_len(u64::from(u32::MAX))
            .expect("the largest addressable table must remain valid");
        let error = manifest
            .validate_artifact_origin_table_len(u64::from(u32::MAX) + 1)
            .expect_err("persisted origin tables must not exceed u32 indices");
        assert!(
            matches!(&error, ZeppelinError::Branch(branch_error)
                if matches!(branch_error.as_ref(), BranchError::ArtifactOriginInvalid { reason, .. }
                    if reason.contains("exceeds u32 address space"))),
            "unexpected capacity error: {error:?}"
        );
    }

    #[test]
    fn origin_canonicalization_is_deterministic_and_remaps_all_refs() {
        let source_a = origin("source-a", 10);
        let source_b = origin("source-b", 11);
        let mut first = Manifest::new();
        first.namespace = Some("target".to_string());
        first
            .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
            .unwrap();
        first.artifact_origins = vec![source_b.clone(), source_a.clone()];
        first.fragments = vec![
            FragmentRef {
                id: Ulid::from(30_u128),
                vector_count: 1,
                delete_count: 0,
                sequence_number: 0,
                size_bytes: 1,
                artifact_origin: Some(ArtifactOriginIndex::new(0)),
            },
            FragmentRef {
                id: Ulid::from(31_u128),
                vector_count: 1,
                delete_count: 0,
                sequence_number: 1,
                size_bytes: 1,
                artifact_origin: Some(ArtifactOriginIndex::new(1)),
            },
        ];

        let mut second = first.clone();
        second.artifact_origins = vec![source_a.clone(), source_b.clone()];
        second.fragments[0].artifact_origin = Some(ArtifactOriginIndex::new(1));
        second.fragments[1].artifact_origin = Some(ArtifactOriginIndex::new(0));

        first.canonicalize_artifact_origins().unwrap();
        second.canonicalize_artifact_origins().unwrap();

        assert_eq!(first.artifact_origins, vec![source_a, source_b]);
        assert_eq!(first.artifact_origins, second.artifact_origins);
        assert_eq!(first.fragments, second.fragments);
        assert_eq!(
            first
                .fragments
                .iter()
                .map(|fragment| fragment.artifact_origin.unwrap().get())
                .collect::<Vec<_>>(),
            vec![1, 0]
        );
    }

    proptest! {
        #[test]
        fn arbitrary_valid_origin_tables_resolve_and_canonicalize_deterministically(
            raw_origins in proptest::collection::vec(
                ("[a-z][a-z0-9-]{0,12}", 1_u128..=u128::MAX),
                1..9,
            ),
            ref_choices in proptest::collection::vec(any::<usize>(), 1..24),
            order_keys in proptest::collection::vec(any::<u64>(), 1..9),
        ) {
            let canonical = raw_origins
                .into_iter()
                .map(|(namespace, incarnation)| ArtifactOrigin {
                    namespace: NamespaceId::parse(namespace).unwrap(),
                    incarnation: NamespaceIncarnationId::from_uuid(uuid::Uuid::from_u128(
                        incarnation,
                    )),
                })
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();

            let mut permuted = canonical
                .iter()
                .cloned()
                .enumerate()
                .map(|(index, origin)| (order_keys[index % order_keys.len()], index, origin))
                .collect::<Vec<_>>();
            permuted.sort_by_key(|(order, index, _)| (*order, *index));
            let permuted = permuted
                .into_iter()
                .map(|(_, _, origin)| origin)
                .collect::<Vec<_>>();

            let expected = permuted
                .iter()
                .cloned()
                .chain(
                    ref_choices
                        .iter()
                        .map(|choice| permuted[*choice % permuted.len()].clone()),
                )
                .collect::<Vec<_>>();
            let mut first = Manifest::new();
            first.namespace = Some("property-target".to_string());
            first
                .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
                .unwrap();
            first.artifact_origins = permuted.clone();
            first.fragments = expected
                .iter()
                .enumerate()
                .map(|(position, expected_origin)| FragmentRef {
                    id: Ulid::from((position + 1) as u128),
                    vector_count: 1,
                    delete_count: 0,
                    sequence_number: position as u64,
                    size_bytes: 1,
                    artifact_origin: Some(ArtifactOriginIndex::new(
                        u32::try_from(
                            permuted
                                .iter()
                                .position(|origin| origin == expected_origin)
                                .unwrap(),
                        )
                        .unwrap(),
                    )),
                })
                .collect();

            for (fragment, expected_origin) in first.fragments.iter().zip(&expected) {
                prop_assert_eq!(first.fragment_origin(fragment).unwrap(), expected_origin.clone());
            }

            let mut second = first.clone();
            second.artifact_origins = canonical.clone();
            for (fragment, expected_origin) in second.fragments.iter_mut().zip(&expected) {
                fragment.artifact_origin = Some(ArtifactOriginIndex::new(
                    u32::try_from(
                        canonical
                            .iter()
                            .position(|origin| origin == expected_origin)
                            .unwrap(),
                    )
                    .unwrap(),
                ));
            }

            first.canonicalize_artifact_origins().unwrap();
            second.canonicalize_artifact_origins().unwrap();

            prop_assert_eq!(&first.artifact_origins, &canonical);
            prop_assert_eq!(&second.artifact_origins, &canonical);
            prop_assert_eq!(first.fragments, second.fragments);
        }
    }

    #[test]
    fn receipt_publication_canonicalizes_only_explicit_origin_metadata() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let local = origin("canonical-publication", 1);
        let unused = origin("unused-origin", 2);
        let mut manifest = Manifest::new();
        manifest.namespace = Some("canonical-publication".to_string());
        manifest
            .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
            .unwrap();
        manifest.artifact_origins = vec![unused, local.clone(), local.clone()];
        manifest.fragments = vec![
            FragmentRef {
                id: Ulid::from(40_u128),
                vector_count: 1,
                delete_count: 0,
                sequence_number: 0,
                size_bytes: 1,
                artifact_origin: None,
            },
            FragmentRef {
                id: Ulid::from(41_u128),
                vector_count: 1,
                delete_count: 0,
                sequence_number: 1,
                size_bytes: 1,
                artifact_origin: Some(ArtifactOriginIndex::new(2)),
            },
        ];
        let mut explicit_segment = make_segment("segment-explicit-local");
        explicit_segment.artifact_origin = Some(ArtifactOriginIndex::new(1));
        manifest.segments = vec![explicit_segment, make_segment("segment-implicit-local")];

        manifest
            .finalize_receipt_root(&store, "canonical-publication")
            .expect("publication must canonicalize valid explicit origins");

        assert_eq!(manifest.artifact_origins, vec![local]);
        assert_eq!(manifest.fragments[0].artifact_origin, None);
        assert_eq!(
            manifest.fragments[1].artifact_origin,
            Some(ArtifactOriginIndex::new(0))
        );
        assert_eq!(
            manifest.segments[0].artifact_origin,
            Some(ArtifactOriginIndex::new(0))
        );
        assert_eq!(manifest.segments[1].artifact_origin, None);
        assert_eq!(
            manifest.receipt_binding_version(),
            Some(ReceiptBindingVersion::V2Origins)
        );
    }

    #[test]
    fn receipt_v2_digest_binds_origin_table_namespace_and_incarnation() {
        let mut manifest = Manifest::new();
        manifest.namespace = Some("target".to_string());
        manifest
            .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
            .unwrap();
        manifest.artifact_origins = vec![origin("source", 2)];
        let mut segment = make_segment("segment-origin-table");
        segment.artifact_origin = Some(ArtifactOriginIndex::new(0));
        manifest.segments.push(segment);
        manifest.receipt_binding_version = Some(ReceiptBindingVersion::V2Origins);
        let baseline = manifest.recompute_receipt_state_digest("target").unwrap();

        let mut namespace_tamper = manifest.clone();
        namespace_tamper.artifact_origins[0] = origin("another-source", 2);
        assert_ne!(
            namespace_tamper
                .recompute_receipt_state_digest("target")
                .unwrap(),
            baseline
        );

        let mut incarnation_tamper = manifest.clone();
        incarnation_tamper.artifact_origins[0] = origin("source", 3);
        assert_ne!(
            incarnation_tamper
                .recompute_receipt_state_digest("target")
                .unwrap(),
            baseline
        );

        let mut table_tamper = manifest;
        table_tamper
            .artifact_origins
            .push(origin("unused-source", 4));
        assert_ne!(
            table_tamper
                .recompute_receipt_state_digest("target")
                .unwrap(),
            baseline
        );
    }

    #[test]
    fn receipt_state_digest_binds_query_routing_topology() {
        let mut manifest = Manifest::new();
        manifest
            .bind_namespace_incarnation(uuid::Uuid::from_u128(7))
            .expect("receipt topology fixture must bind one incarnation");
        manifest.fragments = vec![
            FragmentRef {
                id: Ulid::from(1_u128),
                vector_count: 1,
                delete_count: 0,
                sequence_number: 0,
                size_bytes: 10,
                artifact_origin: None,
            },
            FragmentRef {
                id: Ulid::from(2_u128),
                vector_count: 1,
                delete_count: 0,
                sequence_number: 1,
                size_bytes: 11,
                artifact_origin: None,
            },
        ];
        manifest.segments = vec![make_segment("segment-a"), make_segment("segment-b")];
        manifest.active_segment = Some("segment-b".to_string());
        manifest
            .hierarchical_routing_nodes
            .insert("segment-b".to_string(), vec!["node-0".to_string()]);
        manifest.receipt_binding_version = Some(ReceiptBindingVersion::V1);
        let baseline = manifest
            .recompute_receipt_state_digest("topology")
            .expect("baseline receipt topology must encode");

        let mut reordered = manifest.clone();
        reordered.fragments.swap(0, 1);
        assert_ne!(
            reordered
                .recompute_receipt_state_digest("topology")
                .unwrap(),
            baseline
        );

        let mut rebound = manifest.clone();
        rebound.active_segment = Some("segment-a".to_string());
        assert_ne!(
            rebound.recompute_receipt_state_digest("topology").unwrap(),
            baseline
        );

        let mut descriptor_changed = manifest.clone();
        descriptor_changed.segments[0].cluster_count += 1;
        assert_ne!(
            descriptor_changed
                .recompute_receipt_state_digest("topology")
                .unwrap(),
            baseline
        );

        let mut routing_changed = manifest.clone();
        routing_changed
            .hierarchical_routing_nodes
            .get_mut("segment-b")
            .unwrap()
            .push("node-1".to_string());
        assert_ne!(
            routing_changed
                .recompute_receipt_state_digest("topology")
                .unwrap(),
            baseline
        );

        let mut separately_bound = manifest;
        separately_bound.updated_at += chrono::Duration::seconds(1);
        separately_bound.next_sequence += 1;
        separately_bound.fencing_token += 1;
        separately_bound.segments[0].membership = Some(MembershipRef {
            key: "topology/segments/segment-a/membership.bin".to_string(),
            size_bytes: 1,
            entry_count: 1,
        });
        assert_eq!(
            separately_bound
                .recompute_receipt_state_digest("topology")
                .unwrap(),
            baseline,
            "timestamps, replay allocator, fencing, and write-only membership use separate bindings"
        );
    }

    #[test]
    fn receipt_binding_v1_uses_an_explicit_stable_segment_projection() {
        let mut manifest = Manifest::new();
        manifest
            .bind_namespace_incarnation(uuid::Uuid::from_u128(9))
            .expect("stable projection fixture must bind one incarnation");
        manifest.fragments = vec![FragmentRef {
            id: Ulid::from(9_u128),
            vector_count: 1,
            delete_count: 0,
            sequence_number: 4,
            size_bytes: 5,
            artifact_origin: None,
        }];
        manifest.segments = vec![make_segment("segment-stable")];
        let binding = serde_json::to_value(manifest.execution_binding_v1("stable"))
            .expect("stable v1 execution projection must encode");
        let fragment = binding["fragments"][0]
            .as_object()
            .expect("v1 fragment projection must be an object");
        assert_eq!(
            fragment.keys().map(String::as_str).collect::<Vec<_>>(),
            [
                "delete_count",
                "id",
                "sequence_number",
                "size_bytes",
                "vector_count",
            ],
            "v1 must not inherit future serde-default FragmentRef fields"
        );
        let segment = binding["segments"][0]
            .as_object()
            .expect("v1 segment projection must be an object");
        let keys = segment.keys().map(String::as_str).collect::<Vec<_>>();
        assert_eq!(
            keys,
            [
                "bitmap_fields",
                "bootstrap",
                "cluster_count",
                "cluster_objects",
                "cluster_owners",
                "fts_fields",
                "has_global_fts",
                "hierarchical",
                "id",
                "quantization",
                "sketch",
                "vector_count",
            ],
            "v1 must not inherit future serde-default SegmentRef fields"
        );
        assert!(
            !segment.contains_key("membership"),
            "write-path-only membership is deliberately outside query execution v1"
        );
    }

    #[test]
    fn receipt_v1_root_signing_bytes_and_signature_are_frozen() {
        use ed25519_dalek::Signer as _;

        let bytes =
            manifest_root_signing_bytes([1; 32], 7, 9, ReceiptBindingVersion::V1, [2; 32], None)
                .expect("legacy V1 root binding must encode");
        let expected = concat!(
            r#"{"merkle_root":[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,"#,
            r#"1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],"#,
            r#""manifest_version":7,"fencing_token":9,"binding_version":"v1","#,
            r#""state_digest":[2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,"#,
            r#"2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2]}"#,
        );
        assert_eq!(bytes, expected.as_bytes());

        let signature = ed25519_dalek::SigningKey::from_bytes(&[7; 32])
            .sign(&bytes)
            .to_bytes();
        assert_eq!(
            signature,
            [
                137, 0, 201, 209, 102, 145, 229, 89, 174, 203, 186, 189, 183, 75, 39, 193, 8, 129,
                188, 226, 42, 176, 145, 80, 157, 45, 133, 85, 227, 81, 96, 14, 98, 177, 118, 112,
                155, 84, 173, 5, 19, 177, 14, 126, 67, 126, 97, 92, 119, 124, 126, 74, 44, 23, 98,
                137, 95, 72, 200, 54, 57, 72, 187, 8,
            ]
        );
    }

    #[test]
    fn receipt_v2_uses_fixed_envelope_and_rejects_control_digest() {
        let bytes = manifest_root_signing_bytes(
            [1; 32],
            7,
            9,
            ReceiptBindingVersion::V2Origins,
            [2; 32],
            None,
        )
        .expect("V2 origin-aware root envelope must encode");
        let expected = concat!(
            r#"{"domain":"zeppelin-manifest-root-envelope-v2","#,
            r#""merkle_root":[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,"#,
            r#"1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],"#,
            r#""manifest_generation":7,"fencing_token":9,"#,
            r#""binding_version":"v2_origins","#,
            r#""execution_digest":[2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,"#,
            r#"2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2],"control_digest":null}"#,
        );
        assert_eq!(bytes, expected.as_bytes());

        let error = manifest_root_signing_bytes(
            [1; 32],
            7,
            9,
            ReceiptBindingVersion::V2Origins,
            [2; 32],
            Some([3; 32]),
        )
        .expect_err("V2 origins must not reinterpret a future control digest");
        assert!(matches!(error, ZeppelinError::Serialization(_)));
    }

    #[test]
    fn branch_root_candidates_are_exact_idempotent_bounded_and_removable() {
        let mut manifest = Manifest::new();
        manifest.namespace = Some("root-source".to_string());
        manifest
            .bind_namespace_incarnation(uuid::Uuid::from_u128(10))
            .unwrap();
        manifest.version = 7;
        let predecessor_bytes = manifest.to_bytes().unwrap();
        let digest = ManifestDigest::new(Sha256::digest(&predecessor_bytes).into());
        let branch_id = BranchId::from_ulid(Ulid::from(11_u128));
        let root = branch_root(branch_id, 7, digest, "child-a", 12);

        assert!(manifest
            .insert_branch_root_candidate(root.clone(), 1)
            .unwrap());
        assert!(!manifest
            .insert_branch_root_candidate(root.clone(), 1)
            .unwrap());
        assert_eq!(manifest.branch_roots().get(&branch_id), Some(&root));
        assert_eq!(
            manifest
                .rooted_generations()
                .unwrap()
                .get(&root.source_generation),
            Some(&digest)
        );
        manifest
            .validate_rooted_history_bytes(root.source_generation, &predecessor_bytes)
            .unwrap();

        let conflicting = BranchRoot {
            source_config_sha256: crate::namespace::SourceDataPlaneConfigDigest::new([9; 32]),
            ..root.clone()
        };
        assert!(matches!(
            manifest.insert_branch_root_candidate(conflicting, 1),
            Err(ZeppelinError::Branch(error))
                if matches!(error.as_ref(), BranchError::BranchRootConflict { branch_id: id } if *id == branch_id)
        ));

        let second = branch_root(
            BranchId::from_ulid(Ulid::from(13_u128)),
            7,
            digest,
            "child-b",
            14,
        );
        assert!(matches!(
            manifest.insert_branch_root_candidate(second, 1),
            Err(ZeppelinError::Branch(error))
                if matches!(error.as_ref(), BranchError::BranchRootLimitExceeded { limit: 1 })
        ));

        manifest.remove_branch_root_candidate(&root).unwrap();
        assert!(manifest.branch_roots().is_empty());
        assert!(matches!(
            manifest.remove_branch_root_candidate(&root),
            Err(ZeppelinError::Branch(error))
                if matches!(error.as_ref(), BranchError::BranchRootMissing { branch_id: id } if *id == branch_id)
        ));
    }

    #[test]
    fn branch_root_validation_rejects_malformed_identity_and_fence_overlap() {
        let mut manifest = Manifest::new();
        manifest.namespace = Some("root-validation".to_string());
        manifest
            .bind_namespace_incarnation(uuid::Uuid::from_u128(20))
            .unwrap();
        manifest.version = 3;
        let bytes = manifest.to_bytes().unwrap();
        let digest = ManifestDigest::new(Sha256::digest(bytes).into());

        let branch_id = BranchId::from_ulid(Ulid::from(21_u128));
        let nil_target = branch_root(branch_id, 3, digest, "child", 0);
        assert!(matches!(
            manifest.insert_branch_root_candidate(nil_target, 4),
            Err(ZeppelinError::Branch(error))
                if matches!(error.as_ref(), BranchError::BranchRootInvalid { .. })
        ));

        let root = branch_root(branch_id, 3, digest, "child", 22);
        manifest
            .insert_branch_root_candidate(root.clone(), 4)
            .unwrap();
        manifest.deletion_fence = Some(ManifestDeletionFence {
            destruction_record_key: "_audit/destruction/root-validation.json".to_string(),
        });
        assert!(matches!(
            manifest.validate_branch_root_state("root-validation"),
            Err(ZeppelinError::Branch(error))
                if matches!(error.as_ref(), BranchError::NamespaceHasLiveBranches { .. })
        ));

        manifest.deletion_fence = None;
        manifest.branch_roots.clear();
        let mut mismatched = root;
        mismatched.branch_id = BranchId::from_ulid(Ulid::from(23_u128));
        manifest.branch_roots.insert(branch_id, mismatched);
        assert!(matches!(
            manifest.validate_branch_root_state("root-validation"),
            Err(ZeppelinError::Branch(error))
                if matches!(error.as_ref(), BranchError::BranchRootInvalid { .. })
        ));
    }

    #[test]
    fn v3_roots_reuses_v2_execution_and_never_downgrades() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let mut manifest = Manifest::new();
        manifest.namespace = Some("v3-roots".to_string());
        manifest
            .bind_namespace_incarnation(uuid::Uuid::from_u128(30))
            .unwrap();
        manifest.version = 5;
        let predecessor_bytes = manifest.to_bytes().unwrap();
        let digest = ManifestDigest::new(Sha256::digest(&predecessor_bytes).into());
        let v2_execution = manifest
            .compute_receipt_state_digest("v3-roots", ReceiptBindingVersion::V2Origins)
            .unwrap();
        let root = branch_root(
            BranchId::from_ulid(Ulid::from(31_u128)),
            5,
            digest,
            "v3-child",
            32,
        );
        manifest
            .insert_branch_root_candidate(root.clone(), 4)
            .unwrap();
        manifest.finalize_receipt_root(&store, "v3-roots").unwrap();

        assert_eq!(
            manifest.receipt_binding_version(),
            Some(ReceiptBindingVersion::V3Roots)
        );
        assert_eq!(manifest.receipt_state_digest(), Some(v2_execution));
        let control = manifest.control_state_digest().unwrap();
        assert_eq!(
            manifest.recompute_control_state_digest("v3-roots").unwrap(),
            control
        );
        let mut changed_view = manifest.clone();
        let changed_root = BranchRoot {
            fork_view_sha256: crate::namespace::ForkViewDigest::new([0xa5; 32]),
            ..root.clone()
        };
        changed_view
            .branch_roots
            .insert(root.branch_id, changed_root);
        changed_view
            .finalize_receipt_root(&store, "v3-roots")
            .unwrap();
        let changed_control = changed_view.control_state_digest().unwrap();
        assert_eq!(changed_view.receipt_state_digest(), Some(v2_execution));
        assert_ne!(changed_control, control);
        assert_ne!(
            manifest_root_signing_bytes(
                [1; 32],
                6,
                7,
                ReceiptBindingVersion::V3Roots,
                [2; 32],
                Some(control),
            )
            .unwrap(),
            manifest_root_signing_bytes(
                [1; 32],
                6,
                7,
                ReceiptBindingVersion::V3Roots,
                [2; 32],
                Some(changed_control),
            )
            .unwrap(),
            "fork-view identity must change V3 signing bytes without changing V2 execution"
        );
        let signing = manifest_root_signing_bytes(
            [1; 32],
            6,
            7,
            ReceiptBindingVersion::V3Roots,
            [2; 32],
            Some([3; 32]),
        )
        .unwrap();
        let signing = String::from_utf8(signing).unwrap();
        assert!(signing
            .starts_with(r#"{"domain":"zeppelin-manifest-root-envelope-v2","merkle_root":[1,1"#));
        assert!(signing.contains(r#""binding_version":"v3_roots""#));
        assert!(signing.ends_with(
            r#""control_digest":[3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3]}"#
        ));

        manifest.remove_branch_root_candidate(&root).unwrap();
        manifest.finalize_receipt_root(&store, "v3-roots").unwrap();
        assert_eq!(
            manifest.receipt_binding_version(),
            Some(ReceiptBindingVersion::V3Roots),
            "removing the final root must not downgrade this namespace lifetime"
        );
        assert!(manifest.control_state_digest().is_some());
    }

    #[tokio::test]
    async fn manifest_write_cannot_erase_resurrect_or_downgrade_branch_control() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let namespace = "branch-control-write-rebase";
        let incarnation = uuid::Uuid::from_u128(0x41);
        let mut initial = Manifest::new();
        initial.bind_namespace_incarnation(incarnation).unwrap();
        initial.write(&store, namespace).await.unwrap();
        let mut stale_pre_v3 = Manifest::read(&store, namespace).await.unwrap().unwrap();

        let (mut rooted, root_base) = Manifest::read_versioned(&store, namespace)
            .await
            .unwrap()
            .unwrap();
        let root = branch_root(
            BranchId::from_ulid(Ulid::from(0x42_u128)),
            rooted.version(),
            root_base.exact_manifest_digest().unwrap(),
            "branch-control-write-child",
            0x43,
        );
        rooted
            .insert_branch_root_candidate(root.clone(), 4)
            .unwrap();
        rooted
            .write_conditional(&store, namespace, &root_base)
            .await
            .unwrap();
        let mut stale_rooted = rooted.clone();

        assert!(matches!(
            stale_pre_v3.write(&store, namespace).await,
            Err(ZeppelinError::ManifestConflict { .. })
        ));
        let after_erase_attempt = Manifest::read(&store, namespace).await.unwrap().unwrap();
        assert_eq!(
            after_erase_attempt.branch_roots().get(&root.branch_id),
            Some(&root)
        );
        assert_eq!(
            after_erase_attempt.receipt_binding_version(),
            Some(ReceiptBindingVersion::V3Roots)
        );

        let (mut rootless_v3, rooted_version) = Manifest::read_versioned(&store, namespace)
            .await
            .unwrap()
            .unwrap();
        rootless_v3.remove_branch_root_candidate(&root).unwrap();
        rootless_v3
            .write_conditional(&store, namespace, &rooted_version)
            .await
            .unwrap();

        assert!(matches!(
            stale_rooted.write(&store, namespace).await,
            Err(ZeppelinError::ManifestConflict { .. })
        ));
        assert!(matches!(
            stale_pre_v3.write(&store, namespace).await,
            Err(ZeppelinError::ManifestConflict { .. })
        ));
        let after_resurrection_attempts = Manifest::read(&store, namespace).await.unwrap().unwrap();
        assert!(after_resurrection_attempts.branch_roots().is_empty());
        assert_eq!(
            after_resurrection_attempts.receipt_binding_version(),
            Some(ReceiptBindingVersion::V3Roots)
        );
    }

    #[test]
    fn v3_root_tampering_fails_decode_and_clone_reset_clears_control() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let mut manifest = Manifest::new();
        manifest.namespace = Some("v3-tamper".to_string());
        manifest
            .bind_namespace_incarnation(uuid::Uuid::from_u128(40))
            .unwrap();
        manifest.version = 2;
        let digest = ManifestDigest::new(Sha256::digest(manifest.to_bytes().unwrap()).into());
        let root = branch_root(
            BranchId::from_ulid(Ulid::from(41_u128)),
            2,
            digest,
            "tamper-child",
            42,
        );
        manifest
            .insert_branch_root_candidate(root.clone(), 4)
            .unwrap();
        manifest.finalize_receipt_root(&store, "v3-tamper").unwrap();

        let mut tampered = manifest.clone();
        tampered
            .branch_roots
            .get_mut(&root.branch_id)
            .unwrap()
            .fork_view_sha256 = crate::namespace::ForkViewDigest::new([99; 32]);
        assert!(matches!(
            Manifest::from_bytes_for_namespace(&tampered.to_bytes().unwrap(), "v3-tamper"),
            Err(ZeppelinError::Serialization(_))
        ));

        let mut missing_control = manifest.clone();
        missing_control.control_state_digest = None;
        assert!(matches!(
            Manifest::from_bytes_for_namespace(&missing_control.to_bytes().unwrap(), "v3-tamper"),
            Err(ZeppelinError::Serialization(_))
        ));

        let mut fenced = manifest.clone();
        fenced.branch_roots.clear();
        fenced.deletion_fence = Some(ManifestDeletionFence {
            destruction_record_key: "_audit/destruction/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json"
                .to_string(),
        });
        fenced.finalize_receipt_root(&store, "v3-tamper").unwrap();
        Manifest::from_bytes_for_namespace(&fenced.to_bytes().unwrap(), "v3-tamper").unwrap();

        let mut tampered_fence = fenced;
        tampered_fence
            .deletion_fence
            .as_mut()
            .unwrap()
            .destruction_record_key =
            "_audit/destruction/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json".to_string();
        assert!(matches!(
            Manifest::from_bytes_for_namespace(&tampered_fence.to_bytes().unwrap(), "v3-tamper"),
            Err(ZeppelinError::Serialization(_))
        ));

        manifest.reset_version_for_clone();
        assert!(manifest.branch_roots().is_empty());
        assert_eq!(manifest.control_state_digest(), None);
        assert_eq!(manifest.receipt_binding_version(), None);
    }

    #[test]
    fn receipt_binding_version_never_downgrades_within_one_namespace_lifetime() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let mut manifest = Manifest::new();
        manifest.namespace = Some("binding-monotonic".to_string());
        manifest
            .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
            .unwrap();
        manifest.artifact_origins = vec![origin("binding-monotonic", 1)];
        let mut segment = make_segment("segment-binding-monotonic");
        segment.artifact_origin = Some(ArtifactOriginIndex::new(0));
        manifest.segments.push(segment);

        manifest
            .finalize_receipt_root(&store, "binding-monotonic")
            .unwrap();
        assert_eq!(
            manifest.receipt_binding_version(),
            Some(ReceiptBindingVersion::V2Origins)
        );

        manifest.segments.clear();
        manifest.artifact_origins.clear();
        manifest
            .finalize_receipt_root(&store, "binding-monotonic")
            .unwrap();
        assert_eq!(
            manifest.receipt_binding_version(),
            Some(ReceiptBindingVersion::V2Origins),
            "removing the last explicit origin must not downgrade this namespace lifetime"
        );
    }

    #[test]
    fn receipt_binding_combinations_fail_closed() {
        fn decode_error(mut manifest: Manifest) -> ZeppelinError {
            manifest.namespace = Some("binding-combinations".to_string());
            manifest
                .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
                .unwrap();
            Manifest::from_bytes_for_namespace(
                &manifest.to_bytes().expect("fixture must encode"),
                "binding-combinations",
            )
            .expect_err("invalid receipt binding combination must fail")
        }

        let mut digest_without_version = Manifest::new();
        digest_without_version.receipt_state_digest = Some([1; 32]);
        assert!(matches!(
            decode_error(digest_without_version),
            ZeppelinError::Serialization(_)
        ));

        let mut v1_without_digest = Manifest::new();
        v1_without_digest.receipt_binding_version = Some(ReceiptBindingVersion::V1);
        assert!(matches!(
            decode_error(v1_without_digest),
            ZeppelinError::Serialization(_)
        ));

        let mut v1_with_control = Manifest::new();
        v1_with_control.receipt_binding_version = Some(ReceiptBindingVersion::V1);
        v1_with_control.receipt_state_digest = Some([1; 32]);
        v1_with_control.control_state_digest = Some([2; 32]);
        assert!(matches!(
            decode_error(v1_with_control),
            ZeppelinError::Serialization(_)
        ));

        let mut v2_without_digest = Manifest::new();
        v2_without_digest.receipt_binding_version = Some(ReceiptBindingVersion::V2Origins);
        assert!(matches!(
            decode_error(v2_without_digest),
            ZeppelinError::Serialization(_)
        ));

        let mut v2_with_control = Manifest::new();
        v2_with_control.receipt_binding_version = Some(ReceiptBindingVersion::V2Origins);
        v2_with_control.receipt_state_digest = Some([1; 32]);
        v2_with_control.control_state_digest = Some([2; 32]);
        assert!(matches!(
            decode_error(v2_with_control),
            ZeppelinError::Serialization(_)
        ));

        let mut v1_with_origin = Manifest::new();
        v1_with_origin.receipt_binding_version = Some(ReceiptBindingVersion::V1);
        v1_with_origin.receipt_state_digest = Some([1; 32]);
        v1_with_origin.artifact_origins = vec![origin("binding-combinations", 1)];
        let mut segment = make_segment("segment-v1-origin");
        segment.artifact_origin = Some(ArtifactOriginIndex::new(0));
        v1_with_origin.segments.push(segment);
        assert!(matches!(
            decode_error(v1_with_origin),
            ZeppelinError::Serialization(_)
        ));
    }

    #[test]
    fn legacy_receipt_bindings_reject_all_origin_metadata() {
        fn decode_error(mut manifest: Manifest) -> ZeppelinError {
            manifest.namespace = Some("legacy-origin-binding".to_string());
            manifest
                .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
                .unwrap();
            Manifest::from_bytes_for_namespace(
                &manifest.to_bytes().expect("fixture must encode"),
                "legacy-origin-binding",
            )
            .expect_err("legacy binding must reject origin metadata")
        }

        let local_origin = origin("legacy-origin-binding", 1);

        let mut unbound_table_only = Manifest::new();
        unbound_table_only.artifact_origins = vec![local_origin.clone()];
        assert!(matches!(
            decode_error(unbound_table_only),
            ZeppelinError::Serialization(_)
        ));

        let mut unbound_indexed = Manifest::new();
        unbound_indexed.artifact_origins = vec![local_origin.clone()];
        unbound_indexed.fragments.push(FragmentRef {
            id: Ulid::from(77_u128),
            vector_count: 1,
            delete_count: 0,
            sequence_number: 0,
            size_bytes: 1,
            artifact_origin: Some(ArtifactOriginIndex::new(0)),
        });
        assert!(matches!(
            decode_error(unbound_indexed),
            ZeppelinError::Serialization(_)
        ));

        let mut v1_table_only = Manifest::new();
        v1_table_only.receipt_binding_version = Some(ReceiptBindingVersion::V1);
        v1_table_only.receipt_state_digest = Some([1; 32]);
        v1_table_only.artifact_origins = vec![local_origin];
        assert!(matches!(
            decode_error(v1_table_only),
            ZeppelinError::Serialization(_)
        ));
    }

    #[test]
    fn v4_binding_without_lineage_fails_closed() {
        let mut manifest = Manifest::new();
        manifest.namespace = Some("reserved-binding".to_string());
        manifest
            .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
            .unwrap();
        manifest.receipt_binding_version = Some(ReceiptBindingVersion::V4Lineage);
        manifest.receipt_state_digest = Some([1; 32]);

        let error = Manifest::from_bytes_for_namespace(
            &manifest
                .to_bytes()
                .expect("reserved version must serialize"),
            "reserved-binding",
        )
        .expect_err("reserved version must not become authority");
        assert!(matches!(error, ZeppelinError::Serialization(_)));

        let error = manifest_root_signing_bytes(
            [1; 32],
            1,
            1,
            ReceiptBindingVersion::V4Lineage,
            [2; 32],
            None,
        )
        .expect_err("V4 root projection requires its control digest");
        assert!(matches!(error, ZeppelinError::Serialization(_)));

        let bytes = manifest_root_signing_bytes(
            [1; 32],
            1,
            1,
            ReceiptBindingVersion::V4Lineage,
            [2; 32],
            Some([3; 32]),
        )
        .expect("V4 must use the frozen V2 root envelope");
        let bytes = String::from_utf8(bytes).unwrap();
        assert!(bytes.contains(r#""binding_version":"v4_lineage""#));
        assert!(bytes.starts_with(r#"{"domain":"zeppelin-manifest-root-envelope-v2""#));
    }

    #[test]
    fn zero_copy_fork_keeps_only_visible_state_and_canonical_origins() {
        let source_identity = origin("fork-source-z", 0x101);
        let target_identity = origin("fork-target", 0x102);
        let mut source = bound_manifest(&source_identity, 7);
        source.next_sequence = 9;
        source.compaction_watermark = Some(Ulid::from(0x33_u128));
        source.pending_deletes = vec!["fork-source-z/obsolete".to_string()];
        source.fencing_token = 99;

        let fragment_id = Ulid::from(0x44_u128);
        source.fragments.push(FragmentRef {
            id: fragment_id,
            vector_count: 2,
            delete_count: 0,
            sequence_number: 8,
            size_bytes: 12,
            artifact_origin: None,
        });
        let mut active = make_segment("active");
        active.cluster_count = 0;
        source.active_segment = Some(active.id.clone());
        source.segments.push(make_segment("inactive"));
        source.segments.push(active);
        let fragment_key = crate::wal::WalFragment::s3_key("fork-source-z", &fragment_id);
        let active_key = crate::index::ivf_flat::build::centroids_key("fork-source-z", "active");
        let inactive_key =
            crate::index::ivf_flat::build::centroids_key("fork-source-z", "inactive");
        source.artifact_hashes.insert(fragment_key.clone(), [1; 32]);
        source.artifact_hashes.insert(active_key.clone(), [2; 32]);
        source.artifact_hashes.insert(inactive_key, [3; 32]);

        let prepared = Manifest::prepare_zero_copy_fork(
            &source,
            &source_identity,
            &target_identity,
            lineage_seed(&source_identity, 7, 0x103, 1),
            DateTime::from_timestamp(1_700_000_001, 0).unwrap(),
        )
        .unwrap();
        let target = prepared.manifest;

        assert_eq!(target.version(), 0);
        assert_eq!(
            target.namespace_incarnation(),
            Some(uuid::Uuid::from_u128(0x102))
        );
        assert_eq!(target.next_sequence, 9);
        assert_eq!(target.fragments.len(), 1);
        assert_eq!(target.segments.len(), 1);
        assert_eq!(target.segments[0].id, "active");
        assert_eq!(target.compaction_watermark, None);
        assert!(target.pending_deletes.is_empty());
        assert_eq!(target.fencing_token, 0);
        assert!(target.branch_roots.is_empty());
        assert_eq!(target.artifact_origins, vec![source_identity.clone()]);
        assert_eq!(
            target.fragment_origin(&target.fragments[0]).unwrap(),
            source_identity
        );
        assert_eq!(
            target.segment_origin(&target.segments[0]).unwrap(),
            origin("fork-source-z", 0x101)
        );
        assert_eq!(
            target
                .artifact_hashes
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([fragment_key, active_key])
        );
        assert_eq!(target.branch_lineage(), Some(&prepared.lineage));
        target.validate_initial_fork_view().unwrap();
    }

    #[test]
    fn nested_inherited_only_fork_does_not_add_its_direct_parent_origin() {
        let root_identity = origin("nested-root", 0x201);
        let parent_identity = origin("nested-parent", 0x202);
        let target_identity = origin("nested-target", 0x203);
        let mut root = bound_manifest(&root_identity, 3);
        let fragment_id = Ulid::from(0x204_u128);
        root.fragments.push(FragmentRef {
            id: fragment_id,
            vector_count: 1,
            delete_count: 0,
            sequence_number: 0,
            size_bytes: 4,
            artifact_origin: None,
        });
        root.next_sequence = 1;
        root.artifact_hashes.insert(
            crate::wal::WalFragment::s3_key(root_identity.namespace.as_str(), &fragment_id),
            [4; 32],
        );
        let parent = Manifest::prepare_zero_copy_fork(
            &root,
            &root_identity,
            &parent_identity,
            lineage_seed(&root_identity, 3, 0x205, 1),
            Utc::now(),
        )
        .unwrap();
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let parent = parent
            .manifest
            .preseal_generation_one(&store, &parent_identity)
            .unwrap();
        let nested = Manifest::prepare_zero_copy_fork(
            parent.manifest(),
            &parent_identity,
            &target_identity,
            lineage_seed(&parent_identity, 1, 0x206, 2),
            Utc::now(),
        )
        .unwrap();

        assert_eq!(nested.manifest.artifact_origins, vec![root_identity]);
        assert!(!nested.manifest.artifact_origins.contains(&parent_identity));
        nested.manifest.validate_initial_fork_view().unwrap();
    }

    #[test]
    fn nested_fork_remaps_direct_parent_before_and_after_inherited_origins() {
        for (root_name, parent_name, ordinal) in [
            ("zzz-inherited", "aaa-direct-parent", 0x211_u128),
            ("aaa-inherited", "zzz-direct-parent", 0x221_u128),
        ] {
            let root_identity = origin(root_name, ordinal);
            let parent_identity = origin(parent_name, ordinal + 1);
            let target_identity = origin(&format!("nested-target-{ordinal:x}"), ordinal + 2);
            let mut root = bound_manifest(&root_identity, 3);
            let inherited_id = Ulid::from(ordinal + 3);
            root.fragments.push(FragmentRef {
                id: inherited_id,
                vector_count: 1,
                delete_count: 0,
                sequence_number: 0,
                size_bytes: 4,
                artifact_origin: None,
            });
            root.next_sequence = 1;
            root.artifact_hashes.insert(
                crate::wal::WalFragment::s3_key(root_name, &inherited_id),
                [5; 32],
            );
            let parent = Manifest::prepare_zero_copy_fork(
                &root,
                &root_identity,
                &parent_identity,
                lineage_seed(&root_identity, 3, ordinal + 4, 1),
                Utc::now(),
            )
            .unwrap();
            let store = ZeppelinStore::new(Arc::new(InMemory::new()));
            let mut parent = parent
                .manifest
                .preseal_generation_one(&store, &parent_identity)
                .unwrap()
                .manifest()
                .clone();
            let local_id = Ulid::from(ordinal + 5);
            parent.add_fragment_at(
                FragmentRef {
                    id: local_id,
                    vector_count: 1,
                    delete_count: 0,
                    sequence_number: 0,
                    size_bytes: 4,
                    artifact_origin: None,
                },
                Utc::now(),
            );
            parent.artifact_hashes.insert(
                crate::wal::WalFragment::s3_key(parent_name, &local_id),
                [6; 32],
            );
            parent.version = 2;
            parent
                .finalize_receipt_root(&store, parent_identity.namespace.as_str())
                .unwrap();

            let nested = Manifest::prepare_zero_copy_fork(
                &parent,
                &parent_identity,
                &target_identity,
                lineage_seed(&parent_identity, 2, ordinal + 6, 2),
                Utc::now(),
            )
            .unwrap();
            let expected = BTreeSet::from([root_identity.clone(), parent_identity.clone()]);
            assert_eq!(
                nested
                    .manifest
                    .artifact_origins
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>(),
                expected
            );
            let inherited = nested
                .manifest
                .fragments
                .iter()
                .find(|fragment| fragment.id == inherited_id)
                .unwrap();
            let local = nested
                .manifest
                .fragments
                .iter()
                .find(|fragment| fragment.id == local_id)
                .unwrap();
            assert_eq!(
                nested.manifest.fragment_origin(inherited).unwrap(),
                root_identity
            );
            assert_eq!(
                nested.manifest.fragment_origin(local).unwrap(),
                parent_identity
            );
        }
    }

    #[test]
    fn v4_control_and_execution_tampering_fail_decode() {
        let source_identity = origin("v4-source", 0x301);
        let target_identity = origin("v4-target", 0x302);
        let source = bound_manifest(&source_identity, 4);
        let prepared = Manifest::prepare_zero_copy_fork(
            &source,
            &source_identity,
            &target_identity,
            lineage_seed(&source_identity, 4, 0x303, 1),
            Utc::now(),
        )
        .unwrap();
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let sealed = prepared
            .manifest
            .preseal_generation_one(&store, &target_identity)
            .unwrap();
        assert_eq!(
            sealed.manifest().receipt_binding_version(),
            Some(ReceiptBindingVersion::V4Lineage)
        );
        assert_eq!(
            sealed
                .manifest()
                .recompute_receipt_state_digest("v4-target")
                .unwrap(),
            sealed.manifest().receipt_state_digest().unwrap()
        );
        assert_eq!(
            sealed
                .manifest()
                .recompute_control_state_digest("v4-target")
                .unwrap(),
            sealed.manifest().control_state_digest().unwrap()
        );

        let mut lineage_tamper = sealed.manifest().clone();
        lineage_tamper.branch_lineage.as_mut().unwrap().depth += 1;
        assert!(Manifest::from_bytes_for_namespace(
            &lineage_tamper.to_bytes().unwrap(),
            "v4-target"
        )
        .is_err());

        let mut extra_origin = sealed.manifest().clone();
        extra_origin
            .artifact_origins
            .push(origin("unused-origin", 0x304));
        assert!(
            Manifest::from_bytes_for_namespace(&extra_origin.to_bytes().unwrap(), "v4-target")
                .is_err()
        );
    }

    #[test]
    fn branch_successor_cannot_mutate_lineage_or_expand_foreign_closure() {
        let source_identity = origin("closure-source", 0x401);
        let target_identity = origin("closure-target", 0x402);
        let source = bound_manifest(&source_identity, 2);
        let prepared = Manifest::prepare_zero_copy_fork(
            &source,
            &source_identity,
            &target_identity,
            lineage_seed(&source_identity, 2, 0x403, 1),
            Utc::now(),
        )
        .unwrap();
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let predecessor = prepared
            .manifest
            .preseal_generation_one(&store, &target_identity)
            .unwrap()
            .manifest()
            .clone();

        let mut lineage_tamper = predecessor.clone();
        lineage_tamper.branch_lineage.as_mut().unwrap().depth += 1;
        assert!(matches!(
            lineage_tamper.require_valid_branch_successor(&predecessor, "closure-target"),
            Err(ZeppelinError::ManifestConflict { .. })
        ));

        let mut widened = predecessor.clone();
        let new_origin = origin("closure-foreign", 0x404);
        widened.artifact_origins.push(new_origin);
        widened.fragments.push(FragmentRef {
            id: Ulid::from(0x405_u128),
            vector_count: 1,
            delete_count: 0,
            sequence_number: 0,
            size_bytes: 1,
            artifact_origin: Some(ArtifactOriginIndex::new(0)),
        });
        assert!(matches!(
            widened.require_valid_branch_successor(&predecessor, "closure-target"),
            Err(ZeppelinError::ManifestConflict { .. })
        ));

        let materialized = predecessor.clone();
        materialized
            .require_valid_branch_successor(&predecessor, "closure-target")
            .unwrap();
    }

    #[tokio::test]
    async fn generation_one_publication_is_exact_and_idempotent() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let source_identity = origin("publish-source", 0x501);
        let target_identity = origin("publish-target", 0x502);
        let source = bound_manifest(&source_identity, 11);
        let prepared = Manifest::prepare_zero_copy_fork(
            &source,
            &source_identity,
            &target_identity,
            lineage_seed(&source_identity, 11, 0x503, 1),
            Utc::now(),
        )
        .unwrap();
        let sealed = prepared
            .manifest
            .preseal_generation_one(&store, &target_identity)
            .unwrap();

        let first = Manifest::create_or_verify_generation_one(&store, &target_identity, &sealed)
            .await
            .unwrap();
        let retry = Manifest::create_or_verify_generation_one(&store, &target_identity, &sealed)
            .await
            .unwrap();
        assert_eq!(first.to_bytes().unwrap(), *sealed.exact_bytes());
        assert_eq!(retry.to_bytes().unwrap(), *sealed.exact_bytes());
        assert_eq!(
            store
                .get(&Manifest::s3_key("publish-target"))
                .await
                .unwrap(),
            *sealed.exact_bytes()
        );
        assert_eq!(
            store
                .get(&Manifest::history_key("publish-target", 1))
                .await
                .unwrap(),
            *sealed.exact_bytes()
        );
        assert_eq!(
            sealed.digest(),
            ManifestDigest::new(Sha256::digest(sealed.exact_bytes()).into())
        );
    }

    /// Sketch refs written before v4 decode with no invented rotation seed.
    #[test]
    fn legacy_sketch_ref_decodes_without_rotation_seed() {
        #[derive(serde::Serialize)]
        struct LegacySketchRef {
            key: String,
            version: u32,
            code_dims: usize,
            bytes_per_vector: usize,
            size_bytes: u64,
        }

        let bytes = rmp_serde::to_vec(&LegacySketchRef {
            key: "ns/segments/old/coarse_sketch.bin".into(),
            version: 3,
            code_dims: 64,
            bytes_per_vector: 64,
            size_bytes: 4096,
        })
        .unwrap();
        let decoded: SketchRef = rmp_serde::from_slice(&bytes).unwrap();

        assert_eq!(decoded.version, 3);
        assert_eq!(decoded.rotation_seed, None);
    }

    /// V4 sketch refs preserve their rotation seed through MessagePack.
    #[test]
    fn v4_sketch_ref_roundtrip_preserves_rotation_seed() {
        let sketch_ref = SketchRef {
            key: "ns/segments/new/coarse_sketch.bin".into(),
            version: 4,
            code_dims: 768,
            bytes_per_vector: 200,
            size_bytes: 4_200_000_000,
            rotation_seed: Some(0x5a45_5050_454c_494e),
        };
        let bytes = rmp_serde::to_vec(&sketch_ref).unwrap();
        let decoded: SketchRef = rmp_serde::from_slice(&bytes).unwrap();

        assert_eq!(decoded, sketch_ref);
    }

    /// Verifies that each successful conditional publication advances exactly
    /// one persisted generation and exposes that value to the next reader.
    #[tokio::test]
    async fn manifest_version_increments_across_conditional_writes() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let ns = "manifest_version_epoch";
        Manifest::new().write(&store, ns).await.unwrap();

        let (mut first, first_etag) = Manifest::read_versioned(&store, ns).await.unwrap().unwrap();
        assert_eq!(first.version(), 1);

        first.add_fragment(FragmentRef {
            id: Ulid::from_parts(10_000, 1),
            vector_count: 1,
            delete_count: 0,
            sequence_number: 0,
            size_bytes: 8,
            artifact_origin: None,
        });
        first
            .write_conditional(&store, ns, &first_etag)
            .await
            .unwrap();
        assert_eq!(first.version(), 2);

        let (mut second, second_etag) =
            Manifest::read_versioned(&store, ns).await.unwrap().unwrap();
        assert_eq!(second.version(), 2);

        second.add_fragment(FragmentRef {
            id: Ulid::from_parts(10_001, 2),
            vector_count: 1,
            delete_count: 0,
            sequence_number: 0,
            size_bytes: 8,
            artifact_origin: None,
        });
        second
            .write_conditional(&store, ns, &second_etag)
            .await
            .unwrap();

        let (third, _) = Manifest::read_versioned(&store, ns).await.unwrap().unwrap();
        assert_eq!(third.version(), 3);
    }

    /// Verifies that every successful live commit has an addressable immutable
    /// history object containing the state of that exact generation.
    #[tokio::test]
    async fn manifest_history_is_written_and_addressable_by_committed_version() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let ns = "manifest_history_addressable";

        Manifest::new().write(&store, ns).await.unwrap();
        let (mut first, first_etag) = Manifest::read_versioned(&store, ns).await.unwrap().unwrap();
        first.add_fragment(FragmentRef {
            id: Ulid::from_parts(20_000, 1),
            vector_count: 3,
            delete_count: 0,
            sequence_number: 0,
            size_bytes: 32,
            artifact_origin: None,
        });
        first
            .write_conditional(&store, ns, &first_etag)
            .await
            .unwrap();

        let history = Manifest::list_history(&store, ns).await.unwrap();
        assert_eq!(
            history
                .iter()
                .map(|entry| entry.version)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );

        let v1 = Manifest::read_history(&store, ns, 1)
            .await
            .unwrap()
            .unwrap();
        assert!(
            v1.fragments.is_empty(),
            "history version 1 must preserve the original empty manifest"
        );

        let v2 = Manifest::read_history(&store, ns, 2)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(v2.fragments.len(), 1);
        assert_eq!(v2.fragments[0].vector_count, 3);
    }

    /// Preserves LIST metadata alongside strict, numerically sorted history refs.
    #[tokio::test]
    async fn manifest_history_observations_preserve_versions_and_project_to_refs() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let ns = "manifest_history_observations";

        Manifest::new().write(&store, ns).await.unwrap();
        for _ in 2..=3 {
            let (mut manifest, etag) = Manifest::read_versioned(&store, ns).await.unwrap().unwrap();
            manifest.updated_at = Utc::now();
            manifest.write_conditional(&store, ns, &etag).await.unwrap();
        }

        let observations = Manifest::list_history_observations(&store, ns)
            .await
            .unwrap();
        assert_eq!(
            observations
                .iter()
                .map(|observation| observation.history.version)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert!(
            observations
                .iter()
                .all(|observation| observation.storage_version.is_some()),
            "the in-memory backend supplies opaque object versions"
        );

        let projected = observations
            .into_iter()
            .map(|observation| observation.history)
            .collect::<Vec<_>>();
        assert_eq!(Manifest::list_history(&store, ns).await.unwrap(), projected);
    }

    /// Makes ordering independent of backend LIST order without inventing versions.
    #[test]
    fn manifest_history_observations_sort_numeric_generations_and_keep_none() {
        let ns = "manifest_history_observation_sort";
        let now = Utc::now();
        let listed = vec![
            ListedObject {
                key: Manifest::history_key(ns, 10),
                size: 10,
                last_modified: now,
                version: StorageVersion::from_parts(None, Some("v10".to_string())),
            },
            ListedObject {
                key: Manifest::history_key(ns, 2),
                size: 2,
                last_modified: now,
                version: None,
            },
            ListedObject {
                key: Manifest::history_key(ns, 3),
                size: 3,
                last_modified: now,
                version: StorageVersion::from_parts(Some("etag-3".to_string()), None),
            },
        ];

        let observations = Manifest::history_observations_from_listed(ns, listed).unwrap();

        assert_eq!(
            observations
                .iter()
                .map(|observation| observation.history.version)
                .collect::<Vec<_>>(),
            vec![2, 3, 10]
        );
        assert_eq!(observations[0].storage_version, None);
        assert_eq!(
            observations[1].storage_version,
            StorageVersion::from_parts(Some("etag-3".to_string()), None)
        );
        assert_eq!(
            observations[2].storage_version,
            StorageVersion::from_parts(None, Some("v10".to_string()))
        );
    }

    /// Keeps namespace binding and key-generation checks centralized for memo fills.
    #[test]
    fn manifest_history_body_decode_reuses_namespace_and_generation_validation() {
        let ns = "manifest_history_decode";
        let mut manifest = Manifest::new();
        manifest.version = 7;
        manifest.namespace = Some(ns.to_string());
        let bytes = manifest.to_bytes().unwrap();
        let history = ManifestHistoryRef {
            version: 7,
            key: Manifest::history_key(ns, 7),
        };

        let decoded = Manifest::decode_history_body(&bytes, ns, &history).unwrap();
        assert_eq!(decoded.version(), 7);

        let wrong_generation = ManifestHistoryRef {
            version: 8,
            key: Manifest::history_key(ns, 8),
        };
        let generation_error =
            Manifest::decode_history_body(&bytes, ns, &wrong_generation).unwrap_err();
        assert!(
            matches!(generation_error, ZeppelinError::Serialization(message)
                if message.contains("contains version 7, expected 8"))
        );

        let namespace_error =
            Manifest::decode_history_body(&bytes, "another_namespace", &history).unwrap_err();
        assert!(
            matches!(namespace_error, ZeppelinError::Serialization(message)
            if message.contains(
                "namespace binding mismatch: expected another_namespace, got manifest_history_decode"
            ))
        );
    }

    /// Protects count-only retention: pruning removes the oldest generations
    /// and keeps the requested newest suffix in numeric order.
    #[tokio::test]
    async fn manifest_history_prune_removes_oldest_versions_only() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let ns = "manifest_history_prune";

        Manifest::new().write(&store, ns).await.unwrap();
        for version in 2..=4 {
            let (mut manifest, etag) = Manifest::read_versioned(&store, ns).await.unwrap().unwrap();
            manifest.add_fragment(FragmentRef {
                id: Ulid::from_parts(30_000 + version, u128::from(version)),
                vector_count: version as usize,
                delete_count: 0,
                sequence_number: 0,
                size_bytes: 16,
                artifact_origin: None,
            });
            manifest.write_conditional(&store, ns, &etag).await.unwrap();
        }

        let pruned = Manifest::prune_history(&store, ns, 2).await.unwrap();
        assert_eq!(pruned, 2);
        let history = Manifest::list_history(&store, ns).await.unwrap();
        assert_eq!(
            history
                .iter()
                .map(|entry| entry.version)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );
        assert!(Manifest::read_history(&store, ns, 2)
            .await
            .unwrap()
            .is_none());
        assert!(Manifest::read_history(&store, ns, 4)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn manifest_history_prune_retains_exact_current_branch_root_generation() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let ns = "manifest_history_branch_root";

        let mut initial = Manifest::new();
        initial
            .bind_namespace_incarnation(uuid::Uuid::from_u128(0x500))
            .unwrap();
        initial.write(&store, ns).await.unwrap();
        for _ in 2..=4 {
            let (mut manifest, version) =
                Manifest::read_versioned(&store, ns).await.unwrap().unwrap();
            manifest.updated_at += chrono::Duration::seconds(1);
            manifest
                .write_conditional(&store, ns, &version)
                .await
                .unwrap();
        }

        let (mut rooted, rooted_version) =
            Manifest::read_versioned(&store, ns).await.unwrap().unwrap();
        assert_eq!(rooted.version(), 4);
        let root = branch_root(
            BranchId::from_ulid(Ulid::from(0x501_u128)),
            4,
            rooted_version.exact_manifest_digest().unwrap(),
            "retained-child",
            0x502,
        );
        rooted.insert_branch_root_candidate(root, 4).unwrap();
        rooted
            .write_conditional(&store, ns, &rooted_version)
            .await
            .unwrap();
        NamedSnapshot::create(&store, ns, "beside-root", 3)
            .await
            .unwrap();

        for _ in 0..4 {
            let (mut manifest, version) =
                Manifest::read_versioned(&store, ns).await.unwrap().unwrap();
            manifest.updated_at += chrono::Duration::seconds(1);
            manifest
                .write_conditional(&store, ns, &version)
                .await
                .unwrap();
        }

        let result = Manifest::prune_history(&store, ns, 1).await.unwrap();
        assert!(result > 0);
        assert!(Manifest::read_history(&store, ns, 4)
            .await
            .unwrap()
            .is_some());
        assert!(Manifest::read_history(&store, ns, 3)
            .await
            .unwrap()
            .is_some());
        let retained = Manifest::list_history(&store, ns).await.unwrap();
        assert!(retained.iter().any(|history| history.version == 3));
        assert!(retained.iter().any(|history| history.version == 4));
    }

    /// Protects union retention: count, PITR age, and a named snapshot each keep
    /// a generation independently of the other rules.
    #[tokio::test]
    async fn manifest_history_prune_keeps_count_or_time_or_snapshot_pin() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let ns = "manifest_history_retention";

        let mut initial = Manifest::new();
        initial.updated_at = Utc::now() - chrono::Duration::seconds(60);
        initial.write(&store, ns).await.unwrap();
        for version in 2..=5 {
            let (mut manifest, etag) = Manifest::read_versioned(&store, ns).await.unwrap().unwrap();
            manifest.add_fragment(FragmentRef {
                id: Ulid::from_parts(40_000 + version, u128::from(version)),
                vector_count: version as usize,
                delete_count: 0,
                sequence_number: 0,
                size_bytes: 16,
                artifact_origin: None,
            });
            manifest.updated_at = match version {
                2 => Utc::now() - chrono::Duration::seconds(60),
                3 => Utc::now() - chrono::Duration::seconds(5),
                4 => Utc::now() - chrono::Duration::seconds(60),
                5 => Utc::now(),
                _ => unreachable!(),
            };
            manifest.write_conditional(&store, ns, &etag).await.unwrap();
        }

        NamedSnapshot::create(&store, ns, "pin-v2", 2)
            .await
            .unwrap();
        let result = Manifest::prune_history_with_retention(
            &store,
            ns,
            ManifestHistoryRetention {
                keep_count: 1,
                pitr_retention_secs: 30,
                skew_slop_secs: 0,
            },
        )
        .await
        .unwrap();

        assert_eq!(result.pruned, 2);
        let history = Manifest::list_history(&store, ns).await.unwrap();
        assert_eq!(
            history
                .iter()
                .map(|entry| entry.version)
                .collect::<Vec<_>>(),
            vec![2, 3, 5],
            "history is retained by snapshot pin OR time window OR recent count"
        );
        assert_eq!(
            result
                .retained_manifests
                .iter()
                .map(Manifest::version)
                .collect::<Vec<_>>(),
            vec![2, 3, 5]
        );
    }

    /// Verifies that the configured clock-skew allowance extends the PITR time
    /// window without changing count retention.
    #[tokio::test]
    async fn manifest_history_prune_applies_skew_slop_to_pitr_window() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let ns = "manifest_history_retention_skew";

        let mut initial = Manifest::new();
        initial.updated_at = Utc::now() - chrono::Duration::seconds(12);
        initial.write(&store, ns).await.unwrap();
        for age_secs in [30, 0] {
            let (mut manifest, etag) = Manifest::read_versioned(&store, ns).await.unwrap().unwrap();
            manifest.updated_at = Utc::now() - chrono::Duration::seconds(age_secs);
            manifest.write_conditional(&store, ns, &etag).await.unwrap();
        }

        let result = Manifest::prune_history_with_retention(
            &store,
            ns,
            ManifestHistoryRetention {
                keep_count: 1,
                pitr_retention_secs: 10,
                skew_slop_secs: 5,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            result
                .retained_manifests
                .iter()
                .map(Manifest::version)
                .collect::<Vec<_>>(),
            vec![1, 3],
            "generation 1 is older than the PITR window but inside skew slop"
        );
    }

    /// Verifies immutable snapshot-name semantics: repeating the same target is
    /// idempotent, while retargeting the name is a typed conflict.
    #[tokio::test]
    async fn named_snapshot_create_is_idempotent_but_conflicts_on_generation_change() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let ns = "named_snapshot_create";
        Manifest::new().write(&store, ns).await.unwrap();
        for _ in 0..7 {
            let (mut manifest, etag) = Manifest::read_versioned(&store, ns).await.unwrap().unwrap();
            manifest.updated_at = Utc::now();
            manifest.write_conditional(&store, ns, &etag).await.unwrap();
        }

        let first = NamedSnapshot::create(&store, ns, "daily.2026-07-08", 7)
            .await
            .unwrap();
        let second = NamedSnapshot::create(&store, ns, "daily.2026-07-08", 7)
            .await
            .unwrap();
        assert_eq!(first.generation, second.generation);
        assert_eq!(first.created_at, second.created_at);

        let err = NamedSnapshot::create(&store, ns, "daily.2026-07-08", 8)
            .await
            .unwrap_err();
        assert!(
            matches!(err, ZeppelinError::SnapshotAlreadyExists { .. }),
            "different generation must conflict, got {err:?}"
        );
    }

    /// Ensures a named snapshot cannot pin a generation absent from retained
    /// history, which would create a false promise of recoverability.
    #[tokio::test]
    async fn named_snapshot_create_rejects_missing_history_generation() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let ns = "named_snapshot_missing_history";

        let err = NamedSnapshot::create(&store, ns, "missing", 7)
            .await
            .unwrap_err();

        assert!(
            matches!(err, ZeppelinError::Validation(_)),
            "missing history generation must be a typed validation error, got {err:?}"
        );
    }

    /// Preserves read compatibility for both current prefixed MessagePack pins
    /// and the legacy JSON representation.
    #[test]
    fn named_snapshot_decodes_msgpack_and_json() {
        let snapshot = NamedSnapshot {
            generation: 42,
            created_at: Utc::now(),
        };
        let msgpack = snapshot.to_bytes().unwrap();
        assert_eq!(NamedSnapshot::from_bytes(&msgpack).unwrap(), snapshot);

        let json = serde_json::to_vec(&snapshot).unwrap();
        assert_eq!(NamedSnapshot::from_bytes(&json).unwrap(), snapshot);
    }

    /// Lets namespace-wide inventories reuse the exact snapshot-key grammar
    /// while preserving LIST metadata and canonical name order.
    #[test]
    fn named_snapshot_validates_inventory_objects_and_sorts_by_name() {
        let namespace = "snapshot_inventory";
        let now = Utc::now();
        let listed = vec![
            ListedObject {
                key: NamedSnapshot::key(namespace, "weekly").unwrap(),
                size: 31,
                last_modified: now,
                version: StorageVersion::from_parts(None, Some("weekly-v1".to_string())),
            },
            ListedObject {
                key: NamedSnapshot::key(namespace, "daily").unwrap(),
                size: 29,
                last_modified: now,
                version: StorageVersion::from_parts(Some("daily-etag".to_string()), None),
            },
        ];

        let validated = NamedSnapshot::validate_listed_objects(namespace, listed).unwrap();

        assert_eq!(
            validated
                .iter()
                .map(|object| object.key.as_str())
                .collect::<Vec<_>>(),
            vec![
                "snapshot_inventory/snapshots/daily.msgpack",
                "snapshot_inventory/snapshots/weekly.msgpack",
            ]
        );
        assert_eq!(
            validated[0].version,
            StorageVersion::from_parts(Some("daily-etag".to_string()), None)
        );
        assert_eq!(validated[0].size, 29);
    }

    /// Rejects malformed keys from a namespace-wide LIST instead of silently
    /// excluding them from the snapshot retention root set.
    #[test]
    fn named_snapshot_inventory_rejects_nested_snapshot_key() {
        let namespace = "snapshot_inventory_invalid";
        let malformed = ListedObject {
            key: format!("{namespace}/snapshots/team/daily.msgpack"),
            size: 31,
            last_modified: Utc::now(),
            version: None,
        };

        let error = NamedSnapshot::validate_listed_objects(namespace, vec![malformed])
            .expect_err("nested snapshot keys must fail canonical validation");

        assert!(
            matches!(&error, ZeppelinError::MalformedControlKey { key, family, .. }
                if key.contains("team/daily") && *family == "snapshot"),
            "malformed inventory key must be rejected by snapshot-name grammar, got {error:?}"
        );
    }

    /// Refuses to pair a snapshot body with a different ETag than the
    /// namespace inventory observed for that key.
    #[tokio::test]
    async fn named_snapshot_rejects_body_changed_since_inventory_list() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let namespace = "snapshot_inventory_changed";
        let key = NamedSnapshot::key(namespace, "daily").unwrap();
        let snapshot = NamedSnapshot {
            generation: 7,
            created_at: Utc::now(),
        };
        store.put(&key, snapshot.to_bytes().unwrap()).await.unwrap();
        let listed = ListedObject {
            key: key.clone(),
            size: 31,
            last_modified: Utc::now(),
            version: StorageVersion::from_parts(Some("stale-list-etag".to_string()), None),
        };

        let error = NamedSnapshot::read_listed_observation(&store, namespace, listed)
            .await
            .unwrap_err();

        assert!(
            matches!(&error, ZeppelinError::Serialization(message) if
                message.contains("changed between LIST ETag") && message.contains(&key)),
            "LIST/GET identity mismatch must fail loud, got {error:?}"
        );
    }

    /// Protects exact-set compaction removal when a concurrently appended ULID
    /// sorts within the compacted snapshot's apparent range.
    #[test]
    fn test_remove_compacted_fragments_exact_set() {
        // A fragment appended concurrently with compaction can have a ULID
        // that sorts <= the snapshot's max (same-millisecond random bits).
        // Removal by exact ID set must retain it.
        let mut manifest = Manifest::new();
        let snapshot_a = Ulid::from_parts(1000, 500);
        let snapshot_b = Ulid::from_parts(1000, 900); // snapshot max
        let concurrent = Ulid::from_parts(1000, 700); // sorts between a and b

        for id in [snapshot_a, snapshot_b, concurrent] {
            manifest.add_fragment(FragmentRef {
                id,
                vector_count: 1,
                delete_count: 0,
                sequence_number: 0,
                size_bytes: 0,
                artifact_origin: None,
            });
        }

        let compacted: HashSet<Ulid> = [snapshot_a, snapshot_b].into_iter().collect();
        manifest.remove_compacted_fragments(&compacted);

        assert_eq!(manifest.fragments.len(), 1);
        assert_eq!(
            manifest.fragments[0].id, concurrent,
            "concurrently appended fragment with ULID <= snapshot max must survive"
        );
        assert_eq!(manifest.compaction_watermark, Some(snapshot_b));
    }

    /// Ensures the observability watermark remains monotonic even when a later
    /// compaction removes an older ULID.
    #[test]
    fn test_watermark_never_regresses() {
        let mut manifest = Manifest::new();
        let newer = Ulid::from_parts(2000, 0);
        let older = Ulid::from_parts(1000, 0);

        manifest.add_fragment(FragmentRef {
            id: newer,
            vector_count: 1,
            delete_count: 0,
            sequence_number: 0,
            size_bytes: 0,
            artifact_origin: None,
        });
        manifest.remove_compacted_fragments(&[newer].into_iter().collect());
        assert_eq!(manifest.compaction_watermark, Some(newer));

        manifest.add_fragment(FragmentRef {
            id: older,
            vector_count: 1,
            delete_count: 0,
            sequence_number: 0,
            size_bytes: 0,
            artifact_origin: None,
        });
        manifest.remove_compacted_fragments(&[older].into_iter().collect());
        assert_eq!(
            manifest.compaction_watermark,
            Some(newer),
            "watermark is observability metadata and must not move backwards"
        );
    }

    /// Verifies the namespace metadata estimate combines segment entries and
    /// WAL upserts, then subtracts WAL tombstones.
    #[test]
    fn test_vector_count_includes_segments_fragments_minus_tombstones() {
        let mut manifest = Manifest::new();

        let mut seg_a = make_segment("seg_a");
        seg_a.vector_count = 100;
        let mut seg_b = make_segment("seg_b");
        seg_b.vector_count = 25;
        manifest.add_segment(seg_a);
        manifest.add_segment(seg_b);

        manifest.add_fragment(FragmentRef {
            id: Ulid::from_parts(3000, 1),
            vector_count: 10,
            delete_count: 3,
            sequence_number: 0,
            size_bytes: 111,
            artifact_origin: None,
        });
        manifest.add_fragment(FragmentRef {
            id: Ulid::from_parts(3000, 2),
            vector_count: 5,
            delete_count: 1,
            sequence_number: 0,
            size_bytes: 222,
            artifact_origin: None,
        });

        assert_eq!(
            manifest.vector_count(),
            136,
            "namespace vector_count is the manifest aggregate: segment vectors + WAL entries - tombstones"
        );
    }

    /// Verifies excessive tombstones saturate the aggregate at zero instead of
    /// underflowing an unsigned count.
    #[test]
    fn test_vector_count_is_zero_when_tombstones_exceed_entries() {
        let mut manifest = Manifest::new();
        manifest.add_fragment(FragmentRef {
            id: Ulid::from_parts(4000, 1),
            vector_count: 1,
            delete_count: 3,
            sequence_number: 0,
            size_bytes: 10,
            artifact_origin: None,
        });

        assert_eq!(
            manifest.vector_count(),
            0,
            "manifest-derived counts are lower-bounded at zero for deletes of absent IDs"
        );
    }

    /// The namespace lifetime is persisted in the same bytes and ETag used by
    /// guarded appends, while clone preparation explicitly clears that source
    /// identity before the target binds its own incarnation.
    #[test]
    fn namespace_incarnation_roundtrips_and_clone_rebinds() {
        let source_incarnation = uuid::Uuid::from_u128(0xaced);
        let target_incarnation = uuid::Uuid::from_u128(0xbeef);
        let mut manifest = Manifest::new();
        manifest
            .bind_namespace_incarnation(source_incarnation)
            .expect("a fresh manifest must accept its source incarnation");
        manifest
            .bind_namespace_incarnation(source_incarnation)
            .expect("rebinding the identical incarnation must be idempotent");
        assert!(manifest
            .bind_namespace_incarnation(target_incarnation)
            .is_err());

        let decoded = Manifest::from_bytes(&manifest.to_bytes().unwrap()).unwrap();
        assert_eq!(
            decoded.namespace_incarnation(),
            Some(source_incarnation),
            "the guarded-write incarnation must survive the manifest wire format"
        );

        let mut clone = decoded;
        clone.reset_version_for_clone();
        assert_eq!(clone.namespace_incarnation(), None);
        clone
            .bind_namespace_incarnation(target_incarnation)
            .expect("clone target must bind a fresh namespace lifetime");
        assert_eq!(clone.namespace_incarnation(), Some(target_incarnation));
    }

    #[test]
    fn local_and_foreign_origin_manifests_round_trip_with_fail_closed_admission() {
        let mut local = Manifest::new();
        local.namespace = Some("origin-roundtrip".to_string());
        local
            .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
            .unwrap();
        let local_bytes = local.to_bytes().unwrap();
        let decoded_local =
            Manifest::from_bytes_for_namespace(&local_bytes, "origin-roundtrip").unwrap();
        assert!(decoded_local.artifact_origins.is_empty());
        assert_eq!(
            decoded_local.local_origin().unwrap(),
            origin("origin-roundtrip", 1)
        );

        let mut explicit_local = local.clone();
        explicit_local.artifact_origins = vec![origin("origin-roundtrip", 1)];
        let mut local_segment = make_segment("segment-local-origin");
        local_segment.artifact_origin = Some(ArtifactOriginIndex::new(0));
        explicit_local.segments.push(local_segment);
        explicit_local.receipt_binding_version = Some(ReceiptBindingVersion::V2Origins);
        explicit_local.receipt_state_digest = Some(
            explicit_local
                .compute_receipt_state_digest("origin-roundtrip", ReceiptBindingVersion::V2Origins)
                .unwrap(),
        );
        let decoded_explicit = Manifest::from_bytes_for_namespace(
            &explicit_local.to_bytes().unwrap(),
            "origin-roundtrip",
        )
        .unwrap();
        assert_eq!(
            decoded_explicit
                .segment_origin(&decoded_explicit.segments[0])
                .unwrap(),
            origin("origin-roundtrip", 1)
        );

        let mut foreign = local;
        foreign.artifact_origins = vec![origin("source", 2)];
        let mut foreign_segment = make_segment("segment-foreign-origin");
        foreign_segment.artifact_origin = Some(ArtifactOriginIndex::new(0));
        foreign.segments.push(foreign_segment);
        foreign.receipt_binding_version = Some(ReceiptBindingVersion::V2Origins);
        foreign.receipt_state_digest = Some(
            foreign
                .compute_receipt_state_digest("origin-roundtrip", ReceiptBindingVersion::V2Origins)
                .unwrap(),
        );
        let foreign_bytes = foreign.to_bytes().unwrap();
        let decoded_foreign = Manifest::from_bytes(&foreign_bytes).unwrap();
        decoded_foreign.validate_artifact_origins().unwrap();
        assert_eq!(
            decoded_foreign
                .segment_origin(&decoded_foreign.segments[0])
                .unwrap(),
            origin("source", 2)
        );
        assert!(matches!(
            Manifest::from_bytes_for_namespace(&foreign_bytes, "origin-roundtrip")
                .expect_err("foreign origin admission must stay closed until lineage lands"),
            ZeppelinError::Branch(error)
                if matches!(error.as_ref(), BranchError::BranchingNotReady { .. })
        ));
    }

    #[test]
    fn immediately_pre_origin_manifest_resolves_implicit_refs_locally() {
        #[derive(Serialize)]
        struct OldFragmentBeforeOrigins {
            id: Ulid,
            vector_count: usize,
            delete_count: usize,
            sequence_number: u64,
            size_bytes: u64,
        }

        #[derive(Serialize)]
        struct OldSegmentBeforeOrigins {
            id: String,
            vector_count: usize,
            cluster_count: usize,
            quantization: crate::index::quantization::QuantizationType,
            hierarchical: bool,
            bitmap_fields: Vec<String>,
            fts_fields: Vec<String>,
            has_global_fts: bool,
            cluster_owners: Vec<String>,
            sketch: Option<SketchRef>,
            cluster_objects: Vec<ClusterDataObjectRef>,
            bootstrap: Option<BootstrapRef>,
            membership: Option<MembershipRef>,
        }

        #[derive(Serialize)]
        struct OldManifestBeforeOrigins {
            fragments: Vec<OldFragmentBeforeOrigins>,
            segments: Vec<OldSegmentBeforeOrigins>,
            compaction_watermark: Option<Ulid>,
            active_segment: Option<String>,
            next_sequence: u64,
            pending_deletes: Vec<String>,
            fencing_token: u64,
            updated_at: DateTime<Utc>,
            version: u64,
            namespace: Option<String>,
            namespace_incarnation: Option<ManifestNamespaceIncarnation>,
            deletion_fence: Option<ManifestDeletionFence>,
            artifact_hashes: BTreeMap<String, [u8; 32]>,
            merkle_root: Option<[u8; 32]>,
            root_signature: Option<Vec<u8>>,
            root_signer_node: Option<String>,
            hierarchical_routing_nodes: BTreeMap<String, Vec<String>>,
            receipt_state_digest: Option<[u8; 32]>,
            receipt_binding_version: Option<ReceiptBindingVersion>,
        }

        let fragment_id = Ulid::from(0x123_u128);
        let namespace_incarnation = uuid::Uuid::from_u128(0x456);
        let old = OldManifestBeforeOrigins {
            fragments: vec![OldFragmentBeforeOrigins {
                id: fragment_id,
                vector_count: 1,
                delete_count: 0,
                sequence_number: 0,
                size_bytes: 7,
            }],
            segments: vec![OldSegmentBeforeOrigins {
                id: "legacy-segment".to_string(),
                vector_count: 1,
                cluster_count: 1,
                quantization: crate::index::quantization::QuantizationType::None,
                hierarchical: false,
                bitmap_fields: Vec::new(),
                fts_fields: Vec::new(),
                has_global_fts: false,
                cluster_owners: Vec::new(),
                sketch: None,
                cluster_objects: Vec::new(),
                bootstrap: None,
                membership: None,
            }],
            compaction_watermark: None,
            active_segment: Some("legacy-segment".to_string()),
            next_sequence: 1,
            pending_deletes: Vec::new(),
            fencing_token: 2,
            updated_at: Utc::now(),
            version: 3,
            namespace: Some("legacy-bound".to_string()),
            namespace_incarnation: Some(ManifestNamespaceIncarnation::from_uuid(
                namespace_incarnation,
            )),
            deletion_fence: None,
            artifact_hashes: BTreeMap::new(),
            merkle_root: None,
            root_signature: None,
            root_signer_node: None,
            hierarchical_routing_nodes: BTreeMap::new(),
            receipt_state_digest: None,
            receipt_binding_version: None,
        };

        let mut data = vec![MANIFEST_FORMAT_MSGPACK];
        data.extend_from_slice(&rmp_serde::to_vec(&old).unwrap());
        let decoded = Manifest::from_bytes_for_namespace(&data, "legacy-bound")
            .expect("the immediately pre-origin positional shape must decode locally");

        assert!(decoded.artifact_origins.is_empty());
        assert!(decoded.branch_roots().is_empty());
        assert!(decoded.branch_lineage().is_none());
        assert_eq!(decoded.fragments[0].artifact_origin, None);
        assert_eq!(decoded.segments[0].artifact_origin, None);
        let expected = ArtifactOrigin {
            namespace: NamespaceId::parse("legacy-bound").unwrap(),
            incarnation: NamespaceIncarnationId::from_uuid(namespace_incarnation),
        };
        assert_eq!(decoded.local_origin().unwrap(), expected);
        assert_eq!(
            decoded.fragment_origin(&decoded.fragments[0]).unwrap(),
            expected
        );
        assert_eq!(
            decoded.segment_origin(&decoded.segments[0]).unwrap(),
            expected
        );
    }

    /// Backward compat: manifests serialized BEFORE `FragmentRef.size_bytes`
    /// existed must still decode, in both MessagePack (version byte 0x01,
    /// structs encoded as arrays — new fields must be trailing + defaulted)
    /// and legacy JSON.
    #[test]
    fn test_decode_manifest_without_size_bytes_field() {
        // Replica of the pre-size_bytes wire shape.
        /// Positional fragment shape written before `size_bytes` was appended.
        #[derive(Serialize)]
        struct OldFragmentRef {
            /// Historical fragment identifier field.
            id: Ulid,
            /// Historical upsert-entry count field.
            vector_count: usize,
            /// Historical tombstone count field.
            delete_count: usize,
            /// Historical replay-order field.
            sequence_number: u64,
        }
        /// Positional manifest shape whose fragment element lacks `size_bytes`.
        #[derive(Serialize)]
        struct OldManifest {
            /// Historical visible fragment descriptors.
            fragments: Vec<OldFragmentRef>,
            /// Historical visible segment descriptors.
            segments: Vec<SegmentRef>,
            /// Historical compaction observability watermark.
            compaction_watermark: Option<Ulid>,
            /// Historical active-segment pointer.
            active_segment: Option<String>,
            /// Historical next replay sequence.
            next_sequence: u64,
            /// Historical deferred-delete queue.
            pending_deletes: Vec<String>,
            /// Historical writer fencing token.
            fencing_token: u64,
            /// Historical update timestamp and final field in this wire shape.
            updated_at: DateTime<Utc>,
        }

        let frag_id = Ulid::new();
        let old = OldManifest {
            fragments: vec![OldFragmentRef {
                id: frag_id,
                vector_count: 42,
                delete_count: 3,
                sequence_number: 7,
            }],
            segments: vec![make_segment("seg_old")],
            compaction_watermark: None,
            active_segment: Some("seg_old".to_string()),
            next_sequence: 8,
            pending_deletes: vec!["ns/wal/x.wal".to_string()],
            fencing_token: 2,
            updated_at: Utc::now(),
        };

        // MessagePack with the 0x01 version byte (current on-S3 format).
        let msgpack = rmp_serde::to_vec(&old).unwrap();
        let mut data = vec![MANIFEST_FORMAT_MSGPACK];
        data.extend_from_slice(&msgpack);
        let decoded = Manifest::from_bytes(&data)
            .expect("old msgpack manifest without size_bytes must decode");
        assert_eq!(decoded.fragments.len(), 1);
        assert_eq!(decoded.fragments[0].id, frag_id);
        assert_eq!(decoded.fragments[0].vector_count, 42);
        assert_eq!(decoded.fragments[0].sequence_number, 7);
        assert_eq!(decoded.fragments[0].artifact_origin, None);
        assert!(decoded.artifact_origins.is_empty());
        assert!(decoded.branch_roots().is_empty());
        assert_eq!(
            decoded.fragments[0].size_bytes, 0,
            "missing size_bytes decodes to the serde default (0)"
        );

        // Legacy JSON format (no version byte, starts with '{').
        let json = serde_json::to_vec(&old).unwrap();
        let decoded_json = Manifest::from_bytes(&json)
            .expect("legacy JSON manifest without size_bytes must decode");
        assert_eq!(decoded_json.fragments[0].id, frag_id);
        assert_eq!(decoded_json.fragments[0].size_bytes, 0);
    }

    /// Backward compat: manifests serialized BEFORE `SegmentRef.cluster_owners`
    /// existed must still decode, in both MessagePack (structs as arrays — new
    /// fields must be trailing + defaulted) and legacy JSON. A decoded old
    /// segment must resolve every cluster to its own ID (legacy layout).
    #[test]
    fn test_decode_manifest_without_cluster_owners_field() {
        // Replica of the pre-cluster_owners SegmentRef wire shape (has_global_fts
        // was the last field).
        /// Positional segment shape written before incremental-owner metadata.
        #[derive(Serialize)]
        struct OldSegmentRef {
            /// Historical segment identifier.
            id: String,
            /// Historical vector-entry count.
            vector_count: usize,
            /// Historical IVF cluster count.
            cluster_count: usize,
            /// Historical quantization choice.
            quantization: crate::index::quantization::QuantizationType,
            /// Historical hierarchical-index flag.
            hierarchical: bool,
            /// Historical bitmap-indexed field names.
            bitmap_fields: Vec<String>,
            /// Historical field-level FTS names.
            fts_fields: Vec<String>,
            /// Historical global-FTS presence flag and final field in this shape.
            has_global_fts: bool,
        }
        /// Current outer manifest shape containing an old segment element.
        #[derive(Serialize)]
        struct MixedManifest {
            /// Visible WAL descriptors.
            fragments: Vec<FragmentRef>,
            /// Legacy segment descriptors under compatibility test.
            segments: Vec<OldSegmentRef>,
            /// Compaction observability watermark.
            compaction_watermark: Option<Ulid>,
            /// Active segment pointer.
            active_segment: Option<String>,
            /// Next replay sequence.
            next_sequence: u64,
            /// Deferred-delete queue.
            pending_deletes: Vec<String>,
            /// Writer fencing token.
            fencing_token: u64,
            /// Update timestamp and final field in this historical outer shape.
            updated_at: DateTime<Utc>,
        }

        let old = MixedManifest {
            fragments: vec![],
            segments: vec![OldSegmentRef {
                id: "seg_legacy".to_string(),
                vector_count: 100,
                cluster_count: 4,
                quantization: crate::index::quantization::QuantizationType::None,
                hierarchical: false,
                bitmap_fields: vec![],
                fts_fields: vec![],
                has_global_fts: false,
            }],
            compaction_watermark: None,
            active_segment: Some("seg_legacy".to_string()),
            next_sequence: 0,
            pending_deletes: vec![],
            fencing_token: 0,
            updated_at: Utc::now(),
        };

        // MessagePack with the 0x01 version byte (current on-S3 format).
        let msgpack = rmp_serde::to_vec(&old).unwrap();
        let mut data = vec![MANIFEST_FORMAT_MSGPACK];
        data.extend_from_slice(&msgpack);
        let decoded = Manifest::from_bytes(&data)
            .expect("old msgpack manifest without cluster_owners must decode");
        assert_eq!(decoded.segments.len(), 1);
        let seg = &decoded.segments[0];
        assert_eq!(seg.id, "seg_legacy");
        assert!(
            seg.cluster_owners.is_empty(),
            "missing cluster_owners decodes to the serde default (empty)"
        );
        // Empty cluster_owners ⇒ every cluster owned by the segment itself.
        for i in 0..seg.cluster_count {
            assert_eq!(
                seg.cluster_owner(i),
                "seg_legacy",
                "legacy layout: cluster {i} must resolve to the segment's own ID"
            );
        }

        // Legacy JSON format (no version byte, starts with '{').
        let json = serde_json::to_vec(&old).unwrap();
        let decoded_json = Manifest::from_bytes(&json)
            .expect("legacy JSON manifest without cluster_owners must decode");
        assert!(decoded_json.segments[0].cluster_owners.is_empty());
        assert_eq!(decoded_json.segments[0].cluster_owner(3), "seg_legacy");
    }

    /// Verifies that explicit carried-over owners win and missing owner entries
    /// fall back to the logical segment's own ID.
    #[test]
    fn test_cluster_owner_resolution() {
        let mut seg = make_segment("seg_new");
        seg.cluster_count = 4;
        // Clusters 0 and 2 carried over from an older segment; 1 and 3 rewritten.
        seg.cluster_owners = vec![
            "seg_old".to_string(),
            "seg_new".to_string(),
            "seg_old".to_string(),
            "seg_new".to_string(),
        ];
        assert_eq!(seg.cluster_owner(0), "seg_old");
        assert_eq!(seg.cluster_owner(1), "seg_new");
        assert_eq!(seg.cluster_owner(2), "seg_old");
        assert_eq!(seg.cluster_owner(3), "seg_new");
        // Out-of-range index falls back to the segment's own ID.
        assert_eq!(seg.cluster_owner(99), "seg_new");
    }

    /// Verifies that incremental-compaction owner routing survives a current
    /// manifest encode/decode round trip.
    #[test]
    fn test_cluster_owners_roundtrip() {
        let mut manifest = Manifest::new();
        let mut seg = make_segment("seg_rt");
        seg.cluster_owners = vec!["seg_a".to_string(), "seg_rt".to_string()];
        manifest.add_segment(seg);
        let bytes = manifest.to_bytes().unwrap();
        let decoded = Manifest::from_bytes(&bytes).unwrap();
        assert_eq!(
            decoded.segments[0].cluster_owners,
            vec!["seg_a".to_string(), "seg_rt".to_string()]
        );
    }

    /// Verifies old grouped-cluster refs default to full-object reads while new
    /// live-span and size metadata round-trips exactly.
    #[test]
    fn test_cluster_data_object_live_span_defaults_and_roundtrip() {
        /// Positional grouped-object shape written before ranged live spans.
        #[derive(Serialize)]
        struct OldClusterDataObjectRef {
            /// Immutable grouped-object key.
            key: String,
            /// Logical clusters stored in the object.
            clusters: Vec<usize>,
        }
        /// Segment wire shape combining current fields with the old object ref.
        #[derive(Serialize)]
        struct MixedSegmentRef {
            /// Segment identifier.
            id: String,
            /// Vector-entry count.
            vector_count: usize,
            /// IVF cluster count.
            cluster_count: usize,
            /// Quantization choice.
            quantization: crate::index::quantization::QuantizationType,
            /// Hierarchical-index flag.
            hierarchical: bool,
            /// Bitmap-indexed fields.
            bitmap_fields: Vec<String>,
            /// Field-level FTS indexes.
            fts_fields: Vec<String>,
            /// Global-FTS presence flag.
            has_global_fts: bool,
            /// Incremental cluster owners.
            cluster_owners: Vec<String>,
            /// Optional coarse sketch.
            sketch: Option<SketchRef>,
            /// Old grouped-object descriptors under test.
            cluster_objects: Vec<OldClusterDataObjectRef>,
            /// Optional bootstrap object and final field in this shape.
            bootstrap: Option<BootstrapRef>,
        }
        /// Outer manifest shape containing the mixed segment representation.
        #[derive(Serialize)]
        struct MixedManifest {
            /// Visible WAL descriptors.
            fragments: Vec<FragmentRef>,
            /// Mixed segment descriptors.
            segments: Vec<MixedSegmentRef>,
            /// Compaction watermark.
            compaction_watermark: Option<Ulid>,
            /// Active segment pointer.
            active_segment: Option<String>,
            /// Next replay sequence.
            next_sequence: u64,
            /// Deferred-delete queue.
            pending_deletes: Vec<String>,
            /// Writer fencing token.
            fencing_token: u64,
            /// Update timestamp and final outer field.
            updated_at: DateTime<Utc>,
        }

        let old = MixedManifest {
            fragments: vec![],
            segments: vec![MixedSegmentRef {
                id: "seg_grouped".to_string(),
                vector_count: 10,
                cluster_count: 2,
                quantization: crate::index::quantization::QuantizationType::Scalar,
                hierarchical: false,
                bitmap_fields: vec![],
                fts_fields: vec![],
                has_global_fts: false,
                cluster_owners: vec![],
                sketch: None,
                cluster_objects: vec![OldClusterDataObjectRef {
                    key: "ns/segments/seg_grouped/cluster_group_0.bin".to_string(),
                    clusters: vec![0, 1],
                }],
                bootstrap: None,
            }],
            compaction_watermark: None,
            active_segment: Some("seg_grouped".to_string()),
            next_sequence: 0,
            pending_deletes: vec![],
            fencing_token: 0,
            updated_at: Utc::now(),
        };

        let msgpack = rmp_serde::to_vec(&old).unwrap();
        let mut data = vec![MANIFEST_FORMAT_MSGPACK];
        data.extend_from_slice(&msgpack);
        let decoded = Manifest::from_bytes(&data)
            .expect("old cluster object refs without live spans must decode");
        let object_ref = &decoded.segments[0].cluster_objects[0];
        assert_eq!(object_ref.live_offset, 0);
        assert_eq!(object_ref.live_len, 0);
        assert!(object_ref.live_range().unwrap().is_none());

        let mut manifest = Manifest::new();
        let mut seg = make_segment("seg_live");
        seg.cluster_objects = vec![ClusterDataObjectRef {
            key: "ns/segments/seg_live/cluster_group_0.bin".to_string(),
            clusters: vec![0, 1],
            live_offset: 0,
            live_len: 123,
            size_bytes: 456,
        }];
        manifest.add_segment(seg);
        let bytes = manifest.to_bytes().unwrap();
        let decoded = Manifest::from_bytes(&bytes).unwrap();
        let object_ref = &decoded.segments[0].cluster_objects[0];
        assert_eq!(object_ref.live_range().unwrap(), Some(0..123));
        assert_eq!(object_ref.size_bytes, 456);
    }

    /// Verifies cluster-object refs written before `size_bytes` decode their
    /// unknown size as zero without losing a previously stored live span.
    #[test]
    fn test_decode_cluster_ref_without_size_bytes_field() {
        /// Positional grouped-object shape immediately before `size_bytes`.
        #[derive(Serialize)]
        struct OldClusterDataObjectRef {
            /// Immutable grouped-object key.
            key: String,
            /// Logical clusters in the object.
            clusters: Vec<usize>,
            /// Historical live-span start.
            live_offset: u64,
            /// Historical live-span length and final field in this shape.
            live_len: u64,
        }
        /// Segment wire shape containing the old grouped-object representation.
        #[derive(Serialize)]
        struct MixedSegmentRef {
            /// Segment identifier.
            id: String,
            /// Vector-entry count.
            vector_count: usize,
            /// IVF cluster count.
            cluster_count: usize,
            /// Quantization choice.
            quantization: crate::index::quantization::QuantizationType,
            /// Hierarchical-index flag.
            hierarchical: bool,
            /// Bitmap-indexed fields.
            bitmap_fields: Vec<String>,
            /// Field-level FTS indexes.
            fts_fields: Vec<String>,
            /// Global-FTS presence flag.
            has_global_fts: bool,
            /// Incremental cluster owners.
            cluster_owners: Vec<String>,
            /// Optional coarse sketch.
            sketch: Option<SketchRef>,
            /// Old grouped-object descriptors under test.
            cluster_objects: Vec<OldClusterDataObjectRef>,
            /// Optional bootstrap object and final field in this shape.
            bootstrap: Option<BootstrapRef>,
        }
        /// Outer manifest shape containing the mixed segment representation.
        #[derive(Serialize)]
        struct MixedManifest {
            /// Visible WAL descriptors.
            fragments: Vec<FragmentRef>,
            /// Mixed segment descriptors.
            segments: Vec<MixedSegmentRef>,
            /// Compaction watermark.
            compaction_watermark: Option<Ulid>,
            /// Active segment pointer.
            active_segment: Option<String>,
            /// Next replay sequence.
            next_sequence: u64,
            /// Deferred-delete queue.
            pending_deletes: Vec<String>,
            /// Writer fencing token.
            fencing_token: u64,
            /// Update timestamp and final outer field.
            updated_at: DateTime<Utc>,
        }

        let old = MixedManifest {
            fragments: vec![],
            segments: vec![MixedSegmentRef {
                id: "seg_grouped".to_string(),
                vector_count: 10,
                cluster_count: 2,
                quantization: crate::index::quantization::QuantizationType::Scalar,
                hierarchical: false,
                bitmap_fields: vec![],
                fts_fields: vec![],
                has_global_fts: false,
                cluster_owners: vec![],
                sketch: None,
                cluster_objects: vec![OldClusterDataObjectRef {
                    key: "ns/segments/seg_grouped/cluster_group_0.bin".to_string(),
                    clusters: vec![0, 1],
                    live_offset: 0,
                    live_len: 123,
                }],
                bootstrap: None,
            }],
            compaction_watermark: None,
            active_segment: Some("seg_grouped".to_string()),
            next_sequence: 0,
            pending_deletes: vec![],
            fencing_token: 0,
            updated_at: Utc::now(),
        };

        let msgpack = rmp_serde::to_vec(&old).unwrap();
        let mut data = vec![MANIFEST_FORMAT_MSGPACK];
        data.extend_from_slice(&msgpack);
        let decoded = Manifest::from_bytes(&data)
            .expect("old cluster object refs without size_bytes must decode");
        let object_ref = &decoded.segments[0].cluster_objects[0];
        assert_eq!(object_ref.live_offset, 0);
        assert_eq!(object_ref.live_len, 123);
        assert_eq!(
            object_ref.size_bytes, 0,
            "missing cluster object size_bytes decodes to unknown"
        );
    }

    /// Backward compatibility: manifests serialized before
    /// [`SegmentRef::membership`]
    /// existed must still decode, in both MessagePack (structs as arrays —
    /// new fields must be trailing + defaulted) and legacy JSON.
    #[test]
    fn test_decode_segment_ref_without_membership_field() {
        /// Positional segment shape immediately before membership metadata.
        #[derive(Serialize)]
        struct OldSegmentRef {
            /// Segment identifier.
            id: String,
            /// Vector-entry count.
            vector_count: usize,
            /// IVF cluster count.
            cluster_count: usize,
            /// Quantization choice.
            quantization: crate::index::quantization::QuantizationType,
            /// Hierarchical-index flag.
            hierarchical: bool,
            /// Bitmap-indexed fields.
            bitmap_fields: Vec<String>,
            /// Field-level FTS indexes.
            fts_fields: Vec<String>,
            /// Global-FTS presence flag.
            has_global_fts: bool,
            /// Incremental cluster owners.
            cluster_owners: Vec<String>,
            /// Optional coarse sketch.
            sketch: Option<SketchRef>,
            /// Grouped cluster-data objects.
            cluster_objects: Vec<ClusterDataObjectRef>,
            /// Optional bootstrap and final field in this historical shape.
            bootstrap: Option<BootstrapRef>,
        }
        /// Outer manifest shape containing a pre-membership segment.
        #[derive(Serialize)]
        struct MixedManifest {
            /// Visible WAL descriptors.
            fragments: Vec<FragmentRef>,
            /// Legacy segment descriptors under test.
            segments: Vec<OldSegmentRef>,
            /// Compaction watermark.
            compaction_watermark: Option<Ulid>,
            /// Active segment pointer.
            active_segment: Option<String>,
            /// Next replay sequence.
            next_sequence: u64,
            /// Deferred-delete queue.
            pending_deletes: Vec<String>,
            /// Writer fencing token.
            fencing_token: u64,
            /// Update timestamp and final outer field.
            updated_at: DateTime<Utc>,
        }

        let old = MixedManifest {
            fragments: vec![],
            segments: vec![OldSegmentRef {
                id: "seg_pre_membership".to_string(),
                vector_count: 10,
                cluster_count: 2,
                quantization: crate::index::quantization::QuantizationType::None,
                hierarchical: false,
                bitmap_fields: vec![],
                fts_fields: vec![],
                has_global_fts: false,
                cluster_owners: vec![],
                sketch: None,
                cluster_objects: vec![],
                bootstrap: None,
            }],
            compaction_watermark: None,
            active_segment: Some("seg_pre_membership".to_string()),
            next_sequence: 0,
            pending_deletes: vec![],
            fencing_token: 0,
            updated_at: Utc::now(),
        };

        let msgpack = rmp_serde::to_vec(&old).unwrap();
        let mut data = vec![MANIFEST_FORMAT_MSGPACK];
        data.extend_from_slice(&msgpack);
        let decoded =
            Manifest::from_bytes(&data).expect("old segment refs without membership must decode");
        assert!(
            decoded.segments[0].membership.is_none(),
            "missing membership decodes to the serde default (None)"
        );

        let json = serde_json::to_vec(&old).unwrap();
        let decoded_json = Manifest::from_bytes(&json)
            .expect("legacy JSON segment refs without membership must decode");
        assert!(decoded_json.segments[0].membership.is_none());
        assert!(decoded_json.segments[0].artifact_origin.is_none());
        assert!(decoded_json.artifact_origins.is_empty());
    }

    /// Verifies that a current fragment's stored byte size survives a manifest
    /// encode/decode round trip.
    #[test]
    fn test_size_bytes_roundtrip() {
        let mut manifest = Manifest::new();
        manifest.add_fragment(FragmentRef {
            id: Ulid::new(),
            vector_count: 1,
            delete_count: 0,
            sequence_number: 0,
            size_bytes: 12_345,
            artifact_origin: None,
        });
        let bytes = manifest.to_bytes().unwrap();
        let decoded = Manifest::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.fragments[0].size_bytes, 12_345);
    }

    /// Protects the leak-prevention invariant that pruning metadata never drops
    /// object keys whose remote deletion has not succeeded.
    #[test]
    fn test_prune_never_drops_pending_deletes() {
        // Every pending_deletes entry is an S3 key still awaiting deletion;
        // draining entries without deleting the objects leaks them forever.
        let mut manifest = Manifest::new();
        for i in 0..10 {
            manifest.pending_deletes.push(format!("key_{i}"));
        }
        manifest.prune(5, 10);
        assert_eq!(manifest.pending_deletes.len(), 10);
    }

    /// Verifies segment pruning retains the active descriptor plus the requested
    /// number of recent older descriptors.
    #[test]
    fn test_prune_caps_old_segments() {
        let mut manifest = Manifest::new();
        for i in 0..6 {
            manifest.add_segment_with_limits(make_segment(&format!("seg_{i}")), 1000, 10);
        }
        // 6 segments, active is seg_5
        assert_eq!(manifest.segments.len(), 6);

        // Now prune with max_old_segments=2 → keep active + 2 old = 3
        manifest.prune(1000, 2);
        assert_eq!(manifest.segments.len(), 3);
        // Active segment must be retained
        assert!(manifest.segments.iter().any(|s| s.id == "seg_5"));
    }

    /// Verifies adding with limits prunes only segment metadata and leaves every
    /// deferred object deletion recorded.
    #[test]
    fn test_add_segment_with_limits_prunes_segments_only() {
        let mut manifest = Manifest::new();
        for i in 0..10 {
            manifest.pending_deletes.push(format!("key_{i}"));
        }
        for i in 0..6 {
            manifest.add_segment_with_limits(make_segment(&format!("seg_{i}")), 3, 2);
        }
        // Segment refs are pruned (metadata only)...
        assert_eq!(manifest.segments.len(), 3);
        // ...but pending_deletes entries are never dropped (they are S3 keys
        // still awaiting deletion).
        assert_eq!(manifest.pending_deletes.len(), 10);
    }

    /// Verifies removing the active segment also clears the routing pointer while
    /// leaving unrelated older descriptors intact.
    #[test]
    fn test_remove_segment_clears_active_segment() {
        let mut manifest = Manifest::new();
        manifest.add_segment(make_segment("seg_old"));
        manifest.add_segment(make_segment("seg_live"));

        manifest.remove_segment("seg_live");

        assert_eq!(manifest.active_segment, None);
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(manifest.segments[0].id, "seg_old");
    }

    #[test]
    fn routing_node_inventory_is_pruned_with_removed_and_capped_segments() {
        let mut manifest = Manifest::new();
        let mut first = make_segment("seg_first");
        first.hierarchical = true;
        let mut second = make_segment("seg_second");
        second.hierarchical = true;
        let mut third = make_segment("seg_third");
        third.hierarchical = true;

        manifest.add_segment_with_limits(first, 1_000, 1);
        manifest.set_hierarchical_routing_nodes("seg_first", vec!["node-first".to_string()]);
        manifest.add_segment_with_limits(second, 1_000, 1);
        manifest.set_hierarchical_routing_nodes("seg_second", vec!["node-second".to_string()]);
        manifest.add_segment_with_limits(third, 1_000, 1);
        manifest.set_hierarchical_routing_nodes("seg_third", vec!["node-third".to_string()]);

        assert!(manifest.hierarchical_routing_nodes("seg_first").is_empty());
        assert_eq!(
            manifest.hierarchical_routing_nodes("seg_second"),
            ["node-second".to_string()]
        );
        manifest.remove_segment("seg_second");
        assert!(manifest.hierarchical_routing_nodes("seg_second").is_empty());
        assert_eq!(
            manifest.hierarchical_routing_nodes("seg_third"),
            ["node-third".to_string()]
        );
    }

    #[test]
    fn pending_deletes_must_be_target_local() {
        let mut manifest = Manifest::new();
        manifest.pending_deletes = vec!["target/wal/fragment".to_string()];
        manifest
            .validate_pending_deletes_are_local("target")
            .unwrap();
        manifest
            .pending_deletes
            .push("target/meta.json".to_string());
        assert!(manifest
            .validate_pending_deletes_are_local("target")
            .is_err());
        manifest.pending_deletes.pop();
        manifest
            .pending_deletes
            .push("source/segment/object".to_string());
        assert!(manifest
            .validate_pending_deletes_are_local("target")
            .is_err());
        let bytes = manifest.to_bytes().unwrap();
        assert!(Manifest::from_bytes_for_namespace(&bytes, "target").is_err());
    }
}
