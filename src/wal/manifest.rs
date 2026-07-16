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

use std::collections::HashSet;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::ops::Range;
use ulid::Ulid;

use crate::error::{Result, ZeppelinError};
use crate::storage::store::DELETE_MANY_MAX_KEYS;
use crate::storage::{CreateOnlyOutcome, ListedObject, StorageVersion, ZeppelinStore};

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
    /// NOTE: this field must stay LAST in the struct. MessagePack encodes
    /// structs as arrays, so old manifests decode only if new fields are
    /// trailing and `#[serde(default)]`.
    #[serde(default)]
    pub membership: Option<MembershipRef>,
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
    /// This field remains last because MessagePack encodes structs as
    /// positional arrays. A new query-relevant manifest field requires a new
    /// binding version rather than changing the v1 projection in place.
    #[serde(default)]
    receipt_binding_version: Option<ReceiptBindingVersion>,
}

/// Stable manifest execution projection version used by signed receipts.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptBindingVersion {
    /// Original field-by-field query-routing projection.
    V1,
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
    /// Decoded manifests kept by count, time window, or named pin, in ascending
    /// generation order.
    pub retained_manifests: Vec<Manifest>,
}

/// Union-of-rules policy controlling manifest-history retention.
///
/// A generation is retained when *any* configured rule keeps it: recent count,
/// PITR age window (including skew slop), or a [`NamedSnapshot`] pin.
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
) -> Result<Vec<u8>> {
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
    .map_err(|error| ZeppelinError::Serialization(format!("manifest root signing failed: {error}")))
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
        let reachable = self.receipt_reachable_keys(namespace);
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
        for segment in self.segments.clone() {
            if segment.hierarchical && self.hierarchical_routing_nodes(&segment.id).is_empty() {
                let node_ids =
                    crate::index::hierarchical::build::discover_hierarchical_routing_nodes(
                        store,
                        namespace,
                        &segment.id,
                    )
                    .await?;
                if node_ids.is_empty() {
                    return Err(ZeppelinError::Index(format!(
                        "hierarchical segment {} has no routing-node inventory",
                        segment.id
                    )));
                }
                self.set_hierarchical_routing_nodes(&segment.id, node_ids);
            }
        }
        let reachable = self.receipt_reachable_keys(namespace);
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
        self.receipt_binding_version = None;
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

    /// Recompute the domain-separated query-routing projection digest.
    pub(crate) fn recompute_receipt_state_digest(&self, namespace: &str) -> Result<[u8; 32]> {
        let binding_version = self.receipt_binding_version.ok_or_else(|| {
            ZeppelinError::Serialization(
                "manifest receipt binding version is unavailable".to_string(),
            )
        })?;
        self.compute_receipt_state_digest(namespace, binding_version)
    }

    fn compute_receipt_state_digest(
        &self,
        namespace: &str,
        binding_version: ReceiptBindingVersion,
    ) -> Result<[u8; 32]> {
        self.validate_namespace_binding(namespace)?;
        let bytes = match binding_version {
            ReceiptBindingVersion::V1 => serde_json::to_vec(&self.execution_binding_v1(namespace)),
        };
        let bytes = bytes.map_err(|error| {
            ZeppelinError::Serialization(format!(
                "manifest execution binding serialization failed: {error}"
            ))
        })?;
        Ok(Sha256::digest(bytes).into())
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

    fn receipt_reachable_keys(&self, namespace: &str) -> std::collections::BTreeSet<String> {
        let mut reachable = crate::compaction::gc::reachable_keys(namespace, self);
        for segment in &self.segments {
            if segment.hierarchical {
                for node_id in self.hierarchical_routing_nodes(&segment.id) {
                    reachable.insert(crate::index::hierarchical::tree_node_key(
                        namespace,
                        &segment.id,
                        node_id,
                    ));
                }
            }
            if segment.has_global_fts {
                for cluster_idx in 0..segment.cluster_count {
                    reachable.remove(&crate::fts::inverted_index::fts_index_key(
                        namespace,
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
                    namespace,
                    &segment.id,
                ));
                for cluster_idx in 0..segment.cluster_count {
                    reachable.remove(&crate::index::quantization::sq::sq_cluster_key(
                        namespace,
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
                    namespace,
                    &segment.id,
                ));
            }
            for cluster_idx in 0..segment.cluster_count {
                let owner = segment.cluster_owner(cluster_idx);
                let codes_are_colocated = !segment.cluster_objects.is_empty()
                    || (segment.bootstrap.is_some() && owner == segment.id);
                if codes_are_colocated {
                    reachable.remove(&crate::index::quantization::sq::sq_cluster_key(
                        namespace,
                        owner,
                        cluster_idx,
                    ));
                }
            }
        }
        for pending in &self.pending_deletes {
            reachable.remove(pending);
        }
        reachable
    }

    fn finalize_receipt_root(&mut self, store: &ZeppelinStore, namespace: &str) -> Result<()> {
        let reachable = self.receipt_reachable_keys(namespace);
        self.artifact_hashes
            .retain(|key, _| reachable.contains(key));
        for key in &reachable {
            if !self.artifact_hashes.contains_key(key) {
                if let Some(content_hash) = store.known_content_hash(key) {
                    self.artifact_hashes.insert(key.clone(), content_hash);
                }
            }
        }

        if reachable
            .iter()
            .any(|key| !self.artifact_hashes.contains_key(key))
        {
            self.merkle_root = None;
            self.root_signature = None;
            self.root_signer_node = None;
            self.receipt_state_digest = None;
            self.receipt_binding_version = None;
            return Ok(());
        }

        let root = crate::security::MerkleTree::build(&self.artifact_hashes)?.root();
        let binding_version = ReceiptBindingVersion::V1;
        let state_digest = self.compute_receipt_state_digest(namespace, binding_version)?;
        self.merkle_root = Some(root);
        self.receipt_state_digest = Some(state_digest);
        self.receipt_binding_version = Some(binding_version);
        let payload = manifest_root_signing_bytes(
            root,
            self.version,
            self.fencing_token,
            binding_version,
            state_digest,
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
        self.deletion_fence = None;
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
        target_base.validate_namespace_binding(target_namespace)?;
        if target_base.namespace_incarnation() != Some(target_incarnation)
            || !target_base.fragments.is_empty()
            || !target_base.segments.is_empty()
            || target_base.active_segment.is_some()
            || !target_base.pending_deletes.is_empty()
            || target_base.deletion_fence.is_some()
        {
            return Err(ZeppelinError::ManifestConflict {
                namespace: target_namespace.to_string(),
            });
        }

        self.version = target_base.version;
        self.namespace = Some(target_namespace.to_string());
        self.bind_namespace_incarnation(target_incarnation)
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
        Ok(manifest)
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
    /// predecessor-history, live PUT, or concurrent-publication conflict. A
    /// failed live PUT cannot reserve the speculative generation;
    /// `self.version` remains unchanged and a divergent retry stays possible.
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
                let etag = version.require_etag(namespace, "manifest recovery write")?;
                store
                    .put_if_match(&key, data.clone(), etag, namespace)
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
            Ok((data, etag)) => {
                let manifest = Self::from_bytes_for_namespace(&data, namespace)?;
                let version = ManifestVersion::for_manifest(etag, &manifest, data, false);
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
        let (data, etag) = store.get_with_meta(&key).await?;
        let manifest = Self::from_bytes_for_namespace(&data, namespace)?;
        let version = ManifestVersion::for_manifest(etag, &manifest, data, false);
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
            version.require_etag(namespace, "incarnation-bound manifest read")?;
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

    async fn write_conditional_candidate(
        &mut self,
        store: &ZeppelinStore,
        namespace: &str,
        version: &ManifestVersion,
    ) -> Result<ManifestVersion> {
        let key = Self::s3_key(namespace);
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
        let new_etag = match &version.e_tag {
            Some(etag) => {
                store
                    .put_if_match(&key, data.clone(), etag, namespace)
                    .await?
            }
            None => match store.put_create_outcome(&key, data.clone()).await? {
                CreateOnlyOutcome::Created { e_tag } => e_tag,
                CreateOnlyOutcome::AlreadyExists => {
                    return Err(ZeppelinError::ManifestConflict {
                        namespace: namespace.to_string(),
                    });
                }
            },
        };
        Self::write_immutable_history_snapshot(store, namespace, committed.version(), data.clone())
            .await?;
        let new_version = ManifestVersion::for_manifest(new_etag, &committed, data, true);
        store.forget_known_content_hashes(committed.artifact_hashes.keys());
        *self = committed;
        Ok(new_version)
    }

    /// CAS-publish the governed-destruction fence and return its exact manifest.
    pub(crate) async fn fence_for_destruction(
        store: &ZeppelinStore,
        namespace: &str,
        destruction_record_key: &str,
    ) -> Result<Self> {
        const MAX_FENCE_ATTEMPTS: usize = 8;
        validate_destruction_record_key(destruction_record_key)?;
        for _ in 0..MAX_FENCE_ATTEMPTS {
            let (mut manifest, version) = Self::read_versioned_required(store, namespace).await?;
            version.require_etag(namespace, "governed destruction fence")?;
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
    /// unless a named snapshot pin protects either generation.
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

    /// Prunes history while retaining the union of count, time, and named-pin rules.
    ///
    /// ```text
    /// history generation
    ///       |
    ///       +-- among newest keep_count? -------- keep
    ///       +-- inside PITR window + skew? ------ keep
    ///       +-- named snapshot pins it? --------- keep
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
    /// Lists history and named pins, GETs every history manifest, and DELETEs
    /// generations kept by no rule. It does not modify the live manifest.
    ///
    /// # Consistency
    ///
    /// Retention is an OR, not an AND. `skew_slop_secs` extends only an enabled
    /// PITR time window. Named pins are read before pruning so a generation
    /// observed as pinned in this pass is not deleted. The pin LIST and history
    /// DELETEs are not one object-store transaction; a pin created concurrently
    /// after the LIST can race this pass, so higher layers must serialize those
    /// operations when they require a stronger creation-versus-prune guarantee.
    ///
    /// # Performance
    ///
    /// Performs one history LIST, one snapshot LIST plus a GET per pin, one GET
    /// per history entry, and at most one DELETE request per 1,000 pruned
    /// generations.
    ///
    /// # Examples
    ///
    /// If generation 2 is pinned, generation 3 is within the time window, and
    /// generation 5 is the newest count-retained value, all three survive while
    /// unprotected generations 1 and 4 are deleted.
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
        let history = Self::list_history(store, namespace).await?;
        let keep_from = history.len().saturating_sub(retention.keep_count);
        let pinned_generations = NamedSnapshot::pinned_generations(store, namespace).await?;
        let mut retained_manifests = Vec::new();
        let mut prunable = Vec::new();
        for (index, entry) in history.iter().enumerate() {
            let manifest = Self::read_history(store, namespace, entry.version)
                .await?
                .ok_or_else(|| ZeppelinError::NotFound {
                    key: entry.key.clone(),
                })?;
            let keep_by_count = index >= keep_from;
            let keep_by_pin = pinned_generations.contains(&entry.version);
            let retention_window = retention
                .pitr_retention_secs
                .saturating_add(retention.skew_slop_secs);
            let keep_by_time = retention.pitr_retention_secs > 0
                && now.signed_duration_since(manifest.updated_at).num_seconds()
                    <= retention_window as i64;
            if keep_by_count || keep_by_time || keep_by_pin {
                retained_manifests.push(manifest);
            } else {
                prunable.push(entry.key.clone());
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
        let (data, get_etag) = store.get_with_meta(&object.key).await?;
        if let Some(StorageVersion::Etag(list_etag)) = object.version.as_ref() {
            if get_etag.as_deref() != Some(list_etag.as_str()) {
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
    /// Backend-provided ETag, or `None` only when no conditional version exists.
    e_tag: Option<String>,
    /// Whether the same authoritative read observed a governed-deletion fence.
    deletion_fenced: bool,
    /// Exact live bytes paired with this ETag for predecessor-history repair.
    history_snapshot: Option<Bytes>,
    /// Whether this process observed matching history publication succeed.
    history_confirmed: bool,
}

impl ManifestVersion {
    pub(crate) fn for_manifest(
        e_tag: Option<String>,
        manifest: &Manifest,
        history_snapshot: Bytes,
        history_confirmed: bool,
    ) -> Self {
        Self {
            e_tag,
            deletion_fenced: manifest.deletion_fence.is_some(),
            history_snapshot: Some(history_snapshot),
            history_confirmed,
        }
    }

    pub(crate) fn unversioned() -> Self {
        Self {
            e_tag: None,
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

    /// Borrow the backend ETag carried by this observation, when available.
    #[must_use]
    pub fn e_tag(&self) -> Option<&str> {
        self.e_tag.as_deref()
    }

    pub(crate) fn into_e_tag(self) -> Option<String> {
        self.e_tag
    }

    pub(crate) fn has_e_tag(&self) -> bool {
        self.e_tag.is_some()
    }

    /// Returns the backend version required to replace an existing manifest.
    ///
    /// A missing or empty ETag is never converted into unconditional write
    /// authority. Callers that derive a mutation from an existing live object
    /// must stop before uploading history or replacing the live manifest.
    pub(crate) fn require_etag(&self, namespace: &str, operation: &str) -> Result<&str> {
        self.e_tag
            .as_deref()
            .filter(|etag| !etag.is_empty())
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

    #[test]
    fn conditional_manifest_versions_reject_missing_or_empty_etags() {
        for version in [
            ManifestVersion::unversioned(),
            ManifestVersion {
                e_tag: Some(String::new()),
                deletion_fenced: false,
                history_snapshot: None,
                history_confirmed: false,
            },
        ] {
            let error = version
                .require_etag("catalog", "legacy manifest incarnation migration")
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
        }
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
            },
            FragmentRef {
                id: Ulid::from(2_u128),
                vector_count: 1,
                delete_count: 0,
                sequence_number: 1,
                size_bytes: 11,
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
                version: Some(StorageVersion::BackendVersion("v10".to_string())),
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
                version: Some(StorageVersion::Etag("etag-3".to_string())),
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
            Some(StorageVersion::Etag("etag-3".to_string()))
        );
        assert_eq!(
            observations[2].storage_version,
            Some(StorageVersion::BackendVersion("v10".to_string()))
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
                version: Some(StorageVersion::BackendVersion("weekly-v1".to_string())),
            },
            ListedObject {
                key: NamedSnapshot::key(namespace, "daily").unwrap(),
                size: 29,
                last_modified: now,
                version: Some(StorageVersion::Etag("daily-etag".to_string())),
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
            Some(StorageVersion::Etag("daily-etag".to_string()))
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
            version: Some(StorageVersion::Etag("stale-list-etag".to_string())),
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
        });
        manifest.remove_compacted_fragments(&[newer].into_iter().collect());
        assert_eq!(manifest.compaction_watermark, Some(newer));

        manifest.add_fragment(FragmentRef {
            id: older,
            vector_count: 1,
            delete_count: 0,
            sequence_number: 0,
            size_bytes: 0,
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
        });
        manifest.add_fragment(FragmentRef {
            id: Ulid::from_parts(3000, 2),
            vector_count: 5,
            delete_count: 1,
            sequence_number: 0,
            size_bytes: 222,
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
}
