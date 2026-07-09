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
//! write history candidate (immutable once referenced)
//!                  |
//!                  v
//! publish live manifest.json -------- live PUT fails
//!                  |                       |
//!                  | success               v
//!                  v                 history may be orphaned;
//! readers discover new artifacts      a retry may replace it
//! ```
//!
//! History is written first so a successful live publication always has an
//! addressable generation. A failure of the final live PUT can leave an
//! unreferenced history object. The retry logic distinguishes such an orphan
//! from history already referenced by an equal-or-newer live manifest.
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
//! [`Manifest::write`][crate::wal::manifest::Manifest::write] is deliberately
//! unconditional and is used for bootstrap, cloning, and controlled setup
//! paths; it must not be mistaken for a stale-writer defense. A missing ETag in
//! [`ManifestVersion`][crate::wal::manifest::ManifestVersion] likewise selects
//! an unconditional first write, so update paths must not manufacture
//! `ManifestVersion(None)` after a namespace has existed.
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
use std::ops::Range;
use ulid::Ulid;

use crate::error::{Result, ZeppelinError};
use crate::storage::ZeppelinStore;

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
    /// the next generation. Keep this field last for MessagePack array
    /// decode compatibility with older manifests.
    #[serde(default)]
    version: u64,
}

/// Location of one immutable, addressable historical manifest generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestHistoryRef {
    /// Persisted manifest generation.
    pub version: u64,
    /// Immutable S3 key containing the serialized manifest snapshot.
    pub key: String,
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

/// Error family used when conflicting history is already live and immutable.
enum ReferencedHistoryConflict {
    /// Surface a persisted-format/invariant error to unconditional writers.
    Serialization,
    /// Surface an optimistic-concurrency conflict to conditional writers.
    ManifestConflict,
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
        Self {
            fragments: Vec::new(),
            segments: Vec::new(),
            compaction_watermark: None,
            active_segment: None,
            next_sequence: 0,
            pending_deletes: Vec::new(),
            fencing_token: 0,
            updated_at: Utc::now(),
            version: 0,
        }
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
    pub(crate) fn reset_version_for_clone(&mut self) {
        self.version = 0;
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
    pub fn add_fragment(&mut self, mut fref: FragmentRef) {
        fref.sequence_number = self.next_sequence;
        self.next_sequence += 1;
        self.fragments.push(fref);
        self.updated_at = Utc::now();
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
        self.fragments.retain(|f| !compacted_ids.contains(&f.id));
        if let Some(max_id) = compacted_ids.iter().max() {
            let watermark = match self.compaction_watermark {
                Some(prev) => prev.max(*max_id),
                None => *max_id,
            };
            self.compaction_watermark = Some(watermark);
        }
        self.updated_at = Utc::now();
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
        self.active_segment = Some(sref.id.clone());
        self.segments.push(sref);
        self.updated_at = Utc::now();
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
        if self.active_segment.as_deref() == Some(segment_id) {
            self.active_segment = None;
        }
        self.segments.retain(|segment| segment.id != segment_id);
        self.updated_at = Utc::now();
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
            Ok(data) => Ok(Some(Self::from_bytes(&data)?)),
            Err(crate::error::ZeppelinError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Publishes this candidate with an unconditional live-manifest PUT.
    ///
    /// The method first reads the current live generation, chooses one greater
    /// than both that value and `self.version`, writes the corresponding history
    /// snapshot, and finally writes the live object. Use
    /// [`Manifest::write_conditional`] for normal updates that must reject stale
    /// writers.
    ///
    /// # Parameters
    ///
    /// - `store`: Borrowed storage abstraction for the read and writes.
    /// - `namespace`: Destination namespace. Internal clone paths reset the
    ///   candidate generation before calling this method.
    ///
    /// # Returns
    ///
    /// `Ok(())` after both history and live objects are written. Only then is
    /// `self.version` advanced to the committed generation.
    ///
    /// # Errors
    ///
    /// Returns on read, generation overflow, serialization, history, or live PUT
    /// failure. A history object can already exist if the final live PUT fails;
    /// `self.version` remains unchanged so a retry can reconcile that orphan.
    ///
    /// # Side Effects
    ///
    /// Performs one live-manifest GET, at least one history operation, and one
    /// unconditional live-manifest PUT on the success path.
    ///
    /// # Consistency
    ///
    /// This is not compare-and-swap. Concurrent callers can overwrite each
    /// other's content even though the generation is advanced. Production
    /// mutation paths should pair a fresh ETag with
    /// [`Manifest::write_conditional`].
    ///
    /// # Examples
    ///
    /// Namespace bootstrap writes an empty generation 1. If its history write
    /// fails, the live manifest is untouched. If the later live PUT fails,
    /// history generation 1 may exist without being authoritative.
    pub async fn write(&mut self, store: &ZeppelinStore, namespace: &str) -> Result<()> {
        let key = Self::s3_key(namespace);
        let current_version = Self::read(store, namespace)
            .await?
            .map_or(0, |manifest| manifest.version());
        let base_version = self.version.max(current_version);
        let mut committed = self.clone();
        committed.version = Self::checked_next_version(base_version)?;
        let data = committed.to_bytes()?;
        Self::write_history_snapshot_for_commit(
            store,
            namespace,
            &committed,
            data.clone(),
            ReferencedHistoryConflict::Serialization,
        )
        .await?;
        store.put(&key, data).await?;
        self.version = committed.version;
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
    /// `Some((manifest, ManifestVersion(etag)))` when present, including a
    /// possibly absent ETag as reported by the backend, or `None` for not found.
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
                let manifest = Self::from_bytes(&data)?;
                Ok(Some((manifest, ManifestVersion(etag))))
            }
            Err(crate::error::ZeppelinError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Publishes the next generation using ETag compare-and-swap when available.
    ///
    /// ```text
    /// candidate version N
    ///         |
    ///         v
    /// create history N+1
    ///         |
    ///         v
    /// PUT live manifest if ETag matches ---- mismatch
    ///         |                                |
    ///         v                                v
    /// candidate becomes N+1            reload authoritative state
    /// ```
    ///
    /// # Parameters
    ///
    /// - `store`: Borrowed storage abstraction configured for conditional PUT.
    /// - `namespace`: Namespace whose live manifest is being replaced.
    /// - `version`: ETag returned with the base manifest. `None` selects an
    ///   unconditional first-write path and must not be used to resurrect a
    ///   deleted namespace.
    ///
    /// # Returns
    ///
    /// `Ok(())` after the live manifest is authoritative; `self.version` then
    /// advances by exactly one.
    ///
    /// # Errors
    ///
    /// Returns on generation overflow, serialization, history I/O, live PUT, or
    /// ETag conflict. A failure after history creation can leave an orphaned
    /// generation, but does not advance `self.version`. Uploaded data artifacts
    /// referenced only by this candidate also remain invisible.
    ///
    /// # Side Effects
    ///
    /// Creates or reconciles the immutable history object before attempting one
    /// conditional live PUT (or an unconditional PUT when the ETag is absent).
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
    ) -> Result<()> {
        let key = Self::s3_key(namespace);
        let next_version = self.next_committed_version()?;
        let mut committed = self.clone();
        committed.version = next_version;
        let data = committed.to_bytes()?;
        Self::write_history_snapshot_for_commit(
            store,
            namespace,
            &committed,
            data.clone(),
            ReferencedHistoryConflict::ManifestConflict,
        )
        .await?;
        match &version.0 {
            Some(etag) => store.put_if_match(&key, data, etag, namespace).await,
            None => store.put(&key, data).await,
        }?;
        self.version = next_version;
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
    /// history objects are retained.
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
        let prefix = Self::history_prefix(namespace);
        let mut entries = store
            .list_prefix(&prefix)
            .await?
            .into_iter()
            .map(|key| {
                Ok(ManifestHistoryRef {
                    version: Self::history_version_from_key(namespace, &key)?,
                    key,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        entries.sort_by_key(|entry| entry.version);
        Ok(entries)
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
    /// `Some(manifest)` when retained or `None` when that generation key is
    /// absent.
    ///
    /// # Errors
    ///
    /// Propagates storage and decoding failures. It also returns a serialization
    /// error if the manifest payload's generation differs from the key, because
    /// accepting that mismatch would make PITR address the wrong state.
    ///
    /// # Consistency
    ///
    /// History objects are immutable once referenced by the live manifest.
    /// Reading history does not make it the namespace's current live state.
    ///
    /// # Examples
    ///
    /// Reading generation 4 may return a view with fewer fragments than the live
    /// generation 9. A missing generation returns `None`, never the nearest one.
    pub async fn read_history(
        store: &ZeppelinStore,
        namespace: &str,
        version: u64,
    ) -> Result<Option<Self>> {
        let key = Self::history_key(namespace, version);
        match store.get(&key).await {
            Ok(data) => {
                let manifest = Self::from_bytes(&data)?;
                if manifest.version() != version {
                    return Err(ZeppelinError::Serialization(format!(
                        "manifest history key {key} contains version {}, expected {version}",
                        manifest.version()
                    )));
                }
                Ok(Some(manifest))
            }
            Err(crate::error::ZeppelinError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
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
    /// failures from [`Manifest::prune_history_with_retention`]. Earlier deletes
    /// may already have succeeded if a later delete fails.
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
    /// failures. Deletion is sequential and not transactional: earlier old
    /// generations may already be gone when a later operation fails.
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
    /// per history entry, and one DELETE per pruned generation.
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
        if retention.keep_count == 0 {
            return Err(ZeppelinError::Config(
                "gc.manifest_history_keep_count must be greater than zero".to_string(),
            ));
        }
        let history = Self::list_history(store, namespace).await?;
        let keep_from = history.len().saturating_sub(retention.keep_count);
        let pinned_generations = NamedSnapshot::pinned_generations(store, namespace).await?;
        let now = Utc::now();

        let mut retained_manifests = Vec::new();
        let mut pruned = 0usize;
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
                store.delete(&entry.key).await?;
                pruned += 1;
            }
        }
        Ok(ManifestHistoryPruneResult {
            pruned,
            retained_manifests,
        })
    }

    /// Ensures the candidate history object is safe to pair with a live commit.
    ///
    /// A unique or byte-identical history object is accepted. Different bytes
    /// at the same generation are immutable if the live manifest already
    /// references that generation or a newer one. Otherwise the object is an
    /// orphan from a failed live PUT and can be replaced by this retry.
    ///
    /// # Parameters
    ///
    /// - `store`: Borrowed storage abstraction.
    /// - `namespace`: Namespace receiving the candidate generation.
    /// - `committed`: Borrowed candidate with its nonzero generation assigned.
    /// - `data`: Owned encoding of `committed`, reused when replacing an orphan.
    /// - `referenced_conflict`: Selects the public error appropriate to the
    ///   unconditional or conditional caller.
    ///
    /// # Errors
    ///
    /// Propagates storage and serialization failures. Conflicting referenced
    /// history becomes either a serialization invariant error or a manifest CAS
    /// conflict as selected by the caller.
    ///
    /// # Side Effects
    ///
    /// May create a history object, read the live manifest, or overwrite an
    /// orphaned history object. It does not publish the live manifest.
    ///
    /// # Consistency
    ///
    /// A history object reachable from the live manifest is never overwritten.
    ///
    /// # Examples
    ///
    /// If generation 8 history exists after generation 7's live PUT failed, a
    /// retry based on live generation 7 may replace it. Once live generation 8
    /// exists, different bytes for history 8 are rejected.
    async fn write_history_snapshot_for_commit(
        store: &ZeppelinStore,
        namespace: &str,
        committed: &Self,
        data: Bytes,
        referenced_conflict: ReferencedHistoryConflict,
    ) -> Result<()> {
        match Self::try_write_history_snapshot(store, namespace, committed).await? {
            HistorySnapshotWrite::Stored => Ok(()),
            HistorySnapshotWrite::AlreadyExistsWithDifferentBytes { key } => {
                let live_version = Self::read(store, namespace)
                    .await?
                    .map_or(0, |live| live.version());
                if live_version >= committed.version() {
                    return match referenced_conflict {
                        ReferencedHistoryConflict::Serialization => {
                            Err(Self::history_snapshot_mismatch_error(&key))
                        }
                        ReferencedHistoryConflict::ManifestConflict => {
                            Err(ZeppelinError::ManifestConflict {
                                namespace: namespace.to_string(),
                            })
                        }
                    };
                }
                store.put(&key, data).await
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
        committed: &Self,
    ) -> Result<HistorySnapshotWrite> {
        let version = committed.version();
        if version == 0 {
            return Err(ZeppelinError::Serialization(
                "manifest history requires a committed nonzero version".to_string(),
            ));
        }

        let key = Self::history_key(namespace, version);
        let data = committed.to_bytes()?;
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
        if generation == 0 {
            return Err(ZeppelinError::Validation(
                "snapshot generation must be a committed nonzero manifest generation".into(),
            ));
        }
        let key = Self::key(namespace, name)?;
        if Manifest::read_history(store, namespace, generation)
            .await?
            .is_none()
        {
            return Err(ZeppelinError::Validation(format!(
                "snapshot generation {generation} is not retained for namespace {namespace}"
            )));
        }
        let snapshot = Self {
            generation,
            created_at: Utc::now(),
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
        let prefix = Self::prefix(namespace);
        let mut snapshots = Vec::new();
        for key in store.list_prefix(&prefix).await? {
            let name = snapshot_name_from_key(namespace, &key)?;
            let data = store.get(&key).await?;
            let snapshot = Self::from_bytes(&data)?;
            snapshots.push(NamedSnapshotRef {
                name,
                key,
                generation: snapshot.generation,
                created_at: snapshot.created_at,
            });
        }
        snapshots.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(snapshots)
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
    async fn pinned_generations(store: &ZeppelinStore, namespace: &str) -> Result<HashSet<u64>> {
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
/// exact object read by [`Manifest::read_versioned`]. `None` selects an
/// unconditional first write and is unsafe as a fabricated fallback for a
/// missing manifest in an existing namespace.
///
/// # Rust Notes for Java/C Engineers
///
/// This tuple newtype gives an optional string a domain name, so APIs cannot as
/// easily confuse an ETag with an arbitrary `Option<String>`. Java would often
/// use a small wrapper class; C would use a struct plus a presence flag. Rust's
/// [`Option`] encodes absence without a null `String`.
#[derive(Debug, Clone)]
pub struct ManifestVersion(
    /// Backend-provided ETag, or `None` only when no conditional version exists.
    pub Option<String>,
);

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
}
