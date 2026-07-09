use std::collections::HashSet;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::ops::Range;
use ulid::Ulid;

use crate::error::{Result, ZeppelinError};
use crate::storage::ZeppelinStore;

/// Version byte for manifest format detection.
const MANIFEST_FORMAT_MSGPACK: u8 = 0x01;

/// A reference to a WAL fragment stored on S3.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FragmentRef {
    /// ULID identifying this fragment.
    pub id: Ulid,
    /// Number of vectors in the fragment.
    pub vector_count: usize,
    /// Number of delete tombstones in the fragment.
    pub delete_count: usize,
    /// Monotonic sequence number assigned at manifest write time.
    /// Immune to clock skew — determines merge order instead of ULID.
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

/// A reference to a resident coarse sketch artifact stored on S3.
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

/// A reference to an immutable segment bootstrap artifact stored on S3.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BootstrapRef {
    /// S3 key for the immutable bootstrap artifact.
    pub key: String,
    /// Serialized artifact size in bytes.
    pub size_bytes: u64,
}

/// A reference to an immutable IVF-Flat segment membership artifact.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MembershipRef {
    /// S3 key for the immutable membership artifact.
    pub key: String,
    /// Serialized artifact size in bytes.
    pub size_bytes: u64,
    /// Number of vector-id entries in the artifact.
    pub entry_count: u64,
}

/// A manifest reference to one immutable object containing one or more IVF
/// cluster payloads.
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
    /// Ranged GET span for the flat-scan live bytes, when this object
    /// advertises one.
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

/// A reference to an IVF segment stored on S3.
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
    /// Segment ID that owns cluster `cluster_idx`'s S3 objects.
    ///
    /// Returns the entry in `cluster_owners` when present (incremental
    /// carry-over), otherwise falls back to `self.id` (legacy full-rewrite
    /// layout, and any cluster written by this compaction). Every reader of
    /// a per-cluster S3 key MUST resolve the owner through this method
    /// rather than assuming `self.id` — carried-over clusters live under an
    /// older segment's keys.
    #[must_use]
    pub fn cluster_owner(&self, cluster_idx: usize) -> &str {
        self.cluster_owners
            .get(cluster_idx)
            .map(String::as_str)
            .unwrap_or(&self.id)
    }

    /// Approximate bytes for segment artifacts whose sizes are recorded in the manifest.
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

/// The manifest is the single source of truth for what data exists
/// in a namespace. It tracks WAL fragments and compacted segments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Uncompacted WAL fragment references, in order.
    pub fragments: Vec<FragmentRef>,
    /// Compacted segment references.
    pub segments: Vec<SegmentRef>,
    /// ULID of the last fragment that was compacted.
    /// Fragments with IDs <= this have been incorporated into segments.
    #[serde(default)]
    pub compaction_watermark: Option<Ulid>,
    /// The currently active segment (latest).
    #[serde(default)]
    pub active_segment: Option<String>,
    /// Monotonic counter for assigning sequence numbers to fragments.
    #[serde(default)]
    pub next_sequence: u64,
    /// S3 keys awaiting deferred deletion from a previous compaction cycle.
    #[serde(default)]
    pub pending_deletes: Vec<String>,
    /// Fencing token set by the lease holder during manifest writes.
    /// Prevents zombie writers (expired lease holders) from overwriting
    /// a manifest that a newer lease holder has already written.
    #[serde(default)]
    pub fencing_token: u64,
    /// Last time the manifest was updated.
    pub updated_at: DateTime<Utc>,
    /// Monotonic manifest generation persisted with each manifest commit.
    ///
    /// Legacy manifests decode as `0`; each successful manifest write stores
    /// the next generation. Keep this field last for MessagePack array
    /// decode compatibility with older manifests.
    #[serde(default)]
    version: u64,
}

/// Addressable historical manifest snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestHistoryRef {
    /// Persisted manifest generation.
    pub version: u64,
    /// Immutable S3 key containing the serialized manifest snapshot.
    pub key: String,
}

/// Named PITR snapshot pin stored under `{namespace}/snapshots/{name}.msgpack`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamedSnapshot {
    /// Manifest generation pinned by this snapshot.
    pub generation: u64,
    /// Snapshot creation timestamp.
    pub created_at: DateTime<Utc>,
}

/// Addressable named snapshot pin.
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

/// Result of pruning manifest history.
#[derive(Debug, Clone)]
pub struct ManifestHistoryPruneResult {
    /// Number of history snapshots deleted.
    pub pruned: usize,
    /// Retained manifest snapshots after pruning.
    pub retained_manifests: Vec<Manifest>,
}

/// Policy controlling manifest-history retention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestHistoryRetention {
    /// Most recent generation count to retain.
    pub keep_count: usize,
    /// Time-based PITR retention window in seconds. `0` disables time retention.
    pub pitr_retention_secs: u64,
    /// Additional seconds allowed for writer/read-side clock skew.
    pub skew_slop_secs: u64,
}

enum HistorySnapshotWrite {
    Stored,
    AlreadyExistsWithDifferentBytes { key: String },
}

enum ReferencedHistoryConflict {
    Serialization,
    ManifestConflict,
}

impl Manifest {
    /// Create an empty manifest.
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

    /// Get the S3 key for the manifest of a namespace.
    pub fn s3_key(namespace: &str) -> String {
        format!("{namespace}/manifest.json")
    }

    /// Get the S3 prefix for immutable manifest history snapshots.
    #[must_use]
    pub fn history_prefix(namespace: &str) -> String {
        format!("{namespace}/manifests/")
    }

    /// Get the S3 key for a retained manifest generation.
    #[must_use]
    pub fn history_key(namespace: &str, version: u64) -> String {
        format!("{}{version:020}.msgpack", Self::history_prefix(namespace))
    }

    /// Return the persisted manifest generation.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Reset the persisted generation before writing this manifest into a
    /// different namespace. The target namespace assigns its own generation
    /// during `write`; source history generations must not leak across clones.
    pub(crate) fn reset_version_for_clone(&mut self) {
        self.version = 0;
    }

    fn checked_next_version(version: u64) -> Result<u64> {
        version
            .checked_add(1)
            .ok_or_else(|| ZeppelinError::Serialization("manifest version overflow".to_string()))
    }

    fn next_committed_version(&self) -> Result<u64> {
        Self::checked_next_version(self.version)
    }

    /// Add a fragment reference, assigning the next monotonic sequence number.
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
    /// NOTE: `max_pending_deletes` is currently UNUSED — `pending_deletes` is
    /// deliberately not capped (see `prune()`). The parameter is retained for
    /// call-site compatibility; capping it would leak S3 objects.
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

    /// Add a segment reference and prune with default limits.
    pub fn add_segment(&mut self, sref: SegmentRef) {
        self.add_segment_with_limits(sref, 1000, 10);
    }

    /// Remove a segment reference and clear it as active if it was serving reads.
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

    /// Get uncompacted fragments (those after the compaction watermark).
    pub fn uncompacted_fragments(&self) -> &[FragmentRef] {
        &self.fragments
    }

    /// Total vector count across all segments.
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

    /// Serialize to MessagePack bytes with a version header.
    ///
    /// Format: `[0x01] [msgpack payload]`
    /// Falls back to JSON for human readability during debugging if needed.
    pub fn to_bytes(&self) -> Result<Bytes> {
        let msgpack = rmp_serde::to_vec(self).map_err(|e| {
            ZeppelinError::Serialization(format!("manifest msgpack serialize: {e}"))
        })?;
        let mut data = Vec::with_capacity(1 + msgpack.len());
        data.push(MANIFEST_FORMAT_MSGPACK);
        data.extend_from_slice(&msgpack);
        Ok(Bytes::from(data))
    }

    /// Deserialize from bytes, auto-detecting format (MessagePack or legacy JSON).
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

    /// Read manifest from S3. Returns None if not found.
    pub async fn read(store: &ZeppelinStore, namespace: &str) -> Result<Option<Self>> {
        let key = Self::s3_key(namespace);
        match store.get(&key).await {
            Ok(data) => Ok(Some(Self::from_bytes(&data)?)),
            Err(crate::error::ZeppelinError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Write manifest to S3.
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

    /// Read manifest from S3, returning the manifest along with its ETag version.
    /// Returns None if not found.
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

    /// Write manifest to S3 using conditional PUT (CAS).
    /// If version has an ETag, uses put_if_match for optimistic concurrency.
    /// For first-writes (no ETag), falls back to unconditional put.
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

    /// List retained manifest history snapshots in ascending generation order.
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

    /// Read a retained manifest history snapshot by persisted generation.
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

    /// Prune oldest manifest history snapshots, retaining the most recent `keep_count`.
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

    /// Prune manifest history by count OR PITR retention window OR named snapshot pins.
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

    fn history_snapshot_mismatch_error(key: &str) -> ZeppelinError {
        ZeppelinError::Serialization(format!(
            "manifest history key {key} already exists with different bytes"
        ))
    }

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
    /// Get the S3 prefix for named snapshot pins.
    #[must_use]
    pub fn prefix(namespace: &str) -> String {
        format!("{namespace}/snapshots/")
    }

    /// Get the S3 key for a named snapshot pin.
    pub fn key(namespace: &str, name: &str) -> Result<String> {
        validate_snapshot_name(name)?;
        Ok(format!("{}{}.msgpack", Self::prefix(namespace), name))
    }

    /// Serialize to MessagePack bytes with a version header.
    pub fn to_bytes(&self) -> Result<Bytes> {
        let msgpack = rmp_serde::to_vec(self).map_err(|e| {
            ZeppelinError::Serialization(format!("snapshot msgpack serialize: {e}"))
        })?;
        let mut data = Vec::with_capacity(1 + msgpack.len());
        data.push(MANIFEST_FORMAT_MSGPACK);
        data.extend_from_slice(&msgpack);
        Ok(Bytes::from(data))
    }

    /// Deserialize from bytes, auto-detecting format (MessagePack or legacy JSON).
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

    /// Create or idempotently confirm a named snapshot for `generation`.
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

    /// Read a named snapshot pin.
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

    /// List all named snapshot pins in name order.
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

    /// Delete a named snapshot pin.
    pub async fn delete(store: &ZeppelinStore, namespace: &str, name: &str) -> Result<()> {
        let key = Self::key(namespace, name)?;
        store.delete(&key).await
    }

    async fn pinned_generations(store: &ZeppelinStore, namespace: &str) -> Result<HashSet<u64>> {
        Ok(Self::list(store, namespace)
            .await?
            .into_iter()
            .map(|snapshot| snapshot.generation)
            .collect())
    }
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

/// Wraps the ETag for optimistic concurrency control on manifest writes.
#[derive(Debug, Clone)]
pub struct ManifestVersion(pub Option<String>);

impl Default for Manifest {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    use crate::storage::ZeppelinStore;

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
        #[derive(Serialize)]
        struct OldFragmentRef {
            id: Ulid,
            vector_count: usize,
            delete_count: usize,
            sequence_number: u64,
        }
        #[derive(Serialize)]
        struct OldManifest {
            fragments: Vec<OldFragmentRef>,
            segments: Vec<SegmentRef>,
            compaction_watermark: Option<Ulid>,
            active_segment: Option<String>,
            next_sequence: u64,
            pending_deletes: Vec<String>,
            fencing_token: u64,
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
        #[derive(Serialize)]
        struct OldSegmentRef {
            id: String,
            vector_count: usize,
            cluster_count: usize,
            quantization: crate::index::quantization::QuantizationType,
            hierarchical: bool,
            bitmap_fields: Vec<String>,
            fts_fields: Vec<String>,
            has_global_fts: bool,
        }
        #[derive(Serialize)]
        struct MixedManifest {
            fragments: Vec<FragmentRef>,
            segments: Vec<OldSegmentRef>,
            compaction_watermark: Option<Ulid>,
            active_segment: Option<String>,
            next_sequence: u64,
            pending_deletes: Vec<String>,
            fencing_token: u64,
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

    /// `cluster_owner()` returns carried-over owners where present and falls
    /// back to the segment's own ID for indices beyond the map.
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

    /// Round-trip: cluster_owners survives serialize → deserialize.
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

    #[test]
    fn test_cluster_data_object_live_span_defaults_and_roundtrip() {
        #[derive(Serialize)]
        struct OldClusterDataObjectRef {
            key: String,
            clusters: Vec<usize>,
        }
        #[derive(Serialize)]
        struct MixedSegmentRef {
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
            cluster_objects: Vec<OldClusterDataObjectRef>,
            bootstrap: Option<BootstrapRef>,
        }
        #[derive(Serialize)]
        struct MixedManifest {
            fragments: Vec<FragmentRef>,
            segments: Vec<MixedSegmentRef>,
            compaction_watermark: Option<Ulid>,
            active_segment: Option<String>,
            next_sequence: u64,
            pending_deletes: Vec<String>,
            fencing_token: u64,
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

    #[test]
    fn test_decode_cluster_ref_without_size_bytes_field() {
        #[derive(Serialize)]
        struct OldClusterDataObjectRef {
            key: String,
            clusters: Vec<usize>,
            live_offset: u64,
            live_len: u64,
        }
        #[derive(Serialize)]
        struct MixedSegmentRef {
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
            cluster_objects: Vec<OldClusterDataObjectRef>,
            bootstrap: Option<BootstrapRef>,
        }
        #[derive(Serialize)]
        struct MixedManifest {
            fragments: Vec<FragmentRef>,
            segments: Vec<MixedSegmentRef>,
            compaction_watermark: Option<Ulid>,
            active_segment: Option<String>,
            next_sequence: u64,
            pending_deletes: Vec<String>,
            fencing_token: u64,
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

    /// Backward compat: manifests serialized BEFORE `SegmentRef.membership`
    /// existed must still decode, in both MessagePack (structs as arrays —
    /// new fields must be trailing + defaulted) and legacy JSON.
    #[test]
    fn test_decode_segment_ref_without_membership_field() {
        #[derive(Serialize)]
        struct OldSegmentRef {
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
        }
        #[derive(Serialize)]
        struct MixedManifest {
            fragments: Vec<FragmentRef>,
            segments: Vec<OldSegmentRef>,
            compaction_watermark: Option<Ulid>,
            active_segment: Option<String>,
            next_sequence: u64,
            pending_deletes: Vec<String>,
            fencing_token: u64,
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

    /// Round-trip: size_bytes survives serialize → deserialize.
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
