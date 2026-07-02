use std::collections::HashSet;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
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
    /// NOTE: this field must stay LAST in the struct. MessagePack encodes
    /// structs as arrays, so old manifests decode only if new fields are
    /// trailing and `#[serde(default)]`.
    #[serde(default)]
    pub size_bytes: u64,
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
        }
    }

    /// Get the S3 key for the manifest of a namespace.
    pub fn s3_key(namespace: &str) -> String {
        format!("{namespace}/manifest.json")
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

    /// Add a segment reference and prune old segments/pending_deletes
    /// using the provided limits.
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
    pub async fn write(&self, store: &ZeppelinStore, namespace: &str) -> Result<()> {
        let key = Self::s3_key(namespace);
        let data = self.to_bytes()?;
        store.put(&key, data).await
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
        &self,
        store: &ZeppelinStore,
        namespace: &str,
        version: &ManifestVersion,
    ) -> Result<()> {
        let key = Self::s3_key(namespace);
        let data = self.to_bytes()?;
        match &version.0 {
            Some(etag) => store.put_if_match(&key, data, etag, namespace).await,
            None => store.put(&key, data).await,
        }
    }
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
        }
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
}
