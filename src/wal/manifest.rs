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
    pub id: Ulid,
    pub vector_count: usize,
    pub delete_count: usize,
    /// Monotonic sequence number assigned at manifest write time.
    /// Immune to clock skew — determines merge order instead of ULID.
    #[serde(default)]
    pub sequence_number: u64,
}

/// A reference to an IVF segment stored on S3.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentRef {
    pub id: String,
    pub vector_count: usize,
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

    /// Remove compacted fragments (those <= watermark).
    pub fn remove_compacted_fragments(&mut self, watermark: Ulid) {
        self.fragments.retain(|f| f.id > watermark);
        self.compaction_watermark = Some(watermark);
        self.updated_at = Utc::now();
    }

    /// Maximum number of pending deletes to keep in the manifest.
    /// Older entries beyond this limit are silently dropped (the S3 objects
    /// may already have been deleted, or will be orphaned and cleaned up
    /// by a future GC sweep).
    const MAX_PENDING_DELETES: usize = 1000;

    /// Maximum number of old (non-active) segments to retain.
    /// Only the active segment is used for queries; older segments are
    /// historical and can be pruned to keep the manifest small.
    const MAX_OLD_SEGMENTS: usize = 10;

    /// Add a segment reference and prune old segments/pending_deletes.
    pub fn add_segment(&mut self, sref: SegmentRef) {
        self.active_segment = Some(sref.id.clone());
        self.segments.push(sref);
        self.updated_at = Utc::now();
        self.prune();
    }

    /// Prune the manifest to prevent unbounded growth at 1M+ scale.
    ///
    /// - Caps `pending_deletes` to the most recent MAX_PENDING_DELETES entries.
    /// - Retains only the most recent MAX_OLD_SEGMENTS non-active segments.
    fn prune(&mut self) {
        // Cap pending deletes (keep newest)
        if self.pending_deletes.len() > Self::MAX_PENDING_DELETES {
            let excess = self.pending_deletes.len() - Self::MAX_PENDING_DELETES;
            self.pending_deletes.drain(..excess);
        }

        // Prune old segments: keep active + most recent MAX_OLD_SEGMENTS
        if self.segments.len() > Self::MAX_OLD_SEGMENTS + 1 {
            let active_id = self.active_segment.as_deref();
            // Partition: keep active segment and the newest MAX_OLD_SEGMENTS others.
            // Segments are appended in order, so newest are at the end.
            let keep_from = self.segments.len() - (Self::MAX_OLD_SEGMENTS + 1);
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
        let msgpack = rmp_serde::to_vec(self)
            .map_err(|e| ZeppelinError::Serialization(format!("manifest msgpack serialize: {e}")))?;
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
                        ZeppelinError::Serialization(format!(
                            "manifest msgpack deserialize: {e}"
                        ))
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
