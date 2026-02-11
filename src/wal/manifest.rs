use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::error::Result;
use crate::storage::ZeppelinStore;

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

    /// Add a segment reference.
    pub fn add_segment(&mut self, sref: SegmentRef) {
        self.active_segment = Some(sref.id.clone());
        self.segments.push(sref);
        self.updated_at = Utc::now();
    }

    /// Get uncompacted fragments (those after the compaction watermark).
    pub fn uncompacted_fragments(&self) -> &[FragmentRef] {
        &self.fragments
    }

    /// Total vector count across all segments.
    pub fn segment_vector_count(&self) -> usize {
        self.segments.iter().map(|s| s.vector_count).sum()
    }

    /// Serialize to JSON bytes.
    pub fn to_bytes(&self) -> Result<Bytes> {
        let json = serde_json::to_vec_pretty(self)?;
        Ok(Bytes::from(json))
    }

    /// Deserialize from JSON bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(data)?)
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
