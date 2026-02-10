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
            updated_at: Utc::now(),
        }
    }

    /// Get the S3 key for the manifest of a namespace.
    pub fn s3_key(namespace: &str) -> String {
        format!("{namespace}/manifest.json")
    }

    /// Add a fragment reference.
    pub fn add_fragment(&mut self, fref: FragmentRef) {
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
}

impl Default for Manifest {
    fn default() -> Self {
        Self::new()
    }
}
