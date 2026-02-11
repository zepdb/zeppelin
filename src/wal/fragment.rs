use bytes::Bytes;
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use xxhash_rust::xxh3::xxh3_64;

use crate::error::{Result, ZeppelinError};
use crate::types::{VectorEntry, VectorId};

/// A single WAL fragment containing upserted vectors and/or deletes.
/// Fragments are immutable once written to S3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalFragment {
    /// Unique, time-ordered identifier for this fragment.
    pub id: Ulid,
    /// Vectors to upsert.
    pub vectors: Vec<VectorEntry>,
    /// Vector IDs to delete.
    pub deletes: Vec<VectorId>,
    /// xxHash checksum of the serialized payload (vectors + deletes).
    pub checksum: u64,
}

impl WalFragment {
    /// Create a new WAL fragment, panicking if any vector ID appears in both
    /// upserts and deletes. Per project rule: no fallbacks, crash explicitly.
    pub fn new(vectors: Vec<VectorEntry>, deletes: Vec<VectorId>) -> Self {
        Self::try_new(vectors, deletes)
            .expect("WalFragment::new called with overlapping vector IDs in upserts and deletes")
    }

    /// Create a new WAL fragment, returning an error if any vector ID appears
    /// in both the upsert list and the delete list.
    pub fn try_new(
        vectors: Vec<VectorEntry>,
        deletes: Vec<VectorId>,
    ) -> std::result::Result<Self, ZeppelinError> {
        use std::collections::HashSet;

        let delete_set: HashSet<&str> = deletes.iter().map(|id| id.as_str()).collect();
        for vec in &vectors {
            if delete_set.contains(vec.id.as_str()) {
                return Err(ZeppelinError::Validation(format!(
                    "vector ID '{}' appears in both upserts and deletes within the same fragment",
                    vec.id
                )));
            }
        }

        let id = Ulid::new();
        let checksum = Self::compute_checksum(&vectors, &deletes);
        Ok(Self {
            id,
            vectors,
            deletes,
            checksum,
        })
    }

    /// Compute the checksum for a set of vectors and deletes.
    ///
    /// Uses JSON serialization because `AttributeValue` uses `#[serde(untagged)]`
    /// which is incompatible with bincode's non-self-describing format.
    ///
    /// Attributes are canonicalized via BTreeMap to ensure deterministic key
    /// ordering across serialization round-trips (HashMap iteration order is
    /// not guaranteed to be stable after deserialize → re-serialize).
    fn compute_checksum(vectors: &[VectorEntry], deletes: &[VectorId]) -> u64 {
        use crate::types::AttributeValue;
        use std::collections::BTreeMap;

        #[allow(clippy::type_complexity)]
        let canonical: Vec<(&str, &[f32], Option<BTreeMap<&String, &AttributeValue>>)> = vectors
            .iter()
            .map(|v| {
                let attrs = v
                    .attributes
                    .as_ref()
                    .map(|a| a.iter().collect::<BTreeMap<_, _>>());
                (v.id.as_str(), v.values.as_slice(), attrs)
            })
            .collect();
        let payload =
            serde_json::to_vec(&(&canonical, deletes)).expect("serialization should not fail");
        xxh3_64(&payload)
    }

    /// Validate the checksum of this fragment.
    pub fn validate_checksum(&self) -> Result<()> {
        let expected = Self::compute_checksum(&self.vectors, &self.deletes);
        if self.checksum != expected {
            return Err(ZeppelinError::ChecksumMismatch {
                expected,
                actual: self.checksum,
            });
        }
        Ok(())
    }

    /// Serialize this fragment to JSON bytes.
    pub fn to_bytes(&self) -> Result<Bytes> {
        let data = serde_json::to_vec(self)?;
        Ok(Bytes::from(data))
    }

    /// Deserialize a fragment from JSON bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let fragment: Self = serde_json::from_slice(data)?;
        fragment.validate_checksum()?;
        Ok(fragment)
    }

    /// Get the S3 key for this fragment within a namespace.
    pub fn s3_key(namespace: &str, id: &Ulid) -> String {
        format!("{namespace}/wal/{id}.wal")
    }

    /// Total number of vector operations in this fragment.
    pub fn operation_count(&self) -> usize {
        self.vectors.len() + self.deletes.len()
    }
}
