use bytes::Bytes;
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use xxhash_rust::xxh3::xxh3_64;

use crate::error::{Result, ZeppelinError};
use crate::types::{VectorEntry, VectorId};

/// Version byte prefixed to WAL fragment payloads for format detection.
/// - `0x00` / no prefix (legacy): JSON
/// - `0x01`: MessagePack (rmp-serde)
const WAL_FORMAT_MSGPACK: u8 = 0x01;

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
    #[allow(clippy::expect_used)]
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
    /// Uses JSON serialization for the canonical form because `AttributeValue`
    /// uses `#[serde(untagged)]` which is incompatible with bincode.
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
        #[allow(clippy::expect_used)]
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

    /// Serialize this fragment to MessagePack bytes with a version header.
    ///
    /// Format: `[0x01] [msgpack payload]`
    /// MessagePack is self-describing (safe with `#[serde(untagged)]`) and
    /// 2-5x faster than JSON for deserialization with ~30% smaller payloads.
    pub fn to_bytes(&self) -> Result<Bytes> {
        let msgpack = rmp_serde::to_vec(self)
            .map_err(|e| ZeppelinError::Serialization(format!("msgpack serialize: {e}")))?;
        let mut data = Vec::with_capacity(1 + msgpack.len());
        data.push(WAL_FORMAT_MSGPACK);
        data.extend_from_slice(&msgpack);
        Ok(Bytes::from(data))
    }

    /// Deserialize a fragment from bytes, auto-detecting format.
    ///
    /// - If first byte is `0x01`: MessagePack (new format)
    /// - If first byte is `{` (0x7B): JSON (legacy format)
    /// - Otherwise: try MessagePack, fall back to JSON
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(ZeppelinError::Serialization(
                "empty WAL fragment data".into(),
            ));
        }

        let fragment: Self = match data[0] {
            WAL_FORMAT_MSGPACK => rmp_serde::from_slice(&data[1..])
                .map_err(|e| ZeppelinError::Serialization(format!("msgpack deserialize: {e}")))?,
            // Legacy JSON format: first byte is '{' (0x7B)
            b'{' => serde_json::from_slice(data)?,
            // Unknown: try msgpack (skip version byte), fall back to JSON
            _ => rmp_serde::from_slice(&data[1..])
                .or_else(|_| rmp_serde::from_slice(data))
                .map_err(|e| ZeppelinError::Serialization(format!("msgpack deserialize: {e}")))?,
        };
        fragment.validate_checksum()?;
        Ok(fragment)
    }

    /// Deserialize a fragment from bytes without validating the checksum.
    ///
    /// Only use for fragments already validated on write (e.g. compaction reads).
    /// Skips the `validate_checksum()` call for ~5.7% compaction speedup.
    pub fn from_bytes_unchecked(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(ZeppelinError::Serialization(
                "empty WAL fragment data".into(),
            ));
        }

        let fragment: Self = match data[0] {
            WAL_FORMAT_MSGPACK => rmp_serde::from_slice(&data[1..])
                .map_err(|e| ZeppelinError::Serialization(format!("msgpack deserialize: {e}")))?,
            b'{' => serde_json::from_slice(data)?,
            _ => rmp_serde::from_slice(&data[1..])
                .or_else(|_| rmp_serde::from_slice(data))
                .map_err(|e| ZeppelinError::Serialization(format!("msgpack deserialize: {e}")))?,
        };
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
