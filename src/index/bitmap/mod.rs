//! Bitmap indexes for pre-filter vector search.
//!
//! Each cluster gets a `ClusterBitmapIndex` containing per-field roaring bitmaps.
//! At query time, the filter is evaluated against the bitmap to produce a set of
//! matching vector positions *before* distance computation, skipping non-matching
//! vectors entirely.
//!
//! Serialization format:
//! ```text
//! [4 bytes magic: "ZBMP"] [1 byte version] [JSON payload]
//! ```

pub mod build;
pub mod evaluate;

use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

use crate::types::AttributeValue;

/// Magic bytes for bitmap index files.
const BITMAP_MAGIC: &[u8; 4] = b"ZBMP";

/// Current serialization version.
const BITMAP_VERSION: u8 = 1;

/// Maximum number of distinct values for a field before it's excluded
/// from bitmap indexing (high-cardinality fields waste space).
pub const MAX_CARDINALITY: usize = 10_000;

/// A deterministic string key for an `AttributeValue`, used as the key
/// in the value→bitmap map. Different types produce different keys even
/// for the same "display" value (e.g. Integer(1) vs String("1")).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BitmapKey(pub String);

impl BitmapKey {
    pub fn from_attr(value: &AttributeValue) -> Self {
        match value {
            AttributeValue::String(s) => BitmapKey(format!("s:{s}")),
            AttributeValue::Integer(i) => BitmapKey(format!("i:{i}")),
            AttributeValue::Float(f) => {
                // Use to_bits for deterministic representation (handles -0.0, NaN)
                BitmapKey(format!("f:{}", f.to_bits()))
            }
            AttributeValue::Bool(b) => BitmapKey(format!("b:{b}")),
            // List types: each element becomes its own key during build,
            // but for lookup we need the element key.
            AttributeValue::StringList(_) => BitmapKey("sl:list".to_string()),
            AttributeValue::IntegerList(_) => BitmapKey("il:list".to_string()),
            AttributeValue::FloatList(_) => BitmapKey("fl:list".to_string()),
        }
    }

    /// Create a bitmap key for a single list element.
    pub fn from_string_element(s: &str) -> Self {
        BitmapKey(format!("s:{s}"))
    }

    pub fn from_integer_element(i: i64) -> Self {
        BitmapKey(format!("i:{i}"))
    }

    pub fn from_float_element(f: f64) -> Self {
        BitmapKey(format!("f:{}", f.to_bits()))
    }
}

/// Per-field bitmap data: tracks which positions have a value, and maps
/// each distinct value to its position bitmap.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeBitmaps {
    /// Positions where this field has a non-null value.
    pub present: RoaringBitmap,

    /// Map from value key to position bitmap.
    pub values: BTreeMap<BitmapKey, RoaringBitmap>,

    /// For numeric fields: sorted list of (f64_bits, BitmapKey) for range queries.
    /// Stored as u64 (f64::to_bits) for deterministic ordering.
    #[serde(default)]
    pub sorted_numeric_keys: Vec<(u64, BitmapKey)>,

    /// Whether this field is a list type (inverted index).
    #[serde(default)]
    pub is_list: bool,
}

/// Complete bitmap index for a single cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterBitmapIndex {
    /// Number of vectors in this cluster (defines the universe).
    pub vector_count: u32,

    /// Per-field bitmaps.
    pub fields: HashMap<String, AttributeBitmaps>,
}

impl ClusterBitmapIndex {
    /// Serialize to bytes with magic + version header.
    pub fn to_bytes(&self) -> crate::error::Result<bytes::Bytes> {
        let json = serde_json::to_vec(self)?;
        let mut buf = Vec::with_capacity(5 + json.len());
        buf.extend_from_slice(BITMAP_MAGIC);
        buf.push(BITMAP_VERSION);
        buf.extend_from_slice(&json);
        tracing::debug!(
            byte_count = buf.len(),
            field_count = self.fields.len(),
            vector_count = self.vector_count,
            "serialized bitmap index"
        );
        Ok(bytes::Bytes::from(buf))
    }

    /// Deserialize from bytes, validating magic + version.
    pub fn from_bytes(data: &[u8]) -> crate::error::Result<Self> {
        if data.len() < 5 {
            return Err(crate::error::ZeppelinError::Index(
                "bitmap index data too short".to_string(),
            ));
        }
        if &data[0..4] != BITMAP_MAGIC {
            return Err(crate::error::ZeppelinError::Index(
                format!(
                    "invalid bitmap magic: expected ZBMP, got {:?}",
                    &data[0..4]
                ),
            ));
        }
        if data[4] != BITMAP_VERSION {
            return Err(crate::error::ZeppelinError::Index(
                format!(
                    "unsupported bitmap version: expected {}, got {}",
                    BITMAP_VERSION, data[4]
                ),
            ));
        }
        let index: Self = serde_json::from_slice(&data[5..])?;
        tracing::debug!(
            field_count = index.fields.len(),
            vector_count = index.vector_count,
            "deserialized bitmap index"
        );
        Ok(index)
    }
}

/// S3 key for the bitmap index of a specific cluster within a segment.
pub fn bitmap_key(namespace: &str, segment_id: &str, cluster_idx: usize) -> String {
    format!("{namespace}/segments/{segment_id}/bitmap_{cluster_idx}.bin")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let mut fields = HashMap::new();
        let mut values = BTreeMap::new();
        let mut bm = RoaringBitmap::new();
        bm.insert(0);
        bm.insert(2);
        values.insert(BitmapKey("s:red".to_string()), bm.clone());
        let mut present = RoaringBitmap::new();
        present.insert(0);
        present.insert(1);
        present.insert(2);
        fields.insert(
            "color".to_string(),
            AttributeBitmaps {
                present,
                values,
                sorted_numeric_keys: vec![],
                is_list: false,
            },
        );
        let index = ClusterBitmapIndex {
            vector_count: 3,
            fields,
        };
        let bytes = index.to_bytes().unwrap();
        let recovered = ClusterBitmapIndex::from_bytes(&bytes).unwrap();
        assert_eq!(recovered.vector_count, 3);
        assert_eq!(recovered.fields.len(), 1);
        let color = recovered.fields.get("color").unwrap();
        assert_eq!(color.present.len(), 3);
        assert!(color.values.get(&BitmapKey("s:red".to_string())).unwrap().contains(0));
        assert!(color.values.get(&BitmapKey("s:red".to_string())).unwrap().contains(2));
    }

    #[test]
    fn test_empty_index_roundtrip() {
        let index = ClusterBitmapIndex {
            vector_count: 0,
            fields: HashMap::new(),
        };
        let bytes = index.to_bytes().unwrap();
        let recovered = ClusterBitmapIndex::from_bytes(&bytes).unwrap();
        assert_eq!(recovered.vector_count, 0);
        assert!(recovered.fields.is_empty());
    }

    #[test]
    fn test_invalid_magic_byte_rejected() {
        let mut data = vec![b'X', b'X', b'X', b'X', BITMAP_VERSION];
        data.extend_from_slice(b"{}");
        let result = ClusterBitmapIndex::from_bytes(&data);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("invalid bitmap magic"));
    }

    #[test]
    fn test_bitmap_key_format() {
        let key = bitmap_key("my-ns", "seg-1", 3);
        assert_eq!(key, "my-ns/segments/seg-1/bitmap_3.bin");
    }

    #[test]
    fn test_bitmap_key_deterministic() {
        let v1 = AttributeValue::String("hello".to_string());
        let v2 = AttributeValue::String("hello".to_string());
        assert_eq!(BitmapKey::from_attr(&v1), BitmapKey::from_attr(&v2));
    }

    #[test]
    fn test_bitmap_key_different_types_differ() {
        let int_key = BitmapKey::from_attr(&AttributeValue::Integer(1));
        let str_key = BitmapKey::from_attr(&AttributeValue::String("1".to_string()));
        assert_ne!(int_key, str_key);
    }

    #[test]
    fn test_bitmap_key_float_stability() {
        // 0.0 and -0.0 have different bit patterns
        let pos_zero = BitmapKey::from_attr(&AttributeValue::Float(0.0));
        let neg_zero = BitmapKey::from_attr(&AttributeValue::Float(-0.0));
        assert_ne!(pos_zero, neg_zero);

        // NaN is deterministic
        let nan1 = BitmapKey::from_attr(&AttributeValue::Float(f64::NAN));
        let nan2 = BitmapKey::from_attr(&AttributeValue::Float(f64::NAN));
        assert_eq!(nan1, nan2);
    }

    #[test]
    fn test_version_byte_current() {
        let index = ClusterBitmapIndex {
            vector_count: 1,
            fields: HashMap::new(),
        };
        let bytes = index.to_bytes().unwrap();
        assert_eq!(bytes[4], BITMAP_VERSION);
        assert_eq!(BITMAP_VERSION, 1);
    }
}
