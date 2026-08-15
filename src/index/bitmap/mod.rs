//! Builds, persists, and evaluates metadata bitmap sidecars for vector clusters.
//!
//! This module sits between immutable segment construction and approximate
//! nearest-neighbor (ANN) search. Segment builders and compaction call
//! [`build::build_cluster_bitmaps`] with the attributes stored beside one cluster.
//! They serialize the result with [`ClusterBitmapIndex::to_bytes`] and upload it
//! under [`bitmap_key`]. IVF-Flat and hierarchical search load that sidecar,
//! decode it with [`ClusterBitmapIndex::from_bytes`], and ask
//! [`evaluate::evaluate_filter_bitmap`] which row positions can enter distance
//! computation.
//!
//! A roaring bitmap is a compressed set of non-negative `u32` integers. Here an
//! integer is a row position within one cluster, not a vector ID. Set union,
//! intersection, and difference therefore evaluate logical filters without
//! visiting every vector's attribute map.
//!
//! This module creates and decodes bytes but performs no object-store I/O itself.
//! Its callers store each byte sequence as an immutable segment sidecar. S3 or
//! MinIO remains authoritative, and the containing segment's manifest publication
//! controls visibility. A local copy is only a cache. Search currently treats a
//! missing or undecodable sidecar as an unavailable optimization and evaluates
//! the original attributes instead.
//!
//! ```text
//! cluster attribute rows
//!          |
//!          v
//! build_cluster_bitmaps ---- high-cardinality field ----> omit that field
//!          |
//!          v
//! ClusterBitmapIndex -> ZBMP header + JSON -> immutable S3/MinIO sidecar
//!                                               |
//!                       published segment ------+
//!                                               v
//!                                      decode and evaluate filter
//!                                               |
//!                       +-----------------------+----------------------+
//!                       |                                              |
//!                       v                                              v
//!             Some(candidate rows)                       None (cannot decide)
//!                       |                                              |
//!                       v                                              v
//!              ANN distance scan                         exact attribute filter
//! ```
//!
//! ## Reading map
//!
//! 1. Start with [`ClusterBitmapIndex`] and [`AttributeBitmaps`] for the stored
//!    representation.
//! 2. Read [`BitmapKey`] to see how typed attribute values remain distinct.
//! 3. Continue with [`build::build_cluster_bitmaps`] for construction and
//!    cardinality control.
//! 4. Finish with [`evaluate::evaluate_filter_bitmap`] for recursive set
//!    operations and fallback boundaries.
//!
//! ## Invariants
//!
//! - Every bitmap position is relative to one cluster and is less than
//!   [`ClusterBitmapIndex::vector_count`].
//! - String, integer, float, and Boolean keys occupy separate type-prefixed key
//!   spaces.
//! - A missing field in [`ClusterBitmapIndex::fields`] means "not indexed," not
//!   "proved absent"; evaluation must return `None` so callers can use the exact
//!   attribute path.
//! - [`AttributeBitmaps::present`] distinguishes a missing field from a present
//!   empty list, even though the current evaluator derives negative predicates
//!   from the full cluster universe.
//! - The five-byte header is the compatibility boundary for the JSON payload.
//!
//! ## Rust concepts used here
//!
//! [`RoaringBitmap`] acts like Java's `BitSet` or a C bitmap library, but owns its
//! compressed allocation and frees it automatically. The evaluator borrows an
//! index with `&ClusterBitmapIndex` and returns a newly owned bitmap; it cannot
//! leave a dangling pointer into the index. `Option<RoaringBitmap>` represents a
//! capability result in the type system: `Some` is an exact answer (including an
//! empty set), while `None` requires the caller to choose another evaluation
//! path.

/// Bitmap index construction from cluster attributes.
pub mod build;
/// Bitmap filter evaluation against roaring bitmaps.
pub mod evaluate;

use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

use crate::types::AttributeValue;

/// Identifies bytes as a Zeppelin cluster-bitmap sidecar before JSON decoding.
///
/// The prefix fails loudly on the wrong artifact type instead of asking serde to
/// interpret arbitrary bytes as a bitmap index.
pub(crate) const BITMAP_MAGIC: &[u8; 4] = b"ZBMP";

/// Selects the persisted payload format understood by this implementation.
///
/// Increment this value when a format change cannot be read with the existing
/// serde defaults. [`ClusterBitmapIndex::from_bytes`] rejects every other value.
pub(crate) const BITMAP_VERSION: u8 = 1;

/// Maximum distinct encoded values retained for one indexed field.
///
/// [`build::build_cluster_bitmaps`] omits a field only after its cardinality is
/// greater than this value. Omitting the field bounds sidecar memory and object
/// size; it does not change filter semantics because evaluation returns `None`
/// and the search layer can inspect the original attributes.
pub const MAX_CARDINALITY: usize = 10_000;

/// A stable, type-prefixed lookup key for one [`AttributeValue`].
///
/// Different attribute types never collide merely because their displayed text
/// is equal: integer `1` uses `"i:1"`, while string `"1"` uses `"s:1"`.
/// Floating-point keys preserve the exact IEEE-754 bit pattern, including the
/// distinction between `0.0` and `-0.0` and between different NaN payloads.
///
/// The textual representation is persisted inside the JSON sidecar. Changing a
/// prefix is therefore a format change even if `BITMAP_VERSION` is unchanged.
///
/// # Rust Notes for Java/C Engineers
///
/// This tuple struct is a newtype: it has the runtime representation of a
/// `String`, but Rust will not accidentally accept an unrelated `String` where a
/// `BitmapKey` is required. Java would commonly use a small value class; C would
/// usually rely on a naming convention or wrapper struct without move checking.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BitmapKey(
    /// Encoded type prefix and value used as the persisted map key.
    pub String,
);

impl BitmapKey {
    /// Encodes a scalar attribute, or a list-type sentinel, as an owned key.
    ///
    /// # Parameters
    ///
    /// - `value`: Borrowed attribute whose type and value determine the key.
    ///
    /// # Returns
    ///
    /// A newly allocated key. Scalar strings and numbers contain their value;
    /// list variants produce type-specific sentinels because builders index list
    /// elements individually through the element constructors below.
    ///
    /// # Examples
    ///
    /// Integer `7` becomes `"i:7"`, Boolean `true` becomes `"b:true"`, and a
    /// string list becomes the sentinel `"sl:list"` rather than encoding its
    /// elements as one key.
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

    /// Encodes one borrowed string, usually a string-list element, for lookup.
    ///
    /// # Parameters
    ///
    /// - `s`: UTF-8 value to place in the string key space.
    ///
    /// # Returns
    ///
    /// An owned key such as `"s:sale"`. A scalar string with the same text uses
    /// the same key, which preserves the exact evaluator's list-membership
    /// equality behavior.
    pub fn from_string_element(s: &str) -> Self {
        BitmapKey(format!("s:{s}"))
    }

    /// Encodes one integer, usually an integer-list element, for lookup.
    ///
    /// # Parameters
    ///
    /// - `i`: Signed integer to place in the integer key space.
    ///
    /// # Returns
    ///
    /// An owned key such as `"i:42"`.
    pub fn from_integer_element(i: i64) -> Self {
        BitmapKey(format!("i:{i}"))
    }

    /// Encodes one floating-point value by its exact IEEE-754 bits.
    ///
    /// # Parameters
    ///
    /// - `f`: Floating-point scalar or list element to encode.
    ///
    /// # Returns
    ///
    /// An owned `"f:<bits>"` key. Bitwise encoding is stable across formatting
    /// choices and preserves `-0.0` separately from `0.0`.
    pub fn from_float_element(f: f64) -> Self {
        BitmapKey(format!("f:{}", f.to_bits()))
    }
}

/// Bitmap representation of one attribute field within one vector cluster.
///
/// For `color`, for example, `present` records every row with a color and
/// `values["s:red"]` records only rows whose color is red. List fields use the
/// same value map as an inverted index: each element points to every containing
/// row. Numeric fields additionally retain ordered keys for range evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeBitmaps {
    /// Cluster-relative row positions where the attribute key exists.
    ///
    /// A present empty list is included here even though it contributes no entry
    /// to [`AttributeBitmaps::values`].
    pub present: RoaringBitmap,

    /// Maps each distinct typed value or list element to its containing rows.
    ///
    /// [`BTreeMap`] provides a stable key order in the persisted JSON payload;
    /// each roaring bitmap compresses the corresponding set of `u32` positions.
    pub values: BTreeMap<BitmapKey, RoaringBitmap>,

    /// Numeric values in [`f64::total_cmp`] order with their value-map keys.
    ///
    /// The `u64` is [`f64::to_bits`], not a numeric integer. Deserialization
    /// defaults the list to empty so payloads written before this field existed
    /// remain readable, but those payloads cannot accelerate range predicates.
    #[serde(default)]
    pub sorted_numeric_keys: Vec<(u64, BitmapKey)>,

    /// Whether construction observed a list value for this field.
    ///
    /// [`evaluate::evaluate_filter_bitmap`] uses this marker to distinguish
    /// list-element containment, which bitmaps can answer, from string substring
    /// containment, which requires the original text.
    #[serde(default)]
    pub is_list: bool,
}

/// Complete metadata prefilter index for one immutable vector cluster.
///
/// Row positions in every nested bitmap share the universe
/// `0..vector_count`. This type owns all maps and compressed bitmaps, so it can
/// outlive the attribute data from which the builder created it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterBitmapIndex {
    /// Number of cluster rows and exclusive upper bound for bitmap positions.
    pub vector_count: u32,

    /// Indexed fields keyed by the original attribute name.
    ///
    /// Absence means the field was not indexed, possibly because it exceeded
    /// [`MAX_CARDINALITY`]; callers must not infer that all vectors lack it.
    pub fields: HashMap<String, AttributeBitmaps>,
}

impl ClusterBitmapIndex {
    /// Serializes this index as a versioned immutable sidecar payload.
    ///
    /// # Returns
    ///
    /// Shared [`bytes::Bytes`] laid out as four magic bytes, one version byte,
    /// and a complete JSON representation of this index.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if serde cannot encode the index. No
    /// object-store write has occurred; callers decide whether and where to
    /// upload the returned bytes.
    ///
    /// # Side Effects
    ///
    /// Allocates the JSON/header buffer and emits a structured debug event. It
    /// does not mutate the index or contact S3/MinIO.
    ///
    /// # Performance
    ///
    /// Visits the complete index and allocates a buffer proportional to the JSON
    /// payload. Converting the final `Vec<u8>` into [`bytes::Bytes`] transfers
    /// ownership of that allocation rather than copying it again.
    ///
    /// # Examples
    ///
    /// A three-row color index becomes `ZBMP`, version `1`, then JSON. A segment
    /// builder stores those bytes under [`bitmap_key`]; this method alone does
    /// not make the sidecar authoritative or visible.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `&self` is a temporary shared borrow, comparable to a read-only Java
    /// reference or `const ClusterBitmapIndex *` in C, but statically non-null
    /// and lifetime checked. The returned `Bytes` owns the serialized allocation
    /// independently of that borrow.
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

    /// Validates and decodes one complete bitmap sidecar payload.
    ///
    /// # Parameters
    ///
    /// - `data`: Borrowed bytes fetched by the caller from object storage or a
    ///   disposable cache. The slice must contain the entire sidecar object.
    ///
    /// # Returns
    ///
    /// A newly owned index whose maps and bitmaps no longer borrow `data`.
    ///
    /// # Errors
    ///
    /// Returns an index error when the input is shorter than the five-byte
    /// header, the magic identifies another artifact, or the version is not
    /// supported. Returns a serialization error when the remaining JSON is
    /// malformed or incompatible. No partial index escapes on failure.
    ///
    /// # Consistency
    ///
    /// Header validation protects format interpretation, not artifact
    /// authority. The caller must obtain the key from a manifest-visible segment
    /// and must not prefer a stale local sidecar over authoritative S3/MinIO
    /// state.
    ///
    /// # Performance
    ///
    /// Parses the complete JSON payload and allocates owned maps and roaring
    /// bitmaps. The input bytes themselves are only borrowed during the call.
    ///
    /// # Examples
    ///
    /// Bytes beginning `ZBMP\x01` are decoded as version 1. `XXXX\x01...` fails
    /// before JSON parsing, and `ZBMP\x02...` fails rather than silently guessing
    /// at a newer format.
    pub fn from_bytes(data: &[u8]) -> crate::error::Result<Self> {
        if data.len() < 5 {
            return Err(crate::error::ZeppelinError::Index(
                "bitmap index data too short".to_string(),
            ));
        }
        if &data[0..4] != BITMAP_MAGIC {
            return Err(crate::error::ZeppelinError::Index(format!(
                "invalid bitmap magic: expected ZBMP, got {:?}",
                &data[0..4]
            )));
        }
        if data[4] != BITMAP_VERSION {
            return Err(crate::error::ZeppelinError::Index(format!(
                "unsupported bitmap version: expected {}, got {}",
                BITMAP_VERSION, data[4]
            )));
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

/// Constructs the object-store key for one cluster's bitmap sidecar.
///
/// # Parameters
///
/// - `namespace`: Namespace key prefix already validated by the caller.
/// - `segment_id`: Identifier of the immutable segment that owns the cluster.
/// - `cluster_idx`: Zero-based cluster position within that segment.
///
/// # Returns
///
/// An owned key ending in `bitmap_<cluster_idx>.bin`. This is an S3/MinIO object
/// key, not an HTTP URL path.
///
/// # Examples
///
/// Namespace `catalog`, segment `seg-7`, and cluster `3` produce
/// `catalog/segments/seg-7/bitmap_3.bin`.
pub fn bitmap_key(namespace: &str, segment_id: &str, cluster_idx: usize) -> String {
    format!("{namespace}/segments/{segment_id}/bitmap_{cluster_idx}.bin")
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Serialization and key-format regression tests for bitmap sidecars.

    use super::*;

    /// Proves that a populated value bitmap survives the versioned JSON format.
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
        assert!(color
            .values
            .get(&BitmapKey("s:red".to_string()))
            .unwrap()
            .contains(0));
        assert!(color
            .values
            .get(&BitmapKey("s:red".to_string()))
            .unwrap()
            .contains(2));
    }

    /// Proves that a zero-row index remains valid through serialization.
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

    /// Proves that an artifact with the wrong magic fails before JSON decoding.
    #[test]
    fn test_invalid_magic_byte_rejected() {
        let mut data = vec![b'X', b'X', b'X', b'X', BITMAP_VERSION];
        data.extend_from_slice(b"{}");
        let result = ClusterBitmapIndex::from_bytes(&data);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("invalid bitmap magic"));
    }

    /// Locks down the object-store naming convention used by builders and readers.
    #[test]
    fn test_bitmap_key_format() {
        let key = bitmap_key("my-ns", "seg-1", 3);
        assert_eq!(key, "my-ns/segments/seg-1/bitmap_3.bin");
    }

    /// Proves that equal typed values always receive equal persisted lookup keys.
    #[test]
    fn test_bitmap_key_deterministic() {
        let v1 = AttributeValue::String("hello".to_string());
        let v2 = AttributeValue::String("hello".to_string());
        assert_eq!(BitmapKey::from_attr(&v1), BitmapKey::from_attr(&v2));
    }

    /// Proves that textual equality cannot collide across attribute types.
    #[test]
    fn test_bitmap_key_different_types_differ() {
        let int_key = BitmapKey::from_attr(&AttributeValue::Integer(1));
        let str_key = BitmapKey::from_attr(&AttributeValue::String("1".to_string()));
        assert_ne!(int_key, str_key);
    }

    /// Proves that float keys retain IEEE-754 distinctions and stable NaN bits.
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

    /// Locks down the header byte written for the current payload format.
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
