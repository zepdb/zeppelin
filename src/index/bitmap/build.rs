//! Constructs an in-memory metadata bitmap index from one cluster's attributes.
//!
//! IVF-Flat construction, hierarchical construction, and compaction enter this
//! module through [`build::build_cluster_bitmaps`]. The input order must be
//! identical to the vector-row order stored for that cluster. The builder assigns
//! each row a `u32` position and creates one [`AttributeBitmaps`] value per
//! eligible field.
//! It does not serialize, upload, or publish anything; those steps belong to the
//! segment builder that calls it.
//!
//! For each field, construction records both presence and an inverted value map.
//! An inverted map answers "which rows contain this value?" instead of scanning
//! each row's `HashMap`. List elements become individual entries, numeric values
//! also receive ordered range keys, and a field with more than
//! [`MAX_CARDINALITY`] distinct encoded values is omitted to keep the sidecar
//! bounded. Omission is a performance decision: query evaluation treats the field
//! as unavailable and uses exact attribute filtering.
//!
//! ```text
//! row-aligned attributes
//!   0: {color: red,  tags: [sale, summer]}
//!   1: {color: blue, tags: [sale]}
//!   2: {              tags: []}
//!                 |
//!                 v
//!      collect one FieldBuilder per field
//!                 |
//!       +---------+-------------------+
//!       |                             |
//!       v                             v
//! color.present = {0,1}          tags.present = {0,1,2}
//! color.red     = {0}            tags.sale    = {0,1}
//! color.blue    = {1}            tags.summer  = {0}
//!       |                             |
//!       +-------------+---------------+
//!                     v
//!             ClusterBitmapIndex
//! ```
//!
//! ## Reading map
//!
//! 1. Start with [`build::build_cluster_bitmaps`] for the cluster-wide two-phase
//!    flow.
//! 2. Read `FieldBuilder::add` for scalar, list, and missing-field semantics.
//! 3. Read `FieldBuilder::finish` for range-key ordering and ownership transfer.
//! 4. See [`evaluate::evaluate_filter_bitmap`] for how the resulting sets
//!    become query candidates.
//!
//! ## Invariants
//!
//! - Input slice position and vector row position are the same value.
//! - A missing attribute map or missing field never enters that field's
//!   [`AttributeBitmaps::present`] bitmap.
//! - A present empty list enters `present` but creates no value bitmap.
//! - Cardinality counts distinct typed keys, not rows or repeated list elements.
//! - A field over the cardinality limit is absent from the final index, never
//!   represented by a misleading empty bitmap.
//!
//! ## Rust concepts used here
//!
//! The input type `&[Option<&HashMap<...>>]` layers three ideas: a borrowed slice,
//! an optional attribute map per row, and a borrowed map when one exists. Java
//! would commonly pass a list containing nullable map references; C would pass an
//! array of nullable pointers plus a length. Rust makes the slice bounds and every
//! present map reference non-null and lifetime checked. `FieldBuilder::finish`
//! then consumes each builder, moving its owned maps and bitmaps into the final
//! index without deep-copying them.

use roaring::RoaringBitmap;
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::types::AttributeValue;

use super::{AttributeBitmaps, BitmapKey, ClusterBitmapIndex, MAX_CARDINALITY};

/// Builds the complete bitmap prefilter representation for one vector cluster.
///
/// Construction first accumulates values by field, then removes fields whose
/// distinct key count exceeds [`MAX_CARDINALITY`] and finalizes range metadata.
/// A field omitted for cardinality is intentionally indistinguishable from any
/// other unindexed field so the evaluator will request exact post-filtering.
///
/// # Parameters
///
/// - `attrs`: Row-aligned attribute maps. Slice index `n` is cluster row `n`.
///   `None` means that row has no attribute object; a missing map entry means the
///   specific field is absent. The cluster must fit in the `u32` position space
///   used by roaring bitmaps.
///
/// # Returns
///
/// An owned [`ClusterBitmapIndex`] with the same row count and every field whose
/// encoded distinct-value count is at most [`MAX_CARDINALITY`]. An empty input
/// produces a zero-row index with no fields.
///
/// # Side Effects
///
/// Allocates in-memory maps and roaring bitmaps and emits structured tracing
/// events, including one debug event per omitted high-cardinality field. It does
/// not mutate the input maps or perform object-store I/O.
///
/// # Performance
///
/// The collection phase is linear in the number of attribute values and list
/// elements, subject to hash/tree-map operations. Finalization sorts each
/// field's distinct numeric values and retains compressed bitmaps proportional to
/// the indexed field/value incidence. High-cardinality detection bounds the
/// returned artifact but occurs after values have been accumulated in memory.
///
/// # Examples
///
/// If rows 0 and 2 have `color = "red"` and row 1 has no `color`, the returned
/// color value bitmap is `{0, 2}` and its presence bitmap is also `{0, 2}`. If
/// `user_id` has 10,001 distinct values, `user_id` is absent from `fields` so a
/// query on it takes the exact filtering path.
pub fn build_cluster_bitmaps(
    attrs: &[Option<&HashMap<String, AttributeValue>>],
) -> ClusterBitmapIndex {
    let vector_count = attrs.len() as u32;

    // Phase 1: Collect all field data
    let mut field_data: HashMap<String, FieldBuilder> = HashMap::new();

    for (pos, attr_opt) in attrs.iter().enumerate() {
        let pos = pos as u32;
        if let Some(attr_map) = attr_opt {
            for (field_name, value) in attr_map.iter() {
                let builder = field_data
                    .entry(field_name.clone())
                    .or_insert_with(FieldBuilder::new);
                builder.add(pos, value);
            }
        }
    }

    // Phase 2: Build final bitmaps, excluding high-cardinality fields
    let mut fields = HashMap::new();
    for (field_name, builder) in field_data {
        if builder.cardinality > MAX_CARDINALITY {
            tracing::debug!(
                field = %field_name,
                cardinality = builder.cardinality,
                max = MAX_CARDINALITY,
                "skipping high-cardinality field for bitmap index"
            );
            continue;
        }
        fields.insert(field_name, builder.finish());
    }

    tracing::info!(
        vector_count,
        field_count = fields.len(),
        "built cluster bitmap index"
    );

    ClusterBitmapIndex {
        vector_count,
        fields,
    }
}

/// Mutable accumulator for one field while cluster rows are being scanned.
///
/// The builder owns all intermediate allocations. It exists only during segment
/// construction and becomes immutable [`AttributeBitmaps`] through
/// [`FieldBuilder::finish`].
struct FieldBuilder {
    /// Rows where the field exists, including rows whose value is an empty list.
    present: RoaringBitmap,
    /// Typed scalar or list-element key to every row containing that value.
    values: BTreeMap<BitmapKey, RoaringBitmap>,
    /// IEEE-754 bit patterns used to construct ordered numeric range entries.
    numeric_keys: HashSet<u64>,
    /// Whether any observed value used a list variant.
    is_list: bool,
    /// Count of distinct entries in `seen_keys`, used for the field-size limit.
    cardinality: usize,
    /// Deduplicates values for cardinality accounting across rows and list repeats.
    seen_keys: HashSet<BitmapKey>,
}

impl FieldBuilder {
    /// Creates an empty accumulator for a field observed for the first time.
    ///
    /// # Returns
    ///
    /// A builder with no present rows, values, numeric keys, or list marker.
    ///
    /// # Examples
    ///
    /// The first occurrence of `color` creates this empty state; adding
    /// `color = "red"` then records one present row and one value key.
    fn new() -> Self {
        Self {
            present: RoaringBitmap::new(),
            values: BTreeMap::new(),
            numeric_keys: HashSet::new(),
            is_list: false,
            cardinality: 0,
            seen_keys: HashSet::new(),
        }
    }

    /// Records one row's value in the field's presence and inverted indexes.
    ///
    /// Scalars add one typed value key. Lists add one key for each element while
    /// marking the field as list-valued; repeated elements still produce only one
    /// row membership and one unit of global cardinality. An empty list marks the
    /// row present but adds no value entry.
    ///
    /// # Parameters
    ///
    /// - `pos`: Cluster-relative row position to add to the compressed sets.
    /// - `value`: Borrowed value from that row's attribute map.
    ///
    /// # Side Effects
    ///
    /// Mutates only this in-memory builder and may allocate keys and bitmap
    /// containers. The borrowed attribute remains owned by the caller.
    ///
    /// # Performance
    ///
    /// Scalar work is one key insertion plus one bitmap insertion. List work is
    /// linear in list length. String keys allocate owned text; numeric and Boolean
    /// keys allocate their formatted key strings.
    ///
    /// # Examples
    ///
    /// Adding row 4 with `tags = ["sale", "summer"]` inserts `4` into `present`,
    /// `values["s:sale"]`, and `values["s:summer"]`.
    fn add(&mut self, pos: u32, value: &AttributeValue) {
        self.present.insert(pos);

        match value {
            AttributeValue::StringList(list) => {
                self.is_list = true;
                for s in list {
                    let key = BitmapKey::from_string_element(s);
                    if self.seen_keys.insert(key.clone()) {
                        self.cardinality += 1;
                    }
                    self.values.entry(key).or_default().insert(pos);
                }
            }
            AttributeValue::IntegerList(list) => {
                self.is_list = true;
                for i in list {
                    let key = BitmapKey::from_integer_element(*i);
                    if self.seen_keys.insert(key.clone()) {
                        self.cardinality += 1;
                    }
                    self.values.entry(key).or_default().insert(pos);
                }
            }
            AttributeValue::FloatList(list) => {
                self.is_list = true;
                for f in list {
                    let key = BitmapKey::from_float_element(*f);
                    if self.seen_keys.insert(key.clone()) {
                        self.cardinality += 1;
                    }
                    self.values.entry(key).or_default().insert(pos);
                    self.numeric_keys.insert(f.to_bits());
                }
            }
            AttributeValue::Integer(i) => {
                let key = BitmapKey::from_attr(value);
                if self.seen_keys.insert(key.clone()) {
                    self.cardinality += 1;
                }
                self.values.entry(key).or_default().insert(pos);
                self.numeric_keys.insert((*i as f64).to_bits());
            }
            AttributeValue::Float(f) => {
                let key = BitmapKey::from_attr(value);
                if self.seen_keys.insert(key.clone()) {
                    self.cardinality += 1;
                }
                self.values.entry(key).or_default().insert(pos);
                self.numeric_keys.insert(f.to_bits());
            }
            _ => {
                let key = BitmapKey::from_attr(value);
                if self.seen_keys.insert(key.clone()) {
                    self.cardinality += 1;
                }
                self.values.entry(key).or_default().insert(pos);
            }
        }
    }

    /// Converts the mutable accumulator into the persisted per-field model.
    ///
    /// Numeric bit patterns are paired with their exact integer or float value
    /// keys and sorted by [`f64::total_cmp`] so range evaluation sees numeric
    /// order rather than string or raw-bit order.
    ///
    /// # Returns
    ///
    /// Owned [`AttributeBitmaps`] containing the accumulated sets, sorted range
    /// metadata, and list marker.
    ///
    /// # Performance
    ///
    /// Consumes the builder, moving its `present` and `values` allocations into
    /// the result without cloning them. It allocates and sorts one vector with an
    /// entry per distinct numeric bit pattern.
    ///
    /// # Examples
    ///
    /// A field observed in row order as integer values `30`, `10`, and `20`
    /// finishes with range metadata ordered `10`, `20`, `30`, while each key
    /// still points to its original row.
    ///
    /// TODO(doc): Confirm the supported range for integer attributes. Integer
    /// ordering currently passes through `f64`, so an `i64` that cannot be
    /// represented exactly can reconstruct a rounded key that is absent from the
    /// value map and therefore be skipped by range evaluation.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The `self` parameter is owned rather than borrowed. Calling `finish`
    /// moves the builder and prevents later reuse, allowing Rust to transfer its
    /// collections directly. Java would leave the old mutable object reachable
    /// unless code followed a convention; C would require a documented ownership
    /// handoff to avoid either copying or double-freeing its buffers.
    fn finish(self) -> AttributeBitmaps {
        // Build sorted numeric keys for range query support
        let mut sorted_numeric_keys: Vec<(u64, BitmapKey)> = self
            .numeric_keys
            .into_iter()
            .map(|bits| {
                let f = f64::from_bits(bits);
                let key = BitmapKey(format!("f:{bits}"));
                // For integer values that were stored as f64, try the integer key first
                let actual_key = if let Some(i) = try_f64_to_i64(f) {
                    let int_key = BitmapKey(format!("i:{i}"));
                    if self.values.contains_key(&int_key) {
                        int_key
                    } else if self.values.contains_key(&key) {
                        key
                    } else {
                        // Shouldn't happen, but be safe
                        int_key
                    }
                } else {
                    key
                };
                (bits, actual_key)
            })
            .collect();

        // Sort by f64 value (using total_cmp semantics via to_bits comparison
        // with sign-bit fixup for correct ordering)
        sorted_numeric_keys.sort_by(|a, b| {
            let fa = f64::from_bits(a.0);
            let fb = f64::from_bits(b.0);
            fa.total_cmp(&fb)
        });

        AttributeBitmaps {
            present: self.present,
            values: self.values,
            sorted_numeric_keys,
            is_list: self.is_list,
        }
    }
}

/// Converts an integral, in-range floating value to the corresponding `i64`.
///
/// This helper reconstructs integer-form [`BitmapKey`] values from the `f64`
/// representation used by [`FieldBuilder::numeric_keys`]. "Integral" here means
/// that the supplied floating value has no fractional part and is within the
/// cast's numeric bounds; it does not prove that an earlier arbitrary `i64` to
/// `f64` conversion preserved every low bit.
///
/// # Parameters
///
/// - `f`: Floating value recovered from a stored IEEE-754 bit pattern.
///
/// # Returns
///
/// `Some(integer)` for an integral in-range value; `None` for fractional,
/// non-finite, or out-of-range input.
///
/// # Examples
///
/// `42.0` returns `Some(42)`, while `42.5` and positive infinity return `None`.
fn try_f64_to_i64(f: f64) -> Option<i64> {
    if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
        Some(f as i64)
    } else {
        None
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Construction tests for field presence, value inversion, and range metadata.

    use super::*;

    /// Converts concise fixture pairs into the owned maps accepted by the builder.
    ///
    /// # Parameters
    ///
    /// - `pairs`: Field names borrowed from the test and owned attribute values.
    ///
    /// # Returns
    ///
    /// An owned map with copied field names, suitable for borrowing into a row
    /// slice for the duration of one test.
    fn make_attrs(pairs: Vec<(&str, AttributeValue)>) -> HashMap<String, AttributeValue> {
        pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
    }

    /// Proves that repeated strings map to every matching cluster row.
    #[test]
    fn test_build_simple_string_attrs() {
        let a0 = make_attrs(vec![("color", AttributeValue::String("red".into()))]);
        let a1 = make_attrs(vec![("color", AttributeValue::String("blue".into()))]);
        let a2 = make_attrs(vec![("color", AttributeValue::String("red".into()))]);

        let attrs: Vec<Option<&HashMap<String, AttributeValue>>> =
            vec![Some(&a0), Some(&a1), Some(&a2)];
        let index = build_cluster_bitmaps(&attrs);

        assert_eq!(index.vector_count, 3);
        let color = index.fields.get("color").unwrap();
        assert_eq!(color.present.len(), 3);

        let red = color.values.get(&BitmapKey("s:red".into())).unwrap();
        assert!(red.contains(0));
        assert!(!red.contains(1));
        assert!(red.contains(2));

        let blue = color.values.get(&BitmapKey("s:blue".into())).unwrap();
        assert!(!blue.contains(0));
        assert!(blue.contains(1));
        assert!(!blue.contains(2));
    }

    /// Proves that a missing field is excluded from its presence bitmap.
    #[test]
    fn test_build_null_handling() {
        let a0 = make_attrs(vec![("color", AttributeValue::String("red".into()))]);
        let a1 = make_attrs(vec![]); // no color field
        let a2 = make_attrs(vec![("color", AttributeValue::String("blue".into()))]);

        let attrs: Vec<Option<&HashMap<String, AttributeValue>>> =
            vec![Some(&a0), Some(&a1), Some(&a2)];
        let index = build_cluster_bitmaps(&attrs);

        let color = index.fields.get("color").unwrap();
        // Only positions 0 and 2 have color
        assert!(color.present.contains(0));
        assert!(!color.present.contains(1));
        assert!(color.present.contains(2));
    }

    /// Proves that numeric range metadata is ordered by value, not row order.
    #[test]
    fn test_build_numeric_sorted_keys() {
        let a0 = make_attrs(vec![("size", AttributeValue::Integer(30))]);
        let a1 = make_attrs(vec![("size", AttributeValue::Integer(10))]);
        let a2 = make_attrs(vec![("size", AttributeValue::Integer(20))]);

        let attrs: Vec<Option<&HashMap<String, AttributeValue>>> =
            vec![Some(&a0), Some(&a1), Some(&a2)];
        let index = build_cluster_bitmaps(&attrs);

        let size = index.fields.get("size").unwrap();
        assert_eq!(size.sorted_numeric_keys.len(), 3);

        // Should be sorted by f64 value: 10, 20, 30
        let sorted_vals: Vec<f64> = size
            .sorted_numeric_keys
            .iter()
            .map(|(bits, _)| f64::from_bits(*bits))
            .collect();
        assert_eq!(sorted_vals, vec![10.0, 20.0, 30.0]);
    }

    /// Proves that Boolean values occupy independent true and false bitmaps.
    #[test]
    fn test_build_bool_attrs() {
        let a0 = make_attrs(vec![("active", AttributeValue::Bool(true))]);
        let a1 = make_attrs(vec![("active", AttributeValue::Bool(false))]);
        let a2 = make_attrs(vec![("active", AttributeValue::Bool(true))]);

        let attrs: Vec<Option<&HashMap<String, AttributeValue>>> =
            vec![Some(&a0), Some(&a1), Some(&a2)];
        let index = build_cluster_bitmaps(&attrs);

        let active = index.fields.get("active").unwrap();
        let true_bm = active.values.get(&BitmapKey("b:true".into())).unwrap();
        let false_bm = active.values.get(&BitmapKey("b:false".into())).unwrap();
        assert!(true_bm.contains(0));
        assert!(true_bm.contains(2));
        assert!(false_bm.contains(1));
    }

    /// Proves that each string-list element maps to all containing rows.
    #[test]
    fn test_build_string_list_inverted() {
        let a0 = make_attrs(vec![(
            "tags",
            AttributeValue::StringList(vec!["a".into(), "b".into()]),
        )]);
        let a1 = make_attrs(vec![("tags", AttributeValue::StringList(vec!["a".into()]))]);
        let a2 = make_attrs(vec![(
            "tags",
            AttributeValue::StringList(vec!["b".into(), "c".into()]),
        )]);

        let attrs: Vec<Option<&HashMap<String, AttributeValue>>> =
            vec![Some(&a0), Some(&a1), Some(&a2)];
        let index = build_cluster_bitmaps(&attrs);

        let tags = index.fields.get("tags").unwrap();
        assert!(tags.is_list);

        let a_bm = tags.values.get(&BitmapKey("s:a".into())).unwrap();
        assert!(a_bm.contains(0));
        assert!(a_bm.contains(1));
        assert!(!a_bm.contains(2));

        let b_bm = tags.values.get(&BitmapKey("s:b".into())).unwrap();
        assert!(b_bm.contains(0));
        assert!(!b_bm.contains(1));
        assert!(b_bm.contains(2));

        let c_bm = tags.values.get(&BitmapKey("s:c".into())).unwrap();
        assert!(!c_bm.contains(0));
        assert!(!c_bm.contains(1));
        assert!(c_bm.contains(2));
    }

    /// Proves that integer-list membership uses the same inverted-index model.
    #[test]
    fn test_build_integer_list_inverted() {
        let a0 = make_attrs(vec![("scores", AttributeValue::IntegerList(vec![10, 20]))]);
        let a1 = make_attrs(vec![("scores", AttributeValue::IntegerList(vec![20, 30]))]);

        let attrs: Vec<Option<&HashMap<String, AttributeValue>>> = vec![Some(&a0), Some(&a1)];
        let index = build_cluster_bitmaps(&attrs);

        let scores = index.fields.get("scores").unwrap();
        assert!(scores.is_list);

        let bm_10 = scores.values.get(&BitmapKey("i:10".into())).unwrap();
        assert!(bm_10.contains(0));
        assert!(!bm_10.contains(1));

        let bm_20 = scores.values.get(&BitmapKey("i:20".into())).unwrap();
        assert!(bm_20.contains(0));
        assert!(bm_20.contains(1));
    }

    /// Proves that float-list elements retain exact bit-pattern lookup keys.
    #[test]
    fn test_build_float_list_inverted() {
        let a0 = make_attrs(vec![("ratios", AttributeValue::FloatList(vec![1.5, 2.5]))]);
        let a1 = make_attrs(vec![("ratios", AttributeValue::FloatList(vec![2.5, 3.5]))]);

        let attrs: Vec<Option<&HashMap<String, AttributeValue>>> = vec![Some(&a0), Some(&a1)];
        let index = build_cluster_bitmaps(&attrs);

        let ratios = index.fields.get("ratios").unwrap();
        assert!(ratios.is_list);

        let key_1_5 = BitmapKey::from_float_element(1.5);
        let bm = ratios.values.get(&key_1_5).unwrap();
        assert!(bm.contains(0));
        assert!(!bm.contains(1));

        let key_2_5 = BitmapKey::from_float_element(2.5);
        let bm = ratios.values.get(&key_2_5).unwrap();
        assert!(bm.contains(0));
        assert!(bm.contains(1));
    }

    /// Proves that an empty list is present without inventing an element value.
    #[test]
    fn test_build_empty_list() {
        let a0 = make_attrs(vec![("tags", AttributeValue::StringList(vec![]))]);

        let attrs: Vec<Option<&HashMap<String, AttributeValue>>> = vec![Some(&a0)];
        let index = build_cluster_bitmaps(&attrs);

        let tags = index.fields.get("tags").unwrap();
        // Present (field exists) but no element bitmaps
        assert!(tags.present.contains(0));
        assert!(tags.values.is_empty());
    }

    /// Proves that an over-limit field is omitted without removing eligible fields.
    #[test]
    fn test_build_high_cardinality_skip() {
        // Create vectors with >MAX_CARDINALITY distinct values
        let attr_maps: Vec<HashMap<String, AttributeValue>> = (0..MAX_CARDINALITY + 1)
            .map(|i| {
                let mut m = HashMap::new();
                m.insert("rare".to_string(), AttributeValue::String(format!("v{i}")));
                m.insert("common".to_string(), AttributeValue::String("same".into()));
                m
            })
            .collect();

        let attrs: Vec<Option<&HashMap<String, AttributeValue>>> =
            attr_maps.iter().map(Some).collect();
        let index = build_cluster_bitmaps(&attrs);

        // "rare" field should be excluded (>MAX_CARDINALITY distinct values)
        assert!(!index.fields.contains_key("rare"));
        // "common" field should still be present
        assert!(index.fields.contains_key("common"));
    }

    /// Proves that independently named fields can use every supported value type.
    #[test]
    fn test_build_mixed_types() {
        let a0 = make_attrs(vec![
            ("color", AttributeValue::String("red".into())),
            ("size", AttributeValue::Integer(10)),
            ("weight", AttributeValue::Float(1.5)),
            ("active", AttributeValue::Bool(true)),
            ("tags", AttributeValue::StringList(vec!["a".into()])),
        ]);

        let attrs: Vec<Option<&HashMap<String, AttributeValue>>> = vec![Some(&a0)];
        let index = build_cluster_bitmaps(&attrs);

        assert_eq!(index.fields.len(), 5);
        assert!(index.fields.contains_key("color"));
        assert!(index.fields.contains_key("size"));
        assert!(index.fields.contains_key("weight"));
        assert!(index.fields.contains_key("active"));
        assert!(index.fields.contains_key("tags"));
    }

    /// Proves that rows with no attribute object remain in the cluster universe.
    #[test]
    fn test_build_none_attributes() {
        // Some vectors have None attributes (no attributes at all)
        let a0 = make_attrs(vec![("color", AttributeValue::String("red".into()))]);

        let attrs: Vec<Option<&HashMap<String, AttributeValue>>> = vec![Some(&a0), None, None];
        let index = build_cluster_bitmaps(&attrs);

        assert_eq!(index.vector_count, 3);
        let color = index.fields.get("color").unwrap();
        assert!(color.present.contains(0));
        assert!(!color.present.contains(1));
        assert!(!color.present.contains(2));
    }
}
