//! Build cluster bitmap indexes from vector attributes.
//!
//! The build process iterates over each vector's attributes, constructing:
//! - A `present` bitmap per field (which positions have a non-null value)
//! - A `value` bitmap per distinct value per field (inverted index)
//! - For list types, each element gets its own value bitmap entry
//! - For numeric fields, sorted keys for range query support
//! - Fields with >MAX_CARDINALITY distinct values are excluded entirely

use roaring::RoaringBitmap;
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::types::AttributeValue;

use super::{AttributeBitmaps, BitmapKey, ClusterBitmapIndex, MAX_CARDINALITY};

/// Build a `ClusterBitmapIndex` from a slice of per-vector attributes.
///
/// Each element in `attrs` corresponds to a vector position (0-indexed).
/// `None` means the vector has no attributes at all; fields missing from
/// the HashMap are treated as null for that field.
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

/// Intermediate builder for a single field's bitmaps.
struct FieldBuilder {
    present: RoaringBitmap,
    values: BTreeMap<BitmapKey, RoaringBitmap>,
    numeric_keys: HashSet<u64>,
    is_list: bool,
    cardinality: usize,
    seen_keys: HashSet<BitmapKey>,
}

impl FieldBuilder {
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

/// Try to convert f64 back to i64 losslessly.
fn try_f64_to_i64(f: f64) -> Option<i64> {
    if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
        Some(f as i64)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_attrs(pairs: Vec<(&str, AttributeValue)>) -> HashMap<String, AttributeValue> {
        pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
    }

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
