//! Evaluate filters against bitmap indexes.
//!
//! Returns `Option<RoaringBitmap>`:
//! - `Some(bitmap)` — the set of matching vector positions
//! - `None` — the filter cannot be evaluated via bitmap (fall back to post-filter)
//!
//! `None` is returned when:
//! - The field is not in the bitmap index
//! - Contains is used on a String field (substring match)
//! - A compound filter has a sub-filter that returns `None`

use roaring::RoaringBitmap;

use crate::types::{AttributeValue, Filter};

use super::{BitmapKey, ClusterBitmapIndex};

/// Evaluate a filter against a cluster's bitmap index.
///
/// Returns `Some(bitmap)` with the matching vector positions, or `None`
/// if the filter cannot be resolved via bitmaps (caller must fall back
/// to post-filter evaluation).
pub fn evaluate_filter_bitmap(
    filter: &Filter,
    index: &ClusterBitmapIndex,
) -> Option<RoaringBitmap> {
    let universe = make_universe(index.vector_count);

    match filter {
        Filter::Eq { field, value } => {
            let field_bitmaps = index.fields.get(field)?;
            let key = value_to_key(value);
            Some(field_bitmaps.values.get(&key).cloned().unwrap_or_default())
        }

        Filter::NotEq { field, value } => {
            let field_bitmaps = index.fields.get(field)?;
            let key = value_to_key(value);
            let eq_bitmap = field_bitmaps.values.get(&key).cloned().unwrap_or_default();
            // NotEq: universe minus matching (includes nulls, per evaluate_filter semantics)
            Some(&universe - &eq_bitmap)
        }

        Filter::Range {
            field,
            gte,
            lte,
            gt,
            lt,
        } => {
            let field_bitmaps = index.fields.get(field)?;
            if field_bitmaps.sorted_numeric_keys.is_empty() {
                // No numeric values for this field — empty result
                return Some(RoaringBitmap::new());
            }

            let mut result = RoaringBitmap::new();
            for (bits, key) in &field_bitmaps.sorted_numeric_keys {
                let val = f64::from_bits(*bits);

                if let Some(min) = gte {
                    if val < *min {
                        continue;
                    }
                }
                if let Some(max) = lte {
                    if val > *max {
                        continue;
                    }
                }
                if let Some(min) = gt {
                    if val <= *min {
                        continue;
                    }
                }
                if let Some(max) = lt {
                    if val >= *max {
                        continue;
                    }
                }

                if let Some(bm) = field_bitmaps.values.get(key) {
                    result |= bm;
                }
            }
            Some(result)
        }

        Filter::In { field, values } => {
            let field_bitmaps = index.fields.get(field)?;
            let mut result = RoaringBitmap::new();
            for v in values {
                let key = value_to_key(v);
                if let Some(bm) = field_bitmaps.values.get(&key) {
                    result |= bm;
                }
            }
            Some(result)
        }

        Filter::NotIn { field, values } => {
            let field_bitmaps = index.fields.get(field)?;
            let mut in_bitmap = RoaringBitmap::new();
            for v in values {
                let key = value_to_key(v);
                if let Some(bm) = field_bitmaps.values.get(&key) {
                    in_bitmap |= bm;
                }
            }
            // NotIn: universe minus union of matching values
            Some(&universe - &in_bitmap)
        }

        Filter::And { filters } => {
            let mut result = universe.clone();
            for f in filters {
                let sub = evaluate_filter_bitmap(f, index)?; // None propagates
                result &= &sub;
            }
            Some(result)
        }

        Filter::Or { filters } => {
            let mut result = RoaringBitmap::new();
            for f in filters {
                let sub = evaluate_filter_bitmap(f, index)?; // None propagates
                result |= &sub;
            }
            Some(result)
        }

        Filter::Not { filter } => {
            let sub = evaluate_filter_bitmap(filter, index)?;
            Some(&universe - &sub)
        }

        Filter::Contains { field, value } => {
            let field_bitmaps = index.fields.get(field)?;

            // Contains on list fields: look up the element in the inverted index
            if field_bitmaps.is_list {
                let key = value_to_key(value);
                Some(field_bitmaps.values.get(&key).cloned().unwrap_or_default())
            } else {
                // Contains on string field (substring match) — can't do via bitmap
                None
            }
        }

        // FTS token filters require tokenization — fall back to post-filter
        Filter::ContainsAllTokens { .. } | Filter::ContainsTokenSequence { .. } => None,
    }
}

/// Create a universe bitmap containing positions 0..vector_count.
fn make_universe(vector_count: u32) -> RoaringBitmap {
    (0..vector_count).collect()
}

/// Convert an AttributeValue to a BitmapKey for lookup.
fn value_to_key(value: &AttributeValue) -> BitmapKey {
    match value {
        AttributeValue::String(s) => BitmapKey::from_string_element(s),
        AttributeValue::Integer(i) => BitmapKey::from_integer_element(*i),
        AttributeValue::Float(f) => BitmapKey::from_float_element(*f),
        _ => BitmapKey::from_attr(value),
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::bitmap::build::build_cluster_bitmaps;
    use std::collections::HashMap;

    /// Build a test index with 5 vectors matching the TLA+ spec data:
    /// - vec 0: color=red, size=1, tags=[a,b]
    /// - vec 1: color=blue, size=2, tags=[a]
    /// - vec 2: color=red, size=3, tags=[]
    /// - vec 3: color=NULL, size=2, tags=[b]
    /// - vec 4: color=green, size=NULL, tags=[a,b]
    fn build_test_index() -> ClusterBitmapIndex {
        let a0 = [
            ("color".to_string(), AttributeValue::String("red".into())),
            ("size".to_string(), AttributeValue::Integer(1)),
            (
                "tags".to_string(),
                AttributeValue::StringList(vec!["a".into(), "b".into()]),
            ),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();

        let a1 = [
            ("color".to_string(), AttributeValue::String("blue".into())),
            ("size".to_string(), AttributeValue::Integer(2)),
            (
                "tags".to_string(),
                AttributeValue::StringList(vec!["a".into()]),
            ),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();

        let a2 = [
            ("color".to_string(), AttributeValue::String("red".into())),
            ("size".to_string(), AttributeValue::Integer(3)),
            ("tags".to_string(), AttributeValue::StringList(vec![])),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();

        let a3 = [
            // No color (null)
            ("size".to_string(), AttributeValue::Integer(2)),
            (
                "tags".to_string(),
                AttributeValue::StringList(vec!["b".into()]),
            ),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();

        let a4 = [
            ("color".to_string(), AttributeValue::String("green".into())),
            // No size (null)
            (
                "tags".to_string(),
                AttributeValue::StringList(vec!["a".into(), "b".into()]),
            ),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();

        let attrs: Vec<Option<&HashMap<String, AttributeValue>>> =
            vec![Some(&a0), Some(&a1), Some(&a2), Some(&a3), Some(&a4)];
        build_cluster_bitmaps(&attrs)
    }

    fn bm_to_set(bm: &RoaringBitmap) -> Vec<u32> {
        bm.iter().collect()
    }

    // --- Eq tests ---

    #[test]
    fn test_eval_eq_match() {
        let index = build_test_index();
        let filter = Filter::Eq {
            field: "color".into(),
            value: AttributeValue::String("red".into()),
        };
        let result = evaluate_filter_bitmap(&filter, &index).unwrap();
        assert_eq!(bm_to_set(&result), vec![0, 2]);
    }

    #[test]
    fn test_eval_eq_no_match() {
        let index = build_test_index();
        let filter = Filter::Eq {
            field: "color".into(),
            value: AttributeValue::String("yellow".into()),
        };
        let result = evaluate_filter_bitmap(&filter, &index).unwrap();
        assert!(result.is_empty());
    }

    // --- NotEq tests ---

    #[test]
    fn test_eval_not_eq() {
        let index = build_test_index();
        let filter = Filter::NotEq {
            field: "color".into(),
            value: AttributeValue::String("red".into()),
        };
        let result = evaluate_filter_bitmap(&filter, &index).unwrap();
        // Universe(0..5) minus red(0,2) = {1,3,4}
        // Includes vec 3 which has null color (matches NotEq semantics)
        assert_eq!(bm_to_set(&result), vec![1, 3, 4]);
    }

    // --- Range tests ---

    #[test]
    fn test_eval_range_gte_lte() {
        let index = build_test_index();
        let filter = Filter::Range {
            field: "size".into(),
            gte: Some(1.0),
            lte: Some(2.0),
            gt: None,
            lt: None,
        };
        let result = evaluate_filter_bitmap(&filter, &index).unwrap();
        // size=1 → vec 0, size=2 → vec 1,3
        assert_eq!(bm_to_set(&result), vec![0, 1, 3]);
    }

    #[test]
    fn test_eval_range_open_ended() {
        let index = build_test_index();
        let filter = Filter::Range {
            field: "size".into(),
            gte: Some(2.0),
            lte: None,
            gt: None,
            lt: None,
        };
        let result = evaluate_filter_bitmap(&filter, &index).unwrap();
        // size>=2 → vec 1,2,3
        assert_eq!(bm_to_set(&result), vec![1, 2, 3]);
    }

    #[test]
    fn test_eval_range_gt_lt() {
        let index = build_test_index();
        let filter = Filter::Range {
            field: "size".into(),
            gte: None,
            lte: None,
            gt: Some(1.0),
            lt: Some(3.0),
        };
        let result = evaluate_filter_bitmap(&filter, &index).unwrap();
        // size > 1 and size < 3 → size=2 → vec 1,3
        assert_eq!(bm_to_set(&result), vec![1, 3]);
    }

    // --- In / NotIn tests ---

    #[test]
    fn test_eval_in() {
        let index = build_test_index();
        let filter = Filter::In {
            field: "color".into(),
            values: vec![
                AttributeValue::String("red".into()),
                AttributeValue::String("blue".into()),
            ],
        };
        let result = evaluate_filter_bitmap(&filter, &index).unwrap();
        // red(0,2) ∪ blue(1) = {0,1,2}
        assert_eq!(bm_to_set(&result), vec![0, 1, 2]);
    }

    #[test]
    fn test_eval_not_in() {
        let index = build_test_index();
        let filter = Filter::NotIn {
            field: "color".into(),
            values: vec![
                AttributeValue::String("red".into()),
                AttributeValue::String("blue".into()),
            ],
        };
        let result = evaluate_filter_bitmap(&filter, &index).unwrap();
        // Universe(0..5) minus (red(0,2) ∪ blue(1)) = {3,4}
        assert_eq!(bm_to_set(&result), vec![3, 4]);
    }

    // --- And / Or / Not tests ---

    #[test]
    fn test_eval_and() {
        let index = build_test_index();
        let filter = Filter::And {
            filters: vec![
                Filter::Eq {
                    field: "color".into(),
                    value: AttributeValue::String("red".into()),
                },
                Filter::Range {
                    field: "size".into(),
                    gte: Some(1.0),
                    lte: Some(2.0),
                    gt: None,
                    lt: None,
                },
            ],
        };
        let result = evaluate_filter_bitmap(&filter, &index).unwrap();
        // red(0,2) ∩ size_1_2(0,1,3) = {0}
        assert_eq!(bm_to_set(&result), vec![0]);
    }

    #[test]
    fn test_eval_or() {
        let index = build_test_index();
        let filter = Filter::Or {
            filters: vec![
                Filter::Eq {
                    field: "color".into(),
                    value: AttributeValue::String("red".into()),
                },
                Filter::Eq {
                    field: "color".into(),
                    value: AttributeValue::String("blue".into()),
                },
            ],
        };
        let result = evaluate_filter_bitmap(&filter, &index).unwrap();
        assert_eq!(bm_to_set(&result), vec![0, 1, 2]);
    }

    #[test]
    fn test_eval_not() {
        let index = build_test_index();
        let filter = Filter::Not {
            filter: Box::new(Filter::Eq {
                field: "color".into(),
                value: AttributeValue::String("red".into()),
            }),
        };
        let result = evaluate_filter_bitmap(&filter, &index).unwrap();
        // Universe minus red = {1,3,4}
        assert_eq!(bm_to_set(&result), vec![1, 3, 4]);
    }

    // --- Contains tests ---

    #[test]
    fn test_eval_contains_list() {
        let index = build_test_index();
        let filter = Filter::Contains {
            field: "tags".into(),
            value: AttributeValue::String("a".into()),
        };
        let result = evaluate_filter_bitmap(&filter, &index).unwrap();
        // tags with "a": vec 0,1,4
        assert_eq!(bm_to_set(&result), vec![0, 1, 4]);
    }

    #[test]
    fn test_eval_contains_string_returns_none() {
        // Build index with a non-list string field
        let a0 = [(
            "desc".to_string(),
            AttributeValue::String("hello world".into()),
        )]
        .into_iter()
        .collect::<HashMap<_, _>>();
        let attrs: Vec<Option<&HashMap<String, AttributeValue>>> = vec![Some(&a0)];
        let index = build_cluster_bitmaps(&attrs);

        let filter = Filter::Contains {
            field: "desc".into(),
            value: AttributeValue::String("hello".into()),
        };
        // Substring match on non-list field → None
        assert!(evaluate_filter_bitmap(&filter, &index).is_none());
    }

    #[test]
    fn test_eval_missing_attribute_returns_none() {
        let index = build_test_index();
        let filter = Filter::Eq {
            field: "nonexistent".into(),
            value: AttributeValue::String("x".into()),
        };
        assert!(evaluate_filter_bitmap(&filter, &index).is_none());
    }

    #[test]
    fn test_eval_nested_compound() {
        let index = build_test_index();
        // And(Or(Eq(color,red), Eq(color,blue)), Range(size, 2, 3))
        let filter = Filter::And {
            filters: vec![
                Filter::Or {
                    filters: vec![
                        Filter::Eq {
                            field: "color".into(),
                            value: AttributeValue::String("red".into()),
                        },
                        Filter::Eq {
                            field: "color".into(),
                            value: AttributeValue::String("blue".into()),
                        },
                    ],
                },
                Filter::Range {
                    field: "size".into(),
                    gte: Some(2.0),
                    lte: Some(3.0),
                    gt: None,
                    lt: None,
                },
            ],
        };
        let result = evaluate_filter_bitmap(&filter, &index).unwrap();
        // Or(red(0,2), blue(1)) = {0,1,2}
        // Range size 2-3 = {1,2,3}
        // And = {1,2}
        assert_eq!(bm_to_set(&result), vec![1, 2]);
    }

    // --- Cross-module verification: build then evaluate ---

    #[test]
    fn test_build_then_evaluate_eq() {
        let index = build_test_index();
        let filter = Filter::Eq {
            field: "color".into(),
            value: AttributeValue::String("green".into()),
        };
        let result = evaluate_filter_bitmap(&filter, &index).unwrap();
        assert_eq!(bm_to_set(&result), vec![4]);
    }

    #[test]
    fn test_build_then_evaluate_range() {
        let index = build_test_index();
        let filter = Filter::Range {
            field: "size".into(),
            gte: Some(3.0),
            lte: Some(3.0),
            gt: None,
            lt: None,
        };
        let result = evaluate_filter_bitmap(&filter, &index).unwrap();
        assert_eq!(bm_to_set(&result), vec![2]);
    }

    #[test]
    fn test_build_then_evaluate_compound() {
        let index = build_test_index();
        // (color = red OR size >= 2) AND tags contains "b"
        let filter = Filter::And {
            filters: vec![
                Filter::Or {
                    filters: vec![
                        Filter::Eq {
                            field: "color".into(),
                            value: AttributeValue::String("red".into()),
                        },
                        Filter::Range {
                            field: "size".into(),
                            gte: Some(2.0),
                            lte: None,
                            gt: None,
                            lt: None,
                        },
                    ],
                },
                Filter::Contains {
                    field: "tags".into(),
                    value: AttributeValue::String("b".into()),
                },
            ],
        };
        let result = evaluate_filter_bitmap(&filter, &index).unwrap();
        // Or(red(0,2), size>=2(1,2,3)) = {0,1,2,3}
        // tags_b = {0,3,4}
        // And = {0,3}
        assert_eq!(bm_to_set(&result), vec![0, 3]);
    }

    #[test]
    fn test_bitmap_matches_post_filter_exhaustive() {
        use crate::index::filter::evaluate_filter;

        let a0: HashMap<String, AttributeValue> = [
            ("color".to_string(), AttributeValue::String("red".into())),
            ("size".to_string(), AttributeValue::Integer(1)),
            (
                "tags".to_string(),
                AttributeValue::StringList(vec!["a".into(), "b".into()]),
            ),
        ]
        .into_iter()
        .collect();

        let a1: HashMap<String, AttributeValue> = [
            ("color".to_string(), AttributeValue::String("blue".into())),
            ("size".to_string(), AttributeValue::Integer(2)),
            (
                "tags".to_string(),
                AttributeValue::StringList(vec!["a".into()]),
            ),
        ]
        .into_iter()
        .collect();

        let a2: HashMap<String, AttributeValue> = [
            ("color".to_string(), AttributeValue::String("red".into())),
            ("size".to_string(), AttributeValue::Integer(3)),
            ("tags".to_string(), AttributeValue::StringList(vec![])),
        ]
        .into_iter()
        .collect();

        let all_attrs = [&a0, &a1, &a2];
        let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
            all_attrs.iter().map(|a| Some(*a)).collect();
        let index = build_cluster_bitmaps(&attr_refs);

        // Test several filters against both bitmap and brute-force
        let filters = vec![
            Filter::Eq {
                field: "color".into(),
                value: AttributeValue::String("red".into()),
            },
            Filter::Range {
                field: "size".into(),
                gte: Some(2.0),
                lte: None,
                gt: None,
                lt: None,
            },
            Filter::In {
                field: "color".into(),
                values: vec![
                    AttributeValue::String("red".into()),
                    AttributeValue::String("blue".into()),
                ],
            },
            Filter::Contains {
                field: "tags".into(),
                value: AttributeValue::String("a".into()),
            },
        ];

        for filter in &filters {
            if let Some(bitmap_result) = evaluate_filter_bitmap(filter, &index) {
                // Compare against brute-force for each position
                for (pos, attrs) in all_attrs.iter().enumerate() {
                    let brute = evaluate_filter(filter, attrs);
                    let bitmap = bitmap_result.contains(pos as u32);
                    assert_eq!(
                        brute, bitmap,
                        "Mismatch at pos {pos} for filter {filter:?}: brute={brute}, bitmap={bitmap}"
                    );
                }
            }
        }
    }

    // --- And/Or with None sub-filter ---

    #[test]
    fn test_eval_and_with_none_subfilter() {
        let index = build_test_index();
        // And(Eq(color,red), Eq(nonexistent, x))
        // Second sub-filter returns None → And returns None
        let filter = Filter::And {
            filters: vec![
                Filter::Eq {
                    field: "color".into(),
                    value: AttributeValue::String("red".into()),
                },
                Filter::Eq {
                    field: "nonexistent".into(),
                    value: AttributeValue::String("x".into()),
                },
            ],
        };
        assert!(evaluate_filter_bitmap(&filter, &index).is_none());
    }

    #[test]
    fn test_eval_or_with_none_subfilter() {
        let index = build_test_index();
        // Or(Eq(color,red), Eq(nonexistent, x))
        // Second sub-filter returns None → Or returns None (conservative)
        let filter = Filter::Or {
            filters: vec![
                Filter::Eq {
                    field: "color".into(),
                    value: AttributeValue::String("red".into()),
                },
                Filter::Eq {
                    field: "nonexistent".into(),
                    value: AttributeValue::String("x".into()),
                },
            ],
        };
        assert!(evaluate_filter_bitmap(&filter, &index).is_none());
    }
}
