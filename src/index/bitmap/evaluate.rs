//! Evaluates recursive metadata filters as set operations over cluster rows.
//!
//! IVF-Flat and hierarchical search call [`evaluate::evaluate_filter_bitmap`]
//! after loading a manifest-visible [`ClusterBitmapIndex`]. The returned rows can
//! be applied before distance computation, avoiding CPU work for vectors that
//! cannot match. This module performs no S3/MinIO reads and does not inspect raw
//! attributes; its answer is limited to facts encoded by [`build`].
//!
//! The return type deliberately distinguishes two outcomes:
//!
//! - `Some(bitmap)` is a complete bitmap answer. An empty bitmap proves that no
//!   cluster row matches.
//! - `None` means the bitmap representation cannot answer safely. Search must use
//!   the exact attribute evaluator rather than treating the set as empty.
//!
//! A missing field returns `None` because it may have exceeded
//! [`MAX_CARDINALITY`]; the index cannot distinguish that case from a field
//! absent on every row. String substring and full-text token filters also return
//! `None` because their text content/tokenization is not stored here. For `AND`,
//! `OR`, and `NOT`, one unavailable child makes the complete expression
//! unavailable so an incomplete candidate set never causes false negatives.
//!
//! ```text
//! Filter abstract syntax tree + ClusterBitmapIndex
//!                         |
//!                         v
//!                   evaluate recursively
//!                         |
//!        +----------------+----------------+
//!        |                |                |
//!        v                v                v
//!     AND: ∩            OR: ∪       NOT: universe - child
//!        |                |                |
//!        +----------------+----------------+
//!                         |
//!              +----------+-----------+
//!              |                      |
//!              v                      v
//!       Some(exact row set)      None (unsupported child,
//!              |                 unindexed field, or text)
//!              v                      |
//!       prefilter ANN work             v
//!                              inspect original attributes
//! ```
//!
//! ## Reading map
//!
//! 1. Read [`evaluate::evaluate_filter_bitmap`] as the exhaustive mapping from
//!    [`crate::types::Filter`] variants to set operations.
//! 2. Read `make_universe` for missing-field and logical-negation semantics.
//! 3. Read `value_to_key` beside [`BitmapKey`] to understand typed value
//!    lookup.
//! 4. Compare [`crate::index::filter::evaluate_filter`] for the exact path used
//!    when this module returns `None`.
//!
//! ## Missing-field semantics
//!
//! Within an indexed field, positive predicates (`Eq`, `In`, `Range`, and list
//! `Contains`) exclude rows where the field is missing. `NotEq` and `NotIn`
//! subtract matching rows from the full `0..vector_count` universe, so missing
//! rows match. A general `Not` uses the same universe rule. This mirrors the
//! exact evaluator's handling of absent attributes.
//!
//! ## Rust concepts used here
//!
//! The exhaustive `match` over [`crate::types::Filter`] forces new filter variants
//! to make an explicit bitmap-support decision at compile time. The `?` operator
//! is used on `Option`, not only on `Result`: inside a compound filter it
//! immediately propagates an unavailable child. Java would normally encode this
//! with a nullable return and manual checks; C might use a status code plus output
//! pointer. Rust keeps `None` distinct from an owned empty bitmap and prevents
//! either value from being dereferenced accidentally.
//!
//! TODO(doc): Confirm whether bitmap equality should mirror the exact evaluator's
//! integer/float coercion and epsilon-based float comparison. The current typed
//! keys distinguish integer `1` from float `1.0` and compare float bits exactly.
//!
//! TODO(doc): Confirm whether range predicates are intended to match elements of
//! `FloatList`. The builder currently records float-list elements as numeric range
//! keys, while the exact attribute evaluator accepts only scalar integers/floats.

use roaring::RoaringBitmap;
use std::collections::BTreeSet;

use crate::types::{AttributeValue, Filter};

use super::{BitmapKey, ClusterBitmapIndex};

/// Returns whether complete-field metadata guarantees bitmap evaluation.
///
/// This is a structural capability check only. It performs no artifact reads
/// and deliberately excludes `Contains` because field presence does not prove
/// that every cluster encoded the field as a list.
#[must_use]
pub fn filter_is_guaranteed_by_complete_bitmaps(
    filter: &Filter,
    complete_fields: &BTreeSet<String>,
) -> bool {
    match filter {
        Filter::Eq { field, .. }
        | Filter::NotEq { field, .. }
        | Filter::Range { field, .. }
        | Filter::In { field, .. }
        | Filter::NotIn { field, .. } => complete_fields.contains(field),
        Filter::And { filters } | Filter::Or { filters } => filters
            .iter()
            .all(|child| filter_is_guaranteed_by_complete_bitmaps(child, complete_fields)),
        Filter::Not { filter } => filter_is_guaranteed_by_complete_bitmaps(filter, complete_fields),
        Filter::Contains { .. }
        | Filter::ContainsAllTokens { .. }
        | Filter::ContainsTokenSequence { .. } => false,
    }
}

/// Evaluates one complete filter expression against a cluster bitmap index.
///
/// Leaf predicates look up typed value sets or combine numeric value sets.
/// Logical predicates recursively intersect, union, or subtract those sets. The
/// method is conservative about representation gaps: it returns `None` for the
/// complete expression if any required field or operation is unavailable.
///
/// # Parameters
///
/// - `filter`: Borrowed recursive predicate to evaluate; it is neither cloned nor
///   consumed.
/// - `index`: Borrowed bitmap sidecar for exactly one cluster. Its row count
///   defines the universe for negative predicates.
///
/// # Returns
///
/// `Some(bitmap)` containing every matching cluster-relative row when the index
/// can answer the complete expression. `Some(empty)` is a valid proof of no
/// matches. Returns `None` for an unindexed field, string substring containment,
/// full-text token predicates, or any compound expression containing such a
/// child.
///
/// # Consistency
///
/// This function trusts that every nested bitmap uses the same row ordering and
/// lies below `index.vector_count`. It does not establish that the sidecar belongs
/// to the current manifest-visible segment; the loading caller owns that storage
/// boundary. Negative predicates include rows missing the named field because
/// they subtract matches from the full cluster universe.
///
/// # Performance
///
/// Equality clones one compressed bitmap so the caller owns the answer. `IN`,
/// `AND`, and `OR` allocate a result and apply roaring set operations. `NOT`
/// constructs the cluster universe. Range evaluation visits every distinct
/// numeric key currently stored for the field and unions matching bitmaps; the
/// sorted representation is not used for early termination.
///
/// # Examples
///
/// For five rows where `color = red` at positions `{0, 2}`, `Eq(red)` returns
/// `Some({0, 2})` and `Not(Eq(red))` returns `Some({1, 3, 4})`. Querying an omitted
/// high-cardinality `user_id` field returns `None`, directing search to the exact
/// attribute path instead of incorrectly returning no rows.
///
/// # Rust Notes for Java/C Engineers
///
/// Both inputs are shared borrows. The returned roaring bitmap is owned: cloning
/// a stored value bitmap duplicates its compressed containers, while set
/// operations mutate only newly owned temporaries. Rust therefore prevents this
/// evaluation from changing the immutable loaded index or returning a dangling
/// view into it.
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

/// Creates the complete valid row-position set for one cluster.
///
/// # Parameters
///
/// - `vector_count`: Exclusive upper bound copied from the cluster index.
///
/// # Returns
///
/// An owned roaring bitmap containing every integer in `0..vector_count`; zero
/// returns an empty bitmap.
///
/// # Examples
///
/// A three-row cluster produces `{0, 1, 2}`. Subtracting a red-value bitmap
/// `{0, 2}` then implements a negative predicate as `{1}`.
fn make_universe(vector_count: u32) -> RoaringBitmap {
    (0..vector_count).collect()
}

/// Converts a query value into the key representation used by the builder.
///
/// Scalar strings, integers, and floats intentionally use the same constructors
/// as list elements because equality and containment can test membership in a
/// list. Other variants delegate to [`BitmapKey::from_attr`].
///
/// # Parameters
///
/// - `value`: Borrowed typed query operand.
///
/// # Returns
///
/// A newly owned lookup key; the attribute value remains available to the caller.
///
/// # Examples
///
/// Query string `"sale"` becomes `"s:sale"`, which can select either a scalar
/// string value or every string list containing `"sale"`.
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
    //! Truth-table and fallback-boundary tests for recursive bitmap evaluation.

    use super::*;
    use crate::index::bitmap::build::build_cluster_bitmaps;
    use std::collections::{BTreeSet, HashMap};

    #[test]
    fn bitmap_filter_guarantee_truth_table() {
        let complete_fields = BTreeSet::from(["color".to_string(), "size".to_string()]);
        let value = AttributeValue::String("red".into());
        let supported = [
            Filter::Eq {
                field: "color".into(),
                value: value.clone(),
            },
            Filter::NotEq {
                field: "color".into(),
                value: value.clone(),
            },
            Filter::Range {
                field: "size".into(),
                gte: Some(1.0),
                lte: None,
                gt: None,
                lt: None,
            },
            Filter::In {
                field: "color".into(),
                values: vec![value.clone()],
            },
            Filter::NotIn {
                field: "color".into(),
                values: vec![value.clone()],
            },
        ];
        for filter in &supported {
            assert!(filter_is_guaranteed_by_complete_bitmaps(
                filter,
                &complete_fields
            ));
        }

        assert!(filter_is_guaranteed_by_complete_bitmaps(
            &Filter::And {
                filters: supported.to_vec(),
            },
            &complete_fields
        ));
        assert!(filter_is_guaranteed_by_complete_bitmaps(
            &Filter::Or {
                filters: Vec::new(),
            },
            &complete_fields
        ));
        assert!(filter_is_guaranteed_by_complete_bitmaps(
            &Filter::Not {
                filter: Box::new(supported[0].clone()),
            },
            &complete_fields
        ));
        assert!(!filter_is_guaranteed_by_complete_bitmaps(
            &Filter::Eq {
                field: "missing".into(),
                value: value.clone(),
            },
            &complete_fields
        ));
        assert!(!filter_is_guaranteed_by_complete_bitmaps(
            &Filter::Contains {
                field: "color".into(),
                value: value.clone(),
            },
            &complete_fields
        ));
        assert!(!filter_is_guaranteed_by_complete_bitmaps(
            &Filter::ContainsAllTokens {
                field: "color".into(),
                tokens: vec!["red".into()],
            },
            &complete_fields
        ));
        assert!(!filter_is_guaranteed_by_complete_bitmaps(
            &Filter::ContainsTokenSequence {
                field: "color".into(),
                tokens: vec!["red".into()],
            },
            &complete_fields
        ));
    }

    /// Builds the shared five-row fixture used to compare predicate semantics.
    ///
    /// The rows match the bitmap TLA+ model:
    ///
    /// - row 0: `color=red`, `size=1`, `tags=[a,b]`;
    /// - row 1: `color=blue`, `size=2`, `tags=[a]`;
    /// - row 2: `color=red`, `size=3`, `tags=[]`;
    /// - row 3: missing `color`, `size=2`, `tags=[b]`;
    /// - row 4: `color=green`, missing `size`, `tags=[a,b]`.
    ///
    /// # Returns
    ///
    /// An owned index whose presence sets exercise both missing fields and a
    /// present empty list.
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

    /// Expands a compressed bitmap into ordered positions for readable assertions.
    ///
    /// # Parameters
    ///
    /// - `bm`: Borrowed result bitmap.
    ///
    /// # Returns
    ///
    /// Ascending row positions owned by the test.
    fn bm_to_set(bm: &RoaringBitmap) -> Vec<u32> {
        bm.iter().collect()
    }

    // --- Eq tests ---

    /// Proves equality selects every row carrying the requested scalar value.
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

    /// Proves an indexed but unknown value yields `Some(empty)`, not fallback.
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

    /// Proves inequality includes rows where the compared field is missing.
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

    /// Proves inclusive lower and upper range bounds union all matching values.
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

    /// Proves an omitted upper bound leaves a numeric range open-ended.
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

    /// Proves exclusive bounds reject values exactly equal to either endpoint.
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

    /// Proves `IN` unions each requested value bitmap.
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

    /// Proves `NOT IN` subtracts the union and therefore includes missing fields.
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

    /// Proves conjunction intersects child candidate sets.
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

    /// Proves disjunction unions child candidate sets.
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

    /// Proves logical negation subtracts a child set from the full universe.
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

    /// Proves `Contains` performs element lookup for list-valued fields.
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

    /// Proves string substring containment requests exact attribute evaluation.
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

    /// Proves a wholly unindexed field requests fallback rather than proving empty.
    #[test]
    fn test_eval_missing_attribute_returns_none() {
        let index = build_test_index();
        let filter = Filter::Eq {
            field: "nonexistent".into(),
            value: AttributeValue::String("x".into()),
        };
        assert!(evaluate_filter_bitmap(&filter, &index).is_none());
    }

    /// Proves nested unions and intersections preserve recursive filter structure.
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

    /// Exercises the builder/evaluator boundary for scalar equality.
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

    /// Exercises the builder/evaluator boundary for an exact numeric range value.
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

    /// Exercises build and evaluation for a compound scalar/range/list predicate.
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

    /// Compares supported bitmap predicates with row-by-row exact evaluation.
    ///
    /// The fixture checks equality, range, membership, and list containment at
    /// every row; any bitmap false positive or false negative fails with the
    /// filter and position that diverged.
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

    /// Proves one unavailable child makes an entire conjunction unavailable.
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

    /// Proves disjunction also propagates unavailable children conservatively.
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
