//! Exact metadata-filter evaluation shared by vector and lexical retrieval.
//!
//! [`crate::index::filter::evaluate_filter`] interprets the recursive
//! [`crate::types::Filter`] tree against one candidate's attributes. IVF and
//! hierarchical search use it as the exact post-filter after approximate
//! candidate selection; WAL and BM25 paths use the same semantics. Segment
//! bitmap indexes may preselect candidates, but this evaluator remains the
//! correctness reference for values or operators that require row attributes.
//!
//! ```text
//! ANN / BM25 / WAL candidate
//!            |
//!            | optional bitmap prefilter narrows candidate IDs
//!            v
//! load candidate attributes
//!            |
//!            v
//! evaluate recursive Filter exactly
//!            |
//!      keep or discard candidate
//! ```
//!
//! Filtering after approximate search can remove candidates needed to fill
//! `top_k`. [`crate::index::filter::oversampled_k`] asks the search stage for a
//! larger frontier before exact filtering, then callers trim the survivors.
//! This improves fill rate but is not a guarantee when a predicate is highly
//! selective.
//!
//! ## Missing fields and types
//!
//! Positive predicates such as equality, range, membership, and containment
//! reject a missing or incompatible field. Negative predicates (`not_eq` and
//! `not_in`) accept a missing field. Boolean nodes use ordinary logical
//! identities: an empty `And` is true and an empty `Or` is false.
//!
//! ## Rust concepts used here
//!
//! The evaluator borrows both the filter tree and attribute map, so recursive
//! calls allocate nothing and never take ownership of request data. Exhaustive
//! `match` means adding a new [`crate::types::Filter`] variant forces this module
//! to define its behavior. `let Some(value) = ... else` makes missing-field
//! exits explicit, similar to a Java null check or C lookup-status branch but
//! with non-null access after the pattern succeeds.

use std::collections::HashMap;

use crate::types::{AttributeValue, Filter};

/// Evaluates one recursive filter against one candidate's attributes.
///
/// # Parameters
///
/// - `filter`: Borrowed expression tree to interpret.
/// - `attributes`: Borrowed field-value map for one vector or document.
///
/// # Returns
///
/// Returns `true` exactly when the candidate satisfies the documented operator
/// semantics. Missing or type-incompatible fields are handled per operator and
/// never cause a fallback value to be invented.
///
/// # Performance
///
/// Cost is proportional to visited filter nodes plus list membership and token
/// work. Boolean `all` and `any` short-circuit. Token predicates allocate
/// tokenized query/document collections for each evaluation.
///
/// # Examples
///
/// `color == "red" AND size >= 40` accepts attributes containing red and 42,
/// rejects blue, and rejects a candidate with no `size` field.
///
/// # Rust Notes for Java/C Engineers
///
/// Matching `&Filter` borrows every variant field. Recursive calls receive
/// shared borrows rather than cloning boxed subtrees or maps. The compiler also
/// checks that every enum variant is considered.
pub fn evaluate_filter(filter: &Filter, attributes: &HashMap<String, AttributeValue>) -> bool {
    match filter {
        Filter::Eq { field, value } => attributes
            .get(field)
            .map(|attr| attr_eq(attr, value))
            .unwrap_or(false),

        Filter::NotEq { field, value } => attributes
            .get(field)
            .map(|attr| !attr_eq(attr, value))
            .unwrap_or(true),

        Filter::Range {
            field,
            gte,
            lte,
            gt,
            lt,
        } => {
            let Some(attr) = attributes.get(field) else {
                return false;
            };
            let num = match attr_to_f64(attr) {
                Some(n) => n,
                None => return false,
            };

            if let Some(min) = gte {
                if num < *min {
                    return false;
                }
            }
            if let Some(max) = lte {
                if num > *max {
                    return false;
                }
            }
            if let Some(min) = gt {
                if num <= *min {
                    return false;
                }
            }
            if let Some(max) = lt {
                if num >= *max {
                    return false;
                }
            }
            true
        }

        Filter::In { field, values } => {
            let Some(attr) = attributes.get(field) else {
                return false;
            };
            values.iter().any(|v| attr_eq(attr, v))
        }

        Filter::NotIn { field, values } => {
            let Some(attr) = attributes.get(field) else {
                return true;
            };
            !values.iter().any(|v| attr_eq(attr, v))
        }

        Filter::And { filters } => filters.iter().all(|f| evaluate_filter(f, attributes)),

        Filter::Or { filters } => filters.iter().any(|f| evaluate_filter(f, attributes)),

        Filter::Not { filter } => !evaluate_filter(filter, attributes),

        Filter::Contains { field, value } => {
            let Some(attr) = attributes.get(field) else {
                return false;
            };
            attr_contains(attr, value)
        }

        Filter::ContainsAllTokens { field, tokens } => {
            let Some(attr) = attributes.get(field) else {
                return false;
            };
            let text = match attr {
                AttributeValue::String(s) => s.as_str(),
                _ => return false,
            };
            let config = crate::fts::FtsFieldConfig::default();
            let doc_tokens: std::collections::HashSet<String> =
                crate::fts::tokenizer::tokenize_text(text, &config, false)
                    .into_iter()
                    .collect();
            let query_tokens: Vec<String> = tokens
                .iter()
                .flat_map(|t| crate::fts::tokenizer::tokenize_text(t, &config, false))
                .collect();
            query_tokens.iter().all(|t| doc_tokens.contains(t))
        }

        Filter::ContainsTokenSequence { field, tokens } => {
            let Some(attr) = attributes.get(field) else {
                return false;
            };
            let text = match attr {
                AttributeValue::String(s) => s.as_str(),
                _ => return false,
            };
            let config = crate::fts::FtsFieldConfig::default();
            let doc_tokens = crate::fts::tokenizer::tokenize_text(text, &config, false);
            let query_tokens: Vec<String> = tokens
                .iter()
                .flat_map(|t| crate::fts::tokenizer::tokenize_text(t, &config, false))
                .collect();
            if query_tokens.is_empty() {
                return true;
            }
            if query_tokens.len() > doc_tokens.len() {
                return false;
            }
            doc_tokens
                .windows(query_tokens.len())
                .any(|window| window == query_tokens.as_slice())
        }
    }
}

/// Compares attribute values using filter equality and membership coercions.
///
/// Integers compare with floats after conversion, and a scalar can compare
/// equal to an element of the matching list type. Other cross-type pairs are
/// unequal. Float equality uses an absolute `f64::EPSILON` threshold.
///
/// # Parameters
///
/// - `a`: Stored or query-side value.
/// - `b`: Value to compare symmetrically with `a`.
///
/// # Returns
///
/// Returns whether the pair satisfies these equality rules.
///
/// # Examples
///
/// Integer `3` equals float `3.0`, and string `"red"` equals an element of
/// string list `["blue", "red"]`; string `"3"` does not equal integer `3`.
fn attr_eq(a: &AttributeValue, b: &AttributeValue) -> bool {
    match (a, b) {
        (AttributeValue::String(sa), AttributeValue::String(sb)) => sa == sb,
        (AttributeValue::Integer(ia), AttributeValue::Integer(ib)) => ia == ib,
        (AttributeValue::Float(fa), AttributeValue::Float(fb)) => (fa - fb).abs() < f64::EPSILON,
        (AttributeValue::Bool(ba), AttributeValue::Bool(bb)) => ba == bb,
        // Allow integer-to-float comparison for convenience.
        (AttributeValue::Integer(i), AttributeValue::Float(f))
        | (AttributeValue::Float(f), AttributeValue::Integer(i)) => {
            (*i as f64 - f).abs() < f64::EPSILON
        }
        // Check membership in string lists.
        (AttributeValue::StringList(list), AttributeValue::String(s))
        | (AttributeValue::String(s), AttributeValue::StringList(list)) => list.contains(s),
        // Check membership in integer lists.
        (AttributeValue::IntegerList(list), AttributeValue::Integer(i))
        | (AttributeValue::Integer(i), AttributeValue::IntegerList(list)) => list.contains(i),
        // Check membership in float lists.
        (AttributeValue::FloatList(list), AttributeValue::Float(f))
        | (AttributeValue::Float(f), AttributeValue::FloatList(list)) => {
            list.iter().any(|v| (v - f).abs() < f64::EPSILON)
        }
        _ => false,
    }
}

/// Checks collection membership or string substring containment.
///
/// For list types, checks element membership. For strings, checks substring.
///
/// # Parameters
///
/// - `attr`: Stored container value.
/// - `value`: Scalar member or substring to find.
///
/// # Returns
///
/// Returns `true` for a supported matching container/member pair, otherwise
/// `false` for absence, different scalar types, or unsupported combinations.
///
/// # Examples
///
/// String `"a red widget"` contains `"widget"`; integer list `[10, 20]`
/// contains integer `20`.
fn attr_contains(attr: &AttributeValue, value: &AttributeValue) -> bool {
    match (attr, value) {
        (AttributeValue::StringList(list), AttributeValue::String(s)) => list.contains(s),
        (AttributeValue::IntegerList(list), AttributeValue::Integer(i)) => list.contains(i),
        (AttributeValue::FloatList(list), AttributeValue::Float(f)) => {
            list.iter().any(|v| (v - f).abs() < f64::EPSILON)
        }
        (AttributeValue::String(haystack), AttributeValue::String(needle)) => {
            haystack.contains(needle.as_str())
        }
        _ => false,
    }
}

/// Converts an integer or float attribute to the range evaluator's `f64` form.
///
/// # Parameters
///
/// - `attr`: Borrowed candidate value.
///
/// # Returns
///
/// Returns `Some` for integer and float values, or `None` for strings, booleans,
/// and lists.
///
/// # Examples
///
/// Integer `42` becomes `Some(42.0)`; string `"42"` remains non-numeric.
fn attr_to_f64(attr: &AttributeValue) -> Option<f64> {
    match attr {
        AttributeValue::Integer(i) => Some(*i as f64),
        AttributeValue::Float(f) => Some(*f),
        _ => None,
    }
}

/// Computes the candidate frontier requested before exact post-filtering.
///
/// # Parameters
///
/// - `top_k`: Desired final survivor count.
/// - `oversample_factor`: Multiplicative frontier expansion. Zero is treated as
///   one so the result never falls below `top_k`.
///
/// # Returns
///
/// Returns `max(top_k * factor, top_k)` with saturating multiplication, so an
/// overflow becomes `usize::MAX` rather than wrapping to a small frontier.
///
/// # Examples
///
/// `top_k = 10` and factor `3` request 30 candidates. Factor `0` still requests
/// 10.
///
/// # Rust Notes for Java/C Engineers
///
/// `saturating_mul` makes overflow behavior explicit. Java integer arithmetic
/// and ordinary unsigned C arithmetic wrap; Rust offers checked, wrapping, and
/// saturating operations so the algorithm can choose its intended policy.
#[inline]
pub fn oversampled_k(top_k: usize, oversample_factor: usize) -> usize {
    top_k.saturating_mul(oversample_factor).max(top_k)
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Unit tests pinning exact operator, missing-field, coercion, and nesting
    //! semantics for the reference evaluator.

    use super::*;

    /// Builds one attribute map covering every supported scalar and list type.
    fn make_attrs() -> HashMap<String, AttributeValue> {
        let mut m = HashMap::new();
        m.insert(
            "color".to_string(),
            AttributeValue::String("red".to_string()),
        );
        m.insert("size".to_string(), AttributeValue::Integer(42));
        m.insert("weight".to_string(), AttributeValue::Float(3.125));
        m.insert("active".to_string(), AttributeValue::Bool(true));
        m.insert(
            "tags".to_string(),
            AttributeValue::StringList(vec!["a".to_string(), "b".to_string()]),
        );
        m.insert(
            "scores".to_string(),
            AttributeValue::IntegerList(vec![10, 20, 30]),
        );
        m.insert(
            "ratios".to_string(),
            AttributeValue::FloatList(vec![1.5, 2.5, 3.5]),
        );
        m.insert(
            "description".to_string(),
            AttributeValue::String("a red widget".to_string()),
        );
        m
    }

    /// Verifies string equality accepts the same value and rejects another.
    #[test]
    fn test_eq_string() {
        let attrs = make_attrs();
        let f = Filter::Eq {
            field: "color".into(),
            value: AttributeValue::String("red".into()),
        };
        assert!(evaluate_filter(&f, &attrs));

        let f2 = Filter::Eq {
            field: "color".into(),
            value: AttributeValue::String("blue".into()),
        };
        assert!(!evaluate_filter(&f2, &attrs));
    }

    /// Verifies positive equality rejects a missing field.
    #[test]
    fn test_eq_missing_field() {
        let attrs = make_attrs();
        let f = Filter::Eq {
            field: "nonexistent".into(),
            value: AttributeValue::String("x".into()),
        };
        assert!(!evaluate_filter(&f, &attrs));
    }

    /// Verifies inclusive numeric range bounds for integer attributes.
    #[test]
    fn test_range_gte_lte() {
        let attrs = make_attrs();
        let f = Filter::Range {
            field: "size".into(),
            gte: Some(40.0),
            lte: Some(50.0),
            gt: None,
            lt: None,
        };
        assert!(evaluate_filter(&f, &attrs));

        let f2 = Filter::Range {
            field: "size".into(),
            gte: Some(43.0),
            lte: None,
            gt: None,
            lt: None,
        };
        assert!(!evaluate_filter(&f2, &attrs));
    }

    /// Verifies exclusive numeric range bounds for float attributes.
    #[test]
    fn test_range_gt_lt() {
        let attrs = make_attrs();
        let f = Filter::Range {
            field: "weight".into(),
            gte: None,
            lte: None,
            gt: Some(3.0),
            lt: Some(4.0),
        };
        assert!(evaluate_filter(&f, &attrs));
    }

    /// Verifies `in` accepts any matching candidate value.
    #[test]
    fn test_in_filter() {
        let attrs = make_attrs();
        let f = Filter::In {
            field: "color".into(),
            values: vec![
                AttributeValue::String("red".into()),
                AttributeValue::String("blue".into()),
            ],
        };
        assert!(evaluate_filter(&f, &attrs));

        let f2 = Filter::In {
            field: "color".into(),
            values: vec![AttributeValue::String("green".into())],
        };
        assert!(!evaluate_filter(&f2, &attrs));
    }

    /// Verifies conjunction accepts a candidate when every child matches.
    #[test]
    fn test_and_filter() {
        let attrs = make_attrs();
        let f = Filter::And {
            filters: vec![
                Filter::Eq {
                    field: "color".into(),
                    value: AttributeValue::String("red".into()),
                },
                Filter::Range {
                    field: "size".into(),
                    gte: Some(40.0),
                    lte: Some(50.0),
                    gt: None,
                    lt: None,
                },
            ],
        };
        assert!(evaluate_filter(&f, &attrs));
    }

    /// Verifies conjunction rejects a candidate when one child fails.
    #[test]
    fn test_and_filter_one_fails() {
        let attrs = make_attrs();
        let f = Filter::And {
            filters: vec![
                Filter::Eq {
                    field: "color".into(),
                    value: AttributeValue::String("red".into()),
                },
                Filter::Eq {
                    field: "active".into(),
                    value: AttributeValue::Bool(false),
                },
            ],
        };
        assert!(!evaluate_filter(&f, &attrs));
    }

    /// Verifies equality supports scalar membership in a matching string list.
    #[test]
    fn test_eq_string_list_membership() {
        let attrs = make_attrs();
        let f = Filter::Eq {
            field: "tags".into(),
            value: AttributeValue::String("a".into()),
        };
        assert!(evaluate_filter(&f, &attrs));

        let f2 = Filter::Eq {
            field: "tags".into(),
            value: AttributeValue::String("c".into()),
        };
        assert!(!evaluate_filter(&f2, &attrs));
    }

    /// Verifies frontier expansion, zero inputs, and the minimum `top_k` rule.
    #[test]
    fn test_oversampled_k() {
        assert_eq!(oversampled_k(10, 3), 30);
        assert_eq!(oversampled_k(0, 3), 0);
        assert_eq!(oversampled_k(10, 0), 10); // max(0, 10)
    }

    // --- New filter operator tests ---

    /// Verifies inequality is the inverse of equality for present fields.
    #[test]
    fn test_not_eq() {
        let attrs = make_attrs();
        let f = Filter::NotEq {
            field: "color".into(),
            value: AttributeValue::String("blue".into()),
        };
        assert!(evaluate_filter(&f, &attrs)); // red != blue → true

        let f2 = Filter::NotEq {
            field: "color".into(),
            value: AttributeValue::String("red".into()),
        };
        assert!(!evaluate_filter(&f2, &attrs)); // red != red → false
    }

    /// Verifies inequality accepts a missing field.
    #[test]
    fn test_not_eq_missing_field() {
        let attrs = make_attrs();
        let f = Filter::NotEq {
            field: "nonexistent".into(),
            value: AttributeValue::String("x".into()),
        };
        // Missing field → not equal → true
        assert!(evaluate_filter(&f, &attrs));
    }

    /// Verifies `not_in` rejects only membership in the supplied set.
    #[test]
    fn test_not_in() {
        let attrs = make_attrs();
        let f = Filter::NotIn {
            field: "color".into(),
            values: vec![
                AttributeValue::String("blue".into()),
                AttributeValue::String("green".into()),
            ],
        };
        assert!(evaluate_filter(&f, &attrs)); // red not in [blue, green] → true

        let f2 = Filter::NotIn {
            field: "color".into(),
            values: vec![
                AttributeValue::String("red".into()),
                AttributeValue::String("blue".into()),
            ],
        };
        assert!(!evaluate_filter(&f2, &attrs)); // red in [red, blue] → false
    }

    /// Verifies disjunction short-circuit semantics for matching and failing branches.
    #[test]
    fn test_or_filter() {
        let attrs = make_attrs();
        let f = Filter::Or {
            filters: vec![
                Filter::Eq {
                    field: "color".into(),
                    value: AttributeValue::String("blue".into()),
                },
                Filter::Eq {
                    field: "active".into(),
                    value: AttributeValue::Bool(true),
                },
            ],
        };
        assert!(evaluate_filter(&f, &attrs)); // color != blue but active == true

        let f2 = Filter::Or {
            filters: vec![
                Filter::Eq {
                    field: "color".into(),
                    value: AttributeValue::String("blue".into()),
                },
                Filter::Eq {
                    field: "active".into(),
                    value: AttributeValue::Bool(false),
                },
            ],
        };
        assert!(!evaluate_filter(&f2, &attrs)); // both false
    }

    /// Verifies logical negation reverses the child predicate.
    #[test]
    fn test_not_filter() {
        let attrs = make_attrs();
        let f = Filter::Not {
            filter: Box::new(Filter::Eq {
                field: "color".into(),
                value: AttributeValue::String("blue".into()),
            }),
        };
        assert!(evaluate_filter(&f, &attrs)); // NOT (color == blue) → true

        let f2 = Filter::Not {
            filter: Box::new(Filter::Eq {
                field: "color".into(),
                value: AttributeValue::String("red".into()),
            }),
        };
        assert!(!evaluate_filter(&f2, &attrs)); // NOT (color == red) → false
    }

    /// Verifies `contains` performs membership on string lists.
    #[test]
    fn test_contains_string_list() {
        let attrs = make_attrs();
        let f = Filter::Contains {
            field: "tags".into(),
            value: AttributeValue::String("a".into()),
        };
        assert!(evaluate_filter(&f, &attrs));

        let f2 = Filter::Contains {
            field: "tags".into(),
            value: AttributeValue::String("c".into()),
        };
        assert!(!evaluate_filter(&f2, &attrs));
    }

    /// Verifies `contains` performs membership on integer lists.
    #[test]
    fn test_contains_integer_list() {
        let attrs = make_attrs();
        let f = Filter::Contains {
            field: "scores".into(),
            value: AttributeValue::Integer(20),
        };
        assert!(evaluate_filter(&f, &attrs));

        let f2 = Filter::Contains {
            field: "scores".into(),
            value: AttributeValue::Integer(99),
        };
        assert!(!evaluate_filter(&f2, &attrs));
    }

    /// Verifies `contains` performs epsilon comparison on float lists.
    #[test]
    fn test_contains_float_list() {
        let attrs = make_attrs();
        let f = Filter::Contains {
            field: "ratios".into(),
            value: AttributeValue::Float(2.5),
        };
        assert!(evaluate_filter(&f, &attrs));
    }

    /// Verifies `contains` performs substring matching on strings.
    #[test]
    fn test_contains_substring() {
        let attrs = make_attrs();
        let f = Filter::Contains {
            field: "description".into(),
            value: AttributeValue::String("widget".into()),
        };
        assert!(evaluate_filter(&f, &attrs));

        let f2 = Filter::Contains {
            field: "description".into(),
            value: AttributeValue::String("blue".into()),
        };
        assert!(!evaluate_filter(&f2, &attrs));
    }

    /// Verifies equality supports scalar membership in an integer list.
    #[test]
    fn test_integer_list_eq() {
        let attrs = make_attrs();
        let f = Filter::Eq {
            field: "scores".into(),
            value: AttributeValue::Integer(10),
        };
        assert!(evaluate_filter(&f, &attrs)); // 10 is in [10, 20, 30]
    }

    /// Verifies recursive `And` and `Or` nodes compose without flattening.
    #[test]
    fn test_complex_nested_filter() {
        let attrs = make_attrs();
        // (color == "red" AND size > 40) OR (active == false)
        let f = Filter::Or {
            filters: vec![
                Filter::And {
                    filters: vec![
                        Filter::Eq {
                            field: "color".into(),
                            value: AttributeValue::String("red".into()),
                        },
                        Filter::Range {
                            field: "size".into(),
                            gt: Some(40.0),
                            gte: None,
                            lte: None,
                            lt: None,
                        },
                    ],
                },
                Filter::Eq {
                    field: "active".into(),
                    value: AttributeValue::Bool(false),
                },
            ],
        };
        assert!(evaluate_filter(&f, &attrs)); // First branch matches
    }
}
