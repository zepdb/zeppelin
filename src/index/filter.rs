//! Post-filter evaluation for vector search results.
//!
//! Filters are applied *after* the ANN scan so that the index remains
//! metric-only.  To compensate for filtered-out results, callers should
//! oversample by `config.oversample_factor` and then trim to `top_k`.

use std::collections::HashMap;

use crate::types::{AttributeValue, Filter};

/// Evaluate a filter predicate against a set of attributes.
///
/// Returns `true` if the attributes satisfy the filter.
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
            let config = crate::fts::types::FtsFieldConfig::default();
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
            let config = crate::fts::types::FtsFieldConfig::default();
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

/// Compare two `AttributeValue`s for equality.
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

/// Check if an attribute value contains another value.
///
/// For list types, checks element membership. For strings, checks substring.
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

/// Extract a numeric value from an `AttributeValue`.
fn attr_to_f64(attr: &AttributeValue) -> Option<f64> {
    match attr {
        AttributeValue::Integer(i) => Some(*i as f64),
        AttributeValue::Float(f) => Some(*f),
        _ => None,
    }
}

/// Given a desired `top_k` and an `oversample_factor`, compute how many
/// candidates to fetch before filtering.
#[inline]
pub fn oversampled_k(top_k: usize, oversample_factor: usize) -> usize {
    top_k.saturating_mul(oversample_factor).max(top_k)
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_eq_missing_field() {
        let attrs = make_attrs();
        let f = Filter::Eq {
            field: "nonexistent".into(),
            value: AttributeValue::String("x".into()),
        };
        assert!(!evaluate_filter(&f, &attrs));
    }

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

    #[test]
    fn test_oversampled_k() {
        assert_eq!(oversampled_k(10, 3), 30);
        assert_eq!(oversampled_k(0, 3), 0);
        assert_eq!(oversampled_k(10, 0), 10); // max(0, 10)
    }

    // --- New filter operator tests ---

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

    #[test]
    fn test_contains_float_list() {
        let attrs = make_attrs();
        let f = Filter::Contains {
            field: "ratios".into(),
            value: AttributeValue::Float(2.5),
        };
        assert!(evaluate_filter(&f, &attrs));
    }

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

    #[test]
    fn test_integer_list_eq() {
        let attrs = make_attrs();
        let f = Filter::Eq {
            field: "scores".into(),
            value: AttributeValue::Integer(10),
        };
        assert!(evaluate_filter(&f, &attrs)); // 10 is in [10, 20, 30]
    }

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
