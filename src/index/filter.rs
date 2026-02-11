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
pub fn evaluate_filter(
    filter: &Filter,
    attributes: &HashMap<String, AttributeValue>,
) -> bool {
    match filter {
        Filter::Eq { field, value } => {
            attributes
                .get(field)
                .map(|attr| attr_eq(attr, value))
                .unwrap_or(false)
        }

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

        Filter::And { filters } => filters.iter().all(|f| evaluate_filter(f, attributes)),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_attrs() -> HashMap<String, AttributeValue> {
        let mut m = HashMap::new();
        m.insert("color".to_string(), AttributeValue::String("red".to_string()));
        m.insert("size".to_string(), AttributeValue::Integer(42));
        m.insert("weight".to_string(), AttributeValue::Float(3.125));
        m.insert("active".to_string(), AttributeValue::Bool(true));
        m.insert(
            "tags".to_string(),
            AttributeValue::StringList(vec!["a".to_string(), "b".to_string()]),
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
}
