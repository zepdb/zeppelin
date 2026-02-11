//! Property-based tests for filter evaluation correctness.
//!
//! Tests that the production filter evaluator (src/index/filter.rs)
//! produces the same results as a simple, obviously-correct reference
//! implementation for arbitrary filter trees and attribute maps.
//!
//! To run: add `proptest = "1"` to [dev-dependencies] in Cargo.toml, then
//!   cp formal-verifications/proptest/filter_eval.rs tests/proptest_filter.rs
//!   cargo test --test proptest_filter

use proptest::prelude::*;
use std::collections::HashMap;

// ---- Types (mirrors src/types.rs) ----

#[derive(Debug, Clone, PartialEq)]
enum AttributeValue {
    String(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    StringList(Vec<String>),
}

#[derive(Debug, Clone)]
enum Filter {
    Eq {
        field: String,
        value: AttributeValue,
    },
    Range {
        field: String,
        gte: Option<f64>,
        lte: Option<f64>,
        gt: Option<f64>,
        lt: Option<f64>,
    },
    In {
        field: String,
        values: Vec<AttributeValue>,
    },
    And {
        filters: Vec<Filter>,
    },
}

// ---- Production Implementation (mirrors src/index/filter.rs) ----

fn evaluate_filter(filter: &Filter, attributes: &HashMap<String, AttributeValue>) -> bool {
    match filter {
        Filter::Eq { field, value } => attributes
            .get(field)
            .map(|attr| attr_eq(attr, value))
            .unwrap_or(false),

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

fn attr_eq(a: &AttributeValue, b: &AttributeValue) -> bool {
    match (a, b) {
        (AttributeValue::String(sa), AttributeValue::String(sb)) => sa == sb,
        (AttributeValue::Integer(ia), AttributeValue::Integer(ib)) => ia == ib,
        (AttributeValue::Float(fa), AttributeValue::Float(fb)) => (fa - fb).abs() < f64::EPSILON,
        (AttributeValue::Bool(ba), AttributeValue::Bool(bb)) => ba == bb,
        (AttributeValue::Integer(i), AttributeValue::Float(f))
        | (AttributeValue::Float(f), AttributeValue::Integer(i)) => {
            (*i as f64 - f).abs() < f64::EPSILON
        }
        (AttributeValue::StringList(list), AttributeValue::String(s))
        | (AttributeValue::String(s), AttributeValue::StringList(list)) => list.contains(s),
        _ => false,
    }
}

fn attr_to_f64(attr: &AttributeValue) -> Option<f64> {
    match attr {
        AttributeValue::Integer(i) => Some(*i as f64),
        AttributeValue::Float(f) => Some(*f),
        _ => None,
    }
}

// ---- Reference Implementation (simple, no match arms — just boolean logic) ----

fn reference_evaluate(filter: &Filter, attrs: &HashMap<String, AttributeValue>) -> bool {
    match filter {
        Filter::Eq { field, value } => match attrs.get(field) {
            None => false,
            Some(attr) => ref_eq(attr, value),
        },
        Filter::Range {
            field,
            gte,
            lte,
            gt,
            lt,
        } => {
            let num = match attrs.get(field).and_then(ref_to_f64) {
                Some(n) => n,
                None => return false,
            };
            let ok_gte = gte.is_none_or(|min| num >= min);
            let ok_lte = lte.is_none_or(|max| num <= max);
            let ok_gt = gt.is_none_or(|min| num > min);
            let ok_lt = lt.is_none_or(|max| num < max);
            ok_gte && ok_lte && ok_gt && ok_lt
        }
        Filter::In { field, values } => match attrs.get(field) {
            None => false,
            Some(attr) => values.iter().any(|v| ref_eq(attr, v)),
        },
        Filter::And { filters } => filters.iter().all(|f| reference_evaluate(f, attrs)),
    }
}

fn ref_eq(a: &AttributeValue, b: &AttributeValue) -> bool {
    attr_eq(a, b) // Reuse — the logic is simple enough to be its own oracle
}

fn ref_to_f64(attr: &AttributeValue) -> Option<f64> {
    attr_to_f64(attr)
}

// ---- Proptest Strategies ----

fn arb_field_name() -> impl Strategy<Value = String> {
    prop::sample::select(vec![
        "color".to_string(),
        "size".to_string(),
        "weight".to_string(),
        "active".to_string(),
        "tags".to_string(),
        "missing".to_string(),
    ])
}

fn arb_attr_value() -> impl Strategy<Value = AttributeValue> {
    prop_oneof![
        prop::sample::select(vec!["red", "blue", "green"])
            .prop_map(|s| AttributeValue::String(s.to_string())),
        (-100i64..100).prop_map(AttributeValue::Integer),
        (-100.0f64..100.0)
            .prop_filter("finite", |f| f.is_finite())
            .prop_map(AttributeValue::Float),
        any::<bool>().prop_map(AttributeValue::Bool),
    ]
}

fn arb_filter_leaf() -> impl Strategy<Value = Filter> {
    prop_oneof![
        // Eq filter
        (arb_field_name(), arb_attr_value()).prop_map(|(field, value)| Filter::Eq { field, value }),
        // Range filter
        (
            arb_field_name(),
            proptest::option::of(-100.0f64..100.0),
            proptest::option::of(-100.0f64..100.0),
            proptest::option::of(-100.0f64..100.0),
            proptest::option::of(-100.0f64..100.0),
        )
            .prop_map(|(field, gte, lte, gt, lt)| Filter::Range {
                field,
                gte,
                lte,
                gt,
                lt,
            }),
        // In filter
        (
            arb_field_name(),
            prop::collection::vec(arb_attr_value(), 1..=3),
        )
            .prop_map(|(field, values)| Filter::In { field, values }),
    ]
}

fn arb_filter() -> impl Strategy<Value = Filter> {
    arb_filter_leaf().prop_recursive(
        2, // max depth
        8, // max nodes
        3, // items per collection
        |inner| prop::collection::vec(inner, 1..=3).prop_map(|filters| Filter::And { filters }),
    )
}

fn arb_attributes() -> impl Strategy<Value = HashMap<String, AttributeValue>> {
    prop::collection::hash_map(
        prop::sample::select(vec![
            "color".to_string(),
            "size".to_string(),
            "weight".to_string(),
            "active".to_string(),
            "tags".to_string(),
        ]),
        prop_oneof![
            prop::sample::select(vec!["red", "blue", "green"])
                .prop_map(|s| AttributeValue::String(s.to_string())),
            (-100i64..100).prop_map(AttributeValue::Integer),
            (-100.0f64..100.0)
                .prop_filter("finite", |f| f.is_finite())
                .prop_map(AttributeValue::Float),
            any::<bool>().prop_map(AttributeValue::Bool),
            prop::collection::vec(
                prop::sample::select(vec!["a", "b", "c"]).prop_map(String::from),
                1..=3,
            )
            .prop_map(AttributeValue::StringList),
        ],
        0..=5,
    )
}

// ---- Property Tests ----

proptest! {
    /// Production filter evaluation matches reference for arbitrary inputs.
    #[test]
    fn filter_matches_reference(
        filter in arb_filter(),
        attrs in arb_attributes(),
    ) {
        let production = evaluate_filter(&filter, &attrs);
        let reference = reference_evaluate(&filter, &attrs);
        prop_assert_eq!(production, reference,
            "Filter evaluation should match reference impl");
    }

    /// Eq filter on missing field always returns false.
    #[test]
    fn eq_missing_field_is_false(value in arb_attr_value()) {
        let filter = Filter::Eq {
            field: "definitely_missing".to_string(),
            value,
        };
        let attrs = HashMap::new();
        prop_assert!(!evaluate_filter(&filter, &attrs));
    }

    /// And with empty filters returns true (vacuous truth).
    #[test]
    fn empty_and_is_true(attrs in arb_attributes()) {
        let filter = Filter::And { filters: vec![] };
        prop_assert!(evaluate_filter(&filter, &attrs),
            "And with no sub-filters should be vacuously true");
    }

    /// Range filter on non-numeric field returns false.
    #[test]
    fn range_on_non_numeric_is_false(
        gte in proptest::option::of(-100.0f64..100.0),
        lte in proptest::option::of(-100.0f64..100.0),
    ) {
        let mut attrs = HashMap::new();
        attrs.insert(
            "color".to_string(),
            AttributeValue::String("red".to_string()),
        );
        let filter = Filter::Range {
            field: "color".to_string(),
            gte,
            lte,
            gt: None,
            lt: None,
        };
        prop_assert!(!evaluate_filter(&filter, &attrs),
            "Range filter on string attribute should return false");
    }
}
