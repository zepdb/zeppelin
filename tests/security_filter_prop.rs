//! Property coverage for server-owned read constraints.
//!
//! The corpus is fixed so failures shrink to the filter pair that changed the
//! visible ID set. Filter evaluation itself always goes through Zeppelin's
//! production evaluator; the expected intersection is computed independently
//! from the two unconjoined result sets.

use std::collections::{BTreeSet, HashMap};

use proptest::prelude::*;
use zeppelin::index::filter::evaluate_filter;
use zeppelin::query::compile_effective_filter;
use zeppelin::security::{apply_field_mask, FieldMask};
use zeppelin::types::{AttributeValue, Filter};

#[derive(Debug)]
struct CorpusRow {
    id: &'static str,
    attributes: HashMap<String, AttributeValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum AttributeValueKind {
    String,
    Integer,
    Float,
    Bool,
    StringList,
    IntegerList,
    FloatList,
}

impl AttributeValueKind {
    const GENERATED: [Self; 7] = [
        Self::String,
        Self::Integer,
        Self::Float,
        Self::Bool,
        Self::StringList,
        Self::IntegerList,
        Self::FloatList,
    ];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum FilterLeafKind {
    Eq,
    NotEq,
    Range,
    In,
    NotIn,
    Contains,
    ContainsAllTokens,
    ContainsTokenSequence,
}

impl FilterLeafKind {
    const GENERATED: [Self; 8] = [
        Self::Eq,
        Self::NotEq,
        Self::Range,
        Self::In,
        Self::NotIn,
        Self::Contains,
        Self::ContainsAllTokens,
        Self::ContainsTokenSequence,
    ];
}

fn attributes(
    tenant: &str,
    color: &str,
    rank: i64,
    active: bool,
    tags: &[&str],
) -> HashMap<String, AttributeValue> {
    HashMap::from([
        (
            "tenant".to_string(),
            AttributeValue::String(tenant.to_string()),
        ),
        (
            "color".to_string(),
            AttributeValue::String(color.to_string()),
        ),
        ("rank".to_string(), AttributeValue::Integer(rank)),
        (
            "score".to_string(),
            AttributeValue::Float(rank as f64 + 0.25),
        ),
        ("active".to_string(), AttributeValue::Bool(active)),
        (
            "tags".to_string(),
            AttributeValue::StringList(tags.iter().map(|tag| (*tag).to_string()).collect()),
        ),
        (
            "ordinals".to_string(),
            AttributeValue::IntegerList(vec![rank, rank + 1]),
        ),
        (
            "weights".to_string(),
            AttributeValue::FloatList(vec![rank as f64, rank as f64 + 0.5]),
        ),
        (
            "text".to_string(),
            AttributeValue::String(format!("{tenant} {color} catalog item")),
        ),
        (
            "secret".to_string(),
            AttributeValue::String(format!("secret-{tenant}-{rank}")),
        ),
    ])
}

fn corpus() -> Vec<CorpusRow> {
    vec![
        CorpusRow {
            id: "a-red-0",
            attributes: attributes("a", "red", 0, true, &["new", "sale"]),
        },
        CorpusRow {
            id: "a-blue-1",
            attributes: attributes("a", "blue", 1, false, &["sale"]),
        },
        CorpusRow {
            id: "a-green-2",
            attributes: attributes("a", "green", 2, true, &["archive"]),
        },
        CorpusRow {
            id: "b-red-3",
            attributes: attributes("b", "red", 3, false, &["new"]),
        },
        CorpusRow {
            id: "b-blue-4",
            attributes: attributes("b", "blue", 4, true, &["featured", "sale"]),
        },
        CorpusRow {
            id: "b-green-5",
            attributes: attributes("b", "green", 5, false, &["archive", "featured"]),
        },
        CorpusRow {
            id: "c-red-6",
            attributes: attributes("c", "red", 6, true, &[]),
        },
        CorpusRow {
            id: "c-blue-7",
            attributes: attributes("c", "blue", 7, false, &["new", "archive"]),
        },
    ]
}

fn matching_ids(filter: &Filter, rows: &[CorpusRow]) -> BTreeSet<&'static str> {
    rows.iter()
        .filter(|row| evaluate_filter(filter, &row.attributes))
        .map(|row| row.id)
        .collect()
}

fn field() -> impl Strategy<Value = String> {
    prop::sample::select(vec![
        "tenant", "color", "rank", "score", "active", "tags", "ordinals", "weights", "text",
        "missing",
    ])
    .prop_map(str::to_string)
}

fn attribute_value() -> impl Strategy<Value = AttributeValue> {
    prop_oneof![
        prop::sample::select(vec!["a", "b", "c", "red", "blue", "green", "sale", "new",])
            .prop_map(|value| AttributeValue::String(value.to_string())),
        (-2_i64..=9).prop_map(AttributeValue::Integer),
        (-2.0_f64..=9.0).prop_map(AttributeValue::Float),
        any::<bool>().prop_map(AttributeValue::Bool),
        prop::collection::vec(
            prop::sample::select(vec!["a", "red", "sale", "archive"]).prop_map(str::to_string),
            0..=3,
        )
        .prop_map(AttributeValue::StringList),
        prop::collection::vec(-2_i64..=9, 0..=3).prop_map(AttributeValue::IntegerList),
        prop::collection::vec(-2.0_f64..=9.0, 0..=3).prop_map(AttributeValue::FloatList),
    ]
}

fn token_field() -> impl Strategy<Value = String> {
    prop::sample::select(vec!["tenant", "color", "text", "secret", "missing"])
        .prop_map(str::to_string)
}

fn tokens() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(
        prop::sample::select(vec!["a", "b", "red", "blue", "catalog", "item", "missing"])
            .prop_map(str::to_string),
        0..=3,
    )
}

fn leaf_filter() -> impl Strategy<Value = Filter> {
    prop_oneof![
        (field(), attribute_value()).prop_map(|(field, value)| Filter::Eq { field, value }),
        (field(), attribute_value()).prop_map(|(field, value)| Filter::NotEq { field, value }),
        (field(), prop::collection::vec(attribute_value(), 0..=3),)
            .prop_map(|(field, values)| Filter::In { field, values }),
        (field(), prop::collection::vec(attribute_value(), 0..=3),)
            .prop_map(|(field, values)| Filter::NotIn { field, values }),
        (
            field(),
            proptest::option::of(-2.0_f64..9.0),
            proptest::option::of(-2.0_f64..9.0),
        )
            .prop_map(|(field, gte, lt)| Filter::Range {
                field,
                gte,
                lte: None,
                gt: None,
                lt,
            }),
        (field(), attribute_value()).prop_map(|(field, value)| Filter::Contains { field, value }),
        (token_field(), tokens())
            .prop_map(|(field, tokens)| Filter::ContainsAllTokens { field, tokens }),
        (token_field(), tokens())
            .prop_map(|(field, tokens)| Filter::ContainsTokenSequence { field, tokens }),
    ]
}

fn arbitrary_filter() -> impl Strategy<Value = Filter> {
    leaf_filter().prop_recursive(3, 24, 3, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..=3).prop_map(|filters| Filter::And { filters }),
            prop::collection::vec(inner.clone(), 0..=3).prop_map(|filters| Filter::Or { filters }),
            inner.prop_map(|filter| Filter::Not {
                filter: Box::new(filter),
            }),
        ]
    })
}

fn mask_fields() -> impl Strategy<Value = BTreeSet<String>> {
    prop::collection::btree_set(
        prop::sample::select(vec![
            "tenant", "color", "rank", "score", "active", "tags", "ordinals", "weights", "text",
            "secret", "missing",
        ])
        .prop_map(str::to_string),
        0..=11,
    )
}

#[test]
fn generated_vocabulary_covers_every_filter_leaf_and_attribute_value() {
    assert_eq!(
        BTreeSet::from(AttributeValueKind::GENERATED),
        BTreeSet::from([
            AttributeValueKind::String,
            AttributeValueKind::Integer,
            AttributeValueKind::Float,
            AttributeValueKind::Bool,
            AttributeValueKind::StringList,
            AttributeValueKind::IntegerList,
            AttributeValueKind::FloatList,
        ]),
    );
    assert_eq!(
        BTreeSet::from(FilterLeafKind::GENERATED),
        BTreeSet::from([
            FilterLeafKind::Eq,
            FilterLeafKind::NotEq,
            FilterLeafKind::Range,
            FilterLeafKind::In,
            FilterLeafKind::NotIn,
            FilterLeafKind::Contains,
            FilterLeafKind::ContainsAllTokens,
            FilterLeafKind::ContainsTokenSequence,
        ]),
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Adding a caller predicate can only narrow a mandatory policy predicate.
    #[test]
    fn mandatory_and_caller_filter_is_exact_intersection(
        policy_filter in arbitrary_filter(),
        caller_filter in arbitrary_filter(),
    ) {
        let rows = corpus();
        let policy_ids = matching_ids(&policy_filter, &rows);
        let caller_ids = matching_ids(&caller_filter, &rows);
        let expected = policy_ids
            .intersection(&caller_ids)
            .copied()
            .collect::<BTreeSet<_>>();

        let effective = compile_effective_filter(
            Some(&policy_filter),
            Some(&caller_filter),
        )
        .expect("two supplied filters must produce an effective filter");
        let effective_ids = matching_ids(&effective, &rows);

        prop_assert!(
            effective_ids.is_subset(&policy_ids),
            "caller filter widened policy set: policy={policy_ids:?}, effective={effective_ids:?}",
        );
        prop_assert_eq!(effective_ids, expected);
    }

    /// A field denied by policy is absent from every projected corpus row.
    #[test]
    fn masked_fields_never_appear_in_projected_attributes(denied in mask_fields()) {
        let mask = if denied.is_empty() {
            FieldMask::default()
        } else {
            FieldMask::new(denied).expect("generated field names must form a valid mask")
        };

        for row in corpus() {
            let original = row.attributes.clone();
            let mut projected = row.attributes;
            apply_field_mask(&mask, &mut projected);

            for field in mask.denied_fields() {
                prop_assert!(
                    !projected.contains_key(field),
                    "masked field {field:?} remained in projected row {:?}",
                    row.id,
                );
            }
            for (field, value) in original {
                if !mask.denied_fields().contains(&field) {
                    prop_assert_eq!(projected.get(&field), Some(&value));
                }
            }
        }
    }
}
