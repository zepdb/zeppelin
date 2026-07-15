//! Pure helpers for applying server-owned observation constraints.

use std::collections::{BTreeSet, HashMap};

use crate::index::filter::evaluate_filter;
use crate::types::{AttributeValue, Filter};

use super::FieldMask;

/// Removes every policy-denied attribute from one response map.
///
/// The operation is intentionally subtractive: it never inserts replacement
/// values or null markers that could reveal whether a denied field existed.
pub fn apply_field_mask(mask: &FieldMask, attributes: &mut HashMap<String, AttributeValue>) {
    attributes.retain(|field, _| !mask.denied_fields().contains(field));
}

/// Return whether a filter observes any attribute denied by a field mask.
///
/// Field masks are authorization boundaries, not only response serializers.
/// A predicate over a denied field can reveal that field through membership,
/// counts, or destructive side effects even when response bodies omit it.
#[must_use]
pub fn filter_references_denied_field(filter: &Filter, denied: &BTreeSet<String>) -> bool {
    match filter {
        Filter::Eq { field, .. }
        | Filter::NotEq { field, .. }
        | Filter::Range { field, .. }
        | Filter::In { field, .. }
        | Filter::NotIn { field, .. }
        | Filter::Contains { field, .. }
        | Filter::ContainsAllTokens { field, .. }
        | Filter::ContainsTokenSequence { field, .. } => denied.contains(field),
        Filter::And { filters } | Filter::Or { filters } => filters
            .iter()
            .any(|child| filter_references_denied_field(child, denied)),
        Filter::Not { filter } => filter_references_denied_field(filter, denied),
    }
}

/// Returns whether submitted attributes definitely satisfy a mandatory write scope.
///
/// Query predicates intentionally treat a missing field as matching negative
/// leaves such as `not_eq`. A write boundary cannot use that open-world behavior:
/// otherwise an empty attribute object can create a row inside a policy scope
/// without carrying the scoped field. This evaluator therefore propagates
/// missing leaves as unknown through boolean operators and accepts only a
/// definite match. Query evaluation remains unchanged.
#[must_use]
pub(crate) fn filter_matches_write_scope(
    filter: &Filter,
    attributes: &HashMap<String, AttributeValue>,
) -> bool {
    matches!(
        evaluate_write_scope_filter(filter, attributes),
        WriteScopeMatch::Matches
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteScopeMatch {
    Matches,
    DoesNotMatch,
    Unknown,
}

fn evaluate_write_scope_filter(
    filter: &Filter,
    attributes: &HashMap<String, AttributeValue>,
) -> WriteScopeMatch {
    match filter {
        Filter::And { filters } => {
            let mut saw_unknown = false;
            for child in filters {
                match evaluate_write_scope_filter(child, attributes) {
                    WriteScopeMatch::DoesNotMatch => return WriteScopeMatch::DoesNotMatch,
                    WriteScopeMatch::Unknown => saw_unknown = true,
                    WriteScopeMatch::Matches => {}
                }
            }
            if saw_unknown {
                WriteScopeMatch::Unknown
            } else {
                WriteScopeMatch::Matches
            }
        }
        Filter::Or { filters } => {
            let mut saw_unknown = false;
            for child in filters {
                match evaluate_write_scope_filter(child, attributes) {
                    WriteScopeMatch::Matches => return WriteScopeMatch::Matches,
                    WriteScopeMatch::Unknown => saw_unknown = true,
                    WriteScopeMatch::DoesNotMatch => {}
                }
            }
            if saw_unknown {
                WriteScopeMatch::Unknown
            } else {
                WriteScopeMatch::DoesNotMatch
            }
        }
        Filter::Not { filter } => match evaluate_write_scope_filter(filter, attributes) {
            WriteScopeMatch::Matches => WriteScopeMatch::DoesNotMatch,
            WriteScopeMatch::DoesNotMatch => WriteScopeMatch::Matches,
            WriteScopeMatch::Unknown => WriteScopeMatch::Unknown,
        },
        Filter::Eq { field, .. }
        | Filter::NotEq { field, .. }
        | Filter::Range { field, .. }
        | Filter::In { field, .. }
        | Filter::NotIn { field, .. }
        | Filter::Contains { field, .. }
        | Filter::ContainsAllTokens { field, .. }
        | Filter::ContainsTokenSequence { field, .. } => {
            if !attributes.contains_key(field) {
                WriteScopeMatch::Unknown
            } else if evaluate_filter(filter, attributes) {
                WriteScopeMatch::Matches
            } else {
                WriteScopeMatch::DoesNotMatch
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_negative_leaf_is_not_a_write_scope_match() {
        let filter = Filter::NotEq {
            field: "tenant_id".to_string(),
            value: AttributeValue::String("bravo".to_string()),
        };
        assert!(!filter_matches_write_scope(&filter, &HashMap::new()));
    }

    #[test]
    fn boolean_write_scope_preserves_unknown_until_a_branch_decides() {
        let missing = Filter::Not {
            filter: Box::new(Filter::Eq {
                field: "tenant_id".to_string(),
                value: AttributeValue::String("bravo".to_string()),
            }),
        };
        let visible = Filter::Eq {
            field: "region".to_string(),
            value: AttributeValue::String("west".to_string()),
        };
        let attributes = HashMap::from([(
            "region".to_string(),
            AttributeValue::String("west".to_string()),
        )]);

        assert!(!filter_matches_write_scope(&missing, &attributes));
        assert!(filter_matches_write_scope(
            &Filter::Or {
                filters: vec![missing, visible]
            },
            &attributes
        ));
    }
}
