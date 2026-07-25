//! Pure helpers for applying server-owned observation constraints.
//!
//! When [`super::kernel`] allows a request, the allow decision can carry
//! *obligations*: server-owned narrowings the handler must apply before the
//! caller observes anything. This module owns the pure evaluation of two such
//! obligations — attribute field masks and mandatory write scopes. It reaches
//! no verdict of its own; it holds no policy, performs no I/O, and cannot
//! decide whether an operation is permitted. It only enforces a narrowing that
//! authorization already decided on.
//!
//! Callers are the handlers that touch user attributes:
//! [`server`](crate::server)'s vector and query handlers apply
//! [`apply_field_mask`] to response attributes, screen caller-supplied
//! predicates with [`filter_references_denied_field`], and gate writes with
//! `filter_matches_write_scope`.
//!
//! ## A field mask is an authorization boundary, not a serializer
//!
//! Removing a denied attribute from a response body is not sufficient. A
//! predicate over a denied field leaks that field through membership, result
//! counts, and destructive side effects even when the field never appears in a
//! response. That is why [`filter_references_denied_field`] rejects such a
//! query outright rather than filtering it silently — consistent with the
//! repository's fail-loud rule.
//!
//! [`apply_field_mask`] is deliberately subtractive. It never substitutes a
//! null or placeholder value, because a marker where a denied field used to be
//! reveals that the field existed.
//!
//! ## Why writes use three-valued logic
//!
//! Query evaluation is open-world: a row missing a field still satisfies a
//! negative leaf such as `not_eq`. A write boundary cannot inherit that
//! behavior. If it did, an empty attribute object would satisfy a mandatory
//! scope and create a row inside a policy scope without carrying the scoped
//! field.
//!
//! So the write-scope evaluator propagates a missing leaf as `Unknown` through
//! boolean operators and accepts only a definite match:
//!
//! ```text
//! leaf over a present field   -> Matches / DoesNotMatch  (ordinary evaluation)
//! leaf over a missing field   -> Unknown                 (not "matches")
//!
//! And : any DoesNotMatch -> DoesNotMatch; else any Unknown -> Unknown
//! Or  : any Matches      -> Matches;      else any Unknown -> Unknown
//! Not : Matches <-> DoesNotMatch; Unknown stays Unknown
//!
//! accepted only when the result is exactly Matches
//! ```
//!
//! Query evaluation is untouched by this: the three-valued walk lives only on
//! the write path, and [`evaluate_filter`] still decides
//! present-field leaves.
//!
//! ## Rust concepts used here
//!
//! The private `WriteScopeMatch` enum makes the third state explicit in the
//! type system rather than encoding it as a `bool` plus a side flag. Rust's
//! exhaustive `match` then forces every combining rule above to state what it
//! does with `Unknown` — a new variant or a forgotten branch is a compile
//! error, not a silently permissive write. In Java this resembles a
//! three-valued enum with a switch, but without a `default:` arm quietly
//! absorbing the case someone forgot to think about.

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
