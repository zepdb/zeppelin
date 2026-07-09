//! Parses, serializes, traverses, and evaluates the recursive BM25 ranking
//! expression accepted by Zeppelin's JSON query API.
//!
//! The wire syntax is a TurboPuffer-compatible S-expression represented by
//! heterogeneous JSON arrays. Leaf expressions name an FTS-configured field and
//! query text. Interior expressions add scores, select the maximum, or multiply
//! a subtree by a scalar. Query execution extracts the leaves, tokenizes and
//! scores each field, then calls [`evaluate_rank_by`] to combine the resulting
//! per-field values. This module performs no tokenization, index lookup, object
//! storage access, or result sorting itself.
//!
//! ## Reading map
//!
//! 1. Start with [`RankBy`] for the recursive expression grammar.
//! 2. Read [`RankBy::from_value`] for request validation and AST construction.
//! 3. Read [`RankBy::extract_field_queries`] for the scorer-facing leaf walk.
//! 4. Read the custom [`Deserialize`] and [`Serialize`] implementations for the
//!    JSON compatibility boundary.
//! 5. Finish with [`evaluate_rank_by`] for numeric tree reduction.
//!
//! ## Parse and evaluation flow
//!
//! ```text
//! JSON array in an HTTP query
//!           |
//!           v
//! validate shape and recursively build RankBy
//!           |
//!           +----> extract (field, query) leaves in source order
//!           |                    |
//!           |                    v
//!           |          tokenize and compute BM25 elsewhere
//!           |                    |
//!           v                    v
//!     expression tree + per-field score map
//!                    |
//!                    v
//!        Sum / Max / Product tree reduction
//!                    |
//!                    v
//!       final relevance score (higher is better)
//! ```
//!
//! ## JSON grammar and compatibility
//!
//! ```json
//! ["content", "BM25", "search query"]
//! ["Sum", [["title", "BM25", "q"], ["content", "BM25", "q"]]]
//! ["Max", [["title", "BM25", "q"], ["content", "BM25", "q"]]]
//! ["Product", 2.0, ["title", "BM25", "q"]]
//! ```
//!
//! Parsing accepts title-case or lowercase operator names and `BM25`/`bm25`.
//! Serialization always emits the title-case operators and uppercase `BM25`.
//! `Sum`, `sum`, `Max`, `max`, `Product`, and `product` are reserved in the
//! array's first position, so fields with exactly those names cannot be encoded
//! as leaf expressions by this grammar. Empty `Sum` and `Max` lists are
//! currently accepted and both evaluate to zero.
//!
//! The custom Serde implementation intentionally converts through
//! [`serde_json::Value`] rather than using `#[serde(untagged)]`. The latter is
//! incompatible with Zeppelin type trees that might reach bincode, while this
//! request grammar requires a self-describing representation such as JSON. It
//! is not a portable bincode representation.
//!
//! ## Invariants and current boundary
//!
//! - Leaf order and duplicates are preserved during traversal.
//! - Missing field scores evaluate as zero; no alternate field or score source
//!   is consulted.
//! - `Sum` and `Max` own vectors of child expressions. `Product` owns exactly
//!   one boxed child, keeping the recursive enum a finite size.
//! - Product weights are cast from JSON `f64` to `f32` without sign, range, or
//!   finiteness validation. API clients should supply ordinary finite weights.
//! - `TODO(doc):` Verify whether multiple distinct BM25 leaves for the same
//!   field are a supported expression. Current scoring paths aggregate those
//!   queries into one field entry, and evaluation reuses that entry for every
//!   leaf naming the field.
//!
//! ## Rust concepts used here
//!
//! [`RankBy`] is an algebraic data type: exhaustive `match` expressions ensure
//! every operator participates in parsing, traversal, serialization, and
//! evaluation. Java would commonly use a sealed class hierarchy and C a tagged
//! union plus manual lifetime rules. [`Box`] gives a recursive child one stable,
//! owned heap allocation; [`Vec`] owns a variable number of children. The
//! borrowed tree can be traversed without mutation, while leaf extraction
//! clones strings into an owned result that outlives that borrow.

use serde::{Deserialize, Serialize};

use crate::error::{Result, ZeppelinError};

/// Represents a structurally parsed expression for combining BM25 field scores.
///
/// The enum is both the request AST and the value serialized into request-shaped
/// values and query fingerprints. It owns every field name, query string, child
/// list, and boxed subtree, so it does not borrow from the temporary JSON parse
/// tree.
///
/// # Examples
///
/// `Sum([Product(2, title), content])` expresses a title score weighted twice
/// plus an unweighted content score. The JSON spelling is shown in the module
/// overview.
#[derive(Debug, Clone, PartialEq)]
pub enum RankBy {
    /// Scores one query string against one configured FTS field.
    ///
    /// Wire form: `["field", "BM25", "query"]`.
    Bm25 {
        /// Owned attribute-field name. HTTP validation later rejects names that
        /// are not configured for FTS in the target namespace.
        field: String,
        /// Owned raw query text, analyzed with this field's persisted tokenizer
        /// configuration before index lookup.
        query: String,
    },
    /// Adds child scores in list order.
    ///
    /// Wire form: `["Sum", [expr, ...]]`. An empty list evaluates to `0.0`.
    Sum(Vec<RankBy>),
    /// Selects the greatest child score, using zero as the initial value.
    ///
    /// Wire form: `["Max", [expr, ...]]`. Consequently, an empty list and a
    /// list containing only negative values both evaluate to `0.0`.
    Max(Vec<RankBy>),
    /// Multiplies one child expression by a scalar weight.
    ///
    /// Wire form: `["Product", weight, expr]`.
    Product {
        /// Dimensionless `f32` multiplier. Parsing currently accepts negative,
        /// zero, and values that overflow to infinity during `f64` narrowing.
        weight: f32,
        /// Sole owned child, boxed to break the recursive enum's size cycle.
        expr: Box<RankBy>,
    },
}

impl RankBy {
    /// Parses and structurally validates a ranking expression from a JSON value.
    ///
    /// Operator parsing is recursive. `Sum` and `Max` require one array of child
    /// expressions, `Product` requires a numeric weight and one child, and a
    /// non-operator first string begins a three-element BM25 leaf.
    ///
    /// # Parameters
    ///
    /// - `value`: Borrowed JSON value from the query request. It is inspected but
    ///   not modified or retained.
    ///
    /// # Returns
    ///
    /// Returns an owned [`RankBy`] tree whose strings and child containers no
    /// longer depend on `value`.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Validation`] when the root or a nested child is
    /// not an array, an array is empty, an operator has the wrong arity or
    /// argument type, a leaf has the wrong shape, or its algorithm is not
    /// `BM25`/`bm25`. No partial tree escapes on failure. Whether a leaf field is
    /// configured for the namespace is checked later by the HTTP/domain layer.
    ///
    /// # Examples
    ///
    /// `["Product", 2.0, ["title", "BM25", "rust"]]` becomes a
    /// [`RankBy::Product`] containing a [`RankBy::Bm25`] leaf. Replacing `BM25`
    /// with `TF-IDF` returns a validation error.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `Result<Vec<RankBy>>` can be collected directly from an iterator of
    /// child `Result`s. This is comparable to a loop that returns at the first
    /// Java exception or C error code, but the type system forces the error path
    /// to be handled. `?` propagates that first error while already built child
    /// values are dropped automatically.
    pub fn from_value(value: &serde_json::Value) -> Result<Self> {
        let arr = value
            .as_array()
            .ok_or_else(|| ZeppelinError::Validation("rank_by must be a JSON array".into()))?;

        if arr.is_empty() {
            return Err(ZeppelinError::Validation(
                "rank_by array must not be empty".into(),
            ));
        }

        let first = arr[0]
            .as_str()
            .ok_or_else(|| ZeppelinError::Validation("rank_by[0] must be a string".into()))?;

        match first {
            "Sum" | "sum" => {
                if arr.len() != 2 {
                    return Err(ZeppelinError::Validation(
                        "Sum requires exactly one argument (array of expressions)".into(),
                    ));
                }
                let exprs = arr[1].as_array().ok_or_else(|| {
                    ZeppelinError::Validation("Sum argument must be an array".into())
                })?;
                let parsed: Result<Vec<RankBy>> = exprs.iter().map(RankBy::from_value).collect();
                Ok(RankBy::Sum(parsed?))
            }
            "Max" | "max" => {
                if arr.len() != 2 {
                    return Err(ZeppelinError::Validation(
                        "Max requires exactly one argument (array of expressions)".into(),
                    ));
                }
                let exprs = arr[1].as_array().ok_or_else(|| {
                    ZeppelinError::Validation("Max argument must be an array".into())
                })?;
                let parsed: Result<Vec<RankBy>> = exprs.iter().map(RankBy::from_value).collect();
                Ok(RankBy::Max(parsed?))
            }
            "Product" | "product" => {
                if arr.len() != 3 {
                    return Err(ZeppelinError::Validation(
                        "Product requires exactly two arguments (weight, expression)".into(),
                    ));
                }
                let weight = arr[1].as_f64().ok_or_else(|| {
                    ZeppelinError::Validation("Product weight must be a number".into())
                })? as f32;
                let expr = RankBy::from_value(&arr[2])?;
                Ok(RankBy::Product {
                    weight,
                    expr: Box::new(expr),
                })
            }
            _ => {
                // Any non-operator first string is the field name of a leaf.
                if arr.len() != 3 {
                    return Err(ZeppelinError::Validation(format!(
                        "BM25 expression requires 3 elements [field, algo, query], got {}",
                        arr.len()
                    )));
                }
                let algo = arr[1].as_str().ok_or_else(|| {
                    ZeppelinError::Validation("BM25 expression[1] must be a string".into())
                })?;
                if algo != "BM25" && algo != "bm25" {
                    return Err(ZeppelinError::Validation(format!(
                        "unsupported ranking algorithm: {algo}"
                    )));
                }
                let query = arr[2].as_str().ok_or_else(|| {
                    ZeppelinError::Validation("BM25 expression[2] (query) must be a string".into())
                })?;
                Ok(RankBy::Bm25 {
                    field: first.to_string(),
                    query: query.to_string(),
                })
            }
        }
    }

    /// Extracts every BM25 leaf as owned `(field, query)` pairs.
    ///
    /// Traversal is depth-first and left-to-right. `Product` weights are ignored
    /// because callers need only the underlying leaves to perform tokenization
    /// and index scoring. Despite producing owned strings, this method does not
    /// normalize or deduplicate pairs.
    ///
    /// # Returns
    ///
    /// Returns one cloned pair per [`RankBy::Bm25`] occurrence in expression
    /// order. Duplicate leaves remain duplicated; an expression with no leaves,
    /// such as an empty `Sum`, returns an empty vector.
    ///
    /// # Examples
    ///
    /// `Sum(Product(2, title:"cat"), content:"cat")` returns
    /// `[("title", "cat"), ("content", "cat")]`. The multiplier does not appear
    /// in this scorer-setup list.
    ///
    /// # Performance
    ///
    /// Visits each expression node once and allocates a new [`String`] for each
    /// leaf field and query.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `&self` borrows the tree, while the returned strings are deep clones of
    /// leaf text. This is more work than copying Java references or C pointers,
    /// but it lets callers retain the pairs after the tree borrow ends without
    /// reference counting or lifetime parameters in their own state.
    pub fn extract_field_queries(&self) -> Vec<(String, String)> {
        let mut result = Vec::new();
        self.collect_field_queries(&mut result);
        result
    }

    /// Appends this subtree's BM25 leaves to a caller-owned traversal buffer.
    ///
    /// # Parameters
    ///
    /// - `out`: Mutable borrowed vector receiving cloned `(field, query)` pairs
    ///   in depth-first, left-to-right order. Existing entries are preserved.
    ///
    /// # Side Effects
    ///
    /// Extends `out`; it does not change the expression tree.
    ///
    /// # Examples
    ///
    /// Visiting a `Product` descends through its weight wrapper and appends only
    /// the leaves inside the boxed expression.
    fn collect_field_queries(&self, out: &mut Vec<(String, String)>) {
        match self {
            RankBy::Bm25 { field, query } => {
                out.push((field.clone(), query.clone()));
            }
            RankBy::Sum(exprs) | RankBy::Max(exprs) => {
                for expr in exprs {
                    expr.collect_field_queries(out);
                }
            }
            RankBy::Product { expr, .. } => {
                expr.collect_field_queries(out);
            }
        }
    }
}

/// Decodes the heterogeneous self-describing array grammar into [`RankBy`].
///
/// Conversion through [`serde_json::Value`] centralizes shape validation in
/// [`RankBy::from_value`]. Formats without Serde's self-describing
/// `deserialize_any` capability, notably bincode, are not compatible with this
/// implementation.
impl<'de> Deserialize<'de> for RankBy {
    /// Deserializes one owned ranking-expression tree.
    ///
    /// # Parameters
    ///
    /// - `deserializer`: Serde input expected to represent a self-describing
    ///   value such as JSON.
    ///
    /// # Returns
    ///
    /// Returns the fully parsed tree.
    ///
    /// # Errors
    ///
    /// Returns the deserializer's error when decoding the intermediate value or
    /// a converted validation error when the array grammar is malformed.
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        RankBy::from_value(&value).map_err(serde::de::Error::custom)
    }
}

/// Encodes [`RankBy`] using its canonical heterogeneous array grammar.
///
/// Operators are emitted as `Sum`, `Max`, and `Product`; leaves always emit the
/// algorithm name `BM25`. This keeps output stable even when lowercase aliases
/// were accepted on input.
impl Serialize for RankBy {
    /// Serializes the expression through an owned [`serde_json::Value`] tree.
    ///
    /// # Parameters
    ///
    /// - `serializer`: Destination Serde serializer.
    ///
    /// # Returns
    ///
    /// Returns the serializer's success value after writing the canonical
    /// nested arrays.
    ///
    /// # Errors
    ///
    /// Returns a destination serializer error if it cannot encode the generated
    /// value. Programmatically constructed non-finite weights become JSON
    /// `null` in the intermediate value and therefore do not round-trip through
    /// [`RankBy::from_value`]. JSON cannot spell `NaN`, but a sufficiently large
    /// finite JSON number can still narrow from `f64` to infinite `f32` during
    /// parsing.
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let value = self.to_json_value();
        value.serialize(serializer)
    }
}

impl RankBy {
    /// Converts this expression to the canonical JSON value used by Serde output.
    ///
    /// # Returns
    ///
    /// Returns an owned nested array tree. All field names and query strings are
    /// cloned into the value; operators use canonical title case and `BM25` is
    /// uppercase.
    ///
    /// # Examples
    ///
    /// A leaf created from lowercase `bm25` serializes as
    /// `["field", "BM25", "query"]`.
    fn to_json_value(&self) -> serde_json::Value {
        match self {
            RankBy::Bm25 { field, query } => {
                serde_json::json!([field, "BM25", query])
            }
            RankBy::Sum(exprs) => {
                let arr: Vec<serde_json::Value> = exprs.iter().map(|e| e.to_json_value()).collect();
                serde_json::json!(["Sum", arr])
            }
            RankBy::Max(exprs) => {
                let arr: Vec<serde_json::Value> = exprs.iter().map(|e| e.to_json_value()).collect();
                serde_json::json!(["Max", arr])
            }
            RankBy::Product { weight, expr } => {
                serde_json::json!(["Product", weight, expr.to_json_value()])
            }
        }
    }
}

/// Reduces a ranking expression against already-computed per-field BM25 scores.
///
/// Leaf query strings are not consulted here; they were used earlier to compute
/// the supplied score map. A leaf reads its field value, `Sum` adds children,
/// `Max` takes the greatest child starting from zero, and `Product` scales its
/// child recursively.
///
/// # Parameters
///
/// - `rank_by`: Borrowed expression tree to reduce.
/// - `field_scores`: Borrowed map from field name to the BM25 score accumulated
///   for this document. Missing fields contribute zero.
///
/// # Returns
///
/// Returns the final relevance score. Empty `Sum` and `Max` expressions return
/// `0.0`. Because `Max` starts at zero, it clamps an all-negative set of child
/// scores to zero. Non-finite weights or field values may produce non-finite
/// output; this helper performs no numeric validation.
///
/// # Performance
///
/// Visits each expression node once, performs average-case constant-time map
/// lookup per leaf, and allocates nothing.
///
/// # Examples
///
/// ```text
/// expression: Sum(Product(2, title), content)
/// field map:  title = 1.5, content = 0.7
/// result:     (2 * 1.5) + 0.7 = 3.7
///
/// missing content field:
/// result:     (2 * 1.5) + 0.0 = 3.0
/// ```
///
/// # Rust Notes for Java/C Engineers
///
/// The exhaustive recursive `match` is analogous to virtual dispatch over a
/// sealed Java AST or a switch over a C tagged union, but Rust checks that every
/// enum variant is covered. `copied().unwrap_or(0.0)` turns `Option<&f32>` into
/// a value without a null reference: lookup either copies four bytes or uses the
/// explicit zero identity.
#[must_use]
pub fn evaluate_rank_by(
    rank_by: &RankBy,
    field_scores: &std::collections::HashMap<String, f32>,
) -> f32 {
    match rank_by {
        RankBy::Bm25 { field, .. } => field_scores.get(field).copied().unwrap_or(0.0),
        RankBy::Sum(exprs) => exprs
            .iter()
            .map(|e| evaluate_rank_by(e, field_scores))
            .sum(),
        RankBy::Max(exprs) => exprs
            .iter()
            .map(|e| evaluate_rank_by(e, field_scores))
            .fold(0.0_f32, f32::max),
        RankBy::Product { weight, expr } => weight * evaluate_rank_by(expr, field_scores),
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Protects JSON grammar validation, recursive AST shape, traversal, numeric
    //! reduction, and canonical Serde round trips.

    use super::*;

    /// Verifies the three-element leaf grammar owns its field and query text.
    #[test]
    fn test_parse_simple_bm25() {
        let json = serde_json::json!(["content", "BM25", "search query"]);
        let rank_by = RankBy::from_value(&json).unwrap();
        assert_eq!(
            rank_by,
            RankBy::Bm25 {
                field: "content".to_string(),
                query: "search query".to_string(),
            }
        );
    }

    /// Verifies `Sum` recursively parses a left-to-right child list.
    #[test]
    fn test_parse_sum_multi_field() {
        let json = serde_json::json!(["Sum", [["title", "BM25", "q"], ["content", "BM25", "q"]]]);
        let rank_by = RankBy::from_value(&json).unwrap();
        match rank_by {
            RankBy::Sum(exprs) => {
                assert_eq!(exprs.len(), 2);
            }
            _ => panic!("expected Sum"),
        }
    }

    /// Verifies `Product` parses its scalar and boxed child expression.
    #[test]
    fn test_parse_product_weighted() {
        let json = serde_json::json!(["Product", 2.0, ["title", "BM25", "q"]]);
        let rank_by = RankBy::from_value(&json).unwrap();
        match rank_by {
            RankBy::Product { weight, expr } => {
                assert!((weight - 2.0).abs() < f32::EPSILON);
                assert_eq!(
                    *expr,
                    RankBy::Bm25 {
                        field: "title".to_string(),
                        query: "q".to_string(),
                    }
                );
            }
            _ => panic!("expected Product"),
        }
    }

    /// Verifies recursive operators can nest without losing child variants.
    #[test]
    fn test_parse_nested() {
        let json = serde_json::json!([
            "Sum",
            [
                ["Product", 2.0, ["title", "BM25", "q"]],
                ["content", "BM25", "q"]
            ]
        ]);
        let rank_by = RankBy::from_value(&json).unwrap();
        match rank_by {
            RankBy::Sum(exprs) => {
                assert_eq!(exprs.len(), 2);
                assert!(matches!(exprs[0], RankBy::Product { .. }));
                assert!(matches!(exprs[1], RankBy::Bm25 { .. }));
            }
            _ => panic!("expected Sum"),
        }
    }

    /// Verifies a leaf rejects algorithms outside the BM25 contract.
    #[test]
    fn test_invalid_algo_rejected() {
        let json = serde_json::json!(["content", "TF-IDF", "q"]);
        let result = RankBy::from_value(&json);
        assert!(result.is_err());
    }

    /// Verifies an incomplete BM25 leaf fails structural validation.
    #[test]
    fn test_missing_query_rejected() {
        let json = serde_json::json!(["content", "BM25"]);
        let result = RankBy::from_value(&json);
        assert!(result.is_err());
    }

    /// Verifies an empty root array is not interpreted as an expression.
    #[test]
    fn test_empty_array_rejected() {
        let json = serde_json::json!([]);
        let result = RankBy::from_value(&json);
        assert!(result.is_err());
    }

    /// Verifies a leaf returns the matching field score unchanged.
    #[test]
    fn test_evaluate_simple() {
        let rank_by = RankBy::Bm25 {
            field: "content".to_string(),
            query: "q".to_string(),
        };
        let mut scores = std::collections::HashMap::new();
        scores.insert("content".to_string(), 2.5);
        assert!((evaluate_rank_by(&rank_by, &scores) - 2.5).abs() < f32::EPSILON);
    }

    /// Verifies addition produces the same value for reversed child order.
    #[test]
    fn test_evaluate_sum_commutative() {
        let rank_by_ab = RankBy::Sum(vec![
            RankBy::Bm25 {
                field: "a".to_string(),
                query: "q".to_string(),
            },
            RankBy::Bm25 {
                field: "b".to_string(),
                query: "q".to_string(),
            },
        ]);
        let rank_by_ba = RankBy::Sum(vec![
            RankBy::Bm25 {
                field: "b".to_string(),
                query: "q".to_string(),
            },
            RankBy::Bm25 {
                field: "a".to_string(),
                query: "q".to_string(),
            },
        ]);

        let mut scores = std::collections::HashMap::new();
        scores.insert("a".to_string(), 1.0);
        scores.insert("b".to_string(), 2.0);

        let score_ab = evaluate_rank_by(&rank_by_ab, &scores);
        let score_ba = evaluate_rank_by(&rank_by_ba, &scores);
        assert!((score_ab - score_ba).abs() < f32::EPSILON);
    }

    /// Verifies leaf extraction descends through wrappers and preserves leaves.
    #[test]
    fn test_extract_field_queries() {
        let rank_by = RankBy::Sum(vec![
            RankBy::Product {
                weight: 2.0,
                expr: Box::new(RankBy::Bm25 {
                    field: "title".to_string(),
                    query: "cat".to_string(),
                }),
            },
            RankBy::Bm25 {
                field: "content".to_string(),
                query: "cat".to_string(),
            },
        ]);
        let pairs = rank_by.extract_field_queries();
        assert_eq!(pairs.len(), 2);
    }

    /// Verifies the canonical JSON representation decodes to the same AST.
    #[test]
    fn test_serde_roundtrip() {
        let rank_by = RankBy::Bm25 {
            field: "content".to_string(),
            query: "hello world".to_string(),
        };
        let json = serde_json::to_string(&rank_by).unwrap();
        let back: RankBy = serde_json::from_str(&json).unwrap();
        assert_eq!(rank_by, back);
    }
}
