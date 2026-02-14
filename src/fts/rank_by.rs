//! RankBy expression parser for BM25 queries.
//!
//! Supports TurboPuffer-compatible S-expression JSON arrays:
//! ```json
//! ["content", "BM25", "search query"]                           // single field
//! ["Sum", [["title", "BM25", "q"], ["content", "BM25", "q"]]]  // multi-field
//! ["Product", 2.0, ["title", "BM25", "q"]]                     // weighted
//! ```
//!
//! Custom Deserialize is used because these heterogeneous arrays
//! cannot use `#[serde(untagged)]` with bincode (per learnings Bug 1).

use serde::{Deserialize, Serialize};

use crate::error::{Result, ZeppelinError};

/// A rank_by expression for BM25 scoring.
#[derive(Debug, Clone, PartialEq)]
pub enum RankBy {
    /// Single-field BM25: `["field", "BM25", "query"]`
    Bm25 {
        /// Field name to search.
        field: String,
        /// Query text to score against.
        query: String,
    },
    /// Sum of multiple expressions: `["Sum", [...exprs]]`
    Sum(Vec<RankBy>),
    /// Max of multiple expressions: `["Max", [...exprs]]`
    Max(Vec<RankBy>),
    /// Weighted expression: `["Product", weight, expr]`
    Product {
        /// Scalar weight multiplier.
        weight: f32,
        /// Inner expression to weight.
        expr: Box<RankBy>,
    },
}

impl RankBy {
    /// Parse a RankBy from a serde_json::Value.
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
                // Assume it's a field-level BM25 expression: ["field", "BM25", "query"]
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

    /// Extract all unique (field, query) pairs from this expression.
    pub fn extract_field_queries(&self) -> Vec<(String, String)> {
        let mut result = Vec::new();
        self.collect_field_queries(&mut result);
        result
    }

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

/// Custom Deserialize for RankBy — parse from JSON Value.
impl<'de> Deserialize<'de> for RankBy {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        RankBy::from_value(&value).map_err(serde::de::Error::custom)
    }
}

/// Custom Serialize for RankBy — produce JSON array.
impl Serialize for RankBy {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let value = self.to_json_value();
        value.serialize(serializer)
    }
}

impl RankBy {
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

/// Evaluate a RankBy expression against per-field BM25 scores.
///
/// `field_scores` maps (field_name) → BM25 score for this document.
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
    use super::*;

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

    #[test]
    fn test_invalid_algo_rejected() {
        let json = serde_json::json!(["content", "TF-IDF", "q"]);
        let result = RankBy::from_value(&json);
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_query_rejected() {
        let json = serde_json::json!(["content", "BM25"]);
        let result = RankBy::from_value(&json);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_array_rejected() {
        let json = serde_json::json!([]);
        let result = RankBy::from_value(&json);
        assert!(result.is_err());
    }

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
