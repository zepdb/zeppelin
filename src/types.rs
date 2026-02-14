use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A unique identifier for a vector within a namespace.
pub type VectorId = String;

/// Distance metric for vector comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistanceMetric {
    /// Cosine similarity (1 - cosine distance).
    Cosine,
    /// Euclidean (L2) distance.
    Euclidean,
    /// Dot product similarity.
    DotProduct,
}

impl std::fmt::Display for DistanceMetric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DistanceMetric::Cosine => write!(f, "cosine"),
            DistanceMetric::Euclidean => write!(f, "euclidean"),
            DistanceMetric::DotProduct => write!(f, "dot_product"),
        }
    }
}

/// Attribute values that can be attached to vectors for filtering.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AttributeValue {
    /// A UTF-8 string value.
    String(String),
    /// A 64-bit signed integer value.
    Integer(i64),
    /// A 64-bit floating-point value.
    Float(f64),
    /// A boolean value.
    Bool(bool),
    /// A list of UTF-8 string values.
    StringList(Vec<String>),
    /// A list of 64-bit signed integer values.
    IntegerList(Vec<i64>),
    /// A list of 64-bit floating-point values.
    FloatList(Vec<f64>),
}

/// A vector entry with its ID, values, and optional attributes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorEntry {
    /// Unique identifier for this vector.
    pub id: VectorId,
    /// The raw floating-point vector values.
    pub values: Vec<f32>,
    /// Optional key-value attributes for metadata filtering.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes: Option<HashMap<String, AttributeValue>>,
}

/// A search result containing the vector ID, distance/score, and optional attributes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    /// Identifier of the matched vector.
    pub id: VectorId,
    /// Similarity or distance score for this result.
    pub score: f32,
    /// Optional attributes returned with the result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes: Option<HashMap<String, AttributeValue>>,
}

/// Filter conditions for post-filtering search results.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Filter {
    /// Matches when the field equals the given value.
    Eq {
        /// Attribute field name to compare.
        field: String,
        /// Value to test for equality.
        value: AttributeValue,
    },
    /// Matches when the field does not equal the given value.
    #[serde(rename = "not_eq")]
    NotEq {
        /// Attribute field name to compare.
        field: String,
        /// Value to test for inequality.
        value: AttributeValue,
    },
    /// Matches when a numeric field falls within the specified bounds.
    Range {
        /// Attribute field name to compare.
        field: String,
        /// Optional inclusive lower bound.
        #[serde(skip_serializing_if = "Option::is_none")]
        gte: Option<f64>,
        /// Optional inclusive upper bound.
        #[serde(skip_serializing_if = "Option::is_none")]
        lte: Option<f64>,
        /// Optional exclusive lower bound.
        #[serde(skip_serializing_if = "Option::is_none")]
        gt: Option<f64>,
        /// Optional exclusive upper bound.
        #[serde(skip_serializing_if = "Option::is_none")]
        lt: Option<f64>,
    },
    /// Matches when the field value is one of the given values.
    In {
        /// Attribute field name to compare.
        field: String,
        /// Set of values to match against.
        values: Vec<AttributeValue>,
    },
    /// Matches when the field value is not in the given set.
    #[serde(rename = "not_in")]
    NotIn {
        /// Attribute field name to compare.
        field: String,
        /// Set of values to exclude.
        values: Vec<AttributeValue>,
    },
    /// Logical AND: all sub-filters must match.
    And {
        /// Sub-filters that must all be satisfied.
        filters: Vec<Filter>,
    },
    /// Logical OR: at least one sub-filter must match.
    Or {
        /// Sub-filters where at least one must be satisfied.
        filters: Vec<Filter>,
    },
    /// Logical NOT: the inner filter must not match.
    Not {
        /// The filter to negate.
        filter: Box<Filter>,
    },
    /// Matches when a list field contains the given value.
    Contains {
        /// Attribute field name to search within.
        field: String,
        /// Value that must be present in the list.
        value: AttributeValue,
    },
    /// All specified tokens must be present in the field (order-independent).
    ContainsAllTokens {
        /// Attribute field name to search within.
        field: String,
        /// Tokens that must all appear in the field.
        tokens: Vec<String>,
    },
    /// Tokens must appear as an exact phrase (adjacent, in order).
    ContainsTokenSequence {
        /// Attribute field name to search within.
        field: String,
        /// Ordered sequence of tokens forming the phrase.
        tokens: Vec<String>,
    },
}

/// Consistency level for queries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsistencyLevel {
    /// Read from index + scan all uncompacted WAL fragments
    #[default]
    Strong,
    /// Read from index only (faster, may miss recent writes)
    Eventual,
}

/// Index type for a namespace.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexType {
    /// Inverted File with flat (uncompressed) vectors.
    #[default]
    IvfFlat,
    /// IVF-Flat with Scalar Quantization (4x compression).
    IvfSq,
    /// IVF with Product Quantization (16-32x compression).
    IvfPq,
    /// Hierarchical ANN index (multi-level centroid tree).
    Hierarchical,
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distance_metric_serde_roundtrip() {
        for (variant, expected_json) in [
            (DistanceMetric::Cosine, "\"cosine\""),
            (DistanceMetric::Euclidean, "\"euclidean\""),
            (DistanceMetric::DotProduct, "\"dot_product\""),
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, expected_json);
            let back: DistanceMetric = serde_json::from_str(&json).unwrap();
            assert_eq!(back, variant);
        }
    }

    #[test]
    fn test_distance_metric_display() {
        assert_eq!(DistanceMetric::Cosine.to_string(), "cosine");
        assert_eq!(DistanceMetric::Euclidean.to_string(), "euclidean");
        assert_eq!(DistanceMetric::DotProduct.to_string(), "dot_product");
    }

    #[test]
    fn test_attribute_value_serde_roundtrip() {
        let cases: Vec<(AttributeValue, &str)> = vec![
            (AttributeValue::String("hello".into()), "\"hello\""),
            (AttributeValue::Integer(42), "42"),
            (AttributeValue::Float(1.23), "1.23"),
            (AttributeValue::Bool(true), "true"),
            (
                AttributeValue::StringList(vec!["a".into(), "b".into()]),
                "[\"a\",\"b\"]",
            ),
        ];
        for (val, expected_json) in cases {
            let json = serde_json::to_string(&val).unwrap();
            assert_eq!(json, expected_json);
            let back: AttributeValue = serde_json::from_str(&json).unwrap();
            assert_eq!(back, val);
        }
    }

    #[test]
    fn test_vector_entry_serde_with_attributes() {
        let mut attrs = HashMap::new();
        attrs.insert("color".into(), AttributeValue::String("red".into()));
        attrs.insert("count".into(), AttributeValue::Integer(5));
        let entry = VectorEntry {
            id: "vec-1".into(),
            values: vec![1.0, 2.0, 3.0],
            attributes: Some(attrs),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: VectorEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, "vec-1");
        assert_eq!(back.values, vec![1.0, 2.0, 3.0]);
        assert!(back.attributes.is_some());
        let back_attrs = back.attributes.unwrap();
        assert_eq!(
            back_attrs.get("color"),
            Some(&AttributeValue::String("red".into()))
        );
        assert_eq!(back_attrs.get("count"), Some(&AttributeValue::Integer(5)));
    }

    #[test]
    fn test_vector_entry_serde_without_attributes() {
        let entry = VectorEntry {
            id: "vec-2".into(),
            values: vec![0.5],
            attributes: None,
        };
        let json = serde_json::to_string(&entry).unwrap();
        assert!(!json.contains("attributes"));
        let back: VectorEntry = serde_json::from_str(&json).unwrap();
        assert!(back.attributes.is_none());
    }

    #[test]
    fn test_filter_serde_tagged() {
        // Eq
        let eq = Filter::Eq {
            field: "color".into(),
            value: AttributeValue::String("blue".into()),
        };
        let json = serde_json::to_string(&eq).unwrap();
        assert!(json.contains("\"op\":\"eq\""));
        let back: Filter = serde_json::from_str(&json).unwrap();
        match back {
            Filter::Eq { field, value } => {
                assert_eq!(field, "color");
                assert_eq!(value, AttributeValue::String("blue".into()));
            }
            _ => panic!("expected Eq"),
        }

        // Range
        let range = Filter::Range {
            field: "price".into(),
            gte: Some(10.0),
            lte: Some(100.0),
            gt: None,
            lt: None,
        };
        let json = serde_json::to_string(&range).unwrap();
        assert!(json.contains("\"op\":\"range\""));
        assert!(!json.contains("\"gt\""));
        assert!(!json.contains("\"lt\""));
        let back: Filter = serde_json::from_str(&json).unwrap();
        match back {
            Filter::Range {
                field, gte, lte, ..
            } => {
                assert_eq!(field, "price");
                assert_eq!(gte, Some(10.0));
                assert_eq!(lte, Some(100.0));
            }
            _ => panic!("expected Range"),
        }

        // In
        let in_filter = Filter::In {
            field: "tag".into(),
            values: vec![
                AttributeValue::String("a".into()),
                AttributeValue::String("b".into()),
            ],
        };
        let json = serde_json::to_string(&in_filter).unwrap();
        assert!(json.contains("\"op\":\"in\""));
        let back: Filter = serde_json::from_str(&json).unwrap();
        match back {
            Filter::In { field, values } => {
                assert_eq!(field, "tag");
                assert_eq!(values.len(), 2);
            }
            _ => panic!("expected In"),
        }

        // And
        let and = Filter::And {
            filters: vec![eq.clone(), in_filter.clone()],
        };
        let json = serde_json::to_string(&and).unwrap();
        assert!(json.contains("\"op\":\"and\""));
        let back: Filter = serde_json::from_str(&json).unwrap();
        match back {
            Filter::And { filters } => assert_eq!(filters.len(), 2),
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn test_consistency_level_default() {
        assert_eq!(ConsistencyLevel::default(), ConsistencyLevel::Strong);
    }

    #[test]
    fn test_index_type_default() {
        assert_eq!(IndexType::default(), IndexType::IvfFlat);
    }

    #[test]
    fn test_search_result_serde() {
        // With attributes
        let mut attrs = HashMap::new();
        attrs.insert("tag".into(), AttributeValue::String("test".into()));
        let result = SearchResult {
            id: "vec-1".into(),
            score: 0.95,
            attributes: Some(attrs),
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: SearchResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, "vec-1");
        assert_eq!(back.score, 0.95);
        assert!(back.attributes.is_some());

        // Without attributes
        let result = SearchResult {
            id: "vec-2".into(),
            score: 0.5,
            attributes: None,
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(!json.contains("attributes"));
        let back: SearchResult = serde_json::from_str(&json).unwrap();
        assert!(back.attributes.is_none());
    }

    // --- New filter operator serde tests ---

    #[test]
    fn test_filter_not_eq_serde() {
        let f = Filter::NotEq {
            field: "status".into(),
            value: AttributeValue::String("deleted".into()),
        };
        let json = serde_json::to_string(&f).unwrap();
        assert!(json.contains("\"op\":\"not_eq\""));
        let back: Filter = serde_json::from_str(&json).unwrap();
        match back {
            Filter::NotEq { field, value } => {
                assert_eq!(field, "status");
                assert_eq!(value, AttributeValue::String("deleted".into()));
            }
            _ => panic!("expected NotEq"),
        }
    }

    #[test]
    fn test_filter_not_in_serde() {
        let f = Filter::NotIn {
            field: "category".into(),
            values: vec![
                AttributeValue::String("spam".into()),
                AttributeValue::String("junk".into()),
            ],
        };
        let json = serde_json::to_string(&f).unwrap();
        assert!(json.contains("\"op\":\"not_in\""));
        let back: Filter = serde_json::from_str(&json).unwrap();
        match back {
            Filter::NotIn { field, values } => {
                assert_eq!(field, "category");
                assert_eq!(values.len(), 2);
            }
            _ => panic!("expected NotIn"),
        }
    }

    #[test]
    fn test_filter_or_serde() {
        let f = Filter::Or {
            filters: vec![
                Filter::Eq {
                    field: "a".into(),
                    value: AttributeValue::Integer(1),
                },
                Filter::Eq {
                    field: "b".into(),
                    value: AttributeValue::Integer(2),
                },
            ],
        };
        let json = serde_json::to_string(&f).unwrap();
        assert!(json.contains("\"op\":\"or\""));
        let back: Filter = serde_json::from_str(&json).unwrap();
        match back {
            Filter::Or { filters } => assert_eq!(filters.len(), 2),
            _ => panic!("expected Or"),
        }
    }

    #[test]
    fn test_filter_not_serde() {
        let f = Filter::Not {
            filter: Box::new(Filter::Eq {
                field: "active".into(),
                value: AttributeValue::Bool(false),
            }),
        };
        let json = serde_json::to_string(&f).unwrap();
        assert!(json.contains("\"op\":\"not\""));
        let back: Filter = serde_json::from_str(&json).unwrap();
        match back {
            Filter::Not { filter } => match *filter {
                Filter::Eq { field, value } => {
                    assert_eq!(field, "active");
                    assert_eq!(value, AttributeValue::Bool(false));
                }
                _ => panic!("expected inner Eq"),
            },
            _ => panic!("expected Not"),
        }
    }

    #[test]
    fn test_filter_contains_serde() {
        let f = Filter::Contains {
            field: "tags".into(),
            value: AttributeValue::String("rust".into()),
        };
        let json = serde_json::to_string(&f).unwrap();
        assert!(json.contains("\"op\":\"contains\""));
        let back: Filter = serde_json::from_str(&json).unwrap();
        match back {
            Filter::Contains { field, value } => {
                assert_eq!(field, "tags");
                assert_eq!(value, AttributeValue::String("rust".into()));
            }
            _ => panic!("expected Contains"),
        }
    }

    // --- New attribute type serde tests ---

    #[test]
    fn test_integer_list_serde() {
        let val = AttributeValue::IntegerList(vec![1, 2, 3]);
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, "[1,2,3]");
        let back: AttributeValue = serde_json::from_str(&json).unwrap();
        assert_eq!(back, val);
    }

    #[test]
    fn test_float_list_serde() {
        let val = AttributeValue::FloatList(vec![1.5, 2.5]);
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, "[1.5,2.5]");
        let back: AttributeValue = serde_json::from_str(&json).unwrap();
        assert_eq!(back, val);
    }

    #[test]
    fn test_index_type_serde_roundtrip() {
        for (variant, expected) in [
            (IndexType::IvfFlat, "\"ivf_flat\""),
            (IndexType::IvfSq, "\"ivf_sq\""),
            (IndexType::IvfPq, "\"ivf_pq\""),
            (IndexType::Hierarchical, "\"hierarchical\""),
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, expected);
            let back: IndexType = serde_json::from_str(&json).unwrap();
            assert_eq!(back, variant);
        }
    }
}
