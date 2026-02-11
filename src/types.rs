use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A unique identifier for a vector within a namespace.
pub type VectorId = String;

/// Distance metric for vector comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistanceMetric {
    Cosine,
    Euclidean,
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
    String(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    StringList(Vec<String>),
}

/// A vector entry with its ID, values, and optional attributes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorEntry {
    pub id: VectorId,
    pub values: Vec<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes: Option<HashMap<String, AttributeValue>>,
}

/// A search result containing the vector ID, distance/score, and optional attributes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub id: VectorId,
    pub score: f32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes: Option<HashMap<String, AttributeValue>>,
}

/// Filter conditions for post-filtering search results.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Filter {
    Eq {
        field: String,
        value: AttributeValue,
    },
    Range {
        field: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        gte: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        lte: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        gt: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
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
    #[default]
    IvfFlat,
}

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
        assert_eq!(
            back_attrs.get("count"),
            Some(&AttributeValue::Integer(5))
        );
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
}
