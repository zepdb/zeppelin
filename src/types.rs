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
