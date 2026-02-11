//! Property-based tests for WAL fragment checksum stability.
//!
//! Verifies that WalFragment round-trips (to_bytes → from_bytes) preserve
//! the checksum. This catches issues with non-deterministic serialization
//! (e.g., HashMap key ordering) that could cause checksum mismatches.
//!
//! To run: add `proptest = "1"` to [dev-dependencies] in Cargo.toml, then
//!   cp formal-verifications/proptest/checksum_stability.rs tests/proptest_checksum.rs
//!   cargo test --test proptest_checksum

use proptest::prelude::*;
use std::collections::HashMap;

// These tests use the actual zeppelin types.
// When copying to tests/, uncomment the following:
// use zeppelin::types::{AttributeValue, VectorEntry, VectorId};
// use zeppelin::wal::fragment::WalFragment;

/// Simplified attribute value for standalone testing.
/// When integrating with the actual codebase, replace with `AttributeValue`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
enum TestAttributeValue {
    String(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TestVectorEntry {
    id: String,
    values: Vec<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    attributes: Option<HashMap<String, TestAttributeValue>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TestFragment {
    id: u64, // simplified ULID
    vectors: Vec<TestVectorEntry>,
    deletes: Vec<String>,
    checksum: u64,
}

impl TestFragment {
    fn new(vectors: Vec<TestVectorEntry>, deletes: Vec<String>) -> Self {
        let checksum = Self::compute_checksum(&vectors, &deletes);
        Self {
            id: rand::random(),
            vectors,
            deletes,
            checksum,
        }
    }

    /// Mirrors WalFragment::compute_checksum from src/wal/fragment.rs:44-62.
    /// Uses BTreeMap canonicalization for deterministic HashMap ordering.
    fn compute_checksum(vectors: &[TestVectorEntry], deletes: &[String]) -> u64 {
        use std::collections::BTreeMap;

        #[allow(clippy::type_complexity)]
        let canonical: Vec<(&str, &[f32], Option<BTreeMap<&String, &TestAttributeValue>>)> =
            vectors
                .iter()
                .map(|v| {
                    let attrs = v
                        .attributes
                        .as_ref()
                        .map(|a| a.iter().collect::<BTreeMap<_, _>>());
                    (v.id.as_str(), v.values.as_slice(), attrs)
                })
                .collect();
        let payload =
            serde_json::to_vec(&(&canonical, deletes)).expect("serialization should not fail");
        xxhash_rust::xxh3::xxh3_64(&payload)
    }

    fn validate_checksum(&self) -> bool {
        let expected = Self::compute_checksum(&self.vectors, &self.deletes);
        self.checksum == expected
    }

    fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("serialization should not fail")
    }

    fn from_bytes(data: &[u8]) -> Self {
        serde_json::from_slice(data).expect("deserialization should not fail")
    }
}

// ---- Proptest Strategies ----

fn arb_vector_id() -> impl Strategy<Value = String> {
    prop::string::string_regex("vec_[a-z0-9]{1,6}")
        .unwrap()
        .prop_filter("non-empty", |s| !s.is_empty())
}

fn arb_values() -> impl Strategy<Value = Vec<f32>> {
    prop::collection::vec(prop::num::f32::NORMAL, 2..=8)
}

fn arb_attribute_value() -> impl Strategy<Value = TestAttributeValue> {
    prop_oneof![
        "[a-z]{1,8}".prop_map(TestAttributeValue::String),
        (-1000i64..1000).prop_map(TestAttributeValue::Integer),
        (-100.0f64..100.0).prop_map(TestAttributeValue::Float),
        any::<bool>().prop_map(TestAttributeValue::Bool),
    ]
}

fn arb_attributes() -> impl Strategy<Value = Option<HashMap<String, TestAttributeValue>>> {
    prop_oneof![
        Just(None),
        prop::collection::hash_map("[a-z]{1,4}", arb_attribute_value(), 1..=4).prop_map(Some),
    ]
}

fn arb_vector_entry() -> impl Strategy<Value = TestVectorEntry> {
    (arb_vector_id(), arb_values(), arb_attributes()).prop_map(|(id, values, attributes)| {
        TestVectorEntry {
            id,
            values,
            attributes,
        }
    })
}

fn arb_fragment() -> impl Strategy<Value = TestFragment> {
    (
        prop::collection::vec(arb_vector_entry(), 0..=5),
        prop::collection::vec(arb_vector_id(), 0..=3),
    )
        .prop_map(|(vectors, deletes)| TestFragment::new(vectors, deletes))
}

// ---- Property Tests ----

proptest! {
    /// Checksum is valid after construction.
    #[test]
    fn checksum_valid_after_construction(fragment in arb_fragment()) {
        prop_assert!(fragment.validate_checksum(),
            "Checksum should be valid immediately after construction");
    }

    /// Round-trip: to_bytes → from_bytes preserves checksum.
    #[test]
    fn checksum_stable_after_roundtrip(fragment in arb_fragment()) {
        let bytes = fragment.to_bytes();
        let restored = TestFragment::from_bytes(&bytes);

        prop_assert_eq!(fragment.checksum, restored.checksum,
            "Checksum should be identical after JSON round-trip");

        prop_assert!(restored.validate_checksum(),
            "Checksum should validate after round-trip");
    }

    /// Double round-trip preserves checksum.
    #[test]
    fn checksum_stable_after_double_roundtrip(fragment in arb_fragment()) {
        let bytes1 = fragment.to_bytes();
        let restored1 = TestFragment::from_bytes(&bytes1);
        let bytes2 = restored1.to_bytes();
        let restored2 = TestFragment::from_bytes(&bytes2);

        prop_assert_eq!(fragment.checksum, restored2.checksum,
            "Checksum should survive double round-trip");
    }

    /// Modifying a vector's values should change the checksum.
    #[test]
    fn checksum_changes_on_modification(
        fragment in arb_fragment().prop_filter("has vectors", |f| !f.vectors.is_empty()),
    ) {
        let original_checksum = fragment.checksum;

        let mut modified_vectors = fragment.vectors.clone();
        // Flip the first value of the first vector
        if !modified_vectors[0].values.is_empty() {
            modified_vectors[0].values[0] = -modified_vectors[0].values[0] + 1.0;
        }

        let new_checksum = TestFragment::compute_checksum(&modified_vectors, &fragment.deletes);

        // Note: this could theoretically collide, but xxHash3 collision
        // probability is ~2^-64, so we accept the infinitesimal risk.
        prop_assert_ne!(original_checksum, new_checksum,
            "Modifying vector values should change the checksum");
    }

    /// Attribute HashMap ordering does not affect checksum.
    /// This is the critical property — BTreeMap canonicalization should
    /// ensure determinism regardless of HashMap iteration order.
    #[test]
    fn checksum_independent_of_insertion_order(
        id in arb_vector_id(),
        values in arb_values(),
        k1 in "[a-z]{1,3}",
        v1 in arb_attribute_value(),
        k2 in "[a-z]{1,3}",
        v2 in arb_attribute_value(),
    ) {
        // Insert in order (k1, k2)
        let mut attrs1 = HashMap::new();
        attrs1.insert(k1.clone(), v1.clone());
        attrs1.insert(k2.clone(), v2.clone());
        let vec1 = TestVectorEntry {
            id: id.clone(),
            values: values.clone(),
            attributes: Some(attrs1),
        };

        // Insert in order (k2, k1) — different insertion order
        let mut attrs2 = HashMap::new();
        attrs2.insert(k2, v2);
        attrs2.insert(k1, v1);
        let vec2 = TestVectorEntry {
            id,
            values,
            attributes: Some(attrs2),
        };

        let checksum1 = TestFragment::compute_checksum(&[vec1], &[]);
        let checksum2 = TestFragment::compute_checksum(&[vec2], &[]);

        prop_assert_eq!(checksum1, checksum2,
            "Checksum should be the same regardless of HashMap insertion order");
    }
}
