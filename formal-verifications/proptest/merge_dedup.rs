//! Property-based tests for merge/dedup correctness.
//!
//! Tests that the merge/dedup logic in compaction (src/compaction/mod.rs:107-120)
//! and WAL scan (src/query.rs:129-139) produces the same results as a simple
//! reference implementation.
//!
//! To run: add `proptest = "1"` to [dev-dependencies] in Cargo.toml, then
//!   cp formal-verifications/proptest/merge_dedup.rs tests/proptest_merge_dedup.rs
//!   cargo test --test proptest_merge_dedup

use proptest::prelude::*;
use std::collections::{HashMap, HashSet};

/// A simplified vector entry for testing merge logic.
#[derive(Debug, Clone, PartialEq)]
struct TestVector {
    id: String,
    values: Vec<f32>,
}

/// An operation in a WAL fragment.
#[derive(Debug, Clone)]
enum Op {
    Upsert(TestVector),
    Delete(String),
}

/// A simplified WAL fragment: a sequence of operations.
#[derive(Debug, Clone)]
struct TestFragment {
    ops: Vec<Op>,
}

// ---- Reference Implementation (simple, obviously correct) ----

/// Apply a sequence of fragments and return surviving vectors.
/// This is the "oracle" — simple HashMap-based, no ULID ordering needed
/// because we process fragments in sequence order.
fn reference_merge(fragments: &[TestFragment]) -> HashMap<String, TestVector> {
    let mut latest: HashMap<String, TestVector> = HashMap::new();
    let mut deleted: HashSet<String> = HashSet::new();

    for frag in fragments {
        for op in &frag.ops {
            match op {
                Op::Upsert(v) => {
                    deleted.remove(&v.id);
                    latest.insert(v.id.clone(), v.clone());
                }
                Op::Delete(id) => {
                    deleted.insert(id.clone());
                    latest.remove(id);
                }
            }
        }
    }

    latest
}

// ---- Production-equivalent Implementation ----

/// Mirrors the merge logic from src/compaction/mod.rs:107-120.
/// Processes fragments in order, tracking latest vectors and deletes.
fn production_merge(fragments: &[TestFragment]) -> HashMap<String, TestVector> {
    let mut latest_vectors: HashMap<String, TestVector> = HashMap::new();
    let mut deleted_ids: HashSet<String> = HashSet::new();

    for fragment in fragments {
        // Separate upserts and deletes (as the production code does)
        let mut deletes: Vec<String> = Vec::new();
        let mut upserts: Vec<TestVector> = Vec::new();

        for op in &fragment.ops {
            match op {
                Op::Delete(id) => deletes.push(id.clone()),
                Op::Upsert(v) => upserts.push(v.clone()),
            }
        }

        // Process deletes first (matching production order within a fragment)
        for del_id in &deletes {
            deleted_ids.insert(del_id.clone());
            latest_vectors.remove(del_id);
        }

        // Then upserts
        for vec in &upserts {
            deleted_ids.remove(&vec.id);
            latest_vectors.insert(vec.id.clone(), vec.clone());
        }
    }

    latest_vectors
}

// ---- Proptest Strategies ----

fn arb_vector_id() -> impl Strategy<Value = String> {
    prop::string::string_regex("vec_[0-9]{1,2}")
        .unwrap()
        .prop_filter("non-empty", |s| !s.is_empty())
}

fn arb_values() -> impl Strategy<Value = Vec<f32>> {
    prop::collection::vec(prop::num::f32::NORMAL, 2..=4)
}

fn arb_test_vector() -> impl Strategy<Value = TestVector> {
    (arb_vector_id(), arb_values()).prop_map(|(id, values)| TestVector { id, values })
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        arb_test_vector().prop_map(Op::Upsert),
        arb_vector_id().prop_map(Op::Delete),
    ]
}

fn arb_fragment() -> impl Strategy<Value = TestFragment> {
    prop::collection::vec(arb_op(), 1..=5).prop_map(|ops| TestFragment { ops })
}

fn arb_fragments() -> impl Strategy<Value = Vec<TestFragment>> {
    prop::collection::vec(arb_fragment(), 1..=8)
}

// ---- Property Tests ----

proptest! {
    /// The production merge logic should produce the same surviving vectors
    /// as the reference implementation for any sequence of operations.
    #[test]
    fn merge_matches_reference(fragments in arb_fragments()) {
        let reference = reference_merge(&fragments);
        let production = production_merge(&fragments);

        // Same set of surviving vector IDs
        let ref_ids: HashSet<&String> = reference.keys().collect();
        let prod_ids: HashSet<&String> = production.keys().collect();
        prop_assert_eq!(&ref_ids, &prod_ids,
            "Surviving vector IDs differ");

        // Same values for each surviving vector
        for id in ref_ids {
            prop_assert_eq!(
                reference.get(id).map(|v| &v.values),
                production.get(id).map(|v| &v.values),
                "Values differ for vector {}", id
            );
        }
    }

    /// Deleting a vector then upserting it should make it visible.
    #[test]
    fn delete_then_upsert_survives(
        id in arb_vector_id(),
        values in arb_values(),
    ) {
        let fragments = vec![
            TestFragment { ops: vec![Op::Delete(id.clone())] },
            TestFragment { ops: vec![Op::Upsert(TestVector { id: id.clone(), values: values.clone() })] },
        ];
        let result = production_merge(&fragments);
        prop_assert!(result.contains_key(&id),
            "Vector should survive after delete-then-upsert");
        prop_assert_eq!(&result[&id].values, &values);
    }

    /// Upserting a vector then deleting it should remove it.
    #[test]
    fn upsert_then_delete_removes(
        id in arb_vector_id(),
        values in arb_values(),
    ) {
        let fragments = vec![
            TestFragment { ops: vec![Op::Upsert(TestVector { id: id.clone(), values })] },
            TestFragment { ops: vec![Op::Delete(id.clone())] },
        ];
        let result = production_merge(&fragments);
        prop_assert!(!result.contains_key(&id),
            "Vector should not survive after upsert-then-delete");
    }

    /// Multiple upserts of the same ID: last one wins.
    #[test]
    fn last_upsert_wins(
        id in arb_vector_id(),
        values1 in arb_values(),
        values2 in arb_values(),
    ) {
        let fragments = vec![
            TestFragment { ops: vec![Op::Upsert(TestVector { id: id.clone(), values: values1 })] },
            TestFragment { ops: vec![Op::Upsert(TestVector { id: id.clone(), values: values2.clone() })] },
        ];
        let result = production_merge(&fragments);
        prop_assert!(result.contains_key(&id));
        prop_assert_eq!(&result[&id].values, &values2,
            "Last upsert should win");
    }

    /// Empty fragments should not affect the result.
    #[test]
    fn empty_fragments_are_noop(fragments in arb_fragments()) {
        let without_empty = production_merge(&fragments);
        let mut with_empty = fragments.clone();
        with_empty.insert(0, TestFragment { ops: vec![] });
        with_empty.push(TestFragment { ops: vec![] });
        let with_empty_result = production_merge(&with_empty);

        let ids1: HashSet<&String> = without_empty.keys().collect();
        let ids2: HashSet<&String> = with_empty_result.keys().collect();
        prop_assert_eq!(ids1, ids2,
            "Empty fragments should not change the result");
    }
}
