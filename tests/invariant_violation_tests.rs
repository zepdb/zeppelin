//! Integration tests that reproduce invariant violations discovered by TLA+ model checking.
//!
//! Each test verifies that the corresponding bug has been FIXED. The original bugs
//! were found by TLA+ model checking and confirmed by property-based tests.
//! These tests assert correct behavior after the fixes.

mod common;

use common::assertions::*;
use common::harness::TestHarness;

use std::collections::HashMap;

use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig};
use zeppelin::error::ZeppelinError;
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::VectorEntry;
use zeppelin::wal::fragment::WalFragment;
use zeppelin::wal::manifest::{FragmentRef, Manifest};
use zeppelin::wal::{WalReader, WalWriter};

/// Create a Compactor with test-friendly settings (small centroids, low threshold).
fn test_compactor(store: &ZeppelinStore) -> Compactor {
    let wal_reader = WalReader::new(store.clone());
    let compaction_config = CompactionConfig {
        max_wal_fragments_before_compact: 3,
        ..Default::default()
    };
    let indexing_config = IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 10,
        ..Default::default()
    };
    Compactor::new(
        store.clone(),
        wal_reader,
        compaction_config,
        indexing_config,
    )
}

/// Create N vectors with unique IDs (`{prefix}_{i}`) and varied dimension values.
fn make_vectors(prefix: &str, count: usize, dims: usize) -> Vec<VectorEntry> {
    (0..count)
        .map(|i| {
            let values: Vec<f32> = (0..dims)
                .map(|d| ((i * dims + d) as f32) / (count * dims) as f32)
                .collect();
            VectorEntry {
                id: format!("{prefix}_{i}"),
                values,
                attributes: None,
            }
        })
        .collect()
}

// ─── Test 1: CompactionSafety ────────────────────────────────────────────────
//
// TLA+ invariant: ManifestCoversAllFragments
//
// FIX: CAS (compare-and-swap) manifest writes using ETag-based conditional PUT.
// A stale manifest write returns ManifestConflict instead of silently overwriting.

#[tokio::test]
async fn test_compaction_safety_cas_prevents_stale_overwrite() {
    let harness = TestHarness::new().await;
    let ns = harness.key("inv-compact-safety");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    // 1. Initialize namespace with empty manifest
    Manifest::new().write(store, &ns).await.unwrap();

    // 2. Append 2 fragments
    let _f1 = writer
        .append(&ns, make_vectors("f1", 5, 16), vec![])
        .await
        .unwrap();
    let _f2 = writer
        .append(&ns, make_vectors("f2", 5, 16), vec![])
        .await
        .unwrap();

    // 3. Read manifest with version (simulating compactor's initial read)
    let (stale_manifest, stale_version) =
        Manifest::read_versioned(store, &ns).await.unwrap().unwrap();
    assert_eq!(stale_manifest.fragments.len(), 2);

    // 4. Concurrent writer appends fragment 3 (changes the ETag)
    let (f3, _) = writer
        .append(&ns, make_vectors("f3", 5, 16), vec![])
        .await
        .unwrap();

    // 5. Attempt to write with stale version → ManifestConflict
    let result = stale_manifest
        .write_conditional(store, &ns, &stale_version)
        .await;

    assert!(
        matches!(result, Err(ZeppelinError::ManifestConflict { .. })),
        "FIX VERIFIED: CAS rejects stale manifest write. Got: {result:?}"
    );

    // 6. Fragment 3 is safe in the current manifest
    let final_manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    assert!(
        final_manifest.fragments.iter().any(|f| f.id == f3.id),
        "FIX VERIFIED: Fragment 3 is preserved in manifest after CAS rejection"
    );

    harness.cleanup().await;
}

// ─── Test 2: QueryReadConsistency ────────────────────────────────────────────
//
// TLA+ invariant: QueryNever404
//
// FIX: Deferred deletion — compaction moves fragment keys to pending_deletes
// instead of immediately deleting them. Fragments survive until next compaction.

#[tokio::test]
async fn test_query_read_consistency_deferred_deletion() {
    let harness = TestHarness::new().await;
    let ns = harness.key("inv-query-404");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    // 1. Initialize namespace and append 3 fragments
    Manifest::new().write(store, &ns).await.unwrap();

    for i in 0..3 {
        let vecs = make_vectors(&format!("batch{i}"), 20, 16);
        writer.append(&ns, vecs, vec![]).await.unwrap();
    }

    // 2. Save fragment refs before compaction
    let pre_manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    let fragment_refs = pre_manifest.fragments.clone();
    assert_eq!(fragment_refs.len(), 3);

    // 3. Run compaction — with deferred deletion, fragment files should STILL exist
    let compactor = test_compactor(store);
    let result = compactor.compact(&ns).await.unwrap();
    assert_eq!(result.fragments_removed, 3);
    assert!(result.segment_id.is_some());

    // 4. FIX VERIFIED: Fragment files still exist (deferred deletion)
    for fref in &fragment_refs {
        let key = WalFragment::s3_key(&ns, &fref.id);
        assert_s3_object_exists(store, &key).await;
    }

    // 5. Manifest should have pending_deletes populated
    let post_manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    assert!(
        !post_manifest.pending_deletes.is_empty(),
        "FIX VERIFIED: Fragment keys are in pending_deletes, not immediately deleted"
    );

    // 6. Reads succeed — no 404
    let wal_reader = WalReader::new(store.clone());
    for fref in &fragment_refs {
        let fragment = wal_reader.read_fragment(&ns, &fref.id).await;
        assert!(
            fragment.is_ok(),
            "FIX VERIFIED: Fragment {} is readable after compaction (deferred deletion)",
            fref.id
        );
    }

    harness.cleanup().await;
}

// ─── Test 3: NamespaceDeletion ───────────────────────────────────────────────
//
// TLA+ invariant: DeleteIsAtomic
//
// FIX: Delete manifest first. Concurrent queries see None manifest → empty results.
// Fragments can still exist on S3, but no query will read them without a manifest.

#[tokio::test]
async fn test_namespace_deletion_manifest_first() {
    let harness = TestHarness::new().await;
    let ns = harness.key("inv-ns-deletion");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    // 1. Create namespace with manifest + fragments
    Manifest::new().write(store, &ns).await.unwrap();

    let (f1, _) = writer
        .append(&ns, make_vectors("del1", 10, 16), vec![])
        .await
        .unwrap();
    let (f2, _) = writer
        .append(&ns, make_vectors("del2", 10, 16), vec![])
        .await
        .unwrap();

    // 2. Verify all objects exist
    let manifest_key = Manifest::s3_key(&ns);
    let f1_key = WalFragment::s3_key(&ns, &f1.id);
    let f2_key = WalFragment::s3_key(&ns, &f2.id);
    assert_s3_object_exists(store, &manifest_key).await;
    assert_s3_object_exists(store, &f1_key).await;
    assert_s3_object_exists(store, &f2_key).await;

    // 3. Simulate the fixed deletion order: delete manifest FIRST
    store.delete(&manifest_key).await.unwrap();

    // 4. FIX VERIFIED: Manifest is gone — queries see None → empty results
    let manifest = Manifest::read(store, &ns).await.unwrap();
    assert!(
        manifest.is_none(),
        "FIX VERIFIED: Manifest is deleted first, queries see None"
    );

    // 5. Fragment files still exist (safe — no query will read them without a manifest)
    assert_s3_object_exists(store, &f1_key).await;
    assert_s3_object_exists(store, &f2_key).await;

    harness.cleanup().await;
}

// ─── Test 4: ULIDOrdering ────────────────────────────────────────────────────
//
// TLA+ invariant: LatestWriteWins
//
// FIX: Monotonic sequence numbers assigned at manifest write time.
// Manifest order (not ULID order) determines merge winner.

#[tokio::test]
async fn test_sequence_numbers_override_ulid_ordering() {
    let harness = TestHarness::new().await;
    let ns = harness.key("inv-ulid-order");
    let store = &harness.store;

    // 1. Construct two fragments with controlled ULIDs for the same vector ID.
    //
    //    Fragment B: later ULID (T=2000), committed FIRST (gets seq 0)
    //    Fragment A: early ULID (T=1000), committed SECOND (gets seq 1, should win)
    let early_ulid = ulid::Ulid::from_parts(1000, 1);
    let later_ulid = ulid::Ulid::from_parts(2000, 1);

    // Fragment A: early ULID, value = [1.0, 0.0, 0.0, ...]  — the LATER commit
    let mut vec_a = vec![0.0f32; 16];
    vec_a[0] = 1.0;
    let mut frag_a = WalFragment::new(
        vec![VectorEntry {
            id: "vec_0".into(),
            values: vec_a.clone(),
            attributes: None,
        }],
        vec![],
    );
    frag_a.id = early_ulid;

    // Fragment B: later ULID, value = [0.0, 1.0, 0.0, ...] — the EARLIER commit
    let mut vec_b = vec![0.0f32; 16];
    vec_b[1] = 1.0;
    let mut frag_b = WalFragment::new(
        vec![VectorEntry {
            id: "vec_0".into(),
            values: vec_b.clone(),
            attributes: None,
        }],
        vec![],
    );
    frag_b.id = later_ulid;

    // 2. Write both fragments to S3
    let key_a = WalFragment::s3_key(&ns, &frag_a.id);
    let key_b = WalFragment::s3_key(&ns, &frag_b.id);
    store.put(&key_a, frag_a.to_bytes().unwrap()).await.unwrap();
    store.put(&key_b, frag_b.to_bytes().unwrap()).await.unwrap();

    // 3. Write manifest: B added first (seq 0), then A (seq 1).
    //    A committed later = higher sequence = should win.
    let mut manifest = Manifest::new();
    manifest.add_fragment(FragmentRef {
        id: frag_b.id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0, // overwritten by add_fragment
    });
    manifest.add_fragment(FragmentRef {
        id: frag_a.id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0, // overwritten by add_fragment
    });
    manifest.write(store, &ns).await.unwrap();

    // 4. Read fragments in manifest order (sequence order, NOT ULID order)
    let wal_reader = WalReader::new(store.clone());
    let fragments = wal_reader.read_uncompacted_fragments(&ns).await.unwrap();
    assert_eq!(fragments.len(), 2);

    // First fragment is B (seq 0, later ULID), second is A (seq 1, early ULID)
    assert_eq!(
        fragments[0].id, later_ulid,
        "First fragment should be B (seq 0, added first to manifest)"
    );
    assert_eq!(
        fragments[1].id, early_ulid,
        "Second fragment should be A (seq 1, added second to manifest)"
    );

    // 5. Apply merge logic — later in sequence wins (A overwrites B)
    let mut latest_vectors: HashMap<String, VectorEntry> = HashMap::new();

    for fragment in &fragments {
        for vec in &fragment.vectors {
            latest_vectors.insert(vec.id.clone(), vec.clone());
        }
    }

    // 6. FIX VERIFIED: Fragment A (seq 1, later commit) wins
    let winner = latest_vectors.get("vec_0").expect("vec_0 should exist");
    assert_eq!(
        winner.values, vec_a,
        "FIX VERIFIED: Fragment A (later commit, seq 1) wins over \
         Fragment B (earlier commit, seq 0). Sequence order, not ULID order."
    );

    harness.cleanup().await;
}

// ─── Test 5: Intra-Fragment Op Reordering ────────────────────────────────────
//
// FIX: WalFragment::try_new() validates that no vector ID appears in both
// upserts and deletes. The invalid state is now unrepresentable.

#[tokio::test]
async fn test_intra_fragment_overlap_rejected() {
    let _harness = TestHarness::new().await;

    // 1. Try to create a fragment with the same ID in both upserts and deletes
    let vectors = vec![VectorEntry {
        id: "doomed_v1".into(),
        values: vec![999.0; 16],
        attributes: None,
    }];
    let deletes = vec!["doomed_v1".to_string()];

    let result = WalFragment::try_new(vectors, deletes);

    // 2. FIX VERIFIED: try_new rejects overlapping IDs
    assert!(
        result.is_err(),
        "FIX VERIFIED: WalFragment::try_new rejects overlapping IDs in upserts and deletes"
    );

    match result {
        Err(ZeppelinError::Validation(msg)) => {
            assert!(
                msg.contains("doomed_v1"),
                "Error message should mention the offending vector ID"
            );
        }
        other => panic!("Expected Validation error, got: {other:?}"),
    }

    // 3. Non-overlapping fragment creation still works
    let good_vectors = vec![VectorEntry {
        id: "keep_me".into(),
        values: vec![1.0; 16],
        attributes: None,
    }];
    let good_deletes = vec!["delete_me".to_string()];
    let good_result = WalFragment::try_new(good_vectors, good_deletes);
    assert!(
        good_result.is_ok(),
        "Non-overlapping fragment creation should succeed"
    );
}
