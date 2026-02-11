//! Integration tests that reproduce invariant violations discovered by TLA+ model checking.
//!
//! Each test demonstrates a real bug in Zeppelin by simulating the exact interleaving
//! or condition that breaks the invariant. Tests PASS when the bug exists (they assert
//! the buggy behavior). When the bugs are later fixed, these tests should FAIL — then
//! flip the assertions to confirm the fix.

mod common;

use common::assertions::*;
use common::harness::TestHarness;

use std::collections::{HashMap, HashSet};

use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig};
use zeppelin::error::ZeppelinError;
use zeppelin::query::execute_query;
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::{ConsistencyLevel, DistanceMetric, VectorEntry};
use zeppelin::wal::fragment::WalFragment;
use zeppelin::wal::manifest::{FragmentRef, Manifest, SegmentRef};
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
    Compactor::new(store.clone(), wal_reader, compaction_config, indexing_config)
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
// Bug: Compactor reads manifest, builds segment (slow), then writes manifest back.
// A concurrent writer appends a new fragment and updates the manifest in between.
// The compactor's stale write erases the new fragment reference. The fragment's data
// is still on S3, but no manifest points to it — data loss.

#[tokio::test]
async fn test_compaction_safety_stale_manifest_overwrites_fragment() {
    let harness = TestHarness::new().await;
    let ns = harness.key("inv-compact-safety");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    // 1. Initialize namespace with empty manifest
    Manifest::new().write(store, &ns).await.unwrap();

    // 2. Append 2 fragments (existing data before compaction starts)
    let _f1 = writer
        .append(&ns, make_vectors("f1", 5, 16), vec![])
        .await
        .unwrap();
    let f2 = writer
        .append(&ns, make_vectors("f2", 5, 16), vec![])
        .await
        .unwrap();

    // 3. Compactor reads manifest — this is its stale snapshot (compaction/mod.rs:80)
    let stale_manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    assert_eq!(stale_manifest.fragments.len(), 2);

    // 4. Concurrent writer appends fragment 3 AFTER the compactor read the manifest
    let f3 = writer
        .append(&ns, make_vectors("f3", 5, 16), vec![])
        .await
        .unwrap();

    // Verify f3 is in the real (current) manifest on S3
    let current_manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    assert_eq!(current_manifest.fragments.len(), 3);
    assert!(current_manifest.fragments.iter().any(|f| f.id == f3.id));

    // 5. Compactor finishes building segment from stale snapshot:
    //    removes fragments 1 & 2, adds a segment — has NO IDEA about fragment 3.
    let mut compacted = stale_manifest;
    compacted.remove_compacted_fragments(f2.id);
    compacted.add_segment(SegmentRef {
        id: "fake_seg_001".into(),
        vector_count: 10,
        cluster_count: 2,
    });
    // Compactor writes stale manifest back — OVERWRITES the current one
    compacted.write(store, &ns).await.unwrap();

    // 6. ASSERT: Fragment 3's data exists on S3 but is NOT in the manifest.
    //    This is data loss: fragment 3 is invisible to all future reads.
    let f3_key = WalFragment::s3_key(&ns, &f3.id);
    assert_s3_object_exists(store, &f3_key).await;

    let final_manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    let f3_in_manifest = final_manifest.fragments.iter().any(|f| f.id == f3.id);
    assert!(
        !f3_in_manifest,
        "BUG CONFIRMED: Fragment 3 is on S3 but MISSING from manifest — data loss!"
    );

    // The stale manifest only knows about the segment, not fragment 3
    assert_eq!(final_manifest.fragments.len(), 0);
    assert_eq!(final_manifest.segments.len(), 1);
    assert_eq!(final_manifest.segments[0].id, "fake_seg_001");

    harness.cleanup().await;
}

// ─── Test 2: QueryReadConsistency ────────────────────────────────────────────
//
// TLA+ invariant: QueryNever404
//
// Bug: Query reads manifest (sees fragment references), then compaction runs and
// deletes those .wal files from S3. When the query tries to read the fragments
// using its stale manifest snapshot, it gets 404 errors.

#[tokio::test]
async fn test_query_read_consistency_fragment_404_after_compaction() {
    let harness = TestHarness::new().await;
    let ns = harness.key("inv-query-404");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    // 1. Initialize namespace and append 3 fragments with enough vectors for compaction
    Manifest::new().write(store, &ns).await.unwrap();

    for i in 0..3 {
        let vecs = make_vectors(&format!("batch{i}"), 20, 16);
        writer.append(&ns, vecs, vec![]).await.unwrap();
    }

    // 2. Query reads manifest and saves fragment list (stale snapshot — query.rs:33)
    let query_manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    let stale_fragment_refs = query_manifest.fragments.clone();
    assert_eq!(
        stale_fragment_refs.len(),
        3,
        "Should have 3 fragments before compaction"
    );

    // Verify fragment files exist right now
    for fref in &stale_fragment_refs {
        let key = WalFragment::s3_key(&ns, &fref.id);
        assert_s3_object_exists(store, &key).await;
    }

    // 3. Compaction runs — compacts fragments into a segment AND deletes .wal files
    let compactor = test_compactor(store);
    let result = compactor.compact(&ns).await.unwrap();
    assert_eq!(result.fragments_removed, 3);
    assert!(result.segment_id.is_some());

    // 4. Query tries to read fragments using its stale manifest —
    //    the .wal files have been deleted by compaction.
    let mut not_found_count = 0;
    for fref in &stale_fragment_refs {
        let key = WalFragment::s3_key(&ns, &fref.id);
        match store.get(&key).await {
            Err(ZeppelinError::NotFound { .. }) => not_found_count += 1,
            Ok(_) => panic!(
                "Fragment {} should have been deleted by compaction",
                fref.id
            ),
            Err(e) => panic!("Unexpected error reading fragment {}: {e}", fref.id),
        }
    }

    // 5. ASSERT: All fragment reads fail with NotFound — query gets 404
    assert_eq!(
        not_found_count,
        stale_fragment_refs.len(),
        "BUG CONFIRMED: All {} fragments return 404 after compaction — query would fail!",
        stale_fragment_refs.len()
    );

    harness.cleanup().await;
}

// ─── Test 3: NamespaceDeletion ───────────────────────────────────────────────
//
// TLA+ invariant: DeleteIsAtomic
//
// Bug: delete_namespace calls delete_prefix which deletes S3 keys one-by-one
// (non-atomic). During deletion, a query can read the manifest (not yet deleted)
// but find fragments already gone — partial state.

#[tokio::test]
async fn test_namespace_deletion_partial_state() {
    let harness = TestHarness::new().await;
    let ns = harness.key("inv-ns-deletion");
    let store = &harness.store;
    let writer = WalWriter::new(store.clone());

    // 1. Create namespace with manifest + fragments
    Manifest::new().write(store, &ns).await.unwrap();

    let f1 = writer
        .append(&ns, make_vectors("del1", 10, 16), vec![])
        .await
        .unwrap();
    let f2 = writer
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

    // 3. Simulate partial deletion: delete one fragment but NOT the manifest.
    //    This is what happens mid-way through delete_prefix (manager.rs:170).
    store.delete(&f1_key).await.unwrap();

    // 4. Read manifest — it still exists and still references the deleted fragment
    let manifest = Manifest::read(store, &ns).await.unwrap().unwrap();
    let f1_still_referenced = manifest.fragments.iter().any(|f| f.id == f1.id);
    assert!(
        f1_still_referenced,
        "Manifest should still reference fragment 1 (it hasn't been deleted yet)"
    );

    // 5. Try to read the deleted fragment via the manifest's reference
    let result = store.get(&f1_key).await;

    // 6. ASSERT: Manifest is readable but references a non-existent fragment — partial state
    assert!(
        matches!(result, Err(ZeppelinError::NotFound { .. })),
        "BUG CONFIRMED: Manifest references fragment that no longer exists — partial state!"
    );

    // Fragment 2 and manifest still exist (only f1 was deleted so far)
    assert_s3_object_exists(store, &f2_key).await;
    assert_s3_object_exists(store, &manifest_key).await;

    harness.cleanup().await;
}

// ─── Test 4: ULIDOrdering ────────────────────────────────────────────────────
//
// TLA+ invariant: LatestWriteWins
//
// Bug: ULID ordering determines which value wins during merge, not commit order.
// If clock skew causes a later-committed write to have an EARLIER ULID, its value
// is treated as older and gets overwritten by the earlier-committed write.
//
// Scenario: Writer W1 starts at T=1000 (early ULID), Writer W2 starts at T=2000
// (later ULID). W2 commits first, then W1 commits second. W1's value should be
// authoritative (latest commit), but W2 wins because it has the later ULID.

#[tokio::test]
async fn test_ulid_ordering_clock_skew_wrong_value_wins() {
    let harness = TestHarness::new().await;
    let ns = harness.key("inv-ulid-order");
    let store = &harness.store;

    // 1. Construct two fragments with controlled ULIDs for the same vector ID.
    //
    //    Fragment A: early ULID (T=1000), the LAST commit (should be authoritative)
    //    Fragment B: later ULID (T=2000), committed FIRST (should be overridden)
    let early_ulid = ulid::Ulid::from_parts(1000, 1);
    let later_ulid = ulid::Ulid::from_parts(2000, 1);

    // Fragment A: early ULID, value = [1.0, 0.0, 0.0, ...]
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

    // Fragment B: later ULID, value = [0.0, 1.0, 0.0, ...]
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
    store
        .put(&key_a, frag_a.to_bytes().unwrap())
        .await
        .unwrap();
    store
        .put(&key_b, frag_b.to_bytes().unwrap())
        .await
        .unwrap();

    // 3. Write manifest referencing both fragments (A listed before B)
    let mut manifest = Manifest::new();
    manifest.add_fragment(FragmentRef {
        id: frag_a.id,
        vector_count: 1,
        delete_count: 0,
    });
    manifest.add_fragment(FragmentRef {
        id: frag_b.id,
        vector_count: 1,
        delete_count: 0,
    });
    manifest.write(store, &ns).await.unwrap();

    // 4. Read fragments the way query/compaction does (sorted by ULID)
    let wal_reader = WalReader::new(store.clone());
    let fragments = wal_reader
        .read_uncompacted_fragments(&ns)
        .await
        .unwrap();
    assert_eq!(fragments.len(), 2);
    assert_eq!(
        fragments[0].id, early_ulid,
        "Fragments should be sorted by ULID — early first"
    );
    assert_eq!(
        fragments[1].id, later_ulid,
        "Fragments should be sorted by ULID — later second"
    );

    // 5. Apply the production merge logic (same as compaction/mod.rs:107-120)
    let mut latest_vectors: HashMap<String, VectorEntry> = HashMap::new();
    let mut deleted_ids: HashSet<String> = HashSet::new();

    for fragment in &fragments {
        for del_id in &fragment.deletes {
            deleted_ids.insert(del_id.clone());
            latest_vectors.remove(del_id);
        }
        for vec in &fragment.vectors {
            deleted_ids.remove(&vec.id);
            latest_vectors.insert(vec.id.clone(), vec.clone());
        }
    }

    // 6. ASSERT: Fragment B (later ULID) wins, even though Fragment A committed later.
    //    In a real concurrent scenario, W1 (early ULID) committed AFTER W2 (later ULID),
    //    so W1's value should be authoritative. But ULID ordering makes W2 win.
    let winner = latest_vectors.get("vec_0").expect("vec_0 should exist");
    assert_eq!(
        winner.values, vec_b,
        "BUG CONFIRMED: Later ULID wins, not later commit. \
         Fragment B (earlier commit, ULID={later_ulid}) overwrites \
         Fragment A (later commit, ULID={early_ulid})"
    );
    assert_ne!(
        winner.values, vec_a,
        "Fragment A's value (the later commit) was lost due to ULID ordering"
    );

    harness.cleanup().await;
}

// ─── Test 5: Intra-Fragment Op Reordering ────────────────────────────────────
//
// Bug (from proptest): Within a single fragment, compaction processes ALL deletes
// before ALL upserts. If a fragment contains an Upsert then Delete for the same ID
// (intent: insert it, then remove it → should be gone), the delete runs first (no-op
// since the vector isn't in latest_vectors yet), then the upsert adds it. Result: the
// vector survives even though the user's intent was to delete it.

#[tokio::test]
async fn test_intra_fragment_op_reordering_delete_before_upsert() {
    let harness = TestHarness::new().await;
    let ns = harness.key("inv-op-reorder");
    let store = &harness.store;

    // 1. Create a fragment that represents "upsert doomed_v1, then delete doomed_v1"
    //    The fragment has doomed_v1 in BOTH vectors AND deletes.
    //    Intent: doomed_v1 should NOT exist after processing this fragment.
    //
    //    Include enough other vectors for k-means to converge (need >= 4 for 4 centroids).
    let mut vectors = make_vectors("keep", 20, 16);
    vectors.push(VectorEntry {
        id: "doomed_v1".into(),
        values: vec![999.0; 16], // extreme values to make it easy to find in query
        attributes: None,
    });
    let deletes = vec!["doomed_v1".to_string()];
    let fragment = WalFragment::new(vectors, deletes);

    // 2. Write fragment to S3 and create manifest
    let frag_key = WalFragment::s3_key(&ns, &fragment.id);
    store
        .put(&frag_key, fragment.to_bytes().unwrap())
        .await
        .unwrap();

    let mut manifest = Manifest::new();
    manifest.add_fragment(FragmentRef {
        id: fragment.id,
        vector_count: 21,
        delete_count: 1,
    });
    manifest.write(store, &ns).await.unwrap();

    // 3. Run compaction — this applies the buggy merge logic
    let compactor = test_compactor(store);
    let result = compactor.compact(&ns).await.unwrap();
    assert!(result.segment_id.is_some(), "Compaction should produce a segment");

    // 4. Query for doomed_v1 using Eventual consistency (segment-only, bypasses WAL)
    let wal_reader = WalReader::new(store.clone());
    let query_vec = vec![999.0; 16]; // exact match for doomed_v1
    let query_result = execute_query(
        store,
        &wal_reader,
        &ns,
        &query_vec,
        25, // large enough top_k to return all vectors
        4,
        None,
        ConsistencyLevel::Eventual,
        DistanceMetric::Euclidean,
        3,
        None,
    )
    .await
    .unwrap();

    // 5. ASSERT: doomed_v1 survives in the segment despite the delete.
    //    Because compaction processes deletes before upserts within a fragment:
    //      - Delete "doomed_v1": no-op (not in latest_vectors yet)
    //      - Upsert "doomed_v1": adds it to latest_vectors, removes from deleted_ids
    //    Result: doomed_v1 is alive in the segment.
    let doomed_found = query_result.results.iter().any(|r| r.id == "doomed_v1");
    assert!(
        doomed_found,
        "BUG CONFIRMED: doomed_v1 survives in segment despite delete in same fragment. \
         Compaction processes deletes before upserts within a fragment, so the delete \
         is a no-op and the upsert resurrects the vector. \
         Compacted {} vectors into segment.",
        result.vectors_compacted
    );

    harness.cleanup().await;
}
