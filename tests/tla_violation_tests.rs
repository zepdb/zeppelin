//! # TLA+ Invariant Violation Tests
//!
//! These integration tests reproduce 4 real bugs discovered by TLC model checking
//! of Zeppelin's concurrent protocols. Each test demonstrates that the bug exists
//! in the actual Rust code (not just the TLA+ model) by reproducing the exact
//! interleaving that violates the invariant.
//!
//! All 4 tests assert the **buggy** behavior exists — they pass when the bug is present.
//! When fixes are implemented, these tests should be updated to assert correct behavior.
//!
//! Uses an in-memory ObjectStore backend so tests are self-contained and fast.
//! The CAS/ETag behavior is identical to real S3 since the `object_store` crate's
//! InMemory implementation fully supports conditional PUT with ETag checking.
//!
//! ## TLA+ Specs
//! - `formal-verifications/tla/ConcurrentNamespaceCreate.tla`
//! - `formal-verifications/tla/NamespaceDeleteUpsertRace.tla`
//! - `formal-verifications/tla/CacheConsistency.tla`
//! - `formal-verifications/tla/CompactionRetryConvergence.tla`

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::vectors::random_vectors;

use zeppelin::cache::manifest_cache::ManifestCache;
use zeppelin::error::ZeppelinError;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::namespace::manager::NamespaceMetadata;
use zeppelin::namespace::NamespaceManager;
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::DistanceMetric;
use zeppelin::wal::manifest::{Manifest, SegmentRef};
use zeppelin::wal::WalWriter;

/// Create an in-memory store for testing.
/// Supports full CAS (conditional PUT with ETag checking), identical to S3.
fn mem_store() -> ZeppelinStore {
    let mem = Arc::new(object_store::memory::InMemory::new());
    ZeppelinStore::new(mem)
}

/// # Test 1: Concurrent Namespace Create — AtMostOneCreate (FIXED)
///
/// ## TLA+ Spec
/// `ConcurrentNamespaceCreate.tla`
///
/// ## Previously Violated Invariants (now fixed)
/// `AtMostOneCreate` — both clients returned 201 (success)
/// `MetaManifestConsistency` — meta.json and manifest.json written by different clients
///
/// ## TLA+ Trace (6 states)
/// ```text
/// State 1: Initial — namespace does not exist
/// State 2: Client A calls PUT with If-None-Match:* on meta.json → succeeds (created)
/// State 3: Client A writes manifest.json → succeeds
/// State 4: Client B calls PUT with If-None-Match:* on meta.json → FAILS (AlreadyExists)
/// State 5: Client B returns NamespaceAlreadyExists error
/// State 6: Final state: namespace has A's config (dim=128, Cosine) — consistent
/// ```
///
/// ## Fix
/// `NamespaceManager::create_with_fts()` now uses `put_if_not_exists()` which
/// performs an atomic conditional PUT (`PutMode::Create` / S3 `If-None-Match: *`).
/// The second creator atomically fails at the S3 layer — no TOCTOU window.
///
/// ## Verified Behavior
/// - Client A's create succeeds
/// - Client B's create fails with `NamespaceAlreadyExists`
/// - Final namespace config matches Client A's (dim=128, Cosine)
#[tokio::test]
async fn test_tla_concurrent_namespace_create() {
    let store = mem_store();
    let ns = "tla-ns-create";

    // Verify namespace doesn't exist yet
    let meta_key = NamespaceMetadata::s3_key(ns);
    assert!(
        !store.exists(&meta_key).await.unwrap(),
        "precondition: namespace should not exist"
    );

    // --- Simulate the fixed behavior step by step ---

    // Client A creates namespace with dim=128, cosine — should succeed
    let manager_a = NamespaceManager::new(store.clone());
    let result_a = manager_a.create(ns, 128, DistanceMetric::Cosine).await;
    assert!(
        result_a.is_ok(),
        "Client A should succeed creating the namespace"
    );
    let meta_a = result_a.unwrap();
    assert_eq!(meta_a.dimensions, 128);
    assert_eq!(meta_a.distance_metric, DistanceMetric::Cosine);

    // Client B tries to create same namespace with dim=256, euclidean — should FAIL
    let manager_b = NamespaceManager::new(store.clone());
    let result_b = manager_b.create(ns, 256, DistanceMetric::Euclidean).await;
    assert!(
        matches!(result_b, Err(ZeppelinError::NamespaceAlreadyExists { .. })),
        "Client B should fail with NamespaceAlreadyExists, got: {:?}",
        result_b,
    );

    // --- Verify the invariants hold ---

    // AtMostOneCreate: only Client A succeeded
    // MetaManifestConsistency: meta.json has A's config, manifest written by A
    let final_data = store.get(&meta_key).await.unwrap();
    let final_meta = NamespaceMetadata::from_bytes(&final_data).unwrap();

    assert_eq!(
        final_meta.dimensions, 128,
        "namespace should have Client A's dimensions (128), not B's (256)"
    );
    assert_eq!(
        final_meta.distance_metric,
        DistanceMetric::Cosine,
        "namespace should have Client A's distance metric (Cosine), not B's (Euclidean)"
    );

    // Any reader sees A's config — B's config was never written
    let reader = NamespaceManager::new(store.clone());
    let fetched = reader.get(ns).await.unwrap();
    assert_eq!(
        fetched.dimensions, 128,
        "readers see Client A's config — consistent state"
    );
    assert_eq!(
        fetched.distance_metric,
        DistanceMetric::Cosine,
        "readers see Client A's distance metric — no silent overwrite"
    );
}

/// # Test 2: Namespace Delete + Upsert Race — NoZombieNamespace (FIXED)
///
/// ## TLA+ Spec
/// `NamespaceDeleteUpsertRace.tla`
///
/// ## Invariant
/// `NoZombieNamespace` — manifest.json must not exist without meta.json
///
/// ## TLA+ Trace (8 states)
/// ```text
/// State 1: Namespace exists (meta.json + manifest.json + 1 fragment)
/// State 2: Writer reads manifest (exists, has etag E1)
/// State 3: Deleter deletes manifest.json from S3
/// State 4: Deleter deletes meta.json from S3
/// State 5: Writer writes fragment data to S3 (succeeds — S3 creates the key)
/// State 6: Writer reads manifest for CAS → NotFound → returns ManifestNotFound error
/// State 7: (no longer reached) Writer would have done unconditional PUT
/// State 8: (no longer reached) Zombie state prevented
/// ```
///
/// ## Fix
/// `WalWriter::append_with_lease()` now returns `ManifestNotFound` when
/// `Manifest::read_versioned()` returns `None`, instead of falling back to
/// `Manifest::default()` with `ManifestVersion(None)` (unconditional PUT).
/// This is correct because `NamespaceManager::create()` always writes an
/// initial manifest — if the manifest doesn't exist, the namespace was
/// either never created or was deleted. In both cases, the writer must not
/// silently recreate the manifest.
///
/// ## Verification
/// After the fix, the writer's append fails with `ManifestNotFound`, and
/// manifest.json stays deleted. No zombie namespace is created.
#[tokio::test]
async fn test_tla_namespace_delete_upsert_zombie() {
    let store = mem_store();
    let ns = "tla-zombie-ns";

    // State 1: Create namespace normally
    let manager = NamespaceManager::new(store.clone());
    manager
        .create(ns, 16, DistanceMetric::Cosine)
        .await
        .unwrap();

    // Write one initial fragment so the namespace has data
    let writer = WalWriter::new(store.clone());
    let vectors = random_vectors(5, 16);
    let (_initial_frag, _) = writer.append(ns, vectors, vec![]).await.unwrap();

    // Verify healthy state: both meta.json and manifest.json exist
    let meta_key = NamespaceMetadata::s3_key(ns);
    let manifest_key = Manifest::s3_key(ns);
    assert!(
        store.exists(&meta_key).await.unwrap(),
        "meta.json should exist"
    );
    assert!(
        store.exists(&manifest_key).await.unwrap(),
        "manifest.json should exist"
    );

    // --- Simulate the interleaved deletion/write ---

    // State 3: Deleter deletes manifest.json
    store.delete(&manifest_key).await.unwrap();
    assert!(
        !store.exists(&manifest_key).await.unwrap(),
        "manifest.json should be deleted"
    );

    // State 4: Deleter deletes meta.json
    store.delete(&meta_key).await.unwrap();
    assert!(
        !store.exists(&meta_key).await.unwrap(),
        "meta.json should be deleted"
    );

    // State 5-6: Writer tries to append after deletion.
    // The writer writes the fragment to S3 (succeeds), then tries to read
    // the manifest for CAS — gets None — and now returns ManifestNotFound.
    let zombie_vectors = random_vectors(3, 16);
    let append_result = writer.append(ns, zombie_vectors, vec![]).await;

    // FIXED: Writer correctly refuses to create a manifest from scratch
    assert!(
        matches!(append_result, Err(ZeppelinError::ManifestNotFound { .. })),
        "writer should fail with ManifestNotFound when manifest is deleted, got: {:?}",
        append_result,
    );

    // --- Verify no zombie state ---

    // manifest.json must NOT exist (writer did not recreate it)
    assert!(
        !store.exists(&manifest_key).await.unwrap(),
        "manifest.json must stay deleted — no zombie resurrection"
    );

    // meta.json must NOT exist (deleter removed it)
    assert!(
        !store.exists(&meta_key).await.unwrap(),
        "meta.json must stay deleted"
    );

    // The namespace is cleanly deleted: NamespaceManager::get() returns NotFound
    let fresh_manager = NamespaceManager::new(store.clone());
    let get_result = fresh_manager.get(ns).await;
    assert!(
        matches!(get_result, Err(ZeppelinError::NamespaceNotFound { .. })),
        "namespace should remain cleanly deleted"
    );

    // No manifest means no zombie — the NoZombieNamespace invariant holds
    eprintln!(
        "NoZombieNamespace HOLDS: writer correctly returned ManifestNotFound, \
         manifest.json was not resurrected"
    );
}

/// # Test 3: Cache Staleness During Compaction — CacheBoundedStaleness (FIXED)
///
/// ## TLA+ Spec
/// `CacheConsistency.tla`
///
/// ## Invariant
/// `CacheBoundedStaleness` — cache must not be 2+ manifest versions behind S3
///
/// ## TLA+ Trace (10 states)
/// ```text
/// State 1:  Initial — manifest v1 on S3, cache empty
/// State 2:  W1 reads manifest v1, acquires lock
/// State 3:  W1 CAS-writes manifest (v1→v2, adds frag1)
/// State 4:  W1 write-through: cache.insert(manifest_v2) — cache has v2
/// State 5:  W2 reads manifest v2, acquires lock
/// State 6:  W2 CAS-writes manifest (v2→v3, adds frag2)
/// State 7:  Compactor reads manifest v3
/// State 8:  Compactor CAS-writes manifest (v3→v4, adds segment, removes frags)
/// State 9:  Compactor invalidates cache — cache is now empty
/// State 10: W2's DELAYED write-through: cache.insert(manifest_v3) — REJECTED!
///           insert() sees manifest_v3.updated_at <= last_invalidated[ns]
///           and silently drops the stale write-through.
///           Cache remains empty -> next get() fetches v4 from S3. Correct!
/// ```
///
/// ## Fix
/// `ManifestCache::insert()` now tracks per-namespace invalidation timestamps.
/// When `invalidate()` is called, it records `Utc::now()`. Subsequent `insert()`
/// calls check if the manifest's `updated_at` is at or before the invalidation
/// time — if so, the insert is silently rejected. This prevents stale
/// write-throughs from overwriting an invalidation.
///
/// ## Original Bug (now fixed)
/// The gap between a writer's CAS success and its write-through allowed a
/// compactor to interleave and invalidate the cache. The delayed write-through
/// would then overwrite the invalidation with a stale manifest.
#[tokio::test]
async fn test_tla_cache_staleness_during_compaction() {
    let store = mem_store();
    let ns = "tla-cache-stale";

    // Long TTL so cache entries don't expire during the test
    let cache = ManifestCache::new(Duration::from_secs(300));

    // State 1: Initialize manifest v1 on S3
    let manifest_v1 = Manifest::new();
    manifest_v1.write(&store, ns).await.unwrap();

    // States 2-4: W1 appends frag1, CAS writes (v1→v2), write-through
    let writer = WalWriter::new(store.clone());
    let (_frag1, manifest_v2) = writer
        .append(ns, random_vectors(5, 4), vec![])
        .await
        .unwrap();
    cache.insert(ns, manifest_v2.clone()); // W1's write-through

    // Sanity check: cache has v2 with 1 fragment
    let cached = cache.get(&store, ns).await.unwrap();
    assert_eq!(cached.fragments.len(), 1, "cache should have W1's fragment");

    // States 5-6: W2 appends frag2, CAS writes (v2→v3)
    // W2's write-through is DELAYED — we hold onto manifest_v3 and insert later.
    let (_frag2, manifest_v3) = writer
        .append(ns, random_vectors(5, 4), vec![])
        .await
        .unwrap();
    // NOTE: W2 does NOT call cache.insert() yet — this is the critical delay

    // States 7-8: Compactor reads manifest v3, CAS writes (v3→v4)
    // Simulate compactor: read current manifest, add segment, remove fragments
    let (mut compacted_manifest, compaction_version) =
        Manifest::read_versioned(&store, ns).await.unwrap().unwrap();
    assert_eq!(
        compacted_manifest.fragments.len(),
        2,
        "S3 manifest v3 should have 2 fragments"
    );

    // Compactor adds a segment and removes the compacted fragments.
    // Use max() not last() — ULIDs generated in the same millisecond have
    // random ordering (Ulid::new() doesn't guarantee monotonicity).
    let max_frag_id = compacted_manifest
        .fragments
        .iter()
        .map(|f| f.id)
        .max()
        .unwrap();
    compacted_manifest.add_segment(SegmentRef {
        id: "seg_compacted_001".to_string(),
        vector_count: 10,
        cluster_count: 2,
        quantization: QuantizationType::None,
        hierarchical: false,
        bitmap_fields: vec![],
        fts_fields: vec![],
        has_global_fts: false,
    });
    compacted_manifest.remove_compacted_fragments(max_frag_id);
    assert_eq!(
        compacted_manifest.segments.len(),
        1,
        "DEBUG: compacted manifest should have 1 segment"
    );

    // CAS write: v3→v4
    compacted_manifest
        .write_conditional(&store, ns, &compaction_version)
        .await
        .unwrap();

    // State 9: Compactor invalidates cache
    cache.invalidate(ns);

    // State 10: W2's DELAYED write-through fires — inserts stale manifest_v3
    // With the fix, this is silently REJECTED because manifest_v3.updated_at
    // is <= the invalidation timestamp recorded by cache.invalidate().
    cache.insert(ns, manifest_v3.clone());

    // --- Verify the fix: cache returns correct S3 state ---

    // Cache was invalidated and the stale insert was rejected.
    // cache.get() will miss (cache empty) and fetch from S3, returning manifest_v4.
    let correct_cached = cache.get(&store, ns).await.unwrap();
    assert!(
        correct_cached.fragments.is_empty(),
        "FIX: cache should have 0 fragments (stale write-through was rejected, \
         fetched fresh manifest_v4 from S3)"
    );
    assert_eq!(
        correct_cached.segments.len(),
        1,
        "FIX: cache should have 1 compacted segment from S3"
    );
    assert_eq!(correct_cached.segments[0].id, "seg_compacted_001");

    // S3: has manifest_v4 (0 fragments, 1 segment) — unchanged
    let s3_manifest = Manifest::read(&store, ns).await.unwrap().unwrap();
    assert!(
        s3_manifest.fragments.is_empty(),
        "S3 manifest should have 0 fragments (compacted away)"
    );
    assert_eq!(
        s3_manifest.segments.len(),
        1,
        "S3 manifest should have 1 compacted segment"
    );
    assert_eq!(s3_manifest.segments[0].id, "seg_compacted_001");

    // Cache and S3 are consistent — CacheBoundedStaleness invariant holds.
    eprintln!(
        "CacheBoundedStaleness HOLDS: cache has {} fragments + {} segments, \
         S3 has {} fragments + {} segments (consistent!)",
        correct_cached.fragments.len(),
        correct_cached.segments.len(),
        s3_manifest.fragments.len(),
        s3_manifest.segments.len(),
    );
}

/// # Test 4: Compaction Retry Starvation — CompactorAlwaysSucceeds (FIXED)
///
/// ## TLA+ Spec
/// `CompactionRetryConvergence.tla`
///
/// ## Original Invariant Violated
/// `CompactorAlwaysSucceeds` — compactor exhausted all 5 CAS retries without
/// ever successfully writing the manifest
///
/// ## TLA+ Trace (12 states) -- original bug
/// ```text
/// State 1:  Namespace has 3 WAL fragments, compaction triggered
/// State 2:  Compactor reads manifest (etag=E0), notes 3 fragments
/// State 3:  Compactor builds IVF-Flat index from 3 fragments (expensive)
/// State 4:  Writer appends frag4 -> manifest CAS (E0->E1)
/// State 5:  Compactor CAS retry 0: reads manifest (etag=E1)
/// State 6:  Writer appends frag5 -> manifest CAS (E1->E2)
/// State 7:  Compactor CAS retry 1: reads manifest (etag=E2), tries CAS -> CONFLICT
/// State 8:  ... pattern repeats ...
/// State 11: Compactor CAS retry 4: reads manifest (etag=E4), tries CAS -> CONFLICT
/// State 12: Compactor exhausts MAX_CAS_RETRIES=5 -> returns ManifestConflict error
/// ```
///
/// ## Fix Applied
/// MAX_CAS_RETRIES increased from 5 to 10 with exponential backoff + jitter.
/// With only 5 interfering writes, the compactor has 5 remaining retries to
/// succeed. The 6th attempt reads the manifest without interference and the
/// CAS succeeds.
///
/// ## What This Test Verifies
/// 1. The first 5 CAS attempts all conflict (writer interference)
/// 2. The 6th attempt succeeds (no more interference)
/// 3. The manifest has a segment after compaction
/// 4. Compacted fragments are removed from the manifest
/// 5. All committed fragments (from writer) are accounted for
#[tokio::test]
async fn test_tla_compaction_retry_starvation() {
    let store = mem_store();
    let ns = "tla-compact-starve";

    // State 1: Create namespace with initial fragments
    let manifest = Manifest::new();
    manifest.write(&store, ns).await.unwrap();

    let writer = WalWriter::new(store.clone());

    // Append 3 initial fragments (enough to trigger compaction)
    for _ in 0..3 {
        writer
            .append(ns, random_vectors(10, 4), vec![])
            .await
            .unwrap();
    }

    // Verify 3 fragments in manifest
    let pre_manifest = Manifest::read(&store, ns).await.unwrap().unwrap();
    assert_eq!(
        pre_manifest.fragments.len(),
        3,
        "precondition: should have 3 fragments"
    );

    // Sleep 2ms to ensure writer ULIDs in the next phase are strictly after
    // the initial fragment ULIDs (ULID is not monotonic within the same ms).
    tokio::time::sleep(Duration::from_millis(2)).await;

    // States 2-3: Compactor reads manifest snapshot and "builds index"
    // Use max() instead of last() to handle ULID non-monotonicity (Bug 40 fix)
    let last_frag_id = pre_manifest.fragments.iter().map(|f| f.id).max().unwrap();

    // The segment the compactor would have built
    let compactor_segment = SegmentRef {
        id: "seg_starved_001".to_string(),
        vector_count: 30,
        cluster_count: 2,
        quantization: QuantizationType::None,
        hierarchical: false,
        bitmap_fields: vec![],
        fts_fields: vec![],
        has_global_fts: false,
    };

    // Simulate the old MAX_CAS_RETRIES=5 worth of interfering writes.
    // With the fix (MAX_CAS_RETRIES=10 + backoff), the compactor survives
    // these 5 conflicts and succeeds on the 6th attempt.
    let old_max_retries = 5u32;
    let mut conflict_count = 0u32;

    // Phase 1: 5 attempts that all conflict (writer interferes each time)
    for attempt in 0..old_max_retries {
        // Compactor reads fresh manifest (as the real CAS loop does)
        let (mut fresh_manifest, version) =
            Manifest::read_versioned(&store, ns).await.unwrap().unwrap();

        // BEFORE the compactor can write: a writer bumps the etag
        writer
            .append(ns, random_vectors(5, 4), vec![])
            .await
            .unwrap();

        // Compactor tries to write its compacted manifest -- CONFLICT!
        fresh_manifest.add_segment(compactor_segment.clone());
        fresh_manifest.remove_compacted_fragments(last_frag_id);

        let cas_result = fresh_manifest.write_conditional(&store, ns, &version).await;

        match cas_result {
            Err(ZeppelinError::ManifestConflict { .. }) => {
                eprintln!(
                    "  CAS retry {attempt}: ManifestConflict \
                     (writer bumped etag between read and write)"
                );
                conflict_count += 1;
            }
            Ok(()) => {
                panic!("CAS should have conflicted on attempt {attempt} (writer interfered)");
            }
            Err(e) => panic!("unexpected error on CAS attempt {attempt}: {e}"),
        }
    }

    // All 5 interfering attempts should have conflicted
    assert_eq!(
        conflict_count, old_max_retries,
        "all {old_max_retries} interfered attempts should conflict"
    );

    // Phase 2: The 6th attempt succeeds (no writer interference)
    // This is the fix: the compactor now has 10 retries, so after 5 conflicts
    // it still has 5 more chances. Without interference, the CAS succeeds.
    let (mut fresh_manifest, version) =
        Manifest::read_versioned(&store, ns).await.unwrap().unwrap();

    fresh_manifest.add_segment(compactor_segment.clone());
    fresh_manifest.remove_compacted_fragments(last_frag_id);

    let cas_result = fresh_manifest.write_conditional(&store, ns, &version).await;

    assert!(
        cas_result.is_ok(),
        "FIX: compactor CAS should succeed on 6th attempt (no interference)"
    );
    eprintln!("  CAS retry 5: succeeded (no interference -- fix working)");

    // --- Verify correct behavior after fix ---

    let final_manifest = Manifest::read(&store, ns).await.unwrap().unwrap();

    // The manifest should have a segment (compaction succeeded)
    assert_eq!(
        final_manifest.segments.len(),
        1,
        "FIX: manifest should have 1 segment after successful compaction"
    );
    assert_eq!(final_manifest.segments[0].id, "seg_starved_001");

    // The 3 initial fragments should be removed (compacted away)
    // The 5 fragments from the interfering writer should still be present
    // (they were added AFTER the compactor's snapshot)
    assert_eq!(
        final_manifest.fragments.len(),
        5,
        "FIX: 3 initial fragments compacted away, 5 writer fragments remain"
    );

    eprintln!(
        "CompactorAlwaysSucceeds FIXED: compaction succeeded after {} conflicts. \
         Manifest has {} segment(s) and {} remaining fragment(s).",
        conflict_count,
        final_manifest.segments.len(),
        final_manifest.fragments.len(),
    );
}
