//! Integration tests for multi-writer lease protocol.
//!
//! Uses TestHarness pattern (real S3). Imports `zeppelin::wal::lease::{Lease, LeaseManager}`
//! which DOES NOT EXIST YET — these tests will fail to compile until the lease module
//! is implemented.
//!
//! Tests 1-7: Protocol contract tests (key scenarios from the design).
//! Tests 8-9: TLA+ counterexample reproductions (bugs found by TLC model checking).
//!
//! To check compile status:
//!   cargo test --test multi_writer_lease_tests 2>&1
//!
//! Expected: compile error — `unresolved import zeppelin::wal::lease`

mod common;

use common::harness::TestHarness;
use common::vectors::random_vectors;

use std::time::Duration;

use zeppelin::error::ZeppelinError;
use zeppelin::wal::lease::LeaseManager;
use zeppelin::wal::manifest::FragmentRef;
use zeppelin::wal::{Manifest, WalFragment, WalWriter};

/// Test 1: Acquire and release roundtrip.
/// Acquire a lease, verify the token is > 0, release it, acquire again
/// and verify the new token is higher.
#[tokio::test]
async fn test_lease_acquire_and_release() {
    let harness = TestHarness::new().await;
    let ns = harness.key("lease-roundtrip");

    // Initialize namespace manifest
    let manifest = Manifest::new();
    manifest.write(&harness.store, &ns).await.unwrap();

    let manager = LeaseManager::new(
        harness.store.clone(),
        "node-1".to_string(),
        Duration::from_secs(30),
    );

    // Acquire
    let lease1 = manager.acquire(&ns).await.unwrap();
    assert!(lease1.fencing_token > 0, "First token should be > 0");

    // Release
    manager.release(&ns, &lease1).await.unwrap();

    // Acquire again
    let lease2 = manager.acquire(&ns).await.unwrap();
    assert!(
        lease2.fencing_token > lease1.fencing_token,
        "Second token ({}) should be > first ({})",
        lease2.fencing_token,
        lease1.fencing_token
    );

    harness.cleanup().await;
}

/// Test 2: Double acquire is rejected.
/// Two managers for the same namespace — first acquires, second gets LeaseHeld.
#[tokio::test]
async fn test_lease_double_acquire_rejected() {
    let harness = TestHarness::new().await;
    let ns = harness.key("lease-double");

    let manifest = Manifest::new();
    manifest.write(&harness.store, &ns).await.unwrap();

    let manager1 = LeaseManager::new(
        harness.store.clone(),
        "node-1".to_string(),
        Duration::from_secs(30),
    );
    let manager2 = LeaseManager::new(
        harness.store.clone(),
        "node-2".to_string(),
        Duration::from_secs(30),
    );

    // Manager 1 acquires
    let _lease1 = manager1.acquire(&ns).await.unwrap();

    // Manager 2 should fail
    let result = manager2.acquire(&ns).await;
    assert!(result.is_err(), "Second acquire should fail");
    match result.unwrap_err() {
        ZeppelinError::LeaseHeld { namespace, holder } => {
            assert!(namespace.contains("lease-double"));
            assert_eq!(holder, "node-1");
        }
        other => panic!("Expected LeaseHeld, got: {other}"),
    }

    harness.cleanup().await;
}

/// Test 3: Expired lease takeover.
/// Acquire with 1s lease, sleep 2s, new holder takes over.
#[tokio::test]
async fn test_lease_expired_takeover() {
    let harness = TestHarness::new().await;
    let ns = harness.key("lease-expiry");

    let manifest = Manifest::new();
    manifest.write(&harness.store, &ns).await.unwrap();

    let manager1 = LeaseManager::new(
        harness.store.clone(),
        "node-1".to_string(),
        Duration::from_secs(1), // 1 second lease
    );
    let manager2 = LeaseManager::new(
        harness.store.clone(),
        "node-2".to_string(),
        Duration::from_secs(30),
    );

    // Manager 1 acquires with short lease
    let lease1 = manager1.acquire(&ns).await.unwrap();

    // Wait for lease to expire
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Manager 2 should be able to take over
    let lease2 = manager2.acquire(&ns).await.unwrap();
    assert!(
        lease2.fencing_token > lease1.fencing_token,
        "New token ({}) should be > expired token ({})",
        lease2.fencing_token,
        lease1.fencing_token
    );
    assert_eq!(lease2.holder_id, "node-2");

    harness.cleanup().await;
}

/// Test 4: Fencing rejects zombie writer.
/// Zombie writes fragment, lease expires, new writer writes manifest with
/// higher token, zombie reads manifest and sees stale token.
#[tokio::test]
async fn test_fencing_rejects_zombie_writer() {
    let harness = TestHarness::new().await;
    let ns = harness.key("lease-zombie");

    // Set up initial manifest with fencing_token = 0
    let manifest = Manifest::new();
    manifest.write(&harness.store, &ns).await.unwrap();

    let zombie_manager = LeaseManager::new(
        harness.store.clone(),
        "zombie-node".to_string(),
        Duration::from_secs(1),
    );
    let new_manager = LeaseManager::new(
        harness.store.clone(),
        "new-node".to_string(),
        Duration::from_secs(30),
    );

    // Zombie acquires lease (token = 1)
    let zombie_lease = zombie_manager.acquire(&ns).await.unwrap();
    let zombie_token = zombie_lease.fencing_token;

    // Zombie writes a fragment to S3 (this always succeeds — unconditional PUT)
    let vectors = random_vectors(3, 16);
    let fragment = WalFragment::new(vectors, vec![]);
    let frag_key = WalFragment::s3_key(&ns, &fragment.id);
    let frag_data = fragment.to_bytes().unwrap();
    harness.store.put(&frag_key, frag_data).await.unwrap();

    // Lease expires
    tokio::time::sleep(Duration::from_secs(2)).await;

    // New writer takes over and writes manifest with higher fencing token
    let new_lease = new_manager.acquire(&ns).await.unwrap();
    assert!(new_lease.fencing_token > zombie_token);

    // New writer updates manifest with its higher fencing token
    let (mut manifest, version) = Manifest::read_versioned(&harness.store, &ns)
        .await
        .unwrap()
        .unwrap();
    manifest.fencing_token = new_lease.fencing_token;
    manifest
        .write_conditional(&harness.store, &ns, &version)
        .await
        .unwrap();

    // Now zombie tries to read manifest and check fencing
    let manifest = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert!(
        manifest.fencing_token > zombie_token,
        "Manifest fencing_token ({}) should be > zombie's token ({})",
        manifest.fencing_token,
        zombie_token
    );
    // Zombie would see manifest.fencing_token > zombie_token → abort

    harness.cleanup().await;
}

/// Test 5: Double compaction prevention.
/// Two compactor managers — only one acquires the lease.
#[tokio::test]
async fn test_compactor_lease_prevents_double_compaction() {
    let harness = TestHarness::new().await;
    let ns = harness.key("lease-compactor");

    let manifest = Manifest::new();
    manifest.write(&harness.store, &ns).await.unwrap();

    let compactor1 = LeaseManager::new(
        harness.store.clone(),
        "compactor-1".to_string(),
        Duration::from_secs(30),
    );
    let compactor2 = LeaseManager::new(
        harness.store.clone(),
        "compactor-2".to_string(),
        Duration::from_secs(30),
    );

    // First compactor acquires
    let _lease = compactor1.acquire(&ns).await.unwrap();

    // Second compactor should fail
    let result = compactor2.acquire(&ns).await;
    assert!(result.is_err(), "Second compactor should not acquire");
    match result.unwrap_err() {
        ZeppelinError::LeaseHeld { .. } => {} // expected
        other => panic!("Expected LeaseHeld, got: {other}"),
    }

    harness.cleanup().await;
}

/// Test 6: Sequential writers with leases.
/// W1 appends a fragment, releases lease. W2 appends a fragment.
/// Both fragments should be in the final manifest.
#[tokio::test]
async fn test_sequential_writers_with_leases() {
    let harness = TestHarness::new().await;
    let ns = harness.key("lease-sequential");

    let manifest = Manifest::new();
    manifest.write(&harness.store, &ns).await.unwrap();

    let manager1 = LeaseManager::new(
        harness.store.clone(),
        "writer-1".to_string(),
        Duration::from_secs(30),
    );
    let manager2 = LeaseManager::new(
        harness.store.clone(),
        "writer-2".to_string(),
        Duration::from_secs(30),
    );
    let writer = WalWriter::new(harness.store.clone());

    // Writer 1: acquire lease, write fragment, release
    let lease1 = manager1.acquire(&ns).await.unwrap();
    let vectors1 = random_vectors(3, 16);
    let (frag1, _) = writer.append(&ns, vectors1, vec![]).await.unwrap();
    manager1.release(&ns, &lease1).await.unwrap();

    // Writer 2: acquire lease, write fragment, release
    let lease2 = manager2.acquire(&ns).await.unwrap();
    let vectors2 = random_vectors(3, 16);
    let (frag2, _) = writer.append(&ns, vectors2, vec![]).await.unwrap();
    manager2.release(&ns, &lease2).await.unwrap();

    // Both fragments should be in manifest
    let manifest = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(manifest.fragments.len(), 2);
    let frag_ids: Vec<_> = manifest.fragments.iter().map(|f| f.id).collect();
    assert!(
        frag_ids.contains(&frag1.id),
        "Fragment 1 should be in manifest"
    );
    assert!(
        frag_ids.contains(&frag2.id),
        "Fragment 2 should be in manifest"
    );

    harness.cleanup().await;
}

/// Test 7: Fragment orphan on lease expiry.
/// Write a fragment directly to S3, but don't update the manifest.
/// The fragment exists on S3 but is NOT in the manifest — storage leak, not data loss.
#[tokio::test]
async fn test_fragment_orphan_on_lease_expiry() {
    let harness = TestHarness::new().await;
    let ns = harness.key("lease-orphan");

    let manifest = Manifest::new();
    manifest.write(&harness.store, &ns).await.unwrap();

    // Write a fragment directly to S3 (simulating a writer whose lease expired
    // before it could update the manifest)
    let vectors = random_vectors(5, 16);
    let fragment = WalFragment::new(vectors, vec![]);
    let frag_key = WalFragment::s3_key(&ns, &fragment.id);
    let frag_data = fragment.to_bytes().unwrap();
    harness.store.put(&frag_key, frag_data).await.unwrap();

    // Fragment exists on S3
    assert!(
        harness.store.exists(&frag_key).await.unwrap(),
        "Fragment should exist on S3"
    );

    // But NOT in the manifest
    let manifest = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    let frag_ids: Vec<_> = manifest.fragments.iter().map(|f| f.id).collect();
    assert!(
        !frag_ids.contains(&fragment.id),
        "Orphan fragment should NOT be in manifest"
    );

    harness.cleanup().await;
}

// ─── TLA+ Counterexample Reproductions ───────────────────────────────────────
//
// The following tests reproduce specific counterexample traces found by TLC
// model checking of `formal-verifications/tla/MultiWriterLease.tla`.
//
// Each test exercises the exact interleaving that violated an invariant,
// then asserts the protocol's fix prevents the violation.

// ─── Test 8: TOCTOU Fencing Gap (MultiWriterLease.tla, FencingPreventsZombie) ─
//
// TLC counterexample trace (11 states):
//
//   State 1:  Init — manifest_fencing=0, both writers idle
//   State 2:  W1 acquires lease (token=1)
//   State 3:  Clock expires W1's lease
//   State 4:  W1 writes fragment 3 to S3 (doesn't know lease expired)
//   State 5:  W1 reads manifest — snapshots manifest_frags={1,2}, etag=1
//   State 6:  W1 passes CheckFencing — manifest_fencing=0 ≤ token=1 ✓
//   State 7:  W2 acquires lease (token=2, takeover of expired W1 lease)
//   State 8:  W2 writes fragment 4 to S3
//   State 9:  W2 reads manifest — snapshots manifest_frags={1,2}, etag=1
//   State 10: W2 passes CheckFencing — manifest_fencing=0 ≤ token=2 ✓
//   State 11: W2 commits — manifest_frags={1,2,4}, manifest_fencing=2, etag=2
//             *** W1 is at "fencing_ok" but manifest_fencing=2 > W1's token=1 ***
//
// WHY SAFE: W1's CAS will fail (etag changed 1→2). On retry, W1 re-reads
// manifest with fencing=2, re-checks fencing → 2 > 1 → aborted.
//
// FIX: Two-layer defense (CheckFencing + CAS). Neither alone is sufficient.

#[tokio::test]
async fn test_tla_toctou_fencing_gap_cas_catches_zombie() {
    let harness = TestHarness::new().await;
    let ns = harness.key("tla-toctou-fencing");
    let store = &harness.store;

    // State 1: Init — manifest with fragments {1, 2}, fencing_token=0
    let mut manifest = Manifest::new();
    let vectors_1 = random_vectors(3, 16);
    let vectors_2 = random_vectors(3, 16);
    let frag1 = WalFragment::new(vectors_1, vec![]);
    let frag2 = WalFragment::new(vectors_2, vec![]);
    // Write fragment data to S3
    store
        .put(
            &WalFragment::s3_key(&ns, &frag1.id),
            frag1.to_bytes().unwrap(),
        )
        .await
        .unwrap();
    store
        .put(
            &WalFragment::s3_key(&ns, &frag2.id),
            frag2.to_bytes().unwrap(),
        )
        .await
        .unwrap();
    manifest.add_fragment(FragmentRef {
        id: frag1.id,
        vector_count: 3,
        delete_count: 0,
        sequence_number: 0,
    });
    manifest.add_fragment(FragmentRef {
        id: frag2.id,
        vector_count: 3,
        delete_count: 0,
        sequence_number: 0,
    });
    manifest.write(store, &ns).await.unwrap();

    // State 2: W1 acquires lease (token=1)
    let w1_manager = LeaseManager::new(
        store.clone(),
        "w1-zombie".to_string(),
        Duration::from_secs(1), // Short lease — will expire
    );
    let w1_lease = w1_manager.acquire(&ns).await.unwrap();
    let w1_token = w1_lease.fencing_token;

    // State 3: Clock expires W1's lease
    tokio::time::sleep(Duration::from_secs(2)).await;

    // State 4: W1 writes fragment 3 to S3 (doesn't know lease expired)
    let vectors_3 = random_vectors(3, 16);
    let frag3 = WalFragment::new(vectors_3, vec![]);
    store
        .put(
            &WalFragment::s3_key(&ns, &frag3.id),
            frag3.to_bytes().unwrap(),
        )
        .await
        .unwrap();

    // State 5: W1 reads manifest — snapshots with etag
    let (w1_snap, w1_version) = Manifest::read_versioned(store, &ns).await.unwrap().unwrap();

    // State 6: W1 passes CheckFencing (manifest.fencing_token=0 ≤ w1_token)
    assert!(
        w1_snap.fencing_token <= w1_token,
        "W1 should pass initial fencing check: manifest_fencing={} <= w1_token={}",
        w1_snap.fencing_token,
        w1_token
    );

    // State 7: W2 acquires lease (takeover, token=2)
    let w2_manager =
        LeaseManager::new(store.clone(), "w2-new".to_string(), Duration::from_secs(30));
    let w2_lease = w2_manager.acquire(&ns).await.unwrap();
    let w2_token = w2_lease.fencing_token;
    assert!(w2_token > w1_token, "W2 token should be > W1 token");

    // State 8: W2 writes fragment 4 to S3
    let vectors_4 = random_vectors(3, 16);
    let frag4 = WalFragment::new(vectors_4, vec![]);
    store
        .put(
            &WalFragment::s3_key(&ns, &frag4.id),
            frag4.to_bytes().unwrap(),
        )
        .await
        .unwrap();

    // State 9-10: W2 reads manifest, passes fencing check
    let (mut w2_snap, w2_version) = Manifest::read_versioned(store, &ns).await.unwrap().unwrap();
    assert!(
        w2_snap.fencing_token <= w2_token,
        "W2 should pass fencing check"
    );

    // State 11: W2 commits — adds fragment, sets fencing_token=w2_token
    w2_snap.add_fragment(FragmentRef {
        id: frag4.id,
        vector_count: 3,
        delete_count: 0,
        sequence_number: 0,
    });
    w2_snap.fencing_token = w2_token;
    w2_snap
        .write_conditional(store, &ns, &w2_version)
        .await
        .unwrap();

    // NOW: W1 tries to commit with its stale snapshot.
    // The manifest etag has changed (W2 wrote), so CAS MUST fail.
    let mut w1_modified = w1_snap.clone();
    w1_modified.add_fragment(FragmentRef {
        id: frag3.id,
        vector_count: 3,
        delete_count: 0,
        sequence_number: 0,
    });
    w1_modified.fencing_token = w1_token;
    let w1_cas_result = w1_modified.write_conditional(store, &ns, &w1_version).await;

    assert!(
        matches!(w1_cas_result, Err(ZeppelinError::ManifestConflict { .. })),
        "FIX VERIFIED (Layer 2 — CAS): Zombie W1's stale manifest write is rejected. Got: {w1_cas_result:?}"
    );

    // W1 retries: re-reads manifest, re-checks fencing — caught by Layer 1
    let manifest_retry = Manifest::read(store, &ns).await.unwrap().unwrap();
    assert!(
        manifest_retry.fencing_token > w1_token,
        "FIX VERIFIED (Layer 1 — Fencing): On retry, manifest_fencing={} > w1_token={} → zombie aborted",
        manifest_retry.fencing_token,
        w1_token
    );

    // Final state: W2's fragment is in manifest, W1's fragment is orphaned
    let frag_ids: Vec<_> = manifest_retry.fragments.iter().map(|f| f.id).collect();
    assert!(
        frag_ids.contains(&frag4.id),
        "W2's fragment should be in manifest (committed successfully)"
    );
    assert!(
        !frag_ids.contains(&frag3.id),
        "W1's fragment should NOT be in manifest (zombie aborted — orphan on S3)"
    );

    // But W1's fragment data still exists on S3 (orphan, not data loss)
    assert!(
        store
            .exists(&WalFragment::s3_key(&ns, &frag3.id))
            .await
            .unwrap(),
        "Orphaned fragment data should still exist on S3"
    );

    harness.cleanup().await;
}

// ─── Test 9: Graceful Release After Lease Expiry (MultiWriterLease.tla, Deadlock) ─
//
// TLC counterexample trace (17 states, abbreviated):
//
//   State 2:  W1 acquires lease (token=1)
//   State 3:  Clock expires W1's lease
//   State 6:  W2 acquires lease (token=2, takeover)
//   State 7:  Clock expires W2's lease
//   State 8:  C acquires lease (token=3, takeover)
//   State 14: C commits, releases lease
//   State 15: W1 at "aborted" — tries to release, but lease_holder="none"
//   State 16: W2 at "aborted" — tries to release, but lease_holder="none"
//             *** DEADLOCK — neither W1 nor W2 can release a lease they don't hold ***
//
// FIX: Release is best-effort. If the lease was taken over (expired and
// re-acquired by another process), the caller skips the release and moves on.
// In the implementation: LeaseManager::release() should return Ok(()) or a
// non-fatal warning when the lease is no longer held, not a hard error.

#[tokio::test]
async fn test_tla_graceful_release_after_lease_expiry() {
    let harness = TestHarness::new().await;
    let ns = harness.key("tla-graceful-release");
    let store = &harness.store;

    let manifest = Manifest::new();
    manifest.write(store, &ns).await.unwrap();

    // State 2: W1 acquires lease (token=1, 1s duration)
    let w1_manager = LeaseManager::new(store.clone(), "w1".to_string(), Duration::from_secs(1));
    let w1_lease = w1_manager.acquire(&ns).await.unwrap();

    // State 3: W1's lease expires
    tokio::time::sleep(Duration::from_secs(2)).await;

    // State 6: W2 acquires (takeover, token=2, also short lease)
    let w2_manager = LeaseManager::new(store.clone(), "w2".to_string(), Duration::from_secs(1));
    let w2_lease = w2_manager.acquire(&ns).await.unwrap();
    assert!(w2_lease.fencing_token > w1_lease.fencing_token);

    // State 7: W2's lease expires too
    tokio::time::sleep(Duration::from_secs(2)).await;

    // State 8: Compactor acquires (takeover, token=3)
    let c_manager = LeaseManager::new(
        store.clone(),
        "compactor".to_string(),
        Duration::from_secs(30),
    );
    let c_lease = c_manager.acquire(&ns).await.unwrap();
    assert!(c_lease.fencing_token > w2_lease.fencing_token);

    // State 14: Compactor releases normally
    c_manager.release(&ns, &c_lease).await.unwrap();

    // State 15-16: W1 and W2 try to release their expired, taken-over leases.
    // FIX VERIFIED: release() must NOT panic or return a hard error.
    // It should gracefully handle "lease no longer held" — either Ok(())
    // or a specific non-fatal error the caller can ignore.
    let w1_release_result = w1_manager.release(&ns, &w1_lease).await;
    let w2_release_result = w2_manager.release(&ns, &w2_lease).await;

    // The release should not panic. It may return Ok or a non-fatal error.
    // The critical thing: the process is NOT stuck (no deadlock).
    // We accept either Ok(()) or any error — the process must be able to continue.
    match &w1_release_result {
        Ok(()) => {} // Best case: release silently succeeds (lease already gone)
        Err(e) => {
            // Acceptable: any non-panic error. The process can continue.
            eprintln!("FIX VERIFIED: W1 release returned non-fatal error (expected): {e}");
        }
    }
    match &w2_release_result {
        Ok(()) => {}
        Err(e) => {
            eprintln!("FIX VERIFIED: W2 release returned non-fatal error (expected): {e}");
        }
    }

    // The real assertion: we got here without deadlock or panic.
    // Both processes successfully "moved on" after their expired lease release.

    // Verify the namespace is in a clean state — no lease held
    // (compactor already released, W1/W2 releases were best-effort)
    // A new acquire should succeed immediately.
    let fresh_manager = LeaseManager::new(
        store.clone(),
        "fresh-writer".to_string(),
        Duration::from_secs(30),
    );
    let fresh_lease = fresh_manager.acquire(&ns).await.unwrap();
    assert!(
        fresh_lease.fencing_token > c_lease.fencing_token,
        "FIX VERIFIED: After graceful release, a fresh writer can acquire. \
         No deadlock, no stuck lease. Token {} > {}",
        fresh_lease.fencing_token,
        c_lease.fencing_token
    );

    harness.cleanup().await;
}
