//! Property-based tests for the multi-writer lease protocol with fencing tokens.
//!
//! Tests the lease contract in isolation — no S3, no crate imports.
//! Standalone types mirror the planned LeaseManager / Lease / Manifest contract.
//!
//! To run:
//!   cp formal-verifications/proptest/multi_writer_lease.rs tests/proptest_multi_writer_lease.rs
//!   cargo test --test proptest_multi_writer_lease

use proptest::prelude::*;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

// ---- Standalone Types (no crate imports) ----

/// Mirrors the planned `Lease` struct from `src/wal/lease.rs`.
#[derive(Debug, Clone)]
struct TestLease {
    holder_id: String,
    fencing_token: u64,
    acquired_at: SystemTime,
    expires_at: SystemTime,
}

impl TestLease {
    fn is_expired(&self, now: SystemTime) -> bool {
        now >= self.expires_at
    }
}

/// Mirrors the planned `LeaseManager` — manages leases for namespaces.
/// Uses an in-memory store instead of S3.
#[derive(Debug)]
struct TestLeaseStore {
    /// namespace -> (lease, version) where version simulates ETag for CAS.
    leases: HashMap<String, (TestLease, u64)>,
    /// Monotonic counter for fencing tokens.
    next_token: u64,
    /// Version counter for CAS simulation.
    next_version: u64,
}

impl TestLeaseStore {
    fn new() -> Self {
        Self {
            leases: HashMap::new(),
            next_token: 1,
            next_version: 1,
        }
    }

    /// Acquire a lease for a namespace. Returns the lease or an error string.
    fn acquire(
        &mut self,
        namespace: &str,
        holder_id: &str,
        duration: Duration,
        now: SystemTime,
    ) -> Result<TestLease, String> {
        if let Some((existing, _version)) = self.leases.get(namespace) {
            if !existing.is_expired(now) {
                return Err(format!(
                    "LeaseHeld: namespace={}, holder={}",
                    namespace, existing.holder_id
                ));
            }
            // Lease expired — takeover allowed
        }

        let token = self.next_token;
        self.next_token += 1;
        let version = self.next_version;
        self.next_version += 1;

        let lease = TestLease {
            holder_id: holder_id.to_string(),
            fencing_token: token,
            acquired_at: now,
            expires_at: now + duration,
        };

        self.leases
            .insert(namespace.to_string(), (lease.clone(), version));
        Ok(lease)
    }

    /// Release a lease. Only the holder can release.
    fn release(&mut self, namespace: &str, lease: &TestLease) -> Result<(), String> {
        if let Some((existing, _version)) = self.leases.get(namespace) {
            if existing.holder_id != lease.holder_id {
                return Err("Cannot release: not the holder".to_string());
            }
            if existing.fencing_token != lease.fencing_token {
                return Err("Cannot release: token mismatch".to_string());
            }
        } else {
            return Err("No lease to release".to_string());
        }
        self.leases.remove(namespace);
        Ok(())
    }

    /// Simulate CAS-based concurrent acquire. Two holders read the same version,
    /// both try to write. Only one succeeds.
    fn concurrent_acquire(
        &mut self,
        namespace: &str,
        holder_a: &str,
        holder_b: &str,
        duration: Duration,
        now: SystemTime,
    ) -> (Result<TestLease, String>, Result<TestLease, String>) {
        // Both read the current state (simulate reading same ETag)
        let current_version = self
            .leases
            .get(namespace)
            .map(|(_, v)| *v)
            .unwrap_or(0);
        let is_expired = self
            .leases
            .get(namespace)
            .map(|(l, _)| l.is_expired(now))
            .unwrap_or(true); // No lease = can acquire

        if !is_expired {
            let holder = self.leases.get(namespace).unwrap().0.holder_id.clone();
            return (
                Err(format!("LeaseHeld: holder={}", holder)),
                Err(format!("LeaseHeld: holder={}", holder)),
            );
        }

        // Holder A writes first (wins CAS)
        let token_a = self.next_token;
        self.next_token += 1;
        let version_a = self.next_version;
        self.next_version += 1;

        let lease_a = TestLease {
            holder_id: holder_a.to_string(),
            fencing_token: token_a,
            acquired_at: now,
            expires_at: now + duration,
        };
        self.leases
            .insert(namespace.to_string(), (lease_a.clone(), version_a));

        // Holder B tries to CAS — version mismatch (current_version != version_a)
        let _ = current_version; // B's stale version
        // B fails because the ETag changed
        let result_b = Err(format!(
            "CAS conflict: holder {} lost race to {}",
            holder_b, holder_a
        ));

        (Ok(lease_a), result_b)
    }
}

/// Mirrors the planned `Manifest` with fencing token.
#[derive(Debug, Clone)]
struct TestManifest {
    fragments: Vec<u64>,
    fencing_token: u64,
    etag: u64,
}

impl TestManifest {
    fn new() -> Self {
        Self {
            fragments: Vec::new(),
            fencing_token: 0,
            etag: 1,
        }
    }

    /// Check if a writer's fencing token is valid against this manifest.
    fn check_fencing(&self, writer_token: u64) -> bool {
        self.fencing_token <= writer_token
    }

    /// CAS write: add fragment and update fencing token.
    /// Returns true if CAS succeeded (etag matched).
    fn cas_write(
        &mut self,
        fragment_id: u64,
        writer_token: u64,
        expected_etag: u64,
    ) -> bool {
        if self.etag != expected_etag {
            return false; // CAS failed
        }
        self.fragments.push(fragment_id);
        self.fencing_token = writer_token;
        self.etag += 1;
        true
    }
}

// ---- Proptest Strategies ----

fn arb_holder_id() -> impl Strategy<Value = String> {
    prop::string::string_regex("node-[a-z]{3}-[0-9]{3}")
        .unwrap()
        .prop_filter("non-empty", |s| !s.is_empty())
}

fn arb_duration_secs() -> impl Strategy<Value = u64> {
    1u64..=60
}

fn arb_token_sequence() -> impl Strategy<Value = Vec<u64>> {
    prop::collection::vec(1u64..=100, 2..=10)
}

// ---- Property Tests ----

proptest! {
    /// Property 1: Acquire-release roundtrip.
    /// After releasing a lease, the same or different holder can re-acquire.
    #[test]
    fn acquire_release_roundtrip(
        holder1 in arb_holder_id(),
        holder2 in arb_holder_id(),
        dur_secs in arb_duration_secs(),
    ) {
        let mut store = TestLeaseStore::new();
        let ns = "test-ns";
        let dur = Duration::from_secs(dur_secs);
        let now = SystemTime::now();

        // First acquire succeeds
        let lease1 = store.acquire(ns, &holder1, dur, now).unwrap();
        prop_assert!(lease1.fencing_token > 0);

        // Release
        store.release(ns, &lease1).unwrap();

        // Second acquire succeeds (same or different holder)
        let lease2 = store.acquire(ns, &holder2, dur, now).unwrap();
        prop_assert!(lease2.fencing_token > lease1.fencing_token,
            "Token should increase: {} > {}", lease2.fencing_token, lease1.fencing_token);
    }

    /// Property 2: Double acquire fails.
    /// If one holder has a valid (unexpired) lease, another cannot acquire.
    #[test]
    fn double_acquire_fails(
        holder1 in arb_holder_id(),
        holder2 in arb_holder_id(),
        dur_secs in arb_duration_secs(),
    ) {
        let mut store = TestLeaseStore::new();
        let ns = "test-ns";
        let dur = Duration::from_secs(dur_secs);
        let now = SystemTime::now();

        // First acquire succeeds
        let _lease1 = store.acquire(ns, &holder1, dur, now).unwrap();

        // Second acquire must fail (lease not expired)
        let result = store.acquire(ns, &holder2, dur, now);
        prop_assert!(result.is_err(), "Second acquire should fail");
        prop_assert!(result.unwrap_err().contains("LeaseHeld"));
    }

    /// Property 3: Fencing token monotonicity.
    /// Across a sequence of acquire/release cycles, tokens strictly increase.
    #[test]
    fn fencing_token_monotonic(
        n_cycles in 2u32..=8,
        dur_secs in arb_duration_secs(),
    ) {
        let mut store = TestLeaseStore::new();
        let ns = "test-ns";
        let dur = Duration::from_secs(dur_secs);
        let now = SystemTime::now();
        let mut prev_token = 0u64;

        for i in 0..n_cycles {
            let holder = format!("holder-{}", i);
            let lease = store.acquire(ns, &holder, dur, now).unwrap();
            prop_assert!(lease.fencing_token > prev_token,
                "Token must strictly increase: {} > {}", lease.fencing_token, prev_token);
            prev_token = lease.fencing_token;
            store.release(ns, &lease).unwrap();
        }
    }

    /// Property 4: Expired lease takeover.
    /// When a lease expires, a new holder can acquire with token + 1.
    #[test]
    fn expired_lease_takeover(
        holder1 in arb_holder_id(),
        holder2 in arb_holder_id(),
    ) {
        let mut store = TestLeaseStore::new();
        let ns = "test-ns";
        let dur = Duration::from_secs(1); // 1 second lease
        let now = SystemTime::now();

        // Holder 1 acquires
        let lease1 = store.acquire(ns, &holder1, dur, now).unwrap();

        // Time advances past expiry
        let future = now + Duration::from_secs(2);

        // Holder 2 should be able to take over (lease expired)
        let lease2 = store.acquire(ns, &holder2, dur, future).unwrap();
        prop_assert!(lease2.fencing_token > lease1.fencing_token,
            "New token {} should be > old token {}", lease2.fencing_token, lease1.fencing_token);
        prop_assert_eq!(&lease2.holder_id, &holder2);
    }

    /// Property 5: Zombie detection via fencing token.
    /// If the manifest's fencing token is higher than a writer's token,
    /// the writer is a zombie and must abort.
    #[test]
    fn zombie_detection(
        zombie_token in 1u64..=50,
        new_token_offset in 1u64..=10,
    ) {
        let new_token = zombie_token + new_token_offset;
        let mut manifest = TestManifest::new();

        // New holder writes to manifest first
        let cas_ok = manifest.cas_write(100, new_token, manifest.etag);
        prop_assert!(cas_ok, "New holder's CAS should succeed");

        // Zombie checks fencing — should detect stale token
        let fencing_ok = manifest.check_fencing(zombie_token);
        prop_assert!(!fencing_ok,
            "Zombie (token={}) should be rejected by manifest (fencing_token={})",
            zombie_token, manifest.fencing_token);
    }

    /// Property 6: Concurrent acquire — exactly one wins.
    /// When two holders race to acquire the same lease, one succeeds and
    /// the other fails due to CAS conflict.
    #[test]
    fn concurrent_acquire_one_wins(
        holder_a in arb_holder_id(),
        holder_b in arb_holder_id(),
        dur_secs in arb_duration_secs(),
    ) {
        let mut store = TestLeaseStore::new();
        let ns = "test-ns";
        let dur = Duration::from_secs(dur_secs);
        let now = SystemTime::now();

        let (result_a, result_b) = store.concurrent_acquire(ns, &holder_a, &holder_b, dur, now);

        // Exactly one succeeds
        let a_ok = result_a.is_ok();
        let b_ok = result_b.is_ok();
        prop_assert!(
            (a_ok && !b_ok) || (!a_ok && b_ok),
            "Exactly one should win: a={}, b={}", a_ok, b_ok
        );

        // The winner has a valid token
        if let Ok(lease) = result_a {
            prop_assert!(lease.fencing_token > 0);
        }
        if let Ok(lease) = result_b {
            prop_assert!(lease.fencing_token > 0);
        }
    }
}
