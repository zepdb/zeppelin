//! Task 5 — write path: group commit, CAS backoff+jitter, no orphaned fragments.
//!
//! Invariants under test:
//!   I1 (group commit): N concurrent appends to one namespace complete with far
//!       fewer than N manifest CAS PUTs (waiting writers fold into one update).
//!   I2 (backoff): CAS retries back off + jitter so moderate contention is
//!       absorbed instead of surfacing 409s.
//!   I3 (no orphans): an append whose manifest CAS ultimately fails leaves NO
//!       unreferenced fragment object under the namespace's wal/ prefix.
//!   I4/I5: durability + return contract preserved (a 200 means durable AND
//!       referenced; append returns the manifest that includes this write).

mod common;

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use common::counting::counting_store;
use common::harness::TestHarness;
use common::vectors::random_vectors;

use zeppelin::error::ZeppelinError;
use zeppelin::time::{Clock, TimeSource};
use zeppelin::types::VectorEntry;
use zeppelin::wal::{LeaseManager, Manifest, WalWriter};

#[derive(Debug)]
struct AdjustableTimeSource {
    now_ms: AtomicI64,
}

impl AdjustableTimeSource {
    fn new(now: DateTime<Utc>) -> Self {
        Self {
            now_ms: AtomicI64::new(now.timestamp_millis()),
        }
    }

    fn jump(&self, delta: chrono::Duration) {
        self.now_ms
            .fetch_add(delta.num_milliseconds(), Ordering::SeqCst);
    }
}

impl TimeSource for AdjustableTimeSource {
    fn now(&self) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(self.now_ms.load(Ordering::SeqCst))
            .expect("adjustable write-path timestamp must be representable")
    }
}

/// Count fragment objects under a namespace's wal/ prefix.
async fn wal_fragment_count(store: &zeppelin::storage::ZeppelinStore, ns: &str) -> usize {
    let prefix = format!("{ns}/wal/");
    store
        .list_prefix(&prefix)
        .await
        .unwrap()
        .into_iter()
        .filter(|k| k.ends_with(".wal"))
        .count()
}

/// Group commit must not deadlock when concurrent appends carry DIFFERENT
/// fencing tokens. A leader whose own token differs from the oldest queued
/// waiter's must still commit its OWN ref and release the commit lock — never
/// defer itself and then block on its own reply while holding the lock (which
/// wedges the namespace's write path permanently).
///
/// Reproduces reliably against the buggy code (leader_token taken from
/// `pending[0]` rather than the lock holder's own token) using an in-memory
/// store: instant CAS → rapid leader handoff → the pending queue reliably
/// holds mismatched tokens while a leader holds the commit lock. Two tasks
/// race per iteration with DIFFERENT tokens that CLIMB (`iter*2+1`,
/// `iter*2+2`), so the fencing check itself never rejects (the manifest token
/// only ratchets up to what we set) — a hang is therefore unambiguously the
/// deadlock, not a fencing rejection. The bug surfaces within a few thousand
/// iterations; each iteration is guarded by a 10s timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_group_commit_mixed_fencing_tokens_no_deadlock() {
    let mem = Arc::new(object_store::memory::InMemory::new());
    let store = zeppelin::storage::ZeppelinStore::new(mem);
    let ns = "mixed-token-deadlock";
    Manifest::new().write(&store, ns).await.unwrap();

    let writer = Arc::new(WalWriter::new(store.clone()));

    // Per round, race SEVERAL tasks with DISTINCT climbing tokens. More
    // concurrent pushers sharply raises the odds that a later-pushing task
    // (whose token differs from the queue front) wins the commit lock first —
    // the exact interleaving that wedges a leader that defers its own ref.
    let racers = 6u64;
    for iter in 0..600u64 {
        let base = iter * racers;
        let mut handles = Vec::with_capacity(racers as usize);
        for r in 0..racers {
            // Distinct, climbing tokens so the fencing check never rejects
            // (manifest.fencing_token only ratchets up to what we set); the
            // WITHIN-round mismatch is what exercises the deferral partition.
            let token = base + r + 1;
            let w = writer.clone();
            handles.push(tokio::spawn(async move {
                w.append_with_lease(ns, random_vectors(1, 8), vec![], Some(token))
                    .await
            }));
        }

        let all = async {
            for h in handles {
                let _ = h.await.unwrap();
            }
        };
        if tokio::time::timeout(std::time::Duration::from_secs(10), all)
            .await
            .is_err()
        {
            panic!(
                "DEADLOCK at iteration {iter}: mixed-token appends hung — a lock-holding \
                 leader deferred its own ref (leader_token from pending[0], not self) and \
                 awaits an oneshot nobody can fulfill while holding commit_lock"
            );
        }
    }
}

/// I3: a fencing-stale append (rejected AFTER the fragment PUT) must not leave
/// an orphaned fragment on S3. This is the deterministic orphan trigger — the
/// fragment is written, then the fencing check fails, then the pre-Task-5 code
/// returned the error leaving the object dangling.
#[tokio::test]
async fn test_fencing_rejected_append_leaves_no_orphan() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("orphan-fencing");
    let store = &harness.store;

    // Manifest already advanced to fencing_token = 5 (a newer lease holder).
    let mut manifest = Manifest::new();
    manifest.fencing_token = 5;
    manifest.write(store, &ns).await.unwrap();

    let writer = WalWriter::new(store.clone());
    // A zombie writer with a stale token (3 < 5) — must be rejected.
    let result = writer
        .append_with_lease(&ns, random_vectors(4, 8), vec![], Some(3))
        .await;
    assert!(
        matches!(result, Err(ZeppelinError::FencingTokenStale { .. })),
        "stale fencing token must be rejected, got {result:?}"
    );

    // I3: no orphaned fragment left behind.
    assert_eq!(
        wal_fragment_count(store, &ns).await,
        0,
        "a fencing-rejected append must not leave an orphaned fragment on S3"
    );
    // Manifest must be untouched (no fragment ref added).
    let m = Manifest::read(store, &ns).await.unwrap().unwrap();
    assert!(
        m.fragments.is_empty(),
        "rejected append must not touch the manifest"
    );

    harness.cleanup().await;
}

/// I3: an append against a deleted/missing manifest (ManifestNotFound, raised
/// AFTER the fragment PUT) must also leave no orphan.
#[tokio::test]
async fn test_missing_manifest_append_leaves_no_orphan() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("orphan-nomanifest");
    let store = &harness.store;

    // No manifest written for this namespace (simulates deleted namespace).
    let writer = WalWriter::new(store.clone());
    let result = writer.append(&ns, random_vectors(4, 8), vec![]).await;
    assert!(
        matches!(result, Err(ZeppelinError::ManifestNotFound { .. })),
        "append to a namespace with no manifest must fail ManifestNotFound, got {result:?}"
    );

    assert_eq!(
        wal_fragment_count(store, &ns).await,
        0,
        "an append that fails on a missing manifest must not leave an orphaned fragment"
    );

    harness.cleanup().await;
}

/// I2: under moderate contention, backoff absorbs CAS conflicts — every append
/// eventually succeeds, none surfaces a 409 to the caller. Uses TWO SEPARATE
/// WalWriter instances (independent group-commit state) on the same namespace,
/// so they genuinely contend at the S3 manifest-CAS layer — group commit only
/// coalesces WITHIN one writer, so cross-writer conflicts still exercise the
/// backoff path.
#[tokio::test]
async fn test_concurrent_writers_backoff_absorbs_conflicts() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("backoff-contention");
    let store = &harness.store;
    Manifest::new().write(store, &ns).await.unwrap();

    // Distinct writer instances = distinct group state = real S3 CAS contention.
    let n_per_task = 8;
    let n_writers = 3;

    let mut handles = Vec::new();
    for t in 0..n_writers {
        let writer = WalWriter::new(store.clone());
        let ns = ns.clone();
        handles.push(tokio::spawn(async move {
            let mut oks = 0;
            for i in 0..n_per_task {
                let v = vec![VectorEntry {
                    id: format!("t{t}_v{i}"),
                    values: random_vectors(1, 8)[0].values.clone(),
                    attributes: None,
                }];
                writer
                    .append(&ns, v, vec![])
                    .await
                    .expect("append must succeed under moderate contention (backoff absorbs 409s)");
                oks += 1;
            }
            oks
        }));
    }

    let mut total = 0;
    for h in handles {
        total += h.await.unwrap();
    }
    assert_eq!(total, n_per_task * n_writers);

    // Every fragment must be referenced by the manifest (I4: durable AND referenced).
    let m = Manifest::read(store, &ns).await.unwrap().unwrap();
    assert_eq!(
        m.fragments.len(),
        n_per_task * n_writers,
        "every successful append must be referenced in the manifest"
    );

    harness.cleanup().await;
}

/// I1 (group commit): N concurrent appends to one namespace must complete with
/// FAR fewer than N manifest CAS PUTs — waiting writers' fragment refs fold
/// into a shared manifest update. Pre-Task-5 code serializes: exactly N CAS
/// PUTs (one per append, each under the per-namespace mutex).
///
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_group_commit_coalesces_manifest_puts() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("group-commit");
    let (store, counter) = counting_store(&harness.store);
    Manifest::new().write(&store, &ns).await.unwrap();
    counter.reset();

    let writer = Arc::new(WalWriter::new(store.clone()));
    let n = 20;
    let mut handles = Vec::new();
    for i in 0..n {
        let writer = writer.clone();
        let ns = ns.clone();
        handles.push(tokio::spawn(async move {
            let v = vec![VectorEntry {
                id: format!("gc_{i}"),
                values: random_vectors(1, 8)[0].values.clone(),
                attributes: None,
            }];
            writer.append(&ns, v, vec![]).await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // Every fragment PUT still happens (durability) — one per append.
    let frag_puts = counter.puts_matching("/wal/");
    assert_eq!(frag_puts, n as u64, "each append must PUT its own fragment");

    // But manifest CAS PUTs must be far fewer than N (group commit).
    let manifest_puts = counter.puts_matching("/manifest.json");
    assert!(
        manifest_puts <= 8,
        "I1: {n} concurrent appends must coalesce into <= 8 manifest CAS PUTs, got {manifest_puts}"
    );

    // I4/I5: all N fragments referenced exactly once.
    let m = Manifest::read(&store, &ns).await.unwrap().unwrap();
    assert_eq!(
        m.fragments.len(),
        n,
        "all appends referenced in the manifest"
    );

    harness.cleanup().await;
}

/// I5 + regression: a single uncontended append still returns the manifest that
/// includes its own fragment, with exactly one fragment PUT and one manifest
/// PUT (no batching latency/overhead when nobody else is waiting).
#[tokio::test]
async fn test_single_append_roundtrip_unchanged() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("single-append");
    let (store, counter) = counting_store(&harness.store);
    Manifest::new().write(&store, &ns).await.unwrap();
    counter.reset();

    let writer = WalWriter::new(store.clone());
    let (fragment, manifest) = writer
        .append(&ns, random_vectors(3, 8), vec![])
        .await
        .unwrap();

    // I5: returned manifest includes this write.
    assert!(
        manifest.fragments.iter().any(|f| f.id == fragment.id),
        "append must return the manifest that includes its own fragment"
    );
    assert_eq!(
        counter.puts_matching("/wal/"),
        1,
        "exactly one fragment PUT"
    );
    assert_eq!(
        counter.puts_matching("/manifest.json"),
        1,
        "exactly one manifest CAS PUT for an uncontended append"
    );
    assert_eq!(
        counter.gets_matching("/wal/"),
        0,
        "a conformant successful WAL PUT must not be synchronously read back"
    );
    assert_eq!(
        counter.gets_matching("/manifest.json"),
        1,
        "the append needs one manifest CAS-base GET and no post-PUT verification GET"
    );

    harness.cleanup().await;
}

/// A single writer should carry the manifest and ETag returned by its first
/// successful CAS into later group-commit rounds. S3 still validates every
/// round through the conditional PUT; only the redundant base GET disappears.
#[tokio::test]
async fn test_sequential_group_commit_reuses_committed_manifest_etag() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("group-commit-manifest-memo");
    let (store, counter) = counting_store(&harness.store);
    Manifest::new().write(&store, &ns).await.unwrap();
    counter.reset();

    let writer = WalWriter::new(store);
    let rounds = 4;
    for round in 0..rounds {
        let vectors = vec![VectorEntry {
            id: format!("memo_round_{round}"),
            values: random_vectors(1, 8)[0].values.clone(),
            attributes: None,
        }];
        writer.append(&ns, vectors, vec![]).await.unwrap();
    }

    assert_eq!(
        counter.gets_matching("/manifest.json"),
        1,
        "only the cold group-commit round may read the manifest"
    );
    assert_eq!(
        counter.puts_matching("/manifest.json"),
        rounds,
        "every group-commit round must remain an authoritative manifest CAS"
    );

    let authoritative = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(authoritative.fragments.len(), rounds as usize);

    harness.cleanup().await;
}

/// An out-of-band manifest publication invalidates the writer's optimistic
/// memo. The failed conditional publication must clear it and retry without
/// dropping either writer's change. The slow path uses one live GET to prove
/// the colliding immutable history generation is referenced, then one
/// versioned live GET to seed the retry.
#[tokio::test]
async fn test_group_commit_manifest_memo_conflict_rebuilds_from_authority() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("group-commit-manifest-conflict");
    let (store, counter) = counting_store(&harness.store);
    Manifest::new().write(&store, &ns).await.unwrap();

    let writer = WalWriter::new(store);
    writer
        .append(
            &ns,
            vec![VectorEntry {
                id: "memo_before_conflict".to_string(),
                values: random_vectors(1, 8)[0].values.clone(),
                attributes: None,
            }],
            vec![],
        )
        .await
        .unwrap();

    let (mut external, version) = Manifest::read_versioned(&harness.store, &ns)
        .await
        .unwrap()
        .unwrap();
    external.fencing_token = 17;
    external
        .write_conditional(&harness.store, &ns, &version)
        .await
        .unwrap();

    counter.reset();
    let (_, committed) = writer
        .append(
            &ns,
            vec![VectorEntry {
                id: "memo_after_conflict".to_string(),
                values: random_vectors(1, 8)[0].values.clone(),
                attributes: None,
            }],
            vec![],
        )
        .await
        .unwrap();

    assert_eq!(
        counter.gets_matching("/manifest.json"),
        2,
        "history-conflict classification and CAS retry each require one live GET"
    );
    assert_eq!(committed.fencing_token, 17);
    assert_eq!(committed.fragments.len(), 2);

    let authoritative = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(authoritative.version(), committed.version());
    assert_eq!(authoritative.fencing_token, committed.fencing_token);
    assert_eq!(
        authoritative
            .fragments
            .iter()
            .map(|fragment| fragment.id)
            .collect::<Vec<_>>(),
        committed
            .fragments
            .iter()
            .map(|fragment| fragment.id)
            .collect::<Vec<_>>()
    );

    harness.cleanup().await;
}

/// A stale leaseholder may pass the fencing check against its memo, but it may
/// not publish: the intervening holder changed the manifest ETag. After the
/// conflict, the authoritative reread must expose the new fencing token and
/// reject the zombie while cleaning its uploaded fragment.
#[tokio::test]
async fn test_group_commit_manifest_memo_preserves_fencing_after_takeover() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("group-commit-manifest-fencing");
    let (store, counter) = counting_store(&harness.store);
    Manifest::new().write(&store, &ns).await.unwrap();

    let source = Arc::new(AdjustableTimeSource::new(Utc::now()));
    let clock = Clock::from_source(source.clone());
    let holder_a = LeaseManager::with_clock(
        harness.store.clone(),
        "writer-a".to_string(),
        Duration::from_secs(10),
        clock.clone(),
    );
    let holder_b = LeaseManager::with_clock(
        harness.store.clone(),
        "writer-b".to_string(),
        Duration::from_secs(10),
        clock,
    );

    let lease_a = holder_a.acquire(&ns).await.unwrap();
    let writer_a = WalWriter::new(store);
    writer_a
        .append_with_lease(
            &ns,
            vec![VectorEntry {
                id: "holder_a_visible".to_string(),
                values: random_vectors(1, 8)[0].values.clone(),
                attributes: None,
            }],
            vec![],
            Some(lease_a.fencing_token),
        )
        .await
        .unwrap();

    source.jump(chrono::Duration::seconds(11));
    let lease_b = holder_b.acquire(&ns).await.unwrap();
    let writer_b = WalWriter::new(harness.store.clone());
    writer_b
        .append_with_lease(
            &ns,
            vec![VectorEntry {
                id: "holder_b_visible".to_string(),
                values: random_vectors(1, 8)[0].values.clone(),
                attributes: None,
            }],
            vec![],
            Some(lease_b.fencing_token),
        )
        .await
        .unwrap();

    counter.reset();
    let zombie = writer_a
        .append_with_lease(
            &ns,
            vec![VectorEntry {
                id: "holder_a_zombie".to_string(),
                values: random_vectors(1, 8)[0].values.clone(),
                attributes: None,
            }],
            vec![],
            Some(lease_a.fencing_token),
        )
        .await;

    assert!(matches!(
        zombie,
        Err(ZeppelinError::FencingTokenStale {
            our_token,
            manifest_token,
            ..
        }) if our_token == lease_a.fencing_token && manifest_token == lease_b.fencing_token
    ));
    assert_eq!(
        counter.gets_matching("/manifest.json"),
        2,
        "history authority and the retry ETag require two live manifest reads"
    );

    let authoritative = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(authoritative.fencing_token, lease_b.fencing_token);
    assert_eq!(authoritative.fragments.len(), 2);
    assert_eq!(wal_fragment_count(&harness.store, &ns).await, 2);

    harness.cleanup().await;
}

/// A new writer process has no local group state and must seed itself from S3.
#[tokio::test]
async fn test_group_commit_manifest_memo_restart_is_cold() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("group-commit-manifest-restart");
    let (store, counter) = counting_store(&harness.store);
    Manifest::new().write(&store, &ns).await.unwrap();

    WalWriter::new(store.clone())
        .append(
            &ns,
            vec![VectorEntry {
                id: "before_restart".to_string(),
                values: random_vectors(1, 8)[0].values.clone(),
                attributes: None,
            }],
            vec![],
        )
        .await
        .unwrap();

    counter.reset();
    WalWriter::new(store)
        .append(
            &ns,
            vec![VectorEntry {
                id: "after_restart".to_string(),
                values: random_vectors(1, 8)[0].values.clone(),
                attributes: None,
            }],
            vec![],
        )
        .await
        .unwrap();

    assert_eq!(counter.gets_matching("/manifest.json"), 1);
    harness.cleanup().await;
}

/// Namespace deletion drops the per-namespace group state. Recreating the same
/// name must therefore start cold and never reuse the deleted incarnation's
/// manifest or ETag.
#[tokio::test]
async fn test_group_commit_manifest_memo_namespace_recreate_is_cold() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("group-commit-manifest-recreate");
    let (store, counter) = counting_store(&harness.store);
    Manifest::new().write(&store, &ns).await.unwrap();

    let writer = WalWriter::new(store);
    writer
        .append(
            &ns,
            vec![VectorEntry {
                id: "old_incarnation".to_string(),
                values: random_vectors(1, 8)[0].values.clone(),
                attributes: None,
            }],
            vec![],
        )
        .await
        .unwrap();

    writer.remove_lock(&ns);
    harness
        .store
        .delete_prefix(&format!("{ns}/"))
        .await
        .unwrap();
    Manifest::new().write(&harness.store, &ns).await.unwrap();

    counter.reset();
    writer
        .append(
            &ns,
            vec![VectorEntry {
                id: "new_incarnation".to_string(),
                values: random_vectors(1, 8)[0].values.clone(),
                attributes: None,
            }],
            vec![],
        )
        .await
        .unwrap();

    assert_eq!(counter.gets_matching("/manifest.json"), 1);
    let authoritative = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(authoritative.fragments.len(), 1);

    harness.cleanup().await;
}
