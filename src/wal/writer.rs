//! Uploads immutable WAL fragments and publishes them through manifest CAS.
//!
//! HTTP write handlers enter this module through
//! [`crate::wal::writer::WalWriter::append`] for the v1 single-writer path or
//! [`crate::wal::writer::WalWriter::append_with_lease`] when a caller has a
//! [`crate::wal::lease::Lease`] fencing token. Each append first creates and
//! uploads a distinct [`crate::wal::fragment::WalFragment`]. The object is not
//! visible yet. Per-namespace group commit then folds one or more compatible
//! [`crate::wal::manifest::FragmentRef`] values into the authoritative
//! [`crate::wal::manifest::Manifest`] with ETag compare-and-swap (CAS).
//!
//! ```text
//! concurrent appends for one namespace
//!        |            |            |
//!        v            v            v
//! upload fragment  upload fragment  upload fragment   (immutable, not visible)
//!        |            |            |
//!        `------------+------------'
//!                     v
//!        enqueue refs by fencing-token group
//!                     |
//!                     v
//!          one async commit leader
//!                     |
//!                     v
//!       use last committed manifest + ETag
//!       (cold start reads the pair from S3)
//!                     |
//!          fencing check rejects zombie
//!                     |
//!                     v
//!     one CAS publishes all refs in the group
//!          | success             | conflict
//!          v                     v
//!   reply with manifest     back off, re-read,
//!   (fragments visible)     re-check fencing
//!                                |
//!                         terminal failure
//!                                v
//!                  prove refs absent before cleanup
//! ```
//!
//! ## Reading map
//!
//! 1. Start with [`crate::wal::writer::WalWriter::append_with_lease`] for fragment construction,
//!    upload, queueing, and leader election.
//! 2. Read the private `GroupCommitState` and `PendingCommit` types to understand
//!    the two lock scopes and per-waiter reply channel.
//! 3. Read the private `commit_pending_group` helper for token partitioning,
//!    fencing, manifest CAS, and retry behavior.
//! 4. Finish with the private `fail_batch` and `cleanup_orphan_fragment` helpers
//!    for partial-write cleanup.
//!
//! ## Invariants
//!
//! - Fragment PUT precedes manifest publication. Only a successful manifest CAS
//!   makes the immutable object visible to readers.
//! - Ordinary batched appends share a manifest CAS only when their optional
//!   fencing tokens are equal; mixing generations would let stale work ride with
//!   current work. A guarded derived append always publishes alone and binds the
//!   namespace incarnation, manifest generation, and ETag observed in one GET.
//! - Fencing and CAS are both required. A token check alone has a time-of-check /
//!   time-of-use race; CAS alone does not identify a zombie based on old work.
//! - The last successful manifest/ETag pair is only an optimistic CAS base. Any
//!   conflict or error discards it, and the retry rebuilds from S3 before
//!   rechecking fencing. A backend that omits the new ETag keeps the next round
//!   cold.
//! - Memo safety relies on the S3 contract that a successful conditional PUT
//!   atomically stores the submitted body. If a provider acknowledges different
//!   bytes, the returned ETag can name that corrupt object; this is classified
//!   as provider-contract abuse rather than a supported-v1 storage outcome.
//! - A successful append reply includes a manifest that references that append's
//!   fragment. Definite manifest/fencing failures handled by the leader attempt
//!   exact-object cleanup. An ambiguous manifest-write error first re-reads S3;
//!   possible reachability always suppresses deletion.
//! - The synchronous pending-queue mutex is never held across `.await`. The Tokio
//!   commit mutex is held across remote CAS rounds to serialize one namespace,
//!   while different namespaces retain independent progress.
//!
//! ## Rust concepts used here
//!
//! [`dashmap::DashMap`] provides concurrent per-namespace lookup; each value is
//! an [`std::sync::Arc`] so an in-flight append keeps its state alive even if the map entry
//! is removed. [`std::sync::Mutex`] protects only short, non-async queue edits,
//! while [`tokio::sync::Mutex`] lets a commit leader wait across object-store
//! `.await` points without blocking a runtime thread. Java would commonly use a
//! concurrent map, monitor, and `CompletableFuture`; C would require explicit
//! mutex and lifetime discipline. Rust's ownership plus
//! [`tokio::sync::oneshot`] channels
//! ensure each queued reply sender has one destination and is consumed exactly
//! once when sending.

use dashmap::DashMap;
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex};
use tracing::{debug, error, instrument, warn};

use crate::error::{Result, ZeppelinError};
use crate::storage::ZeppelinStore;
use crate::time::Clock;
use crate::types::{VectorEntry, VectorId};

use super::fragment::WalFragment;
use super::manifest::{FragmentRef, Manifest, ManifestVersion};

/// Maximum number of manifest CAS attempts made for one commit batch.
///
/// The writer path previously used 5 attempts with ZERO backoff, so retries
/// burned out in microseconds under contention and surfaced a 409 that a
/// human-paced retry would have avoided. Eight attempts spread across
/// [`cas_backoff`] absorb moderate contention while keeping failure bounded.
///
/// A batch that loses all attempts fails with
/// [`ZeppelinError::ManifestConflict`] and its fragment objects are cleaned up
/// best-effort.
const MAX_CAS_RETRIES: u32 = 8;

/// Chooses bounded exponential delay plus jitter after a manifest CAS conflict.
///
/// # Parameters
///
/// - `attempt`: Zero-based attempt that just conflicted. Values at and above six
///   use the same capped exponential component.
///
/// # Returns
///
/// A delay with a `10 * 2^attempt` millisecond base capped at 500 ms, plus
/// uniformly selected jitter from 0 through 9 ms.
///
/// # Examples
///
/// Attempts `0`, `1`, and `2` wait approximately 10, 20, and 40 ms. Later
/// attempts wait approximately 500 ms rather than growing without bound.
///
/// # Rust Notes for Java/C Engineers
///
/// [`Duration`] is a value type returned by move with no heap allocation. The
/// bounded shift makes overflow impossible here; Java/C code would need the same
/// explicit cap to avoid shifting a fixed-width integer too far.
fn cas_backoff(attempt: u32) -> Duration {
    let base_ms = (10u64 << attempt.min(6)).min(500);
    let jitter_ms = rand::thread_rng().gen_range(0..10);
    Duration::from_millis(base_ms + jitter_ms)
}

/// Exact authoritative manifest observation required by a derived write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestAppendGuard {
    namespace: String,
    namespace_incarnation: uuid::Uuid,
    generation: u64,
    storage_version: ManifestVersion,
}

impl ManifestAppendGuard {
    /// Binds a manifest generation and incarnation to the ETag from one GET.
    pub fn new(
        namespace: &str,
        manifest: &Manifest,
        storage_version: ManifestVersion,
    ) -> Result<Self> {
        storage_version.require_version(namespace, "scoped manifest append")?;
        let namespace_incarnation = manifest
            .namespace_incarnation()
            .ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "scoped manifest append for namespace {namespace} requires an incarnation-bound manifest"
                ))
            })?;
        Ok(Self {
            namespace: namespace.to_string(),
            namespace_incarnation,
            generation: manifest.version(),
            storage_version,
        })
    }
}

/// One uploaded fragment waiting for a manifest publication result.
///
/// A value enters the queue only after its immutable object PUT succeeds. The
/// commit leader consumes it, publishes its ref, and uses the one-shot sender
/// to return the covering manifest or terminal error. Ordinary appends may
/// share a same-token batch; guarded derived writes always publish alone.
struct PendingCommit {
    /// Reference to the durable-but-not-yet-visible fragment object.
    fref: FragmentRef,
    /// Lease generation carried by this append, or `None` for the v1
    /// single-writer path.
    ///
    /// Only exactly equal options share a batch: `Some(7)` cannot mix with
    /// `Some(8)` or `None`.
    fencing_token: Option<u64>,
    /// Exact manifest generation whose live records authorized this append.
    ///
    /// Scoped deletes and constrained upserts use `Some(guard)` so a namespace
    /// recreation or any intervening manifest write forces the caller to reload
    /// records and re-evaluate its mandatory filter. Ordinary appends use
    /// `None`. Guarded commits never share a group batch, including when their
    /// guards are equal.
    expected_manifest: Option<ManifestAppendGuard>,
    /// Single-use channel that resolves the originating append.
    ///
    /// The sender carries either the manifest that references `fref` or the
    /// terminal publication error. Dropping it without sending closes the
    /// receiver and is surfaced as an internal group-commit error.
    reply: oneshot::Sender<Result<Manifest>>,
}

/// Queue and leader-election state shared by appends to one namespace.
///
/// The two locks deliberately have different roles. [`Self::pending`] uses a
/// standard mutex because its critical sections contain no `.await` and only
/// move queue entries. [`Self::commit_lock`] is async because its leader holds
/// the guard while performing remote manifest reads, writes, and retry sleeps.
#[derive(Default)]
struct GroupCommitState {
    /// Durable fragment refs awaiting publication and their reply senders.
    ///
    /// Poison from a panicked holder is recovered so later appends can fail or
    /// make progress explicitly instead of permanently losing the namespace.
    pending: std::sync::Mutex<Vec<PendingCommit>>,
    /// Elects one task to serialize manifest CAS rounds for this namespace.
    commit_lock: Mutex<()>,
    /// Last manifest this process committed, paired with that CAS PUT's ETag.
    ///
    /// This is disposable optimistic state: only a successful conditional
    /// publication with a backend-provided ETag may populate it. Every
    /// conflict, terminal error, or missing ETag clears it. The mutex is never
    /// held across `.await`.
    committed: std::sync::Mutex<Option<(Manifest, ManifestVersion)>>,
}

impl GroupCommitState {
    fn committed_snapshot(&self) -> Option<(Manifest, ManifestVersion)> {
        self.committed
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    fn replace_committed(&self, value: Option<(Manifest, ManifestVersion)>) {
        *self
            .committed
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = value;
    }

    fn clear_committed(&self) {
        self.replace_committed(None);
    }
}

/// Writes immutable fragments and group-commits their manifest references.
///
/// One writer instance coalesces concurrent appends through its local
/// per-namespace map. Separate processes or writer instances do not share that
/// map; their authoritative coordination occurs at manifest CAS and, when used,
/// fencing tokens.
pub struct WalWriter {
    /// Shared object-store handle used for fragment and manifest operations.
    store: ZeppelinStore,
    /// Process-local group-commit state keyed by namespace.
    ///
    /// Entries are created lazily and removed when namespace deletion has
    /// quiesced local writes.
    groups: DashMap<String, Arc<GroupCommitState>>,
    /// Explicit wall clock used to stamp manifest mutations.
    clock: Clock,
}

impl WalWriter {
    /// Creates a writer with no allocated per-namespace group state.
    ///
    /// # Parameters
    ///
    /// - `store`: Object-store abstraction used for immutable fragment PUTs and
    ///   authoritative manifest CAS.
    ///
    /// # Returns
    ///
    /// A writer ready to accept appends. Construction performs no remote I/O.
    ///
    /// # Examples
    ///
    /// Server startup can create one writer and share it through application
    /// state; the first append to `catalog` creates that namespace's local group.
    pub fn new(store: ZeppelinStore) -> Self {
        Self::with_clock(store, Clock::system())
    }

    /// Creates a writer with an explicitly selected wall-clock source.
    #[must_use]
    pub fn with_clock(store: ZeppelinStore, clock: Clock) -> Self {
        Self {
            store,
            groups: DashMap::new(),
            clock,
        }
    }

    /// Gets or lazily creates shared group-commit state for a namespace.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace whose local append queue and commit leader are
    ///   required.
    ///
    /// # Returns
    ///
    /// An [`Arc`] clone of the state stored in [`Self::groups`]. Cloning the
    /// `Arc` increments a reference count; it does not clone either mutex or the
    /// queued commits.
    ///
    /// # Side Effects
    ///
    /// Allocates and inserts an empty state on the first access for a namespace.
    ///
    /// # Examples
    ///
    /// Ten concurrent appends through one writer obtain handles to the same
    /// `catalog` state and therefore elect one local commit leader.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The `DashMap` entry API performs lookup-or-insert atomically. Java's
    /// `ConcurrentHashMap.computeIfAbsent` is the nearest analogy. In C this
    /// requires an external map lock and reference-counted allocation. Rust's
    /// [`Arc`] guarantees the state outlives every returned handle even if the
    /// map entry is removed.
    fn group(&self, namespace: &str) -> Arc<GroupCommitState> {
        self.groups
            .entry(namespace.to_string())
            .or_insert_with(|| Arc::new(GroupCommitState::default()))
            .value()
            .clone()
    }

    /// Removes this writer's process-local group state after namespace deletion.
    ///
    /// This frees idle queue/lock state; it does not delete a manifest, WAL
    /// object, lease, or any other remote artifact. Namespace deletion callers
    /// must quiesce local appends first. An in-flight append keeps its old state
    /// alive through [`Arc`], and a concurrent later append could create a new
    /// entry, so this method is not itself a write barrier.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Exact map key to remove.
    ///
    /// # Returns
    ///
    /// Returns `()` whether or not the namespace had local state.
    ///
    /// # Side Effects
    ///
    /// Removes one in-memory map entry and drops the map's [`Arc`] reference.
    ///
    /// # Examples
    ///
    /// After `catalog` is deleted and its writes have stopped, removing its state
    /// reduces local bookkeeping. Removing `catalog` a second time is a no-op.
    pub fn remove_lock(&self, namespace: &str) {
        self.groups.remove(namespace);
    }

    /// Returns the number of namespace group states retained by this writer.
    ///
    /// This test-only observation counts map entries, not active locks, queued
    /// appends, object-store leases, or namespaces in S3.
    ///
    /// # Returns
    ///
    /// Current process-local [`Self::groups`] entry count.
    ///
    /// # Examples
    ///
    /// Accessing groups for two namespaces yields `2`; removing one yields `1`.
    #[cfg(test)]
    pub fn lock_count(&self) -> usize {
        self.groups.len()
    }

    /// Appends vectors and deletes through the unfenced single-writer path.
    ///
    /// This delegates to [`Self::append_with_lease`] with no fencing token. It
    /// still uses manifest CAS to prevent lost updates, but assumes the v1
    /// deployment rule that one logical writer owns the namespace.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Existing namespace whose manifest will publish the new
    ///   fragment.
    /// - `vectors`: Owned vector upserts moved into a new immutable fragment.
    /// - `deletes`: Owned vector IDs moved into the same fragment as tombstones.
    ///
    /// # Returns
    ///
    /// The newly created fragment and an owned snapshot of the committed
    /// manifest that references it, suitable for write-through cache update.
    ///
    /// # Errors
    ///
    /// Propagates validation, serialization, fragment PUT, manifest read/CAS,
    /// and terminal contention failures described by
    /// [`Self::append_with_lease`]. A failed publication may have uploaded an
    /// orphan whose best-effort deletion also failed; an internal dropped-reply
    /// invariant failure can likewise leave the uploaded object for GC.
    ///
    /// # Side Effects
    ///
    /// Increments append metrics, uploads one fragment, and publishes one
    /// manifest generation directly or as part of a group batch.
    ///
    /// # Consistency
    ///
    /// ETag CAS prevents overwriting a concurrent manifest, but without a token
    /// this API does not distinguish current and zombie lease generations.
    ///
    /// # Examples
    ///
    /// Appending three vectors to an existing namespace uploads one `.wal`
    /// object. The vectors become visible only after the returned manifest
    /// successfully references that fragment.
    #[instrument(skip(self, vectors, deletes), fields(namespace = namespace))]
    pub async fn append(
        &self,
        namespace: &str,
        vectors: Vec<VectorEntry>,
        deletes: Vec<VectorId>,
    ) -> Result<(WalFragment, Manifest)> {
        self.append_with_lease(namespace, vectors, deletes, None)
            .await
    }

    /// Uploads one immutable fragment and group-commits its manifest reference.
    ///
    /// The upload occurs before local leader election so concurrent fragment PUTs
    /// can overlap. A successful conformant PUT is durable but not visible. The
    /// append then queues its ref for same-token group commit and returns only
    /// after a manifest CAS references it.
    ///
    /// With `Some(token)`, publication applies two defenses:
    ///
    /// 1. **Fencing check:** reject when the fresh manifest token is greater than
    ///    the caller's token; otherwise ratchet the manifest to the caller token.
    /// 2. **CAS:** condition the manifest PUT on the ETag just read. A conflict
    ///    retries from a fresh read and therefore repeats the fencing check.
    ///
    /// A lease expiry is not read here. Until a newer holder publishes a higher
    /// token, an old token can still pass; this is why both lease heartbeat policy
    /// and the manifest's monotonic fencing generation matter.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Existing namespace whose authoritative manifest should
    ///   reference the new WAL fragment.
    /// - `vectors`: Owned upsert entries. A vector ID may not also appear in
    ///   `deletes` for this fragment.
    /// - `deletes`: Owned tombstone IDs moved into the fragment.
    /// - `fencing_token`: Lease generation to enforce, or `None` for the v1
    ///   unfenced single-writer path.
    ///
    /// # Returns
    ///
    /// The owned fragment and an owned clone of the manifest generation that
    /// covers it. The fragment's batch peers can receive equivalent manifest
    /// snapshots.
    ///
    /// # Errors
    ///
    /// Returns validation when an ID is both upserted and deleted, serialization
    /// or fragment PUT failures before queueing, and manifest publication errors
    /// after upload. A missing manifest becomes
    /// [`ZeppelinError::ManifestNotFound`]; a higher manifest token becomes
    /// [`ZeppelinError::FencingTokenStale`]; exhausted CAS retries become
    /// [`ZeppelinError::ManifestConflict`]. Some other manifest read/write errors
    /// are delivered to waiters as [`ZeppelinError::Index`] with context.
    ///
    /// Once upload succeeds, manifest reads before publication, fencing, and
    /// exhausted CAS conflicts trigger exact-key orphan cleanup. A non-conflict
    /// manifest-write error is ambiguous: the writer re-reads S3, recovers
    /// success when every batched ref is live, cleans up only when none are live,
    /// and leaves all objects untouched on missing, unreadable, or partially
    /// matching state. A closed reply channel or impossible no-progress state
    /// reports an internal `Index` error without its own cleanup.
    ///
    /// # Side Effects
    ///
    /// Increments `WAL_APPENDS_TOTAL` before validation, creates one ULID, uploads
    /// one complete fragment, mutates the process-local group queue, and may lead
    /// one or more manifest GET/CAS rounds. Success advances manifest sequence
    /// numbers and can publish other same-token waiters at the same time.
    ///
    /// # Consistency
    ///
    /// The fragment is invisible between its PUT and successful manifest CAS.
    /// Grouping never crosses fencing-token options. Guarded appends additionally
    /// bind the namespace incarnation, manifest generation, and exact object-store
    /// ETag and publish alone, so two writes derived from one observation cannot
    /// both succeed. The returned success is the acknowledgment boundary: it
    /// proves the fragment is durable and referenced by the committed manifest
    /// observed by this operation.
    ///
    /// # Performance
    ///
    /// Serializes and uploads the whole fragment once. Group commit can reduce N
    /// concurrent same-instance appends from N manifest writes toward one, though
    /// scheduling may produce multiple batches. CAS conflicts use bounded
    /// exponential backoff and repeat a full manifest GET and serialization. A
    /// non-conflict CAS error adds one authoritative GET to resolve whether the
    /// atomic batch committed before its acknowledgement was lost.
    ///
    /// # Examples
    ///
    /// ```text
    /// token 7 append -> fragment PUT -> manifest token 6, ETag v20
    ///                -> set token 7 + add ref -> PUT-if-v20 succeeds -> visible
    ///
    /// zombie token 7 -> fragment PUT -> manifest token 8
    ///                -> FencingTokenStale -> exact fragment delete attempted
    ///
    /// token 8 race   -> check passes at v21 -> another writer publishes v22
    ///                -> CAS conflict -> back off -> read v22 -> re-check token
    /// ```
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `vectors` and `deletes` are moved into [`WalFragment::try_new`], so the
    /// caller cannot use those vectors afterward; Java has no equivalent
    /// compiler-enforced ownership transfer, while C would rely on a documented
    /// ownership convention. The fragment is retained as an owned return value,
    /// while its manifest ref clones only selected metadata. A one-shot channel
    /// lets the eventual leader resolve this task without sharing a mutable
    /// result object.
    #[instrument(skip(self, vectors, deletes), fields(namespace = namespace))]
    pub async fn append_with_lease(
        &self,
        namespace: &str,
        vectors: Vec<VectorEntry>,
        deletes: Vec<VectorId>,
        fencing_token: Option<u64>,
    ) -> Result<(WalFragment, Manifest)> {
        self.append_with_guards(namespace, vectors, deletes, fencing_token, None)
            .await
    }

    /// Appends only if the authoritative manifest still matches a read snapshot.
    ///
    /// This is the publication boundary for operations such as scoped deletes
    /// whose exact tombstone set was derived from live records. Any manifest
    /// generation change, including a same-process queued write, returns a
    /// conflict instead of retrying against data the caller never authorized.
    #[instrument(skip(self, deletes), fields(namespace = namespace))]
    pub async fn append_deletes_if_manifest_unchanged(
        &self,
        namespace: &str,
        deletes: Vec<VectorId>,
        expected_manifest: ManifestAppendGuard,
    ) -> Result<(WalFragment, Manifest)> {
        self.append_with_guards(
            namespace,
            Vec::new(),
            deletes,
            None,
            Some(expected_manifest),
        )
        .await
    }

    /// Appends replacement rows only while an authorization preflight snapshot
    /// remains the exact authoritative manifest.
    ///
    /// Scoped upserts inspect existing rows before publication so they cannot
    /// capture an out-of-scope ID or erase a protected field. They must not retry
    /// against a newer manifest whose rows were never evaluated by that request.
    #[instrument(skip(self, vectors), fields(namespace = namespace))]
    pub async fn append_upserts_if_manifest_unchanged(
        &self,
        namespace: &str,
        vectors: Vec<VectorEntry>,
        expected_manifest: ManifestAppendGuard,
    ) -> Result<(WalFragment, Manifest)> {
        self.append_with_guards(
            namespace,
            vectors,
            Vec::new(),
            None,
            Some(expected_manifest),
        )
        .await
    }

    async fn append_with_guards(
        &self,
        namespace: &str,
        vectors: Vec<VectorEntry>,
        deletes: Vec<VectorId>,
        fencing_token: Option<u64>,
        expected_manifest: Option<ManifestAppendGuard>,
    ) -> Result<(WalFragment, Manifest)> {
        crate::metrics::WAL_APPENDS_TOTAL
            .with_label_values(&[namespace])
            .inc();

        let fragment = WalFragment::try_new(vectors, deletes)?;

        // 1. Write the fragment to S3 — OUTSIDE any manifest critical section,
        //    so concurrent appends' PUTs run in parallel (I1).
        let key = WalFragment::s3_key(namespace, &fragment.id);
        let data = fragment.to_bytes()?;
        let size_bytes = data.len() as u64;
        self.store.put(&key, data).await?;

        debug!(
            fragment_id = %fragment.id,
            vectors = fragment.vectors.len(),
            deletes = fragment.deletes.len(),
            "wrote WAL fragment"
        );

        // 2. Enqueue our fragment ref for group commit and contend to lead.
        let group = self.group(namespace);
        let (reply_tx, mut reply_rx) = oneshot::channel();
        {
            // A poisoned lock means a prior leader panicked mid-commit; recover
            // the guard and carry on rather than propagating the panic.
            let mut pending = group.pending.lock().unwrap_or_else(|e| e.into_inner());
            pending.push(PendingCommit {
                fref: FragmentRef {
                    id: fragment.id,
                    vector_count: fragment.vectors.len(),
                    delete_count: fragment.deletes.len(),
                    sequence_number: 0, // assigned by add_fragment at commit
                    size_bytes,
                    artifact_origin: None,
                },
                fencing_token,
                expected_manifest,
                reply: reply_tx,
            });
        }

        // 3. Acquire the per-namespace commit lock. The holder is the leader:
        //    it commits rounds (each folding one fencing-token group into a
        //    single CAS) until ITS OWN oneshot is resolved, then releases the
        //    lock. Looping until self-resolved is what prevents the deadlock
        //    where a leader defers its own ref (its token differs from the
        //    queue front) and then blocks on its own reply while still holding
        //    the lock — nobody else could ever acquire it. If a prior leader
        //    already committed our ref before we got the lock, `try_recv`
        //    short-circuits; we still lead one round to sweep any stragglers.
        let _guard = group.commit_lock.lock().await;

        if let Ok(result) = reply_rx.try_recv() {
            // A prior leader handled us. Sweep any stragglers that queued
            // behind us so they don't wait for the next arriving writer.
            self.commit_pending_group(namespace, &group).await;
            return result.map(|manifest| (fragment, manifest));
        }

        // Lead rounds until our own reply lands. Each round commits the
        // oldest-token group; deferred groups are picked up by our next
        // iteration (we hold the lock the whole time), so our own ref is
        // guaranteed to be committed before we exit — no self-deadlock.
        loop {
            let committed_any = self.commit_pending_group(namespace, &group).await;
            match reply_rx.try_recv() {
                Ok(result) => return result.map(|manifest| (fragment, manifest)),
                Err(oneshot::error::TryRecvError::Closed) => {
                    return Err(ZeppelinError::Index(format!(
                        "group commit dropped reply for namespace {namespace}"
                    )));
                }
                Err(oneshot::error::TryRecvError::Empty) => {
                    // Our ref wasn't committed yet. It must still be pending
                    // (a leader only drains under the same lock we hold), so
                    // another round will reach it. `committed_any == false`
                    // with our reply still empty would be a logic bug — guard
                    // against a spin by asserting progress in debug builds.
                    debug_assert!(
                        committed_any,
                        "group commit made no progress but our ref is uncommitted"
                    );
                    if !committed_any {
                        // Defensive: avoid a hot spin in release builds if the
                        // invariant is ever violated.
                        return Err(ZeppelinError::Index(format!(
                            "group commit stalled for namespace {namespace}"
                        )));
                    }
                }
            }
        }
    }

    /// Commits one oldest fencing-token group and resolves all of its waiters.
    ///
    /// The caller must hold [`GroupCommitState::commit_lock`]. This method drains
    /// the queue briefly, selects the token option on its oldest element, and
    /// defers every different-token item for a later round by the same leader.
    /// It then performs the fencing/CAS retry protocol for the selected batch.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace whose manifest should publish the selected refs.
    /// - `group`: Borrowed per-namespace state whose commit lock is already held.
    ///
    /// # Returns
    ///
    /// `true` when a non-empty batch was consumed and every waiter was sent a
    /// success or failure result. `false` means the pending queue was empty (or,
    /// defensively, no batch could be selected). Publication errors travel over
    /// the waiters' channels rather than in this return value.
    ///
    /// # Side Effects
    ///
    /// Removes one token group from the local queue, reads and conditionally
    /// writes the authoritative manifest, sleeps after CAS conflicts, clones a
    /// committed manifest per waiter, and attempts orphan cleanup only after
    /// proving the batch absent from authoritative state.
    ///
    /// # Consistency
    ///
    /// The first attempt may use the last manifest/ETag pair this process
    /// successfully committed. CAS remains authoritative: a conflict clears the
    /// memo, and every retry reads a fresh pair before repeating the fencing
    /// check. Same-token grouping prevents one lease generation from gaining
    /// visibility under another generation's successful check. Deferred items
    /// remain queued while the caller retains leadership, avoiding the
    /// self-deadlock where a leader waits for a reply only it can produce.
    ///
    /// # Performance
    ///
    /// Queue partitioning is linear in pending waiters. A cold attempt performs
    /// one manifest GET; consecutive successful rounds reuse the prior CAS
    /// result and perform no manifest GET. Success adds one history/publication
    /// sequence through [`Manifest::write_conditional`]. Cloning [`Manifest`]
    /// for the memo and every reply can allocate in proportion to manifest size
    /// and batch length.
    ///
    /// # Examples
    ///
    /// For queue tokens `[7, 8, 7, None]`, the first round commits both token-7
    /// refs together, leaving `[8, None]`. Later rounds commit token 8 and `None`
    /// separately while the same append task still owns leadership.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Draining the `Vec` moves each [`PendingCommit`] into either `batch` or
    /// `deferred`; no queue entry is shallow-copied. The short standard mutex
    /// guard drops at the end of the block before any `.await`. Rust's scope
    /// therefore makes the no-lock-across-I/O rule visible to the compiler and
    /// reviewer; Java/C implementations rely more heavily on disciplined unlock
    /// paths.
    async fn commit_pending_group(&self, namespace: &str, group: &GroupCommitState) -> bool {
        // Drain the pending queue, partitioning by fencing token and expected
        // manifest generation. Every guarded append publishes alone: even two
        // requests derived from the same snapshot must linearize independently
        // so the second observes and conflicts with the first publication.
        let batch: Vec<PendingCommit> = {
            let mut pending = group.pending.lock().unwrap_or_else(|e| e.into_inner());
            if pending.is_empty() {
                return false;
            }
            let leader_token = pending[0].fencing_token;
            let leader_expected_manifest = pending[0].expected_manifest.clone();
            let leader_is_guarded = leader_expected_manifest.is_some();
            let mut batch = Vec::with_capacity(pending.len());
            let mut deferred = Vec::new();
            for item in pending.drain(..) {
                if item.fencing_token == leader_token
                    && item.expected_manifest == leader_expected_manifest
                    && (!leader_is_guarded || batch.is_empty())
                {
                    batch.push(item);
                } else {
                    deferred.push(item);
                }
            }
            *pending = deferred;
            batch
        };
        if batch.is_empty() {
            return false;
        }
        let fencing_token = batch[0].fencing_token;
        let expected_manifest = batch[0].expected_manifest.clone();

        // CAS retry loop: one manifest update carrying ALL batched refs.
        //
        // When authoritative state proves a terminal failure left every batched
        // fragment orphaned, best-effort delete each exact wal/ key (I3), never
        // a prefix or segment/cluster object. A non-conflict write error is not
        // proof: the conditional PUT may have committed before its reply was
        // lost, so that path re-reads S3 before deciding whether cleanup is safe.
        for attempt in 0..MAX_CAS_RETRIES {
            let memo = if attempt == 0 && expected_manifest.is_none() {
                group.committed_snapshot()
            } else {
                None
            };
            let (mut manifest, version) = match memo {
                Some(pair) => pair,
                None => match Manifest::read_versioned(&self.store, namespace).await {
                    Ok(Some(pair)) => pair,
                    Ok(None) => {
                        group.clear_committed();
                        self.fail_batch(namespace, batch, |_| ZeppelinError::ManifestNotFound {
                            namespace: namespace.to_string(),
                        })
                        .await;
                        return true;
                    }
                    Err(e) => {
                        group.clear_committed();
                        let msg = e.to_string();
                        self.fail_batch(namespace, batch, |_| {
                            ZeppelinError::Index(format!("manifest read failed: {msg}"))
                        })
                        .await;
                        return true;
                    }
                },
            };

            if expected_manifest.as_ref().is_some_and(|expected| {
                expected.namespace != namespace
                    || manifest.namespace_incarnation() != Some(expected.namespace_incarnation)
                    || manifest.version() != expected.generation
                    || !version.matches_live_authority(&expected.storage_version)
            }) {
                group.clear_committed();
                self.fail_batch(namespace, batch, |_| ZeppelinError::ManifestConflict {
                    namespace: namespace.to_string(),
                })
                .await;
                return true;
            }

            // Layer 1: Fencing check — reject zombie writers before committing.
            if let Some(token) = fencing_token {
                if manifest.fencing_token > token {
                    group.clear_committed();
                    let mtoken = manifest.fencing_token;
                    self.fail_batch(namespace, batch, |_| ZeppelinError::FencingTokenStale {
                        namespace: namespace.to_string(),
                        our_token: token,
                        manifest_token: mtoken,
                    })
                    .await;
                    return true;
                }
                manifest.fencing_token = token;
            }

            // Fold every batched fragment ref into this one manifest.
            let manifest_stamp = self.clock.now();
            for item in &batch {
                manifest.add_fragment_at(item.fref.clone(), manifest_stamp);
            }

            // Layer 2: CAS — catches TOCTOU between fencing check and write.
            match manifest
                .write_conditional(&self.store, namespace, &version)
                .await
            {
                Ok(new_version) => {
                    let next_memo = if new_version.has_version() {
                        Some((manifest.clone(), new_version))
                    } else {
                        None
                    };
                    group.replace_committed(next_memo);
                    debug!(
                        batched = batch.len(),
                        fragment_count = manifest.fragments.len(),
                        attempt,
                        "group commit updated manifest"
                    );
                    for item in batch {
                        let _ = item.reply.send(Ok(manifest.clone()));
                    }
                    return true;
                }
                Err(ZeppelinError::ManifestConflict { .. }) => {
                    group.clear_committed();
                    warn!(
                        attempt,
                        namespace,
                        batched = batch.len(),
                        "group commit CAS conflict, retrying with backoff"
                    );
                    tokio::time::sleep(cas_backoff(attempt)).await;
                    continue;
                }
                Err(e) => {
                    group.clear_committed();
                    let write_error = e.to_string();
                    match Manifest::read(&self.store, namespace).await {
                        Ok(Some(authoritative)) => {
                            let published = batch
                                .iter()
                                .filter(|item| {
                                    authoritative
                                        .fragments
                                        .iter()
                                        .any(|fref| fref.id == item.fref.id)
                                })
                                .count();

                            if published == batch.len() {
                                warn!(
                                    namespace,
                                    batched = batch.len(),
                                    error = %write_error,
                                    "recovered committed manifest after lost write acknowledgement"
                                );
                                for item in batch {
                                    let _ = item.reply.send(Ok(authoritative.clone()));
                                }
                            } else if published == 0 {
                                self.fail_batch(namespace, batch, |_| {
                                    ZeppelinError::Index(format!(
                                        "manifest write failed: {write_error}"
                                    ))
                                })
                                .await;
                            } else {
                                error!(
                                    namespace,
                                    published,
                                    batched = batch.len(),
                                    error = %write_error,
                                    "manifest write outcome violated atomic batch publication"
                                );
                                Self::reply_batch_error(batch, |_| {
                                    ZeppelinError::Index(format!(
                                        "manifest write outcome invariant violation for namespace \
                                         {namespace}: authoritative manifest contains {published} \
                                         batched refs after write error, expected either 0 or all: \
                                         {write_error}"
                                    ))
                                });
                            }
                        }
                        Ok(None) => {
                            error!(
                                namespace,
                                batched = batch.len(),
                                error = %write_error,
                                "manifest disappeared while resolving ambiguous write"
                            );
                            Self::reply_batch_error(batch, |_| {
                                ZeppelinError::Index(format!(
                                    "manifest write outcome indeterminate for namespace \
                                     {namespace}: authoritative manifest missing after write \
                                     error: {write_error}"
                                ))
                            });
                        }
                        Err(read_error) => {
                            error!(
                                namespace,
                                batched = batch.len(),
                                error = %write_error,
                                reread_error = %read_error,
                                "failed to resolve ambiguous manifest write"
                            );
                            Self::reply_batch_error(batch, |_| {
                                ZeppelinError::Index(format!(
                                    "manifest write outcome indeterminate for namespace \
                                     {namespace}: write failed: {write_error}; authoritative \
                                     reread failed: {read_error}"
                                ))
                            });
                        }
                    }
                    return true;
                }
            }
        }

        // Retries exhausted under sustained contention: the only terminal error
        // reachable here is a persistent CAS conflict (other errors return
        // early above). Orphan every batched fragment and fail all waiters 409.
        group.clear_committed();
        self.fail_batch(namespace, batch, |_| ZeppelinError::ManifestConflict {
            namespace: namespace.to_string(),
        })
        .await;
        true
    }

    /// Attempts orphan cleanup for a failed batch and errors every waiter.
    ///
    /// Cleanup happens before replies so the common failure path leaves no
    /// unreferenced `.wal` objects. The closure constructs a separate error for
    /// each waiter because [`ZeppelinError`] is not uniformly cloneable.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace used to derive each exact fragment key.
    /// - `batch`: Owned commits whose fragments were uploaded but not published.
    /// - `make_err`: Synchronous factory borrowing each item to build its reply
    ///   error after cleanup.
    ///
    /// # Returns
    ///
    /// Returns `()` after all cleanup attempts and sends. It deliberately does
    /// not return cleanup or dropped-receiver failures.
    ///
    /// # Side Effects
    ///
    /// Performs one best-effort DELETE per fragment, then consumes every one-shot
    /// sender with an error. A caller that dropped its receiver simply does not
    /// observe the send.
    ///
    /// # Consistency
    ///
    /// No manifest references these objects through this failed batch. Exact-key
    /// deletion cannot remove a segment or a different fragment. Cleanup failure
    /// leaves an orphan for GC but never changes the terminal append failure.
    ///
    /// # Examples
    ///
    /// Three uploaded fragments encounter a stale token. This method attempts
    /// three exact `.wal` deletes and sends `FencingTokenStale` independently to
    /// all three waiting appends.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `impl Fn(&PendingCommit) -> ZeppelinError` is a statically dispatched
    /// closure parameter: the compiler specializes this helper for each factory
    /// without a virtual call. It resembles a Java functional interface or a C
    /// function pointer plus captured context, but Rust checks captured-value and
    /// borrow lifetimes at compile time.
    async fn fail_batch(
        &self,
        namespace: &str,
        batch: Vec<PendingCommit>,
        make_err: impl Fn(&PendingCommit) -> ZeppelinError,
    ) {
        for item in &batch {
            let key = WalFragment::s3_key(namespace, &item.fref.id);
            self.cleanup_orphan_fragment(namespace, &key).await;
        }
        Self::reply_batch_error(batch, make_err);
    }

    /// Sends one terminal error per waiter without deleting uploaded fragments.
    ///
    /// This is the only safe failure path when an authoritative re-read cannot
    /// prove the entire batch unpublished. Possible orphans are intentionally
    /// left for reachability GC because exact cleanup could otherwise delete an
    /// immutable object already referenced by S3 state.
    fn reply_batch_error(
        batch: Vec<PendingCommit>,
        make_err: impl Fn(&PendingCommit) -> ZeppelinError,
    ) {
        for item in batch {
            let err = make_err(&item);
            let _ = item.reply.send(Err(err));
        }
    }

    /// Best-effort deletes one fragment that failed to reach the manifest.
    ///
    /// The key must be the exact `.../wal/<ulid>.wal` object created by the
    /// failed append. A deletion error is logged and swallowed because the
    /// publication error is the caller-visible failure and later GC can discover
    /// the orphan.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace included in structured diagnostics.
    /// - `key`: Exact fragment object key; never a prefix or segment key.
    ///
    /// # Returns
    ///
    /// Returns `()` whether deletion succeeds or fails.
    ///
    /// # Side Effects
    ///
    /// Performs one object-store DELETE and logs its outcome.
    ///
    /// # Consistency
    ///
    /// This helper is safe only after publication has definitively failed. It
    /// must never be called for a ref visible through an authoritative manifest.
    ///
    /// # Examples
    ///
    /// If `catalog/wal/01H....wal` was uploaded before a CAS retry exhausted,
    /// this method deletes exactly that object. A backend failure leaves it
    /// unreferenced for reachability-based GC.
    async fn cleanup_orphan_fragment(&self, namespace: &str, key: &str) {
        match self.store.delete(key).await {
            Ok(()) => {
                debug!(
                    namespace,
                    key, "cleaned up orphaned fragment after failed append"
                );
            }
            Err(e) => {
                warn!(
                    namespace,
                    key,
                    error = %e,
                    "failed to clean up orphaned fragment (left for GC)"
                );
            }
        }
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Protects lifecycle cleanup for process-local namespace group state.
    //!
    //! These unit tests use an in-memory object store because they exercise only
    //! process-local state in [`crate::wal::writer::WalWriter`], not storage
    //! semantics. Integration tests cover real S3/MinIO fragment publication,
    //! CAS contention, fencing, and orphan cleanup.

    use super::*;

    /// Verifies that deleting namespaces removes their independent local states.
    ///
    /// The test creates two group entries, removes them one at a time, and would
    /// catch retention caused by using the wrong namespace key.
    #[test]
    fn test_remove_lock_cleans_up() {
        let mem = std::sync::Arc::new(object_store::memory::InMemory::new());
        let store = crate::storage::ZeppelinStore::new(mem);
        let writer = WalWriter::new(store);

        // Create per-namespace group state by accessing it
        let _g1 = writer.group("ns1");
        let _g2 = writer.group("ns2");
        assert_eq!(writer.lock_count(), 2);

        // Remove one lock
        writer.remove_lock("ns1");
        assert_eq!(writer.lock_count(), 1);

        // Remove the other
        writer.remove_lock("ns2");
        assert_eq!(writer.lock_count(), 0);
    }

    /// Verifies that cleanup is idempotent for a namespace with no local state.
    ///
    /// Namespace deletion may call cleanup after state is already absent; this
    /// test protects the no-panic, zero-count contract.
    #[test]
    fn test_remove_lock_nonexistent_is_noop() {
        let mem = std::sync::Arc::new(object_store::memory::InMemory::new());
        let store = crate::storage::ZeppelinStore::new(mem);
        let writer = WalWriter::new(store);

        // Should not panic
        writer.remove_lock("nonexistent");
        assert_eq!(writer.lock_count(), 0);
    }
}
