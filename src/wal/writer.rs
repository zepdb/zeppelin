use dashmap::DashMap;
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, info, instrument, warn};

use crate::error::{Result, ZeppelinError};
use crate::storage::ZeppelinStore;
use crate::types::{VectorEntry, VectorId};

use super::fragment::WalFragment;
use super::manifest::{FragmentRef, Manifest};

/// Maximum CAS retry attempts for manifest updates.
///
/// The writer path previously used 5 attempts with ZERO backoff, so retries
/// burned out in microseconds under contention and surfaced a 409 that a
/// human-paced retry would have avoided. We now use more attempts spread over
/// exponential backoff + jitter (see `cas_backoff`), matching the compactor.
const MAX_CAS_RETRIES: u32 = 8;

/// Backoff before CAS retry `attempt` (0-indexed): exponential base ~10ms,
/// capped ~500ms, plus up to 10ms of jitter to de-synchronize contending
/// writers. `10 * 2^attempt` → 10, 20, 40, 80, 160, 320, 500(cap)…
fn cas_backoff(attempt: u32) -> Duration {
    let base_ms = (10u64 << attempt.min(6)).min(500);
    let jitter_ms = rand::thread_rng().gen_range(0..10);
    Duration::from_millis(base_ms + jitter_ms)
}

/// One waiting writer's fragment ref plus the channel to hand back the
/// manifest that ultimately references it. Enqueued after the fragment is
/// durably PUT; drained by whichever append becomes the commit leader.
struct PendingCommit {
    fref: FragmentRef,
    /// Fencing token this append carries (`None` for the common single-writer
    /// path). The leader only folds in waiters whose token matches its own —
    /// mixing tokens in one CAS would be unsafe. Same-token (incl. all-None)
    /// is the only case that occurs under single-writer-per-namespace.
    fencing_token: Option<u64>,
    /// Reply with the committed manifest (Ok) or the failure (Err). A dropped
    /// sender means the leader panicked; the waiter treats that as an error.
    reply: oneshot::Sender<Result<Manifest>>,
}

/// Per-namespace group-commit state: a queue of fragment refs waiting to be
/// folded into a single manifest CAS, plus the mutex that elects one leader.
#[derive(Default)]
struct GroupCommitState {
    /// Fragment refs whose objects are durable and awaiting a manifest commit.
    pending: std::sync::Mutex<Vec<PendingCommit>>,
    /// Held by the current commit leader; serializes manifest CAS per namespace.
    commit_lock: Mutex<()>,
}

/// WAL writer with per-namespace group commit to ensure single-writer
/// semantics while coalescing concurrent appends into shared manifest updates.
pub struct WalWriter {
    store: ZeppelinStore,
    /// Per-namespace group-commit state (pending queue + commit mutex).
    groups: DashMap<String, Arc<GroupCommitState>>,
}

impl WalWriter {
    /// Create a new WAL writer backed by the given store.
    pub fn new(store: ZeppelinStore) -> Self {
        Self {
            store,
            groups: DashMap::new(),
        }
    }

    /// Get or create the per-namespace group-commit state.
    fn group(&self, namespace: &str) -> Arc<GroupCommitState> {
        self.groups
            .entry(namespace.to_string())
            .or_insert_with(|| Arc::new(GroupCommitState::default()))
            .value()
            .clone()
    }

    /// Remove the per-namespace group-commit state. Called on namespace deletion.
    pub fn remove_lock(&self, namespace: &str) {
        self.groups.remove(namespace);
    }

    /// Return the number of namespace group states held (for testing).
    #[cfg(test)]
    pub fn lock_count(&self) -> usize {
        self.groups.len()
    }

    /// Append vectors and deletes to the WAL for a namespace.
    /// Creates a new fragment, writes it to S3, and updates the manifest.
    /// Uses CAS (compare-and-swap) for manifest updates to prevent concurrent overwrites.
    ///
    /// Returns the fragment and the updated manifest for write-through caching.
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

    /// Append with an optional fencing token from a lease.
    ///
    /// When `fencing_token` is `Some(token)`:
    /// - **Layer 1 (CheckFencing)**: Before CAS write, checks
    ///   `manifest.fencing_token <= token`. If false → `FencingTokenStale`.
    /// - **Layer 2 (CAS)**: If the ETag changed (concurrent write), retries
    ///   from read, re-checking fencing. A zombie is caught on retry.
    ///
    /// When `fencing_token` is `None`: behaves identically to `append()`.
    ///
    /// Returns the fragment and the updated manifest for write-through caching.
    #[instrument(skip(self, vectors, deletes), fields(namespace = namespace))]
    pub async fn append_with_lease(
        &self,
        namespace: &str,
        vectors: Vec<VectorEntry>,
        deletes: Vec<VectorId>,
        fencing_token: Option<u64>,
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
            let mut pending = group
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            pending.push(PendingCommit {
                fref: FragmentRef {
                    id: fragment.id,
                    vector_count: fragment.vectors.len(),
                    delete_count: fragment.deletes.len(),
                    sequence_number: 0, // assigned by add_fragment at commit
                    size_bytes,
                },
                fencing_token,
                reply: reply_tx,
            });
        }

        // 3. Acquire the per-namespace commit lock. The holder is the leader:
        //    it drains whatever is pending (folding many appends into one CAS)
        //    and replies to every drained waiter. If a prior leader already
        //    committed our ref while we waited, our oneshot is already
        //    fulfilled — but we still lead a round for anyone who arrived
        //    after that leader drained, so nobody is left uncommitted.
        let _guard = group.commit_lock.lock().await;

        // Fast path: a prior leader may have already handled us.
        if let Ok(result) = reply_rx.try_recv() {
            // Still drain any stragglers that queued behind us before we got
            // the lock, so they don't wait for the next arriving writer.
            self.commit_pending_group(namespace, &group).await;
            return result.map(|manifest| (fragment, manifest));
        }

        // We are the leader for the current pending batch (which includes us).
        self.commit_pending_group(namespace, &group).await;

        // Our reply must now be fulfilled by the round we just led.
        match reply_rx.await {
            Ok(result) => result.map(|manifest| (fragment, manifest)),
            Err(_) => Err(ZeppelinError::Index(format!(
                "group commit dropped reply for namespace {namespace}"
            ))),
        }
    }

    /// Leader routine: drain the namespace's pending queue and fold every
    /// same-fencing-token fragment ref into a SINGLE manifest CAS, then reply
    /// to each drained waiter with the committed manifest (or the error).
    ///
    /// Must be called while holding `group.commit_lock`. Waiters whose token
    /// differs from the batch leader's are left in the queue for a subsequent
    /// leader (never mixed into one CAS — see `PendingCommit::fencing_token`).
    async fn commit_pending_group(&self, namespace: &str, group: &GroupCommitState) {
        // Drain the pending queue, partitioning by fencing token: the leader
        // commits the token group of the OLDEST waiter (queue front) and puts
        // any differing-token waiters back for the next leader.
        let batch: Vec<PendingCommit> = {
            let mut pending = group
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if pending.is_empty() {
                return;
            }
            let leader_token = pending[0].fencing_token;
            let mut batch = Vec::with_capacity(pending.len());
            let mut deferred = Vec::new();
            for item in pending.drain(..) {
                if item.fencing_token == leader_token {
                    batch.push(item);
                } else {
                    deferred.push(item);
                }
            }
            *pending = deferred;
            batch
        };
        if batch.is_empty() {
            return;
        }
        let fencing_token = batch[0].fencing_token;

        // CAS retry loop: one manifest update carrying ALL batched refs.
        //
        // On terminal failure every batched fragment is an orphan — best-effort
        // delete each one's exact wal/ key (I3), never a prefix or a segment/
        // cluster object (carried cluster objects live under old segment keys,
        // Task 2B). Then reply Err to each waiter so no 200 is acked without a
        // covering manifest (I4).
        for attempt in 0..MAX_CAS_RETRIES {
            let (mut manifest, version) =
                match Manifest::read_versioned(&self.store, namespace).await {
                    Ok(Some(pair)) => pair,
                    Ok(None) => {
                        self.fail_batch(namespace, batch, |_| ZeppelinError::ManifestNotFound {
                            namespace: namespace.to_string(),
                        })
                        .await;
                        return;
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        self.fail_batch(namespace, batch, |_| {
                            ZeppelinError::Index(format!("manifest read failed: {msg}"))
                        })
                        .await;
                        return;
                    }
                };

            // Layer 1: Fencing check — reject zombie writers before committing.
            if let Some(token) = fencing_token {
                if manifest.fencing_token > token {
                    let mtoken = manifest.fencing_token;
                    self.fail_batch(namespace, batch, |_| ZeppelinError::FencingTokenStale {
                        namespace: namespace.to_string(),
                        our_token: token,
                        manifest_token: mtoken,
                    })
                    .await;
                    return;
                }
                manifest.fencing_token = token;
            }

            // Fold every batched fragment ref into this one manifest.
            for item in &batch {
                manifest.add_fragment(item.fref.clone());
            }

            // Layer 2: CAS — catches TOCTOU between fencing check and write.
            match manifest
                .write_conditional(&self.store, namespace, &version)
                .await
            {
                Ok(()) => {
                    debug!(
                        batched = batch.len(),
                        fragment_count = manifest.fragments.len(),
                        attempt,
                        "group commit updated manifest"
                    );
                    for item in batch {
                        let _ = item.reply.send(Ok(manifest.clone()));
                    }
                    return;
                }
                Err(ZeppelinError::ManifestConflict { .. }) => {
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
                    let msg = e.to_string();
                    self.fail_batch(namespace, batch, |_| {
                        ZeppelinError::Index(format!("manifest write failed: {msg}"))
                    })
                    .await;
                    return;
                }
            }
        }

        // Retries exhausted under sustained contention: the only terminal error
        // reachable here is a persistent CAS conflict (other errors return
        // early above). Orphan every batched fragment and fail all waiters 409.
        self.fail_batch(namespace, batch, |_| ZeppelinError::ManifestConflict {
            namespace: namespace.to_string(),
        })
        .await;
    }

    /// Clean up every batched fragment object (orphans, I3) and reply Err to
    /// each waiter. `make_err` produces a fresh error per waiter (errors aren't
    /// `Clone`-friendly across all variants).
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
        for item in batch {
            let err = make_err(&item);
            let _ = item.reply.send(Err(err));
        }
    }

    /// Best-effort delete of a fragment object that failed to be referenced by
    /// the manifest (I3). Never fatal — a delete failure just leaves an orphan
    /// for the GC sweep (Task 19); the append error is what the caller sees.
    /// Deletes exactly `key` (a `.../wal/<ulid>.wal` object), nothing else.
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

/// A write request queued for batched manifest flush.
struct WriteRequest {
    namespace: String,
    fragment: WalFragment,
    /// Serialized fragment size in bytes (known at PUT time).
    size_bytes: u64,
    reply: oneshot::Sender<Result<Manifest>>,
}

/// Batched WAL writer that buffers fragment refs and flushes manifests in bulk.
///
/// Fragments are written to S3 immediately (durability preserved).
/// Only the manifest update is batched — N fragment refs are collected
/// and written in a single CAS operation instead of N separate CAS ops,
/// flushing when the batch reaches `batch_size` or `timeout` expires.
///
/// **Default disabled**: `batch_manifest_size=1` gives per-append behavior.
pub struct BatchWalWriter {
    tx: mpsc::Sender<WriteRequest>,
}

impl BatchWalWriter {
    /// Create a new batch writer with the given parameters.
    ///
    /// Spawns a background flush task on the current tokio runtime.
    pub fn new(store: ZeppelinStore, batch_size: usize, timeout_ms: u64) -> Self {
        let (tx, rx) = mpsc::channel::<WriteRequest>(1024);

        tokio::spawn(flush_loop(store, rx, batch_size, timeout_ms));

        Self { tx }
    }

    /// Write a fragment to S3 and queue its ref for batched manifest update.
    ///
    /// The fragment is written to S3 immediately (durable).
    /// The manifest update is batched — the caller blocks until the batch flushes.
    ///
    /// Returns the updated manifest after flush.
    pub async fn append(
        &self,
        store: &ZeppelinStore,
        namespace: &str,
        fragment: WalFragment,
    ) -> Result<(WalFragment, Manifest)> {
        // Write fragment to S3 immediately (durability)
        let key = WalFragment::s3_key(namespace, &fragment.id);
        let data = fragment.to_bytes()?;
        let size_bytes = data.len() as u64;
        store.put(&key, data).await?;

        debug!(
            fragment_id = %fragment.id,
            vectors = fragment.vectors.len(),
            deletes = fragment.deletes.len(),
            "wrote WAL fragment (batched)"
        );

        // Queue for batched manifest update
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(WriteRequest {
                namespace: namespace.to_string(),
                fragment: fragment.clone(),
                size_bytes,
                reply: reply_tx,
            })
            .await
            .map_err(|_| ZeppelinError::Index("batch writer channel closed".to_string()))?;

        // Wait for manifest flush
        let manifest = reply_rx.await.map_err(|_| {
            ZeppelinError::Index("batch writer reply channel dropped".to_string())
        })??;

        Ok((fragment, manifest))
    }
}

/// Background flush loop that collects write requests and flushes in batches.
async fn flush_loop(
    store: ZeppelinStore,
    mut rx: mpsc::Receiver<WriteRequest>,
    batch_size: usize,
    timeout_ms: u64,
) {
    info!(batch_size, timeout_ms, "batch manifest flush loop started");

    loop {
        // Collect a batch
        let mut batch: Vec<WriteRequest> = Vec::with_capacity(batch_size);

        // Wait for at least one request
        match rx.recv().await {
            Some(req) => batch.push(req),
            None => {
                info!("batch writer channel closed, stopping flush loop");
                break;
            }
        }

        // Try to fill the batch up to batch_size, with a timeout
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_millis(timeout_ms);

        while batch.len() < batch_size {
            tokio::select! {
                req = rx.recv() => {
                    match req {
                        Some(r) => batch.push(r),
                        None => break,
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    break;
                }
            }
        }

        if batch.is_empty() {
            continue;
        }

        debug!(batch_len = batch.len(), "flushing manifest batch");

        // Group by namespace
        let mut by_namespace: std::collections::HashMap<String, Vec<WriteRequest>> =
            std::collections::HashMap::new();
        for req in batch {
            by_namespace
                .entry(req.namespace.clone())
                .or_default()
                .push(req);
        }

        // Flush each namespace
        for (namespace, requests) in by_namespace {
            let result = flush_namespace(&store, &namespace, &requests).await;
            match &result {
                Ok(manifest) => {
                    for req in requests {
                        let _ = req.reply.send(Ok(manifest.clone()));
                    }
                }
                Err(e) => {
                    warn!(namespace = %namespace, error = %e, "batch manifest flush failed");
                    // Preserve typed variants so the HTTP layer maps them
                    // correctly (409 for conflict, 404 for missing namespace)
                    // instead of collapsing everything to a 500.
                    for req in requests {
                        let err = match e {
                            ZeppelinError::ManifestConflict { namespace } => {
                                ZeppelinError::ManifestConflict {
                                    namespace: namespace.clone(),
                                }
                            }
                            ZeppelinError::ManifestNotFound { namespace } => {
                                ZeppelinError::ManifestNotFound {
                                    namespace: namespace.clone(),
                                }
                            }
                            other => ZeppelinError::Index(format!("batch flush failed: {other}")),
                        };
                        let _ = req.reply.send(Err(err));
                    }
                }
            }
        }
    }
}

/// Flush a batch of fragment refs for a single namespace.
async fn flush_namespace(
    store: &ZeppelinStore,
    namespace: &str,
    requests: &[WriteRequest],
) -> Result<Manifest> {
    for attempt in 0..MAX_CAS_RETRIES {
        // A missing manifest means the namespace was deleted (or never
        // created) — namespace creation always writes an initial manifest.
        // Falling back to Manifest::default() would unconditionally PUT and
        // resurrect a deleted namespace (NoZombieNamespace invariant, same
        // as WalWriter::append_with_lease).
        let (mut manifest, version) = match Manifest::read_versioned(store, namespace).await? {
            Some(pair) => pair,
            None => {
                return Err(ZeppelinError::ManifestNotFound {
                    namespace: namespace.to_string(),
                });
            }
        };

        for req in requests {
            manifest.add_fragment(FragmentRef {
                id: req.fragment.id,
                vector_count: req.fragment.vectors.len(),
                delete_count: req.fragment.deletes.len(),
                sequence_number: 0, // assigned by add_fragment
                size_bytes: req.size_bytes,
            });
        }

        match manifest.write_conditional(store, namespace, &version).await {
            Ok(()) => {
                debug!(
                    namespace = namespace,
                    fragments_flushed = requests.len(),
                    attempt,
                    "batched manifest flush complete"
                );
                return Ok(manifest);
            }
            Err(ZeppelinError::ManifestConflict { .. }) => {
                warn!(
                    attempt,
                    namespace, "manifest CAS conflict in batch flush, retrying"
                );
                continue;
            }
            Err(e) => return Err(e),
        }
    }

    Err(ZeppelinError::ManifestConflict {
        namespace: namespace.to_string(),
    })
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_remove_lock_nonexistent_is_noop() {
        let mem = std::sync::Arc::new(object_store::memory::InMemory::new());
        let store = crate::storage::ZeppelinStore::new(mem);
        let writer = WalWriter::new(store);

        // Should not panic
        writer.remove_lock("nonexistent");
        assert_eq!(writer.lock_count(), 0);
    }

    #[test]
    fn test_write_request_fields() {
        // Smoke test: WriteRequest struct can be constructed
        let frag = WalFragment::new(vec![], vec![]);
        let (tx, _rx) = oneshot::channel();
        let _req = WriteRequest {
            namespace: "test".to_string(),
            fragment: frag,
            size_bytes: 0,
            reply: tx,
        };
    }
}
