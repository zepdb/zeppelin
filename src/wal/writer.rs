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

/// WAL writer with per-namespace mutexes to ensure single-writer semantics.
pub struct WalWriter {
    store: ZeppelinStore,
    /// Per-namespace locks to serialize writes within a namespace.
    locks: DashMap<String, Arc<Mutex<()>>>,
}

impl WalWriter {
    /// Create a new WAL writer backed by the given store.
    pub fn new(store: ZeppelinStore) -> Self {
        Self {
            store,
            locks: DashMap::new(),
        }
    }

    /// Get or create the per-namespace lock.
    fn namespace_lock(&self, namespace: &str) -> Arc<Mutex<()>> {
        self.locks
            .entry(namespace.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .value()
            .clone()
    }

    /// Remove the per-namespace lock entry. Called on namespace deletion.
    pub fn remove_lock(&self, namespace: &str) {
        self.locks.remove(namespace);
    }

    /// Return the number of namespace locks held (for testing).
    #[cfg(test)]
    pub fn lock_count(&self) -> usize {
        self.locks.len()
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
        let lock = self.namespace_lock(namespace);
        let _guard = lock.lock().await;

        crate::metrics::WAL_APPENDS_TOTAL
            .with_label_values(&[namespace])
            .inc();

        let fragment = WalFragment::try_new(vectors, deletes)?;

        // Write the fragment to S3
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

        // CAS retry loop for manifest update.
        //
        // On a non-conflict error, or after exhausting retries, the fragment
        // we PUT above is unreferenced by any manifest — an orphan. We
        // best-effort delete it (I3) so a failed append leaves no dangling
        // object; the manifest stays the source of truth for what exists.
        // Deletion targets ONLY this fragment's exact WAL key — never a prefix
        // and never a segment/cluster object (a carried cluster object is live
        // under an older segment's key; see incremental compaction / Task 2B).
        let mut last_err: Option<ZeppelinError> = None;
        for attempt in 0..MAX_CAS_RETRIES {
            let (mut manifest, version) =
                match Manifest::read_versioned(&self.store, namespace).await {
                    Ok(Some(pair)) => pair,
                    Ok(None) => {
                        self.cleanup_orphan_fragment(namespace, &key).await;
                        return Err(ZeppelinError::ManifestNotFound {
                            namespace: namespace.to_string(),
                        });
                    }
                    Err(e) => {
                        self.cleanup_orphan_fragment(namespace, &key).await;
                        return Err(e);
                    }
                };

            // Layer 1: Fencing check — reject zombie writers.
            if let Some(token) = fencing_token {
                if manifest.fencing_token > token {
                    self.cleanup_orphan_fragment(namespace, &key).await;
                    return Err(ZeppelinError::FencingTokenStale {
                        namespace: namespace.to_string(),
                        our_token: token,
                        manifest_token: manifest.fencing_token,
                    });
                }
                manifest.fencing_token = token;
            }

            manifest.add_fragment(FragmentRef {
                id: fragment.id,
                vector_count: fragment.vectors.len(),
                delete_count: fragment.deletes.len(),
                sequence_number: 0, // assigned by add_fragment
                size_bytes,
            });

            // Layer 2: CAS — catches TOCTOU gap between fencing check and write.
            match manifest
                .write_conditional(&self.store, namespace, &version)
                .await
            {
                Ok(()) => {
                    debug!(
                        fragment_count = manifest.fragments.len(),
                        attempt, "updated manifest"
                    );
                    return Ok((fragment, manifest));
                }
                Err(ZeppelinError::ManifestConflict { .. }) => {
                    warn!(
                        attempt,
                        namespace, "manifest CAS conflict in writer, retrying with backoff"
                    );
                    last_err = Some(ZeppelinError::ManifestConflict {
                        namespace: namespace.to_string(),
                    });
                    tokio::time::sleep(cas_backoff(attempt)).await;
                    continue;
                }
                Err(e) => {
                    self.cleanup_orphan_fragment(namespace, &key).await;
                    return Err(e);
                }
            }
        }

        // Retries exhausted under sustained contention: the fragment is an
        // orphan. Clean it up so a client retry does not duplicate data.
        self.cleanup_orphan_fragment(namespace, &key).await;
        Err(last_err.unwrap_or(ZeppelinError::ManifestConflict {
            namespace: namespace.to_string(),
        }))
    }

    /// Best-effort delete of a fragment object that failed to be referenced by
    /// the manifest (I3). Never fatal — a delete failure just leaves an orphan
    /// for the GC sweep (Task 19); the append error is what the caller sees.
    /// Deletes exactly `key` (a `.../wal/<ulid>.wal` object), nothing else.
    async fn cleanup_orphan_fragment(&self, namespace: &str, key: &str) {
        match self.store.delete(key).await {
            Ok(()) => {
                debug!(namespace, key, "cleaned up orphaned fragment after failed append");
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

        // Create locks by accessing namespace_lock
        let _lock1 = writer.namespace_lock("ns1");
        let _lock2 = writer.namespace_lock("ns2");
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
