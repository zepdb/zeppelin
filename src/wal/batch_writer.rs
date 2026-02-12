//! Batched WAL writer that buffers fragment refs and flushes manifests in bulk.
//!
//! Fragments are written to S3 immediately (durability preserved).
//! Only the manifest update is batched — N fragment refs are collected
//! and written in a single CAS operation instead of N separate CAS ops.
//!
//! **Default disabled**: `batch_manifest_size=1` gives per-append behavior.

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use crate::error::{Result, ZeppelinError};
use crate::storage::ZeppelinStore;
use crate::wal::fragment::WalFragment;
use crate::wal::manifest::{FragmentRef, Manifest, ManifestVersion};

/// Maximum CAS retry attempts for batched manifest updates.
const MAX_CAS_RETRIES: u32 = 5;

/// A write request queued for batched manifest flush.
struct WriteRequest {
    namespace: String,
    fragment: WalFragment,
    reply: oneshot::Sender<Result<Manifest>>,
}

/// Batched WAL writer.
///
/// Fragments are written to S3 immediately via the caller.
/// Fragment refs are buffered in a channel and flushed to the manifest
/// when the batch reaches `batch_size` or `timeout` expires.
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
                reply: reply_tx,
            })
            .await
            .map_err(|_| {
                ZeppelinError::Index("batch writer channel closed".to_string())
            })?;

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
        let deadline = tokio::time::Instant::now()
            + tokio::time::Duration::from_millis(timeout_ms);

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
                    for req in requests {
                        let _ = req.reply.send(Err(ZeppelinError::Index(format!(
                            "batch flush failed: {e}"
                        ))));
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
        let (mut manifest, version) =
            match Manifest::read_versioned(store, namespace).await? {
                Some(pair) => pair,
                None => (Manifest::default(), ManifestVersion(None)),
            };

        for req in requests {
            manifest.add_fragment(FragmentRef {
                id: req.fragment.id,
                vector_count: req.fragment.vectors.len(),
                delete_count: req.fragment.deletes.len(),
                sequence_number: 0, // assigned by add_fragment
            });
        }

        match manifest
            .write_conditional(store, namespace, &version)
            .await
        {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_request_fields() {
        // Smoke test: WriteRequest struct can be constructed
        let frag = WalFragment::new(vec![], vec![]);
        let (tx, _rx) = oneshot::channel();
        let _req = WriteRequest {
            namespace: "test".to_string(),
            fragment: frag,
            reply: tx,
        };
    }
}
