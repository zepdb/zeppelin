use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::cache::manifest_cache::ManifestCache;
use crate::error::ZeppelinError;
use crate::namespace::NamespaceManager;
use crate::wal::LeaseManager;

use super::Compactor;

/// Background compaction loop that periodically checks all namespaces
/// and compacts any that exceed the fragment threshold.
///
/// Runs on a dedicated tokio runtime to isolate compaction CPU from
/// query-serving threads. This prevents k-means training and FTS index
/// building from stealing CPU time during query processing.
///
/// `compaction_workers` controls the number of tokio worker threads for
/// the compaction runtime. Set via `CpuBudget::auto()` (typically CPUs - 1).
#[allow(clippy::expect_used)]
pub fn start_compaction_thread(
    compactor: Arc<Compactor>,
    namespace_manager: Arc<NamespaceManager>,
    shutdown: tokio::sync::watch::Receiver<bool>,
    compaction_workers: usize,
    manifest_cache: Arc<ManifestCache>,
    lease_manager: Arc<LeaseManager>,
) -> std::thread::JoinHandle<()> {
    info!(compaction_workers, "starting compaction runtime");
    std::thread::Builder::new()
        .name("compaction-runtime".to_string())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(compaction_workers)
                .thread_name("compaction-worker")
                .enable_all()
                .build()
                .expect("failed to build compaction runtime");

            rt.block_on(compaction_loop(
                compactor,
                namespace_manager,
                shutdown,
                manifest_cache,
                lease_manager,
            ));
        })
        .expect("failed to spawn compaction thread")
}

/// Background compaction loop (runs on the compaction runtime).
pub async fn compaction_loop(
    compactor: Arc<Compactor>,
    namespace_manager: Arc<NamespaceManager>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    manifest_cache: Arc<ManifestCache>,
    lease_manager: Arc<LeaseManager>,
) {
    info!(
        interval_secs = compactor.config().interval_secs,
        "background compaction loop started"
    );

    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(compactor.config().interval_secs)) => {},
            _ = shutdown.changed() => {
                info!("background compaction loop shutting down");
                break;
            }
        }

        let namespaces = match namespace_manager.list(None).await {
            Ok(ns) => ns,
            Err(e) => {
                warn!(error = %e, "failed to list namespaces for compaction");
                continue;
            }
        };

        debug!(namespace_count = namespaces.len(), "compaction loop tick");

        for ns in &namespaces {
            match compactor.should_compact(&ns.name).await {
                Ok(true) => {
                    // Acquire the per-namespace lease so only one node
                    // compacts a namespace at a time. LeaseHeld means another
                    // node is on it — skip quietly, it isn't a failure.
                    let lease = match lease_manager.acquire(&ns.name).await {
                        Ok(lease) => lease,
                        Err(ZeppelinError::LeaseHeld { holder, .. }) => {
                            debug!(
                                namespace = %ns.name,
                                holder = %holder,
                                "compaction lease held by another node, skipping"
                            );
                            continue;
                        }
                        Err(e) => {
                            warn!(namespace = %ns.name, error = %e, "failed to acquire compaction lease");
                            continue;
                        }
                    };

                    info!(
                        namespace = %ns.name,
                        fencing_token = lease.fencing_token,
                        "triggering compaction"
                    );
                    match compactor
                        .compact_with_fts(&ns.name, Some(lease.fencing_token), &ns.full_text_search)
                        .await
                    {
                        Ok(result) => {
                            crate::metrics::COMPACTIONS_TOTAL
                                .with_label_values(&[ns.name.as_str(), "success"])
                                .inc();
                            // Invalidate manifest cache so queries see new segment.
                            manifest_cache.invalidate(&ns.name);
                            info!(
                                namespace = %ns.name,
                                vectors_compacted = result.vectors_compacted,
                                fragments_removed = result.fragments_removed,
                                "compaction completed"
                            );
                        }
                        Err(e) => {
                            crate::metrics::COMPACTIONS_TOTAL
                                .with_label_values(&[ns.name.as_str(), "failure"])
                                .inc();
                            warn!(namespace = %ns.name, error = %e, "compaction failed");
                        }
                    }

                    // Best-effort release; never blocks the loop.
                    if let Err(e) = lease_manager.release(&ns.name, &lease).await {
                        warn!(namespace = %ns.name, error = %e, "lease release failed (best-effort)");
                    }
                }
                Ok(false) => {
                    debug!(namespace = %ns.name, "compaction not needed");
                }
                Err(e) => {
                    warn!(namespace = %ns.name, error = %e, "failed to check compaction status");
                }
            }
        }
    }
}
