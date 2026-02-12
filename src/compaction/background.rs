use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::cache::manifest_cache::ManifestCache;
use crate::namespace::NamespaceManager;

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
pub fn start_compaction_thread(
    compactor: Arc<Compactor>,
    namespace_manager: Arc<NamespaceManager>,
    shutdown: tokio::sync::watch::Receiver<bool>,
    compaction_workers: usize,
    manifest_cache: Arc<ManifestCache>,
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
                    info!(namespace = %ns.name, "triggering compaction");
                    match compactor
                        .compact_with_fts(&ns.name, None, &ns.full_text_search)
                        .await
                    {
                        Ok(result) => {
                            crate::metrics::COMPACTIONS_TOTAL
                                .with_label_values(&[&ns.name, "success"])
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
                                .with_label_values(&[&ns.name, "failure"])
                                .inc();
                            warn!(namespace = %ns.name, error = %e, "compaction failed");
                        }
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
