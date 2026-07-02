use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::cache::manifest_cache::ManifestCache;
use crate::cache::DiskCache;
use crate::error::ZeppelinError;
use crate::namespace::NamespaceManager;
use crate::storage::ZeppelinStore;
use crate::wal::LeaseManager;
use crate::wal::Manifest;

use super::Compactor;

/// Eagerly warm the active segment's index metadata (IVF-Flat centroids or
/// hierarchical tree_meta.json) into the tiered cache and pin it.
///
/// Runs as a spawned background task after a successful compaction. Reads
/// the manifest fresh from S3 (source of truth) to discover the active
/// segment — the compaction that scheduled this task just committed it.
///
/// Best-effort by design (invariant I5): failures are logged at WARN and
/// swallowed HERE ONLY — the query path keeps its own fail-loud fetch, so a
/// missed warm just means the first query pays the cold GET.
async fn warm_segment_index_meta(store: ZeppelinStore, cache: Arc<DiskCache>, namespace: String) {
    let result = async {
        let manifest = Manifest::read(&store, &namespace)
            .await?
            .unwrap_or_default();
        let seg_ref = manifest.active_segment.as_ref().and_then(|segment_id| {
            manifest
                .segments
                .iter()
                .find(|s| s.id == *segment_id)
                .cloned()
        });
        let Some(seg_ref) = seg_ref else {
            return Ok::<Option<String>, ZeppelinError>(None);
        };
        let key = if seg_ref.hierarchical {
            crate::index::hierarchical::tree_meta_key(&namespace, &seg_ref.id)
        } else {
            crate::index::ivf_flat::build::centroids_key(&namespace, &seg_ref.id)
        };
        cache.get_or_fetch(&key, || store.get(&key)).await?;
        cache.pin_scoped(&namespace, &key).await;
        Ok(Some(key))
    }
    .await;

    match result {
        Ok(Some(key)) => {
            debug!(namespace = %namespace, key = %key, "warmed segment index metadata post-compaction");
        }
        Ok(None) => {
            debug!(namespace = %namespace, "no active segment to warm post-compaction");
        }
        Err(e) => {
            warn!(
                namespace = %namespace,
                error = %e,
                "post-compaction cache warming failed (non-fatal — first query pays the cold fetch)"
            );
        }
    }
}

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
    cache: Arc<DiskCache>,
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
                cache,
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
    cache: Arc<DiskCache>,
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
                            // Warm the new segment's index metadata (centroids /
                            // tree_meta) into the cache eagerly, so the first
                            // query after compaction doesn't pay the cold fetch.
                            // Background + best-effort: warming is an
                            // optimization; its failure must never affect the
                            // compaction loop (queries keep fail-loud fetches).
                            if result.segment_id.is_some() {
                                tokio::spawn(warm_segment_index_meta(
                                    compactor.store().clone(),
                                    cache.clone(),
                                    ns.name.clone(),
                                ));
                            }
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
