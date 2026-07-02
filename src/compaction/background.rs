use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tracing::{debug, error, info, warn};

use crate::cache::manifest_cache::ManifestCache;
use crate::cache::DiskCache;
use crate::error::{Result, ZeppelinError};
use crate::fts::FtsFieldConfig;
use crate::namespace::NamespaceManager;
use crate::storage::ZeppelinStore;
use crate::wal::Lease;
use crate::wal::LeaseManager;
use crate::wal::Manifest;

use super::{CompactionResult, Compactor};

/// Lease-renewal heartbeat for a running compaction (Task 2 Phase A).
///
/// Renews the namespace lease at `lease_duration / 3` intervals so a
/// compaction that outlasts the lease duration keeps its lease (invariant
/// A1). Renewal goes through `LeaseManager::renew` — the existing CAS on
/// the lease object; the fencing token is never bumped, only the expiry
/// extends.
///
/// If a renewal fails because the lease was taken over (`LeaseExpired`),
/// or the last successfully-renewed expiry has passed (we can no longer
/// PROVE we hold the lease), the heartbeat flips `lease_lost` and stops.
/// The compactor checks that flag before every manifest CAS attempt and
/// aborts (invariant A2). Transient storage errors are retried at the next
/// tick as long as the wall-clock expiry has not passed.
struct LeaseHeartbeat {
    lease_lost: Arc<AtomicBool>,
    handle: tokio::task::JoinHandle<()>,
}

impl LeaseHeartbeat {
    fn spawn(lease_manager: Arc<LeaseManager>, namespace: String, lease: Lease) -> Self {
        let lease_lost = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&lease_lost);
        // Renew at <= lease_duration / 3: two consecutive renewal attempts
        // still fit inside the remaining lease window, so a single missed
        // tick (transient S3 error) does not lose the lease.
        let interval = lease_manager.lease_duration() / 3;

        let handle = tokio::spawn(async move {
            let mut current = lease;
            loop {
                tokio::time::sleep(interval).await;

                match lease_manager.renew(&namespace, &current).await {
                    Ok(renewed) => {
                        crate::metrics::COMPACTION_LEASE_RENEWALS_TOTAL
                            .with_label_values(&[namespace.as_str()])
                            .inc();
                        debug!(
                            namespace = %namespace,
                            fencing_token = renewed.fencing_token,
                            expires_at = %renewed.expires_at,
                            "compaction lease renewed (heartbeat)"
                        );
                        current = renewed;
                    }
                    Err(ZeppelinError::LeaseExpired { .. }) => {
                        // Lease stolen / expired-and-taken: signal abort.
                        crate::metrics::COMPACTION_LEASE_LOST_TOTAL
                            .with_label_values(&[namespace.as_str()])
                            .inc();
                        error!(
                            namespace = %namespace,
                            fencing_token = current.fencing_token,
                            "compaction lease lost mid-flight (taken over by another node); \
                             signaling compaction abort"
                        );
                        flag.store(true, Ordering::SeqCst);
                        return;
                    }
                    Err(e) => {
                        // Transient renewal failure. If our last confirmed
                        // expiry has passed we can no longer prove we hold
                        // the lease — treat as lost (fail loud). Otherwise
                        // retry at the next tick.
                        if current.expires_at <= Utc::now() {
                            crate::metrics::COMPACTION_LEASE_LOST_TOTAL
                                .with_label_values(&[namespace.as_str()])
                                .inc();
                            error!(
                                namespace = %namespace,
                                error = %e,
                                expires_at = %current.expires_at,
                                "compaction lease renewal failed past expiry; \
                                 signaling compaction abort"
                            );
                            flag.store(true, Ordering::SeqCst);
                            return;
                        }
                        warn!(
                            namespace = %namespace,
                            error = %e,
                            "compaction lease renewal failed (transient); retrying next tick"
                        );
                    }
                }
            }
        });

        Self { lease_lost, handle }
    }

    /// Stop the heartbeat. The compaction is done (committed or aborted);
    /// the lease no longer needs extending.
    fn stop(self) {
        self.handle.abort();
    }
}

/// Run one leased compaction cycle for a namespace.
///
/// This is the single production entry point for lease-protected
/// compaction:
/// 1. acquire the per-namespace lease,
/// 2. start the lease-renewal heartbeat (invariant A1),
/// 3. compact with the fencing token and the heartbeat's abort signal
///    (invariant A2: renewal failure aborts before the final manifest CAS),
/// 4. stop the heartbeat and release the lease (best-effort — never an
///    error, and never deletes the lease object).
///
/// `LeaseHeld` from the acquire step propagates so the caller can skip
/// quietly (another node is compacting this namespace).
pub async fn compact_namespace_under_lease(
    compactor: &Compactor,
    lease_manager: &Arc<LeaseManager>,
    namespace: &str,
    fts_configs: &HashMap<String, FtsFieldConfig>,
) -> Result<CompactionResult> {
    let lease = lease_manager.acquire(namespace).await?;
    info!(
        namespace = %namespace,
        fencing_token = lease.fencing_token,
        lease_expires_at = %lease.expires_at,
        "compaction lease acquired, starting compaction"
    );

    let heartbeat = LeaseHeartbeat::spawn(
        Arc::clone(lease_manager),
        namespace.to_string(),
        lease.clone(),
    );

    let result = compactor
        .compact_with_fts_signaled(
            namespace,
            Some(lease.fencing_token),
            fts_configs,
            Some(Arc::clone(&heartbeat.lease_lost)),
        )
        .await;

    heartbeat.stop();

    // Best-effort release; never blocks or fails the cycle. If the lease
    // was taken over, release() detects the holder/token mismatch and
    // returns Ok without touching the thief's lease.
    if let Err(e) = lease_manager.release(namespace, &lease).await {
        warn!(namespace = %namespace, error = %e, "lease release failed (best-effort)");
    }

    result
}

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
                    // Compact under the per-namespace lease (acquire →
                    // heartbeat → compact → release). LeaseHeld means
                    // another node is on it — skip quietly, not a failure.
                    match compact_namespace_under_lease(
                        &compactor,
                        &lease_manager,
                        &ns.name,
                        &ns.full_text_search,
                    )
                    .await
                    {
                        Err(ZeppelinError::LeaseHeld { holder, .. }) => {
                            debug!(
                                namespace = %ns.name,
                                holder = %holder,
                                "compaction lease held by another node, skipping"
                            );
                        }
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
