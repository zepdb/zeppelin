use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::namespace::NamespaceManager;

use super::Compactor;

/// Background compaction loop that periodically checks all namespaces
/// and compacts any that exceed the fragment threshold.
pub async fn compaction_loop(
    compactor: Arc<Compactor>,
    namespace_manager: Arc<NamespaceManager>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
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
