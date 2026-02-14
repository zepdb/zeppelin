use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, instrument, warn};

use crate::error::{Result, ZeppelinError};
use crate::storage::ZeppelinStore;
use crate::types::{VectorEntry, VectorId};

use super::fragment::WalFragment;
use super::manifest::{FragmentRef, Manifest};

/// Maximum CAS retry attempts for manifest updates.
const MAX_CAS_RETRIES: u32 = 5;

/// WAL writer with per-namespace mutexes to ensure single-writer semantics.
pub struct WalWriter {
    store: ZeppelinStore,
    /// Per-namespace locks to serialize writes within a namespace.
    locks: DashMap<String, Arc<Mutex<()>>>,
}

impl WalWriter {
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
        self.store.put(&key, data).await?;

        debug!(
            fragment_id = %fragment.id,
            vectors = fragment.vectors.len(),
            deletes = fragment.deletes.len(),
            "wrote WAL fragment"
        );

        // CAS retry loop for manifest update
        for attempt in 0..MAX_CAS_RETRIES {
            let (mut manifest, version) =
                match Manifest::read_versioned(&self.store, namespace).await? {
                    Some(pair) => pair,
                    None => {
                        return Err(ZeppelinError::ManifestNotFound {
                            namespace: namespace.to_string(),
                        });
                    }
                };

            // Layer 1: Fencing check — reject zombie writers.
            if let Some(token) = fencing_token {
                if manifest.fencing_token > token {
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
                        namespace, "manifest CAS conflict in writer, retrying"
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
}
