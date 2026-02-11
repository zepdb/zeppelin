use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, instrument, warn};

use crate::error::{Result, ZeppelinError};
use crate::storage::ZeppelinStore;
use crate::types::{VectorEntry, VectorId};

use super::fragment::WalFragment;
use super::manifest::{FragmentRef, Manifest, ManifestVersion};

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
    #[instrument(skip(self, vectors, deletes), fields(namespace = namespace))]
    pub async fn append(
        &self,
        namespace: &str,
        vectors: Vec<VectorEntry>,
        deletes: Vec<VectorId>,
    ) -> Result<WalFragment> {
        let lock = self.namespace_lock(namespace);
        let _guard = lock.lock().await;

        crate::metrics::WAL_APPENDS_TOTAL
            .with_label_values(&[namespace])
            .inc();

        let fragment = WalFragment::new(vectors, deletes);

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
                    None => (Manifest::default(), ManifestVersion(None)),
                };

            manifest.add_fragment(FragmentRef {
                id: fragment.id,
                vector_count: fragment.vectors.len(),
                delete_count: fragment.deletes.len(),
                sequence_number: 0, // assigned by add_fragment
            });

            match manifest
                .write_conditional(&self.store, namespace, &version)
                .await
            {
                Ok(()) => {
                    debug!(
                        fragment_count = manifest.fragments.len(),
                        attempt, "updated manifest"
                    );
                    return Ok(fragment);
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
