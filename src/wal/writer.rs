use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, instrument};

use crate::error::Result;
use crate::storage::ZeppelinStore;
use crate::types::{VectorEntry, VectorId};

use super::fragment::WalFragment;
use super::manifest::{FragmentRef, Manifest};

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

        // Update the manifest
        let mut manifest = Manifest::read(&self.store, namespace)
            .await?
            .unwrap_or_default();

        manifest.add_fragment(FragmentRef {
            id: fragment.id,
            vector_count: fragment.vectors.len(),
            delete_count: fragment.deletes.len(),
        });

        manifest.write(&self.store, namespace).await?;

        debug!(
            fragment_count = manifest.fragments.len(),
            "updated manifest"
        );

        Ok(fragment)
    }
}
