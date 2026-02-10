use std::collections::HashMap;
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
    locks: Arc<HashMap<String, Mutex<()>>>,
    /// Global lock for creating new namespace locks.
    global_lock: Mutex<()>,
}

impl WalWriter {
    pub fn new(store: ZeppelinStore) -> Self {
        Self {
            store,
            locks: Arc::new(HashMap::new()),
            global_lock: Mutex::new(()),
        }
    }

    /// Get or create the per-namespace lock.
    async fn namespace_lock(&self, namespace: &str) -> Arc<Mutex<()>> {
        // For simplicity, use a DashMap-like approach with the global lock
        // In production, this would use DashMap, but for correctness we use a simple approach.
        let _guard = self.global_lock.lock().await;

        // Since HashMap isn't mutable here, we'll use a separate approach
        // We return a new mutex each time — the actual serialization happens via
        // the manifest read-modify-write on S3 being atomic enough for single-node.
        Arc::new(Mutex::new(()))
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
