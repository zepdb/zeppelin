use tracing::{debug, instrument};
use ulid::Ulid;

use crate::error::Result;
use crate::storage::ZeppelinStore;

use super::fragment::WalFragment;
use super::manifest::Manifest;

/// WAL reader for listing and reading uncompacted fragments.
pub struct WalReader {
    store: ZeppelinStore,
}

impl WalReader {
    pub fn new(store: ZeppelinStore) -> Self {
        Self { store }
    }

    /// List all WAL fragment keys for a namespace.
    #[instrument(skip(self), fields(namespace = namespace))]
    pub async fn list_fragment_keys(&self, namespace: &str) -> Result<Vec<String>> {
        let prefix = format!("{namespace}/wal/");
        let keys = self.store.list_prefix(&prefix).await?;
        Ok(keys.into_iter().filter(|k| k.ends_with(".wal")).collect())
    }

    /// Read a specific WAL fragment by its ULID.
    #[instrument(skip(self), fields(namespace = namespace, fragment_id = %fragment_id))]
    pub async fn read_fragment(&self, namespace: &str, fragment_id: &Ulid) -> Result<WalFragment> {
        let key = WalFragment::s3_key(namespace, fragment_id);
        let data = self.store.get(&key).await?;
        WalFragment::from_bytes(&data)
    }

    /// Read all uncompacted fragments for a namespace, in ULID order.
    #[instrument(skip(self), fields(namespace = namespace))]
    pub async fn read_uncompacted_fragments(&self, namespace: &str) -> Result<Vec<WalFragment>> {
        let manifest = Manifest::read(&self.store, namespace).await?;
        let manifest = match manifest {
            Some(m) => m,
            None => return Ok(Vec::new()),
        };

        let mut fragments = Vec::new();
        for fref in manifest.uncompacted_fragments() {
            let fragment = self.read_fragment(namespace, &fref.id).await?;
            fragments.push(fragment);
        }

        // Fragments should already be in order since we append in order,
        // but sort by ULID to be safe.
        fragments.sort_by_key(|f| f.id);

        debug!(
            fragment_count = fragments.len(),
            "read uncompacted fragments"
        );

        Ok(fragments)
    }
}
