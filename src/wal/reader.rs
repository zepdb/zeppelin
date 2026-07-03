use std::collections::HashSet;

use tracing::{debug, instrument, warn};
use ulid::Ulid;

use crate::error::{Result, ZeppelinError};
use crate::storage::ZeppelinStore;

use super::fragment::WalFragment;
use super::manifest::{FragmentRef, Manifest};

/// WAL reader for listing and reading uncompacted fragments.
pub struct WalReader {
    store: ZeppelinStore,
}

impl WalReader {
    /// Create a new WAL reader backed by the given store.
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

    /// Read all uncompacted fragments for a namespace, in manifest order.
    /// Manifest order reflects sequence number assignment (monotonic), which is
    /// immune to clock skew — unlike ULID ordering.
    #[instrument(skip(self), fields(namespace = namespace))]
    pub async fn read_uncompacted_fragments(&self, namespace: &str) -> Result<Vec<WalFragment>> {
        let manifest = Manifest::read(&self.store, namespace).await?;
        let manifest = match manifest {
            Some(m) => m,
            None => return Ok(Vec::new()),
        };

        let refs = manifest.uncompacted_fragments().to_vec();
        self.read_fragments_from_refs(namespace, &refs).await
    }

    /// Read specific fragments by their refs, preserving the caller's ordering.
    /// Skips NotFound only when a fresh manifest confirms compaction removed
    /// that fragment ref.
    #[instrument(skip(self, refs), fields(namespace = namespace, ref_count = refs.len()))]
    pub async fn read_fragments_from_refs(
        &self,
        namespace: &str,
        refs: &[FragmentRef],
    ) -> Result<Vec<WalFragment>> {
        // Parallel prefetch all fragments concurrently.
        let results = futures::future::join_all(
            refs.iter()
                .map(|fref| self.read_fragment(namespace, &fref.id)),
        )
        .await;

        let fragments = self
            .finish_fragment_results(namespace, refs, results)
            .await?;

        debug!(fragment_count = fragments.len(), "read fragments from refs");

        Ok(fragments)
    }

    /// Read a specific WAL fragment by its ULID without checksum validation.
    ///
    /// Only use for fragments already validated on write (compaction reads).
    #[instrument(skip(self), fields(namespace = namespace, fragment_id = %fragment_id))]
    pub async fn read_fragment_unchecked(
        &self,
        namespace: &str,
        fragment_id: &Ulid,
    ) -> Result<WalFragment> {
        let key = WalFragment::s3_key(namespace, fragment_id);
        let data = self.store.get(&key).await?;
        WalFragment::from_bytes_unchecked(&data)
    }

    /// Read specific fragments by their refs without checksum validation.
    ///
    /// Same as `read_fragments_from_refs()` but skips checksum validation
    /// for fragments already validated on write. Used by compaction.
    #[instrument(skip(self, refs), fields(namespace = namespace, ref_count = refs.len()))]
    pub async fn read_fragments_from_refs_unchecked(
        &self,
        namespace: &str,
        refs: &[FragmentRef],
    ) -> Result<Vec<WalFragment>> {
        let results = futures::future::join_all(
            refs.iter()
                .map(|fref| self.read_fragment_unchecked(namespace, &fref.id)),
        )
        .await;

        let fragments = self
            .finish_fragment_results(namespace, refs, results)
            .await?;

        debug!(
            fragment_count = fragments.len(),
            "read fragments from refs (unchecked)"
        );

        Ok(fragments)
    }

    async fn finish_fragment_results(
        &self,
        namespace: &str,
        refs: &[FragmentRef],
        results: Vec<Result<WalFragment>>,
    ) -> Result<Vec<WalFragment>> {
        let mut fragments = Vec::new();
        let mut missing = Vec::new();

        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(fragment) => fragments.push(fragment),
                Err(ZeppelinError::NotFound { key }) => {
                    missing.push((refs[i].id, key));
                }
                Err(e) => return Err(e),
            }
        }

        if missing.is_empty() {
            return Ok(fragments);
        }

        let fresh_manifest = Manifest::read(&self.store, namespace).await?;
        let Some(fresh_manifest) = fresh_manifest else {
            let (_, key) = missing.remove(0);
            return Err(ZeppelinError::NotFound { key });
        };
        let live_fragment_ids: HashSet<Ulid> = fresh_manifest
            .uncompacted_fragments()
            .iter()
            .map(|fref| fref.id)
            .collect();

        for (fragment_id, key) in &missing {
            if live_fragment_ids.contains(fragment_id) {
                return Err(ZeppelinError::NotFound { key: key.clone() });
            }
        }

        for (fragment_id, key) in missing {
            warn!(
                namespace = %namespace,
                fragment_id = %fragment_id,
                key = %key,
                "WAL fragment not found; fresh manifest no longer references it, treating as compaction GC race"
            );
            crate::metrics::WAL_FRAGMENT_GC_RACE_SKIPPED_TOTAL
                .with_label_values(&[namespace])
                .inc();
        }

        Ok(fragments)
    }
}
