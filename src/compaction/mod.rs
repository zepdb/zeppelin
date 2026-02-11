pub mod background;

use std::collections::{HashMap, HashSet};

use tracing::{debug, info, instrument, warn};
use ulid::Ulid;

use crate::config::{CompactionConfig, IndexingConfig};
use crate::error::{Result, ZeppelinError};
use crate::index::ivf_flat::build::{
    attrs_key, build_ivf_flat, cluster_key, deserialize_attrs, deserialize_cluster,
};
use crate::storage::ZeppelinStore;
use crate::types::VectorEntry;
use crate::wal::fragment::WalFragment;
use crate::wal::manifest::{Manifest, ManifestVersion, SegmentRef};
use crate::wal::WalReader;

/// Maximum CAS retry attempts for manifest updates.
const MAX_CAS_RETRIES: u32 = 5;

/// Result of a compaction run.
#[derive(Debug)]
pub struct CompactionResult {
    /// ID of the new segment, or None if no-op.
    pub segment_id: Option<String>,
    /// Number of vectors in the compacted segment.
    pub vectors_compacted: usize,
    /// Number of WAL fragments that were removed.
    pub fragments_removed: usize,
    /// ID of the old segment that was replaced, if any.
    pub old_segment_removed: Option<String>,
}

/// Compacts WAL fragments into IVF-Flat segments on S3.
pub struct Compactor {
    store: ZeppelinStore,
    wal_reader: WalReader,
    config: CompactionConfig,
    indexing_config: IndexingConfig,
}

impl Compactor {
    pub fn new(
        store: ZeppelinStore,
        wal_reader: WalReader,
        config: CompactionConfig,
        indexing_config: IndexingConfig,
    ) -> Self {
        Self {
            store,
            wal_reader,
            config,
            indexing_config,
        }
    }

    pub fn config(&self) -> &CompactionConfig {
        &self.config
    }

    /// Check whether compaction should be triggered for a namespace.
    #[instrument(skip(self), fields(namespace = namespace))]
    pub async fn should_compact(&self, namespace: &str) -> Result<bool> {
        let manifest = Manifest::read(&self.store, namespace)
            .await?
            .unwrap_or_default();
        let count = manifest.uncompacted_fragments().len();
        debug!(
            fragment_count = count,
            threshold = self.config.max_wal_fragments_before_compact,
            "checking compaction trigger"
        );
        Ok(count >= self.config.max_wal_fragments_before_compact)
    }

    /// Compact all uncompacted WAL fragments into a new IVF-Flat segment.
    ///
    /// Uses CAS (compare-and-swap) for manifest updates to prevent concurrent overwrites.
    /// Fragment deletion is deferred: keys are added to `pending_deletes` in the manifest
    /// and cleaned up at the start of the next compaction cycle.
    #[instrument(skip(self), fields(namespace = namespace))]
    pub async fn compact(&self, namespace: &str) -> Result<CompactionResult> {
        self.compact_with_lease(namespace, None).await
    }

    /// Compact with an optional fencing token from a lease.
    ///
    /// When `fencing_token` is `Some(token)`:
    /// - **Layer 1 (CheckFencing)**: Before each CAS write, checks
    ///   `manifest.fencing_token <= token`. If false → `FencingTokenStale`.
    /// - **Layer 2 (CAS)**: If the ETag changed, retries with re-check.
    ///
    /// When `fencing_token` is `None`: behaves identically to `compact()`.
    #[instrument(skip(self), fields(namespace = namespace))]
    pub async fn compact_with_lease(
        &self,
        namespace: &str,
        fencing_token: Option<u64>,
    ) -> Result<CompactionResult> {
        let start = std::time::Instant::now();

        // 0. GC: delete any pending_deletes from a previous compaction cycle
        {
            let manifest = Manifest::read(&self.store, namespace)
                .await?
                .unwrap_or_default();
            if !manifest.pending_deletes.is_empty() {
                debug!(
                    pending_count = manifest.pending_deletes.len(),
                    "cleaning up deferred deletes from previous compaction"
                );
                for key in &manifest.pending_deletes {
                    if let Err(e) = self.store.delete(key).await {
                        warn!(key = %key, error = %e, "failed to delete deferred key");
                    }
                }
            }
        }

        // 1. Read manifest to get fragment list (snapshot for segment building)
        let manifest = Manifest::read(&self.store, namespace)
            .await?
            .unwrap_or_default();

        // 2. If no uncompacted fragments → no-op
        if manifest.uncompacted_fragments().is_empty() {
            debug!("no uncompacted fragments, skipping");
            return Ok(CompactionResult {
                segment_id: None,
                vectors_compacted: 0,
                fragments_removed: 0,
                old_segment_removed: None,
            });
        }

        let fragment_refs = manifest.uncompacted_fragments().to_vec();
        let fragments_removed = fragment_refs.len();
        let last_fragment_id = fragment_refs.last().unwrap().id;

        info!(fragment_count = fragments_removed, "starting compaction");

        // 3. Read fragments using snapshot refs (not re-reading manifest)
        let fragments = self
            .wal_reader
            .read_fragments_from_refs(namespace, &fragment_refs)
            .await?;

        // 4. Merge vectors: process in manifest order (sequence number), latest wins
        let mut latest_vectors: HashMap<String, VectorEntry> = HashMap::new();
        let mut deleted_ids: HashSet<String> = HashSet::new();

        for fragment in &fragments {
            for del_id in &fragment.deletes {
                deleted_ids.insert(del_id.clone());
                latest_vectors.remove(del_id);
            }
            for vec in &fragment.vectors {
                deleted_ids.remove(&vec.id);
                latest_vectors.insert(vec.id.clone(), vec.clone());
            }
        }

        // 5. If existing active_segment: load vectors from it, merge
        let old_segment_id = manifest.active_segment.clone();
        if let Some(ref seg_id) = old_segment_id {
            let existing_vecs = load_segment_vectors(&self.store, namespace, seg_id).await?;
            for vec in existing_vecs {
                // WAL overrides: only insert if not already in latest_vectors and not deleted
                if !latest_vectors.contains_key(&vec.id) && !deleted_ids.contains(&vec.id) {
                    latest_vectors.insert(vec.id.clone(), vec);
                }
            }
        }

        // 6. Collect surviving vectors
        let vectors: Vec<VectorEntry> = latest_vectors.into_values().collect();
        let vectors_compacted = vectors.len();

        // Collect keys for deferred deletion
        let mut deferred_deletes: Vec<String> = Vec::new();
        for fref in &fragment_refs {
            deferred_deletes.push(WalFragment::s3_key(namespace, &fref.id));
        }
        if let Some(ref seg_id) = old_segment_id {
            let prefix = format!("{namespace}/segments/{seg_id}/");
            if let Ok(keys) = self.store.list_prefix(&prefix).await {
                deferred_deletes.extend(keys);
            }
        }

        if vectors.is_empty() {
            // Edge case: all vectors were deleted
            // CAS loop to update manifest
            for attempt in 0..MAX_CAS_RETRIES {
                let (mut fresh_manifest, version) =
                    match Manifest::read_versioned(&self.store, namespace).await? {
                        Some(pair) => pair,
                        None => (Manifest::default(), ManifestVersion(None)),
                    };

                // Layer 1: Fencing check.
                if let Some(token) = fencing_token {
                    if fresh_manifest.fencing_token > token {
                        return Err(ZeppelinError::FencingTokenStale {
                            namespace: namespace.to_string(),
                            our_token: token,
                            manifest_token: fresh_manifest.fencing_token,
                        });
                    }
                    fresh_manifest.fencing_token = token;
                }

                fresh_manifest.remove_compacted_fragments(last_fragment_id);
                fresh_manifest.pending_deletes = deferred_deletes.clone();

                // Layer 2: CAS.
                match fresh_manifest
                    .write_conditional(&self.store, namespace, &version)
                    .await
                {
                    Ok(()) => {
                        let elapsed = start.elapsed();
                        crate::metrics::COMPACTION_DURATION
                            .with_label_values(&[namespace])
                            .observe(elapsed.as_secs_f64());

                        info!(
                            elapsed_ms = elapsed.as_millis(),
                            attempt, "compaction complete (all vectors deleted)"
                        );
                        return Ok(CompactionResult {
                            segment_id: None,
                            vectors_compacted: 0,
                            fragments_removed,
                            old_segment_removed: old_segment_id,
                        });
                    }
                    Err(ZeppelinError::ManifestConflict { .. }) => {
                        warn!(
                            attempt,
                            "manifest CAS conflict in compactor (empty), retrying"
                        );
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
            return Err(ZeppelinError::ManifestConflict {
                namespace: namespace.to_string(),
            });
        }

        // 7. Generate new segment ID
        let segment_id = format!("seg_{}", Ulid::new());

        // 8. Build IVF-Flat index (expensive, done once — NOT retried)
        let index = build_ivf_flat(
            &vectors,
            &self.indexing_config,
            &self.store,
            namespace,
            &segment_id,
        )
        .await?;

        // 9. CAS loop: re-read manifest, apply changes, write conditionally
        for attempt in 0..MAX_CAS_RETRIES {
            let (mut fresh_manifest, version) =
                match Manifest::read_versioned(&self.store, namespace).await? {
                    Some(pair) => pair,
                    None => (Manifest::default(), ManifestVersion(None)),
                };

            // Layer 1: Fencing check.
            if let Some(token) = fencing_token {
                if fresh_manifest.fencing_token > token {
                    return Err(ZeppelinError::FencingTokenStale {
                        namespace: namespace.to_string(),
                        our_token: token,
                        manifest_token: fresh_manifest.fencing_token,
                    });
                }
                fresh_manifest.fencing_token = token;
            }

            fresh_manifest.add_segment(SegmentRef {
                id: segment_id.clone(),
                vector_count: vectors_compacted,
                cluster_count: index.num_clusters(),
            });
            fresh_manifest.remove_compacted_fragments(last_fragment_id);
            fresh_manifest.pending_deletes = deferred_deletes.clone();

            // Layer 2: CAS.
            match fresh_manifest
                .write_conditional(&self.store, namespace, &version)
                .await
            {
                Ok(()) => {
                    let elapsed = start.elapsed();
                    crate::metrics::COMPACTION_DURATION
                        .with_label_values(&[namespace])
                        .observe(elapsed.as_secs_f64());

                    info!(
                        segment_id = %segment_id,
                        vectors_compacted,
                        fragments_removed,
                        elapsed_ms = elapsed.as_millis(),
                        attempt,
                        "compaction complete"
                    );

                    return Ok(CompactionResult {
                        segment_id: Some(segment_id),
                        vectors_compacted,
                        fragments_removed,
                        old_segment_removed: old_segment_id,
                    });
                }
                Err(ZeppelinError::ManifestConflict { .. }) => {
                    warn!(attempt, "manifest CAS conflict in compactor, retrying");
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

/// Load all vectors from an existing IVF-Flat segment on S3.
async fn load_segment_vectors(
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
) -> Result<Vec<VectorEntry>> {
    use crate::index::IvfFlatIndex;

    let index = IvfFlatIndex::load(store, namespace, segment_id).await?;
    let num_clusters = index.num_clusters();
    let mut vectors = Vec::new();

    for i in 0..num_clusters {
        let cvec_key = cluster_key(namespace, segment_id, i);
        let cluster_data = store.get(&cvec_key).await?;
        let cluster = deserialize_cluster(&cluster_data)?;

        let cattr_key = attrs_key(namespace, segment_id, i);
        let attrs = match store.get(&cattr_key).await {
            Ok(data) => deserialize_attrs(&data)?,
            Err(_) => vec![None; cluster.ids.len()],
        };

        for (j, id) in cluster.ids.into_iter().enumerate() {
            vectors.push(VectorEntry {
                id,
                values: cluster.vectors[j].clone(),
                attributes: attrs.get(j).cloned().flatten(),
            });
        }
    }

    debug!(
        segment_id = segment_id,
        vectors_loaded = vectors.len(),
        "loaded vectors from existing segment"
    );

    Ok(vectors)
}
