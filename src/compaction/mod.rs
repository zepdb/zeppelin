pub mod background;

use std::collections::{HashMap, HashSet};

use tracing::{debug, info, instrument, warn};
use ulid::Ulid;

use crate::config::{CompactionConfig, IndexingConfig};
use crate::error::{Result, ZeppelinError};
use crate::fts::inverted_index::{fts_index_key, InvertedIndex};
use crate::fts::types::FtsFieldConfig;
use crate::index::hierarchical::build::build_hierarchical;
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
        self.compact_with_fts(namespace, fencing_token, &HashMap::new())
            .await
    }

    /// Compact with optional fencing token and FTS field configurations.
    #[instrument(skip(self, fts_configs), fields(namespace = namespace))]
    pub async fn compact_with_fts(
        &self,
        namespace: &str,
        fencing_token: Option<u64>,
        fts_configs: &HashMap<String, FtsFieldConfig>,
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
                let delete_futs: Vec<_> = manifest
                    .pending_deletes
                    .iter()
                    .map(|key| self.store.delete(key))
                    .collect();
                let delete_results = futures::future::join_all(delete_futs).await;
                for (key, result) in manifest.pending_deletes.iter().zip(delete_results) {
                    if let Err(e) = result {
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

        // 3. Read fragments using snapshot refs (not re-reading manifest).
        // Uses unchecked read — fragments were validated on write.
        let fragments = self
            .wal_reader
            .read_fragments_from_refs_unchecked(namespace, &fragment_refs)
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

        // 8. Build index (expensive, done once — NOT retried)
        // Choose hierarchical or flat based on config.
        let build_start = std::time::Instant::now();
        let (cluster_count, is_hierarchical, bitmap_fields) = if self.indexing_config.hierarchical {
            let h_index = build_hierarchical(
                &vectors,
                &self.indexing_config,
                &self.store,
                namespace,
                &segment_id,
            )
            .await?;
            let bf = h_index.bitmap_fields.clone();
            (h_index.num_leaf_clusters(), true, bf)
        } else {
            let index = build_ivf_flat(
                &vectors,
                &self.indexing_config,
                &self.store,
                namespace,
                &segment_id,
            )
            .await?;
            let bf = index.bitmap_fields.clone();
            (index.num_clusters(), false, bf)
        };
        let build_elapsed = build_start.elapsed();
        let index_type_label = if is_hierarchical {
            "hierarchical"
        } else {
            "ivf_flat"
        };
        crate::metrics::INDEX_BUILD_DURATION
            .with_label_values(&[namespace, index_type_label])
            .observe(build_elapsed.as_secs_f64());
        debug!(
            index_type = index_type_label,
            build_duration_ms = build_elapsed.as_millis() as u64,
            "index build phase complete"
        );

        // 8b. Build FTS inverted indexes (if FTS fields configured)
        let fts_fields: Vec<String> = if !fts_configs.is_empty() && self.indexing_config.fts_index {
            let fts_start = std::time::Instant::now();
            let mut fts_field_names = Vec::new();

            // Phase 1: Parallel reads of cluster attributes.
            let attr_keys: Vec<String> = (0..cluster_count)
                .map(|i| attrs_key(namespace, &segment_id, i))
                .collect();
            let read_futs: Vec<_> = attr_keys.iter().map(|k| self.store.get(k)).collect();
            let read_results = futures::future::join_all(read_futs).await;

            // Phase 2: CPU — build inverted indexes (parallelized via spawn_blocking).
            let fts_configs_clone = fts_configs.clone();
            let segment_id_clone = segment_id.clone();
            let namespace_clone = namespace.to_string();

            // Collect successful deserializations
            let mut cluster_data: Vec<(usize, bytes::Bytes)> = Vec::new();
            for (cluster_idx, result) in read_results.into_iter().enumerate() {
                match result {
                    Ok(data) => cluster_data.push((cluster_idx, data)),
                    Err(_) => continue,
                }
            }

            // Build inverted indexes in parallel using spawn_blocking
            let build_futs: Vec<_> = cluster_data
                .into_iter()
                .map(|(cluster_idx, data)| {
                    let configs = fts_configs_clone.clone();
                    let ns = namespace_clone.clone();
                    let seg = segment_id_clone.clone();
                    tokio::task::spawn_blocking(move || {
                        let cluster_attrs = deserialize_attrs(&data)?;
                        let attr_refs: Vec<
                            Option<&HashMap<String, crate::types::AttributeValue>>,
                        > = cluster_attrs.iter().map(|a| a.as_ref()).collect();
                        let inv_index = InvertedIndex::build(&attr_refs, &configs);
                        let field_names: Vec<String> =
                            inv_index.fields.keys().cloned().collect();
                        let fts_data = inv_index.to_bytes()?;
                        let fts_key = fts_index_key(&ns, &seg, cluster_idx);
                        Ok::<_, ZeppelinError>((fts_key, fts_data, field_names))
                    })
                })
                .collect();

            let build_results = futures::future::join_all(build_futs).await;
            let mut write_payloads = Vec::new();
            for result in build_results {
                let (fts_key, fts_data, field_names) = result
                    .map_err(|e| ZeppelinError::Index(format!("FTS build task failed: {e}")))?
                    ?;
                for name in field_names {
                    if !fts_field_names.contains(&name) {
                        fts_field_names.push(name);
                    }
                }
                write_payloads.push((fts_key, fts_data));
            }

            // Phase 3: Parallel writes of FTS indexes.
            let write_futs: Vec<_> = write_payloads
                .iter()
                .map(|(key, data)| self.store.put(key, data.clone()))
                .collect();
            let write_results = futures::future::join_all(write_futs).await;
            for result in write_results {
                result?;
            }

            let fts_elapsed = fts_start.elapsed();
            crate::metrics::FTS_INDEX_BUILD_DURATION
                .with_label_values(&[namespace])
                .observe(fts_elapsed.as_secs_f64());
            debug!(
                fts_fields = ?fts_field_names,
                fts_build_duration_ms = fts_elapsed.as_millis() as u64,
                clusters = cluster_count,
                "FTS inverted index build complete"
            );

            fts_field_names
        } else {
            Vec::new()
        };

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
                cluster_count,
                quantization: self.indexing_config.quantization,
                hierarchical: is_hierarchical,
                bitmap_fields: bitmap_fields.clone(),
                fts_fields: fts_fields.clone(),
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
///
/// Fetches all clusters in parallel (2 S3 GETs per cluster) for ~15%
/// compaction speedup vs sequential loading.
async fn load_segment_vectors(
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
) -> Result<Vec<VectorEntry>> {
    use crate::index::IvfFlatIndex;

    let index = IvfFlatIndex::load(store, namespace, segment_id).await?;
    let num_clusters = index.num_clusters();

    // Parallel fetch: 2 GETs per cluster via tokio::join!
    let cluster_results = futures::future::join_all((0..num_clusters).map(|i| {
        let cvec_key = cluster_key(namespace, segment_id, i);
        let cattr_key = attrs_key(namespace, segment_id, i);
        async move {
            let (cluster_res, attrs_res) =
                tokio::join!(store.get(&cvec_key), store.get(&cattr_key),);
            (i, cluster_res, attrs_res)
        }
    }))
    .await;

    // Sequential deserialization (CPU-bound, no I/O)
    let mut vectors = Vec::new();
    for (_i, cluster_res, attrs_res) in cluster_results {
        let cluster = deserialize_cluster(&cluster_res?)?;
        let attrs = match attrs_res {
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
