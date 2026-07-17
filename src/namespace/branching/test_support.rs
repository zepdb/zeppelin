//! Feature-gated synthetic foreign-origin fixtures for external integration tests.
//!
//! This module deliberately exposes no publication or HTTP admission path. It
//! creates one target-bound manifest only in memory and hands that snapshot to
//! the same supplied-manifest query function used by production batch and
//! historical execution. Persisting the bytes still passes through normal
//! manifest admission and is rejected until lineage authorization lands.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use crate::cache::hydration::HydrationTarget;
use crate::cache::DiskCache;
use crate::config::DEFAULT_RERANK_COALESCE_GAP_BYTES;
use crate::error::{Result, ZeppelinError};
use crate::fts::rank_by::RankBy;
use crate::fts::FtsFieldConfig;
use crate::query::{
    execute_bm25_query_with_manifest, execute_bm25_query_with_manifest_debug,
    execute_query_with_manifest, execute_query_with_manifest_debug, QueryParams,
};
use crate::storage::ZeppelinStore;
use crate::types::{
    AttributeValue, ConsistencyLevel, DistanceMetric, Filter, SearchResult, VectorId,
};
use crate::wal::manifest::Manifest;
use crate::wal::WalReader;

use super::{ArtifactOrigin, ArtifactOriginIndex};
use crate::namespace::{NamespaceId, NamespaceIncarnationId};

/// Observable result of one ANN query through a synthetic target manifest.
#[derive(Debug, Clone)]
pub struct SyntheticForeignQueryResult {
    /// Ranked vector IDs returned by the production ANN merge path.
    pub ids: Vec<String>,
    /// Complete ranked hits, including projected attributes when requested.
    pub results: Vec<SearchResult>,
    /// Exact immutable object keys traversed for receipt construction.
    pub touched_artifact_keys: Vec<String>,
    /// Number of visible WAL fragments scored by the query.
    pub scanned_fragments: usize,
    /// Number of active immutable segments searched by the query.
    pub scanned_segments: usize,
    /// Whether the production diagnostic path emitted a debug block.
    pub debug_present: bool,
    /// Immutable-cache hits observed inside the diagnostic scope.
    pub cache_hits: u64,
    /// Immutable-cache misses observed inside the diagnostic scope.
    pub cache_misses: u64,
}

/// One exact by-ID record projected through the production lookup seam.
#[derive(Debug, Clone, PartialEq)]
pub struct SyntheticForeignFetchRecord {
    /// Stable logical vector ID.
    pub id: VectorId,
    /// Full coordinates when requested by the fixture projection.
    pub values: Option<Vec<f32>>,
    /// Complete or field-projected attributes when requested.
    pub attributes: Option<HashMap<String, AttributeValue>>,
}

/// Observable exact-fetch result for one synthetic target view.
#[derive(Debug, Clone, PartialEq)]
pub struct SyntheticForeignFetchResult {
    /// Live projected records in request-relative order.
    pub records: Vec<SyntheticForeignFetchRecord>,
    /// Absent or tombstoned IDs in request-relative order.
    pub missing: Vec<VectorId>,
    /// Exact immutable segment keys consumed by lookup.
    pub touched_artifact_keys: Vec<String>,
}

/// One positional query request executed against a shared synthetic manifest.
#[derive(Debug, Clone)]
pub enum SyntheticForeignQuerySpec {
    /// Approximate-neighbor retrieval.
    Ann {
        /// Query coordinates.
        query: Vec<f32>,
        /// Maximum returned candidates.
        top_k: usize,
        /// IVF or hierarchical probe width.
        nprobe: usize,
    },
    /// Full-text retrieval.
    Bm25 {
        /// Lexical scoring expression.
        rank_by: RankBy,
        /// Maximum returned candidates.
        top_k: usize,
    },
    /// ANN plus BM25 reduced by the production default RRF implementation.
    Hybrid {
        /// ANN query coordinates.
        query: Vec<f32>,
        /// Lexical scoring expression.
        rank_by: RankBy,
        /// Maximum fused candidates.
        top_k: usize,
        /// IVF or hierarchical probe width.
        nprobe: usize,
    },
}

/// Opaque target-bound view whose descriptors point at one source lifetime.
///
/// The view is never written to object storage. Callers can execute the
/// production supplied-manifest query seam and can independently confirm that
/// normal persisted-manifest decoding remains fail-closed.
#[derive(Clone)]
pub struct SyntheticForeignOriginView {
    store: ZeppelinStore,
    target_namespace: String,
    target_origin: ArtifactOrigin,
    manifest: Manifest,
}

impl SyntheticForeignOriginView {
    /// Load a source manifest and build an unpublished target-bound view.
    ///
    /// Every fragment and segment descriptor is assigned the source's physical
    /// origin. No source object is copied and no target manifest is published.
    pub async fn from_source(
        store: ZeppelinStore,
        source_namespace: &str,
        target_namespace: &str,
    ) -> Result<Self> {
        let source_manifest = Manifest::read(&store, source_namespace)
            .await?
            .ok_or_else(|| ZeppelinError::ManifestNotFound {
                namespace: source_namespace.to_string(),
            })?;
        let source_origin = source_manifest.local_origin()?;

        let target_namespace_id =
            NamespaceId::parse(target_namespace.to_string()).map_err(|_| {
                ZeppelinError::Validation(format!(
                    "invalid synthetic target namespace: {target_namespace}"
                ))
            })?;
        let (target_origin, target_incarnation, target_base, local_tail) =
            match Manifest::read(&store, target_namespace).await? {
                Some(mut existing) => {
                    if !existing.segments.is_empty() || existing.active_segment.is_some() {
                        return Err(ZeppelinError::Validation(
                            "synthetic target may contain local WAL but not a local segment"
                                .to_string(),
                        ));
                    }
                    let origin = existing.local_origin()?;
                    let incarnation = origin.incarnation.as_uuid();
                    let local_tail = std::mem::take(&mut existing.fragments);
                    (origin, incarnation, existing, local_tail)
                }
                None => {
                    let incarnation = uuid::Uuid::new_v4();
                    let origin = ArtifactOrigin {
                        namespace: target_namespace_id,
                        incarnation: NamespaceIncarnationId::from_uuid(incarnation),
                    };
                    let mut base = Manifest::new();
                    base.bind_namespace_incarnation(incarnation)?;
                    (origin, incarnation, base, Vec::new())
                }
            };

        let mut manifest = source_manifest;
        manifest.reset_version_for_clone();
        manifest.prepare_clone_publication(target_namespace, target_incarnation, &target_base)?;
        manifest.artifact_origins = vec![source_origin];
        for fragment in &mut manifest.fragments {
            fragment.artifact_origin = Some(ArtifactOriginIndex::new(0));
        }
        for segment in &mut manifest.segments {
            segment.artifact_origin = Some(ArtifactOriginIndex::new(0));
        }
        manifest.fragments.extend(local_tail);
        manifest.bind_synthetic_origin_receipt_for_test_support(target_namespace)?;

        // Validate the exact view once at construction. This is structural
        // resolution only; it intentionally does not open persisted admission.
        manifest.artifact_origin_resolver(&target_origin)?;

        Ok(Self {
            store,
            target_namespace: target_namespace.to_string(),
            target_origin,
            manifest,
        })
    }

    /// Execute ANN through the production supplied-manifest query path.
    #[allow(clippy::too_many_arguments)]
    pub async fn query_ann(
        &self,
        query: &[f32],
        top_k: usize,
        nprobe: usize,
        distance_metric: DistanceMetric,
        consistency: ConsistencyLevel,
    ) -> Result<SyntheticForeignQueryResult> {
        self.query_ann_with_options(
            query,
            top_k,
            nprobe,
            distance_metric,
            consistency,
            None,
            false,
            None,
            false,
        )
        .await
    }

    /// Execute configurable ANN through the same supplied-manifest path.
    #[allow(clippy::too_many_arguments)]
    pub async fn query_ann_with_options(
        &self,
        query: &[f32],
        top_k: usize,
        nprobe: usize,
        distance_metric: DistanceMetric,
        consistency: ConsistencyLevel,
        filter: Option<&Filter>,
        include_attributes: bool,
        cache: Option<&Arc<DiskCache>>,
        emit_debug: bool,
    ) -> Result<SyntheticForeignQueryResult> {
        let response = self
            .execute_ann_with_manifest(
                self.manifest.clone(),
                query,
                top_k,
                nprobe,
                distance_metric,
                consistency,
                filter,
                include_attributes,
                cache,
                emit_debug,
            )
            .await?;
        Ok(Self::query_result(response))
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_ann_with_manifest(
        &self,
        manifest: Manifest,
        query: &[f32],
        top_k: usize,
        nprobe: usize,
        distance_metric: DistanceMetric,
        consistency: ConsistencyLevel,
        filter: Option<&Filter>,
        include_attributes: bool,
        cache: Option<&Arc<DiskCache>>,
        emit_debug: bool,
    ) -> Result<crate::query::QueryResponse> {
        let wal_reader = WalReader::new(self.store.clone());
        let params = QueryParams {
            store: &self.store,
            wal_reader: &wal_reader,
            namespace: &self.target_namespace,
            query,
            top_k,
            nprobe,
            filter,
            consistency,
            distance_metric,
            oversample_factor: 3,
            rerank_coalesce_gap_bytes: DEFAULT_RERANK_COALESCE_GAP_BYTES,
            cache,
            manifest_cache: None,
            include_attributes,
        };
        let response = if emit_debug {
            execute_query_with_manifest_debug(
                params,
                manifest,
                None,
                None,
                Some(self.target_origin.clone()),
            )
            .await?
        } else {
            execute_query_with_manifest(
                params,
                manifest,
                None,
                None,
                Some(self.target_origin.clone()),
            )
            .await?
        };
        Ok(response)
    }

    /// Execute BM25 against the synthetic target's foreign segment and WAL.
    #[allow(clippy::too_many_arguments)]
    pub async fn query_bm25(
        &self,
        rank_by: &RankBy,
        fts_configs: &HashMap<String, FtsFieldConfig>,
        top_k: usize,
        filter: Option<&Filter>,
        consistency: ConsistencyLevel,
        include_attributes: bool,
        emit_debug: bool,
    ) -> Result<SyntheticForeignQueryResult> {
        let response = self
            .execute_bm25_with_manifest(
                self.manifest.clone(),
                rank_by,
                fts_configs,
                top_k,
                filter,
                consistency,
                include_attributes,
                emit_debug,
            )
            .await?;
        Ok(Self::query_result(response))
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_bm25_with_manifest(
        &self,
        manifest: Manifest,
        rank_by: &RankBy,
        fts_configs: &HashMap<String, FtsFieldConfig>,
        top_k: usize,
        filter: Option<&Filter>,
        consistency: ConsistencyLevel,
        include_attributes: bool,
        emit_debug: bool,
    ) -> Result<crate::query::QueryResponse> {
        let wal_reader = WalReader::new(self.store.clone());
        let response = if emit_debug {
            execute_bm25_query_with_manifest_debug(
                &self.store,
                &wal_reader,
                &self.target_namespace,
                rank_by,
                fts_configs,
                top_k,
                filter,
                None,
                consistency,
                false,
                None,
                None,
                None,
                None,
                0,
                0,
                include_attributes,
                manifest,
                Some(self.target_origin.clone()),
            )
            .await?
        } else {
            execute_bm25_query_with_manifest(
                &self.store,
                &wal_reader,
                &self.target_namespace,
                rank_by,
                fts_configs,
                top_k,
                filter,
                None,
                consistency,
                false,
                None,
                None,
                None,
                None,
                0,
                0,
                include_attributes,
                manifest,
                Some(self.target_origin.clone()),
            )
            .await?
        };
        Ok(response)
    }

    /// Execute independent positional entries against clones of one manifest.
    ///
    /// An entry error is retained at its original index and does not prevent
    /// later entries from executing. Hybrid entries execute ANN and BM25 from
    /// the same manifest clone and delegate reduction to the production fusion
    /// function used by retrieval algebra.
    #[allow(clippy::too_many_arguments)]
    pub async fn query_batch(
        &self,
        specs: &[SyntheticForeignQuerySpec],
        fts_configs: &HashMap<String, FtsFieldConfig>,
        distance_metric: DistanceMetric,
        consistency: ConsistencyLevel,
        include_attributes: bool,
        emit_debug: bool,
    ) -> Vec<Result<SyntheticForeignQueryResult>> {
        let mut entries = Vec::with_capacity(specs.len());
        for spec in specs {
            let response = self
                .execute_query_spec(
                    spec,
                    fts_configs,
                    distance_metric,
                    consistency,
                    include_attributes,
                    emit_debug,
                    self.manifest.clone(),
                )
                .await
                .map(Self::query_result);
            entries.push(response);
        }
        entries
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_query_spec(
        &self,
        spec: &SyntheticForeignQuerySpec,
        fts_configs: &HashMap<String, FtsFieldConfig>,
        distance_metric: DistanceMetric,
        consistency: ConsistencyLevel,
        include_attributes: bool,
        emit_debug: bool,
        manifest: Manifest,
    ) -> Result<crate::query::QueryResponse> {
        match spec {
            SyntheticForeignQuerySpec::Ann {
                query,
                top_k,
                nprobe,
            } => {
                self.execute_ann_with_manifest(
                    manifest,
                    query,
                    *top_k,
                    *nprobe,
                    distance_metric,
                    consistency,
                    None,
                    include_attributes,
                    None,
                    emit_debug,
                )
                .await
            }
            SyntheticForeignQuerySpec::Bm25 { rank_by, top_k } => {
                self.execute_bm25_with_manifest(
                    manifest,
                    rank_by,
                    fts_configs,
                    *top_k,
                    None,
                    consistency,
                    include_attributes,
                    emit_debug,
                )
                .await
            }
            SyntheticForeignQuerySpec::Hybrid {
                query,
                rank_by,
                top_k,
                nprobe,
            } => {
                let ann = self
                    .execute_ann_with_manifest(
                        manifest.clone(),
                        query,
                        *top_k,
                        *nprobe,
                        distance_metric,
                        consistency,
                        None,
                        include_attributes,
                        None,
                        emit_debug,
                    )
                    .await?;
                let bm25 = self
                    .execute_bm25_with_manifest(
                        manifest,
                        rank_by,
                        fts_configs,
                        *top_k,
                        None,
                        consistency,
                        include_attributes,
                        emit_debug,
                    )
                    .await?;
                crate::server::handlers::query::fuse_ann_bm25_for_test_support(
                    ann,
                    bm25,
                    *top_k,
                    *nprobe,
                    distance_metric,
                    consistency,
                    include_attributes,
                    emit_debug,
                )
            }
        }
    }

    /// Fetch exact IDs with vector and attribute projection through production lookup.
    pub async fn fetch_by_ids(
        &self,
        ids: &[VectorId],
        consistency: ConsistencyLevel,
        include_vector: bool,
        include_attributes: bool,
        attribute_fields: Option<&[String]>,
    ) -> Result<SyntheticForeignFetchResult> {
        let (response, touched) =
            crate::server::handlers::vectors::fetch_vectors_by_id_for_test_support(
                &self.store,
                &self.target_namespace,
                ids,
                consistency,
                include_vector,
                include_attributes,
                attribute_fields,
                self.manifest.clone(),
                self.target_origin.clone(),
            )
            .await?;
        Ok(SyntheticForeignFetchResult {
            records: response
                .results
                .into_iter()
                .map(|record| SyntheticForeignFetchRecord {
                    id: record.id,
                    values: record.values,
                    attributes: record.attributes,
                })
                .collect(),
            missing: response.missing,
            touched_artifact_keys: touched.into_iter().collect(),
        })
    }

    /// Resolve the active segment into an owned logical/physical hydration target.
    pub fn hydration_target(&self) -> Result<Option<HydrationTarget>> {
        HydrationTarget::from_active_manifest_with_origin(&self.manifest, &self.target_origin)
    }

    /// Return the exact physical keys referenced by this logical target view.
    pub fn reachable_artifact_keys(&self) -> Result<BTreeSet<String>> {
        crate::compaction::gc::reachable_keys(&self.target_namespace, &self.manifest)
    }

    /// Classify one inventory key through the destructive target-GC guard.
    pub fn classify_target_sweep_candidate(&self, key: String) -> Result<String> {
        crate::compaction::gc::classify_target_owned_deletion_key_for_test_support(
            &self.target_namespace,
            key,
        )
    }

    /// Produce a structurally invalid origin-table reference for fail-loud tests.
    #[must_use]
    pub fn with_corrupt_active_segment_origin(mut self) -> Self {
        if let Some(active) = self.manifest.active_segment.clone() {
            if let Some(segment) = self
                .manifest
                .segments
                .iter_mut()
                .find(|segment| segment.id == active)
            {
                let invalid_index = match u32::try_from(self.manifest.artifact_origins.len()) {
                    Ok(index) => index,
                    Err(_) => panic!("synthetic artifact-origin table exceeds u32"),
                };
                segment.artifact_origin = Some(ArtifactOriginIndex::new(invalid_index));
            }
        }
        self
    }

    fn query_result(response: crate::query::QueryResponse) -> SyntheticForeignQueryResult {
        let ids = response
            .results
            .iter()
            .map(|result| result.id.clone())
            .collect();
        let (debug_present, cache_hits, cache_misses) =
            response.debug.as_ref().map_or((false, 0, 0), |debug| {
                (true, debug.cache.hits, debug.cache.misses)
            });
        SyntheticForeignQueryResult {
            ids,
            results: response.results,
            touched_artifact_keys: response.receipt_touched_artifacts.into_iter().collect(),
            scanned_fragments: response.scanned_fragments,
            scanned_segments: response.scanned_segments,
            debug_present,
            cache_hits,
            cache_misses,
        }
    }

    /// Run the normal persisted-manifest decoder against this foreign view.
    ///
    /// Until lineage authorization is implemented this returns
    /// `BranchingNotReady`; an `Ok` result would mean the fixture accidentally
    /// weakened production admission.
    pub fn production_admission_result(&self) -> Result<()> {
        let bytes = self.manifest.to_bytes()?;
        Manifest::from_bytes_for_namespace(&bytes, &self.target_namespace).map(|_| ())
    }
}
