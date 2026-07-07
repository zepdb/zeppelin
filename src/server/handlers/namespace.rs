use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{info, instrument};
use uuid::Uuid;

use crate::compaction::background::compact_namespace_under_lease;
use crate::error::ZeppelinError;
use crate::fts::FtsFieldConfig;
use crate::index::quantization::QuantizationType;
use crate::namespace::manager::{
    CreateNamespaceOutcome, NamespaceIndexConfig, NamespaceMetadata,
    COMPACTION_DEGRADED_FAILURE_THRESHOLD,
};
use crate::server::AppState;
use crate::types::{DistanceMetric, IndexType};
use crate::wal::manifest::SegmentRef;
use crate::wal::Manifest;

use super::ApiError;

/// Request body for creating a new namespace.
#[derive(Debug, Deserialize)]
pub struct CreateNamespaceRequest {
    /// Optional client-specified namespace name.
    pub name: Option<String>,
    /// Dimensionality of vectors stored in this namespace.
    pub dimensions: usize,
    /// Distance metric for similarity search (defaults to Cosine).
    #[serde(default = "default_distance_metric")]
    pub distance_metric: DistanceMetric,
    /// Full-text search field configurations (empty map disables FTS).
    #[serde(default)]
    pub full_text_search: std::collections::HashMap<String, FtsFieldConfig>,
    /// Optional per-namespace index configuration for future compactions.
    #[serde(default)]
    pub index_config: Option<CreateNamespaceIndexConfig>,
}

/// Optional index config overrides accepted at namespace creation.
#[derive(Debug, Deserialize)]
pub struct CreateNamespaceIndexConfig {
    /// Number of IVF centroids/clusters.
    #[serde(default)]
    pub nlist: Option<usize>,
    /// Quantization mode.
    #[serde(default)]
    pub quantization: Option<QuantizationType>,
    /// Number of product-quantization subquantizers.
    #[serde(default)]
    pub pq_m: Option<usize>,
    /// Build hierarchical indexes for future compactions.
    #[serde(default)]
    pub hierarchical: Option<bool>,
    /// Build FTS indexes for configured full-text fields.
    #[serde(default)]
    pub fts_index: Option<bool>,
    /// Build bitmap indexes for future compactions.
    #[serde(default)]
    pub bitmap_index: Option<bool>,
}

impl CreateNamespaceIndexConfig {
    fn is_empty(&self) -> bool {
        self.nlist.is_none()
            && self.quantization.is_none()
            && self.pq_m.is_none()
            && self.hierarchical.is_none()
            && self.fts_index.is_none()
            && self.bitmap_index.is_none()
    }
}

/// Partial index config patch staged for the next compaction.
pub type PatchNamespaceIndexConfigRequest = CreateNamespaceIndexConfig;

fn default_distance_metric() -> DistanceMetric {
    DistanceMetric::Cosine
}

/// Response body containing namespace metadata.
#[derive(Debug, Serialize)]
pub struct NamespaceResponse {
    /// Namespace name.
    pub name: String,
    /// Vector dimensionality.
    pub dimensions: usize,
    /// Distance metric used for similarity search.
    pub distance_metric: DistanceMetric,
    /// Total number of vectors in this namespace.
    pub vector_count: u64,
    /// Number of uncompacted WAL fragments currently referenced by the manifest.
    pub uncompacted_fragments: usize,
    /// Number of segment references currently tracked by the manifest.
    pub segment_count: usize,
    /// Approximate live storage bytes known from manifest object-size refs.
    pub approximate_storage_bytes: u64,
    /// Quantization mode of the active segment, or null before first compaction.
    pub quantization: Option<QuantizationType>,
    /// Index kind used by the namespace's active segment.
    pub index_kind: IndexType,
    /// Effective index parameters for future compactions.
    pub index_config: NamespaceIndexConfig,
    /// Vector count in the active segment only.
    pub active_segment_vector_count: usize,
    /// RFC 3339 timestamp of the last compaction outcome, if any.
    pub last_compaction_at: Option<String>,
    /// Last recorded compaction status.
    pub last_compaction_status: String,
    /// Last compaction error, present only after a failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_compaction_error: Option<String>,
    /// Consecutive compaction failures since the last success.
    pub consecutive_compaction_failures: u32,
    /// True after repeated compaction failures.
    pub index_degraded: bool,
    /// RFC 3339 timestamp of namespace creation.
    pub created_at: String,
    /// RFC 3339 timestamp of the last update.
    pub updated_at: String,
    /// Namespace lifecycle state.
    pub state: String,
    /// Full-text search field configurations (omitted when empty).
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub full_text_search: std::collections::HashMap<String, FtsFieldConfig>,
}

/// Response body for asynchronous namespace deletion.
#[derive(Debug, Serialize)]
pub struct DeleteNamespaceResponse {
    /// Current lifecycle state.
    pub state: &'static str,
}

/// Response body for an accepted admin hydration request.
#[derive(Debug, Serialize)]
pub struct HydrateNamespaceResponse {
    /// Namespace whose active segment was queued for hydration.
    pub namespace: String,
    /// Active segment id queued into the hydrator pipeline.
    pub segment_id: String,
}

/// Response body for an index config update.
#[derive(Debug, Serialize)]
pub struct UpdateIndexConfigResponse {
    /// Namespace whose desired index config was updated.
    pub namespace: String,
    /// Effective index config staged for the next compaction.
    pub index_config: NamespaceIndexConfig,
    /// Stable status string for clients.
    pub status: &'static str,
    /// How to observe the asynchronous rewrite.
    pub observe: String,
}

/// Manifest-derived status for manual compaction clients.
#[derive(Debug, Serialize)]
pub struct CompactionStatusResponse {
    /// Namespace name.
    pub namespace: String,
    /// Persisted manifest generation read from S3.
    pub manifest_generation: u64,
    /// Number of WAL fragments still waiting for compaction.
    pub uncompacted_fragments: usize,
    /// Number of segment references currently tracked by the manifest.
    pub segment_count: usize,
    /// Active segment id, if compaction has produced one.
    pub active_segment: Option<String>,
    /// Vector count in the active segment only.
    pub active_segment_vector_count: usize,
    /// True when no WAL fragments are waiting for compaction.
    pub ready: bool,
}

/// Response body for a manual compaction trigger.
#[derive(Debug, Serialize)]
pub struct CompactNamespaceResponse {
    /// Namespace name.
    pub namespace: String,
    /// Stable outcome string for this trigger.
    pub status: &'static str,
    /// Persisted manifest generation read from S3.
    pub manifest_generation: u64,
    /// Number of WAL fragments still waiting for compaction.
    pub uncompacted_fragments: usize,
    /// Number of segment references currently tracked by the manifest.
    pub segment_count: usize,
    /// Active segment id, if compaction has produced one.
    pub active_segment: Option<String>,
    /// Vector count in the active segment only.
    pub active_segment_vector_count: usize,
    /// True when no WAL fragments are waiting for compaction.
    pub ready: bool,
}

/// Response body for namespace creation.
#[derive(Debug, Serialize)]
pub struct CreateNamespaceResponse {
    /// Namespace metadata.
    #[serde(flatten)]
    pub namespace: NamespaceResponse,
    /// Creation note.
    pub warning: String,
}

impl NamespaceResponse {
    /// Converts namespace metadata plus the authoritative manifest into the API response.
    #[must_use]
    pub fn from_manifest(
        meta: NamespaceMetadata,
        manifest: &Manifest,
        default_indexing: &crate::config::IndexingConfig,
    ) -> Self {
        let index_kind = namespace_index_kind(&meta, manifest);
        let active_segment = active_segment_ref(manifest);
        let compaction_health = meta.compaction_health.clone();
        Self {
            name: meta.name,
            dimensions: meta.dimensions,
            distance_metric: meta.distance_metric,
            vector_count: manifest.vector_count(),
            uncompacted_fragments: manifest.uncompacted_fragments().len(),
            segment_count: manifest.segments.len(),
            approximate_storage_bytes: manifest.approximate_storage_bytes(),
            quantization: active_segment.map(|segment| segment.quantization),
            index_kind,
            index_config: meta
                .index_config
                .unwrap_or_else(|| NamespaceIndexConfig::from_indexing_config(default_indexing)),
            active_segment_vector_count: active_segment.map_or(0, |segment| segment.vector_count),
            last_compaction_at: compaction_health
                .last_compaction_at
                .map(|timestamp| timestamp.to_rfc3339()),
            last_compaction_status: compaction_health
                .last_compaction_status
                .as_str()
                .to_string(),
            last_compaction_error: compaction_health.last_compaction_error,
            consecutive_compaction_failures: compaction_health.consecutive_failures,
            index_degraded: compaction_health.consecutive_failures
                >= COMPACTION_DEGRADED_FAILURE_THRESHOLD,
            created_at: meta.created_at.to_rfc3339(),
            updated_at: meta.updated_at.to_rfc3339(),
            state: meta.state.as_str().to_string(),
            full_text_search: meta.full_text_search,
        }
    }
}

impl CompactNamespaceResponse {
    fn from_status(status: &'static str, response: CompactionStatusResponse) -> Self {
        Self {
            namespace: response.namespace,
            status,
            manifest_generation: response.manifest_generation,
            uncompacted_fragments: response.uncompacted_fragments,
            segment_count: response.segment_count,
            active_segment: response.active_segment,
            active_segment_vector_count: response.active_segment_vector_count,
            ready: response.ready,
        }
    }
}

/// Creates a new namespace.
#[instrument(skip(state), fields(dimensions = req.dimensions))]
pub async fn create_namespace(
    State(state): State<AppState>,
    Json(req): Json<CreateNamespaceRequest>,
) -> Result<(StatusCode, Json<CreateNamespaceResponse>), ApiError> {
    if req.dimensions == 0 || req.dimensions > state.config.server.max_dimensions {
        return Err(ApiError(ZeppelinError::Validation(format!(
            "dimensions {} must be between 1 and {}",
            req.dimensions, state.config.server.max_dimensions
        ))));
    }
    let index_config = resolve_namespace_index_config(
        req.index_config.as_ref(),
        &state.config.indexing,
        req.dimensions,
    )
    .map_err(ApiError::from)?;

    if let Some(name) = req.name {
        info!(namespace = %name, dimensions = req.dimensions, "creating namespace by client name");
        let outcome = state
            .namespace_manager
            .create_idempotent_with_fts_and_index_config(
                &name,
                req.dimensions,
                req.distance_metric,
                req.full_text_search,
                Some(index_config),
            )
            .await
            .map_err(ApiError::from)?;
        let (status, meta) = match outcome {
            CreateNamespaceOutcome::Created(meta) => {
                info!(namespace = %name, "namespace created by client name");
                (StatusCode::CREATED, meta)
            }
            CreateNamespaceOutcome::Existing(meta) => {
                info!(namespace = %name, "namespace already existed with matching config");
                (StatusCode::OK, meta)
            }
        };
        return Ok((
            status,
            Json(CreateNamespaceResponse {
                namespace: NamespaceResponse::from_manifest(
                    meta,
                    &Manifest::new(),
                    &state.config.indexing,
                ),
                warning:
                    "Client-specified namespace names are idempotent for identical configuration."
                        .to_string(),
            }),
        ));
    }

    let name = generated_namespace_name(state.namespace_name_prefix.as_deref());
    info!(namespace = %name, dimensions = req.dimensions, "creating generated namespace");
    let meta = state
        .namespace_manager
        .create_with_fts_and_index_config(
            &name,
            req.dimensions,
            req.distance_metric,
            req.full_text_search,
            Some(index_config),
        )
        .await
        .map_err(ApiError::from)?;

    info!(namespace = %name, "generated namespace created");
    Ok((
        StatusCode::CREATED,
        Json(CreateNamespaceResponse {
            namespace: NamespaceResponse::from_manifest(
                meta,
                &Manifest::new(),
                &state.config.indexing,
            ),
            warning: "Save this namespace name. It cannot be recovered if lost.".to_string(),
        }),
    ))
}

fn resolve_namespace_index_config(
    request: Option<&CreateNamespaceIndexConfig>,
    defaults: &crate::config::IndexingConfig,
    dimensions: usize,
) -> Result<NamespaceIndexConfig, ZeppelinError> {
    let mut config = NamespaceIndexConfig::from_indexing_config(defaults);
    if let Some(request) = request {
        if let Some(nlist) = request.nlist {
            config.nlist = nlist;
        }
        if let Some(quantization) = request.quantization {
            config.quantization = quantization;
        }
        if let Some(pq_m) = request.pq_m {
            config.pq_m = pq_m;
        }
        if let Some(hierarchical) = request.hierarchical {
            config.hierarchical = hierarchical;
        }
        if let Some(fts_index) = request.fts_index {
            config.fts_index = fts_index;
        }
        if let Some(bitmap_index) = request.bitmap_index {
            config.bitmap_index = bitmap_index;
        }
    }
    config.validate(dimensions)?;
    Ok(config)
}

fn apply_namespace_index_config_patch(
    mut config: NamespaceIndexConfig,
    request: &PatchNamespaceIndexConfigRequest,
    dimensions: usize,
) -> Result<NamespaceIndexConfig, ZeppelinError> {
    if let Some(nlist) = request.nlist {
        config.nlist = nlist;
    }
    if let Some(quantization) = request.quantization {
        config.quantization = quantization;
    }
    if let Some(pq_m) = request.pq_m {
        config.pq_m = pq_m;
    }
    if let Some(hierarchical) = request.hierarchical {
        config.hierarchical = hierarchical;
    }
    if let Some(fts_index) = request.fts_index {
        config.fts_index = fts_index;
    }
    if let Some(bitmap_index) = request.bitmap_index {
        config.bitmap_index = bitmap_index;
    }
    config.validate(dimensions)?;
    Ok(config)
}

fn generated_namespace_name(prefix: Option<&str>) -> String {
    let uuid = Uuid::new_v4().to_string();
    match prefix {
        Some(prefix) => format!("{prefix}-{uuid}"),
        None => uuid,
    }
}

/// Lists all namespaces (not routed — disabled to prevent namespace enumeration).
#[allow(dead_code)]
#[instrument(skip(state))]
pub async fn list_namespaces(
    State(state): State<AppState>,
) -> Result<Json<Vec<NamespaceResponse>>, ApiError> {
    let namespaces = state
        .namespace_manager
        .list(None)
        .await
        .map_err(ApiError::from)?;

    info!(count = namespaces.len(), "listed namespaces");
    let empty_manifest = Manifest::new();
    let responses: Vec<NamespaceResponse> = namespaces
        .into_iter()
        .map(|meta| NamespaceResponse::from_manifest(meta, &empty_manifest, &state.config.indexing))
        .collect();
    Ok(Json(responses))
}

/// Returns metadata for a single namespace.
#[instrument(skip(state), fields(namespace = %ns))]
pub async fn get_namespace(
    State(state): State<AppState>,
    Path(ns): Path<String>,
) -> Result<Json<NamespaceResponse>, ApiError> {
    let meta = state
        .namespace_manager
        .get_including_deleting(&ns)
        .await
        .map_err(ApiError::from)?;

    // Stats are manifest aggregates. This strong read is the same manifest
    // freshness path used by read handlers; the response below does not list,
    // HEAD, or fetch WAL/segment objects.
    let manifest = state
        .manifest_cache
        .get_strong(&state.store, &ns)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(NamespaceResponse::from_manifest(
        meta,
        &manifest,
        &state.config.indexing,
    )))
}

/// Reports whether a namespace has WAL fragments pending compaction.
#[instrument(skip(state), fields(namespace = %ns))]
pub async fn get_compaction_status(
    State(state): State<AppState>,
    Path(ns): Path<String>,
) -> Result<Json<CompactionStatusResponse>, ApiError> {
    state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;
    let manifest = state
        .manifest_cache
        .get_strong(&state.store, &ns)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(compaction_status_from_manifest(&ns, &manifest)))
}

/// Runs one manual, lease-protected compaction cycle for a namespace.
#[instrument(skip(state), fields(namespace = %ns))]
pub async fn compact_namespace(
    State(state): State<AppState>,
    Path(ns): Path<String>,
) -> Result<Json<CompactNamespaceResponse>, ApiError> {
    let meta = state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;
    let before = state
        .manifest_cache
        .get_strong(&state.store, &ns)
        .await
        .map_err(ApiError::from)?;

    if before.uncompacted_fragments().is_empty() {
        return Ok(Json(CompactNamespaceResponse::from_status(
            "noop",
            compaction_status_from_manifest(&ns, &before),
        )));
    }

    info!(namespace = %ns, "manual compaction requested");
    let result = compact_namespace_under_lease(
        &state.compactor,
        &state.lease_manager,
        &ns,
        &meta.full_text_search,
    )
    .await
    .map_err(ApiError::from)?;

    state.manifest_cache.invalidate(&ns);
    let after = state
        .manifest_cache
        .get_strong(&state.store, &ns)
        .await
        .map_err(ApiError::from)?;
    let status = if result.fragments_removed == 0 && result.segment_id.is_none() {
        "noop"
    } else {
        "compacted"
    };

    Ok(Json(CompactNamespaceResponse::from_status(
        status,
        compaction_status_from_manifest(&ns, &after),
    )))
}

/// Stages a new per-namespace index config for the next compaction.
#[instrument(skip(state), fields(namespace = %ns))]
pub async fn patch_index_config(
    State(state): State<AppState>,
    Path(ns): Path<String>,
    Json(req): Json<PatchNamespaceIndexConfigRequest>,
) -> Result<(StatusCode, Json<UpdateIndexConfigResponse>), ApiError> {
    if req.is_empty() {
        return Err(ApiError(ZeppelinError::Validation(
            "index_config patch must include at least one field".into(),
        )));
    }
    let meta = state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;
    let current = meta
        .index_config
        .clone()
        .unwrap_or_else(|| NamespaceIndexConfig::from_indexing_config(&state.config.indexing));
    let next = apply_namespace_index_config_patch(current, &req, meta.dimensions)
        .map_err(ApiError::from)?;
    let updated = state
        .namespace_manager
        .update_index_config(&ns, next)
        .await
        .map_err(ApiError::from)?;
    let index_config = updated.index_config.ok_or_else(|| {
        ApiError(ZeppelinError::Index(
            "index_config update did not persist index_config".into(),
        ))
    })?;

    Ok((
        StatusCode::ACCEPTED,
        Json(UpdateIndexConfigResponse {
            namespace: ns.clone(),
            index_config,
            status: "accepted",
            observe: format!(
                "applies asynchronously on the next compaction; observe GET /v1/namespaces/{ns}"
            ),
        }),
    ))
}

/// Deletes a namespace and cleans up associated in-memory state.
#[instrument(skip(state), fields(namespace = %ns))]
pub async fn delete_namespace(
    State(state): State<AppState>,
    Path(ns): Path<String>,
) -> Result<(StatusCode, Json<DeleteNamespaceResponse>), ApiError> {
    info!(namespace = %ns, "deleting namespace");
    state
        .namespace_manager
        .start_delete(&ns)
        .await
        .map_err(ApiError::from)?;

    // Clean up per-namespace in-memory state to prevent unbounded growth
    state.wal_writer.remove_lock(&ns);
    state.manifest_cache.invalidate(&ns);

    let namespace_manager = state.namespace_manager.clone();
    let ns_for_task = ns.clone();
    tokio::spawn(async move {
        match namespace_manager
            .finish_delete(&ns_for_task, Duration::from_secs(25))
            .await
        {
            Ok(outcome) => {
                if outcome.complete {
                    tracing::info!(
                        namespace = %ns_for_task,
                        objects_deleted = outcome.deleted,
                        "namespace background delete completed"
                    );
                } else {
                    tracing::warn!(
                        namespace = %ns_for_task,
                        objects_deleted = outcome.deleted,
                        "namespace background delete budget exhausted; retry DELETE to resume"
                    );
                }
            }
            Err(e) => {
                tracing::error!(
                    namespace = %ns_for_task,
                    error = %e,
                    "namespace background delete failed; retry DELETE to resume"
                );
            }
        }
    });

    info!(namespace = %ns, state = "deleting", "namespace delete accepted");
    Ok((
        StatusCode::ACCEPTED,
        Json(DeleteNamespaceResponse { state: "deleting" }),
    ))
}

/// Enqueues warm-set hydration for the namespace's current active segment.
#[instrument(skip(state), fields(namespace = %ns))]
pub async fn trigger_hydration(
    State(state): State<AppState>,
    Path(ns): Path<String>,
) -> Result<(StatusCode, Json<HydrateNamespaceResponse>), ApiError> {
    let hydrator = state
        .hydrator
        .as_ref()
        .ok_or(ApiError(ZeppelinError::HydrationDisabled))?;

    state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;
    let manifest = state
        .manifest_cache
        .get_strong(&state.store, &ns)
        .await
        .map_err(ApiError::from)?;
    let segment = active_segment_snapshot(&manifest).ok_or_else(|| {
        ApiError(ZeppelinError::Validation(format!(
            "namespace {ns} has no active segment to hydrate"
        )))
    })?;
    let segment_id = segment.id.clone();

    hydrator.request_hydration(&ns, &segment);
    info!(
        namespace = %ns,
        segment_id = %segment_id,
        "namespace hydration accepted"
    );
    Ok((
        StatusCode::ACCEPTED,
        Json(HydrateNamespaceResponse {
            namespace: ns,
            segment_id,
        }),
    ))
}

fn active_segment_snapshot(
    manifest: &crate::wal::Manifest,
) -> Option<crate::wal::manifest::SegmentRef> {
    active_segment_ref(manifest).cloned()
}

fn compaction_status_from_manifest(
    namespace: &str,
    manifest: &Manifest,
) -> CompactionStatusResponse {
    let active_segment = active_segment_ref(manifest);
    let uncompacted_fragments = manifest.uncompacted_fragments().len();
    CompactionStatusResponse {
        namespace: namespace.to_string(),
        manifest_generation: manifest.version(),
        uncompacted_fragments,
        segment_count: manifest.segments.len(),
        active_segment: active_segment.map(|segment| segment.id.clone()),
        active_segment_vector_count: active_segment.map_or(0, |segment| segment.vector_count),
        ready: uncompacted_fragments == 0,
    }
}

fn active_segment_ref(manifest: &Manifest) -> Option<&SegmentRef> {
    let active_segment = manifest.active_segment.as_ref()?;
    manifest
        .segments
        .iter()
        .find(|segment| segment.id == *active_segment)
}

fn namespace_index_kind(meta: &NamespaceMetadata, manifest: &Manifest) -> IndexType {
    if active_segment_ref(manifest).is_some_and(|segment| segment.hierarchical) {
        IndexType::Hierarchical
    } else {
        meta.index_type
    }
}
