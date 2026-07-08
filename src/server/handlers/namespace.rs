use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::Utc;
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::{info, instrument, warn};
use uuid::Uuid;

use crate::compaction::background::run_compaction_with_lease;
use crate::compaction::gc::reachable_keys;
use crate::error::ZeppelinError;
use crate::fts::FtsFieldConfig;
use crate::index::quantization::QuantizationType;
use crate::namespace::manager::{
    CreateNamespaceOutcome, NamespaceIndexConfig, NamespaceMetadata,
    COMPACTION_DEGRADED_FAILURE_THRESHOLD,
};
use crate::server::AppState;
use crate::types::{DistanceMetric, IndexType};
use crate::wal::manifest::{NamedSnapshot, NamedSnapshotRef, SegmentRef};
use crate::wal::Manifest;

use super::{as_of, ApiError};

const CLONE_COPY_CONCURRENCY: usize = 16;
const CLONE_INTERNAL_SNAPSHOT_PREFIX: &str = "__clone_";

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

/// Response body for a named PITR snapshot.
#[derive(Debug, Serialize)]
pub struct SnapshotResponse {
    /// Snapshot name.
    pub name: String,
    /// Manifest generation pinned by this snapshot.
    pub generation: u64,
    /// RFC 3339 timestamp when the snapshot was created.
    pub created_at: String,
}

/// Response body for listing named PITR snapshots.
#[derive(Debug, Serialize)]
pub struct ListSnapshotsResponse {
    /// Snapshot pins for this namespace.
    pub snapshots: Vec<SnapshotResponse>,
}

/// Request body for restoring a namespace as a new clone.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloneNamespaceRequest {
    /// Fresh target namespace to create.
    pub target: String,
    /// Source point-in-time target: generation, RFC3339 timestamp, or `snapshot:name`.
    pub as_of: String,
}

/// Response body for restoring a namespace as a new clone.
#[derive(Debug, Serialize)]
pub struct CloneNamespaceResponse {
    /// Source namespace read at the requested point in time.
    pub source: String,
    /// Fresh namespace created by the clone operation.
    pub target: String,
    /// Source manifest generation that was materialized.
    pub generation: u64,
    /// Target namespace manifest generation after materialization.
    pub target_generation: u64,
    /// Clone materialization mode.
    pub mode: &'static str,
    /// Target namespace metadata and manifest-derived status.
    pub namespace: NamespaceResponse,
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

impl SnapshotResponse {
    fn from_ref(snapshot: NamedSnapshotRef) -> Self {
        Self {
            name: snapshot.name,
            generation: snapshot.generation,
            created_at: snapshot.created_at.to_rfc3339(),
        }
    }
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

/// Creates a named PITR snapshot pin for the current committed generation.
#[instrument(skip(state), fields(namespace = %ns, snapshot = %name))]
pub async fn put_snapshot(
    State(state): State<AppState>,
    Path((ns, name)): Path<(String, String)>,
) -> Result<(StatusCode, Json<SnapshotResponse>), ApiError> {
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
    let snapshot = NamedSnapshot::create(&state.store, &ns, &name, manifest.version())
        .await
        .map_err(ApiError::from)?;
    Ok((
        StatusCode::CREATED,
        Json(SnapshotResponse::from_ref(snapshot)),
    ))
}

/// Lists named PITR snapshot pins for a namespace.
#[instrument(skip(state), fields(namespace = %ns))]
pub async fn list_snapshots(
    State(state): State<AppState>,
    Path(ns): Path<String>,
) -> Result<Json<ListSnapshotsResponse>, ApiError> {
    state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;
    let snapshots = NamedSnapshot::list(&state.store, &ns)
        .await
        .map_err(ApiError::from)?
        .into_iter()
        .map(SnapshotResponse::from_ref)
        .collect();
    Ok(Json(ListSnapshotsResponse { snapshots }))
}

/// Gets one named PITR snapshot pin.
#[instrument(skip(state), fields(namespace = %ns, snapshot = %name))]
pub async fn get_snapshot(
    State(state): State<AppState>,
    Path((ns, name)): Path<(String, String)>,
) -> Result<Json<SnapshotResponse>, ApiError> {
    state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;
    let snapshot = NamedSnapshot::read(&state.store, &ns, &name)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| {
            ApiError(ZeppelinError::SnapshotNotFound {
                namespace: ns.clone(),
                name: name.clone(),
            })
        })?;
    Ok(Json(SnapshotResponse::from_ref(snapshot)))
}

/// Deletes one named PITR snapshot pin.
#[instrument(skip(state), fields(namespace = %ns, snapshot = %name))]
pub async fn delete_snapshot(
    State(state): State<AppState>,
    Path((ns, name)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;
    if NamedSnapshot::read(&state.store, &ns, &name)
        .await
        .map_err(ApiError::from)?
        .is_none()
    {
        return Err(ApiError(ZeppelinError::SnapshotNotFound {
            namespace: ns,
            name,
        }));
    }
    NamedSnapshot::delete(&state.store, &ns, &name)
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
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

/// Restores a retained source generation as a fresh, independently writable namespace.
#[instrument(skip(state, req), fields(source = %source))]
pub async fn clone_namespace(
    State(state): State<AppState>,
    Path(source): Path<String>,
    Json(req): Json<CloneNamespaceRequest>,
) -> Result<(StatusCode, Json<CloneNamespaceResponse>), ApiError> {
    let target = req.target;
    let as_of = req.as_of;
    if target == source {
        return Err(ApiError(ZeppelinError::Validation(
            "clone target must differ from source namespace".into(),
        )));
    }

    let source_meta = state
        .namespace_manager
        .get(&source)
        .await
        .map_err(ApiError::from)?;
    let source_manifest = as_of::resolve_manifest(&state.store, &source, &as_of)
        .await
        .map_err(ApiError::from)?;
    let source_generation = source_manifest.version();
    let clone_pin_name = internal_clone_pin_name();
    NamedSnapshot::create(&state.store, &source, &clone_pin_name, source_generation)
        .await
        .map_err(ApiError::from)?;

    let index_config = source_meta
        .index_config
        .clone()
        .unwrap_or_else(|| NamespaceIndexConfig::from_indexing_config(&state.config.indexing));

    let target_meta = match state
        .namespace_manager
        .create_with_fts_and_index_config(
            &target,
            source_meta.dimensions,
            source_meta.distance_metric,
            source_meta.full_text_search.clone(),
            Some(index_config),
        )
        .await
    {
        Ok(meta) => meta,
        Err(e) => {
            release_internal_clone_pin(&state, &source, &clone_pin_name).await;
            return Err(ApiError::from(e));
        }
    };

    let mut target_manifest =
        match materialize_clone_manifest(&state, &source, &target, source_manifest).await {
            Ok(manifest) => manifest,
            Err(e) => {
                cleanup_failed_clone_target(&state, &target).await;
                release_internal_clone_pin(&state, &source, &clone_pin_name).await;
                return Err(ApiError::from(e));
            }
        };
    if let Err(e) = target_manifest.write(&state.store, &target).await {
        cleanup_failed_clone_target(&state, &target).await;
        release_internal_clone_pin(&state, &source, &clone_pin_name).await;
        return Err(ApiError::from(e));
    }
    state.manifest_cache.invalidate(&target);
    release_internal_clone_pin(&state, &source, &clone_pin_name).await;

    info!(
        source = %source,
        target = %target,
        source_generation,
        target_generation = target_manifest.version(),
        mode = "copy",
        "namespace clone materialized"
    );

    Ok((
        StatusCode::CREATED,
        Json(CloneNamespaceResponse {
            source,
            target: target.clone(),
            generation: source_generation,
            target_generation: target_manifest.version(),
            mode: "copy",
            namespace: NamespaceResponse::from_manifest(
                target_meta,
                &target_manifest,
                &state.config.indexing,
            ),
        }),
    ))
}

fn internal_clone_pin_name() -> String {
    format!(
        "{CLONE_INTERNAL_SNAPSHOT_PREFIX}{}",
        Uuid::new_v4().simple()
    )
}

async fn release_internal_clone_pin(state: &AppState, source: &str, name: &str) {
    match NamedSnapshot::delete(&state.store, source, name).await {
        Ok(()) | Err(ZeppelinError::NotFound { .. }) => {}
        Err(e) => warn!(
            source,
            snapshot = name,
            error = %e,
            "failed to release temporary clone snapshot pin"
        ),
    }
}

async fn cleanup_failed_clone_target(state: &AppState, target: &str) {
    state.manifest_cache.invalidate(target);
    match state.namespace_manager.delete(target).await {
        Ok(()) | Err(ZeppelinError::NamespaceNotFound { .. }) => {}
        Err(e) => warn!(
            target,
            error = %e,
            "failed to clean up target namespace after clone failure"
        ),
    }
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

async fn materialize_clone_manifest(
    state: &AppState,
    source: &str,
    target: &str,
    mut manifest: Manifest,
) -> Result<Manifest, ZeppelinError> {
    manifest.pending_deletes.clear();
    let copies = clone_copy_map(source, target, &manifest)?;
    rewrite_manifest_stored_keys(source, target, &mut manifest)?;
    manifest.fencing_token = 0;
    manifest.updated_at = Utc::now();
    manifest.reset_version_for_clone();

    futures::stream::iter(copies.into_iter().map(|(from, to)| {
        let store = state.store.clone();
        let target = target.to_string();
        async move { store.copy_if_not_exists(&from, &to, &target).await }
    }))
    .buffer_unordered(CLONE_COPY_CONCURRENCY)
    .try_collect::<Vec<_>>()
    .await?;

    Ok(manifest)
}

fn clone_copy_map(
    source: &str,
    target: &str,
    manifest: &Manifest,
) -> Result<BTreeMap<String, String>, ZeppelinError> {
    reachable_keys(source, manifest)
        .into_iter()
        .map(|source_key| {
            let target_key = rewrite_namespace_key(source, target, &source_key)?;
            Ok((source_key, target_key))
        })
        .collect()
}

fn rewrite_manifest_stored_keys(
    source: &str,
    target: &str,
    manifest: &mut Manifest,
) -> Result<(), ZeppelinError> {
    for segment in &mut manifest.segments {
        if let Some(sketch) = &mut segment.sketch {
            sketch.key = rewrite_namespace_key(source, target, &sketch.key)?;
        }
        if let Some(bootstrap) = &mut segment.bootstrap {
            bootstrap.key = rewrite_namespace_key(source, target, &bootstrap.key)?;
        }
        if let Some(membership) = &mut segment.membership {
            membership.key = rewrite_namespace_key(source, target, &membership.key)?;
        }
        for object_ref in &mut segment.cluster_objects {
            object_ref.key = rewrite_namespace_key(source, target, &object_ref.key)?;
        }
    }
    Ok(())
}

fn rewrite_namespace_key(source: &str, target: &str, key: &str) -> Result<String, ZeppelinError> {
    let source_prefix = format!("{source}/");
    let suffix = key.strip_prefix(&source_prefix).ok_or_else(|| {
        ZeppelinError::Index(format!(
            "clone source manifest key {key:?} is outside source prefix {source_prefix:?}"
        ))
    })?;
    Ok(format!("{target}/{suffix}"))
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
) -> Result<(StatusCode, Json<CompactNamespaceResponse>), ApiError> {
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
        return Ok((
            StatusCode::OK,
            Json(CompactNamespaceResponse::from_status(
                "noop",
                compaction_status_from_manifest(&ns, &before),
            )),
        ));
    }

    let lease = state
        .lease_manager
        .acquire(&ns)
        .await
        .map_err(ApiError::from)?;
    info!(
        namespace = %ns,
        fencing_token = lease.fencing_token,
        "manual compaction accepted"
    );

    let compactor = state.compactor.clone();
    let lease_manager = state.lease_manager.clone();
    let manifest_cache = state.manifest_cache.clone();
    let ns_for_task = ns.clone();
    let fts_configs = meta.full_text_search.clone();
    tokio::spawn(async move {
        match run_compaction_with_lease(
            &compactor,
            &lease_manager,
            &ns_for_task,
            lease,
            &fts_configs,
        )
        .await
        {
            Ok(result) => {
                manifest_cache.invalidate(&ns_for_task);
                info!(
                    namespace = %ns_for_task,
                    vectors_compacted = result.vectors_compacted,
                    fragments_removed = result.fragments_removed,
                    "manual compaction completed"
                );
            }
            Err(e) => {
                manifest_cache.invalidate(&ns_for_task);
                tracing::error!(
                    namespace = %ns_for_task,
                    error = %e,
                    "manual compaction failed"
                );
            }
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(CompactNamespaceResponse::from_status(
            "accepted",
            compaction_status_from_manifest(&ns, &before),
        )),
    ))
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
