use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{info, instrument};
use uuid::Uuid;

use crate::error::ZeppelinError;
use crate::fts::FtsFieldConfig;
use crate::namespace::manager::{CreateNamespaceOutcome, NamespaceMetadata};
use crate::server::AppState;
use crate::types::DistanceMetric;

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
}

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

/// Response body for namespace creation.
#[derive(Debug, Serialize)]
pub struct CreateNamespaceResponse {
    /// Namespace metadata.
    #[serde(flatten)]
    pub namespace: NamespaceResponse,
    /// Creation note.
    pub warning: String,
}

/// Converts internal `NamespaceMetadata` into the API response representation.
impl From<NamespaceMetadata> for NamespaceResponse {
    fn from(meta: NamespaceMetadata) -> Self {
        Self {
            name: meta.name,
            dimensions: meta.dimensions,
            distance_metric: meta.distance_metric,
            vector_count: meta.vector_count,
            created_at: meta.created_at.to_rfc3339(),
            updated_at: meta.updated_at.to_rfc3339(),
            state: meta.state.as_str().to_string(),
            full_text_search: meta.full_text_search,
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

    if let Some(name) = req.name {
        info!(namespace = %name, dimensions = req.dimensions, "creating namespace by client name");
        let outcome = state
            .namespace_manager
            .create_idempotent_with_fts(
                &name,
                req.dimensions,
                req.distance_metric,
                req.full_text_search,
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
                namespace: NamespaceResponse::from(meta),
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
        .create_with_fts(
            &name,
            req.dimensions,
            req.distance_metric,
            req.full_text_search,
        )
        .await
        .map_err(ApiError::from)?;

    info!(namespace = %name, "generated namespace created");
    Ok((
        StatusCode::CREATED,
        Json(CreateNamespaceResponse {
            namespace: NamespaceResponse::from(meta),
            warning: "Save this namespace name. It cannot be recovered if lost.".to_string(),
        }),
    ))
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
    let responses: Vec<NamespaceResponse> = namespaces.into_iter().map(Into::into).collect();
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

    Ok(Json(NamespaceResponse::from(meta)))
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
    let active_segment = manifest.active_segment.as_ref()?;
    manifest
        .segments
        .iter()
        .find(|segment| segment.id == *active_segment)
        .cloned()
}
