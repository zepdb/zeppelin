use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};
use uuid::Uuid;

use crate::error::ZeppelinError;
use crate::fts::types::FtsFieldConfig;
use crate::namespace::manager::NamespaceMetadata;
use crate::server::AppState;
use crate::types::DistanceMetric;

use super::ApiError;

/// Request body for creating a new namespace (server generates UUID name).
#[derive(Debug, Deserialize)]
pub struct CreateNamespaceRequest {
    /// Dimensionality of vectors stored in this namespace.
    pub dimensions: usize,
    /// Distance metric for similarity search (defaults to Cosine).
    #[serde(default = "default_distance_metric")]
    pub distance_metric: DistanceMetric,
    /// Full-text search field configurations (empty map disables FTS).
    #[serde(default)]
    pub full_text_search: std::collections::HashMap<String, FtsFieldConfig>,
}

/// Legacy request body that includes a user-specified name (kept for rollback).
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct CreateNamespaceWithNameRequest {
    /// Unique namespace name.
    pub name: String,
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
    /// Full-text search field configurations (omitted when empty).
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub full_text_search: std::collections::HashMap<String, FtsFieldConfig>,
}

/// Response body for namespace creation (includes warning about saving UUID).
#[derive(Debug, Serialize)]
pub struct CreateNamespaceResponse {
    /// Namespace metadata.
    #[serde(flatten)]
    pub namespace: NamespaceResponse,
    /// Warning to save the UUID — it cannot be recovered.
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
            full_text_search: meta.full_text_search,
        }
    }
}

/// Creates a new namespace with a server-generated UUID v4 name.
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

    let name = Uuid::new_v4().to_string();

    info!(namespace = %name, dimensions = req.dimensions, "creating namespace");
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

    info!(namespace = %name, "namespace created");
    Ok((
        StatusCode::CREATED,
        Json(CreateNamespaceResponse {
            namespace: NamespaceResponse::from(meta),
            warning: "Save this namespace name. It cannot be recovered if lost.".to_string(),
        }),
    ))
}

/// Legacy handler: creates a namespace with a user-specified name (not routed, kept for rollback).
#[allow(dead_code)]
#[instrument(skip(state), fields(namespace = %req.name, dimensions = req.dimensions))]
pub async fn create_namespace_with_name(
    State(state): State<AppState>,
    Json(req): Json<CreateNamespaceWithNameRequest>,
) -> Result<(StatusCode, Json<NamespaceResponse>), ApiError> {
    if req.dimensions == 0 || req.dimensions > state.config.server.max_dimensions {
        return Err(ApiError(ZeppelinError::Validation(format!(
            "dimensions {} must be between 1 and {}",
            req.dimensions, state.config.server.max_dimensions
        ))));
    }

    info!(namespace = %req.name, dimensions = req.dimensions, "creating namespace");
    let meta = state
        .namespace_manager
        .create_with_fts(
            &req.name,
            req.dimensions,
            req.distance_metric,
            req.full_text_search,
        )
        .await
        .map_err(ApiError::from)?;

    info!(namespace = %req.name, "namespace created");
    Ok((StatusCode::CREATED, Json(NamespaceResponse::from(meta))))
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
        .get(&ns)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(NamespaceResponse::from(meta)))
}

/// Deletes a namespace and cleans up associated in-memory state.
#[instrument(skip(state), fields(namespace = %ns))]
pub async fn delete_namespace(
    State(state): State<AppState>,
    Path(ns): Path<String>,
) -> Result<StatusCode, ApiError> {
    info!(namespace = %ns, "deleting namespace");
    state
        .namespace_manager
        .delete(&ns)
        .await
        .map_err(ApiError::from)?;

    // Clean up per-namespace in-memory state to prevent unbounded growth
    state.wal_writer.remove_lock(&ns);
    state.manifest_cache.invalidate(&ns);

    info!(namespace = %ns, "namespace deleted");
    Ok(StatusCode::NO_CONTENT)
}
