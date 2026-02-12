use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

use crate::error::ZeppelinError;
use crate::fts::types::FtsFieldConfig;
use crate::namespace::manager::NamespaceMetadata;
use crate::server::AppState;
use crate::types::DistanceMetric;

use super::ApiError;

#[derive(Debug, Deserialize)]
pub struct CreateNamespaceRequest {
    pub name: String,
    pub dimensions: usize,
    #[serde(default = "default_distance_metric")]
    pub distance_metric: DistanceMetric,
    #[serde(default)]
    pub full_text_search: std::collections::HashMap<String, FtsFieldConfig>,
}

fn default_distance_metric() -> DistanceMetric {
    DistanceMetric::Cosine
}

#[derive(Debug, Serialize)]
pub struct NamespaceResponse {
    pub name: String,
    pub dimensions: usize,
    pub distance_metric: DistanceMetric,
    pub vector_count: u64,
    pub created_at: String,
    pub updated_at: String,
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub full_text_search: std::collections::HashMap<String, FtsFieldConfig>,
}

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

#[instrument(skip(state), fields(namespace = %req.name, dimensions = req.dimensions))]
pub async fn create_namespace(
    State(state): State<AppState>,
    Json(req): Json<CreateNamespaceRequest>,
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

    info!(namespace = %ns, "namespace deleted");
    Ok(StatusCode::NO_CONTENT)
}
