use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

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
        }
    }
}

pub async fn create_namespace(
    State(state): State<AppState>,
    Json(req): Json<CreateNamespaceRequest>,
) -> Result<(StatusCode, Json<NamespaceResponse>), ApiError> {
    let meta = state
        .namespace_manager
        .create(&req.name, req.dimensions, req.distance_metric)
        .await
        .map_err(ApiError::from)?;

    Ok((StatusCode::CREATED, Json(NamespaceResponse::from(meta))))
}

pub async fn list_namespaces(
    State(state): State<AppState>,
) -> Result<Json<Vec<NamespaceResponse>>, ApiError> {
    let namespaces = state
        .namespace_manager
        .list(None)
        .await
        .map_err(ApiError::from)?;

    let responses: Vec<NamespaceResponse> = namespaces.into_iter().map(Into::into).collect();
    Ok(Json(responses))
}

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

pub async fn delete_namespace(
    State(state): State<AppState>,
    Path(ns): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .namespace_manager
        .delete(&ns)
        .await
        .map_err(ApiError::from)?;

    Ok(StatusCode::NO_CONTENT)
}
