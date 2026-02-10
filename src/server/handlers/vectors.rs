use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::ZeppelinError;
use crate::server::AppState;
use crate::types::{VectorEntry, VectorId};

use super::ApiError;

#[derive(Debug, Deserialize)]
pub struct UpsertVectorsRequest {
    pub vectors: Vec<VectorEntry>,
}

#[derive(Debug, Serialize)]
pub struct UpsertVectorsResponse {
    pub upserted: usize,
}

#[derive(Debug, Deserialize)]
pub struct DeleteVectorsRequest {
    pub ids: Vec<VectorId>,
}

#[derive(Debug, Serialize)]
pub struct DeleteVectorsResponse {
    pub deleted: usize,
}

pub async fn upsert_vectors(
    State(state): State<AppState>,
    Path(ns): Path<String>,
    Json(req): Json<UpsertVectorsRequest>,
) -> Result<(StatusCode, Json<UpsertVectorsResponse>), ApiError> {
    // Validate namespace exists and check dimensions
    let meta = state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;

    for vec in &req.vectors {
        if vec.values.len() != meta.dimensions {
            return Err(ApiError(ZeppelinError::DimensionMismatch {
                expected: meta.dimensions,
                actual: vec.values.len(),
            }));
        }
    }

    let count = req.vectors.len();
    state
        .wal_writer
        .append(&ns, req.vectors, vec![])
        .await
        .map_err(ApiError::from)?;

    Ok((
        StatusCode::OK,
        Json(UpsertVectorsResponse { upserted: count }),
    ))
}

pub async fn delete_vectors(
    State(state): State<AppState>,
    Path(ns): Path<String>,
    Json(req): Json<DeleteVectorsRequest>,
) -> Result<Json<DeleteVectorsResponse>, ApiError> {
    // Validate namespace exists
    state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;

    let count = req.ids.len();
    state
        .wal_writer
        .append(&ns, vec![], req.ids)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(DeleteVectorsResponse { deleted: count }))
}
