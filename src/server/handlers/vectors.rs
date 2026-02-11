use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

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

#[instrument(skip(state, req), fields(namespace = %ns, vector_count = req.vectors.len()))]
pub async fn upsert_vectors(
    State(state): State<AppState>,
    Path(ns): Path<String>,
    Json(req): Json<UpsertVectorsRequest>,
) -> Result<(StatusCode, Json<UpsertVectorsResponse>), ApiError> {
    if req.vectors.is_empty() {
        return Err(ApiError(ZeppelinError::Validation(
            "vectors array cannot be empty".into(),
        )));
    }
    if req.vectors.len() > state.config.server.max_batch_size {
        return Err(ApiError(ZeppelinError::Validation(format!(
            "batch size {} exceeds maximum of {}",
            req.vectors.len(),
            state.config.server.max_batch_size
        ))));
    }

    for vec in &req.vectors {
        if vec.id.is_empty() {
            return Err(ApiError(ZeppelinError::Validation(
                "vector id cannot be empty".into(),
            )));
        }
        if vec.id.len() > state.config.server.max_vector_id_length {
            return Err(ApiError(ZeppelinError::Validation(format!(
                "vector id length {} exceeds maximum of {}",
                vec.id.len(),
                state.config.server.max_vector_id_length
            ))));
        }
    }

    info!(count = req.vectors.len(), "upserting vectors");

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

    info!(upserted = count, "vectors upserted");
    Ok((
        StatusCode::OK,
        Json(UpsertVectorsResponse { upserted: count }),
    ))
}

#[instrument(skip(state, req), fields(namespace = %ns, delete_count = req.ids.len()))]
pub async fn delete_vectors(
    State(state): State<AppState>,
    Path(ns): Path<String>,
    Json(req): Json<DeleteVectorsRequest>,
) -> Result<Json<DeleteVectorsResponse>, ApiError> {
    info!(count = req.ids.len(), "deleting vectors");

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

    info!(deleted = count, "vectors deleted");
    Ok(Json(DeleteVectorsResponse { deleted: count }))
}
