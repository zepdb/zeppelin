use axum::extract::{Path, State};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::ZeppelinError;
use crate::query;
use crate::server::AppState;
use crate::types::{ConsistencyLevel, Filter, SearchResult};

use super::ApiError;

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub vector: Vec<f32>,
    #[serde(default = "default_top_k")]
    pub top_k: usize,
    #[serde(default)]
    pub filter: Option<Filter>,
    #[serde(default)]
    pub consistency: ConsistencyLevel,
    #[serde(default)]
    pub nprobe: Option<usize>,
}

fn default_top_k() -> usize {
    10
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub results: Vec<SearchResult>,
    pub scanned_fragments: usize,
    pub scanned_segments: usize,
}

pub async fn query_namespace(
    State(state): State<AppState>,
    Path(ns): Path<String>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    let meta = state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;

    if req.vector.len() != meta.dimensions {
        return Err(ApiError(ZeppelinError::DimensionMismatch {
            expected: meta.dimensions,
            actual: req.vector.len(),
        }));
    }

    let nprobe = req
        .nprobe
        .unwrap_or(state.config.indexing.default_nprobe)
        .min(state.config.indexing.max_nprobe);

    let result = query::execute_query(
        &state.store,
        &state.wal_reader,
        &ns,
        &req.vector,
        req.top_k,
        nprobe,
        req.filter.as_ref(),
        req.consistency,
        meta.distance_metric,
        state.config.indexing.oversample_factor,
    )
    .await
    .map_err(ApiError::from)?;

    Ok(Json(result))
}
