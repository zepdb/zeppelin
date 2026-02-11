use axum::extract::{Path, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

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

#[instrument(skip(state, req), fields(namespace = %ns, top_k = req.top_k))]
pub async fn query_namespace(
    State(state): State<AppState>,
    Path(ns): Path<String>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    let start = std::time::Instant::now();
    crate::metrics::ACTIVE_QUERIES.inc();
    let _guard = crate::metrics::GaugeGuard(&crate::metrics::ACTIVE_QUERIES);
    crate::metrics::QUERIES_TOTAL.with_label_values(&[&ns]).inc();

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

    if req.top_k == 0 {
        return Err(ApiError(ZeppelinError::Validation(
            "top_k must be > 0".into(),
        )));
    }
    if req.top_k > state.config.server.max_top_k {
        return Err(ApiError(ZeppelinError::Validation(format!(
            "top_k {} exceeds maximum of {}",
            req.top_k, state.config.server.max_top_k
        ))));
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
        Some(&state.cache),
    )
    .await
    .map_err(ApiError::from)?;

    let elapsed = start.elapsed();
    crate::metrics::QUERY_DURATION
        .with_label_values(&[&ns])
        .observe(elapsed.as_secs_f64());

    info!(
        results = result.results.len(),
        scanned_fragments = result.scanned_fragments,
        scanned_segments = result.scanned_segments,
        elapsed_ms = elapsed.as_millis(),
        "query complete"
    );

    Ok(Json(result))
}
