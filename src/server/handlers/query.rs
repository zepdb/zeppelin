use axum::extract::{Path, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

use crate::error::ZeppelinError;
use crate::fts::rank_by::RankBy;
use crate::query;
use crate::server::AppState;
use crate::types::{ConsistencyLevel, Filter, SearchResult};

use super::ApiError;

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    /// Vector for ANN search. Required unless `rank_by` is provided.
    #[serde(default)]
    pub vector: Option<Vec<f32>>,
    /// BM25 ranking expression. Required unless `vector` is provided.
    #[serde(default)]
    pub rank_by: Option<RankBy>,
    /// Whether the last token of each BM25 query should be treated as a prefix.
    #[serde(default)]
    pub last_as_prefix: bool,
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
    crate::metrics::QUERIES_TOTAL
        .with_label_values(&[&ns])
        .inc();

    // Exactly one of vector or rank_by must be provided
    if req.vector.is_none() && req.rank_by.is_none() {
        return Err(ApiError(ZeppelinError::Validation(
            "exactly one of 'vector' or 'rank_by' must be provided".into(),
        )));
    }
    if req.vector.is_some() && req.rank_by.is_some() {
        return Err(ApiError(ZeppelinError::Validation(
            "cannot provide both 'vector' and 'rank_by'".into(),
        )));
    }

    let meta = state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;

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

    let result = if let Some(ref rank_by) = req.rank_by {
        // BM25 query path
        // Validate all referenced fields are configured
        for (field, _) in rank_by.extract_field_queries() {
            if !meta.full_text_search.contains_key(&field) {
                return Err(ApiError(ZeppelinError::FtsFieldNotConfigured {
                    namespace: ns.clone(),
                    field,
                }));
            }
        }

        crate::metrics::FTS_QUERIES_TOTAL
            .with_label_values(&[&ns])
            .inc();

        query::execute_bm25_query(
            &state.store,
            &state.wal_reader,
            &ns,
            rank_by,
            &meta.full_text_search,
            req.top_k,
            req.filter.as_ref(),
            req.consistency,
            req.last_as_prefix,
        )
        .await
        .map_err(ApiError::from)?
    } else {
        // Vector query path
        let vector = req.vector.as_ref().unwrap();
        if vector.len() != meta.dimensions {
            return Err(ApiError(ZeppelinError::DimensionMismatch {
                expected: meta.dimensions,
                actual: vector.len(),
            }));
        }

        let nprobe = req
            .nprobe
            .unwrap_or(state.config.indexing.default_nprobe)
            .min(state.config.indexing.max_nprobe);

        query::execute_query(
            &state.store,
            &state.wal_reader,
            &ns,
            vector,
            req.top_k,
            nprobe,
            req.filter.as_ref(),
            req.consistency,
            meta.distance_metric,
            state.config.indexing.oversample_factor,
            Some(&state.cache),
        )
        .await
        .map_err(ApiError::from)?
    };

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
