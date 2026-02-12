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
    #[serde(default)]
    pub top_k: Option<usize>,
    #[serde(default)]
    pub filter: Option<Filter>,
    #[serde(default)]
    pub consistency: ConsistencyLevel,
    #[serde(default)]
    pub nprobe: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub results: Vec<SearchResult>,
    pub scanned_fragments: usize,
    pub scanned_segments: usize,
}

#[instrument(skip(state, req), fields(namespace = %ns))]
pub async fn query_namespace(
    State(state): State<AppState>,
    Path(ns): Path<String>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    let start = std::time::Instant::now();
    let ns_for_metrics = ns.clone();
    let _duration_guard = DurationGuard {
        start,
        namespace: ns_for_metrics,
    };
    let top_k = req.top_k.unwrap_or(state.config.server.default_top_k);
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

    if top_k == 0 {
        return Err(ApiError(ZeppelinError::Validation(
            "top_k must be > 0".into(),
        )));
    }
    if top_k > state.config.server.max_top_k {
        return Err(ApiError(ZeppelinError::Validation(format!(
            "top_k {} exceeds maximum of {}",
            top_k, state.config.server.max_top_k
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
            top_k,
            req.filter.as_ref(),
            req.consistency,
            req.last_as_prefix,
            Some(&state.manifest_cache),
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

        let nprobe = req.nprobe.unwrap_or(state.config.indexing.default_nprobe);
        if nprobe > state.config.indexing.max_nprobe {
            return Err(ApiError(ZeppelinError::Validation(format!(
                "nprobe {} exceeds maximum of {}",
                nprobe, state.config.indexing.max_nprobe
            ))));
        }

        query::execute_query(query::QueryParams {
            store: &state.store,
            wal_reader: &state.wal_reader,
            namespace: &ns,
            query: vector,
            top_k,
            nprobe,
            filter: req.filter.as_ref(),
            consistency: req.consistency,
            distance_metric: meta.distance_metric,
            oversample_factor: state.config.indexing.oversample_factor,
            cache: Some(&state.cache),
            manifest_cache: Some(&state.manifest_cache),
        })
        .await
        .map_err(ApiError::from)?
    };

    info!(
        results = result.results.len(),
        scanned_fragments = result.scanned_fragments,
        scanned_segments = result.scanned_segments,
        "query complete"
    );

    Ok(Json(result))
}

/// RAII guard that records query duration on drop (including error paths).
struct DurationGuard {
    start: std::time::Instant,
    namespace: String,
}

impl Drop for DurationGuard {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        crate::metrics::QUERY_DURATION
            .with_label_values(&[&self.namespace])
            .observe(elapsed.as_secs_f64());
    }
}
