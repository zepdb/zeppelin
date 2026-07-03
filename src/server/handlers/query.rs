use axum::extract::{Path, State};
use axum::Json;
use serde::Deserialize;
use tracing::{info, instrument};

use crate::error::ZeppelinError;
use crate::fts::rank_by::RankBy;
use crate::query;
use crate::query::QueryResponse;
use crate::server::AppState;
use crate::types::{ConsistencyLevel, Filter};

use super::ApiError;

/// Request body for querying vectors by ANN or BM25 ranking.
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
    /// Maximum number of results to return (defaults to server config).
    #[serde(default)]
    pub top_k: Option<usize>,
    /// Optional attribute filter applied before ranking.
    #[serde(default)]
    pub filter: Option<Filter>,
    /// Read consistency level (eventual or strong).
    #[serde(default)]
    pub consistency: ConsistencyLevel,
    /// Number of IVF clusters to probe (defaults to server config).
    #[serde(default)]
    pub nprobe: Option<usize>,
}

/// Query handler using direct serde_json deserialization (skips Axum's
/// serde_path_to_error wrapper which adds 18-26% CPU overhead per query).
#[instrument(skip(state, body), fields(namespace = %ns))]
pub async fn query_namespace(
    State(state): State<AppState>,
    Path(ns): Path<String>,
    body: bytes::Bytes,
) -> Result<Json<QueryResponse>, ApiError> {
    let req: QueryRequest = serde_json::from_slice(&body).map_err(|e| {
        ApiError(ZeppelinError::Validation(format!(
            "invalid request body: {e}"
        )))
    })?;

    // ---- Phase 1: request-shape validation (NO I/O, needs no metadata) ----
    // Runs BEFORE namespace resolution so a malformed request to a missing
    // namespace is a 400 (bad request), not a 404 (Task 14 I2). Also runs
    // BEFORE the query metrics increment so rejected requests aren't counted
    // as queries (I3).
    let top_k = req.top_k.unwrap_or(state.config.server.default_top_k);

    // Exactly one of vector or rank_by must be provided.
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

    // top_k bounds (api yaml: minimum 1, maximum max_top_k).
    if top_k == 0 {
        return Err(ApiError(ZeppelinError::Validation(
            "top_k must be >= 1".into(),
        )));
    }
    if top_k > state.config.server.max_top_k {
        return Err(ApiError(ZeppelinError::Validation(format!(
            "top_k {} exceeds maximum of {}",
            top_k, state.config.server.max_top_k
        ))));
    }

    // nprobe bounds (api yaml: minimum 1, maximum max_nprobe). Vector-search
    // only, but the bound is a request-shape property so it's validated here
    // regardless of path: nprobe:0 previously slipped through and probed zero
    // clusters, returning an empty 200 (Task 14 I1).
    let nprobe = req.nprobe.unwrap_or(state.config.indexing.default_nprobe);
    if let Some(requested) = req.nprobe {
        if requested == 0 {
            return Err(ApiError(ZeppelinError::Validation(
                "nprobe must be >= 1".into(),
            )));
        }
    }
    if nprobe > state.config.indexing.max_nprobe {
        return Err(ApiError(ZeppelinError::Validation(format!(
            "nprobe {} exceeds maximum of {}",
            nprobe, state.config.indexing.max_nprobe
        ))));
    }

    // ---- Phase 2: metrics (only requests that passed shape validation) ----
    let start = std::time::Instant::now();
    let ns_for_metrics = ns.clone();
    let _duration_guard = DurationGuard {
        start,
        namespace: ns_for_metrics,
    };
    crate::metrics::ACTIVE_QUERIES.inc();
    let _guard = crate::metrics::GaugeGuard(&crate::metrics::ACTIVE_QUERIES);
    crate::metrics::QUERIES_TOTAL
        .with_label_values(&[&ns])
        .inc();

    // ---- Phase 3: namespace resolution, then metadata-dependent checks ----
    let meta = state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;

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
            Some(&state.fts_cache),
            state.config.indexing.bm25_max_full_scan_clusters,
        )
        .await
        .map_err(ApiError::from)?
    } else {
        // Vector query path
        let vector = req.vector.as_ref().ok_or_else(|| {
            ApiError(ZeppelinError::Validation(
                "vector must be provided for ANN search".into(),
            ))
        })?;
        if vector.len() != meta.dimensions {
            return Err(ApiError(ZeppelinError::DimensionMismatch {
                expected: meta.dimensions,
                actual: vector.len(),
            }));
        }
        // Reject NaN/inf: non-finite query values make every distance
        // comparison nondeterministic (partial_cmp falls back to Equal).
        // Single is_finite() pass over an already-deserialized slice — keeps
        // the direct-serde fast path intact.
        if let Some((dim_idx, kind)) = super::find_non_finite(vector) {
            return Err(ApiError(ZeppelinError::Validation(format!(
                "query vector contains a non-finite value ({kind}) at dimension {dim_idx}"
            ))));
        }

        // `nprobe` was validated (>= 1, <= max) in Phase 1.
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
