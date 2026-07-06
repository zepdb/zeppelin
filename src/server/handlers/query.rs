use std::net::SocketAddr;
use std::time::Instant;

use axum::extract::{ConnectInfo, Path, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

use crate::error::ZeppelinError;
use crate::fts::rank_by::RankBy;
use crate::namespace::manager::NamespaceMetadata;
use crate::query;
use crate::query::QueryResponse;
use crate::runtime_config::QueryKnobs;
use crate::server::AppState;
use crate::types::{ConsistencyLevel, Filter};
use crate::wal::Manifest;

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
    /// Whether result attributes should be included. Defaults to true.
    #[serde(default)]
    pub include_attributes: Option<bool>,
}

/// Request body for batch querying vectors or BM25 expressions.
#[derive(Debug, Deserialize)]
pub struct BatchQueryRequest {
    /// Positional list of query requests. Each entry uses the single-query body.
    pub queries: Vec<QueryRequest>,
}

#[derive(Debug, Clone, Copy)]
struct ValidatedQuery {
    top_k: usize,
    nprobe: usize,
    include_attributes: bool,
}

/// Response body for a batch query request.
#[derive(Debug, Serialize)]
pub struct BatchQueryResponse {
    /// Positional entry responses matching the request's `queries` order.
    pub results: Vec<BatchQueryEntry>,
}

/// One positional batch query entry result.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum BatchQueryEntry {
    /// Successful entry with the same response body as the single-query route.
    Success {
        /// Always `true` for successful entries.
        ok: bool,
        /// Single-query-compatible response for this entry.
        response: QueryResponse,
        /// Per-entry metadata.
        metadata: BatchQueryEntryMetadata,
    },
    /// Failed entry with a canonical error envelope.
    Error {
        /// Always `false` for failed entries.
        ok: bool,
        /// Error envelope for this entry.
        error: BatchQueryError,
        /// Per-entry metadata.
        metadata: BatchQueryEntryMetadata,
    },
}

/// Metadata attached to each batch query entry.
#[derive(Debug, Serialize)]
pub struct BatchQueryEntryMetadata {
    /// Server-side latency for this entry, measured independently.
    pub latency_ms: u64,
}

/// Canonical error envelope embedded in a failed batch query entry.
#[derive(Debug, Serialize)]
pub struct BatchQueryError {
    /// Stable machine-readable error code.
    pub code: &'static str,
    /// Client-safe human-readable error message.
    pub error: String,
    /// HTTP status code that this entry would have returned as a single query.
    pub status: u16,
    /// Whether retrying the same entry unchanged is reasonable.
    pub retryable: bool,
}

impl BatchQueryError {
    fn from_error(err: &ZeppelinError) -> Self {
        Self {
            code: err.error_code(),
            error: err.client_message(),
            status: err.status_code(),
            retryable: err.retryable(),
        }
    }
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
    let knobs = state.runtime_query_config.snapshot();

    // ---- Phase 1: request-shape validation (NO I/O, needs no metadata) ----
    // Runs BEFORE namespace resolution so a malformed request to a missing
    // namespace is a 400 (bad request), not a 404 (Task 14 I2). Also runs
    // BEFORE the query metrics increment so rejected requests aren't counted
    // as queries (I3).
    let validated = validate_query_shape(&req, knobs.as_ref(), &state).map_err(ApiError::from)?;

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

    let result = execute_validated_query(&state, &ns, &meta, &req, validated, knobs.as_ref(), None)
        .await
        .map_err(ApiError::from)?;

    info!(
        results = result.results.len(),
        scanned_fragments = result.scanned_fragments,
        scanned_segments = result.scanned_segments,
        "query complete"
    );

    Ok(Json(result))
}

/// Batch query handler using direct serde_json deserialization.
#[instrument(skip(state, body), fields(namespace = %ns))]
pub async fn batch_query_namespace(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path(ns): Path<String>,
    body: bytes::Bytes,
) -> Result<Json<BatchQueryResponse>, ApiError> {
    let req: BatchQueryRequest = serde_json::from_slice(&body).map_err(|e| {
        ApiError(ZeppelinError::Validation(format!(
            "invalid request body: {e}"
        )))
    })?;
    if req.queries.is_empty() {
        return Err(ApiError(ZeppelinError::Validation(
            "queries must not be empty".into(),
        )));
    }
    if req.queries.len() > state.config.server.max_query_batch_size {
        return Err(ApiError(ZeppelinError::PayloadTooLarge {
            resource: "query batch",
            actual: req.queries.len(),
            limit: state.config.server.max_query_batch_size,
        }));
    }

    crate::server::consume_rate_limit(
        &state,
        addr.ip(),
        req.queries.len().saturating_sub(1) as u64,
    )
    .map_err(ApiError::from)?;

    let knobs = state.runtime_query_config.snapshot();
    let validations: Vec<Result<ValidatedQuery, ZeppelinError>> = req
        .queries
        .iter()
        .map(|query| validate_query_shape(query, knobs.as_ref(), &state))
        .collect();
    for _ in validations.iter().filter(|validation| validation.is_ok()) {
        crate::metrics::QUERIES_TOTAL
            .with_label_values(&[&ns])
            .inc();
    }

    let meta = state.namespace_manager.get(&ns).await;
    let meta = match meta {
        Ok(meta) => meta,
        Err(err) => {
            let results = validations
                .into_iter()
                .map(|validation| {
                    let start = Instant::now();
                    match validation {
                        Ok(_) => batch_error_entry(&err, start),
                        Err(validation_err) => batch_error_entry(&validation_err, start),
                    }
                })
                .collect();
            return Ok(Json(BatchQueryResponse { results }));
        }
    };

    let manifest = match validations.iter().any(Result::is_ok) {
        true => {
            let consistency = strongest_consistency(&req.queries, &validations);
            match query::read_manifest_for_query(
                &state.store,
                &ns,
                consistency,
                Some(&state.manifest_cache),
            )
            .await
            {
                Ok(manifest) => Some(manifest),
                Err(err) => {
                    let results = validations
                        .into_iter()
                        .map(|validation| {
                            let start = Instant::now();
                            match validation {
                                Ok(_) => batch_error_entry(&err, start),
                                Err(validation_err) => batch_error_entry(&validation_err, start),
                            }
                        })
                        .collect();
                    return Ok(Json(BatchQueryResponse { results }));
                }
            }
        }
        false => None,
    };

    let mut results = Vec::with_capacity(req.queries.len());
    for (idx, query_req) in req.queries.iter().enumerate() {
        let start = Instant::now();
        let entry = match &validations[idx] {
            Ok(validated) => {
                match execute_validated_query(
                    &state,
                    &ns,
                    &meta,
                    query_req,
                    *validated,
                    knobs.as_ref(),
                    manifest.clone(),
                )
                .await
                {
                    Ok(response) => batch_success_entry(response, start),
                    Err(err) => batch_error_entry(&err, start),
                }
            }
            Err(err) => batch_error_entry(err, start),
        };
        results.push(entry);
    }

    Ok(Json(BatchQueryResponse { results }))
}

fn validate_query_shape(
    req: &QueryRequest,
    knobs: &QueryKnobs,
    state: &AppState,
) -> Result<ValidatedQuery, ZeppelinError> {
    let top_k = req.top_k.unwrap_or(knobs.default_top_k);
    let include_attributes = req.include_attributes.unwrap_or(true);

    // Exactly one of vector or rank_by must be provided.
    if req.vector.is_none() && req.rank_by.is_none() {
        return Err(ZeppelinError::Validation(
            "exactly one of 'vector' or 'rank_by' must be provided".into(),
        ));
    }
    if req.vector.is_some() && req.rank_by.is_some() {
        return Err(ZeppelinError::Validation(
            "cannot provide both 'vector' and 'rank_by'".into(),
        ));
    }

    // top_k bounds (api yaml: minimum 1, maximum max_top_k).
    if top_k == 0 {
        return Err(ZeppelinError::Validation("top_k must be >= 1".into()));
    }
    if top_k > state.config.server.max_top_k {
        return Err(ZeppelinError::Validation(format!(
            "top_k {} exceeds maximum of {}",
            top_k, state.config.server.max_top_k
        )));
    }

    // nprobe bounds (api yaml: minimum 1, maximum max_nprobe). Vector-search
    // only, but the bound is a request-shape property so it's validated here
    // regardless of path: nprobe:0 previously slipped through and probed zero
    // clusters, returning an empty 200 (Task 14 I1).
    let nprobe = req.nprobe.unwrap_or(knobs.default_nprobe);
    if let Some(requested) = req.nprobe {
        if requested == 0 {
            return Err(ZeppelinError::Validation("nprobe must be >= 1".into()));
        }
    }
    if nprobe > state.config.indexing.max_nprobe {
        return Err(ZeppelinError::Validation(format!(
            "nprobe {} exceeds maximum of {}",
            nprobe, state.config.indexing.max_nprobe
        )));
    }

    Ok(ValidatedQuery {
        top_k,
        nprobe,
        include_attributes,
    })
}

async fn execute_validated_query(
    state: &AppState,
    ns: &str,
    meta: &NamespaceMetadata,
    req: &QueryRequest,
    validated: ValidatedQuery,
    knobs: &QueryKnobs,
    manifest: Option<Manifest>,
) -> Result<QueryResponse, ZeppelinError> {
    if let Some(ref rank_by) = req.rank_by {
        // BM25 query path
        // Validate all referenced fields are configured.
        for (field, _) in rank_by.extract_field_queries() {
            if !meta.full_text_search.contains_key(&field) {
                return Err(ZeppelinError::FtsFieldNotConfigured {
                    namespace: ns.to_string(),
                    field,
                });
            }
        }

        crate::metrics::FTS_QUERIES_TOTAL
            .with_label_values(&[ns])
            .inc();

        return match manifest {
            Some(manifest) => {
                query::execute_bm25_query_with_manifest(
                    &state.store,
                    &state.wal_reader,
                    ns,
                    rank_by,
                    &meta.full_text_search,
                    validated.top_k,
                    req.filter.as_ref(),
                    req.consistency,
                    req.last_as_prefix,
                    Some(&state.fts_cache),
                    Some(&state.cache),
                    knobs.bm25_max_full_scan_clusters,
                    validated.include_attributes,
                    manifest,
                )
                .await
            }
            None => {
                query::execute_bm25_query(
                    &state.store,
                    &state.wal_reader,
                    ns,
                    rank_by,
                    &meta.full_text_search,
                    validated.top_k,
                    req.filter.as_ref(),
                    req.consistency,
                    req.last_as_prefix,
                    Some(&state.manifest_cache),
                    Some(&state.fts_cache),
                    Some(&state.cache),
                    knobs.bm25_max_full_scan_clusters,
                    validated.include_attributes,
                )
                .await
            }
        };
    }

    // Vector query path
    let vector = req.vector.as_ref().ok_or_else(|| {
        ZeppelinError::Validation("vector must be provided for ANN search".into())
    })?;
    if vector.len() != meta.dimensions {
        return Err(ZeppelinError::DimensionMismatch {
            expected: meta.dimensions,
            actual: vector.len(),
        });
    }
    // Reject NaN/inf: non-finite query values make every distance comparison
    // nondeterministic (partial_cmp falls back to Equal).
    if let Some((dim_idx, kind)) = super::find_non_finite(vector) {
        return Err(ZeppelinError::Validation(format!(
            "query vector contains a non-finite value ({kind}) at dimension {dim_idx}"
        )));
    }

    let params = query::QueryParams {
        store: &state.store,
        wal_reader: &state.wal_reader,
        namespace: ns,
        query: vector,
        top_k: validated.top_k,
        nprobe: validated.nprobe,
        filter: req.filter.as_ref(),
        consistency: req.consistency,
        distance_metric: meta.distance_metric,
        oversample_factor: state.config.indexing.oversample_factor,
        rerank_coalesce_gap_bytes: knobs.rerank_coalesce_gap_bytes,
        cache: Some(&state.cache),
        manifest_cache: Some(&state.manifest_cache),
        include_attributes: validated.include_attributes,
    };

    match manifest {
        Some(manifest) => query::execute_query_with_manifest(params, manifest).await,
        None => query::execute_query(params).await,
    }
}

fn strongest_consistency(
    queries: &[QueryRequest],
    validations: &[Result<ValidatedQuery, ZeppelinError>],
) -> ConsistencyLevel {
    if queries
        .iter()
        .zip(validations.iter())
        .any(|(query, validation)| {
            validation.is_ok() && query.consistency == ConsistencyLevel::Strong
        })
    {
        ConsistencyLevel::Strong
    } else {
        ConsistencyLevel::Eventual
    }
}

fn batch_success_entry(response: QueryResponse, start: Instant) -> BatchQueryEntry {
    BatchQueryEntry::Success {
        ok: true,
        response,
        metadata: BatchQueryEntryMetadata {
            latency_ms: start.elapsed().as_millis() as u64,
        },
    }
}

fn batch_error_entry(err: &ZeppelinError, start: Instant) -> BatchQueryEntry {
    let status = err.status_code();
    if status >= 500 {
        tracing::error!(
            error = %err,
            code = err.error_code(),
            status,
            "batch query entry failed"
        );
    } else {
        tracing::warn!(
            error = %err,
            code = err.error_code(),
            status,
            "batch query entry failed"
        );
    }
    BatchQueryEntry::Error {
        ok: false,
        error: BatchQueryError::from_error(err),
        metadata: BatchQueryEntryMetadata {
            latency_ms: start.elapsed().as_millis() as u64,
        },
    }
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
