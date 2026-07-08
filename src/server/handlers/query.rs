use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;

use axum::extract::{Extension, Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};
use xxhash_rust::xxh3::xxh3_64;

use crate::error::ZeppelinError;
use crate::fts::bm25::{self, Bm25Params};
use crate::fts::rank_by::RankBy;
use crate::fts::tokenizer::tokenize_text;
use crate::fts::FtsFieldConfig;
use crate::index::distance::compute_distance;
use crate::namespace::manager::NamespaceMetadata;
use crate::query;
use crate::query::{
    QueryDebug, QueryDebugCache, QueryExplain, QueryExplainCursor, QueryExplainFusion,
    QueryExplainGrouping, QueryExplainMode, QueryExplainPath, QueryExplainPlan,
    QueryExplainProjection, QueryExplainRerank, QueryExplainResult, QueryExplainResultSource,
    QueryExplainSource, QueryExplainSourceKind, QueryFacets, QueryResponse, QueryResultGroup,
};
use crate::runtime_config::QueryKnobs;
use crate::server::{AppState, RateLimitClass, RateLimitIdentity};
use crate::types::{AttributeValue, ConsistencyLevel, Filter, SearchResult};
use crate::wal::manifest::SegmentRef;
use crate::wal::Manifest;

use super::as_of;
use super::ApiError;

/// Request body for querying vectors by ANN or BM25 ranking.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRequest {
    /// Vector for legacy ANN search. Required unless `rank_by` or `sources` is provided.
    #[serde(default)]
    pub vector: Option<Vec<f32>>,
    /// BM25 ranking expression for legacy FTS search.
    /// Required unless `vector` or `sources` is provided.
    #[serde(default)]
    pub rank_by: Option<RankBy>,
    /// Whether the last token of each BM25 query should be treated as a prefix.
    #[serde(default)]
    pub last_as_prefix: Option<bool>,
    /// Maximum number of results to return (defaults to server config).
    #[serde(default)]
    pub top_k: Option<usize>,
    /// Per-source candidate count before multi-source fusion.
    #[serde(default)]
    pub candidate_k: Option<usize>,
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
    /// Typed retrieval-algebra candidate sources.
    #[serde(default)]
    pub sources: Option<Vec<CandidateSource>>,
    /// Multi-source fusion strategy.
    #[serde(default)]
    pub fusion: Option<FusionSpec>,
    /// Optional reranking strategy.
    #[serde(default)]
    pub rerank: Option<RerankSpec>,
    /// Optional grouping strategy.
    #[serde(default)]
    pub grouping: Option<GroupingSpec>,
    /// Attribute fields to facet over the filtered candidate frontier.
    #[serde(default)]
    pub facets: Option<Vec<FacetSpec>>,
    /// Response projection settings.
    #[serde(default)]
    pub projection: Option<ProjectionSpec>,
    /// Pagination cursor.
    #[serde(default)]
    pub cursor: Option<CursorSpec>,
    /// Explain output request.
    #[serde(default)]
    pub explain: Option<ExplainSpec>,
    /// Include an opt-in query diagnostics block in the response.
    #[serde(default)]
    pub debug: Option<bool>,
}

/// A typed candidate source in the retrieval-algebra request AST.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CandidateSource {
    /// ANN vector candidate source.
    Ann {
        /// Query vector for ANN search.
        #[serde(default)]
        vector: Option<Vec<f32>>,
        /// Stored vector ID to load and use as the ANN query vector.
        #[serde(default)]
        id: Option<String>,
        /// Number of IVF clusters to probe for this source.
        #[serde(default)]
        nprobe: Option<usize>,
    },
    /// BM25 full-text candidate source.
    Bm25 {
        /// BM25 ranking expression.
        rank_by: RankBy,
        /// Treat the last token of each BM25 query as a prefix.
        #[serde(default)]
        last_as_prefix: Option<bool>,
    },
}

/// Candidate fusion strategy in the retrieval-algebra request AST.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum FusionSpec {
    /// No fusion. Valid only with one source.
    None,
    /// Reciprocal rank fusion. Reserved for multi-source retrieval.
    Rrf {
        /// RRF smoothing constant.
        #[serde(default)]
        k: Option<usize>,
    },
    /// Weighted score fusion. Reserved for multi-source retrieval.
    Weighted {
        /// Per-source weights, in the same order as `sources`.
        weights: Vec<f32>,
    },
}

impl FusionSpec {
    fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }
}

/// Reranking strategy in the retrieval-algebra request AST.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RerankSpec {
    /// Use the engine default rerank behavior.
    Default,
    /// Disable explicit rerank. Vector execution still performs required exact rerank.
    None,
    /// Reserved for future vector reranking over a different vector.
    Vector {
        /// Reranking vector.
        vector: Vec<f32>,
    },
    /// Reserved for future BM25 reranking.
    Bm25 {
        /// BM25 reranking expression.
        rank_by: RankBy,
    },
}

impl RerankSpec {
    fn is_explicit(&self) -> bool {
        matches!(self, Self::Vector { .. } | Self::Bm25 { .. })
    }
}

/// Grouping strategy in the retrieval-algebra request AST.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum GroupingSpec {
    /// Do not group results.
    None,
    /// Reserved for field-based grouping.
    Field {
        /// Attribute field to group by.
        field: String,
        /// Maximum results per group.
        max_per_group: usize,
    },
}

/// Facet field request in the retrieval-algebra request AST.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(transparent)]
pub struct FacetSpec(String);

impl FacetSpec {
    fn field(&self) -> &str {
        &self.0
    }
}

/// Projection settings in the retrieval-algebra request AST.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionSpec {
    /// Whether result attributes should be included. Defaults to true.
    #[serde(default)]
    pub include_attributes: Option<bool>,
    /// Reserved for field-level attribute projection.
    #[serde(default)]
    pub fields: Option<Vec<String>>,
    /// Reserved for returning vectors in query results.
    #[serde(default)]
    pub include_vectors: Option<bool>,
}

/// Cursor strategy in the retrieval-algebra request AST.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CursorSpec {
    /// No cursor.
    None,
    /// Reserved for opaque continuation tokens.
    After {
        /// Opaque cursor token returned by a previous request.
        token: String,
    },
}

/// Explain request in the retrieval-algebra request AST.
#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum ExplainSpec {
    /// Boolean explain toggle.
    Flag(bool),
    /// Named explain mode.
    Mode(ExplainMode),
}

/// Explain mode in the retrieval-algebra request AST.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExplainMode {
    /// Do not include explain output.
    None,
    /// Reserved for physical/logical plan explain output.
    Plan,
    /// Reserved for per-result score/source details.
    Full,
}

/// Request body for batch querying vectors or BM25 expressions.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchQueryRequest {
    /// Positional list of query requests. Each entry uses the single-query body.
    pub queries: Vec<QueryRequest>,
}

#[derive(Debug, Clone, Copy)]
struct ValidatedQuery {
    top_k: usize,
    candidate_k: usize,
    nprobe: usize,
    include_attributes: bool,
    source: ValidatedSource,
}

#[derive(Debug, Clone, Copy)]
enum ValidatedSource {
    LegacyVector,
    LegacyBm25,
    AlgebraAnn { index: usize },
    AlgebraBm25 { index: usize },
    AlgebraHybrid { source_count: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuerySourceKind {
    Ann,
    Bm25,
}

#[derive(Debug)]
enum QuerySourceRef<'a> {
    Ann {
        vector: Cow<'a, [f32]>,
        exclude_id: Option<String>,
    },
    Bm25 {
        rank_by: &'a RankBy,
        last_as_prefix: bool,
    },
}

struct SourceQueryResponse {
    kind: QuerySourceKind,
    response: QueryResponse,
}

const DEFAULT_RRF_K: usize = 60;

struct QueryExecutionOptions {
    manifest: Option<Manifest>,
    notify_hydration: bool,
}

/// Query-string parameters for the single-query route.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRouteParams {
    /// Optional point-in-time target: generation, RFC3339 timestamp, or `snapshot:name`.
    #[serde(default)]
    as_of: Option<String>,
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
        response: Box<QueryResponse>,
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
    Query(params): Query<QueryRouteParams>,
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
    let as_of_manifest = match params.as_of.as_deref() {
        Some(as_of) => Some(as_of::resolve_manifest(&state.store, &ns, as_of).await?),
        None => None,
    };
    let notify_hydration = as_of_manifest.is_none();

    let result = execute_validated_query(
        &state,
        &ns,
        &meta,
        &req,
        validated,
        knobs.as_ref(),
        QueryExecutionOptions {
            manifest: as_of_manifest,
            notify_hydration,
        },
    )
    .await
    .map_err(ApiError::from)?;

    let request_id = crate::server::current_request_id();
    info!(
        request_id = request_id.as_deref().unwrap_or(""),
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
    Extension(rate_limit_identity): Extension<RateLimitIdentity>,
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
        rate_limit_identity.ip,
        RateLimitClass::Read,
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
    if let Some(manifest) = manifest.as_ref() {
        notify_hydrator(&state, &ns, manifest);
    }

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
                    QueryExecutionOptions {
                        manifest: manifest.clone(),
                        notify_hydration: false,
                    },
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
    let (source, source_nprobe) =
        validate_query_source(req, state.config.server.max_vector_id_length)?;
    validate_retrieval_algebra_options(req)?;
    let include_attributes = validate_projection(req)?;
    let candidate_k = validate_candidate_k(req, top_k)?;

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
    validate_nprobe_requests(req, state.config.indexing.max_nprobe)?;
    let nprobe = source_nprobe.or(req.nprobe).unwrap_or(knobs.default_nprobe);

    Ok(ValidatedQuery {
        top_k,
        candidate_k,
        nprobe,
        include_attributes,
        source,
    })
}

fn validate_query_source(
    req: &QueryRequest,
    max_vector_id_length: usize,
) -> Result<(ValidatedSource, Option<usize>), ZeppelinError> {
    if let Some(sources) = req.sources.as_ref() {
        if req.vector.is_some() || req.rank_by.is_some() {
            return Err(ZeppelinError::Validation(
                "cannot mix legacy query fields with retrieval algebra 'sources'".into(),
            ));
        }
        if sources.is_empty() {
            return Err(ZeppelinError::Validation(
                "sources must contain at least one candidate source".into(),
            ));
        }
        for source in sources {
            validate_candidate_source_shape(source, max_vector_id_length)?;
        }
        if sources.len() > 1 {
            return validate_multi_source_request(req, sources);
        }
        return match &sources[0] {
            CandidateSource::Ann { nprobe, .. } => {
                if nprobe.is_some() && req.nprobe.is_some() {
                    return Err(ZeppelinError::Validation(
                        "cannot provide both top-level 'nprobe' and source 'nprobe'".into(),
                    ));
                }
                Ok((ValidatedSource::AlgebraAnn { index: 0 }, *nprobe))
            }
            CandidateSource::Bm25 { .. } => Ok((ValidatedSource::AlgebraBm25 { index: 0 }, None)),
        };
    }

    if retrieval_algebra_without_sources(req) {
        if req.vector.is_some() || req.rank_by.is_some() {
            return Err(ZeppelinError::Validation(
                "cannot mix legacy query fields with retrieval algebra".into(),
            ));
        }
        return Err(ZeppelinError::Validation(
            "retrieval algebra requests must provide 'sources'".into(),
        ));
    }

    // Legacy contract: exactly one of vector or rank_by must be provided.
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
    if req.vector.is_some() {
        Ok((ValidatedSource::LegacyVector, None))
    } else {
        Ok((ValidatedSource::LegacyBm25, None))
    }
}

fn validate_candidate_source_shape(
    source: &CandidateSource,
    max_vector_id_length: usize,
) -> Result<(), ZeppelinError> {
    match source {
        CandidateSource::Ann { vector, id, .. } => match (vector.is_some(), id.as_ref()) {
            (true, None) => Ok(()),
            (false, Some(id)) => {
                super::vectors::validate_vector_id_for_request(id, max_vector_id_length)
            }
            _ => Err(ZeppelinError::Validation(
                "ann source must provide exactly one of 'vector' or 'id'".into(),
            )),
        },
        CandidateSource::Bm25 { .. } => Ok(()),
    }
}

fn validate_multi_source_request(
    req: &QueryRequest,
    sources: &[CandidateSource],
) -> Result<(ValidatedSource, Option<usize>), ZeppelinError> {
    if req.nprobe.is_some()
        && sources.iter().any(|source| {
            matches!(
                source,
                CandidateSource::Ann {
                    nprobe: Some(_),
                    ..
                }
            )
        })
    {
        return Err(ZeppelinError::Validation(
            "cannot provide both top-level 'nprobe' and source 'nprobe'".into(),
        ));
    }

    match req.fusion.as_ref() {
        Some(FusionSpec::Rrf { k }) => {
            if matches!(k, Some(0)) {
                return Err(ZeppelinError::Validation(
                    "fusion.rrf.k must be >= 1".into(),
                ));
            }
            Ok((
                ValidatedSource::AlgebraHybrid {
                    source_count: sources.len(),
                },
                None,
            ))
        }
        Some(FusionSpec::Weighted { weights }) => {
            if weights.len() != sources.len() {
                return Err(ZeppelinError::Validation(
                    "fusion weights length must match sources length".into(),
                ));
            }
            Ok((
                ValidatedSource::AlgebraHybrid {
                    source_count: sources.len(),
                },
                None,
            ))
        }
        None => Ok((
            ValidatedSource::AlgebraHybrid {
                source_count: sources.len(),
            },
            None,
        )),
        Some(FusionSpec::None) => Err(ZeppelinError::Validation(
            "multiple candidate sources require a supported fusion strategy".into(),
        )),
    }
}

fn retrieval_algebra_without_sources(req: &QueryRequest) -> bool {
    req.candidate_k.is_some()
        || req.fusion.is_some()
        || req.rerank.is_some()
        || req.grouping.is_some()
        || req.facets.is_some()
        || req.projection.is_some()
        || req.cursor.is_some()
}

fn validate_retrieval_algebra_options(req: &QueryRequest) -> Result<(), ZeppelinError> {
    if let Some(fusion) = req.fusion.as_ref() {
        if req.sources.as_ref().map_or(0, Vec::len) < 2 && !fusion.is_none() {
            return Err(ZeppelinError::Validation(
                "fusion requires at least two candidate sources".into(),
            ));
        }
    }
    if let Some(grouping) = req.grouping.as_ref() {
        match grouping {
            GroupingSpec::None => {}
            GroupingSpec::Field { max_per_group, .. } if *max_per_group == 0 => {
                return Err(ZeppelinError::Validation(
                    "grouping.max_per_group must be >= 1".into(),
                ));
            }
            GroupingSpec::Field { .. } => {}
        }
    }
    if grouping_requested(req) && cursor_requested(req) {
        return Err(ZeppelinError::Validation(
            "grouping cannot be combined with cursor pagination".into(),
        ));
    }
    if let Some(facets) = req.facets.as_ref() {
        for facet in facets {
            if facet.field().is_empty() {
                return Err(ZeppelinError::Validation(
                    "facet field must not be empty".into(),
                ));
            }
        }
    }
    if let Some(cursor) = req.cursor.as_ref() {
        match cursor {
            CursorSpec::None => {}
            CursorSpec::After { token } => {
                decode_cursor_token(token)?;
            }
        }
    }
    Ok(())
}

fn validate_candidate_k(req: &QueryRequest, top_k: usize) -> Result<usize, ZeppelinError> {
    if let Some(candidate_k) = req.candidate_k {
        if candidate_k == 0 {
            return Err(ZeppelinError::Validation("candidate_k must be >= 1".into()));
        }
        return Ok(candidate_k);
    }

    Ok(top_k.saturating_mul(4).max(100))
}

fn validate_nprobe_requests(req: &QueryRequest, max_nprobe: usize) -> Result<(), ZeppelinError> {
    if let Some(nprobe) = req.nprobe {
        validate_nprobe(nprobe, max_nprobe)?;
    }
    if let Some(sources) = req.sources.as_ref() {
        for source in sources {
            if let CandidateSource::Ann {
                nprobe: Some(nprobe),
                ..
            } = source
            {
                validate_nprobe(*nprobe, max_nprobe)?;
            }
        }
    }
    Ok(())
}

fn validate_nprobe(nprobe: usize, max_nprobe: usize) -> Result<(), ZeppelinError> {
    if nprobe == 0 {
        return Err(ZeppelinError::Validation("nprobe must be >= 1".into()));
    }
    if nprobe > max_nprobe {
        return Err(ZeppelinError::Validation(format!(
            "nprobe {} exceeds maximum of {}",
            nprobe, max_nprobe
        )));
    }
    Ok(())
}

fn validate_projection(req: &QueryRequest) -> Result<bool, ZeppelinError> {
    let Some(projection) = req.projection.as_ref() else {
        return Ok(req.include_attributes.unwrap_or(true));
    };
    if req.include_attributes.is_some() && projection.include_attributes.is_some() {
        return Err(ZeppelinError::Validation(
            "cannot provide both 'include_attributes' and 'projection.include_attributes'".into(),
        ));
    }
    if projection.fields.is_some() {
        return Err(ZeppelinError::NotImplemented {
            feature: "field projection",
        });
    }
    if projection.include_vectors.unwrap_or(false) {
        return Err(ZeppelinError::NotImplemented {
            feature: "vector projection",
        });
    }
    Ok(projection
        .include_attributes
        .or(req.include_attributes)
        .unwrap_or(true))
}

async fn execute_validated_query(
    state: &AppState,
    ns: &str,
    meta: &NamespaceMetadata,
    req: &QueryRequest,
    validated: ValidatedQuery,
    knobs: &QueryKnobs,
    options: QueryExecutionOptions,
) -> Result<QueryResponse, ZeppelinError> {
    if let ValidatedSource::AlgebraHybrid { source_count } = validated.source {
        return execute_hybrid_query(
            state,
            ns,
            meta,
            req,
            validated,
            source_count,
            knobs,
            options,
        )
        .await;
    }

    let manifest = read_manifest_for_execution(state, ns, req.consistency, options).await?;
    let source_ref =
        resolve_query_source_ref(state, ns, req, validated.source, manifest.clone()).await?;
    validate_query_source_metadata(ns, meta, &source_ref)?;
    let emit_debug = req.debug.unwrap_or(false);
    let first_stage_top_k = first_stage_top_k(req, validated);
    let rerank_limit = rerank_output_k(req, validated, first_stage_top_k);
    let first_stage_include_attributes =
        first_stage_include_attributes(req, validated.include_attributes);
    let explain_source =
        explain_source_for_ref(0, &source_ref, validated.nprobe, first_stage_top_k);
    let mut explain = build_explain_accumulator(
        req,
        validated,
        explain_path(validated.source),
        first_stage_top_k,
        vec![explain_source],
    );
    let source = execute_query_source_with_manifest(
        state,
        ns,
        meta,
        req,
        source_ref,
        first_stage_top_k,
        validated.nprobe,
        first_stage_include_attributes,
        knobs,
        manifest.clone(),
        emit_debug,
    )
    .await?;
    if let Some(explain) = explain.as_mut() {
        explain.capture_single_source(0, source.kind, &source.response.results);
    }
    apply_rerank_if_requested(
        RerankExecutionContext {
            state,
            ns,
            meta,
            req,
            top_k: validated.top_k,
            rerank_limit,
            include_attributes: validated.include_attributes,
            manifest,
        },
        source.response,
        explain,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn execute_hybrid_query(
    state: &AppState,
    ns: &str,
    meta: &NamespaceMetadata,
    req: &QueryRequest,
    validated: ValidatedQuery,
    source_count: usize,
    knobs: &QueryKnobs,
    options: QueryExecutionOptions,
) -> Result<QueryResponse, ZeppelinError> {
    let sources = req
        .sources
        .as_ref()
        .ok_or_else(|| ZeppelinError::Validation("retrieval algebra sources missing".into()))?;
    if sources.len() != source_count {
        return Err(ZeppelinError::Validation(
            "validated source count does not match request".into(),
        ));
    }

    let manifest = read_manifest_for_execution(state, ns, req.consistency, options).await?;
    let mut source_responses = Vec::with_capacity(source_count);
    let emit_debug = req.debug.unwrap_or(false);
    let first_stage_top_k = first_stage_top_k(req, validated);
    let rerank_limit = rerank_output_k(req, validated, first_stage_top_k);
    let first_stage_include_attributes =
        first_stage_include_attributes(req, validated.include_attributes);
    let excluded_seed_ids = algebra_seed_exclusion_ids(req);
    let source_candidate_k = validated
        .candidate_k
        .saturating_add(excluded_seed_ids.len());
    let mut explain_sources = Vec::with_capacity(source_count);
    for index in 0..source_count {
        let source_ref =
            resolve_algebra_source_ref(state, ns, req, index, manifest.clone()).await?;
        validate_query_source_metadata(ns, meta, &source_ref)?;
        let nprobe = nprobe_for_algebra_source(req, index, knobs.default_nprobe)?;
        explain_sources.push(explain_source_for_request_source(
            req,
            index,
            nprobe,
            source_candidate_k,
        )?);
        let mut source_response = execute_query_source_with_manifest(
            state,
            ns,
            meta,
            req,
            source_ref,
            source_candidate_k,
            nprobe,
            first_stage_include_attributes,
            knobs,
            manifest.clone(),
            emit_debug,
        )
        .await?;
        exclude_seed_ids_from_response(
            &mut source_response.response,
            &excluded_seed_ids,
            validated.candidate_k,
        );
        source_responses.push(source_response);
    }

    let mut explain = build_explain_accumulator(
        req,
        validated,
        QueryExplainPath::AlgebraHybrid,
        first_stage_top_k,
        explain_sources,
    );
    if let Some(explain) = explain.as_mut() {
        explain.capture_hybrid_sources(req.fusion.as_ref(), &source_responses)?;
    }
    let response = fuse_source_responses(
        req.fusion.as_ref(),
        source_responses,
        first_stage_top_k,
        req.consistency,
        emit_debug,
    )?;
    apply_rerank_if_requested(
        RerankExecutionContext {
            state,
            ns,
            meta,
            req,
            top_k: validated.top_k,
            rerank_limit,
            include_attributes: validated.include_attributes,
            manifest,
        },
        response,
        explain,
    )
    .await
}

fn first_stage_top_k(req: &QueryRequest, validated: ValidatedQuery) -> usize {
    if req.rerank.as_ref().is_some_and(RerankSpec::is_explicit)
        || grouping_requested(req)
        || facet_counts_requested(req)
        || cursor_requested(req)
    {
        let min_cursor_frontier = validated.top_k.saturating_add(1);
        if cursor_requested(req) {
            validated.candidate_k.max(min_cursor_frontier)
        } else {
            validated.candidate_k
        }
    } else {
        validated.top_k
    }
}

fn rerank_output_k(
    req: &QueryRequest,
    validated: ValidatedQuery,
    first_stage_top_k: usize,
) -> usize {
    if cursor_requested(req) {
        first_stage_top_k
    } else {
        validated.top_k
    }
}

fn first_stage_include_attributes(req: &QueryRequest, include_attributes: bool) -> bool {
    include_attributes
        || matches!(req.rerank, Some(RerankSpec::Bm25 { .. }))
        || grouping_requested(req)
        || facet_counts_requested(req)
}

fn cursor_requested(req: &QueryRequest) -> bool {
    req.cursor.is_some()
}

fn facet_counts_requested(req: &QueryRequest) -> bool {
    req.facets.as_ref().is_some_and(|facets| !facets.is_empty())
}

fn grouping_requested(req: &QueryRequest) -> bool {
    matches!(req.grouping, Some(GroupingSpec::Field { .. }))
}

fn algebra_seed_exclusion_ids(req: &QueryRequest) -> HashSet<String> {
    req.sources
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter_map(|source| match source {
            CandidateSource::Ann { id: Some(id), .. } => Some(id.clone()),
            _ => None,
        })
        .collect()
}

fn exclude_seed_ids_from_response(
    response: &mut QueryResponse,
    excluded_seed_ids: &HashSet<String>,
    top_k: usize,
) {
    if excluded_seed_ids.is_empty() {
        return;
    }
    response
        .results
        .retain(|result| !excluded_seed_ids.contains(&result.id));
    response.results.truncate(top_k);
}

struct ExplainAccumulator {
    mode: QueryExplainMode,
    plan: QueryExplainPlan,
    results: HashMap<String, QueryExplainResult>,
}

impl ExplainAccumulator {
    fn new(mode: QueryExplainMode, plan: QueryExplainPlan) -> Self {
        Self {
            mode,
            plan,
            results: HashMap::new(),
        }
    }

    fn capture_single_source(
        &mut self,
        source_index: usize,
        kind: QuerySourceKind,
        results: &[SearchResult],
    ) {
        if self.mode != QueryExplainMode::Full {
            return;
        }
        for result in results {
            self.record_source_score(
                &result.id,
                source_index,
                kind,
                result.score,
                None,
                result.score,
            );
        }
    }

    fn capture_hybrid_sources(
        &mut self,
        fusion: Option<&FusionSpec>,
        sources: &[SourceQueryResponse],
    ) -> Result<(), ZeppelinError> {
        if self.mode != QueryExplainMode::Full {
            return Ok(());
        }
        match fusion {
            Some(FusionSpec::Weighted { weights }) => {
                self.capture_weighted_sources(sources, weights)
            }
            Some(FusionSpec::Rrf { k }) => {
                self.capture_rrf_sources(sources, k.unwrap_or(DEFAULT_RRF_K));
                Ok(())
            }
            Some(FusionSpec::None) => Err(ZeppelinError::Validation(
                "multiple candidate sources require a supported fusion strategy".into(),
            )),
            None => {
                self.capture_rrf_sources(sources, DEFAULT_RRF_K);
                Ok(())
            }
        }
    }

    fn capture_rrf_sources(&mut self, sources: &[SourceQueryResponse], k: usize) {
        for (source_index, source) in sources.iter().enumerate() {
            for (rank, result) in source.response.results.iter().enumerate() {
                let contribution = 1.0_f32 / (k as f32 + (rank + 1) as f32);
                self.record_source_score(
                    &result.id,
                    source_index,
                    source.kind,
                    result.score,
                    None,
                    contribution,
                );
            }
        }
    }

    fn capture_weighted_sources(
        &mut self,
        sources: &[SourceQueryResponse],
        weights: &[f32],
    ) -> Result<(), ZeppelinError> {
        if weights.len() != sources.len() {
            return Err(ZeppelinError::Validation(
                "fusion weights length must match sources length".into(),
            ));
        }
        for (source_index, (source, weight)) in
            sources.iter().zip(weights.iter().copied()).enumerate()
        {
            if !weight.is_finite() {
                return Err(ZeppelinError::Validation(
                    "fusion weights must be finite".into(),
                ));
            }
            if source.response.results.is_empty() {
                continue;
            }
            let (min_score, max_score) = source.response.results.iter().fold(
                (f32::INFINITY, f32::NEG_INFINITY),
                |(min_score, max_score), result| {
                    (min_score.min(result.score), max_score.max(result.score))
                },
            );
            for result in &source.response.results {
                if !result.score.is_finite() {
                    return Err(ZeppelinError::Validation(
                        "source result scores must be finite".into(),
                    ));
                }
                let normalized =
                    normalize_source_score(source.kind, result.score, min_score, max_score);
                self.record_source_score(
                    &result.id,
                    source_index,
                    source.kind,
                    result.score,
                    Some(normalized),
                    weight * normalized,
                );
            }
        }
        Ok(())
    }

    fn record_source_score(
        &mut self,
        id: &str,
        source_index: usize,
        kind: QuerySourceKind,
        raw_score: f32,
        normalized_score: Option<f32>,
        contribution: f32,
    ) {
        let entry = self
            .results
            .entry(id.to_string())
            .or_insert_with(|| QueryExplainResult {
                id: id.to_string(),
                sources: Vec::new(),
                fused_score: 0.0,
                rerank_score: None,
            });
        entry.sources.push(QueryExplainResultSource {
            index: source_index,
            kind: explain_source_kind(kind),
            raw_score,
            normalized_score,
            contribution,
        });
        entry.fused_score += contribution;
    }

    fn capture_rerank_scores(&mut self, results: &[SearchResult]) {
        if self.mode != QueryExplainMode::Full {
            return;
        }
        for result in results {
            if let Some(explain_result) = self.results.get_mut(&result.id) {
                explain_result.rerank_score = Some(result.score);
            }
        }
    }

    fn finish(mut self, results: &[SearchResult]) -> Result<QueryExplain, ZeppelinError> {
        let results = if self.mode == QueryExplainMode::Full {
            let mut explain_results = Vec::with_capacity(results.len());
            for result in results {
                let explain_result = self.results.remove(&result.id).ok_or_else(|| {
                    ZeppelinError::Index(format!(
                        "explain provenance missing for result {}",
                        result.id
                    ))
                })?;
                explain_results.push(explain_result);
            }
            Some(explain_results)
        } else {
            None
        };
        Ok(QueryExplain {
            mode: self.mode,
            plan: self.plan,
            results,
        })
    }
}

fn requested_explain_mode(req: &QueryRequest) -> Option<QueryExplainMode> {
    match req.explain.as_ref()? {
        ExplainSpec::Flag(false) => None,
        ExplainSpec::Flag(true) => Some(QueryExplainMode::Plan),
        ExplainSpec::Mode(ExplainMode::None) => None,
        ExplainSpec::Mode(ExplainMode::Plan) => Some(QueryExplainMode::Plan),
        ExplainSpec::Mode(ExplainMode::Full) => Some(QueryExplainMode::Full),
    }
}

fn build_explain_accumulator(
    req: &QueryRequest,
    validated: ValidatedQuery,
    path: QueryExplainPath,
    first_stage_top_k: usize,
    sources: Vec<QueryExplainSource>,
) -> Option<ExplainAccumulator> {
    let mode = requested_explain_mode(req)?;
    Some(ExplainAccumulator::new(
        mode,
        QueryExplainPlan {
            path,
            candidate_k: validated.candidate_k,
            first_stage_top_k,
            top_k: validated.top_k,
            consistency: req.consistency,
            sources,
            fusion: explain_fusion(req),
            rerank: explain_rerank(req.rerank.as_ref()),
            grouping: explain_grouping(req.grouping.as_ref()),
            cursor: explain_cursor(req.cursor.as_ref()),
            facets: explain_facets(req.facets.as_deref()),
            projection: QueryExplainProjection {
                include_attributes: validated.include_attributes,
            },
        },
    ))
}

fn explain_fusion(req: &QueryRequest) -> QueryExplainFusion {
    match req.fusion.as_ref() {
        Some(FusionSpec::None) => QueryExplainFusion::None,
        Some(FusionSpec::Rrf { k }) => QueryExplainFusion::Rrf {
            k: k.unwrap_or(DEFAULT_RRF_K),
        },
        Some(FusionSpec::Weighted { weights }) => QueryExplainFusion::Weighted {
            weights: weights.clone(),
        },
        None if req.sources.as_ref().map_or(0, Vec::len) > 1 => {
            QueryExplainFusion::Rrf { k: DEFAULT_RRF_K }
        }
        None => QueryExplainFusion::None,
    }
}

fn explain_rerank(rerank: Option<&RerankSpec>) -> Option<QueryExplainRerank> {
    rerank.map(|rerank| match rerank {
        RerankSpec::Default => QueryExplainRerank::Default,
        RerankSpec::None => QueryExplainRerank::None,
        RerankSpec::Vector { .. } => QueryExplainRerank::Vector,
        RerankSpec::Bm25 { .. } => QueryExplainRerank::Bm25,
    })
}

fn explain_grouping(grouping: Option<&GroupingSpec>) -> Option<QueryExplainGrouping> {
    grouping.map(|grouping| match grouping {
        GroupingSpec::None => QueryExplainGrouping::None,
        GroupingSpec::Field {
            field,
            max_per_group,
        } => QueryExplainGrouping::Field {
            field: field.clone(),
            max_per_group: *max_per_group,
        },
    })
}

fn explain_cursor(cursor: Option<&CursorSpec>) -> QueryExplainCursor {
    QueryExplainCursor {
        requested: cursor.is_some(),
        after: matches!(cursor, Some(CursorSpec::After { .. })),
    }
}

fn explain_facets(facets: Option<&[FacetSpec]>) -> Vec<String> {
    facets
        .unwrap_or_default()
        .iter()
        .map(|facet| facet.field().to_string())
        .collect()
}

fn explain_source_for_ref(
    index: usize,
    source_ref: &QuerySourceRef<'_>,
    nprobe: usize,
    candidate_k: usize,
) -> QueryExplainSource {
    match source_ref {
        QuerySourceRef::Ann { .. } => QueryExplainSource {
            index,
            kind: QueryExplainSourceKind::Ann,
            nprobe: Some(nprobe),
            candidate_k,
        },
        QuerySourceRef::Bm25 { .. } => QueryExplainSource {
            index,
            kind: QueryExplainSourceKind::Bm25,
            nprobe: None,
            candidate_k,
        },
    }
}

fn explain_source_for_request_source(
    req: &QueryRequest,
    index: usize,
    nprobe: usize,
    candidate_k: usize,
) -> Result<QueryExplainSource, ZeppelinError> {
    let sources = req
        .sources
        .as_ref()
        .ok_or_else(|| ZeppelinError::Validation("retrieval algebra sources missing".into()))?;
    match sources.get(index) {
        Some(CandidateSource::Ann { .. }) => Ok(QueryExplainSource {
            index,
            kind: QueryExplainSourceKind::Ann,
            nprobe: Some(nprobe),
            candidate_k,
        }),
        Some(CandidateSource::Bm25 { .. }) => Ok(QueryExplainSource {
            index,
            kind: QueryExplainSourceKind::Bm25,
            nprobe: None,
            candidate_k,
        }),
        None => Err(ZeppelinError::Validation(
            "validated algebra source is missing".into(),
        )),
    }
}

fn explain_source_kind(kind: QuerySourceKind) -> QueryExplainSourceKind {
    match kind {
        QuerySourceKind::Ann => QueryExplainSourceKind::Ann,
        QuerySourceKind::Bm25 => QueryExplainSourceKind::Bm25,
    }
}

fn explain_path(source: ValidatedSource) -> QueryExplainPath {
    match source {
        ValidatedSource::LegacyVector => QueryExplainPath::LegacyVector,
        ValidatedSource::LegacyBm25 => QueryExplainPath::LegacyBm25,
        ValidatedSource::AlgebraAnn { .. } | ValidatedSource::AlgebraBm25 { .. } => {
            QueryExplainPath::AlgebraSingle
        }
        ValidatedSource::AlgebraHybrid { .. } => QueryExplainPath::AlgebraHybrid,
    }
}

struct RerankExecutionContext<'a> {
    state: &'a AppState,
    ns: &'a str,
    meta: &'a NamespaceMetadata,
    req: &'a QueryRequest,
    top_k: usize,
    rerank_limit: usize,
    include_attributes: bool,
    manifest: Manifest,
}

async fn apply_rerank_if_requested(
    ctx: RerankExecutionContext<'_>,
    response: QueryResponse,
    mut explain: Option<ExplainAccumulator>,
) -> Result<QueryResponse, ZeppelinError> {
    let facets = compute_facets_if_requested(ctx.req, &response.results)?;
    let keep_attrs_after_rerank = ctx.include_attributes || grouping_requested(ctx.req);
    let response = match ctx.req.rerank.as_ref() {
        Some(RerankSpec::Vector { vector }) => apply_vector_rerank(&ctx, response, vector).await?,
        Some(RerankSpec::Bm25 { rank_by }) => apply_bm25_rerank(
            ctx.meta,
            response,
            ctx.rerank_limit,
            keep_attrs_after_rerank,
            rank_by,
        )?,
        _ => response,
    };
    if ctx.req.rerank.as_ref().is_some_and(RerankSpec::is_explicit) {
        if let Some(explain) = explain.as_mut() {
            explain.capture_rerank_scores(&response.results);
        }
    }
    let response =
        apply_grouping_if_requested(ctx.req, response, ctx.top_k, ctx.include_attributes)?;
    let mut response = apply_cursor_if_requested(ctx.ns, ctx.req, response, ctx.top_k)?;
    if !grouping_requested(ctx.req) && !cursor_requested(ctx.req) {
        response.results.truncate(ctx.top_k);
    }
    strip_attributes_if_needed(&mut response, ctx.include_attributes);
    response.facets = facets;
    if let Some(explain) = explain {
        response.explain = Some(explain.finish(&response.results)?);
    }
    Ok(response)
}

fn strip_attributes_if_needed(response: &mut QueryResponse, include_attributes: bool) {
    if include_attributes {
        return;
    }
    for result in &mut response.results {
        result.attributes = None;
    }
    if let Some(groups) = response.groups.as_mut() {
        for group in groups {
            for result in &mut group.results {
                result.attributes = None;
            }
        }
    }
}

fn compute_facets_if_requested(
    req: &QueryRequest,
    results: &[SearchResult],
) -> Result<Option<QueryFacets>, ZeppelinError> {
    let Some(facets) = req.facets.as_ref() else {
        return Ok(None);
    };

    let mut fields = BTreeMap::<String, BTreeMap<String, usize>>::new();
    let mut requested_fields = Vec::<String>::new();
    for facet in facets {
        let field = facet.field();
        if fields.contains_key(field) {
            continue;
        }
        fields.insert(field.to_string(), BTreeMap::new());
        requested_fields.push(field.to_string());
    }

    for result in results {
        let Some(attrs) = result.attributes.as_ref() else {
            continue;
        };
        for field in &requested_fields {
            let Some(value) = attrs.get(field) else {
                continue;
            };
            for facet_value in facet_attribute_values(value)? {
                let counts = fields.get_mut(field).ok_or_else(|| {
                    ZeppelinError::Index("requested facet field missing from accumulator".into())
                })?;
                *counts.entry(facet_value).or_insert(0) += 1;
            }
        }
    }

    Ok(Some(QueryFacets { fields }))
}

fn facet_attribute_values(value: &AttributeValue) -> Result<Vec<String>, ZeppelinError> {
    match value {
        AttributeValue::String(value) => Ok(vec![value.clone()]),
        AttributeValue::Integer(value) => Ok(vec![value.to_string()]),
        AttributeValue::Float(value) => {
            if !value.is_finite() {
                return Err(ZeppelinError::Validation(
                    "facet field contains a non-finite value".into(),
                ));
            }
            Ok(vec![value.to_string()])
        }
        AttributeValue::Bool(value) => Ok(vec![value.to_string()]),
        AttributeValue::StringList(values) => Ok(values.clone()),
        AttributeValue::IntegerList(values) => Ok(values.iter().map(i64::to_string).collect()),
        AttributeValue::FloatList(values) => {
            if values.iter().any(|value| !value.is_finite()) {
                return Err(ZeppelinError::Validation(
                    "facet field contains a non-finite value".into(),
                ));
            }
            Ok(values.iter().map(f64::to_string).collect())
        }
    }
}

async fn apply_vector_rerank(
    ctx: &RerankExecutionContext<'_>,
    mut response: QueryResponse,
    rerank_vector: &[f32],
) -> Result<QueryResponse, ZeppelinError> {
    if rerank_vector.len() != ctx.meta.dimensions {
        return Err(ZeppelinError::DimensionMismatch {
            expected: ctx.meta.dimensions,
            actual: rerank_vector.len(),
        });
    }
    if let Some((dim_idx, kind)) = super::find_non_finite(rerank_vector) {
        return Err(ZeppelinError::Validation(format!(
            "rerank vector contains a non-finite value ({kind}) at dimension {dim_idx}"
        )));
    }
    if response.results.is_empty() {
        return Ok(response);
    }

    let ids: Vec<String> = response
        .results
        .iter()
        .map(|result| result.id.clone())
        .collect();
    let values = super::vectors::fetch_vector_values_by_ids(
        ctx.state,
        ctx.ns,
        &ids,
        ctx.req.consistency,
        ctx.manifest.clone(),
    )
    .await?;

    for result in &mut response.results {
        let vector = values.get(&result.id).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "rerank candidate {} missing vector values",
                result.id
            ))
        })?;
        result.score = compute_distance(rerank_vector, vector, ctx.meta.distance_metric);
    }
    response
        .results
        .sort_by(|a, b| a.score.total_cmp(&b.score).then_with(|| a.id.cmp(&b.id)));
    response.results.truncate(ctx.rerank_limit);
    Ok(response)
}

fn apply_grouping_if_requested(
    req: &QueryRequest,
    mut response: QueryResponse,
    top_k: usize,
    include_attributes: bool,
) -> Result<QueryResponse, ZeppelinError> {
    let Some(GroupingSpec::Field {
        field,
        max_per_group,
    }) = req.grouping.as_ref()
    else {
        response.groups = None;
        return Ok(response);
    };

    let mut groups = Vec::<QueryResultGroup>::new();
    let mut group_indexes = HashMap::<String, usize>::new();
    for result in response.results {
        let (internal_key, display_key) = grouping_keys(&result, field)?;
        let group_index = match group_indexes.get(&internal_key).copied() {
            Some(index) => index,
            None => {
                let index = groups.len();
                groups.push(QueryResultGroup {
                    key: display_key,
                    results: Vec::new(),
                });
                group_indexes.insert(internal_key, index);
                index
            }
        };
        if groups[group_index].results.len() < *max_per_group {
            groups[group_index].results.push(result);
        }
    }

    groups.truncate(top_k);
    if !include_attributes {
        for group in &mut groups {
            for result in &mut group.results {
                result.attributes = None;
            }
        }
    }
    response.results = groups
        .iter()
        .flat_map(|group| group.results.iter().cloned())
        .collect();
    response.groups = Some(groups);
    Ok(response)
}

fn grouping_keys(result: &SearchResult, field: &str) -> Result<(String, String), ZeppelinError> {
    if let Some(value) = result
        .attributes
        .as_ref()
        .and_then(|attrs| attrs.get(field))
    {
        let display = group_attribute_value(value)?;
        return Ok((format!("field:{display}"), display));
    }
    Ok((format!("missing:{}", result.id), result.id.clone()))
}

fn group_attribute_value(value: &AttributeValue) -> Result<String, ZeppelinError> {
    match value {
        AttributeValue::String(value) => Ok(value.clone()),
        AttributeValue::Integer(value) => Ok(value.to_string()),
        AttributeValue::Float(value) => {
            if !value.is_finite() {
                return Err(ZeppelinError::Validation(
                    "grouping field contains a non-finite value".into(),
                ));
            }
            Ok(value.to_string())
        }
        AttributeValue::Bool(value) => Ok(value.to_string()),
        AttributeValue::StringList(values) => Ok(values.join(",")),
        AttributeValue::IntegerList(values) => Ok(values
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(",")),
        AttributeValue::FloatList(values) => {
            if values.iter().any(|value| !value.is_finite()) {
                return Err(ZeppelinError::Validation(
                    "grouping field contains a non-finite value".into(),
                ));
            }
            Ok(values
                .iter()
                .map(f64::to_string)
                .collect::<Vec<_>>()
                .join(","))
        }
    }
}

struct DecodedCursor {
    fingerprint: u64,
    score_bits: u32,
    id: String,
}

fn apply_cursor_if_requested(
    ns: &str,
    req: &QueryRequest,
    mut response: QueryResponse,
    top_k: usize,
) -> Result<QueryResponse, ZeppelinError> {
    let Some(cursor) = req.cursor.as_ref() else {
        response.next_cursor = None;
        return Ok(response);
    };

    let cursor_cmp = cursor_result_cmp(req);
    response.results.sort_by(cursor_cmp);
    let fingerprint = cursor_fingerprint(ns, req)?;
    if let CursorSpec::After { token } = cursor {
        let decoded = decode_cursor_token(token)?;
        if decoded.fingerprint != fingerprint {
            return Err(ZeppelinError::Validation(
                "cursor token does not match query".into(),
            ));
        }
        let marker = SearchResult {
            id: decoded.id,
            score: f32::from_bits(decoded.score_bits),
            attributes: None,
        };
        response
            .results
            .retain(|result| cursor_cmp(result, &marker) == Ordering::Greater);
    }

    let has_more = response.results.len() > top_k;
    response.results.truncate(top_k);
    response.next_cursor = if has_more {
        response
            .results
            .last()
            .map(|result| encode_cursor_token(fingerprint, result))
            .transpose()?
    } else {
        None
    };
    Ok(response)
}

fn cursor_result_cmp(req: &QueryRequest) -> fn(&SearchResult, &SearchResult) -> Ordering {
    if cursor_lower_score_is_better(req) {
        distance_result_cmp
    } else {
        fused_result_cmp
    }
}

fn cursor_lower_score_is_better(req: &QueryRequest) -> bool {
    match req.rerank.as_ref() {
        Some(RerankSpec::Vector { .. }) => return true,
        Some(RerankSpec::Bm25 { .. }) => return false,
        Some(RerankSpec::Default | RerankSpec::None) | None => {}
    }
    let Some(sources) = req.sources.as_ref() else {
        return false;
    };
    matches!(sources.as_slice(), [CandidateSource::Ann { .. }])
}

fn distance_result_cmp(a: &SearchResult, b: &SearchResult) -> Ordering {
    a.score.total_cmp(&b.score).then_with(|| a.id.cmp(&b.id))
}

fn cursor_fingerprint(ns: &str, req: &QueryRequest) -> Result<u64, ZeppelinError> {
    let mut value = serde_json::to_value(req)?;
    let object = value.as_object_mut().ok_or_else(|| {
        ZeppelinError::Validation("cursor fingerprint requires object query".into())
    })?;
    object.remove("cursor");
    object.remove("debug");
    object.remove("explain");
    object.remove("facets");
    object.remove("include_attributes");
    object.remove("projection");
    object.remove("consistency");
    let payload = serde_json::to_vec(&(ns, value))?;
    Ok(xxh3_64(&payload))
}

fn encode_cursor_token(fingerprint: u64, result: &SearchResult) -> Result<String, ZeppelinError> {
    if !result.score.is_finite() {
        return Err(ZeppelinError::Validation(
            "cursor cannot encode non-finite score".into(),
        ));
    }
    Ok(format!(
        "zp1:{fingerprint:016x}:{:08x}:{}",
        result.score.to_bits(),
        hex_encode(result.id.as_bytes())
    ))
}

fn decode_cursor_token(token: &str) -> Result<DecodedCursor, ZeppelinError> {
    let parts: Vec<&str> = token.split(':').collect();
    if parts.len() != 4 || parts[0] != "zp1" {
        return Err(ZeppelinError::Validation("invalid cursor token".into()));
    }
    let fingerprint = u64::from_str_radix(parts[1], 16)
        .map_err(|_| ZeppelinError::Validation("invalid cursor token".into()))?;
    let score_bits = u32::from_str_radix(parts[2], 16)
        .map_err(|_| ZeppelinError::Validation("invalid cursor token".into()))?;
    let score = f32::from_bits(score_bits);
    if !score.is_finite() {
        return Err(ZeppelinError::Validation(
            "invalid cursor token score".into(),
        ));
    }
    let id_bytes = hex_decode(parts[3])?;
    let id = String::from_utf8(id_bytes)
        .map_err(|_| ZeppelinError::Validation("invalid cursor token id".into()))?;
    Ok(DecodedCursor {
        fingerprint,
        score_bits,
        id,
    })
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(input: &str) -> Result<Vec<u8>, ZeppelinError> {
    if input.len() % 2 != 0 {
        return Err(ZeppelinError::Validation("invalid cursor token hex".into()));
    }
    input
        .as_bytes()
        .chunks_exact(2)
        .map(|chunk| {
            let hi = hex_value(chunk[0])?;
            let lo = hex_value(chunk[1])?;
            Ok((hi << 4) | lo)
        })
        .collect()
}

fn hex_value(byte: u8) -> Result<u8, ZeppelinError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(ZeppelinError::Validation("invalid cursor token hex".into())),
    }
}

struct RerankFieldQuery {
    field: String,
    query_tokens: Vec<String>,
    params: Bm25Params,
    config: FtsFieldConfig,
}

struct RerankCorpusStats {
    doc_count: u32,
    avg_doc_length: f32,
    term_doc_freqs: HashMap<String, u32>,
}

type RerankFieldData = HashMap<String, HashMap<String, (u32, HashMap<String, u32>)>>;

fn apply_bm25_rerank(
    meta: &NamespaceMetadata,
    mut response: QueryResponse,
    top_k: usize,
    include_attributes: bool,
    rank_by: &RankBy,
) -> Result<QueryResponse, ZeppelinError> {
    let field_queries = bm25_rerank_field_queries(meta, rank_by)?;
    let field_data = bm25_rerank_field_data(&response.results, &field_queries);
    let corpus_stats = bm25_rerank_corpus_stats(&field_data);

    for result in &mut response.results {
        let doc_data = field_data.get(result.id.as_str());
        let mut field_scores = HashMap::new();
        for field_query in &field_queries {
            let Some(corpus) = corpus_stats.get(field_query.field.as_str()) else {
                continue;
            };
            let Some((doc_length, tf_map)) =
                doc_data.and_then(|data| data.get(field_query.field.as_str()))
            else {
                continue;
            };
            let term_data: Vec<(f32, u32)> = field_query
                .query_tokens
                .iter()
                .map(|token| {
                    let doc_freq = corpus.term_doc_freqs.get(token).copied().unwrap_or(0);
                    let term_idf = bm25::idf(corpus.doc_count, doc_freq);
                    let tf = tf_map.get(token).copied().unwrap_or(0);
                    (term_idf, tf)
                })
                .collect();
            let score = bm25::bm25_score(
                &term_data,
                *doc_length,
                corpus.avg_doc_length,
                &field_query.params,
            );
            *field_scores.entry(field_query.field.clone()).or_insert(0.0) += score;
        }
        result.score = crate::fts::rank_by::evaluate_rank_by(rank_by, &field_scores);
    }

    response.results.sort_by(fused_result_cmp);
    response.results.truncate(top_k);
    if !include_attributes {
        for result in &mut response.results {
            result.attributes = None;
        }
    }
    Ok(response)
}

fn bm25_rerank_field_queries(
    meta: &NamespaceMetadata,
    rank_by: &RankBy,
) -> Result<Vec<RerankFieldQuery>, ZeppelinError> {
    rank_by
        .extract_field_queries()
        .into_iter()
        .map(|(field, query)| {
            let config = meta.full_text_search.get(&field).ok_or_else(|| {
                ZeppelinError::FtsFieldNotConfigured {
                    namespace: meta.name.clone(),
                    field: field.clone(),
                }
            })?;
            Ok(RerankFieldQuery {
                field,
                query_tokens: tokenize_text(&query, config, false),
                params: Bm25Params {
                    k1: config.k1,
                    b: config.b,
                },
                config: config.clone(),
            })
        })
        .collect()
}

fn bm25_rerank_field_data(
    results: &[SearchResult],
    field_queries: &[RerankFieldQuery],
) -> RerankFieldData {
    let mut field_data = HashMap::new();
    for result in results {
        let Some(attrs) = result.attributes.as_ref() else {
            continue;
        };
        for field_query in field_queries {
            let Some(AttributeValue::String(text)) = attrs.get(&field_query.field) else {
                continue;
            };
            let tokens = tokenize_text(text, &field_query.config, false);
            let doc_length = tokens.len() as u32;
            if doc_length == 0 {
                continue;
            }
            let mut tf_map = HashMap::new();
            for token in tokens {
                *tf_map.entry(token).or_insert(0) += 1;
            }
            field_data
                .entry(result.id.clone())
                .or_insert_with(HashMap::new)
                .insert(field_query.field.clone(), (doc_length, tf_map));
        }
    }
    field_data
}

fn bm25_rerank_corpus_stats(field_data: &RerankFieldData) -> HashMap<String, RerankCorpusStats> {
    let mut stats_by_field = HashMap::new();
    for doc_data in field_data.values() {
        for (field, (doc_length, tf_map)) in doc_data {
            let stats = stats_by_field
                .entry(field.clone())
                .or_insert_with(|| RerankCorpusStats {
                    doc_count: 0,
                    avg_doc_length: 0.0,
                    term_doc_freqs: HashMap::new(),
                });
            stats.doc_count += 1;
            stats.avg_doc_length += *doc_length as f32;
            for token in tf_map.keys() {
                *stats.term_doc_freqs.entry(token.clone()).or_insert(0) += 1;
            }
        }
    }
    for stats in stats_by_field.values_mut() {
        if stats.doc_count > 0 {
            stats.avg_doc_length /= stats.doc_count as f32;
        }
    }
    stats_by_field
}

fn validate_query_source_metadata(
    ns: &str,
    meta: &NamespaceMetadata,
    source_ref: &QuerySourceRef<'_>,
) -> Result<(), ZeppelinError> {
    match source_ref {
        QuerySourceRef::Bm25 { rank_by, .. } => {
            for (field, _) in rank_by.extract_field_queries() {
                if !meta.full_text_search.contains_key(&field) {
                    return Err(ZeppelinError::FtsFieldNotConfigured {
                        namespace: ns.to_string(),
                        field,
                    });
                }
            }
            Ok(())
        }
        QuerySourceRef::Ann { vector, .. } => {
            if vector.as_ref().len() != meta.dimensions {
                return Err(ZeppelinError::DimensionMismatch {
                    expected: meta.dimensions,
                    actual: vector.as_ref().len(),
                });
            }
            if let Some((dim_idx, kind)) = super::find_non_finite(vector.as_ref()) {
                return Err(ZeppelinError::Validation(format!(
                    "query vector contains a non-finite value ({kind}) at dimension {dim_idx}"
                )));
            }
            Ok(())
        }
    }
}

async fn read_manifest_for_execution(
    state: &AppState,
    ns: &str,
    consistency: ConsistencyLevel,
    options: QueryExecutionOptions,
) -> Result<Manifest, ZeppelinError> {
    let manifest = match options.manifest {
        Some(manifest) => manifest,
        None => {
            query::read_manifest_for_query(
                &state.store,
                ns,
                consistency,
                Some(&state.manifest_cache),
            )
            .await?
        }
    };
    if options.notify_hydration {
        notify_hydrator(state, ns, &manifest);
    }
    Ok(manifest)
}

#[allow(clippy::too_many_arguments)]
async fn execute_query_source_with_manifest(
    state: &AppState,
    ns: &str,
    meta: &NamespaceMetadata,
    req: &QueryRequest,
    source_ref: QuerySourceRef<'_>,
    top_k: usize,
    nprobe: usize,
    include_attributes: bool,
    knobs: &QueryKnobs,
    manifest: Manifest,
    emit_debug: bool,
) -> Result<SourceQueryResponse, ZeppelinError> {
    match source_ref {
        QuerySourceRef::Bm25 {
            rank_by,
            last_as_prefix,
        } => {
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

            let response = if emit_debug {
                query::execute_bm25_query_with_manifest_debug(
                    &state.store,
                    &state.wal_reader,
                    ns,
                    rank_by,
                    &meta.full_text_search,
                    top_k,
                    req.filter.as_ref(),
                    req.consistency,
                    last_as_prefix,
                    Some(&state.fts_cache),
                    Some(&state.cache),
                    knobs.bm25_max_full_scan_clusters,
                    knobs.bm25_max_full_scan_vectors,
                    include_attributes,
                    manifest,
                )
                .await
            } else {
                query::execute_bm25_query_with_manifest(
                    &state.store,
                    &state.wal_reader,
                    ns,
                    rank_by,
                    &meta.full_text_search,
                    top_k,
                    req.filter.as_ref(),
                    req.consistency,
                    last_as_prefix,
                    Some(&state.fts_cache),
                    Some(&state.cache),
                    knobs.bm25_max_full_scan_clusters,
                    knobs.bm25_max_full_scan_vectors,
                    include_attributes,
                    manifest,
                )
                .await
            };
            response.map(|response| SourceQueryResponse {
                kind: QuerySourceKind::Bm25,
                response,
            })
        }
        QuerySourceRef::Ann { vector, exclude_id } => {
            if vector.as_ref().len() != meta.dimensions {
                return Err(ZeppelinError::DimensionMismatch {
                    expected: meta.dimensions,
                    actual: vector.as_ref().len(),
                });
            }
            // Reject NaN/inf: non-finite query values make every distance
            // comparison nondeterministic (partial_cmp falls back to Equal).
            if let Some((dim_idx, kind)) = super::find_non_finite(vector.as_ref()) {
                return Err(ZeppelinError::Validation(format!(
                    "query vector contains a non-finite value ({kind}) at dimension {dim_idx}"
                )));
            }

            let search_top_k = if exclude_id.is_some() {
                top_k.saturating_add(1)
            } else {
                top_k
            };
            let params = query::QueryParams {
                store: &state.store,
                wal_reader: &state.wal_reader,
                namespace: ns,
                query: vector.as_ref(),
                top_k: search_top_k,
                nprobe,
                filter: req.filter.as_ref(),
                consistency: req.consistency,
                distance_metric: meta.distance_metric,
                oversample_factor: state.config.indexing.oversample_factor,
                rerank_coalesce_gap_bytes: knobs.rerank_coalesce_gap_bytes,
                cache: Some(&state.cache),
                manifest_cache: Some(&state.manifest_cache),
                include_attributes,
            };

            let response = if emit_debug {
                query::execute_query_with_manifest_debug(params, manifest).await
            } else {
                query::execute_query_with_manifest(params, manifest).await
            };
            let mut response = response?;
            if let Some(exclude_id) = exclude_id {
                response.results.retain(|result| result.id != exclude_id);
                response.results.truncate(top_k);
            }
            Ok(SourceQueryResponse {
                kind: QuerySourceKind::Ann,
                response,
            })
        }
    }
}

fn fuse_source_responses(
    fusion: Option<&FusionSpec>,
    sources: Vec<SourceQueryResponse>,
    top_k: usize,
    consistency: ConsistencyLevel,
    emit_debug: bool,
) -> Result<QueryResponse, ZeppelinError> {
    let scanned_fragments = sources
        .iter()
        .map(|source| source.response.scanned_fragments)
        .sum();
    let scanned_segments = sources
        .iter()
        .map(|source| source.response.scanned_segments)
        .sum();
    let source_debugs: Vec<QueryDebug> = if emit_debug {
        sources
            .iter()
            .filter_map(|source| source.response.debug.clone())
            .collect()
    } else {
        Vec::new()
    };

    let results = match fusion {
        Some(FusionSpec::None) => {
            return Err(ZeppelinError::Validation(
                "multiple candidate sources require a supported fusion strategy".into(),
            ));
        }
        Some(FusionSpec::Weighted { weights }) => fuse_weighted_results(sources, weights, top_k)?,
        Some(FusionSpec::Rrf { k }) => fuse_rrf_results(sources, k.unwrap_or(DEFAULT_RRF_K), top_k),
        None => fuse_rrf_results(sources, DEFAULT_RRF_K, top_k),
    };
    let debug = emit_debug.then(|| {
        aggregate_source_debug(
            &source_debugs,
            scanned_fragments,
            scanned_segments,
            results.len(),
            top_k,
            consistency,
        )
    });

    Ok(QueryResponse {
        results,
        scanned_fragments,
        scanned_segments,
        debug,
        next_cursor: None,
        groups: None,
        facets: None,
        explain: None,
    })
}

fn aggregate_source_debug(
    source_debugs: &[QueryDebug],
    scanned_fragments: usize,
    scanned_segments: usize,
    results_len: usize,
    top_k: usize,
    consistency: ConsistencyLevel,
) -> QueryDebug {
    let wal_ms = source_debugs.iter().map(|debug| debug.wal_ms).sum();
    let segment_ms = source_debugs.iter().map(|debug| debug.segment_ms).sum();
    let merge_ms = source_debugs.iter().map(|debug| debug.merge_ms).sum();
    let clusters_probed = source_debugs
        .iter()
        .map(|debug| debug.clusters_probed)
        .sum();
    let cache = QueryDebugCache {
        hits: source_debugs.iter().map(|debug| debug.cache.hits).sum(),
        misses: source_debugs.iter().map(|debug| debug.cache.misses).sum(),
    };
    let underfill_reason = if results_len >= top_k {
        None
    } else {
        source_debugs
            .iter()
            .filter_map(|debug| debug.underfill_reason.as_deref())
            .find(|reason| *reason == "eventual_skipped_wal")
            .map(str::to_string)
            .or_else(|| Some("not_enough_matches".to_string()))
    };
    QueryDebug {
        wal_ms,
        segment_ms,
        merge_ms,
        fragments_scanned: scanned_fragments,
        segments_scanned: scanned_segments,
        clusters_probed,
        cache,
        consistency_effective: consistency,
        underfill_reason,
    }
}

fn fuse_rrf_results(
    sources: Vec<SourceQueryResponse>,
    k: usize,
    top_k: usize,
) -> Vec<SearchResult> {
    let mut fused = HashMap::new();
    for source in sources {
        for (rank, result) in source.response.results.into_iter().enumerate() {
            let contribution = 1.0_f32 / (k as f32 + (rank + 1) as f32);
            add_fused_candidate(&mut fused, result, contribution);
        }
    }
    sorted_fused_results(fused, top_k)
}

fn fuse_weighted_results(
    sources: Vec<SourceQueryResponse>,
    weights: &[f32],
    top_k: usize,
) -> Result<Vec<SearchResult>, ZeppelinError> {
    if weights.len() != sources.len() {
        return Err(ZeppelinError::Validation(
            "fusion weights length must match sources length".into(),
        ));
    }

    let mut fused = HashMap::new();
    for (source, weight) in sources.into_iter().zip(weights.iter().copied()) {
        if !weight.is_finite() {
            return Err(ZeppelinError::Validation(
                "fusion weights must be finite".into(),
            ));
        }
        if source.response.results.is_empty() {
            continue;
        }

        let (min_score, max_score) = source.response.results.iter().fold(
            (f32::INFINITY, f32::NEG_INFINITY),
            |(min_score, max_score), result| {
                (min_score.min(result.score), max_score.max(result.score))
            },
        );

        for result in source.response.results {
            if !result.score.is_finite() {
                return Err(ZeppelinError::Validation(
                    "source result scores must be finite".into(),
                ));
            }
            let normalized =
                normalize_source_score(source.kind, result.score, min_score, max_score);
            add_fused_candidate(&mut fused, result, weight * normalized);
        }
    }

    Ok(sorted_fused_results(fused, top_k))
}

fn normalize_source_score(
    kind: QuerySourceKind,
    score: f32,
    min_score: f32,
    max_score: f32,
) -> f32 {
    if (max_score - min_score).abs() < f32::EPSILON {
        return 1.0;
    }

    match kind {
        QuerySourceKind::Ann => (max_score - score) / (max_score - min_score),
        QuerySourceKind::Bm25 => (score - min_score) / (max_score - min_score),
    }
}

fn add_fused_candidate(
    fused: &mut HashMap<String, SearchResult>,
    mut result: SearchResult,
    contribution: f32,
) {
    match fused.entry(result.id.clone()) {
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            let existing = entry.get_mut();
            existing.score += contribution;
            if existing.attributes.is_none() {
                existing.attributes = result.attributes.take();
            }
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
            result.score = contribution;
            entry.insert(result);
        }
    }
}

fn sorted_fused_results(fused: HashMap<String, SearchResult>, top_k: usize) -> Vec<SearchResult> {
    let mut results: Vec<SearchResult> = fused.into_values().collect();
    results.sort_by(fused_result_cmp);
    results.truncate(top_k);
    results
}

fn fused_result_cmp(a: &SearchResult, b: &SearchResult) -> Ordering {
    b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id))
}

async fn resolve_query_source_ref<'a>(
    state: &AppState,
    ns: &str,
    req: &'a QueryRequest,
    source: ValidatedSource,
    manifest: Manifest,
) -> Result<QuerySourceRef<'a>, ZeppelinError> {
    match source {
        ValidatedSource::LegacyVector => req
            .vector
            .as_deref()
            .map(|vector| QuerySourceRef::Ann {
                vector: Cow::Borrowed(vector),
                exclude_id: None,
            })
            .ok_or_else(|| ZeppelinError::Validation("vector must be provided".into())),
        ValidatedSource::LegacyBm25 => req
            .rank_by
            .as_ref()
            .map(|rank_by| QuerySourceRef::Bm25 {
                rank_by,
                last_as_prefix: req.last_as_prefix.unwrap_or(false),
            })
            .ok_or_else(|| ZeppelinError::Validation("rank_by must be provided".into())),
        ValidatedSource::AlgebraAnn { index } | ValidatedSource::AlgebraBm25 { index } => {
            resolve_algebra_source_ref(state, ns, req, index, manifest).await
        }
        ValidatedSource::AlgebraHybrid { .. } => Err(ZeppelinError::Validation(
            "hybrid query must execute through all algebra sources".into(),
        )),
    }
}

async fn resolve_algebra_source_ref<'a>(
    state: &AppState,
    ns: &str,
    req: &'a QueryRequest,
    index: usize,
    manifest: Manifest,
) -> Result<QuerySourceRef<'a>, ZeppelinError> {
    let sources = req
        .sources
        .as_ref()
        .ok_or_else(|| ZeppelinError::Validation("retrieval algebra sources missing".into()))?;
    match sources.get(index) {
        Some(CandidateSource::Ann { vector, id, .. }) => match (vector.as_deref(), id.as_deref()) {
            (Some(vector), None) => Ok(QuerySourceRef::Ann {
                vector: Cow::Borrowed(vector),
                exclude_id: None,
            }),
            (None, Some(id)) => {
                let vector = super::vectors::fetch_vector_values_by_id(
                    state,
                    ns,
                    id,
                    req.consistency,
                    manifest,
                )
                .await?
                .ok_or_else(|| ZeppelinError::VectorNotFound {
                    namespace: ns.to_string(),
                    id: id.to_string(),
                })?;
                Ok(QuerySourceRef::Ann {
                    vector: Cow::Owned(vector),
                    exclude_id: Some(id.to_string()),
                })
            }
            _ => Err(ZeppelinError::Validation(
                "ann source must provide exactly one of 'vector' or 'id'".into(),
            )),
        },
        Some(CandidateSource::Bm25 {
            rank_by,
            last_as_prefix,
        }) => Ok(QuerySourceRef::Bm25 {
            rank_by,
            last_as_prefix: last_as_prefix.or(req.last_as_prefix).unwrap_or(false),
        }),
        None => Err(ZeppelinError::Validation(
            "validated algebra source is missing".into(),
        )),
    }
}

fn nprobe_for_algebra_source(
    req: &QueryRequest,
    index: usize,
    default_nprobe: usize,
) -> Result<usize, ZeppelinError> {
    let sources = req
        .sources
        .as_ref()
        .ok_or_else(|| ZeppelinError::Validation("retrieval algebra sources missing".into()))?;
    match sources.get(index) {
        Some(CandidateSource::Ann { nprobe, .. }) => {
            Ok(nprobe.or(req.nprobe).unwrap_or(default_nprobe))
        }
        Some(CandidateSource::Bm25 { .. }) => Ok(default_nprobe),
        None => Err(ZeppelinError::Validation(
            "validated algebra source is missing".into(),
        )),
    }
}

fn notify_hydrator(state: &AppState, namespace: &str, manifest: &Manifest) {
    let Some(hydrator) = state.hydrator.as_ref() else {
        return;
    };
    let Some(segment) = active_segment_snapshot(manifest) else {
        return;
    };
    hydrator.observe_query(namespace, &segment);
}

fn active_segment_snapshot(manifest: &Manifest) -> Option<SegmentRef> {
    let active_segment = manifest.active_segment.as_ref()?;
    manifest
        .segments
        .iter()
        .find(|segment| segment.id == *active_segment)
        .cloned()
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
        response: Box::new(response),
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
