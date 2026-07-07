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
use crate::wal::manifest::SegmentRef;
use crate::wal::Manifest;

use super::ApiError;

/// Request body for querying vectors by ANN or BM25 ranking.
#[derive(Debug, Deserialize)]
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
    /// Response projection settings.
    #[serde(default)]
    pub projection: Option<ProjectionSpec>,
    /// Pagination cursor.
    #[serde(default)]
    pub cursor: Option<CursorSpec>,
    /// Explain output request.
    #[serde(default)]
    pub explain: Option<ExplainSpec>,
}

/// A typed candidate source in the retrieval-algebra request AST.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CandidateSource {
    /// ANN vector candidate source.
    Ann {
        /// Query vector for ANN search.
        vector: Vec<f32>,
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
#[derive(Debug, Deserialize)]
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
#[derive(Debug, Deserialize)]
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
    fn is_supported_contract_only(&self) -> bool {
        matches!(self, Self::Default | Self::None)
    }
}

/// Grouping strategy in the retrieval-algebra request AST.
#[derive(Debug, Deserialize)]
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

/// Projection settings in the retrieval-algebra request AST.
#[derive(Debug, Deserialize)]
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
#[derive(Debug, Deserialize)]
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

impl CursorSpec {
    fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }
}

/// Explain request in the retrieval-algebra request AST.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ExplainSpec {
    /// Boolean explain toggle.
    Flag(bool),
    /// Named explain mode.
    Mode(ExplainMode),
}

impl ExplainSpec {
    fn is_enabled(&self) -> bool {
        match self {
            Self::Flag(enabled) => *enabled,
            Self::Mode(mode) => !matches!(mode, ExplainMode::None),
        }
    }
}

/// Explain mode in the retrieval-algebra request AST.
#[derive(Debug, Deserialize)]
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
}

enum QuerySourceRef<'a> {
    Ann {
        vector: &'a [f32],
    },
    Bm25 {
        rank_by: &'a RankBy,
        last_as_prefix: bool,
    },
}

struct QueryExecutionOptions {
    manifest: Option<Manifest>,
    notify_hydration: bool,
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

    let result = execute_validated_query(
        &state,
        &ns,
        &meta,
        &req,
        validated,
        knobs.as_ref(),
        QueryExecutionOptions {
            manifest: None,
            notify_hydration: true,
        },
    )
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
    let (source, source_nprobe) = validate_query_source(req)?;
    validate_retrieval_algebra_options(req)?;
    let include_attributes = validate_projection(req)?;

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
    let nprobe = source_nprobe.or(req.nprobe).unwrap_or(knobs.default_nprobe);
    if let Some(requested) = source_nprobe.or(req.nprobe) {
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
        source,
    })
}

fn validate_query_source(
    req: &QueryRequest,
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

fn validate_multi_source_request(
    req: &QueryRequest,
    sources: &[CandidateSource],
) -> Result<(ValidatedSource, Option<usize>), ZeppelinError> {
    match req.fusion.as_ref() {
        Some(FusionSpec::Rrf { .. }) => Err(ZeppelinError::NotImplemented {
            feature: "multi-source RRF fusion",
        }),
        Some(FusionSpec::Weighted { weights }) => {
            if weights.len() != sources.len() {
                return Err(ZeppelinError::Validation(
                    "fusion weights length must match sources length".into(),
                ));
            }
            Err(ZeppelinError::NotImplemented {
                feature: "multi-source weighted fusion",
            })
        }
        Some(FusionSpec::None) | None => Err(ZeppelinError::Validation(
            "multiple candidate sources require a supported fusion strategy".into(),
        )),
    }
}

fn retrieval_algebra_without_sources(req: &QueryRequest) -> bool {
    req.fusion.is_some()
        || req.rerank.is_some()
        || req.grouping.is_some()
        || req.projection.is_some()
        || req.cursor.is_some()
        || req.explain.is_some()
}

fn validate_retrieval_algebra_options(req: &QueryRequest) -> Result<(), ZeppelinError> {
    if let Some(fusion) = req.fusion.as_ref() {
        if req.sources.as_ref().map_or(0, Vec::len) < 2 && !fusion.is_none() {
            return Err(ZeppelinError::Validation(
                "fusion requires at least two candidate sources".into(),
            ));
        }
    }
    if let Some(rerank) = req.rerank.as_ref() {
        if !rerank.is_supported_contract_only() {
            return Err(ZeppelinError::NotImplemented {
                feature: "explicit rerank",
            });
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
            GroupingSpec::Field { .. } => {
                return Err(ZeppelinError::NotImplemented {
                    feature: "grouped results",
                });
            }
        }
    }
    if let Some(cursor) = req.cursor.as_ref() {
        if !cursor.is_none() {
            return Err(ZeppelinError::NotImplemented {
                feature: "cursor pagination",
            });
        }
    }
    if let Some(explain) = req.explain.as_ref() {
        if explain.is_enabled() {
            return Err(ZeppelinError::NotImplemented {
                feature: "query explain",
            });
        }
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
    match query_source_ref(req, validated.source)? {
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

            let manifest = match options.manifest {
                Some(manifest) => manifest,
                None => {
                    query::read_manifest_for_query(
                        &state.store,
                        ns,
                        req.consistency,
                        Some(&state.manifest_cache),
                    )
                    .await?
                }
            };
            if options.notify_hydration {
                notify_hydrator(state, ns, &manifest);
            }

            query::execute_bm25_query_with_manifest(
                &state.store,
                &state.wal_reader,
                ns,
                rank_by,
                &meta.full_text_search,
                validated.top_k,
                req.filter.as_ref(),
                req.consistency,
                last_as_prefix,
                Some(&state.fts_cache),
                Some(&state.cache),
                knobs.bm25_max_full_scan_clusters,
                validated.include_attributes,
                manifest,
            )
            .await
        }
        QuerySourceRef::Ann { vector } => {
            if vector.len() != meta.dimensions {
                return Err(ZeppelinError::DimensionMismatch {
                    expected: meta.dimensions,
                    actual: vector.len(),
                });
            }
            // Reject NaN/inf: non-finite query values make every distance
            // comparison nondeterministic (partial_cmp falls back to Equal).
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

            let manifest = match options.manifest {
                Some(manifest) => manifest,
                None => {
                    query::read_manifest_for_query(
                        &state.store,
                        ns,
                        req.consistency,
                        Some(&state.manifest_cache),
                    )
                    .await?
                }
            };
            if options.notify_hydration {
                notify_hydrator(state, ns, &manifest);
            }
            query::execute_query_with_manifest(params, manifest).await
        }
    }
}

fn query_source_ref<'a>(
    req: &'a QueryRequest,
    source: ValidatedSource,
) -> Result<QuerySourceRef<'a>, ZeppelinError> {
    match source {
        ValidatedSource::LegacyVector => req
            .vector
            .as_deref()
            .map(|vector| QuerySourceRef::Ann { vector })
            .ok_or_else(|| ZeppelinError::Validation("vector must be provided".into())),
        ValidatedSource::LegacyBm25 => req
            .rank_by
            .as_ref()
            .map(|rank_by| QuerySourceRef::Bm25 {
                rank_by,
                last_as_prefix: req.last_as_prefix.unwrap_or(false),
            })
            .ok_or_else(|| ZeppelinError::Validation("rank_by must be provided".into())),
        ValidatedSource::AlgebraAnn { index } => {
            let sources = req.sources.as_ref().ok_or_else(|| {
                ZeppelinError::Validation("retrieval algebra sources missing".into())
            })?;
            match sources.get(index) {
                Some(CandidateSource::Ann { vector, .. }) => Ok(QuerySourceRef::Ann { vector }),
                _ => Err(ZeppelinError::Validation(
                    "validated ANN source is missing".into(),
                )),
            }
        }
        ValidatedSource::AlgebraBm25 { index } => {
            let sources = req.sources.as_ref().ok_or_else(|| {
                ZeppelinError::Validation("retrieval algebra sources missing".into())
            })?;
            match sources.get(index) {
                Some(CandidateSource::Bm25 {
                    rank_by,
                    last_as_prefix,
                }) => Ok(QuerySourceRef::Bm25 {
                    rank_by,
                    last_as_prefix: last_as_prefix.or(req.last_as_prefix).unwrap_or(false),
                }),
                _ => Err(ZeppelinError::Validation(
                    "validated BM25 source is missing".into(),
                )),
            }
        }
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
