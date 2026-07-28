//! Validates HTTP query requests and orchestrates Zeppelin's retrieval algebra.
//!
//! This is the API boundary between Axum and the domain query engine in
//! [`crate::query`]. The handlers deserialize the wire format, reject malformed
//! combinations before doing namespace or storage work, choose one authoritative
//! manifest snapshot, execute ANN and/or BM25 candidate sources, and apply
//! response-level fusion, reranking, facets, grouping, cursor pagination,
//! projection, explain data, and diagnostics. They deliberately delegate WAL,
//! immutable-segment, bitmap-filter, index, and cache mechanics to the domain
//! layer rather than reproducing them here.
//!
//! [`query_namespace`][crate::server::handlers::query::query_namespace] serves
//! `POST /v1/namespaces/:ns/query`, including optional point-in-time reads
//! selected by `?as_of=...`.
//! [`batch_query_namespace`][crate::server::handlers::query::batch_query_namespace] serves
//! `POST /v1/namespaces/:ns/query/batch`; it validates entries independently,
//! resolves the namespace and one shared live manifest once, and returns a
//! positional success-or-error envelope for every entry.
//!
//! ## Reading map
//!
//! 1. Start with [`QueryRequest`][crate::server::handlers::query::QueryRequest]
//!    and [`CandidateSource`][crate::server::handlers::query::CandidateSource]
//!    for the legacy and retrieval-algebra request shapes.
//! 2. Read [`query_namespace`][crate::server::handlers::query::query_namespace]
//!    and [`batch_query_namespace`][crate::server::handlers::query::batch_query_namespace]
//!    for HTTP ordering,
//!    metrics, rate charging, snapshot choice, and error boundaries.
//! 3. Follow `validate_query_shape`, `validate_query_source`, and
//!    `validate_retrieval_algebra_options` for the I/O-free request contract.
//! 4. Continue with `execute_validated_query`, `execute_hybrid_query`, and
//!    `execute_query_source_with_manifest` for source execution against one
//!    manifest.
//! 5. Read `fuse_source_responses` and `apply_rerank_if_requested` for the
//!    response pipeline, then the facet, grouping, and cursor helpers.
//! 6. Finish with `ExplainAccumulator`, hydration notification, and batch entry
//!    construction for observational side effects and response metadata.
//!
//! ## Single-query flow
//!
//! ```text
//! HTTP bytes + path + query string
//!             |
//!             v
//! deserialize and validate shape -------- invalid -> ApiError (no query metric)
//!             |
//!             v
//! resolve namespace metadata
//!             |
//!             +--> as_of present -> resolve retained manifest from S3/MinIO
//!             |
//!             `--> live query ----> strong/eventual manifest read or cache
//!                                      |
//!                                      v
//!                       one authoritative Manifest snapshot
//!                                      |
//!                +---------------------+---------------------+
//!                |                                           |
//!                v                                           v
//!          ANN source(s)                               BM25 source(s)
//!      lower distance is better                  higher relevance is better
//!                |                                           |
//!                +------------- fuse if needed --------------+
//!                                      |
//!                                      v
//!             facet frontier -> optional rerank -> group/cursor -> projection
//!                                      |
//!                                      v
//!                   QueryResponse + optional explain/debug
//! ```
//!
//! A metadata [`Filter`][crate::types::Filter] is passed to each source so
//! filtering happens while the source candidate frontier is built, before
//! fusion and later presentation transforms. ANN and BM25 raw scores have
//! opposite directions and unrelated scales: reciprocal-rank fusion uses only
//! source positions, while weighted fusion converts each source to a
//! direction-adjusted `[0, 1]` range first. Exact vector reranking again yields
//! a distance, so smaller is better; BM25 reranking yields relevance, so larger
//! is better.
//!
//! ## Batch snapshot and failure flow
//!
//! ```text
//! batch body -> validate each entry -> resolve namespace once
//!      |                  |                    |
//!      |                  | invalid            | missing/error
//!      |                  v                    v
//!      |          preserve entry error     all valid entries receive
//!      |                                   the namespace error
//!      v
//! strongest consistency among valid entries
//!      |
//!      v
//! read one live Manifest and clone the snapshot for every valid entry
//!      |
//!      +--> entry succeeds -> { ok: true, response, latency }
//!      `--> entry fails ----> { ok: false, error, latency }
//! ```
//!
//! Cloning a [`Manifest`][crate::wal::Manifest] gives each sequential batch
//! execution the same owned visibility description; no entry silently upgrades
//! to a newer generation. A batch has no `as_of` query parameter. It chooses
//! strong manifest freshness if any shape-valid entry requests strong
//! consistency, although each source still applies its own requested WAL
//! semantics.
//!
//! ## Invariants
//!
//! - Request-shape validation precedes namespace I/O and single-query metrics,
//!   so malformed input remains a client error even for a missing namespace.
//! - The selected S3/MinIO manifest controls artifact visibility. Caches and
//!   hydration never add artifacts to the current query snapshot.
//! - Every source in one hybrid or batch execution receives the selected owned
//!   manifest rather than re-reading visibility independently.
//! - Cursor tokens are opaque integrity checks, not durable server-side state.
//!   They bind namespace, ranking-affecting request fields, score bits, and ID;
//!   they do not freeze a live manifest generation.
//! - Grouping and cursoring are mutually exclusive. Facets count the filtered,
//!   pre-rerank candidate frontier, not the entire namespace or final page.
//! - Unsupported projections and inconsistent request combinations fail
//!   explicitly; this layer does not silently approximate them.
//!
//! ## Rust concepts used here
//!
//! Serde's tagged enums model the request grammar as closed Rust variants. This
//! resembles a Java sealed hierarchy and a C tagged union, but exhaustive
//! `match` expressions make omitted cases a compiler error. `Option<T>` records
//! whether a field was absent without null sentinels, and `Result<T, E>` plus
//! `?` keeps validation, storage, and index failures on explicit paths.
//!
//! [`Cow`][std::borrow::Cow] lets an ANN source borrow coordinates embedded in
//! the request or own coordinates fetched by ID behind one type. Java would use
//! references without compiler-enforced lifetime distinctions; C would need a
//! pointer plus an ownership convention. Rust proves borrowed request data
//! cannot outlive the request and drops an owned fetched vector automatically.
//!
//! Handler extractors move shared [`AppState`][crate::server::AppState] handles
//! and owned request bytes into an async future. Small validated enums are
//! `Copy`, while manifest clones duplicate the manifest data intentionally.
//! `DurationGuard` uses RAII: its [`Drop`] implementation records latency on
//! both success and every early error, analogous to Java `finally` or a C
//! cleanup label but enforced by scope.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;

use axum::extract::{Extension, Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tracing::{info, instrument};
use xxhash_rust::xxh3::xxh3_64;

use crate::cache::hydration::HydrationTarget;
use crate::config::IndexingConfig;
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
use crate::security::{
    apply_field_mask, filter_references_denied_field, AllowDecision, CursorBindingKey, FieldMask,
    PolicyVersion, SecurityError,
};
use crate::server::{AppState, AuditRequest, RateLimitClass, RateLimitIdentity};
use crate::types::{AttributeValue, ConsistencyLevel, Filter, SearchResult};
use crate::wal::manifest::SegmentRef;
use crate::wal::Manifest;

use super::as_of;
use super::ApiError;

/// Describes one legacy or retrieval-algebra query request.
///
/// A legacy request supplies exactly one of `vector` and `rank_by`. An algebra
/// request supplies `sources` and may add fusion, reranking, grouping, facets,
/// projection, cursoring, and explain output. Unknown JSON fields are rejected
/// so misspelled controls never degrade silently into defaults.
///
/// `consistency` defaults to [`ConsistencyLevel::Strong`]. `top_k` and the
/// wider `candidate_k` frontier are finalized from one runtime snapshot during
/// validation. An omitted `nprobe` remains absent until the fixed manifest
/// reveals the active flat segment's cluster count.
///
/// # Examples
///
/// A legacy ANN body can provide a vector and `top_k = 10`. A hybrid algebra
/// body can provide one ANN and one BM25 source, request RRF, and ask each source
/// for 100 candidates before returning the best ten fused IDs.
///
/// # Rust Notes for Java/C Engineers
///
/// Serde constructs this owned value from JSON. Optional fields retain the
/// distinction between “absent, use the server snapshot” and an explicit
/// value. In Java this usually requires nullable fields plus validation; in C
/// it requires presence flags alongside values.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRequest {
    /// Coordinates for legacy ANN search; mutually exclusive with `rank_by` and `sources`.
    #[serde(default)]
    pub vector: Option<Vec<f32>>,
    /// BM25 expression for legacy lexical search; mutually exclusive with vector sources.
    #[serde(default)]
    pub rank_by: Option<RankBy>,
    /// Whether each BM25 field query's last token also matches indexed prefixes.
    #[serde(default)]
    pub last_as_prefix: Option<bool>,
    /// Maximum flat results, groups, or page entries to return after all transforms.
    #[serde(default)]
    pub top_k: Option<usize>,
    /// Wider per-source frontier used by fusion and post-retrieval transforms.
    #[serde(default)]
    pub candidate_k: Option<usize>,
    /// Metadata predicate pushed into every source before candidates are retained.
    #[serde(default)]
    pub filter: Option<Filter>,
    /// WAL freshness mode; omitted JSON selects strong consistency.
    #[serde(default)]
    pub consistency: ConsistencyLevel,
    /// Top-level IVF cluster probe count for ANN sources that do not override it.
    #[serde(default)]
    pub nprobe: Option<usize>,
    /// Legacy attribute projection flag; defaults to `true`.
    #[serde(default)]
    pub include_attributes: Option<bool>,
    /// Ordered algebra sources; their positions align with weighted-fusion weights.
    #[serde(default)]
    pub sources: Option<Vec<CandidateSource>>,
    /// Multi-source combination strategy; omitted multi-source fusion defaults to RRF.
    #[serde(default)]
    pub fusion: Option<FusionSpec>,
    /// Optional second-stage scorer applied only to first-stage candidates.
    #[serde(default)]
    pub rerank: Option<RerankSpec>,
    /// Optional response grouping applied after source scoring and reranking.
    #[serde(default)]
    pub grouping: Option<GroupingSpec>,
    /// Attribute fields counted over the filtered pre-rerank candidate frontier.
    #[serde(default)]
    pub facets: Option<Vec<FacetSpec>>,
    /// Response materialization controls; field and vector projection are unsupported.
    #[serde(default)]
    pub projection: Option<ProjectionSpec>,
    /// Stateless page marker request; presence also widens retrieval by one result.
    #[serde(default)]
    pub cursor: Option<CursorSpec>,
    /// Plan or full per-result provenance output request.
    #[serde(default)]
    pub explain: Option<ExplainSpec>,
    /// Whether source execution should collect and return timing/cache diagnostics.
    #[serde(default)]
    pub debug: Option<bool>,
}

/// Validates server-owned query constraints before any retrieval planning.
fn validate_query_security_constraints(
    req: &QueryRequest,
    decision: &AllowDecision,
) -> Result<(), ZeppelinError> {
    if decision.mandatory_filter.is_some() && req.debug == Some(true) {
        return Err(SecurityError::ConstraintViolation.into());
    }

    let Some(mask) = decision.field_mask.as_ref() else {
        return Ok(());
    };
    let denied = mask.denied_fields();
    let masked_filter = req
        .filter
        .as_ref()
        .is_some_and(|filter| filter_references_denied_field(filter, denied));
    let masked_rank = req
        .rank_by
        .iter()
        .chain(
            req.sources
                .iter()
                .flatten()
                .filter_map(|source| match source {
                    CandidateSource::Bm25 { rank_by, .. } => Some(rank_by),
                    CandidateSource::Ann { .. } => None,
                }),
        )
        .chain(req.rerank.iter().filter_map(|rerank| match rerank {
            RerankSpec::Bm25 { rank_by } => Some(rank_by),
            RerankSpec::Default | RerankSpec::None | RerankSpec::Vector { .. } => None,
        }))
        .any(|rank_by| {
            rank_by
                .extract_field_queries()
                .iter()
                .any(|(field, _)| denied.contains(field))
        });
    if masked_filter
        || masked_rank
        || req
            .facets
            .as_ref()
            .is_some_and(|facets| facets.iter().any(|facet| denied.contains(facet.field())))
        || matches!(
            req.grouping.as_ref(),
            Some(GroupingSpec::Field { field, .. }) if denied.contains(field)
        )
    {
        return Err(SecurityError::ConstraintViolation.into());
    }
    Ok(())
}

/// Selects one typed candidate generator in the retrieval-algebra request.
///
/// ANN can use caller-provided coordinates or load a stored vector by ID. The
/// by-ID form excludes that seed ID from results. BM25 carries a lexical ranking
/// expression. A hybrid request retains source order for explain output and
/// weighted fusion.
///
/// # Examples
///
/// `Ann { id: "seed-7", ... }` fetches `seed-7` from the selected manifest and
/// searches for neighbors without returning the seed itself. `Bm25` retrieves
/// records matching its configured full-text fields.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CandidateSource {
    /// Approximate nearest-neighbor source where lower raw distance is better.
    Ann {
        /// Inline query coordinates; exactly one of `vector` and `id` is required.
        #[serde(default)]
        vector: Option<Vec<f32>>,
        /// Stored vector whose coordinates become the query and whose ID is excluded.
        #[serde(default)]
        id: Option<String>,
        /// Source-local IVF probe count, mutually exclusive with top-level `nprobe`.
        #[serde(default)]
        nprobe: Option<usize>,
    },
    /// Full-text source where higher raw relevance is better.
    Bm25 {
        /// Expression whose field queries are validated against namespace FTS config.
        rank_by: RankBy,
        /// Source-local prefix choice, falling back to the top-level option when absent.
        #[serde(default)]
        last_as_prefix: Option<bool>,
    },
}

/// Chooses how multiple source rankings become one comparable score list.
///
/// RRF is scale independent and rewards source rank. Weighted fusion first
/// min-max normalizes ANN distances and BM25 relevance so that `1.0` is best in
/// both source kinds, then multiplies by each positional weight.
///
/// # Examples
///
/// With `Rrf { k: 60 }`, a rank-one hit contributes `1 / 61` from that source.
/// With weights `[0.25, 0.75]`, the second source's normalized score contributes
/// three times as much as the first source's score.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum FusionSpec {
    /// No fusion; rejected when more than one source is present.
    None,
    /// Reciprocal rank fusion over source positions.
    Rrf {
        /// Positive smoothing constant; omitted means the private `DEFAULT_RRF_K` value.
        #[serde(default)]
        k: Option<usize>,
    },
    /// Direction-adjusted min-max score fusion.
    Weighted {
        /// Finite weights in exactly the same order and count as `sources`.
        weights: Vec<f32>,
    },
}

impl FusionSpec {
    /// Reports whether this specification explicitly disables fusion.
    ///
    /// # Returns
    ///
    /// `true` only for [`FusionSpec::None`].
    ///
    /// # Examples
    ///
    /// Validation accepts `None` for a single source but rejects it for two.
    fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }
}

/// Selects an optional scorer for the already-retrieved candidate frontier.
///
/// Explicit vector reranking fetches each candidate's stored coordinates and
/// sorts by exact distance to a second vector. BM25 reranking tokenizes
/// candidate attributes and scores only that in-memory frontier; it is not a
/// replacement for the namespace-wide BM25 source.
///
/// # Examples
///
/// A request can retrieve 100 ANN candidates, rerank them by a different vector,
/// and return ten. No reranker can introduce an ID absent from those 100.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RerankSpec {
    /// Keep the source engine's built-in behavior without another HTTP-layer scorer.
    Default,
    /// Add no HTTP-layer reranker; ANN indexes may still exact-rerank internally.
    None,
    /// Recompute candidate scores as distance to another vector.
    Vector {
        /// Finite coordinates whose dimensions must match the namespace.
        vector: Vec<f32>,
    },
    /// Recompute relevance from candidate attributes using a BM25 expression.
    Bm25 {
        /// Expression over namespace fields configured for full-text search.
        rank_by: RankBy,
    },
}

impl RerankSpec {
    /// Reports whether this variant requests an actual second-stage scorer.
    ///
    /// # Returns
    ///
    /// `true` for vector or BM25 reranking and `false` for default/no-op modes.
    ///
    /// # Examples
    ///
    /// An explicit scorer widens first-stage retrieval to `candidate_k`; a
    /// `Default` request can stay at `top_k`.
    fn is_explicit(&self) -> bool {
        matches!(self, Self::Vector { .. } | Self::Bm25 { .. })
    }
}

/// Selects flat output or post-ranking groups keyed by one attribute field.
///
/// Groups preserve the first appearance of each key in ranked order and retain
/// at most `max_per_group` members. Records missing the field become separate
/// singleton groups keyed by their own IDs.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum GroupingSpec {
    /// Return the ordinary flat ranked list.
    None,
    /// Group results by a string representation of one attribute.
    Field {
        /// Attribute field whose scalar or list value becomes the display key.
        field: String,
        /// Positive maximum number of ranked members retained in one group.
        max_per_group: usize,
    },
}

/// Names one attribute field to count over the filtered candidate frontier.
///
/// The transparent newtype serializes as a JSON string while preventing facet
/// names from being confused with unrelated strings inside this module.
/// Duplicate requested fields are counted once; an absent field returns an
/// empty count map.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(transparent)]
pub struct FacetSpec(
    /// Exact request field name; validation rejects the empty string.
    String,
);

impl FacetSpec {
    /// Borrows the requested field name without allocating.
    ///
    /// # Returns
    ///
    /// The exact field string supplied by the request.
    ///
    /// # Examples
    ///
    /// A JSON facet string `"category"` yields the borrowed name `category`.
    fn field(&self) -> &str {
        &self.0
    }
}

/// Controls which stored payloads are materialized into result JSON.
///
/// Attribute inclusion is implemented and can avoid metadata work in the
/// source engine. Field-level projection and returning vectors are rejected as
/// not implemented rather than ignored.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionSpec {
    /// Whether complete result attribute maps should be returned; defaults to `true`.
    #[serde(default)]
    pub include_attributes: Option<bool>,
    /// Requested attribute subset; any present value is currently rejected.
    #[serde(default)]
    pub fields: Option<Vec<String>>,
    /// Requested stored-vector output; `true` is currently rejected.
    #[serde(default)]
    pub include_vectors: Option<bool>,
}

/// Selects the first page or continuation after an opaque rank marker.
///
/// Cursor paging is stateless: the token embeds a request fingerprint, score
/// bits, ID, and issuing policy version under an HMAC-SHA256 tag. It rejects
/// deliberate field splicing as well as accidental reuse with another query,
/// and the fingerprint binds the query route's exact `as_of` selector. It does
/// not otherwise pin a live manifest generation.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CursorSpec {
    /// Request the first page while enabling cursor output.
    None,
    /// Continue strictly after a prior page's final score-and-ID marker.
    After {
        /// Versioned token returned as `next_cursor` by a compatible request.
        token: String,
    },
}

/// Accepts either a boolean shorthand or a named explain mode.
///
/// The untagged Serde representation keeps wire compatibility with both
/// `"explain": true` and `"explain": "full"`. It must therefore remain in a
/// self-describing JSON format rather than positional binary serialization.
#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum ExplainSpec {
    /// Boolean shorthand: `true` means plan mode and `false` means omitted.
    Flag(bool),
    /// Explicit `none`, `plan`, or `full` mode.
    Mode(ExplainMode),
}

/// Chooses whether explain output is absent, plan-only, or per-result.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExplainMode {
    /// Omit explain output.
    None,
    /// Return the effective source and transform plan.
    Plan,
    /// Return the plan plus source, fusion, and explicit-rerank provenance.
    Full,
}

/// Holds the positional requests submitted to the batch route.
///
/// The outer request must contain at least one entry and may not exceed the
/// configured batch size. Entry failures remain isolated response elements once
/// namespace and shared-manifest setup succeeds.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchQueryRequest {
    /// Query bodies whose output entries preserve this exact order.
    pub queries: Vec<QueryRequest>,
}

/// I/O-free result of expanding non-segment defaults and validating shape.
///
/// This compact `Copy` value is passed through execution so downstream code
/// cannot reinterpret absent fields with a newer runtime snapshot. Omitted
/// nprobe remains explicit in this value until manifest-aware resolution.
#[derive(Debug, Clone, Copy)]
struct ValidatedQuery {
    /// Final response/page/group limit after configuration defaults.
    top_k: usize,
    /// Per-source frontier retained for transforms that need extra candidates.
    candidate_k: usize,
    /// Explicit top-level or single-source ANN probes; `None` preserves omission.
    nprobe: Option<usize>,
    /// Effective response attribute projection.
    include_attributes: bool,
    /// Validated source syntax and execution path.
    source: ValidatedSource,
    /// Caller-visible query identity computed before server-owned filters are attached.
    cursor_fingerprint: Option<u64>,
}

/// Identifies the request syntax and source cardinality after validation.
///
/// Indices point into the request's owned `sources` vector. The hybrid variant
/// stores the validated count so execution can detect an impossible mismatch
/// instead of indexing unchecked input.
#[derive(Debug, Clone, Copy)]
enum ValidatedSource {
    /// Legacy top-level vector source.
    LegacyVector,
    /// Legacy top-level BM25 expression.
    LegacyBm25,
    /// One algebra ANN source at the given request position.
    AlgebraAnn {
        /// Zero-based position in [`QueryRequest::sources`].
        index: usize,
    },
    /// One algebra BM25 source at the given request position.
    AlgebraBm25 {
        /// Zero-based position in [`QueryRequest::sources`].
        index: usize,
    },
    /// Multiple algebra sources that require fusion.
    AlgebraHybrid {
        /// Cardinality checked again before source iteration.
        source_count: usize,
    },
}

/// Records the native score direction of an executed source.
///
/// ANN distances sort ascending; BM25 relevance sorts descending. Carrying this
/// enum beside each response prevents fusion from guessing score direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuerySourceKind {
    /// Approximate-neighbor distance, where lower is better.
    Ann,
    /// Lexical relevance, where higher is better.
    Bm25,
}

/// Provides one source's executable inputs as borrowed or owned views.
///
/// Inline ANN coordinates borrow from [`QueryRequest`]. A vector loaded by ID
/// is owned by [`Cow::Owned`] because it must survive after the fetch future
/// returns. BM25 expressions always borrow the request.
///
/// ```text
/// QueryRequest owns inline vector/rank_by ---- shared borrow ----+
///                                                              |
/// selected Manifest -> fetch vector by ID -> owned Vec<f32> ----+--> source execution
/// ```
#[derive(Debug)]
enum QuerySourceRef<'a> {
    /// ANN coordinates and an optional seed ID to remove from results.
    Ann {
        /// Borrowed inline slice or owned by-ID vector.
        vector: Cow<'a, [f32]>,
        /// Stored seed ID excluded after requesting one extra candidate.
        exclude_id: Option<String>,
    },
    /// BM25 expression and effective final-token prefix setting.
    Bm25 {
        /// Borrowed ranking AST from the request.
        rank_by: &'a RankBy,
        /// Whether each field query's last token matches prefixes.
        last_as_prefix: bool,
    },
}

/// Couples a source response with the information needed to interpret scores.
struct SourceQueryResponse {
    /// Native score direction used by normalization and explain output.
    kind: QuerySourceKind,
    /// Domain response, including source work counters and optional diagnostics.
    response: QueryResponse,
}

/// Default reciprocal-rank-fusion offset when the request omits `k`.
///
/// Rank one contributes `1 / 61`, which keeps any single top rank from
/// overwhelming evidence contributed by another source.
const DEFAULT_RRF_K: usize = 60;

/// Supplies a preselected manifest and hydration policy to one execution.
///
/// Single live queries normally leave `manifest` absent so consistency-aware
/// loading occurs here. Historical and batch/hybrid callers pass a snapshot so
/// every source sees the same artifact membership.
struct QueryExecutionOptions {
    /// Manifest selected by the caller, or `None` to read the current one.
    manifest: Option<Manifest>,
    /// Whether a live active segment should contribute to hydrator heat.
    notify_hydration: bool,
}

/// Complete borrowed and frozen state for one validated query execution.
///
/// Keeping the execution seam to one typed value prevents single and batch
/// callers from swapping the request, policy decision, runtime knobs, or
/// manifest options positionally.
struct ValidatedQueryExecution<'a> {
    state: &'a AppState,
    ns: &'a str,
    meta: &'a NamespaceMetadata,
    req: &'a QueryRequest,
    validated: ValidatedQuery,
    knobs: &'a QueryKnobs,
    security: &'a AllowDecision,
    options: QueryExecutionOptions,
}

/// Parses optional point-in-time selection for the single-query route.
///
/// Unknown parameters are rejected. Batch queries intentionally have no
/// corresponding extractor and always use a live manifest.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRouteParams {
    /// Retained generation, RFC3339 timestamp, or `snapshot:name` selector.
    #[serde(default)]
    as_of: Option<String>,
}

/// Returns one positional outcome for every batch request entry.
///
/// The route itself returns HTTP 200 after it can interpret the outer batch;
/// individual domain failures carry their single-query-equivalent status in
/// [`BatchQueryError::status`]. Outer-body, size, and rate-limit failures remain
/// top-level HTTP errors.
#[derive(Debug, Serialize)]
pub struct BatchQueryResponse {
    /// Success/error entries aligned one-for-one with the input requests.
    pub results: Vec<BatchQueryEntry>,
}

/// Represents either a complete query response or a canonical per-entry error.
///
/// Serde's untagged layout preserves the established `ok` discriminator while
/// allowing success and error payloads to differ. `Box<QueryResponse>` keeps the
/// enum's stack size small despite the larger success variant.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum BatchQueryEntry {
    /// Successful entry with the same payload shape as the single-query route.
    Success {
        /// Always `true` for successful entries.
        ok: bool,
        /// Owned single-query response allocated behind a compact pointer.
        response: Box<QueryResponse>,
        /// Per-entry metadata.
        metadata: BatchQueryEntryMetadata,
    },
    /// Failed entry that does not abort later batch entries.
    Error {
        /// Always `false` for failed entries.
        ok: bool,
        /// Client-safe projection of the domain error.
        error: BatchQueryError,
        /// Per-entry metadata.
        metadata: BatchQueryEntryMetadata,
    },
}

/// Reports work measured independently for one batch entry.
#[derive(Debug, Serialize)]
pub struct BatchQueryEntryMetadata {
    /// Milliseconds from entry execution/envelope construction to completion.
    pub latency_ms: u64,
}

/// Projects a [`ZeppelinError`] into stable, client-safe batch fields.
///
/// The original error remains available to server logs. This type mirrors the
/// single-query HTTP classification without leaking internal error details.
#[derive(Debug, Serialize)]
pub struct BatchQueryError {
    /// Stable machine-readable code returned by [`ZeppelinError::error_code`].
    pub code: &'static str,
    /// Sanitized message suitable for an API response.
    pub error: String,
    /// HTTP status the equivalent single-query failure would use.
    pub status: u16,
    /// Whether retrying unchanged may succeed according to the domain error.
    pub retryable: bool,
}

impl BatchQueryError {
    /// Converts a domain error into the canonical public batch envelope.
    ///
    /// # Parameters
    ///
    /// - `err`: Borrowed error whose safe classification should be copied.
    ///
    /// # Returns
    ///
    /// An owned error envelope containing static code, safe text, status, and
    /// retry advice.
    ///
    /// # Examples
    ///
    /// A dimension mismatch becomes a non-retryable 400-class entry while an
    /// internal storage failure retains its server-error classification.
    fn from_error(err: &ZeppelinError) -> Self {
        Self {
            code: err.error_code(),
            error: err.client_message(),
            status: err.status_code(),
            retryable: err.retryable(),
        }
    }
}

/// Executes one query request against a live or retained manifest snapshot.
///
/// The handler parses JSON directly to avoid Axum's more expensive path-aware
/// JSON wrapper, then validates the complete request shape before resolving the
/// namespace or incrementing query metrics. After metadata and optional `as_of`
/// resolution, it delegates source work to [`crate::query`] and applies the
/// retrieval-algebra response pipeline defined in this module.
///
/// # Parameters
///
/// - `state`: Shared server services, runtime configuration, caches, store,
///   namespace manager, WAL reader, and optional hydrator.
/// - `ns`: Namespace captured from the route path.
/// - `params`: Strict query-string parameters, including optional `as_of`.
/// - `body`: Owned raw request bytes subject to router body limits.
///
/// # Returns
///
/// JSON containing ranked results, source scan counters, and requested optional
/// cursor, groups, facets, explain, or debug sections.
///
/// # Errors
///
/// Returns [`ApiError`] for malformed JSON or request combinations, missing
/// namespaces, invalid dimensions/FTS fields, point-in-time retention failures,
/// and manifest, WAL, segment, cache, or index errors. Shape failures occur
/// before storage work and before query metrics are counted. No partial response
/// is returned when source execution fails.
///
/// # Side Effects
///
/// Increments active/total query metrics after shape validation, records latency
/// through `DurationGuard` on every later exit, logs successful completion, and
/// may notify the background hydrator for a live active segment. It performs
/// read-only object-store/cache operations and publishes no artifacts.
///
/// # Consistency
///
/// A live request loads a strong or eventual current manifest through the
/// domain layer. An `as_of` request reads an exact retained manifest from object
/// storage and passes that owned snapshot through every source; it does not
/// notify live-segment hydration. The manifest controls visible WAL fragments
/// and segments throughout execution.
///
/// # Performance
///
/// Direct deserialization is one full JSON parse. Shape checks are in-memory.
/// Namespace metadata and `as_of` resolution may perform remote reads; source
/// cost then depends on consistency, cache hits, visible WAL, active index,
/// filters, `nprobe`, candidate width, and optional vector enrichment/reranking.
///
/// # Examples
///
/// A strong ANN request for ten results validates first, reads current namespace
/// metadata and manifest, searches visible WAL and the active segment, and may
/// notify hydration. The same body with `?as_of=12` searches only generation
/// 12's artifacts. A malformed body sent to a missing namespace returns
/// validation rather than allowing the missing namespace to mask that error.
///
/// # Rust Notes for Java/C Engineers
///
/// Axum extractors move owned path/body values and a cheaply cloned shared state
/// handle into the async future. `?`-style conversions preserve typed failures;
/// RAII guards decrement the active gauge and observe duration even when an
/// awaited operation returns early.
#[instrument(skip(state, decision, body), fields(namespace = %ns))]
pub async fn query_namespace(
    State(state): State<AppState>,
    Extension(decision): Extension<AllowDecision>,
    Path(ns): Path<String>,
    Query(params): Query<QueryRouteParams>,
    body: bytes::Bytes,
) -> Result<Json<QueryResponse>, ApiError> {
    let req: QueryRequest = serde_json::from_slice(&body).map_err(|e| {
        ApiError(ZeppelinError::Validation(format!(
            "invalid request body: {e}"
        )))
    })?;
    let mut cursor_fingerprint = if params.as_of.is_none() {
        cursor_fingerprint_if_requested(&ns, None, &req).map_err(ApiError::from)?
    } else {
        None
    };
    validate_query_security_constraints(&req, &decision).map_err(ApiError::from)?;
    let knobs = state.runtime_query_config.snapshot();

    // ---- Phase 1: request-shape validation (NO I/O, needs no metadata) ----
    // Runs BEFORE namespace resolution so a malformed request to a missing
    // namespace is a 400 (bad request), not a 404 (Task 14 I2). Also runs
    // BEFORE the query metrics increment so rejected requests aren't counted
    // as queries (I3).
    let mut validated = validate_query_shape(&req, knobs.as_ref(), &state, cursor_fingerprint)
        .map_err(ApiError::from)?;
    validate_cursor_security_binding(&req, decision.policy_version, decision.cursor_binding_key)
        .map_err(ApiError::from)?;
    if params.as_of.is_none() {
        validate_cursor_query_binding(&req, cursor_fingerprint).map_err(ApiError::from)?;
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
    let as_of_manifest = match params.as_of.as_deref() {
        Some(as_of) => Some(as_of::resolve_manifest(&state.store, &ns, as_of).await?),
        None => None,
    };
    if let (Some(selector), Some(manifest)) = (params.as_of.as_deref(), as_of_manifest.as_ref()) {
        cursor_fingerprint =
            cursor_fingerprint_if_requested(&ns, Some((selector, manifest.version())), &req)
                .map_err(ApiError::from)?;
        validate_cursor_query_binding(&req, cursor_fingerprint).map_err(ApiError::from)?;
        validated.cursor_fingerprint = cursor_fingerprint;
    }
    let notify_hydration = as_of_manifest.is_none();
    let result = execute_validated_query(ValidatedQueryExecution {
        state: &state,
        ns: &ns,
        meta: &meta,
        req: &req,
        validated,
        knobs: knobs.as_ref(),
        security: &decision,
        options: QueryExecutionOptions {
            manifest: as_of_manifest,
            notify_hydration,
        },
    })
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

/// Executes a bounded positional batch while sharing namespace and manifest setup.
///
/// The router's read limiter charges one request before entry. This handler
/// charges `N - 1` additional units, validates every body independently, and
/// counts only shape-valid entries as queries. Namespace and manifest setup are
/// shared; source executions remain sequential and each produces a success or
/// error envelope without aborting later entries.
///
/// # Parameters
///
/// - `state`: Shared server services and configured batch/limit bounds.
/// - `rate_limit_identity`: Client identity inserted by rate-limit middleware.
/// - `ns`: Namespace captured from the route path.
/// - `body`: Raw batch JSON bytes.
///
/// # Returns
///
/// JSON with exactly one [`BatchQueryEntry`] per input request, in the same
/// order. Once the outer batch is accepted, namespace, manifest, validation,
/// and execution failures are represented inside entries.
///
/// # Errors
///
/// Returns a top-level [`ApiError`] only when the outer JSON is malformed, the
/// list is empty, the configured maximum batch size is exceeded, or additional
/// rate-limit capacity is unavailable. Per-entry failures do not change the
/// outer response into an HTTP error.
///
/// # Side Effects
///
/// Consumes read-rate tokens, increments total-query metrics for shape-valid
/// entries, logs each entry failure at warning or error level, and may notify
/// hydration once for the shared live active segment. It reads but does not
/// publish object-store state.
///
/// # Consistency
///
/// If any valid entry requests strong consistency, manifest selection uses the
/// strong path; otherwise it may use the eventual manifest cache. The selected
/// owned manifest is cloned into every valid entry, so all batch entries share
/// one visibility generation. Each entry still applies its own strong/eventual
/// WAL scoring rules. Batch requests do not support `as_of`.
///
/// # Performance
///
/// Validation is linear in entry count and source count. Namespace and manifest
/// setup happen once, but entries execute sequentially and may each perform WAL,
/// index, cache, fusion, enrichment, or rerank work. Manifest clones duplicate
/// its in-memory descriptors; they do not perform another GET.
///
/// # Examples
///
/// For three valid entries—strong ANN, eventual BM25, and invalid-dimension
/// ANN—the handler reads one strongly verified manifest. It returns two normal
/// responses and one 400-class error envelope in their original positions. If
/// namespace lookup fails, all shape-valid entries receive that error while an
/// independently malformed entry retains its more specific validation error.
#[instrument(skip(state, decision, audit, body), fields(namespace = %ns))]
pub async fn batch_query_namespace(
    State(state): State<AppState>,
    Extension(decision): Extension<AllowDecision>,
    audit: Option<Extension<AuditRequest>>,
    Extension(rate_limit_identity): Extension<RateLimitIdentity>,
    Path(ns): Path<String>,
    body: bytes::Bytes,
) -> Result<Json<BatchQueryResponse>, ApiError> {
    let audit = audit.map(|Extension(audit)| audit);
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
        &rate_limit_identity,
        RateLimitClass::Read,
        req.queries.len().saturating_sub(1) as u64,
    )
    .map_err(ApiError::from)?;

    let knobs = state.runtime_query_config.snapshot();
    let redact_timing = decision.mandatory_filter.is_some();
    let validations: Vec<Result<ValidatedQuery, ZeppelinError>> = req
        .queries
        .iter()
        .map(|query| {
            let cursor_fingerprint = cursor_fingerprint_if_requested(&ns, None, query)?;
            validate_query_security_constraints(query, &decision)?;
            let validated =
                validate_query_shape(query, knobs.as_ref(), &state, cursor_fingerprint)?;
            validate_cursor_security_binding(
                query,
                decision.policy_version,
                decision.cursor_binding_key,
            )?;
            validate_cursor_query_binding(query, cursor_fingerprint)?;
            Ok(validated)
        })
        .collect();
    for _ in validations.iter().filter(|validation| validation.is_ok()) {
        crate::metrics::QUERIES_TOTAL
            .with_label_values(&[&ns])
            .inc();
    }

    // When every entry is malformed or carries an unauthenticated cursor,
    // settle the independent validation envelopes before namespace lookup.
    // An all-invalid batch must not turn cheap request bytes into S3 work.
    if validations.iter().all(Result::is_err) {
        let results = validations
            .into_iter()
            .map(|validation| match validation {
                Err(error) => batch_error_entry(&error, Instant::now(), redact_timing),
                Ok(_) => unreachable!("all batch validations were checked as errors"),
            })
            .collect();
        return Ok(finish_batch_response(audit.as_ref(), results));
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
                        Ok(_) => batch_error_entry(&err, start, redact_timing),
                        Err(validation_err) => {
                            batch_error_entry(&validation_err, start, redact_timing)
                        }
                    }
                })
                .collect();
            return Ok(finish_batch_response(audit.as_ref(), results));
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
                                Ok(_) => batch_error_entry(&err, start, redact_timing),
                                Err(validation_err) => {
                                    batch_error_entry(&validation_err, start, redact_timing)
                                }
                            }
                        })
                        .collect();
                    return Ok(finish_batch_response(audit.as_ref(), results));
                }
            }
        }
        false => None,
    };
    if let Some(manifest) = manifest.as_ref() {
        let authoritative_origin = meta.artifact_origin().map_err(ApiError::from)?;
        notify_hydrator(&state, manifest, authoritative_origin.as_ref()).map_err(ApiError::from)?;
    }

    let mut results = Vec::with_capacity(req.queries.len());
    for (idx, query_req) in req.queries.iter().enumerate() {
        let start = Instant::now();
        let entry = match &validations[idx] {
            Ok(validated) => {
                match execute_validated_query(ValidatedQueryExecution {
                    state: &state,
                    ns: &ns,
                    meta: &meta,
                    req: query_req,
                    validated: *validated,
                    knobs: knobs.as_ref(),
                    security: &decision,
                    options: QueryExecutionOptions {
                        manifest: manifest.clone(),
                        notify_hydration: false,
                    },
                })
                .await
                {
                    Ok(response) => batch_success_entry(response, start, redact_timing),
                    Err(err) => batch_error_entry(&err, start, redact_timing),
                }
            }
            Err(err) => batch_error_entry(err, start, redact_timing),
        };
        results.push(entry);
    }

    Ok(finish_batch_response(audit.as_ref(), results))
}

/// Annotates per-entry constraint denials before returning a positional batch.
fn finish_batch_response(
    audit: Option<&AuditRequest>,
    results: Vec<BatchQueryEntry>,
) -> Json<BatchQueryResponse> {
    let denied_entries = results
        .iter()
        .filter(|entry| {
            matches!(
                entry,
                BatchQueryEntry::Error { error, .. }
                    if error.code == "constraint_violation"
            )
        })
        .count();
    if denied_entries > 0 {
        let Some(audit) = audit else {
            panic!("batch constraint denial reached response assembly without audit context");
        };
        audit.mark_batch_constraint_denial(denied_entries, results.len());
    }
    Json(BatchQueryResponse { results })
}

/// Validates request-only constraints and freezes effective query controls.
///
/// This phase performs no namespace lookup or object-store access. It selects
/// defaults from the already-captured [`QueryKnobs`] snapshot, validates source
/// grammar and presentation options, and enforces configured top-k, ID, and
/// nprobe limits.
///
/// # Parameters
///
/// - `req`: Borrowed deserialized request.
/// - `knobs`: One immutable runtime-query snapshot for this request.
/// - `state`: Server state used only for static configured bounds.
///
/// # Returns
///
/// A small [`ValidatedQuery`] containing effective widths, projection, and
/// source path.
///
/// # Errors
///
/// Returns validation or not-implemented errors for conflicting source syntax,
/// invalid widths, malformed cursor/fusion/grouping/facet options, invalid
/// by-ID source IDs, and unsupported projection controls.
///
/// # Examples
///
/// With default `top_k = 10`, an ANN body omitting `nprobe` retains that
/// omission until its manifest segment is known. `nprobe = 0` fails here
/// rather than executing an empty successful search.
fn validate_query_shape(
    req: &QueryRequest,
    knobs: &QueryKnobs,
    state: &AppState,
    cursor_fingerprint: Option<u64>,
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
    let nprobe = source_nprobe.or(req.nprobe);

    Ok(ValidatedQuery {
        top_k,
        candidate_k,
        nprobe,
        include_attributes,
        source,
        cursor_fingerprint,
    })
}

/// Classifies legacy versus algebra source syntax and its nprobe override.
///
/// # Parameters
///
/// - `req`: Request whose source-bearing fields are inspected.
/// - `max_vector_id_length`: Configured length bound for by-ID ANN seeds.
///
/// # Returns
///
/// The validated source path plus a single-source local nprobe when present.
/// Hybrid per-source probe counts are resolved later by source index.
///
/// # Errors
///
/// Rejects missing or multiple legacy sources, mixing legacy fields with
/// algebra, empty algebra sources, invalid source shapes, and invalid
/// multi-source fusion/probe combinations.
///
/// # Examples
///
/// `{ "vector": [...] }` becomes `LegacyVector`; one algebra BM25 source
/// becomes `AlgebraBm25 { index: 0 }`; two sources become `AlgebraHybrid`.
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

/// Checks the mutually exclusive payload choices inside one candidate source.
///
/// # Parameters
///
/// - `source`: Candidate source borrowed from the request.
/// - `max_vector_id_length`: Configured by-ID length limit.
///
/// # Returns
///
/// Unit when BM25 is structurally valid or ANN contains exactly one of inline
/// coordinates and a request-valid stored ID.
///
/// # Errors
///
/// Returns validation when ANN supplies both/neither choice or when its ID is
/// too long or contains forbidden path characters.
///
/// # Examples
///
/// ANN with `id = "seed-1"` succeeds; ANN with both `id` and `vector` fails
/// before any lookup of `seed-1`.
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

/// Validates controls whose meaning depends on having multiple sources.
///
/// # Parameters
///
/// - `req`: Full request containing top-level probe and fusion options.
/// - `sources`: Non-empty source slice known to contain more than one entry.
///
/// # Returns
///
/// A hybrid source descriptor carrying the validated source count. Omitted
/// fusion means default RRF.
///
/// # Errors
///
/// Rejects a top-level nprobe combined with any source-local nprobe, zero RRF
/// offset, a weighted list with the wrong length, and explicit no-fusion.
/// Weight finiteness is checked during fusion so the execution and explain paths
/// share one rule.
///
/// # Examples
///
/// Two sources with two weights succeed. Two sources with one weight or with
/// `{ "type": "none" }` fail.
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

/// Detects algebra-only transforms used without an explicit `sources` array.
///
/// # Parameters
///
/// - `req`: Request to inspect.
///
/// # Returns
///
/// `true` when a candidate width, fusion, rerank, grouping, facets, projection,
/// or cursor field is present. Explain/debug remain valid with legacy syntax and
/// therefore do not trigger this check.
///
/// # Examples
///
/// A legacy vector plus `candidate_k` is classified as an invalid syntax mix;
/// a legacy vector plus `explain = true` is not.
fn retrieval_algebra_without_sources(req: &QueryRequest) -> bool {
    req.candidate_k.is_some()
        || req.fusion.is_some()
        || req.rerank.is_some()
        || req.grouping.is_some()
        || req.facets.is_some()
        || req.projection.is_some()
        || req.cursor.is_some()
}

/// Validates cross-cutting retrieval-algebra presentation options.
///
/// # Parameters
///
/// - `req`: Request whose fusion, grouping, facets, and cursor are checked.
///
/// # Returns
///
/// Unit when option combinations and immediately decodable values are valid.
///
/// # Errors
///
/// Rejects non-none fusion without two sources, zero group capacity, grouping
/// combined with cursoring, empty facet names, and malformed cursor tokens.
/// Cursor/query fingerprint compatibility is checked later after the namespace
/// and complete request are available.
///
/// # Examples
///
/// Grouping by `tenant` with two members per group succeeds. Adding an after
/// cursor fails because there is no unambiguous flat page marker for grouped
/// output.
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

/// Chooses the first-stage candidate frontier.
///
/// # Parameters
///
/// - `req`: Request that may explicitly set `candidate_k`.
/// - `top_k`: Effective final result limit.
///
/// # Returns
///
/// An explicit positive value, or `max(top_k * 4, 100)` using saturating
/// arithmetic. This helper does not force an explicit frontier to exceed
/// `top_k`; downstream behavior follows the caller's requested bound.
///
/// # Errors
///
/// Returns validation only when an explicitly supplied value is zero.
///
/// # Examples
///
/// `top_k = 10` with no candidate width produces 100; explicit `candidate_k =
/// 40` produces 40.
fn validate_candidate_k(req: &QueryRequest, top_k: usize) -> Result<usize, ZeppelinError> {
    if let Some(candidate_k) = req.candidate_k {
        if candidate_k == 0 {
            return Err(ZeppelinError::Validation("candidate_k must be >= 1".into()));
        }
        return Ok(candidate_k);
    }

    Ok(top_k.saturating_mul(4).max(100))
}

/// Applies the configured nprobe bound to every possible ANN setting.
///
/// # Parameters
///
/// - `req`: Request containing top-level and/or source-local probe counts.
/// - `max_nprobe`: Inclusive server maximum.
///
/// # Returns
///
/// Unit when every present count is in `1..=max_nprobe`.
///
/// # Errors
///
/// Propagates the first invalid count found. BM25 sources have no local probe
/// count and require no check.
///
/// # Examples
///
/// A hybrid request with ANN probe counts 4 and 12 passes when the maximum is
/// 16; a source count of zero fails.
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

/// Validates one IVF probe count against nonzero and configured bounds.
///
/// # Parameters
///
/// - `nprobe`: Number of centroid clusters the ANN source would visit.
/// - `max_nprobe`: Inclusive server limit.
///
/// # Returns
///
/// Unit for values from one through the maximum.
///
/// # Errors
///
/// Returns validation for zero or a value above the maximum.
///
/// # Examples
///
/// With maximum 64, `nprobe = 8` succeeds and `nprobe = 65` fails before index
/// work begins.
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

/// Resolves legacy and algebra attribute-projection controls.
///
/// # Parameters
///
/// - `req`: Request containing optional legacy and structured projection fields.
///
/// # Returns
///
/// Whether complete attribute maps should be included, defaulting to `true`.
///
/// # Errors
///
/// Rejects specifying both attribute flags. Any present field-list projection
/// and `include_vectors = true` return explicit not-implemented errors;
/// `include_vectors = false` is accepted.
///
/// # Examples
///
/// `projection.include_attributes = false` allows source execution to avoid
/// returning metadata unless another transform needs it internally. Asking for
/// `fields = ["title"]` fails instead of returning all fields silently.
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

/// Executes one already shape-validated legacy or single-source algebra query.
///
/// Hybrid requests are dispatched to `execute_hybrid_query`. Other paths select
/// one manifest, resolve borrowed/owned source input, validate it against
/// namespace metadata, execute the source, and run response transforms.
///
/// # Parameters
///
/// - `execution`: Original request, validated controls, policy decision, and
///   shared execution dependencies captured as one consistent value.
///
/// # Returns
///
/// A fully transformed [`QueryResponse`] ready for JSON serialization.
///
/// # Errors
///
/// Propagates manifest, by-ID source, metadata validation, source execution,
/// reranking, facets, grouping, cursor, and explain-integrity failures. It does
/// not return a partially transformed response.
///
/// # Consistency
///
/// One owned manifest controls source retrieval, by-ID seed loading, and vector
/// reranking. A preselected historical or batch snapshot is never replaced by
/// a later live manifest.
///
/// # Performance
///
/// Performs one source query plus optional by-ID lookup, candidate enrichment,
/// and reranking. Candidate width increases only when a requested downstream
/// transform needs a wider frontier.
///
/// # Examples
///
/// A single BM25 algebra source retrieves 100 candidates for requested facets,
/// counts facets on that frontier, truncates to `top_k`, and returns the source's
/// scan counters unchanged.
async fn execute_validated_query(
    execution: ValidatedQueryExecution<'_>,
) -> Result<QueryResponse, ZeppelinError> {
    if let ValidatedSource::AlgebraHybrid { source_count } = execution.validated.source {
        return execute_hybrid_query(execution, source_count).await;
    }

    let ValidatedQueryExecution {
        state,
        ns,
        meta,
        req,
        validated,
        knobs,
        security,
        options,
    } = execution;

    let manifest =
        read_manifest_for_execution(state, ns, req.consistency, options, meta.artifact_origin()?)
            .await?;
    let source_ref = resolve_query_source_ref(
        state,
        ns,
        req,
        validated.source,
        manifest.clone(),
        security.mandatory_filter.as_ref(),
        meta.artifact_origin()?,
    )
    .await?;
    validate_query_source_metadata(ns, meta, &source_ref)?;
    let emit_debug = req.debug.unwrap_or(false);
    let first_stage_top_k = first_stage_top_k(req, validated);
    let rerank_limit = rerank_output_k(req, validated, first_stage_top_k);
    let first_stage_include_attributes =
        first_stage_include_attributes(req, validated.include_attributes);
    let nprobe = resolve_source_nprobe(
        &source_ref,
        &manifest,
        validated.nprobe,
        &state.config.indexing,
        knobs.default_nprobe,
    )?;
    let explain_source = explain_source_for_ref(0, &source_ref, nprobe, first_stage_top_k);
    let effective_filter =
        query::compile_effective_filter(security.mandatory_filter.as_ref(), req.filter.as_ref());
    let mut explain = build_explain_accumulator(
        req,
        validated,
        explain_path(validated.source),
        first_stage_top_k,
        vec![explain_source],
        security.mandatory_filter.is_some(),
    );
    let source = execute_query_source_with_manifest(
        state,
        ns,
        meta,
        req,
        effective_filter.as_ref(),
        security.mandatory_filter.as_ref(),
        source_ref,
        first_stage_top_k,
        nprobe,
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
            field_mask: security.field_mask.as_ref(),
            policy_filter_applied: security.mandatory_filter.is_some(),
            policy_version: security.policy_version,
            cursor_binding_key: security.cursor_binding_key,
            cursor_fingerprint: validated.cursor_fingerprint,
            manifest,
        },
        source.response,
        explain,
    )
    .await
}

/// Executes every source in a validated hybrid request and fuses their results.
///
/// Sources run sequentially in request order against clones of one manifest.
/// Stored-vector seed IDs are fetched from that snapshot, excluded from all
/// source responses, and compensated for by requesting extra candidates before
/// truncation. The fused frontier then enters the same rerank/presentation path
/// as a single source.
///
/// # Parameters
///
/// - `execution`: Same execution dependencies and frozen controls as
///   `execute_validated_query`.
/// - `source_count`: Source cardinality captured during shape validation.
///
/// # Returns
///
/// A fused, optionally reranked/grouped/paged [`QueryResponse`]. Scan counters
/// and debug data aggregate work from every source.
///
/// # Errors
///
/// Returns validation if the request no longer matches its validated source
/// count, and propagates snapshot, source, fusion, transform, and explain errors.
/// A failure in any source aborts the entire hybrid response.
///
/// # Consistency
///
/// Every ANN/BM25 source and by-ID seed lookup receives the same manifest
/// snapshot. This avoids fusing candidates from different visibility versions.
///
/// # Performance
///
/// Source costs add because the loop awaits them sequentially. Each source may
/// scan WAL and an active segment; by-ID seeds add lookup work. Fusion is linear
/// in returned candidates plus the final in-memory sort.
///
/// # Examples
///
/// An ANN-by-ID and BM25 request with `candidate_k = 100` fetches the seed,
/// retrieves and trims 100 non-seed candidates from each source, combines them
/// using requested/default fusion, then returns the requested page.
async fn execute_hybrid_query(
    execution: ValidatedQueryExecution<'_>,
    source_count: usize,
) -> Result<QueryResponse, ZeppelinError> {
    let ValidatedQueryExecution {
        state,
        ns,
        meta,
        req,
        validated,
        knobs,
        security,
        options,
    } = execution;
    let sources = req
        .sources
        .as_ref()
        .ok_or_else(|| ZeppelinError::Validation("retrieval algebra sources missing".into()))?;
    if sources.len() != source_count {
        return Err(ZeppelinError::Validation(
            "validated source count does not match request".into(),
        ));
    }

    let manifest =
        read_manifest_for_execution(state, ns, req.consistency, options, meta.artifact_origin()?)
            .await?;
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
    let effective_filter =
        query::compile_effective_filter(security.mandatory_filter.as_ref(), req.filter.as_ref());
    for index in 0..source_count {
        let source_ref = resolve_algebra_source_ref(
            state,
            ns,
            req,
            index,
            manifest.clone(),
            security.mandatory_filter.as_ref(),
            meta.artifact_origin()?,
        )
        .await?;
        validate_query_source_metadata(ns, meta, &source_ref)?;
        let requested_nprobe = nprobe_for_algebra_source(req, index)?;
        let nprobe = resolve_source_nprobe(
            &source_ref,
            &manifest,
            requested_nprobe,
            &state.config.indexing,
            knobs.default_nprobe,
        )?;
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
            effective_filter.as_ref(),
            security.mandatory_filter.as_ref(),
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
        security.mandatory_filter.is_some(),
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
            field_mask: security.field_mask.as_ref(),
            policy_filter_applied: security.mandatory_filter.is_some(),
            policy_version: security.policy_version,
            cursor_binding_key: security.cursor_binding_key,
            cursor_fingerprint: validated.cursor_fingerprint,
            manifest,
        },
        response,
        explain,
    )
    .await
}

/// Chooses how many candidates first-stage retrieval must preserve.
///
/// # Parameters
///
/// - `req`: Request whose transforms may need a wider frontier.
/// - `validated`: Effective final and candidate widths.
///
/// # Returns
///
/// `candidate_k` for explicit rerank, grouping, nonempty facets, or cursoring;
/// otherwise `top_k`. Cursoring also guarantees at least `top_k + 1` so the
/// handler can detect another page, using saturating arithmetic.
///
/// # Examples
///
/// A plain top-ten query retrieves ten. The same request with cursoring and
/// `candidate_k = 10` retrieves eleven to decide whether `next_cursor` exists.
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

/// Chooses how many candidates an explicit reranker may return before paging.
///
/// # Parameters
///
/// - `req`: Request used to detect cursor paging.
/// - `validated`: Effective final width.
/// - `first_stage_top_k`: Candidate width entering the reranker.
///
/// # Returns
///
/// The whole first-stage frontier for cursoring, because paging needs the extra
/// marker candidates; otherwise the final `top_k`.
///
/// # Examples
///
/// Reranking 100 candidates for an unpaged top ten keeps ten. With cursoring it
/// keeps the wider ranked frontier until page slicing.
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

/// Decides whether source execution must materialize attributes internally.
///
/// # Parameters
///
/// - `req`: Request whose transforms may consume metadata.
/// - `include_attributes`: Effective client-facing projection.
///
/// # Returns
///
/// `true` when the response needs attributes or BM25 reranking, grouping, or
/// facets needs them before final stripping.
///
/// # Examples
///
/// A client can request no attributes while grouping by `tenant`; sources still
/// load attributes for grouping and the handler removes them from output later.
fn first_stage_include_attributes(req: &QueryRequest, include_attributes: bool) -> bool {
    include_attributes
        || matches!(req.rerank, Some(RerankSpec::Bm25 { .. }))
        || grouping_requested(req)
        || facet_counts_requested(req)
}

/// Reports whether the request contains a cursor block, including `None`.
///
/// Presence requests cursor-aware frontier sizing and first-page token output.
///
/// # Parameters
///
/// - `req`: Request to inspect.
///
/// # Returns
///
/// `true` for either cursor variant and `false` when the field is absent.
///
/// # Examples
///
/// `cursor = { type: none }` returns `true` because it requests a first page and
/// possible continuation token.
fn cursor_requested(req: &QueryRequest) -> bool {
    req.cursor.is_some()
}

/// Reports whether at least one facet field requires counting.
///
/// An explicitly empty list produces an empty facet object but does not widen
/// first-stage retrieval.
///
/// # Parameters
///
/// - `req`: Request to inspect.
///
/// # Returns
///
/// `true` only for a present, nonempty facet list.
///
/// # Examples
///
/// Facets `[]` return false; facets `["category"]` return true.
fn facet_counts_requested(req: &QueryRequest) -> bool {
    req.facets.as_ref().is_some_and(|facets| !facets.is_empty())
}

/// Reports whether field-based grouping, rather than an explicit no-op, is requested.
///
/// # Parameters
///
/// - `req`: Request to inspect.
///
/// # Returns
///
/// `true` only for [`GroupingSpec::Field`].
///
/// # Examples
///
/// Omitted grouping and `{type: none}` return false.
fn grouping_requested(req: &QueryRequest) -> bool {
    matches!(req.grouping, Some(GroupingSpec::Field { .. }))
}

/// Collects stored-vector seed IDs that must not appear as hybrid neighbors.
///
/// # Parameters
///
/// - `req`: Algebra request whose ANN sources may name stored IDs.
///
/// # Returns
///
/// An owned deduplicated set of seed IDs. Inline vectors and BM25 sources add
/// nothing.
///
/// # Examples
///
/// Two ANN sources using the same `seed-1` produce a one-element exclusion set.
///
/// # Rust Notes for Java/C Engineers
///
/// The iterator borrows the source slice, but `id.clone()` intentionally
/// allocates owned keys because the set outlives each match borrow. Rust makes
/// that ownership transition explicit; a C implementation would need matching
/// allocation/free rules.
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

/// Removes by-ID seeds from one source response and restores its target width.
///
/// # Parameters
///
/// - `response`: Mutable source response whose result vector is filtered in place.
/// - `excluded_seed_ids`: Deduplicated seed IDs from the whole algebra request.
/// - `top_k`: Maximum non-seed candidates to retain.
///
/// # Side Effects
///
/// Mutates only `response.results`; source scan/debug counters remain unchanged.
///
/// # Examples
///
/// If a source returns `[seed, a, b]` for target two, the result becomes
/// `[a, b]`.
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

/// Accumulates observational explain data alongside real query execution.
///
/// This type never chooses candidates or scores. It mirrors the plan and score
/// contributions already used by execution, then retains provenance only for
/// IDs surviving the final response pipeline.
struct ExplainAccumulator {
    /// Requested verbosity; plan mode avoids per-result collection.
    mode: QueryExplainMode,
    /// Effective plan assembled from validated controls.
    plan: QueryExplainPlan,
    /// Full-mode provenance keyed by result ID until final response order is known.
    results: HashMap<String, QueryExplainResult>,
}

impl ExplainAccumulator {
    /// Creates an empty accumulator for one effective plan.
    ///
    /// # Parameters
    ///
    /// - `mode`: Plan-only or full provenance mode.
    /// - `plan`: Owned description of the execution already selected.
    ///
    /// # Returns
    ///
    /// An accumulator with no result provenance recorded yet.
    fn new(mode: QueryExplainMode, plan: QueryExplainPlan) -> Self {
        Self {
            mode,
            plan,
            results: HashMap::new(),
        }
    }

    /// Records raw scores from one unfused candidate source in full mode.
    ///
    /// # Parameters
    ///
    /// - `source_index`: Position used by the request/explain source list.
    /// - `kind`: Native ANN or BM25 score direction.
    /// - `results`: Source-ranked candidates to observe.
    ///
    /// # Side Effects
    ///
    /// In full mode, inserts one source entry per result and treats the raw score
    /// as both pre-fusion score and contribution. Plan mode is a no-op.
    ///
    /// # Examples
    ///
    /// A single ANN hit at distance `0.2` records raw and fused scores of `0.2`.
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

    /// Records hybrid-source contributions using the requested fusion semantics.
    ///
    /// # Parameters
    ///
    /// - `fusion`: Explicit strategy or `None` for default RRF.
    /// - `sources`: Source responses in request order.
    ///
    /// # Returns
    ///
    /// Unit after recording full-mode provenance; plan mode returns immediately.
    ///
    /// # Errors
    ///
    /// Propagates invalid no-fusion, mismatched/non-finite weights, and
    /// non-finite source-score validation from the weighted path.
    ///
    /// # Examples
    ///
    /// A hit present in ANN and BM25 receives two RRF source entries whose
    /// contributions sum to its fused score.
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

    /// Adds reciprocal-rank contributions for every source candidate.
    ///
    /// # Parameters
    ///
    /// - `sources`: Source-ranked lists in request order.
    /// - `k`: Positive smoothing offset previously validated.
    ///
    /// # Side Effects
    ///
    /// Extends per-ID provenance and fused totals. Raw scores remain available,
    /// but source magnitude/direction does not affect contribution.
    ///
    /// # Examples
    ///
    /// At `k = 60`, source ranks one and two contribute `1/61` and `1/62`.
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

    /// Adds direction-adjusted min-max weighted contributions.
    ///
    /// # Parameters
    ///
    /// - `sources`: Source responses whose raw score ranges are normalized independently.
    /// - `weights`: Finite positional weights aligned with `sources`.
    ///
    /// # Returns
    ///
    /// Unit after provenance matches the weighted fusion calculation.
    ///
    /// # Errors
    ///
    /// Rejects count mismatch, non-finite weights, and non-finite source scores.
    /// An empty source contributes nothing.
    ///
    /// # Examples
    ///
    /// The best ANN distance and best BM25 relevance both normalize to `1.0`
    /// before their separate weights are applied.
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

    /// Merges one observed source score into the result-provenance map.
    ///
    /// # Parameters
    ///
    /// - `id`: Stable record identifier.
    /// - `source_index`: Request-order source position.
    /// - `kind`: Native source kind.
    /// - `raw_score`: Unmodified source distance or relevance.
    /// - `normalized_score`: Direction-adjusted weighted-fusion value, if used.
    /// - `contribution`: Amount added to the pre-rerank fused score.
    ///
    /// # Side Effects
    ///
    /// Allocates the ID on first sight, appends one source record, and adds the
    /// contribution to the fused total.
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

    /// Records final explicit-reranker scores for surviving candidate IDs.
    ///
    /// # Parameters
    ///
    /// - `results`: Reranked frontier before grouping/cursor/truncation.
    ///
    /// # Side Effects
    ///
    /// Updates existing full-mode provenance only. Plan mode and an ID missing
    /// from the source map are ignored here; finalization catches missing data
    /// for any ID that actually reaches output.
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

    /// Finalizes explain output in the response's exact result order.
    ///
    /// # Parameters
    ///
    /// - `results`: Final flat response results after grouping/cursor/projection.
    ///
    /// # Returns
    ///
    /// The owned plan and, in full mode, provenance aligned with `results`.
    /// Candidates removed by later transforms are discarded.
    ///
    /// # Errors
    ///
    /// Returns an index-integrity error if a final result has no captured source
    /// provenance. This fails loudly rather than emitting misleading explain data.
    ///
    /// # Examples
    ///
    /// If cursoring returns IDs `[b, c]`, full explain returns only provenance
    /// for `b` and `c` in that order, even when the source frontier also had `a`.
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

/// Converts the wire-level explain choice into an executable verbosity.
///
/// # Parameters
///
/// - `req`: Request containing optional boolean or named mode.
///
/// # Returns
///
/// `None` for absent/false/`none`, plan mode for `true`/`plan`, and full mode
/// for `full`.
///
/// # Examples
///
/// `"explain": true` is shorthand for the plan without per-hit provenance.
fn requested_explain_mode(req: &QueryRequest) -> Option<QueryExplainMode> {
    match req.explain.as_ref()? {
        ExplainSpec::Flag(false) => None,
        ExplainSpec::Flag(true) => Some(QueryExplainMode::Plan),
        ExplainSpec::Mode(ExplainMode::None) => None,
        ExplainSpec::Mode(ExplainMode::Plan) => Some(QueryExplainMode::Plan),
        ExplainSpec::Mode(ExplainMode::Full) => Some(QueryExplainMode::Full),
    }
}

/// Builds the explain plan after effective defaults and source widths are known.
///
/// # Parameters
///
/// - `req`: Original request supplying transform choices.
/// - `validated`: Effective widths and projection.
/// - `path`: Executed legacy/single/hybrid path.
/// - `first_stage_top_k`: Actual source retrieval width.
/// - `sources`: Effective source descriptions in request order.
///
/// # Returns
///
/// `None` when explain is disabled, otherwise an empty accumulator containing
/// the complete effective plan.
///
/// # Examples
///
/// A hybrid request omitting fusion records default RRF with `k = 60`, not an
/// ambiguous absent strategy.
fn build_explain_accumulator(
    req: &QueryRequest,
    validated: ValidatedQuery,
    path: QueryExplainPath,
    first_stage_top_k: usize,
    mut sources: Vec<QueryExplainSource>,
    policy_filter_applied: bool,
) -> Option<ExplainAccumulator> {
    let mode = requested_explain_mode(req)?;
    if policy_filter_applied {
        for source in &mut sources {
            source.nprobe = None;
        }
    }
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
            policy_filter_applied,
        },
    ))
}

/// Describes the effective fusion strategy for explain output.
///
/// # Returns
///
/// The explicit variant, default RRF for multiple sources, or no fusion for a
/// single source. Weight vectors are cloned into owned response metadata.
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

/// Converts an optional rerank request into its explain-only enum.
///
/// # Returns
///
/// `None` when omitted, otherwise the same strategy without copying large
/// reranking vectors or ranking expressions.
fn explain_rerank(rerank: Option<&RerankSpec>) -> Option<QueryExplainRerank> {
    rerank.map(|rerank| match rerank {
        RerankSpec::Default => QueryExplainRerank::Default,
        RerankSpec::None => QueryExplainRerank::None,
        RerankSpec::Vector { .. } => QueryExplainRerank::Vector,
        RerankSpec::Bm25 { .. } => QueryExplainRerank::Bm25,
    })
}

/// Copies grouping controls into explain metadata.
///
/// # Returns
///
/// `None` when grouping was omitted; otherwise an explicit no-op or an owned
/// field name and capacity for field grouping.
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

/// Summarizes whether cursor paging and an after marker were requested.
fn explain_cursor(cursor: Option<&CursorSpec>) -> QueryExplainCursor {
    QueryExplainCursor {
        requested: cursor.is_some(),
        after: matches!(cursor, Some(CursorSpec::After { .. })),
    }
}

/// Copies requested facet names into explain order without deduplicating them.
///
/// Actual counting deduplicates fields; preserving request order here explains
/// exactly what the client sent.
fn explain_facets(facets: Option<&[FacetSpec]>) -> Vec<String> {
    facets
        .unwrap_or_default()
        .iter()
        .map(|facet| facet.field().to_string())
        .collect()
}

/// Describes one already-resolved source for explain output.
///
/// # Parameters
///
/// - `index`: Source position.
/// - `source_ref`: Resolved ANN or BM25 input.
/// - `nprobe`: Effective ANN probe count.
/// - `candidate_k`: Executed source width.
///
/// # Returns
///
/// A source descriptor; BM25 omits nprobe because it does not probe IVF vector
/// centroids.
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

/// Describes one request source without resolving its vector payload again.
///
/// # Errors
///
/// Returns validation if `sources` or the validated index is unexpectedly
/// absent. This protects explain data from drifting away from execution.
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

/// Maps the internal score-direction enum to its serializable explain counterpart.
fn explain_source_kind(kind: QuerySourceKind) -> QueryExplainSourceKind {
    match kind {
        QuerySourceKind::Ann => QueryExplainSourceKind::Ann,
        QuerySourceKind::Bm25 => QueryExplainSourceKind::Bm25,
    }
}

/// Maps validated request syntax to the public explain path classification.
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

/// Bundles the borrowed services and owned snapshot needed by response transforms.
///
/// Grouping these parameters keeps vector reranking and later transforms tied to
/// the same namespace metadata, request, consistency mode, and manifest used by
/// first-stage retrieval.
struct RerankExecutionContext<'a> {
    /// Shared server services.
    state: &'a AppState,
    /// Namespace name.
    ns: &'a str,
    /// Namespace dimensions, metric, and FTS field configuration.
    meta: &'a NamespaceMetadata,
    /// Original request containing transform choices.
    req: &'a QueryRequest,
    /// Final response/group/page width.
    top_k: usize,
    /// Frontier width an explicit reranker may retain.
    rerank_limit: usize,
    /// Whether attributes survive into client output.
    include_attributes: bool,
    /// Server-owned fields removed from every returned attribute map.
    field_mask: Option<&'a FieldMask>,
    /// Whether physical namespace-wide diagnostics must be withheld.
    policy_filter_applied: bool,
    /// Policy generation bound into every emitted or consumed cursor.
    policy_version: PolicyVersion,
    /// Server-only key authenticating cursor version, shape, and marker fields.
    cursor_binding_key: CursorBindingKey,
    /// Caller-visible query identity captured before mandatory-filter injection.
    cursor_fingerprint: Option<u64>,
    /// Owned visibility snapshot reused by vector-value fetches.
    manifest: Manifest,
}

/// Applies facets, optional reranking, grouping/cursoring, projection, and explain.
///
/// Ordering matters: facets observe the filtered first-stage frontier; explicit
/// rerank changes candidate order; grouping or cursor paging shapes output;
/// projection strips internal attributes last; explain is finalized against
/// final result order.
///
/// ```text
/// filtered source/fused frontier
///       |
///       +--> facet counts (snapshot before rerank/page)
///       v
/// explicit vector/BM25 rerank
///       v
/// grouping OR cursor paging
///       v
/// top-k + attribute stripping + explain finalization
/// ```
///
/// # Parameters
///
/// - `ctx`: Borrowed request/services plus the owned manifest snapshot.
/// - `response`: First-stage or fused response to transform.
/// - `explain`: Optional accumulator populated during source execution.
///
/// # Returns
///
/// The completed response with requested enrichments and final result shape.
///
/// # Errors
///
/// Propagates facet conversion, vector fetch/distance, BM25 configuration,
/// grouping conversion, cursor integrity, and explain-provenance errors.
///
/// # Side Effects
///
/// Vector reranking may read candidate values through caches/object storage.
/// Other stages mutate owned in-memory response data only.
///
/// # Consistency
///
/// Vector enrichment uses `ctx.manifest`, preserving first-stage visibility.
/// Facets, grouping, and paging never consult namespace state independently.
///
/// # Examples
///
/// A filtered hybrid request can facet 100 fused candidates, vector-rerank
/// them, return page two of ten, omit attributes, and still expose full score
/// provenance for only the ten final IDs.
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
    let mut response = apply_cursor_if_requested(
        ctx.req,
        response,
        ctx.top_k,
        ctx.policy_version,
        ctx.cursor_binding_key,
        ctx.cursor_fingerprint,
    )?;
    if !grouping_requested(ctx.req) && !cursor_requested(ctx.req) {
        response.results.truncate(ctx.top_k);
    }
    strip_attributes_if_needed(&mut response, ctx.include_attributes);
    if let Some(mask) = ctx.field_mask {
        apply_response_field_mask(&mut response, mask);
    }
    redact_policy_scoped_diagnostics(&mut response, ctx.policy_filter_applied)?;
    response.facets = facets;
    if let Some(explain) = explain {
        response.explain = Some(explain.finish(&response.results)?);
    }
    Ok(response)
}

/// Withholds namespace-wide physical work counters from row-scoped callers.
///
/// WAL-fragment, segment, cache, cluster, and timing diagnostics describe work
/// across the shared namespace rather than only rows admitted by a mandatory
/// filter. Returning them would create a cross-slice activity oracle. Explicit
/// debug requests are rejected before execution; observing a debug block here
/// is therefore an internal enforcement failure rather than something to
/// silently redact.
fn redact_policy_scoped_diagnostics(
    response: &mut QueryResponse,
    policy_filter_applied: bool,
) -> Result<(), ZeppelinError> {
    if !policy_filter_applied {
        return Ok(());
    }
    if response.debug.is_some() {
        return Err(ZeppelinError::Index(
            "policy-scoped query reached response assembly with physical diagnostics".into(),
        ));
    }
    response.scanned_fragments = 0;
    response.scanned_segments = 0;
    Ok(())
}

/// Removes attributes that were loaded only for internal transforms.
///
/// # Parameters
///
/// - `response`: Flat and possibly grouped results to mutate.
/// - `include_attributes`: Effective client projection.
///
/// # Side Effects
///
/// When false, sets attributes to `None` in both the flat result list and every
/// cloned grouped result. Scores, IDs, grouping keys, and facets are preserved.
///
/// # Examples
///
/// Grouping may require `tenant` internally even when the client asks to omit
/// attributes; this helper removes the maps after keys are established.
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

/// Removes policy-denied attributes from flat and grouped response copies.
fn apply_response_field_mask(response: &mut QueryResponse, mask: &FieldMask) {
    fn mask_result(result: &mut SearchResult, mask: &FieldMask) {
        if let Some(attributes) = result.attributes.as_mut() {
            apply_field_mask(mask, attributes);
            if attributes.is_empty() {
                result.attributes = None;
            }
        }
    }

    for result in &mut response.results {
        mask_result(result, mask);
    }
    if let Some(groups) = response.groups.as_mut() {
        for group in groups {
            for result in &mut group.results {
                mask_result(result, mask);
            }
        }
    }
}

/// Counts requested attribute values over the current candidate frontier.
///
/// Fields and value keys use [`BTreeMap`] for deterministic response ordering.
/// Duplicate requested field names are ignored. Missing fields do not create a
/// value bucket; a requested field with no values remains an empty map. List
/// attributes count every element.
///
/// # Parameters
///
/// - `req`: Request containing optional facet field names.
/// - `results`: Filtered source/fused frontier before explicit rerank and paging.
///
/// # Returns
///
/// `None` when facets were omitted, otherwise counts for every distinct
/// requested field, including an empty object for no matches.
///
/// # Errors
///
/// Rejects non-finite float attributes and reports an internal index error if
/// the accumulator invariant is broken. No partial facet object is returned.
///
/// # Performance
///
/// Scans every frontier result and requested field. Scalar conversion allocates
/// display strings; list fields may allocate one string per element.
///
/// # Examples
///
/// Faceting `category` over filtered candidates `[a, a, b]` yields counts
/// `{a: 2, b: 1}` even when the final top-k page contains only one candidate.
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

/// Converts one typed attribute into the string values used as facet buckets.
///
/// # Parameters
///
/// - `value`: Scalar or list attribute borrowed from a candidate.
///
/// # Returns
///
/// One string for scalar values and one per list element, preserving list order
/// and duplicates.
///
/// # Errors
///
/// Returns validation for a non-finite scalar/list float because JSON-visible
/// bucket names must be deterministic finite values.
///
/// # Examples
///
/// `StringList(["red", "fresh"])` contributes to both buckets; integer `7`
/// contributes to bucket `"7"`.
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

/// Reorders the candidate frontier by exact distance to a second query vector.
///
/// The function fetches stored coordinates for all candidate IDs from the same
/// manifest used by first-stage retrieval, replaces each score with distance
/// according to the namespace metric, sorts lower-first with ID tie breaking,
/// and truncates to the rerank frontier.
///
/// # Parameters
///
/// - `ctx`: Namespace metadata, consistency mode, services, and manifest snapshot.
/// - `response`: Candidate response to score in place.
/// - `rerank_vector`: Borrowed second-stage coordinates.
///
/// # Returns
///
/// The response with distance-ordered candidates and unchanged scan counters.
/// Empty candidates return after rerank-vector validation without a fetch.
///
/// # Errors
///
/// Returns dimension/finite-value validation errors, propagates candidate vector
/// fetch failures, and reports an index invariant error if a requested candidate
/// has no vector value in the selected snapshot.
///
/// # Side Effects
///
/// Reads visible WAL/segment vector values and may populate immutable caches.
/// It publishes no state.
///
/// # Consistency
///
/// The supplied manifest and request consistency govern enrichment; a reranker
/// cannot fetch a value from a newer visibility generation.
///
/// # Performance
///
/// Fetch cost scales with candidate IDs and storage layout. Distance scoring is
/// `O(candidates * dimensions)` and sorting is `O(candidates log candidates)`.
///
/// # Examples
///
/// ANN source ordering `[a, b]` can become `[b, a]` when `b` is nearer the
/// rerank vector. Both returned scores are now distances, so smaller is better.
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
        ctx.meta.artifact_origin()?,
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

/// Converts a ranked flat list into first-seen field groups when requested.
///
/// # Parameters
///
/// - `req`: Request containing optional grouping controls.
/// - `response`: Ranked response consumed and rebuilt in place.
/// - `top_k`: Maximum number of groups, not total members.
/// - `include_attributes`: Whether cloned group/flat results retain attributes.
///
/// # Returns
///
/// Without field grouping, the original flat results and `groups = None`. With
/// grouping, at most `top_k` groups in first ranked appearance order; each has
/// at most `max_per_group` results, and `response.results` becomes their
/// flattened member list.
///
/// # Errors
///
/// Propagates non-finite float conversion failures from grouping keys.
///
/// # Performance
///
/// One pass over ranked candidates plus clones when rebuilding the flat list.
/// The hash map provides average constant-time group lookup.
///
/// # Examples
///
/// Ranked categories `[books, games, books]` with two members per group yields
/// groups `books: [first, third]` then `games: [second]`.
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

/// Derives collision-resistant internal and client-visible group keys.
///
/// # Parameters
///
/// - `result`: Candidate whose attributes and ID are inspected.
/// - `field`: Requested grouping attribute.
///
/// # Returns
///
/// Present values return `("field:<display>", "<display>")`. A missing field
/// returns a unique `missing:<id>` internal key and the ID as display key so
/// unrelated missing records remain singleton groups.
///
/// # Errors
///
/// Propagates non-finite float conversion errors.
///
/// # Examples
///
/// A record with `tenant = "acme"` joins the `acme` group. Missing-field record
/// `doc-9` gets its own visible `doc-9` group.
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

/// Converts one typed attribute to the display representation used for grouping.
///
/// # Parameters
///
/// - `value`: Scalar or list attribute.
///
/// # Returns
///
/// Scalars use their ordinary string form; list elements are comma-joined in
/// stored order. The representation is the current API contract and does not
/// escape embedded commas, so distinct list shapes with the same joined text
/// intentionally share a group.
///
/// # Errors
///
/// Rejects non-finite float values.
///
/// # Examples
///
/// Integer list `[1, 2]` becomes `"1,2"`; boolean `true` becomes `"true"`.
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

/// Decoded fields carried by a policy-bound opaque cursor token.
struct DecodedCursor {
    /// Authoritative security policy generation at cursor issuance.
    policy_version: u64,
    /// Non-cryptographic fingerprint of namespace and ranking request.
    fingerprint: u64,
    /// Exact IEEE-754 bits of the page-ending finite score.
    score_bits: u32,
    /// UTF-8 ID used as deterministic tie breaker.
    id: String,
    /// HMAC-SHA256 over every preceding token field.
    authentication_tag: [u8; 32],
}

/// Sorts, filters, slices, and emits a stateless cursor page when requested.
///
/// Paging order follows final score semantics: ascending for single ANN/vector
/// rerank distance and descending for fused/BM25 relevance, with ID ascending as
/// the deterministic tie breaker. An after token must match the request
/// fingerprint and filters out the marker plus everything before it.
///
/// # Parameters
///
/// - `req`: Request defining ranking semantics and optional marker.
/// - `response`: Ranked frontier with at least one extra entry when available.
/// - `top_k`: Page size.
///
/// # Returns
///
/// Without a cursor field, clears `next_cursor` and preserves ordering. With a
/// cursor field, returns at most `top_k` results and a new token only when
/// another candidate remains in the supplied frontier.
///
/// # Errors
///
/// Rejects malformed/mismatched tokens and non-finite scores that cannot form a
/// stable marker. Serialization failures while fingerprinting also propagate.
///
/// # Consistency
///
/// Tokens bind the query shape, consistency mode, policy version, and optional
/// historical selector, but not a manifest generation. Live data may change
/// between pages; clients requiring a frozen view should query an explicit
/// retained `as_of` snapshot.
///
/// # Performance
///
/// Sorts the candidate frontier in memory and, for an after token, scans it once
/// to discard prior markers. It performs no storage I/O.
///
/// # Examples
///
/// For distance-ranked `[a:0.1, b:0.2, c:0.3]` and page size two, the first page
/// returns `a,b` plus a token for `b`; the next page returns `c`. Reusing that
/// token with another namespace or ranking vector fails validation.
fn apply_cursor_if_requested(
    req: &QueryRequest,
    mut response: QueryResponse,
    top_k: usize,
    policy_version: PolicyVersion,
    cursor_binding_key: CursorBindingKey,
    fingerprint: Option<u64>,
) -> Result<QueryResponse, ZeppelinError> {
    let Some(cursor) = req.cursor.as_ref() else {
        response.next_cursor = None;
        return Ok(response);
    };

    let cursor_cmp = cursor_result_cmp(req);
    response.results.sort_by(cursor_cmp);
    let fingerprint = fingerprint.ok_or_else(|| {
        ZeppelinError::Index(
            "cursor request reached response assembly without a caller fingerprint".into(),
        )
    })?;
    if let CursorSpec::After { token } = cursor {
        let decoded = decode_cursor_token(token)?;
        let expected_tag = cursor_authentication_tag(
            cursor_binding_key,
            decoded.policy_version,
            decoded.fingerprint,
            decoded.score_bits,
            decoded.id.as_bytes(),
        );
        if expected_tag.ct_eq(&decoded.authentication_tag).unwrap_u8() != 1 {
            return Err(ZeppelinError::Validation(
                "invalid cursor token authentication".into(),
            ));
        }
        if decoded.policy_version != policy_version.get() {
            return Err(SecurityError::CursorPolicyStale.into());
        }
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
            .map(|result| {
                encode_cursor_token(cursor_binding_key, policy_version, fingerprint, result)
            })
            .transpose()?
    } else {
        None
    };
    Ok(response)
}

/// Selects the comparator matching the final score space.
///
/// # Returns
///
/// A plain function pointer that sorts ascending distance or descending
/// fused/relevance score, always with ascending ID ties. A function pointer has
/// no captured state and can be reused by sorting and marker filtering.
fn cursor_result_cmp(req: &QueryRequest) -> fn(&SearchResult, &SearchResult) -> Ordering {
    if cursor_lower_score_is_better(req) {
        distance_result_cmp
    } else {
        fused_result_cmp
    }
}

/// Determines whether the final cursor score treats smaller values as better.
///
/// Explicit vector rerank and a single algebra ANN source use distance order.
/// BM25 rerank, BM25 sources, and hybrid fused scores use descending order.
///
/// # Examples
///
/// A hybrid ANN+BM25 query followed by vector rerank returns `true` because the
/// reranker replaces fused scores with distances.
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

/// Orders distance results from nearest to farthest with stable ID ties.
fn distance_result_cmp(a: &SearchResult, b: &SearchResult) -> Ordering {
    a.score.total_cmp(&b.score).then_with(|| a.id.cmp(&b.id))
}

/// Hashes namespace and ranking-affecting request fields into a cursor identity.
///
/// # Parameters
///
/// - `ns`: Namespace preventing cross-namespace token reuse.
/// - `as_of`: Caller-visible historical selector paired with the exact
///   immutable generation it resolved to, when present.
/// - `req`: Serializable request copied into a canonical struct-shaped JSON value.
///
/// # Returns
///
/// An XXH3 64-bit non-cryptographic fingerprint after removing cursor, debug,
/// explain, facets, and projection/attribute output controls. Those fields do
/// not define the score marker identity in the current contract.
///
/// # Errors
///
/// Propagates serialization failure or an impossible non-object request value.
///
/// # Consistency
///
/// XXH3 supplies the compact query identity. The complete token authenticates
/// that identity together with the policy version and page marker using
/// HMAC-SHA256, so clients cannot replace or splice any individual field.
///
/// # Examples
///
/// Changing the raw `as_of` selector, its resolved generation, consistency,
/// `top_k`, source vector, filter, fusion, or reranker changes the fingerprint.
/// Toggling debug or attribute projection does not.
fn cursor_fingerprint(
    ns: &str,
    as_of: Option<(&str, u64)>,
    req: &QueryRequest,
) -> Result<u64, ZeppelinError> {
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
    let payload = serde_json::to_vec(&(ns, as_of, value))?;
    Ok(xxh3_64(&payload))
}

/// Captures a cursor identity from the caller-visible request before policy
/// constraints mutate the execution filter. The policy version carried beside
/// the fingerprint binds authorization changes without exposing a digest of a
/// server-only predicate in the opaque token; the final HMAC authenticates both
/// values and the page marker as one unit.
fn cursor_fingerprint_if_requested(
    ns: &str,
    as_of: Option<(&str, u64)>,
    req: &QueryRequest,
) -> Result<Option<u64>, ZeppelinError> {
    req.cursor
        .as_ref()
        .map(|_| cursor_fingerprint(ns, as_of, req))
        .transpose()
}

/// Authenticates an after-cursor before namespace lookup or retrieval work.
///
/// The server-only HMAC is checked before either the policy version or request
/// fingerprint is trusted. A genuine token from an older policy receives the
/// typed stale error; a caller-spliced token remains a generic validation
/// failure and cannot trigger ANN, BM25, or object-store work.
fn validate_cursor_security_binding(
    req: &QueryRequest,
    policy_version: PolicyVersion,
    cursor_binding_key: CursorBindingKey,
) -> Result<(), ZeppelinError> {
    let Some(CursorSpec::After { token }) = req.cursor.as_ref() else {
        return Ok(());
    };
    let decoded = decode_cursor_token(token)?;
    let expected_tag = cursor_authentication_tag(
        cursor_binding_key,
        decoded.policy_version,
        decoded.fingerprint,
        decoded.score_bits,
        decoded.id.as_bytes(),
    );
    if expected_tag.ct_eq(&decoded.authentication_tag).unwrap_u8() != 1 {
        return Err(ZeppelinError::Validation(
            "invalid cursor token authentication".into(),
        ));
    }
    if decoded.policy_version != policy_version.get() {
        return Err(SecurityError::CursorPolicyStale.into());
    }
    Ok(())
}

/// Compares an authenticated cursor with the request's complete query identity.
///
/// Live and batch requests perform this check before namespace I/O. Historical
/// requests perform it immediately after resolving `as_of`, because the
/// fingerprint binds both the caller's selector and the immutable manifest
/// generation it selected. HMAC and policy validation always happen first, so
/// forged cursors cannot force snapshot or manifest reads.
fn validate_cursor_query_binding(
    req: &QueryRequest,
    fingerprint: Option<u64>,
) -> Result<(), ZeppelinError> {
    let Some(CursorSpec::After { token }) = req.cursor.as_ref() else {
        return Ok(());
    };
    let decoded = decode_cursor_token(token)?;
    if Some(decoded.fingerprint) != fingerprint {
        return Err(ZeppelinError::Validation(
            "cursor token does not match query".into(),
        ));
    }
    Ok(())
}

/// Encodes one finite page-ending result as an authenticated cursor token.
///
/// # Parameters
///
/// - `fingerprint`: Query identity produced by `cursor_fingerprint`.
/// - `result`: Final result whose score and ID form the exclusive marker.
///
/// # Returns
///
/// `zp3:<policy-version-hex>:<fingerprint-hex>:<score-bits-hex>:<id-utf8-hex>:<hmac-hex>`.
/// Encoding exact score bits avoids decimal round-trip changes at page boundaries;
/// the HMAC prevents a caller from splicing a current policy version onto an old
/// page marker.
///
/// # Errors
///
/// Rejects NaN or infinite scores.
///
/// # Examples
///
/// A result ID containing punctuation remains safe because its UTF-8 bytes are
/// hex encoded rather than placed raw between separators.
fn encode_cursor_token(
    cursor_binding_key: CursorBindingKey,
    policy_version: PolicyVersion,
    fingerprint: u64,
    result: &SearchResult,
) -> Result<String, ZeppelinError> {
    if !result.score.is_finite() {
        return Err(ZeppelinError::Validation(
            "cursor cannot encode non-finite score".into(),
        ));
    }
    let score_bits = result.score.to_bits();
    let id_bytes = result.id.as_bytes();
    let authentication_tag = cursor_authentication_tag(
        cursor_binding_key,
        policy_version.get(),
        fingerprint,
        score_bits,
        id_bytes,
    );
    Ok(format!(
        "zp3:{:016x}:{fingerprint:016x}:{score_bits:08x}:{}:{}",
        policy_version.get(),
        hex_encode(id_bytes),
        hex_encode(&authentication_tag),
    ))
}

/// Computes HMAC-SHA256 over the complete policy-bound cursor marker.
fn cursor_authentication_tag(
    cursor_binding_key: CursorBindingKey,
    policy_version: u64,
    fingerprint: u64,
    score_bits: u32,
    id: &[u8],
) -> [u8; 32] {
    const BLOCK_BYTES: usize = 64;
    let mut inner_pad = [0x36_u8; BLOCK_BYTES];
    let mut outer_pad = [0x5c_u8; BLOCK_BYTES];
    for (index, key_byte) in cursor_binding_key.as_bytes().iter().enumerate() {
        inner_pad[index] ^= key_byte;
        outer_pad[index] ^= key_byte;
    }
    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(b"zeppelin-cursor-v3\0");
    inner.update(policy_version.to_be_bytes());
    inner.update(fingerprint.to_be_bytes());
    inner.update(score_bits.to_be_bytes());
    inner.update((id.len() as u64).to_be_bytes());
    inner.update(id);
    let inner_digest = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_digest);
    outer.finalize().into()
}

/// Parses and validates the structural contents of an authenticated cursor token.
///
/// # Parameters
///
/// - `token`: Opaque token supplied by a client.
///
/// # Returns
///
/// Decoded fingerprint, exact score bits, and UTF-8 ID. Query compatibility is
/// checked separately by `apply_cursor_if_requested`.
///
/// # Errors
///
/// Returns validation for wrong version/field count, invalid hex widths or
/// digits, non-finite score bits, and non-UTF-8 IDs.
///
/// # Examples
///
/// Any truncated token or token beginning with another version prefix fails
/// before it can affect result filtering.
fn decode_cursor_token(token: &str) -> Result<DecodedCursor, ZeppelinError> {
    let parts: Vec<&str> = token.split(':').collect();
    if parts.len() != 6 || parts[0] != "zp3" {
        return Err(ZeppelinError::Validation("invalid cursor token".into()));
    }
    let policy_version = u64::from_str_radix(parts[1], 16)
        .map_err(|_| ZeppelinError::Validation("invalid cursor token".into()))?;
    let fingerprint = u64::from_str_radix(parts[2], 16)
        .map_err(|_| ZeppelinError::Validation("invalid cursor token".into()))?;
    let score_bits = u32::from_str_radix(parts[3], 16)
        .map_err(|_| ZeppelinError::Validation("invalid cursor token".into()))?;
    let score = f32::from_bits(score_bits);
    if !score.is_finite() {
        return Err(ZeppelinError::Validation(
            "invalid cursor token score".into(),
        ));
    }
    let id_bytes = hex_decode(parts[4])?;
    let id = String::from_utf8(id_bytes)
        .map_err(|_| ZeppelinError::Validation("invalid cursor token id".into()))?;
    let authentication_tag = hex_decode(parts[5])?
        .try_into()
        .map_err(|_| ZeppelinError::Validation("invalid cursor token authentication".into()))?;
    Ok(DecodedCursor {
        policy_version,
        fingerprint,
        score_bits,
        id,
        authentication_tag,
    })
}

/// Converts bytes to lowercase hexadecimal without an external allocation helper.
///
/// # Parameters
///
/// - `bytes`: Borrowed byte slice.
///
/// # Returns
///
/// An owned string with exactly two ASCII characters per byte.
///
/// # Examples
///
/// Bytes `[0x0a, 0xff]` become `"0aff"`.
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Decodes an even-length hexadecimal string into owned bytes.
///
/// # Errors
///
/// Rejects odd length or any non-hexadecimal digit through `hex_value`.
///
/// # Examples
///
/// `"646f63"` becomes the bytes for `doc`.
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

/// Maps one ASCII hexadecimal digit to its four-bit numeric value.
///
/// # Returns
///
/// Values zero through fifteen for decimal, lowercase, or uppercase hex.
///
/// # Errors
///
/// Returns validation for every other byte.
fn hex_value(byte: u8) -> Result<u8, ZeppelinError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(ZeppelinError::Validation("invalid cursor token hex".into())),
    }
}

/// Prepared lexical query for one field used by candidate-local BM25 reranking.
struct RerankFieldQuery {
    /// Namespace attribute/FTS field name.
    field: String,
    /// Query tokens produced with this field's tokenizer configuration.
    query_tokens: Vec<String>,
    /// Field-specific BM25 saturation and length-normalization parameters.
    params: Bm25Params,
    /// Owned tokenizer settings reused for candidate attribute text.
    config: FtsFieldConfig,
}

/// Candidate-frontier corpus statistics for one BM25 rerank field.
struct RerankCorpusStats {
    /// Candidates with a nonempty string value for this field.
    doc_count: u32,
    /// Mean token count across those candidate documents.
    avg_doc_length: f32,
    /// Number of candidate documents containing each distinct token.
    term_doc_freqs: HashMap<String, u32>,
}

/// Candidate ID -> field -> `(document length, token frequencies)`.
///
/// This is intentionally scoped to first-stage candidates, not persisted global
/// FTS statistics. It supports an HTTP-layer second-stage scorer that cannot
/// retrieve records outside its input frontier.
type RerankFieldData = HashMap<String, HashMap<String, (u32, HashMap<String, u32>)>>;

/// Reranks existing candidates using BM25 over their string attributes.
///
/// The scorer prepares configured field queries, tokenizes candidate text,
/// computes document frequency and average length over the candidate frontier,
/// evaluates the [`RankBy`] expression, and sorts higher scores first. This is
/// candidate-local reranking, not the persisted/global BM25 source search.
///
/// # Parameters
///
/// - `meta`: Namespace FTS configuration and name for errors.
/// - `response`: Candidate response whose scores/order are replaced.
/// - `top_k`: Maximum reranked candidates to retain.
/// - `include_attributes`: Whether attributes remain after scoring.
/// - `rank_by`: BM25/arithmetic ranking expression to evaluate.
///
/// # Returns
///
/// The response ordered by descending rerank relevance, with ID tie breaking.
/// Scan counters continue to describe first-stage retrieval.
///
/// # Errors
///
/// Returns `FtsFieldNotConfigured` if any expression field lacks namespace
/// configuration and propagates ranking-expression errors. Candidate attributes
/// that are absent, non-string, or tokenize empty simply contribute no field
/// score.
///
/// # Performance
///
/// Tokenizes every candidate string for every referenced field, builds in-memory
/// frequency maps, scores, and sorts. It performs no object-store reads because
/// source execution materializes the attributes first.
///
/// # Examples
///
/// One hundred ANN candidates can be reranked by occurrences of `"database"`
/// in configured `title` and `body` fields, then truncated to ten. A namespace
/// without configured `body` fails instead of treating it as zero relevance.
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

/// Prepares all field-level token queries referenced by a rerank expression.
///
/// # Parameters
///
/// - `meta`: Namespace whose FTS configuration supplies tokenization and BM25 parameters.
/// - `rank_by`: Expression from which `(field, query text)` pairs are extracted.
///
/// # Returns
///
/// Owned prepared queries in expression extraction order. Repeated fields stay
/// repeated so expression semantics are preserved.
///
/// # Errors
///
/// Returns `FtsFieldNotConfigured` for the first referenced field absent from
/// namespace metadata.
///
/// # Examples
///
/// A clause `title BM25 "rust storage"` becomes tokens using `title`'s
/// lowercase/stemming configuration and copies that field's `k1`/`b` values.
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

/// Tokenizes candidate attributes into per-document rerank data.
///
/// # Parameters
///
/// - `results`: First-stage candidates with attributes materialized.
/// - `field_queries`: Prepared fields and tokenization settings.
///
/// # Returns
///
/// Nested owned maps only for candidates/fields with a nonempty string value.
/// Non-string values and missing attributes are omitted.
///
/// # Performance
///
/// Allocates token/frequency maps proportional to candidate text and referenced
/// fields. Repeated prepared queries for one field may re-tokenize and replace
/// the same stored field data.
///
/// # Examples
///
/// Candidate `a` with `title = "rust rust storage"` records length three and
/// frequencies `{rust: 2, storage: 1}`.
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

/// Computes candidate-local document statistics for every prepared field.
///
/// # Parameters
///
/// - `field_data`: Token lengths and term frequencies keyed by candidate/field.
///
/// # Returns
///
/// Per-field document count, average document length, and document frequencies.
/// Each token counts at most once per document because the helper iterates term
/// frequency keys.
///
/// # Examples
///
/// Field documents of lengths two and four yield `doc_count = 2` and average
/// length three; a token present in both has document frequency two.
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

/// Validates resolved source inputs against namespace metadata.
///
/// # Parameters
///
/// - `ns`: Namespace included in FTS configuration errors.
/// - `meta`: Authoritative namespace dimensions and configured FTS fields.
/// - `source_ref`: Resolved ANN coordinates or borrowed BM25 expression.
///
/// # Returns
///
/// Unit when every BM25 field is configured or ANN coordinates have matching
/// dimensions and finite values.
///
/// # Errors
///
/// Returns field-configuration, dimension-mismatch, or non-finite validation
/// errors. No source I/O has begun.
///
/// # Examples
///
/// A 384-dimensional namespace rejects a 768-dimensional embedding; a BM25
/// clause over unconfigured `notes` returns a field-specific error.
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

/// Obtains the one manifest snapshot used by an execution and optionally records heat.
///
/// # Parameters
///
/// - `state`: Store, manifest cache, and optional hydrator.
/// - `ns`: Namespace whose live manifest may be read.
/// - `consistency`: Strong/eventual live-manifest policy when no snapshot is supplied.
/// - `options`: Preselected snapshot and hydration-notification choice.
///
/// # Returns
///
/// The caller-supplied manifest or a consistency-aware current manifest.
///
/// # Errors
///
/// Propagates live manifest cache/store/decode failures. Hydration notification
/// is best effort and does not turn queue pressure into a query failure.
///
/// # Side Effects
///
/// May read/populate the manifest cache and notify active-segment heat.
///
/// # Consistency
///
/// A supplied snapshot always wins and is returned unchanged. Cache state can
/// optimize live lookup but cannot add artifacts absent from the manifest.
///
/// # Examples
///
/// A historical query passes generation 12 and disables hydration. A live strong
/// query passes no manifest, verifies current state, and observes its active
/// segment for possible background warming.
async fn read_manifest_for_execution(
    state: &AppState,
    ns: &str,
    consistency: ConsistencyLevel,
    options: QueryExecutionOptions,
    authoritative_origin: Option<crate::namespace::branching::ArtifactOrigin>,
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
        notify_hydrator(state, &manifest, authoritative_origin.as_ref())?;
    }
    Ok(manifest)
}

#[allow(clippy::too_many_arguments)]
/// Runs one resolved ANN or BM25 source against a supplied manifest snapshot.
///
/// BM25 delegates to the lexical domain path with configured field settings,
/// filter, cache, consistency, and full-scan budgets. ANN constructs
/// [`crate::query::QueryParams`], delegates index/WAL work, and removes a by-ID
/// seed after requesting one compensating candidate.
///
/// # Parameters
///
/// - `state`: Domain query dependencies and configured ANN oversampling.
/// - `ns`, `meta`: Namespace identity and query metadata.
/// - `req`: Filter and consistency controls.
/// - `source_ref`: Resolved source payload.
/// - `top_k`: Candidate width requested from this source.
/// - `nprobe`: ANN cluster budget; ignored by BM25 execution.
/// - `include_attributes`: Whether source hits should materialize metadata.
/// - `knobs`: Runtime query limits and rerank coalescing controls.
/// - `manifest`: Owned authoritative visibility snapshot.
/// - `emit_debug`: Whether to collect detailed source diagnostics.
///
/// # Returns
///
/// The domain response paired with native score direction. ANN is lower-first;
/// BM25 is higher-first.
///
/// # Errors
///
/// Rejects invalid dimensions/non-finite ANN values or unconfigured FTS fields,
/// and propagates WAL, segment, cache, filter, index, and decoding failures.
///
/// # Side Effects
///
/// Increments the FTS query counter for BM25 and may populate immutable caches.
/// It performs read-only storage work and publishes no manifest/artifact.
///
/// # Consistency
///
/// Both paths consume exactly `manifest`; no source reselects a current
/// generation. Strong/eventual affects WAL participation inside the domain
/// query functions.
///
/// # Performance
///
/// Source work may read visible WAL and active-segment ranges. ANN cost scales
/// with `nprobe`, filters, quantization, oversampling, and rerank coalescing;
/// BM25 cost depends on global/per-cluster index availability and configured
/// full-scan budgets. Debug mode adds diagnostics collection.
///
/// # Examples
///
/// An ANN-by-ID source asking for ten requests eleven, removes its seed if
/// present, then truncates back to ten. A BM25 source over an unconfigured field
/// fails before scanning lexical artifacts.
async fn execute_query_source_with_manifest(
    state: &AppState,
    ns: &str,
    meta: &NamespaceMetadata,
    req: &QueryRequest,
    effective_filter: Option<&Filter>,
    mandatory_filter: Option<&Filter>,
    source_ref: QuerySourceRef<'_>,
    top_k: usize,
    nprobe: usize,
    include_attributes: bool,
    knobs: &QueryKnobs,
    manifest: Manifest,
    emit_debug: bool,
) -> Result<SourceQueryResponse, ZeppelinError> {
    let authoritative_origin = meta.artifact_origin()?;
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
                    effective_filter,
                    mandatory_filter,
                    req.consistency,
                    last_as_prefix,
                    Some(&state.fts_cache),
                    Some(&state.fragment_cache),
                    Some(&state.decoded_artifact_cache),
                    Some(&state.cache),
                    knobs.bm25_max_full_scan_clusters,
                    knobs.bm25_max_full_scan_vectors,
                    include_attributes,
                    manifest,
                    authoritative_origin.clone(),
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
                    effective_filter,
                    mandatory_filter,
                    req.consistency,
                    last_as_prefix,
                    Some(&state.fts_cache),
                    Some(&state.fragment_cache),
                    Some(&state.decoded_artifact_cache),
                    Some(&state.cache),
                    knobs.bm25_max_full_scan_clusters,
                    knobs.bm25_max_full_scan_vectors,
                    include_attributes,
                    manifest,
                    authoritative_origin.clone(),
                )
                .await
            };
            response.map(|response| SourceQueryResponse {
                kind: QuerySourceKind::Bm25,
                response,
            })
        }
        QuerySourceRef::Ann {
            vector, exclude_id, ..
        } => {
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
                filter: effective_filter,
                consistency: req.consistency,
                distance_metric: meta.distance_metric,
                oversample_factor: state.config.indexing.oversample_factor,
                rerank_coalesce_gap_bytes: knobs.rerank_coalesce_gap_bytes,
                resident_row_bypass: knobs.resident_row_bypass,
                cache: Some(&state.cache),
                manifest_cache: Some(&state.manifest_cache),
                include_attributes,
            };
            let scoped_indexing_config = meta.index_config.as_ref().map_or_else(
                || state.config.indexing.clone(),
                |config| config.apply_to_indexing_config(&state.config.indexing),
            );
            let scoped_ann = mandatory_filter.map(|mandatory_filter| query::ScopedAnnQuery {
                mandatory_filter,
                indexing_config: &scoped_indexing_config,
                decoded_artifact_cache: &state.decoded_artifact_cache,
            });

            let response = if emit_debug {
                query::execute_query_with_manifest_debug(
                    params,
                    manifest,
                    Some(&state.fragment_cache),
                    scoped_ann,
                    authoritative_origin,
                )
                .await
            } else {
                query::execute_query_with_manifest(
                    params,
                    manifest,
                    Some(&state.fragment_cache),
                    scoped_ann,
                    authoritative_origin,
                )
                .await
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

/// Combines multiple source responses and aggregates their observable work.
///
/// # Parameters
///
/// - `fusion`: Explicit strategy or `None` for default RRF.
/// - `sources`: Owned source responses in request order.
/// - `top_k`: Maximum fused candidates to retain.
/// - `consistency`: Effective request consistency recorded in aggregate debug data.
/// - `emit_debug`: Whether to synthesize a hybrid diagnostics block.
///
/// # Returns
///
/// A response with fused higher-is-better scores, summed scan counters, optional
/// aggregate diagnostics, and response-transform fields initially empty.
///
/// # Errors
///
/// Rejects explicit no-fusion and propagates weighted count/finite validation.
/// No partial fused response is returned.
///
/// # Performance
///
/// Consumes all source candidates into an ID map, then sorts unique IDs. RRF is
/// linear before sort; weighted fusion also scans each source for min/max.
///
/// # Examples
///
/// If an ID appears in ANN and BM25 lists, its two contributions add into one
/// hit while scan counters include work from both sources.
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

/// Fuse one ANN and one BM25 response through the production hybrid reducer.
///
/// This feature-only seam accepts responses that were already executed against
/// the same supplied manifest. It exists so external artifact-origin tests can
/// exercise the real score-direction, scan-counter, and debug
/// aggregation without opening foreign-origin HTTP admission.
#[cfg(feature = "branching-test-support")]
// Dependency wiring: every argument is a distinct collaborator passed
// once. A params struct would rename the same fields, not reduce them.
#[allow(clippy::too_many_arguments)]
pub(crate) fn fuse_ann_bm25_for_test_support(
    ann: QueryResponse,
    bm25: QueryResponse,
    top_k: usize,
    nprobe: usize,
    distance_metric: crate::types::DistanceMetric,
    consistency: ConsistencyLevel,
    attributes_loaded: bool,
    emit_debug: bool,
) -> Result<QueryResponse, ZeppelinError> {
    let _ = (nprobe, distance_metric, attributes_loaded);
    fuse_source_responses(
        None,
        vec![
            SourceQueryResponse {
                kind: QuerySourceKind::Ann,
                response: ann,
            },
            SourceQueryResponse {
                kind: QuerySourceKind::Bm25,
                response: bm25,
            },
        ],
        top_k,
        consistency,
        emit_debug,
    )
}

/// Sums per-source diagnostics into one hybrid query diagnostics block.
///
/// # Parameters
///
/// - `source_debugs`: Diagnostics actually emitted by sources.
/// - `scanned_fragments`, `scanned_segments`: Already aggregated work counters.
/// - `results_len`, `top_k`: Used to classify underfill.
/// - `consistency`: Effective consistency reported to the client.
///
/// # Returns
///
/// Summed phase times, cache counts, and cluster probes. An underfilled response
/// prefers `eventual_skipped_wal` when any source reports it; otherwise it uses
/// `not_enough_matches`.
///
/// # Examples
///
/// ANN and BM25 cache hits 3 and 2 aggregate to 5. Their phase durations are
/// summed as measured work, not claimed as end-to-end wall time.
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

/// Fuses source positions with reciprocal-rank contributions.
///
/// # Parameters
///
/// - `sources`: Owned ranked source responses.
/// - `k`: Rank smoothing offset.
/// - `top_k`: Maximum unique fused IDs.
///
/// # Returns
///
/// Higher-first fused results. A candidate at zero-based index `rank` contributes
/// `1 / (k + rank + 1)` from that source.
///
/// # Examples
///
/// With `k = 60`, an ID ranked first in two sources receives `2 / 61`.
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

/// Fuses direction-adjusted normalized source scores with positional weights.
///
/// # Parameters
///
/// - `sources`: Owned ANN/BM25 responses.
/// - `weights`: Finite weights aligned one-for-one with sources. Negative finite
///   weights are accepted and subtract contribution.
/// - `top_k`: Maximum unique fused IDs.
///
/// # Returns
///
/// Higher-first weighted results after independently normalizing every nonempty
/// source to best=`1.0`, worst=`0.0`. A constant-score source assigns `1.0` to
/// all its results.
///
/// # Errors
///
/// Rejects length mismatch, non-finite weights, and non-finite source scores.
///
/// # Examples
///
/// Weights `[1.0, 0.0]` preserve the first source's normalized evidence while
/// still allowing IDs unique to the zero-weight source into tie-broken output.
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

/// Converts one source-native score to a higher-is-better unit interval.
///
/// # Parameters
///
/// - `kind`: ANN distance or BM25 relevance direction.
/// - `score`: Finite source score.
/// - `min_score`, `max_score`: Finite range over that source's candidates.
///
/// # Returns
///
/// `1.0` for all values when the range is effectively constant. Otherwise ANN
/// inverts the range while BM25 preserves it.
///
/// # Examples
///
/// ANN distances from `0.1` to `0.5` map `0.1` to one and `0.5` to zero; BM25
/// relevance does the opposite mapping for the same numeric endpoints.
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

/// Adds one candidate contribution to the fused ID map.
///
/// # Parameters
///
/// - `fused`: Unique candidates accumulated so far.
/// - `result`: Owned source result whose score will be replaced/added.
/// - `contribution`: RRF or weighted score contribution.
///
/// # Side Effects
///
/// Inserts a new candidate or adds to an existing score. The first available
/// attribute map is retained; later attributes fill only an absent map.
///
/// # Examples
///
/// Contributions `0.02` and `0.01` for the same ID produce fused score `0.03`.
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

/// Sorts unique fused candidates by descending score and stable ID tie break.
///
/// # Returns
///
/// At most `top_k` owned results.
fn sorted_fused_results(fused: HashMap<String, SearchResult>, top_k: usize) -> Vec<SearchResult> {
    let mut results: Vec<SearchResult> = fused.into_values().collect();
    results.sort_by(fused_result_cmp);
    results.truncate(top_k);
    results
}

/// Orders higher fused/BM25 scores first with ascending ID ties.
fn fused_result_cmp(a: &SearchResult, b: &SearchResult) -> Ordering {
    b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id))
}

/// Resolves a validated legacy or single algebra source into executable inputs.
///
/// # Parameters
///
/// - `state`, `ns`: Services and namespace used if an algebra ANN source loads by ID.
/// - `req`: Request that owns borrowed inline vector/rank expressions.
/// - `source`: Validated source path.
/// - `manifest`: Snapshot used by any by-ID lookup.
///
/// # Returns
///
/// A [`QuerySourceRef`] borrowing request data or owning a fetched seed vector.
///
/// # Errors
///
/// Reports impossible missing validated fields, delegates algebra resolution
/// errors, and rejects a hybrid descriptor because all hybrid sources must be
/// handled together.
///
/// # Examples
///
/// A legacy vector returns a borrowed slice with no excluded ID; a single
/// algebra by-ID source may return owned fetched coordinates and its seed ID.
///
/// # Rust Notes for Java/C Engineers
///
/// The returned lifetime is tied to `req` only when data is borrowed. `Cow`
/// erases the branch difference for callers without erasing ownership safety.
async fn resolve_query_source_ref<'a>(
    state: &AppState,
    ns: &str,
    req: &'a QueryRequest,
    source: ValidatedSource,
    manifest: Manifest,
    mandatory_filter: Option<&Filter>,
    authoritative_origin: Option<crate::namespace::branching::ArtifactOrigin>,
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
            resolve_algebra_source_ref(
                state,
                ns,
                req,
                index,
                manifest,
                mandatory_filter,
                authoritative_origin,
            )
            .await
        }
        ValidatedSource::AlgebraHybrid { .. } => Err(ZeppelinError::Validation(
            "hybrid query must execute through all algebra sources".into(),
        )),
    }
}

/// Resolves one indexed algebra source, fetching stored ANN coordinates when needed.
///
/// # Parameters
///
/// - `state`, `ns`: Vector fetch services and namespace.
/// - `req`: Request owning the source list and consistency choice.
/// - `index`: Validated source position.
/// - `manifest`: Visibility snapshot for a by-ID fetch.
///
/// # Returns
///
/// Borrowed inline ANN/BM25 input or an owned by-ID ANN vector. BM25 prefix mode
/// prefers the source-local setting, then top-level setting, then `false`.
///
/// # Errors
///
/// Returns validation for missing/malformed source state, `VectorNotFound` for a
/// seed absent from the snapshot, and propagates vector fetch errors.
///
/// # Side Effects
///
/// A by-ID source may read WAL/segment data and populate caches.
///
/// # Consistency
///
/// The seed is loaded with the request consistency mode from exactly the
/// supplied manifest; no newer coordinates can leak into the query.
///
/// # Examples
///
/// Source `{id: "doc-7"}` loads `doc-7`'s coordinates and marks `doc-7` for
/// exclusion. An absent ID fails rather than substituting an empty vector.
async fn resolve_algebra_source_ref<'a>(
    state: &AppState,
    ns: &str,
    req: &'a QueryRequest,
    index: usize,
    manifest: Manifest,
    mandatory_filter: Option<&Filter>,
    authoritative_origin: Option<crate::namespace::branching::ArtifactOrigin>,
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
                let vector = super::vectors::fetch_vector_values_by_id_scoped(
                    state,
                    ns,
                    id,
                    req.consistency,
                    manifest,
                    mandatory_filter,
                    authoritative_origin,
                )
                .await?;
                let vector = vector.ok_or_else(|| ZeppelinError::VectorNotFound {
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

/// Resolves the effective probe count for one algebra source position.
///
/// # Parameters
///
/// - `req`: Request containing source-local and top-level controls.
/// - `index`: Validated source position.
///
/// # Returns
///
/// ANN uses source-local then top-level precedence while preserving omission.
/// BM25 returns `None` because it does not probe vector clusters.
///
/// # Errors
///
/// Returns validation if sources or the indexed source are missing.
fn nprobe_for_algebra_source(
    req: &QueryRequest,
    index: usize,
) -> Result<Option<usize>, ZeppelinError> {
    let sources = req
        .sources
        .as_ref()
        .ok_or_else(|| ZeppelinError::Validation("retrieval algebra sources missing".into()))?;
    match sources.get(index) {
        Some(CandidateSource::Ann { nprobe, .. }) => Ok(nprobe.or(req.nprobe)),
        Some(CandidateSource::Bm25 { .. }) => Ok(None),
        None => Err(ZeppelinError::Validation(
            "validated algebra source is missing".into(),
        )),
    }
}

/// Resolves one source's effective probe count from the fixed manifest.
fn resolve_source_nprobe(
    source: &QuerySourceRef<'_>,
    manifest: &Manifest,
    requested_nprobe: Option<usize>,
    config: &IndexingConfig,
    default_nprobe_floor: usize,
) -> Result<usize, ZeppelinError> {
    match source {
        QuerySourceRef::Ann { .. } => {
            let active_segment = active_segment_snapshot(manifest);
            resolve_ann_nprobe(
                config,
                active_segment.as_ref(),
                requested_nprobe,
                default_nprobe_floor,
            )
        }
        QuerySourceRef::Bm25 { .. } => Ok(default_nprobe_floor),
    }
}

/// Resolves explicit or omitted ANN probes without changing hierarchy policy.
fn resolve_ann_nprobe(
    config: &IndexingConfig,
    active_segment: Option<&SegmentRef>,
    requested_nprobe: Option<usize>,
    default_nprobe_floor: usize,
) -> Result<usize, ZeppelinError> {
    if let Some(segment) = active_segment {
        if !segment.hierarchical && segment.cluster_count == 0 {
            return Err(ZeppelinError::Index(format!(
                "active flat segment {} advertises zero clusters",
                segment.id
            )));
        }
    }
    if let Some(nprobe) = requested_nprobe {
        return Ok(nprobe);
    }
    match active_segment {
        Some(segment) if !segment.hierarchical => {
            Ok(config
                .effective_default_nprobe_with_floor(segment.cluster_count, default_nprobe_floor))
        }
        _ => Ok(default_nprobe_floor),
    }
}

/// Reports a live query observation for the manifest's active segment.
///
/// # Parameters
///
/// - `state`: Server state containing the optional background hydrator.
/// - `namespace`: Namespace whose heat should be recorded.
/// - `manifest`: Same visibility snapshot selected for the query.
///
/// # Side Effects
///
/// If hydration is enabled and a matching active descriptor exists, updates heat
/// policy and may non-blockingly enqueue immutable segment warming. Queue
/// pressure or absence of a segment does not fail the query.
///
/// # Consistency
///
/// Only the descriptor selected from this manifest is observed. Hydration may
/// warm cache bytes but cannot alter the query's artifact membership.
///
/// # Examples
///
/// Repeated live queries can enqueue active `seg-42`. Historical queries disable
/// this notification so old snapshots do not heat the current-segment policy.
fn notify_hydrator(
    state: &AppState,
    manifest: &Manifest,
    authoritative_origin: Option<&crate::namespace::branching::ArtifactOrigin>,
) -> Result<(), ZeppelinError> {
    let Some(hydrator) = state.hydrator.as_ref() else {
        return Ok(());
    };
    let target = match authoritative_origin {
        Some(origin) => HydrationTarget::from_active_manifest_with_origin(manifest, origin)?,
        None => HydrationTarget::from_active_manifest(manifest)?,
    };
    let Some(target) = target else {
        return Ok(());
    };
    hydrator.observe_query(&target);
    Ok(())
}

/// Finds and clones the descriptor named by a manifest's active-segment ID.
///
/// # Parameters
///
/// - `manifest`: Query visibility snapshot.
///
/// # Returns
///
/// An owned [`SegmentRef`] when `active_segment` names an entry in `segments`,
/// or `None` for no active segment or an unmatched ID. This helper is only for
/// best-effort hydration notification; domain query execution validates/handles
/// its own segment state.
///
/// # Examples
///
/// A manifest retaining old and current descriptors returns only the one named
/// by `active_segment`.
fn active_segment_snapshot(manifest: &Manifest) -> Option<SegmentRef> {
    let active_segment = manifest.active_segment.as_ref()?;
    manifest
        .segments
        .iter()
        .find(|segment| segment.id == *active_segment)
        .cloned()
}

/// Chooses manifest-read freshness for a batch's shape-valid entries.
///
/// # Parameters
///
/// - `queries`: Positional request bodies.
/// - `validations`: Positional validation outcomes of equal logical length.
///
/// # Returns
///
/// Strong when any validation-success entry requests strong; eventual otherwise.
/// Invalid entries do not force manifest work or freshness.
///
/// # Examples
///
/// One valid strong entry plus three eventual entries selects a strongly
/// verified shared manifest. A strong-but-invalid entry among valid eventual
/// entries does not.
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

/// Wraps a successful domain response as one timed batch entry.
///
/// # Parameters
///
/// - `response`: Owned single-query response.
/// - `start`: Entry-local start instant.
///
/// # Returns
///
/// `ok = true`, boxed response, and elapsed whole milliseconds.
fn batch_success_entry(
    response: QueryResponse,
    start: Instant,
    redact_timing: bool,
) -> BatchQueryEntry {
    BatchQueryEntry::Success {
        ok: true,
        response: Box::new(response),
        metadata: BatchQueryEntryMetadata {
            latency_ms: batch_entry_latency_ms(start, redact_timing),
        },
    }
}

/// Logs and wraps one domain failure as a timed batch entry.
///
/// # Parameters
///
/// - `err`: Borrowed error to classify and expose safely.
/// - `start`: Entry-local start instant.
///
/// # Returns
///
/// `ok = false`, canonical [`BatchQueryError`], and elapsed whole milliseconds.
///
/// # Side Effects
///
/// Logs 5xx-class failures at error level and client/other failures at warning
/// level, preserving internal details in server telemetry.
fn batch_error_entry(err: &ZeppelinError, start: Instant, redact_timing: bool) -> BatchQueryEntry {
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
            latency_ms: batch_entry_latency_ms(start, redact_timing),
        },
    }
}

/// Returns entry timing unless a row-scoped policy makes it an activity oracle.
fn batch_entry_latency_ms(start: Instant, redact_timing: bool) -> u64 {
    if redact_timing {
        0
    } else {
        start.elapsed().as_millis() as u64
    }
}

/// Records one validated single-query duration when its scope exits.
///
/// This RAII guard covers namespace lookup, manifest resolution, and execution.
/// It is constructed only after request-shape validation, matching query metric
/// counting semantics.
///
/// # Rust Notes for Java/C Engineers
///
/// Rust calls [`Drop::drop`] automatically during normal return and `?` error
/// unwinding. It is analogous to Java `finally`; unlike a C cleanup convention,
/// the compiler inserts the call for every scope exit.
struct DurationGuard {
    /// Monotonic start instant captured after validation.
    start: std::time::Instant,
    /// Owned metric label that remains valid until drop.
    namespace: String,
}

impl Drop for DurationGuard {
    /// Observes elapsed seconds in the namespace query-duration histogram.
    ///
    /// # Side Effects
    ///
    /// Updates process-local metrics exactly once for this guard.
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        crate::metrics::QUERY_DURATION
            .with_label_values(&[&self.namespace])
            .observe(elapsed.as_secs_f64());
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    //! Pins omitted-probe resolution without object-store I/O.

    use super::*;
    use crate::config::IndexingConfig;

    fn segment(cluster_count: usize, hierarchical: bool) -> SegmentRef {
        SegmentRef {
            id: "seg_probe_policy".to_string(),
            vector_count: 1_000_000,
            cluster_count,
            quantization: crate::index::quantization::QuantizationType::Scalar,
            hierarchical,
            bitmap_fields: Vec::new(),
            fts_fields: Vec::new(),
            has_global_fts: false,
            cluster_owners: Vec::new(),
            sketch: None,
            cluster_objects: Vec::new(),
            bootstrap: None,
            membership: None,
            artifact_origin: None,
        }
    }

    /// Omitted flat probes scale per segment while explicit values stay exact.
    #[test]
    fn ann_nprobe_resolves_omission_per_flat_segment() {
        let config = IndexingConfig::default();
        let one_million = segment(334, false);
        let two_million = segment(667, false);

        assert_eq!(
            resolve_ann_nprobe(&config, Some(&one_million), None, 32).unwrap(),
            63
        );
        assert_eq!(
            resolve_ann_nprobe(&config, Some(&two_million), None, 32).unwrap(),
            126
        );
        assert_eq!(
            resolve_ann_nprobe(&config, Some(&two_million), Some(7), 32).unwrap(),
            7
        );
    }

    /// Hierarchical and WAL-only queries retain the captured runtime floor.
    #[test]
    fn ann_nprobe_keeps_non_flat_default_semantics() {
        let config = IndexingConfig::default();
        let hierarchical = segment(667, true);
        let invalid_flat = segment(0, false);

        assert_eq!(
            resolve_ann_nprobe(&config, Some(&hierarchical), None, 40).unwrap(),
            40
        );
        assert_eq!(resolve_ann_nprobe(&config, None, None, 40).unwrap(), 40);
        assert!(resolve_ann_nprobe(&config, Some(&invalid_flat), None, 40).is_err());
    }

    /// Stateless nodes configured with one key agree on tags; another key cannot.
    #[test]
    fn cursor_hmac_is_stable_across_nodes_and_key_separated() {
        let node_a = CursorBindingKey::from_config_hex(&"11".repeat(32)).unwrap();
        let node_b = CursorBindingKey::from_config_hex(&"11".repeat(32)).unwrap();
        let other = CursorBindingKey::from_config_hex(&"22".repeat(32)).unwrap();
        let expected = cursor_authentication_tag(node_a, 7, 11, 0x3f80_0000, b"row-1");

        assert_eq!(
            cursor_authentication_tag(node_b, 7, 11, 0x3f80_0000, b"row-1"),
            expected
        );
        assert_ne!(
            cursor_authentication_tag(other, 7, 11, 0x3f80_0000, b"row-1"),
            expected
        );
    }
}
