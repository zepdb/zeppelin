//! Executes ANN and BM25 retrieval against one authoritative manifest snapshot.
//!
//! This module is the domain query engine below the HTTP handlers. It reads the
//! manifest that defines visible immutable artifacts, searches the active
//! compacted segment, optionally scans uncompacted write-ahead-log (WAL)
//! fragments, and merges both views without resurrecting deleted or superseded
//! records. Callers enter through [`crate::query::execute_query`] for vector
//! retrieval or [`crate::query::execute_bm25_query`] for lexical retrieval.
//! Batch, historical (`as_of`),
//! and retrieval-algebra callers may instead supply a manifest they selected
//! earlier so every source observes the same snapshot.
//!
//! This file deliberately does **not** validate HTTP requests, choose historical
//! manifests, fuse ANN and BM25 source lists, rerank fused candidates, paginate,
//! group, or compute facets. Those orchestration steps live in
//! `server::handlers::query`; this module returns
//! [`crate::query::QueryResponse`] as the common carrier that those later stages
//! enrich.
//!
//! Object storage remains authoritative. [`crate::wal::Manifest`] membership
//! determines which immutable WAL fragments and segment descriptor are visible.
//! A [`crate::cache::manifest_cache::ManifestCache`] may avoid downloading
//! unchanged manifest bytes, while a [`crate::cache::DiskCache`] may serve
//! immutable index objects; neither cache may introduce an artifact absent from
//! the chosen snapshot.
//!
//! ## Reading map
//!
//! 1. Start with [`crate::query::QueryParams`] and
//!    [`crate::query::QueryResponse`] for the vector-query input and shared
//!    response contracts.
//! 2. Read [`crate::query::execute_query`] and then
//!    `execute_query_with_manifest_scoped` for manifest selection, concurrent
//!    WAL/segment work, refill, and merge.
//! 3. Read `wal_scan`, `segment_search`, and `merge_results` for the vector
//!    source implementations and latest-write-wins rules.
//! 4. Read [`crate::query::execute_bm25_query`] and
//!    `execute_bm25_query_with_manifest_scoped` for the equivalent lexical path.
//! 5. Finish with `segment_bm25_search`, its global-index and compatibility
//!    paths, and `merge_bm25_results`.
//! 6. The `QueryExplain*` types describe plans assembled by the HTTP layer;
//!    they are response data, not execution decisions made in this module.
//!
//! ## Query and visibility flow
//!
//! ```text
//! chosen manifest snapshot (authority for this execution)
//!        |
//!        +-----------------------+------------------------+
//!        |                        |                        |
//!        v                        v                        v
//! active immutable segment   uncompacted WAL         WAL tombstones
//! ANN or BM25 index search    Strong: score live      Strong + Eventual:
//!                             updates                 suppress old hits
//!        |                        |                        |
//!        +------------------------+------------------------+
//!                                 v
//!                    suppress superseded/deleted IDs
//!                    and retain score-ordered top-k
//!                                 |
//!                                 v
//!                          QueryResponse
//! ```
//!
//! Strong reads score both live WAL updates and the active segment. Eventual
//! reads skip WAL upsert scoring to reduce work, but still read tombstones:
//! staleness may omit a recent write, but it must not resurrect a known delete.
//! For ANN, smaller distance is better. For BM25, larger relevance is better.
//! The two score directions are kept separate here; the HTTP retrieval-algebra
//! layer normalizes or rank-fuses source results before hybrid reranking.
//!
//! ## Snapshot and cache boundaries
//!
//! ```text
//! ordinary query                    as_of / batch / hybrid query
//!       |                                      |
//!       v                                      v
//! read current manifest              caller selects one Manifest
//! Strong: verify with S3              and clones that same snapshot
//! Eventual: TTL cache allowed         for each source execution
//!       |                                      |
//!       +------------------+-------------------+
//!                          v
//!              immutable artifact reads
//!          memory/disk cache hit -> otherwise S3
//! ```
//!
//! A historical caller bypasses current-manifest selection by passing the
//! already-read [`crate::wal::Manifest`]. That preserves point-in-time source
//! membership; immutable artifact caches remain safe because a key's bytes never
//! change. No source may silently substitute a newer manifest after execution
//! begins.
//!
//! ## Invariants
//!
//! - A single owned [`crate::wal::Manifest`] snapshot governs one execution.
//!   WAL and segment futures borrow that same value and never re-read visibility
//!   state.
//! - Only `active_segment` is queried; older segment descriptors retained for
//!   history or garbage collection are not part of the active view.
//! - WAL fragments replay oldest to newest. A later upsert replaces an earlier
//!   value, and a tombstone removes any prior value for the same ID.
//! - Strong merges suppress every segment ID with a live WAL replacement or
//!   tombstone. Eventual merges suppress tombstones even though they omit live
//!   WAL replacements.
//! - Storage, decoding, index, and consistency failures are returned. A failed
//!   branch does not produce a partial [`crate::query::QueryResponse`].
//!
//! ## Rust concepts used here
//!
//! [`crate::query::QueryParams`] borrows stores, caches, filters, and the query
//! slice for the duration of an async call. This resembles passing object
//! references in Java or `const` pointers in C, but Rust proves that none outlive
//! their owners and that borrowed inputs remain valid across `.await`.
//! [`std::sync::Arc`] clones share cache state by incrementing a reference count
//! rather than deep-copying it.
//!
//! `tokio::join!` polls independent WAL and segment futures concurrently in the
//! same task. It is closer to joining two Java `CompletableFuture` operations
//! than to creating two OS threads; in C, the equivalent lifetime and cleanup
//! bookkeeping would be manual. `Result` plus `?` makes either branch's failure
//! abort the response while owned values are cleaned up by RAII.
//!
//! Iterator pipelines and the bounded `TopK` container consume owned
//! candidates and retain only the useful frontier. Borrowed WAL vectors avoid
//! cloning payloads during scoring; only IDs and attributes that must survive
//! fragment ownership are materialized into the returned results.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use serde::Serialize;
use tracing::{debug, instrument};

use crate::cache::decoded_cache::DecodedArtifactCache;
use crate::cache::manifest_cache::ManifestCache;
use crate::cache::{with_cache_diagnostics, CacheDiagnostics, DiskCache};
use crate::config::IndexingConfig;
use crate::error::Result;
use crate::fts::bm25::Bm25Params;
use crate::fts::inverted_index::{fts_index_key, InvertedIndex};
use crate::fts::rank_by::{evaluate_rank_by, RankBy};
use crate::fts::tokenizer::tokenize_text;
use crate::fts::wal_cache::WalFtsCache;
use crate::fts::wal_scan::wal_bm25_scan;
use crate::fts::FtsFieldConfig;
use crate::index::distance::compute_distance;
use crate::index::filter::{combine_filters, evaluate_filter};
use crate::index::topk::{partial_topk_by, TopK};
use crate::index::HierarchicalIndex;
use crate::index::IvfFlatIndex;
use crate::namespace::branching::ArtifactOrigin;
use crate::retrieval_scope::{
    scoped_ann_cache_key, scoped_fts_cache_key, segment_corpus_cache_key, ScopedAnnIndex,
    ScopedFtsIndex, ScopedSegmentCorpus,
};
use crate::storage::ZeppelinStore;
use crate::types::{AttributeValue, ConsistencyLevel, DistanceMetric, Filter, SearchResult};
use crate::wal::manifest::{
    CoarsePayloadEncoding, LocatedFragmentRef, LocatedSegmentRef, SegmentRef,
};
use crate::wal::Manifest;
use crate::wal::{FragmentCachePolicy, WalFragmentCache, WalReader};

/// Compiles the policy-owned and caller-owned predicates for source execution.
///
/// The mandatory predicate is always conjoined with the caller predicate, so
/// the caller can only narrow the policy-visible row set. Keeping composition
/// at this domain query seam prevents an HTTP handler from accidentally
/// mutating, exposing, or omitting the server-owned predicate while planning a
/// particular source.
#[must_use]
pub fn compile_effective_filter(
    mandatory_filter: Option<&Filter>,
    caller_filter: Option<&Filter>,
) -> Option<Filter> {
    combine_filters(mandatory_filter.cloned(), caller_filter.cloned())
}

/// Carries one ranked query result set and the optional HTTP enrichments.
///
/// ANN execution stores distances in [`SearchResult::score`] and therefore
/// orders smaller values first. BM25 execution stores relevance scores and
/// orders larger values first. Consumers must use the executed source or
/// explain plan to interpret the direction; the response does not normalize
/// these two score spaces. The HTTP layer serializes this type directly and may
/// populate pagination, grouping, facets, or explain data after this module
/// returns the base results.
///
/// # Examples
///
/// A strong ANN query that scans two WAL fragments and one active segment may
/// return five distance-ordered hits with `scanned_fragments = 2` and
/// `scanned_segments = 1`. A later grouping phase keeps those counters and adds
/// `groups` without changing which storage work already occurred.
///
/// # Rust Notes for Java/C Engineers
///
/// `Option<T>` makes an omitted response section distinct from an empty
/// section. Serde's `skip_serializing_if` omits `None` fields from JSON; Java
/// would commonly use nullable fields plus serializer annotations, while C
/// would need an explicit presence flag beside each payload.
#[derive(Debug, Serialize)]
pub struct QueryResponse {
    /// Ranked hits in the score direction of the executed source.
    pub results: Vec<SearchResult>,
    /// Number of WAL fragments whose live records were scored.
    ///
    /// Eventual reads may inspect tombstones while reporting zero here because
    /// they deliberately skip WAL candidate scoring.
    pub scanned_fragments: usize,
    /// Number of active compacted segments searched; currently zero or one.
    pub scanned_segments: usize,
    /// Optional query diagnostics, returned only when the request asks for them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub debug: Option<QueryDebug>,
    /// Opaque pagination cursor, returned only when cursor paging is enabled
    /// and more ranked results remain in the current candidate frontier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Grouped result hits, returned only when the request asks for grouping.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups: Option<Vec<QueryResultGroup>>,
    /// Facet counts, returned only when the request asks for facets.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<QueryFacets>,
    /// Query execution explain output, returned only when requested.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explain: Option<QueryExplain>,
}

/// Groups ranked hits that share one response-level attribute value.
///
/// Grouping is performed by the HTTP query orchestrator after source retrieval
/// and optional reranking. A hit with no requested group field receives its own
/// ID as a singleton key so unrelated missing values are not collapsed.
///
/// # Examples
///
/// Grouping by `category` can produce key `"books"` with three ranked hits;
/// missing-category hit `doc-9` appears alone under key `"doc-9"`.
#[derive(Debug, Clone, Serialize)]
pub struct QueryResultGroup {
    /// Group key. Missing group fields use the hit id as the singleton key.
    pub key: String,
    /// Ranked hits in this group.
    pub results: Vec<SearchResult>,
}

/// Holds response-level facet counts for requested attribute fields.
///
/// The outer map is deterministic for stable JSON output. Each inner map counts
/// stringified values over the candidate frontier selected by the HTTP layer,
/// rather than over every record stored in the namespace.
///
/// # Examples
///
/// Faceting field `color` over four candidates can yield `red: 3` and
/// `blue: 1`, nested beneath the `color` field key.
#[derive(Debug, Clone, Serialize)]
pub struct QueryFacets {
    /// Counts for each requested field.
    #[serde(flatten)]
    pub fields: BTreeMap<String, BTreeMap<String, usize>>,
}

/// Describes the executed retrieval plan and, optionally, result provenance.
///
/// The HTTP layer constructs this after validating the request. Plan mode
/// describes source selection and downstream transforms; full mode additionally
/// records how each returned ID scored in its sources and during fusion/rerank.
/// This is observational metadata and does not drive execution.
///
/// # Examples
///
/// A two-source ANN/BM25 request can report [`QueryExplainPath::AlgebraHybrid`]
/// with reciprocal-rank fusion. In full mode, each final hit also lists the raw
/// ANN distance, raw BM25 score, and each source's fusion contribution.
#[derive(Debug, Clone, Serialize)]
pub struct QueryExplain {
    /// Explain verbosity.
    pub mode: QueryExplainMode,
    /// Executed query plan.
    pub plan: QueryExplainPlan,
    /// Per-result provenance, present only for `full` explain.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub results: Option<Vec<QueryExplainResult>>,
}

/// Selects how much already-executed query information appears in explain data.
///
/// This `Copy` enum is a small closed set: matching it is exhaustive, unlike a
/// Java string constant or C integer whose unknown values require convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryExplainMode {
    /// Returns the physical plan but omits per-result score provenance.
    Plan,
    /// Returns the plan and provenance for each result that survives all stages.
    Full,
}

/// Captures the effective query plan after validation and default expansion.
///
/// `candidate_k`, `first_stage_top_k`, and `top_k` may differ: the handler can
/// retrieve a wider frontier for fusion, reranking, grouping, facets, or cursor
/// detection before returning the requested page size.
///
/// # Examples
///
/// A request with `top_k = 10` and `candidate_k = 100` may execute two sources
/// at a first-stage width of 100, fuse them, rerank a smaller frontier, and
/// finally return ten hits. This plan records all three widths.
#[derive(Debug, Clone, Serialize)]
pub struct QueryExplainPlan {
    /// Physical query path.
    pub path: QueryExplainPath,
    /// Candidate frontier requested by the client or chosen by defaults.
    pub candidate_k: usize,
    /// Actual width passed into each first-stage retrieval/fusion operation.
    pub first_stage_top_k: usize,
    /// Final hit count requested by the client or chosen by defaults.
    pub top_k: usize,
    /// Consistency mode applied to every source sharing this plan snapshot.
    pub consistency: ConsistencyLevel,
    /// Candidate sources in request order, which also indexes fusion weights.
    pub sources: Vec<QueryExplainSource>,
    /// Fusion strategy.
    pub fusion: QueryExplainFusion,
    /// Rerank strategy, or null when omitted.
    pub rerank: Option<QueryExplainRerank>,
    /// Grouping strategy, or null when omitted.
    pub grouping: Option<QueryExplainGrouping>,
    /// Cursor request details.
    pub cursor: QueryExplainCursor,
    /// Requested facet fields in client order.
    pub facets: Vec<String>,
    /// Projection details.
    pub projection: QueryExplainProjection,
    /// Whether a server-owned mandatory filter constrained this execution.
    ///
    /// The predicate itself is deliberately never exposed because doing so can
    /// reveal tenant or authorization topology.
    pub policy_filter_applied: bool,
}

/// Identifies which request syntax and number of retrieval sources were used.
///
/// Legacy and algebra paths can ultimately call the same ANN/BM25 primitives;
/// this value explains orchestration, not a different persisted index format.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryExplainPath {
    /// One ANN source selected through the legacy vector request fields.
    LegacyVector,
    /// One BM25 source selected through the legacy lexical request fields.
    LegacyBm25,
    /// One source selected through the explicit retrieval-algebra syntax.
    AlgebraSingle,
    /// Multiple retrieval-algebra sources combined by a fusion stage.
    AlgebraHybrid,
}

/// Records one first-stage candidate source in request order.
///
/// ANN sources include their effective IVF probe count. BM25 sources leave
/// `nprobe` absent because lexical index lookup does not use vector clusters as
/// an approximation parameter.
#[derive(Debug, Clone, Serialize)]
pub struct QueryExplainSource {
    /// Zero-based source position, also used to align weights and provenance.
    pub index: usize,
    /// Source kind.
    #[serde(rename = "type")]
    pub kind: QueryExplainSourceKind,
    /// Effective IVF probe count for ANN, or `None` for BM25.
    pub nprobe: Option<usize>,
    /// Candidate count requested from this source.
    pub candidate_k: usize,
}

/// Distinguishes vector-distance retrieval from lexical-relevance retrieval.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryExplainSourceKind {
    /// Approximate nearest-neighbor source where a lower raw distance is better.
    Ann,
    /// BM25 text source where a higher raw relevance score is better.
    Bm25,
}

/// Describes how multiple source rankings were combined by the HTTP layer.
///
/// Raw ANN and BM25 scores point in opposite directions and have unrelated
/// scales. Reciprocal rank fusion uses only position. Weighted fusion first
/// min-max normalizes each source and then applies the listed weights.
///
/// # Examples
///
/// `Rrf { k: 60 }` gives a hit at source rank 1 a contribution of
/// `1 / (60 + 1)` from that source. A hit present in both sources receives both
/// contributions.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum QueryExplainFusion {
    /// No fusion because only one source executed.
    None,
    /// Reciprocal rank fusion.
    Rrf {
        /// Positive rank offset that controls how quickly contribution decays.
        k: usize,
    },
    /// Weighted min-max normalized fusion.
    Weighted {
        /// Per-source weights aligned with [`QueryExplainSource::index`].
        weights: Vec<f32>,
    },
}

/// Describes the optional second-stage scorer applied after candidate retrieval.
///
/// Reranking may reorder the candidate frontier but cannot retrieve an ID that
/// no first-stage source produced.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum QueryExplainRerank {
    /// Uses the engine's source-dependent default rerank behavior.
    Default,
    /// Preserves fusion or source ordering without a second-stage scorer.
    None,
    /// Recomputes ordering with vector distance over candidate vectors.
    Vector,
    /// Recomputes ordering with lexical relevance over candidate documents.
    Bm25,
}

/// Describes the response-level grouping applied after ranking.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum QueryExplainGrouping {
    /// Returns the ordinary flat ranked list.
    None,
    /// Field grouping.
    Field {
        /// Attribute field whose stringified value selects a group.
        field: String,
        /// Maximum ranked hits retained in any one group.
        max_per_group: usize,
    },
}

/// Records whether cursor pagination participated in this execution.
///
/// `requested` distinguishes an explicitly requested first page from an
/// ordinary unpaginated query. `after` records whether a prior-page token was
/// decoded and applied.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct QueryExplainCursor {
    /// Whether the request included a cursor block.
    pub requested: bool,
    /// Whether an after-token was applied.
    pub after: bool,
}

/// Records the response projection that affected retrieval materialization.
///
/// Attribute omission can avoid decoding or cloning metadata on hot query
/// paths. Vector projection is not represented because this execution layer
/// returns IDs, scores, and optional attributes rather than stored vectors.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct QueryExplainProjection {
    /// Whether result attributes are included in the response.
    pub include_attributes: bool,
}

/// Traces one final result through source scoring, fusion, and explicit rerank.
///
/// Source entries remain in request order. `fused_score` is the value before an
/// explicit reranker; when no fusion ran it is simply the single source's raw
/// score. `rerank_score` is absent unless the handler ran an explicit reranker.
///
/// # Examples
///
/// A hybrid hit can show ANN raw distance `0.12`, BM25 raw score `8.4`, fused
/// RRF score `0.031`, and a final vector rerank distance of `0.09`.
#[derive(Debug, Clone, Serialize)]
pub struct QueryExplainResult {
    /// Stable record ID of the returned hit.
    pub id: String,
    /// Score details for sources that contributed or considered this ID.
    pub sources: Vec<QueryExplainResultSource>,
    /// Fused score before explicit rerank, or the source score for single-source queries.
    pub fused_score: f32,
    /// Explicit rerank score, present only when an explicit rerank ran.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rerank_score: Option<f32>,
}

/// Explains one source's contribution to one final result.
///
/// `raw_score` retains the source-native direction. `normalized_score` is only
/// present for weighted min-max fusion; RRF operates on rank position instead.
#[derive(Debug, Clone, Serialize)]
pub struct QueryExplainResultSource {
    /// Zero-based request position identifying the candidate source.
    pub index: usize,
    /// Source kind.
    #[serde(rename = "type")]
    pub kind: QueryExplainSourceKind,
    /// Native ANN distance or BM25 relevance before fusion.
    pub raw_score: f32,
    /// Direction-adjusted, min-max score for weighted fusion, when applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub normalized_score: Option<f32>,
    /// Score contribution after fusion weighting or RRF contribution.
    pub contribution: f32,
}

/// Reports measured work for one source execution when debug mode is enabled.
///
/// Durations are phase wall times rather than a sum that predicts end-to-end
/// latency: WAL and segment work normally overlap. For hybrid queries, the HTTP
/// layer aggregates per-source diagnostics. Counters describe this execution,
/// not global cache or index totals.
///
/// # Examples
///
/// An eventual query may report `fragments_scanned = 0`, one segment, and
/// `underfill_reason = "eventual_skipped_wal"` even though it read WAL
/// tombstones. The counter measures candidate scoring, while the reason exposes
/// the deliberate freshness tradeoff.
#[derive(Debug, Clone, Serialize)]
pub struct QueryDebug {
    /// WAL scan phase latency in milliseconds.
    pub wal_ms: u64,
    /// Segment search phase latency in milliseconds.
    pub segment_ms: u64,
    /// Result merge phase latency in milliseconds.
    pub merge_ms: u64,
    /// Number of WAL fragments scanned, matching `scanned_fragments`.
    pub fragments_scanned: usize,
    /// Number of compacted segments scanned, matching `scanned_segments`.
    pub segments_scanned: usize,
    /// IVF clusters probed, or clusters scanned by BM25's compatibility path.
    ///
    /// Global BM25 index search reports zero because it does not fan out over
    /// every per-cluster inverted index.
    pub clusters_probed: usize,
    /// Cache activity observed during this query.
    pub cache: QueryDebugCache,
    /// Effective consistency level used by execution.
    pub consistency_effective: ConsistencyLevel,
    /// Stable reason code for underfill, or `None` when `top_k` was filled.
    pub underfill_reason: Option<String>,
}

/// Counts immutable-object cache outcomes scoped to one debug execution.
///
/// A hit avoids an object-store body read. A miss may trigger one fetch whose
/// bytes are then stored for later queries; these values do not describe the
/// manifest cache's semantic freshness decision.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct QueryDebugCache {
    /// Cache hits observed during this query.
    pub hits: u64,
    /// Cache misses observed during this query.
    pub misses: u64,
}

/// Borrows every dependency and tuning value required by one ANN execution.
///
/// The HTTP layer validates dimensions and non-finite coordinates before
/// constructing this value. This layer assumes the selected distance metric and
/// query dimensions agree with namespace/index metadata. Grouping parameters
/// keeps call sites explicit while avoiding a long positional argument list.
///
/// # Examples
///
/// A strong cosine query can borrow a 768-element vector, the namespace's
/// readers and caches, request ten results, and probe eight IVF clusters. The
/// struct itself owns none of those dependencies; they remain usable by the
/// caller after the async query completes.
///
/// # Rust Notes for Java/C Engineers
///
/// The single lifetime `'a` ties all references to a scope that outlives query
/// execution. Java's garbage collector would keep referenced objects alive at
/// runtime; C would rely on the caller's discipline. Rust rejects a future that
/// could outlive any borrowed store, cache, filter, string, or slice. An
/// `Option<&Arc<DiskCache>>` borrows the caller's shared-owner handle; cloning an
/// `Arc` elsewhere increments a refcount, not the cached data.
pub struct QueryParams<'a> {
    /// Object-store boundary used for authoritative S3/MinIO artifact reads.
    pub store: &'a ZeppelinStore,
    /// Reader that decodes visible immutable WAL fragments and tombstones.
    pub wal_reader: &'a WalReader,
    /// Validated namespace whose manifest and artifact keys are addressed.
    pub namespace: &'a str,
    /// Borrowed query coordinates with the namespace's configured dimensions.
    pub query: &'a [f32],
    /// Maximum number of ranked results to return; zero yields an empty set.
    pub top_k: usize,
    /// Requested IVF clusters to probe, trading object reads/CPU for recall.
    pub nprobe: usize,
    /// Optional metadata predicate applied to WAL and segment candidates.
    pub filter: Option<&'a Filter>,
    /// Whether live WAL upserts participate in candidate retrieval.
    pub consistency: ConsistencyLevel,
    /// Namespace distance metric; lower computed distance is always ranked first.
    pub distance_metric: DistanceMetric,
    /// Candidate multiplier used to compensate for post-filter rejection.
    pub oversample_factor: usize,
    /// Largest byte gap allowed when coalescing exact-rerank range GETs.
    pub rerank_coalesce_gap_bytes: usize,
    /// Whether an unfiltered quantized segment scan may pick its exact-rerank
    /// frontier from resident sketch row scores instead of reading coarse
    /// payloads. Off is the shipped default; see
    /// [`Config::effective_resident_row_bypass`](crate::config::Config::effective_resident_row_bypass).
    pub resident_row_bypass: bool,
    /// Disposable immutable-artifact cache; absence reads through the store.
    pub cache: Option<&'a Arc<DiskCache>>,
    /// Optional current-manifest cache with consistency-aware freshness rules.
    pub manifest_cache: Option<&'a Arc<ManifestCache>>,
    /// Whether winning hits should materialize response attributes.
    pub include_attributes: bool,
}

/// Mandatory-scope inputs required to derive a policy-local ANN artifact.
#[derive(Clone, Copy)]
pub(crate) struct ScopedAnnQuery<'a> {
    pub(crate) mandatory_filter: &'a Filter,
    pub(crate) indexing_config: &'a IndexingConfig,
    pub(crate) decoded_artifact_cache: &'a Arc<DecodedArtifactCache>,
}

/// Orders ANN hits by distance ascending and then by ID ascending.
///
/// # Parameters
///
/// - `a`: First distance-scored hit.
/// - `b`: Second distance-scored hit.
///
/// # Returns
///
/// A total ordering suitable for deterministic sorting and bounded top-k. The
/// ID tie-break prevents hash-map iteration order from affecting equal scores.
///
/// # Examples
///
/// Distance `0.1` ranks before `0.2`; equal-distance IDs `a` and `b` rank in
/// that lexical order.
fn distance_result_cmp(a: &SearchResult, b: &SearchResult) -> Ordering {
    a.score.total_cmp(&b.score).then_with(|| a.id.cmp(&b.id))
}

/// Orders lexical hits by BM25 relevance descending and then by ID ascending.
///
/// # Parameters
///
/// - `a`: First relevance-scored hit.
/// - `b`: Second relevance-scored hit.
///
/// # Returns
///
/// A deterministic total ordering with the highest score first.
///
/// # Examples
///
/// Relevance `8.0` ranks before `3.0`; equal-score IDs use lexical order.
fn bm25_result_cmp(a: &SearchResult, b: &SearchResult) -> Ordering {
    b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id))
}

#[cfg(test)]
/// Counts attribute-map clones performed while materializing WAL test results.
///
/// Production builds omit this counter entirely. Tests reset it before a scan
/// and verify that rejected candidates do not pay a deep-clone cost.
static WAL_ATTR_CLONES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Materializes one winning WAL candidate's borrowed attributes.
///
/// # Parameters
///
/// - `attrs`: Attribute map borrowed from a decoded immutable WAL fragment.
///
/// # Returns
///
/// An owned deep clone that can outlive the fragment batch. Test builds also
/// increment `WAL_ATTR_CLONES`.
///
/// # Examples
///
/// A winning candidate with `{category: "book"}` receives an owned response
/// map; a candidate discarded by top-k never calls this helper.
///
/// # Rust Notes for Java/C Engineers
///
/// Cloning a `HashMap<String, AttributeValue>` allocates and clones its owned
/// entries; this is not a cheap pointer copy. The query deliberately delays it
/// until after top-k. Java references would normally share a mutable map unless
/// explicitly copied, while C would require a hand-written deep-copy contract.
fn clone_wal_result_attrs(
    attrs: &HashMap<String, AttributeValue>,
) -> HashMap<String, AttributeValue> {
    #[cfg(test)]
    WAL_ATTR_CLONES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    attrs.clone()
}

/// Loads the manifest snapshot that will define one current query's visibility.
///
/// Strong consistency asks the manifest cache to verify freshness against
/// object storage. Eventual consistency may reuse a TTL-valid cached snapshot.
/// Without a cache, both modes read the current object directly; a missing
/// object becomes an empty manifest for the namespace.
///
/// # Parameters
///
/// - `store`: Authoritative object-store boundary.
/// - `namespace`: Namespace whose current manifest is required.
/// - `consistency`: Freshness contract for cached manifest selection.
/// - `manifest_cache`: Optional shared cache; `None` forces a direct read.
///
/// # Returns
///
/// An owned [`Manifest`] snapshot. Later query phases use this exact value and
/// do not independently refresh it.
///
/// # Errors
///
/// Propagates object-store, conditional-read, and manifest-decode failures. A
/// stale cache entry is not served as a fallback after a required refresh
/// fails, and no query work has begun when this function returns an error.
///
/// # Side Effects
///
/// May perform a manifest GET or conditional GET and populate/refresh the
/// shared manifest cache.
///
/// # Consistency
///
/// The returned value, not later cache changes, is the authority for this
/// execution. Strong means remotely verified freshness; eventual means bounded
/// manifest staleness plus the separate tombstone rules enforced downstream.
///
/// # Performance
///
/// A valid eventual cache entry avoids object-store I/O. Strong normally
/// performs a conditional GET, which can avoid downloading unchanged bytes.
/// Concurrent cache misses or verifications are coalesced per namespace.
///
/// # Examples
///
/// If cached generation 12 is still within the eventual TTL, an eventual query
/// reuses it. A strong query verifies its ETag; if generation 13 is current,
/// generation 13 is decoded and governs both WAL and segment work.
///
/// # Rust Notes for Java/C Engineers
///
/// The nested exhaustive `match` covers both independent enums/options, so no
/// null or mode falls through implicitly. `?` propagates a typed error and RAII
/// releases any cache singleflight guard on every return path.
pub(crate) async fn read_manifest_for_query(
    store: &ZeppelinStore,
    namespace: &str,
    consistency: ConsistencyLevel,
    manifest_cache: Option<&Arc<ManifestCache>>,
) -> Result<Manifest> {
    match manifest_cache {
        Some(mc) => match consistency {
            ConsistencyLevel::Strong => mc.get_strong_required(store, namespace).await,
            ConsistencyLevel::Eventual => mc.get_required(store, namespace).await,
        },
        None => Manifest::read_required(store, namespace).await,
    }
}

/// Classifies why a debug query returned fewer than its requested top-k.
///
/// # Parameters
///
/// - `results_len`: Number of final ranked hits.
/// - `top_k`: Requested result count.
/// - `consistency`: Effective consistency mode.
/// - `eventual_skipped_wal`: Whether visible uncompacted upserts were omitted.
/// - `scanned_fragments`: WAL fragment scoring count.
/// - `scanned_segments`: Active segment search count.
///
/// # Returns
///
/// `None` when the result set is full. Otherwise returns one stable reason code:
/// eventual WAL omission takes precedence, then absence of any candidate source,
/// then the general `not_enough_matches` condition.
///
/// # Examples
///
/// Three hits for `top_k = 10` with an eventual skipped WAL returns
/// `eventual_skipped_wal`; zero hits with no WAL or segment returns
/// `no_index_or_wal_data`.
fn query_underfill_reason(
    results_len: usize,
    top_k: usize,
    consistency: ConsistencyLevel,
    eventual_skipped_wal: bool,
    scanned_fragments: usize,
    scanned_segments: usize,
) -> Option<String> {
    if results_len >= top_k {
        return None;
    }
    if consistency == ConsistencyLevel::Eventual && eventual_skipped_wal {
        return Some("eventual_skipped_wal".to_string());
    }
    if scanned_fragments == 0 && scanned_segments == 0 {
        return Some("no_index_or_wal_data".to_string());
    }
    Some("not_enough_matches".to_string())
}

/// Resolves the manifest's active segment ID to an owned descriptor.
///
/// # Parameters
///
/// - `manifest`: Chosen visibility snapshot.
///
/// # Returns
///
/// `Ok(Some)` with a cloned [`SegmentRef`] when `active_segment` names a retained
/// descriptor, or `Ok(None)` when no active ID exists.
///
/// # Errors
///
/// Returns [`crate::error::ZeppelinError::Index`] when the active ID is present
/// but its descriptor is absent. That malformed authoritative state must never
/// be reinterpreted as an empty compacted view.
///
/// # Consistency
///
/// Resolution is confined to the supplied snapshot; the helper never lists
/// objects or chooses another retained segment as a fallback.
///
/// # Examples
///
/// A manifest retaining `seg_old` and active `seg_new` yields a clone of
/// `seg_new`; historical descriptors are not searched.
///
/// # Rust Notes for Java/C Engineers
///
/// `as_ref`, `and_then`, and `find` borrow through nested optional state, then
/// `cloned` creates an owned descriptor so async closures can share it without
/// retaining an iterator borrow into the manifest.
/// Decides whether suppression requires a wider second segment search.
///
/// A first segment search can fill `requested_top_k` entirely with IDs that a
/// newer WAL upsert or tombstone will later remove. When that happens, this
/// helper conservatively widens the segment frontier by the number of distinct
/// IDs that *could* be suppressed, capped by the segment's vector count.
///
/// # Parameters
///
/// - `segment_results`: Initial score-ordered segment frontier.
/// - `requested_top_k`: Original source result count.
/// - `consistency`: Determines whether live WAL replacements suppress segment
///   hits or only tombstones do.
/// - `wal_overriding_ids`: Every live latest WAL ID.
/// - `wal_deleted_ids`: Every effective WAL tombstone ID.
/// - `segment_vector_count`: Manifest-declared upper bound on segment hits.
///
/// # Returns
///
/// A larger `top_k` for one retry, or `None` when the initial frontier is not
/// full, contains no suppressed hit, cannot be widened, or requested zero.
///
/// # Performance
///
/// Scans the initial frontier and suppression sets in memory. Returning `Some`
/// causes a second ANN segment search, including its cache/S3 and rerank costs.
///
/// # Examples
///
/// If top three segment hits contain two IDs replaced in strong WAL state and
/// the segment has at least five records, this returns a retry width of five.
/// Eventual mode ignores live replacements but still widens for tombstones.
fn segment_refill_top_k(
    segment_results: &[SearchResult],
    requested_top_k: usize,
    consistency: ConsistencyLevel,
    wal_overriding_ids: &HashSet<String>,
    wal_deleted_ids: &HashSet<String>,
    segment_vector_count: usize,
) -> Option<usize> {
    if requested_top_k == 0 || segment_results.len() != requested_top_k {
        return None;
    }
    if count_suppressed_segment_hits(
        segment_results,
        consistency,
        wal_overriding_ids,
        wal_deleted_ids,
    ) == 0
    {
        return None;
    }
    let suppressed_upper_bound =
        suppressed_segment_id_upper_bound(consistency, wal_overriding_ids, wal_deleted_ids);
    if suppressed_upper_bound == 0 {
        return None;
    }
    requested_top_k
        .saturating_add(suppressed_upper_bound)
        .min(segment_vector_count)
        .checked_sub(requested_top_k)
        .filter(|extra| *extra > 0)
        .map(|extra| requested_top_k + extra)
}

/// Counts initial segment hits that the selected consistency mode will remove.
///
/// # Parameters
///
/// - `segment_results`: Candidate frontier to inspect.
/// - `consistency`: Strong or eventual suppression policy.
/// - `wal_overriding_ids`: IDs with a newer live WAL version.
/// - `wal_deleted_ids`: IDs whose latest WAL state is deleted.
///
/// # Returns
///
/// Number of candidates that cannot survive final merge.
///
/// # Examples
///
/// For segment IDs `[a, b, c]`, live WAL replacement `a`, and tombstone `b`,
/// strong returns two while eventual returns one.
fn count_suppressed_segment_hits(
    segment_results: &[SearchResult],
    consistency: ConsistencyLevel,
    wal_overriding_ids: &HashSet<String>,
    wal_deleted_ids: &HashSet<String>,
) -> usize {
    segment_results
        .iter()
        .filter(|result| {
            segment_hit_suppressed(&result.id, consistency, wal_overriding_ids, wal_deleted_ids)
        })
        .count()
}

/// Tests whether one segment ID is hidden by newer WAL state.
///
/// # Parameters
///
/// - `id`: Borrowed segment result ID.
/// - `consistency`: Strong suppresses replacements and deletes; eventual only
///   suppresses deletes.
/// - `wal_overriding_ids`: Live latest WAL IDs.
/// - `wal_deleted_ids`: Effective WAL tombstones.
///
/// # Returns
///
/// `true` when final merge must omit this segment version.
///
/// # Examples
///
/// A freshly updated ID is suppressed under strong consistency but remains the
/// older segment version under eventual consistency; a deleted ID is always
/// suppressed.
fn segment_hit_suppressed(
    id: &str,
    consistency: ConsistencyLevel,
    wal_overriding_ids: &HashSet<String>,
    wal_deleted_ids: &HashSet<String>,
) -> bool {
    match consistency {
        ConsistencyLevel::Strong => wal_overriding_ids.contains(id) || wal_deleted_ids.contains(id),
        ConsistencyLevel::Eventual => wal_deleted_ids.contains(id),
    }
}

/// Computes the maximum number of distinct segment IDs suppression may remove.
///
/// # Parameters
///
/// - `consistency`: Selects replacement-plus-delete or delete-only semantics.
/// - `wal_overriding_ids`: Live latest WAL IDs.
/// - `wal_deleted_ids`: Effective WAL tombstone IDs.
///
/// # Returns
///
/// Size of the union for strong reads or tombstone count for eventual reads.
/// Overlapping replacement/delete sets count once.
///
/// # Examples
///
/// Override set `{a, b}` and tombstone set `{b, c}` produce a strong upper
/// bound of three and an eventual upper bound of two.
fn suppressed_segment_id_upper_bound(
    consistency: ConsistencyLevel,
    wal_overriding_ids: &HashSet<String>,
    wal_deleted_ids: &HashSet<String>,
) -> usize {
    match consistency {
        ConsistencyLevel::Strong => wal_overriding_ids.union(wal_deleted_ids).count(),
        ConsistencyLevel::Eventual => wal_deleted_ids.len(),
    }
}

/// Executes one vector query against the namespace's current manifest snapshot.
///
/// This convenience entry point first applies consistency-aware manifest
/// loading, then searches the snapshot through the same path used by batch and
/// historical callers. It does not add HTTP-level fusion, rerank, pagination,
/// grouping, facets, or explain output.
///
/// # Parameters
///
/// - `params`: Borrowed vector query dependencies and tuning values. Callers
///   must validate dimensions and finite coordinates before entering this hot
///   path.
///
/// # Returns
///
/// A [`QueryResponse`] whose results are ordered by ascending distance and
/// contain at most `top_k` entries. Debug and response-enrichment fields are
/// absent.
///
/// # Errors
///
/// Propagates manifest freshness/read/decode failures, WAL reads, immutable
/// segment loads, cache fetches, index decoding, and ANN search errors. If any
/// concurrent branch fails, no partial response is returned.
///
/// # Side Effects
///
/// Reads the current manifest and visible immutable artifacts, may populate
/// manifest/disk caches, and emits tracing events. It publishes no state.
///
/// # Consistency
///
/// Strong mode remotely verifies the current manifest and includes latest WAL
/// upserts. Eventual mode may use a TTL-valid manifest and skips WAL upserts,
/// but still reads visible WAL tombstones so deletes do not reappear.
///
/// # Performance
///
/// After manifest selection, WAL and segment branches run concurrently. A
/// cache miss incurs object-store GETs for visible WAL/index artifacts. ANN
/// segment cost depends on index family, `nprobe`, filtering, quantization, and
/// exact-rerank range coalescing. Suppression can trigger one wider retry.
///
/// # Examples
///
/// With active segment `seg-9` and two uncompacted fragments, a strong query
/// searches `seg-9`, replays both fragments, removes segment hits replaced or
/// deleted by WAL state, and returns the nearest ten. If a required cluster
/// object is missing, it returns an error rather than the WAL-only partial list.
///
/// # Rust Notes for Java/C Engineers
///
/// `QueryParams<'_>` is moved into the async function, but its fields are
/// borrows. Moving the wrapper does not move the stores or query coordinates.
/// The `#[instrument(skip(params))]` macro adds tracing without trying to format
/// large or sensitive borrowed inputs.
#[instrument(skip(params), fields(namespace = params.namespace))]
pub async fn execute_query(params: QueryParams<'_>) -> Result<QueryResponse> {
    let manifest = read_manifest_for_query(
        params.store,
        params.namespace,
        params.consistency,
        params.manifest_cache,
    )
    .await?;
    execute_query_with_manifest_inner(params, manifest, None, None, None, false).await
}

/// Executes a vector query with a disposable decoded-WAL memo.
///
/// Visibility is still selected from the same authoritative manifest path as
/// [`execute_query`]. The supplied cache can only satisfy exact fragment IDs
/// referenced by that snapshot.
#[instrument(skip(params, fragment_cache), fields(namespace = params.namespace))]
pub async fn execute_query_with_fragment_cache(
    params: QueryParams<'_>,
    fragment_cache: &Arc<WalFragmentCache>,
) -> Result<QueryResponse> {
    let manifest = read_manifest_for_query(
        params.store,
        params.namespace,
        params.consistency,
        params.manifest_cache,
    )
    .await?;
    execute_query_with_manifest_inner(params, manifest, Some(fragment_cache), None, None, false)
        .await
}

/// Executes a vector query against an already-selected manifest snapshot.
///
/// Batch, hybrid, and historical query orchestration uses this to share one
/// freshness/as-of decision instead of letting each source read a different
/// current manifest.
///
/// # Parameters
///
/// - `params`: Borrowed ANN dependencies and settings; its manifest-cache field
///   is ignored because visibility has already been selected.
/// - `manifest`: Owned authoritative snapshot for this execution.
///
/// # Returns
///
/// Distance-ordered results and scan counts, with no debug block.
///
/// # Errors
///
/// Propagates artifact reads, cache operations, decoding, and index search
/// failures. The supplied manifest is not replaced with a newer one on error.
///
/// # Consistency
///
/// `consistency` still controls WAL participation, but it does not change the
/// supplied snapshot's artifact membership. This is what preserves `as_of` and
/// same-snapshot multi-source semantics.
///
/// # Examples
///
/// A batch handler reads generation 42 once, clones it for three requests, and
/// calls this function three times. A concurrently published generation 43 is
/// not mixed into that batch.
pub(crate) async fn execute_query_with_manifest(
    params: QueryParams<'_>,
    manifest: Manifest,
    fragment_cache: Option<&Arc<WalFragmentCache>>,
    scoped_ann: Option<ScopedAnnQuery<'_>>,
    authoritative_origin: Option<ArtifactOrigin>,
) -> Result<QueryResponse> {
    execute_query_with_manifest_inner(
        params,
        manifest,
        fragment_cache,
        scoped_ann,
        authoritative_origin,
        false,
    )
    .await
}

/// Executes a supplied-snapshot vector query while collecting scoped diagnostics.
///
/// # Parameters
///
/// - `params`: Borrowed ANN dependencies and execution settings.
/// - `manifest`: Owned visibility snapshot selected by the caller.
///
/// # Returns
///
/// The normal distance-ranked response plus WAL, segment, merge, cluster, cache,
/// consistency, and underfill diagnostics.
///
/// # Errors
///
/// Returns the same failures as [`execute_query`]; diagnostics never turn a
/// failed source into a partial successful response.
///
/// # Side Effects
///
/// Installs task-scoped cache counters for the duration of execution.
///
/// # Examples
///
/// A debug query whose immutable cluster is already cached reports a cache hit
/// while returning exactly the same result ordering as the non-debug path.
pub(crate) async fn execute_query_with_manifest_debug(
    params: QueryParams<'_>,
    manifest: Manifest,
    fragment_cache: Option<&Arc<WalFragmentCache>>,
    scoped_ann: Option<ScopedAnnQuery<'_>>,
    authoritative_origin: Option<ArtifactOrigin>,
) -> Result<QueryResponse> {
    execute_query_with_manifest_inner(
        params,
        manifest,
        fragment_cache,
        scoped_ann,
        authoritative_origin,
        true,
    )
    .await
}

/// Selects normal or diagnostic vector execution for one supplied snapshot.
///
/// # Parameters
///
/// - `params`: Borrowed ANN execution dependencies.
/// - `manifest`: Owned visibility snapshot.
/// - `emit_debug`: Whether cache diagnostics should be installed and returned.
///
/// # Returns
///
/// The completed query response, with `debug` populated only when requested.
///
/// # Errors
///
/// Propagates every error from scoped vector execution.
///
/// # Side Effects
///
/// Debug mode creates an [`Arc`] around per-query counters and installs it in
/// task-local diagnostic scope. Normal mode avoids that allocation.
///
/// # Examples
///
/// `emit_debug = false` executes directly. `true` counts cache outcomes in
/// nested index fetches without changing candidate selection.
///
/// # Rust Notes for Java/C Engineers
///
/// `async move` transfers the wrapper values and one cloned `Arc` handle into
/// the scoped future. The original counter remains shared through atomic
/// reference counting; Rust prevents either handle from being freed while the
/// future can still use it.
async fn execute_query_with_manifest_inner(
    params: QueryParams<'_>,
    manifest: Manifest,
    fragment_cache: Option<&Arc<WalFragmentCache>>,
    scoped_ann: Option<ScopedAnnQuery<'_>>,
    authoritative_origin: Option<ArtifactOrigin>,
    emit_debug: bool,
) -> Result<QueryResponse> {
    if emit_debug {
        let diagnostics = Arc::new(CacheDiagnostics::default());
        return with_cache_diagnostics(Arc::clone(&diagnostics), async move {
            execute_query_with_manifest_scoped(
                params,
                manifest,
                fragment_cache,
                scoped_ann,
                authoritative_origin,
                Some(diagnostics),
            )
            .await
        })
        .await;
    }
    execute_query_with_manifest_scoped(
        params,
        manifest,
        fragment_cache,
        scoped_ann,
        authoritative_origin,
        None,
    )
    .await
}

/// Runs all vector-query phases against one immutable manifest view.
///
/// The function concurrently obtains the latest permitted WAL view and the
/// active segment frontier, optionally widens segment search after WAL
/// suppression, then performs a deterministic latest-write-wins merge.
///
/// ```text
///                         one owned Manifest
///                         /                \
///                        v                  v
///             WAL future                    segment future
/// Strong: replay + score          active descriptor -> ANN search
/// Eventual: tombstones only       IVF-Flat or hierarchical
///                        \                  /
///                         +------ join -----+
///                                  |
///                    suppressed hit in full frontier?
///                       | no              | yes
///                       |                 v
///                       |       one wider segment retry
///                       +-----------------+
///                                  v
///                 WAL overrides/tombstones -> top-k merge
/// ```
///
/// # Parameters
///
/// - `params`: Borrowed vector, store/readers, consistency, filter, cache, and
///   ANN tuning. The manifest-cache reference is intentionally unused because
///   the snapshot is already fixed.
/// - `manifest`: Owned snapshot defining visible WAL refs and active segment.
/// - `cache_diagnostics`: Optional shared counters already installed in task
///   scope by the wrapper.
///
/// # Returns
///
/// A response containing ascending-distance hits, scored-source counts, and an
/// optional debug block. Enrichment fields remain absent for the HTTP layer.
///
/// # Errors
///
/// Returns if either concurrent branch fails, or if a later refill fails. No
/// partial WAL-only or segment-only response escapes. Reads may already have
/// warmed disposable caches before the error.
///
/// # Side Effects
///
/// Reads visible immutable objects, can populate disk cache entries, updates
/// scoped cache counters, and emits phase tracing. It never mutates a manifest
/// or artifact.
///
/// # Consistency
///
/// Both futures borrow the same manifest. Strong scans WAL live state and
/// tombstones; eventual reads tombstones only. A live WAL ID suppresses an old
/// segment version under strong mode even when the live record fails filtering
/// or top-k. Tombstones suppress segment results under both modes.
///
/// # Performance
///
/// `tokio::join!` overlaps I/O but waits for both branches. `nprobe` is capped
/// only for the debug counter; the index implementation owns effective probing.
/// A full initial frontier containing suppressed IDs may cause one additional
/// segment search with a bounded larger `top_k`.
///
/// # Examples
///
/// Segment top three are `a, b, c`; WAL contains a newer `a` that ranks poorly
/// and tombstones `b`. Strong merge must not return stale `a` or deleted `b`.
/// The refill asks for deeper segment candidates so `c, d, ...` can fill the
/// response before the newer WAL `a` is considered by distance.
///
/// # Rust Notes for Java/C Engineers
///
/// Destructuring `QueryParams` creates local borrowed bindings without cloning
/// the underlying stores or vector. Both async blocks borrow immutable snapshot
/// data, so Rust permits concurrent access. `tokio::join!` does not detach work:
/// the function owns and polls both futures until both complete.
async fn execute_query_with_manifest_scoped(
    params: QueryParams<'_>,
    manifest: Manifest,
    fragment_cache: Option<&Arc<WalFragmentCache>>,
    scoped_ann: Option<ScopedAnnQuery<'_>>,
    authoritative_origin: Option<ArtifactOrigin>,
    cache_diagnostics: Option<Arc<CacheDiagnostics>>,
) -> Result<QueryResponse> {
    let QueryParams {
        store,
        wal_reader,
        namespace: _,
        query,
        top_k,
        nprobe,
        filter,
        consistency,
        distance_metric,
        oversample_factor,
        rerank_coalesce_gap_bytes,
        resident_row_bypass,
        cache,
        manifest_cache: _,
        include_attributes,
    } = params;

    let local_origin = match authoritative_origin {
        Some(origin) => origin,
        None => manifest.local_origin()?,
    };
    let origin_resolver = manifest.artifact_origin_resolver(&local_origin)?;
    let located_fragments = origin_resolver.uncompacted_located_fragments()?;
    if let Some(fragment_cache) = fragment_cache {
        let active_identities = located_fragments
            .iter()
            .copied()
            .map(LocatedFragmentRef::identity)
            .collect::<Vec<_>>();
        fragment_cache.evict_compacted_located(&local_origin, &active_identities);
    }
    let eventual_skipped_wal =
        consistency == ConsistencyLevel::Eventual && !manifest.uncompacted_fragments().is_empty();
    let segment_ref = origin_resolver.active_located_segment()?;
    let coarse_payload_encoding = segment_ref.map_or(CoarsePayloadEncoding::Sq8, |located| {
        manifest.coarse_payload_encoding(&located.segment.id)
    });

    // WAL work and segment search are independent — they share only the
    // manifest snapshot — so run them concurrently. Strong scans and scores
    // WAL vectors. Eventual skips WAL vector scoring but still reads
    // tombstones, because deletes are correctness rather than freshness.
    let wal_future = async {
        // Short-circuit: skip WAL scan if no uncompacted fragments exist.
        let wal_start = std::time::Instant::now();
        let scan_result = match consistency {
            ConsistencyLevel::Strong if !manifest.uncompacted_fragments().is_empty() => {
                wal_scan(
                    wal_reader,
                    &located_fragments,
                    query,
                    filter,
                    distance_metric,
                    cache,
                    fragment_cache,
                    include_attributes,
                    top_k,
                )
                .await?
            }
            ConsistencyLevel::Eventual if !manifest.uncompacted_fragments().is_empty() => {
                let deleted_ids = wal_reader
                    .read_located_delete_ids_unchecked(
                        &located_fragments,
                        cache.map_or(FragmentCachePolicy::Bypass, FragmentCachePolicy::ReadWrite),
                        fragment_cache,
                    )
                    .await?;
                WalScanResult {
                    results: Vec::new(),
                    overriding_ids: HashSet::new(),
                    fragment_count: 0,
                    deleted_ids,
                }
            }
            _ => WalScanResult {
                results: Vec::new(),
                overriding_ids: HashSet::new(),
                fragment_count: 0,
                deleted_ids: HashSet::new(),
            },
        };
        let wal_ms = wal_start.elapsed().as_millis() as u64;
        debug!(
            wal_duration_ms = wal_ms,
            fragments_scanned = scan_result.fragment_count,
            "query phase: WAL scan"
        );
        Ok::<_, crate::error::ZeppelinError>((scan_result, wal_ms))
    };

    let segment_future = async {
        let segment_start = std::time::Instant::now();
        let (results, scanned, clusters_probed) = if let Some(seg_ref) = segment_ref {
            let output = segment_search(
                store,
                seg_ref,
                coarse_payload_encoding,
                query,
                top_k,
                nprobe,
                filter,
                distance_metric,
                oversample_factor,
                rerank_coalesce_gap_bytes,
                resident_row_bypass,
                cache,
                include_attributes,
                scoped_ann,
            )
            .await?;
            (output.results, 1, output.clusters_probed)
        } else {
            (Vec::new(), 0, 0)
        };
        let segment_ms = segment_start.elapsed().as_millis() as u64;
        debug!(
            segment_duration_ms = segment_ms,
            segments_scanned = scanned,
            "query phase: segment search"
        );
        Ok::<_, crate::error::ZeppelinError>((results, scanned, segment_ms, clusters_probed))
    };

    let (wal_result, segment_result) = tokio::join!(wal_future, segment_future);
    let (
        WalScanResult {
            results: wal_results,
            overriding_ids: wal_overriding_ids,
            fragment_count: scanned_fragments,
            deleted_ids: wal_deleted_ids,
        },
        wal_ms,
    ) = wal_result?;
    let (mut segment_results, scanned_segments, mut segment_ms, clusters_probed) = segment_result?;

    if let Some(seg_ref) = segment_ref {
        if let Some(refill_top_k) = segment_refill_top_k(
            &segment_results,
            top_k,
            consistency,
            &wal_overriding_ids,
            &wal_deleted_ids,
            seg_ref.segment.vector_count,
        ) {
            let segment_retry_start = std::time::Instant::now();
            let refill = segment_search(
                store,
                seg_ref,
                coarse_payload_encoding,
                query,
                refill_top_k,
                nprobe,
                filter,
                distance_metric,
                oversample_factor,
                rerank_coalesce_gap_bytes,
                resident_row_bypass,
                cache,
                include_attributes,
                scoped_ann,
            )
            .await?;
            segment_results = refill.results;
            let segment_retry_ms = segment_retry_start.elapsed().as_millis() as u64;
            segment_ms += segment_retry_ms;
            debug!(
                segment_retry_duration_ms = segment_retry_ms,
                refill_top_k, "query phase: segment refill search"
            );
        }
    }

    // Merge results — pass WAL tombstones so segment results for deleted
    // vectors are excluded (a delete of an already-compacted vector exists
    // only as a WAL tombstone until the next compaction).
    let merge_start = std::time::Instant::now();
    let results = merge_results(
        wal_results,
        &wal_overriding_ids,
        segment_results,
        top_k,
        consistency,
        &wal_deleted_ids,
    );
    let merge_duration = merge_start.elapsed();
    debug!(
        merge_duration_ms = merge_duration.as_millis() as u64,
        final_results = results.len(),
        "query phase: merge"
    );
    let merge_ms = merge_duration.as_millis() as u64;
    let debug = cache_diagnostics.map(|diagnostics| {
        let cache_snapshot = diagnostics.snapshot();
        QueryDebug {
            wal_ms,
            segment_ms,
            merge_ms,
            fragments_scanned: scanned_fragments,
            segments_scanned: scanned_segments,
            clusters_probed,
            cache: QueryDebugCache {
                hits: cache_snapshot.hits,
                misses: cache_snapshot.misses,
            },
            consistency_effective: consistency,
            underfill_reason: query_underfill_reason(
                results.len(),
                top_k,
                consistency,
                eventual_skipped_wal,
                scanned_fragments,
                scanned_segments,
            ),
        }
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

/// Separates scored WAL hits from the metadata needed to suppress stale segments.
///
/// `results` alone is insufficient for correctness: a latest WAL upsert may
/// fail a filter or fall outside top-k yet must still hide the same ID's older
/// compacted version under strong consistency.
///
/// # Examples
///
/// If WAL updates `a` to metadata that fails the query filter and deletes `b`,
/// `results` can be empty while `overriding_ids = {a}` and
/// `deleted_ids = {b}`. Strong merge removes both old segment records.
struct WalScanResult {
    /// Top-k scored search results, sorted ascending by distance.
    results: Vec<SearchResult>,
    /// All live WAL IDs after dedup/delete processing, including IDs that
    /// filter out or rank outside top-k. These suppress stale segment hits.
    overriding_ids: HashSet<String>,
    /// Number of decoded fragments replayed, including delete-only fragments.
    fragment_count: usize,
    /// IDs whose latest effective WAL state is a delete tombstone.
    ///
    /// A later upsert removes an ID from this set. Both consistency modes use
    /// it to prevent a compacted record from being resurrected.
    deleted_ids: HashSet<String>,
}

/// Replays and distance-scores every visible uncompacted WAL fragment.
///
/// Fragment refs come from the supplied manifest snapshot; this function never
/// refreshes visibility state. It decodes the referenced immutable fragments,
/// applies oldest-to-newest last-write-wins replay, evaluates metadata filters,
/// and keeps a bounded nearest-neighbor frontier. It also preserves complete
/// replacement/tombstone sets for final segment suppression.
///
/// ```text
/// manifest.fragment refs (oldest -> newest)
///                  |
///                  v
///       cache or S3 immutable reads
///                  |
///                  v
/// deletes remove ID; later upserts replace/revive ID
///                  |
///          +-------+-------------------+
///          |                           |
///          v                           v
/// all live IDs for suppression    filter -> distance -> TopK
///                                      |
///                                      v
///                          clone attributes for winners only
/// ```
///
/// # Parameters
///
/// - `wal_reader`: Reader for manifest-selected immutable fragment objects.
/// - `namespace`: Namespace prefix used to resolve fragment keys.
/// - `manifest`: Fixed authoritative snapshot; refs must be in replay order.
/// - `query`: Borrowed query coordinates matching stored vector dimensions.
/// - `filter`: Optional metadata predicate. A record without attributes fails
///   any supplied filter.
/// - `distance_metric`: Metric used for every live vector; lower is better.
/// - `cache`: Optional immutable fragment cache.
/// - `include_attributes`: Whether winning hits deep-clone their attributes.
/// - `top_k`: Maximum number of scored results to materialize.
///
/// # Returns
///
/// Ranked WAL hits plus every effective live replacement ID, fragment count,
/// and final tombstone ID. Results sort by distance then ID and contain at most
/// `top_k` entries.
///
/// # Errors
///
/// Propagates fragment fetch, decode, and checksum failures. The reader keeps
/// its historical `unchecked` name for compatibility, but cache misses still
/// validate immutable payload integrity and this path never skips a missing or
/// malformed visible object.
///
/// # Panics
///
/// Distance kernels require query/stored vector dimensions to agree. The HTTP
/// and write paths establish this invariant before execution; violating it can
/// trigger a debug assertion and would make optimized kernels invalid.
///
/// # Side Effects
///
/// May populate the disk cache and emits one debug event. It performs no writes.
///
/// # Consistency
///
/// Slice order defines replay order. Deletes within one fragment are processed
/// before its vectors, so an upsert for the same ID in that fragment is live.
/// `overriding_ids` includes filtered and out-of-frontier live records because
/// they still supersede compacted versions.
///
/// # Performance
///
/// Reads each visible uncompacted fragment, uses memory proportional to distinct
/// replayed IDs, scores every latest live record passing the filter, and keeps
/// only `top_k` scored wrappers. Vector payloads and attributes stay borrowed;
/// IDs and optional attributes are allocated only for suppression metadata and
/// final winners.
///
/// # Examples
///
/// Fragment 1 upserts `a` and `b`; fragment 2 deletes `a` and updates `b`.
/// The scan returns no `a`, scores only the new `b`, records `a` as deleted,
/// and records `b` as overriding even if `b` fails the filter.
///
/// # Rust Notes for Java/C Engineers
///
/// `latest_vectors` stores `&str`, `&[f32]`, and attribute references into the
/// owned fragment vector. Rust's lifetimes prevent those borrowed views from
/// escaping after fragments are dropped. Java would retain object references;
/// C would need manual guarantees that backing allocations remain alive. The
/// final iterator converts only winners into fully owned API values.
#[allow(clippy::too_many_arguments)]
async fn wal_scan(
    wal_reader: &WalReader,
    refs: &[LocatedFragmentRef<'_>],
    query: &[f32],
    filter: Option<&Filter>,
    distance_metric: DistanceMetric,
    cache: Option<&Arc<DiskCache>>,
    fragment_cache: Option<&Arc<WalFragmentCache>>,
    include_attributes: bool,
    top_k: usize,
) -> Result<WalScanResult> {
    // The historical `unchecked` name is compatibility-only: misses still
    // validate payload checksums, while decoded-cache hits reuse values that
    // already passed that validation.
    let fragments = wal_reader
        .read_located_query_fragments_unchecked(
            refs,
            cache.map_or(FragmentCachePolicy::Bypass, FragmentCachePolicy::ReadWrite),
            fragment_cache,
        )
        .await?;
    let frag_count = fragments.len();

    if fragments.is_empty() {
        return Ok(WalScanResult {
            results: Vec::new(),
            overriding_ids: HashSet::new(),
            fragment_count: 0,
            deleted_ids: HashSet::new(),
        });
    }

    // Collect all delete tombstones
    let mut deleted_ids: HashSet<String> = HashSet::new();
    // Latest vector state per ID (latest fragment wins), borrowed from the
    // fragment batch so vector payloads and attrs are not cloned before top-k.
    #[allow(clippy::type_complexity)]
    let mut latest_vectors: HashMap<
        &str,
        (
            &[f32],
            Option<&HashMap<String, crate::types::AttributeValue>>,
        ),
    > = HashMap::new();

    // Process fragments in ULID order (oldest first, so later overwrites earlier)
    for fragment in &fragments {
        for del_id in &fragment.deletes {
            deleted_ids.insert(del_id.clone());
            latest_vectors.remove(del_id.as_str());
        }
        for vec in &fragment.vectors {
            deleted_ids.remove(&vec.id);
            latest_vectors.insert(
                vec.id.as_str(),
                (vec.values.as_slice(), vec.attributes.as_ref()),
            );
        }
    }

    /// Borrows one scored candidate until bounded selection chooses winners.
    struct ScoredWalVector<'a> {
        /// Logical vector ID borrowed from the replay map.
        id: &'a str,
        /// Source-native distance; smaller values rank first.
        score: f32,
        /// Winning upsert's optional attributes, still borrowed from its fragment.
        attrs: Option<&'a HashMap<String, AttributeValue>>,
    }

    let mut overriding_ids = HashSet::with_capacity(latest_vectors.len());
    let mut top_results = TopK::new(top_k, |a: &ScoredWalVector<'_>, b: &ScoredWalVector<'_>| {
        a.score.total_cmp(&b.score).then_with(|| a.id.cmp(b.id))
    });

    // Score surviving vectors, but keep only the bounded top-k candidates.
    for (id, (values, attrs)) in &latest_vectors {
        overriding_ids.insert((*id).to_string());
        let passes_filter = {
            if let Some(f) = filter {
                match attrs {
                    Some(a) => evaluate_filter(f, a),
                    None => false,
                }
            } else {
                true
            }
        };
        if !passes_filter {
            continue;
        }

        let score = compute_distance(query, values, distance_metric);
        top_results.push(ScoredWalVector {
            id,
            score,
            attrs: *attrs,
        });
    }

    let results: Vec<SearchResult> = top_results
        .into_sorted_vec()
        .into_iter()
        .map(|scored| SearchResult {
            id: scored.id.to_string(),
            score: scored.score,
            attributes: if include_attributes {
                scored.attrs.map(clone_wal_result_attrs)
            } else {
                None
            },
        })
        .collect();

    debug!(
        surviving_vectors = overriding_ids.len(),
        topk_returned = results.len(),
        total_fragments = frag_count,
        "WAL scan complete"
    );

    Ok(WalScanResult {
        results,
        overriding_ids,
        fragment_count: frag_count,
        deleted_ids,
    })
}

/// Searches one active immutable segment through its manifest-declared index family.
///
/// Manifest metadata selects hierarchical navigation or IVF-Flat directly,
/// avoiding object-store probes to infer the layout. Bitmap-field metadata is
/// attached after loading so the index search can prefilter candidates where
/// persisted bitmaps exist. Both paths eventually return exact or approximate
/// distance-scored candidates according to their index implementation.
///
/// ```text
/// SegmentRef from chosen manifest
///          |
///          +-- hierarchical = true --> load hierarchy -> beam search
///          |
///          +-- hierarchical = false -> load IVF metadata -> nprobe clusters
///                                                      |
/// filter + bitmap fields ------------------------------+
///                                                      v
///                                    optional exact rerank -> distance top-k
/// ```
///
/// # Parameters
///
/// - `store`: Object-store boundary for immutable segment artifacts.
/// - `namespace`: Namespace prefix owning the logical segment.
/// - `segment_ref`: Active descriptor from the fixed manifest snapshot.
/// - `query`: Borrowed query coordinates matching the segment dimensions.
/// - `top_k`: Maximum candidate results returned by the index.
/// - `nprobe`: IVF cluster count or hierarchical beam width, depending on index
///   family; larger values generally spend more I/O/CPU for better recall.
/// - `filter`: Optional metadata predicate, using bitmap prefilter where
///   available and exact attribute evaluation for correctness.
/// - `distance_metric`: Namespace metric; returned scores are lower-is-better.
/// - `oversample_factor`: Extra candidate multiplier for filtered retrieval.
/// - `rerank_coalesce_gap_bytes`: Range gap threshold for coalescing exact
///   rerank reads in applicable IVF layouts.
/// - `cache`: Optional disposable cache for immutable artifacts.
/// - `include_attributes`: Whether returned hits should carry attributes.
///
/// # Returns
///
/// At most `top_k` segment hits ordered by distance and ID according to the
/// selected index implementation.
///
/// # Errors
///
/// Propagates missing/corrupt object, cache, index-load, bitmap/filter, cluster
/// decode, and search failures. No alternate index family or partial result is
/// substituted.
///
/// # Side Effects
///
/// Performs immutable object reads, may populate the disk cache, and updates no
/// authoritative state.
///
/// # Consistency
///
/// The descriptor was selected by the manifest before this call. Object
/// existence alone cannot make another segment visible, and retained older
/// descriptors are never probed as fallback.
///
/// # Performance
///
/// Manifest-provided cluster count, quantization, ownership, packed-object,
/// sketch, and bootstrap metadata avoid discovery GETs. Actual reads scale with
/// index navigation, `nprobe`, filter selectivity, cache hits, and reranking.
///
/// # Examples
///
/// A flat segment with 100 clusters and `nprobe = 8` selects nearby clusters,
/// reads their immutable data (or cache entries), filters candidates, and
/// returns the nearest ten. A hierarchical descriptor uses `8` as beam width
/// instead of pretending its artifacts are flat.
///
/// # Rust Notes for Java/C Engineers
///
/// Both index values are owned locals; only one branch is constructed. Mutable
/// access is used briefly to attach manifest bitmap metadata before the index is
/// borrowed across async search. Rust prevents another alias from mutating that
/// metadata during the await, unlike an unsynchronized Java object or C struct.
#[allow(clippy::too_many_arguments)]
struct SegmentSearchOutput {
    results: Vec<SearchResult>,
    clusters_probed: usize,
}

#[allow(clippy::too_many_arguments)]
async fn segment_search(
    store: &ZeppelinStore,
    located: LocatedSegmentRef<'_>,
    coarse_payload_encoding: CoarsePayloadEncoding,
    query: &[f32],
    top_k: usize,
    nprobe: usize,
    filter: Option<&Filter>,
    distance_metric: DistanceMetric,
    oversample_factor: usize,
    rerank_coalesce_gap_bytes: usize,
    resident_row_bypass: bool,
    cache: Option<&Arc<DiskCache>>,
    include_attributes: bool,
    scoped_ann: Option<ScopedAnnQuery<'_>>,
) -> Result<SegmentSearchOutput> {
    let segment_ref = located.segment;
    if let Some(scoped_ann) = scoped_ann {
        return scoped_segment_search(
            store,
            located,
            query,
            top_k,
            nprobe,
            filter,
            distance_metric,
            oversample_factor,
            rerank_coalesce_gap_bytes,
            resident_row_bypass,
            cache,
            include_attributes,
            scoped_ann,
        )
        .await;
    }

    // Use manifest metadata to determine index type — no S3 probe needed.
    if segment_ref.hierarchical {
        let mut index =
            HierarchicalIndex::load_from_located_manifest(store, located, cache).await?;
        index.bitmap_fields = segment_ref.bitmap_fields.clone();
        use crate::index::hierarchical::search::search_hierarchical_with_trace;
        let output = search_hierarchical_with_trace(
            &index,
            query,
            top_k,
            nprobe, // beam_width uses nprobe
            filter,
            distance_metric,
            store,
            oversample_factor,
            cache,
            include_attributes,
        )
        .await?;
        return Ok(SegmentSearchOutput {
            clusters_probed: output.probed_centroids.len(),
            results: output.results,
        });
    }

    // Use manifest metadata to skip cluster-count probing and quant detection.
    let mut index = IvfFlatIndex::load_from_located_manifest(store, located, cache).await?;
    index.bitmap_fields = segment_ref.bitmap_fields.clone();
    use crate::index::ivf_flat::search::search_ivf_flat_with_trace;
    let output = search_ivf_flat_with_trace(
        &index,
        coarse_payload_encoding,
        query,
        top_k,
        nprobe,
        filter,
        distance_metric,
        store,
        oversample_factor,
        cache,
        include_attributes,
        rerank_coalesce_gap_bytes,
        resident_row_bypass,
    )
    .await?;

    Ok(SegmentSearchOutput {
        clusters_probed: output.probed_centroids.len(),
        results: output.results,
    })
}

#[allow(clippy::too_many_arguments)]
async fn scoped_segment_search(
    store: &ZeppelinStore,
    located: LocatedSegmentRef<'_>,
    query: &[f32],
    top_k: usize,
    nprobe: usize,
    filter: Option<&Filter>,
    distance_metric: DistanceMetric,
    oversample_factor: usize,
    rerank_coalesce_gap_bytes: usize,
    resident_row_bypass: bool,
    cache: Option<&Arc<DiskCache>>,
    include_attributes: bool,
    scoped_ann: ScopedAnnQuery<'_>,
) -> Result<SegmentSearchOutput> {
    let segment_ref = located.segment;
    let corpus_key = segment_corpus_cache_key(located)?;
    let ann_key = scoped_ann_cache_key(
        located,
        scoped_ann.mandatory_filter,
        scoped_ann.indexing_config,
    )?;
    let index = scoped_ann
        .decoded_artifact_cache
        .get_or_build_scoped_ann(&ann_key, || async {
            ScopedAnnIndex::load_or_build(
                crate::retrieval_scope::ScopedAnnBuildRequest {
                    store,
                    logical_origin: located.logical_origin.as_origin(),
                    source_segment_id: &segment_ref.id,
                    scope_cache_key: &ann_key,
                    mandatory_filter: scoped_ann.mandatory_filter,
                    config: scoped_ann.indexing_config,
                    cache,
                },
                || async {
                    scoped_ann
                        .decoded_artifact_cache
                        .get_or_build_segment_corpus(&corpus_key, || async {
                            materialize_scoped_segment_corpus(store, located, query.len(), cache)
                                .await
                        })
                        .await
                },
            )
            .await
        })
        .await?;
    let result = index
        .search(
            query,
            top_k,
            nprobe,
            filter,
            distance_metric,
            store,
            cache,
            oversample_factor,
            rerank_coalesce_gap_bytes,
            resident_row_bypass,
            include_attributes,
        )
        .await?;
    Ok(SegmentSearchOutput {
        results: result.results,
        clusters_probed: result.clusters_probed,
    })
}

/// Executes one BM25/ranking-expression query against the current manifest.
///
/// The function chooses a consistency-appropriate current snapshot, then runs
/// WAL lexical scoring and active-segment inverted-index search concurrently.
/// Strong merge keeps latest WAL documents and suppresses older segment
/// versions. Eventual merge uses the segment ranking but still honors WAL
/// tombstones. Larger scores rank first.
///
/// ```text
///                    current Manifest
///                    /              \
///                   v                v
/// Strong: WAL BM25 scan       active segment FTS
/// Eventual: tombstones only   global index preferred
///                   \                /
///                    +------ join ---+
///                            |
///             suppress stale/deleted segment IDs
///                            |
///                            v
///                  BM25 score descending
/// ```
///
/// # Parameters
///
/// - `store`: Object-store boundary for manifest and immutable index reads.
/// - `wal_reader`: Decoder for visible uncompacted WAL fragments/tombstones.
/// - `namespace`: Validated namespace name.
/// - `rank_by`: Parsed expression naming BM25 field/query leaves and how their
///   scores combine.
/// - `fts_configs`: Tokenizer and BM25 parameters for every configured field.
///   Normal HTTP callers validate every `rank_by` field before this call.
/// - `top_k`: Maximum relevance-ranked hits to return.
/// - `filter`: Optional metadata predicate; missing attributes do not match.
/// - `consistency`: Whether live uncompacted documents are scored.
/// - `last_as_prefix`: Whether the final query token uses prefix matching.
/// - `manifest_cache`: Optional consistency-aware current-manifest cache.
/// - `fts_cache`: Optional CPU cache for WAL tokenization/statistics inputs.
/// - `cache`: Optional disk cache for immutable WAL and segment objects.
/// - `max_full_scan_clusters`: Circuit-breaker cluster limit when an old segment
///   lacks the global FTS artifact; zero disables this limit.
/// - `max_full_scan_vectors`: Equivalent vector-count circuit breaker; zero
///   disables this limit.
/// - `include_attributes`: Whether winning hits materialize attributes.
///
/// # Returns
///
/// A [`QueryResponse`] with at most `top_k` hits ordered by descending
/// [`RankBy`] score, source scan counts, and no debug/enrichment fields.
///
/// # Errors
///
/// Propagates manifest/WAL/index object reads and decoding failures. Returns an
/// index-unavailable error when the active segment lacks a requested FTS field,
/// or when a missing global FTS index would exceed either compatibility-scan
/// budget. No partial source list is returned.
///
/// # Side Effects
///
/// Reads current authority and immutable artifacts, may populate the manifest,
/// byte, decoded-WAL, WAL-FTS, and decoded-segment caches, evicts compacted
/// fragment entries from the WAL FTS cache, emits
/// tracing, and may emit an operator warning for the old full-scan path.
///
/// # Consistency
///
/// One manifest snapshot governs both futures. Strong reads include latest WAL
/// documents; every live WAL ID suppresses its segment version even if it does
/// not match the text/filter. Eventual reads omit live WAL documents but apply
/// tombstones. Uploaded artifacts absent from the snapshot are invisible.
///
/// # Performance
///
/// WAL and segment I/O overlap. A modern segment uses one global inverted-index
/// object plus only cluster ID/attribute objects for matching positions. An old
/// segment fans out over every cluster and is refused before I/O when configured
/// limits are exceeded. WAL and segment BM25 statistics are computed over their
/// respective corpora; merge compares the resulting scores directly.
///
/// # Examples
///
/// A strong title/content query sees a newly updated WAL document immediately,
/// suppresses its older compacted version, and may rank it with segment hits.
/// If the segment predates the requested `title` FTS field, the query fails with
/// an operator-facing index-unavailable error instead of silently searching
/// only WAL content.
///
/// # Rust Notes for Java/C Engineers
///
/// Most inputs are shared borrows, so the function cannot mutate or retain
/// caller-owned request/config data beyond its future. Optional caches use
/// `Option<&Arc<T>>`: the caller controls whether shared state exists, and Rust
/// proves every reference stays valid across concurrent awaits.
#[allow(clippy::too_many_arguments)]
#[instrument(
    skip(
        store,
        wal_reader,
        rank_by,
        fts_configs,
        filter,
        manifest_cache,
        fts_cache,
        fragment_cache,
        decoded_artifact_cache,
        cache
    ),
    fields(namespace = namespace)
)]
pub async fn execute_bm25_query(
    store: &ZeppelinStore,
    wal_reader: &WalReader,
    namespace: &str,
    rank_by: &RankBy,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    top_k: usize,
    filter: Option<&Filter>,
    consistency: ConsistencyLevel,
    last_as_prefix: bool,
    manifest_cache: Option<&Arc<ManifestCache>>,
    fts_cache: Option<&Arc<WalFtsCache>>,
    fragment_cache: Option<&Arc<WalFragmentCache>>,
    decoded_artifact_cache: Option<&Arc<DecodedArtifactCache>>,
    cache: Option<&Arc<DiskCache>>,
    max_full_scan_clusters: usize,
    max_full_scan_vectors: usize,
    include_attributes: bool,
) -> Result<QueryResponse> {
    let manifest = read_manifest_for_query(store, namespace, consistency, manifest_cache).await?;
    execute_bm25_query_with_manifest_inner(
        store,
        wal_reader,
        namespace,
        rank_by,
        fts_configs,
        top_k,
        filter,
        None,
        consistency,
        last_as_prefix,
        fts_cache,
        fragment_cache,
        decoded_artifact_cache,
        cache,
        max_full_scan_clusters,
        max_full_scan_vectors,
        include_attributes,
        manifest,
        None,
        false,
    )
    .await
}

/// Executes BM25 retrieval against an already-selected manifest snapshot.
///
/// Batch, hybrid, and historical orchestration use this entry point so all
/// sources share one current/as-of decision. Parameters match
/// [`execute_bm25_query`] except that manifest loading is deliberately absent.
///
/// # Parameters
///
/// - `store`: Object-store boundary for immutable artifact reads.
/// - `wal_reader`: Reader for visible WAL refs.
/// - `namespace`: Namespace owning the snapshot artifacts.
/// - `rank_by`: Validated lexical ranking expression.
/// - `fts_configs`: Per-field tokenizer/BM25 configuration.
/// - `top_k`: Maximum returned hits.
/// - `filter`: Effective policy-and-caller candidate predicate.
/// - `mandatory_filter`: Server-owned scope whose rows define BM25 corpus statistics.
/// - `consistency`: WAL inclusion policy applied within the supplied snapshot.
/// - `last_as_prefix`: Whether the final query token is a prefix.
/// - `fts_cache`: Optional shared WAL lexical cache.
/// - `fragment_cache`: Optional decoded immutable WAL memo.
/// - `decoded_artifact_cache`: Optional decoded immutable segment FTS memo.
/// - `cache`: Optional immutable-object disk cache.
/// - `max_full_scan_clusters`: Old-index cluster fan-out limit, or zero.
/// - `max_full_scan_vectors`: Old-index vector-count limit, or zero.
/// - `include_attributes`: Whether winners include attribute maps.
/// - `manifest`: Owned authoritative snapshot selected by the caller.
///
/// # Returns
///
/// Descending-score results and scan counts, without diagnostics.
///
/// # Errors
///
/// Returns the artifact, decode, configuration-availability, and circuit-breaker
/// errors described by [`execute_bm25_query`]. It never refreshes or substitutes
/// the supplied snapshot.
///
/// # Examples
///
/// An `as_of` handler resolves generation 17 and passes it here. Even if current
/// generation 20 exists, lexical source membership remains generation 17.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_bm25_query_with_manifest(
    store: &ZeppelinStore,
    wal_reader: &WalReader,
    namespace: &str,
    rank_by: &RankBy,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    top_k: usize,
    filter: Option<&Filter>,
    mandatory_filter: Option<&Filter>,
    consistency: ConsistencyLevel,
    last_as_prefix: bool,
    fts_cache: Option<&Arc<WalFtsCache>>,
    fragment_cache: Option<&Arc<WalFragmentCache>>,
    decoded_artifact_cache: Option<&Arc<DecodedArtifactCache>>,
    cache: Option<&Arc<DiskCache>>,
    max_full_scan_clusters: usize,
    max_full_scan_vectors: usize,
    include_attributes: bool,
    manifest: Manifest,
    authoritative_origin: Option<ArtifactOrigin>,
) -> Result<QueryResponse> {
    execute_bm25_query_with_manifest_inner(
        store,
        wal_reader,
        namespace,
        rank_by,
        fts_configs,
        top_k,
        filter,
        mandatory_filter,
        consistency,
        last_as_prefix,
        fts_cache,
        fragment_cache,
        decoded_artifact_cache,
        cache,
        max_full_scan_clusters,
        max_full_scan_vectors,
        include_attributes,
        manifest,
        authoritative_origin,
        false,
    )
    .await
}

/// Executes supplied-snapshot BM25 retrieval with scoped cache diagnostics.
///
/// # Parameters
///
/// Parameters have the same meaning and preconditions as
/// `execute_bm25_query_with_manifest`; the function always enables debug
/// measurement for this call.
///
/// # Returns
///
/// The normal relevance-ranked response with phase timings, cache counters,
/// effective consistency, cluster work, and underfill reason populated.
///
/// # Errors
///
/// Returns the same fail-loud errors as [`execute_bm25_query`]. Debug collection
/// does not permit partial results.
///
/// # Side Effects
///
/// Installs task-scoped cache counters in addition to normal immutable reads and
/// WAL FTS cache maintenance.
///
/// # Examples
///
/// A global-index query can report `clusters_probed = 0` and a disk-cache hit;
/// the same query on an old compatibility index reports every scanned cluster.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_bm25_query_with_manifest_debug(
    store: &ZeppelinStore,
    wal_reader: &WalReader,
    namespace: &str,
    rank_by: &RankBy,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    top_k: usize,
    filter: Option<&Filter>,
    mandatory_filter: Option<&Filter>,
    consistency: ConsistencyLevel,
    last_as_prefix: bool,
    fts_cache: Option<&Arc<WalFtsCache>>,
    fragment_cache: Option<&Arc<WalFragmentCache>>,
    decoded_artifact_cache: Option<&Arc<DecodedArtifactCache>>,
    cache: Option<&Arc<DiskCache>>,
    max_full_scan_clusters: usize,
    max_full_scan_vectors: usize,
    include_attributes: bool,
    manifest: Manifest,
    authoritative_origin: Option<ArtifactOrigin>,
) -> Result<QueryResponse> {
    execute_bm25_query_with_manifest_inner(
        store,
        wal_reader,
        namespace,
        rank_by,
        fts_configs,
        top_k,
        filter,
        mandatory_filter,
        consistency,
        last_as_prefix,
        fts_cache,
        fragment_cache,
        decoded_artifact_cache,
        cache,
        max_full_scan_clusters,
        max_full_scan_vectors,
        include_attributes,
        manifest,
        authoritative_origin,
        true,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
/// Selects normal or diagnostic BM25 execution for a supplied snapshot.
///
/// # Parameters
///
/// - All retrieval parameters have the same contracts as
///   `execute_bm25_query_with_manifest`.
/// - `emit_debug`: Whether to allocate/install scoped cache diagnostics.
///
/// # Returns
///
/// The complete BM25 response with debug data present only when requested.
///
/// # Errors
///
/// Propagates every scoped-execution failure.
///
/// # Rust Notes for Java/C Engineers
///
/// The debug branch clones only an `Arc` handle to shared counters, then moves
/// borrowed references and the owned manifest into one future. RAII destroys the
/// diagnostic scope and counter handles on both success and error.
async fn execute_bm25_query_with_manifest_inner(
    store: &ZeppelinStore,
    wal_reader: &WalReader,
    namespace: &str,
    rank_by: &RankBy,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    top_k: usize,
    filter: Option<&Filter>,
    mandatory_filter: Option<&Filter>,
    consistency: ConsistencyLevel,
    last_as_prefix: bool,
    fts_cache: Option<&Arc<WalFtsCache>>,
    fragment_cache: Option<&Arc<WalFragmentCache>>,
    decoded_artifact_cache: Option<&Arc<DecodedArtifactCache>>,
    cache: Option<&Arc<DiskCache>>,
    max_full_scan_clusters: usize,
    max_full_scan_vectors: usize,
    include_attributes: bool,
    manifest: Manifest,
    authoritative_origin: Option<ArtifactOrigin>,
    emit_debug: bool,
) -> Result<QueryResponse> {
    if emit_debug {
        let diagnostics = Arc::new(CacheDiagnostics::default());
        return with_cache_diagnostics(Arc::clone(&diagnostics), async move {
            execute_bm25_query_with_manifest_scoped(
                store,
                wal_reader,
                namespace,
                rank_by,
                fts_configs,
                top_k,
                filter,
                mandatory_filter,
                consistency,
                last_as_prefix,
                fts_cache,
                fragment_cache,
                decoded_artifact_cache,
                cache,
                max_full_scan_clusters,
                max_full_scan_vectors,
                include_attributes,
                manifest,
                authoritative_origin,
                Some(diagnostics),
            )
            .await
        })
        .await;
    }
    execute_bm25_query_with_manifest_scoped(
        store,
        wal_reader,
        namespace,
        rank_by,
        fts_configs,
        top_k,
        filter,
        mandatory_filter,
        consistency,
        last_as_prefix,
        fts_cache,
        fragment_cache,
        decoded_artifact_cache,
        cache,
        max_full_scan_clusters,
        max_full_scan_vectors,
        include_attributes,
        manifest,
        authoritative_origin,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
/// Runs WAL and segment BM25 phases against one manifest and merges their scores.
///
/// The function also keeps the derived WAL FTS cache bounded by removing entries
/// for fragments no longer uncompacted in this snapshot. A global segment index
/// is preferred; an older per-cluster layout is subject to explicit fan-out
/// budgets before any compatibility-scan I/O.
///
/// # Parameters
///
/// Retrieval arguments match `execute_bm25_query_with_manifest`.
/// `cache_diagnostics` optionally owns the counters installed by the wrapper.
///
/// # Returns
///
/// At most `top_k` descending-score hits, scored fragment/segment counts, and
/// optional diagnostics. Attributes are stripped after merge when not requested.
///
/// # Errors
///
/// Either concurrent branch can fail for storage, decode, missing FTS field, or
/// compatibility budget reasons. Both futures are polled to completion by
/// `tokio::join!`, but no partial response is returned. Cache warming or WAL FTS
/// eviction may already have occurred.
///
/// # Side Effects
///
/// Evicts obsolete entries from [`WalFtsCache`], reads immutable objects, may
/// populate caches, records diagnostics, and emits tracing/warnings.
///
/// # Consistency
///
/// Strong WAL replay yields both candidates and complete live override/delete
/// sets. Eventual mode reads only deletes. Both branches use one manifest; an
/// active segment is resolved from that value, never from object listing. An
/// active ID with no matching retained descriptor fails as malformed
/// authoritative state.
///
/// # Performance
///
/// WAL and segment futures overlap. When uncompacted fragments exist, segment
/// search requests an unbounded intermediate frontier (`usize::MAX`) so later
/// suppression cannot underfill BM25 results; final merge bounds it to `top_k`.
/// The global index avoids scanning all clusters. A decoded-artifact cache hit
/// also avoids reparsing the immutable global or legacy per-cluster FTS bytes.
///
/// # Examples
///
/// Segment document `p7` scores 9, but strong WAL updates `p7` to text scoring
/// zero. `p7` remains in `wal_overriding_ids`, so the stale score-9 segment hit
/// is removed even though no WAL replacement appears in the response.
///
/// # Rust Notes for Java/C Engineers
///
/// The two async blocks capture shared immutable borrows and the same owned
/// manifest by reference. `tokio::join!` provides structured concurrency: work
/// cannot outlive this call, and `?` propagates typed branch errors after local
/// values are cleaned up.
async fn execute_bm25_query_with_manifest_scoped(
    store: &ZeppelinStore,
    wal_reader: &WalReader,
    _namespace: &str,
    rank_by: &RankBy,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    top_k: usize,
    filter: Option<&Filter>,
    mandatory_filter: Option<&Filter>,
    consistency: ConsistencyLevel,
    last_as_prefix: bool,
    fts_cache: Option<&Arc<WalFtsCache>>,
    fragment_cache: Option<&Arc<WalFragmentCache>>,
    decoded_artifact_cache: Option<&Arc<DecodedArtifactCache>>,
    cache: Option<&Arc<DiskCache>>,
    max_full_scan_clusters: usize,
    max_full_scan_vectors: usize,
    include_attributes: bool,
    manifest: Manifest,
    authoritative_origin: Option<ArtifactOrigin>,
    cache_diagnostics: Option<Arc<CacheDiagnostics>>,
) -> Result<QueryResponse> {
    let local_origin = match authoritative_origin {
        Some(origin) => origin,
        None => manifest.local_origin()?,
    };
    let origin_resolver = manifest.artifact_origin_resolver(&local_origin)?;
    let located_fragments = origin_resolver.uncompacted_located_fragments()?;
    let active_segment = origin_resolver.active_located_segment()?;
    let eventual_skipped_wal =
        consistency == ConsistencyLevel::Eventual && !manifest.uncompacted_fragments().is_empty();

    let active_identities = located_fragments
        .iter()
        .copied()
        .map(LocatedFragmentRef::identity)
        .collect::<Vec<_>>();

    // Evict compacted fragments from the derived caches to bound local memory.
    if let Some(cache) = fts_cache {
        cache.evict_compacted_located(&local_origin, &active_identities);
    }
    if let Some(cache) = fragment_cache {
        cache.evict_compacted_located(&local_origin, &active_identities);
    }

    // A row-scoped query must resolve the complete WAL visibility state before
    // segment corpus statistics are computed. In particular, a latest WAL
    // upsert that moves an ID outside the filter still suppresses the stale
    // segment row before document counts and frequencies are derived. Keep the
    // historical concurrent path byte-for-byte for unfiltered queries.
    if let Some(mandatory_filter) = mandatory_filter {
        let candidate_filter = filter.ok_or_else(|| {
            crate::error::ZeppelinError::Index(
                "policy-scoped BM25 execution is missing its compiled effective filter".into(),
            )
        })?;
        return execute_filtered_bm25_query_with_manifest(
            store,
            wal_reader,
            &local_origin,
            rank_by,
            fts_configs,
            top_k,
            mandatory_filter,
            candidate_filter,
            consistency,
            last_as_prefix,
            fragment_cache,
            decoded_artifact_cache,
            cache,
            include_attributes,
            &manifest,
            &located_fragments,
            active_segment,
            cache_diagnostics,
            eventual_skipped_wal,
        )
        .await;
    }

    // WAL BM25 work and segment BM25 search are independent — run them
    // concurrently over the same manifest snapshot. Strong scans and scores
    // WAL docs; Eventual reads only WAL tombstones.
    let wal_future = async {
        // Short-circuit: skip WAL scan if no uncompacted fragments exist.
        let wal_start = std::time::Instant::now();
        let mut scanned_fragments = 0;
        let mut wal_deleted_ids = std::collections::HashSet::new();
        let mut wal_overriding_ids = std::collections::HashSet::new();
        let wal_results = match consistency {
            ConsistencyLevel::Strong if !manifest.uncompacted_fragments().is_empty() => {
                // The historical `unchecked` name is compatibility-only;
                // misses still validate checksums before memo insertion.
                let fragments = wal_reader
                    .read_located_query_fragments_unchecked(
                        &located_fragments,
                        cache.map_or(FragmentCachePolicy::Bypass, FragmentCachePolicy::ReadWrite),
                        fragment_cache,
                    )
                    .await?;
                let scan_result = wal_bm25_scan(
                    &fragments,
                    &local_origin,
                    rank_by,
                    fts_configs,
                    last_as_prefix,
                    fts_cache.map(|c| c.as_ref()),
                    filter,
                    include_attributes,
                    Some(top_k),
                );
                scanned_fragments = scan_result.fragment_count;
                wal_deleted_ids = scan_result.deleted_ids;
                wal_overriding_ids = scan_result.overriding_ids;
                scan_result.results
            }
            ConsistencyLevel::Eventual if !manifest.uncompacted_fragments().is_empty() => {
                wal_deleted_ids = wal_reader
                    .read_located_delete_ids_unchecked(
                        &located_fragments,
                        cache.map_or(FragmentCachePolicy::Bypass, FragmentCachePolicy::ReadWrite),
                        fragment_cache,
                    )
                    .await?;
                Vec::new()
            }
            _ => Vec::new(),
        };
        let wal_ms = wal_start.elapsed().as_millis() as u64;
        debug!(
            wal_duration_ms = wal_ms,
            fragments_scanned = scanned_fragments,
            "BM25 query phase: WAL scan"
        );
        Ok::<_, crate::error::ZeppelinError>((
            wal_results,
            scanned_fragments,
            wal_deleted_ids,
            wal_overriding_ids,
            wal_ms,
        ))
    };

    let segment_future = async {
        let segment_start = std::time::Instant::now();
        let (results, scanned, clusters_probed) = match active_segment {
            Some(seg_ref) => {
                let segment_top_k = if manifest.uncompacted_fragments().is_empty() {
                    top_k
                } else {
                    usize::MAX
                };
                let output = segment_bm25_search(
                    store,
                    seg_ref,
                    rank_by,
                    fts_configs,
                    filter,
                    last_as_prefix,
                    decoded_artifact_cache,
                    cache,
                    max_full_scan_clusters,
                    max_full_scan_vectors,
                    segment_top_k,
                    include_attributes,
                )
                .await?;
                (output.results, 1, output.clusters_probed)
            }
            _ => (Vec::new(), 0, 0),
        };
        let segment_ms = segment_start.elapsed().as_millis() as u64;
        debug!(
            segment_duration_ms = segment_ms,
            segments_scanned = scanned,
            "BM25 query phase: segment search"
        );
        Ok::<_, crate::error::ZeppelinError>((results, scanned, segment_ms, clusters_probed))
    };

    let (wal_result, segment_result) = tokio::join!(wal_future, segment_future);
    let (wal_results, scanned_fragments, wal_deleted_ids, wal_overriding_ids, wal_ms) = wal_result?;
    let (segment_results, scanned_segments, segment_ms, clusters_probed) = segment_result?;

    // Merge results — BM25 is higher-is-better
    // Pass deleted IDs so segment results for deleted docs are excluded
    let merge_start = std::time::Instant::now();
    let mut results = merge_bm25_results(
        wal_results,
        &wal_overriding_ids,
        segment_results,
        top_k,
        consistency,
        &wal_deleted_ids,
    );
    if !include_attributes {
        for result in &mut results {
            result.attributes = None;
        }
    }
    let merge_ms = merge_start.elapsed().as_millis() as u64;
    let debug = cache_diagnostics.map(|diagnostics| {
        let cache_snapshot = diagnostics.snapshot();
        QueryDebug {
            wal_ms,
            segment_ms,
            merge_ms,
            fragments_scanned: scanned_fragments,
            segments_scanned: scanned_segments,
            clusters_probed,
            cache: QueryDebugCache {
                hits: cache_snapshot.hits,
                misses: cache_snapshot.misses,
            },
            consistency_effective: consistency,
            underfill_reason: query_underfill_reason(
                results.len(),
                top_k,
                consistency,
                eventual_skipped_wal,
                scanned_fragments,
                scanned_segments,
            ),
        }
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

/// Executes BM25 through a snapshot-bound policy-corpus artifact.
///
/// A bounded decoded-artifact cache retains the derived index by manifest,
/// consistency mode, mandatory filter, and FTS configuration. Cache misses
/// rebuild from the exact manifest-selected segment/WAL snapshot; caller
/// filters narrow scored candidates but never alter policy-corpus statistics.
#[allow(clippy::too_many_arguments)]
async fn execute_filtered_bm25_query_with_manifest(
    store: &ZeppelinStore,
    wal_reader: &WalReader,
    logical_origin: &ArtifactOrigin,
    rank_by: &RankBy,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    top_k: usize,
    mandatory_filter: &Filter,
    candidate_filter: &Filter,
    consistency: ConsistencyLevel,
    last_as_prefix: bool,
    fragment_cache: Option<&Arc<WalFragmentCache>>,
    decoded_artifact_cache: Option<&Arc<DecodedArtifactCache>>,
    cache: Option<&Arc<DiskCache>>,
    include_attributes: bool,
    manifest: &Manifest,
    located_fragments: &[LocatedFragmentRef<'_>],
    segment_ref: Option<LocatedSegmentRef<'_>>,
    cache_diagnostics: Option<Arc<CacheDiagnostics>>,
    eventual_skipped_wal: bool,
) -> Result<QueryResponse> {
    let scanned_segments = usize::from(segment_ref.is_some());
    let clusters_probed = segment_ref.map_or(0, |segment| segment.segment.cluster_count);
    let scanned_fragments = if consistency == ConsistencyLevel::Strong {
        manifest.uncompacted_fragments().len()
    } else {
        0
    };
    let artifact_start = std::time::Instant::now();
    let artifact_key = scoped_fts_cache_key(
        logical_origin,
        manifest,
        consistency,
        mandatory_filter,
        fts_configs,
    )?;
    let durable_source_segment_id = manifest
        .uncompacted_fragments()
        .is_empty()
        .then(|| segment_ref.map(|segment| segment.segment.id.as_str()))
        .flatten();
    let index = match decoded_artifact_cache {
        Some(decoded_artifact_cache) => {
            decoded_artifact_cache
                .get_or_build_scoped_fts(&artifact_key, || async {
                    ScopedFtsIndex::load_or_build(
                        store,
                        logical_origin.namespace.as_str(),
                        durable_source_segment_id,
                        &artifact_key,
                        cache,
                        || async {
                            build_scoped_fts_snapshot(
                                store,
                                wal_reader,
                                fts_configs,
                                mandatory_filter,
                                consistency,
                                fragment_cache,
                                Some(decoded_artifact_cache),
                                cache,
                                located_fragments,
                                segment_ref,
                            )
                            .await
                        },
                    )
                    .await
                })
                .await?
        }
        None => Arc::new(
            ScopedFtsIndex::load_or_build(
                store,
                logical_origin.namespace.as_str(),
                durable_source_segment_id,
                &artifact_key,
                cache,
                || async {
                    build_scoped_fts_snapshot(
                        store,
                        wal_reader,
                        fts_configs,
                        mandatory_filter,
                        consistency,
                        fragment_cache,
                        None,
                        cache,
                        located_fragments,
                        segment_ref,
                    )
                    .await
                },
            )
            .await?,
        ),
    };
    let segment_ms = artifact_start.elapsed().as_millis() as u64;
    let wal_ms = 0;
    let merge_start = std::time::Instant::now();
    let results = index.search(
        rank_by,
        fts_configs,
        candidate_filter,
        last_as_prefix,
        top_k,
        include_attributes,
    )?;
    let merge_ms = merge_start.elapsed().as_millis() as u64;
    let debug = cache_diagnostics.map(|diagnostics| {
        let cache_snapshot = diagnostics.snapshot();
        QueryDebug {
            wal_ms,
            segment_ms,
            merge_ms,
            fragments_scanned: scanned_fragments,
            segments_scanned: scanned_segments,
            clusters_probed,
            cache: QueryDebugCache {
                hits: cache_snapshot.hits,
                misses: cache_snapshot.misses,
            },
            consistency_effective: consistency,
            underfill_reason: query_underfill_reason(
                results.len(),
                top_k,
                consistency,
                eventual_skipped_wal,
                scanned_fragments,
                scanned_segments,
            ),
        }
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

#[allow(clippy::too_many_arguments)]
async fn build_scoped_fts_snapshot(
    store: &ZeppelinStore,
    wal_reader: &WalReader,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    mandatory_filter: &Filter,
    consistency: ConsistencyLevel,
    fragment_cache: Option<&Arc<WalFragmentCache>>,
    decoded_artifact_cache: Option<&Arc<DecodedArtifactCache>>,
    cache: Option<&Arc<DiskCache>>,
    located_fragments: &[LocatedFragmentRef<'_>],
    segment_ref: Option<LocatedSegmentRef<'_>>,
) -> Result<ScopedFtsIndex> {
    let mut dimensions = 0;
    let base_corpus = if let Some(segment_ref) = segment_ref {
        let corpus = match decoded_artifact_cache {
            Some(decoded_artifact_cache) => {
                let corpus_key = segment_corpus_cache_key(segment_ref)?;
                decoded_artifact_cache
                    .get_or_build_segment_corpus(&corpus_key, || async {
                        materialize_scoped_segment_corpus(store, segment_ref, 0, cache).await
                    })
                    .await?
            }
            None => {
                Arc::new(materialize_scoped_segment_corpus(store, segment_ref, 0, cache).await?)
            }
        };
        dimensions = corpus.dimensions();
        Some(corpus)
    } else {
        None
    };

    let mut strong_fragments = Vec::new();
    let mut eventual_deleted_ids = HashSet::new();
    match consistency {
        ConsistencyLevel::Strong if !located_fragments.is_empty() => {
            strong_fragments = wal_reader
                .read_located_query_fragments_unchecked(
                    located_fragments,
                    cache.map_or(FragmentCachePolicy::Bypass, FragmentCachePolicy::ReadWrite),
                    fragment_cache,
                )
                .await?;
        }
        ConsistencyLevel::Eventual if !located_fragments.is_empty() => {
            eventual_deleted_ids = wal_reader
                .read_located_delete_ids_unchecked(
                    located_fragments,
                    cache.map_or(FragmentCachePolicy::Bypass, FragmentCachePolicy::ReadWrite),
                    fragment_cache,
                )
                .await?;
        }
        _ => {}
    }

    let mandatory_filter = mandatory_filter.clone();
    let fts_configs = fts_configs.clone();
    tokio::task::spawn_blocking(move || {
        let base_len = base_corpus.as_ref().map_or(0, |corpus| corpus.rows().len());
        let mut logical_rows: HashMap<String, crate::types::VectorEntry> =
            HashMap::with_capacity(base_len);
        if let Some(corpus) = base_corpus {
            logical_rows.extend(
                corpus
                    .rows()
                    .iter()
                    .cloned()
                    .map(|row| (row.id.clone(), row)),
            );
        }
        for fragment in strong_fragments {
            for deleted_id in &fragment.deletes {
                logical_rows.remove(deleted_id);
            }
            for vector in &fragment.vectors {
                logical_rows.insert(vector.id.clone(), vector.clone());
            }
        }
        for deleted_id in eventual_deleted_ids {
            logical_rows.remove(&deleted_id);
        }
        let corpus = ScopedSegmentCorpus::new(logical_rows.into_values().collect(), dimensions)?;
        Ok(ScopedFtsIndex::build(
            &corpus,
            &mandatory_filter,
            &fts_configs,
        ))
    })
    .await
    .map_err(|error| {
        crate::error::ZeppelinError::from(crate::retrieval_scope::RetrievalScopeError::Worker(
            error.to_string(),
        ))
    })?
}

/// Fetches one immutable query artifact through the optional disk cache.
///
/// # Parameters
///
/// - `cache`: Disposable cache shared by query operations, or `None` for a
///   direct object-store read.
/// - `store`: Authoritative backing object store.
/// - `key`: Exact immutable artifact key copied/derived from manifest metadata.
///
/// # Returns
///
/// Shared [`bytes::Bytes`] containing the complete object body.
///
/// # Errors
///
/// Propagates cache coordination and object-store GET failures. Missing objects
/// are errors; the helper never substitutes empty bytes or another key.
///
/// # Side Effects
///
/// A cache miss performs one GET and can persist the returned immutable bytes.
///
/// # Consistency
///
/// Cache use is safe only because artifact keys are write-once. Visibility still
/// comes from the manifest; this helper does not list or discover objects.
///
/// # Performance
///
/// A hit avoids S3/MinIO. Cache miss coalescing is delegated to [`DiskCache`].
///
/// # Examples
///
/// Fetching `.../global_fts.bin` returns cached bytes when present; otherwise it
/// downloads that exact object and makes it available to later queries.
/// Fetch an immutable object through an incarnation-qualified cache identity.
async fn fetch_located_query_object(
    cache: Option<&Arc<DiskCache>>,
    store: &ZeppelinStore,
    located: LocatedSegmentRef<'_>,
    store_key: &str,
) -> Result<bytes::Bytes> {
    if let Some(cache) = cache {
        let cache_key = located.cache_key(store_key);
        cache
            .get_or_fetch(&cache_key, || store.get(store_key))
            .await
    } else {
        store.get(store_key).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Bounds the expensive per-cluster BM25 compatibility path.
///
/// A value of zero disables the corresponding bound. The type is `Copy` because
/// it contains only two machine-size counters and carries no ownership.
struct Bm25FullScanBudget {
    /// Maximum segment cluster count accepted for a full scan, or zero.
    max_clusters: usize,
    /// Maximum segment vector count accepted for a full scan, or zero.
    max_vectors: usize,
}

/// Builds the operator-facing error for an unusable segment FTS layout.
///
/// # Parameters
///
/// - `namespace`: Namespace requiring index maintenance.
/// - `reason`: Specific missing capability or exceeded budget.
///
/// # Returns
///
/// [`crate::error::ZeppelinError::IndexUnavailable`] that distinguishes an
/// operator/compaction condition from invalid client syntax.
///
/// # Examples
///
/// A segment missing its global FTS index can report that scanning 600 clusters
/// exceeds limit 500 and instruct operators to rebuild through compaction.
fn bm25_index_unavailable(namespace: &str, reason: impl AsRef<str>) -> crate::error::ZeppelinError {
    crate::error::ZeppelinError::IndexUnavailable(format!(
        "BM25 FTS index unavailable for namespace {namespace}: {}. \
         Build the namespace FTS index with compaction; this is a server/operator condition, \
         not a request validation error.",
        reason.as_ref()
    ))
}

/// Verifies that the active segment was built for every requested lexical field.
///
/// # Parameters
///
/// - `namespace`: Namespace included in an operator-facing error.
/// - `segment_ref`: Manifest descriptor advertising persisted FTS fields.
/// - `rank_by`: Ranking expression whose BM25 leaves name required fields.
///
/// # Returns
///
/// `Ok(())` when every requested field is advertised, including when the
/// expression has no leaves. Otherwise returns an index-unavailable error that
/// lists missing fields or identifies a pre-FTS segment.
///
/// # Errors
///
/// Returns only the availability error described above. It performs no I/O, so
/// no partial search has started.
///
/// # Consistency
///
/// Capability comes from the active descriptor selected by the manifest. The
/// function does not probe S3 and does not silently search only fields that
/// happen to exist.
///
/// # Examples
///
/// A `RankBy` expression requiring `title` and `body` fails before reads if the
/// segment advertises only `body`.
fn ensure_segment_fts_fields_available(
    namespace: &str,
    segment_ref: &SegmentRef,
    rank_by: &RankBy,
) -> Result<()> {
    let field_queries = rank_by.extract_field_queries();
    let missing_fields: Vec<String> = field_queries
        .iter()
        .map(|(field, _)| field)
        .filter(|field| !segment_ref.fts_fields.contains(*field))
        .cloned()
        .collect();
    if missing_fields.is_empty() {
        return Ok(());
    }

    let reason = if segment_ref.fts_fields.is_empty() {
        "active segment has no FTS fields; it predates FTS index construction".to_string()
    } else {
        format!(
            "active segment is missing FTS fields [{}]",
            missing_fields.join(", ")
        )
    };
    Err(bm25_index_unavailable(namespace, reason))
}

/// Rejects an old segment whose BM25 fallback would exceed configured work.
///
/// Cluster count is checked first, then vector count. Each zero limit is
/// disabled independently.
///
/// # Parameters
///
/// - `namespace`: Namespace included in the operator-facing error.
/// - `segment_ref`: Descriptor whose declared size predicts fallback cost.
/// - `budget`: Maximum accepted cluster and vector counts.
///
/// # Returns
///
/// `Ok(())` when both enabled limits admit the scan.
///
/// # Errors
///
/// Returns index-unavailable before artifact I/O when either declared count is
/// over its enabled limit.
///
/// # Performance
///
/// Constant-time manifest metadata checks designed specifically to avoid an
/// accidental object-store fan-out.
///
/// # Examples
///
/// A 20,001-vector legacy segment is rejected by a 20,000-vector budget even if
/// its four clusters fit the cluster budget. Setting both limits to zero admits
/// the compatibility scan.
fn validate_bm25_full_scan_budget(
    namespace: &str,
    segment_ref: &SegmentRef,
    budget: Bm25FullScanBudget,
) -> Result<()> {
    if budget.max_clusters > 0 && segment_ref.cluster_count > budget.max_clusters {
        return Err(bm25_index_unavailable(
            namespace,
            format!(
                "global FTS index is missing and fallback would scan {} clusters (limit {})",
                segment_ref.cluster_count, budget.max_clusters
            ),
        ));
    }
    if budget.max_vectors > 0 && segment_ref.vector_count > budget.max_vectors {
        return Err(bm25_index_unavailable(
            namespace,
            format!(
                "global FTS index is missing and fallback would scan {} vectors (limit {})",
                segment_ref.vector_count, budget.max_vectors
            ),
        ));
    }
    Ok(())
}

/// Selects the safe BM25 search path for one active immutable segment.
///
/// The descriptor must advertise every requested field. A modern segment uses
/// its global inverted index. An older segment may use the per-cluster
/// compatibility path only when both configured work budgets allow it.
///
/// ```text
/// SegmentRef + RankBy
///          |
///          v
/// all requested FTS fields present? -- no --> IndexUnavailable
///          |
///         yes
///          v
/// has_global_fts? -- yes --> one global index + matching cluster data
///          |
///         no
///          v
/// within cluster/vector budgets? -- no --> IndexUnavailable
///          |
///         yes
///          v
/// warn + scan every per-cluster index
/// ```
///
/// # Parameters
///
/// - `store`: Object-store boundary for immutable index/cluster reads.
/// - `namespace`: Namespace owning the logical segment.
/// - `segment_ref`: Active descriptor from the selected manifest.
/// - `rank_by`: Parsed BM25 expression and field/query leaves.
/// - `fts_configs`: Tokenization and scoring configuration by field.
/// - `filter`: Optional post-score metadata predicate.
/// - `last_as_prefix`: Whether final query tokens use prefix lookup.
/// - `cache`: Optional immutable-object disk cache.
/// - `max_full_scan_clusters`: Old-layout cluster limit, or zero to disable.
/// - `max_full_scan_vectors`: Old-layout vector limit, or zero to disable.
/// - `top_k`: Maximum segment candidates; `usize::MAX` requests all for a
///   later WAL-suppression merge.
/// - `include_attributes`: Whether winning hits retain attributes.
///
/// # Returns
///
/// Segment-local hits ordered by descending final `RankBy` score.
///
/// # Errors
///
/// Returns index-unavailable for missing fields or an over-budget old layout.
/// Otherwise propagates storage, cache, and decode/search errors. No alternate
/// field or partial cluster set is used.
///
/// # Side Effects
///
/// Reads immutable artifacts, may populate cache entries, and warns when the
/// compatibility path is used.
///
/// # Consistency
///
/// Layout and capability decisions come only from the manifest descriptor.
/// This function neither discovers nor publishes segment artifacts.
///
/// # Performance
///
/// Modern global lookup avoids an all-cluster inverted-index fan-out. The
/// compatibility path can issue reads for FTS, cluster IDs, and optional attrs
/// for every cluster, which is why it is budgeted.
///
/// # Examples
///
/// A 400-cluster old segment is admitted by limit 500 and attempts full scan;
/// a 600-cluster segment fails before any index GET. A modern descriptor routes
/// directly to the global artifact regardless of fallback limits.
#[derive(Debug)]
struct Bm25SearchOutput {
    results: Vec<SearchResult>,
    clusters_probed: usize,
}

#[allow(clippy::too_many_arguments)]
async fn segment_bm25_search(
    store: &ZeppelinStore,
    located: LocatedSegmentRef<'_>,
    rank_by: &RankBy,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    filter: Option<&Filter>,
    last_as_prefix: bool,
    decoded_artifact_cache: Option<&Arc<DecodedArtifactCache>>,
    cache: Option<&Arc<DiskCache>>,
    max_full_scan_clusters: usize,
    max_full_scan_vectors: usize,
    top_k: usize,
    include_attributes: bool,
) -> Result<Bm25SearchOutput> {
    let namespace = located.logical_namespace;
    let segment_ref = located.segment;
    ensure_segment_fts_fields_available(namespace, segment_ref, rank_by)?;

    if segment_ref.has_global_fts {
        return segment_bm25_search_global(
            store,
            located,
            rank_by,
            fts_configs,
            filter,
            last_as_prefix,
            decoded_artifact_cache,
            cache,
            top_k,
            include_attributes,
        )
        .await;
    }

    validate_bm25_full_scan_budget(
        namespace,
        segment_ref,
        Bm25FullScanBudget {
            max_clusters: max_full_scan_clusters,
            max_vectors: max_full_scan_vectors,
        },
    )?;

    tracing::warn!(
        namespace = namespace,
        segment_id = %segment_ref.id,
        cluster_count = segment_ref.cluster_count,
        "BM25 falling back to full cluster scan — segment missing global FTS index. \
         Recompact with fts_index=true for 10-100x faster BM25 queries."
    );
    segment_bm25_search_full_scan(
        store,
        located,
        rank_by,
        fts_configs,
        filter,
        last_as_prefix,
        decoded_artifact_cache,
        cache,
        top_k,
        include_attributes,
    )
    .await
}

/// Scores a segment through its global inverted index and fetches matching rows.
///
/// The global index maps field terms to `(cluster, position, score)` without
/// reading every cluster. After evaluating the `RankBy` expression, the function
/// fetches IDs and, only when filtering or projection requires them, attributes
/// for clusters containing positive candidates.
///
/// ```text
/// global FTS object (one logical fetch)
///          |
/// field query tokenization + postings search
///          |
/// (cluster, position) -> per-field scores -> RankBy
///          |
/// only needed clusters: IDs + optional attrs
///          |
/// exact filter -> deduplicate by ID -> BM25 top-k
/// ```
///
/// # Parameters
///
/// - `store`: Object-store boundary for global and cluster artifacts.
/// - `namespace`: Namespace key prefix.
/// - `segment_ref`: Active descriptor with `has_global_fts = true`.
/// - `rank_by`: Expression combining per-field BM25 scores.
/// - `fts_configs`: Per-field analyzer and BM25 parameters.
/// - `filter`: Optional attribute predicate applied after row materialization.
/// - `last_as_prefix`: Selects exact-token or last-token-prefix postings search.
/// - `cache`: Optional immutable-object disk cache.
/// - `top_k`: Maximum descending-score hits retained.
/// - `include_attributes`: Whether response hits retain loaded attributes.
///
/// # Returns
///
/// Deduplicated segment hits ordered by score descending, then ID ascending.
/// A missing field config is skipped; normal HTTP callers validate configs.
///
/// # Errors
///
/// Propagates global-index/cluster/attribute fetches, format decoding, packed
/// object validation, and missing expected attribute data. Invalid positions
/// or unresolvable cluster entries are skipped rather than indexed out of bounds.
///
/// # Side Effects
///
/// Reads and may cache immutable objects. No manifest or index bytes are changed.
///
/// # Consistency
///
/// Cluster ownership and packed-object keys come from the supplied descriptor,
/// including references carried forward from older incremental compactions.
///
/// # Performance
///
/// Reads one global index object, then fans out only to clusters with lexical
/// matches. Attribute objects are omitted when neither filtering nor response
/// projection requires them. CPU scales with matched postings and candidates.
///
/// # Examples
///
/// Query `title: rust` may produce positions in clusters 2 and 9. The function
/// fetches row IDs for only those clusters, loads attrs only if requested, drops
/// filter failures, and retains the highest-scoring `top_k` IDs.
///
/// # Rust Notes for Java/C Engineers
///
/// Maps own intermediate keys and scores, while borrowed request/config data is
/// never copied wholesale. `entry(...).or_default()` performs typed in-place
/// aggregation without nullable map values. Iterator collection consumes owned
/// entries into final results, making transfer of ownership explicit.
#[allow(clippy::too_many_arguments)]
async fn segment_bm25_search_global(
    store: &ZeppelinStore,
    located: LocatedSegmentRef<'_>,
    rank_by: &RankBy,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    filter: Option<&Filter>,
    last_as_prefix: bool,
    decoded_artifact_cache: Option<&Arc<DecodedArtifactCache>>,
    cache: Option<&Arc<DiskCache>>,
    top_k: usize,
    include_attributes: bool,
) -> Result<Bm25SearchOutput> {
    use crate::fts::bm25::Bm25Params;
    use crate::fts::global_index::{global_fts_key, GlobalInvertedIndex};
    use crate::fts::rank_by::evaluate_rank_by;

    let segment_ref = located.segment;
    let segment_id = &segment_ref.id;
    let physical_namespace = located.physical_namespace();

    // Load and decode the global FTS index once per retained immutable key.
    let gkey = global_fts_key(physical_namespace, segment_id);
    let global_cache_key = located.cache_key(&gkey);
    let global_index = match decoded_artifact_cache {
        Some(decoded_cache) => {
            decoded_cache
                .get_or_decode_global_fts(&global_cache_key, || {
                    fetch_located_query_object(cache, store, located, &gkey)
                })
                .await?
        }
        None => Arc::new(GlobalInvertedIndex::from_bytes(
            &fetch_located_query_object(cache, store, located, &gkey).await?,
        )?),
    };

    let field_queries = rank_by.extract_field_queries();

    // Score documents via global index
    let mut position_field_scores: HashMap<(u16, u32), HashMap<String, f32>> = HashMap::new();

    for (field, query) in &field_queries {
        let config = match fts_configs.get(field.as_str()) {
            Some(c) => c,
            None => continue,
        };

        let query_tokens = tokenize_text(query, config, last_as_prefix);
        let params = Bm25Params {
            k1: config.k1,
            b: config.b,
        };

        let results = if last_as_prefix {
            global_index.search_prefix(field, &query_tokens, &params)
        } else {
            global_index.search(field, &query_tokens, &params)
        };

        for (cluster_idx, position, score) in results {
            let entry = position_field_scores
                .entry((cluster_idx, position))
                .or_default();
            *entry.entry(field.to_string()).or_insert(0.0) += score;
        }
    }

    if position_field_scores.is_empty() {
        return Ok(Bm25SearchOutput {
            results: Vec::new(),
            clusters_probed: 0,
        });
    }

    // Identify which clusters we need to fetch attrs from
    let needed_clusters: HashSet<u16> = position_field_scores.keys().map(|(c, _)| *c).collect();

    let load_attrs = filter.is_some() || include_attributes;
    let clusters_probed = needed_clusters.len();
    let cluster_data =
        fetch_bm25_cluster_attrs_and_ids(store, located, &needed_clusters, load_attrs, cache)
            .await?;

    // Collect results
    let mut all_results: HashMap<
        String,
        (f32, Option<HashMap<String, crate::types::AttributeValue>>),
    > = HashMap::new();

    for ((cluster_idx, position), field_scores) in position_field_scores {
        let final_score = evaluate_rank_by(rank_by, &field_scores);
        if final_score <= 0.0 {
            continue;
        }

        let (attrs, cluster) = match cluster_data.get(&cluster_idx) {
            Some(data) => data,
            None => continue,
        };

        let pos = position as usize;
        if pos >= cluster.ids.len() {
            continue;
        }

        let id = cluster.ids[pos].clone();
        let attr = attrs
            .as_ref()
            .and_then(|cluster_attrs| cluster_attrs.get(pos))
            .cloned()
            .flatten();

        // Apply post-filter
        if let Some(f) = filter {
            match &attr {
                Some(a) => {
                    if !evaluate_filter(f, a) {
                        continue;
                    }
                }
                None => continue,
            }
        }

        let response_attr = if include_attributes { attr } else { None };
        let entry = all_results
            .entry(id)
            .or_insert((0.0, response_attr.clone()));
        if final_score > entry.0 {
            entry.0 = final_score;
            entry.1 = response_attr;
        }
    }

    let mut results: Vec<SearchResult> = all_results
        .into_iter()
        .map(|(id, (score, attributes))| SearchResult {
            id,
            score,
            attributes,
        })
        .collect();

    partial_topk_by(&mut results, top_k, bm25_result_cmp);

    Ok(Bm25SearchOutput {
        results,
        clusters_probed,
    })
}

/// Rows needed to prove exact policy-visible membership for a segment scorer.
type Bm25ClusterRows = HashMap<
    u16,
    (
        Option<Vec<Option<HashMap<String, AttributeValue>>>>,
        crate::index::ivf_flat::build::ClusterData,
    ),
>;

/// Converts the manifest's complete cluster range into global-index addresses.
fn all_bm25_cluster_addresses(segment_ref: &SegmentRef) -> Result<HashSet<u16>> {
    (0..segment_ref.cluster_count)
        .map(|cluster_idx| {
            u16::try_from(cluster_idx).map_err(|_| {
                crate::error::ZeppelinError::Index(format!(
                    "BM25 cluster index does not fit global address space: {cluster_idx}"
                ))
            })
        })
        .collect()
}

#[allow(clippy::type_complexity)]
/// Loads row IDs and optional attributes for the clusters needed by global BM25.
///
/// The helper understands both legacy one-object-per-cluster layout and newer
/// packed cluster objects. For packed layout it groups requested clusters by
/// object key, fetches each relevant object once, and extracts individual
/// cluster sections. Attribute artifacts remain per cluster and are fetched only
/// when filtering or response projection requires them.
///
/// ```text
/// needed cluster set + SegmentRef layout
///            |
///            +-- no cluster_objects --> parallel cluster.bin (+ attrs.bin)
///            |
///            +-- packed refs --------> group clusters by object key
///                                         |          |
///                                   fetch once    attrs if needed
///                                         \          /
///                                          decode sections
///                                                |
///                              cluster -> (optional attrs, row IDs)
/// ```
///
/// # Parameters
///
/// - `store`: Object-store boundary for immutable cluster artifacts.
/// - `namespace`: Namespace used for legacy/per-cluster attribute keys.
/// - `segment_ref`: Descriptor supplying owners and packed-object membership.
/// - `needed_clusters`: Cluster indexes referenced by positive global postings.
/// - `load_attrs`: Whether attribute rows are required.
/// - `cache`: Optional immutable-object disk cache.
///
/// # Returns
///
/// A map from cluster index to `(attributes, row IDs)`. Attributes are `None`
/// when `load_attrs` is false; when loaded, the vector preserves row-position
/// alignment and each row may itself have no attributes.
///
/// # Errors
///
/// Propagates object/cache reads and cluster/attribute decoding. Packed layout
/// also errors when requested attrs are missing, a multi-cluster reference
/// points at a legacy single-cluster body, or section metadata is malformed.
/// Bytes fetched before another concurrent result fails may remain cached.
///
/// # Side Effects
///
/// Performs parallel immutable-object reads and may populate cache entries.
///
/// # Consistency
///
/// `cluster_owner` and `cluster_objects` come from the selected manifest and can
/// route carried-forward clusters to older immutable segment keys. The helper
/// never searches object listings for alternatives.
///
/// # Performance
///
/// `join_all` overlaps all requested reads. Legacy layout costs one cluster GET
/// and optionally one attrs GET per needed cluster. Packed layout costs one GET
/// per relevant packed object plus optional per-cluster attrs GETs.
///
/// # Examples
///
/// Needed clusters `{2, 3}` that share `clusters-0.bin` cause one packed-object
/// fetch, not two. With `load_attrs = false`, the return carries both ID vectors
/// and no attribute vectors.
///
/// # Rust Notes for Java/C Engineers
///
/// Async iterator closures borrow the store/cache but move each owned key and
/// cluster list into its future. `join_all` owns those futures until completion,
/// so Rust proves no borrowed reference or temporary key dangles. Java would
/// rely on captured-object reachability; C would require explicit request-state
/// allocation and cleanup for every concurrent operation.
async fn fetch_bm25_cluster_attrs_and_ids(
    store: &ZeppelinStore,
    located: LocatedSegmentRef<'_>,
    needed_clusters: &HashSet<u16>,
    load_attrs: bool,
    cache: Option<&Arc<DiskCache>>,
) -> Result<Bm25ClusterRows> {
    use crate::index::ivf_flat::build::{
        attrs_key, cluster_key, cluster_object_sections, deserialize_attrs, deserialize_cluster,
        deserialize_cluster_from_object,
    };

    let namespace = located.physical_namespace();
    let segment_ref = located.segment;
    if segment_ref.cluster_objects.is_empty() {
        let cluster_attrs_results =
            futures::future::join_all(needed_clusters.iter().map(|&cluster_idx| {
                let owner = segment_ref.cluster_owner(cluster_idx as usize);
                let akey = attrs_key(namespace, owner, cluster_idx as usize);
                let ckey = cluster_key(namespace, owner, cluster_idx as usize);
                async move {
                    if load_attrs {
                        let (attrs_res, cluster_res) = tokio::join!(
                            fetch_located_query_object(cache, store, located, &akey),
                            fetch_located_query_object(cache, store, located, &ckey)
                        );
                        (cluster_idx, Some(attrs_res), cluster_res)
                    } else {
                        (
                            cluster_idx,
                            None,
                            fetch_located_query_object(cache, store, located, &ckey).await,
                        )
                    }
                }
            }))
            .await;

        let mut cluster_data = HashMap::new();
        for (cluster_idx, attrs_res, cluster_res) in cluster_attrs_results {
            let attrs = match attrs_res {
                Some(Ok(data)) => Some(deserialize_attrs(&data)?),
                Some(Err(e)) => return Err(e),
                None => None,
            };
            let cluster = match cluster_res {
                Ok(data) => deserialize_cluster(&data)?,
                Err(e) => return Err(e),
            };
            cluster_data.insert(cluster_idx, (attrs, cluster));
        }
        return Ok(cluster_data);
    }

    let mut attrs_by_cluster = HashMap::new();
    if load_attrs {
        let attrs_results = futures::future::join_all(needed_clusters.iter().map(|&cluster_idx| {
            let owner = segment_ref.cluster_owner(cluster_idx as usize);
            let akey = attrs_key(namespace, owner, cluster_idx as usize);
            async move {
                (
                    cluster_idx,
                    fetch_located_query_object(cache, store, located, &akey).await,
                )
            }
        }))
        .await;
        for (cluster_idx, attrs_res) in attrs_results {
            let attrs = match attrs_res {
                Ok(data) => deserialize_attrs(&data)?,
                Err(e) => return Err(e),
            };
            attrs_by_cluster.insert(cluster_idx, attrs);
        }
    }

    let object_fetches = segment_ref.cluster_objects.iter().filter_map(|object_ref| {
        let clusters: Vec<u16> = object_ref
            .clusters
            .iter()
            .copied()
            .filter_map(|cluster_idx| {
                u16::try_from(cluster_idx)
                    .ok()
                    .filter(|idx| needed_clusters.contains(idx))
            })
            .collect();
        (!clusters.is_empty()).then_some((object_ref.key.as_str(), clusters))
    });

    let object_results =
        futures::future::join_all(object_fetches.map(|(key, clusters)| async move {
            (
                clusters,
                fetch_located_query_object(cache, store, located, key).await,
            )
        }))
        .await;

    let mut cluster_data = HashMap::new();
    for (clusters, object_res) in object_results {
        let object_data = match object_res {
            Ok(data) => data,
            Err(e) => return Err(e),
        };
        if cluster_object_sections(&object_data)?.is_some() {
            for cluster_idx in clusters {
                let attrs = if load_attrs {
                    Some(attrs_by_cluster.remove(&cluster_idx).ok_or_else(|| {
                        crate::error::ZeppelinError::Index(format!(
                            "missing attrs for BM25 cluster {cluster_idx}"
                        ))
                    })?)
                } else {
                    None
                };
                let cluster = deserialize_cluster_from_object(&object_data, cluster_idx as usize)?;
                cluster_data.insert(cluster_idx, (attrs, cluster));
            }
        } else {
            if clusters.len() != 1 {
                return Err(crate::error::ZeppelinError::Index(format!(
                    "legacy cluster object must reference exactly one cluster, got {}",
                    clusters.len()
                )));
            }
            let cluster_idx = clusters[0];
            let attrs = if load_attrs {
                Some(attrs_by_cluster.remove(&cluster_idx).ok_or_else(|| {
                    crate::error::ZeppelinError::Index(format!(
                        "missing attrs for BM25 cluster {cluster_idx}"
                    ))
                })?)
            } else {
                None
            };
            let cluster = deserialize_cluster(&object_data)?;
            cluster_data.insert(cluster_idx, (attrs, cluster));
        }
    }

    Ok(cluster_data)
}

/// Decodes every logical row from one manifest-selected immutable segment.
async fn materialize_scoped_segment_corpus(
    store: &ZeppelinStore,
    located: LocatedSegmentRef<'_>,
    dimensions: usize,
    cache: Option<&Arc<DiskCache>>,
) -> Result<ScopedSegmentCorpus> {
    let segment_ref = located.segment;
    let all_clusters = all_bm25_cluster_addresses(segment_ref)?;
    let cluster_rows =
        fetch_bm25_cluster_attrs_and_ids(store, located, &all_clusters, true, cache).await?;
    let expected_vectors = segment_ref.vector_count;
    tokio::task::spawn_blocking(move || {
        assemble_scoped_segment_corpus(cluster_rows, expected_vectors, dimensions)
    })
    .await
    .map_err(|error| {
        crate::error::ZeppelinError::from(crate::retrieval_scope::RetrievalScopeError::Worker(
            error.to_string(),
        ))
    })?
}

fn assemble_scoped_segment_corpus(
    cluster_rows: Bm25ClusterRows,
    expected_vectors: usize,
    dimensions: usize,
) -> Result<ScopedSegmentCorpus> {
    let mut clusters: Vec<_> = cluster_rows.into_iter().collect();
    clusters.sort_unstable_by_key(|(cluster_idx, _)| *cluster_idx);
    let mut rows = Vec::with_capacity(expected_vectors);
    for (cluster_idx, (attributes, cluster)) in clusters {
        let attributes = attributes.ok_or_else(|| {
            crate::error::ZeppelinError::Index(format!(
                "scoped corpus cluster {cluster_idx} was loaded without attributes"
            ))
        })?;
        if cluster.ids.len() != cluster.vectors.len() || cluster.ids.len() != attributes.len() {
            return Err(crate::error::ZeppelinError::Index(format!(
                "scoped corpus cluster {cluster_idx} row alignment mismatch: ids={}, vectors={}, attributes={}",
                cluster.ids.len(),
                cluster.vectors.len(),
                attributes.len()
            )));
        }
        rows.extend(
            cluster
                .ids
                .into_iter()
                .zip(cluster.vectors)
                .zip(attributes)
                .map(|((id, values), attributes)| crate::types::VectorEntry {
                    id,
                    values,
                    attributes,
                }),
        );
    }
    if rows.len() != expected_vectors {
        return Err(crate::error::ZeppelinError::Index(format!(
            "scoped corpus materialized {} rows but manifest declares {}",
            rows.len(),
            expected_vectors
        )));
    }
    ScopedSegmentCorpus::new(rows, dimensions)
}

/// Searches every legacy per-cluster inverted index when no global index exists.
///
/// This compatibility path preserves queryability for older FTS-enabled
/// segments, but it is intentionally guarded by manifest-size budgets in its
/// caller. It first loads coarse IVF metadata to determine cluster count, then
/// concurrently prefetches each cluster's inverted index and row-ID object plus
/// attributes when needed. Recompaction should replace this fan-out with a
/// global FTS artifact.
///
/// ```text
/// legacy SegmentRef
///       |
/// load coarse metadata -> cluster count
///       |
///       +-- for every cluster, in parallel -------------------+
///       |        FTS index + row IDs + optional attrs          |
///       +------------------------------------------------------+
///                               |
///                    tokenize/search each field
///                               |
///                 RankBy -> filter -> deduplicate -> top-k
/// ```
///
/// # Parameters
///
/// - `store`: Object-store boundary for immutable legacy artifacts.
/// - `namespace`: Namespace key prefix.
/// - `segment_ref`: Active descriptor known to lack a global FTS index.
/// - `rank_by`: Expression combining field BM25 scores.
/// - `fts_configs`: Per-field analyzer and BM25 settings.
/// - `filter`: Optional attribute predicate applied after lexical scoring.
/// - `last_as_prefix`: Whether to prefix-match the final query token.
/// - `cache`: Optional cache for per-cluster FTS, ID, and attribute bodies.
/// - `top_k`: Maximum descending-score hits retained.
/// - `include_attributes`: Whether result maps retain loaded attributes.
///
/// # Returns
///
/// Deduplicated segment hits ordered by score descending and ID ascending.
///
/// # Errors
///
/// Propagates coarse metadata, per-cluster FTS/ID/attribute reads, and decode
/// failures. The compatibility reader resolves legacy per-cluster keys through
/// `cluster_owner`; a descriptor/object mismatch fails rather than guessing a
/// replacement layout. Some concurrent reads may have warmed cache before an
/// error is observed.
///
/// # Side Effects
///
/// Performs parallel immutable reads and may populate cache entries.
///
/// # Consistency
///
/// Cluster count, owners, and field capability all come from the chosen
/// manifest snapshot. Carried-forward clusters may resolve under older segment
/// IDs, but no object absent from the descriptor becomes visible.
///
/// # Performance
///
/// Object-store request count grows linearly with cluster count: every cluster
/// needs an FTS and ID object, plus attrs when filtering/projection requires
/// them. `join_all` overlaps these requests but also holds every result until
/// all complete. CPU scans matching postings across all cluster indexes.
///
/// # Examples
///
/// A 100-cluster legacy segment issues 200 logical object fetches without attrs
/// or 300 with attrs, subject to cache hits. A modern global index would first
/// fetch one postings object and then row data only for matching clusters.
///
/// # Rust Notes for Java/C Engineers
///
/// The future vector owns every in-flight operation and `join_all` awaits them
/// as a group. Each tuple contains `Result` values rather than throwing from a
/// worker; the processing loop returns the first observed error and RAII drops
/// all fetched buffers that are no longer needed.
#[allow(clippy::too_many_arguments)]
async fn segment_bm25_search_full_scan(
    store: &ZeppelinStore,
    located: LocatedSegmentRef<'_>,
    rank_by: &RankBy,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    filter: Option<&Filter>,
    last_as_prefix: bool,
    decoded_artifact_cache: Option<&Arc<DecodedArtifactCache>>,
    cache: Option<&Arc<DiskCache>>,
    top_k: usize,
    include_attributes: bool,
) -> Result<Bm25SearchOutput> {
    use crate::index::ivf_flat::build::{attrs_key, deserialize_attrs};

    let namespace = located.physical_namespace();
    let segment_ref = located.segment;
    let fts_fields = &segment_ref.fts_fields;

    // Load the IVF-Flat index using manifest metadata to skip cluster probing.
    // The decoded FTS memo is separate from this index-metadata load.
    let index = IvfFlatIndex::load_from_located_manifest(store, located, cache).await?;
    let num_clusters = index.num_clusters();
    let field_queries = rank_by.extract_field_queries();
    let load_attrs = filter.is_some() || include_attributes;
    let mut all_results: HashMap<String, (f32, Option<HashMap<String, AttributeValue>>)> =
        HashMap::new();
    // Parallel prefetch all cluster data (fts index, attrs, cluster vectors).
    let prefetched = futures::future::join_all((0..num_clusters).map(|cluster_idx| {
        // Per-cluster keys route through cluster_owner(): a carried-over
        // cluster's objects live under an older segment's ID.
        let owner = segment_ref.cluster_owner(cluster_idx);
        let fts_key = fts_index_key(namespace, owner, cluster_idx);
        let fts_cache_key = located.cache_key(&fts_key);
        let akey = attrs_key(namespace, owner, cluster_idx);
        let ckey = crate::index::ivf_flat::build::cluster_key(namespace, owner, cluster_idx);
        async move {
            let fts_future = async {
                match decoded_artifact_cache {
                    Some(decoded_cache) => {
                        decoded_cache
                            .get_or_decode_cluster_fts(&fts_cache_key, || {
                                fetch_located_query_object(cache, store, located, &fts_key)
                            })
                            .await
                    }
                    None => Ok(Arc::new(InvertedIndex::from_bytes(
                        &fetch_located_query_object(cache, store, located, &fts_key).await?,
                    )?)),
                }
            };
            if load_attrs {
                let (fts_res, attrs_res, cluster_res) = tokio::join!(
                    fts_future,
                    fetch_located_query_object(cache, store, located, &akey),
                    fetch_located_query_object(cache, store, located, &ckey),
                );
                (cluster_idx, fts_res, Some(attrs_res), cluster_res)
            } else {
                let (fts_res, cluster_res) = tokio::join!(
                    fts_future,
                    fetch_located_query_object(cache, store, located, &ckey),
                );
                (cluster_idx, fts_res, None, cluster_res)
            }
        }
    }))
    .await;

    // Process prefetched results — CPU-bound, no I/O.
    for (_cluster_idx, fts_res, attrs_res, cluster_res) in prefetched {
        let inv_index = match fts_res {
            Ok(index) => index,
            Err(e) => return Err(e),
        };

        let cluster_attrs = match attrs_res {
            Some(Ok(data)) => Some(deserialize_attrs(&data)?),
            Some(Err(e)) => return Err(e),
            None => None,
        };

        let cluster_data = match cluster_res {
            Ok(data) => data,
            Err(e) => return Err(e),
        };
        let cluster = crate::index::ivf_flat::build::deserialize_cluster(&cluster_data)?;

        // For each field+query, search the inverted index
        let mut position_field_scores: HashMap<u32, HashMap<String, f32>> = HashMap::new();

        for (field, query) in &field_queries {
            let config = match fts_configs.get(field.as_str()) {
                Some(c) => c,
                None => continue,
            };

            if !fts_fields.contains(field) {
                continue;
            }

            let query_tokens = tokenize_text(query, config, last_as_prefix);

            let params = Bm25Params {
                k1: config.k1,
                b: config.b,
            };
            let results = if last_as_prefix {
                inv_index.search_prefix(field, &query_tokens, &params)
            } else {
                inv_index.search(field, &query_tokens, &params)
            };

            for (pos, score) in results {
                let entry = position_field_scores.entry(pos).or_default();
                *entry.entry(field.to_string()).or_insert(0.0) += score;
            }
        }

        // Evaluate rank_by expression and collect results
        for (pos, field_scores) in position_field_scores {
            let final_score = evaluate_rank_by(rank_by, &field_scores);
            if final_score <= 0.0 {
                continue;
            }

            let pos_usize = pos as usize;
            if pos_usize >= cluster.ids.len() {
                continue;
            }

            let id = cluster.ids[pos_usize].clone();
            let attrs = cluster_attrs
                .as_ref()
                .and_then(|cluster_attrs| cluster_attrs.get(pos_usize))
                .cloned()
                .flatten();

            // Apply post-filter
            if let Some(f) = filter {
                match &attrs {
                    Some(a) => {
                        if !evaluate_filter(f, a) {
                            continue;
                        }
                    }
                    None => continue,
                }
            }

            // Accumulate: same ID might appear in multiple clusters (shouldn't, but be safe)
            let response_attrs = if include_attributes { attrs } else { None };
            let entry = all_results
                .entry(id.clone())
                .or_insert((0.0, response_attrs.clone()));
            if final_score > entry.0 {
                entry.0 = final_score;
                entry.1 = response_attrs;
            }
        }
    }

    let mut results: Vec<SearchResult> = all_results
        .into_iter()
        .map(|(id, (score, attributes))| SearchResult {
            id,
            score,
            attributes,
        })
        .collect();

    partial_topk_by(&mut results, top_k, bm25_result_cmp);

    Ok(Bm25SearchOutput {
        results,
        clusters_probed: num_clusters,
    })
}

/// Merges BM25 WAL and segment candidates without exposing stale document versions.
///
/// Strong mode first admits WAL hits, then admits only segment IDs absent from
/// the complete live-WAL override and tombstone sets. Eventual mode intentionally
/// ignores WAL candidates and live overrides, but removes tombstoned segment IDs.
/// Higher score wins and IDs make ties deterministic.
///
/// ```text
/// Strong                               Eventual
/// WAL scored hits ----+                segment hits
///                     |                     |
/// segment hits -- suppress live WAL         +-- suppress tombstones
///                 IDs + tombstones          |
///                     |                     v
///                     v              BM25 top-k only
///              BM25 TopK merge
/// ```
///
/// # Parameters
///
/// - `wal_results`: Owned positive-score hits from latest WAL documents.
/// - `wal_overriding_ids`: All live WAL IDs, including non-matches and results
///   outside the WAL top-k.
/// - `segment_results`: Owned active-segment candidates.
/// - `top_k`: Maximum merged results.
/// - `consistency`: Selects strong or eventual participation rules.
/// - `wal_deleted_ids`: Effective tombstones that always suppress segment hits.
///
/// # Returns
///
/// Owned hits ordered by score descending and ID ascending, truncated to
/// `top_k`. Input vectors are consumed.
///
/// # Consistency
///
/// WAL overwrite authority is by ID, not by score: a lower-scoring or
/// non-matching latest WAL record still hides a higher-scoring old segment
/// version under strong consistency. Deletes are honored under both modes.
///
/// # Performance
///
/// Uses a bounded heap and suppression-set lookups; memory is proportional to
/// `top_k` plus the already-built sets. Eventual mode filters its segment vector
/// in place before partial top-k selection.
///
/// # Examples
///
/// Segment `a` scores 10, but WAL updates `a` to text scoring zero. In strong
/// mode `a` is absent because its ID is overriding; the stale score 10 cannot
/// win. In eventual mode the old segment `a` may remain unless WAL deleted it.
///
/// # Rust Notes for Java/C Engineers
///
/// The vectors are moved into this function and then their elements are moved
/// into the result frontier, avoiding deep copies. Java collections would still
/// be usable aliases unless convention forbade it; C would need an explicit
/// ownership-transfer rule. Rust rejects use of the consumed vectors afterward.
fn merge_bm25_results(
    wal_results: Vec<SearchResult>,
    wal_overriding_ids: &HashSet<String>,
    segment_results: Vec<SearchResult>,
    top_k: usize,
    consistency: ConsistencyLevel,
    wal_deleted_ids: &HashSet<String>,
) -> Vec<SearchResult> {
    match consistency {
        ConsistencyLevel::Strong => {
            let mut merged = TopK::new(
                top_k,
                bm25_result_cmp as fn(&SearchResult, &SearchResult) -> Ordering,
            );

            for sr in wal_results {
                merged.push(sr);
            }
            for sr in segment_results {
                // Exclude if WAL has a newer version OR if explicitly deleted
                if !wal_overriding_ids.contains(&sr.id) && !wal_deleted_ids.contains(&sr.id) {
                    merged.push(sr);
                }
            }

            merged.into_sorted_vec()
        }
        ConsistencyLevel::Eventual => {
            let mut results: Vec<SearchResult> = segment_results
                .into_iter()
                .filter(|sr| !wal_deleted_ids.contains(&sr.id))
                .collect();
            partial_topk_by(&mut results, top_k, bm25_result_cmp);
            results
        }
    }
}

/// Merges distance-scored WAL and segment candidates under read-consistency rules.
///
/// Strong mode treats latest WAL state as authoritative over the compacted
/// segment: it inserts WAL hits and excludes every segment ID with a live WAL
/// replacement or tombstone. Eventual mode uses segment candidates only, while
/// still excluding tombstoned IDs. Lower distance wins.
///
/// # Parameters
///
/// - `wal_results`: Owned latest-WAL candidates sorted by ascending distance.
/// - `wal_overriding_ids`: Every live latest WAL ID, not only scored winners.
/// - `segment_results`: Owned active-segment candidates.
/// - `top_k`: Maximum final result count.
/// - `consistency`: Determines whether live WAL candidates/replacements apply.
/// - `wal_deleted_ids`: Effective tombstones applied in both modes.
///
/// # Returns
///
/// At most `top_k` owned results ordered by distance ascending, then ID. No ID
/// can appear from both its old segment version and latest WAL version.
///
/// # Consistency
///
/// Suppression is independent of whether the newer WAL vector passed filtering
/// or fit the WAL frontier. This prevents a filtered-out update from revealing
/// stale attributes/vector values in the segment. Eventual staleness permits an
/// older non-deleted value but never a tombstoned one.
///
/// # Performance
///
/// Strong merge performs hash-set checks and bounded heap insertion over both
/// candidate lists. Eventual filters the segment list and uses partial top-k.
/// Inputs are consumed rather than cloned.
///
/// # Examples
///
/// Segment holds `a@0.1`; WAL updates `a@0.5` and adds `b@0.2`. Strong returns
/// `b` then the new `a` (subject to `top_k`), never old `a@0.1`. Eventual may
/// return old `a@0.1`. If `a` is tombstoned, neither mode returns it.
fn merge_results(
    wal_results: Vec<SearchResult>,
    wal_overriding_ids: &HashSet<String>,
    segment_results: Vec<SearchResult>,
    top_k: usize,
    consistency: ConsistencyLevel,
    wal_deleted_ids: &HashSet<String>,
) -> Vec<SearchResult> {
    match consistency {
        ConsistencyLevel::Strong => {
            // WAL results already have the latest state.
            // Remove segment results whose IDs appear in WAL results (WAL is
            // authoritative) or were deleted in the WAL.
            let mut merged = TopK::new(
                top_k,
                distance_result_cmp as fn(&SearchResult, &SearchResult) -> Ordering,
            );

            for sr in wal_results {
                merged.push(sr);
            }
            for sr in segment_results {
                if !wal_overriding_ids.contains(&sr.id) && !wal_deleted_ids.contains(&sr.id) {
                    merged.push(sr);
                }
            }

            merged.into_sorted_vec()
        }
        ConsistencyLevel::Eventual => {
            let mut results: Vec<SearchResult> = segment_results
                .into_iter()
                .filter(|sr| !wal_deleted_ids.contains(&sr.id))
                .collect();
            partial_topk_by(&mut results, top_k, distance_result_cmp);
            results
        }
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::single_match)]
#[cfg(test)]
mod tests {
    //! Protects query merge freshness, WAL materialization, and BM25 fallback limits.
    //!
    //! The merge tests use small owned result lists so failures identify the
    //! exact override/tombstone rule. WAL and circuit-breaker tests use the
    //! in-memory object-store implementation because they exercise decoding and
    //! pre-I/O decisions rather than S3 integration semantics.

    use super::*;

    /// Constructs a minimal owned hit for merge-policy tests.
    ///
    /// # Parameters
    ///
    /// - `id`: Logical record ID.
    /// - `score`: Source-native distance or BM25 score chosen by the test.
    ///
    /// # Returns
    ///
    /// A result with no attributes.
    ///
    /// # Examples
    ///
    /// `make_result("a", 0.1)` represents a close ANN hit; the same helper can
    /// represent BM25 by interpreting the score as higher-is-better.
    fn make_result(id: &str, score: f32) -> SearchResult {
        SearchResult {
            id: id.to_string(),
            score,
            attributes: None,
        }
    }

    /// Builds a two-dimensional WAL vector with a clone-detectable payload.
    ///
    /// # Parameters
    ///
    /// - `id`: Vector ID used in both the record and payload string.
    /// - `offset`: First coordinate, controlling distance from the zero query.
    ///
    /// # Returns
    ///
    /// An owned vector `[offset, 0]` with one string attribute.
    ///
    /// # Examples
    ///
    /// ID `v_003` and offset `3` produce payload `payload-v_003`, allowing the
    /// test to verify winner-only attribute cloning.
    fn wal_vector_with_attrs(id: &str, offset: f32) -> crate::types::VectorEntry {
        let mut attrs = HashMap::new();
        attrs.insert(
            "payload".to_string(),
            AttributeValue::String(format!("payload-{id}")),
        );
        crate::types::VectorEntry {
            id: id.to_string(),
            values: vec![offset, 0.0],
            attributes: Some(attrs),
        }
    }

    /// Verifies that WAL attributes are deep-cloned only for returned winners.
    ///
    /// The test writes 100 live vectors, requests top five twice, and checks the
    /// test-only counter: projection disabled performs zero clones; projection
    /// enabled performs exactly five. It catches eager cloning before bounded
    /// top-k selection.
    #[tokio::test]
    async fn test_wal_scan_materializes_attrs_only_for_returned_topk() {
        let store = crate::storage::ZeppelinStore::new(std::sync::Arc::new(
            object_store::memory::InMemory::new(),
        ));
        let namespace = "wal-scan-attrs-clone";
        let mut initial_manifest = crate::wal::manifest::Manifest::new();
        initial_manifest
            .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
            .unwrap();
        initial_manifest.write(&store, namespace).await.unwrap();

        let vectors: Vec<_> = (0..100)
            .map(|idx| wal_vector_with_attrs(&format!("v_{idx:03}"), idx as f32))
            .collect();
        let (_, manifest) = crate::wal::WalWriter::new(store.clone())
            .append(namespace, vectors, vec![])
            .await
            .unwrap();
        let wal_reader = WalReader::new(store.clone());
        let local_origin = manifest.local_origin().unwrap();
        let located_fragments = manifest
            .artifact_origin_resolver(&local_origin)
            .unwrap()
            .uncompacted_located_fragments()
            .unwrap();

        WAL_ATTR_CLONES.store(0, std::sync::atomic::Ordering::Relaxed);
        let without_attrs = wal_scan(
            &wal_reader,
            &located_fragments,
            &[0.0, 0.0],
            None,
            DistanceMetric::Euclidean,
            None,
            None,
            false,
            5,
        )
        .await
        .unwrap();
        assert_eq!(without_attrs.results.len(), 5);
        assert!(without_attrs
            .results
            .iter()
            .all(|result| result.attributes.is_none()));
        assert_eq!(
            WAL_ATTR_CLONES.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "include_attributes=false must not clone WAL attrs"
        );

        WAL_ATTR_CLONES.store(0, std::sync::atomic::Ordering::Relaxed);
        let with_attrs = wal_scan(
            &wal_reader,
            &located_fragments,
            &[0.0, 0.0],
            None,
            DistanceMetric::Euclidean,
            None,
            None,
            true,
            5,
        )
        .await
        .unwrap();
        assert_eq!(with_attrs.results.len(), 5);
        assert!(with_attrs
            .results
            .iter()
            .all(|result| result.attributes.is_some()));
        assert_eq!(
            WAL_ATTR_CLONES.load(std::sync::atomic::Ordering::Relaxed),
            5,
            "WAL attrs should be cloned only for returned top-k results"
        );
    }

    /// Ensures a WAL tombstone removes an already-compacted ANN hit.
    ///
    /// The deleted ID has no live WAL result, so this test catches merges that
    /// suppress only IDs present in `wal_results` instead of using tombstones.
    #[test]
    fn test_merge_strong_excludes_wal_deleted_segment_results() {
        // Vector "compacted" lives only in the segment; its WAL tombstone
        // produces no WAL result. The merge must still drop it.
        let wal_results = vec![make_result("fresh", 0.1)];
        let segment_results = vec![make_result("compacted", 0.2), make_result("kept", 0.3)];
        let deleted: HashSet<String> = ["compacted".to_string()].into_iter().collect();
        let overriding: HashSet<String> = ["fresh".to_string()].into_iter().collect();

        let merged = merge_results(
            wal_results,
            &overriding,
            segment_results,
            10,
            ConsistencyLevel::Strong,
            &deleted,
        );

        let ids: Vec<&str> = merged.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["fresh", "kept"]);
    }

    /// Ensures a live WAL ANN version replaces, rather than duplicates, a segment ID.
    ///
    /// The stale segment version has the better distance deliberately. The test
    /// proves freshness by ID takes precedence over score.
    #[test]
    fn test_merge_strong_wal_overrides_segment() {
        // Same ID in WAL and segment: WAL version wins, no duplicate.
        let wal_results = vec![make_result("v1", 0.5)];
        let segment_results = vec![make_result("v1", 0.1)];
        let overriding: HashSet<String> = ["v1".to_string()].into_iter().collect();

        let merged = merge_results(
            wal_results,
            &overriding,
            segment_results,
            10,
            ConsistencyLevel::Strong,
            &HashSet::new(),
        );

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].id, "v1");
        assert_eq!(merged[0].score, 0.5);
    }

    /// Ensures strong ANN search refills after its initial segment hits are suppressed.
    ///
    /// Two of three initial hits are hidden by WAL state. The test checks a
    /// segment-size-capped retry width and proves the deeper candidate survives
    /// final top-k merge.
    #[test]
    fn test_strong_segment_refill_after_wal_suppression() {
        let initial_segment_results = vec![
            make_result("a", 10.0),
            make_result("b", 20.0),
            make_result("c", 30.0),
        ];
        let wal_results = vec![make_result("a", 100.0), make_result("b", 130.0)];
        let overriding: HashSet<String> = ["a".to_string(), "b".to_string()].into_iter().collect();
        let deleted: HashSet<String> = ["a".to_string(), "b".to_string()].into_iter().collect();

        let refill_top_k = segment_refill_top_k(
            &initial_segment_results,
            3,
            ConsistencyLevel::Strong,
            &overriding,
            &deleted,
            4,
        );

        assert_eq!(refill_top_k, Some(4));

        let refilled_segment_results = vec![
            make_result("a", 10.0),
            make_result("b", 20.0),
            make_result("c", 30.0),
            make_result("d", 40.0),
        ];
        let merged = merge_results(
            wal_results,
            &overriding,
            refilled_segment_results,
            3,
            ConsistencyLevel::Strong,
            &deleted,
        );

        let ids = merged
            .iter()
            .map(|result| result.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["c", "d", "a"]);
    }

    /// Ensures eventual ANN reads still apply uncompacted delete tombstones.
    ///
    /// It catches the incorrect interpretation that eventual mode may resurrect
    /// deleted data merely because it skips live WAL upsert scoring.
    #[test]
    fn test_merge_eventual_applies_tombstones() {
        // Eventual skips WAL vector scoring, but deletes are correctness:
        // segment results with WAL tombstones must be excluded immediately.
        let segment_results = vec![make_result("a", 0.1), make_result("b", 0.2)];
        let deleted: HashSet<String> = ["a".to_string()].into_iter().collect();

        let merged = merge_results(
            Vec::new(),
            &HashSet::new(),
            segment_results,
            10,
            ConsistencyLevel::Eventual,
            &deleted,
        );

        let ids: Vec<&str> = merged.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["b"]);
    }

    /// Ensures eventual BM25 reads enforce the same delete rule as ANN reads.
    ///
    /// The higher-scoring segment document is tombstoned and must disappear even
    /// though eventual lexical merge has no WAL candidates.
    #[test]
    fn test_merge_bm25_eventual_applies_tombstones() {
        // Same invariant for BM25/rank_by: Eventual segment hits must not
        // resurrect a document deleted in the WAL.
        let segment_results = vec![make_result("a", 2.0), make_result("b", 1.0)];
        let deleted: HashSet<String> = ["a".to_string()].into_iter().collect();

        let merged = merge_bm25_results(
            Vec::new(),
            &HashSet::new(),
            segment_results,
            10,
            ConsistencyLevel::Eventual,
            &deleted,
        );

        let ids: Vec<&str> = merged.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["b"]);
    }

    /// Builds the minimal flat segment descriptor needed by BM25 path tests.
    ///
    /// # Parameters
    ///
    /// - `cluster_count`: Declared fallback fan-out.
    /// - `has_global_fts`: Whether the dispatcher should bypass full-scan limits.
    ///
    /// # Returns
    ///
    /// A 10,000-vector descriptor advertising FTS field `text`, with no actual
    /// artifact objects. Tests can therefore distinguish pre-I/O availability
    /// rejection from the later expected missing-object error.
    fn make_segment_ref(cluster_count: usize, has_global_fts: bool) -> SegmentRef {
        SegmentRef {
            id: "seg_test".to_string(),
            vector_count: 10000,
            cluster_count,
            quantization: crate::index::quantization::QuantizationType::None,
            hierarchical: false,
            bitmap_fields: Vec::new(),
            fts_fields: vec!["text".to_string()],
            has_global_fts,
            cluster_owners: Vec::new(),
            sketch: None,
            cluster_objects: Vec::new(),
            bootstrap: None,
            membership: None,
            artifact_origin: None,
        }
    }

    /// Binds a local segment descriptor to the same validated origin seam used by production.
    fn local_segment_manifest(segment: SegmentRef) -> (Manifest, ArtifactOrigin) {
        let mut manifest = Manifest::new();
        let incarnation = uuid::Uuid::from_u128(0x0051_5545_5259);
        manifest.bind_namespace_incarnation(incarnation).unwrap();
        manifest.segments.push(segment);
        let origin = ArtifactOrigin {
            namespace: crate::namespace::NamespaceId::parse("ns").unwrap(),
            incarnation: crate::namespace::NamespaceIncarnationId::from_uuid(incarnation),
        };
        (manifest, origin)
    }

    /// Verifies that excessive legacy cluster fan-out fails before object reads.
    ///
    /// A 600-cluster segment under limit 500 must return the operator-facing
    /// index-unavailable variant with both counts in its message.
    #[tokio::test]
    async fn test_bm25_circuit_breaker_rejects_over_limit() {
        let mem = std::sync::Arc::new(object_store::memory::InMemory::new());
        let store = crate::storage::ZeppelinStore::new(mem);
        let rank_by = RankBy::Bm25 {
            field: "text".to_string(),
            query: "hello".to_string(),
        };
        let fts_configs = HashMap::new();
        let (manifest, origin) = local_segment_manifest(make_segment_ref(600, false));
        let seg = manifest
            .artifact_origin_resolver(&origin)
            .unwrap()
            .locate_segment(&manifest.segments[0])
            .unwrap();

        let result = segment_bm25_search(
            &store,
            seg,
            &rank_by,
            &fts_configs,
            None,
            false,
            None,
            None,
            500,
            100_000,
            10,
            true,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            crate::error::ZeppelinError::IndexUnavailable(msg) => {
                assert!(msg.contains("600 clusters"));
                assert!(msg.contains("limit 500"));
                assert!(msg.contains("operator"));
            }
            _ => panic!("expected IndexUnavailable error, got: {err:?}"),
        }
    }

    /// Verifies that the independent vector-count budget is enforced before I/O.
    ///
    /// Cluster count is within limit, so the test catches implementations that
    /// forget to protect large few-cluster segments.
    #[test]
    fn test_bm25_full_scan_budget_rejects_vector_count_before_io() {
        let mut seg = make_segment_ref(4, false);
        seg.vector_count = 20_001;

        let err = validate_bm25_full_scan_budget(
            "ns",
            &seg,
            Bm25FullScanBudget {
                max_clusters: 10,
                max_vectors: 20_000,
            },
        )
        .unwrap_err();

        match err {
            crate::error::ZeppelinError::IndexUnavailable(msg) => {
                assert!(msg.contains("FTS index"));
                assert!(msg.contains("20,001 vectors") || msg.contains("20001 vectors"));
                assert!(msg.contains("operator"));
            }
            _ => panic!("expected IndexUnavailable error, got: {err:?}"),
        }
    }

    /// Verifies that an under-limit legacy segment proceeds to artifact loading.
    ///
    /// No artifacts are installed, so a non-availability failure proves the
    /// circuit breaker admitted the scan without requiring a successful query.
    #[tokio::test]
    async fn test_bm25_circuit_breaker_allows_under_limit() {
        let mem = std::sync::Arc::new(object_store::memory::InMemory::new());
        let store = crate::storage::ZeppelinStore::new(mem);
        let rank_by = RankBy::Bm25 {
            field: "text".to_string(),
            query: "hello".to_string(),
        };
        let fts_configs = HashMap::new();
        let (manifest, origin) = local_segment_manifest(make_segment_ref(400, false));
        let seg = manifest
            .artifact_origin_resolver(&origin)
            .unwrap()
            .locate_segment(&manifest.segments[0])
            .unwrap();

        // Under limit: should attempt the scan (will fail with NotFound on the index,
        // not with a Validation error)
        let result = segment_bm25_search(
            &store,
            seg,
            &rank_by,
            &fts_configs,
            None,
            false,
            None,
            None,
            500,
            100_000,
            10,
            true,
        )
        .await;

        // Should NOT be an availability/budget error (it'll be NotFound or similar
        // from missing data).
        match &result {
            Err(crate::error::ZeppelinError::IndexUnavailable(_)) => {
                panic!("should not have triggered circuit breaker");
            }
            _ => {} // Any other result is fine (expected to fail on missing data)
        }
    }

    /// Verifies that zero disables both BM25 full-scan limits.
    ///
    /// Even a 9,999-cluster descriptor must reach the expected missing-object
    /// path instead of returning index-unavailable from budget validation.
    #[tokio::test]
    async fn test_bm25_circuit_breaker_disabled_when_zero() {
        let mem = std::sync::Arc::new(object_store::memory::InMemory::new());
        let store = crate::storage::ZeppelinStore::new(mem);
        let rank_by = RankBy::Bm25 {
            field: "text".to_string(),
            query: "hello".to_string(),
        };
        let fts_configs = HashMap::new();
        let (manifest, origin) = local_segment_manifest(make_segment_ref(9999, false));
        let seg = manifest
            .artifact_origin_resolver(&origin)
            .unwrap()
            .locate_segment(&manifest.segments[0])
            .unwrap();

        // Limit=0 means disabled — should not reject
        let result = segment_bm25_search(
            &store,
            seg,
            &rank_by,
            &fts_configs,
            None,
            false,
            None,
            None,
            0,
            0,
            10,
            true,
        )
        .await;

        match &result {
            Err(crate::error::ZeppelinError::IndexUnavailable(_)) => {
                panic!("should not have triggered circuit breaker when limit=0");
            }
            _ => {} // Expected to fail on missing data, not circuit breaker
        }
    }
}
