//! CPU-side BM25 evaluation for documents still visible in uncompacted WAL.
//!
//! Compacted segments carry immutable inverted indexes, but newly published WAL
//! fragments have not yet been folded into those artifacts. For a **strong**
//! lexical query, [`crate::query`] reads one authoritative manifest snapshot,
//! asks [`crate::wal::reader::WalReader`] for its visible uncompacted fragments,
//! and calls [`wal_bm25_scan`] to score the latest WAL version of each document.
//! The query coordinator then merges these results with segment hits, using
//! [`WalBm25ScanResult::overriding_ids`] and
//! [`WalBm25ScanResult::deleted_ids`] to suppress stale segment versions.
//!
//! This file performs no S3/MinIO or manifest I/O. Receiving a decoded fragment
//! does not make it authoritative: the caller must pass only refs selected by
//! its manifest snapshot, in manifest sequence-number order. WAL objects remain
//! immutable, and the scan only builds ephemeral in-memory statistics. Eventual
//! queries skip WAL scoring in the current coordinator and read tombstones only.
//!
//! ```text
//! authoritative manifest snapshot
//!            |
//!            | refs in oldest -> newest sequence order
//!            v
//! decoded uncompacted fragments
//!            |
//!            v
//! replay upserts + tombstones --------> live IDs + final deleted IDs
//!            |
//!            v
//! tokenize required fields (optional WalFtsCache)
//!            |
//!            v
//! build WAL-only corpus statistics
//!            |
//!            v
//! filter -> per-field BM25 -> RankBy expression -> optional top-k
//!            |
//!            v
//! query merge suppresses stale/deleted segment hits
//! ```
//!
//! ## Reading map
//!
//! 1. Read [`WalBm25ScanResult`] to understand the downstream merge contract.
//! 2. Read [`wal_bm25_scan`] from replay through scoring and bounded selection.
//! 3. Read [`crate::fts::wal_cache::WalFtsCache`] for the optional CPU cache and
//!    its cache-key limitations.
//! 4. Continue in [`crate::query`] for concurrent segment search and final
//!    strong/eventual merge behavior.
//!
//! ## Scoring and visibility invariants
//!
//! - Slice order, not ULID order, defines last-write-wins replay. A newer upsert
//!   revives an older tombstone; a newer tombstone removes an older upsert.
//! - Every surviving live WAL ID overrides the same ID in a compacted segment,
//!   even if metadata filtering, an empty query, a zero score, or top-k prevents
//!   that WAL document from appearing in `results`.
//! - Corpus statistics include all surviving WAL documents with token data,
//!   including documents later rejected by the metadata filter.
//! - WAL and segment searches compute BM25 against separate corpora. The query
//!   merge currently compares their resulting numeric scores directly.
//! - Higher BM25/`RankBy` scores rank first; exact ties use document ID ascending.
//!
//! ## Rust concepts used here
//!
//! Most intermediate maps own strings and token data, but `latest_vectors` and
//! `ScoredBm25Doc` borrow IDs and attributes from the input fragments. This is
//! analogous to read-only Java references or `const` C pointers, with compiler
//! checked non-null lifetimes. The final [`SearchResult`] values copy IDs and,
//! only when requested, clone attributes so they can outlive the fragment
//! borrow. Iterator pipelines build query states and term inputs without
//! virtual dispatch, while `TopK` owns only the best bounded set.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use tracing::debug;

use crate::fts::bm25::{self, Bm25Params};
use crate::fts::rank_by::{evaluate_rank_by, RankBy};
use crate::fts::tokenizer::tokenize_text;
use crate::fts::wal_cache::WalFtsCache;
use crate::fts::FtsFieldConfig;
use crate::index::filter::evaluate_filter_on_optional_attributes;
use crate::index::topk::TopK;
use crate::namespace::branching::ArtifactOrigin;
use crate::types::{AttributeValue, Filter, SearchResult};
use crate::wal::reader::{LocatedInputFragment, LocatedWalFragment};

/// Ephemeral token statistics keyed first by document ID and then field name.
///
/// Each leaf is `(document_length, term_to_frequency)`. Unlike a segment
/// inverted index, this representation is rebuilt for the current WAL snapshot
/// and optimized for scanning a relatively small uncompacted tail.
type DocFieldData = HashMap<String, HashMap<String, (u32, HashMap<String, u32>)>>;

/// Orders borrowed WAL candidates by relevance descending, then ID ascending.
///
/// # Parameters
///
/// - `a`: First borrowed candidate to compare.
/// - `b`: Second borrowed candidate to compare.
///
/// # Returns
///
/// An [`Ordering`] suitable for best-first sorting and `TopK`. `f32::total_cmp`
/// gives a total order even for unusual floating-point values.
///
/// # Examples
///
/// Score `2.0` ranks before `1.0`; tied IDs `a` and `b` rank as `a`, then `b`.
fn bm25_scored_cmp(a: &ScoredBm25Doc<'_>, b: &ScoredBm25Doc<'_>) -> Ordering {
    b.score.total_cmp(&a.score).then_with(|| a.id.cmp(b.id))
}

/// Borrowed scored candidate used until the scan creates an owned API result.
///
/// Holding references avoids cloning every candidate's ID and attributes before
/// top-k rejection. Rust prevents the value from outliving its source fragment.
struct ScoredBm25Doc<'a> {
    /// Logical document ID borrowed from the replay map.
    id: &'a str,
    /// Final `RankBy` score; larger positive values are better.
    score: f32,
    /// Optional source attributes borrowed from the winning WAL upsert.
    attrs: Option<&'a HashMap<String, AttributeValue>>,
}

/// Owned WAL contribution and suppression metadata for final BM25 merging.
///
/// The result deliberately distinguishes scored hits from all live overrides
/// and tombstones. Strong merging must suppress a compacted segment's older
/// record even when the latest WAL record did not match the lexical query.
///
/// # Examples
///
/// If WAL upserts `p42` to text that does not match and deletes `p17`, `results`
/// may be empty while `overriding_ids = {p42}` and `deleted_ids = {p17}`. The
/// merge removes both stale segment records.
pub struct WalBm25ScanResult {
    /// Positive-score WAL hits sorted by score descending and ID ascending on ties.
    ///
    /// The vector contains at most the requested `top_k` when one was supplied.
    /// Attributes are present only when `include_attributes` was true and the
    /// winning WAL record had attributes.
    pub results: Vec<SearchResult>,
    /// IDs of all live latest WAL upserts after replay.
    ///
    /// This includes IDs filtered out, lacking text, scoring zero, or falling
    /// outside top-k. Strong merge uses the set to hide older segment versions.
    pub overriding_ids: HashSet<String>,
    /// Number of fragment values supplied to the scan.
    ///
    /// This is input count, including delete-only fragments and fragments that
    /// contribute no surviving document or score.
    pub fragment_count: usize,
    /// IDs whose final replayed WAL operation is an effective tombstone.
    ///
    /// A later upsert removes an ID from this set. Both strong and eventual
    /// merge paths use final tombstones to exclude matching segment results.
    pub deleted_ids: HashSet<String>,
}

/// Replays visible WAL fragments and ranks their latest live documents with BM25.
///
/// The scan first establishes latest-write-wins state, then tokenizes fields
/// referenced by the ranking expression, computes per-field corpus statistics,
/// applies metadata filtering, evaluates BM25 leaves and their [`RankBy`]
/// combination, and materializes positive-score results. It is synchronous CPU
/// work over already-decoded fragments; storage failures occur before this
/// boundary in the caller.
///
/// ```text
/// fragments (oldest -> newest)
///       |
///       +-- replay deletes/upserts --> latest_vectors, deleted_ids
///       |
///       +-- RankBy leaves ----------> configured query tokens
///                                      |
/// latest vectors + optional cache -----+--> token maps + corpus stats
///                                                |
/// filter ----------------------------------------+--> score > 0
///                                                        |
///                                               all sorted or bounded top-k
/// ```
///
/// # Parameters
///
/// - `fragments`: Manifest-selected, decoded immutable fragments in ascending
///   replay sequence, each retaining its resolved physical origin identity.
///   Reversing the slice changes update/delete outcomes.
/// - `logical_origin`: Exact target namespace lifetime whose manifest made the
///   fragment sequence visible. Derived-cache ownership is scoped to this value.
/// - `rank_by`: Borrowed lexical ranking expression. BM25 leaves identify field
///   and query text; sum, max, and product nodes combine per-field scores.
/// - `fts_configs`: Validated tokenization and BM25 settings keyed by field.
///   Leaves for missing fields are skipped; no error is returned here.
/// - `last_as_prefix`: When true, leaves the final query token unstemmed and
///   matches it against the prefixes of normally tokenized document terms.
/// - `fts_cache`: Optional shared derived-data cache. `None` tokenizes only the
///   latest replayed record inline. See [`WalFtsCache`] for cache-key caveats.
/// - `filter`: Optional metadata predicate. A record with no attributes cannot
///   satisfy a supplied filter. Filtering does not change corpus statistics.
/// - `include_attributes`: Whether each returned hit clones the winning record's
///   attribute map. Suppression sets are unaffected.
/// - `top_k`: Optional result bound. `Some(0)` returns no scored hits while still
///   computing override/delete sets; `None` sorts every positive-score hit.
///
/// # Returns
///
/// A [`WalBm25ScanResult`] containing owned ranked hits, every live overriding
/// WAL ID, final tombstones, and input fragment count. Empty fragments or no
/// usable configured query tokens produce empty hits without losing suppression
/// metadata already established by replay.
///
/// # Side Effects
///
/// With a cache, fragment misses can populate shared token entries. The
/// function also emits one debug event after a non-early-return scan. It does
/// not read or write object storage, publish a manifest, or mutate fragments.
///
/// # Consistency
///
/// The caller is responsible for manifest authority and slice order. Within a
/// fragment, deletes are processed before vectors; normal fragment constructors
/// reject overlap between those collections. Across fragments, every later
/// operation replaces the earlier state for the same ID.
///
/// On the cached path, token maps from each fragment are copied for any ID that
/// survives globally. A newer cached record overwrites older token data when it
/// has the requested field. If the latest upsert omits that field while an
/// older version had it, the older cached field data currently remains even
/// though the no-cache path uses only the latest record.
///
/// TODO(doc): Verify whether cached WAL scanning is intended to preserve stale
/// field text when a newer upsert removes or changes that field to a non-string;
/// current cached and uncached behavior diverges in that case.
///
/// Each document's score map is keyed only by field name. If one `RankBy`
/// expression contains multiple BM25 leaves for the same field but different
/// query text, the later extracted leaf overwrites the earlier field score and
/// both expression leaves read that one value.
///
/// TODO(doc): Verify whether repeated BM25 leaves for one field are supported or
/// should be rejected/represented by a `(field, query)` score key.
///
/// Prefix scoring also has a WAL-specific aggregation rule: it sums term
/// frequencies for every document term beginning with the final query token,
/// but uses the maximum document frequency among those terms for one IDF value.
/// Segment posting-list search expands and scores matching terms separately, so
/// the two sources can assign different numeric scores to the same prefix match.
///
/// TODO(doc): Verify whether WAL prefix scoring should match segment posting-list
/// expansion exactly before their scores are compared in the final merge.
///
/// # Performance
///
/// Replay is linear in WAL operations. Uncached tokenization is linear in live
/// text; cached scans trade that CPU for deep map clones. Corpus construction is
/// linear in cached terms. Exact term scoring is roughly documents times query
/// leaves/tokens; prefix scoring additionally scans every term in each relevant
/// document map. `Some(k)` keeps `O(k)` scored candidates, whereas `None`
/// materializes all positive hits. Attribute cloning occurs only for retained
/// results.
///
/// # Examples
///
/// ```text
/// sequence 7: upsert p42 {content: "red shoe"}
/// sequence 8: delete p17; upsert p42 {content: "blue shoe"}
/// query: BM25(content, "blue"), top_k = 10
///
/// results        = [p42]
/// overriding_ids = {p42}
/// deleted_ids    = {p17}
///
/// final merge keeps p42's WAL score and suppresses old segment rows for
/// both p42 and p17.
/// ```
///
/// If the query tokenizes to nothing or references only unconfigured fields,
/// `results` is empty, but `p42` and `p17` still carry the same suppression
/// meaning.
///
/// # Rust Notes for Java/C Engineers
///
/// The input slice and expression/configuration values are borrowed, so this
/// function neither takes ownership nor can retain them after return. Internal
/// `&str` and attribute references point into `fragments`; Rust's lifetimes
/// prevent those pointers from escaping. `Option` makes the cache, filter, and
/// bound cases explicit instead of using null/sentinel values. The final
/// iterator consumes borrowed candidates and creates owned API values only
/// after top-k selection, avoiding Java-style eager object copies and manual C
/// ownership bookkeeping.
#[allow(clippy::too_many_arguments)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn wal_bm25_scan(
    fragments: &[LocatedWalFragment],
    logical_origin: &ArtifactOrigin,
    rank_by: &RankBy,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    last_as_prefix: bool,
    fts_cache: Option<&WalFtsCache>,
    filter: Option<&Filter>,
    include_attributes: bool,
    top_k: Option<usize>,
) -> WalBm25ScanResult {
    wal_bm25_scan_with_inputs(
        fragments,
        &[],
        logical_origin,
        rank_by,
        fts_configs,
        last_as_prefix,
        fts_cache,
        filter,
        include_attributes,
        top_k,
    )
}

/// Replays dense and typed-input WAL fragments in their shared manifest order.
#[allow(clippy::too_many_arguments)]
pub(crate) fn wal_bm25_scan_with_inputs(
    fragments: &[LocatedWalFragment],
    input_fragments: &[LocatedInputFragment],
    logical_origin: &ArtifactOrigin,
    rank_by: &RankBy,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    last_as_prefix: bool,
    fts_cache: Option<&WalFtsCache>,
    filter: Option<&Filter>,
    include_attributes: bool,
    top_k: Option<usize>,
) -> WalBm25ScanResult {
    let frag_count = fragments.len() + input_fragments.len();

    if fragments.is_empty() && input_fragments.is_empty() {
        return WalBm25ScanResult {
            results: Vec::new(),
            overriding_ids: HashSet::new(),
            fragment_count: 0,
            deleted_ids: HashSet::new(),
        };
    }

    // Replay in caller-supplied manifest order. The separate live and deleted
    // maps preserve enough state to suppress stale segment rows after scoring.
    let mut deleted_ids: HashSet<String> = HashSet::new();
    let mut latest_vectors: HashMap<&str, Option<&HashMap<String, AttributeValue>>> =
        HashMap::new();
    let mut latest_sequences: HashMap<&str, u64> = HashMap::new();

    enum ReplayFragment<'a> {
        Dense(&'a LocatedWalFragment),
        Input(&'a LocatedInputFragment),
    }
    impl ReplayFragment<'_> {
        fn sequence_number(&self) -> u64 {
            match self {
                Self::Dense(fragment) => fragment.sequence_number,
                Self::Input(fragment) => fragment.sequence_number,
            }
        }
    }
    let mut replay = fragments
        .iter()
        .map(ReplayFragment::Dense)
        .chain(input_fragments.iter().map(ReplayFragment::Input))
        .collect::<Vec<_>>();
    replay.sort_by_key(ReplayFragment::sequence_number);

    for fragment in &replay {
        match fragment {
            ReplayFragment::Dense(located) => {
                for del_id in &located.fragment.deletes {
                    deleted_ids.insert(del_id.clone());
                    latest_vectors.remove(del_id.as_str());
                    latest_sequences.remove(del_id.as_str());
                }
                for vector in &located.fragment.vectors {
                    deleted_ids.remove(&vector.id);
                    latest_vectors.insert(vector.id.as_str(), vector.attributes.as_ref());
                    latest_sequences.insert(vector.id.as_str(), located.sequence_number);
                }
            }
            ReplayFragment::Input(located) => {
                for del_id in &located.fragment.deletes {
                    deleted_ids.insert(del_id.clone());
                    latest_vectors.remove(del_id.as_str());
                    latest_sequences.remove(del_id.as_str());
                }
                for record in &located.fragment.upserts {
                    deleted_ids.remove(&record.id);
                    latest_vectors.insert(record.id.as_str(), record.attributes.as_ref());
                    latest_sequences.insert(record.id.as_str(), located.sequence_number);
                }
            }
        }
    }

    let overriding_ids: HashSet<String> = latest_vectors
        .keys()
        .map(|doc_id| (*doc_id).to_string())
        .collect();

    if latest_vectors.is_empty() {
        return WalBm25ScanResult {
            results: Vec::new(),
            overriding_ids,
            fragment_count: frag_count,
            deleted_ids,
        };
    }

    // Extract leaves before touching document text so unconfigured or empty
    // queries can return early while retaining replay suppression metadata.
    let field_queries = rank_by.extract_field_queries();

    /// Tokenized and configured state for one BM25 leaf in the ranking tree.
    struct FieldQueryState {
        /// Owned field name used to find document data and publish its score.
        field: String,
        /// Normalized exact terms, with the last term prefix-ready when requested.
        query_tokens: Vec<String>,
        /// Field-specific saturation and length-normalization parameters.
        params: Bm25Params,
    }

    let field_query_states: Vec<FieldQueryState> = field_queries
        .iter()
        .filter_map(|(field, query)| {
            let config = fts_configs.get(field)?;
            let tokens = tokenize_text(query, config, last_as_prefix);
            if tokens.is_empty() {
                return None;
            }
            Some(FieldQueryState {
                field: field.clone(),
                query_tokens: tokens,
                params: Bm25Params {
                    k1: config.k1,
                    b: config.b,
                },
            })
        })
        .collect();

    if field_query_states.is_empty() {
        return WalBm25ScanResult {
            results: Vec::new(),
            overriding_ids,
            fragment_count: frag_count,
            deleted_ids,
        };
    }

    // Tokenize each physical field once even if the expression references it
    // repeatedly. Hash-set order is irrelevant to final scoring.
    let fields_needed: Vec<&str> = field_query_states
        .iter()
        .map(|s| s.field.as_str())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    // Materialize the query-local document view. Cached values are cloned so no
    // DashMap guard or cache lifetime reaches the scoring phase.
    let mut doc_field_data: DocFieldData = HashMap::new();

    if let Some(cache) = fts_cache {
        // Cache hits avoid tokenizer CPU but still clone owned maps.
        for fragment in &replay {
            let (cached, sequence_number) = match fragment {
                ReplayFragment::Dense(fragment) => (
                    cache.get_or_tokenize(
                        logical_origin,
                        &fragment.identity,
                        fragment.fragment.as_ref(),
                        fts_configs,
                        &fields_needed,
                    ),
                    fragment.sequence_number,
                ),
                ReplayFragment::Input(fragment) => (
                    cache.get_or_tokenize_input(
                        logical_origin,
                        &fragment.identity,
                        fragment.fragment.as_ref(),
                        fts_configs,
                        &fields_needed,
                    ),
                    fragment.sequence_number,
                ),
            };
            for ((doc_id, field_name), token_data) in &cached.doc_field_data {
                // Only the fragment owning the final upsert may contribute
                // fields. This prevents a removed/non-string field in a newer
                // record from retaining stale tokens from an older cache entry.
                if latest_sequences.get(doc_id.as_str()) == Some(&sequence_number) {
                    doc_field_data.entry(doc_id.clone()).or_default().insert(
                        field_name.clone(),
                        (token_data.doc_length, token_data.term_freqs.clone()),
                    );
                }
            }
        }
    } else {
        // Without a cache, tokenize exactly the latest borrowed record per ID.
        for (doc_id, attrs_opt) in &latest_vectors {
            let attrs = match attrs_opt {
                Some(a) => a,
                None => continue,
            };

            for &field_name in &fields_needed {
                let config = match fts_configs.get(field_name) {
                    Some(c) => c,
                    None => continue,
                };

                let text = match attrs.get(field_name) {
                    Some(AttributeValue::String(s)) => s.as_str(),
                    _ => continue,
                };

                let tokens = tokenize_text(text, config, false);
                let doc_length = tokens.len() as u32;

                if doc_length == 0 {
                    continue;
                }

                let mut tf_map: HashMap<String, u32> = HashMap::new();
                for token in &tokens {
                    *tf_map.entry(token.clone()).or_insert(0) += 1;
                }

                doc_field_data
                    .entry((*doc_id).to_string())
                    .or_default()
                    .insert(field_name.to_string(), (doc_length, tf_map));
            }
        }
    }

    /// Aggregate BM25 corpus statistics for one field in the live WAL tail.
    struct CorpusStats {
        /// Number of live WAL documents that produced token data for this field.
        doc_count: u32,
        /// Mean retained token count, accumulated as a sum before finalization.
        avg_doc_length: f32,
        /// Number of field-bearing WAL documents containing each term.
        term_doc_freqs: HashMap<String, u32>,
    }

    let mut field_corpus_stats: HashMap<String, CorpusStats> = HashMap::new();

    for doc_data in doc_field_data.values() {
        for (field_name, (doc_length, tf_map)) in doc_data {
            let stats = field_corpus_stats
                .entry(field_name.clone())
                .or_insert_with(|| CorpusStats {
                    doc_count: 0,
                    avg_doc_length: 0.0,
                    term_doc_freqs: HashMap::new(),
                });
            stats.doc_count += 1;
            // Keep the running token total in the eventual average field to
            // avoid a second per-document accumulator map.
            stats.avg_doc_length += *doc_length as f32; // accumulate total

            for term in tf_map.keys() {
                *stats.term_doc_freqs.entry(term.clone()).or_insert(0) += 1;
            }
        }
    }

    // Convert accumulated token totals into means before scoring.
    for stats in field_corpus_stats.values_mut() {
        if stats.doc_count > 0 {
            stats.avg_doc_length /= stats.doc_count as f32;
        }
    }

    // Score borrowed candidates and delay owned ID/attribute clones until the
    // final retained set is known.
    let mut results: Vec<ScoredBm25Doc<'_>> = Vec::new();
    let mut top_results = top_k.map(|k| {
        TopK::new(
            k,
            bm25_scored_cmp as fn(&ScoredBm25Doc, &ScoredBm25Doc) -> Ordering,
        )
    });

    for (doc_id, attrs_opt) in &latest_vectors {
        if filter.is_some_and(|f| {
            !evaluate_filter_on_optional_attributes(f, attrs_opt.as_ref().copied())
        }) {
            continue;
        }

        let doc_data = doc_field_data.get(*doc_id);

        let mut field_scores: HashMap<String, f32> = HashMap::new();

        for fq_state in &field_query_states {
            let corpus = match field_corpus_stats.get(&fq_state.field) {
                Some(c) => c,
                None => continue,
            };

            let (doc_length, tf_map) = match doc_data.and_then(|d| d.get(&fq_state.field)) {
                Some(data) => data,
                None => continue,
            };

            let last_idx = fq_state.query_tokens.len().saturating_sub(1);
            let term_data: Vec<(f32, u32)> = fq_state
                .query_tokens
                .iter()
                .enumerate()
                .map(|(i, token)| {
                    if last_as_prefix && i == last_idx {
                        let mut total_tf = 0u32;
                        let mut total_df = 0u32;
                        for (doc_term, &freq) in tf_map.iter() {
                            if doc_term.starts_with(token.as_str()) {
                                total_tf += freq;
                                total_df = total_df
                                    .max(corpus.term_doc_freqs.get(doc_term).copied().unwrap_or(0));
                            }
                        }
                        let term_idf = bm25::idf(corpus.doc_count, total_df);
                        (term_idf, total_tf)
                    } else {
                        let global_df = corpus.term_doc_freqs.get(token).copied().unwrap_or(0);
                        let term_idf = bm25::idf(corpus.doc_count, global_df);
                        let tf = tf_map.get(token).copied().unwrap_or(0);
                        (term_idf, tf)
                    }
                })
                .collect();

            let score = bm25::bm25_score(
                &term_data,
                *doc_length,
                corpus.avg_doc_length,
                &fq_state.params,
            );
            field_scores.insert(fq_state.field.clone(), score);
        }

        let final_score = evaluate_rank_by(rank_by, &field_scores);
        if final_score > 0.0 {
            let result = ScoredBm25Doc {
                id: doc_id,
                score: final_score,
                attrs: *attrs_opt,
            };
            if let Some(top_results) = &mut top_results {
                top_results.push(result);
            } else {
                results.push(result);
            }
        }
    }

    let results = if let Some(top_results) = top_results {
        top_results.into_sorted_vec()
    } else {
        results.sort_by(bm25_scored_cmp);
        results
    };
    let results: Vec<SearchResult> = results
        .into_iter()
        .map(|scored| SearchResult {
            id: scored.id.to_string(),
            score: scored.score,
            attributes: if include_attributes {
                scored.attrs.cloned()
            } else {
                None
            },
        })
        .collect();

    debug!(
        surviving_vectors = overriding_ids.len(),
        topk_returned = results.len(),
        total_fragments = frag_count,
        "WAL BM25 scan complete"
    );

    WalBm25ScanResult {
        results,
        overriding_ids,
        fragment_count: frag_count,
        deleted_ids,
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Unit tests for WAL replay, cached reuse, scoring, and early-return contracts.
    //!
    //! These tests pass decoded fragments directly and therefore isolate pure
    //! scan behavior from manifest reads, S3/MinIO, checksum validation, and the
    //! downstream WAL/segment merge.

    use super::*;
    use crate::namespace::{NamespaceId, NamespaceIncarnationId};
    use crate::types::VectorEntry;
    use crate::wal::fragment::WalFragment;
    use crate::wal::manifest::LocatedFragmentIdentity;
    use std::sync::Arc;
    use ulid::Ulid;

    fn origin(namespace: &str, incarnation: u128) -> ArtifactOrigin {
        ArtifactOrigin {
            namespace: NamespaceId::parse(namespace).unwrap(),
            incarnation: NamespaceIncarnationId::from_uuid(uuid::Uuid::from_u128(incarnation)),
        }
    }

    /// Builds a fragment fixture with an independent ULID cache key.
    ///
    /// # Parameters
    ///
    /// - `vectors`: Upserts moved into the fragment.
    /// - `deletes`: Tombstone IDs moved into the fragment.
    ///
    /// # Returns
    ///
    /// A decoded fragment with an unused dummy checksum. Vector/delete overlap
    /// is not validated because the fixture bypasses production constructors.
    fn make_fragment(vectors: Vec<VectorEntry>, deletes: Vec<String>) -> LocatedWalFragment {
        make_fragment_at(origin("physical", 1), Ulid::new(), vectors, deletes)
    }

    fn make_fragment_at(
        physical_origin: ArtifactOrigin,
        id: Ulid,
        vectors: Vec<VectorEntry>,
        deletes: Vec<String>,
    ) -> LocatedWalFragment {
        LocatedWalFragment {
            identity: LocatedFragmentIdentity {
                physical_origin,
                id,
            },
            fragment: Arc::new(WalFragment {
                id,
                vectors,
                deletes,
                checksum: 0,
            }),
            sequence_number: 0,
        }
    }

    /// Builds one document whose searchable text lives in `content`.
    ///
    /// # Parameters
    ///
    /// - `id`: Logical ID copied into the fixture.
    /// - `text`: Content string copied into the attribute map.
    ///
    /// # Returns
    ///
    /// A vector entry with a placeholder coordinate and owned attributes.
    fn make_vec_entry(id: &str, text: &str) -> VectorEntry {
        let mut attrs = HashMap::new();
        attrs.insert(
            "content".to_string(),
            AttributeValue::String(text.to_string()),
        );
        VectorEntry {
            id: id.to_string(),
            values: vec![0.0],
            attributes: Some(attrs),
        }
    }

    /// Creates exact, deterministic tokenization settings for `content`.
    ///
    /// # Returns
    ///
    /// A one-field map with stemming and stop-word removal disabled.
    fn make_configs() -> HashMap<String, FtsFieldConfig> {
        let mut configs = HashMap::new();
        configs.insert(
            "content".to_string(),
            FtsFieldConfig {
                stemming: false,
                remove_stopwords: false,
                ..Default::default()
            },
        );
        configs
    }

    #[test]
    /// Protects basic positive matching, result order, and override completeness.
    ///
    /// The nonmatching `v3` must still override any older segment copy even
    /// though only `v1` and `v2` become scored hits.
    fn test_wal_scan_basic() {
        let fragments = vec![make_fragment(
            vec![
                make_vec_entry("v1", "cat dog"),
                make_vec_entry("v2", "cat bird"),
                make_vec_entry("v3", "fish"),
            ],
            vec![],
        )];

        let rank_by = RankBy::Bm25 {
            field: "content".to_string(),
            query: "cat".to_string(),
        };

        let result = wal_bm25_scan(
            &fragments,
            &origin("logical", 100),
            &rank_by,
            &make_configs(),
            false,
            None,
            None,
            true,
            None,
        );
        assert_eq!(result.fragment_count, 1);
        assert_eq!(result.results.len(), 2); // v1 and v2 contain "cat"
        assert_eq!(result.overriding_ids.len(), 3);
        assert!(result.overriding_ids.contains("v3"));
        assert!(result.results[0].score >= result.results[1].score);
    }

    #[test]
    /// Protects result parity and stable entry count across a repeated cached scan.
    ///
    /// The first call populates one fragment entry and the second reuses it
    /// without changing the ranked result set.
    fn test_wal_scan_with_cache() {
        let fragments = vec![make_fragment(
            vec![
                make_vec_entry("v1", "cat dog"),
                make_vec_entry("v2", "cat bird"),
                make_vec_entry("v3", "fish"),
            ],
            vec![],
        )];

        let rank_by = RankBy::Bm25 {
            field: "content".to_string(),
            query: "cat".to_string(),
        };

        let cache = WalFtsCache::new();

        // The first scan exercises the tokenization-and-insert miss path.
        let result1 = wal_bm25_scan(
            &fragments,
            &origin("logical", 100),
            &rank_by,
            &make_configs(),
            false,
            Some(&cache),
            None,
            true,
            None,
        );
        assert_eq!(result1.results.len(), 2);
        assert_eq!(cache.len(), 1);

        // The identical second scan exercises the deep-cloned hit path.
        let result2 = wal_bm25_scan(
            &fragments,
            &origin("logical", 100),
            &rank_by,
            &make_configs(),
            false,
            Some(&cache),
            None,
            true,
            None,
        );
        assert_eq!(result2.results.len(), 2);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    /// Protects newest-fragment tombstone replay before lexical scoring.
    ///
    /// A regression would leak deleted `v1` as either a WAL hit or a live
    /// override that could displace the segment tombstone behavior.
    fn test_wal_scan_with_deletes() {
        let fragments = vec![
            make_fragment(
                vec![
                    make_vec_entry("v1", "cat dog"),
                    make_vec_entry("v2", "cat bird"),
                ],
                vec![],
            ),
            make_fragment(vec![], vec!["v1".to_string()]),
        ];

        let rank_by = RankBy::Bm25 {
            field: "content".to_string(),
            query: "cat".to_string(),
        };

        let result = wal_bm25_scan(
            &fragments,
            &origin("logical", 100),
            &rank_by,
            &make_configs(),
            false,
            None,
            None,
            true,
            None,
        );
        assert_eq!(result.results.len(), 1); // v1 was deleted
        assert_eq!(result.overriding_ids, HashSet::from(["v2".to_string()]));
        assert_eq!(result.results[0].id, "v2");
    }

    #[test]
    /// Protects the zero-work result for an empty manifest-selected WAL slice.
    ///
    /// No hits, suppression IDs, tombstones, or scanned fragments should be
    /// reported.
    fn test_wal_scan_empty_fragments() {
        let result = wal_bm25_scan(
            &[] as &[LocatedWalFragment],
            &origin("logical", 100),
            &RankBy::Bm25 {
                field: "content".to_string(),
                query: "cat".to_string(),
            },
            &make_configs(),
            false,
            None,
            None,
            true,
            None,
        );
        assert!(result.results.is_empty());
        assert_eq!(result.fragment_count, 0);
    }

    #[test]
    /// Protects suppression metadata when query normalization yields no tokens.
    ///
    /// Even with no scored hit, live `v1` must remain an override so strong
    /// merge cannot resurrect its older segment version.
    fn test_wal_scan_empty_query() {
        let fragments = vec![make_fragment(vec![make_vec_entry("v1", "cat dog")], vec![])];

        let rank_by = RankBy::Bm25 {
            field: "content".to_string(),
            query: "".to_string(),
        };

        let result = wal_bm25_scan(
            &fragments,
            &origin("logical", 100),
            &rank_by,
            &make_configs(),
            false,
            None,
            None,
            true,
            None,
        );
        assert!(result.results.is_empty());
        assert_eq!(result.overriding_ids, HashSet::from(["v1".to_string()]));
    }

    #[test]
    /// Protects multi-field score composition through a `RankBy::Sum` expression.
    ///
    /// The fixture has searchable text in both fields; losing either field's
    /// token data or expression traversal would invalidate its positive result.
    fn test_wal_scan_multi_field_sum() {
        let fragments = vec![make_fragment(
            vec![{
                let mut attrs = HashMap::new();
                attrs.insert(
                    "title".to_string(),
                    AttributeValue::String("cat".to_string()),
                );
                attrs.insert(
                    "content".to_string(),
                    AttributeValue::String("the cat sat on a mat".to_string()),
                );
                VectorEntry {
                    id: "v1".to_string(),
                    values: vec![0.0],
                    attributes: Some(attrs),
                }
            }],
            vec![],
        )];

        let rank_by = RankBy::Sum(vec![
            RankBy::Bm25 {
                field: "title".to_string(),
                query: "cat".to_string(),
            },
            RankBy::Bm25 {
                field: "content".to_string(),
                query: "cat".to_string(),
            },
        ]);

        let mut configs = make_configs();
        configs.insert(
            "title".to_string(),
            FtsFieldConfig {
                stemming: false,
                remove_stopwords: false,
                ..Default::default()
            },
        );

        let result = wal_bm25_scan(
            &fragments,
            &origin("logical", 100),
            &rank_by,
            &configs,
            false,
            None,
            None,
            true,
            None,
        );
        assert_eq!(result.results.len(), 1);
        assert!(result.results[0].score > 0.0);
    }

    #[test]
    fn cached_scan_keeps_equal_ulids_from_different_origins_distinct() {
        let shared_id = Ulid::new();
        let fragments = vec![
            make_fragment_at(
                origin("source", 1),
                shared_id,
                vec![make_vec_entry("source-doc", "apple")],
                Vec::new(),
            ),
            make_fragment_at(
                origin("target", 2),
                shared_id,
                vec![make_vec_entry("target-doc", "banana")],
                Vec::new(),
            ),
        ];
        let cache = WalFtsCache::new();

        let result = wal_bm25_scan(
            &fragments,
            &origin("logical", 100),
            &RankBy::Bm25 {
                field: "content".to_string(),
                query: "banana".to_string(),
            },
            &make_configs(),
            false,
            Some(&cache),
            None,
            false,
            None,
        );

        assert_eq!(result.results.len(), 1);
        assert_eq!(result.results[0].id, "target-doc");
        assert_eq!(cache.len(), 2);
    }
}
