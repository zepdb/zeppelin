//! Brute-force BM25 scoring over uncompacted WAL fragments.
//!
//! Same pattern as vector WAL scan in `src/query.rs`: read fragments from
//! manifest snapshot, dedup, apply deletes, tokenize on the fly, score.
//!
//! When a `WalFtsCache` is provided, pre-tokenized data is reused across
//! queries instead of re-tokenizing every document on every query.

use std::collections::{HashMap, HashSet};

use tracing::debug;

use crate::fts::bm25::{self, Bm25Params};
use crate::fts::rank_by::{evaluate_rank_by, RankBy};
use crate::fts::tokenizer::tokenize_text;
use crate::fts::types::FtsFieldConfig;
use crate::fts::wal_cache::WalFtsCache;
use crate::types::{AttributeValue, SearchResult};
use crate::wal::fragment::WalFragment;

/// Per-doc, per-field data: (doc_length, term→term_frequency).
type DocFieldData = HashMap<String, HashMap<String, (u32, HashMap<String, u32>)>>;

/// Result of a WAL BM25 scan.
pub struct WalBm25ScanResult {
    pub results: Vec<SearchResult>,
    pub fragment_count: usize,
    /// IDs that were explicitly deleted in the WAL.
    /// Used by the merge step to exclude these from segment results.
    pub deleted_ids: HashSet<String>,
}

/// Brute-force BM25 scan over WAL fragments.
///
/// 1. Deduplicate / apply deletes (latest fragment wins)
/// 2. For each surviving doc, extract text fields, tokenize, build ephemeral stats
/// 3. Score via BM25
/// 4. Evaluate rank_by expression
/// 5. Return sorted results (higher score = better)
///
/// When `fts_cache` is provided, tokenization results are cached per-fragment
/// and reused across queries, eliminating the dominant CPU cost.
///
/// When `top_k` is provided, results are truncated to the top K after scoring.
/// This enables callers to limit work in the merge phase.
pub fn wal_bm25_scan(
    fragments: &[WalFragment],
    rank_by: &RankBy,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    last_as_prefix: bool,
    fts_cache: Option<&WalFtsCache>,
    top_k: Option<usize>,
) -> WalBm25ScanResult {
    let frag_count = fragments.len();

    if fragments.is_empty() {
        return WalBm25ScanResult {
            results: Vec::new(),
            fragment_count: 0,
            deleted_ids: HashSet::new(),
        };
    }

    // 1. Dedup: latest fragment wins, apply deletes
    let mut deleted_ids: HashSet<String> = HashSet::new();
    let mut latest_vectors: HashMap<String, Option<HashMap<String, AttributeValue>>> =
        HashMap::new();

    for fragment in fragments {
        for del_id in &fragment.deletes {
            deleted_ids.insert(del_id.clone());
            latest_vectors.remove(del_id);
        }
        for vec in &fragment.vectors {
            deleted_ids.remove(&vec.id);
            latest_vectors.insert(vec.id.clone(), vec.attributes.clone());
        }
    }

    if latest_vectors.is_empty() {
        return WalBm25ScanResult {
            results: Vec::new(),
            fragment_count: frag_count,
            deleted_ids,
        };
    }

    // 2. Extract (field, query) pairs from rank_by
    let field_queries = rank_by.extract_field_queries();

    // 3. For each (field, query), tokenize the query, then build ephemeral corpus stats
    // and score each document
    struct FieldQueryState {
        field: String,
        query_tokens: Vec<String>,
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
            fragment_count: frag_count,
            deleted_ids,
        };
    }

    // Gather all unique fields we need to index
    let fields_needed: Vec<&str> = field_query_states
        .iter()
        .map(|s| s.field.as_str())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    // 4. Build per-doc, per-field tokenized data — using cache when available
    let mut doc_field_data: DocFieldData = HashMap::new();

    if let Some(cache) = fts_cache {
        // Fast path: use cached pre-tokenized data
        for fragment in fragments {
            let cached = cache.get_or_tokenize(fragment, fts_configs, &fields_needed);
            for ((doc_id, field_name), token_data) in &cached.doc_field_data {
                // Only include docs that survived dedup
                if latest_vectors.contains_key(doc_id) {
                    doc_field_data.entry(doc_id.clone()).or_default().insert(
                        field_name.clone(),
                        (token_data.doc_length, token_data.term_freqs.clone()),
                    );
                }
            }
        }
    } else {
        // Slow path: tokenize inline (no cache)
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
                    .entry(doc_id.clone())
                    .or_default()
                    .insert(field_name.to_string(), (doc_length, tf_map));
            }
        }
    }

    // 5. Build ephemeral corpus stats per field
    struct CorpusStats {
        doc_count: u32,
        avg_doc_length: f32,
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
            stats.avg_doc_length += *doc_length as f32; // accumulate total

            for term in tf_map.keys() {
                *stats.term_doc_freqs.entry(term.clone()).or_insert(0) += 1;
            }
        }
    }

    // Finalize avg doc length
    for stats in field_corpus_stats.values_mut() {
        if stats.doc_count > 0 {
            stats.avg_doc_length /= stats.doc_count as f32;
        }
    }

    // 6. Score each document
    let mut results: Vec<SearchResult> = Vec::new();

    for (doc_id, attrs_opt) in &latest_vectors {
        let doc_data = doc_field_data.get(doc_id);

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
            results.push(SearchResult {
                id: doc_id.clone(),
                score: final_score,
                attributes: attrs_opt.clone(),
            });
        }
    }

    // Sort by score descending (higher = better for BM25)
    results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // Early termination: truncate to top_k if specified
    if let Some(k) = top_k {
        results.truncate(k);
    }

    debug!(
        surviving_vectors = results.len(),
        total_fragments = frag_count,
        "WAL BM25 scan complete"
    );

    WalBm25ScanResult {
        results,
        fragment_count: frag_count,
        deleted_ids,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::VectorEntry;
    use ulid::Ulid;

    fn make_fragment(vectors: Vec<VectorEntry>, deletes: Vec<String>) -> WalFragment {
        WalFragment {
            id: Ulid::new(),
            vectors,
            deletes,
            checksum: 0,
        }
    }

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

        let result = wal_bm25_scan(&fragments, &rank_by, &make_configs(), false, None, None);
        assert_eq!(result.fragment_count, 1);
        assert_eq!(result.results.len(), 2); // v1 and v2 contain "cat"
        assert!(result.results[0].score >= result.results[1].score);
    }

    #[test]
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

        // First scan (populates cache)
        let result1 = wal_bm25_scan(
            &fragments,
            &rank_by,
            &make_configs(),
            false,
            Some(&cache),
            None,
        );
        assert_eq!(result1.results.len(), 2);
        assert_eq!(cache.len(), 1);

        // Second scan (uses cache)
        let result2 = wal_bm25_scan(
            &fragments,
            &rank_by,
            &make_configs(),
            false,
            Some(&cache),
            None,
        );
        assert_eq!(result2.results.len(), 2);
        assert_eq!(cache.len(), 1);
    }

    #[test]
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

        let result = wal_bm25_scan(&fragments, &rank_by, &make_configs(), false, None, None);
        assert_eq!(result.results.len(), 1); // v1 was deleted
        assert_eq!(result.results[0].id, "v2");
    }

    #[test]
    fn test_wal_scan_empty_fragments() {
        let result = wal_bm25_scan(
            &[],
            &RankBy::Bm25 {
                field: "content".to_string(),
                query: "cat".to_string(),
            },
            &make_configs(),
            false,
            None,
            None,
        );
        assert!(result.results.is_empty());
        assert_eq!(result.fragment_count, 0);
    }

    #[test]
    fn test_wal_scan_empty_query() {
        let fragments = vec![make_fragment(vec![make_vec_entry("v1", "cat dog")], vec![])];

        let rank_by = RankBy::Bm25 {
            field: "content".to_string(),
            query: "".to_string(),
        };

        let result = wal_bm25_scan(&fragments, &rank_by, &make_configs(), false, None, None);
        assert!(result.results.is_empty());
    }

    #[test]
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

        let result = wal_bm25_scan(&fragments, &rank_by, &configs, false, None, None);
        assert_eq!(result.results.len(), 1);
        assert!(result.results[0].score > 0.0);
    }
}
