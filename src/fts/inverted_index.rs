//! Inverted index for BM25 full-text search.
//!
//! Each cluster in a segment gets its own inverted index, serialized with
//! the `ZFTS` magic bytes. Structure:
//!
//! ```text
//! [4 bytes: "ZFTS"] [1 byte: version] [JSON payload]
//! ```

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::{Result, ZeppelinError};
use crate::fts::bm25::{self, Bm25Params};
use crate::fts::tokenizer::tokenize_text;
use crate::fts::types::FtsFieldConfig;
use crate::types::AttributeValue;

/// Magic bytes for FTS inverted index files.
const ZFTS_MAGIC: &[u8; 4] = b"ZFTS";
/// Current version of the FTS index format.
const ZFTS_VERSION: u8 = 1;

/// Per-cluster inverted index covering all FTS-configured fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvertedIndex {
    /// Number of vectors (documents) in this cluster.
    pub vector_count: u32,
    /// Per-field index data.
    pub fields: BTreeMap<String, FieldIndex>,
}

/// Inverted index data for a single text field within a cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldIndex {
    /// Average document length (in tokens) for this field in the cluster.
    pub avg_doc_length: f32,
    /// Total number of documents that have this field.
    pub doc_count: u32,
    /// term → posting list
    pub postings: BTreeMap<String, PostingList>,
}

/// Posting list for a single term.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostingList {
    /// Document frequency: how many docs in the cluster contain this term.
    pub df: u32,
    /// Individual postings, sorted by position.
    pub entries: Vec<Posting>,
}

/// A single posting: a document (vector position) and its term frequency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Posting {
    /// Vector position within the cluster (0-based).
    pub position: u32,
    /// Term frequency in this document for this field.
    pub tf: u32,
}

impl InvertedIndex {
    /// Build an inverted index for a cluster from attribute data.
    ///
    /// `attrs` is the attribute map for each vector in the cluster (by position).
    /// `fts_configs` maps field name → config for fields that should be indexed.
    #[must_use]
    pub fn build(
        attrs: &[Option<&HashMap<String, AttributeValue>>],
        fts_configs: &HashMap<String, FtsFieldConfig>,
    ) -> Self {
        let vector_count = attrs.len() as u32;
        let mut fields = BTreeMap::new();

        for (field_name, config) in fts_configs {
            let field_index = build_field_index(attrs, field_name, config);
            if field_index.doc_count > 0 {
                fields.insert(field_name.clone(), field_index);
            }
        }

        Self {
            vector_count,
            fields,
        }
    }

    /// Search the inverted index for query terms in a specific field.
    ///
    /// Returns `(position, score)` pairs for all matching documents,
    /// sorted by score descending (higher = more relevant).
    #[must_use]
    pub fn search(
        &self,
        field: &str,
        query_tokens: &[String],
        params: &Bm25Params,
    ) -> Vec<(u32, f32)> {
        let Some(field_index) = self.fields.get(field) else {
            return Vec::new();
        };

        // Compute IDF for each query token
        let token_idfs: Vec<(String, f32)> = query_tokens
            .iter()
            .filter_map(|token| {
                field_index.postings.get(token).map(|pl| {
                    let term_idf = bm25::idf(field_index.doc_count, pl.df);
                    (token.clone(), term_idf)
                })
            })
            .collect();

        if token_idfs.is_empty() {
            return Vec::new();
        }

        // Collect per-document scores
        let mut doc_scores: HashMap<u32, f32> = HashMap::new();
        // Also need doc lengths per position
        let doc_lengths = compute_doc_lengths(field_index);

        for (token, term_idf) in &token_idfs {
            if let Some(posting_list) = field_index.postings.get(token) {
                for posting in &posting_list.entries {
                    let dl = doc_lengths.get(&posting.position).copied().unwrap_or(0);
                    let term_score = bm25::bm25_term_score(
                        *term_idf,
                        posting.tf,
                        dl,
                        field_index.avg_doc_length,
                        params,
                    );
                    *doc_scores.entry(posting.position).or_insert(0.0) += term_score;
                }
            }
        }

        let mut results: Vec<(u32, f32)> = doc_scores.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    /// Search with prefix matching on the last query token.
    #[must_use]
    pub fn search_prefix(
        &self,
        field: &str,
        query_tokens: &[String],
        params: &Bm25Params,
    ) -> Vec<(u32, f32)> {
        let Some(field_index) = self.fields.get(field) else {
            return Vec::new();
        };

        if query_tokens.is_empty() {
            return Vec::new();
        }

        // Split into exact tokens and prefix token
        let (exact_tokens, prefix_token) = query_tokens.split_at(query_tokens.len() - 1);

        // Expand prefix: find all terms that start with the prefix
        let prefix = &prefix_token[0];
        let prefix_matches: Vec<String> = field_index
            .postings
            .keys()
            .filter(|k| k.starts_with(prefix.as_str()))
            .cloned()
            .collect();

        if prefix_matches.is_empty() && exact_tokens.is_empty() {
            return Vec::new();
        }

        // Combine exact + all prefix-expanded terms
        let mut all_tokens: Vec<String> = exact_tokens.to_vec();
        all_tokens.extend(prefix_matches);

        self.search(field, &all_tokens, params)
    }

    /// Serialize to bytes with ZFTS magic header.
    pub fn to_bytes(&self) -> Result<Bytes> {
        let json = serde_json::to_vec(self)?;
        let mut buf = Vec::with_capacity(5 + json.len());
        buf.extend_from_slice(ZFTS_MAGIC);
        buf.push(ZFTS_VERSION);
        buf.extend_from_slice(&json);
        Ok(Bytes::from(buf))
    }

    /// Deserialize from bytes, validating magic header.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 5 {
            return Err(ZeppelinError::Index("FTS index data too short".to_string()));
        }
        if &data[0..4] != ZFTS_MAGIC {
            return Err(ZeppelinError::Index(format!(
                "invalid FTS index magic: expected ZFTS, got {:?}",
                &data[0..4]
            )));
        }
        let version = data[4];
        if version != ZFTS_VERSION {
            return Err(ZeppelinError::Index(format!(
                "unsupported FTS index version: {version}"
            )));
        }
        let index: Self = serde_json::from_slice(&data[5..])?;
        Ok(index)
    }
}

/// S3 key for the FTS inverted index of a specific cluster.
pub fn fts_index_key(namespace: &str, segment_id: &str, cluster_idx: usize) -> String {
    format!("{namespace}/segments/{segment_id}/fts_index_{cluster_idx}.bin")
}

/// S3 key for the FTS metadata of a segment.
pub fn fts_meta_key(namespace: &str, segment_id: &str) -> String {
    format!("{namespace}/segments/{segment_id}/fts_meta.json")
}

/// Segment-level FTS metadata, stored as fts_meta.json.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FtsSegmentMeta {
    /// Which fields are FTS-indexed in this segment.
    pub fields: Vec<String>,
    /// Total document count across all clusters.
    pub total_docs: u32,
    /// Per-field global stats for cross-cluster IDF normalization.
    pub field_stats: BTreeMap<String, FtsFieldStats>,
}

/// Global statistics for a single FTS field across the segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FtsFieldStats {
    /// Total documents with this field.
    pub doc_count: u32,
    /// Average document length.
    pub avg_doc_length: f32,
    /// term → global document frequency.
    pub term_doc_freqs: BTreeMap<String, u32>,
}

impl FtsSegmentMeta {
    /// Build segment-level FTS metadata by aggregating cluster-level indexes.
    pub fn from_cluster_indexes(
        cluster_indexes: &[InvertedIndex],
        fts_field_names: &[String],
    ) -> Self {
        let total_docs: u32 = cluster_indexes.iter().map(|idx| idx.vector_count).sum();
        let mut field_stats = BTreeMap::new();

        for field_name in fts_field_names {
            let mut doc_count: u32 = 0;
            let mut total_length: f64 = 0.0;
            let mut term_doc_freqs: BTreeMap<String, u32> = BTreeMap::new();

            for idx in cluster_indexes {
                if let Some(fi) = idx.fields.get(field_name) {
                    doc_count += fi.doc_count;
                    total_length += fi.avg_doc_length as f64 * fi.doc_count as f64;
                    for (term, pl) in &fi.postings {
                        *term_doc_freqs.entry(term.clone()).or_insert(0) += pl.df;
                    }
                }
            }

            let avg_doc_length = if doc_count > 0 {
                (total_length / doc_count as f64) as f32
            } else {
                0.0
            };

            field_stats.insert(
                field_name.clone(),
                FtsFieldStats {
                    doc_count,
                    avg_doc_length,
                    term_doc_freqs,
                },
            );
        }

        Self {
            fields: fts_field_names.to_vec(),
            total_docs,
            field_stats,
        }
    }

    /// Serialize to JSON bytes.
    pub fn to_bytes(&self) -> Result<Bytes> {
        let json = serde_json::to_vec_pretty(self)?;
        Ok(Bytes::from(json))
    }

    /// Deserialize from JSON bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(data)?)
    }

    /// Search across all clusters using global IDF stats.
    ///
    /// Returns `(cluster_idx, position, score)` triples.
    pub fn search_global(
        &self,
        field: &str,
        query_tokens: &[String],
        params: &Bm25Params,
        cluster_indexes: &[(usize, InvertedIndex)],
    ) -> Vec<(usize, u32, f32)> {
        let Some(stats) = self.field_stats.get(field) else {
            return Vec::new();
        };

        // Compute global IDF for each query token
        let token_idfs: Vec<(&str, f32)> = query_tokens
            .iter()
            .filter_map(|token| {
                stats.term_doc_freqs.get(token).map(|&global_df| {
                    let term_idf = bm25::idf(stats.doc_count, global_df);
                    (token.as_str(), term_idf)
                })
            })
            .collect();

        if token_idfs.is_empty() {
            return Vec::new();
        }

        let mut results = Vec::new();

        for (cluster_idx, idx) in cluster_indexes {
            let Some(field_index) = idx.fields.get(field) else {
                continue;
            };

            let doc_lengths = compute_doc_lengths(field_index);

            let mut doc_scores: HashMap<u32, f32> = HashMap::new();
            for (token, term_idf) in &token_idfs {
                if let Some(pl) = field_index.postings.get(*token) {
                    for posting in &pl.entries {
                        let dl = doc_lengths.get(&posting.position).copied().unwrap_or(0);
                        let score = bm25::bm25_term_score(
                            *term_idf,
                            posting.tf,
                            dl,
                            stats.avg_doc_length,
                            params,
                        );
                        *doc_scores.entry(posting.position).or_insert(0.0) += score;
                    }
                }
            }

            for (position, score) in doc_scores {
                results.push((*cluster_idx, position, score));
            }
        }

        results.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        results
    }
}

/// Build the inverted index for a single field across all vectors in a cluster.
fn build_field_index(
    attrs: &[Option<&HashMap<String, AttributeValue>>],
    field_name: &str,
    config: &FtsFieldConfig,
) -> FieldIndex {
    let mut postings: BTreeMap<String, Vec<Posting>> = BTreeMap::new();
    let mut doc_count: u32 = 0;
    let mut total_tokens: u64 = 0;
    let mut doc_lengths: Vec<u32> = Vec::new();

    for (position, attr_opt) in attrs.iter().enumerate() {
        let text = attr_opt
            .and_then(|a| a.get(field_name))
            .and_then(|v| match v {
                AttributeValue::String(s) => Some(s.as_str()),
                _ => None,
            });

        let Some(text) = text else {
            doc_lengths.push(0);
            continue;
        };

        let tokens = tokenize_text(text, config, false);
        let token_count = tokens.len() as u32;
        doc_lengths.push(token_count);

        if token_count == 0 {
            continue;
        }

        doc_count += 1;
        total_tokens += token_count as u64;

        // Count term frequencies
        let mut tf_map: HashMap<String, u32> = HashMap::new();
        for token in &tokens {
            *tf_map.entry(token.clone()).or_insert(0) += 1;
        }

        // Add to posting lists
        for (term, tf) in tf_map {
            postings.entry(term).or_default().push(Posting {
                position: position as u32,
                tf,
            });
        }
    }

    // Sort each posting list by position
    for entries in postings.values_mut() {
        entries.sort_by_key(|p| p.position);
    }

    // Convert to PostingList structs
    let postings: BTreeMap<String, PostingList> = postings
        .into_iter()
        .map(|(term, entries)| {
            let df = entries.len() as u32;
            (term, PostingList { df, entries })
        })
        .collect();

    let avg_doc_length = if doc_count > 0 {
        total_tokens as f32 / doc_count as f32
    } else {
        0.0
    };

    FieldIndex {
        avg_doc_length,
        doc_count,
        postings,
    }
}

/// Compute the total token count (doc length) for each document position
/// by summing all term frequencies.
fn compute_doc_lengths(field_index: &FieldIndex) -> HashMap<u32, u32> {
    let mut lengths: HashMap<u32, u32> = HashMap::new();
    for pl in field_index.postings.values() {
        for posting in &pl.entries {
            *lengths.entry(posting.position).or_insert(0) += posting.tf;
        }
    }
    lengths
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> HashMap<String, FtsFieldConfig> {
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

    fn make_attrs(texts: &[&str]) -> Vec<Option<HashMap<String, AttributeValue>>> {
        texts
            .iter()
            .map(|t| {
                let mut m = HashMap::new();
                m.insert("content".to_string(), AttributeValue::String(t.to_string()));
                Some(m)
            })
            .collect()
    }

    #[test]
    fn test_build_basic() {
        let attrs = make_attrs(&["hello world", "hello rust", "world of rust"]);
        let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
            attrs.iter().map(|a| a.as_ref()).collect();
        let idx = InvertedIndex::build(&attr_refs, &make_config());

        assert_eq!(idx.vector_count, 3);
        assert!(idx.fields.contains_key("content"));

        let fi = &idx.fields["content"];
        assert_eq!(fi.doc_count, 3);
        assert!(fi.postings.contains_key("hello"));
        assert!(fi.postings.contains_key("world"));
        assert!(fi.postings.contains_key("rust"));

        // "hello" appears in 2 docs
        assert_eq!(fi.postings["hello"].df, 2);
        // "rust" appears in 2 docs
        assert_eq!(fi.postings["rust"].df, 2);
    }

    #[test]
    fn test_build_multiple_fields() {
        let mut configs = HashMap::new();
        configs.insert(
            "title".to_string(),
            FtsFieldConfig {
                stemming: false,
                remove_stopwords: false,
                ..Default::default()
            },
        );
        configs.insert(
            "body".to_string(),
            FtsFieldConfig {
                stemming: false,
                remove_stopwords: false,
                ..Default::default()
            },
        );

        let attrs: Vec<Option<HashMap<String, AttributeValue>>> = vec![{
            let mut m = HashMap::new();
            m.insert(
                "title".to_string(),
                AttributeValue::String("hello".to_string()),
            );
            m.insert(
                "body".to_string(),
                AttributeValue::String("world of code".to_string()),
            );
            Some(m)
        }];
        let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
            attrs.iter().map(|a| a.as_ref()).collect();

        let idx = InvertedIndex::build(&attr_refs, &configs);
        assert!(idx.fields.contains_key("title"));
        assert!(idx.fields.contains_key("body"));
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let attrs = make_attrs(&["hello world", "foo bar"]);
        let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
            attrs.iter().map(|a| a.as_ref()).collect();
        let idx = InvertedIndex::build(&attr_refs, &make_config());

        let bytes = idx.to_bytes().unwrap();
        let restored = InvertedIndex::from_bytes(&bytes).unwrap();

        assert_eq!(restored.vector_count, idx.vector_count);
        assert_eq!(restored.fields.len(), idx.fields.len());
    }

    #[test]
    fn test_magic_byte_validation() {
        let result = InvertedIndex::from_bytes(b"BAAD\x01{}");
        assert!(result.is_err());
    }

    #[test]
    fn test_posting_list_sorted() {
        let attrs = make_attrs(&["alpha", "beta alpha", "gamma", "alpha beta"]);
        let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
            attrs.iter().map(|a| a.as_ref()).collect();
        let idx = InvertedIndex::build(&attr_refs, &make_config());

        let fi = &idx.fields["content"];
        let alpha_postings = &fi.postings["alpha"];
        // Should be sorted by position
        for w in alpha_postings.entries.windows(2) {
            assert!(w[0].position < w[1].position);
        }
    }

    #[test]
    fn test_idf_stats() {
        let attrs = make_attrs(&["cat dog", "cat", "dog bird"]);
        let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
            attrs.iter().map(|a| a.as_ref()).collect();
        let idx = InvertedIndex::build(&attr_refs, &make_config());

        let fi = &idx.fields["content"];
        assert_eq!(fi.doc_count, 3);
        assert_eq!(fi.postings["cat"].df, 2);
        assert_eq!(fi.postings["dog"].df, 2);
        assert_eq!(fi.postings["bird"].df, 1);
    }

    #[test]
    fn test_empty_corpus() {
        let attrs: Vec<Option<HashMap<String, AttributeValue>>> = vec![];
        let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
            attrs.iter().map(|a| a.as_ref()).collect();
        let idx = InvertedIndex::build(&attr_refs, &make_config());
        assert_eq!(idx.vector_count, 0);
        assert!(idx.fields.is_empty());
    }

    #[test]
    fn test_single_doc() {
        let attrs = make_attrs(&["hello hello world"]);
        let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
            attrs.iter().map(|a| a.as_ref()).collect();
        let idx = InvertedIndex::build(&attr_refs, &make_config());

        let fi = &idx.fields["content"];
        assert_eq!(fi.doc_count, 1);
        assert_eq!(fi.postings["hello"].entries[0].tf, 2);
        assert_eq!(fi.postings["world"].entries[0].tf, 1);
    }

    #[test]
    fn test_search_single_term() {
        let attrs = make_attrs(&["cat dog", "cat", "dog bird"]);
        let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
            attrs.iter().map(|a| a.as_ref()).collect();
        let idx = InvertedIndex::build(&attr_refs, &make_config());

        let params = Bm25Params::default();
        let results = idx.search("content", &["cat".to_string()], &params);
        assert_eq!(results.len(), 2); // 2 docs contain "cat"
        assert!(results[0].1 >= results[1].1); // sorted by score desc
    }

    #[test]
    fn test_search_multi_term() {
        let attrs = make_attrs(&["cat dog", "cat", "dog bird", "cat dog bird"]);
        let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
            attrs.iter().map(|a| a.as_ref()).collect();
        let idx = InvertedIndex::build(&attr_refs, &make_config());

        let params = Bm25Params::default();
        let results = idx.search("content", &["cat".to_string(), "dog".to_string()], &params);
        // Doc 0 ("cat dog") and Doc 3 ("cat dog bird") match both terms
        assert!(results.len() >= 2);
        // Docs matching both terms should score highest
        let top_positions: Vec<u32> = results.iter().take(2).map(|r| r.0).collect();
        assert!(top_positions.contains(&0) || top_positions.contains(&3));
    }

    #[test]
    fn test_search_prefix() {
        let attrs = make_attrs(&["program programming", "test", "programmer"]);
        let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
            attrs.iter().map(|a| a.as_ref()).collect();
        let idx = InvertedIndex::build(&attr_refs, &make_config());

        let params = Bm25Params::default();
        let results = idx.search_prefix("content", &["prog".to_string()], &params);
        // Should match docs containing terms starting with "prog"
        assert!(results.len() >= 2);
    }

    #[test]
    fn test_search_no_matches() {
        let attrs = make_attrs(&["hello world"]);
        let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
            attrs.iter().map(|a| a.as_ref()).collect();
        let idx = InvertedIndex::build(&attr_refs, &make_config());

        let params = Bm25Params::default();
        let results = idx.search("content", &["nonexistent".to_string()], &params);
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_missing_field() {
        let attrs = make_attrs(&["hello"]);
        let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
            attrs.iter().map(|a| a.as_ref()).collect();
        let idx = InvertedIndex::build(&attr_refs, &make_config());

        let params = Bm25Params::default();
        let results = idx.search("nonexistent_field", &["hello".to_string()], &params);
        assert!(results.is_empty());
    }
}
