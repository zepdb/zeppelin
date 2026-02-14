//! Global (per-segment) inverted index for BM25 full-text search.
//!
//! Instead of scanning all N clusters to find matching documents, the
//! global index maps `term → [(cluster_idx, position, tf)]` across the
//! entire segment. A BM25 query loads one ~50KB file instead of N
//! per-cluster indexes.
//!
//! Serialized with `ZGFTS` magic bytes + MessagePack payload.

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::{Result, ZeppelinError};
use crate::fts::bm25::{self, Bm25Params};
use crate::fts::inverted_index::InvertedIndex;

/// Magic bytes for global FTS index files.
const ZGFTS_MAGIC: &[u8; 5] = b"ZGFTS";
/// Current version of the global FTS index format.
const ZGFTS_VERSION: u8 = 1;

/// Per-segment global inverted index covering all FTS-configured fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalInvertedIndex {
    /// Total document count across all clusters.
    pub total_docs: u32,
    /// Per-field index data.
    pub fields: BTreeMap<String, GlobalFieldIndex>,
}

/// Global index data for a single text field across all clusters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalFieldIndex {
    /// Average document length (in tokens) across the whole segment.
    pub avg_doc_length: f32,
    /// Total docs with this field across all clusters.
    pub doc_count: u32,
    /// term → posting list
    pub postings: BTreeMap<String, GlobalPostingList>,
}

/// Global posting list for a single term across all clusters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalPostingList {
    /// Document frequency across all clusters.
    pub df: u32,
    /// Postings sorted by (cluster_idx, position).
    pub entries: Vec<GlobalPosting>,
}

/// A single posting in the global index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalPosting {
    /// Cluster index within the segment.
    pub cluster_idx: u16,
    /// Vector position within the cluster (0-based).
    pub position: u32,
    /// Term frequency in this document for this field.
    pub tf: u32,
}

impl GlobalInvertedIndex {
    /// Build a global index from per-cluster inverted indexes.
    ///
    /// `cluster_indexes` is `(cluster_idx, InvertedIndex)` for each cluster.
    #[must_use]
    pub fn build(cluster_indexes: &[(usize, &InvertedIndex)]) -> Self {
        let total_docs: u32 = cluster_indexes
            .iter()
            .map(|(_, idx)| idx.vector_count)
            .sum();
        let mut fields: BTreeMap<String, GlobalFieldIndex> = BTreeMap::new();

        for &(cluster_idx, idx) in cluster_indexes {
            for (field_name, field_index) in &idx.fields {
                let global_field =
                    fields
                        .entry(field_name.clone())
                        .or_insert_with(|| GlobalFieldIndex {
                            avg_doc_length: 0.0,
                            doc_count: 0,
                            postings: BTreeMap::new(),
                        });

                global_field.doc_count += field_index.doc_count;

                for (term, posting_list) in &field_index.postings {
                    let global_pl =
                        global_field
                            .postings
                            .entry(term.clone())
                            .or_insert_with(|| GlobalPostingList {
                                df: 0,
                                entries: Vec::new(),
                            });

                    global_pl.df += posting_list.df;

                    for posting in &posting_list.entries {
                        global_pl.entries.push(GlobalPosting {
                            cluster_idx: cluster_idx as u16,
                            position: posting.position,
                            tf: posting.tf,
                        });
                    }
                }
            }
        }

        // Compute weighted average doc lengths
        for (field_name, global_field) in &mut fields {
            if global_field.doc_count == 0 {
                continue;
            }
            let mut total_length = 0.0f64;
            for &(_, idx) in cluster_indexes {
                if let Some(fi) = idx.fields.get(field_name.as_str()) {
                    total_length += fi.avg_doc_length as f64 * fi.doc_count as f64;
                }
            }
            global_field.avg_doc_length = (total_length / global_field.doc_count as f64) as f32;
        }

        Self { total_docs, fields }
    }

    /// Search the global index for matching documents.
    ///
    /// Returns `(cluster_idx, position, score)` triples sorted by score descending.
    pub fn search(
        &self,
        field: &str,
        tokens: &[String],
        params: &Bm25Params,
    ) -> Vec<(u16, u32, f32)> {
        let field_index = match self.fields.get(field) {
            Some(fi) => fi,
            None => return Vec::new(),
        };

        // Accumulate scores per (cluster, position)
        let mut scores: BTreeMap<(u16, u32), f32> = BTreeMap::new();

        for token in tokens {
            let pl = match field_index.postings.get(token) {
                Some(pl) => pl,
                None => continue,
            };

            let idf = bm25::idf(field_index.doc_count, pl.df);

            for posting in &pl.entries {
                let score = bm25::bm25_term_score(
                    idf,
                    posting.tf,
                    0, // doc_length not tracked per-posting, use avg
                    field_index.avg_doc_length,
                    params,
                );
                *scores
                    .entry((posting.cluster_idx, posting.position))
                    .or_insert(0.0) += score;
            }
        }

        let mut results: Vec<(u16, u32, f32)> =
            scores.into_iter().map(|((c, p), s)| (c, p, s)).collect();
        results.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    /// Search with prefix matching on the last token.
    pub fn search_prefix(
        &self,
        field: &str,
        tokens: &[String],
        params: &Bm25Params,
    ) -> Vec<(u16, u32, f32)> {
        if tokens.is_empty() {
            return Vec::new();
        }

        let field_index = match self.fields.get(field) {
            Some(fi) => fi,
            None => return Vec::new(),
        };

        let mut scores: BTreeMap<(u16, u32), f32> = BTreeMap::new();

        // Exact match for all tokens except the last
        for token in &tokens[..tokens.len() - 1] {
            let pl = match field_index.postings.get(token) {
                Some(pl) => pl,
                None => continue,
            };
            let idf = bm25::idf(field_index.doc_count, pl.df);
            for posting in &pl.entries {
                let score =
                    bm25::bm25_term_score(idf, posting.tf, 0, field_index.avg_doc_length, params);
                *scores
                    .entry((posting.cluster_idx, posting.position))
                    .or_insert(0.0) += score;
            }
        }

        // Prefix match on the last token
        let prefix = &tokens[tokens.len() - 1];
        for (term, pl) in &field_index.postings {
            if term.starts_with(prefix.as_str()) {
                let idf = bm25::idf(field_index.doc_count, pl.df);
                for posting in &pl.entries {
                    let score = bm25::bm25_term_score(
                        idf,
                        posting.tf,
                        0,
                        field_index.avg_doc_length,
                        params,
                    );
                    *scores
                        .entry((posting.cluster_idx, posting.position))
                        .or_insert(0.0) += score;
                }
            }
        }

        let mut results: Vec<(u16, u32, f32)> =
            scores.into_iter().map(|((c, p), s)| (c, p, s)).collect();
        results.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    /// Get the set of cluster indices that contain any matching documents.
    pub fn matching_clusters(&self, field: &str, tokens: &[String]) -> Vec<u16> {
        let field_index = match self.fields.get(field) {
            Some(fi) => fi,
            None => return Vec::new(),
        };

        let mut clusters = std::collections::BTreeSet::new();
        for token in tokens {
            if let Some(pl) = field_index.postings.get(token) {
                for posting in &pl.entries {
                    clusters.insert(posting.cluster_idx);
                }
            }
        }

        clusters.into_iter().collect()
    }

    /// Serialize to bytes: `ZGFTS` magic (5B) + version (1B) + MessagePack payload.
    pub fn to_bytes(&self) -> Result<Bytes> {
        let msgpack = rmp_serde::to_vec(self).map_err(|e| {
            ZeppelinError::Serialization(format!("global FTS index serialize: {e}"))
        })?;
        let mut data = Vec::with_capacity(6 + msgpack.len());
        data.extend_from_slice(ZGFTS_MAGIC);
        data.push(ZGFTS_VERSION);
        data.extend_from_slice(&msgpack);
        Ok(Bytes::from(data))
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 6 {
            return Err(ZeppelinError::Serialization(
                "global FTS index too small".into(),
            ));
        }
        if &data[0..5] != ZGFTS_MAGIC {
            return Err(ZeppelinError::Serialization(
                "invalid global FTS magic bytes".into(),
            ));
        }
        // data[5] is version — forward compatible
        rmp_serde::from_slice(&data[6..])
            .map_err(|e| ZeppelinError::Serialization(format!("global FTS index deserialize: {e}")))
    }
}

/// S3 key for the global FTS index of a segment.
pub fn global_fts_key(namespace: &str, segment_id: &str) -> String {
    format!("{namespace}/segments/{segment_id}/global_fts.bin")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fts::inverted_index::{FieldIndex, InvertedIndex, Posting, PostingList};
    use std::collections::BTreeMap;

    fn make_cluster_index(
        vector_count: u32,
        field: &str,
        terms: &[(&str, Vec<(u32, u32)>)],
    ) -> InvertedIndex {
        let mut postings = BTreeMap::new();
        for (term, entries) in terms {
            postings.insert(
                term.to_string(),
                PostingList {
                    df: entries.len() as u32,
                    entries: entries
                        .iter()
                        .map(|&(pos, tf)| Posting { position: pos, tf })
                        .collect(),
                },
            );
        }
        let mut fields = BTreeMap::new();
        fields.insert(
            field.to_string(),
            FieldIndex {
                avg_doc_length: 5.0,
                doc_count: vector_count,
                postings,
            },
        );
        InvertedIndex {
            vector_count,
            fields,
        }
    }

    #[test]
    fn test_build_from_cluster_indexes() {
        let idx0 = make_cluster_index(3, "title", &[("hello", vec![(0, 1), (2, 2)])]);
        let idx1 = make_cluster_index(
            2,
            "title",
            &[("hello", vec![(0, 1)]), ("world", vec![(1, 1)])],
        );

        let global = GlobalInvertedIndex::build(&[(0, &idx0), (1, &idx1)]);

        assert_eq!(global.total_docs, 5);
        let title_field = global.fields.get("title").unwrap();
        assert_eq!(title_field.doc_count, 5);

        let hello_pl = title_field.postings.get("hello").unwrap();
        assert_eq!(hello_pl.df, 3); // 2 from cluster 0 + 1 from cluster 1
        assert_eq!(hello_pl.entries.len(), 3);
    }

    #[test]
    fn test_search_basic() {
        let idx0 = make_cluster_index(3, "title", &[("rust", vec![(0, 2), (1, 1)])]);
        let idx1 = make_cluster_index(2, "title", &[("rust", vec![(0, 3)])]);

        let global = GlobalInvertedIndex::build(&[(0, &idx0), (1, &idx1)]);
        let params = Bm25Params::default();
        let results = global.search("title", &["rust".to_string()], &params);

        assert_eq!(results.len(), 3);
        // Highest TF should score highest
        assert_eq!(results[0].0, 1); // cluster 1, position 0, tf=3
    }

    #[test]
    fn test_search_missing_field() {
        let idx0 = make_cluster_index(3, "title", &[("hello", vec![(0, 1)])]);
        let global = GlobalInvertedIndex::build(&[(0, &idx0)]);
        let params = Bm25Params::default();
        let results = global.search("body", &["hello".to_string()], &params);
        assert!(results.is_empty());
    }

    #[test]
    fn test_matching_clusters() {
        let idx0 = make_cluster_index(3, "title", &[("rust", vec![(0, 1)])]);
        let idx1 = make_cluster_index(2, "title", &[("python", vec![(0, 1)])]);
        let idx2 = make_cluster_index(2, "title", &[("rust", vec![(1, 1)])]);

        let global = GlobalInvertedIndex::build(&[(0, &idx0), (1, &idx1), (2, &idx2)]);
        let clusters = global.matching_clusters("title", &["rust".to_string()]);
        assert_eq!(clusters, vec![0, 2]);
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let idx0 = make_cluster_index(3, "title", &[("hello", vec![(0, 1), (2, 2)])]);
        let global = GlobalInvertedIndex::build(&[(0, &idx0)]);

        let bytes = global.to_bytes().unwrap();
        let restored = GlobalInvertedIndex::from_bytes(&bytes).unwrap();

        assert_eq!(restored.total_docs, global.total_docs);
        assert_eq!(restored.fields.len(), global.fields.len());
    }

    #[test]
    fn test_invalid_magic_rejected() {
        let data = b"WRONG12345";
        let result = GlobalInvertedIndex::from_bytes(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_search_prefix() {
        let idx0 = make_cluster_index(
            3,
            "title",
            &[
                ("rustlang", vec![(0, 1)]),
                ("rustic", vec![(1, 1)]),
                ("python", vec![(2, 1)]),
            ],
        );
        let global = GlobalInvertedIndex::build(&[(0, &idx0)]);
        let params = Bm25Params::default();
        let results = global.search_prefix("title", &["rust".to_string()], &params);
        assert_eq!(results.len(), 2); // "rustlang" and "rustic"
    }
}
