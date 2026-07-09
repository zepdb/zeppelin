//! Builds and searches the segment-wide BM25 accelerator.
//!
//! [`GlobalInvertedIndex`] merges every cluster-local
//! [`InvertedIndex`] in one segment into a token-to-postings map whose document
//! address is `(cluster index, position)`. Compaction uploads that immutable
//! sidecar beside the segment and sets
//! [`crate::wal::manifest::SegmentRef::has_global_fts`] only in the manifest it
//! later publishes. The query fast path therefore discovers this artifact
//! through authoritative manifest state, fetches one global object through the
//! cache/storage boundary, and loads only the clusters that actually produced
//! hits.
//!
//! ```text
//! per-cluster InvertedIndex values
//!              |
//!              | merge term postings and field statistics
//!              v
//! GlobalInvertedIndex: term -> (cluster, position, tf)
//!              |
//!              | "ZGFTS" + version byte + MessagePack
//!              v
//! upload immutable global_fts.bin (not visible yet)
//!              |
//!              v
//! manifest CAS publishes SegmentRef { has_global_fts: true }
//!              |
//!              v
//! query fetches one index -> scores hits -> fetches needed clusters
//! ```
//!
//! This format deliberately trades ranking detail for one-object discovery:
//! [`GlobalPosting`] stores term frequency but not document length. The current
//! [`GlobalInvertedIndex::search`] and [`GlobalInvertedIndex::search_prefix`]
//! pass a document length of zero to BM25. They preserve TF and segment-wide
//! IDF but do not apply per-document length normalization, so their scores are
//! not numerically interchangeable with [`InvertedIndex::search`].
//!
//! ## Reading map
//!
//! 1. Start with [`GlobalPosting`] and [`GlobalPostingList`] for document
//!    addressing across clusters.
//! 2. Read [`GlobalFieldIndex`] and [`GlobalInvertedIndex`] for persisted corpus
//!    statistics.
//! 3. Follow [`GlobalInvertedIndex::build`] for compaction-time merging.
//! 4. Follow [`GlobalInvertedIndex::search`] and
//!    [`GlobalInvertedIndex::search_prefix`] for query scoring.
//! 5. Read [`GlobalInvertedIndex::to_bytes`] and
//!    [`GlobalInvertedIndex::from_bytes`] before changing the persisted schema.
//!
//! ## Invariants
//!
//! - Each `(cluster_idx, position)` addresses the same document as the segment's
//!   vector, ID, and attribute artifacts.
//! - Cluster indexes must fit in `u16`; this public builder currently casts
//!   rather than returning a validation error.
//! - The build caller supplies cluster-local indexes from the same immutable
//!   logical segment and in the order desired for persisted postings.
//! - An object is query-visible only through a published manifest whose segment
//!   advertises `has_global_fts`; cache presence is never authority.
//! - MessagePack struct layout and field order are persisted compatibility
//!   constraints.
//!
//! ## Rust concepts used here
//!
//! The build input `&[(usize, &InvertedIndex)]` borrows both the tuple slice and
//! each index, so the merge can read shared cluster data without cloning entire
//! indexes. The result owns its strings and postings. `BTreeMap` supplies both
//! deterministic serialization order and deterministic hit accumulation.
//! Java would rely on ordinary references and garbage collection; C would need
//! explicit pointer lifetimes and cleanup. Rust proves that the borrowed
//! indexes remain valid during the merge and automatically drops partially
//! built owned state if construction unwinds.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::{Result, ZeppelinError};
use crate::fts::bm25::{self, Bm25Params};
use crate::fts::inverted_index::InvertedIndex;
use crate::index::topk::partial_topk_by;

/// Five-byte artifact discriminator preceding every global FTS payload.
const ZGFTS_MAGIC: &[u8; 5] = b"ZGFTS";
/// Version byte written by the current global FTS encoder.
///
/// The current decoder skips this byte after checking the magic and attempts to
/// decode the payload with the current schema; it does not reject another
/// version value.
const ZGFTS_VERSION: u8 = 1;

/// Orders global hits by descending score, cluster, and position.
///
/// # Parameters
///
/// - `a`: First `(cluster, position, score)` candidate.
/// - `b`: Second candidate.
///
/// # Returns
///
/// Best-first ordering with stable value-based tie-breakers. For example,
/// `(0, 9, 3.0)` precedes `(1, 0, 3.0)`.
fn global_doc_score_cmp(a: &(u16, u32, f32), b: &(u16, u32, f32)) -> Ordering {
    b.2.total_cmp(&a.2)
        .then_with(|| a.0.cmp(&b.0))
        .then_with(|| a.1.cmp(&b.1))
}

/// Segment-wide token index used by the BM25 query fast path.
///
/// This object is derived entirely from immutable cluster-local indexes. It is
/// a query accelerator, while the manifest remains the authority for whether
/// the corresponding object belongs to the visible segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalInvertedIndex {
    /// Sum of cluster vector counts, including vectors without indexed text.
    pub total_docs: u32,
    /// Non-empty fields collected from cluster indexes, in lexical key order.
    pub fields: BTreeMap<String, GlobalFieldIndex>,
}

/// Segment-wide postings and BM25 corpus statistics for one text field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalFieldIndex {
    /// Weighted mean token count among documents indexed for this field.
    pub avg_doc_length: f32,
    /// Number of documents that produced at least one indexed token.
    pub doc_count: u32,
    /// Normalized token to global posting list in lexical order.
    pub postings: BTreeMap<String, GlobalPostingList>,
}

/// Occurrences of one normalized term throughout a logical segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalPostingList {
    /// Number of distinct containing documents across all cluster indexes.
    pub df: u32,
    /// Postings appended in the cluster-index order passed to
    /// [`GlobalInvertedIndex::build`].
    ///
    /// Entries are sorted by `(cluster_idx, position)` when that input is sorted
    /// by cluster and each source [`crate::fts::inverted_index::PostingList`] is
    /// position-sorted, as in the production compaction path.
    pub entries: Vec<GlobalPosting>,
}

/// Term-frequency record for one cluster-local document in a segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalPosting {
    /// Zero-based logical cluster index, stored as `u16` to reduce artifact size.
    pub cluster_idx: u16,
    /// Zero-based vector/ID/attribute position inside the cluster.
    pub position: u32,
    /// Count of this normalized term in the indexed field.
    pub tf: u32,
}

impl GlobalInvertedIndex {
    /// Merges cluster-local indexes into one segment-wide lookup artifact.
    ///
    /// Field document counts and posting-list document frequencies are summed.
    /// Field average length is a document-count-weighted mean of cluster
    /// averages. Every source posting is rewritten with its logical cluster
    /// index so the query path can later fetch only clusters containing hits.
    ///
    /// # Parameters
    ///
    /// - `cluster_indexes`: Borrowed `(logical cluster index, index)` pairs for
    ///   one segment. Each index may be reused after this call. Every cluster
    ///   index must fit in `u16`; values outside that range are truncated by the
    ///   current cast and would produce incorrect document addresses.
    ///
    /// # Returns
    ///
    /// An owned segment-wide index. Empty input produces zero documents and no
    /// fields. Persisted posting order follows the supplied pair order and each
    /// source posting order.
    ///
    /// # Performance
    ///
    /// Visits and clones every field name, term, and posting once. Memory is
    /// proportional to all source postings; source indexes are only borrowed.
    /// Compaction performs this CPU work before parallel artifact uploads.
    ///
    /// # Examples
    ///
    /// Cluster 0 containing `rust` at position 2 and cluster 3 containing it at
    /// position 1 become one posting list with addresses `(0,2)` and `(3,1)`.
    /// A query can then discover both documents from a single global object.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The nested shared references mean this function neither owns nor copies
    /// the source indexes. Strings and postings are cloned into the returned
    /// owner. `entry(...).or_insert_with(...)` gives one exclusive mutable
    /// reference to an accumulator, which prevents invalidation bugs common
    /// when C code keeps pointers into a growing map.
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

        // Weight by indexed documents, not cluster count: a one-document
        // cluster must not influence the corpus mean like a thousand-document
        // cluster.
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

    /// Scores all global postings that match at least one exact query token.
    ///
    /// Matching uses additive OR semantics. Each known query token contributes
    /// its segment-wide IDF and document term frequency. Because global postings
    /// do not retain individual lengths, this method passes `doc_length = 0` to
    /// BM25; it therefore omits relative document-length normalization.
    ///
    /// # Parameters
    ///
    /// - `field`: Indexed field to search.
    /// - `tokens`: Already normalized exact query terms. Duplicate terms are
    ///   intentionally scored more than once because this method does not
    ///   deduplicate them.
    /// - `params`: BM25 term-frequency and length-normalization parameters.
    ///
    /// # Returns
    ///
    /// Owned `(cluster index, position, score)` triples sorted by descending
    /// score, ascending cluster, then ascending position. A missing field,
    /// empty token slice, or no matching postings returns an empty vector.
    ///
    /// # Performance
    ///
    /// Visits each posting for each matching token, retains one `BTreeMap`
    /// accumulator per matching document, then sorts all results. There is no
    /// I/O here; the query layer pays one cache/object-store load for the global
    /// artifact before calling this method.
    ///
    /// # Examples
    ///
    /// Searching `title` for `rust` can return `(0, 4, score)` and
    /// `(7, 2, score)` from one index. The query layer then loads clusters 0 and
    /// 7 for IDs, filtering, and optional attributes.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `BTreeMap<(u16, u32), f32>` uses a tuple as a value-typed composite key.
    /// Rust's `entry` API guarantees the returned mutable score reference is the
    /// only active mutable reference to that value. Java offers a similar
    /// map-merge operation; C would require a concrete map implementation and
    /// explicit key comparison.
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

        // A document address is only unique when both cluster and position are
        // present; position alone restarts at zero in every cluster.
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
                    0, // Global postings omit per-document lengths.
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
        let k = results.len();
        partial_topk_by(&mut results, k, global_doc_score_cmp);
        results
    }

    /// Scores exact tokens plus dictionary expansions of the final prefix token.
    ///
    /// Every token except the last is looked up exactly. The last token is
    /// compared against the complete field dictionary and every term beginning
    /// with it contributes postings. Like [`Self::search`], this method uses
    /// segment-wide IDF but passes zero for per-document length.
    ///
    /// # Parameters
    ///
    /// - `field`: Field whose global dictionary to search.
    /// - `tokens`: Normalized tokens with the last element treated as a prefix.
    ///   An empty slice returns immediately.
    /// - `params`: BM25 field parameters.
    ///
    /// # Returns
    ///
    /// Sorted `(cluster, position, score)` triples. An unknown field produces no
    /// hits. Exact earlier terms may still produce hits when the final prefix
    /// has no expansion.
    ///
    /// # Performance
    ///
    /// Exact terms use logarithmic map lookups. Prefix expansion scans the
    /// complete term dictionary and visits all postings under matching terms;
    /// a broad or empty prefix may touch every posting in the field.
    ///
    /// # Examples
    ///
    /// Tokens `["object", "stor"]` look up `object` exactly and expand `stor`
    /// to `storage`, `store`, and any other indexed prefix match. Scores from
    /// all those terms accumulate at each `(cluster, position)` address.
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

        // Borrowing the prefix-free subslice avoids cloning exact query tokens.
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

        // The BTreeMap is ordered, but this implementation performs a full scan
        // rather than narrowing to a lexical range.
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
        let k = results.len();
        partial_topk_by(&mut results, k, global_doc_score_cmp);
        results
    }

    /// Finds the sorted unique clusters containing any exact query token.
    ///
    /// This is a discovery helper only: it does not calculate BM25 scores,
    /// interpret the last token as a prefix, or prove that every posting survives
    /// a later metadata filter.
    ///
    /// # Parameters
    ///
    /// - `field`: Indexed field whose postings to inspect.
    /// - `tokens`: Exact normalized terms. Duplicate and unknown tokens have no
    ///   effect on the result.
    ///
    /// # Returns
    ///
    /// Ascending unique cluster indexes. A missing field or no matches returns
    /// an empty vector.
    ///
    /// # Performance
    ///
    /// Visits every posting under matching tokens and stores at most one `u16`
    /// per cluster in a temporary ordered set.
    ///
    /// # Examples
    ///
    /// If `rust` occurs in clusters 5, 2, and 5 again, the result is `[2, 5]`.
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

    /// Encodes this global index as a versioned MessagePack artifact.
    ///
    /// # Returns
    ///
    /// Shared immutable bytes laid out as five `ZGFTS` magic bytes, the current
    /// version byte, and a MessagePack representation of this struct.
    ///
    /// # Errors
    ///
    /// Returns a serialization error when `rmp-serde` cannot encode the value.
    /// No object-store write or manifest change has happened at that point.
    ///
    /// # Performance
    ///
    /// Allocates a MessagePack buffer and then a second final buffer holding the
    /// header plus payload. Moving the final `Vec<u8>` into [`Bytes`] does not
    /// copy it; subsequent `Bytes` clones share the allocation.
    ///
    /// # Examples
    ///
    /// Compaction serializes the merged index, uploads it to [`global_fts_key`],
    /// waits for all sidecar PUTs to succeed, and only later advertises it via a
    /// successful manifest CAS.
    ///
    /// # Consistency
    ///
    /// Serialization creates an immutable candidate artifact. It does not make
    /// the object query-visible and does not update cache or manifest state.
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

    /// Validates the global artifact discriminator and decodes its MessagePack.
    ///
    /// # Parameters
    ///
    /// - `data`: Complete bytes fetched for `global_fts.bin`.
    ///
    /// # Returns
    ///
    /// A fully owned index independent of the input buffer.
    ///
    /// # Errors
    ///
    /// Returns a serialization error when fewer than six bytes are present, the
    /// `ZGFTS` magic is wrong, or the payload cannot decode as the current
    /// struct shape. The decoder does not silently return an empty index.
    ///
    /// # Consistency
    ///
    /// The byte at offset 5 is currently skipped rather than compared with
    /// `ZGFTS_VERSION`. Consequently an unfamiliar version is accepted only
    /// if the remaining bytes happen to decode with today's MessagePack schema;
    /// the byte is not a negotiated forward-compatibility mechanism.
    ///
    /// # Examples
    ///
    /// [`Self::to_bytes`] output round-trips. Bytes starting with `ZFTS` (the
    /// cluster-local format) fail the magic check, and truncated MessagePack
    /// fails decoding.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `Result` forces callers to handle corrupt or wrong-type artifacts. The
    /// `?`-style propagation at the query layer is comparable to checked Java
    /// exception propagation, while C would conventionally pair a status code
    /// with an output pointer and manual cleanup.
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
        // Preserve the current wire contract: byte 5 is reserved as a version
        // marker, but compatibility is decided by whether today's schema can
        // decode the payload.
        rmp_serde::from_slice(&data[6..])
            .map_err(|e| ZeppelinError::Serialization(format!("global FTS index deserialize: {e}")))
    }
}

/// Constructs the object-store key for a segment-wide FTS artifact.
///
/// # Parameters
///
/// - `namespace`: Logical namespace object-prefix component.
/// - `segment_id`: Segment that owns the global artifact. Unlike carried
///   cluster-local sidecars, the global index is built under the new logical
///   segment ID.
///
/// # Returns
///
/// `<namespace>/segments/<segment_id>/global_fts.bin` as an owned string.
/// This helper performs no storage request and does not establish visibility.
///
/// # Examples
///
/// Namespace `catalog` and segment `seg_01` produce
/// `catalog/segments/seg_01/global_fts.bin`.
pub fn global_fts_key(namespace: &str, segment_id: &str) -> String {
    format!("{namespace}/segments/{segment_id}/global_fts.bin")
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Focused tests for segment-wide posting aggregation and lookup.
    //!
    //! Fixtures construct cluster indexes directly so each test isolates global
    //! addressing, document frequency, score ordering, prefix expansion, and
    //! the MessagePack artifact discriminator without object-store setup.

    use super::*;
    use crate::fts::inverted_index::{FieldIndex, InvertedIndex, Posting, PostingList};
    use std::collections::BTreeMap;

    /// Builds one minimal cluster-local index from explicit term postings.
    ///
    /// # Parameters
    ///
    /// - `vector_count`: Total cluster vector slots and synthetic field document
    ///   count.
    /// - `field`: Single field name to place in the fixture.
    /// - `terms`: Term strings paired with `(position, term frequency)` entries.
    ///
    /// # Returns
    ///
    /// An owned [`InvertedIndex`] with fixed average document length 5.0. The
    /// caller owns the fixture and lends it to [`GlobalInvertedIndex::build`].
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
    /// Verifies that merging preserves cluster addresses and sums field statistics.
    ///
    /// A regression here could make the fast path fetch the wrong cluster or
    /// compute segment IDF from only one source index.
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
    /// Protects exact-term scoring and descending term-frequency behavior.
    ///
    /// The highest-TF posting should lead when field and corpus statistics are
    /// otherwise identical.
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
    /// Confirms a missing field returns no hits instead of crossing field data.
    fn test_search_missing_field() {
        let idx0 = make_cluster_index(3, "title", &[("hello", vec![(0, 1)])]);
        let global = GlobalInvertedIndex::build(&[(0, &idx0)]);
        let params = Bm25Params::default();
        let results = global.search("body", &["hello".to_string()], &params);
        assert!(results.is_empty());
    }

    #[test]
    /// Confirms cluster discovery deduplicates and sorts matching cluster IDs.
    fn test_matching_clusters() {
        let idx0 = make_cluster_index(3, "title", &[("rust", vec![(0, 1)])]);
        let idx1 = make_cluster_index(2, "title", &[("python", vec![(0, 1)])]);
        let idx2 = make_cluster_index(2, "title", &[("rust", vec![(1, 1)])]);

        let global = GlobalInvertedIndex::build(&[(0, &idx0), (1, &idx1), (2, &idx2)]);
        let clusters = global.matching_clusters("title", &["rust".to_string()]);
        assert_eq!(clusters, vec![0, 2]);
    }

    #[test]
    /// Confirms the global MessagePack artifact round-trips through its header.
    fn test_serialize_deserialize_roundtrip() {
        let idx0 = make_cluster_index(3, "title", &[("hello", vec![(0, 1), (2, 2)])]);
        let global = GlobalInvertedIndex::build(&[(0, &idx0)]);

        let bytes = global.to_bytes().unwrap();
        let restored = GlobalInvertedIndex::from_bytes(&bytes).unwrap();

        assert_eq!(restored.total_docs, global.total_docs);
        assert_eq!(restored.fields.len(), global.fields.len());
    }

    #[test]
    /// Ensures bytes from another artifact family fail the magic check loudly.
    fn test_invalid_magic_rejected() {
        let data = b"WRONG12345";
        let result = GlobalInvertedIndex::from_bytes(data);
        assert!(result.is_err());
    }

    #[test]
    /// Verifies that the final token matches every global dictionary prefix.
    ///
    /// The unrelated `python` posting protects against over-broad expansion.
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
