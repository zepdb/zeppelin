//! Builds, encodes, and searches cluster-local BM25 inverted indexes.
//!
//! Compaction calls [`InvertedIndex::build`] after it has written one IVF
//! cluster's attribute sidecar. Each configured text field becomes a mapping
//! from a normalized token to a posting list. A posting identifies a vector by
//! its zero-based position inside that cluster and records how often the term
//! occurs in that document. The query path normally uses the segment-wide
//! [`crate::fts::global_index::GlobalInvertedIndex`]; these per-cluster indexes
//! remain the exact-length implementation and the compatibility path for older
//! segments.
//!
//! The values in this file are **artifact contents**, not authority. Compaction
//! serializes an index, uploads it through [`crate::storage::ZeppelinStore`],
//! and only then publishes a [`crate::wal::manifest::SegmentRef`] with an ETag
//! compare-and-swap. An uploaded `fts_index_<cluster>.bin` is immutable but is
//! not query-visible until that manifest update succeeds. A disk-cache copy is
//! disposable; the manifest and object store determine which object key to
//! read.
//!
//! ```text
//! cluster attribute maps + field configuration
//!                       |
//!                       v
//! tokenize each string -> term frequencies -> sorted posting lists
//!                       |
//!                       v
//!       "ZFTS" | version 1 | compact JSON
//!                       |
//!                       v
//!        upload immutable object (not visible yet)
//!                       |
//!                       v
//!       publish SegmentRef with manifest CAS
//!                       |
//!                       v
//!        query may load, decode, and BM25-score it
//! ```
//!
//! ## Reading map
//!
//! 1. Start with [`Posting`] and [`PostingList`] to learn the leaf format.
//! 2. Read [`FieldIndex`] and [`InvertedIndex`] for cluster-local statistics.
//! 3. Follow [`InvertedIndex::build`] and [`InvertedIndex::search`] for the
//!    normal construction and query paths.
//! 4. Read [`InvertedIndex::to_bytes`] and [`InvertedIndex::from_bytes`] for the
//!    persisted `ZFTS` compatibility boundary.
//! 5. Read [`FtsSegmentMeta`] only for the separate segment-statistics format;
//!    the current compaction and query paths do not write or load that format.
//!
//! ## Invariants
//!
//! - Posting positions use the same cluster-local order as vector and
//!   attribute artifacts.
//! - A term appears at most once per document's posting list, and its posting
//!   stores the complete term frequency for that document.
//! - Posting lists built here are sorted by position so results remain
//!   deterministic when scores tie.
//! - Index-time and query-time tokens must use the same [`FtsFieldConfig`].
//! - Persisted structs and the JSON representation are compatibility-sensitive;
//!   changing their shape or numeric meaning requires a format-version plan.
//!
//! ## Rust concepts used here
//!
//! [`InvertedIndex::build`] receives a borrowed slice containing optional
//! borrowed maps. Java would pass object references and C might pass an array
//! of nullable pointers; Rust additionally proves that every non-null map stays
//! alive for the build and prevents mutation through these shared borrows.
//! `BTreeMap` gives persisted term and field maps deterministic key order,
//! while temporary `HashMap`s provide average constant-time accumulation.
//! Serde derives turn owned Rust structs into the artifact representation, and
//! [`Bytes`] transfers the finished buffer to async storage code with cheap
//! shared-buffer clones rather than copying its contents.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::{Result, ZeppelinError};
use crate::fts::bm25::{self, Bm25Params};
use crate::fts::tokenizer::tokenize_text;
use crate::fts::FtsFieldConfig;
use crate::index::topk::partial_topk_by;
use crate::types::AttributeValue;

/// Four-byte discriminator at the start of every cluster-local FTS artifact.
///
/// The header lets a decoder reject a different artifact type before asking
/// Serde to interpret its payload.
const ZFTS_MAGIC: &[u8; 4] = b"ZFTS";
/// Only cluster-local FTS artifact version accepted by this decoder.
///
/// Version 1 stores a compact JSON payload immediately after the version byte.
const ZFTS_VERSION: u8 = 1;

/// Orders cluster-local hits by descending BM25 score, then ascending position.
///
/// # Parameters
///
/// - `a`: First `(position, score)` hit to compare.
/// - `b`: Second `(position, score)` hit to compare.
///
/// # Returns
///
/// An ordering suitable for a "best first" selection routine. For example,
/// `(2, 4.0)` ranks before `(7, 3.0)`, while equal scores rank the lower
/// position first.
fn doc_score_cmp(a: &(u32, f32), b: &(u32, f32)) -> Ordering {
    b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0))
}

/// Complete full-text index for one immutable IVF cluster.
///
/// The index owns its token strings and posting buffers so it can move from a
/// blocking compaction task into serialization without borrowing the attribute
/// input. Its persisted representation is the versioned `ZFTS` format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvertedIndex {
    /// Number of vector slots in the cluster, including documents with no
    /// indexed text.
    pub vector_count: u32,
    /// Non-empty configured fields keyed by their namespace configuration name.
    ///
    /// A configured field whose documents are all missing, non-string, empty,
    /// or fully removed by tokenization is absent from this map.
    pub fields: BTreeMap<String, FieldIndex>,
}

/// Posting lists and BM25 corpus statistics for one field in one cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldIndex {
    /// Mean token count among documents that produced at least one token.
    ///
    /// Documents with a missing or empty field are excluded from the divisor.
    pub avg_doc_length: f32,
    /// Number of documents that produced at least one indexed token.
    pub doc_count: u32,
    /// Token-to-posting-list map in deterministic lexical key order.
    pub postings: BTreeMap<String, PostingList>,
}

/// All documents in one cluster field that contain one normalized token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostingList {
    /// Document frequency: the number of distinct documents in [`Self::entries`].
    pub df: u32,
    /// One posting per matching document, sorted by ascending cluster position
    /// when produced by [`InvertedIndex::build`].
    pub entries: Vec<Posting>,
}

/// Occurrence summary for one term in one cluster-local document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Posting {
    /// Zero-based position shared by this cluster's vector, ID, and attribute
    /// arrays.
    pub position: u32,
    /// Number of times the normalized term occurs in this field's token stream.
    pub tf: u32,
}

impl InvertedIndex {
    /// Builds the cluster-local posting lists for every configured FTS field.
    ///
    /// Slice position is document identity within the cluster. Missing attribute
    /// maps, missing fields, non-string values, and strings that tokenize to no
    /// terms remain part of [`Self::vector_count`] but do not contribute to a
    /// field's BM25 document count or average length.
    ///
    /// # Parameters
    ///
    /// - `attrs`: One optional borrowed attribute map per cluster vector, in the
    ///   exact order used by the vector, ID, and attribute artifacts. The slice
    ///   length must fit in `u32` for a lossless persisted count.
    /// - `fts_configs`: Validated field configurations keyed by attribute name.
    ///   The same tokenization settings must be used when preparing queries.
    ///
    /// # Returns
    ///
    /// An owned index with one entry for each configured field that produced at
    /// least one token. Posting lists are sorted by position.
    ///
    /// # Performance
    ///
    /// Tokenization examines every configured field of every vector. Temporary
    /// memory is proportional to distinct `(field, term, document)` matches;
    /// the returned index owns all token strings and postings. Compaction runs
    /// this CPU work in a blocking worker rather than on a Tokio runtime thread.
    ///
    /// # Examples
    ///
    /// For documents `"rust storage"`, `"rust rust"`, and a missing map, the
    /// `rust` posting list contains `(position=0, tf=1)` and
    /// `(position=1, tf=2)`. `vector_count` is 3 but the field's `doc_count` is
    /// 2.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `&[Option<&HashMap<...>>]` layers three guarantees: the outer slice is a
    /// borrowed contiguous view, `Option` represents absence without a null
    /// reference, and each present map is itself borrowed. No attribute data is
    /// copied or retained. The returned `Self` is fully owned and may outlive
    /// every input borrow.
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

    /// Scores every document containing at least one requested token in a field.
    ///
    /// This is OR semantics: each matching token contributes one BM25 term
    /// score, and contributions are summed per document. Tokens absent from the
    /// field are ignored. The caller is responsible for applying the same
    /// tokenizer configuration used at build time; this method accepts already
    /// normalized tokens.
    ///
    /// # Parameters
    ///
    /// - `field`: Configured attribute field to search.
    /// - `query_tokens`: Borrowed normalized terms. Repeated terms contribute
    ///   repeatedly; this method does not deduplicate the query.
    /// - `params`: BM25 saturation and length-normalization parameters.
    ///
    /// # Returns
    ///
    /// Owned `(cluster position, score)` pairs for matching documents, sorted
    /// by descending score and then ascending position. An unknown field, an
    /// empty token slice, or no known terms returns an empty vector.
    ///
    /// # Performance
    ///
    /// Reconstructing document lengths visits every posting in the field. Score
    /// accumulation then visits the postings of every matching query term, and
    /// the complete result set is sorted. No object-store I/O occurs here; the
    /// caller has already fetched and decoded the immutable artifact.
    ///
    /// # Examples
    ///
    /// With documents `"rust storage"` and `"rust rust"`, searching `rust`
    /// returns both positions. Term frequency and BM25 length normalization
    /// decide their scores; higher scores appear first. Searching an unindexed
    /// `body` field returns `[]` rather than consulting another field.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The `let Some(...) = ... else` branch makes absence explicit and exits
    /// before scoring. Unlike a nullable Java reference or unchecked C pointer,
    /// Rust requires this branch before the contained [`FieldIndex`] can be
    /// borrowed. Iterator adapters build only the token/IDF pairs that exist.
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

        // IDF is based on this cluster's field corpus, not the entire segment.
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

        // One document can receive contributions from several query terms.
        let mut doc_scores: HashMap<u32, f32> = HashMap::new();
        // Lengths are derivable from persisted term frequencies, so the format
        // avoids storing a second per-document array.
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
        let k = results.len();
        partial_topk_by(&mut results, k, doc_score_cmp);
        results
    }

    /// Expands the final query token as a lexical prefix, then performs BM25.
    ///
    /// Earlier tokens remain exact. Every indexed term whose normalized form
    /// starts with the final token is added to the query passed to
    /// [`Self::search`]. The tokenizer normally leaves the user's last token
    /// unstemmed in prefix mode so `prog` can match `program` and `programmer`.
    ///
    /// # Parameters
    ///
    /// - `field`: Configured field whose term dictionary is scanned.
    /// - `query_tokens`: Normalized query tokens; the last element is the
    ///   prefix. An empty slice returns no matches.
    /// - `params`: BM25 parameters forwarded unchanged to [`Self::search`].
    ///
    /// # Returns
    ///
    /// The same sorted `(position, score)` representation as [`Self::search`].
    /// A missing field or a lone prefix with no expansion returns an empty
    /// vector. Exact earlier tokens can still produce results when the prefix
    /// has no match.
    ///
    /// # Performance
    ///
    /// Prefix expansion currently scans every term in the field dictionary and
    /// clones each matching token before running the ordinary search. Broad or
    /// empty prefixes can therefore expand to the entire vocabulary.
    ///
    /// # Examples
    ///
    /// Tokens `["distributed", "stor"]` search `distributed` exactly and
    /// expand `stor` to terms such as `storage` and `store`. Tokens `[]` return
    /// immediately without scanning the dictionary.
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

        // split_at borrows two non-overlapping views; it does not copy tokens.
        let (exact_tokens, prefix_token) = query_tokens.split_at(query_tokens.len() - 1);

        // BTreeMap iteration is lexical, so expansion order is deterministic.
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

        // The search API owns token Strings, so the borrowed exact terms and
        // matching dictionary keys are cloned into one temporary query.
        let mut all_tokens: Vec<String> = exact_tokens.to_vec();
        all_tokens.extend(prefix_matches);

        self.search(field, &all_tokens, params)
    }

    /// Encodes this index as a versioned cluster-local FTS artifact.
    ///
    /// # Returns
    ///
    /// Shared immutable bytes laid out as four `ZFTS` magic bytes, one version
    /// byte, and a compact JSON representation of this index.
    ///
    /// # Errors
    ///
    /// Propagates a serialization error if Serde cannot encode the index. No
    /// object-store write has occurred when this method returns an error.
    ///
    /// # Performance
    ///
    /// Allocates one contiguous buffer proportional to all token strings and
    /// postings, then moves that buffer into [`Bytes`]. Cloning the returned
    /// `Bytes` later shares its allocation by reference counting.
    ///
    /// # Examples
    ///
    /// A compaction build serializes the index, uploads the returned bytes to
    /// [`fts_index_key`], and only exposes that object by publishing its segment
    /// in the manifest.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `Bytes::from(buf)` transfers ownership of the `Vec<u8>` allocation; it
    /// does not copy the payload. Java's closest model is handing an immutable
    /// byte-buffer wrapper to another component. In C, ownership transfer would
    /// require an explicit convention for who frees the allocation.
    pub fn to_bytes(&self) -> Result<Bytes> {
        let json = serde_json::to_vec(self)?;
        let mut buf = Vec::with_capacity(5 + json.len());
        buf.extend_from_slice(ZFTS_MAGIC);
        buf.push(ZFTS_VERSION);
        buf.extend_from_slice(&json);
        Ok(Bytes::from(buf))
    }

    /// Validates and decodes one version-1 cluster-local FTS artifact.
    ///
    /// # Parameters
    ///
    /// - `data`: Complete object contents beginning with the `ZFTS` header.
    ///   The returned index owns decoded strings and postings; it does not
    ///   borrow from this slice.
    ///
    /// # Returns
    ///
    /// A fully owned [`InvertedIndex`] when the header, version, and JSON
    /// payload are valid.
    ///
    /// # Errors
    ///
    /// Returns an index error for input shorter than five bytes, the wrong
    /// magic, or a version other than 1. Malformed or schema-incompatible JSON
    /// returns a serialization error. The method does not substitute an empty
    /// index, preserving Zeppelin's fail-loud artifact contract.
    ///
    /// # Examples
    ///
    /// Bytes produced by [`Self::to_bytes`] round-trip. A bitmap artifact or a
    /// future `ZFTS` version is rejected before its payload can be mistaken for
    /// current postings.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The input is a borrowed byte slice similar to `const uint8_t *` plus a
    /// length in C, but Rust guarantees it remains valid during decoding. Serde
    /// creates a separate owned object graph, so the caller may drop the bytes
    /// immediately after this method returns.
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

/// Constructs the object-store key for one cluster-local FTS artifact.
///
/// # Parameters
///
/// - `namespace`: Logical namespace prefix, already validated by the caller.
/// - `segment_id`: Segment that owns the cluster sidecar. For incrementally
///   carried clusters, callers must pass the owner resolved from the manifest,
///   which may differ from the active segment ID.
/// - `cluster_idx`: Zero-based cluster position within the logical segment.
///
/// # Returns
///
/// A key of the form
/// `<namespace>/segments/<segment_id>/fts_index_<cluster_idx>.bin`.
/// Constructing the string performs no I/O and does not prove the artifact is
/// visible in the manifest.
///
/// # Examples
///
/// Namespace `catalog`, owner `seg_01`, and cluster 7 produce
/// `catalog/segments/seg_01/fts_index_7.bin`.
pub fn fts_index_key(namespace: &str, segment_id: &str, cluster_idx: usize) -> String {
    format!("{namespace}/segments/{segment_id}/fts_index_{cluster_idx}.bin")
}

/// Constructs the conventional key for the optional segment FTS metadata JSON.
///
/// # Parameters
///
/// - `namespace`: Namespace object-prefix component.
/// - `segment_id`: Owning segment identifier.
///
/// # Returns
///
/// `<namespace>/segments/<segment_id>/fts_meta.json` as an owned string. The
/// current production compaction and query paths do not write or load this
/// object; constructing the key has no storage side effect.
///
/// # Examples
///
/// `fts_meta_key("catalog", "seg_01")` names
/// `catalog/segments/seg_01/fts_meta.json`.
pub fn fts_meta_key(namespace: &str, segment_id: &str) -> String {
    format!("{namespace}/segments/{segment_id}/fts_meta.json")
}

/// Optional JSON summary of full-text fields and corpus statistics by segment.
///
/// This type can aggregate per-cluster indexes and score them with segment-wide
/// IDF while retaining exact per-document lengths. It is distinct from the
/// compact [`crate::fts::global_index::GlobalInvertedIndex`]. No current
/// production caller persists or loads this metadata; it remains a public
/// artifact helper rather than the manifest-selected BM25 fast path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FtsSegmentMeta {
    /// Requested FTS field names in caller-provided order.
    ///
    /// Unlike [`Self::field_stats`], this vector is not sorted or deduplicated.
    pub fields: Vec<String>,
    /// Sum of all cluster vector counts, including vectors without indexed text.
    pub total_docs: u32,
    /// Segment-wide BM25 statistics keyed in deterministic field-name order.
    pub field_stats: BTreeMap<String, FtsFieldStats>,
}

/// Segment-wide BM25 corpus statistics for one configured text field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FtsFieldStats {
    /// Number of documents across all clusters that produced indexed tokens.
    pub doc_count: u32,
    /// Weighted mean token count across those indexed documents.
    pub avg_doc_length: f32,
    /// Normalized term to number of containing documents across all clusters.
    pub term_doc_freqs: BTreeMap<String, u32>,
}

impl FtsSegmentMeta {
    /// Encodes segment FTS metadata as human-readable JSON.
    ///
    /// # Returns
    ///
    /// Pretty-printed JSON bytes with no magic header or explicit version.
    /// `BTreeMap` fields use deterministic key order; the `fields` vector keeps
    /// caller order.
    ///
    /// # Errors
    ///
    /// Propagates Serde JSON encoding errors. No remote write occurs here.
    ///
    /// # Examples
    ///
    /// A tool can call this method and store the result at [`fts_meta_key`].
    /// That object does not become authoritative merely because it was uploaded.
    ///
    /// # Consistency
    ///
    /// Because this format has neither a header nor a version byte, persisted
    /// schema changes must remain Serde-compatible or be introduced under a new
    /// artifact contract.
    pub fn to_bytes(&self) -> Result<Bytes> {
        let json = serde_json::to_vec_pretty(self)?;
        Ok(Bytes::from(json))
    }

    /// Decodes the unversioned segment FTS metadata JSON representation.
    ///
    /// # Parameters
    ///
    /// - `data`: Complete JSON object contents.
    ///
    /// # Returns
    ///
    /// Owned metadata independent of the input buffer.
    ///
    /// # Errors
    ///
    /// Returns a serialization error for malformed JSON, missing required
    /// fields, or values incompatible with the current schema. There is no
    /// empty/default fallback.
    ///
    /// # Examples
    ///
    /// Output from [`Self::to_bytes`] round-trips. Passing the versioned binary
    /// bytes from [`InvertedIndex::to_bytes`] fails because this decoder expects
    /// JSON at byte zero.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(data)?)
    }
}

/// Builds token postings and BM25 statistics for one cluster field.
///
/// # Parameters
///
/// - `attrs`: Attribute maps in cluster position order.
/// - `field_name`: Attribute key to read. Missing and non-string values are not
///   indexed.
/// - `config`: Tokenization rules for the field.
///
/// # Returns
///
/// A field index. It may have zero documents and postings; the outer build
/// method decides whether to retain such an empty field.
///
/// # Performance
///
/// Performs one tokenization pass and one temporary term-frequency map per
/// indexed document. Final posting lists are sorted by position.
///
/// # Examples
///
/// For field `title`, a string `"rust rust storage"` contributes one `rust`
/// posting with `tf=2` and one `storage` posting with `tf=1`. An integer-valued
/// `title` contributes nothing.
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

        // Aggregate within the document first so a posting list has exactly one
        // entry per document, which makes its length the document frequency.
        // Tokens are owned and unused afterward, so they move into the map.
        let mut tf_map: HashMap<String, u32> = HashMap::new();
        for token in tokens {
            *tf_map.entry(token).or_insert(0) += 1;
        }

        // Moving each owned term out of tf_map avoids cloning it again.
        for (term, tf) in tf_map {
            postings.entry(term).or_default().push(Posting {
                position: position as u32,
                tf,
            });
        }
    }

    // HashMap iteration above is intentionally unordered; restore the persisted
    // position-order invariant explicitly.
    for entries in postings.values_mut() {
        entries.sort_by_key(|p| p.position);
    }

    // Once positions are unique per term, entry count is exactly document
    // frequency and need not be recomputed during search.
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

/// Reconstructs each indexed document's token length from its postings.
///
/// The persisted field format stores term frequencies but no separate document
/// length array. Summing a document's term frequencies exactly recovers the
/// number of indexed tokens used by BM25 length normalization.
///
/// # Parameters
///
/// - `field_index`: Borrowed field whose posting entries are internally
///   consistent.
///
/// # Returns
///
/// A map from cluster-local position to total indexed-token count. Documents
/// with no postings are absent rather than mapped to zero.
///
/// # Performance
///
/// Visits every posting once and allocates one hash-map entry per indexed
/// document.
///
/// # Examples
///
/// If position 4 has `rust: tf=2` and `storage: tf=1`, the returned length for
/// position 4 is 3.
fn compute_doc_lengths(field_index: &FieldIndex) -> HashMap<u32, u32> {
    let mut lengths: HashMap<u32, u32> = HashMap::new();
    for pl in field_index.postings.values() {
        for posting in &pl.entries {
            *lengths.entry(posting.position).or_insert(0) += posting.tf;
        }
    }
    lengths
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Focused contract tests for cluster-local FTS construction and search.
    //!
    //! These tests use in-memory attributes rather than object storage. They
    //! protect posting frequency/order, field handling, BM25 result ordering,
    //! prefix expansion, and the persisted `ZFTS` discriminator.

    use super::*;

    /// Creates a deterministic `content` configuration without stemming or
    /// stopword removal so fixtures map directly to persisted terms.
    ///
    /// # Returns
    ///
    /// One field configuration keyed by `content`.
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

    /// Converts text fixtures into owned single-field attribute maps.
    ///
    /// # Parameters
    ///
    /// - `texts`: Document strings in the cluster position order under test.
    ///
    /// # Returns
    ///
    /// One present `content` map per input string. Tests subsequently borrow
    /// these owned maps when calling [`InvertedIndex::build`].
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
    /// Protects basic field creation, document counts, and document frequency.
    ///
    /// A regression would misstate BM25 IDF even when tokenization succeeded.
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
    /// Verifies that one cluster index retains each independently configured field.
    ///
    /// This catches an implementation that accidentally reuses or overwrites a
    /// field accumulator while iterating configuration entries.
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
    /// Confirms that the versioned JSON artifact preserves its top-level shape.
    ///
    /// The round trip would fail if encoding and decoding disagreed about the
    /// header or required persisted fields.
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
    /// Ensures a non-FTS artifact discriminator fails loudly during decoding.
    ///
    /// Accepting `BAAD` would allow unrelated bytes to cross the artifact type
    /// boundary and fail later with a less useful error.
    fn test_magic_byte_validation() {
        let result = InvertedIndex::from_bytes(b"BAAD\x01{}");
        assert!(result.is_err());
    }

    #[test]
    /// Protects ascending document positions within every built posting list.
    ///
    /// Deterministic posting order is required even though per-document term
    /// frequency is first accumulated in an unordered hash map.
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
    /// Verifies corpus and per-term document frequencies used by BM25 IDF.
    ///
    /// It distinguishes document frequency from raw occurrence count.
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
    /// Confirms an empty cluster produces no field artifacts or phantom documents.
    fn test_empty_corpus() {
        let attrs: Vec<Option<HashMap<String, AttributeValue>>> = vec![];
        let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
            attrs.iter().map(|a| a.as_ref()).collect();
        let idx = InvertedIndex::build(&attr_refs, &make_config());
        assert_eq!(idx.vector_count, 0);
        assert!(idx.fields.is_empty());
    }

    #[test]
    /// Verifies repeated terms become one posting with a larger term frequency.
    ///
    /// This protects the one-posting-per-term-per-document invariant.
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
    /// Confirms exact-term search returns every containing document best-first.
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
    /// Exercises additive OR scoring when a query contains multiple terms.
    ///
    /// Documents containing both query terms should compete at the top rather
    /// than being emitted as separate per-term hits.
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
    /// Verifies the final token expands across every matching dictionary term.
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
    /// Confirms an unknown token returns an empty result set without fabrication.
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
    /// Confirms an unindexed field is a clean no-match result, not another field.
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
