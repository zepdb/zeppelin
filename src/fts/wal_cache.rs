//! Disposable in-memory token cache for BM25 scans of uncompacted WAL data.
//!
//! Strong-consistency BM25 queries must inspect every [`WalFragment`] selected
//! by their manifest snapshot. Re-running Unicode segmentation, stop-word
//! removal, and stemming for the same immutable fragment would waste CPU, so
//! [`WalFtsCache`] remembers each document's normalized term frequencies and
//! token count. [`crate::fts::wal_scan::wal_bm25_scan`] is the normal caller.
//!
//! This module performs no object-store reads and does not decide which WAL
//! fragments are visible. [`crate::query`] first reads an authoritative
//! manifest, [`crate::wal::reader::WalReader`] loads the referenced immutable
//! objects, and only then may this cache accelerate tokenization. A missing or
//! evicted entry is therefore a performance event, never permission to omit an
//! authoritative fragment.
//!
//! ```text
//! authoritative manifest snapshot
//!            |
//!            | visible uncompacted fragment IDs
//!            +---------------------------> evict_compacted
//!            |                                  |
//!            v                                  v
//! decoded immutable WalFragment          retain matching cache entries
//!            |
//!            v
//!       get_or_tokenize
//!        /           \
//!   ID hit           ID miss
//!      |                 |
//! deep-clone       tokenize requested fields
//! cached maps             |
//!      |             insert in DashMap
//!      +---------> owned CachedFragmentFts
//! ```
//!
//! ## Reading map
//!
//! 1. Start with [`DocTokenData`] for the cached unit of BM25 statistics.
//! 2. Read [`CachedFragmentFts`] for the fragment-level key space.
//! 3. Read [`WalFtsCache::get_or_tokenize`] for hit, miss, and concurrency
//!    behavior.
//! 4. Finish with [`WalFtsCache::evict_compacted`] for manifest-driven
//!    lifecycle cleanup.
//!
//! ## Invariants and current cache-key boundary
//!
//! - Fragment ULIDs identify immutable payloads. Reusing a ULID for different
//!   bytes would make every cached answer unsafe.
//! - Cached data is derived state. S3/MinIO and its manifest remain
//!   authoritative, and clearing the whole cache must preserve query results.
//! - Document text is tokenized with `prefix_mode = false`; query-side prefix
//!   behavior expands against these normal document tokens.
//! - The current key contains only the fragment ULID, not namespace, requested
//!   fields, or [`FtsFieldConfig`]. The first miss fixes the fields and
//!   configuration represented by that entry; later hits do not extend or
//!   re-tokenize it.
//!
//! TODO(doc): Verify whether the intended production key should include the
//! namespace, field set, and tokenization configuration, or whether callers
//! will guarantee one stable, complete tokenization context per fragment.
//!
//! ## Rust concepts used here
//!
//! [`Arc`] provides reference-counted ownership of the map and [`DashMap`]
//! provides sharded synchronization, so methods can mutate shared cache state
//! through `&self`. Java engineers can think of a concurrent map behind a
//! shared reference. In C, both lifetime and locking would normally be manual.
//! Rust guarantees the map remains allocated while an owner exists and that
//! its entries cannot be accessed without the map's synchronization protocol.
//! Cloning [`CachedFragmentFts`] is not an `Arc`-style pointer copy: it deeply
//! clones the owned strings and hash maps.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use ulid::Ulid;

use crate::fts::tokenizer::tokenize_text;
use crate::fts::FtsFieldConfig;
use crate::types::AttributeValue;
use crate::wal::fragment::WalFragment;

/// BM25 input derived from one document's value for one configured text field.
///
/// The data is independent of a query: document text always uses normal
/// tokenization, while query tokens and prefix behavior are computed later.
/// Because both fields are owned, a returned value can outlive the fragment
/// borrow used to create it.
///
/// # Examples
///
/// The field value `"cat dog cat"` with stemming and stop-word removal disabled
/// produces `doc_length = 3` and frequencies `{"cat": 2, "dog": 1}`.
#[derive(Debug, Clone)]
pub struct DocTokenData {
    /// Number of normalized tokens retained for this field.
    ///
    /// Stop words, over-length words, and other discarded input do not
    /// contribute. The WAL scanner uses this value for BM25 length
    /// normalization.
    pub doc_length: u32,
    /// Frequency of each normalized term within this document field.
    ///
    /// Keys reflect case folding and stemming from the field configuration;
    /// values count repeated occurrences after those transformations.
    pub term_freqs: HashMap<String, u32>,
}

/// Owned token statistics computed for selected fields in one WAL fragment.
///
/// Absence of a `(document, field)` key is deliberately not diagnostic: the
/// field may not have been requested, may lack configuration, may be absent or
/// non-string in the document, or may tokenize to no terms.
///
/// # Examples
///
/// A fragment containing documents `p1` and `p2`, with only `content`
/// requested, can contain keys `(p1, content)` and `(p2, content)` but no
/// `title` entries even when title attributes exist.
#[derive(Debug, Clone)]
pub struct CachedFragmentFts {
    /// Token data keyed by owned `(document ID, field name)` pairs.
    ///
    /// Both strings are cloned from the fragment/request context so this map
    /// has no lifetime dependency on either input.
    pub doc_field_data: HashMap<(String, String), DocTokenData>,
}

/// Process-local cache of pre-tokenized WAL fragment data.
///
/// The cache is safe for concurrent access but is neither persistent nor
/// authoritative. Its key is a fragment ULID, and its value is a complete
/// owned snapshot from the first tokenization miss for that ULID. See the
/// module-level cache-key caveat before changing call sites or FTS settings.
pub struct WalFtsCache {
    /// Sharded concurrent map shared through reference-counted ownership.
    ///
    /// A [`DashMap`] entry guard is held only long enough to clone a hit. The
    /// cache performs tokenization outside the map, so a slow miss does not hold
    /// a shard lock across the CPU-heavy work.
    cache: Arc<DashMap<Ulid, CachedFragmentFts>>,
}

impl WalFtsCache {
    /// Creates an empty process-local WAL token cache.
    ///
    /// # Returns
    ///
    /// A cache with no fragment entries. Construction performs no I/O and does
    /// not allocate token data.
    ///
    /// # Examples
    ///
    /// A server creates one cache during startup; its first strong BM25 query
    /// populates entries for the visible uncompacted fragments it scans.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The method returns an owned value rather than a nullable reference. The
    /// caller may move it into an outer `Arc<WalFtsCache>` shared by server
    /// tasks; Rust then prevents accidental use of the moved local binding.
    pub fn new() -> Self {
        Self {
            cache: Arc::new(DashMap::new()),
        }
    }

    /// Returns cached token statistics for a fragment or computes them once on a miss.
    ///
    /// A hit is based only on `fragment.id`; the supplied configuration and
    /// field slice are ignored after a matching entry exists. On a miss, the
    /// method visits vector upserts, skips deletes, and tokenizes only requested
    /// fields that both have a configuration and contain a string attribute.
    /// Empty token streams are not stored.
    ///
    /// # Parameters
    ///
    /// - `fragment`: Borrowed immutable fragment whose vector attributes provide
    ///   document text. Its checksum and manifest sequence are not inspected.
    /// - `fts_configs`: Borrowed field-to-tokenizer settings. A requested field
    ///   absent from this map is skipped rather than treated as an error.
    /// - `fields_needed`: Field names needed by the current ranking expression.
    ///   Duplicates repeat tokenization work on a miss but collapse to one map
    ///   entry; the normal WAL scanner supplies a unique set.
    ///
    /// # Returns
    ///
    /// An owned [`CachedFragmentFts`]. Both hit and miss paths leave a separate
    /// deep-cloned value in either the caller or the map, so subsequent eviction
    /// does not invalidate the returned data.
    ///
    /// # Side Effects
    ///
    /// A miss inserts one entry into the shared [`DashMap`]. The method performs
    /// no object-store, manifest, disk-cache, logging, or metric operation.
    ///
    /// # Consistency
    ///
    /// The fragment must obey Zeppelin's immutable-ULID invariant. The cache is
    /// disposable derived data and cannot make an unreferenced fragment visible.
    /// Because lookup and insertion are separate, concurrent misses may both
    /// tokenize and the later insertion replaces the earlier value. This is
    /// result-equivalent only when both calls use the same field/configuration
    /// context.
    ///
    /// # Performance
    ///
    /// A miss is linear in fragment vectors, requested fields, and emitted
    /// tokens, plus owned-string/hash-map allocation. A hit avoids tokenization
    /// but deep-clones every cached key, term, and frequency map; it is not an
    /// `O(1)` shared-pointer return.
    ///
    /// # Examples
    ///
    /// ```text
    /// fragment 01H... contains p1.content = "cat dog cat"
    /// fields_needed = ["content"]
    /// first call  -> tokenize, cache {(p1, content): len 3, cat 2, dog 1}
    /// second call -> clone that cached map without tokenizing p1 again
    /// ```
    ///
    /// If the first call requests only `content`, a later call for `title`
    /// currently hits the same ULID entry and receives no newly computed title
    /// data. Callers must account for the module-level cache-key boundary.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `&WalFragment`, `&HashMap`, and `&[&str]` are checked shared borrows: like
    /// read-only Java references or `const` C pointers, but non-null and valid
    /// for the call by construction. `match` handles every `Option` case, so
    /// missing attributes cannot become a null dereference. The returned maps
    /// are owned allocations, not views into the borrowed fragment.
    pub fn get_or_tokenize(
        &self,
        fragment: &WalFragment,
        fts_configs: &HashMap<String, FtsFieldConfig>,
        fields_needed: &[&str],
    ) -> CachedFragmentFts {
        // Clone while the DashMap guard is alive, then release the shard before
        // returning owned data to query scoring.
        if let Some(cached) = self.cache.get(&fragment.id) {
            return cached.clone();
        }

        // Tokenization intentionally happens without a map guard so unrelated
        // cache operations are not serialized behind CPU work.
        let mut doc_field_data = HashMap::new();

        for vec in &fragment.vectors {
            let attrs = match &vec.attributes {
                Some(a) => a,
                None => continue,
            };

            for &field_name in fields_needed {
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

                let mut term_freqs: HashMap<String, u32> = HashMap::new();
                for token in &tokens {
                    *term_freqs.entry(token.clone()).or_insert(0) += 1;
                }

                doc_field_data.insert(
                    (vec.id.clone(), field_name.to_string()),
                    DocTokenData {
                        doc_length,
                        term_freqs,
                    },
                );
            }
        }

        let cached = CachedFragmentFts { doc_field_data };
        self.cache.insert(fragment.id, cached.clone());
        cached
    }

    /// Retains only entries whose fragment IDs occur in the supplied active set.
    ///
    /// The BM25 query coordinator derives `active_fragment_ids` from the
    /// authoritative manifest snapshot before scanning. This bounds memory once
    /// compaction publishes a segment and removes incorporated WAL references.
    /// Eviction does not delete WAL objects or change manifest visibility.
    ///
    /// # Parameters
    ///
    /// - `active_fragment_ids`: Borrowed IDs that should remain cached. An empty
    ///   slice clears the map.
    ///
    /// # Side Effects
    ///
    /// Removes every shared-map entry whose ULID is absent from the slice.
    /// Already returned [`CachedFragmentFts`] values remain valid because they
    /// own their data.
    ///
    /// # Consistency
    ///
    /// This is best-effort lifecycle cleanup, not a visibility check. A query
    /// still scans the fragments selected by its manifest even if their cache
    /// entries disappear concurrently.
    ///
    /// The server owns one cache across namespaces, while this method receives
    /// one namespace's active IDs and the key contains no namespace. Therefore
    /// a call currently evicts entries belonging to other namespaces as well;
    /// this reduces hit rate but does not change results because misses rebuild.
    ///
    /// TODO(doc): Verify whether cross-namespace eviction is intentional or the
    /// cache lifecycle should retain entries per namespace.
    ///
    /// # Performance
    ///
    /// Builds an `O(a)` borrowed-ID set for `a` active IDs, then scans all `c`
    /// cache entries in `O(a + c)` expected time. It clones no ULIDs.
    ///
    /// # Examples
    ///
    /// If the cache holds fragments `[10, 11, 12]` and a newly published
    /// manifest retains only `[12]`, this call removes token data for `10` and
    /// `11`. A still-running query that already cloned those values is unaffected.
    pub fn evict_compacted(&self, active_fragment_ids: &[Ulid]) {
        let active_set: std::collections::HashSet<&Ulid> = active_fragment_ids.iter().collect();
        self.cache.retain(|id, _| active_set.contains(id));
    }

    /// Reports the number of fragment entries currently retained.
    ///
    /// # Returns
    ///
    /// A momentary concurrent-map length. Other tasks may insert or evict
    /// immediately after it is read, so it is suitable for diagnostics and
    /// tests rather than synchronization decisions.
    ///
    /// # Examples
    ///
    /// A new cache reports zero; after tokenizing one previously unseen
    /// fragment it normally reports one.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Reports whether the cache has no retained fragment entries at this instant.
    ///
    /// # Returns
    ///
    /// `true` for a momentarily empty concurrent map and `false` otherwise.
    /// This observation can become stale as soon as the method returns.
    ///
    /// # Examples
    ///
    /// Startup and eviction with an empty active-ID list both leave this method
    /// returning `true` until another query populates an entry.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

impl Default for WalFtsCache {
    /// Creates the same empty cache as [`WalFtsCache::new`].
    ///
    /// # Returns
    ///
    /// A fresh process-local cache with no tokenized fragments.
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Focused checks for hit reuse, token statistics, and manifest-style eviction.
    //!
    //! Fixtures construct decoded fragments directly. They deliberately use a
    //! dummy checksum because this cache never performs WAL serialization,
    //! integrity verification, object-store access, or manifest publication.

    use super::*;
    use crate::types::VectorEntry;

    /// Wraps owned vector fixtures in a fresh, delete-free fragment.
    ///
    /// # Parameters
    ///
    /// - `vectors`: Test documents moved into the fragment.
    ///
    /// # Returns
    ///
    /// A fragment with a unique cache key and a deliberately unused checksum.
    fn make_fragment(vectors: Vec<VectorEntry>) -> WalFragment {
        WalFragment {
            id: Ulid::new(),
            vectors,
            deletes: vec![],
            checksum: 0,
        }
    }

    /// Builds one document with a string-valued `content` attribute.
    ///
    /// # Parameters
    ///
    /// - `id`: Logical document ID copied into owned fixture storage.
    /// - `text`: Content copied into the attribute map.
    ///
    /// # Returns
    ///
    /// A one-dimensional vector entry suitable for token-cache tests.
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

    /// Creates deterministic `content` tokenization without stemming or stop words.
    ///
    /// # Returns
    ///
    /// A single-field configuration map that preserves fixture words exactly.
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
    /// Protects reuse of one ULID entry across repeated identical requests.
    ///
    /// A regression would grow the map or return structurally different token
    /// data on the second call.
    fn test_cache_hit() {
        let cache = WalFtsCache::new();
        let fragment = make_fragment(vec![make_vec_entry("v1", "hello world")]);
        let configs = make_configs();
        let fields = vec!["content"];

        // The miss establishes the one cache entry.
        let result1 = cache.get_or_tokenize(&fragment, &configs, &fields);
        assert_eq!(cache.len(), 1);

        // An identical request must reuse rather than append another entry.
        let result2 = cache.get_or_tokenize(&fragment, &configs, &fields);
        assert_eq!(cache.len(), 1);

        // Equal shapes demonstrate that the owned hit clone retained all data.
        assert_eq!(result1.doc_field_data.len(), result2.doc_field_data.len());
    }

    #[test]
    /// Protects post-tokenization document lengths and repeated-term counts.
    ///
    /// This catches accidental counting of unique terms instead of token
    /// occurrences and accidental mixing of data between document IDs.
    fn test_tokenization_correct() {
        let cache = WalFtsCache::new();
        let fragment = make_fragment(vec![
            make_vec_entry("v1", "cat dog cat"),
            make_vec_entry("v2", "bird"),
        ]);
        let configs = make_configs();
        let fields = vec!["content"];

        let result = cache.get_or_tokenize(&fragment, &configs, &fields);

        let v1_data = result
            .doc_field_data
            .get(&("v1".to_string(), "content".to_string()))
            .unwrap();
        assert_eq!(v1_data.doc_length, 3);
        assert_eq!(*v1_data.term_freqs.get("cat").unwrap(), 2);
        assert_eq!(*v1_data.term_freqs.get("dog").unwrap(), 1);

        let v2_data = result
            .doc_field_data
            .get(&("v2".to_string(), "content".to_string()))
            .unwrap();
        assert_eq!(v2_data.doc_length, 1);
    }

    #[test]
    /// Protects retain-style cleanup after a manifest drops compacted fragments.
    ///
    /// The active fragment must survive while an unlisted peer is removed.
    fn test_evict_compacted() {
        let cache = WalFtsCache::new();
        let f1 = make_fragment(vec![make_vec_entry("v1", "hello")]);
        let f2 = make_fragment(vec![make_vec_entry("v2", "world")]);
        let configs = make_configs();
        let fields = vec!["content"];

        cache.get_or_tokenize(&f1, &configs, &fields);
        cache.get_or_tokenize(&f2, &configs, &fields);
        assert_eq!(cache.len(), 2);

        // Model a manifest snapshot where compaction removed `f1` but retained `f2`.
        cache.evict_compacted(&[f2.id]);
        assert_eq!(cache.len(), 1);
    }
}
