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
//!            | visible origin-qualified fragment identities
//!            +--------------------> evict_compacted_located
//!            |                                  |
//!            v                                  v
//! decoded immutable WalFragment          retain matching cache entries
//!            |
//!            v
//!       get_or_tokenize
//!        /           \
//! identity hit      identity miss
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
//! 4. Finish with [`WalFtsCache::evict_compacted_located`] for manifest-driven
//!    lifecycle cleanup.
//!
//! ## Invariants and cache-key boundary
//!
//! - A physical namespace lifetime plus fragment ULID identifies one immutable
//!   payload. Equal ULIDs from different origins are distinct cache entries.
//! - A canonical sorted field/config discriminator identifies the derived token
//!   view. Logical namespaces with equal analysis contexts share work; contexts
//!   with different fields or configuration never alias.
//! - Cached data is derived state. S3/MinIO and its manifest remain
//!   authoritative, and clearing the whole cache must preserve query results.
//! - Document text is tokenized with `prefix_mode = false`; query-side prefix
//!   behavior expands against these normal document tokens.
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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dashmap::DashMap;

use crate::fts::tokenizer::tokenize_text;
use crate::fts::{FtsFieldConfig, FtsLanguage};
use crate::namespace::branching::ArtifactOrigin;
use crate::types::AttributeValue;
use crate::wal::fragment::WalFragment;
use crate::wal::input_fragment::EncoderInputWalFragment;
use crate::wal::manifest::{LocatedFragmentIdentity, LocatedInputFragmentIdentity};

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
/// authoritative. Its key combines the fragment's physical namespace lifetime
/// and ULID with a canonical requested-field/full-config discriminator. Equal
/// physical data can therefore be reused only when its derived token view is
/// also equal.
pub struct WalFtsCache {
    /// Sharded concurrent map shared through reference-counted ownership.
    ///
    /// A [`DashMap`] entry guard is held only long enough to clone a hit. The
    /// cache performs tokenization outside the map, so a slow miss does not hold
    /// a shard lock across the CPU-heavy work.
    cache: Arc<DashMap<WalFtsCacheKey, CacheEntry>>,
}

/// Complete identity of one cached tokenization result.
#[derive(Clone, PartialEq, Eq, Hash)]
struct WalFtsCacheKey {
    identity: WalFtsFragmentIdentity,
    discriminator: FtsCacheDiscriminator,
}

#[derive(Clone, PartialEq, Eq, Hash)]
enum WalFtsFragmentIdentity {
    Dense(LocatedFragmentIdentity),
    Input(LocatedInputFragmentIdentity),
}

/// Canonical requested-field and configuration projection.
#[derive(Clone, PartialEq, Eq, Hash)]
struct FtsCacheDiscriminator {
    fields: Vec<FtsFieldCacheDiscriminator>,
}

/// Stable cache identity for one configured requested field.
#[derive(Clone, PartialEq, Eq, Hash)]
struct FtsFieldCacheDiscriminator {
    field: String,
    language: FtsLanguageCacheDiscriminator,
    stemming: bool,
    remove_stopwords: bool,
    case_sensitive: bool,
    k1_bits: u32,
    b_bits: u32,
    max_token_length: usize,
}

/// Hashable exhaustive projection of the persisted analyzer language.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum FtsLanguageCacheDiscriminator {
    English,
}

impl FtsCacheDiscriminator {
    /// Build an order- and duplicate-insensitive context key.
    fn new(fts_configs: &HashMap<String, FtsFieldConfig>, fields_needed: &[&str]) -> Self {
        let mut fields = fields_needed
            .iter()
            .filter_map(|field| {
                let config = fts_configs.get(*field)?;
                let language = match config.language {
                    FtsLanguage::English => FtsLanguageCacheDiscriminator::English,
                };
                Some(FtsFieldCacheDiscriminator {
                    field: (*field).to_string(),
                    language,
                    stemming: config.stemming,
                    remove_stopwords: config.remove_stopwords,
                    case_sensitive: config.case_sensitive,
                    k1_bits: config.k1.to_bits(),
                    b_bits: config.b.to_bits(),
                    max_token_length: config.max_token_length,
                })
            })
            .collect::<Vec<_>>();
        fields.sort_unstable_by(|left, right| left.field.cmp(&right.field));
        fields.dedup_by(|left, right| left.field == right.field);
        Self { fields }
    }
}

/// One physical token snapshot and the logical namespace lifetimes using it.
struct CacheEntry {
    logical_origins: HashSet<ArtifactOrigin>,
    token_data: CachedFragmentFts,
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
    /// A hit is based on the fragment's origin-qualified identity plus a
    /// canonical projection of configured requested fields. Both hits and
    /// misses register the logical namespace lifetime that is currently using
    /// that exact derived view. On a miss, the method visits vector upserts,
    /// skips deletes, and tokenizes only requested fields that both have a
    /// configuration and contain a string attribute. Empty token streams are
    /// not stored.
    ///
    /// # Parameters
    ///
    /// - `logical_origin`: Exact target namespace lifetime whose manifest made
    ///   this physical fragment visible.
    /// - `identity`: Exact physical namespace lifetime and ULID of `fragment`.
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
    /// The fragment must obey Zeppelin's immutable origin-plus-ULID invariant,
    /// and `identity.id` must equal `fragment.id` or the method fails loudly.
    /// The cache is disposable derived data and cannot make an unreferenced
    /// fragment visible. Concurrent misses may both tokenize, but entry
    /// insertion keeps the first equal-context snapshot and only adds the later
    /// caller's logical owner. Different field/configuration contexts use
    /// different entries even when the immutable fragment identity is shared.
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
    /// computes and retains a separate derived view of the same fragment.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `&WalFragment`, `&HashMap`, and `&[&str]` are checked shared borrows: like
    /// read-only Java references or `const` C pointers, but non-null and valid
    /// for the call by construction. `match` handles every `Option` case, so
    /// missing attributes cannot become a null dereference. The returned maps
    /// are owned allocations, not views into the borrowed fragment.
    pub(crate) fn get_or_tokenize(
        &self,
        logical_origin: &ArtifactOrigin,
        identity: &LocatedFragmentIdentity,
        fragment: &WalFragment,
        fts_configs: &HashMap<String, FtsFieldConfig>,
        fields_needed: &[&str],
    ) -> CachedFragmentFts {
        assert_eq!(
            identity.id, fragment.id,
            "WAL FTS cache identity does not match decoded fragment"
        );
        let cache_key = WalFtsCacheKey {
            identity: WalFtsFragmentIdentity::Dense(identity.clone()),
            discriminator: FtsCacheDiscriminator::new(fts_configs, fields_needed),
        };
        // Clone while the DashMap guard is alive, then release the shard before
        // returning owned data to query scoring.
        if let Some(mut cached) = self.cache.get_mut(&cache_key) {
            cached.logical_origins.insert(logical_origin.clone());
            return cached.token_data.clone();
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

        let token_data = CachedFragmentFts { doc_field_data };
        match self.cache.entry(cache_key) {
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                occupied
                    .get_mut()
                    .logical_origins
                    .insert(logical_origin.clone());
                occupied.get().token_data.clone()
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                vacant.insert(CacheEntry {
                    logical_origins: HashSet::from([logical_origin.clone()]),
                    token_data: token_data.clone(),
                });
                token_data
            }
        }
    }

    /// Returns cached token statistics for one immutable typed-input fragment.
    pub(crate) fn get_or_tokenize_input(
        &self,
        logical_origin: &ArtifactOrigin,
        identity: &LocatedInputFragmentIdentity,
        fragment: &EncoderInputWalFragment,
        fts_configs: &HashMap<String, FtsFieldConfig>,
        fields_needed: &[&str],
    ) -> CachedFragmentFts {
        assert_eq!(
            identity.id, fragment.id,
            "input WAL FTS cache identity does not match decoded fragment"
        );
        let cache_key = WalFtsCacheKey {
            identity: WalFtsFragmentIdentity::Input(identity.clone()),
            discriminator: FtsCacheDiscriminator::new(fts_configs, fields_needed),
        };
        if let Some(mut cached) = self.cache.get_mut(&cache_key) {
            cached.logical_origins.insert(logical_origin.clone());
            return cached.token_data.clone();
        }

        let mut doc_field_data = HashMap::new();
        for record in &fragment.upserts {
            let Some(attributes) = record.attributes.as_ref() else {
                continue;
            };
            for &field_name in fields_needed {
                let Some(config) = fts_configs.get(field_name) else {
                    continue;
                };
                let Some(AttributeValue::String(text)) = attributes.get(field_name) else {
                    continue;
                };
                let tokens = tokenize_text(text, config, false);
                let doc_length = tokens.len() as u32;
                if doc_length == 0 {
                    continue;
                }
                let mut term_freqs = HashMap::new();
                for token in tokens {
                    *term_freqs.entry(token).or_insert(0) += 1;
                }
                doc_field_data.insert(
                    (record.id.clone(), field_name.to_string()),
                    DocTokenData {
                        doc_length,
                        term_freqs,
                    },
                );
            }
        }

        let token_data = CachedFragmentFts { doc_field_data };
        match self.cache.entry(cache_key) {
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                occupied
                    .get_mut()
                    .logical_origins
                    .insert(logical_origin.clone());
                occupied.get().token_data.clone()
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                vacant.insert(CacheEntry {
                    logical_origins: HashSet::from([logical_origin.clone()]),
                    token_data: token_data.clone(),
                });
                token_data
            }
        }
    }

    /// Retires one logical owner's references absent from its active set.
    ///
    /// The BM25 query coordinator derives `active_fragment_identities` from the
    /// authoritative manifest snapshot before scanning. This bounds memory once
    /// compaction publishes a segment and removes incorporated WAL references.
    /// Eviction does not delete WAL objects or change manifest visibility.
    ///
    /// # Parameters
    ///
    /// - `logical_origin`: Exact logical namespace lifetime being reconciled.
    /// - `active_fragment_identities`: Origin-qualified physical identities that
    ///   remain visible to this logical owner. An empty slice retires all of the
    ///   owner's observations without disturbing other owners.
    ///
    /// # Side Effects
    ///
    /// Removes `logical_origin` from entries absent from its active set, then
    /// removes only entries with no remaining logical owners. Already returned
    /// [`CachedFragmentFts`] values remain valid because they own their data.
    ///
    /// # Consistency
    ///
    /// This is best-effort lifecycle cleanup, not a visibility check. A query
    /// still scans the fragments selected by its manifest even if their cache
    /// entries disappear concurrently.
    ///
    /// # Performance
    ///
    /// Builds an `O(a)` owned-identity set for `a` active refs, then scans all
    /// `c` cache entries in `O(a + c)` expected time.
    ///
    /// # Examples
    ///
    /// If the cache holds fragments `[10, 11, 12]` and a newly published
    /// manifest retains only `[12]`, this call removes token data for `10` and
    /// `11`. A still-running query that already cloned those values is unaffected.
    pub(crate) fn evict_compacted_located(
        &self,
        logical_origin: &ArtifactOrigin,
        active_fragment_identities: &[LocatedFragmentIdentity],
    ) {
        let active = active_fragment_identities
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        self.cache.retain(|key, entry| {
            let remains_active = match &key.identity {
                WalFtsFragmentIdentity::Dense(identity) => active.contains(identity),
                WalFtsFragmentIdentity::Input(_) => true,
            };
            if entry.logical_origins.contains(logical_origin) && !remains_active {
                entry.logical_origins.remove(logical_origin);
            }
            !entry.logical_origins.is_empty()
        });
    }

    /// Retires typed-input cache entries absent from one authoritative manifest.
    pub(crate) fn evict_input_fragments_located(
        &self,
        logical_origin: &ArtifactOrigin,
        active_fragment_identities: &[LocatedInputFragmentIdentity],
    ) {
        let active = active_fragment_identities
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        self.cache.retain(|key, entry| {
            let remains_active = match &key.identity {
                WalFtsFragmentIdentity::Dense(_) => true,
                WalFtsFragmentIdentity::Input(identity) => active.contains(identity),
            };
            if entry.logical_origins.contains(logical_origin) && !remains_active {
                entry.logical_origins.remove(logical_origin);
            }
            !entry.logical_origins.is_empty()
        });
    }

    /// Reports the number of fragment-and-analysis-context entries retained.
    ///
    /// # Returns
    ///
    /// A momentary concurrent-map length. Other tasks may insert or evict
    /// immediately after it is read, so it is suitable for diagnostics and
    /// tests rather than synchronization decisions.
    ///
    /// # Examples
    ///
    /// A new cache reports zero; one fragment tokenized under two distinct
    /// field/configuration contexts reports two.
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
    use crate::namespace::branching::ArtifactOrigin;
    use crate::namespace::{NamespaceId, NamespaceIncarnationId};
    use crate::types::VectorEntry;
    use ulid::Ulid;

    fn origin(namespace: &str, incarnation: u128) -> ArtifactOrigin {
        ArtifactOrigin {
            namespace: NamespaceId::parse(namespace).unwrap(),
            incarnation: NamespaceIncarnationId::from_uuid(uuid::Uuid::from_u128(incarnation)),
        }
    }

    fn identity(origin: &ArtifactOrigin, id: Ulid) -> LocatedFragmentIdentity {
        LocatedFragmentIdentity {
            physical_origin: origin.clone(),
            id,
        }
    }

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
        let owner = origin("owner", 1);
        let identity = identity(&owner, fragment.id);
        let configs = make_configs();
        let fields = vec!["content"];

        // The miss establishes the one cache entry.
        let result1 = cache.get_or_tokenize(&owner, &identity, &fragment, &configs, &fields);
        assert_eq!(cache.len(), 1);

        // An identical request must reuse rather than append another entry.
        let result2 = cache.get_or_tokenize(&owner, &identity, &fragment, &configs, &fields);
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
        let owner = origin("owner", 1);
        let configs = make_configs();
        let fields = vec!["content"];

        let result = cache.get_or_tokenize(
            &owner,
            &identity(&owner, fragment.id),
            &fragment,
            &configs,
            &fields,
        );

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
        let owner = origin("owner", 1);
        let f1_identity = identity(&owner, f1.id);
        let f2_identity = identity(&owner, f2.id);
        let configs = make_configs();
        let fields = vec!["content"];

        cache.get_or_tokenize(&owner, &f1_identity, &f1, &configs, &fields);
        cache.get_or_tokenize(&owner, &f2_identity, &f2, &configs, &fields);
        assert_eq!(cache.len(), 2);

        // Model a manifest snapshot where compaction removed `f1` but retained `f2`.
        cache.evict_compacted_located(&owner, &[f2_identity]);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn equal_ulids_from_different_physical_origins_do_not_alias() {
        let cache = WalFtsCache::new();
        let shared_id = Ulid::new();
        let source = origin("source", 1);
        let target = origin("target", 2);
        let logical = origin("logical", 3);
        let source_fragment = WalFragment {
            id: shared_id,
            vectors: vec![make_vec_entry("source-doc", "source term")],
            deletes: Vec::new(),
            checksum: 0,
        };
        let target_fragment = WalFragment {
            id: shared_id,
            vectors: vec![make_vec_entry("target-doc", "target term")],
            deletes: Vec::new(),
            checksum: 0,
        };
        let configs = make_configs();
        let fields = ["content"];

        let source_tokens = cache.get_or_tokenize(
            &logical,
            &identity(&source, shared_id),
            &source_fragment,
            &configs,
            &fields,
        );
        let target_tokens = cache.get_or_tokenize(
            &logical,
            &identity(&target, shared_id),
            &target_fragment,
            &configs,
            &fields,
        );

        assert!(source_tokens
            .doc_field_data
            .contains_key(&("source-doc".to_string(), "content".to_string())));
        assert!(target_tokens
            .doc_field_data
            .contains_key(&("target-doc".to_string(), "content".to_string())));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn shared_physical_entry_survives_one_logical_scope_eviction() {
        let cache = WalFtsCache::new();
        let source = origin("source", 1);
        let target = origin("target", 2);
        let fragment = make_fragment(vec![make_vec_entry("shared-doc", "shared term")]);
        let identity = identity(&source, fragment.id);
        let configs = make_configs();
        let fields = ["content"];

        cache.get_or_tokenize(&source, &identity, &fragment, &configs, &fields);
        cache.get_or_tokenize(&target, &identity, &fragment, &configs, &fields);

        cache.evict_compacted_located(&target, &[]);
        assert_eq!(cache.len(), 1);

        cache.evict_compacted_located(&source, &[]);
        assert!(cache.is_empty());
    }

    #[test]
    fn cache_discriminator_separates_logical_field_sets_for_one_physical_fragment() {
        let cache = WalFtsCache::new();
        let source = origin("source", 1);
        let branch = origin("branch", 2);
        let mut vector = make_vec_entry("shared-doc", "source content");
        vector.attributes.as_mut().unwrap().insert(
            "title".to_string(),
            AttributeValue::String("branch title".to_string()),
        );
        let fragment = make_fragment(vec![vector]);
        let identity = identity(&source, fragment.id);
        let mut configs = make_configs();
        configs.insert(
            "title".to_string(),
            FtsFieldConfig {
                stemming: false,
                remove_stopwords: false,
                ..Default::default()
            },
        );

        let source_tokens =
            cache.get_or_tokenize(&source, &identity, &fragment, &configs, &["content"]);
        let branch_tokens =
            cache.get_or_tokenize(&branch, &identity, &fragment, &configs, &["title"]);

        assert!(source_tokens
            .doc_field_data
            .contains_key(&("shared-doc".to_string(), "content".to_string())));
        assert!(branch_tokens
            .doc_field_data
            .contains_key(&("shared-doc".to_string(), "title".to_string())));
        assert!(!branch_tokens
            .doc_field_data
            .contains_key(&("shared-doc".to_string(), "content".to_string())));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn cache_discriminator_separates_logical_analyzers_for_one_physical_fragment() {
        let cache = WalFtsCache::new();
        let source = origin("source", 1);
        let branch = origin("branch", 2);
        let fragment = make_fragment(vec![make_vec_entry("shared-doc", "MiXeD")]);
        let identity = identity(&source, fragment.id);
        let source_configs = make_configs();
        let mut branch_configs = make_configs();
        branch_configs.get_mut("content").unwrap().case_sensitive = true;

        let source_tokens =
            cache.get_or_tokenize(&source, &identity, &fragment, &source_configs, &["content"]);
        let branch_tokens =
            cache.get_or_tokenize(&branch, &identity, &fragment, &branch_configs, &["content"]);

        let source_data = source_tokens
            .doc_field_data
            .get(&("shared-doc".to_string(), "content".to_string()))
            .unwrap();
        let branch_data = branch_tokens
            .doc_field_data
            .get(&("shared-doc".to_string(), "content".to_string()))
            .unwrap();
        assert!(source_data.term_freqs.contains_key("mixed"));
        assert!(branch_data.term_freqs.contains_key("MiXeD"));
        assert!(!branch_data.term_freqs.contains_key("mixed"));
        assert_eq!(cache.len(), 2);
    }
}
