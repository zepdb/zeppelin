//! Pre-tokenized WAL fragment cache for BM25 queries.
//!
//! Instead of brute-force tokenizing every document on every BM25 query,
//! this cache stores pre-tokenized term frequencies per document per field.
//! Fragments are keyed by their immutable ULID — once a fragment is cached,
//! its tokenized data never changes.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use ulid::Ulid;

use crate::fts::tokenizer::tokenize_text;
use crate::fts::types::FtsFieldConfig;
use crate::types::AttributeValue;
use crate::wal::fragment::WalFragment;

/// Per-document, per-field pre-tokenized data.
#[derive(Debug, Clone)]
pub struct DocTokenData {
    /// Number of tokens in this field for this document.
    pub doc_length: u32,
    /// Term → term frequency map.
    pub term_freqs: HashMap<String, u32>,
}

/// Pre-tokenized data for a single WAL fragment.
#[derive(Debug, Clone)]
pub struct CachedFragmentFts {
    /// Per-doc, per-field tokenized data.
    /// Key: (doc_id, field_name) → DocTokenData
    pub doc_field_data: HashMap<(String, String), DocTokenData>,
}

/// Cache of pre-tokenized WAL fragment data, keyed by fragment ULID.
///
/// Thread-safe (DashMap) and safe for concurrent reads/writes.
/// Fragments are immutable, so once cached, data never changes.
pub struct WalFtsCache {
    cache: Arc<DashMap<Ulid, CachedFragmentFts>>,
}

impl WalFtsCache {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(DashMap::new()),
        }
    }

    /// Get or compute pre-tokenized data for a fragment.
    ///
    /// If the fragment is already cached, returns the cached data.
    /// Otherwise, tokenizes all text fields and caches the result.
    pub fn get_or_tokenize(
        &self,
        fragment: &WalFragment,
        fts_configs: &HashMap<String, FtsFieldConfig>,
        fields_needed: &[&str],
    ) -> CachedFragmentFts {
        // Fast path: check cache first
        if let Some(cached) = self.cache.get(&fragment.id) {
            return cached.clone();
        }

        // Slow path: tokenize and cache
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

    /// Evict fragments that are no longer in the manifest (compacted away).
    pub fn evict_compacted(&self, active_fragment_ids: &[Ulid]) {
        let active_set: std::collections::HashSet<&Ulid> = active_fragment_ids.iter().collect();
        self.cache.retain(|id, _| active_set.contains(id));
    }

    /// Number of cached fragments.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

impl Default for WalFtsCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::VectorEntry;

    fn make_fragment(vectors: Vec<VectorEntry>) -> WalFragment {
        WalFragment {
            id: Ulid::new(),
            vectors,
            deletes: vec![],
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
    fn test_cache_hit() {
        let cache = WalFtsCache::new();
        let fragment = make_fragment(vec![make_vec_entry("v1", "hello world")]);
        let configs = make_configs();
        let fields = vec!["content"];

        // First call tokenizes
        let result1 = cache.get_or_tokenize(&fragment, &configs, &fields);
        assert_eq!(cache.len(), 1);

        // Second call hits cache
        let result2 = cache.get_or_tokenize(&fragment, &configs, &fields);
        assert_eq!(cache.len(), 1);

        // Both should have the same data
        assert_eq!(result1.doc_field_data.len(), result2.doc_field_data.len());
    }

    #[test]
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
    fn test_evict_compacted() {
        let cache = WalFtsCache::new();
        let f1 = make_fragment(vec![make_vec_entry("v1", "hello")]);
        let f2 = make_fragment(vec![make_vec_entry("v2", "world")]);
        let configs = make_configs();
        let fields = vec!["content"];

        cache.get_or_tokenize(&f1, &configs, &fields);
        cache.get_or_tokenize(&f2, &configs, &fields);
        assert_eq!(cache.len(), 2);

        // Evict f1 (only f2 is active)
        cache.evict_compacted(&[f2.id]);
        assert_eq!(cache.len(), 1);
    }
}
