//! Bounded process-local memo for decoded immutable FTS artifacts.
//!
//! The authoritative manifest selects an exact, segment-scoped S3 key before
//! query execution reaches this module. [`DecodedArtifactCache`] only avoids
//! repeatedly decoding the write-once bytes at that key. It never discovers a
//! segment, decides visibility, or substitutes local state for S3 authority.
//!
//! Both cached variants are disposable. Clearing or evicting an entry changes
//! CPU work only: the next lookup falls through to the caller's existing byte
//! fetch and decodes the same immutable object again. Failed fetches and failed
//! decodes are returned loudly and are never cached.

use std::future::Future;
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use dashmap::DashMap;
use rand::Rng;

use crate::error::{Result, ZeppelinError};
use crate::fts::global_index::{GlobalInvertedIndex, GlobalPosting};
use crate::fts::inverted_index::{InvertedIndex, Posting};
use crate::retrieval_scope::{ScopedAnnIndex, ScopedFtsIndex, ScopedSegmentCorpus};

/// Number of candidates compared during one approximate-LRU choice.
const EVICTION_SAMPLE_SIZE: usize = 16;

/// One deliberately supported decoded or derived immutable artifact family.
enum CachedArtifact {
    GlobalFts(Arc<GlobalInvertedIndex>),
    ClusterFts(Arc<InvertedIndex>),
    SegmentCorpus(Arc<ScopedSegmentCorpus>),
    ScopedAnn(Arc<ScopedAnnIndex>),
    ScopedFts(Arc<ScopedFtsIndex>),
}

/// Shared decoded value plus approximate capacity and recency metadata.
struct CacheEntry {
    artifact: CachedArtifact,
    size_bytes: usize,
    last_accessed: Instant,
}

/// Byte-bounded memo of decoded, write-once segment FTS artifacts.
pub struct DecodedArtifactCache {
    entries: DashMap<String, CacheEntry>,
    bytes: AtomicUsize,
    max_bytes: usize,
    decode_count: AtomicU64,
    global_decode_count: AtomicU64,
    cluster_decode_count: AtomicU64,
    mutation: Mutex<()>,
}

impl DecodedArtifactCache {
    /// Creates an empty cache with an approximate decoded-payload byte budget.
    #[must_use]
    pub fn new(max_bytes: usize) -> Self {
        Self {
            entries: DashMap::new(),
            bytes: AtomicUsize::new(0),
            max_bytes,
            decode_count: AtomicU64::new(0),
            global_decode_count: AtomicU64::new(0),
            cluster_decode_count: AtomicU64::new(0),
            mutation: Mutex::new(()),
        }
    }

    /// Returns or decodes one segment-wide immutable FTS index.
    ///
    /// Concurrent misses may perform duplicate decodes. Both values come from
    /// the same immutable key, so a later insertion can safely replace the
    /// earlier equivalent value; the diagnostic counter records both decodes.
    pub async fn get_or_decode_global_fts<F, Fut>(
        &self,
        key: &str,
        fetch: F,
    ) -> Result<Arc<GlobalInvertedIndex>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Bytes>>,
    {
        if let Some(index) = self.get_global(key)? {
            return Ok(index);
        }

        let bytes = fetch().await?;
        let index = Arc::new(GlobalInvertedIndex::from_bytes(&bytes)?);
        self.decode_count.fetch_add(1, Ordering::Relaxed);
        self.global_decode_count.fetch_add(1, Ordering::Relaxed);
        let size_bytes = approximate_global_size(&index)
            .checked_add(key.len())
            .unwrap_or_else(|| panic!("decoded artifact cache entry size overflowed"));
        self.insert(
            key,
            CachedArtifact::GlobalFts(Arc::clone(&index)),
            size_bytes,
        );
        Ok(index)
    }

    /// Returns or decodes one legacy cluster-local immutable FTS index.
    pub async fn get_or_decode_cluster_fts<F, Fut>(
        &self,
        key: &str,
        fetch: F,
    ) -> Result<Arc<InvertedIndex>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Bytes>>,
    {
        if let Some(index) = self.get_cluster(key)? {
            return Ok(index);
        }

        let bytes = fetch().await?;
        let index = Arc::new(InvertedIndex::from_bytes(&bytes)?);
        self.decode_count.fetch_add(1, Ordering::Relaxed);
        self.cluster_decode_count.fetch_add(1, Ordering::Relaxed);
        let size_bytes = approximate_cluster_size(&index)
            .checked_add(key.len())
            .unwrap_or_else(|| panic!("decoded artifact cache entry size overflowed"));
        self.insert(
            key,
            CachedArtifact::ClusterFts(Arc::clone(&index)),
            size_bytes,
        );
        Ok(index)
    }

    /// Returns or derives one logical corpus from a manifest-selected segment.
    pub(crate) async fn get_or_build_segment_corpus<F, Fut>(
        &self,
        key: &str,
        build: F,
    ) -> Result<Arc<ScopedSegmentCorpus>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<ScopedSegmentCorpus>>,
    {
        if let Some(corpus) = self.get_segment_corpus(key)? {
            return Ok(corpus);
        }
        let corpus = Arc::new(build().await?);
        let size_bytes = corpus
            .estimated_size_bytes()
            .checked_add(key.len())
            .unwrap_or_else(|| panic!("decoded artifact cache entry size overflowed"));
        self.insert(
            key,
            CachedArtifact::SegmentCorpus(Arc::clone(&corpus)),
            size_bytes,
        );
        Ok(corpus)
    }

    /// Returns or derives one ANN structure trained on a mandatory row slice.
    pub(crate) async fn get_or_build_scoped_ann<F, Fut>(
        &self,
        key: &str,
        build: F,
    ) -> Result<Arc<ScopedAnnIndex>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<ScopedAnnIndex>>,
    {
        if let Some(index) = self.get_scoped_ann(key)? {
            return Ok(index);
        }
        let index = Arc::new(build().await?);
        let size_bytes = index
            .estimated_size_bytes()
            .checked_add(key.len())
            .unwrap_or_else(|| panic!("decoded artifact cache entry size overflowed"));
        self.insert(
            key,
            CachedArtifact::ScopedAnn(Arc::clone(&index)),
            size_bytes,
        );
        Ok(index)
    }

    /// Returns or derives one BM25 structure over a mandatory row corpus.
    pub(crate) async fn get_or_build_scoped_fts<F, Fut>(
        &self,
        key: &str,
        build: F,
    ) -> Result<Arc<ScopedFtsIndex>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<ScopedFtsIndex>>,
    {
        if let Some(index) = self.get_scoped_fts(key)? {
            return Ok(index);
        }
        let index = Arc::new(build().await?);
        let size_bytes = index
            .estimated_size_bytes()
            .checked_add(key.len())
            .unwrap_or_else(|| panic!("decoded artifact cache entry size overflowed"));
        self.insert(
            key,
            CachedArtifact::ScopedFts(Arc::clone(&index)),
            size_bytes,
        );
        Ok(index)
    }

    /// Clears all disposable decoded values without resetting diagnostics.
    pub fn clear(&self) {
        let _guard = self
            .mutation
            .lock()
            .unwrap_or_else(|_| panic!("decoded artifact cache mutation lock poisoned"));
        self.entries.clear();
        self.bytes.store(0, Ordering::Relaxed);
    }

    /// Returns the momentary number of retained decoded objects.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Reports whether no decoded objects are currently retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the approximate decoded bytes currently charged to the cache.
    #[must_use]
    pub fn total_size(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Returns all successful FTS decodes since cache construction.
    #[must_use]
    pub fn decode_count(&self) -> u64 {
        self.decode_count.load(Ordering::Relaxed)
    }

    /// Returns successful global-index decodes since construction.
    #[must_use]
    pub fn global_decode_count(&self) -> u64 {
        self.global_decode_count.load(Ordering::Relaxed)
    }

    /// Returns successful legacy cluster-index decodes since construction.
    #[must_use]
    pub fn cluster_decode_count(&self) -> u64 {
        self.cluster_decode_count.load(Ordering::Relaxed)
    }

    /// Reports whether a decoded object is currently retained for `key`.
    #[must_use]
    pub fn contains(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    fn get_global(&self, key: &str) -> Result<Option<Arc<GlobalInvertedIndex>>> {
        let Some(mut entry) = self.entries.get_mut(key) else {
            return Ok(None);
        };
        entry.last_accessed = Instant::now();
        match &entry.artifact {
            CachedArtifact::GlobalFts(index) => Ok(Some(Arc::clone(index))),
            _ => Err(ZeppelinError::Cache(format!(
                "decoded artifact key {key} was reused across FTS artifact variants"
            ))),
        }
    }

    fn get_cluster(&self, key: &str) -> Result<Option<Arc<InvertedIndex>>> {
        let Some(mut entry) = self.entries.get_mut(key) else {
            return Ok(None);
        };
        entry.last_accessed = Instant::now();
        match &entry.artifact {
            CachedArtifact::ClusterFts(index) => Ok(Some(Arc::clone(index))),
            _ => Err(ZeppelinError::Cache(format!(
                "decoded artifact key {key} was reused across FTS artifact variants"
            ))),
        }
    }

    fn get_segment_corpus(&self, key: &str) -> Result<Option<Arc<ScopedSegmentCorpus>>> {
        let Some(mut entry) = self.entries.get_mut(key) else {
            return Ok(None);
        };
        entry.last_accessed = Instant::now();
        match &entry.artifact {
            CachedArtifact::SegmentCorpus(corpus) => Ok(Some(Arc::clone(corpus))),
            _ => Err(ZeppelinError::Cache(format!(
                "decoded artifact key {key} was reused across scoped artifact variants"
            ))),
        }
    }

    fn get_scoped_ann(&self, key: &str) -> Result<Option<Arc<ScopedAnnIndex>>> {
        let Some(mut entry) = self.entries.get_mut(key) else {
            return Ok(None);
        };
        entry.last_accessed = Instant::now();
        match &entry.artifact {
            CachedArtifact::ScopedAnn(index) => Ok(Some(Arc::clone(index))),
            _ => Err(ZeppelinError::Cache(format!(
                "decoded artifact key {key} was reused across scoped artifact variants"
            ))),
        }
    }

    fn get_scoped_fts(&self, key: &str) -> Result<Option<Arc<ScopedFtsIndex>>> {
        let Some(mut entry) = self.entries.get_mut(key) else {
            return Ok(None);
        };
        entry.last_accessed = Instant::now();
        match &entry.artifact {
            CachedArtifact::ScopedFts(index) => Ok(Some(Arc::clone(index))),
            _ => Err(ZeppelinError::Cache(format!(
                "decoded artifact key {key} was reused across scoped artifact variants"
            ))),
        }
    }

    fn insert(&self, key: &str, artifact: CachedArtifact, size_bytes: usize) {
        // An oversized entry is still returned to its caller, but it must not
        // evict useful residents only to be evicted itself immediately.
        if self.max_bytes == 0 || size_bytes > self.max_bytes {
            return;
        }

        let _guard = self
            .mutation
            .lock()
            .unwrap_or_else(|_| panic!("decoded artifact cache mutation lock poisoned"));
        let previous = self.entries.insert(
            key.to_string(),
            CacheEntry {
                artifact,
                size_bytes,
                last_accessed: Instant::now(),
            },
        );
        let previous_size = previous.as_ref().map_or(0, |entry| entry.size_bytes);
        let current = self.bytes.load(Ordering::Relaxed);
        let without_previous = current.checked_sub(previous_size).unwrap_or_else(|| {
            panic!("decoded artifact cache byte accounting regressed during replacement")
        });
        self.bytes.store(
            without_previous.checked_add(size_bytes).unwrap_or_else(|| {
                panic!("decoded artifact cache byte accounting overflowed during insertion")
            }),
            Ordering::Relaxed,
        );
        self.evict_to_budget_locked();
    }

    fn evict_to_budget_locked(&self) {
        while self.bytes.load(Ordering::Relaxed) > self.max_bytes {
            let Some(victim) = self.sampled_victim() else {
                break;
            };
            self.remove_locked(&victim);
        }
    }

    fn remove_locked(&self, key: &str) {
        if let Some((_, entry)) = self.entries.remove(key) {
            let current = self.bytes.load(Ordering::Relaxed);
            self.bytes.store(
                current.checked_sub(entry.size_bytes).unwrap_or_else(|| {
                    panic!("decoded artifact cache byte accounting regressed during eviction")
                }),
                Ordering::Relaxed,
            );
        }
    }

    fn sampled_victim(&self) -> Option<String> {
        let len = self.entries.len();
        if len == 0 {
            return None;
        }

        let start = rand::thread_rng().gen_range(0..len);
        let mut sampled = 0usize;
        let mut victim: Option<(String, Instant)> = None;
        for entry in self.entries.iter().skip(start) {
            if victim
                .as_ref()
                .map(|(_, last_accessed)| entry.value().last_accessed < *last_accessed)
                .unwrap_or(true)
            {
                victim = Some((entry.key().clone(), entry.value().last_accessed));
            }
            sampled += 1;
            if sampled == EVICTION_SAMPLE_SIZE {
                return victim.map(|(key, _)| key);
            }
        }
        for entry in self.entries.iter() {
            if victim
                .as_ref()
                .map(|(_, last_accessed)| entry.value().last_accessed < *last_accessed)
                .unwrap_or(true)
            {
                victim = Some((entry.key().clone(), entry.value().last_accessed));
            }
            sampled += 1;
            if sampled == EVICTION_SAMPLE_SIZE {
                break;
            }
        }
        victim.map(|(key, _)| key)
    }
}

/// Estimates owned global-index allocations retained by one cache entry.
///
/// The estimate charges the top-level structs, owned string capacities, map
/// values, and posting vector capacities. It intentionally omits allocator and
/// `BTreeMap` node overhead; the budget is a stable bound, not a heap profiler.
fn approximate_global_size(index: &GlobalInvertedIndex) -> usize {
    let mut bytes = size_of::<GlobalInvertedIndex>();
    for (field_name, field) in &index.fields {
        bytes = bytes
            .saturating_add(field_name.capacity())
            .saturating_add(size_of_val(field));
        for (term, postings) in &field.postings {
            bytes = bytes
                .saturating_add(term.capacity())
                .saturating_add(size_of_val(postings))
                .saturating_add(
                    postings
                        .entries
                        .capacity()
                        .saturating_mul(size_of::<GlobalPosting>()),
                );
        }
    }
    bytes
}

/// Estimates owned legacy cluster-index allocations using the same formula.
fn approximate_cluster_size(index: &InvertedIndex) -> usize {
    let mut bytes = size_of::<InvertedIndex>();
    for (field_name, field) in &index.fields {
        bytes = bytes
            .saturating_add(field_name.capacity())
            .saturating_add(size_of_val(field));
        for (term, postings) in &field.postings {
            bytes = bytes
                .saturating_add(term.capacity())
                .saturating_add(size_of_val(postings))
                .saturating_add(
                    postings
                        .entries
                        .capacity()
                        .saturating_mul(size_of::<Posting>()),
                );
        }
    }
    bytes
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::types::VectorEntry;

    fn global_bytes() -> Bytes {
        GlobalInvertedIndex {
            total_docs: 0,
            fields: BTreeMap::new(),
        }
        .to_bytes()
        .expect("empty global FTS fixture must serialize")
    }

    fn cluster_bytes() -> Bytes {
        InvertedIndex {
            vector_count: 0,
            fields: BTreeMap::new(),
        }
        .to_bytes()
        .expect("empty cluster FTS fixture must serialize")
    }

    #[tokio::test]
    async fn successful_decode_is_pointer_reused() {
        let cache = DecodedArtifactCache::new(1024 * 1024);
        let first = cache
            .get_or_decode_global_fts("ns/segments/seg/global_fts.bin", || async {
                Ok(global_bytes())
            })
            .await
            .expect("cold global FTS fixture must decode");
        let second = cache
            .get_or_decode_global_fts("ns/segments/seg/global_fts.bin", || async {
                panic!("warm decoded lookup fetched bytes")
            })
            .await
            .expect("warm global FTS fixture must be retained");

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(cache.decode_count(), 1);
        assert_eq!(cache.global_decode_count(), 1);
    }

    #[tokio::test]
    async fn decode_error_is_not_cached() {
        let cache = DecodedArtifactCache::new(1024 * 1024);
        let key = "ns/segments/seg/global_fts.bin";

        let result = cache
            .get_or_decode_global_fts(key, || async { Ok(Bytes::from_static(b"invalid")) })
            .await;
        assert!(result.is_err());
        assert!(cache.is_empty());
        assert_eq!(cache.decode_count(), 0);

        cache
            .get_or_decode_global_fts(key, || async { Ok(global_bytes()) })
            .await
            .expect("valid retry must decode after malformed bytes");
        assert_eq!(cache.decode_count(), 1);
        assert_eq!(cache.len(), 1);
    }

    #[tokio::test]
    async fn zero_and_oversized_budgets_decode_without_retaining() {
        for budget in [0, 1] {
            let cache = DecodedArtifactCache::new(budget);
            for _ in 0..2 {
                cache
                    .get_or_decode_global_fts("ns/segments/seg/global_fts.bin", || async {
                        Ok(global_bytes())
                    })
                    .await
                    .expect("valid uncached global FTS fixture must decode");
            }
            assert!(cache.is_empty());
            assert_eq!(cache.total_size(), 0);
            assert_eq!(cache.decode_count(), 2);
        }
    }

    #[tokio::test]
    async fn one_key_cannot_change_decoded_artifact_variant() {
        let cache = DecodedArtifactCache::new(1024 * 1024);
        let key = "ns/segments/seg/shared.bin";
        cache
            .get_or_decode_global_fts(key, || async { Ok(global_bytes()) })
            .await
            .expect("global fixture must populate the typed key");

        let error = cache
            .get_or_decode_cluster_fts(key, || async { Ok(cluster_bytes()) })
            .await
            .expect_err("one immutable key must not change decoded variants");
        assert!(matches!(error, ZeppelinError::Cache(_)));
        assert_eq!(cache.decode_count(), 1);
        assert_eq!(cache.cluster_decode_count(), 0);
    }

    #[tokio::test]
    async fn scoped_segment_corpus_is_bounded_and_pointer_reused() {
        let cache = DecodedArtifactCache::new(1024 * 1024);
        let builds = AtomicUsize::new(0);
        let key = "segment-corpus:v1:ns:fixture";
        let first = cache
            .get_or_build_segment_corpus(key, || async {
                builds.fetch_add(1, Ordering::Relaxed);
                ScopedSegmentCorpus::new(
                    vec![VectorEntry {
                        id: "visible".to_string(),
                        values: vec![1.0, 0.0],
                        attributes: None,
                    }],
                    2,
                )
            })
            .await
            .expect("cold scoped corpus must build");
        let second = cache
            .get_or_build_segment_corpus(key, || async {
                panic!("warm scoped corpus lookup rebuilt authoritative rows")
            })
            .await
            .expect("warm scoped corpus must be retained");

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(builds.load(Ordering::Relaxed), 1);
        assert!(cache.contains(key));
        assert!(cache.total_size() > 0);
        assert!(cache.total_size() <= 1024 * 1024);
    }
}
