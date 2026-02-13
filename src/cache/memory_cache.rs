use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use dashmap::DashMap;
use tracing::debug;

/// In-memory LRU cache tier for hot cluster data.
///
/// Sits above `DiskCache` in the tiered caching hierarchy:
/// `MemoryCache` → `DiskCache` → S3.
///
/// Uses `DashMap` for concurrent access with per-shard locking.
/// Eviction is approximate LRU via DashMap iteration when over capacity.
pub struct MemoryCache {
    entries: DashMap<String, MemCacheEntry>,
    max_size_bytes: u64,
    total_size: AtomicU64,
}

struct MemCacheEntry {
    data: Bytes,
    size: u64,
    last_accessed: Instant,
}

impl MemoryCache {
    /// Create a new memory cache with the given max size in bytes.
    pub fn new(max_size_bytes: u64) -> Self {
        Self {
            entries: DashMap::new(),
            max_size_bytes,
            total_size: AtomicU64::new(0),
        }
    }

    /// Get a cached value by key.
    pub fn get(&self, key: &str) -> Option<Bytes> {
        let mut entry = self.entries.get_mut(key)?;
        entry.last_accessed = Instant::now();
        Some(entry.data.clone())
    }

    /// Insert a value into the cache, evicting if needed.
    pub fn insert(&self, key: &str, data: Bytes) {
        let size = data.len() as u64;

        let old = self.entries.insert(
            key.to_string(),
            MemCacheEntry {
                data,
                size,
                last_accessed: Instant::now(),
            },
        );

        if let Some(old_entry) = old {
            self.total_size.fetch_sub(old_entry.size, Ordering::Relaxed);
        }
        self.total_size.fetch_add(size, Ordering::Relaxed);

        self.evict_if_needed();
    }

    /// Invalidate a single key.
    pub fn invalidate(&self, key: &str) {
        if let Some((_, entry)) = self.entries.remove(key) {
            self.total_size.fetch_sub(entry.size, Ordering::Relaxed);
        }
    }

    /// Invalidate all keys that start with the given prefix.
    pub fn invalidate_prefix(&self, prefix: &str) {
        let matching: Vec<String> = self
            .entries
            .iter()
            .filter(|r| r.key().starts_with(prefix))
            .map(|r| r.key().clone())
            .collect();

        for key in matching {
            if let Some((_, entry)) = self.entries.remove(&key) {
                self.total_size.fetch_sub(entry.size, Ordering::Relaxed);
            }
        }
    }

    /// Get the total size of all cached data in bytes.
    pub fn total_size(&self) -> u64 {
        self.total_size.load(Ordering::Relaxed)
    }

    /// O(1) approximate LRU eviction via random sampling (Redis-style).
    ///
    /// Instead of scanning all entries to find the global LRU victim,
    /// sample a small fixed number of entries and evict the oldest.
    /// At 20K+ entries, this reduces eviction from O(N) to O(1).
    const EVICTION_SAMPLE_SIZE: usize = 16;

    fn evict_if_needed(&self) {
        while self.total_size.load(Ordering::Relaxed) > self.max_size_bytes {
            // Sample EVICTION_SAMPLE_SIZE entries and evict the oldest.
            let victim = self
                .entries
                .iter()
                .take(Self::EVICTION_SAMPLE_SIZE)
                .min_by_key(|r| r.value().last_accessed)
                .map(|r| r.key().clone());

            match victim {
                Some(key) => {
                    if let Some((_, entry)) = self.entries.remove(&key) {
                        self.total_size.fetch_sub(entry.size, Ordering::Relaxed);
                        debug!(key = %key, size = entry.size, "evicted memory cache entry");
                    }
                }
                None => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_cache_get_insert() {
        let cache = MemoryCache::new(1024 * 1024);
        assert!(cache.get("key1").is_none());

        cache.insert("key1", Bytes::from_static(b"hello"));
        let data = cache.get("key1").unwrap();
        assert_eq!(data.as_ref(), b"hello");
    }

    #[test]
    fn test_memory_cache_overwrite() {
        let cache = MemoryCache::new(1024 * 1024);
        cache.insert("key1", Bytes::from_static(b"v1"));
        cache.insert("key1", Bytes::from_static(b"v2"));

        let data = cache.get("key1").unwrap();
        assert_eq!(data.as_ref(), b"v2");
        assert_eq!(cache.total_size(), 2);
    }

    #[test]
    fn test_memory_cache_invalidate() {
        let cache = MemoryCache::new(1024 * 1024);
        cache.insert("key1", Bytes::from_static(b"hello"));
        cache.invalidate("key1");
        assert!(cache.get("key1").is_none());
        assert_eq!(cache.total_size(), 0);
    }

    #[test]
    fn test_memory_cache_invalidate_prefix() {
        let cache = MemoryCache::new(1024 * 1024);
        cache.insert("ns/seg/cluster_0", Bytes::from_static(b"a"));
        cache.insert("ns/seg/cluster_1", Bytes::from_static(b"b"));
        cache.insert("other/seg/cluster_0", Bytes::from_static(b"c"));

        cache.invalidate_prefix("ns/seg/");
        assert!(cache.get("ns/seg/cluster_0").is_none());
        assert!(cache.get("ns/seg/cluster_1").is_none());
        assert!(cache.get("other/seg/cluster_0").is_some());
    }

    #[test]
    fn test_memory_cache_eviction() {
        // Max 10 bytes — insert 3 entries of 5 bytes each
        let cache = MemoryCache::new(10);
        cache.insert("k1", Bytes::from_static(b"aaaaa"));
        cache.insert("k2", Bytes::from_static(b"bbbbb"));
        // Third insert should trigger eviction of k1 (oldest)
        cache.insert("k3", Bytes::from_static(b"ccccc"));

        // At least one eviction should have happened
        assert!(cache.total_size() <= 10);
    }

    #[test]
    fn test_memory_cache_total_size() {
        let cache = MemoryCache::new(1024 * 1024);
        cache.insert("k1", Bytes::from_static(b"abc"));
        cache.insert("k2", Bytes::from_static(b"defgh"));
        assert_eq!(cache.total_size(), 8);
    }
}
