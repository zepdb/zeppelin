pub mod manifest_cache;
pub mod memory_cache;

use std::collections::HashSet;
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::RwLock;
use tracing::{debug, instrument};

use crate::config::CacheConfig;
use crate::error::{Result, ZeppelinError};

use self::memory_cache::MemoryCache;

/// Metadata for a cached entry.
struct CacheEntry {
    /// Filename on disk (key with `/` replaced by `__`).
    filename: String,
    /// Size in bytes.
    size: u64,
    /// Last access time for LRU eviction.
    last_accessed: Instant,
}

/// Tiered LRU cache: memory → disk → S3.
///
/// Files are stored at `{dir}/{filename}` where filename is the key with
/// `/` replaced by `__`. On startup, the directory is scanned to rebuild
/// the in-memory index.
///
/// Uses `DashMap` for the entry index to allow concurrent reads without
/// write-lock contention. The previous `RwLock<HashMap>` took a write lock
/// on every cache hit (to update `last_accessed`), causing p99 regressions
/// at c=4+ concurrency.
pub struct DiskCache {
    dir: PathBuf,
    max_size_bytes: u64,
    entries: DashMap<String, CacheEntry>,
    pinned: RwLock<HashSet<String>>,
    total_size: AtomicU64,
    /// Optional in-memory tier sitting above disk.
    memory: Option<MemoryCache>,
}

impl DiskCache {
    /// Create a new disk cache from config.
    pub fn new(config: &CacheConfig) -> Result<Self> {
        let max_bytes = config.max_size_gb * 1024 * 1024 * 1024;
        let memory_max = config.memory_cache_max_mb as u64 * 1024 * 1024;
        let memory = if memory_max > 0 {
            Some(MemoryCache::new(memory_max))
        } else {
            None
        };
        Self::new_with_options(config.dir.clone(), max_bytes, memory)
    }

    /// Create a new disk cache with an explicit max size in bytes.
    pub fn new_with_max_bytes(dir: PathBuf, max_size_bytes: u64) -> Result<Self> {
        Self::new_with_options(dir, max_size_bytes, None)
    }

    /// Create a new disk cache with explicit options.
    pub fn new_with_options(
        dir: PathBuf,
        max_size_bytes: u64,
        memory: Option<MemoryCache>,
    ) -> Result<Self> {
        // Ensure directory exists
        std::fs::create_dir_all(&dir).map_err(|e| {
            ZeppelinError::Cache(format!("failed to create cache dir {:?}: {}", dir, e))
        })?;

        let cache = Self {
            dir,
            max_size_bytes,
            entries: DashMap::new(),
            pinned: RwLock::new(HashSet::new()),
            total_size: AtomicU64::new(0),
            memory,
        };

        // Scan existing files to rebuild index
        cache.rebuild_index_sync();

        Ok(cache)
    }

    /// Rebuild the in-memory index from files on disk.
    fn rebuild_index_sync(&self) {
        let entries_dir = match std::fs::read_dir(&self.dir) {
            Ok(d) => d,
            Err(_) => return,
        };

        let mut total = 0u64;

        for entry in entries_dir.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            let filename = match path.file_name().and_then(|f| f.to_str()) {
                Some(f) => f.to_string(),
                None => continue,
            };

            // Skip .tmp files
            if filename.ends_with(".tmp") {
                let _ = std::fs::remove_file(&path);
                continue;
            }

            let size = match entry.metadata() {
                Ok(m) => m.len(),
                Err(_) => continue,
            };

            let key = filename.replace("__", "/");
            total += size;

            self.entries.insert(
                key,
                CacheEntry {
                    filename,
                    size,
                    last_accessed: Instant::now(),
                },
            );
        }

        self.total_size.store(total, Ordering::Relaxed);
    }

    fn key_to_filename(key: &str) -> String {
        key.replace('/', "__")
    }

    fn file_path(&self, key: &str) -> PathBuf {
        self.dir.join(Self::key_to_filename(key))
    }

    /// Get a cached value by key.
    ///
    /// Checks memory tier first (sub-microsecond), then disk.
    /// On disk hit, promotes to memory tier.
    #[instrument(skip(self), fields(key = key))]
    pub async fn get(&self, key: &str) -> Option<Bytes> {
        // Memory tier check
        if let Some(ref mem) = self.memory {
            if let Some(data) = mem.get(key) {
                crate::metrics::CACHE_HITS_TOTAL
                    .with_label_values(&["memory_hit"])
                    .inc();
                debug!("memory cache hit");
                return Some(data);
            }
        }

        // Check disk index and update last_accessed.
        {
            let mut entry = self.entries.get_mut(key)?;
            entry.last_accessed = Instant::now();
        }

        // Read from disk
        let path = self.file_path(key);
        match tokio::fs::read(&path).await {
            Ok(data) => {
                let bytes = Bytes::from(data);
                // Promote to memory tier
                if let Some(ref mem) = self.memory {
                    mem.insert(key, bytes.clone());
                }
                crate::metrics::CACHE_HITS_TOTAL
                    .with_label_values(&["hit"])
                    .inc();
                debug!("cache hit");
                Some(bytes)
            }
            Err(_) => {
                // File disappeared — remove from index
                if let Some((_, entry)) = self.entries.remove(key) {
                    self.total_size.fetch_sub(entry.size, Ordering::Relaxed);
                    crate::metrics::CACHE_ENTRIES.dec();
                }
                crate::metrics::CACHE_HITS_TOTAL
                    .with_label_values(&["miss"])
                    .inc();
                debug!("cache miss (file missing)");
                None
            }
        }
    }

    /// Put a value into the cache (both memory and disk tiers).
    #[instrument(skip(self, data), fields(key = key, size = data.len()))]
    pub async fn put(&self, key: &str, data: &Bytes) -> Result<()> {
        let size = data.len() as u64;
        let path = self.file_path(key);
        let tmp_name = format!(
            "{}.{}.tmp",
            Self::key_to_filename(key),
            uuid::Uuid::new_v4()
        );
        let tmp_path = self.dir.join(tmp_name);

        // Atomic write: write to .tmp then rename
        tokio::fs::write(&tmp_path, data)
            .await
            .map_err(|e| ZeppelinError::Cache(format!("failed to write cache file: {e}")))?;
        tokio::fs::rename(&tmp_path, &path)
            .await
            .map_err(|e| ZeppelinError::Cache(format!("failed to rename cache file: {e}")))?;

        // Insert into memory tier
        if let Some(ref mem) = self.memory {
            mem.insert(key, data.clone());
        }

        // Update disk index
        let old = self.entries.insert(
            key.to_string(),
            CacheEntry {
                filename: Self::key_to_filename(key),
                size,
                last_accessed: Instant::now(),
            },
        );
        let is_new = if let Some(old_entry) = old {
            self.total_size.fetch_sub(old_entry.size, Ordering::Relaxed);
            false
        } else {
            true
        };
        self.total_size.fetch_add(size, Ordering::Relaxed);
        if is_new {
            crate::metrics::CACHE_ENTRIES.inc();
        }

        debug!("cache put");

        // Evict if over limit
        self.evict_if_needed().await?;

        Ok(())
    }

    /// Get a value from cache, or fetch it using the provided function if not cached.
    ///
    /// Three-tier: memory → disk → fetch (S3). Populates both tiers on miss.
    pub async fn get_or_fetch<F, Fut>(&self, key: &str, fetch: F) -> Result<Bytes>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Bytes>>,
    {
        if let Some(data) = self.get(key).await {
            return Ok(data);
        }

        crate::metrics::CACHE_HITS_TOTAL
            .with_label_values(&["miss"])
            .inc();
        let data = fetch().await?;
        self.put(key, &data).await?;
        Ok(data)
    }

    /// Pin a key so it won't be evicted.
    pub async fn pin(&self, key: &str) {
        let mut pinned = self.pinned.write().await;
        pinned.insert(key.to_string());
        debug!(key = key, "pinned cache key");
    }

    /// Unpin a key so it can be evicted normally.
    pub async fn unpin(&self, key: &str) {
        let mut pinned = self.pinned.write().await;
        pinned.remove(key);
    }

    /// Invalidate (remove) a single key from both memory and disk tiers.
    #[instrument(skip(self), fields(key = key))]
    pub async fn invalidate(&self, key: &str) -> Result<()> {
        // Invalidate memory tier
        if let Some(ref mem) = self.memory {
            mem.invalidate(key);
        }

        if let Some((_, entry)) = self.entries.remove(key) {
            self.total_size.fetch_sub(entry.size, Ordering::Relaxed);
            crate::metrics::CACHE_ENTRIES.dec();
            let path = self.dir.join(&entry.filename);
            let _ = tokio::fs::remove_file(&path).await;
            debug!("invalidated cache key");
        }

        // Also remove from pinned
        let mut pinned = self.pinned.write().await;
        pinned.remove(key);

        Ok(())
    }

    /// Invalidate all keys that start with the given prefix from both tiers.
    #[instrument(skip(self), fields(prefix = prefix))]
    pub async fn invalidate_prefix(&self, prefix: &str) -> Result<()> {
        // Invalidate memory tier
        if let Some(ref mem) = self.memory {
            mem.invalidate_prefix(prefix);
        }

        // Collect matching keys first to avoid holding DashMap shards during I/O.
        let matching: Vec<(String, CacheEntry)> = self
            .entries
            .iter()
            .filter(|r| r.key().starts_with(prefix))
            .map(|r| (r.key().clone(), CacheEntry {
                filename: r.value().filename.clone(),
                size: r.value().size,
                last_accessed: r.value().last_accessed,
            }))
            .collect();

        for (key, entry) in &matching {
            self.entries.remove(key);
            self.total_size.fetch_sub(entry.size, Ordering::Relaxed);
            crate::metrics::CACHE_ENTRIES.dec();
            let path = self.dir.join(&entry.filename);
            let _ = tokio::fs::remove_file(&path).await;
        }

        let mut pinned = self.pinned.write().await;
        for (key, _) in &matching {
            pinned.remove(key);
        }

        debug!(removed = matching.len(), "invalidated prefix");
        Ok(())
    }

    /// Get the total size of all cached data on disk in bytes.
    pub fn total_size(&self) -> u64 {
        self.total_size.load(Ordering::Relaxed)
    }

    /// O(1) approximate LRU eviction via random sampling (Redis-style).
    ///
    /// Instead of scanning all entries to find the global LRU victim,
    /// sample a small fixed number of unpinned entries and evict the oldest.
    /// At 20K+ entries, this reduces eviction from O(N) to O(16).
    const EVICTION_SAMPLE_SIZE: usize = 16;

    async fn evict_if_needed(&self) -> Result<()> {
        loop {
            let current = self.total_size.load(Ordering::Relaxed);
            if current <= self.max_size_bytes {
                break;
            }

            let pinned = self.pinned.read().await;

            // Sample EVICTION_SAMPLE_SIZE unpinned entries and evict the oldest.
            let victim = self
                .entries
                .iter()
                .filter(|r| !pinned.contains(r.key()))
                .take(Self::EVICTION_SAMPLE_SIZE)
                .min_by_key(|r| r.value().last_accessed)
                .map(|r| r.key().clone());

            drop(pinned);

            match victim {
                Some(key) => {
                    // Also evict from memory tier
                    if let Some(ref mem) = self.memory {
                        mem.invalidate(&key);
                    }
                    if let Some((_, entry)) = self.entries.remove(&key) {
                        self.total_size.fetch_sub(entry.size, Ordering::Relaxed);
                        crate::metrics::CACHE_ENTRIES.dec();
                        crate::metrics::CACHE_EVICTIONS_TOTAL.inc();
                        let path = self.dir.join(&entry.filename);
                        let _ = tokio::fs::remove_file(&path).await;
                        debug!(key = %key, size = entry.size, "evicted cache entry");
                    }
                }
                None => {
                    // All sampled entries are pinned, can't evict more
                    break;
                }
            }
        }
        Ok(())
    }
}
