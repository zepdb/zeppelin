pub mod manifest_cache;

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

/// Metadata for a cached entry.
struct CacheEntry {
    /// Filename on disk (key with `/` replaced by `__`).
    filename: String,
    /// Size in bytes.
    size: u64,
    /// Last access time for LRU eviction.
    last_accessed: Instant,
}

/// LRU disk cache for segment cluster data.
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
}

impl DiskCache {
    /// Create a new disk cache from config.
    pub fn new(config: &CacheConfig) -> Result<Self> {
        let max_bytes = config.max_size_gb * 1024 * 1024 * 1024;
        Self::new_with_max_bytes(config.dir.clone(), max_bytes)
    }

    /// Create a new disk cache with an explicit max size in bytes.
    pub fn new_with_max_bytes(dir: PathBuf, max_size_bytes: u64) -> Result<Self> {
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
    #[instrument(skip(self), fields(key = key))]
    pub async fn get(&self, key: &str) -> Option<Bytes> {
        // Check in-memory index and update last_accessed — no global write lock needed.
        {
            let mut entry = self.entries.get_mut(key)?;
            entry.last_accessed = Instant::now();
        }

        // Read from disk
        let path = self.file_path(key);
        match tokio::fs::read(&path).await {
            Ok(data) => {
                crate::metrics::CACHE_HITS_TOTAL
                    .with_label_values(&["hit"])
                    .inc();
                debug!("cache hit");
                Some(Bytes::from(data))
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

    /// Put a value into the cache.
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

        // Update index
        let old = self.entries.insert(
            key.to_string(),
            CacheEntry {
                filename: Self::key_to_filename(key),
                size,
                last_accessed: Instant::now(),
            },
        );
        let is_new = if let Some(old_entry) = old {
            // Replacing existing entry: subtract old size
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

    /// Invalidate (remove) a single key from the cache.
    #[instrument(skip(self), fields(key = key))]
    pub async fn invalidate(&self, key: &str) -> Result<()> {
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

    /// Invalidate all keys that start with the given prefix.
    #[instrument(skip(self), fields(prefix = prefix))]
    pub async fn invalidate_prefix(&self, prefix: &str) -> Result<()> {
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

    /// Get the total size of all cached data in bytes.
    pub fn total_size(&self) -> u64 {
        self.total_size.load(Ordering::Relaxed)
    }

    /// Evict the oldest unpinned entries until total size is under max.
    async fn evict_if_needed(&self) -> Result<()> {
        loop {
            let current = self.total_size.load(Ordering::Relaxed);
            if current <= self.max_size_bytes {
                break;
            }

            let pinned = self.pinned.read().await;

            // Find the oldest unpinned entry by iterating the DashMap.
            let victim = self
                .entries
                .iter()
                .filter(|r| !pinned.contains(r.key()))
                .min_by_key(|r| r.value().last_accessed)
                .map(|r| r.key().clone());

            drop(pinned);

            match victim {
                Some(key) => {
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
                    // All entries are pinned, can't evict more
                    break;
                }
            }
        }
        Ok(())
    }
}
