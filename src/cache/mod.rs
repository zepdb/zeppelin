/// Warm-set hydration policy and worker support.
pub mod hydration;
/// Manifest-level TTL cache to avoid repeated S3 reads.
pub mod manifest_cache;

use std::any::Any;
use std::collections::HashSet;
use std::future::Future;
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;
use rand::Rng;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, instrument};

use crate::config::CacheConfig;
use crate::error::{Result, ZeppelinError};

/// Sample size for O(1) approximate LRU eviction (Redis-style).
///
/// Instead of scanning all entries to find the global LRU victim,
/// sample a small fixed number of entries and evict the oldest.
/// At 20K+ entries, this reduces eviction from O(N) to O(16).
const EVICTION_SAMPLE_SIZE: usize = 16;
const EVICTION_MAX_CONSECUTIVE_FAILURES: usize = 3;
const EVICTION_FAILURE_BACKOFF: Duration = Duration::from_secs(1);

#[cfg(test)]
static EVICTION_TEST_PANIC_ON_START: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static EVICTION_TEST_REMOVE_FAILURES: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static EVICTION_TEST_REMOVE_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static EVICTION_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
    entries: Arc<DashMap<String, CacheEntry>>,
    pinned: Arc<RwLock<HashSet<String>>>,
    /// One pinned key per scope (namespace): pinning a new key for a scope
    /// unpins the scope's previous key. Used for active-segment index
    /// metadata (centroids / tree meta) that must survive LRU pressure but
    /// release automatically on segment rotation.
    scoped_pins: DashMap<String, String>,
    total_size: Arc<AtomicU64>,
    /// Decoded immutable metadata keyed by the same S3 key as the bytes cache.
    decoded: DashMap<String, Arc<dyn Any + Send + Sync>>,
    /// Optional in-memory tier sitting above disk.
    memory: Option<Arc<MemoryCache>>,
    /// Per-key mutexes that coalesce concurrent cold misses.
    inflight: DashMap<String, Arc<Mutex<()>>>,
    /// Guards the background eviction worker.
    ///
    /// Cache size may transiently exceed `max_size_bytes` by the size of
    /// in-flight puts while this flag is set.
    eviction_running: Arc<AtomicBool>,
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
            entries: Arc::new(DashMap::new()),
            pinned: Arc::new(RwLock::new(HashSet::new())),
            scoped_pins: DashMap::new(),
            total_size: Arc::new(AtomicU64::new(0)),
            decoded: DashMap::new(),
            memory: memory.map(Arc::new),
            inflight: DashMap::new(),
            eviction_running: Arc::new(AtomicBool::new(false)),
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

    fn inflight_lock(&self, key: &str) -> Arc<Mutex<()>> {
        self.inflight
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .value()
            .clone()
    }

    fn remove_idle_inflight_lock(&self, key: &str, lock: Arc<Mutex<()>>) {
        drop(lock);
        self.inflight
            .remove_if(key, |_, current| Arc::strong_count(current) == 1);
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

        // Evict in the background if over limit.
        self.spawn_eviction_if_needed();

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

        let lock = self.inflight_lock(key);
        let guard = lock.lock().await;

        let result = async {
            if let Some(data) = self.get(key).await {
                return Ok(data);
            }

            crate::metrics::CACHE_HITS_TOTAL
                .with_label_values(&["miss"])
                .inc();
            let data = fetch().await?;
            if let Err(error) = self.put(key, &data).await {
                error!(
                    key = key,
                    error = %error,
                    "cache write failed after successful fetch"
                );
            }
            Ok(data)
        }
        .await;

        drop(guard);
        self.remove_idle_inflight_lock(key, lock);
        result
    }

    /// Get a decoded immutable metadata entry from the in-process cache.
    pub fn get_decoded<T>(&self, key: &str) -> Result<Option<Arc<T>>>
    where
        T: Any + Send + Sync + 'static,
    {
        let Some(entry) = self.decoded.get(key) else {
            return Ok(None);
        };
        let decoded = Arc::clone(entry.value());
        Arc::downcast::<T>(decoded)
            .map(Some)
            .map_err(|_| ZeppelinError::Cache(format!("decoded cache type mismatch for key {key}")))
    }

    /// Insert a decoded immutable metadata entry keyed by its authoritative S3 key.
    pub fn insert_decoded<T>(&self, key: &str, decoded: Arc<T>)
    where
        T: Any + Send + Sync + 'static,
    {
        self.decoded.insert(key.to_string(), decoded);
    }

    /// Whether a decoded metadata entry exists for `key`.
    #[must_use]
    pub fn has_decoded(&self, key: &str) -> bool {
        self.decoded.contains_key(key)
    }

    /// Pin a key so it won't be evicted.
    pub async fn pin(&self, key: &str) {
        let mut pinned = self.pinned.write().await;
        pinned.insert(key.to_string());
        if let Some(ref mem) = self.memory {
            mem.pin(key);
        }
        debug!(key = key, "pinned cache key");
    }

    /// Unpin a key so it can be evicted normally.
    pub async fn unpin(&self, key: &str) {
        let mut pinned = self.pinned.write().await;
        pinned.remove(key);
        self.decoded.remove(key);
        if let Some(ref mem) = self.memory {
            mem.unpin(key);
        }
    }

    /// Pin `key` under `scope`, unpinning the scope's previously pinned key.
    ///
    /// A scope (namespace) has at most one pinned key at a time. This keeps
    /// the active segment's index metadata (centroids / tree meta) safe from
    /// LRU eviction while automatically releasing the old segment's entry
    /// when the active segment rotates after compaction.
    pub async fn pin_scoped(&self, scope: &str, key: &str) {
        // Fast path: already pinned for this scope — one lock-free map read
        // plus a shared-lock membership check on the hot query path.
        if let Some(current) = self.scoped_pins.get(scope) {
            if current.value() == key && self.pinned.read().await.contains(key) {
                if let Some(ref mem) = self.memory {
                    mem.pin(key);
                }
                return;
            }
        }

        let old = self.scoped_pins.insert(scope.to_string(), key.to_string());
        let mut pinned = self.pinned.write().await;
        if let Some(old_key) = old {
            if old_key != key {
                pinned.remove(&old_key);
                self.decoded.remove(&old_key);
                if let Some(ref mem) = self.memory {
                    mem.unpin(&old_key);
                }
                debug!(scope = scope, key = %old_key, "unpinned rotated cache key");
            }
        }
        pinned.insert(key.to_string());
        if let Some(ref mem) = self.memory {
            mem.pin(key);
        }
        debug!(scope = scope, key = key, "pinned cache key for scope");
    }

    /// Remove the pinned key for `scope`, if any.
    pub async fn unpin_scoped(&self, scope: &str) {
        let Some((_, old_key)) = self.scoped_pins.remove(scope) else {
            return;
        };
        let mut pinned = self.pinned.write().await;
        pinned.remove(&old_key);
        self.decoded.remove(&old_key);
        if let Some(ref mem) = self.memory {
            mem.unpin(&old_key);
        }
        debug!(scope = scope, key = %old_key, "unpinned scoped cache key");
    }

    /// Whether a key is currently pinned.
    pub async fn is_pinned(&self, key: &str) -> bool {
        self.pinned.read().await.contains(key)
    }

    /// Invalidate (remove) a single key from both memory and disk tiers.
    #[instrument(skip(self), fields(key = key))]
    pub async fn invalidate(&self, key: &str) -> Result<()> {
        // Invalidate memory tier
        if let Some(ref mem) = self.memory {
            mem.invalidate(key);
        }
        self.decoded.remove(key);

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
        let decoded_keys: Vec<String> = self
            .decoded
            .iter()
            .filter(|r| r.key().starts_with(prefix))
            .map(|r| r.key().clone())
            .collect();
        for key in decoded_keys {
            self.decoded.remove(&key);
        }

        // Collect matching keys first to avoid holding DashMap shards during I/O.
        let matching: Vec<(String, CacheEntry)> = self
            .entries
            .iter()
            .filter(|r| r.key().starts_with(prefix))
            .map(|r| {
                (
                    r.key().clone(),
                    CacheEntry {
                        filename: r.value().filename.clone(),
                        size: r.value().size,
                        last_accessed: r.value().last_accessed,
                    },
                )
            })
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

    fn spawn_eviction_if_needed(&self) {
        if self.total_size.load(Ordering::Relaxed) <= self.max_size_bytes {
            return;
        }
        if self.eviction_running.swap(true, Ordering::AcqRel) {
            return;
        }

        let dir = self.dir.clone();
        let max_size_bytes = self.max_size_bytes;
        let entries = Arc::clone(&self.entries);
        let pinned = Arc::clone(&self.pinned);
        let total_size = Arc::clone(&self.total_size);
        let memory = self.memory.clone();
        let eviction_running = Arc::clone(&self.eviction_running);

        tokio::spawn(async move {
            let _reset_running = EvictionRunningReset::new(Arc::clone(&eviction_running));
            let mut consecutive_failures = 0usize;
            loop {
                #[cfg(test)]
                if EVICTION_TEST_PANIC_ON_START.swap(false, Ordering::AcqRel) {
                    panic!("injected disk cache eviction panic");
                }

                let outcome = evict_if_needed_background(
                    &dir,
                    max_size_bytes,
                    &entries,
                    &pinned,
                    &total_size,
                    memory.as_deref(),
                )
                .await;
                match outcome {
                    EvictionPassOutcome::UnderBudget => {
                        consecutive_failures = 0;
                    }
                    EvictionPassOutcome::NoVictim => {
                        debug!(
                            current_size = total_size.load(Ordering::Relaxed),
                            max_size_bytes, "disk cache eviction stopped with no unpinned victim"
                        );
                        break;
                    }
                    EvictionPassOutcome::Failed => {
                        consecutive_failures += 1;
                        if consecutive_failures >= EVICTION_MAX_CONSECUTIVE_FAILURES {
                            error!(
                                failures = consecutive_failures,
                                backoff_ms = EVICTION_FAILURE_BACKOFF.as_millis(),
                                "pausing disk cache eviction after repeated failures"
                            );
                            tokio::time::sleep(EVICTION_FAILURE_BACKOFF).await;
                            consecutive_failures = 0;
                        }
                    }
                }

                eviction_running.store(false, Ordering::Release);
                if total_size.load(Ordering::Relaxed) <= max_size_bytes {
                    break;
                }
                if eviction_running
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    break;
                }
            }
        });
    }
}

struct EvictionRunningReset {
    eviction_running: Arc<AtomicBool>,
}

impl EvictionRunningReset {
    fn new(eviction_running: Arc<AtomicBool>) -> Self {
        Self { eviction_running }
    }
}

impl Drop for EvictionRunningReset {
    fn drop(&mut self) {
        self.eviction_running.store(false, Ordering::Release);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvictionPassOutcome {
    UnderBudget,
    NoVictim,
    Failed,
}

fn sampled_disk_victim(
    entries: &DashMap<String, CacheEntry>,
    pinned: &HashSet<String>,
) -> Option<String> {
    let len = entries.len();
    if len == 0 {
        return None;
    }

    let start = rand::thread_rng().gen_range(0..len);
    let mut sampled = 0usize;
    let mut victim: Option<(String, Instant)> = None;

    for entry in entries.iter().skip(start) {
        if pinned.contains(entry.key()) {
            continue;
        }
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

    for entry in entries.iter() {
        if pinned.contains(entry.key()) {
            continue;
        }
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

async fn evict_if_needed_background(
    dir: &std::path::Path,
    max_size_bytes: u64,
    entries: &DashMap<String, CacheEntry>,
    pinned: &RwLock<HashSet<String>>,
    total_size: &AtomicU64,
    memory: Option<&MemoryCache>,
) -> EvictionPassOutcome {
    loop {
        let current = total_size.load(Ordering::Relaxed);
        if current <= max_size_bytes {
            return EvictionPassOutcome::UnderBudget;
        }

        let pinned_keys = pinned.read().await;
        let victim = sampled_disk_victim(entries, &pinned_keys);
        drop(pinned_keys);

        match victim {
            Some(key) => {
                if let Some(mem) = memory {
                    mem.invalidate(&key);
                }
                let Some(entry) = entries.get(&key).map(|entry| CacheEntry {
                    filename: entry.filename.clone(),
                    size: entry.size,
                    last_accessed: entry.last_accessed,
                }) else {
                    continue;
                };
                let path = dir.join(&entry.filename);
                match remove_cache_file(&path).await {
                    Ok(()) => {
                        if let Some((_, removed)) = entries.remove(&key) {
                            total_size.fetch_sub(removed.size, Ordering::Relaxed);
                            crate::metrics::CACHE_ENTRIES.dec();
                            crate::metrics::CACHE_EVICTIONS_TOTAL.inc();
                            debug!(key = %key, size = removed.size, "evicted cache entry");
                        }
                    }
                    Err(error) if error.kind() == ErrorKind::NotFound => {
                        if let Some((_, removed)) = entries.remove(&key) {
                            total_size.fetch_sub(removed.size, Ordering::Relaxed);
                            crate::metrics::CACHE_ENTRIES.dec();
                            crate::metrics::CACHE_EVICTIONS_TOTAL.inc();
                            error!(
                                key = %key,
                                error = %error,
                                "cache entry missing on disk during eviction"
                            );
                        }
                    }
                    Err(error) => {
                        error!(
                            key = %key,
                            path = %path.display(),
                            error = %error,
                            "failed to evict cache entry"
                        );
                        return EvictionPassOutcome::Failed;
                    }
                }
            }
            None => {
                // All sampled entries are pinned, can't evict more
                return EvictionPassOutcome::NoVictim;
            }
        }
    }
}

async fn remove_cache_file(path: &Path) -> std::io::Result<()> {
    #[cfg(test)]
    {
        EVICTION_TEST_REMOVE_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
        if EVICTION_TEST_REMOVE_FAILURES
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(std::io::Error::new(
                ErrorKind::PermissionDenied,
                "injected cache eviction removal failure",
            ));
        }
    }

    tokio::fs::remove_file(path).await
}

/// In-memory LRU cache tier for hot cluster data.
///
/// Sits above `DiskCache` in the tiered caching hierarchy:
/// `MemoryCache` → `DiskCache` → S3.
///
/// Uses `DashMap` for concurrent access with per-shard locking.
/// Eviction is approximate LRU via DashMap iteration when over capacity.
pub struct MemoryCache {
    entries: DashMap<String, MemCacheEntry>,
    pinned: DashMap<String, ()>,
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
            pinned: DashMap::new(),
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

    /// Pin a key so the memory tier does not evict it.
    pub fn pin(&self, key: &str) {
        self.pinned.insert(key.to_string(), ());
    }

    /// Unpin a key so it can be evicted normally.
    pub fn unpin(&self, key: &str) {
        self.pinned.remove(key);
    }

    /// Whether a key is pinned in the memory tier.
    #[must_use]
    pub fn is_pinned(&self, key: &str) -> bool {
        self.pinned.contains_key(key)
    }

    /// Invalidate a single key.
    pub fn invalidate(&self, key: &str) {
        if let Some((_, entry)) = self.entries.remove(key) {
            self.total_size.fetch_sub(entry.size, Ordering::Relaxed);
        }
        self.pinned.remove(key);
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
            self.pinned.remove(&key);
        }
    }

    /// Get the total size of all cached data in bytes.
    pub fn total_size(&self) -> u64 {
        self.total_size.load(Ordering::Relaxed)
    }

    fn evict_if_needed(&self) {
        while self.total_size.load(Ordering::Relaxed) > self.max_size_bytes {
            let victim = self.sampled_victim();

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

    fn sampled_victim(&self) -> Option<String> {
        let len = self.entries.len();
        if len == 0 {
            return None;
        }

        let start = rand::thread_rng().gen_range(0..len);
        let mut sampled = 0usize;
        let mut victim: Option<(String, Instant)> = None;

        for entry in self.entries.iter().skip(start) {
            if self.pinned.contains_key(entry.key()) {
                continue;
            }
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
            if self.pinned.contains_key(entry.key()) {
                continue;
            }
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

#[allow(clippy::unwrap_used, clippy::expect_used)]
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
    fn test_memory_cache_pin_survives_eviction() {
        let cache = MemoryCache::new(10);
        cache.insert("metadata", Bytes::from_static(b"aaaaa"));
        cache.pin("metadata");
        cache.insert("k2", Bytes::from_static(b"bbbbb"));
        cache.insert("k3", Bytes::from_static(b"ccccc"));

        assert!(cache.get("metadata").is_some());
        assert!(cache.is_pinned("metadata"));
    }

    #[test]
    fn test_memory_cache_total_size() {
        let cache = MemoryCache::new(1024 * 1024);
        cache.insert("k1", Bytes::from_static(b"abc"));
        cache.insert("k2", Bytes::from_static(b"defgh"));
        assert_eq!(cache.total_size(), 8);
    }

    #[tokio::test]
    async fn test_decoded_cache_type_checked_and_invalidated() {
        let dir = tempfile::TempDir::new().unwrap();
        let cache = DiskCache::new_with_max_bytes(dir.path().to_path_buf(), 1024).unwrap();
        cache.insert_decoded("immutable.bin", Arc::new(String::from("decoded")));

        let decoded = cache
            .get_decoded::<String>("immutable.bin")
            .unwrap()
            .unwrap();
        assert_eq!(decoded.as_str(), "decoded");
        assert!(cache.get_decoded::<u64>("immutable.bin").is_err());

        cache.invalidate("immutable.bin").await.unwrap();
        assert!(cache
            .get_decoded::<String>("immutable.bin")
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn disk_eviction_worker_panic_resets_running_flag() {
        let _test_guard = EVICTION_TEST_LOCK.lock().await;
        EVICTION_TEST_PANIC_ON_START.store(false, Ordering::SeqCst);
        EVICTION_TEST_REMOVE_FAILURES.store(0, Ordering::SeqCst);
        EVICTION_TEST_REMOVE_ATTEMPTS.store(0, Ordering::SeqCst);

        let dir = tempfile::TempDir::new().unwrap();
        let cache = DiskCache::new_with_max_bytes(dir.path().to_path_buf(), 1).unwrap();

        EVICTION_TEST_PANIC_ON_START.store(true, Ordering::SeqCst);
        cache
            .put("panic-entry", &Bytes::from(vec![1_u8; 16]))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;

        assert!(
            !cache.eviction_running.load(Ordering::Acquire),
            "eviction_running must be reset after an eviction worker panic"
        );

        cache.spawn_eviction_if_needed();
        for _ in 0..100 {
            if cache.total_size() <= 1 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!(
            "subsequent eviction did not run after panic: size={}",
            cache.total_size()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn disk_eviction_removal_errors_do_not_spin() {
        let _test_guard = EVICTION_TEST_LOCK.lock().await;
        EVICTION_TEST_PANIC_ON_START.store(false, Ordering::SeqCst);

        let dir = tempfile::TempDir::new().unwrap();
        let cache = DiskCache::new_with_max_bytes(dir.path().to_path_buf(), 1).unwrap();
        EVICTION_TEST_REMOVE_ATTEMPTS.store(0, Ordering::SeqCst);
        EVICTION_TEST_REMOVE_FAILURES.store(1_000, Ordering::SeqCst);

        cache
            .put("permission-denied-entry", &Bytes::from(vec![2_u8; 16]))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let attempts = EVICTION_TEST_REMOVE_ATTEMPTS.load(Ordering::SeqCst);
        EVICTION_TEST_REMOVE_FAILURES.store(0, Ordering::SeqCst);
        assert!(
            attempts <= 3,
            "persistent eviction removal errors must be retry-bounded, got {attempts} attempts"
        );
    }
}
