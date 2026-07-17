//! Disposable raw-byte and decoded-object caches for immutable artifacts.
//!
//! This module owns Zeppelin's process-local memory tier and local-disk tier.
//! Query, WAL, indexing, compaction, and hydration code enter through
//! [`crate::cache::DiskCache`] with an object key that they obtained from authoritative
//! metadata. [`crate::cache::DiskCache::get_or_fetch`] checks memory, then disk, and finally
//! invokes a caller-supplied fetch closure—normally an S3/MinIO GET through the
//! storage layer. [`crate::cache::MemoryCache`] implements the optional hottest tier.
//!
//! This module does **not** decide which WAL fragments or segments are visible.
//! The manifest in object storage remains authoritative, and a cache hit cannot
//! publish, resurrect, or hide an artifact. Nor does this module compare a
//! cached value with S3. Reuse is correct only when the caller supplies the
//! exact key of an immutable object selected by its current manifest snapshot.
//! Code that reuses a key for mutable bytes must invalidate it explicitly; that
//! pattern is outside Zeppelin's write-once artifact contract.
//!
//! [`crate::cache::manifest_cache`] is separate because manifests are mutable
//! visibility records with TTL and freshness rules.
//! [`crate::cache::hydration`] fills this raw-object cache speculatively, but
//! hydration failure never changes authoritative data.
//!
//! ## Reading map
//!
//! 1. Start with [`crate::cache::DiskCache`] for the shared two-tier façade and
//!    its authority and concurrency contract.
//! 2. Read [`crate::cache::DiskCache::get`] and
//!    [`crate::cache::DiskCache::get_or_fetch`] for normal lookup and per-key
//!    miss coalescing.
//! 3. Read [`crate::cache::DiskCache::put`] for atomic local publication, then
//!    the eviction helpers below the implementation for approximate-LRU
//!    capacity control.
//! 4. Read [`crate::cache::DiskCache::pin_scoped`] for active-segment metadata
//!    retention and [`crate::cache::DiskCache::get_decoded`] for typed,
//!    allocation-sharing decode reuse.
//! 5. Finish with [`crate::cache::MemoryCache`] to see the synchronous hottest
//!    tier.
//!
//! ## Lookup and authority
//!
//! ```text
//! current manifest selects immutable object key
//!                       |
//!                       v
//!                get_or_fetch(key)
//!                       |
//!              +--------+--------+
//!              | memory hit      | miss
//!              v                 v
//!        shared Bytes       indexed disk file
//!                                |
//!                       +--------+--------+
//!                       | hit             | miss
//!                       v                 v
//!               promote to memory   per-key async mutex
//!                                           |
//!                                           v
//!                                   recheck both tiers
//!                                           |
//!                                           v
//!                                  caller fetch closure
//!                                  (normally S3/MinIO)
//!                                           |
//!                              +------------+------------+
//!                              | return authoritative     |
//!                              | bytes to every waiter    |
//!                              | and best-effort cache    |
//!                              +--------------------------+
//!
//! S3/MinIO bytes and the manifest are authoritative; both local tiers are
//! disposable accelerators.
//! ```
//!
//! ## Pinning, eviction, and decoded values
//!
//! ```text
//! scope (for example "photos:centroids")
//!                 |
//!                 v
//!       scope -> current object key -------> decoded Arc<T>
//!                 |                           keyed by same S3 key
//!                 v
//!       disk + memory pin sets
//!                 |
//!                 | excluded when a worker chooses a victim
//!                 v
//! approximate-LRU sample -> remove memory handle -> unlink disk file
//! ```
//!
//! A scoped rotation removes the previous key's decoded entry and pin before
//! retaining the new key. Pinning affects eviction only; it does not prove that
//! bytes exist locally and it does not make an object visible to queries.
//!
//! ## Concurrency and lock scope
//!
//! ```text
//! task A misses key K ----+
//!                         +--> Arc<Mutex<()>> for K -- held across fetch await
//! task B misses key K ----+             |
//!                                       +--> one fetch; B rechecks after A
//!
//! task C misses key J --------> different mutex; fetches independently
//! ```
//!
//! The same-key mutex is intentionally held across `.await`: it serializes the
//! expensive backend operation, not the whole cache. DashMap shard guards are
//! otherwise kept away from filesystem awaits; prefix invalidation first takes
//! owned snapshots. Capacity accounting and eviction are concurrent and may be
//! transiently above the configured limit while puts or a worker are in flight.
//!
//! ## Invariants and failure model
//!
//! - Cache contents are never a visibility or freshness authority.
//! - Keys supplied to raw and decoded caches identify immutable bytes.
//! - Disk publication writes a unique temporary file and renames it before the
//!   in-memory index advertises the entry.
//! - Fetch errors remain errors and do not populate the cache. A successful
//!   fetch is still returned if only the optional cache fill fails.
//! - Pinned entries are excluded when eviction selects a victim. Capacity may
//!   remain above budget when every candidate is pinned.
//! - The eviction-running flag is reset by RAII even if its Tokio task panics or
//!   is cancelled.
//!
//! ## Rust concepts used here
//!
//! [`std::sync::Arc`] gives query tasks and the background eviction task shared ownership
//! without a Java-style garbage collector. Cloning an `Arc` increments a
//! reference count; cloning [`bytes::Bytes`] similarly shares an immutable buffer
//! instead of copying its payload. In C these lifetimes would normally require
//! an explicit refcount and carefully paired cleanup calls.
//!
//! [`dashmap::DashMap`] supplies sharded synchronization for unrelated keys.
//! Tokio [`tokio::sync::Mutex`] values coalesce asynchronous work without blocking an executor
//! thread, while atomics maintain approximate counters without one global
//! lock. `EvictionRunningReset` uses [`std::ops::Drop`] as RAII: it is comparable
//! to Java `finally` or a C cleanup label, but Rust invokes it automatically on
//! every normal unwind path.
//!
//! The decoded tier stores `Arc<dyn Any + Send + Sync>` and recovers the
//! concrete type with a checked downcast. Java would use a runtime cast from
//! `Object`; C would usually rely on a tag plus `void *`. Rust retains the
//! runtime check while `Send + Sync + 'static` prevents storing a borrowed or
//! thread-unsafe value in this process-wide concurrent cache.

/// Bounded decoded memo for immutable segment FTS artifacts.
pub mod decoded_cache;
/// Warm-set hydration policy and worker support.
pub mod hydration;
/// Manifest-level TTL cache to avoid repeated S3 reads.
pub mod manifest_cache;

use std::any::Any;
use std::collections::{HashMap, HashSet};
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

/// Maximum number of unpinned candidates compared in one approximate-LRU sample.
///
/// Sampling avoids a full oldest-entry scan in the common case. Finding 16
/// unpinned candidates can still inspect more map entries when many are pinned.
const EVICTION_SAMPLE_SIZE: usize = 16;
/// Consecutive unlink failures allowed before the disk worker pauses briefly.
const EVICTION_MAX_CONSECUTIVE_FAILURES: usize = 3;
/// Delay after repeated unlink failures, preventing a tight retry loop.
const EVICTION_FAILURE_BACKOFF: Duration = Duration::from_secs(1);

/// Test-only switch that injects a panic at the start of the next disk worker.
#[cfg(test)]
static EVICTION_TEST_PANIC_ON_START: AtomicBool = AtomicBool::new(false);
/// Test-only count of upcoming cache-file removals that should fail.
#[cfg(test)]
static EVICTION_TEST_REMOVE_FAILURES: AtomicUsize = AtomicUsize::new(0);
/// Test-only count of cache-file removal attempts made by eviction.
#[cfg(test)]
static EVICTION_TEST_REMOVE_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
/// Serializes tests that mutate the process-wide eviction fault switches.
#[cfg(test)]
static EVICTION_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

tokio::task_local! {
    /// Diagnostics destination visible while a query future is being polled.
    static CACHE_DIAGNOSTICS: Arc<CacheDiagnostics>;
}

/// Per-query cache diagnostics, enabled only inside an explicit task-local scope.
///
/// These counters describe cache events, not unique keys or object-store
/// requests. A single high-level operation can probe more than once, while a
/// concurrent waiter can record a hit after the leading task fills the cache.
/// Outside [`with_cache_diagnostics`], recording helpers intentionally do
/// nothing.
///
/// # Rust Notes for Java/C Engineers
///
/// The atomics allow nested cache code to increment shared counters through an
/// immutable reference. `Relaxed` ordering is sufficient because the values are
/// telemetry: the snapshot needs atomic numbers, not a synchronization barrier
/// for cache contents. Java's `AtomicLong` is the nearest analogy. In C this
/// requires `_Atomic uint64_t` and the same explicit memory-order decision.
#[derive(Debug, Default)]
pub struct CacheDiagnostics {
    /// Number of locally observed hit events in the active scope.
    hits: AtomicU64,
    /// Number of locally observed miss events in the active scope.
    misses: AtomicU64,
}

/// Snapshot of cache diagnostics counters.
///
/// The owned integers can be embedded in a debug response without retaining
/// the task-local [`CacheDiagnostics`] allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheDiagnosticsSnapshot {
    /// Cache hits recorded in the scope.
    pub hits: u64,
    /// Cache misses recorded in the scope.
    pub misses: u64,
}

impl CacheDiagnostics {
    /// Returns a point-in-time copy of the hit and miss counters.
    ///
    /// # Returns
    ///
    /// An owned [`CacheDiagnosticsSnapshot`]. Concurrent increments may occur
    /// between the two independent loads, so this is telemetry rather than a
    /// transactional pair.
    ///
    /// # Examples
    ///
    /// A query debug response can call `snapshot` after execution and report
    /// the local cache events without exposing mutable atomic state.
    #[must_use]
    pub fn snapshot(&self) -> CacheDiagnosticsSnapshot {
        CacheDiagnosticsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }
}

/// Polls a future with per-query cache diagnostics installed task-locally.
///
/// Cache operations reached while `future` is polled can update `diagnostics`
/// without threading another argument through every index and WAL helper.
/// Spawned tasks do not automatically use this scope unless their work is
/// polled inside it.
///
/// # Parameters
///
/// - `diagnostics`: Shared destination for hit and miss events. Ownership of
///   this [`Arc`] moves into the scope; callers may retain another clone.
/// - `future`: Query or sub-operation whose cache activity should be counted.
///
/// # Returns
///
/// Exactly the output produced by `future`; the wrapper does not translate
/// errors or alter cancellation.
///
/// # Side Effects
///
/// Temporarily installs a Tokio task-local value while the future is polled and
/// permits nested cache operations to increment its atomics.
///
/// # Examples
///
/// A debug vector query scopes its execution, awaits the response, and then
/// reads the same `Arc<CacheDiagnostics>` to attach hit/miss counts.
///
/// # Rust Notes for Java/C Engineers
///
/// Tokio task-local state resembles Java `ThreadLocal` in purpose, but async
/// futures may move between worker threads, so the value follows the scoped
/// future rather than an OS thread. C has no standard equivalent; it would
/// normally require explicit context plumbing. The generic `F::Output` keeps
/// the wrapper zero-cost and preserves the future's exact result type.
pub async fn with_cache_diagnostics<F>(diagnostics: Arc<CacheDiagnostics>, future: F) -> F::Output
where
    F: Future,
{
    CACHE_DIAGNOSTICS.scope(diagnostics, future).await
}

/// Records one hit when the caller is inside an explicit diagnostics scope.
///
/// # Side Effects
///
/// Increments the scope's hit atomic. With no active scope, `try_with` fails and
/// the event is deliberately ignored.
fn record_cache_hit_for_diagnostics() {
    let _ = CACHE_DIAGNOSTICS.try_with(|diagnostics| {
        diagnostics.hits.fetch_add(1, Ordering::Relaxed);
    });
}

/// Records one miss when the caller is inside an explicit diagnostics scope.
///
/// # Side Effects
///
/// Increments the scope's miss atomic. A normal non-debug query has no scope,
/// so this helper performs no mutation.
fn record_cache_miss_for_diagnostics() {
    let _ = CACHE_DIAGNOSTICS.try_with(|diagnostics| {
        diagnostics.misses.fetch_add(1, Ordering::Relaxed);
    });
}

/// Process-local index metadata for one complete disk-cache file.
///
/// This is reconstructed from disposable files at startup. `last_accessed` is
/// deliberately not persisted; all rebuilt entries begin with a fresh process
/// timestamp and therefore have no stable relative LRU order from before the
/// restart.
struct CacheEntry {
    /// Local filename derived from the object key by replacing `/` with `__`.
    filename: String,
    /// File length charged to `DiskCache::total_size`, in bytes.
    size: u64,
    /// Monotonic timestamp of the last indexed disk access in this process.
    last_accessed: Instant,
}

/// Process-local ownership for scoped cache pins.
///
/// `scope_keys` records the one physical cache key retained by each logical
/// scope. `key_refcounts` is the inverse ownership count, so two branched
/// namespaces can retain the same immutable physical artifact independently.
/// Both maps live behind one async mutex to make rotation and release one
/// atomic state transition.
#[derive(Default)]
struct ScopedPinState {
    scope_keys: HashMap<String, String>,
    key_refcounts: HashMap<String, usize>,
}

impl ScopedPinState {
    fn add_key_owner(&mut self, key: &str) {
        let count = self.key_refcounts.entry(key.to_string()).or_default();
        *count = match count.checked_add(1) {
            Some(next) => next,
            None => panic!("scoped pin owner count overflowed"),
        };
    }

    #[must_use]
    fn remove_key_owner(&mut self, key: &str) -> bool {
        let last_owner = {
            let Some(count) = self.key_refcounts.get_mut(key) else {
                panic!("scoped pin must have an owner count");
            };
            *count = match count.checked_sub(1) {
                Some(next) => next,
                None => panic!("scoped pin owner count must be positive"),
            };
            *count == 0
        };
        if last_owner {
            self.key_refcounts.remove(key);
        }
        last_owner
    }
}

/// Shared façade over an optional memory cache and a persistent local disk cache.
///
/// Files are stored at `{dir}/{filename}`, with `/` in the object key replaced
/// by `__`. On startup, [`DiskCache::new_with_options`] scans that directory to
/// rebuild the process-local index. The directory and every value are
/// disposable: callers must be able to fetch the manifest-selected immutable
/// object from S3/MinIO after a miss.
///
/// The type performs no content checksum or freshness check. The key is its
/// identity and consistency boundary. Using one key for changing bytes would
/// allow a stale raw or decoded hit, so normal callers use immutable WAL and
/// segment object keys from an authoritative manifest snapshot.
///
/// `DashMap` allows unrelated keys to update access timestamps without one
/// global write lock. Per-key Tokio mutexes single-flight cold backend fetches;
/// each mutex is held across the fetch `.await`, while other keys continue.
///
/// # Rust Notes for Java/C Engineers
///
/// Startup normally wraps this value in `Arc<DiskCache>`. Its internal `Arc`
/// fields let an independently spawned eviction future own the state it needs
/// even though the original `&self` borrow ends when `put` returns. Java would
/// share references under garbage collection. C would need an explicit object
/// lifetime protocol plus locks around the maps and counters.
pub struct DiskCache {
    /// Directory containing complete cached objects and temporary write files.
    dir: PathBuf,
    /// Target capacity for indexed disk files, in bytes.
    max_size_bytes: u64,
    /// Object key to local-file metadata, sharded for concurrent access.
    entries: Arc<DashMap<String, CacheEntry>>,
    /// Object keys that approximate-LRU eviction must skip when selecting.
    pinned: Arc<RwLock<HashSet<String>>>,
    /// One pinned key per logical scope plus the inverse owner count for each
    /// physical key. Pinning a new key rotates only that scope; a physical key
    /// remains pinned while any other scope still owns it.
    scoped_pins: Mutex<ScopedPinState>,
    /// Sum of indexed disk-entry sizes; external filesystem changes may make
    /// it differ temporarily from bytes physically present in the directory.
    total_size: Arc<AtomicU64>,
    /// Type-erased decoded immutable metadata keyed by the same S3 object key.
    decoded: DashMap<String, Arc<dyn Any + Send + Sync>>,
    /// Optional in-memory tier consulted before the disk index.
    memory: Option<Arc<MemoryCache>>,
    /// Per-object async mutexes that coalesce concurrent cold misses.
    inflight: DashMap<String, Arc<Mutex<()>>>,
    /// Guards the background eviction worker.
    ///
    /// Cache size may transiently exceed `max_size_bytes` by the size of
    /// in-flight puts while this flag is set.
    eviction_running: Arc<AtomicBool>,
}

impl DiskCache {
    /// Creates the process cache from boot-time cache configuration.
    ///
    /// Disk gigabytes and memory megabytes are converted to byte capacities. A
    /// zero memory capacity disables [`MemoryCache`]; the disk tier is always
    /// constructed and scans any files already present in the configured
    /// directory.
    ///
    /// # Parameters
    ///
    /// - `config`: Borrowed cache settings. The directory path is cloned; no
    ///   reference to the configuration is retained.
    ///
    /// # Returns
    ///
    /// A ready cache whose disk index reflects readable regular files found at
    /// startup.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Cache`] if the cache directory cannot be
    /// created. Individual directory-scan and metadata failures are treated as
    /// disposable-cache misses by the rebuild helper rather than construction
    /// failures.
    ///
    /// # Side Effects
    ///
    /// Creates the configured local directory if needed, scans it
    /// synchronously, and best-effort removes abandoned `.tmp` files.
    ///
    /// # Performance
    ///
    /// Startup work is linear in the number of directory entries. No S3/MinIO
    /// request occurs.
    ///
    /// # Examples
    ///
    /// With a 50 GiB disk limit and 256 MiB memory limit, startup restores the
    /// disk index and creates a 256 MiB hottest tier. With memory set to zero,
    /// reads begin at disk.
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

    /// Creates a disk-only cache with an explicit byte capacity.
    ///
    /// This entry point is useful for tests and embedded callers that do not
    /// need the configuration unit conversions or the memory tier.
    ///
    /// # Parameters
    ///
    /// - `dir`: Owned directory path; the cache keeps it for later file I/O.
    /// - `max_size_bytes`: Target size of indexed disk entries in bytes. Zero
    ///   is permitted and causes every unpinned put to request eviction.
    ///
    /// # Returns
    ///
    /// A disk-only [`DiskCache`] rebuilt from `dir`.
    ///
    /// # Errors
    ///
    /// Returns a cache error when `dir` cannot be created.
    ///
    /// # Examples
    ///
    /// A test can create a 100-byte cache, insert three 50-byte objects, and
    /// wait for background eviction to reduce indexed size to the target.
    pub fn new_with_max_bytes(dir: PathBuf, max_size_bytes: u64) -> Result<Self> {
        Self::new_with_options(dir, max_size_bytes, None)
    }

    /// Creates a cache from byte capacities and an optional owned memory tier.
    ///
    /// This is the common constructor behind the configuration and disk-only
    /// entry points. Existing local files are indexed, but they are not decoded,
    /// loaded into memory, or checked against object storage.
    ///
    /// # Parameters
    ///
    /// - `dir`: Local disposable-cache directory.
    /// - `max_size_bytes`: Target disk capacity in bytes.
    /// - `memory`: Optional ready [`MemoryCache`]. Passing `None` makes disk the
    ///   first tier; passing `Some` moves the cache into shared ownership.
    ///
    /// # Returns
    ///
    /// A cache whose disk metadata index has been rebuilt from regular files.
    /// Pin sets, decoded entries, and in-flight locks always start empty.
    ///
    /// # Errors
    ///
    /// Returns a cache error if local directory creation fails. Scan errors for
    /// this optional acceleration layer are skipped rather than masking the
    /// authoritative object-store path.
    ///
    /// # Side Effects
    ///
    /// Creates and scans `dir`; abandoned temporary files are removed on a
    /// best-effort basis.
    ///
    /// # Consistency
    ///
    /// Rebuilt files are trusted only as values for exact keys. This operation
    /// does not read a manifest or make any artifact visible.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `Option<MemoryCache>` makes the enabled/disabled states explicit without
    /// a nullable pointer. `memory.map(Arc::new)` moves the owned cache into an
    /// `Arc`; after that move this constructor cannot use the original value.
    /// Java references do not express that ownership transfer, and C would
    /// enforce it only by convention.
    pub fn new_with_options(
        dir: PathBuf,
        max_size_bytes: u64,
        memory: Option<MemoryCache>,
    ) -> Result<Self> {
        // Directory creation is the only startup failure that prevents the
        // cache from serving later reads and writes.
        std::fs::create_dir_all(&dir).map_err(|e| {
            ZeppelinError::Cache(format!("failed to create cache dir {:?}: {}", dir, e))
        })?;

        let cache = Self {
            dir,
            max_size_bytes,
            entries: Arc::new(DashMap::new()),
            pinned: Arc::new(RwLock::new(HashSet::new())),
            scoped_pins: Mutex::new(ScopedPinState::default()),
            total_size: Arc::new(AtomicU64::new(0)),
            decoded: DashMap::new(),
            memory: memory.map(Arc::new),
            inflight: DashMap::new(),
            eviction_running: Arc::new(AtomicBool::new(false)),
        };

        // Disk contents are disposable, so individual unreadable entries are
        // skipped rather than making Zeppelin unavailable.
        cache.rebuild_index_sync();

        Ok(cache)
    }

    /// Rebuilds the process-local disk index from regular cache files.
    ///
    /// Every accepted file is charged at its current metadata length and gets a
    /// fresh access timestamp. Directory entries, non-UTF-8 filenames, and files
    /// whose metadata cannot be read are skipped. Temporary files are removed
    /// best-effort because no completed put advertises them.
    ///
    /// # Side Effects
    ///
    /// Replaces `total_size`, inserts reconstructed entries, and may delete
    /// `.tmp` files. It performs synchronous local filesystem I/O and no
    /// object-store I/O.
    ///
    /// # Performance
    ///
    /// Scans the whole directory once. All restored entries receive nearly the
    /// same [`Instant`], so their pre-restart recency is intentionally lost.
    ///
    /// # Examples
    ///
    /// If a previous process left two completed 10-byte files and one temporary
    /// write, restart indexes 20 bytes and attempts to remove the temporary one.
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

            // A completed put is published by rename, so a remaining temporary
            // name can only be an interrupted or failed write.
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

    /// Encodes an object-store key as the cache's flat local filename.
    ///
    /// # Parameters
    ///
    /// - `key`: Full cache identity, normally an immutable S3/MinIO object key.
    ///
    /// # Returns
    ///
    /// A newly allocated string with every `/` replaced by `__`.
    ///
    /// # Examples
    ///
    /// `photos/segments/seg-7/centroids.bin` becomes
    /// `photos__segments__seg-7__centroids.bin`.
    ///
    /// TODO(doc): Verify that cacheable keys cannot contain a literal `__`.
    /// This encoding is otherwise non-injective, and startup decoding would
    /// interpret those underscores as a path separator.
    fn key_to_filename(key: &str) -> String {
        key.replace('/', "__")
    }

    /// Resolves an object key to its deterministic cache-directory path.
    ///
    /// # Parameters
    ///
    /// - `key`: Object-store key accepted by `key_to_filename`.
    ///
    /// # Returns
    ///
    /// An owned path below this cache's configured directory. The helper does
    /// not access the filesystem or establish that the file exists.
    ///
    /// # Examples
    ///
    /// For directory `/cache` and key `ns/wal/f1`, the result ends in
    /// `/cache/ns__wal__f1`.
    fn file_path(&self, key: &str) -> PathBuf {
        self.dir.join(Self::key_to_filename(key))
    }

    /// Returns the shared asynchronous single-flight mutex for one object key.
    ///
    /// # Parameters
    ///
    /// - `key`: Exact cache key whose cold fetches must be coalesced.
    ///
    /// # Returns
    ///
    /// A cloned [`Arc`] handle to the existing mutex or to a newly inserted one.
    /// Different keys receive independent mutexes.
    ///
    /// # Side Effects
    ///
    /// May allocate a key string, mutex, and reference-counted owner in the
    /// in-flight map.
    ///
    /// # Examples
    ///
    /// Thirty-two misses for one cluster key all receive handles to one mutex;
    /// a simultaneous miss for another cluster does not wait on it.
    fn inflight_lock(&self, key: &str) -> Arc<Mutex<()>> {
        self.inflight
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .value()
            .clone()
    }

    /// Removes a per-key mutex after the last active caller releases its handle.
    ///
    /// # Parameters
    ///
    /// - `key`: Map key associated with `lock`.
    /// - `lock`: This caller's owned [`Arc`] handle after its mutex guard has
    ///   already been dropped.
    ///
    /// # Side Effects
    ///
    /// Drops the caller's handle, then conditionally removes the map entry only
    /// when the map is its sole remaining owner. A waiter holding another handle
    /// keeps the mutex available and prevents premature replacement.
    ///
    /// # Examples
    ///
    /// The leading fetch cannot remove key `K` while a second task still owns a
    /// handle and is waiting to recheck the cache.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `Arc::strong_count` observes reference-counted ownership. Dropping this
    /// function's handle before `remove_if` distinguishes an idle map-owned
    /// mutex from one still used by a task. Java's garbage collector exposes no
    /// equivalent deterministic owner count; C would need an atomic refcount.
    fn remove_idle_inflight_lock(&self, key: &str, lock: Arc<Mutex<()>>) {
        drop(lock);
        self.inflight
            .remove_if(key, |_, current| Arc::strong_count(current) == 1);
    }

    /// Looks up complete immutable bytes in memory and then on local disk.
    ///
    /// A disk hit updates recency and promotes a shared buffer into the optional
    /// memory tier. Any disk-read error is treated as a disposable-cache miss:
    /// the stale index record is removed and callers may fetch the authoritative
    /// object through [`DiskCache::get_or_fetch`].
    ///
    /// # Parameters
    ///
    /// - `key`: Exact immutable object key. The function borrows the string only
    ///   for this lookup.
    ///
    /// # Returns
    ///
    /// `Some(Bytes)` for a memory or disk hit. `None` means no indexed local
    /// value could be read; it does not mean the authoritative object is absent.
    ///
    /// # Side Effects
    ///
    /// Updates access time, metrics, and active diagnostics. A disk hit may
    /// insert into memory. A failed disk read removes the index entry and
    /// adjusts indexed-size accounting.
    ///
    /// # Consistency
    ///
    /// No manifest or object-store freshness check occurs. Correct reuse relies
    /// on the caller selecting an immutable key from authoritative metadata.
    ///
    /// # Performance
    ///
    /// A memory hit clones a [`Bytes`] handle. A disk hit performs one complete
    /// local-file read and allocates its buffer; it never performs a range read
    /// or S3 GET.
    ///
    /// # Examples
    ///
    /// After a cluster object was cached, the next query obtains its complete
    /// bytes locally. If an operator deleted the file behind the process, this
    /// method returns `None` and repairs the in-memory index.
    #[instrument(skip(self), fields(key = key))]
    pub async fn get(&self, key: &str) -> Option<Bytes> {
        // The hottest tier owns shared immutable buffers, so a hit avoids local
        // filesystem work and payload copying.
        if let Some(ref mem) = self.memory {
            if let Some(data) = mem.get(key) {
                crate::metrics::CACHE_HITS_TOTAL
                    .with_label_values(&["memory_hit"])
                    .inc();
                record_cache_hit_for_diagnostics();
                debug!("memory cache hit");
                return Some(data);
            }
        }

        // End the DashMap guard before filesystem I/O so this shard remains
        // available while Tokio waits for the read.
        {
            let mut entry = self.entries.get_mut(key)?;
            entry.last_accessed = Instant::now();
        }

        // Cache files are optional acceleration data: an unreadable or externally
        // removed file is repaired as a miss rather than replacing S3 authority.
        let path = self.file_path(key);
        match tokio::fs::read(&path).await {
            Ok(data) => {
                let bytes = Bytes::from(data);
                // Bytes wraps the newly read buffer; cloning it into memory is a
                // refcount increment rather than a second payload allocation.
                if let Some(ref mem) = self.memory {
                    mem.insert(key, bytes.clone());
                }
                crate::metrics::CACHE_HITS_TOTAL
                    .with_label_values(&["hit"])
                    .inc();
                record_cache_hit_for_diagnostics();
                debug!("cache hit");
                Some(bytes)
            }
            Err(_) => {
                // The index no longer advertises a file that cannot be read.
                if let Some((_, entry)) = self.entries.remove(key) {
                    self.total_size.fetch_sub(entry.size, Ordering::Relaxed);
                    crate::metrics::CACHE_ENTRIES.dec();
                }
                crate::metrics::CACHE_HITS_TOTAL
                    .with_label_values(&["miss"])
                    .inc();
                record_cache_miss_for_diagnostics();
                debug!("cache miss (file missing)");
                None
            }
        }
    }

    /// Publishes complete bytes atomically to disk, then updates both local tiers.
    ///
    /// The payload is written to a unique temporary file in the cache directory
    /// and renamed to the deterministic final path before the disk index or
    /// memory tier advertises it. Capacity enforcement is asynchronous: this
    /// method returns after scheduling eviction and the cache may temporarily be
    /// over budget.
    ///
    /// # Parameters
    ///
    /// - `key`: Exact object identity. Normal callers use an immutable S3/MinIO
    ///   key selected by a manifest.
    /// - `data`: Borrowed complete object bytes. Disk I/O consumes the bytes
    ///   during this call; the caller retains its [`Bytes`] handle.
    ///
    /// # Returns
    ///
    /// `Ok(())` once the final file, memory tier, and process index are updated
    /// and any necessary eviction worker has been requested.
    ///
    /// # Errors
    ///
    /// Returns a cache error if writing the temporary file or renaming it fails.
    /// A failed write may leave a uniquely named `.tmp` file, which startup
    /// later removes best-effort. Neither memory nor the index is updated before
    /// a successful rename.
    ///
    /// # Side Effects
    ///
    /// Writes and renames a local file, updates metrics and capacity counters,
    /// may promote a shared value to memory, and may spawn a Tokio eviction task.
    /// It never writes object storage.
    ///
    /// # Consistency
    ///
    /// Rename prevents readers from observing a partially written cache file.
    /// Cache publication has no bearing on manifest visibility. Reusing a key
    /// for different bytes is unsupported because decoded entries are not
    /// replaced or validated here.
    ///
    /// # Performance
    ///
    /// Writes the full payload once to local disk. The memory insertion clones
    /// only a [`Bytes`] reference. Eviction file I/O occurs after this call on a
    /// background task.
    ///
    /// # Examples
    ///
    /// A cold query downloads a 4 MiB cluster object, writes a temporary local
    /// file, renames it, and returns. If that crosses the disk budget, a worker
    /// later evicts unpinned approximate-LRU entries.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `data: &Bytes` is a temporary shared borrow, while `data.clone()` creates
    /// another cheap refcounted handle for memory. It does not copy 4 MiB. Rust
    /// statically prevents the cache from retaining the borrowed `&Bytes` after
    /// this future returns.
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

        // A unique sibling temporary path avoids concurrent writers sharing a
        // partial file; rename is the local visibility boundary.
        tokio::fs::write(&tmp_path, data)
            .await
            .map_err(|e| ZeppelinError::Cache(format!("failed to write cache file: {e}")))?;
        tokio::fs::rename(&tmp_path, &path)
            .await
            .map_err(|e| ZeppelinError::Cache(format!("failed to rename cache file: {e}")))?;

        // Only a fully renamed file is eligible for promotion.
        if let Some(ref mem) = self.memory {
            mem.insert(key, data.clone());
        }

        // Replacing an index entry removes its old size before charging the new
        // file, keeping overwrite accounting balanced in the sequential case.
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

        // Capacity is a target rather than an inline latency penalty.
        self.spawn_eviction_if_needed();

        Ok(())
    }

    /// Returns cached bytes or single-flights one caller-supplied backend fetch.
    ///
    /// The fast path probes memory and disk without locking. On a miss, callers
    /// for the same key share one async mutex. The winner rechecks the cache,
    /// holds that per-key mutex across `fetch().await`, and attempts to populate
    /// local tiers. Waiters recheck after the winner releases the mutex and
    /// therefore normally avoid duplicate S3/MinIO GETs.
    ///
    /// # Parameters
    ///
    /// - `key`: Immutable object key shared by all work being coalesced.
    /// - `fetch`: One-shot closure that creates an async authoritative read.
    ///   It is invoked only if both cache checks miss for this caller.
    ///
    /// # Returns
    ///
    /// Shared complete bytes from memory, disk, or `fetch`. Successful fetched
    /// bytes are returned even when the optional local cache write fails.
    ///
    /// # Errors
    ///
    /// Propagates an error returned by `fetch`; the failed result is not cached
    /// and does not poison the per-key mutex, so a later caller can retry. Cache
    /// fill failures are logged and deliberately do not replace a successful
    /// authoritative read with an error.
    ///
    /// # Side Effects
    ///
    /// Updates cache metrics and diagnostics, may await a per-key mutex, may
    /// execute one backend operation, and may write both local tiers. Idle
    /// per-key mutexes are removed after the operation.
    ///
    /// # Consistency
    ///
    /// Single-flight reduces duplicate reads; it is not a visibility protocol.
    /// The supplied fetch closure and immutable key must come from current
    /// authoritative metadata. A miss never substitutes empty bytes.
    ///
    /// # Performance
    ///
    /// Hits have memory or local-disk cost. A cold key normally causes one full
    /// backend read regardless of same-process concurrency, plus one local full
    /// write. Different keys fetch independently.
    ///
    /// # Examples
    ///
    /// If 32 cold queries request one grouped cluster object, one task performs
    /// the S3 GET while 31 wait. All receive shared bytes. If that GET fails,
    /// all relevant calls observe errors or retry under the mutex; a later query
    /// can run a fresh fetch.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `F: FnOnce() -> Fut` permits the closure to move an owned store handle or
    /// key into exactly one fetch. Java would use a `Supplier<CompletionStage>`;
    /// C would pass a function pointer plus context. Rust's trait bounds preserve
    /// the concrete future type without virtual dispatch. The mutex guard's
    /// lexical lifetime deliberately spans `.await`, but only for this key.
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
            record_cache_miss_for_diagnostics();
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

    /// Retrieves typed decoded metadata for an immutable object key.
    ///
    /// The decoded tier avoids repeatedly parsing centroids, grouped-object
    /// directories, sketches, and similar metadata. It is process-local and is
    /// independent of whether raw bytes are currently present in memory or on
    /// disk.
    ///
    /// # Parameters
    ///
    /// - `key`: Authoritative S3/MinIO object key used when the value was
    ///   inserted. Generic type `T` must be the one associated with that key.
    ///
    /// # Returns
    ///
    /// `Ok(Some(Arc<T>))` with shared ownership of the decoded allocation on a
    /// typed hit, or `Ok(None)` when no decoded entry exists.
    ///
    /// # Errors
    ///
    /// Returns a cache error when the key exists but contains a different
    /// concrete type. The mismatch fails loudly rather than pretending to be a
    /// miss and decoding a second interpretation of the same key.
    ///
    /// # Consistency
    ///
    /// This method performs no byte, checksum, manifest, or freshness check.
    /// Callers must key decoded values by immutable object identity and use one
    /// concrete decoded type per key.
    ///
    /// # Performance
    ///
    /// Performs a sharded-map lookup, clones one [`Arc`], and checks its runtime
    /// type. It does not clone the decoded allocation or perform I/O.
    ///
    /// # Examples
    ///
    /// Once a grouped cluster header is decoded as `ClusterObjectLayout`, later
    /// queries share that layout. Requesting the same key as `ResidentSketch`
    /// returns a type-mismatch error.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `Any` is Rust's opt-in runtime type information. `Arc::downcast` consumes
    /// the cloned type-erased handle and returns a typed one only when its
    /// concrete type ID matches `T`. This resembles a checked Java cast from
    /// `Object`; unlike a C `void *` cast, a wrong type cannot be dereferenced.
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

    /// Associates shared decoded metadata with its immutable object-store key.
    ///
    /// Any existing type-erased value for `key` is replaced. The method does not
    /// verify that raw bytes are cached or that `decoded` was derived from them;
    /// those are caller responsibilities at the parsing boundary.
    ///
    /// # Parameters
    ///
    /// - `key`: Exact immutable S3/MinIO key that identifies the encoded bytes.
    /// - `decoded`: Shared owned decoded value. The cache takes this [`Arc`]
    ///   handle; callers may retain another clone.
    ///
    /// # Side Effects
    ///
    /// Allocates an owned key and replaces the corresponding process-local
    /// decoded-map entry.
    ///
    /// # Consistency
    ///
    /// Reusing `key` for new bytes or a different decoded type would make hits
    /// stale or produce a type mismatch. Zeppelin's immutable artifact keys
    /// avoid that ambiguity.
    ///
    /// # Examples
    ///
    /// After validating and decoding `seg-7/coarse_sketch.bin`, indexing stores
    /// its `Arc<ResidentSketch>` so queries can share the allocation.
    pub fn insert_decoded<T>(&self, key: &str, decoded: Arc<T>)
    where
        T: Any + Send + Sync + 'static,
    {
        self.decoded.insert(key.to_string(), decoded);
    }

    /// Reports whether any decoded value is stored for a key.
    ///
    /// # Parameters
    ///
    /// - `key`: Exact immutable object key to test.
    ///
    /// # Returns
    ///
    /// `true` when a type-erased entry exists, regardless of its concrete type;
    /// `false` otherwise. Use [`DiskCache::get_decoded`] for a checked typed hit.
    ///
    /// # Examples
    ///
    /// Hydration can use this as a cheap presence hint before deciding whether
    /// decode work is already resident.
    #[must_use]
    pub fn has_decoded(&self, key: &str) -> bool {
        self.decoded.contains_key(key)
    }

    /// Adds a key to both disk and memory eviction-exclusion sets.
    ///
    /// Pinning is allowed before bytes exist, so it expresses retention intent
    /// rather than presence. It does not fetch, decode, validate, or publish an
    /// object.
    ///
    /// # Parameters
    ///
    /// - `key`: Exact cache key that future victim selection should skip.
    ///
    /// # Side Effects
    ///
    /// Acquires the async pin-set write lock, allocates the key if newly pinned,
    /// and mirrors the marker into the optional memory tier.
    ///
    /// # Consistency
    ///
    /// The pin affects cache eviction only. The manifest still decides whether
    /// the object is visible, and a pin does not prove the local file exists.
    ///
    /// # Examples
    ///
    /// A query path pins the active segment's centroids so cold cluster inserts
    /// are evicted first under capacity pressure.
    ///
    /// TODO(doc): Verify whether pinning must rescue a key already selected by a
    /// concurrent disk-eviction pass. The worker checks pins while selecting,
    /// releases that lock, and does not re-check immediately before unlinking.
    pub async fn pin(&self, key: &str) {
        let mut pinned = self.pinned.write().await;
        pinned.insert(key.to_string());
        if let Some(ref mem) = self.memory {
            mem.pin(key);
        }
        debug!(key = key, "pinned cache key");
    }

    /// Removes a direct pin and its decoded value for one key.
    ///
    /// The raw memory and disk bytes remain until normal capacity eviction or
    /// explicit invalidation. Removing the decoded entry releases this cache's
    /// shared owner so rotated metadata can be reclaimed.
    ///
    /// # Parameters
    ///
    /// - `key`: Cache key whose direct disk/memory pin should be removed.
    ///
    /// # Side Effects
    ///
    /// Mutates both pin sets and removes the decoded-map entry. It does not
    /// remove any `scope -> key` association; callers using scoped retention
    /// should call [`DiskCache::unpin_scoped`].
    ///
    /// # Examples
    ///
    /// Once index metadata is no longer active, unpinning makes its raw bytes a
    /// normal approximate-LRU candidate and drops decoded-cache ownership.
    pub async fn unpin(&self, key: &str) {
        let mut pinned = self.pinned.write().await;
        pinned.remove(key);
        self.decoded.remove(key);
        if let Some(ref mem) = self.memory {
            mem.unpin(key);
        }
    }

    /// Rotates one logical scope to a new pinned cache key.
    ///
    /// A scope has one current key. Pinning the same key repairs/mirrors its
    /// disk and memory pins. Pinning a new key releases the old physical key
    /// only when no other logical scope retains it, then retains the
    /// replacement.
    ///
    /// # Parameters
    ///
    /// - `scope`: Stable logical owner, commonly a namespace plus metadata kind
    ///   such as `photos:centroids`.
    /// - `key`: Immutable active-object key to retain for that scope.
    ///
    /// # Side Effects
    ///
    /// Updates the scope ownership maps, both pin sets, decoded entries for an
    /// unowned rotated key, and structured logs. No local or object-store I/O
    /// occurs.
    ///
    /// # Consistency
    ///
    /// This is a retention lifecycle, not an authority boundary. Compaction
    /// publishes a manifest separately; rotating a pin alone cannot change the
    /// segment readers observe.
    ///
    /// # Performance
    ///
    /// Scoped transitions are serialized by one async mutex and update
    /// constant-time hash-map entries. Rotation allocates two strings and takes
    /// the pin-set write lock; it does not scan cache entries or scopes.
    ///
    /// # Examples
    ///
    /// Scope `photos:centroids` initially points to segment 7. After compaction
    /// makes segment 8 active, rotating the scope releases segment 7 if this
    /// was its last owner, drops its decoded centroids, and retains segment 8.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The scoped-state mutex stays held while the derived pin set is updated,
    /// giving concurrent rotations one lock order: scoped ownership, then the
    /// disk pin set. Rust makes that guard lifetime explicit; Java/C code relies
    /// more heavily on manual lock-scope review.
    pub async fn pin_scoped(&self, scope: &str, key: &str) {
        let mut scoped = self.scoped_pins.lock().await;

        if scoped
            .scope_keys
            .get(scope)
            .is_some_and(|current| current == key)
        {
            let mut pinned = self.pinned.write().await;
            pinned.insert(key.to_string());
            if let Some(ref mem) = self.memory {
                mem.pin(key);
            }
            return;
        }

        let old_key = scoped.scope_keys.insert(scope.to_string(), key.to_string());
        let release_old = old_key
            .as_deref()
            .is_some_and(|old_key| scoped.remove_key_owner(old_key));
        scoped.add_key_owner(key);

        let mut pinned = self.pinned.write().await;
        if let Some(old_key) = old_key.as_deref().filter(|_| release_old) {
            pinned.remove(old_key);
            self.decoded.remove(old_key);
            if let Some(ref mem) = self.memory {
                mem.unpin(old_key);
            }
            debug!(scope = scope, key = old_key, "unpinned rotated cache key");
        }
        pinned.insert(key.to_string());
        if let Some(ref mem) = self.memory {
            mem.pin(key);
        }
        debug!(scope = scope, key = key, "pinned cache key for scope");
    }

    /// Removes a scope and releases the cache resources tied to its current key.
    ///
    /// # Parameters
    ///
    /// - `scope`: Logical owner previously passed to
    ///   [`DiskCache::pin_scoped`].
    ///
    /// # Side Effects
    ///
    /// If the scope exists, removes its ownership. Disk and memory pin markers
    /// and the decoded entry are released only when this was the physical key's
    /// last logical owner. Raw bytes then become eligible for ordinary eviction.
    /// An unknown scope is a no-op.
    ///
    /// # Examples
    ///
    /// Removing `photos:bootstrap` after active metadata is restored from normal
    /// centroids releases the bootstrap decode and makes its bytes evictable.
    pub async fn unpin_scoped(&self, scope: &str) {
        let mut scoped = self.scoped_pins.lock().await;
        let Some(old_key) = scoped.scope_keys.remove(scope) else {
            return;
        };

        let release_key = scoped.remove_key_owner(&old_key);
        if !release_key {
            debug!(scope = scope, key = %old_key, "released shared scoped cache pin owner");
            return;
        }

        let mut pinned = self.pinned.write().await;
        pinned.remove(&old_key);
        self.decoded.remove(&old_key);
        if let Some(ref mem) = self.memory {
            mem.unpin(&old_key);
        }
        debug!(scope = scope, key = %old_key, "unpinned scoped cache key");
    }

    /// Checks whether disk eviction currently sees a key in the pin set.
    ///
    /// # Parameters
    ///
    /// - `key`: Exact cache key to inspect.
    ///
    /// # Returns
    ///
    /// `true` when the disk pin set contains `key`; this does not prove bytes or
    /// a decoded value are present.
    ///
    /// # Examples
    ///
    /// Tests use this after a scoped rotation to verify the old centroid key is
    /// released and the new key is retained.
    pub async fn is_pinned(&self, key: &str) -> bool {
        self.pinned.read().await.contains(key)
    }

    /// Forgets one key from raw, decoded, pinned, and indexed local state.
    ///
    /// The in-memory raw and decoded entries are removed first. If the disk
    /// index contains the key, accounting is decremented and its file is
    /// unlinked best-effort. This method does not remove a scoped-pin mapping,
    /// so a later same-key [`DiskCache::pin_scoped`] call can repair the pin.
    ///
    /// # Parameters
    ///
    /// - `key`: Exact cache key to invalidate. An absent key is accepted.
    ///
    /// # Returns
    ///
    /// Currently returns `Ok(())` after local bookkeeping, including when no
    /// entry existed or the best-effort file removal failed.
    ///
    /// # Errors
    ///
    /// The current implementation constructs no error. The `Result` leaves room
    /// for callers to propagate future invalidation failures, but local unlink
    /// errors are intentionally discarded today.
    ///
    /// # Side Effects
    ///
    /// Mutates both cache tiers, decoded state, pin state, capacity metrics, and
    /// possibly the local filesystem. It never deletes the S3/MinIO object.
    ///
    /// # Consistency
    ///
    /// Invalidation cannot change manifest visibility. Because unlink is
    /// best-effort, a failed delete can leave an unindexed file that a future
    /// process rebuild may discover again; immutable-key callers still retain
    /// object-store authority.
    ///
    /// # Performance
    ///
    /// Performs constant-time map operations and at most one local unlink.
    ///
    /// # Examples
    ///
    /// A range reader that detects a wrong-length full-object cache entry calls
    /// this method, then reads the authoritative range from S3 instead of using
    /// the local bytes.
    #[instrument(skip(self), fields(key = key))]
    pub async fn invalidate(&self, key: &str) -> Result<()> {
        // Process-local representations disappear before the awaited unlink, so
        // new lookups cannot use the entry while local cleanup is pending.
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

        // A stale pin without an indexed value would waste retention state.
        let mut pinned = self.pinned.write().await;
        pinned.remove(key);

        Ok(())
    }

    /// Forgets the current snapshot of cache keys beginning with a prefix.
    ///
    /// Prefix invalidation is useful when a namespace or immutable-artifact
    /// family is retired. It collects owned disk metadata before awaiting any
    /// unlink, so no DashMap shard guard crosses filesystem I/O. Entries inserted
    /// concurrently after that snapshot are not guaranteed to be removed.
    ///
    /// # Parameters
    ///
    /// - `prefix`: Byte-for-byte object-key prefix. An empty string matches all
    ///   entries visible during the snapshots.
    ///
    /// # Returns
    ///
    /// Currently returns `Ok(())` after processing all snapshotted keys.
    ///
    /// # Errors
    ///
    /// The current implementation constructs no error and ignores per-file
    /// unlink failures because local cache cleanup is best-effort.
    ///
    /// # Side Effects
    ///
    /// Removes matching memory and decoded entries, then removes snapshotted disk
    /// index entries, their accounting/metrics, and their corresponding disk pin
    /// markers. It attempts to unlink each indexed file. A disk pin with no index
    /// entry and every scoped-pin mapping are left untouched.
    ///
    /// # Consistency
    ///
    /// This removes acceleration state only. S3/MinIO objects and manifest
    /// visibility remain unchanged. Concurrent insertion can race the snapshot,
    /// so callers needing a quiescent purge must provide their own lifecycle
    /// ordering.
    ///
    /// # Performance
    ///
    /// Scans decoded and disk maps and performs one local unlink per matching
    /// indexed entry; work is linear in current cache population plus matches.
    ///
    /// # Examples
    ///
    /// Invalidating `photos/segments/seg-7/` removes that segment's current
    /// local objects while leaving segment 8 entries and all authoritative
    /// object-store data untouched.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The temporary `Vec` deliberately clones keys and metadata. That allocation
    /// shortens concurrent-map borrows before `.await`. In Java, an iterator may
    /// retain map synchronization implicitly; in C, callers must manually copy
    /// stable identifiers before releasing a lock. Rust's borrow checker makes
    /// the ownership handoff visible in the types.
    #[instrument(skip(self), fields(prefix = prefix))]
    pub async fn invalidate_prefix(&self, prefix: &str) -> Result<()> {
        // Clear the synchronous hottest tier before any disk await.
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

        // Owned snapshots keep DashMap shard guards out of filesystem awaits.
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

    /// Returns the bytes currently charged to the disk index.
    ///
    /// # Returns
    ///
    /// The relaxed atomic sum of indexed `CacheEntry` sizes. In-flight puts,
    /// background eviction, ignored unlink failures, or external filesystem
    /// changes can make this differ transiently from physical directory usage.
    ///
    /// # Examples
    ///
    /// After sequentially caching 100-, 200-, and 300-byte objects without
    /// eviction, this reports 600.
    pub fn total_size(&self) -> u64 {
        self.total_size.load(Ordering::Relaxed)
    }

    /// Returns the configured target capacity of the disk tier in bytes.
    ///
    /// # Returns
    ///
    /// The immutable constructor value. It is a target rather than an invariant:
    /// puts return before background eviction and pinned entries may keep the
    /// cache above it.
    ///
    /// # Examples
    ///
    /// A cache created with `new_with_max_bytes(..., 1024)` always reports 1024
    /// here even while an in-flight put has temporarily charged more.
    #[must_use]
    pub fn max_size_bytes(&self) -> u64 {
        self.max_size_bytes
    }

    /// Starts or hands off background disk eviction when indexed size is over budget.
    ///
    /// An atomic flag suppresses redundant workers. The spawned task repeatedly
    /// runs approximate-LRU passes until size is within the target, no unpinned
    /// victim exists, another worker wins a handoff, or removal failures trigger
    /// bounded retry/backoff. An RAII guard clears the flag on every exit path.
    ///
    /// # Side Effects
    ///
    /// May clone shared cache state and spawn a detached Tokio task. That task
    /// unlinks local files, removes memory handles and disk-index entries,
    /// updates metrics, logs failures, and sleeps after repeated errors.
    ///
    /// # Consistency
    ///
    /// Eviction cannot affect object-store bytes or manifest visibility. The
    /// worker snapshots pin membership only while selecting a victim and releases
    /// the pin lock before awaiting the unlink.
    ///
    /// # Performance
    ///
    /// The method itself is constant-time. File deletion and candidate sampling
    /// occur off the put path. The cache may remain over target while the task
    /// runs or indefinitely when every entry is pinned.
    ///
    /// # Examples
    ///
    /// Inserting a second 60-byte value into a 100-byte cache returns with 120
    /// bytes charged. This helper schedules a worker that later removes an
    /// unpinned candidate and brings the charge back under 100.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The spawned future must own everything it uses after `&self` expires, so
    /// paths are cloned and concurrent state is shared through [`Arc`]. Java's
    /// executor captures garbage-collected references. C needs explicit
    /// refcount increments before scheduling and decrements on every exit path.
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

/// RAII guard that releases the disk worker's single-runner flag on every exit.
///
/// The spawned task binds this guard for its full lifetime. Normal completion,
/// early return, cancellation, and panic unwinding all drop it, allowing a later
/// over-budget put to start another worker.
struct EvictionRunningReset {
    /// Shared flag set while a disk eviction worker claims responsibility.
    eviction_running: Arc<AtomicBool>,
}

impl EvictionRunningReset {
    /// Creates a reset guard owning one shared handle to the worker flag.
    ///
    /// # Parameters
    ///
    /// - `eviction_running`: Flag to clear when the returned guard is dropped.
    ///   The caller has already set it to `true`.
    ///
    /// # Returns
    ///
    /// An owned guard whose lifetime should cover the spawned worker body.
    ///
    /// # Examples
    ///
    /// The eviction task creates this before its loop; an injected test panic
    /// unwinds the task and still clears the flag.
    fn new(eviction_running: Arc<AtomicBool>) -> Self {
        Self { eviction_running }
    }
}

impl Drop for EvictionRunningReset {
    /// Clears the worker flag as the guard leaves scope.
    ///
    /// # Side Effects
    ///
    /// Performs a release-store of `false`, publishing completion to tasks that
    /// attempt a subsequent acquire or compare-exchange.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// [`Drop`] is deterministic RAII cleanup. It resembles a Java `finally`
    /// block or C cleanup label, but the compiler inserts it automatically when
    /// ownership ends, including unwinding. It cannot make forced process abort
    /// or power loss observable as a clean shutdown.
    fn drop(&mut self) {
        self.eviction_running.store(false, Ordering::Release);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Result of one background disk-capacity pass.
///
/// The worker uses this explicit enum to distinguish success from a legitimate
/// all-pinned stop and from an unlink failure that should be retried with
/// backoff.
enum EvictionPassOutcome {
    /// Indexed size is now at or below the configured target.
    UnderBudget,
    /// Size is over target, but no currently unpinned entry can be selected.
    NoVictim,
    /// A selected cache file could not be removed for a non-`NotFound` reason.
    Failed,
}

/// Chooses the least recently accessed key from a bounded random sample.
///
/// Iteration starts at a random map position and wraps once. Pinned entries do
/// not count toward the sample, so the helper can inspect more than 16 map
/// entries when pin density is high. The selection is approximate rather than
/// a global LRU ordering.
///
/// # Parameters
///
/// - `entries`: Current sharded disk index.
/// - `pinned`: Stable pin-set view held by the caller during selection.
///
/// # Returns
///
/// An owned unpinned key with the oldest timestamp among at most
/// `EVICTION_SAMPLE_SIZE` candidates, or `None` when the index is empty or all
/// inspected entries are pinned.
///
/// # Performance
///
/// Common-case work is bounded by 16 unpinned candidates rather than the full
/// index. It can scan the full index if most or all entries are pinned.
///
/// # Examples
///
/// Given sampled access times 10s, 3s, and 8s, the 3s key is selected unless it
/// is pinned; another unpinned key is then the candidate.
///
/// # Rust Notes for Java/C Engineers
///
/// DashMap iteration yields temporary guarded views. The function clones only
/// the winning key, so it returns owned data that remains valid after all shard
/// guards end. A C implementation must not return a pointer into an unlocked
/// hash table; Java would usually allocate or retain a key object implicitly.
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

/// Removes unpinned disk entries until the cache reaches its target or must stop.
///
/// Each iteration reads the pin set only long enough to choose a candidate, then
/// releases it before awaiting local deletion. Memory is invalidated first. A
/// successful unlink, or an already-missing file, removes the disk index entry
/// and charges an eviction; another I/O error ends this pass as `Failed`.
///
/// # Parameters
///
/// - `dir`: Borrowed cache directory used to resolve stored filenames.
/// - `max_size_bytes`: Target indexed disk capacity.
/// - `entries`: Shared object-key index owned by the worker's parent cache.
/// - `pinned`: Async set of keys excluded during victim selection.
/// - `total_size`: Atomic indexed-byte counter adjusted after removal.
/// - `memory`: Optional hottest tier from which a chosen key is removed first.
///
/// # Returns
///
/// [`EvictionPassOutcome::UnderBudget`] when the size target is met,
/// [`EvictionPassOutcome::NoVictim`] when no unpinned key is available, or
/// [`EvictionPassOutcome::Failed`] after a non-`NotFound` unlink error.
///
/// # Side Effects
///
/// Invalidates memory entries, unlinks local files, mutates the disk index and
/// byte counter, updates cache metrics, and emits structured diagnostics.
///
/// # Consistency
///
/// This operation touches disposable local copies only. It never deletes an
/// S3/MinIO object and cannot change manifest visibility. The pin read guard is
/// deliberately dropped before filesystem `.await`; see [`DiskCache::pin`] for
/// the resulting selection-versus-pin race that needs contract confirmation.
///
/// # Performance
///
/// Performs one approximate sample and at most one unlink per removed entry.
/// It loops until under budget, so a large overshoot can require many local I/O
/// operations in one pass.
///
/// # Examples
///
/// At 150 bytes with a 100-byte target, removing one 50-byte unpinned file
/// returns `UnderBudget`. If all three files are pinned it returns `NoVictim`;
/// if unlink reports permission denied it returns `Failed` for worker backoff.
///
/// # Rust Notes for Java/C Engineers
///
/// Borrowed `&DashMap`, `&RwLock`, and `&AtomicU64` references are safe because
/// the spawned task owns `Arc`s that keep their allocations alive. The async
/// pin guard is explicitly dropped before `.await`, a lock-scope discipline
/// analogous to releasing Java/C locks before blocking I/O, now visible in the
/// Rust lifetime.
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
                // No unpinned candidate exists in the current index snapshot;
                // capacity is allowed to remain high rather than evict a pin.
                return EvictionPassOutcome::NoVictim;
            }
        }
    }
}

/// Unlinks one cache file, with deterministic fault injection in unit tests.
///
/// # Parameters
///
/// - `path`: Borrowed local path selected by the eviction pass.
///
/// # Returns
///
/// `Ok(())` when the file was removed, otherwise the original `std::io::Error`.
/// Under `cfg(test)`, configured injected failures return `PermissionDenied`
/// before touching the filesystem and every call increments an attempt counter.
///
/// # Errors
///
/// Propagates Tokio filesystem errors such as not found, permission denied, or
/// other local I/O failures. The caller decides whether not found counts as a
/// completed eviction.
///
/// # Side Effects
///
/// Removes one local cache file in production. Tests additionally mutate global
/// atomic fault-injection counters.
///
/// # Examples
///
/// A normal victim path is unlinked. A test can inject three permission errors
/// to prove the outer worker pauses instead of spinning.
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

/// Concurrent in-memory approximate-LRU tier for hot immutable object bytes.
///
/// This sits above disk in [`DiskCache`]. It neither fetches S3/MinIO nor
/// persists data; the parent façade promotes disk/backend bytes into it. Values
/// are [`Bytes`] handles, so a hit shares immutable payload storage instead of
/// copying the complete object.
///
/// [`DashMap`] gives per-shard concurrency, and capacity enforcement runs inline
/// after insertion. Eviction chooses the oldest key from a bounded random
/// sample, not the global oldest key. Pinned keys are skipped when sampled; if
/// every candidate is pinned, size may remain above the configured target.
///
/// # Rust Notes for Java/C Engineers
///
/// Methods take `&self` even when they mutate because `DashMap` and atomics
/// provide interior mutability with synchronization. This resembles Java
/// concurrent collections and atomics. In C, the fields would need explicit
/// mutex/atomic APIs. Rust's `Sync` rules permit shared cross-thread references
/// because those field types enforce synchronization.
pub struct MemoryCache {
    /// Object key to immutable buffer, size, and access timestamp.
    entries: DashMap<String, MemCacheEntry>,
    /// Keys excluded when approximate eviction examines them.
    pinned: DashMap<String, ()>,
    /// Target payload-byte capacity; key/map overhead is not included.
    max_size_bytes: u64,
    /// Sum of entry payload sizes maintained by insert, removal, and eviction.
    total_size: AtomicU64,
}

/// Metadata and shared payload for one in-memory cache entry.
struct MemCacheEntry {
    /// Shared immutable complete-object buffer returned on hits.
    data: Bytes,
    /// Payload length charged to memory capacity, in bytes.
    size: u64,
    /// Monotonic time of the latest successful get or insertion.
    last_accessed: Instant,
}

impl MemoryCache {
    /// Creates an empty memory tier with a payload-byte target.
    ///
    /// # Parameters
    ///
    /// - `max_size_bytes`: Maximum unpinned payload bytes targeted after each
    ///   insertion. Zero is valid; inserted unpinned entries are immediately
    ///   considered for eviction.
    ///
    /// # Returns
    ///
    /// An empty cache with no pins and zero charged bytes.
    ///
    /// # Examples
    ///
    /// A 256 MiB configuration creates this tier with `268_435_456` bytes.
    pub fn new(max_size_bytes: u64) -> Self {
        Self {
            entries: DashMap::new(),
            pinned: DashMap::new(),
            max_size_bytes,
            total_size: AtomicU64::new(0),
        }
    }

    /// Returns a shared buffer for a key and refreshes its eviction recency.
    ///
    /// # Parameters
    ///
    /// - `key`: Exact immutable object key to look up.
    ///
    /// # Returns
    ///
    /// `Some(Bytes)` with a cloned shared handle on hit, or `None` on miss.
    ///
    /// # Side Effects
    ///
    /// A hit updates `last_accessed` while holding the relevant DashMap shard.
    ///
    /// # Performance
    ///
    /// Expected constant-time lookup plus one [`Bytes`] refcount increment; the
    /// payload is not copied.
    ///
    /// # Examples
    ///
    /// Repeated centroid reads refresh that key so colder sampled cluster data
    /// is more likely to be evicted.
    pub fn get(&self, key: &str) -> Option<Bytes> {
        let mut entry = self.entries.get_mut(key)?;
        entry.last_accessed = Instant::now();
        Some(entry.data.clone())
    }

    /// Inserts or replaces one shared buffer and enforces capacity inline.
    ///
    /// # Parameters
    ///
    /// - `key`: Immutable object identity. The cache allocates an owned key.
    /// - `data`: Owned [`Bytes`] handle moved into the entry; moving the handle
    ///   does not copy its payload.
    ///
    /// # Side Effects
    ///
    /// Replaces any existing entry, updates the atomic payload total, and
    /// synchronously evicts sampled unpinned entries until at target or no
    /// victim can be selected.
    ///
    /// # Consistency
    ///
    /// The key is not checked against S3/MinIO. Safe replacement relies on the
    /// parent cache's immutable-key contract.
    ///
    /// # Performance
    ///
    /// Expected constant-time insertion plus one or more bounded samples when
    /// over capacity. An oversized insert may cause several map removals before
    /// returning.
    ///
    /// # Examples
    ///
    /// Inserting a third 5-byte object into a 10-byte cache removes an unpinned
    /// sampled victim before the method returns.
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

    /// Marks a key for exclusion from future memory-victim samples.
    ///
    /// # Parameters
    ///
    /// - `key`: Cache key to retain. It need not exist yet.
    ///
    /// # Side Effects
    ///
    /// Allocates and inserts a marker. The operation does not load bytes or
    /// reduce current size.
    ///
    /// # Examples
    ///
    /// Pinning active centroids before cold inserts makes sampled eviction skip
    /// them. A pin racing a victim already selected by another thread does not
    /// retroactively cancel that selection.
    pub fn pin(&self, key: &str) {
        self.pinned.insert(key.to_string(), ());
    }

    /// Removes a memory retention marker without removing the bytes.
    ///
    /// # Parameters
    ///
    /// - `key`: Key to make eligible for later approximate-LRU selection.
    ///
    /// # Side Effects
    ///
    /// Removes the marker if present. An unknown key is a no-op, and no eviction
    /// pass runs until a later insertion.
    ///
    /// # Examples
    ///
    /// After segment rotation, unpinning old centroids lets future insert
    /// pressure reclaim their memory.
    pub fn unpin(&self, key: &str) {
        self.pinned.remove(key);
    }

    /// Checks whether the memory eviction marker set contains a key.
    ///
    /// # Parameters
    ///
    /// - `key`: Cache key to inspect.
    ///
    /// # Returns
    ///
    /// `true` when a pin marker exists. Presence of a marker does not imply the
    /// byte entry exists.
    ///
    /// # Examples
    ///
    /// A retention test can distinguish a pin marker from a successful
    /// [`MemoryCache::get`] hit.
    #[must_use]
    pub fn is_pinned(&self, key: &str) -> bool {
        self.pinned.contains_key(key)
    }

    /// Removes one memory entry and its pin marker.
    ///
    /// # Parameters
    ///
    /// - `key`: Exact key to forget; absence is accepted.
    ///
    /// # Side Effects
    ///
    /// Drops this cache's [`Bytes`] handle, subtracts its payload length when
    /// present, and clears the pin. Other `Bytes` clones may keep the allocation
    /// alive after invalidation.
    ///
    /// # Examples
    ///
    /// Disk eviction calls this before unlinking the same object's local file so
    /// the hottest tier cannot continue serving it.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Removing a [`Bytes`] value decrements a refcount; it does not forcibly
    /// free memory still shared by an active query. Java has similar reachability
    /// behavior through GC, while C needs an explicit reference-count protocol.
    pub fn invalidate(&self, key: &str) {
        if let Some((_, entry)) = self.entries.remove(key) {
            self.total_size.fetch_sub(entry.size, Ordering::Relaxed);
        }
        self.pinned.remove(key);
    }

    /// Removes the current snapshot of entries beginning with a key prefix.
    ///
    /// # Parameters
    ///
    /// - `prefix`: Byte-for-byte key prefix. Empty matches every snapshotted
    ///   entry.
    ///
    /// # Side Effects
    ///
    /// Allocates matching owned keys, then removes their entries, payload
    /// charges, and pins. Concurrent insertions after the snapshot may survive.
    ///
    /// # Performance
    ///
    /// Scans the full entries map and performs expected constant-time removal
    /// for every match.
    ///
    /// # Examples
    ///
    /// Prefix `ns/segments/seg-7/` removes segment 7 buffers without disturbing
    /// keys under segment 8.
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

    /// Returns the payload bytes currently charged to memory entries.
    ///
    /// # Returns
    ///
    /// A relaxed atomic snapshot. It excludes key strings, map overhead, and
    /// allocations retained only by [`Bytes`] clones outside this cache.
    ///
    /// # Examples
    ///
    /// Two sequential entries of three and five bytes produce a total of eight.
    pub fn total_size(&self) -> u64 {
        self.total_size.load(Ordering::Relaxed)
    }

    /// Evicts sampled unpinned entries until memory is at target or has no victim.
    ///
    /// # Side Effects
    ///
    /// Removes entries, decrements payload accounting, and emits debug logs. If
    /// all entries are pinned, it returns with size above target.
    ///
    /// # Performance
    ///
    /// Runs synchronously on the inserting thread/task. Each loop selects at
    /// most one victim, so a large oversized value can require many iterations.
    ///
    /// # Examples
    ///
    /// At 15 bytes with a 10-byte target, one 5-byte victim is sufficient. At
    /// 15 pinned bytes, no victim exists and the total remains 15.
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

    /// Selects the oldest unpinned key from a bounded randomized sample.
    ///
    /// # Returns
    ///
    /// An owned candidate key, or `None` when the map is empty or every entry
    /// encountered across the wrapped scan is pinned.
    ///
    /// # Performance
    ///
    /// Usually examines 16 unpinned candidates. A high pinned fraction can make
    /// it inspect the full map. Only the selected key is cloned for return.
    ///
    /// # Examples
    ///
    /// Frequently accessed hot keys carry recent timestamps and tend to survive
    /// a flood of cold insertions, although approximate sampling gives no strict
    /// global-LRU guarantee.
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
    //! Focused unit tests for memory semantics and disk-worker failure recovery.
    //!
    //! Cross-process persistence, single-flight S3 integration, prefix
    //! invalidation, and capacity behavior are exercised more broadly in
    //! `tests/cache_tests.rs`. These local tests can access private fault
    //! switches and therefore protect RAII/reset and retry-loop invariants.

    use super::*;

    /// Verifies a memory miss becomes a shared-byte hit after insertion.
    ///
    /// This catches broken key lookup, lost insertion, or payload corruption in
    /// the hottest tier.
    #[test]
    fn test_memory_cache_get_insert() {
        let cache = MemoryCache::new(1024 * 1024);
        assert!(cache.get("key1").is_none());

        cache.insert("key1", Bytes::from_static(b"hello"));
        let data = cache.get("key1").unwrap();
        assert_eq!(data.as_ref(), b"hello");
    }

    /// Verifies same-key replacement exposes only the new bytes and rebalances size.
    ///
    /// Without the old-size subtraction, repeated promotion of one key would
    /// falsely grow capacity accounting and trigger unnecessary eviction.
    #[test]
    fn test_memory_cache_overwrite() {
        let cache = MemoryCache::new(1024 * 1024);
        cache.insert("key1", Bytes::from_static(b"v1"));
        cache.insert("key1", Bytes::from_static(b"v2"));

        let data = cache.get("key1").unwrap();
        assert_eq!(data.as_ref(), b"v2");
        assert_eq!(cache.total_size(), 2);
    }

    /// Verifies single-key invalidation removes bytes and their capacity charge.
    #[test]
    fn test_memory_cache_invalidate() {
        let cache = MemoryCache::new(1024 * 1024);
        cache.insert("key1", Bytes::from_static(b"hello"));
        cache.invalidate("key1");
        assert!(cache.get("key1").is_none());
        assert_eq!(cache.total_size(), 0);
    }

    /// Verifies prefix invalidation is selective across neighboring key families.
    ///
    /// The test protects namespace/segment cleanup from accidentally removing an
    /// object whose key does not share the requested prefix.
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

    /// Verifies insertion pressure synchronously restores the memory-size target.
    ///
    /// The exact approximate-LRU victim is not the property under test; bounded
    /// charged bytes are.
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

    /// Verifies a key pinned before capacity pressure survives sampled eviction.
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

    /// Verifies payload-byte accounting sums independent memory entries.
    #[test]
    fn test_memory_cache_total_size() {
        let cache = MemoryCache::new(1024 * 1024);
        cache.insert("k1", Bytes::from_static(b"abc"));
        cache.insert("k2", Bytes::from_static(b"defgh"));
        assert_eq!(cache.total_size(), 8);
    }

    /// Verifies decoded hits are type-checked and invalidation drops decoded state.
    ///
    /// This catches unsafe type confusion as well as a stale decoded value
    /// surviving raw-key invalidation.
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

    /// Verifies panic unwinding releases the single-worker flag for a later pass.
    ///
    /// The process-wide test lock serializes fault switches. The retained
    /// `TempDir` handle keeps the disk path alive while the restarted worker
    /// proves RAII cleanup made progress possible.
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

    /// Verifies persistent unlink errors use bounded retries instead of hot spinning.
    ///
    /// A current-thread runtime makes the 50 ms observation window sensitive to
    /// an accidental tight loop; at most three attempts are allowed before the
    /// configured backoff yields the executor.
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
