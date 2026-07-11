//! Short-lived in-memory caching and strong freshness checks for manifests.
//!
//! Every query needs a namespace [`crate::wal::Manifest`] to discover which
//! immutable WAL
//! fragments and segments are visible. Reading that object from S3/MinIO on
//! every request is correct but expensive, so
//! [`crate::cache::manifest_cache::ManifestCache`] keeps cloned manifest
//! snapshots in process memory for a configured TTL. The cache is never
//! authoritative: object storage remains the source of truth, and strong reads
//! revalidate against it even while a TTL entry is fresh.
//!
//! Query execution enters through
//! [`ManifestCache::get`][crate::cache::manifest_cache::ManifestCache::get] for
//! bounded-staleness reads or
//! [`ManifestCache::get_strong`][crate::cache::manifest_cache::ManifestCache::get_strong]
//! for object-store-verified reads. WAL publication uses
//! [`ManifestCache::insert`][crate::cache::manifest_cache::ManifestCache::insert]
//! as a write-through optimization, while compaction and other visibility
//! changes call
//! [`ManifestCache::invalidate`][crate::cache::manifest_cache::ManifestCache::invalidate]
//! so a later read reloads authoritative state.
//!
//! ## Reading map
//!
//! 1. Start with [`crate::cache::manifest_cache::ManifestCache`] and
//!    `CachedManifest` for resident state.
//! 2. Read
//!    [`ManifestCache::get`][crate::cache::manifest_cache::ManifestCache::get]
//!    for TTL lookup and per-namespace singleflight.
//! 3. Read
//!    [`ManifestCache::get_strong`][crate::cache::manifest_cache::ManifestCache::get_strong]
//!    for ETag conditional verification.
//! 4. Read
//!    [`ManifestCache::insert`][crate::cache::manifest_cache::ManifestCache::insert]
//!    and
//!    [`ManifestCache::invalidate`][crate::cache::manifest_cache::ManifestCache::invalidate]
//!    together for write-through race protection.
//! 5. Finish with `fetch_and_cache` for the authoritative object-store read.
//!
//! ## Read and freshness flow
//!
//! ```text
//! bounded read (`get`)                 strong read (`get_strong`)
//!          |                                      |
//!          v                                      v
//! fresh TTL entry? -- yes --> clone       per-namespace singleflight lock
//!          | no                                   |
//!          v                                      v
//! per-namespace singleflight lock          verified after request began?
//!          |                                      | no
//!          v                                      v
//! recheck cache -> one S3/MinIO read        ETag conditional GET
//!          |                               /              \
//!          v                         unchanged            changed
//! clone cached manifest                refresh age      decode + replace
//! ```
//!
//! ## Invariants
//!
//! - Cache presence never makes a fragment or segment visible; only the
//!   manifest object in S3/MinIO is authoritative.
//! - At most one TTL miss or strong freshness check per namespace performs
//!   remote I/O at a time. Different namespaces do not share that lock.
//! - A strong read never accepts TTL age as proof of freshness.
//! - Write-through insertion never replaces a cached equal/higher
//!   `next_sequence` and rejects values timestamped at or before the latest
//!   invalidation.
//! - Storage, ETag, and decoding failures propagate; the cache does not silently
//!   reuse an entry when verification fails.
//!
//! TODO(doc): Verify whether
//! [`ManifestCache::invalidate`][crate::cache::manifest_cache::ManifestCache::invalidate]
//! must fence a `fetch_and_cache` operation that began before invalidation. The
//! invalidation timestamp currently rejects delayed
//! [`ManifestCache::insert`][crate::cache::manifest_cache::ManifestCache::insert]
//! calls, but a pre-existing remote fetch can still repopulate `entries` after
//! removal.
//!
//! ## Rust concepts used here
//!
//! [`DashMap`] provides sharded concurrent maps for independent namespaces.
//! Each singleflight value is an [`Arc`] containing a [`Mutex`]: cloning the
//! `Arc` shares one lock, and holding its async guard across the object-store
//! `.await` is intentional because it coalesces duplicate reads for that
//! namespace. Java might use a
//! `ConcurrentHashMap<String, CompletableFuture<_>>`; C would need explicit
//! lock ownership and lifetime rules. Rust ensures the guard and shared lock
//! remain alive until the awaited operation completes.
//!
//! [`Instant`] measures local TTL/freshness intervals and cannot move backward,
//! while `DateTime<Utc>` compares persisted manifest timestamps with
//! invalidation time. Cached manifests are cloned into owned return values, so
//! callers never retain a map guard or borrow cache storage across later work.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use tokio::sync::Mutex;

use crate::error::Result;
use crate::storage::ZeppelinStore;
use crate::wal::{Manifest, ManifestVersion};

/// Process-local manifest snapshots with per-namespace TTL and singleflight.
///
/// A normal read may serve a snapshot until its TTL expires. A strong read
/// always revalidates against S3/MinIO, using an ETag conditional GET when the
/// cache knows the object's version. The value is an optimization only; it
/// cannot publish or supersede remote manifest state.
///
/// # Concurrency
///
/// A namespace-specific async mutex serializes cache-miss and strong-refresh
/// I/O. Concurrent readers for other namespaces proceed independently. The
/// `inflight` and invalidation maps retain one entry per namespace observed by
/// this process; there is no namespace-lock eviction in this type.
///
/// # Examples
///
/// Twenty queries arriving after namespace `catalog` expires share one remote
/// manifest read. A simultaneous query for `inventory` uses a different lock.
pub struct ManifestCache {
    /// Cached snapshot and freshness metadata keyed by namespace.
    entries: DashMap<String, CachedManifest>,
    /// Maximum age accepted by bounded-staleness [`Self::get`] calls.
    ttl: Duration,
    /// Per-namespace async mutexes that coalesce remote reads.
    inflight: DashMap<String, Arc<Mutex<()>>>,
    /// Wall-clock invalidation time used to reject delayed write-throughs.
    last_invalidated: DashMap<String, DateTime<Utc>>,
}

/// One owned manifest snapshot and the evidence supporting its freshness.
struct CachedManifest {
    /// Cloned manifest returned to readers without retaining a map guard.
    manifest: Manifest,
    /// Object-store ETag when the snapshot came from a versioned read.
    ///
    /// Write-through entries carry `None` until a strong/full read verifies
    /// their remote version.
    version: ManifestVersion,
    /// Local time at which TTL age was last established or refreshed.
    fetched_at: Instant,
    /// Local time of the latest successful object-store verification.
    ///
    /// `None` distinguishes an unverified write-through from a remote read.
    verified_at: Option<Instant>,
}

impl ManifestCache {
    /// Creates an empty manifest cache with a bounded-read TTL.
    ///
    /// # Parameters
    ///
    /// - `ttl`: Maximum local age accepted by [`Self::get`]. `Duration::ZERO`
    ///   forces every bounded read through singleflight and object storage.
    ///
    /// # Returns
    ///
    /// A cache with no namespace snapshots, locks, or invalidation timestamps.
    ///
    /// # Examples
    ///
    /// A 500 ms TTL lets queries arriving shortly after one remote read reuse
    /// its snapshot. Strong reads still verify S3/MinIO regardless of this value.
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: DashMap::new(),
            ttl,
            inflight: DashMap::new(),
            last_invalidated: DashMap::new(),
        }
    }

    /// Returns the shared singleflight mutex for one namespace.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Logical namespace whose remote manifest reads should be
    ///   coalesced.
    ///
    /// # Returns
    ///
    /// A cloned [`Arc`] pointing at the namespace's async mutex. The lock entry
    /// remains in `inflight` after callers release their guards.
    ///
    /// # Examples
    ///
    /// Two concurrent `catalog` misses receive clones of the same mutex, while
    /// a miss for `inventory` receives a different mutex.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Cloning an `Arc` increments a reference count; it does not copy the
    /// mutex. Rust guarantees the allocation outlives every cloned handle.
    fn inflight_lock(&self, namespace: &str) -> Arc<Mutex<()>> {
        self.inflight
            .entry(namespace.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .value()
            .clone()
    }

    /// Clones the cached manifest and version for a strong-read decision.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace whose resident entry should be inspected.
    ///
    /// # Returns
    ///
    /// `Some((manifest, version))` with owned clones when an entry exists, or
    /// `None` for a cold/invalidated namespace.
    ///
    /// # Examples
    ///
    /// A write-through entry returns a manifest paired with
    /// `ManifestVersion(None)`, causing strong reads to perform a full fetch.
    fn cached_manifest(&self, namespace: &str) -> Option<(Manifest, ManifestVersion)> {
        self.entries
            .get(namespace)
            .map(|entry| (entry.manifest.clone(), entry.version.clone()))
    }

    /// Reuses a manifest verified after a waiting strong reader began.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace whose verification time should be checked.
    /// - `since`: Start time captured before waiting for the singleflight lock.
    ///
    /// # Returns
    ///
    /// An owned manifest when another task verified it at or after `since`;
    /// otherwise `None`.
    ///
    /// # Examples
    ///
    /// Reader B begins while reader A owns the lock. If A completes a
    /// conditional GET before B acquires it, B clones A's verified result and
    /// does not issue another request.
    fn cached_verified_since(&self, namespace: &str, since: Instant) -> Option<Manifest> {
        self.entries.get(namespace).and_then(|entry| {
            entry
                .verified_at
                .filter(|verified_at| *verified_at >= since)
                .map(|_| entry.manifest.clone())
        })
    }

    /// Reads the authoritative manifest and replaces the resident snapshot.
    ///
    /// A missing manifest object denotes a namespace with no published WAL or
    /// segment state and is represented by [`Manifest::default`]. This is a
    /// domain state, not a swallowed storage error.
    ///
    /// # Parameters
    ///
    /// - `store`: Object-store boundary used for the versioned manifest read.
    /// - `namespace`: Namespace whose manifest key should be read.
    ///
    /// # Returns
    ///
    /// An owned clone of the fetched/default manifest. The cache retains a
    /// separate owned clone with its ETag and verification timestamps.
    ///
    /// # Errors
    ///
    /// Propagates object-store and manifest decoding errors. On failure, this
    /// method does not replace the existing cache entry.
    ///
    /// # Side Effects
    ///
    /// Performs one versioned manifest read and replaces `entries[namespace]`
    /// after successful decoding.
    ///
    /// # Consistency
    ///
    /// Remote state is authoritative. A successful fetch marks both TTL age and
    /// strong verification at the same local [`Instant`].
    ///
    /// # Examples
    ///
    /// Reading a published version 12 stores its ETag and returns version 12.
    /// Reading a new namespace with no manifest stores and returns an empty
    /// default manifest. A malformed object returns an error instead.
    async fn fetch_and_cache(&self, store: &ZeppelinStore, namespace: &str) -> Result<Manifest> {
        let (manifest, version) = Manifest::read_versioned(store, namespace)
            .await?
            .unwrap_or_else(|| (Manifest::default(), ManifestVersion(None)));
        let now = Instant::now();

        self.entries.insert(
            namespace.to_string(),
            CachedManifest {
                manifest: manifest.clone(),
                version,
                fetched_at: now,
                verified_at: Some(now),
            },
        );

        Ok(manifest)
    }

    /// Returns a bounded-staleness manifest, coalescing an expired miss.
    ///
    /// The lock-free fast path clones an entry younger than the configured TTL.
    /// On a miss or expiry, the method acquires the namespace mutex, rechecks
    /// the cache because another waiter may have refreshed it, then performs one
    /// authoritative read.
    ///
    /// # Parameters
    ///
    /// - `store`: Object-store boundary used only after a cold/expired recheck.
    /// - `namespace`: Namespace whose visibility snapshot is requested.
    ///
    /// # Returns
    ///
    /// An owned manifest clone. A namespace with no remote manifest returns
    /// [`Manifest::default`].
    ///
    /// # Errors
    ///
    /// Returns an error when a required remote read or decode fails. An expired
    /// cached value is not used as a fallback.
    ///
    /// # Side Effects
    ///
    /// On a miss, may add a namespace mutex and performs one versioned GET for
    /// all concurrent waiters, then replaces the cache entry.
    ///
    /// # Consistency
    ///
    /// This is a bounded-staleness API: a fresh entry may lag S3/MinIO until its
    /// TTL expires or a writer invalidates/replaces it. Use [`Self::get_strong`]
    /// when a query must verify remote freshness.
    ///
    /// # Performance
    ///
    /// A hit performs one concurrent-map lookup and clones the manifest. A cold
    /// namespace performs one remote read; waiters serialize behind the same
    /// namespace lock but do not duplicate I/O.
    ///
    /// # Examples
    ///
    /// If version 12 was fetched 100 ms ago under a 500 ms TTL, this returns its
    /// clone immediately. At 600 ms, the first caller refreshes from S3 and the
    /// rest reuse that refresh.
    pub async fn get(&self, store: &ZeppelinStore, namespace: &str) -> Result<Manifest> {
        // Fast path: check cache first (no lock)
        if let Some(entry) = self.entries.get(namespace) {
            if entry.fetched_at.elapsed() < self.ttl {
                return Ok(entry.manifest.clone());
            }
        }

        // Singleflight: acquire per-namespace mutex so only one fetch proceeds.
        let lock = self.inflight_lock(namespace);

        let _guard = lock.lock().await;

        // Re-check cache after acquiring lock — another task may have just fetched.
        if let Some(entry) = self.entries.get(namespace) {
            if entry.fetched_at.elapsed() < self.ttl {
                return Ok(entry.manifest.clone());
            }
        }

        // We won the race — fetch from S3.
        self.fetch_and_cache(store, namespace).await
    }

    /// Returns a manifest whose freshness was verified against object storage.
    ///
    /// Strong reads never trust TTL age. A cached ETag enables
    /// `If-None-Match`: an unchanged response refreshes local timestamps, while
    /// returned bytes are decoded and replace the entry. A write-through entry
    /// has no ETag and therefore requires a full versioned read.
    ///
    /// # Parameters
    ///
    /// - `store`: Object-store boundary for conditional or full reads.
    /// - `namespace`: Namespace whose manifest must be verified.
    ///
    /// # Returns
    ///
    /// An owned manifest verified no earlier than this request began, or reused
    /// from a concurrent verification completed while this task waited.
    ///
    /// # Errors
    ///
    /// Propagates conditional/full GET and decode errors. The method never
    /// treats verification failure as permission to serve an unverified entry.
    ///
    /// # Side Effects
    ///
    /// Performs at most one remote verification for concurrent strong readers
    /// of a namespace and updates verification/TTL timestamps or replaces the
    /// resident snapshot.
    ///
    /// # Consistency
    ///
    /// A `None` conditional response means the ETag still matches and the
    /// cached bytes remain authoritative for that version. Returned bytes mean
    /// the object changed and must decode successfully before replacement.
    ///
    /// # Performance
    ///
    /// Requires remote verification unless a concurrent reader already
    /// completed one after this request started. An ETag match avoids
    /// transferring the manifest body.
    ///
    /// # Examples
    ///
    /// If cached version 12 still matches S3, the conditional request returns no
    /// body and this method refreshes version 12's age. If version 13 exists, it
    /// decodes and caches version 13. A delayed second reader reuses that check.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The async mutex guard intentionally lives across `.await`; that normally
    /// deserves scrutiny, but here it is the singleflight mechanism. The guard
    /// protects only one namespace and is released automatically by RAII on
    /// success, error, or early return.
    pub async fn get_strong(&self, store: &ZeppelinStore, namespace: &str) -> Result<Manifest> {
        let requested_at = Instant::now();
        let lock = self.inflight_lock(namespace);
        let _guard = lock.lock().await;

        if let Some(manifest) = self.cached_verified_since(namespace, requested_at) {
            return Ok(manifest);
        }

        let Some((_, cached_version)) = self.cached_manifest(namespace) else {
            return self.fetch_and_cache(store, namespace).await;
        };

        let Some(etag) = cached_version.0 else {
            return self.fetch_and_cache(store, namespace).await;
        };

        let key = Manifest::s3_key(namespace);
        match store.get_if_none_match(&key, &etag).await? {
            Some((data, next_etag)) => {
                let manifest = Manifest::from_bytes_for_namespace(&data, namespace)?;
                let now = Instant::now();
                self.entries.insert(
                    namespace.to_string(),
                    CachedManifest {
                        manifest: manifest.clone(),
                        version: ManifestVersion(next_etag),
                        fetched_at: now,
                        verified_at: Some(now),
                    },
                );
                Ok(manifest)
            }
            None => {
                let now = Instant::now();
                if let Some(mut entry) = self.entries.get_mut(namespace) {
                    entry.fetched_at = now;
                    entry.verified_at = Some(now);
                    Ok(entry.manifest.clone())
                } else {
                    self.fetch_and_cache(store, namespace).await
                }
            }
        }
    }

    /// Inserts a newly published manifest as an unverified write-through.
    ///
    /// WAL publication calls this after remote success so nearby bounded reads
    /// see the new sequence without another object-store roundtrip. Because the
    /// ETag is not supplied, a later strong read performs a full versioned GET.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace whose entry may be advanced.
    /// - `manifest`: Owned published snapshot. Its `updated_at` must be later
    ///   than the last invalidation and its `next_sequence` must exceed the
    ///   currently cached sequence.
    ///
    /// # Returns
    ///
    /// Returns unit. A stale/equal candidate is deliberately ignored; callers
    /// can rely on the remote publication result rather than cache acceptance.
    ///
    /// # Side Effects
    ///
    /// Replaces the in-memory entry when both ordering guards pass. It performs
    /// no object-store I/O.
    ///
    /// # Consistency
    ///
    /// Timestamp fencing prevents a delayed WAL write-through from undoing a
    /// newer invalidation, and sequence ordering prevents version N from
    /// replacing cached N+1. The entry is marked unverified because it has no
    /// associated ETag.
    ///
    /// # Examples
    ///
    /// If sequence 4 is cached, a delayed sequence 3 or duplicate sequence 4 is
    /// ignored. Sequence 5 is accepted unless its timestamp predates the latest
    /// compaction invalidation.
    pub fn insert(&self, namespace: &str, manifest: Manifest) {
        // Reject if older than last invalidation
        if let Some(inv_time) = self.last_invalidated.get(namespace) {
            if manifest.updated_at <= *inv_time {
                return;
            }
        }
        // Reject if older than current cached version (prevents write-through races
        // where a slow write-through inserts version N after version N+1 is already cached)
        if let Some(entry) = self.entries.get(namespace) {
            if manifest.next_sequence <= entry.manifest.next_sequence {
                return;
            }
        }
        self.entries.insert(
            namespace.to_string(),
            CachedManifest {
                manifest,
                version: ManifestVersion(None),
                fetched_at: Instant::now(),
                verified_at: None,
            },
        );
    }

    /// Removes one namespace snapshot using the system-clock fence time.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace whose resident snapshot must no longer satisfy
    ///   a bounded read.
    ///
    /// # Returns
    ///
    /// Returns unit whether or not an entry existed.
    ///
    /// # Side Effects
    ///
    /// Delegates to [`Self::invalidate_at`] with the current UTC time. It does
    /// not remove the namespace's singleflight mutex or cancel an active read.
    ///
    /// # Consistency
    ///
    /// Later [`Self::insert`] calls with `updated_at` at or before this instant
    /// are ignored. The next ordinary read misses the removed snapshot and
    /// reloads S3/MinIO.
    ///
    /// # Examples
    ///
    /// After compaction publishes a new segment, invalidating `catalog` forces
    /// the next query to discover that manifest instead of serving the old TTL
    /// entry. Invalidating an already-cold namespace is harmless.
    pub fn invalidate(&self, namespace: &str) {
        self.invalidate_at(namespace, Utc::now());
    }

    /// Removes one namespace snapshot and fences older write-throughs.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace whose resident snapshot must no longer satisfy
    ///   a bounded read.
    /// - `invalidated_at`: Wall-clock timestamp supplied by the caller's shared
    ///   correctness clock.
    ///
    /// # Side Effects
    ///
    /// Advances the namespace's invalidation fence and removes its resident
    /// entry. A backward wall-clock jump cannot lower an existing fence.
    ///
    /// # Consistency
    ///
    /// Callers that stamp manifests with an injected clock must pass that same
    /// clock here. Otherwise a future-stamped delayed write-through could appear
    /// newer than a host-clock invalidation and repopulate stale cache state.
    pub fn invalidate_at(&self, namespace: &str, invalidated_at: DateTime<Utc>) {
        self.last_invalidated
            .entry(namespace.to_string())
            .and_modify(|current| {
                if invalidated_at > *current {
                    *current = invalidated_at;
                }
            })
            .or_insert(invalidated_at);
        self.entries.remove(namespace);
    }
}

#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]
#[cfg(test)]
mod tests {
    //! Unit tests for local TTL, invalidation, and sequence-ordering behavior.

    use super::*;

    /// Creates an empty cache without allocating namespace state eagerly.
    #[test]
    fn test_manifest_cache_new() {
        let cache = ManifestCache::new(Duration::from_millis(500));
        assert_eq!(cache.entries.len(), 0);
    }

    /// Makes invalidation idempotent for a namespace with no resident entry.
    #[test]
    fn test_manifest_cache_invalidate_empty() {
        let cache = ManifestCache::new(Duration::from_millis(500));
        // Should not panic on non-existent key
        cache.invalidate("nonexistent");
    }

    /// Serves a fresh write-through without consulting the supplied store.
    ///
    /// The in-memory store has no manifest, so a remote read would change the
    /// observed value and expose a regression in the fast path.
    #[tokio::test]
    async fn test_manifest_cache_singleflight_insert_and_get() {
        let cache = ManifestCache::new(Duration::from_millis(500));
        let manifest = Manifest::default();
        cache.insert("test_ns", manifest.clone());

        // Create a dummy store — won't be used since cache is fresh.
        let mem = std::sync::Arc::new(object_store::memory::InMemory::new());
        let store = crate::storage::ZeppelinStore::new(mem);
        let result = cache.get(&store, "test_ns").await.unwrap();
        assert_eq!(result.fragments.len(), manifest.fragments.len());
    }

    /// Prevents a delayed lower sequence from replacing newer cached state.
    #[test]
    fn test_insert_rejects_older_version() {
        let cache = ManifestCache::new(Duration::from_millis(500));

        // Insert manifest v3
        let mut v3 = Manifest::default();
        v3.next_sequence = 3;
        cache.insert("ns", v3);

        // Try to insert manifest v2 (older) — should be rejected
        let mut v2 = Manifest::default();
        v2.next_sequence = 2;
        cache.insert("ns", v2);

        // Cache should still hold v3
        let entry = cache.entries.get("ns").unwrap();
        assert_eq!(entry.manifest.next_sequence, 3);
    }

    /// Allows a strictly higher sequence to advance the write-through entry.
    #[test]
    fn test_insert_accepts_newer_version() {
        let cache = ManifestCache::new(Duration::from_millis(500));

        // Insert manifest v3
        let mut v3 = Manifest::default();
        v3.next_sequence = 3;
        cache.insert("ns", v3);

        // Insert manifest v4 (newer) — should be accepted
        let mut v4 = Manifest::default();
        v4.next_sequence = 4;
        cache.insert("ns", v4);

        let entry = cache.entries.get("ns").unwrap();
        assert_eq!(entry.manifest.next_sequence, 4);
    }

    /// Rejects equal sequences so a duplicate cannot replace cached contents.
    #[test]
    fn test_insert_rejects_equal_version() {
        let cache = ManifestCache::new(Duration::from_millis(500));

        // Insert manifest v3
        let mut v3 = Manifest::default();
        v3.next_sequence = 3;
        cache.insert("ns", v3);

        // Insert another manifest with same sequence — should be rejected (not newer)
        let mut v3_dup = Manifest::default();
        v3_dup.next_sequence = 3;
        cache.insert("ns", v3_dup);

        let entry = cache.entries.get("ns").unwrap();
        assert_eq!(entry.manifest.next_sequence, 3);
    }

    /// Fences a write-through whose timestamp predates an invalidation.
    #[test]
    fn test_insert_rejects_stale_after_invalidation() {
        let cache = ManifestCache::new(Duration::from_millis(500));

        // Insert then invalidate
        let mut v3 = Manifest::default();
        v3.next_sequence = 3;
        cache.insert("ns", v3);
        cache.invalidate("ns");

        // Insert with updated_at before invalidation — should be rejected
        let mut stale = Manifest::default();
        stale.next_sequence = 4;
        stale.updated_at = chrono::Utc::now() - chrono::Duration::seconds(10);
        cache.insert("ns", stale);

        assert!(cache.entries.get("ns").is_none());
    }

    /// Uses the injected invalidation stamp to reject a delayed write-through.
    #[test]
    fn test_insert_rejects_manifest_older_than_injected_invalidation() {
        let cache = ManifestCache::new(Duration::from_millis(500));
        let host_now = Utc::now();
        let invalidated_at = host_now + chrono::Duration::hours(2);
        cache.invalidate_at("ns", invalidated_at);

        let mut delayed = Manifest::default();
        delayed.next_sequence = 1;
        delayed.updated_at = host_now + chrono::Duration::hours(1);
        cache.insert("ns", delayed);

        assert!(cache.entries.get("ns").is_none());
    }
}
