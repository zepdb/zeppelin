use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use tokio::sync::Mutex;

use crate::error::Result;
use crate::storage::ZeppelinStore;
use crate::wal::Manifest;

/// In-memory manifest cache with per-namespace TTL and singleflight.
///
/// Every query reads the manifest from S3 (~27ms). With a short TTL (500ms),
/// we can serve most queries from memory while keeping staleness bounded.
///
/// **Singleflight**: When the TTL expires and multiple queries arrive
/// simultaneously, only ONE fetch goes to S3 — the rest wait on the same
/// result. This prevents thundering herd on manifest reads (20+ concurrent
/// queries all fetching the same manifest from S3).
pub struct ManifestCache {
    entries: DashMap<String, CachedManifest>,
    ttl: Duration,
    /// Per-namespace mutex to coalesce concurrent fetches (singleflight).
    inflight: DashMap<String, Arc<Mutex<()>>>,
    /// Tracks the last invalidation time per namespace.
    /// Used to reject stale write-throughs that arrive after an invalidation.
    last_invalidated: DashMap<String, DateTime<Utc>>,
}

struct CachedManifest {
    manifest: Manifest,
    fetched_at: Instant,
}

impl ManifestCache {
    /// Create a new manifest cache with the given TTL.
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: DashMap::new(),
            ttl,
            inflight: DashMap::new(),
            last_invalidated: DashMap::new(),
        }
    }

    /// Get the manifest for a namespace, using the cache if fresh.
    ///
    /// Uses singleflight to coalesce concurrent fetches: if multiple queries
    /// hit an expired entry simultaneously, only one fetches from S3.
    ///
    /// Returns `Ok(Manifest::default())` if the namespace has no manifest on S3.
    pub async fn get(&self, store: &ZeppelinStore, namespace: &str) -> Result<Manifest> {
        // Fast path: check cache first (no lock)
        if let Some(entry) = self.entries.get(namespace) {
            if entry.fetched_at.elapsed() < self.ttl {
                return Ok(entry.manifest.clone());
            }
        }

        // Singleflight: acquire per-namespace mutex so only one fetch proceeds.
        let lock = self
            .inflight
            .entry(namespace.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .value()
            .clone();

        let _guard = lock.lock().await;

        // Re-check cache after acquiring lock — another task may have just fetched.
        if let Some(entry) = self.entries.get(namespace) {
            if entry.fetched_at.elapsed() < self.ttl {
                return Ok(entry.manifest.clone());
            }
        }

        // We won the race — fetch from S3.
        let manifest = Manifest::read(store, namespace).await?.unwrap_or_default();

        self.entries.insert(
            namespace.to_string(),
            CachedManifest {
                manifest: manifest.clone(),
                fetched_at: Instant::now(),
            },
        );

        Ok(manifest)
    }

    /// Insert a manifest directly into the cache (write-through).
    ///
    /// Called after WAL writes so the next query sees fresh data without
    /// an S3 roundtrip. Avoids the invalidate-then-refetch pattern.
    ///
    /// Rejects stale write-throughs: if the manifest's `updated_at` is at or
    /// before the last invalidation time for this namespace, the insert is
    /// silently dropped. This prevents a delayed write-through from overwriting
    /// a cache invalidation that was triggered by a more recent compaction.
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
                fetched_at: Instant::now(),
            },
        );
    }

    /// Invalidate the cached manifest for a namespace.
    ///
    /// Called after compaction to ensure the next read sees fresh data.
    /// Records the invalidation timestamp so that stale write-throughs
    /// arriving after this point are rejected by `insert()`.
    pub fn invalidate(&self, namespace: &str) {
        self.last_invalidated
            .insert(namespace.to_string(), Utc::now());
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
    use super::*;

    #[test]
    fn test_manifest_cache_new() {
        let cache = ManifestCache::new(Duration::from_millis(500));
        assert_eq!(cache.entries.len(), 0);
    }

    #[test]
    fn test_manifest_cache_invalidate_empty() {
        let cache = ManifestCache::new(Duration::from_millis(500));
        // Should not panic on non-existent key
        cache.invalidate("nonexistent");
    }

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
}
