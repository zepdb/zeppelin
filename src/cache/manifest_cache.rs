use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::error::Result;
use crate::storage::ZeppelinStore;
use crate::wal::Manifest;

/// In-memory manifest cache with per-namespace TTL.
///
/// Every query reads the manifest from S3 (~27ms). With a short TTL (500ms),
/// we can serve most queries from memory while keeping staleness bounded.
/// The cache is automatically invalidated when the TTL expires.
pub struct ManifestCache {
    entries: DashMap<String, CachedManifest>,
    ttl: Duration,
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
        }
    }

    /// Get the manifest for a namespace, using the cache if fresh.
    ///
    /// Returns `Ok(Manifest::default())` if the namespace has no manifest on S3.
    pub async fn get(&self, store: &ZeppelinStore, namespace: &str) -> Result<Manifest> {
        // Check cache first
        if let Some(entry) = self.entries.get(namespace) {
            if entry.fetched_at.elapsed() < self.ttl {
                return Ok(entry.manifest.clone());
            }
        }

        // Cache miss or expired — fetch from S3
        let manifest = Manifest::read(store, namespace)
            .await?
            .unwrap_or_default();

        self.entries.insert(
            namespace.to_string(),
            CachedManifest {
                manifest: manifest.clone(),
                fetched_at: Instant::now(),
            },
        );

        Ok(manifest)
    }

    /// Invalidate the cached manifest for a namespace.
    ///
    /// Called after writes or compaction to ensure the next read sees fresh data.
    pub fn invalidate(&self, namespace: &str) {
        self.entries.remove(namespace);
    }
}

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
}
