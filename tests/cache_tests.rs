mod common;

use bytes::Bytes;
use common::counting::{counting_store, ArtifactClass};
use common::harness::TestHarness;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::Barrier;

use zeppelin::cache::{DiskCache, MemoryCache};
use zeppelin::config::{IndexingConfig, DEFAULT_RERANK_COALESCE_GAP_BYTES};
use zeppelin::error::ZeppelinError;
use zeppelin::index::ivf_flat::search::search_ivf_flat;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::index::{IvfFlatIndex, VectorIndex};
use zeppelin::types::{AttributeValue, DistanceMetric, VectorEntry};

/// Create a test cache with a given max size in bytes.
fn test_cache(dir: &Path, max_bytes: u64) -> DiskCache {
    DiskCache::new_with_max_bytes(dir.to_path_buf(), max_bytes).unwrap()
}

async fn wait_for_cache_size_at_most(cache: &DiskCache, max_bytes: u64) {
    for _ in 0..100 {
        if cache.total_size() <= max_bytes {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "cache size stayed above max: size={} max={}",
        cache.total_size(),
        max_bytes
    );
}

fn single_cluster_config() -> IndexingConfig {
    IndexingConfig {
        default_num_centroids: 1,
        kmeans_max_iterations: 4,
        quantization: QuantizationType::None,
        bitmap_index: false,
        ..Default::default()
    }
}

fn single_cluster_vectors(prefix: &str, count: usize) -> Vec<VectorEntry> {
    (0..count)
        .map(|i| {
            let mut attributes = HashMap::new();
            attributes.insert(
                "tenant".to_string(),
                AttributeValue::String(format!("{prefix}_tenant")),
            );
            VectorEntry {
                id: format!("{prefix}_{i}"),
                values: vec![i as f32 * 0.001, (i % 11) as f32 * 0.01, 1.0, 0.5],
                attributes: Some(attributes),
            }
        })
        .collect()
}

#[tokio::test]
async fn test_cache_put_and_get() {
    let dir = TempDir::new().unwrap();
    let cache = test_cache(dir.path(), 1024 * 1024);

    cache.put("k1", &Bytes::from("hello")).await.unwrap();

    let result = cache.get("k1").await;
    assert_eq!(result, Some(Bytes::from("hello")));
}

#[tokio::test]
async fn test_cache_miss_returns_none() {
    let dir = TempDir::new().unwrap();
    let cache = test_cache(dir.path(), 1024 * 1024);

    let result = cache.get("nonexistent").await;
    assert_eq!(result, None);
}

#[tokio::test]
async fn test_cache_get_or_fetch() {
    let dir = TempDir::new().unwrap();
    let cache = test_cache(dir.path(), 1024 * 1024);

    // First call should trigger the fetch function
    let fetched = cache
        .get_or_fetch("k1", || async { Ok(Bytes::from("fetched_value")) })
        .await
        .unwrap();
    assert_eq!(fetched, Bytes::from("fetched_value"));

    // Second call should hit cache (not call fetch again)
    let cached = cache.get("k1").await;
    assert_eq!(cached, Some(Bytes::from("fetched_value")));
}

#[tokio::test]
async fn test_cache_eviction_lru() {
    let dir = TempDir::new().unwrap();
    // Max 100 bytes
    let cache = test_cache(dir.path(), 100);

    // Put k1 (50 bytes)
    cache.put("k1", &Bytes::from(vec![b'a'; 50])).await.unwrap();

    // Put k2 (50 bytes) — total now 100, at limit
    cache.put("k2", &Bytes::from(vec![b'b'; 50])).await.unwrap();

    // Put k3 (50 bytes) — total would be 150, so evict oldest (k1)
    cache.put("k3", &Bytes::from(vec![b'c'; 50])).await.unwrap();
    wait_for_cache_size_at_most(&cache, 100).await;

    // k1 should be evicted
    assert_eq!(cache.get("k1").await, None);
    // k2 and k3 should still be present
    assert!(cache.get("k2").await.is_some());
    assert!(cache.get("k3").await.is_some());
}

#[tokio::test]
async fn test_cache_size_tracking() {
    let dir = TempDir::new().unwrap();
    let cache = test_cache(dir.path(), 1024 * 1024);

    let d1 = Bytes::from(vec![b'a'; 100]);
    let d2 = Bytes::from(vec![b'b'; 200]);
    let d3 = Bytes::from(vec![b'c'; 300]);

    cache.put("k1", &d1).await.unwrap();
    cache.put("k2", &d2).await.unwrap();
    cache.put("k3", &d3).await.unwrap();

    assert_eq!(cache.total_size(), 600);
}

#[tokio::test]
async fn test_cache_pin_survives_eviction() {
    let dir = TempDir::new().unwrap();
    // Small max — 100 bytes
    let cache = test_cache(dir.path(), 100);

    // Pin "centroids" (40 bytes)
    cache
        .put("centroids", &Bytes::from(vec![b'C'; 40]))
        .await
        .unwrap();
    cache.pin("centroids").await;

    // Put unpinned data (40 bytes)
    cache
        .put("data1", &Bytes::from(vec![b'D'; 40]))
        .await
        .unwrap();

    // Put more data (40 bytes) — would exceed 100, should evict unpinned "data1"
    cache
        .put("data2", &Bytes::from(vec![b'E'; 40]))
        .await
        .unwrap();
    wait_for_cache_size_at_most(&cache, 100).await;

    // Pinned "centroids" should survive
    assert!(cache.get("centroids").await.is_some());
    // "data1" should be evicted (it's the oldest unpinned entry)
    assert_eq!(cache.get("data1").await, None);
    // "data2" should be present
    assert!(cache.get("data2").await.is_some());
}

#[tokio::test]
async fn test_cache_pin_scoped_rotates_on_new_key() {
    let dir = TempDir::new().unwrap();
    let cache = test_cache(dir.path(), 100);

    // Pin seg1 centroids under the namespace scope.
    cache
        .put("ns/segments/seg1/centroids.bin", &Bytes::from(vec![1; 30]))
        .await
        .unwrap();
    cache
        .pin_scoped("ns", "ns/segments/seg1/centroids.bin")
        .await;
    assert!(cache.is_pinned("ns/segments/seg1/centroids.bin").await);

    // Re-pinning the same key is a no-op.
    cache
        .pin_scoped("ns", "ns/segments/seg1/centroids.bin")
        .await;
    assert!(cache.is_pinned("ns/segments/seg1/centroids.bin").await);

    // Segment rotation: pinning seg2 under the same scope unpins seg1.
    cache
        .put("ns/segments/seg2/centroids.bin", &Bytes::from(vec![2; 30]))
        .await
        .unwrap();
    cache
        .pin_scoped("ns", "ns/segments/seg2/centroids.bin")
        .await;
    assert!(cache.is_pinned("ns/segments/seg2/centroids.bin").await);
    assert!(
        !cache.is_pinned("ns/segments/seg1/centroids.bin").await,
        "old segment's key must be unpinned when the scope rotates"
    );

    // Different scope does not disturb this scope's pin.
    cache
        .pin_scoped("other", "other/segments/segX/centroids.bin")
        .await;
    assert!(cache.is_pinned("ns/segments/seg2/centroids.bin").await);
}

#[tokio::test]
async fn test_cache_invalidate() {
    let dir = TempDir::new().unwrap();
    let cache = test_cache(dir.path(), 1024 * 1024);

    let data = Bytes::from(vec![b'x'; 100]);
    cache.put("k1", &data).await.unwrap();
    assert_eq!(cache.total_size(), 100);

    cache.invalidate("k1").await.unwrap();

    assert_eq!(cache.get("k1").await, None);
    assert_eq!(cache.total_size(), 0);
}

#[tokio::test]
async fn test_cache_invalidate_prefix() {
    let dir = TempDir::new().unwrap();
    let cache = test_cache(dir.path(), 1024 * 1024);

    cache
        .put("seg_001/a", &Bytes::from("data_a"))
        .await
        .unwrap();
    cache
        .put("seg_001/b", &Bytes::from("data_b"))
        .await
        .unwrap();
    cache
        .put("seg_002/a", &Bytes::from("data_c"))
        .await
        .unwrap();

    cache.invalidate_prefix("seg_001/").await.unwrap();

    // seg_001 entries should be gone
    assert_eq!(cache.get("seg_001/a").await, None);
    assert_eq!(cache.get("seg_001/b").await, None);
    // seg_002 should still be present
    assert!(cache.get("seg_002/a").await.is_some());
}

#[tokio::test]
async fn test_cache_concurrent_access() {
    let dir = TempDir::new().unwrap();
    let cache = std::sync::Arc::new(test_cache(dir.path(), 1024 * 1024));

    let mut handles = vec![];
    for i in 0..10 {
        let cache = cache.clone();
        handles.push(tokio::spawn(async move {
            let key = format!("key_{i}");
            let value = Bytes::from(format!("value_{i}"));
            cache.put(&key, &value).await.unwrap();
            let got = cache.get(&key).await;
            assert_eq!(got, Some(value));
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // All 10 keys should be readable
    for i in 0..10 {
        let key = format!("key_{i}");
        assert!(cache.get(&key).await.is_some(), "key {key} missing");
    }
}

#[tokio::test]
async fn test_cache_persists_across_instances() {
    let dir = TempDir::new().unwrap();

    // First instance: put k1
    {
        let cache = test_cache(dir.path(), 1024 * 1024);
        cache
            .put("k1", &Bytes::from("persistent_data"))
            .await
            .unwrap();
    }

    // Second instance: should find k1
    {
        let cache = test_cache(dir.path(), 1024 * 1024);
        let result = cache.get("k1").await;
        assert_eq!(result, Some(Bytes::from("persistent_data")));
    }
}

#[tokio::test]
async fn test_cache_get_or_fetch_error_propagates() {
    let dir = TempDir::new().unwrap();
    let cache = test_cache(dir.path(), 1024 * 1024);

    let result = cache
        .get_or_fetch("error_key", || async {
            Err(ZeppelinError::Cache("simulated fetch error".into()))
        })
        .await;

    assert!(result.is_err(), "error from fetch should propagate");
    // Cache should not be populated
    assert_eq!(cache.get("error_key").await, None);
}

#[tokio::test]
async fn test_cache_concurrent_get_or_fetch() {
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(test_cache(dir.path(), 1024 * 1024));

    let fetch_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut handles = vec![];
    for _ in 0..10 {
        let cache = cache.clone();
        let fetch_count = fetch_count.clone();
        handles.push(tokio::spawn(async move {
            cache
                .get_or_fetch("shared_key", || {
                    let fc = fetch_count.clone();
                    async move {
                        fc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        // Small delay to simulate network fetch
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        Ok(Bytes::from("shared_value"))
                    }
                })
                .await
                .unwrap()
        }));
    }

    let mut results = vec![];
    for handle in handles {
        results.push(handle.await.unwrap());
    }

    // All results should be the same value
    for result in &results {
        assert_eq!(result, &Bytes::from("shared_value"));
    }

    // The value should be in the cache
    assert_eq!(
        cache.get("shared_key").await,
        Some(Bytes::from("shared_value"))
    );
}

#[tokio::test]
async fn test_cache_get_or_fetch_singleflight_coalesces_concurrent_misses() {
    const CONCURRENCY: usize = 32;

    let dir = TempDir::new().unwrap();
    let cache = Arc::new(test_cache(dir.path(), 1024 * 1024));
    let fetch_count = Arc::new(AtomicUsize::new(0));
    let start = Arc::new(Barrier::new(CONCURRENCY + 1));

    let mut handles = Vec::with_capacity(CONCURRENCY);
    for _ in 0..CONCURRENCY {
        let cache = Arc::clone(&cache);
        let fetch_count = Arc::clone(&fetch_count);
        let start = Arc::clone(&start);
        handles.push(tokio::spawn(async move {
            start.wait().await;
            cache
                .get_or_fetch("singleflight_key", || {
                    let fetch_count = Arc::clone(&fetch_count);
                    async move {
                        fetch_count.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok(Bytes::from_static(b"coalesced"))
                    }
                })
                .await
                .unwrap()
        }));
    }

    start.wait().await;
    let mut results = Vec::with_capacity(CONCURRENCY);
    for handle in handles {
        results.push(handle.await.unwrap());
    }

    assert!(results.iter().all(|result| result.as_ref() == b"coalesced"));
    assert_eq!(
        fetch_count.load(Ordering::SeqCst),
        1,
        "concurrent cache misses on one key must share one backend fetch"
    );
}

#[tokio::test]
async fn test_cache_get_or_fetch_retry_after_failed_fetch_is_not_poisoned() {
    const CONCURRENCY: usize = 8;

    let dir = TempDir::new().unwrap();
    let cache = Arc::new(test_cache(dir.path(), 1024 * 1024));
    let attempts = Arc::new(AtomicUsize::new(0));

    let first = cache
        .get_or_fetch("retry_key", || {
            let attempts = Arc::clone(&attempts);
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(ZeppelinError::Cache("first fetch failed".into()))
            }
        })
        .await;
    assert!(first.is_err());
    assert_eq!(cache.get("retry_key").await, None);

    let start = Arc::new(Barrier::new(CONCURRENCY + 1));
    let mut handles = Vec::with_capacity(CONCURRENCY);
    for _ in 0..CONCURRENCY {
        let cache = Arc::clone(&cache);
        let attempts = Arc::clone(&attempts);
        let start = Arc::clone(&start);
        handles.push(tokio::spawn(async move {
            start.wait().await;
            cache
                .get_or_fetch("retry_key", || {
                    let attempts = Arc::clone(&attempts);
                    async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        Ok(Bytes::from_static(b"eventual bytes"))
                    }
                })
                .await
                .unwrap()
        }));
    }

    start.wait().await;
    for handle in handles {
        assert_eq!(handle.await.unwrap(), Bytes::from_static(b"eventual bytes"));
    }
    assert_eq!(
        cache.get("retry_key").await,
        Some(Bytes::from_static(b"eventual bytes"))
    );
}

#[cfg(unix)]
#[tokio::test]
async fn test_cache_get_or_fetch_returns_fetched_bytes_when_cache_write_fails() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new().unwrap();
    let cache = test_cache(dir.path(), 1024 * 1024);
    let original_permissions = std::fs::metadata(dir.path()).unwrap().permissions();

    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o500)).unwrap();
    let result = cache
        .get_or_fetch("readonly_key", || async {
            Ok(Bytes::from_static(b"uncached but returned"))
        })
        .await;
    std::fs::set_permissions(dir.path(), original_permissions).unwrap();

    assert_eq!(
        result.unwrap(),
        Bytes::from_static(b"uncached but returned")
    );
    assert_eq!(cache.get("readonly_key").await, None);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cache_put_does_not_evict_inline_on_capacity_overflow() {
    let dir = TempDir::new().unwrap();
    let cache = test_cache(dir.path(), 100);

    cache.put("k1", &Bytes::from(vec![b'a'; 60])).await.unwrap();
    assert_eq!(cache.total_size(), 60);

    cache.put("k2", &Bytes::from(vec![b'b'; 60])).await.unwrap();
    assert_eq!(
        cache.total_size(),
        120,
        "put must return after indexing the new value, before eviction I/O runs"
    );

    wait_for_cache_size_at_most(&cache, 100).await;
}

#[tokio::test]
async fn test_cache_pin_scoped_survives_capacity_pressure() {
    let dir = TempDir::new().unwrap();
    let cache = test_cache(dir.path(), 60);

    cache
        .put("ns/segments/seg1/centroids.bin", &Bytes::from(vec![1; 30]))
        .await
        .unwrap();
    cache
        .pin_scoped("ns", "ns/segments/seg1/centroids.bin")
        .await;

    for i in 0..10 {
        cache
            .put(&format!("cold_{i}"), &Bytes::from(vec![i as u8; 30]))
            .await
            .unwrap();
        wait_for_cache_size_at_most(&cache, 60).await;
        assert!(
            cache.get("ns/segments/seg1/centroids.bin").await.is_some(),
            "scoped pin was evicted under pressure"
        );
    }
}

#[test]
fn test_memory_cache_pin_survives_capacity_pressure() {
    let cache = MemoryCache::new(60);
    cache.insert("metadata", Bytes::from(vec![1; 30]));
    cache.pin("metadata");

    for i in 0..10 {
        cache.insert(&format!("cold_{i}"), Bytes::from(vec![i as u8; 30]));
        assert!(
            cache.get("metadata").is_some(),
            "memory pinned key was evicted under pressure"
        );
    }
}

#[test]
fn test_memory_cache_hot_set_survives_cold_insert_flood_statistically() {
    const ROUNDS: usize = 32;
    const HOT_KEYS: usize = 12;
    const COLD_INSERTS: usize = 128;
    const ENTRY_SIZE: usize = 8;

    let mut hot_survivors = 0usize;
    for round in 0..ROUNDS {
        let cache = MemoryCache::new((HOT_KEYS * ENTRY_SIZE * 2) as u64);
        for hot in 0..HOT_KEYS {
            cache.insert(
                &format!("hot_{round}_{hot}"),
                Bytes::from(vec![b'h'; ENTRY_SIZE]),
            );
        }

        for cold in 0..COLD_INSERTS {
            for hot in 0..HOT_KEYS {
                let _ = cache.get(&format!("hot_{round}_{hot}"));
            }
            cache.insert(
                &format!("cold_{round}_{cold}"),
                Bytes::from(vec![b'c'; ENTRY_SIZE]),
            );
        }

        for hot in 0..HOT_KEYS {
            if cache.get(&format!("hot_{round}_{hot}")).is_some() {
                hot_survivors += 1;
            }
        }
    }

    let possible = ROUNDS * HOT_KEYS;
    assert!(
        hot_survivors * 100 >= possible * 75,
        "hot set survival too low: survived {hot_survivors}/{possible}"
    );
}

#[tokio::test]
async fn test_concurrent_cold_index_searches_share_one_cluster_get() {
    const CONCURRENCY: usize = 16;

    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let ns = harness.key("cache-singleflight-index");
    let vectors = single_cluster_vectors("sf", 512);
    let index = Arc::new(
        IvfFlatIndex::build(&vectors, &single_cluster_config(), &store, &ns, "seg_sf")
            .await
            .unwrap(),
    );
    let cache_dir = TempDir::new().unwrap();
    let cache = Arc::new(test_cache(cache_dir.path(), 100 * 1024 * 1024));
    let start = Arc::new(Barrier::new(CONCURRENCY + 1));

    counter.reset();
    let mut handles = Vec::with_capacity(CONCURRENCY);
    for _ in 0..CONCURRENCY {
        let index = Arc::clone(&index);
        let store = store.clone();
        let cache = Arc::clone(&cache);
        let query = vectors[0].values.clone();
        let start = Arc::clone(&start);
        handles.push(tokio::spawn(async move {
            start.wait().await;
            search_ivf_flat(
                &index,
                &query,
                1,
                1,
                None,
                DistanceMetric::Euclidean,
                &store,
                1,
                Some(&cache),
                true,
                DEFAULT_RERANK_COALESCE_GAP_BYTES,
            )
            .await
            .unwrap()
        }));
    }

    start.wait().await;
    for handle in handles {
        let results = handle.await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "sf_0");
    }

    assert_eq!(
        counter.gets_for(ArtifactClass::Cluster),
        1,
        "concurrent cold searches on one immutable cluster object must share one S3 GET"
    );
    harness.cleanup().await;
}
