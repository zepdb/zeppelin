use bytes::Bytes;
use std::path::Path;
use tempfile::TempDir;

use zeppelin::cache::DiskCache;

/// Create a test cache with a given max size in bytes.
fn test_cache(dir: &Path, max_bytes: u64) -> DiskCache {
    DiskCache::new_with_max_bytes(dir.to_path_buf(), max_bytes).unwrap()
}

#[tokio::test]
async fn test_cache_put_and_get() {
    let dir = TempDir::new().unwrap();
    let cache = test_cache(dir.path(), 1024 * 1024);

    cache
        .put("k1", &Bytes::from("hello"))
        .await
        .unwrap();

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
    cache
        .put("k1", &Bytes::from(vec![b'a'; 50]))
        .await
        .unwrap();

    // Put k2 (50 bytes) — total now 100, at limit
    cache
        .put("k2", &Bytes::from(vec![b'b'; 50]))
        .await
        .unwrap();

    // Put k3 (50 bytes) — total would be 150, so evict oldest (k1)
    cache
        .put("k3", &Bytes::from(vec![b'c'; 50]))
        .await
        .unwrap();

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

    // Pinned "centroids" should survive
    assert!(cache.get("centroids").await.is_some());
    // "data1" should be evicted (it's the oldest unpinned entry)
    assert_eq!(cache.get("data1").await, None);
    // "data2" should be present
    assert!(cache.get("data2").await.is_some());
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
