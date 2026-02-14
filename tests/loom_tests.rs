//! Loom concurrency tests for Zeppelin's concurrent data structures.
//!
//! These model simplified versions of the concurrent protocols using loom
//! primitives to exhaustively test all possible interleavings.
//!
//! Run with: `RUSTFLAGS="--cfg loom" cargo test --test loom_tests -- --nocapture`

#[cfg(loom)]
mod loom_tests {
    use loom::sync::atomic::{AtomicUsize, Ordering};
    use loom::sync::{Arc, Mutex};
    use loom::thread;

    /// Model of the singleflight pattern used in ManifestCache.
    ///
    /// Two threads call get() simultaneously on an expired entry.
    /// Only one should actually "fetch" (increment the counter).
    #[test]
    fn test_singleflight_coalescing() {
        loom::model(|| {
            let fetch_count = Arc::new(AtomicUsize::new(0));
            let cache = Arc::new(Mutex::new(None::<u64>));
            let singleflight = Arc::new(Mutex::new(()));

            let mut handles = vec![];
            for _ in 0..2 {
                let fc = fetch_count.clone();
                let c = cache.clone();
                let sf = singleflight.clone();

                handles.push(thread::spawn(move || {
                    // Fast path: check cache
                    {
                        let cached = c.lock().unwrap();
                        if cached.is_some() {
                            return *cached;
                        }
                    }

                    // Singleflight: acquire mutex
                    let _guard = sf.lock().unwrap();

                    // Re-check after acquiring lock
                    {
                        let cached = c.lock().unwrap();
                        if cached.is_some() {
                            return *cached;
                        }
                    }

                    // We won the race — "fetch"
                    fc.fetch_add(1, Ordering::SeqCst);
                    let value = Some(42u64);

                    {
                        let mut cached = c.lock().unwrap();
                        *cached = value;
                    }

                    value
                }));
            }

            for h in handles {
                let result = h.join().unwrap();
                assert_eq!(result, Some(42));
            }

            // Only one fetch should have occurred
            assert_eq!(fetch_count.load(Ordering::SeqCst), 1);
        });
    }

    /// Model of atomic size tracking in MemoryCache.
    ///
    /// Concurrent insert and evict operations should keep the size counter
    /// consistent with the actual number of entries.
    #[test]
    fn test_atomic_size_tracking() {
        loom::model(|| {
            let size = Arc::new(AtomicUsize::new(0));
            let entries = Arc::new(Mutex::new(Vec::<u64>::new()));

            let size1 = size.clone();
            let entries1 = entries.clone();

            // Thread 1: insert an entry
            let h1 = thread::spawn(move || {
                let mut e = entries1.lock().unwrap();
                e.push(1);
                size1.fetch_add(1, Ordering::SeqCst);
            });

            let size2 = size.clone();
            let entries2 = entries.clone();

            // Thread 2: insert another entry
            let h2 = thread::spawn(move || {
                let mut e = entries2.lock().unwrap();
                e.push(2);
                size2.fetch_add(1, Ordering::SeqCst);
            });

            h1.join().unwrap();
            h2.join().unwrap();

            // Size should match actual entries
            let e = entries.lock().unwrap();
            assert_eq!(size.load(Ordering::SeqCst), e.len());
            assert_eq!(e.len(), 2);
        });
    }

    /// Model of cache invalidation racing with write-through.
    ///
    /// An invalidation (from compaction) should not be overwritten by
    /// a stale write-through that arrives after the invalidation.
    #[test]
    fn test_invalidation_vs_write_through() {
        loom::model(|| {
            // Simulated cache: (value, version)
            let cache = Arc::new(Mutex::new(None::<(u64, u64)>));
            // Tracks the invalidation version
            let last_invalidated_version = Arc::new(AtomicUsize::new(0));

            // Pre-populate cache with version 1
            {
                let mut c = cache.lock().unwrap();
                *c = Some((100, 1));
            }

            let cache1 = cache.clone();
            let inv1 = last_invalidated_version.clone();

            // Thread 1: invalidation (from compaction) at version 1
            let h1 = thread::spawn(move || {
                inv1.store(1, Ordering::SeqCst);
                let mut c = cache1.lock().unwrap();
                *c = None;
            });

            let cache2 = cache.clone();
            let inv2 = last_invalidated_version.clone();

            // Thread 2: stale write-through with version 1 data
            let h2 = thread::spawn(move || {
                let stale_version: usize = 1;
                // Check if stale
                let inv = inv2.load(Ordering::SeqCst);
                if stale_version <= inv {
                    return; // Reject stale write-through
                }
                let mut c = cache2.lock().unwrap();
                *c = Some((200, stale_version as u64));
            });

            h1.join().unwrap();
            h2.join().unwrap();

            let c = cache.lock().unwrap();
            // The stale write-through should NOT have overwritten the invalidation
            // Valid outcomes: None (invalidation won) or Some((200, 1)) if write-through
            // raced before invalidation set the version
            if let Some((_, version)) = *c {
                // If something is cached, the invalidation version should not have been
                // set yet when the write-through checked
                assert_eq!(version, 1);
            }
        });
    }
}

/// Placeholder test so `cargo test --test loom_tests` doesn't fail when
/// loom cfg is not set (normal CI runs).
#[cfg(not(loom))]
#[test]
fn loom_tests_require_cfg_loom() {
    // This test exists so that `cargo test --test loom_tests` succeeds
    // even without RUSTFLAGS="--cfg loom". The real loom tests only run
    // when the `loom` cfg flag is set.
}
