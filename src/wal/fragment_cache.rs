//! Bounded process-local memo for decoded immutable WAL fragments.
//!
//! Query execution already selects exact fragment IDs from an authoritative
//! manifest before reaching this module. [`WalFragmentCache`] only replaces
//! repeated MessagePack decoding for those selected IDs; it never decides
//! which fragments exist or are visible. A miss therefore falls through to the
//! byte cache and S3 reader, while a hit returns an [`Arc`] to the same decoded
//! immutable value.
//!
//! The cache is disposable. Clearing it, capacity eviction, and lifecycle
//! eviction can change CPU work but must never change query results. Fragment
//! ULIDs are safe keys because WAL objects are write-once and a published ULID
//! is never rebound to different bytes.

use std::collections::HashSet;
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use dashmap::DashMap;
use rand::Rng;
use ulid::Ulid;

use crate::types::{AttributeValue, VectorEntry};

use super::fragment::WalFragment;

/// Number of candidate entries considered by approximate-LRU eviction.
const EVICTION_SAMPLE_SIZE: usize = 16;

/// One shared decoded fragment plus capacity and recency metadata.
struct CacheEntry {
    namespace: String,
    fragment: Arc<WalFragment>,
    size_bytes: usize,
    last_accessed: Instant,
}

/// Byte-bounded memo of decoded immutable WAL fragments used by queries.
pub struct WalFragmentCache {
    entries: DashMap<Ulid, CacheEntry>,
    bytes: AtomicUsize,
    max_bytes: usize,
    decode_count: AtomicU64,
    mutation: Mutex<()>,
}

impl WalFragmentCache {
    /// Creates an empty cache with an approximate decoded-payload byte budget.
    #[must_use]
    pub fn new(max_bytes: usize) -> Self {
        Self {
            entries: DashMap::new(),
            bytes: AtomicUsize::new(0),
            max_bytes,
            decode_count: AtomicU64::new(0),
            mutation: Mutex::new(()),
        }
    }

    /// Returns a shared decoded fragment and refreshes its eviction recency.
    #[must_use]
    pub(crate) fn get(&self, id: &Ulid) -> Option<Arc<WalFragment>> {
        let mut entry = self.entries.get_mut(id)?;
        entry.last_accessed = Instant::now();
        Some(Arc::clone(&entry.fragment))
    }

    /// Records one successful decode, inserts it, and enforces the byte budget.
    ///
    /// Concurrent misses may decode the same immutable ID more than once. The
    /// later insertion replaces an equivalent value; this affects CPU only.
    pub(crate) fn insert_decoded(&self, namespace: &str, fragment: Arc<WalFragment>) {
        self.decode_count.fetch_add(1, Ordering::Relaxed);
        let size_bytes = approximate_fragment_size(&fragment)
            .checked_add(namespace.len())
            .unwrap_or_else(|| panic!("WAL fragment cache entry size overflowed"));
        let _guard = self
            .mutation
            .lock()
            .unwrap_or_else(|_| panic!("WAL fragment cache mutation lock poisoned"));

        let previous = self.entries.insert(
            fragment.id,
            CacheEntry {
                namespace: namespace.to_string(),
                fragment,
                size_bytes,
                last_accessed: Instant::now(),
            },
        );
        let previous_size = previous.as_ref().map_or(0, |entry| entry.size_bytes);
        let current = self.bytes.load(Ordering::Relaxed);
        let without_previous = current.checked_sub(previous_size).unwrap_or_else(|| {
            panic!("WAL fragment cache byte accounting regressed during replacement")
        });
        self.bytes.store(
            without_previous.checked_add(size_bytes).unwrap_or_else(|| {
                panic!("WAL fragment cache byte accounting overflowed during insertion")
            }),
            Ordering::Relaxed,
        );
        self.evict_to_budget_locked();
    }

    /// Drops entries retired from one namespace's authoritative WAL ref set.
    ///
    /// The namespace metadata is lifecycle-only and is not part of the cache
    /// key. Scanning only the byte-bounded entries prevents a second unbounded
    /// per-namespace bookkeeping structure.
    pub fn evict_compacted(&self, namespace: &str, active_fragment_ids: &[Ulid]) {
        let _guard = self
            .mutation
            .lock()
            .unwrap_or_else(|_| panic!("WAL fragment cache mutation lock poisoned"));
        let active: HashSet<&Ulid> = active_fragment_ids.iter().collect();
        let retired: Vec<Ulid> = self
            .entries
            .iter()
            .filter(|entry| entry.value().namespace == namespace && !active.contains(entry.key()))
            .map(|entry| *entry.key())
            .collect();
        for id in retired {
            self.remove_locked(&id);
        }
    }

    /// Clears every decoded entry and lifecycle observation.
    pub fn clear(&self) {
        let _guard = self
            .mutation
            .lock()
            .unwrap_or_else(|_| panic!("WAL fragment cache mutation lock poisoned"));
        self.entries.clear();
        self.bytes.store(0, Ordering::Relaxed);
    }

    /// Returns the momentary number of decoded entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Reports whether no decoded entries are retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns approximate decoded bytes currently charged to the cache.
    #[must_use]
    pub fn total_size(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Returns successful decode insertions since construction.
    ///
    /// This diagnostic deliberately counts duplicate concurrent decodes. It is
    /// used to prove warm sequential queries perform no decoding.
    #[must_use]
    pub fn decode_count(&self) -> u64 {
        self.decode_count.load(Ordering::Relaxed)
    }

    fn evict_to_budget_locked(&self) {
        while self.bytes.load(Ordering::Relaxed) > self.max_bytes {
            let Some(victim) = self.sampled_victim() else {
                break;
            };
            self.remove_locked(&victim);
        }
    }

    fn remove_locked(&self, id: &Ulid) {
        if let Some((_, entry)) = self.entries.remove(id) {
            let current = self.bytes.load(Ordering::Relaxed);
            self.bytes.store(
                current.checked_sub(entry.size_bytes).unwrap_or_else(|| {
                    panic!("WAL fragment cache byte accounting regressed during eviction")
                }),
                Ordering::Relaxed,
            );
        }
    }

    fn sampled_victim(&self) -> Option<Ulid> {
        let len = self.entries.len();
        if len == 0 {
            return None;
        }

        let start = rand::thread_rng().gen_range(0..len);
        let mut sampled = 0usize;
        let mut victim: Option<(Ulid, Instant)> = None;
        for entry in self.entries.iter().skip(start) {
            if victim
                .as_ref()
                .map(|(_, last_accessed)| entry.value().last_accessed < *last_accessed)
                .unwrap_or(true)
            {
                victim = Some((*entry.key(), entry.value().last_accessed));
            }
            sampled += 1;
            if sampled == EVICTION_SAMPLE_SIZE {
                return victim.map(|(id, _)| id);
            }
        }
        for entry in self.entries.iter() {
            if victim
                .as_ref()
                .map(|(_, last_accessed)| entry.value().last_accessed < *last_accessed)
                .unwrap_or(true)
            {
                victim = Some((*entry.key(), entry.value().last_accessed));
            }
            sampled += 1;
            if sampled == EVICTION_SAMPLE_SIZE {
                break;
            }
        }
        victim.map(|(id, _)| id)
    }
}

/// Estimates owned allocations retained by one decoded fragment.
fn approximate_fragment_size(fragment: &WalFragment) -> usize {
    let mut bytes = size_of::<WalFragment>()
        .saturating_add(
            fragment
                .vectors
                .capacity()
                .saturating_mul(size_of::<VectorEntry>()),
        )
        .saturating_add(
            fragment
                .deletes
                .capacity()
                .saturating_mul(size_of::<String>()),
        );

    for vector in &fragment.vectors {
        bytes = bytes
            .saturating_add(vector.id.capacity())
            .saturating_add(vector.values.capacity().saturating_mul(size_of::<f32>()));
        if let Some(attributes) = &vector.attributes {
            bytes = bytes.saturating_add(
                attributes
                    .capacity()
                    .saturating_mul(size_of::<(String, AttributeValue)>()),
            );
            for (key, value) in attributes {
                bytes = bytes
                    .saturating_add(key.capacity())
                    .saturating_add(approximate_attribute_size(value));
            }
        }
    }
    for id in &fragment.deletes {
        bytes = bytes.saturating_add(id.capacity());
    }
    bytes
}

fn approximate_attribute_size(value: &AttributeValue) -> usize {
    match value {
        AttributeValue::String(value) => value.capacity(),
        AttributeValue::StringList(values) => values
            .capacity()
            .saturating_mul(size_of::<String>())
            .saturating_add(
                values
                    .iter()
                    .map(String::capacity)
                    .fold(0usize, usize::saturating_add),
            ),
        AttributeValue::IntegerList(values) => values.capacity().saturating_mul(size_of::<i64>()),
        AttributeValue::FloatList(values) => values.capacity().saturating_mul(size_of::<f64>()),
        AttributeValue::Integer(_) | AttributeValue::Float(_) | AttributeValue::Bool(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fragment(id: Ulid, vector_bytes: usize) -> Arc<WalFragment> {
        Arc::new(WalFragment {
            id,
            vectors: vec![VectorEntry {
                id: format!("vector-{id}"),
                values: vec![1.0; vector_bytes / size_of::<f32>()],
                attributes: None,
            }],
            deletes: Vec::new(),
            checksum: 0,
        })
    }

    #[test]
    fn hit_returns_the_same_allocation_and_clear_is_non_load_bearing() {
        let cache = WalFragmentCache::new(1024 * 1024);
        let value = fragment(Ulid::new(), 128);
        cache.insert_decoded("ns", Arc::clone(&value));
        let hit = match cache.get(&value.id) {
            Some(hit) => hit,
            None => panic!("inserted fragment missing"),
        };
        assert!(Arc::ptr_eq(&value, &hit));
        assert_eq!(cache.decode_count(), 1);
        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.total_size(), 0);
    }

    #[test]
    fn zero_budget_evicts_every_insert_without_losing_the_decode_observation() {
        let cache = WalFragmentCache::new(0);
        cache.insert_decoded("ns", fragment(Ulid::new(), 128));
        assert!(cache.is_empty());
        assert_eq!(cache.total_size(), 0);
        assert_eq!(cache.decode_count(), 1);
    }

    #[test]
    fn overflow_evicts_to_the_configured_approximate_byte_budget() {
        let first = fragment(Ulid::new(), 256);
        let second = fragment(Ulid::new(), 256);
        let one_entry_budget = approximate_fragment_size(&first) + "ns".len();
        let cache = WalFragmentCache::new(one_entry_budget);
        cache.insert_decoded("ns", first);
        cache.insert_decoded("ns", second);
        assert!(cache.total_size() <= one_entry_budget);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.decode_count(), 2);
    }

    #[test]
    fn lifecycle_eviction_is_scoped_to_the_observed_namespace() {
        let cache = WalFragmentCache::new(1024 * 1024);
        let first = fragment(Ulid::new(), 128);
        let second = fragment(Ulid::new(), 128);
        cache.insert_decoded("ns-a", Arc::clone(&first));
        cache.insert_decoded("ns-b", Arc::clone(&second));
        cache.evict_compacted("ns-a", &[]);
        assert!(cache.get(&first.id).is_none());
        assert!(cache.get(&second.id).is_some());
    }
}
