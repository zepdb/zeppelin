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

use crate::namespace::branching::ArtifactOrigin;
use crate::types::{AttributeValue, VectorEntry};
use dashmap::DashMap;
use rand::Rng;

use super::fragment::WalFragment;
use super::manifest::LocatedFragmentIdentity;

/// Number of candidate entries considered by approximate-LRU eviction.
const EVICTION_SAMPLE_SIZE: usize = 16;

/// One shared decoded fragment plus capacity and recency metadata.
struct CacheEntry {
    logical_origins: HashSet<ArtifactOrigin>,
    fragment: Arc<WalFragment>,
    size_bytes: usize,
    last_accessed: Instant,
}

/// Byte-bounded memo of decoded immutable WAL fragments used by queries.
pub struct WalFragmentCache {
    entries: DashMap<LocatedFragmentIdentity, CacheEntry>,
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
    pub(crate) fn get(&self, identity: &LocatedFragmentIdentity) -> Option<Arc<WalFragment>> {
        let mut entry = self.entries.get_mut(identity)?;
        entry.last_accessed = Instant::now();
        Some(Arc::clone(&entry.fragment))
    }

    /// Records one successful decode, inserts it, and enforces the byte budget.
    ///
    /// Concurrent misses may decode the same immutable ID more than once. The
    /// later insertion replaces an equivalent value; this affects CPU only.
    pub(crate) fn insert_decoded(
        &self,
        logical_origin: &ArtifactOrigin,
        identity: LocatedFragmentIdentity,
        fragment: Arc<WalFragment>,
    ) {
        self.decode_count.fetch_add(1, Ordering::Relaxed);
        let size_bytes = approximate_fragment_size(&fragment)
            .checked_add(identity.physical_origin.namespace.as_str().len())
            .and_then(|size| size.checked_add(size_of::<LocatedFragmentIdentity>()))
            .unwrap_or_else(|| panic!("WAL fragment cache entry size overflowed"));
        let _guard = self
            .mutation
            .lock()
            .unwrap_or_else(|_| panic!("WAL fragment cache mutation lock poisoned"));

        let previous = self.entries.remove(&identity).map(|(_, entry)| entry);
        let mut logical_origins = previous
            .as_ref()
            .map(|entry| entry.logical_origins.clone())
            .unwrap_or_default();
        logical_origins.insert(logical_origin.clone());
        self.entries.insert(
            identity,
            CacheEntry {
                logical_origins,
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
    pub(crate) fn evict_compacted_located(
        &self,
        logical_origin: &ArtifactOrigin,
        active_fragment_identities: &[LocatedFragmentIdentity],
    ) {
        let _guard = self
            .mutation
            .lock()
            .unwrap_or_else(|_| panic!("WAL fragment cache mutation lock poisoned"));
        let active: HashSet<&LocatedFragmentIdentity> = active_fragment_identities.iter().collect();
        let mut retired = Vec::new();
        for mut entry in self.entries.iter_mut() {
            if entry.value().logical_origins.contains(logical_origin)
                && !active.contains(entry.key())
            {
                entry.value_mut().logical_origins.remove(logical_origin);
                if entry.value().logical_origins.is_empty() {
                    retired.push(entry.key().clone());
                }
            }
        }
        for identity in retired {
            self.remove_locked(&identity);
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

    fn remove_locked(&self, identity: &LocatedFragmentIdentity) {
        if let Some((_, entry)) = self.entries.remove(identity) {
            let current = self.bytes.load(Ordering::Relaxed);
            self.bytes.store(
                current.checked_sub(entry.size_bytes).unwrap_or_else(|| {
                    panic!("WAL fragment cache byte accounting regressed during eviction")
                }),
                Ordering::Relaxed,
            );
        }
    }

    fn sampled_victim(&self) -> Option<LocatedFragmentIdentity> {
        let len = self.entries.len();
        if len == 0 {
            return None;
        }

        let start = rand::thread_rng().gen_range(0..len);
        let mut sampled = 0usize;
        let mut victim: Option<(LocatedFragmentIdentity, Instant)> = None;
        for entry in self.entries.iter().skip(start) {
            if victim
                .as_ref()
                .map(|(_, last_accessed)| entry.value().last_accessed < *last_accessed)
                .unwrap_or(true)
            {
                victim = Some((entry.key().clone(), entry.value().last_accessed));
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
                victim = Some((entry.key().clone(), entry.value().last_accessed));
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
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::namespace::branching::ArtifactOrigin;
    use crate::namespace::{NamespaceId, NamespaceIncarnationId};
    use ulid::Ulid;

    fn origin(namespace: &str, incarnation: u128) -> ArtifactOrigin {
        ArtifactOrigin {
            namespace: NamespaceId::parse(namespace).unwrap(),
            incarnation: NamespaceIncarnationId::from_uuid(uuid::Uuid::from_u128(incarnation)),
        }
    }

    fn identity(origin: &ArtifactOrigin, id: Ulid) -> LocatedFragmentIdentity {
        LocatedFragmentIdentity {
            physical_origin: origin.clone(),
            id,
        }
    }

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
        let origin = origin("ns", 1);
        let identity = identity(&origin, value.id);
        cache.insert_decoded(&origin, identity.clone(), Arc::clone(&value));
        let hit = match cache.get(&identity) {
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
        let value = fragment(Ulid::new(), 128);
        let origin = origin("ns", 1);
        cache.insert_decoded(&origin, identity(&origin, value.id), value);
        assert!(cache.is_empty());
        assert_eq!(cache.total_size(), 0);
        assert_eq!(cache.decode_count(), 1);
    }

    #[test]
    fn overflow_evicts_to_the_configured_approximate_byte_budget() {
        let first = fragment(Ulid::new(), 256);
        let second = fragment(Ulid::new(), 256);
        let one_entry_budget =
            approximate_fragment_size(&first) + "ns".len() + size_of::<LocatedFragmentIdentity>();
        let cache = WalFragmentCache::new(one_entry_budget);
        let origin = origin("ns", 1);
        cache.insert_decoded(&origin, identity(&origin, first.id), first);
        cache.insert_decoded(&origin, identity(&origin, second.id), second);
        assert!(cache.total_size() <= one_entry_budget);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.decode_count(), 2);
    }

    #[test]
    fn lifecycle_eviction_is_scoped_to_the_observed_namespace() {
        let cache = WalFragmentCache::new(1024 * 1024);
        let first = fragment(Ulid::new(), 128);
        let second = fragment(Ulid::new(), 128);
        let first_origin = origin("ns-a", 1);
        let second_origin = origin("ns-b", 2);
        let first_identity = identity(&first_origin, first.id);
        let second_identity = identity(&second_origin, second.id);
        cache.insert_decoded(&first_origin, first_identity.clone(), Arc::clone(&first));
        cache.insert_decoded(&second_origin, second_identity.clone(), Arc::clone(&second));
        cache.evict_compacted_located(&first_origin, &[]);
        assert!(cache.get(&first_identity).is_none());
        assert!(cache.get(&second_identity).is_some());
    }

    #[test]
    fn shared_physical_fragment_survives_one_logical_scope_eviction() {
        let cache = WalFragmentCache::new(1024 * 1024);
        let value = fragment(Ulid::new(), 128);
        let source = origin("source", 1);
        let target = origin("target", 2);
        let identity = identity(&source, value.id);
        cache.insert_decoded(&source, identity.clone(), Arc::clone(&value));
        cache.insert_decoded(&target, identity.clone(), value);

        cache.evict_compacted_located(&target, &[]);

        assert!(cache.get(&identity).is_some());
    }
}
