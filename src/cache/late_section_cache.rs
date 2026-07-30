//! Bounded process-local memo for immutable manifest late-state sections.
//!
//! Entries are selected only after an authoritative manifest names an exact
//! object key and checksum. The cache cannot make a section visible and never
//! publishes state. Section objects are immutable and content-addressed, so
//! entries need neither a TTL nor invalidation.

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};

use crate::wal::late_section::{LateStateSection, ManifestSectionRef};

/// Maximum number of decoded late-state sections retained process-wide.
pub(crate) const LATE_SECTION_CACHE_MAX_ENTRIES: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LateSectionCacheKey {
    key: String,
    checksum: [u8; 32],
    size_bytes: u64,
    format_version: u32,
}

impl From<&ManifestSectionRef> for LateSectionCacheKey {
    fn from(reference: &ManifestSectionRef) -> Self {
        Self {
            key: reference.key.clone(),
            checksum: reference.checksum,
            size_bytes: reference.size_bytes,
            format_version: reference.format_version,
        }
    }
}

#[derive(Default)]
struct LateSectionCacheInner {
    entries: HashMap<LateSectionCacheKey, LateStateSection>,
    insertion_order: VecDeque<LateSectionCacheKey>,
}

struct LateSectionCache {
    inner: Mutex<LateSectionCacheInner>,
}

impl LateSectionCache {
    fn new() -> Self {
        Self {
            inner: Mutex::new(LateSectionCacheInner::default()),
        }
    }

    fn get(&self, reference: &ManifestSectionRef) -> Option<LateStateSection> {
        self.inner
            .lock()
            .unwrap_or_else(|error| panic!("late-state section cache mutex poisoned: {error}"))
            .entries
            .get(&LateSectionCacheKey::from(reference))
            .cloned()
    }

    fn insert(&self, reference: &ManifestSectionRef, section: LateStateSection) {
        let key = LateSectionCacheKey::from(reference);
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|error| panic!("late-state section cache mutex poisoned: {error}"));
        if let Some(existing) = inner.entries.get_mut(&key) {
            *existing = section;
            return;
        }
        while inner.entries.len() >= LATE_SECTION_CACHE_MAX_ENTRIES {
            let Some(evicted) = inner.insertion_order.pop_front() else {
                break;
            };
            inner.entries.remove(&evicted);
        }
        inner.insertion_order.push_back(key.clone());
        inner.entries.insert(key, section);
    }
}

fn global_cache() -> &'static LateSectionCache {
    static CACHE: OnceLock<LateSectionCache> = OnceLock::new();
    CACHE.get_or_init(LateSectionCache::new)
}

pub(crate) fn get(reference: &ManifestSectionRef) -> Option<LateStateSection> {
    global_cache().get(reference)
}

pub(crate) fn insert(reference: &ManifestSectionRef, section: LateStateSection) {
    global_cache().insert(reference, section);
}

#[cfg(test)]
mod tests {
    use super::{LateSectionCache, LateSectionCacheKey, LATE_SECTION_CACHE_MAX_ENTRIES};
    use crate::wal::late_section::{LateStateSection, ManifestSectionRef};

    fn reference(index: usize) -> ManifestSectionRef {
        let mut checksum = [0_u8; 32];
        checksum[..8].copy_from_slice(&(index as u64).to_le_bytes());
        ManifestSectionRef {
            key: LateStateSection::s3_key("ns", &checksum),
            checksum,
            size_bytes: 6,
            format_version: 1,
            artifact_origin: None,
        }
    }

    #[test]
    fn immutable_section_cache_is_strictly_bounded() {
        let cache = LateSectionCache::new();
        for index in 0..=LATE_SECTION_CACHE_MAX_ENTRIES {
            cache.insert(&reference(index), LateStateSection::new());
        }
        let inner = cache
            .inner
            .lock()
            .expect("late-state section cache mutex poisoned");
        assert_eq!(inner.entries.len(), LATE_SECTION_CACHE_MAX_ENTRIES);
        assert!(!inner
            .entries
            .contains_key(&LateSectionCacheKey::from(&reference(0))));
        assert!(inner
            .entries
            .contains_key(&LateSectionCacheKey::from(&reference(
                LATE_SECTION_CACHE_MAX_ENTRIES
            ))));
    }
}
