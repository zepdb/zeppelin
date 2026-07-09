//! Bounded selection primitives shared by vector and lexical ranking.
//!
//! Query paths often score far more candidates than they return. This module
//! provides two allocation-aware strategies under one comparator contract:
//! `partial_topk_by` reduces an existing vector in place,
//! while `TopK` accepts a stream and keeps at most `k` owned items. In both
//! cases the comparator must order the **best** item first.
//!
//! ```text
//! materialized candidates             streaming candidates
//!          |                                  |
//!          v                                  v
//! partial selection + sort k        worst-at-root bounded heap
//!          |                                  |
//!          +------------> sorted best k <-----+
//! ```
//!
//! Equal comparator results are unstable. Callers that need deterministic
//! output include an ID or another stable tie-breaker in the comparator.
//!
//! ## Rust concepts used here
//!
//! `T` remains generic, so the same code ranks vector distances, BM25 scores,
//! and merged query results without boxing. `FnMut` permits comparators with
//! mutable captured state. `TopK::push` takes ownership of each candidate:
//! rejected values are dropped immediately, while retained values move into the
//! heap. Java would store object references; C would need element-size or
//! callback conventions plus explicit cleanup.

use std::cmp::Ordering;

/// Keep the best `k` items according to `cmp`.
///
/// Selection is intentionally unstable: equal items under `cmp` do not retain
/// scan order. Callers that need deterministic ties must include the tie-break
/// in `cmp`, as the ANN coarse-ranking paths do with id ordering.
///
/// # Parameters
///
/// - `vec`: Owned candidate buffer modified in place.
/// - `k`: Maximum number of best items to retain.
/// - `cmp`: Comparator that returns `Less` when its first item should rank
///   before its second.
///
/// # Returns
///
/// Returns unit. `vec` contains the best `min(k, original_len)` items in sorted
/// comparator order. `k = 0` clears the vector.
///
/// # Side Effects
///
/// Reorders and truncates the caller's vector, dropping rejected items.
///
/// # Performance
///
/// For `0 < k < n`, performs expected linear unstable selection followed by
/// sorting `k` retained values. If `k >= n`, it sorts the full vector.
///
/// # Examples
///
/// With ascending numeric comparison, candidates `[8, 2, 5, 1]` and `k = 2`
/// become `[1, 2]`.
///
/// # Rust Notes for Java/C Engineers
///
/// The mutable borrow prevents any other access to the vector during selection.
/// The comparator is monomorphized at compile time rather than invoked through
/// Java-style interface dispatch or an untyped C callback.
pub(crate) fn partial_topk_by<T>(
    vec: &mut Vec<T>,
    k: usize,
    mut cmp: impl FnMut(&T, &T) -> Ordering,
) {
    if k == 0 {
        vec.clear();
        return;
    }

    if k >= vec.len() {
        vec.sort_by(cmp);
        return;
    }

    let _ = vec.select_nth_unstable_by(k, &mut cmp);
    vec.truncate(k);
    vec.sort_by(cmp);
}

/// Streaming best-`k` collector with the current worst item at the heap root.
///
/// The heap never grows beyond `k`, making it suitable for WAL or fused-source
/// scans where materializing every scored candidate would waste memory.
pub(crate) struct TopK<T, C>
where
    C: FnMut(&T, &T) -> Ordering,
{
    /// Maximum retained item count.
    k: usize,
    /// Retained items arranged as a worst-at-root binary heap.
    heap: Vec<T>,
    /// Ordering where `Less` means the left item is better.
    cmp: C,
}

impl<T, C> TopK<T, C>
where
    C: FnMut(&T, &T) -> Ordering,
{
    /// Creates an empty bounded collector.
    ///
    /// # Parameters
    ///
    /// - `k`: Maximum retained count; zero rejects every pushed item.
    /// - `cmp`: Comparator ordering best items first.
    ///
    /// # Returns
    ///
    /// Returns a collector with capacity reserved for `k` items.
    ///
    /// # Examples
    ///
    /// A query requesting 10 results constructs `TopK::new(10, cmp)` and streams
    /// scored WAL vectors through [`TopK::push`].
    #[must_use]
    pub(crate) fn new(k: usize, cmp: C) -> Self {
        Self {
            k,
            heap: Vec::with_capacity(k),
            cmp,
        }
    }

    /// Returns the number of candidates currently retained.
    ///
    /// # Returns
    ///
    /// Returns a value no greater than the configured `k`.
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.heap.len()
    }

    /// Consumes one candidate and retains it only when it belongs in the best `k`.
    ///
    /// # Parameters
    ///
    /// - `item`: Owned candidate. Rejected candidates are dropped before return.
    ///
    /// # Returns
    ///
    /// Returns unit with the bounded worst-at-root heap invariant restored.
    ///
    /// # Performance
    ///
    /// Filling the first `k` slots and replacing the current worst item cost
    /// `O(log k)`; rejecting a worse candidate is `O(1)` after comparison.
    ///
    /// # Examples
    ///
    /// A full collector holding distances `[1, 3, 5]` accepts distance `2`,
    /// evicts `5`, and rejects distance `8`.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Ownership makes rejection cleanup automatic. Assigning into `heap[0]`
    /// drops the evicted value exactly once, without a Java garbage collector or
    /// manual C destructor call.
    pub(crate) fn push(&mut self, item: T) {
        if self.k == 0 {
            return;
        }

        if self.heap.len() < self.k {
            self.heap.push(item);
            self.sift_up(self.heap.len() - 1);
            debug_assert!(self.len() <= self.k);
            return;
        }

        if self.item_is_better_than_worst(&item) {
            self.heap[0] = item;
            self.sift_down(0);
        }
        debug_assert!(self.len() <= self.k);
    }

    /// Consumes the collector and returns retained items in final rank order.
    ///
    /// # Returns
    ///
    /// Returns an owned vector sorted best first. The collector cannot be used
    /// afterward because its heap and comparator move into this method.
    ///
    /// # Performance
    ///
    /// Sorts at most `k` items in `O(k log k)` time.
    #[must_use]
    pub(crate) fn into_sorted_vec(mut self) -> Vec<T> {
        self.heap.sort_by(self.cmp);
        self.heap
    }

    /// Compares a candidate with the current worst retained item.
    ///
    /// # Returns
    ///
    /// Returns `true` when `item` ranks before heap root zero.
    fn item_is_better_than_worst(&mut self, item: &T) -> bool {
        let cmp = &mut self.cmp;
        cmp(item, &self.heap[0]).is_lt()
    }

    /// Compares two heap indexes under the worst-at-root ordering.
    ///
    /// # Returns
    ///
    /// Returns `true` when `left` ranks after `right`.
    ///
    /// # Panics
    ///
    /// Panics if either index lies outside the internal heap. Callers derive
    /// indexes from the heap shape.
    fn index_is_worse(&mut self, left: usize, right: usize) -> bool {
        let cmp = &mut self.cmp;
        cmp(&self.heap[left], &self.heap[right]).is_gt()
    }

    /// Restores the worst-at-root invariant after appending one item.
    ///
    /// # Parameters
    ///
    /// - `idx`: Valid index of the newly appended item.
    fn sift_up(&mut self, mut idx: usize) {
        while idx > 0 {
            let parent = (idx - 1) / 2;
            if !self.index_is_worse(idx, parent) {
                break;
            }
            self.heap.swap(idx, parent);
            idx = parent;
        }
    }

    /// Restores the worst-at-root invariant after replacing one heap item.
    ///
    /// # Parameters
    ///
    /// - `idx`: Valid index whose value may be better than its children.
    fn sift_down(&mut self, mut idx: usize) {
        loop {
            let left = idx * 2 + 1;
            let right = left + 1;
            if left >= self.heap.len() {
                break;
            }

            let mut worst = left;
            if right < self.heap.len() && self.index_is_worse(right, left) {
                worst = right;
            }

            if !self.index_is_worse(worst, idx) {
                break;
            }

            self.heap.swap(idx, worst);
            idx = worst;
        }
    }
}

#[cfg(test)]
mod tests {
    //! Oracle-based tests for materialized and streaming top-k selection.

    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    #[derive(Clone, Debug, PartialEq)]
    /// Deterministically tie-breakable candidate used by both ranking directions.
    struct Scored {
        /// Stable secondary ordering key.
        id: String,
        /// Primary distance or relevance score.
        score: f32,
    }

    /// Orders lower distance first and IDs ascending on ties.
    fn distance_cmp(a: &Scored, b: &Scored) -> Ordering {
        a.score.total_cmp(&b.score).then_with(|| a.id.cmp(&b.id))
    }

    /// Orders higher BM25 relevance first and IDs ascending on ties.
    fn bm25_cmp(a: &Scored, b: &Scored) -> Ordering {
        b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id))
    }

    /// Generates deterministic candidates with many tied score buckets.
    fn candidates(seed: u64, len: usize) -> Vec<Scored> {
        let mut rng = StdRng::seed_from_u64(seed);
        (0..len)
            .map(|idx| {
                let bucket = rng.gen_range(-12..=12);
                Scored {
                    id: format!("id_{seed:02}_{idx:04}"),
                    score: bucket as f32 / 4.0,
                }
            })
            .collect()
    }

    /// Compares partial selection with a full-sort reference result.
    fn assert_partial_matches_oracle(
        input: &[Scored],
        k: usize,
        cmp: fn(&Scored, &Scored) -> Ordering,
    ) {
        let mut expected = input.to_vec();
        expected.sort_by(cmp);
        expected.truncate(k.min(expected.len()));

        let mut actual = input.to_vec();
        partial_topk_by(&mut actual, k, cmp);
        assert_eq!(actual, expected);
    }

    /// Verifies in-place selection across sizes, boundaries, ties, and rankings.
    #[test]
    fn partial_topk_matches_full_sort_oracle_with_ties() {
        for seed in 0..32 {
            let len = (seed as usize * 17) % 211;
            let input = candidates(seed, len);
            let ks = [0, 1, 2, 3, 5, 8, 13, len / 2, len, len + 7];
            for k in ks {
                assert_partial_matches_oracle(&input, k, distance_cmp);
                assert_partial_matches_oracle(&input, k, bm25_cmp);
            }
        }
    }

    /// Verifies streaming selection matches full sort while staying bounded.
    #[test]
    fn streaming_topk_matches_full_sort_and_stays_bounded() {
        let k = 37;
        let input = candidates(99, 100_000);
        let mut expected = input.clone();
        expected.sort_by(distance_cmp);
        expected.truncate(k);

        let mut topk = TopK::new(k, distance_cmp as fn(&Scored, &Scored) -> Ordering);
        let mut max_len = 0usize;
        for candidate in input {
            topk.push(candidate);
            max_len = max_len.max(topk.len());
            assert!(topk.len() <= k);
        }

        assert_eq!(max_len, k);
        assert_eq!(topk.into_sorted_vec(), expected);
    }
}
