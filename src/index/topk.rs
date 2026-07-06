use std::cmp::Ordering;

/// Keep the best `k` items according to `cmp`.
///
/// Selection is intentionally unstable: equal items under `cmp` do not retain
/// scan order. Callers that need deterministic ties must include the tie-break
/// in `cmp`, as the ANN coarse-ranking paths do with id ordering.
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

pub(crate) struct TopK<T, C>
where
    C: FnMut(&T, &T) -> Ordering,
{
    k: usize,
    heap: Vec<T>,
    cmp: C,
}

impl<T, C> TopK<T, C>
where
    C: FnMut(&T, &T) -> Ordering,
{
    #[must_use]
    pub(crate) fn new(k: usize, cmp: C) -> Self {
        Self {
            k,
            heap: Vec::with_capacity(k),
            cmp,
        }
    }

    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.heap.len()
    }

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

    #[must_use]
    pub(crate) fn into_sorted_vec(mut self) -> Vec<T> {
        self.heap.sort_by(self.cmp);
        self.heap
    }

    fn item_is_better_than_worst(&mut self, item: &T) -> bool {
        let cmp = &mut self.cmp;
        cmp(item, &self.heap[0]).is_lt()
    }

    fn index_is_worse(&mut self, left: usize, right: usize) -> bool {
        let cmp = &mut self.cmp;
        cmp(&self.heap[left], &self.heap[right]).is_gt()
    }

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
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    #[derive(Clone, Debug, PartialEq)]
    struct Scored {
        id: String,
        score: f32,
    }

    fn distance_cmp(a: &Scored, b: &Scored) -> Ordering {
        a.score.total_cmp(&b.score).then_with(|| a.id.cmp(&b.id))
    }

    fn bm25_cmp(a: &Scored, b: &Scored) -> Ordering {
        b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id))
    }

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
