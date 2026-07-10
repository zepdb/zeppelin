//! Trains the centroids that divide vectors into IVF search clusters.
//!
//! A centroid is the representative point for a cluster. IVF (inverted file)
//! search is an approximate-nearest-neighbor (ANN) technique: a query first
//! finds nearby centroids and then reads only some of their clusters instead of
//! scanning every vector. [`train_kmeans`] is the shared CPU-only training seam
//! used by the IVF-Flat builder, hierarchical IVF, and product quantization.
//! This module does not read or publish S3 objects; its callers place the
//! resulting centroids into immutable segment artifacts.
//!
//! Small data sets use full Lloyd iterations, which reassign every vector on
//! every pass. Data sets larger than `MINI_BATCH_THRESHOLD` use sampled
//! updates to bound per-iteration CPU. Both paths begin with k-means++ seeding
//! and use a seed derived from the exact input, so identical ordered inputs and
//! parameters produce identical centroids.
//!
//! ```text
//! validated borrowed vectors
//!            |
//!            v
//! deterministic k-means++ seeds
//!            |
//!            +-- at most 10,000 rows --> full Lloyd passes
//!            |
//!            +-- more than 10,000 ----> sampled online updates
//!            |
//!            v
//! owned centroids --> caller serializes an immutable segment/codebook
//! ```
//!
//! ## Reading map
//!
//! 1. Start with [`train_kmeans`] for validation, deterministic seeding, and
//!    training-mode selection.
//! 2. Read `kmeans_pp_init` for the deliberately spread-out initial points.
//! 3. Compare `train_lloyds` with `train_mini_batch` to understand the CPU
//!    and convergence tradeoff.
//! 4. Finish with `deterministic_seed` and `squared_l2` for reproducibility
//!    and the distance primitive.
//!
//! ## Invariants
//!
//! - Every input vector must have exactly `dim` finite components. Production
//!   callers validate this before training; debug assertions catch violations.
//! - The returned centroid count is `min(k, vectors.len())`, never greater than
//!   the number of training points.
//! - Reaching the iteration limit is a usable, explicitly logged result, not a
//!   silent switch to another algorithm.
//! - Input order is part of the deterministic seed and therefore part of the
//!   reproducibility contract.
//!
//! ## Rust concepts used here
//!
//! The input type `&[&[f32]]` is a borrowed slice of borrowed vector slices.
//! It resembles a Java array of read-only views or a C array of `const float *`,
//! but Rust proves that neither the outer collection nor the vector storage can
//! disappear while training is running. Training allocates and returns owned
//! `Vec<Vec<f32>>` centroids, so the caller can keep them after the borrows end.
//! Buffer swapping in `train_lloyds` transfers ownership of two `Vec` handles
//! without copying their floating-point contents.

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use tracing::{debug, info, warn};

use crate::error::{Result, ZeppelinError};

/// Largest data-set size trained with full Lloyd passes.
///
/// A set with exactly this many rows still uses Lloyd's algorithm; the
/// mini-batch path begins at the next row.
const MINI_BATCH_THRESHOLD: usize = 10_000;

/// Minimum vectors sampled by one mini-batch iteration.
const DEFAULT_BATCH_SIZE: usize = 1024;

/// Training rows sampled per centroid by a mini-batch iteration.
const BATCH_ROWS_PER_CENTROID: usize = 32;

/// Fixed worker count keeps balance-repair reduction order reproducible.
const BALANCE_REPAIR_WORKERS: usize = 12;

/// Resolves a k-scaled mini-batch without exceeding the available rows.
#[must_use]
fn mini_batch_size(n: usize, k: usize) -> usize {
    let scaled = k
        .checked_mul(BATCH_ROWS_PER_CENTROID)
        .unwrap_or_else(|| panic!("k-means mini-batch size overflow"));
    DEFAULT_BATCH_SIZE.max(scaled).min(n)
}

/// Trains deterministic centroids for an IVF index or quantization codebook.
///
/// K-means++ chooses spread-out seeds, after which the function automatically
/// selects full Lloyd passes or mini-batch updates according to the number of
/// rows. A centroid is an average-like representative; assigning a vector to
/// its nearest centroid determines which IVF cluster stores it.
///
/// # Parameters
///
/// - `vectors`: Ordered borrowed training vectors. Every vector must have
///   exactly `dim` finite values.
/// - `dim`: Number of floating-point components in each vector.
/// - `k`: Requested number of centroids. Values greater than the number of
///   vectors are reduced to the vector count and logged.
/// - `max_iters`: Maximum refinement passes after initialization. Zero returns
///   the initialized centroids.
/// - `epsilon`: Exclusive convergence threshold for the maximum **squared** L2
///   centroid movement measured by a pass or convergence checkpoint.
///
/// # Returns
/// Owned centroids in deterministic training order. The outer length is
/// `min(k, vectors.len())`; each inner vector has `dim` components when the
/// input contract is satisfied.
///
/// # Errors
///
/// Returns [`ZeppelinError::Index`] when the data set is empty or `k` is zero.
/// Initialization also reports an index error if its internal non-empty
/// centroid invariant is violated. No partial persistent artifact exists
/// because this function performs only in-memory CPU work.
///
/// # Panics
///
/// Debug builds panic when a component is NaN or infinite or when distance
/// operands have different lengths. Malformed vector dimensions can also cause
/// indexing panics; callers must validate the stated dimension contract.
///
/// # Performance
///
/// K-means++ costs `O(n * k * dim)`. Each Lloyd pass has the same order and
/// stores one assignment per vector. Mini-batch passes score
/// `min(n, max(1,024, 32 * k))` sampled vectors and also shuffle an `O(n)`
/// index array on each pass.
/// Centroid buffers occupy `O(k * dim)` floats.
///
/// # Examples
///
/// With 100 two-dimensional vectors forming groups near `(0, 0)` and `(10,
/// 10)`, requesting two centroids produces one representative near each group.
/// Repeating the call with the same ordered values and parameters produces the
/// same centroid order and values. Requesting five centroids from only two rows
/// returns two centroids rather than inventing three empty clusters.
///
/// # Rust Notes for Java/C Engineers
///
/// The function borrows all input storage and returns newly owned storage. It
/// never clones the complete data set: only chosen seed rows and centroid
/// buffers are allocated. Java references do not encode this non-retention
/// promise, and a C API would need conventions around pointer lifetime and
/// mutability; Rust checks both at compile time.
pub fn train_kmeans(
    vectors: &[&[f32]],
    dim: usize,
    k: usize,
    max_iters: usize,
    epsilon: f64,
) -> Result<Vec<Vec<f32>>> {
    let n = vectors.len();

    if n == 0 {
        return Err(ZeppelinError::Index(
            "cannot train k-means on empty dataset".into(),
        ));
    }
    if k == 0 {
        return Err(ZeppelinError::Index("k must be > 0".into()));
    }

    // Non-finite inputs must never reach centroid math: a single NaN/inf
    // silently corrupts every centroid it touches. The API boundary rejects
    // them and compaction skips pre-fix bad data before calling us; this
    // assert catches any future call path that forgets the guard.
    debug_assert!(
        vectors.iter().all(|v| v.iter().all(|x| x.is_finite())),
        "train_kmeans received non-finite vector values; callers must filter these out"
    );

    // If we have fewer points than centroids, just use the points as centroids.
    let effective_k = k.min(n);
    if effective_k < k {
        warn!(
            requested_k = k,
            actual_k = effective_k,
            n = n,
            "fewer vectors than centroids, reducing k"
        );
    }

    info!(
        n = n,
        k = effective_k,
        dim = dim,
        "starting k-means++ initialization"
    );

    let seed = deterministic_seed(vectors, dim, effective_k, max_iters, epsilon);
    let mut rng = StdRng::seed_from_u64(seed);

    // --- k-means++ initialization ---
    let centroids = kmeans_pp_init(vectors, dim, effective_k, &mut rng)?;

    // Choose training mode based on dataset size
    if n > MINI_BATCH_THRESHOLD {
        let batch_size = mini_batch_size(n, effective_k);
        info!(
            n = n,
            batch_size = batch_size,
            "using mini-batch k-means (dataset exceeds threshold)"
        );
        train_mini_batch(
            vectors,
            dim,
            effective_k,
            max_iters,
            epsilon,
            centroids,
            &mut rng,
        )
    } else {
        train_lloyds(vectors, dim, effective_k, max_iters, epsilon, centroids)
    }
}

/// Refines centroids by repeatedly assigning every row and recomputing means.
///
/// # Parameters
///
/// - `vectors`: Validated training vectors borrowed from the caller.
/// - `dim`: Component count shared by every vector and centroid.
/// - `k`: Number of centroids and assignment buckets.
/// - `max_iters`: Maximum complete assignment/update passes.
/// - `epsilon`: Exclusive convergence threshold for maximum squared movement.
/// - `centroids`: Owned initial centroids, normally from [`kmeans_pp_init`].
///
/// # Returns
///
/// The refined owned centroid buffers. If convergence is not reached, returns
/// the state after `max_iters` and logs that fact.
///
/// # Errors
///
/// The current implementation has no recoverable error after its preconditions
/// are met; the `Result` keeps this training mode interchangeable with the
/// initialization and mini-batch stages.
///
/// # Panics
///
/// Panics when vector, centroid, `dim`, and `k` shapes disagree. These are
/// internal caller-contract violations.
///
/// # Performance
///
/// Each pass costs `O(n * k * dim)` and reuses assignment, count, and centroid
/// accumulator allocations across passes.
///
/// # Examples
///
/// Four rows around two distant locations are assigned to the nearer of two
/// seeds. The next centroids become the two cluster means; passes stop once the
/// largest squared movement is below `epsilon`.
///
/// # Rust Notes for Java/C Engineers
///
/// `std::mem::swap` exchanges the two owned `Vec<Vec<f32>>` handles after each
/// pass. Unlike copying Java arrays or `memcpy`-ing C buffers, it moves only
/// small vector descriptors; both heap allocations remain valid and are reused.
fn train_lloyds(
    vectors: &[&[f32]],
    dim: usize,
    k: usize,
    max_iters: usize,
    epsilon: f64,
    mut centroids: Vec<Vec<f32>>,
) -> Result<Vec<Vec<f32>>> {
    let n = vectors.len();
    let mut assignments = vec![0usize; n];
    let mut counts = vec![0usize; k];
    let mut new_centroids = vec![vec![0.0f32; dim]; k];

    for iter in 0..max_iters {
        // Assignment step: assign each vector to the nearest centroid.
        for (i, vec) in vectors.iter().enumerate() {
            let mut best_dist = f32::MAX;
            let mut best_idx = 0usize;
            for (c, centroid) in centroids.iter().enumerate() {
                let d = squared_l2(vec, centroid);
                if d < best_dist {
                    best_dist = d;
                    best_idx = c;
                }
            }
            assignments[i] = best_idx;
        }

        // Zero the accumulators.
        for (c, new_centroid) in new_centroids.iter_mut().enumerate().take(k) {
            counts[c] = 0;
            for val in new_centroid.iter_mut() {
                *val = 0.0;
            }
        }

        // Accumulate sums.
        for (i, vec) in vectors.iter().enumerate() {
            let c = assignments[i];
            counts[c] += 1;
            for d in 0..dim {
                new_centroids[c][d] += vec[d];
            }
        }

        // Compute means and track maximum centroid shift.
        let mut max_shift: f64 = 0.0;
        for (c, new_centroid) in new_centroids.iter_mut().enumerate().take(k) {
            if counts[c] == 0 {
                // Empty cluster: keep old centroid (degenerate but safe).
                new_centroid.copy_from_slice(&centroids[c]);
                continue;
            }
            let inv = 1.0 / counts[c] as f32;
            for val in new_centroid.iter_mut() {
                *val *= inv;
            }
            let shift = squared_l2(&centroids[c], new_centroid) as f64;
            if shift > max_shift {
                max_shift = shift;
            }
        }

        // Swap buffers.
        std::mem::swap(&mut centroids, &mut new_centroids);

        debug!(
            iter = iter + 1,
            max_shift = max_shift,
            epsilon = epsilon,
            "k-means Lloyd's iteration complete"
        );

        if max_shift < epsilon {
            info!(
                iterations = iter + 1,
                max_shift = max_shift,
                "k-means converged"
            );
            return Ok(centroids);
        }
    }

    warn!(
        max_iters = max_iters,
        "k-means did not converge within iteration limit, using current centroids"
    );
    Ok(centroids)
}

/// Refines centroids from a bounded random sample on each iteration.
///
/// Each iteration samples `batch_size` vectors instead of scanning all N.
/// Uses an online learning rate: `eta = 1 / (count[c] + 1)` per centroid,
/// so centroids converge gradually without needing to track full assignments.
///
/// Reference: Sculley (2010), "Web-Scale K-Means Clustering"
///
/// # Parameters
///
/// - `vectors`: Validated training vectors borrowed from the caller.
/// - `dim`: Component count shared by every vector and centroid.
/// - `k`: Number of centroid buckets.
/// - `max_iters`: Maximum sampled update passes.
/// - `epsilon`: Exclusive maximum squared-shift threshold checked every five
///   passes and on the final pass.
/// - `centroids`: Owned k-means++ seeds updated in place.
/// - `rng`: Deterministically seeded generator used to shuffle row indexes.
///
/// # Returns
///
/// The owned online-updated centroids, including the current centroids if the
/// iteration limit is reached before convergence.
///
/// # Errors
///
/// The current implementation returns no recoverable error after validated
/// inputs; its `Result` matches the surrounding training pipeline.
///
/// # Panics
///
/// Panics if vector shapes, centroid shapes, or `k` disagree.
///
/// # Performance
///
/// Each pass shuffles `O(n)` indexes and scores
/// `min(n, max(1,024, 32 * k))` rows against all `k` centroids. It allocates
/// the index vector once and an `O(batch_size)` assignment vector per pass;
/// centroid history costs `O(k * dim)`.
///
/// # Examples
///
/// For twelve thousand rows and 256 centroids, a pass shuffles row indexes,
/// uses the first 8,192, and nudges each winning centroid by
/// `1 / observations_for_that_centroid`. Later observations therefore make
/// progressively smaller changes.
///
/// # Rust Notes for Java/C Engineers
///
/// `rng: &mut StdRng` is an exclusive borrow: while this function runs, no
/// other code may use that generator. This is similar to passing a mutable C
/// pointer, but Rust prevents aliases that could race; Java normally relies on
/// object discipline or synchronization for the same property.
fn train_mini_batch(
    vectors: &[&[f32]],
    dim: usize,
    k: usize,
    max_iters: usize,
    epsilon: f64,
    mut centroids: Vec<Vec<f32>>,
    rng: &mut StdRng,
) -> Result<Vec<Vec<f32>>> {
    let n = vectors.len();
    let batch_size = mini_batch_size(n, k);

    // Per-centroid sample count (for learning rate decay)
    let mut centroid_counts = vec![0u64; k];

    // Indices buffer for sampling
    let mut indices: Vec<usize> = (0..n).collect();

    // Previous centroids for convergence check
    let mut prev_centroids = centroids.clone();

    for iter in 0..max_iters {
        // Sample a mini-batch (shuffle and take first batch_size)
        indices.shuffle(rng);
        let batch = &indices[..batch_size];

        // Assign batch vectors to nearest centroids
        let mut batch_assignments = Vec::with_capacity(batch_size);
        for &idx in batch {
            let vec = vectors[idx];
            let mut best_dist = f32::MAX;
            let mut best_idx = 0usize;
            for (c, centroid) in centroids.iter().enumerate() {
                let d = squared_l2(vec, centroid);
                if d < best_dist {
                    best_dist = d;
                    best_idx = c;
                }
            }
            batch_assignments.push((idx, best_idx));
        }

        // Update centroids with online learning rate
        for &(vec_idx, centroid_idx) in &batch_assignments {
            centroid_counts[centroid_idx] += 1;
            let eta = 1.0 / centroid_counts[centroid_idx] as f32;
            let vec = vectors[vec_idx];
            let centroid = &mut centroids[centroid_idx];
            for d in 0..dim {
                centroid[d] = (1.0 - eta) * centroid[d] + eta * vec[d];
            }
        }

        // Check convergence every 5 iterations (comparing to previous check)
        if (iter + 1) % 5 == 0 || iter == max_iters - 1 {
            let mut max_shift: f64 = 0.0;
            for c in 0..k {
                let shift = squared_l2(&prev_centroids[c], &centroids[c]) as f64;
                if shift > max_shift {
                    max_shift = shift;
                }
            }

            debug!(
                iter = iter + 1,
                max_shift = max_shift,
                epsilon = epsilon,
                "mini-batch k-means convergence check"
            );

            if max_shift < epsilon {
                info!(
                    iterations = iter + 1,
                    max_shift = max_shift,
                    "mini-batch k-means converged"
                );
                return Ok(centroids);
            }

            // Save current centroids for next convergence check
            for c in 0..k {
                prev_centroids[c].copy_from_slice(&centroids[c]);
            }
        }
    }

    warn!(
        max_iters = max_iters,
        "mini-batch k-means did not converge, using current centroids"
    );
    Ok(centroids)
}

/// Repairs pathologically overfull clusters by deterministic centroid splits.
///
/// Each round assigns all rows in fixed contiguous worker ranges, joins those
/// ranges in row order, and recomputes centroid means in input order. The
/// largest overfull clusters donate their farthest, lowest-index row to the
/// emptiest available centroid slots. Rows naturally re-home on the next
/// round; no row is copied or persisted by this CPU-only stage.
///
/// A `max_ratio` of zero explicitly disables repair. Reaching `max_rounds`
/// emits a warning and returns the last deterministic centroid state.
pub(crate) fn repair_cluster_balance(
    vectors: &[&[f32]],
    dim: usize,
    centroids: &mut [Vec<f32>],
    max_ratio: f64,
    max_rounds: usize,
) {
    if max_ratio == 0.0 {
        return;
    }
    assert!(
        max_ratio.is_finite() && max_ratio >= 1.0,
        "balance max ratio must be zero or finite and at least one"
    );
    assert!(
        max_rounds > 0,
        "enabled balance repair requires at least one round"
    );
    assert!(!vectors.is_empty(), "balance repair requires input rows");
    assert!(!centroids.is_empty(), "balance repair requires centroids");
    assert!(
        vectors.iter().all(|vector| vector.len() == dim),
        "balance repair vector dimension mismatch"
    );
    assert!(
        centroids.iter().all(|centroid| centroid.len() == dim),
        "balance repair centroid dimension mismatch"
    );

    let cluster_count = centroids.len();
    let mean_occupancy = vectors.len() as f64 / cluster_count as f64;
    let worker_count = BALANCE_REPAIR_WORKERS.min(vectors.len());
    let rows_per_worker = vectors.len().div_ceil(worker_count);

    for round in 0..max_rounds {
        let centroid_view: &[Vec<f32>] = centroids;
        let partials: Vec<(usize, Vec<usize>, Vec<f32>)> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..worker_count)
                .filter_map(|worker| {
                    let start = worker * rows_per_worker;
                    let end = ((worker + 1) * rows_per_worker).min(vectors.len());
                    (start < end).then(|| {
                        scope.spawn(move || {
                            let mut assignments = Vec::with_capacity(end - start);
                            let mut distances = Vec::with_capacity(end - start);
                            for vector in &vectors[start..end] {
                                let mut best_cluster = 0usize;
                                let mut best_distance = f32::MAX;
                                for (cluster, centroid) in centroid_view.iter().enumerate() {
                                    let distance = squared_l2(vector, centroid);
                                    if distance < best_distance {
                                        best_distance = distance;
                                        best_cluster = cluster;
                                    }
                                }
                                assignments.push(best_cluster);
                                distances.push(best_distance);
                            }
                            (start, assignments, distances)
                        })
                    })
                })
                .collect();

            handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .unwrap_or_else(|_| panic!("balance repair worker panicked"))
                })
                .collect()
        });

        let mut assignments = vec![0usize; vectors.len()];
        let mut distances = vec![0.0f32; vectors.len()];
        for (start, worker_assignments, worker_distances) in partials {
            let end = start + worker_assignments.len();
            assignments[start..end].copy_from_slice(&worker_assignments);
            distances[start..end].copy_from_slice(&worker_distances);
        }

        let mut counts = vec![0usize; cluster_count];
        let mut sums = vec![vec![0.0f64; dim]; cluster_count];
        for (row, &cluster) in assignments.iter().enumerate() {
            counts[cluster] += 1;
            for (sum, &value) in sums[cluster].iter_mut().zip(vectors[row]) {
                *sum += f64::from(value);
            }
        }

        let mut overfull: Vec<usize> = (0..cluster_count)
            .filter(|&cluster| counts[cluster] as f64 > max_ratio * mean_occupancy)
            .collect();
        overfull.sort_by(|&left, &right| {
            counts[right]
                .cmp(&counts[left])
                .then_with(|| left.cmp(&right))
        });
        if overfull.is_empty() {
            info!(
                rounds = round,
                max_ratio = max_ratio,
                "k-means balance repair converged"
            );
            return;
        }

        for cluster in 0..cluster_count {
            if counts[cluster] == 0 {
                continue;
            }
            let inverse = 1.0 / counts[cluster] as f64;
            for (value, &sum) in centroids[cluster].iter_mut().zip(&sums[cluster]) {
                *value = (sum * inverse) as f32;
            }
        }

        let mut donors: Vec<usize> = (0..cluster_count).collect();
        donors.sort_by_key(|&cluster| (counts[cluster], cluster));
        let mut is_overfull = vec![false; cluster_count];
        for &cluster in &overfull {
            is_overfull[cluster] = true;
        }
        let mut used_donors = vec![false; cluster_count];
        let mut splits = Vec::with_capacity(overfull.len());
        for &source in &overfull {
            let donor = donors
                .iter()
                .copied()
                .find(|&candidate| {
                    candidate != source && !is_overfull[candidate] && !used_donors[candidate]
                })
                .unwrap_or_else(|| {
                    panic!("balance repair requires one donor per overfull cluster")
                });
            used_donors[donor] = true;

            let mut farthest_row = None;
            let mut farthest_distance = f32::NEG_INFINITY;
            for (row, (&cluster, &distance)) in assignments.iter().zip(&distances).enumerate() {
                if cluster == source && distance > farthest_distance {
                    farthest_row = Some(row);
                    farthest_distance = distance;
                }
            }
            splits.push((
                donor,
                farthest_row.unwrap_or_else(|| panic!("overfull cluster must contain a row")),
            ));
        }

        for (donor, row) in splits.iter().copied() {
            centroids[donor].copy_from_slice(vectors[row]);
        }
        info!(
            round = round + 1,
            splits = splits.len(),
            max_occupancy = counts
                .iter()
                .copied()
                .max()
                .unwrap_or_else(|| panic!("balance repair occupancy cannot be empty")),
            max_ratio = max_ratio,
            "k-means balance repair split overfull clusters"
        );
    }

    warn!(
        max_rounds = max_rounds,
        max_ratio = max_ratio,
        "k-means balance repair exhausted its round budget"
    );
}

/// Chooses spread-out initial centroids with k-means++ weighted sampling.
///
/// After a uniform first row, each new seed is sampled in proportion to its
/// squared distance from the nearest existing seed. Coincident data cannot
/// provide distinct seeds, so remaining slots deliberately duplicate the last
/// centroid.
///
/// # Parameters
///
/// - `vectors`: Non-empty validated training rows.
/// - `dim`: Expected component count for each selected seed.
/// - `k`: Number of seeds to return; must be between one and `vectors.len()`.
/// - `rng`: Exclusive access to the caller's deterministic random stream.
///
/// # Returns
///
/// `k` owned centroid vectors. Selecting a seed clones only that row's float
/// buffer into the result.
///
/// # Errors
///
/// Returns an index error only if the invariant that at least one centroid has
/// already been chosen is unexpectedly violated while duplicating coincident
/// points.
///
/// # Panics
///
/// An empty input panics during first-seed selection. Zero `k` or inconsistent
/// dimensions can trip debug assertions, and malformed dimensions can panic
/// during distance calculation. [`train_kmeans`] establishes the non-empty,
/// positive, dimension-consistent preconditions and bounds `k` to the row count.
///
/// # Performance
///
/// Costs `O(n * k * dim)`, stores one minimum distance per row, and allocates
/// `k * dim` floats for the result.
///
/// # Examples
///
/// After choosing a seed near `(0, 0)`, rows near `(10, 10)` receive much more
/// sampling weight than neighboring rows, making a distant second seed likely.
fn kmeans_pp_init(
    vectors: &[&[f32]],
    dim: usize,
    k: usize,
    rng: &mut StdRng,
) -> Result<Vec<Vec<f32>>> {
    let n = vectors.len();

    let mut centroids: Vec<Vec<f32>> = Vec::with_capacity(k);

    // Pick the first centroid uniformly at random.
    let first_idx = rng.gen_range(0..n);
    centroids.push(vectors[first_idx].to_vec());

    // Distance from each point to the nearest centroid chosen so far.
    let mut min_dists = vec![f32::MAX; n];

    for c in 1..k {
        // Update min distances with the last-added centroid.
        let last = &centroids[c - 1];
        let mut total_dist: f64 = 0.0;
        for (i, vec) in vectors.iter().enumerate() {
            let d = squared_l2(vec, last);
            if d < min_dists[i] {
                min_dists[i] = d;
            }
            total_dist += min_dists[i] as f64;
        }

        if total_dist <= 0.0 {
            // All remaining points coincide with existing centroids.
            // Fill remaining centroids with the last known centroid.
            warn!(
                chosen = c,
                k = k,
                "all remaining distances are zero, duplicating last centroid"
            );
            while centroids.len() < k {
                centroids.push(
                    centroids
                        .last()
                        .ok_or_else(|| ZeppelinError::Index("empty centroids in k-means++".into()))?
                        .clone(),
                );
            }
            return Ok(centroids);
        }

        // Weighted random selection.
        let threshold = rng.gen::<f64>() * total_dist;
        let mut cumulative: f64 = 0.0;
        let mut chosen = n - 1; // fallback
        for (i, &d) in min_dists.iter().enumerate() {
            cumulative += d as f64;
            if cumulative >= threshold {
                chosen = i;
                break;
            }
        }

        centroids.push(vectors[chosen].to_vec());
        debug!(
            centroid = c,
            chosen_idx = chosen,
            "k-means++ selected centroid"
        );
    }

    debug_assert_eq!(centroids.len(), k);
    debug_assert!(centroids.iter().all(|c| c.len() == dim));

    Ok(centroids)
}

/// Derives the reproducible random seed from all training inputs and options.
///
/// The hash incorporates vector order, dimensions, parameters, and the exact
/// IEEE-754 bit pattern of every component. This is for reproducibility, not
/// security or persisted checksum validation.
///
/// # Parameters
///
/// - `vectors`: Ordered borrowed vector data included in the hash.
/// - `dim`: Declared vector dimension.
/// - `k`: Effective centroid count.
/// - `max_iters`: Requested iteration cap.
/// - `epsilon`: Convergence threshold, hashed by its raw bits.
///
/// # Returns
///
/// A deterministic 64-bit seed. Changing input order or a floating-point bit
/// pattern normally changes the seed.
///
/// # Examples
///
/// Two builds over the same ordered segment rows obtain the same seed and
/// therefore the same k-means++ sampling stream.
#[must_use]
fn deterministic_seed(
    vectors: &[&[f32]],
    dim: usize,
    k: usize,
    max_iters: usize,
    epsilon: f64,
) -> u64 {
    // Standard FNV-1a offset basis starts the stable cross-process hash.
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    // Standard FNV-1a prime mixes each input byte with wrapping arithmetic.
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    /// Mixes one 64-bit value into the local FNV-1a state byte by byte.
    fn mix(hash: &mut u64, value: u64) {
        for byte in value.to_le_bytes() {
            *hash ^= u64::from(byte);
            *hash = hash.wrapping_mul(FNV_PRIME);
        }
    }

    let mut hash = FNV_OFFSET;
    mix(&mut hash, vectors.len() as u64);
    mix(&mut hash, dim as u64);
    mix(&mut hash, k as u64);
    mix(&mut hash, max_iters as u64);
    mix(&mut hash, epsilon.to_bits());
    for vector in vectors {
        mix(&mut hash, vector.len() as u64);
        for &value in *vector {
            mix(&mut hash, u64::from(value.to_bits()));
        }
    }
    hash
}

/// Computes squared Euclidean (L2) distance without taking a square root.
///
/// # Parameters
///
/// - `a`: First borrowed vector.
/// - `b`: Second borrowed vector with the same length as `a`.
///
/// # Returns
///
/// The sum of squared component differences. Squared distance preserves nearest
/// ordering while avoiding the square-root cost.
///
/// # Panics
///
/// Debug builds assert equal lengths. Any build panics if `b` is shorter than
/// `a`; callers must always provide equal shapes.
///
/// # Examples
///
/// The distance between `[1, 2, 3]` and `[4, 5, 6]` is `27`.
#[inline]
fn squared_l2(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut sum = 0.0f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum
}

/// Unit tests for training validation, quality, mode selection, and determinism.
#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    use super::*;

    /// A single row is returned unchanged when one centroid is requested.
    #[test]
    fn test_train_single_point() {
        let data = [vec![1.0, 2.0, 3.0]];
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();
        let centroids = train_kmeans(&refs, 3, 1, 10, 1e-4).unwrap();
        assert_eq!(centroids.len(), 1);
        assert_eq!(centroids[0], vec![1.0, 2.0, 3.0]);
    }

    /// Requests larger than the data set reduce the centroid count to `n`.
    #[test]
    fn test_train_k_gt_n() {
        let data = [vec![1.0, 0.0], vec![0.0, 1.0]];
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();
        // k=5 but only 2 points -> should produce 2 centroids.
        let centroids = train_kmeans(&refs, 2, 5, 10, 1e-4).unwrap();
        assert_eq!(centroids.len(), 2);
    }

    /// Empty training data is rejected instead of creating meaningless output.
    #[test]
    fn test_train_empty() {
        let refs: Vec<&[f32]> = vec![];
        let result = train_kmeans(&refs, 3, 2, 10, 1e-4);
        assert!(result.is_err());
    }

    /// Lloyd iterations separate two obvious clusters and settle near their means.
    #[test]
    fn test_train_converges() {
        // Two well-separated clusters.
        let mut data = Vec::new();
        for i in 0..50 {
            data.push(vec![i as f32 * 0.01, 0.0]);
        }
        for i in 0..50 {
            data.push(vec![10.0 + i as f32 * 0.01, 0.0]);
        }
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();
        let centroids = train_kmeans(&refs, 2, 2, 100, 1e-6).unwrap();

        // Centroids should be near ~0.245 and ~10.245.
        let c0 = centroids[0][0].min(centroids[1][0]);
        let c1 = centroids[0][0].max(centroids[1][0]);
        assert!(c0 < 1.0, "lower centroid should be near 0, got {c0}");
        assert!(c1 > 9.0, "upper centroid should be near 10, got {c1}");
    }

    /// The distance primitive sums squared component deltas exactly for integers.
    #[test]
    fn test_squared_l2() {
        let a = [1.0, 2.0, 3.0];
        let b = [4.0, 5.0, 6.0];
        // (3^2 + 3^2 + 3^2) = 27
        assert!((squared_l2(&a, &b) - 27.0).abs() < 1e-6);
    }

    /// Data above the threshold exercises mini-batch training without losing separation.
    #[test]
    fn test_mini_batch_two_clusters() {
        // Generate enough data to trigger mini-batch mode
        let mut data = Vec::new();
        // Cluster near 0
        for i in 0..6000 {
            data.push(vec![(i % 100) as f32 * 0.01, 0.0]);
        }
        // Cluster near 10
        for i in 0..6000 {
            data.push(vec![10.0 + (i % 100) as f32 * 0.01, 0.0]);
        }
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();
        assert!(refs.len() > MINI_BATCH_THRESHOLD); // Verify mini-batch will be used

        let centroids = train_kmeans(&refs, 2, 2, 50, 1e-4).unwrap();
        let c0 = centroids[0][0].min(centroids[1][0]);
        let c1 = centroids[0][0].max(centroids[1][0]);
        assert!(c0 < 2.0, "lower centroid should be near 0, got {c0}");
        assert!(c1 > 8.0, "upper centroid should be near 10, got {c1}");
    }

    /// Direct Lloyd training preserves the two-cluster result after k-means++ seeding.
    #[test]
    fn test_lloyds_directly() {
        // Test full Lloyd's with a small dataset (below threshold)
        let data = [
            vec![0.0, 0.0],
            vec![0.1, 0.1],
            vec![10.0, 10.0],
            vec![10.1, 10.1],
        ];
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();
        let mut rng = StdRng::seed_from_u64(deterministic_seed(&refs, 2, 2, 100, 1e-6));
        let centroids = kmeans_pp_init(&refs, 2, 2, &mut rng).unwrap();
        let result = train_lloyds(&refs, 2, 2, 100, 1e-6, centroids).unwrap();
        assert_eq!(result.len(), 2);

        let c0 = result[0][0].min(result[1][0]);
        let c1 = result[0][0].max(result[1][0]);
        assert!(c0 < 1.0, "lower centroid should be near 0, got {c0}");
        assert!(c1 > 9.0, "upper centroid should be near 10, got {c1}");
    }

    /// Repeated calls with identical ordered inputs produce bit-identical centroids.
    #[test]
    fn test_train_kmeans_is_deterministic() {
        let data = [
            vec![0.0, 0.0],
            vec![0.1, 0.0],
            vec![10.0, 10.0],
            vec![10.1, 10.0],
            vec![20.0, 20.0],
            vec![20.1, 20.0],
        ];
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();

        let first = train_kmeans(&refs, 2, 3, 25, 1e-4).unwrap();
        let second = train_kmeans(&refs, 2, 3, 25, 1e-4).unwrap();

        assert_eq!(first, second);
    }

    /// The sampled training budget grows with centroid count and caps at N.
    #[test]
    fn mini_batch_budget_scales_with_centroid_count() {
        assert_eq!(mini_batch_size(2_000_000, 256), 8_192);
        assert_eq!(mini_batch_size(2_000_000, 667), 21_344);
        assert_eq!(mini_batch_size(100, 667), 100);
    }

    /// Repair deterministically splits a synthetic 10:1 occupancy skew.
    #[test]
    fn balance_repair_eliminates_synthetic_skew_deterministically() {
        let mut data = Vec::new();
        for row in 0..90 {
            data.push(vec![row as f32 * 0.01, 0.0]);
        }
        for row in 0..10 {
            data.push(vec![100.0 + row as f32 * 0.01, 0.0]);
        }
        let refs: Vec<&[f32]> = data.iter().map(Vec::as_slice).collect();
        let initial: Vec<Vec<f32>> = std::iter::once(vec![0.0, 0.0])
            .chain(std::iter::repeat_n(vec![100.0, 0.0], 9))
            .collect();
        let mut first = initial.clone();
        let mut second = initial;

        repair_cluster_balance(&refs, 2, &mut first, 4.0, 8);
        repair_cluster_balance(&refs, 2, &mut second, 4.0, 8);

        assert_eq!(first, second);
        let mut occupancy = vec![0usize; first.len()];
        for vector in &refs {
            let cluster = first
                .iter()
                .enumerate()
                .min_by(|(_, left), (_, right)| {
                    squared_l2(vector, left).total_cmp(&squared_l2(vector, right))
                })
                .unwrap()
                .0;
            occupancy[cluster] += 1;
        }
        assert!(
            occupancy.iter().copied().max().unwrap() <= 40,
            "repair left occupancy above 4x mean: {occupancy:?}"
        );
    }
}
