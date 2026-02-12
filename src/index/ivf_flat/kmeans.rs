//! k-means++ initialization and Lloyd's/mini-batch iteration for IVF centroid training.
//!
//! This module provides two training modes:
//! - **Full Lloyd's**: Scans all vectors every iteration. Exact but O(k×n×d) per iter.
//! - **Mini-batch**: Samples a batch of vectors each iteration. 3-5x faster at scale.
//!
//! Both modes use k-means++ initialization and are allocation-aware:
//! centroid storage is pre-allocated and reused across iterations.

use rand::seq::SliceRandom;
use rand::Rng;
use tracing::{debug, info, warn};

use crate::error::{Result, ZeppelinError};

/// Threshold above which mini-batch k-means is used instead of full Lloyd's.
const MINI_BATCH_THRESHOLD: usize = 10_000;

/// Default mini-batch size (number of vectors sampled per iteration).
const DEFAULT_BATCH_SIZE: usize = 1024;

/// Train `k` centroids from the given data points using k-means++
/// initialization followed by Lloyd's or mini-batch iterations.
///
/// Automatically selects mini-batch mode when `n > MINI_BATCH_THRESHOLD`.
///
/// # Arguments
/// * `vectors`     - Slice of data points, each of length `dim`.
/// * `dim`         - Dimensionality of each vector.
/// * `k`           - Number of centroids to produce.
/// * `max_iters`   - Maximum number of iterations.
/// * `epsilon`     - Convergence threshold on the maximum centroid shift.
///
/// # Returns
/// A `Vec<Vec<f32>>` of length `k`, each inner vec of length `dim`.
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

    // --- k-means++ initialization ---
    let centroids = kmeans_pp_init(vectors, dim, effective_k)?;

    // Choose training mode based on dataset size
    if n > MINI_BATCH_THRESHOLD {
        info!(
            n = n,
            batch_size = DEFAULT_BATCH_SIZE,
            "using mini-batch k-means (dataset exceeds threshold)"
        );
        train_mini_batch(vectors, dim, effective_k, max_iters, epsilon, centroids)
    } else {
        train_lloyds(vectors, dim, effective_k, max_iters, epsilon, centroids)
    }
}

/// Full Lloyd's iteration (original algorithm).
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

/// Mini-batch k-means training.
///
/// Each iteration samples `batch_size` vectors instead of scanning all N.
/// Uses an online learning rate: `eta = 1 / (count[c] + 1)` per centroid,
/// so centroids converge gradually without needing to track full assignments.
///
/// Reference: Sculley (2010), "Web-Scale K-Means Clustering"
fn train_mini_batch(
    vectors: &[&[f32]],
    dim: usize,
    k: usize,
    max_iters: usize,
    epsilon: f64,
    mut centroids: Vec<Vec<f32>>,
) -> Result<Vec<Vec<f32>>> {
    let n = vectors.len();
    let batch_size = DEFAULT_BATCH_SIZE.min(n);
    let mut rng = rand::thread_rng();

    // Per-centroid sample count (for learning rate decay)
    let mut centroid_counts = vec![0u64; k];

    // Indices buffer for sampling
    let mut indices: Vec<usize> = (0..n).collect();

    // Previous centroids for convergence check
    let mut prev_centroids = centroids.clone();

    for iter in 0..max_iters {
        // Sample a mini-batch (shuffle and take first batch_size)
        indices.shuffle(&mut rng);
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

/// k-means++ seeding: pick initial centroids with probability proportional
/// to squared distance from the nearest already-chosen centroid.
fn kmeans_pp_init(vectors: &[&[f32]], dim: usize, k: usize) -> Result<Vec<Vec<f32>>> {
    let n = vectors.len();
    let mut rng = rand::thread_rng();

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
                centroids.push(centroids.last().unwrap().clone());
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

/// Squared L2 distance between two vectors.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_train_single_point() {
        let data = [vec![1.0, 2.0, 3.0]];
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();
        let centroids = train_kmeans(&refs, 3, 1, 10, 1e-4).unwrap();
        assert_eq!(centroids.len(), 1);
        assert_eq!(centroids[0], vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_train_k_gt_n() {
        let data = [vec![1.0, 0.0], vec![0.0, 1.0]];
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();
        // k=5 but only 2 points -> should produce 2 centroids.
        let centroids = train_kmeans(&refs, 2, 5, 10, 1e-4).unwrap();
        assert_eq!(centroids.len(), 2);
    }

    #[test]
    fn test_train_empty() {
        let refs: Vec<&[f32]> = vec![];
        let result = train_kmeans(&refs, 3, 2, 10, 1e-4);
        assert!(result.is_err());
    }

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

    #[test]
    fn test_squared_l2() {
        let a = [1.0, 2.0, 3.0];
        let b = [4.0, 5.0, 6.0];
        // (3^2 + 3^2 + 3^2) = 27
        assert!((squared_l2(&a, &b) - 27.0).abs() < 1e-6);
    }

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
        let centroids = kmeans_pp_init(&refs, 2, 2).unwrap();
        let result = train_lloyds(&refs, 2, 2, 100, 1e-6, centroids).unwrap();
        assert_eq!(result.len(), 2);

        let c0 = result[0][0].min(result[1][0]);
        let c1 = result[0][0].max(result[1][0]);
        assert!(c0 < 1.0, "lower centroid should be near 0, got {c0}");
        assert!(c1 > 9.0, "upper centroid should be near 10, got {c1}");
    }
}
