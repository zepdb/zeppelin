//! k-means++ initialization and Lloyd's iteration for IVF centroid training.
//!
//! This module is intentionally allocation-aware: centroid storage is
//! pre-allocated and reused across iterations to avoid per-iteration heap
//! churn on large datasets.

use rand::Rng;
use tracing::{debug, info, warn};

use crate::error::{Result, ZeppelinError};

/// Train `k` centroids from the given data points using k-means++
/// initialization followed by Lloyd's iterations.
///
/// # Arguments
/// * `vectors`     - Slice of data points, each of length `dim`.
/// * `dim`         - Dimensionality of each vector.
/// * `k`           - Number of centroids to produce.
/// * `max_iters`   - Maximum number of Lloyd iterations.
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
        return Err(ZeppelinError::Index("cannot train k-means on empty dataset".into()));
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

    info!(n = n, k = effective_k, dim = dim, "starting k-means++ initialization");

    // --- k-means++ initialization ---
    let mut centroids = kmeans_pp_init(vectors, dim, effective_k)?;

    // --- Lloyd's iterations ---
    let mut assignments = vec![0usize; n];
    let mut counts = vec![0usize; effective_k];
    // Scratch buffer for accumulating new centroids.
    let mut new_centroids = vec![vec![0.0f32; dim]; effective_k];

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
        for (c, new_centroid) in new_centroids.iter_mut().enumerate().take(effective_k) {
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
        for (c, new_centroid) in new_centroids.iter_mut().enumerate().take(effective_k) {
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
            "k-means iteration complete"
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

    // We exhausted iterations without converging.  This is acceptable for
    // IVF training -- the centroids are still usable, just not perfectly
    // converged.
    warn!(
        max_iters = max_iters,
        "k-means did not converge within iteration limit, using current centroids"
    );
    Ok(centroids)
}

/// k-means++ seeding: pick initial centroids with probability proportional
/// to squared distance from the nearest already-chosen centroid.
fn kmeans_pp_init(
    vectors: &[&[f32]],
    dim: usize,
    k: usize,
) -> Result<Vec<Vec<f32>>> {
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
        debug!(centroid = c, chosen_idx = chosen, "k-means++ selected centroid");
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
}
