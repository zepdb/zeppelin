//! Property-based tests for IVF-Flat search recall.
//!
//! Verifies that IVF-Flat approximate nearest neighbor search achieves
//! acceptable recall compared to brute-force exact search.
//!
//! This test uses the production distance functions but simulates the
//! IVF-Flat clustering and probing logic without requiring S3.
//!
//! To run: add `proptest = "1"` to [dev-dependencies] in Cargo.toml, then
//!   cp formal-verifications/proptest/ivf_recall.rs tests/proptest_ivf_recall.rs
//!   cargo test --test proptest_ivf_recall

use proptest::prelude::*;
use std::collections::HashSet;

// ---- Distance Functions (mirrors src/index/distance.rs) ----

#[allow(dead_code)]
fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let mut dot: f32 = 0.0;
    let mut norm_a: f32 = 0.0;
    let mut norm_b: f32 = 0.0;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }
    let denom = (norm_a * norm_b).sqrt();
    if denom < f32::EPSILON {
        return 1.0;
    }
    1.0 - (dot / denom).clamp(-1.0, 1.0)
}

fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(ai, bi)| (ai - bi) * (ai - bi))
        .sum()
}

// ---- Simplified IVF-Flat Index ----

struct SimpleIvfFlat {
    centroids: Vec<Vec<f32>>,
    clusters: Vec<Vec<(String, Vec<f32>)>>, // cluster_id -> [(vec_id, vec_values)]
}

impl SimpleIvfFlat {
    /// Build an IVF-Flat index from vectors using simple k-means.
    fn build(vectors: &[(String, Vec<f32>)], num_clusters: usize, max_iters: usize) -> Self {
        let dim = vectors[0].1.len();
        let k = num_clusters.min(vectors.len());

        // Initialize centroids from first k vectors
        let mut centroids: Vec<Vec<f32>> = vectors[..k].iter().map(|(_, v)| v.clone()).collect();

        // Simple k-means
        for _ in 0..max_iters {
            let mut assignments: Vec<Vec<usize>> = vec![vec![]; k];

            // Assign each vector to nearest centroid
            for (i, (_, vec)) in vectors.iter().enumerate() {
                let nearest = centroids
                    .iter()
                    .enumerate()
                    .min_by(|(_, a), (_, b)| {
                        euclidean_distance(vec, a)
                            .partial_cmp(&euclidean_distance(vec, b))
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .map(|(idx, _)| idx)
                    .unwrap_or(0);
                assignments[nearest].push(i);
            }

            // Update centroids
            for (c, assigned) in assignments.iter().enumerate() {
                if assigned.is_empty() {
                    continue;
                }
                let mut new_centroid = vec![0.0f32; dim];
                for &idx in assigned {
                    for (d, val) in vectors[idx].1.iter().enumerate() {
                        new_centroid[d] += val;
                    }
                }
                let count = assigned.len() as f32;
                for val in &mut new_centroid {
                    *val /= count;
                }
                centroids[c] = new_centroid;
            }
        }

        // Build clusters
        let mut clusters: Vec<Vec<(String, Vec<f32>)>> = vec![vec![]; k];
        for (id, vec) in vectors {
            let nearest = centroids
                .iter()
                .enumerate()
                .min_by(|(_, a), (_, b)| {
                    euclidean_distance(vec, a)
                        .partial_cmp(&euclidean_distance(vec, b))
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|(idx, _)| idx)
                .unwrap_or(0);
            clusters[nearest].push((id.clone(), vec.clone()));
        }

        SimpleIvfFlat {
            centroids,
            clusters,
        }
    }

    /// Search the index with nprobe probing.
    fn search(&self, query: &[f32], top_k: usize, nprobe: usize) -> Vec<(String, f32)> {
        // Find nearest nprobe centroids
        let mut centroid_dists: Vec<(usize, f32)> = self
            .centroids
            .iter()
            .enumerate()
            .map(|(i, c)| (i, euclidean_distance(query, c)))
            .collect();
        centroid_dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        let probes = nprobe.min(centroid_dists.len());

        // Scan probed clusters
        let mut candidates: Vec<(String, f32)> = Vec::new();
        for &(cluster_idx, _) in &centroid_dists[..probes] {
            for (id, vec) in &self.clusters[cluster_idx] {
                let dist = euclidean_distance(query, vec);
                candidates.push((id.clone(), dist));
            }
        }

        // Sort by distance and take top_k
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        candidates.truncate(top_k);
        candidates
    }
}

/// Brute-force exact nearest neighbors.
fn brute_force_search(
    vectors: &[(String, Vec<f32>)],
    query: &[f32],
    top_k: usize,
) -> Vec<(String, f32)> {
    let mut results: Vec<(String, f32)> = vectors
        .iter()
        .map(|(id, vec)| (id.clone(), euclidean_distance(query, vec)))
        .collect();
    results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    results.truncate(top_k);
    results
}

/// Compute recall: fraction of brute-force top-k that appear in IVF results.
fn recall(ivf_results: &[(String, f32)], bf_results: &[(String, f32)]) -> f64 {
    if bf_results.is_empty() {
        return 1.0;
    }
    let bf_ids: HashSet<&String> = bf_results.iter().map(|(id, _)| id).collect();
    let hits = ivf_results
        .iter()
        .filter(|(id, _)| bf_ids.contains(id))
        .count();
    hits as f64 / bf_results.len() as f64
}

// ---- Proptest Strategies ----

fn arb_vector(dim: usize) -> impl Strategy<Value = (String, Vec<f32>)> {
    (
        prop::string::string_regex("v[0-9]{1,3}")
            .unwrap()
            .prop_filter("non-empty", |s| !s.is_empty()),
        prop::collection::vec(-10.0f32..10.0, dim..=dim),
    )
}

fn arb_dataset(
    dim: usize,
    min_size: usize,
    max_size: usize,
) -> impl Strategy<Value = Vec<(String, Vec<f32>)>> {
    prop::collection::vec(arb_vector(dim), min_size..=max_size)
        // Ensure unique IDs
        .prop_map(|mut vecs| {
            let mut seen = HashSet::new();
            vecs.retain(|(id, _)| seen.insert(id.clone()));
            vecs
        })
        .prop_filter("at least 2 vectors", |v| v.len() >= 2)
}

// ---- Property Tests ----

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))] // fewer cases (expensive)

    /// IVF-Flat with nprobe = num_clusters should have perfect recall
    /// (equivalent to brute-force since all clusters are probed).
    #[test]
    fn full_probe_perfect_recall(
        dataset in arb_dataset(4, 10, 30),
        query in prop::collection::vec(-10.0f32..10.0, 4..=4),
    ) {
        let num_clusters = 3;
        let index = SimpleIvfFlat::build(&dataset, num_clusters, 10);
        let top_k = 5.min(dataset.len());

        // nprobe = num_clusters means we search every cluster
        let ivf_results = index.search(&query, top_k, num_clusters);
        let bf_results = brute_force_search(&dataset, &query, top_k);

        let r = recall(&ivf_results, &bf_results);
        prop_assert!(r >= 0.99,
            "Full probe should have near-perfect recall, got {}", r);
    }

    /// IVF-Flat recall should be monotonically non-decreasing with nprobe.
    #[test]
    fn recall_increases_with_nprobe(
        dataset in arb_dataset(4, 15, 40),
        query in prop::collection::vec(-10.0f32..10.0, 4..=4),
    ) {
        let num_clusters = 4;
        let index = SimpleIvfFlat::build(&dataset, num_clusters, 10);
        let top_k = 5.min(dataset.len());
        let bf_results = brute_force_search(&dataset, &query, top_k);

        let mut prev_recall = 0.0f64;
        for nprobe in 1..=num_clusters {
            let ivf_results = index.search(&query, top_k, nprobe);
            let r = recall(&ivf_results, &bf_results);
            prop_assert!(r >= prev_recall - 0.01, // small tolerance for ties
                "Recall should not decrease: nprobe={} recall={} < prev={}",
                nprobe, r, prev_recall);
            prev_recall = r;
        }
    }

    /// With reasonable nprobe (>= num_clusters/2), recall should be decent.
    #[test]
    fn reasonable_probe_decent_recall(
        dataset in arb_dataset(4, 20, 50),
        query in prop::collection::vec(-10.0f32..10.0, 4..=4),
    ) {
        let num_clusters = 4;
        let nprobe = 2; // half of clusters
        let index = SimpleIvfFlat::build(&dataset, num_clusters, 10);
        let top_k = 5.min(dataset.len());

        let ivf_results = index.search(&query, top_k, nprobe);
        let bf_results = brute_force_search(&dataset, &query, top_k);

        let r = recall(&ivf_results, &bf_results);
        // With nprobe=2 out of 4 clusters, we expect reasonable recall
        prop_assert!(r >= 0.4,
            "Half-probe recall should be >= 0.4, got {}", r);
    }
}
