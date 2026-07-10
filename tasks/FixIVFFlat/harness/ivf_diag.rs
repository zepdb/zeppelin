//! Coarse-IVF diagnostic runner — reference harness for the FixIVFFlat
//! Phase 0 experiment (see tasks/FixIVFFlat/top2_experiment.md).
//!
//! Provenance: produced tasks/FixIVFFlat/results/baseline_matrix.jsonl on
//! 2026-07-09 at commit 75271612523250f04cdb466a683137fcbc709ba7. To run:
//! copy into src/bin/ of a SEPARATE worktree (never the main checkout) and
//! `cargo build --release --bin ivf_diag`. Measures exact in-probe recall
//! ceilings for production-fidelity IVF builds on the pinned devbench
//! datasets (wikidpr1m/wikidpr2m/dbpedia100k).
//!
//! Production fidelity:
//! - Training: `zeppelin::index::ivf_flat::kmeans::train_kmeans` (the exact
//!   production seam used by `build_ivf_flat`), full-corpus input, production
//!   defaults for iterations/epsilon unless overridden.
//! - Assignment: nearest centroid by `distance::euclidean_distance`, exactly
//!   as `build_ivf_flat` does (src/index/ivf_flat/build.rs:2736), or by
//!   cosine when testing the spherical-assignment hypothesis.
//! - Probing: centroids ranked by `cosine_distance` (production cosine
//!   namespaces, src/index/ivf_flat/search.rs:1109-1123) or by
//!   `euclidean_distance` for the L2-consistent contrast, stable-sorted with
//!   ties keeping centroid index order (matches production sort_by).
//!
//! No sketch, no SQ8, no S3: this isolates the coarse partition + probe
//! stages that upper-bound everything downstream.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::fs;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::time::Instant;

use zeppelin::index::distance::{cosine_distance, dot_product_distance, euclidean_distance};
use zeppelin::index::ivf_flat::kmeans::train_kmeans;

const TOP_K: usize = 100;

#[derive(Clone, Copy, PartialEq)]
enum Metric {
    L2,
    Cosine,
}

impl Metric {
    fn parse(s: &str) -> Metric {
        match s {
            "l2" => Metric::L2,
            "cosine" => Metric::Cosine,
            other => panic!("unknown metric {other:?} (want l2|cosine)"),
        }
    }
    fn name(self) -> &'static str {
        match self {
            Metric::L2 => "l2",
            Metric::Cosine => "cosine",
        }
    }
    fn dist(self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            Metric::L2 => euclidean_distance(a, b),
            Metric::Cosine => cosine_distance(a, b),
        }
    }
}

struct EvalSpec {
    assign: Metric,
    probe: Metric,
    nprobes: Vec<usize>,
}

#[derive(Clone, Copy, PartialEq)]
struct Candidate {
    score: f32,
    row: u32,
}

impl Eq for Candidate {}

// Max-heap on "worseness": the heap top is the WORST kept candidate so we can
// evict it. Better = higher score, ties broken by lower row index (matches the
// dataset builder's tie rule: score descending, then row ascending).
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // self is "greater" (worse, evicted first) when score is lower,
        // or score equal and row higher.
        other
            .score
            .partial_cmp(&self.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| self.row.cmp(&other.row))
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn retain_best(heap: &mut BinaryHeap<Candidate>, cand: Candidate, cap: usize) {
    if heap.len() < cap {
        heap.push(cand);
    } else if let Some(worst) = heap.peek() {
        // cand better than worst kept?
        if cand.cmp(worst) == Ordering::Less {
            heap.pop();
            heap.push(cand);
        }
    }
}

fn sorted_rows(heap: BinaryHeap<Candidate>) -> Vec<u32> {
    let mut v: Vec<Candidate> = heap.into_vec();
    v.sort_by(|a, b| a.cmp(b)); // best (Less) first
    v.into_iter().map(|c| c.row).collect()
}

struct DedupedTopK {
    heap: BinaryHeap<Candidate>,
    rows: HashSet<u32>,
}

impl DedupedTopK {
    fn new() -> Self {
        Self {
            heap: BinaryHeap::with_capacity(TOP_K + 1),
            rows: HashSet::with_capacity(TOP_K + 1),
        }
    }

    fn retain(&mut self, cand: Candidate) {
        if self.rows.contains(&cand.row) {
            return;
        }
        if self.heap.len() < TOP_K {
            self.rows.insert(cand.row);
            self.heap.push(cand);
        } else if self
            .heap
            .peek()
            .is_some_and(|worst| cand.cmp(worst) == Ordering::Less)
        {
            let evicted = self.heap.pop().expect("non-empty top-k heap");
            self.rows.remove(&evicted.row);
            self.rows.insert(cand.row);
            self.heap.push(cand);
        }
    }

    fn sorted_rows(self) -> Vec<u32> {
        sorted_rows(self.heap)
    }
}

fn recall_at_k(retrieved: &[u32], gt: &[u32], k: usize) -> f64 {
    let gt_k = &gt[..k];
    let hits = retrieved
        .iter()
        .take(k)
        .filter(|r| gt_k.contains(r))
        .count();
    hits as f64 / k as f64
}

fn read_exact_f32(path: &Path, expected_bytes: u64) -> Vec<f32> {
    let meta = fs::metadata(path).unwrap_or_else(|e| panic!("stat {path:?}: {e}"));
    assert_eq!(
        meta.len(),
        expected_bytes,
        "size mismatch for {path:?}: got {}, want {expected_bytes}",
        meta.len()
    );
    let mut file = fs::File::open(path).unwrap_or_else(|e| panic!("open {path:?}: {e}"));
    let mut buf = vec![0u8; expected_bytes as usize];
    file.read_exact(&mut buf)
        .unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    // Little-endian f32; this host is little-endian (aarch64).
    let mut out = vec![0f32; buf.len() / 4];
    for (i, chunk) in buf.chunks_exact(4).enumerate() {
        out[i] = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
    }
    out
}

fn read_exact_u32(path: &Path, expected_bytes: u64) -> Vec<u32> {
    let meta = fs::metadata(path).unwrap_or_else(|e| panic!("stat {path:?}: {e}"));
    assert_eq!(
        meta.len(),
        expected_bytes,
        "size mismatch for {path:?}: got {}, want {expected_bytes}",
        meta.len()
    );
    let mut file = fs::File::open(path).unwrap_or_else(|e| panic!("open {path:?}: {e}"));
    let mut buf = vec![0u8; expected_bytes as usize];
    file.read_exact(&mut buf)
        .unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    buf.chunks_exact(4)
        .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

/// Splits `total` items into contiguous per-thread ranges.
fn thread_ranges(total: usize, threads: usize) -> Vec<(usize, usize)> {
    let chunk = total.div_ceil(threads);
    (0..threads)
        .map(|t| (t * chunk, ((t + 1) * chunk).min(total)))
        .filter(|(s, e)| s < e)
        .collect()
}

/// Exact top-100 by dot product (stored vectors are L2-normalized so dot ==
/// cosine), tie-break score descending then row ascending — the dataset
/// builder's rule. Parallel over queries.
fn compute_prefix_ground_truth(
    corpus: &[f32],
    dim: usize,
    rows: usize,
    queries: &[f32],
    query_n: usize,
    threads: usize,
) -> Vec<u32> {
    let mut gt = vec![0u32; query_n * TOP_K];
    let ranges = thread_ranges(query_n, threads);
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for (qs, qe) in ranges {
            handles.push(scope.spawn(move || {
                let mut out = vec![0u32; (qe - qs) * TOP_K];
                for qi in qs..qe {
                    let q = &queries[qi * dim..(qi + 1) * dim];
                    let mut heap = BinaryHeap::with_capacity(TOP_K + 1);
                    for row in 0..rows {
                        let v = &corpus[row * dim..(row + 1) * dim];
                        let score = -dot_product_distance(q, v);
                        retain_best(
                            &mut heap,
                            Candidate {
                                score,
                                row: row as u32,
                            },
                            TOP_K,
                        );
                    }
                    let rows_sorted = sorted_rows(heap);
                    assert_eq!(rows_sorted.len(), TOP_K, "prefix GT underfilled");
                    out[(qi - qs) * TOP_K..(qi - qs + 1) * TOP_K].copy_from_slice(&rows_sorted);
                }
                (qs, out)
            }));
        }
        for h in handles {
            let (qs, out) = h.join().expect("GT thread panicked");
            gt[qs * TOP_K..qs * TOP_K + out.len()].copy_from_slice(&out);
        }
    });
    gt
}

fn percentile(sorted: &[usize], p: f64) -> usize {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

fn assign_primary(
    corpus: &[f32],
    dim: usize,
    rows: usize,
    centroids: &[Vec<f32>],
    metric: Metric,
    threads: usize,
) -> (Vec<u32>, Vec<u32>, Vec<f32>, Vec<f32>) {
    let mut primary = vec![0u32; rows];
    let mut secondary = vec![0u32; rows];
    let mut first_distance = vec![f32::MAX; rows];
    let mut second_distance = vec![f32::MAX; rows];
    let ranges = thread_ranges(rows, threads);
    let chunks: Vec<_> = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for (rs, re) in ranges {
            handles.push(scope.spawn(move || {
                let mut out = Vec::with_capacity(re - rs);
                for row in rs..re {
                    let vector = &corpus[row * dim..(row + 1) * dim];
                    let mut best = (0u32, f32::MAX);
                    let mut second = (0u32, f32::MAX);
                    for (cluster, centroid) in centroids.iter().enumerate() {
                        let distance = metric.dist(vector, centroid);
                        if distance < best.1 {
                            second = best;
                            best = (cluster as u32, distance);
                        } else if distance < second.1 {
                            second = (cluster as u32, distance);
                        }
                    }
                    out.push((best.0, second.0, best.1, second.1));
                }
                (rs, out)
            }));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().expect("assignment thread panicked"))
            .collect()
    });
    for (start, chunk) in chunks {
        for (offset, (best, second, d1, d2)) in chunk.into_iter().enumerate() {
            let row = start + offset;
            primary[row] = best;
            secondary[row] = second;
            first_distance[row] = d1;
            second_distance[row] = d2;
        }
    }
    (primary, secondary, first_distance, second_distance)
}

fn repair_balance(
    corpus: &[f32],
    dim: usize,
    rows: usize,
    centroids: &mut [Vec<f32>],
    max_ratio: f64,
    rounds: usize,
    threads: usize,
    label: &str,
) {
    if max_ratio == 0.0 || rounds == 0 {
        return;
    }
    assert!(max_ratio >= 1.0, "--balance-max-ratio must be 0 or >= 1");
    let k = centroids.len();
    let mean = rows as f64 / k as f64;
    for round in 0..rounds {
        let (primary, _, distances, _) =
            assign_primary(corpus, dim, rows, centroids, Metric::L2, threads);
        let mut counts = vec![0usize; k];
        let mut sums = vec![vec![0f64; dim]; k];
        for (row, &cluster) in primary.iter().enumerate() {
            let cluster = cluster as usize;
            counts[cluster] += 1;
            let vector = &corpus[row * dim..(row + 1) * dim];
            for (sum, &value) in sums[cluster].iter_mut().zip(vector) {
                *sum += f64::from(value);
            }
        }
        let mut overfull: Vec<usize> = (0..k)
            .filter(|&cluster| counts[cluster] as f64 > max_ratio * mean)
            .collect();
        overfull.sort_by(|&a, &b| counts[b].cmp(&counts[a]).then_with(|| a.cmp(&b)));
        if overfull.is_empty() {
            eprintln!("[{label}] balance repair converged after {round} rounds");
            return;
        }

        let mut donors: Vec<usize> = (0..k).collect();
        donors.sort_by_key(|&cluster| (counts[cluster], cluster));
        let mut used_donors = HashSet::with_capacity(overfull.len());
        let mut splits = Vec::with_capacity(overfull.len());
        for &source in &overfull {
            let donor = donors
                .iter()
                .copied()
                .find(|candidate| {
                    *candidate != source
                        && !overfull.contains(candidate)
                        && !used_donors.contains(candidate)
                })
                .expect("balance repair requires a donor cluster");
            used_donors.insert(donor);
            let farthest = primary
                .iter()
                .enumerate()
                .filter(|(_, cluster)| **cluster as usize == source)
                .max_by(|(row_a, _), (row_b, _)| {
                    distances[*row_a]
                        .partial_cmp(&distances[*row_b])
                        .unwrap_or(Ordering::Equal)
                        .then_with(|| row_b.cmp(row_a))
                })
                .map(|(row, _)| row)
                .expect("overfull cluster has a member");
            splits.push((donor, farthest));
        }

        for cluster in 0..k {
            if counts[cluster] > 0 {
                for (value, sum) in centroids[cluster].iter_mut().zip(&sums[cluster]) {
                    *value = (*sum / counts[cluster] as f64) as f32;
                }
            }
        }
        for (donor, row) in splits {
            centroids[donor].copy_from_slice(&corpus[row * dim..(row + 1) * dim]);
        }
        eprintln!(
            "[{label}] balance round {} split {} overfull clusters (max occupancy {})",
            round + 1,
            overfull.len(),
            counts.iter().copied().max().unwrap_or(0)
        );
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut data_dir = String::new();
    let mut rows: usize = 0;
    let mut nlist: usize = 0;
    let mut iters: usize = 25;
    let mut epsilon: f64 = 1e-4;
    let mut threads: usize = 12;
    let mut gt_cache_dir = String::new();
    let mut evals_raw = String::new();
    let mut label = String::new();
    let mut spill_ratios_sq = vec![0.0f32];
    let mut balance_max_ratio = 0.0f64;
    let mut balance_rounds = 0usize;
    let mut batch_scale = false;

    let mut i = 1;
    while i < args.len() {
        let (key, val) = args[i]
            .split_once('=')
            .unwrap_or_else(|| panic!("bad arg {:?} (want --key=value)", args[i]));
        match key {
            "--data-dir" => data_dir = val.to_string(),
            "--rows" => rows = val.parse().expect("--rows"),
            "--nlist" => nlist = val.parse().expect("--nlist"),
            "--iters" => iters = val.parse().expect("--iters"),
            "--epsilon" => epsilon = val.parse().expect("--epsilon"),
            "--threads" => threads = val.parse().expect("--threads"),
            "--gt-cache-dir" => gt_cache_dir = val.to_string(),
            "--evals" => evals_raw = val.to_string(),
            "--label" => label = val.to_string(),
            "--spill-ratio-sq" => {
                spill_ratios_sq = val
                    .split(',')
                    .map(|ratio| ratio.parse().expect("--spill-ratio-sq"))
                    .collect();
            }
            "--balance-max-ratio" => balance_max_ratio = val.parse().expect("--balance-max-ratio"),
            "--balance-rounds" => balance_rounds = val.parse().expect("--balance-rounds"),
            "--batch-scale" => batch_scale = val.parse().expect("--batch-scale=true|false"),
            other => panic!("unknown arg {other:?}"),
        }
        i += 1;
    }
    assert!(!data_dir.is_empty(), "--data-dir required");
    assert!(rows > 0, "--rows required");
    assert!(nlist > 0, "--nlist required");
    assert!(!gt_cache_dir.is_empty(), "--gt-cache-dir required");
    assert!(!evals_raw.is_empty(), "--evals required");
    assert!(
        !spill_ratios_sq.is_empty() && spill_ratios_sq.iter().all(|ratio| *ratio >= 0.0),
        "--spill-ratio-sq must be non-negative"
    );

    // Parse evals: "l2:cosine:8,16,32;cosine:cosine:8,16" -> EvalSpec list.
    let evals: Vec<EvalSpec> = evals_raw
        .split(';')
        .map(|spec| {
            let parts: Vec<&str> = spec.split(':').collect();
            assert_eq!(parts.len(), 3, "bad eval spec {spec:?}");
            EvalSpec {
                assign: Metric::parse(parts[0]),
                probe: Metric::parse(parts[1]),
                nprobes: parts[2]
                    .split(',')
                    .map(|n| n.parse().expect("nprobe"))
                    .collect(),
            }
        })
        .collect();

    let dir = PathBuf::from(&data_dir);
    let meta_raw = fs::read_to_string(dir.join("meta.json")).expect("meta.json");
    let meta: serde_json::Value = serde_json::from_str(&meta_raw).expect("parse meta.json");
    let corpus_n = meta["corpus_n"].as_u64().expect("corpus_n") as usize;
    let dim = meta["dims"].as_u64().expect("dims") as usize;
    let query_n = meta["query_n"].as_u64().expect("query_n") as usize;
    let gt_k = meta["gt_k"].as_u64().expect("gt_k") as usize;
    assert_eq!(gt_k, TOP_K, "gt_k mismatch");
    assert!(
        rows <= corpus_n,
        "--rows {rows} exceeds corpus_n {corpus_n}"
    );

    eprintln!(
        "[{label}] loading corpus prefix rows={rows} dim={dim} (corpus_n={corpus_n}) threads={threads}"
    );
    // Validate full corpus file size, then read only the prefix.
    let corpus_path = dir.join("corpus_vectors.f32");
    let cmeta = fs::metadata(&corpus_path).expect("stat corpus");
    assert_eq!(cmeta.len(), (corpus_n * dim * 4) as u64, "corpus size");
    let corpus: Vec<f32> = {
        let mut file = fs::File::open(&corpus_path).expect("open corpus");
        let want = rows * dim * 4;
        let mut buf = vec![0u8; want];
        file.read_exact(&mut buf).expect("read corpus prefix");
        let mut out = vec![0f32; rows * dim];
        for (j, chunk) in buf.chunks_exact(4).enumerate() {
            out[j] = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        }
        out
    };
    let queries = read_exact_f32(&dir.join("query_vectors.f32"), (query_n * dim * 4) as u64);

    // Ground truth: exact file at full corpus, recomputed+cached for prefixes.
    let gt: Vec<u32> = if rows == corpus_n {
        read_exact_u32(
            &dir.join("ground_truth_top100.u32"),
            (query_n * TOP_K * 4) as u64,
        )
    } else {
        let cache = PathBuf::from(&gt_cache_dir).join(format!("gt_prefix_{rows}.u32"));
        if cache.exists() {
            eprintln!("[{label}] using cached prefix GT {cache:?}");
            read_exact_u32(&cache, (query_n * TOP_K * 4) as u64)
        } else {
            eprintln!("[{label}] computing exact prefix GT for rows={rows}");
            let t0 = Instant::now();
            let gt = compute_prefix_ground_truth(&corpus, dim, rows, &queries, query_n, threads);
            eprintln!(
                "[{label}] prefix GT computed in {:.1}s",
                t0.elapsed().as_secs_f64()
            );
            let bytes: Vec<u8> = gt.iter().flat_map(|v| v.to_le_bytes()).collect();
            fs::write(&cache, bytes).expect("write GT cache");
            gt
        }
    };

    // --- Train with the production seam ---
    eprintln!("[{label}] training: production train_kmeans k={nlist} iters={iters} eps={epsilon}");
    let refs: Vec<&[f32]> = (0..rows).map(|r| &corpus[r * dim..(r + 1) * dim]).collect();
    let t_train = Instant::now();
    let mut centroids = train_kmeans(&refs, dim, nlist, iters, epsilon).expect("train_kmeans");
    let train_secs = t_train.elapsed().as_secs_f64();
    assert_eq!(centroids.len(), nlist, "centroid count");
    eprintln!("[{label}] training done in {train_secs:.1}s");

    let t_repair = Instant::now();
    repair_balance(
        &corpus,
        dim,
        rows,
        &mut centroids,
        balance_max_ratio,
        balance_rounds,
        threads,
        &label,
    );
    let repair_secs = t_repair.elapsed().as_secs_f64();

    // Centroid norm stats.
    let norms: Vec<f64> = centroids
        .iter()
        .map(|c| {
            c.iter()
                .map(|x| (*x as f64) * (*x as f64))
                .sum::<f64>()
                .sqrt()
        })
        .collect();
    let (mut nmin, mut nmax, mut nsum) = (f64::MAX, f64::MIN, 0.0);
    for &n in &norms {
        nmin = nmin.min(n);
        nmax = nmax.max(n);
        nsum += n;
    }
    let nmean = nsum / norms.len() as f64;

    println!(
        "{{\"label\":{label:?},\"rows\":{rows},\"nlist\":{nlist},\"iters\":{iters},\"batch_scaled\":{batch_scale},\"balance_max_ratio\":{balance_max_ratio},\"balance_rounds\":{balance_rounds},\"train_secs\":{train_secs:.1},\"repair_secs\":{repair_secs:.1},\"centroid_norm_min\":{nmin:.4},\"centroid_norm_mean\":{nmean:.4},\"centroid_norm_max\":{nmax:.4}}}"
    );

    for spec in &evals {
        // --- Assign (production semantics for L2; cosine variant optional) ---
        let assign_metric = spec.assign;
        eprintln!(
            "[{label}] assigning rows by {} distance",
            assign_metric.name()
        );
        assert!(
            spill_ratios_sq.iter().all(|ratio| *ratio == 0.0) || assign_metric == Metric::L2,
            "spill threshold is defined only for squared-L2 assignment"
        );
        let t_assign = Instant::now();
        let (assignment, secondary, first_distance, second_distance) =
            assign_primary(&corpus, dim, rows, &centroids, assign_metric, threads);
        let assign_secs = t_assign.elapsed().as_secs_f64();

        for &spill_ratio_sq in &spill_ratios_sq {
            // Cluster membership in deterministic row order.
            let mut clusters: Vec<Vec<u32>> = vec![Vec::new(); nlist];
            let mut spilled = 0usize;
            for (row, &c) in assignment.iter().enumerate() {
                clusters[c as usize].push(row as u32);
                if spill_ratio_sq > 0.0
                    && first_distance[row] > 0.0
                    && secondary[row] != c
                    && second_distance[row] <= spill_ratio_sq * first_distance[row]
                {
                    clusters[secondary[row] as usize].push(row as u32);
                    spilled += 1;
                }
            }
            let storage_inflation = (rows + spilled) as f64 / rows as f64;
            let mut occupancy: Vec<usize> = clusters.iter().map(|c| c.len()).collect();
            occupancy.sort_unstable();
            let empties = occupancy.iter().filter(|&&o| o == 0).count();
            eprintln!(
            "[{label}] assign({}) done in {assign_secs:.1}s; occupancy min={} p50={} p90={} max={} empty={empties}",
            assign_metric.name(),
            occupancy[0],
            percentile(&occupancy, 0.5),
            percentile(&occupancy, 0.9),
            occupancy[occupancy.len() - 1]
        );

            // --- GT cluster-spread diagnostic ---
            // For each query: how many distinct clusters own its 100 true
            // neighbors, and the minimum number of clusters (greedy by owned-GT
            // count) covering >=90% and >=96% of them. If the median spread
            // exceeds nprobe, no probe policy of that size can reach high recall
            // under this partition, independent of centroid-ranking quality.
            {
                let mut spread = Vec::with_capacity(query_n);
                let mut cov90 = Vec::with_capacity(query_n);
                let mut cov96 = Vec::with_capacity(query_n);
                for qi in 0..query_n {
                    let gt_row = &gt[qi * TOP_K..(qi + 1) * TOP_K];
                    let mut counts: std::collections::HashMap<u32, usize> =
                        std::collections::HashMap::new();
                    for &row in gt_row {
                        *counts.entry(assignment[row as usize]).or_insert(0) += 1;
                    }
                    let mut owned: Vec<usize> = counts.values().copied().collect();
                    owned.sort_unstable_by(|a, b| b.cmp(a));
                    spread.push(owned.len());
                    let mut acc = 0usize;
                    let (mut c90, mut c96) = (0usize, 0usize);
                    for (i, o) in owned.iter().enumerate() {
                        acc += o;
                        if c90 == 0 && acc * 100 >= 90 * TOP_K {
                            c90 = i + 1;
                        }
                        if acc * 100 >= 96 * TOP_K {
                            c96 = i + 1;
                            break;
                        }
                    }
                    cov90.push(c90);
                    cov96.push(c96);
                }
                spread.sort_unstable();
                cov90.sort_unstable();
                cov96.sort_unstable();
                let mean = |v: &[usize]| v.iter().sum::<usize>() as f64 / v.len() as f64;
                println!(
                "{{\"label\":{label:?},\"rows\":{rows},\"nlist\":{nlist},\"iters\":{iters},\"assign\":\"{}\",\"stat\":\"gt_cluster_spread\",\"spread_mean\":{:.2},\"spread_p50\":{},\"spread_p90\":{},\"cov90_mean\":{:.2},\"cov90_p50\":{},\"cov96_mean\":{:.2},\"cov96_p50\":{},\"cov96_p90\":{}}}",
                assign_metric.name(),
                mean(&spread),
                percentile(&spread, 0.5),
                percentile(&spread, 0.9),
                mean(&cov90),
                percentile(&cov90, 0.5),
                mean(&cov96),
                percentile(&cov96, 0.5),
                percentile(&cov96, 0.9)
            );
            }

            // --- Probe + exact in-probe ceiling ---
            let probe_metric = spec.probe;
            let mut nprobes = spec.nprobes.clone();
            nprobes.sort_unstable();
            nprobes.dedup();
            let max_np = *nprobes.iter().max().expect("nprobes").min(&nlist);

            let t_eval = Instant::now();
            let ranges = thread_ranges(query_n, threads);
            // Per-thread partial sums: (recall10, recall100, scanned) per nprobe.
            let results: Vec<(Vec<f64>, Vec<f64>, Vec<u64>)> = std::thread::scope(|scope| {
                let mut handles = Vec::new();
                let centroids = &centroids;
                let corpus = &corpus;
                let queries = &queries;
                let clusters = &clusters;
                let gt = &gt;
                let nprobes = &nprobes;
                for (qs, qe) in ranges {
                    handles.push(scope.spawn(move || {
                        let mut r10 = vec![0f64; nprobes.len()];
                        let mut r100 = vec![0f64; nprobes.len()];
                        let mut scanned = vec![0u64; nprobes.len()];
                        for qi in qs..qe {
                            let q = &queries[qi * dim..(qi + 1) * dim];
                            // Production probe: rank centroids by requested metric,
                            // stable sort, ties keep index order (search.rs:1117).
                            let mut dists: Vec<(usize, f32)> = centroids
                                .iter()
                                .enumerate()
                                .map(|(ci, c)| (ci, probe_metric.dist(q, c)))
                                .collect();
                            dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
                            let mut heaps: Vec<DedupedTopK> =
                                nprobes.iter().map(|_| DedupedTopK::new()).collect();
                            for (rank, (ci, _)) in dists.iter().take(max_np).enumerate() {
                                for &row in &clusters[*ci] {
                                    let v = &corpus[row as usize * dim..(row as usize + 1) * dim];
                                    let score = -dot_product_distance(q, v);
                                    let cand = Candidate { score, row };
                                    for (npi, &np) in nprobes.iter().enumerate() {
                                        if rank < np {
                                            heaps[npi].retain(cand);
                                        }
                                    }
                                }
                                for (npi, &np) in nprobes.iter().enumerate() {
                                    if rank < np {
                                        scanned[npi] += clusters[*ci].len() as u64;
                                    }
                                }
                            }
                            let gt_row = &gt[qi * TOP_K..(qi + 1) * TOP_K];
                            for (npi, heap) in heaps.into_iter().enumerate() {
                                let retrieved = heap.sorted_rows();
                                assert!(
                                    retrieved.len() >= TOP_K,
                                    "query {qi} nprobe {} underfilled: {} rows",
                                    nprobes[npi],
                                    retrieved.len()
                                );
                                r10[npi] += recall_at_k(&retrieved, gt_row, 10);
                                r100[npi] += recall_at_k(&retrieved, gt_row, TOP_K);
                            }
                        }
                        (r10, r100, scanned)
                    }));
                }
                handles
                    .into_iter()
                    .map(|h| h.join().expect("eval thread panicked"))
                    .collect()
            });
            let eval_secs = t_eval.elapsed().as_secs_f64();

            for (npi, &np) in nprobes.iter().enumerate() {
                let r10: f64 = results.iter().map(|r| r.0[npi]).sum::<f64>() / query_n as f64;
                let r100: f64 = results.iter().map(|r| r.1[npi]).sum::<f64>() / query_n as f64;
                let scanned: u64 = results.iter().map(|r| r.2[npi]).sum();
                let mean_scanned = scanned as f64 / query_n as f64;
                let frac = mean_scanned / rows as f64;
                println!(
                "{{\"label\":{label:?},\"rows\":{rows},\"nlist\":{nlist},\"iters\":{iters},\"batch_scaled\":{batch_scale},\"balance_max_ratio\":{balance_max_ratio},\"balance_rounds\":{balance_rounds},\"spill_ratio_sq\":{spill_ratio_sq},\"spilled\":{spilled},\"storage_inflation\":{storage_inflation:.5},\"assign\":\"{}\",\"probe\":\"{}\",\"nprobe\":{np},\"recall_at_10\":{r10:.6},\"recall_at_100\":{r100:.6},\"mean_rows_scanned\":{mean_scanned:.0},\"scan_fraction\":{frac:.5},\"occ_min\":{},\"occ_p50\":{},\"occ_max\":{},\"occ_empty\":{empties},\"assign_secs\":{assign_secs:.1},\"eval_secs\":{eval_secs:.1}}}",
                assign_metric.name(),
                probe_metric.name(),
                occupancy[0],
                percentile(&occupancy, 0.5),
                occupancy[occupancy.len() - 1]
            );
            }
            eprintln!(
                "[{label}] eval assign={} probe={} done in {eval_secs:.1}s",
                assign_metric.name(),
                probe_metric.name()
            );
        }
    }
}
