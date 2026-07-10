//! Binding real-dataset recall gate for the shipped IVF partition policy.
//!
//! The test is ignored because it loads the pinned 1M/2M datasets and scores
//! every logical row for the full-probe sentinel. It deliberately calls
//! `partition_vectors`, the same CPU seam consumed by production builds.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::fs;
use std::io::Read as _;
use std::path::{Path, PathBuf};

use zeppelin::config::IndexingConfig;
use zeppelin::index::distance::compute_distance;
use zeppelin::index::ivf_flat::build::{partition_vectors, IvfPartition};
use zeppelin::types::DistanceMetric;

const TOP_K: usize = 100;
const QUERY_THREADS: usize = 12;
const MIN_RECALL_AT_100: f64 = 0.96;
const MAX_SCAN_FRACTION: f64 = 0.20;
const MAX_STORAGE_INFLATION: f64 = 1.5;
const MIN_FULL_PROBE_RECALL: f64 = 0.999;

#[derive(Debug)]
struct Dataset {
    corpus: Vec<f32>,
    queries: Vec<f32>,
    ground_truth: Vec<u32>,
    corpus_n: usize,
    query_n: usize,
    dim: usize,
}

#[derive(Debug, Clone, Copy)]
struct EvalMetrics {
    recall_at_10: f64,
    recall_at_100: f64,
    scan_fraction: f64,
    storage_inflation: f64,
    full_probe_recall_at_100: f64,
}

#[derive(Debug, Clone, Copy)]
struct EvaluationData<'a> {
    corpus: &'a [f32],
    queries: &'a [f32],
    ground_truth: &'a [u32],
    dim: usize,
    logical_rows: usize,
    query_n: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Candidate {
    distance: f32,
    row: u32,
}

impl Eq for Candidate {}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.row.cmp(&other.row))
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
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

    fn retain(&mut self, candidate: Candidate) {
        // One logical row always has the same exact score. If a future stored
        // representation duplicates it, retaining the first occurrence is
        // therefore equivalent to retaining its best exact score.
        if self.rows.contains(&candidate.row) {
            return;
        }
        if self.heap.len() < TOP_K {
            self.rows.insert(candidate.row);
            self.heap.push(candidate);
            return;
        }
        if self
            .heap
            .peek()
            .is_some_and(|worst| candidate.cmp(worst) == Ordering::Less)
        {
            let evicted = self.heap.pop().expect("full top-k has a worst row");
            self.rows.remove(&evicted.row);
            self.rows.insert(candidate.row);
            self.heap.push(candidate);
        }
    }

    fn sorted_rows(self) -> Vec<u32> {
        let mut candidates = self.heap.into_vec();
        candidates.sort();
        candidates
            .into_iter()
            .map(|candidate| candidate.row)
            .collect()
    }
}

fn thread_ranges(total: usize, threads: usize) -> Vec<(usize, usize)> {
    let chunk = total.div_ceil(threads.max(1));
    (0..threads.max(1))
        .map(|worker| (worker * chunk, ((worker + 1) * chunk).min(total)))
        .filter(|(start, end)| start < end)
        .collect()
}

fn read_exact_f32(path: &Path, expected_values: usize) -> Vec<f32> {
    let expected_bytes = expected_values
        .checked_mul(std::mem::size_of::<f32>())
        .expect("f32 byte size overflow");
    let metadata = fs::metadata(path).unwrap_or_else(|error| {
        panic!(
            "cannot stat pinned dataset artifact {}: {error}",
            path.display()
        )
    });
    assert_eq!(
        metadata.len(),
        expected_bytes as u64,
        "pinned dataset size mismatch for {}",
        path.display()
    );
    let mut file = fs::File::open(path)
        .unwrap_or_else(|error| panic!("cannot open {}: {error}", path.display()));
    let mut values = vec![0f32; expected_values];
    let mut buffer = vec![0u8; 8 * 1024 * 1024];
    for output in values.chunks_mut(buffer.len() / 4) {
        let bytes = &mut buffer[..output.len() * 4];
        file.read_exact(bytes)
            .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
        for (value, encoded) in output.iter_mut().zip(bytes.chunks_exact(4)) {
            *value = f32::from_le_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        }
    }
    values
}

fn read_exact_u32(path: &Path, expected_values: usize) -> Vec<u32> {
    let expected_bytes = expected_values
        .checked_mul(std::mem::size_of::<u32>())
        .expect("u32 byte size overflow");
    let metadata = fs::metadata(path).unwrap_or_else(|error| {
        panic!(
            "cannot stat pinned dataset artifact {}: {error}",
            path.display()
        )
    });
    assert_eq!(
        metadata.len(),
        expected_bytes as u64,
        "pinned dataset size mismatch for {}",
        path.display()
    );
    let mut file = fs::File::open(path)
        .unwrap_or_else(|error| panic!("cannot open {}: {error}", path.display()));
    let mut values = vec![0u32; expected_values];
    let mut buffer = vec![0u8; 8 * 1024 * 1024];
    for output in values.chunks_mut(buffer.len() / 4) {
        let bytes = &mut buffer[..output.len() * 4];
        file.read_exact(bytes)
            .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
        for (value, encoded) in output.iter_mut().zip(bytes.chunks_exact(4)) {
            *value = u32::from_le_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        }
    }
    values
}

fn load_dataset(root: &Path, name: &str) -> Dataset {
    let dir = root.join(name);
    assert!(
        dir.is_dir(),
        "missing pinned recall dataset {}; set ZEPPELIN_RECALL_GATE_DATA to the directory containing wikidpr1m and wikidpr2m",
        dir.display()
    );
    let meta_path = dir.join("meta.json");
    let meta_raw = fs::read_to_string(&meta_path)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", meta_path.display()));
    let meta: serde_json::Value = serde_json::from_str(&meta_raw)
        .unwrap_or_else(|error| panic!("cannot parse {}: {error}", meta_path.display()));
    let field = |key: &str| {
        meta[key]
            .as_u64()
            .unwrap_or_else(|| panic!("{} missing integer {key}", meta_path.display()))
            as usize
    };
    let corpus_n = field("corpus_n");
    let query_n = field("query_n");
    let dim = field("dims");
    let gt_k = field("gt_k");
    assert_eq!(gt_k, TOP_K, "{} gt_k must be 100", meta_path.display());
    assert_eq!(
        meta["metric"].as_str(),
        Some("cosine"),
        "{} metric must be cosine",
        meta_path.display()
    );

    Dataset {
        corpus: read_exact_f32(&dir.join("corpus_vectors.f32"), corpus_n * dim),
        queries: read_exact_f32(&dir.join("query_vectors.f32"), query_n * dim),
        ground_truth: read_exact_u32(&dir.join("ground_truth_top100.u32"), query_n * TOP_K),
        corpus_n,
        query_n,
        dim,
    }
}

fn assignment_hash(primary: &[u32]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for assignment in primary {
        for byte in assignment.to_le_bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash
}

fn recall_at_k(retrieved: &[u32], ground_truth: &[u32], k: usize) -> f64 {
    let hits = retrieved
        .iter()
        .take(k)
        .filter(|row| ground_truth[..k].contains(row))
        .count();
    hits as f64 / k as f64
}

fn evaluate_partition(
    partition: &IvfPartition,
    data: EvaluationData<'_>,
    nprobe: usize,
) -> EvalMetrics {
    let ranges = thread_ranges(data.query_n, QUERY_THREADS);
    let partials: Vec<(f64, f64, f64, u64)> = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(ranges.len());
        for (query_start, query_end) in ranges {
            handles.push(scope.spawn(move || {
                let mut recall10 = 0.0;
                let mut recall100 = 0.0;
                let mut full_recall100 = 0.0;
                let mut scanned = 0u64;
                for query_idx in query_start..query_end {
                    let query = &data.queries[query_idx * data.dim..(query_idx + 1) * data.dim];
                    let mut centroid_distances: Vec<(usize, f32)> = partition
                        .centroids
                        .iter()
                        .enumerate()
                        .map(|(cluster_idx, centroid)| {
                            (
                                cluster_idx,
                                compute_distance(query, centroid, DistanceMetric::Cosine),
                            )
                        })
                        .collect();
                    centroid_distances.sort_by(|left, right| left.1.total_cmp(&right.1));

                    let mut default_top = DedupedTopK::new();
                    let mut full_top = DedupedTopK::new();
                    for (rank, (cluster_idx, _)) in centroid_distances.iter().enumerate() {
                        let rows = &partition.clusters[*cluster_idx];
                        if rank < nprobe {
                            scanned += rows.len() as u64;
                        }
                        for &row in rows {
                            let vector = &data.corpus
                                [row as usize * data.dim..(row as usize + 1) * data.dim];
                            let candidate = Candidate {
                                distance: compute_distance(query, vector, DistanceMetric::Cosine),
                                row,
                            };
                            full_top.retain(candidate);
                            if rank < nprobe {
                                default_top.retain(candidate);
                            }
                        }
                    }

                    let default_rows = default_top.sorted_rows();
                    let full_rows = full_top.sorted_rows();
                    assert_eq!(default_rows.len(), TOP_K, "default probe underfilled");
                    assert_eq!(full_rows.len(), TOP_K, "full probe underfilled");
                    let expected = &data.ground_truth[query_idx * TOP_K..(query_idx + 1) * TOP_K];
                    recall10 += recall_at_k(&default_rows, expected, 10);
                    recall100 += recall_at_k(&default_rows, expected, TOP_K);
                    full_recall100 += recall_at_k(&full_rows, expected, TOP_K);
                }
                (recall10, recall100, full_recall100, scanned)
            }));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().expect("query worker panicked"))
            .collect()
    });

    let query_n_f64 = data.query_n as f64;
    let scored_rows = partials.iter().map(|partial| partial.3).sum::<u64>();
    EvalMetrics {
        recall_at_10: partials.iter().map(|partial| partial.0).sum::<f64>() / query_n_f64,
        recall_at_100: partials.iter().map(|partial| partial.1).sum::<f64>() / query_n_f64,
        scan_fraction: scored_rows as f64 / query_n_f64 / data.logical_rows as f64,
        storage_inflation: (data.logical_rows + partition.spilled) as f64
            / data.logical_rows as f64,
        full_probe_recall_at_100: partials.iter().map(|partial| partial.2).sum::<f64>()
            / query_n_f64,
    }
}

fn exact_ground_truth(
    corpus: &[f32],
    queries: &[f32],
    dim: usize,
    logical_rows: usize,
    query_n: usize,
) -> Vec<u32> {
    let ranges = thread_ranges(query_n, QUERY_THREADS);
    let partials: Vec<(usize, Vec<u32>)> = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(ranges.len());
        for (query_start, query_end) in ranges {
            handles.push(scope.spawn(move || {
                let mut output = Vec::with_capacity((query_end - query_start) * TOP_K);
                for query_idx in query_start..query_end {
                    let query = &queries[query_idx * dim..(query_idx + 1) * dim];
                    let mut top = DedupedTopK::new();
                    for row in 0..logical_rows {
                        let vector = &corpus[row * dim..(row + 1) * dim];
                        top.retain(Candidate {
                            distance: compute_distance(query, vector, DistanceMetric::Cosine),
                            row: row as u32,
                        });
                    }
                    output.extend(top.sorted_rows());
                }
                (query_start, output)
            }));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().expect("ground-truth worker panicked"))
            .collect()
    });
    let mut ground_truth = vec![0u32; query_n * TOP_K];
    for (query_start, rows) in partials {
        let offset = query_start * TOP_K;
        ground_truth[offset..offset + rows.len()].copy_from_slice(&rows);
    }
    ground_truth
}

fn report(name: &str, partition: &IvfPartition, nprobe: usize, metrics: EvalMetrics) {
    println!(
        "{name:<12} clusters={:<4} nprobe={:<4} recall@10={:.6} recall@100={:.6} scan={:.5} storage={:.5} full={:.6}",
        partition.centroids.len(),
        nprobe,
        metrics.recall_at_10,
        metrics.recall_at_100,
        metrics.scan_fraction,
        metrics.storage_inflation,
        metrics.full_probe_recall_at_100,
    );
}

fn run_dataset(root: &Path, name: &str, config: &IndexingConfig) -> EvalMetrics {
    let dataset = load_dataset(root, name);
    let refs: Vec<&[f32]> = dataset.corpus.chunks_exact(dataset.dim).collect();
    let partition = partition_vectors(&refs, dataset.dim, config).expect("partition build failed");
    let repeated = partition_vectors(&refs, dataset.dim, config)
        .expect("repeated deterministic partition build failed");
    assert_eq!(
        assignment_hash(&partition.primary),
        assignment_hash(&repeated.primary),
        "{name} partition assignment hash changed across identical builds"
    );
    drop(repeated);

    // Commit 1 uses the current constant default. Commit 3 replaces this with
    // the production scale-aware default resolver once that policy lands.
    let nprobe = config.default_nprobe.min(partition.centroids.len());
    let metrics = evaluate_partition(
        &partition,
        EvaluationData {
            corpus: &dataset.corpus,
            queries: &dataset.queries,
            ground_truth: &dataset.ground_truth,
            dim: dataset.dim,
            logical_rows: dataset.corpus_n,
            query_n: dataset.query_n,
        },
        nprobe,
    );
    report(name, &partition, nprobe, metrics);

    assert!(
        metrics.scan_fraction <= MAX_SCAN_FRACTION,
        "{name} scan fraction {:.6} exceeds {:.6}",
        metrics.scan_fraction,
        MAX_SCAN_FRACTION
    );
    assert!(
        metrics.storage_inflation <= MAX_STORAGE_INFLATION,
        "{name} storage inflation {:.6} exceeds {:.6}",
        metrics.storage_inflation,
        MAX_STORAGE_INFLATION
    );
    assert!(
        metrics.full_probe_recall_at_100 >= MIN_FULL_PROBE_RECALL,
        "{name} full-probe sentinel {:.6} is below {:.6}",
        metrics.full_probe_recall_at_100,
        MIN_FULL_PROBE_RECALL
    );

    if name == "wikidpr1m" {
        let prefix_rows = 100_000;
        let prefix_corpus = &dataset.corpus[..prefix_rows * dataset.dim];
        let prefix_refs: Vec<&[f32]> = prefix_corpus.chunks_exact(dataset.dim).collect();
        let prefix_partition =
            partition_vectors(&prefix_refs, dataset.dim, config).expect("100k partition failed");
        let prefix_ground_truth = exact_ground_truth(
            prefix_corpus,
            &dataset.queries,
            dataset.dim,
            prefix_rows,
            dataset.query_n,
        );
        let prefix_nprobe = config.default_nprobe.min(prefix_partition.centroids.len());
        let prefix_metrics = evaluate_partition(
            &prefix_partition,
            EvaluationData {
                corpus: prefix_corpus,
                queries: &dataset.queries,
                ground_truth: &prefix_ground_truth,
                dim: dataset.dim,
                logical_rows: prefix_rows,
                query_n: dataset.query_n,
            },
            prefix_nprobe,
        );
        report(
            "wikidpr100k",
            &prefix_partition,
            prefix_nprobe,
            prefix_metrics,
        );
    }

    metrics
}

#[test]
#[ignore = "requires ZEPPELIN_RECALL_GATE_DATA and the pinned 1M/2M datasets"]
fn ivf_recall_gate() {
    let root = std::env::var_os("ZEPPELIN_RECALL_GATE_DATA")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            panic!(
                "set ZEPPELIN_RECALL_GATE_DATA to the directory containing the pinned wikidpr1m and wikidpr2m datasets"
            )
        });
    let config = IndexingConfig::default();
    let one_million = run_dataset(&root, "wikidpr1m", &config);
    let two_million = run_dataset(&root, "wikidpr2m", &config);

    assert!(
        one_million.recall_at_100 >= MIN_RECALL_AT_100,
        "wikidpr1m recall@100 {:.6} is below {:.6}",
        one_million.recall_at_100,
        MIN_RECALL_AT_100
    );
    assert!(
        two_million.recall_at_100 >= MIN_RECALL_AT_100,
        "wikidpr2m recall@100 {:.6} is below {:.6}",
        two_million.recall_at_100,
        MIN_RECALL_AT_100
    );
}
