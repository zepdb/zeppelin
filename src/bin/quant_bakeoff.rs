//! Deterministic, offline Phase 1 quantization bake-off.
//!
//! This binary deliberately has no storage or production-search integration.
//! It loads two fixed devbench datasets, validates their stored unit vectors,
//! trains an in-memory IVF partition, and evaluates four coarse estimators with
//! exact full-precision reranking. Missing or malformed inputs are fatal; in
//! particular, the wiki DPR slice is never replaced with another dataset.

#[allow(dead_code)]
#[path = "../index/quantization/rabitq.rs"]
mod rabitq;

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::env;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Instant;

use serde::Deserialize;
use thiserror::Error;
use uuid::Uuid;
use zeppelin::error::ZeppelinError;
use zeppelin::index::distance::cosine_distance;
use zeppelin::index::ivf_flat::kmeans::train_kmeans;
use zeppelin::index::quantization::pq::PqCodebook;
use zeppelin::index::quantization::sq::SqCalibration;
use zeppelin::types::DistanceMetric;

const DATA_ROOT: &str = "/Users/aghatage/Documents/code/zeppelin-devbench/data";
const RESULTS_PATH: &str =
    "/Users/aghatage/Documents/code/zeppelin/tasks/July10Quant/results/bakeoff.md";
const DBPEDIA_DIR: &str = "dbpedia100k";
const WIKI_DIR: &str = "wikidpr2m";
const REQUIRED_FILES: [&str; 6] = [
    "meta.json",
    "corpus_vectors.f32",
    "corpus_ids.txt",
    "query_vectors.f32",
    "query_ids.txt",
    "ground_truth_top100.u32",
];

const TOP_K: usize = 100;
const QUERY_LIMIT: usize = 1_000;
const NPROBES: [usize; 3] = [8, 16, 32];
const MARGINS: [usize; 4] = [2, 3, 4, 5];
const MAX_CANDIDATES: usize = TOP_K * MARGINS[MARGINS.len() - 1];
const DBPEDIA_TARGET_ROWS_PER_CLUSTER: usize = 2_500;
const WIKI_TARGET_ROWS_PER_CLUSTER: usize = 2_500;
const MIN_ROWS_PER_CLUSTER: f64 = 2_000.0;
const MAX_ROWS_PER_CLUSTER: f64 = 3_000.0;
const IVF_TRAIN_ROWS_PER_CLUSTER: usize = 32;
const IVF_MIN_TRAIN_ROWS: usize = 4_096;
const IVF_KMEANS_ITERS: usize = 25;
const KMEANS_EPSILON: f64 = 1e-4;
const PQ_SUBQUANTIZERS: usize = 64;
const PQ_TRAIN_ROWS: usize = 4_096;
const PQ_KMEANS_ITERS: usize = 6;
const ROTATION_SEED: u64 = 0x5a45_5050_454c_494e;
const IO_BUFFER_BYTES: usize = 4 * 1024 * 1024;
const UNIT_NORM_SQUARED_TOLERANCE: f64 = 1e-4;
const ESTIMATOR_ERROR_SAMPLE_MODULUS: u64 = 1_024;
const PQ_ANCHOR_TARGET: f64 = 0.88;
const PQ_ANCHOR_TOLERANCE: f64 = 0.03;
const PQ_ANCHOR_NPROBE: usize = 16;
const PQ_ANCHOR_MARGIN: usize = 3;
const GATE_RECALL: f64 = 0.96;
const WIKI_DATA_GENERATION: &str = "1778486380300287";
const WIKI_QUERIES_GENERATION: &str = "1778486223485507";

type Result<T> = std::result::Result<T, BakeoffError>;

#[derive(Debug, Error)]
enum BakeoffError {
    #[error("invalid quant_bakeoff invocation: {0}")]
    Usage(String),
    #[error("I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("invalid JSON at {path}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("dataset integrity check failed: {0}")]
    Dataset(String),
    #[error("bake-off integrity check failed: {0}")]
    Integrity(String),
    #[error("RaBitQ error: {0}")]
    Rabitq(String),
    #[error("Zeppelin error: {0}")]
    Zeppelin(#[from] ZeppelinError),
}

#[derive(Debug, Clone, Copy)]
struct DatasetSpec {
    report_name: &'static str,
    directory: &'static str,
    is_gate_dataset: bool,
}

const DATASETS: [DatasetSpec; 2] = [
    DatasetSpec {
        report_name: "dbpedia100k",
        directory: DBPEDIA_DIR,
        is_gate_dataset: false,
    },
    DatasetSpec {
        report_name: "wiki_dpr_e5",
        directory: WIKI_DIR,
        is_gate_dataset: true,
    },
];

#[derive(Debug, Deserialize)]
struct DatasetMeta {
    corpus_n: usize,
    query_n: usize,
    dims: usize,
    metric: String,
    gt_k: usize,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    slice: Option<WikiSliceMeta>,
    #[serde(default)]
    sources: Option<WikiSourcesMeta>,
    #[serde(default)]
    artifacts: Option<WikiArtifactsMeta>,
    #[serde(default)]
    ground_truth: Option<WikiGroundTruthMeta>,
}

#[derive(Debug, Deserialize)]
struct WikiSliceMeta {
    corpus: String,
    queries: String,
    full_corpus_closest_ids_used: bool,
}

#[derive(Debug, Deserialize)]
struct WikiSourcesMeta {
    data: WikiSourceMeta,
    queries: WikiSourceMeta,
}

#[derive(Debug, Deserialize)]
struct WikiSourceMeta {
    generation: String,
    url: String,
}

#[derive(Debug, Deserialize)]
struct WikiArtifactsMeta {
    #[serde(rename = "corpus_vectors.f32")]
    corpus_vectors: WikiArtifactMeta,
    #[serde(rename = "query_vectors.f32")]
    query_vectors: WikiArtifactMeta,
    #[serde(rename = "ground_truth_top100.u32")]
    ground_truth: WikiArtifactMeta,
}

#[derive(Debug, Deserialize)]
struct WikiArtifactMeta {
    sha256: String,
}

#[derive(Debug, Deserialize)]
struct WikiGroundTruthMeta {
    algorithm: String,
    distance: String,
    examined_corpus_rows_per_query: usize,
    tie_break: String,
    output: String,
}

#[derive(Debug, Clone)]
enum DatasetProvenance {
    Dbpedia { source: String },
    Wiki(Box<WikiProvenance>),
}

#[derive(Debug, Clone)]
struct WikiProvenance {
    corpus_slice: String,
    query_slice: String,
    data_url: String,
    data_generation: String,
    queries_url: String,
    queries_generation: String,
    corpus_sha256: String,
    queries_sha256: String,
    ground_truth_sha256: String,
    ground_truth_algorithm: String,
    ground_truth_distance: String,
    ground_truth_examined_rows: usize,
    ground_truth_tie_break: String,
    ground_truth_output: String,
}

struct Dataset {
    spec: DatasetSpec,
    meta: DatasetMeta,
    provenance: DatasetProvenance,
    corpus: Vec<f32>,
    queries: Vec<f32>,
    ground_truth: Vec<u32>,
}

impl Dataset {
    #[inline]
    fn corpus_row(&self, row: usize) -> &[f32] {
        let start = row * self.meta.dims;
        &self.corpus[start..start + self.meta.dims]
    }

    #[inline]
    fn query_row(&self, row: usize) -> &[f32] {
        let start = row * self.meta.dims;
        &self.queries[start..start + self.meta.dims]
    }

    #[inline]
    fn ground_truth_row(&self, row: usize) -> &[u32] {
        let start = row * self.meta.gt_k;
        &self.ground_truth[start..start + self.meta.gt_k]
    }
}

struct IvfModel {
    centroids: Vec<Vec<f32>>,
    /// Corpus row ids grouped by cluster; rows remain ascending within a cluster.
    clusters: Vec<Vec<u32>>,
    /// Cluster-order code offset, including one terminal offset.
    cluster_offsets: Vec<usize>,
    training_rows: usize,
    average_rows_per_cluster: f64,
    assignment_workers: usize,
}

#[derive(Debug, Clone)]
struct MatrixCell {
    nprobe: usize,
    margin: usize,
    recall_at_10: f64,
    recall_at_100: f64,
}

#[derive(Debug, Clone)]
struct RawRecall {
    nprobe: usize,
    recall_at_10: f64,
    recall_at_100: f64,
}

#[derive(Debug, Clone)]
struct ProbeCeiling {
    nprobe: usize,
    recall_at_10: f64,
    recall_at_100: f64,
}

#[derive(Debug, Clone)]
struct EncoderResult {
    name: &'static str,
    logical_bytes_per_vector: usize,
    encode_seconds: f64,
    encode_vectors_per_second_per_core: f64,
    error_samples: u64,
    estimator_bias: f64,
    estimator_variance: f64,
    raw_recall: Vec<RawRecall>,
    cells: Vec<MatrixCell>,
}

struct DatasetResult {
    name: &'static str,
    corpus_n: usize,
    query_n: usize,
    dims: usize,
    nlist: usize,
    average_rows_per_cluster: f64,
    ivf_training_rows: usize,
    assignment_workers: usize,
    probe_ceilings: Vec<ProbeCeiling>,
    provenance: DatasetProvenance,
    encoders: Vec<EncoderResult>,
}

#[derive(Debug, Default)]
struct RunningMoments {
    count: u64,
    mean: f64,
    m2: f64,
}

impl RunningMoments {
    fn push(&mut self, value: f64) {
        self.count += 1;
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        let delta_after = value - self.mean;
        self.m2 += delta * delta_after;
    }

    fn variance(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.m2 / self.count as f64
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Candidate {
    score: f32,
    row: u32,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.score.to_bits() == other.score.to_bits() && self.row == other.row
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.row.cmp(&other.row))
    }
}

trait ApproximateScorer {
    fn prepare_query(&mut self, query_index: usize, query: &[f32]) -> Result<()>;

    fn prepare_cluster(
        &mut self,
        query_index: usize,
        cluster_index: usize,
        centroid: &[f32],
    ) -> Result<()>;

    fn score(&self, code_index: usize) -> Result<f32>;
}

fn main() {
    if let Err(error) = run() {
        eprintln!("quant_bakeoff error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    validate_cli()?;
    preflight_datasets()?;
    eprintln!("running deterministic structured-vs-dense 768d rotation oracle");
    let rotation_quality = rabitq::compare_structured_dense_quality_768().map_err(rabitq_error)?;
    if rotation_quality.mse_delta.abs() > 5.0 * rotation_quality.mse_delta_standard_error {
        return Err(BakeoffError::Integrity(format!(
            "structured-vs-dense rotation MSE delta {} exceeds five standard errors ({})",
            rotation_quality.mse_delta, rotation_quality.mse_delta_standard_error
        )));
    }

    let mut results = Vec::with_capacity(DATASETS.len());
    for spec in DATASETS {
        let path = Path::new(DATA_ROOT).join(spec.directory);
        eprintln!("loading {} from {}", spec.report_name, path.display());
        let dataset = load_dataset(spec, &path)?;
        validate_dataset_unit_norms(&dataset)?;
        let ivf = train_and_assign_ivf(&dataset)?;
        let result = evaluate_dataset(&dataset, &ivf)?;
        validate_dataset_anchors(&result)?;
        results.push(result);
    }

    let report = render_report(&results, &rotation_quality)?;
    write_report(Path::new(RESULTS_PATH), report.as_bytes())?;
    eprintln!("wrote {}", RESULTS_PATH);
    Ok(())
}

fn validate_cli() -> Result<()> {
    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "quant_bakeoff".to_string());
    if let Some(argument) = args.next() {
        if (argument == "--help" || argument == "-h") && args.next().is_none() {
            println!(
                "{program}\n\nLoads the fixed dbpedia100k and wikidpr2m datasets from\n\
                 {DATA_ROOT} and writes {RESULTS_PATH}.\n\
                 No dataset substitution or command-line path override is supported."
            );
            std::process::exit(0);
        }
        return Err(BakeoffError::Usage(format!(
            "unexpected argument {argument:?}; this deterministic driver accepts no arguments"
        )));
    }
    Ok(())
}

fn preflight_datasets() -> Result<()> {
    for spec in DATASETS {
        let directory = Path::new(DATA_ROOT).join(spec.directory);
        if !directory.is_dir() {
            let detail = if spec.is_gate_dataset {
                "the required wiki_dpr_e5 prefix slice has not been built; follow the five-step runbook in tasks/July10Quant/01-bakeoff.md and do not substitute another dataset"
            } else {
                "the required devbench dataset directory is missing"
            };
            return Err(BakeoffError::Dataset(format!(
                "{} is not a directory: {detail}",
                directory.display()
            )));
        }
        for file_name in REQUIRED_FILES {
            let path = directory.join(file_name);
            if !path.is_file() {
                return Err(BakeoffError::Dataset(format!(
                    "required dataset file is missing or not regular: {}",
                    path.display()
                )));
            }
        }
    }
    Ok(())
}

fn load_dataset(spec: DatasetSpec, directory: &Path) -> Result<Dataset> {
    let meta_path = directory.join("meta.json");
    let meta_bytes = read_all_buffered(&meta_path)?;
    let meta: DatasetMeta =
        serde_json::from_slice(&meta_bytes).map_err(|source| BakeoffError::Json {
            path: meta_path.clone(),
            source,
        })?;
    validate_meta(spec, &meta)?;
    let provenance = validated_provenance(spec, &meta)?;

    let corpus_values = checked_product(meta.corpus_n, meta.dims, "corpus shape")?;
    let query_values = checked_product(meta.query_n, meta.dims, "query shape")?;
    let gt_values = checked_product(meta.query_n, meta.gt_k, "ground-truth shape")?;

    let corpus = read_f32_file(&directory.join("corpus_vectors.f32"), corpus_values)?;
    validate_id_file(&directory.join("corpus_ids.txt"), meta.corpus_n)?;
    let queries = read_f32_file(&directory.join("query_vectors.f32"), query_values)?;
    validate_id_file(&directory.join("query_ids.txt"), meta.query_n)?;
    let ground_truth = read_u32_file(&directory.join("ground_truth_top100.u32"), gt_values)?;
    validate_ground_truth(&ground_truth, &meta)?;

    Ok(Dataset {
        spec,
        meta,
        provenance,
        corpus,
        queries,
        ground_truth,
    })
}

fn validate_meta(spec: DatasetSpec, meta: &DatasetMeta) -> Result<()> {
    if meta.corpus_n == 0 || meta.query_n == 0 || meta.dims == 0 {
        return Err(BakeoffError::Dataset(format!(
            "{} meta.json contains a zero shape: corpus_n={}, query_n={}, dims={}",
            spec.report_name, meta.corpus_n, meta.query_n, meta.dims
        )));
    }
    if meta.corpus_n > u32::MAX as usize {
        return Err(BakeoffError::Dataset(format!(
            "{} corpus_n {} exceeds the u32 row-id format",
            spec.report_name, meta.corpus_n
        )));
    }
    if meta.query_n != QUERY_LIMIT {
        return Err(BakeoffError::Dataset(format!(
            "{} has {} queries; the Phase 1 runbook requires exactly {}",
            spec.report_name, meta.query_n, QUERY_LIMIT
        )));
    }
    if meta.metric != "cosine" {
        return Err(BakeoffError::Dataset(format!(
            "{} metric is {:?}; this driver requires exact cosine ground truth",
            spec.report_name, meta.metric
        )));
    }
    if meta.gt_k != TOP_K {
        return Err(BakeoffError::Dataset(format!(
            "{} gt_k is {}; ground_truth_top100.u32 requires exactly {}",
            spec.report_name, meta.gt_k, TOP_K
        )));
    }
    if meta.dims % rabitq::BLOCK_DIM != 0 {
        return Err(BakeoffError::Dataset(format!(
            "{} dimension {} is not divisible by the structured-rotation block size {}",
            spec.report_name,
            meta.dims,
            rabitq::BLOCK_DIM
        )));
    }
    if meta.dims % PQ_SUBQUANTIZERS != 0 {
        return Err(BakeoffError::Dataset(format!(
            "{} dimension {} is not divisible by the current-v3 PQ width {}",
            spec.report_name, meta.dims, PQ_SUBQUANTIZERS
        )));
    }
    match spec.report_name {
        "dbpedia100k" if meta.corpus_n != 100_000 || meta.dims != 1_536 => {
            return Err(BakeoffError::Dataset(format!(
                "dbpedia100k shape is {} x {}, expected 100000 x 1536",
                meta.corpus_n, meta.dims
            )));
        }
        "wiki_dpr_e5" if !(1_000_000..=2_000_000).contains(&meta.corpus_n) || meta.dims != 768 => {
            return Err(BakeoffError::Dataset(format!(
                "wiki_dpr_e5 shape is {} x {}, expected 1000000..=2000000 x 768",
                meta.corpus_n, meta.dims
            )));
        }
        _ => {}
    }
    Ok(())
}

fn validated_provenance(spec: DatasetSpec, meta: &DatasetMeta) -> Result<DatasetProvenance> {
    if !spec.is_gate_dataset {
        let source = meta.source.as_deref().ok_or_else(|| {
            BakeoffError::Dataset(format!(
                "{} meta.json is missing its top-level source provenance",
                spec.report_name
            ))
        })?;
        if source.trim().is_empty() {
            return Err(BakeoffError::Dataset(format!(
                "{} meta.json has empty source provenance",
                spec.report_name
            )));
        }
        return Ok(DatasetProvenance::Dbpedia {
            source: source.to_string(),
        });
    }

    let slice = meta.slice.as_ref().ok_or_else(|| {
        BakeoffError::Dataset("wiki_dpr_e5 meta.json is missing slice provenance".into())
    })?;
    let sources = meta.sources.as_ref().ok_or_else(|| {
        BakeoffError::Dataset("wiki_dpr_e5 meta.json is missing pinned sources".into())
    })?;
    let artifacts = meta.artifacts.as_ref().ok_or_else(|| {
        BakeoffError::Dataset("wiki_dpr_e5 meta.json is missing artifact hashes".into())
    })?;
    let ground_truth = meta.ground_truth.as_ref().ok_or_else(|| {
        BakeoffError::Dataset("wiki_dpr_e5 meta.json is missing ground-truth semantics".into())
    })?;

    if slice.corpus.trim().is_empty() || slice.queries.trim().is_empty() {
        return Err(BakeoffError::Dataset(
            "wiki_dpr_e5 slice descriptions must be non-empty".into(),
        ));
    }
    if slice.full_corpus_closest_ids_used {
        return Err(BakeoffError::Dataset(
            "wiki_dpr_e5 metadata says full-corpus closest_ids were reused; sliced ground truth must be recomputed"
                .into(),
        ));
    }
    validate_pinned_source("wiki data", &sources.data, WIKI_DATA_GENERATION)?;
    validate_pinned_source("wiki queries", &sources.queries, WIKI_QUERIES_GENERATION)?;
    validate_sha256("corpus_vectors.f32", &artifacts.corpus_vectors.sha256)?;
    validate_sha256("query_vectors.f32", &artifacts.query_vectors.sha256)?;
    validate_sha256("ground_truth_top100.u32", &artifacts.ground_truth.sha256)?;
    if ground_truth.examined_corpus_rows_per_query != meta.corpus_n {
        return Err(BakeoffError::Dataset(format!(
            "wiki_dpr_e5 ground truth examined {} corpus rows per query, expected the complete {}-row slice",
            ground_truth.examined_corpus_rows_per_query, meta.corpus_n
        )));
    }
    for (field, value) in [
        ("algorithm", ground_truth.algorithm.as_str()),
        ("distance", ground_truth.distance.as_str()),
        ("tie_break", ground_truth.tie_break.as_str()),
        ("output", ground_truth.output.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(BakeoffError::Dataset(format!(
                "wiki_dpr_e5 ground-truth {field} provenance is empty"
            )));
        }
    }

    Ok(DatasetProvenance::Wiki(Box::new(WikiProvenance {
        corpus_slice: slice.corpus.clone(),
        query_slice: slice.queries.clone(),
        data_url: sources.data.url.clone(),
        data_generation: sources.data.generation.clone(),
        queries_url: sources.queries.url.clone(),
        queries_generation: sources.queries.generation.clone(),
        corpus_sha256: artifacts.corpus_vectors.sha256.clone(),
        queries_sha256: artifacts.query_vectors.sha256.clone(),
        ground_truth_sha256: artifacts.ground_truth.sha256.clone(),
        ground_truth_algorithm: ground_truth.algorithm.clone(),
        ground_truth_distance: ground_truth.distance.clone(),
        ground_truth_examined_rows: ground_truth.examined_corpus_rows_per_query,
        ground_truth_tie_break: ground_truth.tie_break.clone(),
        ground_truth_output: ground_truth.output.clone(),
    })))
}

fn validate_pinned_source(
    label: &str,
    source: &WikiSourceMeta,
    expected_generation: &str,
) -> Result<()> {
    if source.generation != expected_generation {
        return Err(BakeoffError::Dataset(format!(
            "{label} generation is {}, expected pinned generation {expected_generation}",
            source.generation
        )));
    }
    let generation_query = format!("generation={expected_generation}");
    if source.url.trim().is_empty() || !source.url.contains(&generation_query) {
        return Err(BakeoffError::Dataset(format!(
            "{label} URL does not name pinned {generation_query}: {}",
            source.url
        )));
    }
    Ok(())
}

fn validate_sha256(label: &str, value: &str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(BakeoffError::Dataset(format!(
            "{label} sha256 is not 64 hexadecimal characters: {value:?}"
        )));
    }
    Ok(())
}

fn read_all_buffered(path: &Path) -> Result<Vec<u8>> {
    let file = open_file(path)?;
    let expected = file_len(path, &file)?;
    let expected_usize = usize::try_from(expected).map_err(|_| {
        BakeoffError::Dataset(format!(
            "{} is too large for this platform: {expected} bytes",
            path.display()
        ))
    })?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, file);
    let mut bytes = Vec::with_capacity(expected_usize);
    reader
        .read_to_end(&mut bytes)
        .map_err(|source| io_error(path, source))?;
    if bytes.len() != expected_usize {
        return Err(BakeoffError::Dataset(format!(
            "{} changed while being read: metadata said {expected_usize} bytes, read {}",
            path.display(),
            bytes.len()
        )));
    }
    Ok(bytes)
}

fn read_f32_file(path: &Path, expected_values: usize) -> Result<Vec<f32>> {
    let expected_bytes = checked_product(expected_values, 4, "f32 file byte size")?;
    let file = open_exact_size(path, expected_bytes)?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, file);
    let mut values = Vec::with_capacity(expected_values);
    let mut buffer = vec![0u8; IO_BUFFER_BYTES - (IO_BUFFER_BYTES % 4)];

    while values.len() < expected_values {
        let remaining = expected_values - values.len();
        let value_count = remaining.min(buffer.len() / 4);
        let byte_count = value_count * 4;
        reader
            .read_exact(&mut buffer[..byte_count])
            .map_err(|source| io_error(path, source))?;
        for chunk in buffer[..byte_count].chunks_exact(4) {
            let value = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
            if !value.is_finite() {
                return Err(BakeoffError::Dataset(format!(
                    "{} contains non-finite f32 at element {}",
                    path.display(),
                    values.len()
                )));
            }
            values.push(value);
        }
    }
    require_eof(path, &mut reader)?;
    Ok(values)
}

fn read_u32_file(path: &Path, expected_values: usize) -> Result<Vec<u32>> {
    let expected_bytes = checked_product(expected_values, 4, "u32 file byte size")?;
    let file = open_exact_size(path, expected_bytes)?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, file);
    let mut values = Vec::with_capacity(expected_values);
    let mut buffer = vec![0u8; IO_BUFFER_BYTES - (IO_BUFFER_BYTES % 4)];

    while values.len() < expected_values {
        let remaining = expected_values - values.len();
        let value_count = remaining.min(buffer.len() / 4);
        let byte_count = value_count * 4;
        reader
            .read_exact(&mut buffer[..byte_count])
            .map_err(|source| io_error(path, source))?;
        values.extend(
            buffer[..byte_count]
                .chunks_exact(4)
                .map(|chunk| u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])),
        );
    }
    require_eof(path, &mut reader)?;
    Ok(values)
}

fn validate_id_file(path: &Path, expected_lines: usize) -> Result<()> {
    let file = open_file(path)?;
    let reader = BufReader::with_capacity(IO_BUFFER_BYTES, file);
    let mut count = 0usize;
    for line in reader.lines() {
        let line = line.map_err(|source| io_error(path, source))?;
        if line.is_empty() {
            return Err(BakeoffError::Dataset(format!(
                "{} contains an empty id at line {}",
                path.display(),
                count + 1
            )));
        }
        count = count.checked_add(1).ok_or_else(|| {
            BakeoffError::Dataset(format!("{} line count overflowed usize", path.display()))
        })?;
        if count > expected_lines {
            return Err(BakeoffError::Dataset(format!(
                "{} has more than the expected {expected_lines} lines",
                path.display()
            )));
        }
    }
    if count != expected_lines {
        return Err(BakeoffError::Dataset(format!(
            "{} has {count} lines, expected {expected_lines}",
            path.display()
        )));
    }
    Ok(())
}

fn validate_ground_truth(ground_truth: &[u32], meta: &DatasetMeta) -> Result<()> {
    for (query_index, rows) in ground_truth.chunks_exact(meta.gt_k).enumerate() {
        for &row in rows {
            if row as usize >= meta.corpus_n {
                return Err(BakeoffError::Dataset(format!(
                    "ground truth query {query_index} contains out-of-range corpus row {row} (corpus_n={})",
                    meta.corpus_n
                )));
            }
        }
        let mut ordered = rows.to_vec();
        ordered.sort_unstable();
        if let Some(duplicate) = ordered.windows(2).find(|pair| pair[0] == pair[1]) {
            return Err(BakeoffError::Dataset(format!(
                "ground truth query {query_index} repeats corpus row {}",
                duplicate[0]
            )));
        }
    }
    Ok(())
}

fn validate_dataset_unit_norms(dataset: &Dataset) -> Result<()> {
    eprintln!(
        "validating stored unit norms for {} without rewriting f32 bytes",
        dataset.spec.report_name
    );
    validate_unit_rows(
        &dataset.corpus,
        dataset.meta.dims,
        dataset.spec.report_name,
        "corpus",
    )?;
    validate_unit_rows(
        &dataset.queries,
        dataset.meta.dims,
        dataset.spec.report_name,
        "queries",
    )?;
    Ok(())
}

fn validate_unit_rows(
    values: &[f32],
    dims: usize,
    dataset_name: &str,
    section: &str,
) -> Result<()> {
    for (row_index, row) in values.chunks_exact(dims).enumerate() {
        let norm_squared: f64 = row
            .iter()
            .map(|&value| {
                let value = value as f64;
                value * value
            })
            .sum();
        if !norm_squared.is_finite() || (norm_squared - 1.0).abs() > UNIT_NORM_SQUARED_TOLERANCE {
            return Err(BakeoffError::Dataset(format!(
                "{dataset_name} {section} row {row_index} has norm squared {norm_squared}, outside 1 +/- {UNIT_NORM_SQUARED_TOLERANCE}; refusing to renormalize bytes after ground-truth construction"
            )));
        }
    }
    Ok(())
}

fn train_and_assign_ivf(dataset: &Dataset) -> Result<IvfModel> {
    let corpus_n = dataset.meta.corpus_n;
    let target_rows_per_cluster = if dataset.spec.is_gate_dataset {
        WIKI_TARGET_ROWS_PER_CLUSTER
    } else {
        DBPEDIA_TARGET_ROWS_PER_CLUSTER
    };
    let nlist = corpus_n.div_ceil(target_rows_per_cluster);
    if nlist < NPROBES[NPROBES.len() - 1] {
        return Err(BakeoffError::Integrity(format!(
            "{} rows produce nlist={nlist}, fewer than max nprobe {}",
            dataset.spec.report_name,
            NPROBES[NPROBES.len() - 1]
        )));
    }
    let average_rows_per_cluster = corpus_n as f64 / nlist as f64;
    if !(MIN_ROWS_PER_CLUSTER..=MAX_ROWS_PER_CLUSTER).contains(&average_rows_per_cluster) {
        return Err(BakeoffError::Integrity(format!(
            "{} nlist={nlist} gives {average_rows_per_cluster:.3} rows/cluster, outside [{MIN_ROWS_PER_CLUSTER}, {MAX_ROWS_PER_CLUSTER}]",
            dataset.spec.report_name
        )));
    }

    let desired_training_rows = nlist
        .checked_mul(IVF_TRAIN_ROWS_PER_CLUSTER)
        .ok_or_else(|| BakeoffError::Integrity("IVF training row count overflowed".into()))?
        .max(IVF_MIN_TRAIN_ROWS);
    let training_rows = desired_training_rows.min(corpus_n);
    let training_indices = evenly_spaced_indices(corpus_n, training_rows)?;
    let training_refs: Vec<&[f32]> = training_indices
        .iter()
        .map(|&row| dataset.corpus_row(row))
        .collect();

    eprintln!(
        "training {} IVF centroids from {} deterministic rows for {}",
        nlist, training_rows, dataset.spec.report_name
    );
    let centroids = train_kmeans(
        &training_refs,
        dataset.meta.dims,
        nlist,
        IVF_KMEANS_ITERS,
        KMEANS_EPSILON,
    )?;
    if centroids.len() != nlist {
        return Err(BakeoffError::Integrity(format!(
            "train_kmeans returned {} centroids, expected {nlist}",
            centroids.len()
        )));
    }
    for (index, centroid) in centroids.iter().enumerate() {
        if centroid.len() != dataset.meta.dims || centroid.iter().any(|value| !value.is_finite()) {
            return Err(BakeoffError::Integrity(format!(
                "IVF centroid {index} is malformed or non-finite"
            )));
        }
    }

    eprintln!(
        "assigning all {} corpus rows to {} IVF clusters",
        corpus_n, nlist
    );
    let assignment_workers = thread::available_parallelism()
        .map_err(|error| {
            BakeoffError::Integrity(format!(
                "could not determine parallelism for full-corpus IVF assignment: {error}"
            ))
        })?
        .get()
        .min(corpus_n);
    let rows_per_worker = corpus_n.div_ceil(assignment_workers);
    let assignments = thread::scope(|scope| -> Result<Vec<usize>> {
        let mut handles = Vec::with_capacity(assignment_workers);
        for worker in 0..assignment_workers {
            let start = worker * rows_per_worker;
            let end = (start + rows_per_worker).min(corpus_n);
            if start == end {
                continue;
            }
            let centroids = &centroids;
            handles.push(scope.spawn(move || {
                let mut local = Vec::with_capacity(end - start);
                for row_index in start..end {
                    local.push(nearest_l2_centroid(
                        dataset.corpus_row(row_index),
                        centroids,
                    ));
                }
                local
            }));
        }

        let mut ordered = Vec::with_capacity(corpus_n);
        for handle in handles {
            let mut local = handle.join().map_err(|_| {
                BakeoffError::Integrity(
                    "a full-corpus IVF assignment worker panicked; no partial assignment is usable"
                        .into(),
                )
            })?;
            ordered.append(&mut local);
        }
        if ordered.len() != corpus_n {
            return Err(BakeoffError::Integrity(format!(
                "parallel IVF assignment produced {} rows, expected {corpus_n}",
                ordered.len()
            )));
        }
        Ok(ordered)
    })?;
    let mut counts = vec![0usize; nlist];
    for &cluster in &assignments {
        counts[cluster] += 1;
    }

    let mut clusters: Vec<Vec<u32>> = counts
        .iter()
        .map(|&count| Vec::with_capacity(count))
        .collect();
    for (row_index, &cluster) in assignments.iter().enumerate() {
        clusters[cluster].push(row_index as u32);
    }
    let mut cluster_offsets = Vec::with_capacity(nlist + 1);
    cluster_offsets.push(0usize);
    for cluster in &clusters {
        let next = cluster_offsets
            .last()
            .copied()
            .and_then(|offset| offset.checked_add(cluster.len()))
            .ok_or_else(|| BakeoffError::Integrity("cluster offset overflowed usize".into()))?;
        cluster_offsets.push(next);
    }
    if cluster_offsets.last().copied() != Some(corpus_n) {
        return Err(BakeoffError::Integrity(format!(
            "IVF cluster partition covers {} rows, expected {corpus_n}",
            cluster_offsets.last().copied().unwrap_or(0)
        )));
    }

    Ok(IvfModel {
        centroids,
        clusters,
        cluster_offsets,
        training_rows,
        average_rows_per_cluster,
        assignment_workers,
    })
}

fn evaluate_dataset(dataset: &Dataset, ivf: &IvfModel) -> Result<DatasetResult> {
    let probe_ceilings = evaluate_probe_ceilings(dataset, ivf)?;

    let encoders = vec![
        run_one_bit(dataset, ivf)?,
        run_two_bit(dataset, ivf)?,
        run_current_pq(dataset, ivf)?,
        run_sq8(dataset, ivf)?,
    ];

    Ok(DatasetResult {
        name: dataset.spec.report_name,
        corpus_n: dataset.meta.corpus_n,
        query_n: dataset.meta.query_n,
        dims: dataset.meta.dims,
        nlist: ivf.centroids.len(),
        average_rows_per_cluster: ivf.average_rows_per_cluster,
        ivf_training_rows: ivf.training_rows,
        assignment_workers: ivf.assignment_workers,
        probe_ceilings,
        provenance: dataset.provenance.clone(),
        encoders,
    })
}

fn evaluate_probe_ceilings(dataset: &Dataset, ivf: &IvfModel) -> Result<Vec<ProbeCeiling>> {
    eprintln!(
        "computing exact in-probe ceilings for {}",
        dataset.spec.report_name
    );
    let mut recall_at_10 = [0.0f64; NPROBES.len()];
    let mut recall_at_100 = [0.0f64; NPROBES.len()];

    for query_index in 0..dataset.meta.query_n {
        let query = dataset.query_row(query_index);
        let ground_truth = dataset.ground_truth_row(query_index);
        let probes = nearest_probe_clusters(query, &ivf.centroids)?;
        let mut heaps: [BinaryHeap<Candidate>; NPROBES.len()] =
            std::array::from_fn(|_| BinaryHeap::with_capacity(TOP_K + 1));

        for (probe_rank, &cluster_index) in probes.iter().enumerate() {
            for &row in &ivf.clusters[cluster_index] {
                let candidate = Candidate {
                    score: exact_cosine_in_l2_units(query, dataset.corpus_row(row as usize)),
                    row,
                };
                for (nprobe_index, &nprobe) in NPROBES.iter().enumerate() {
                    if probe_rank < nprobe {
                        retain_best(&mut heaps[nprobe_index], candidate, TOP_K);
                    }
                }
            }
        }

        for (nprobe_index, heap) in heaps.into_iter().enumerate() {
            if heap.len() < TOP_K {
                return Err(BakeoffError::Integrity(format!(
                    "exact ceiling query {query_index} at nprobe {} produced only {} rows, fewer than top-k {TOP_K}",
                    NPROBES[nprobe_index],
                    heap.len()
                )));
            }
            let exact = sorted_candidates(heap);
            recall_at_10[nprobe_index] += recall_at_k(&exact, ground_truth, 10);
            recall_at_100[nprobe_index] += recall_at_k(&exact, ground_truth, 100);
        }
    }

    let query_count = dataset.meta.query_n as f64;
    Ok(NPROBES
        .iter()
        .enumerate()
        .map(|(index, &nprobe)| ProbeCeiling {
            nprobe,
            recall_at_10: recall_at_10[index] / query_count,
            recall_at_100: recall_at_100[index] / query_count,
        })
        .collect())
}

fn run_one_bit(dataset: &Dataset, ivf: &IvfModel) -> Result<EncoderResult> {
    eprintln!("encoding {} with 1-bit RaBitQ", dataset.spec.report_name);
    let rotation = make_rotation(dataset.meta.dims)?;
    let rotated_centroids = rotate_centroids(&rotation, &ivf.centroids)?;
    let words_per_code = rabitq::words_per_code(dataset.meta.dims).map_err(rabitq_error)?;
    let word_capacity = checked_product(
        dataset.meta.corpus_n,
        words_per_code,
        "1-bit RaBitQ word capacity",
    )?;
    let mut rotated = vec![0.0f32; dataset.meta.dims];
    let mut scratch = vec![0.0f32; dataset.meta.dims];
    let mut words = Vec::with_capacity(word_capacity);
    let mut factors = Vec::with_capacity(dataset.meta.corpus_n);

    let started = Instant::now();
    for (cluster_index, rows) in ivf.clusters.iter().enumerate() {
        let centroid = &ivf.centroids[cluster_index];
        for &row in rows {
            rotation
                .rotate_residual(
                    dataset.corpus_row(row as usize),
                    centroid,
                    &mut rotated,
                    &mut scratch,
                )
                .map_err(rabitq_error)?;
            let start = words.len();
            words.resize(start + words_per_code, 0);
            factors.push(
                rabitq::encode_one_bit_into(&rotated, &mut words[start..start + words_per_code])
                    .map_err(rabitq_error)?,
            );
        }
    }
    let encode_seconds = nonzero_elapsed(started, "1-bit RaBitQ encode")?;
    require_code_count("1-bit RaBitQ factor", factors.len(), dataset.meta.corpus_n)?;
    if words.len() != word_capacity {
        return Err(BakeoffError::Integrity(format!(
            "1-bit RaBitQ emitted {} words, expected {word_capacity}",
            words.len()
        )));
    }

    let mut scorer = OneBitScorer::new(
        &rotation,
        &rotated_centroids,
        &words,
        &factors,
        words_per_code,
        dataset.meta.dims,
    );
    let evaluation = evaluate_encoder(dataset, ivf, &mut scorer)?;
    Ok(finish_encoder_result(
        "rabitq-1bit",
        dataset.meta.dims / 8 + 8,
        encode_seconds,
        dataset.meta.corpus_n,
        evaluation,
    ))
}

fn run_two_bit(dataset: &Dataset, ivf: &IvfModel) -> Result<EncoderResult> {
    eprintln!(
        "encoding {} with 2-bit Extended RaBitQ",
        dataset.spec.report_name
    );
    let rotation = make_rotation(dataset.meta.dims)?;
    let rotated_centroids = rotate_centroids(&rotation, &ivf.centroids)?;
    let words_per_code = rabitq::words_per_code(dataset.meta.dims).map_err(rabitq_error)?;
    let plane_capacity = checked_product(
        dataset.meta.corpus_n,
        words_per_code,
        "2-bit Extended RaBitQ plane capacity",
    )?;
    let mut rotated = vec![0.0f32; dataset.meta.dims];
    let mut scratch = vec![0.0f32; dataset.meta.dims];
    let mut order_scratch = vec![0usize; dataset.meta.dims];
    let mut low_plane = Vec::with_capacity(plane_capacity);
    let mut high_plane = Vec::with_capacity(plane_capacity);
    let mut factors = Vec::with_capacity(dataset.meta.corpus_n);

    let started = Instant::now();
    for (cluster_index, rows) in ivf.clusters.iter().enumerate() {
        let centroid = &ivf.centroids[cluster_index];
        for &row in rows {
            rotation
                .rotate_residual(
                    dataset.corpus_row(row as usize),
                    centroid,
                    &mut rotated,
                    &mut scratch,
                )
                .map_err(rabitq_error)?;
            let start = low_plane.len();
            low_plane.resize(start + words_per_code, 0);
            high_plane.resize(start + words_per_code, 0);
            factors.push(
                rabitq::encode_two_bit_into(
                    &rotated,
                    &mut low_plane[start..start + words_per_code],
                    &mut high_plane[start..start + words_per_code],
                    &mut order_scratch,
                )
                .map_err(rabitq_error)?,
            );
        }
    }
    let encode_seconds = nonzero_elapsed(started, "2-bit Extended RaBitQ encode")?;
    require_code_count(
        "2-bit Extended RaBitQ factor",
        factors.len(),
        dataset.meta.corpus_n,
    )?;
    if low_plane.len() != plane_capacity || high_plane.len() != plane_capacity {
        return Err(BakeoffError::Integrity(format!(
            "2-bit Extended RaBitQ emitted low/high plane words {}/{}, expected {plane_capacity} each",
            low_plane.len(),
            high_plane.len()
        )));
    }

    let mut scorer = TwoBitScorer::new(
        &rotation,
        &rotated_centroids,
        &low_plane,
        &high_plane,
        &factors,
        words_per_code,
        dataset.meta.dims,
    );
    let evaluation = evaluate_encoder(dataset, ivf, &mut scorer)?;
    Ok(finish_encoder_result(
        "rabitq-2bit",
        dataset.meta.dims / 4 + 8,
        encode_seconds,
        dataset.meta.corpus_n,
        evaluation,
    ))
}

fn run_current_pq(dataset: &Dataset, ivf: &IvfModel) -> Result<EncoderResult> {
    eprintln!(
        "training current-v3 PQ control for {} (M={}, sample={}, iters={})",
        dataset.spec.report_name, PQ_SUBQUANTIZERS, PQ_TRAIN_ROWS, PQ_KMEANS_ITERS
    );
    let sample_count = PQ_TRAIN_ROWS.min(dataset.meta.corpus_n);
    let sample_positions = evenly_spaced_indices(dataset.meta.corpus_n, sample_count)?;
    let sample_rows: Vec<usize> = sample_positions
        .iter()
        .map(|&position| cluster_order_row(ivf, position))
        .collect::<Result<Vec<_>>>()?;
    let sample_refs: Vec<&[f32]> = sample_rows
        .iter()
        .map(|&row| dataset.corpus_row(row))
        .collect();
    let codebook = PqCodebook::train(
        &sample_refs,
        dataset.meta.dims,
        PQ_SUBQUANTIZERS,
        PQ_KMEANS_ITERS,
    )?;

    let capacity = checked_product(dataset.meta.corpus_n, PQ_SUBQUANTIZERS, "PQ code capacity")?;
    let mut codes = Vec::with_capacity(capacity);
    let started = Instant::now();
    for rows in &ivf.clusters {
        for &row in rows {
            let encoded = codebook.encode(dataset.corpus_row(row as usize));
            codes.extend_from_slice(&encoded);
        }
    }
    let encode_seconds = nonzero_elapsed(started, "current-v3 PQ encode")?;
    if codes.len() != capacity {
        return Err(BakeoffError::Integrity(format!(
            "current-v3 PQ emitted {} bytes, expected {capacity}",
            codes.len()
        )));
    }

    let mut scorer = PqScorer {
        codebook: &codebook,
        codes: &codes,
        table: Vec::new(),
    };
    let evaluation = evaluate_encoder(dataset, ivf, &mut scorer)?;
    Ok(finish_encoder_result(
        "current-v3-pq",
        PQ_SUBQUANTIZERS,
        encode_seconds,
        dataset.meta.corpus_n,
        evaluation,
    ))
}

fn run_sq8(dataset: &Dataset, ivf: &IvfModel) -> Result<EncoderResult> {
    eprintln!("calibrating SQ8 control for {}", dataset.spec.report_name);
    let corpus_refs: Vec<&[f32]> = (0..dataset.meta.corpus_n)
        .map(|row| dataset.corpus_row(row))
        .collect();
    let calibration = SqCalibration::calibrate(&corpus_refs, dataset.meta.dims);
    drop(corpus_refs);

    let capacity = checked_product(
        dataset.meta.corpus_n,
        dataset.meta.dims,
        "SQ8 code capacity",
    )?;
    let mut codes = Vec::with_capacity(capacity);
    let started = Instant::now();
    for rows in &ivf.clusters {
        for &row in rows {
            let encoded = calibration.encode(dataset.corpus_row(row as usize));
            codes.extend_from_slice(&encoded);
        }
    }
    let encode_seconds = nonzero_elapsed(started, "SQ8 encode")?;
    if codes.len() != capacity {
        return Err(BakeoffError::Integrity(format!(
            "SQ8 emitted {} bytes, expected {capacity}",
            codes.len()
        )));
    }

    let mut scorer = Sq8Scorer {
        calibration: &calibration,
        codes: &codes,
        query: vec![0.0f32; dataset.meta.dims],
    };
    let evaluation = evaluate_encoder(dataset, ivf, &mut scorer)?;
    Ok(finish_encoder_result(
        "sq8",
        dataset.meta.dims,
        encode_seconds,
        dataset.meta.corpus_n,
        evaluation,
    ))
}

struct EvaluationResult {
    moments: RunningMoments,
    raw_recall: Vec<RawRecall>,
    cells: Vec<MatrixCell>,
}

fn evaluate_encoder(
    dataset: &Dataset,
    ivf: &IvfModel,
    scorer: &mut dyn ApproximateScorer,
) -> Result<EvaluationResult> {
    let mut recall_10 = vec![0.0f64; NPROBES.len() * MARGINS.len()];
    let mut recall_100 = vec![0.0f64; NPROBES.len() * MARGINS.len()];
    let mut raw_recall_10 = vec![0.0f64; NPROBES.len()];
    let mut raw_recall_100 = vec![0.0f64; NPROBES.len()];
    let mut moments = RunningMoments::default();

    for query_index in 0..dataset.meta.query_n {
        let query = dataset.query_row(query_index);
        let ground_truth = dataset.ground_truth_row(query_index);
        let probes = nearest_probe_clusters(query, &ivf.centroids)?;
        scorer.prepare_query(query_index, query)?;
        let mut heaps: [BinaryHeap<Candidate>; NPROBES.len()] =
            std::array::from_fn(|_| BinaryHeap::with_capacity(MAX_CANDIDATES + 1));

        for (probe_rank, &cluster_index) in probes.iter().enumerate() {
            scorer.prepare_cluster(query_index, cluster_index, &ivf.centroids[cluster_index])?;
            let code_start = ivf.cluster_offsets[cluster_index];
            for (local_index, &row) in ivf.clusters[cluster_index].iter().enumerate() {
                let code_index = code_start + local_index;
                let approximate = scorer.score(code_index)?;
                if !approximate.is_finite() {
                    return Err(BakeoffError::Integrity(format!(
                        "non-finite approximate score for query {query_index}, cluster {cluster_index}, row {row}"
                    )));
                }

                if estimator_error_sample(query_index, cluster_index, row) {
                    let exact = exact_cosine_in_l2_units(query, dataset.corpus_row(row as usize));
                    moments.push((approximate - exact) as f64);
                }
                let candidate = Candidate {
                    score: approximate,
                    row,
                };
                for (nprobe_index, &nprobe) in NPROBES.iter().enumerate() {
                    if probe_rank < nprobe {
                        retain_best(&mut heaps[nprobe_index], candidate, MAX_CANDIDATES);
                    }
                }
            }
        }

        for (nprobe_index, heap) in heaps.into_iter().enumerate() {
            if heap.len() < MAX_CANDIDATES {
                return Err(BakeoffError::Integrity(format!(
                    "query {query_index} at nprobe {} produced only {} candidates, fewer than required {MAX_CANDIDATES}",
                    NPROBES[nprobe_index],
                    heap.len()
                )));
            }
            let approximate = sorted_candidates(heap);
            raw_recall_10[nprobe_index] += recall_at_k(&approximate, ground_truth, 10);
            raw_recall_100[nprobe_index] += recall_at_k(&approximate, ground_truth, 100);

            for (margin_index, &margin) in MARGINS.iter().enumerate() {
                let limit = margin * TOP_K;
                let mut exact_candidates: Vec<Candidate> = approximate[..limit]
                    .iter()
                    .map(|candidate| Candidate {
                        score: exact_cosine_in_l2_units(
                            query,
                            dataset.corpus_row(candidate.row as usize),
                        ),
                        row: candidate.row,
                    })
                    .collect();
                exact_candidates.sort_unstable();
                let cell = nprobe_index * MARGINS.len() + margin_index;
                recall_10[cell] += recall_at_k(&exact_candidates, ground_truth, 10);
                recall_100[cell] += recall_at_k(&exact_candidates, ground_truth, 100);
            }
        }
    }

    if moments.count == 0 {
        return Err(BakeoffError::Integrity(
            "estimator bias/variance received zero score samples".into(),
        ));
    }
    let query_count = dataset.meta.query_n as f64;
    let raw_recall = NPROBES
        .iter()
        .enumerate()
        .map(|(index, &nprobe)| RawRecall {
            nprobe,
            recall_at_10: raw_recall_10[index] / query_count,
            recall_at_100: raw_recall_100[index] / query_count,
        })
        .collect();
    let mut cells = Vec::with_capacity(NPROBES.len() * MARGINS.len());
    for (nprobe_index, &nprobe) in NPROBES.iter().enumerate() {
        for (margin_index, &margin) in MARGINS.iter().enumerate() {
            let cell = nprobe_index * MARGINS.len() + margin_index;
            cells.push(MatrixCell {
                nprobe,
                margin,
                recall_at_10: recall_10[cell] / query_count,
                recall_at_100: recall_100[cell] / query_count,
            });
        }
    }

    Ok(EvaluationResult {
        moments,
        raw_recall,
        cells,
    })
}

fn finish_encoder_result(
    name: &'static str,
    logical_bytes_per_vector: usize,
    encode_seconds: f64,
    corpus_n: usize,
    evaluation: EvaluationResult,
) -> EncoderResult {
    EncoderResult {
        name,
        logical_bytes_per_vector,
        encode_seconds,
        encode_vectors_per_second_per_core: corpus_n as f64 / encode_seconds,
        error_samples: evaluation.moments.count,
        estimator_bias: evaluation.moments.mean,
        estimator_variance: evaluation.moments.variance(),
        raw_recall: evaluation.raw_recall,
        cells: evaluation.cells,
    }
}

struct OneBitScorer<'a> {
    rotation: &'a rabitq::StructuredRotation,
    rotated_centroids: &'a [Vec<f32>],
    words: &'a [u64],
    factors: &'a [rabitq::OneBitFactors],
    words_per_code: usize,
    rotated_query: Vec<f32>,
    rotated_residual: Vec<f32>,
    scratch: Vec<f32>,
    cluster_query: Option<rabitq::QueryAdc4>,
    query_residual_norm_squared: f32,
}

impl<'a> OneBitScorer<'a> {
    fn new(
        rotation: &'a rabitq::StructuredRotation,
        rotated_centroids: &'a [Vec<f32>],
        words: &'a [u64],
        factors: &'a [rabitq::OneBitFactors],
        words_per_code: usize,
        dims: usize,
    ) -> Self {
        Self {
            rotation,
            rotated_centroids,
            words,
            factors,
            words_per_code,
            rotated_query: vec![0.0; dims],
            rotated_residual: vec![0.0; dims],
            scratch: vec![0.0; dims],
            cluster_query: None,
            query_residual_norm_squared: 0.0,
        }
    }
}

impl ApproximateScorer for OneBitScorer<'_> {
    fn prepare_query(&mut self, _query_index: usize, query: &[f32]) -> Result<()> {
        self.rotated_query.copy_from_slice(query);
        self.rotation
            .rotate_in_place(&mut self.rotated_query, &mut self.scratch)
            .map_err(rabitq_error)
    }

    fn prepare_cluster(
        &mut self,
        query_index: usize,
        cluster_index: usize,
        _centroid: &[f32],
    ) -> Result<()> {
        for ((residual, &query), &centroid) in self
            .rotated_residual
            .iter_mut()
            .zip(self.rotated_query.iter())
            .zip(self.rotated_centroids[cluster_index].iter())
        {
            *residual = query - centroid;
        }
        self.query_residual_norm_squared = squared_norm(&self.rotated_residual);
        let seed = query_adc_seed(query_index, cluster_index);
        self.cluster_query =
            Some(rabitq::prepare_query_adc4(&self.rotated_residual, seed).map_err(rabitq_error)?);
        Ok(())
    }

    fn score(&self, code_index: usize) -> Result<f32> {
        let query = self.cluster_query.as_ref().ok_or_else(|| {
            BakeoffError::Integrity("1-bit scorer used before cluster query preparation".into())
        })?;
        let start = code_index
            .checked_mul(self.words_per_code)
            .ok_or_else(|| BakeoffError::Integrity("1-bit code offset overflowed".into()))?;
        rabitq::estimate_l2_one_bit_parts(
            &self.words[start..start + self.words_per_code],
            self.factors[code_index],
            query,
            self.query_residual_norm_squared,
        )
        .map_err(rabitq_error)
    }
}

struct TwoBitScorer<'a> {
    rotation: &'a rabitq::StructuredRotation,
    rotated_centroids: &'a [Vec<f32>],
    low_plane: &'a [u64],
    high_plane: &'a [u64],
    factors: &'a [rabitq::TwoBitFactors],
    words_per_code: usize,
    rotated_query: Vec<f32>,
    rotated_residual: Vec<f32>,
    scratch: Vec<f32>,
    cluster_query: Option<rabitq::QueryAdc4>,
    query_residual_norm_squared: f32,
}

impl<'a> TwoBitScorer<'a> {
    fn new(
        rotation: &'a rabitq::StructuredRotation,
        rotated_centroids: &'a [Vec<f32>],
        low_plane: &'a [u64],
        high_plane: &'a [u64],
        factors: &'a [rabitq::TwoBitFactors],
        words_per_code: usize,
        dims: usize,
    ) -> Self {
        Self {
            rotation,
            rotated_centroids,
            low_plane,
            high_plane,
            factors,
            words_per_code,
            rotated_query: vec![0.0; dims],
            rotated_residual: vec![0.0; dims],
            scratch: vec![0.0; dims],
            cluster_query: None,
            query_residual_norm_squared: 0.0,
        }
    }
}

impl ApproximateScorer for TwoBitScorer<'_> {
    fn prepare_query(&mut self, _query_index: usize, query: &[f32]) -> Result<()> {
        self.rotated_query.copy_from_slice(query);
        self.rotation
            .rotate_in_place(&mut self.rotated_query, &mut self.scratch)
            .map_err(rabitq_error)
    }

    fn prepare_cluster(
        &mut self,
        query_index: usize,
        cluster_index: usize,
        _centroid: &[f32],
    ) -> Result<()> {
        for ((residual, &query), &centroid) in self
            .rotated_residual
            .iter_mut()
            .zip(self.rotated_query.iter())
            .zip(self.rotated_centroids[cluster_index].iter())
        {
            *residual = query - centroid;
        }
        self.query_residual_norm_squared = squared_norm(&self.rotated_residual);
        let seed = query_adc_seed(query_index, cluster_index);
        self.cluster_query =
            Some(rabitq::prepare_query_adc4(&self.rotated_residual, seed).map_err(rabitq_error)?);
        Ok(())
    }

    fn score(&self, code_index: usize) -> Result<f32> {
        let query = self.cluster_query.as_ref().ok_or_else(|| {
            BakeoffError::Integrity("2-bit scorer used before cluster query preparation".into())
        })?;
        let start = code_index
            .checked_mul(self.words_per_code)
            .ok_or_else(|| BakeoffError::Integrity("2-bit code offset overflowed".into()))?;
        rabitq::estimate_l2_two_bit_parts(
            &self.low_plane[start..start + self.words_per_code],
            &self.high_plane[start..start + self.words_per_code],
            self.factors[code_index],
            query,
            self.query_residual_norm_squared,
        )
        .map_err(rabitq_error)
    }
}

struct PqScorer<'a> {
    codebook: &'a PqCodebook,
    codes: &'a [u8],
    table: Vec<f32>,
}

impl ApproximateScorer for PqScorer<'_> {
    fn prepare_query(&mut self, _query_index: usize, query: &[f32]) -> Result<()> {
        self.table = self.codebook.build_adc_table(query, DistanceMetric::Cosine);
        Ok(())
    }

    fn prepare_cluster(
        &mut self,
        _query_index: usize,
        _cluster_index: usize,
        _centroid: &[f32],
    ) -> Result<()> {
        Ok(())
    }

    fn score(&self, code_index: usize) -> Result<f32> {
        let start = code_index
            .checked_mul(PQ_SUBQUANTIZERS)
            .ok_or_else(|| BakeoffError::Integrity("PQ code offset overflowed".into()))?;
        let end = start + PQ_SUBQUANTIZERS;
        Ok(self
            .codebook
            .adc_distance(&self.table, &self.codes[start..end]))
    }
}

struct Sq8Scorer<'a> {
    calibration: &'a SqCalibration,
    codes: &'a [u8],
    query: Vec<f32>,
}

impl ApproximateScorer for Sq8Scorer<'_> {
    fn prepare_query(&mut self, _query_index: usize, query: &[f32]) -> Result<()> {
        self.query.copy_from_slice(query);
        Ok(())
    }

    fn prepare_cluster(
        &mut self,
        _query_index: usize,
        _cluster_index: usize,
        _centroid: &[f32],
    ) -> Result<()> {
        Ok(())
    }

    fn score(&self, code_index: usize) -> Result<f32> {
        let start = code_index
            .checked_mul(self.calibration.dim)
            .ok_or_else(|| BakeoffError::Integrity("SQ8 code offset overflowed".into()))?;
        let end = start + self.calibration.dim;
        Ok(2.0
            * self
                .calibration
                .asymmetric_cosine(&self.query, &self.codes[start..end]))
    }
}

fn validate_dataset_anchors(result: &DatasetResult) -> Result<()> {
    let one_bit = encoder(result, "rabitq-1bit")?;
    let pq = encoder(result, "current-v3-pq")?;
    let sq8 = encoder(result, "sq8")?;

    for one_cell in &one_bit.cells {
        let sq_cell = cell(sq8, one_cell.nprobe, one_cell.margin)?;
        if sq_cell.recall_at_10 + f64::EPSILON < one_cell.recall_at_10
            || sq_cell.recall_at_100 + f64::EPSILON < one_cell.recall_at_100
        {
            return Err(BakeoffError::Integrity(format!(
                "SQ8 anchor failed on {} at nprobe={}, margin={}: SQ8 recall@10/@100={:.6}/{:.6}, 1-bit={:.6}/{:.6}",
                result.name,
                one_cell.nprobe,
                one_cell.margin,
                sq_cell.recall_at_10,
                sq_cell.recall_at_100,
                one_cell.recall_at_10,
                one_cell.recall_at_100
            )));
        }
    }

    if result.name == "dbpedia100k" {
        let raw = pq
            .raw_recall
            .iter()
            .find(|entry| entry.nprobe == PQ_ANCHOR_NPROBE)
            .ok_or_else(|| BakeoffError::Integrity("missing PQ nprobe=16 raw anchor".into()))?;
        let anchor = cell(pq, PQ_ANCHOR_NPROBE, PQ_ANCHOR_MARGIN)?;
        let delta = (anchor.recall_at_100 - PQ_ANCHOR_TARGET).abs();
        if delta > PQ_ANCHOR_TOLERANCE {
            let rerank_summary = pq
                .cells
                .iter()
                .filter(|entry| entry.nprobe == PQ_ANCHOR_NPROBE)
                .map(|entry| format!("{}x={:.6}", entry.margin, entry.recall_at_100))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(BakeoffError::Integrity(format!(
                "current-v3 PQ anchor failed on dbpedia100k: nprobe={} margin={}x exact-rerank recall@100 was {:.6}, outside {:.2} +/- {:.2}; unrescored recall@100 was {:.6}, and exact-rerank recall@100 by margin was [{rerank_summary}]; do not trust the bake-off matrix",
                PQ_ANCHOR_NPROBE,
                PQ_ANCHOR_MARGIN,
                anchor.recall_at_100,
                PQ_ANCHOR_TARGET,
                PQ_ANCHOR_TOLERANCE,
                raw.recall_at_100,
            )));
        }
    }
    Ok(())
}

fn render_report(
    results: &[DatasetResult],
    rotation_quality: &rabitq::RotationQuality,
) -> Result<String> {
    let wiki = results
        .iter()
        .find(|result| result.name == "wiki_dpr_e5")
        .ok_or_else(|| BakeoffError::Integrity("missing wiki_dpr_e5 result".into()))?;
    let gate_encoder = encoder(wiki, "rabitq-1bit")?;
    let gate_cell = cell(gate_encoder, 16, 4)?;
    let gate_ceiling = wiki
        .probe_ceilings
        .iter()
        .find(|ceiling| ceiling.nprobe == 16)
        .ok_or_else(|| {
            BakeoffError::Integrity(
                "wiki_dpr_e5 exact in-probe ceiling is missing nprobe 16".into(),
            )
        })?;
    let gate_passed = gate_cell.recall_at_100 >= GATE_RECALL;
    let gate_interpretation = if gate_passed {
        "quantizer gate passed"
    } else if gate_ceiling.recall_at_100 < GATE_RECALL {
        "coarse-IVF-limited; quantizer go/no-go inconclusive"
    } else {
        "quantizer-limited; evaluate the plan's wider-code decision"
    };

    let mut output = String::new();
    output.push_str("# Phase 1 Quantization Bake-off\n\n");
    output.push_str(
        "Deterministic offline evaluation over the datasets' stored, already-normalized cosine f32 vectors. The driver validates unit norms without rewriting bytes, preserving the exact vectors used to construct ground truth. Zeppelin's public k-means provides deterministic centroids; full-corpus assignment uses the production builder's squared-L2 geometry and query probing uses production cosine geometry. Approximate estimators rank only rows in those IVF probe clusters; each reported margin is then reranked by exact production f32 cosine, multiplied by two only to use squared-L2-equivalent score units. Lower estimator scores are better.\n\n",
    );
    output.push_str("## Gate\n\n");
    output.push_str(&format!(
        "> **{}: 1-bit RaBitQ residual at nprobe 16, margin 4x on wiki_dpr_e5 achieved recall@100 = {:.6} (required >= {:.2}).**\n\n",
        if gate_passed { "PASS" } else { "FAIL" },
        gate_cell.recall_at_100,
        GATE_RECALL
    ));
    output.push_str(&format!("> **Interpretation: {gate_interpretation}.**\n\n"));
    output.push_str(
        "A failed gate is a decision result, not permission to select a different dataset or encoder silently.\n\n",
    );
    output.push_str(&format!(
        "The exact f32 in-probe recall@100 ceiling at nprobe 16 is {:.6}. {}\n\n",
        gate_ceiling.recall_at_100,
        if gate_ceiling.recall_at_100 < GATE_RECALL {
            "The coarse partition therefore makes the 0.96 end-to-end gate unreachable before quantization; the full encoder matrix remains the required evidence, but it cannot by itself justify selecting a wider code."
        } else {
            "The coarse partition clears the target, so the gate cell isolates whether estimator ranking and the 4x exact rerank retain enough candidates."
        }
    ));
    output.push_str(&format!(
        "The 1-bit gate cell recovers {:.3}% of that aggregate exact ceiling. This ratio is diagnostic rather than a replacement for the absolute recall gate.\n\n",
        100.0 * gate_cell.recall_at_100 / gate_ceiling.recall_at_100
    ));

    output.push_str("## Structured rotation quality oracle\n\n");
    output.push_str("The deterministic 768-dimensional oracle compares the production structured rotation with a dense Gaussian/Gram-Schmidt rotation on paired anisotropic sparse inputs. The driver aborts unless the paired MSE delta is within five standard errors; it also inherits the module's requirement that structured rotation materially beat identity.\n\n");
    output.push_str("| pairs | structured RMSE | dense RMSE | identity RMSE | structured/identity | max ratio | structured MSE - dense MSE | delta standard error | 5-SE limit | verdict |\n");
    output.push_str("| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n");
    output.push_str(&format!(
        "| {} | {:.9} | {:.9} | {:.9} | {:.6} | {:.6} | {:.12} | {:.12} | {:.12} | PASS |\n\n",
        rotation_quality.pairs,
        rotation_quality.structured_rmse,
        rotation_quality.dense_rmse,
        rotation_quality.identity_rmse,
        rotation_quality.structured_rmse / rotation_quality.identity_rmse,
        rabitq::ROTATION_IDENTITY_MAX_RMSE_RATIO,
        rotation_quality.mse_delta,
        rotation_quality.mse_delta_standard_error,
        5.0 * rotation_quality.mse_delta_standard_error
    ));

    output.push_str("## Dataset and IVF setup\n\n");
    output.push_str(
        "| dataset | corpus rows | queries | dims | nlist | rows/cluster | IVF train rows | assignment workers |\n",
    );
    output.push_str("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for result in results {
        output.push_str(&format!(
            "| {} | {} | {} | {} | {} | {:.3} | {} | {} |\n",
            result.name,
            result.corpus_n,
            result.query_n,
            result.dims,
            result.nlist,
            result.average_rows_per_cluster,
            result.ivf_training_rows,
            result.assignment_workers
        ));
    }
    output.push('\n');

    output.push_str("## Dataset provenance\n\n");
    output.push_str("The wiki source generations are hard-pinned by the loader. Artifact SHA-256 values below are carried from the builder's `meta.json`; this dependency-free driver independently validates exact artifact sizes, shapes, finite values, row ids, and ground-truth indexes but does not reimplement SHA-256.\n\n");
    for result in results {
        output.push_str(&format!("### {}\n\n", result.name));
        match &result.provenance {
            DatasetProvenance::Dbpedia { source } => {
                output.push_str(&format!("- Source: {source}\n"));
            }
            DatasetProvenance::Wiki(details) => {
                let WikiProvenance {
                    corpus_slice,
                    query_slice,
                    data_url,
                    data_generation,
                    queries_url,
                    queries_generation,
                    corpus_sha256,
                    queries_sha256,
                    ground_truth_sha256,
                    ground_truth_algorithm,
                    ground_truth_distance,
                    ground_truth_examined_rows,
                    ground_truth_tie_break,
                    ground_truth_output,
                } = details.as_ref();
                output.push_str(&format!("- Corpus slice: {corpus_slice}\n"));
                output.push_str(&format!("- Query slice: {query_slice}\n"));
                output.push_str("- Full-corpus `closest_ids` reused: false\n");
                output.push_str(&format!(
                    "- Data source generation: `{data_generation}`; URL: `{data_url}`\n"
                ));
                output.push_str(&format!(
                    "- Query source generation: `{queries_generation}`; URL: `{queries_url}`\n"
                ));
                output.push_str(&format!(
                    "- `corpus_vectors.f32` SHA-256: `{corpus_sha256}`\n"
                ));
                output.push_str(&format!(
                    "- `query_vectors.f32` SHA-256: `{queries_sha256}`\n"
                ));
                output.push_str(&format!(
                    "- `ground_truth_top100.u32` SHA-256: `{ground_truth_sha256}`\n"
                ));
                output.push_str(&format!(
                    "- Ground truth: {ground_truth_algorithm}; distance: {ground_truth_distance}; examined corpus rows/query: {ground_truth_examined_rows}; tie break: {ground_truth_tie_break}; output: {ground_truth_output}.\n"
                ));
            }
        }
        output.push('\n');
    }

    output.push_str(&format!(
        "IVF centroids use Zeppelin's public `train_kmeans` with {} deterministic evenly spaced training rows per requested centroid (minimum {}, capped by corpus size), {} iterations, and epsilon `{}`. Every corpus row is assigned by minimum squared L2, matching `build.rs`; scoped workers write disjoint row ranges and are joined in range order, preserving deterministic row order and lower-cluster-id tie breaks. Query probes rank the non-unit centroids with production cosine, matching `search.rs`.\n\n",
        IVF_TRAIN_ROWS_PER_CLUSTER,
        IVF_MIN_TRAIN_ROWS,
        IVF_KMEANS_ITERS,
        KMEANS_EPSILON,
    ));
    output.push_str("Production geometry caveat: `build.rs` assigns with squared L2 while `search.rs` probes non-unit centroids with namespace cosine. The canonical run deliberately mirrors that current behavior rather than silently substituting a new partition. A rejected custom 2M full-corpus spherical run produced only 0.858650 exact nprobe-16 recall@100 and is preserved in `bakeoff-spherical-rejected.md`; it is not the Phase 1 verdict. The canonical exact ceiling below quantifies the current coarse-IVF limit directly.\n\n");
    output.push_str("Methodology caveat: production passes every segment vector to `train_kmeans`, but k-means++ initialization over two million rows and the requested nlist is prohibitively expensive for this offline run. The driver uses a deterministic `32 * nlist` evenly spaced training sample (with a 4,096-row minimum), then assigns every corpus row. The 25-iteration cap and epsilon match production defaults. The exact in-probe ceiling is a diagnostic upper bound on every encoder's end-to-end recall; a ceiling below 0.96 is reported as part of the gate failure rather than aborting, because the phase plan requires measured curves either way.\n\n");

    output.push_str("## Exact in-probe ceilings\n\n");
    output.push_str("These are exact production-cosine scans of every vector in the same centroid-probed clusters, with no quantization or candidate margin. They isolate IVF loss from estimator loss.\n\n");
    output.push_str("| dataset | nprobe | exact recall@10 ceiling | exact recall@100 ceiling |\n");
    output.push_str("| --- | ---: | ---: | ---: |\n");
    for result in results {
        for ceiling in &result.probe_ceilings {
            output.push_str(&format!(
                "| {} | {} | {:.6} | {:.6} |\n",
                result.name, ceiling.nprobe, ceiling.recall_at_10, ceiling.recall_at_100
            ));
        }
    }
    output.push('\n');

    output.push_str("## Single-core encode throughput\n\n");
    output.push_str("Encoding loops are sequential and exclude model training/calibration, centroid rotation, and evaluation. Logical bytes include the two f32 correction scalars for RaBitQ.\n\n");
    output.push_str(
        "| dataset | encoder | logical bytes/vector | encode seconds | vectors/sec/core |\n",
    );
    output.push_str("| --- | --- | ---: | ---: | ---: |\n");
    for result in results {
        for encoder in &result.encoders {
            output.push_str(&format!(
                "| {} | {} | {} | {:.6} | {:.3} |\n",
                result.name,
                encoder.name,
                encoder.logical_bytes_per_vector,
                encoder.encode_seconds,
                encoder.encode_vectors_per_second_per_core
            ));
        }
    }
    output.push('\n');

    output.push_str("## Estimator error\n\n");
    output.push_str(&format!(
        "Bias and population variance are for `estimated squared-L2-equivalent score - 2 * exact cosine distance` over a deterministic approximately 1/{} sample of rows scored at nprobe 32. A pair is sampled when `splitmix64(rotation_seed XOR mixed(query_index, cluster_index, row_id)) mod {} == 0`; the exact sample count is reported per encoder. Inputs are validated unit-normalized without mutation. PQ uses the current-v3 cosine ADC table. SQ8 uses production asymmetric cosine multiplied by two solely to express its error in comparable squared-L2 units.\n\n",
        ESTIMATOR_ERROR_SAMPLE_MODULUS, ESTIMATOR_ERROR_SAMPLE_MODULUS
    ));
    output.push_str("| dataset | encoder | samples | bias | variance |\n");
    output.push_str("| --- | --- | ---: | ---: | ---: |\n");
    for result in results {
        for encoder in &result.encoders {
            output.push_str(&format!(
                "| {} | {} | {} | {:.9} | {:.9} |\n",
                result.name,
                encoder.name,
                encoder.error_samples,
                encoder.estimator_bias,
                encoder.estimator_variance
            ));
        }
    }
    output.push('\n');

    output.push_str("## Unrescored estimator controls\n\n");
    output.push_str("These top-100 numbers are before exact reranking. They expose how much each margin recovers but are not the plan's approximately-0.88 PQ anchor, because the historical number included an exact rerank after approximate selection. The pinned anchor is the dbpedia current-v3 PQ nprobe-16, margin-3x matrix cell below. It is still not a literal reproduction of the historical path: Zeppelin's accepted PQ path ranked clusters and used different nlist, bit-width, and selection-budget semantics, while this driver vector-ranks the requested v3 64-subquantizer control with 2,000–3,000-row IVF clusters.\n\n");
    output.push_str("| dataset | encoder | nprobe | raw recall@10 | raw recall@100 |\n");
    output.push_str("| --- | --- | ---: | ---: | ---: |\n");
    for result in results {
        for encoder in &result.encoders {
            for raw in &encoder.raw_recall {
                output.push_str(&format!(
                    "| {} | {} | {} | {:.6} | {:.6} |\n",
                    result.name, encoder.name, raw.nprobe, raw.recall_at_10, raw.recall_at_100
                ));
            }
        }
    }
    output.push('\n');

    output.push_str("## Full exact-rerank recall matrix\n\n");
    output.push_str("| dataset | encoder | nprobe | margin | recall@10 | recall@100 |\n");
    output.push_str("| --- | --- | ---: | ---: | ---: | ---: |\n");
    for result in results {
        for encoder in &result.encoders {
            for cell in &encoder.cells {
                output.push_str(&format!(
                    "| {} | {} | {} | {}x | {:.6} | {:.6} |\n",
                    result.name,
                    encoder.name,
                    cell.nprobe,
                    cell.margin,
                    cell.recall_at_10,
                    cell.recall_at_100
                ));
            }
        }
    }
    output.push('\n');
    output.push_str("## Sanity anchors\n\n");
    output.push_str(&format!(
        "- Current-v3 PQ on dbpedia100k at nprobe {}, margin {}x must have exact-rerank recall@100 within `{:.2} +/- {:.2}`.\n",
        PQ_ANCHOR_NPROBE, PQ_ANCHOR_MARGIN, PQ_ANCHOR_TARGET, PQ_ANCHOR_TOLERANCE
    ));
    output.push_str("- SQ8 must meet or exceed 1-bit recall@10 and recall@100 in every matching dataset/nprobe/margin cell.\n");
    output.push_str("- The wiki_dpr_e5 exact nprobe-16 in-probe recall@100 ceiling is reported as the upper bound on the gate cell; falling below 0.96 is a result, not a control failure.\n");
    output.push_str("- The driver aborts before writing this report if a data-integrity, rotation, PQ, or SQ8 control anchor fails; a Phase 1 gate failure is written explicitly.\n");
    output.push_str("\n## Determinism scope\n\n");
    output.push_str("Codes, cluster assignments, probe sets, sampled quality metrics, recall, and rotation-oracle metrics reproduce exactly on the same build and hardware. Wall-clock encode seconds/vectors-per-second and the detected assignment-worker count inherently vary with runtime load and host availability. The plan's simultaneous wall-clock measurement and every-number-identical requirements cannot both hold for timing values; this report does not claim otherwise.\n");
    Ok(output)
}

fn write_report(path: &Path, contents: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        BakeoffError::Integrity(format!("results path has no parent: {}", path.display()))
    })?;
    fs::create_dir_all(parent).map_err(|source| io_error(parent, source))?;
    let file_name = path.file_name().ok_or_else(|| {
        BakeoffError::Integrity(format!("results path has no file name: {}", path.display()))
    })?;
    let mut temporary_name = file_name.to_os_string();
    temporary_name.push(format!(".{}.tmp", Uuid::new_v4()));
    let temporary = parent.join(temporary_name);
    {
        let file = File::create(&temporary).map_err(|source| io_error(&temporary, source))?;
        let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, file);
        writer
            .write_all(contents)
            .map_err(|source| io_error(&temporary, source))?;
        writer
            .flush()
            .map_err(|source| io_error(&temporary, source))?;
        writer
            .get_ref()
            .sync_all()
            .map_err(|source| io_error(&temporary, source))?;
    }
    fs::rename(&temporary, path).map_err(|source| io_error(path, source))?;
    Ok(())
}

fn encoder<'a>(result: &'a DatasetResult, name: &str) -> Result<&'a EncoderResult> {
    result
        .encoders
        .iter()
        .find(|encoder| encoder.name == name)
        .ok_or_else(|| {
            BakeoffError::Integrity(format!("{} is missing encoder {name}", result.name))
        })
}

fn cell(encoder: &EncoderResult, nprobe: usize, margin: usize) -> Result<&MatrixCell> {
    encoder
        .cells
        .iter()
        .find(|cell| cell.nprobe == nprobe && cell.margin == margin)
        .ok_or_else(|| {
            BakeoffError::Integrity(format!(
                "{} is missing nprobe={nprobe}, margin={margin}",
                encoder.name
            ))
        })
}

fn make_rotation(dims: usize) -> Result<rabitq::StructuredRotation> {
    let rotation = rabitq::StructuredRotation::new(dims, ROTATION_SEED).map_err(rabitq_error)?;
    if rotation.dim() != dims {
        return Err(BakeoffError::Integrity(format!(
            "structured rotation reported dimension {}, expected {dims}",
            rotation.dim()
        )));
    }
    Ok(rotation)
}

fn rotate_centroids(
    rotation: &rabitq::StructuredRotation,
    centroids: &[Vec<f32>],
) -> Result<Vec<Vec<f32>>> {
    let mut rotated = Vec::with_capacity(centroids.len());
    let mut scratch = vec![0.0f32; rotation.dim()];
    for centroid in centroids {
        let mut value = centroid.clone();
        rotation
            .rotate_in_place(&mut value, &mut scratch)
            .map_err(rabitq_error)?;
        rotated.push(value);
    }
    Ok(rotated)
}

fn nearest_probe_clusters(query: &[f32], centroids: &[Vec<f32>]) -> Result<Vec<usize>> {
    let max_nprobe = NPROBES[NPROBES.len() - 1];
    if centroids.len() < max_nprobe {
        return Err(BakeoffError::Integrity(format!(
            "only {} centroids available for nprobe {max_nprobe}",
            centroids.len()
        )));
    }
    let mut ranked: Vec<(f32, usize)> = centroids
        .iter()
        .enumerate()
        .map(|(cluster, centroid)| (cosine_distance(query, centroid), cluster))
        .collect();
    ranked.sort_unstable_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    Ok(ranked
        .into_iter()
        .take(max_nprobe)
        .map(|(_, cluster)| cluster)
        .collect())
}

fn nearest_l2_centroid(vector: &[f32], centroids: &[Vec<f32>]) -> usize {
    let mut best_cluster = 0usize;
    let mut best_distance = f32::INFINITY;
    for (cluster, centroid) in centroids.iter().enumerate() {
        let distance = squared_l2(vector, centroid);
        if distance < best_distance {
            best_distance = distance;
            best_cluster = cluster;
        }
    }
    best_cluster
}

#[inline]
fn squared_l2(left: &[f32], right: &[f32]) -> f32 {
    debug_assert_eq!(left.len(), right.len());
    left.iter()
        .zip(right.iter())
        .map(|(&left, &right)| {
            let difference = left - right;
            difference * difference
        })
        .sum()
}

#[inline]
fn exact_cosine_in_l2_units(left: &[f32], right: &[f32]) -> f32 {
    2.0 * cosine_distance(left, right)
}

#[inline]
fn squared_norm(values: &[f32]) -> f32 {
    values.iter().map(|value| value * value).sum()
}

fn retain_best(heap: &mut BinaryHeap<Candidate>, candidate: Candidate, limit: usize) {
    if heap.len() < limit {
        heap.push(candidate);
        return;
    }
    if heap.peek().is_some_and(|worst| candidate < *worst) {
        let _ = heap.pop();
        heap.push(candidate);
    }
}

fn sorted_candidates(heap: BinaryHeap<Candidate>) -> Vec<Candidate> {
    let mut values = heap.into_vec();
    values.sort_unstable();
    values
}

fn recall_at_k(candidates: &[Candidate], ground_truth: &[u32], k: usize) -> f64 {
    let hits = candidates
        .iter()
        .take(k)
        .filter(|candidate| ground_truth[..k].contains(&candidate.row))
        .count();
    hits as f64 / k as f64
}

fn evenly_spaced_indices(vector_count: usize, sample_count: usize) -> Result<Vec<usize>> {
    if vector_count == 0 || sample_count == 0 || sample_count > vector_count {
        return Err(BakeoffError::Integrity(format!(
            "invalid deterministic sample: vector_count={vector_count}, sample_count={sample_count}"
        )));
    }
    if sample_count == vector_count {
        return Ok((0..vector_count).collect());
    }
    Ok((0..sample_count)
        .map(|index| index * vector_count / sample_count)
        .collect())
}

fn cluster_order_row(ivf: &IvfModel, position: usize) -> Result<usize> {
    let total = ivf.cluster_offsets.last().copied().unwrap_or(0);
    if position >= total {
        return Err(BakeoffError::Integrity(format!(
            "cluster-order position {position} is outside {total} rows"
        )));
    }
    let upper = ivf
        .cluster_offsets
        .partition_point(|&offset| offset <= position);
    let cluster = upper.checked_sub(1).ok_or_else(|| {
        BakeoffError::Integrity(format!("no cluster contains code position {position}"))
    })?;
    let local = position - ivf.cluster_offsets[cluster];
    ivf.clusters[cluster]
        .get(local)
        .copied()
        .map(|row| row as usize)
        .ok_or_else(|| {
            BakeoffError::Integrity(format!(
                "cluster {cluster} has no local row {local} for code position {position}"
            ))
        })
}

fn query_adc_seed(query_index: usize, cluster_index: usize) -> u64 {
    let input = ROTATION_SEED
        ^ (query_index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ (cluster_index as u64).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    splitmix64(input)
}

fn estimator_error_sample(query_index: usize, cluster_index: usize, row: u32) -> bool {
    let input = ROTATION_SEED
        ^ (query_index as u64).wrapping_mul(0xd6e8_feb8_6659_fd93)
        ^ (cluster_index as u64).wrapping_mul(0xa5a3_58a5_28f1_3d2d)
        ^ u64::from(row).wrapping_mul(0x94d0_49bb_1331_11eb);
    splitmix64(input) % ESTIMATOR_ERROR_SAMPLE_MODULUS == 0
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn nonzero_elapsed(started: Instant, label: &str) -> Result<f64> {
    let seconds = started.elapsed().as_secs_f64();
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err(BakeoffError::Integrity(format!(
            "{label} produced invalid wall-clock duration {seconds}"
        )));
    }
    Ok(seconds)
}

fn require_code_count(label: &str, actual: usize, expected: usize) -> Result<()> {
    if actual != expected {
        return Err(BakeoffError::Integrity(format!(
            "{label} emitted {actual} codes, expected {expected}"
        )));
    }
    Ok(())
}

fn checked_product(left: usize, right: usize, label: &str) -> Result<usize> {
    left.checked_mul(right).ok_or_else(|| {
        BakeoffError::Dataset(format!(
            "{label} overflows usize: {left} multiplied by {right}"
        ))
    })
}

fn open_file(path: &Path) -> Result<File> {
    File::open(path).map_err(|source| io_error(path, source))
}

fn file_len(path: &Path, file: &File) -> Result<u64> {
    file.metadata()
        .map(|metadata| metadata.len())
        .map_err(|source| io_error(path, source))
}

fn open_exact_size(path: &Path, expected_bytes: usize) -> Result<File> {
    let file = open_file(path)?;
    let actual = file_len(path, &file)?;
    if actual != expected_bytes as u64 {
        return Err(BakeoffError::Dataset(format!(
            "{} is {actual} bytes, expected {expected_bytes}",
            path.display()
        )));
    }
    Ok(file)
}

fn require_eof(path: &Path, reader: &mut impl Read) -> Result<()> {
    let mut extra = [0u8; 1];
    let read = reader
        .read(&mut extra)
        .map_err(|source| io_error(path, source))?;
    if read != 0 {
        return Err(BakeoffError::Dataset(format!(
            "{} contains trailing bytes after the declared shape",
            path.display()
        )));
    }
    Ok(())
}

fn io_error(path: &Path, source: io::Error) -> BakeoffError {
    BakeoffError::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn rabitq_error(error: impl std::fmt::Display) -> BakeoffError {
    BakeoffError::Rabitq(error.to_string())
}
