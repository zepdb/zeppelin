use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::env;
use std::fmt;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Instant;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;
use zeppelin::compaction::Compactor;
use zeppelin::config::{Config, StorageBackend, StorageConfig};
use zeppelin::error::ZeppelinError;
use zeppelin::index::distance::compute_distance;
use zeppelin::index::quantization::sq::{sq_calibration_key, sq_cluster_key, SqCalibration};
use zeppelin::index::quantization::QuantizationType;
use zeppelin::namespace::manager::NamespaceManager;
use zeppelin::query::{execute_query, QueryParams};
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::{ConsistencyLevel, DistanceMetric, VectorEntry};
use zeppelin::wal::{Manifest, WalReader, WalWriter};

const DEFAULT_SEED_FILE: &str = "/Users/aghatage/Documents/code/zeppelin-holdout/holdout_seed.toml";
const DEFAULT_DATASET: &str = "d1_primary";
const INGEST_BATCH_SIZE: usize = 1_000;

#[derive(Debug, Error)]
enum RecallEvalError {
    #[error("{0}")]
    Usage(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("seed TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("json serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("zeppelin error: {0}")]
    Zeppelin(#[from] ZeppelinError),
    #[error("invalid recall-eval configuration: {0}")]
    Config(String),
    #[error("recall integrity check failed: {0}")]
    Integrity(String),
}

type Result<T> = std::result::Result<T, RecallEvalError>;

#[derive(Debug)]
struct Cli {
    seed_file: PathBuf,
    query_mode: QueryModeSelection,
    nprobe: Option<NprobeArg>,
    top_k: Option<usize>,
    dataset_selector: String,
    json: bool,
}

#[derive(Debug, Clone, Copy)]
enum NprobeArg {
    Count(usize),
    All,
}

impl fmt::Display for NprobeArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Count(value) => write!(f, "{value}"),
            Self::All => write!(f, "all"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum QueryModeSelection {
    One(QueryMode),
    All,
}

impl QueryModeSelection {
    #[must_use]
    fn modes(self) -> Vec<QueryMode> {
        match self {
            Self::One(mode) => vec![mode],
            Self::All => vec![QueryMode::Centroid, QueryMode::Boundary, QueryMode::Uniform],
        }
    }
}

impl fmt::Display for QueryModeSelection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::One(mode) => write!(f, "{mode}"),
            Self::All => write!(f, "all"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
enum QueryMode {
    Centroid,
    Boundary,
    Uniform,
}

impl QueryMode {
    #[must_use]
    fn parse(value: &str) -> Option<Self> {
        match value {
            "centroid" => Some(Self::Centroid),
            "boundary" => Some(Self::Boundary),
            "uniform" => Some(Self::Uniform),
            _ => None,
        }
    }

    #[must_use]
    fn as_str(self) -> &'static str {
        match self {
            Self::Centroid => "centroid",
            Self::Boundary => "boundary",
            Self::Uniform => "uniform",
        }
    }
}

impl fmt::Display for QueryMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Deserialize)]
struct SeedFile {
    meta: SeedMeta,
    datasets: BTreeMap<String, DatasetSpec>,
    queries: QueriesSpec,
    ground_truth: GroundTruthSpec,
}

#[derive(Debug, Deserialize)]
struct SeedMeta {
    generation: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct DatasetSpec {
    seed: u64,
    n_vectors: usize,
    dims: usize,
    n_clusters: usize,
    noise_sigma_range: [f32; 2],
    cluster_size_distribution: String,
    id_prefix: String,
    distance_metric: String,
}

#[derive(Debug, Deserialize)]
struct QueriesSpec {
    count_per_mode: usize,
    top_k: usize,
    centroid: QueryModeSpec,
    boundary: QueryModeSpec,
    uniform: QueryModeSpec,
}

impl QueriesSpec {
    #[must_use]
    fn seed_for(&self, mode: QueryMode) -> u64 {
        match mode {
            QueryMode::Centroid => self.centroid.seed,
            QueryMode::Boundary => self.boundary.seed,
            QueryMode::Uniform => self.uniform.seed,
        }
    }
}

#[derive(Debug, Deserialize)]
struct QueryModeSpec {
    seed: u64,
}

#[derive(Debug, Deserialize)]
struct GroundTruthSpec {
    k: usize,
    tie_break: String,
}

#[derive(Debug)]
struct GeneratedDataset {
    entries: Vec<VectorEntry>,
    exact: ExactDataset,
    centroids: Vec<Vec<f32>>,
    sigmas: Vec<f32>,
    distance_metric: DistanceMetric,
}

#[derive(Debug)]
struct ExactDataset {
    ids: Vec<String>,
    vectors: Vec<Vec<f32>>,
    normalized_vectors: Option<Vec<Vec<f32>>>,
}

#[derive(Debug)]
struct Neighbor {
    id: String,
    score: f32,
}

#[derive(Debug, Serialize)]
struct Report {
    holdout_generation: u64,
    seed_file: String,
    dataset: String,
    query_mode: String,
    config: ReportConfig,
    segment_verification: SegmentVerification,
    modes: Vec<ModeReport>,
}

#[derive(Debug, Serialize)]
struct ReportConfig {
    quantization: String,
    nprobe_requested: String,
    nprobe_resolved: usize,
    default_centroids: usize,
    actual_clusters: usize,
    oversample_factor: usize,
    top_k: usize,
    consistency: String,
}

#[derive(Debug, Serialize)]
struct SegmentVerification {
    compacted_segment: bool,
    wal_fragments_after_compaction: usize,
    segment_quantization: String,
    sq_calibration_present: bool,
    sq_cluster_zero_present: bool,
}

#[derive(Debug, Serialize)]
struct ModeReport {
    mode: QueryMode,
    recall_at_k: f64,
    elapsed_ms: u128,
}

struct PreparedNamespace {
    namespace: String,
    wal_reader: WalReader,
    segment: SegmentSummary,
}

struct EvalContext<'a> {
    store: &'a ZeppelinStore,
    wal_reader: &'a WalReader,
    namespace: &'a str,
    dataset: &'a GeneratedDataset,
    queries_spec: &'a QueriesSpec,
    top_k: usize,
    nprobe: usize,
    oversample_factor: usize,
}

#[derive(Clone)]
struct SegmentSummary {
    quantization: QuantizationType,
    cluster_count: usize,
    wal_fragments_after_compaction: usize,
    sq_calibration_present: bool,
    sq_cluster_zero_present: bool,
}

#[tokio::main]
async fn main() {
    match run().await {
        Ok(()) => {}
        Err(err) => {
            eprintln!("recall_eval error: {err}");
            std::process::exit(1);
        }
    }
}

async fn run() -> Result<()> {
    let Some(cli) = parse_cli(env::args().skip(1))? else {
        println!("{}", usage());
        return Ok(());
    };

    let seed_file = read_seed_file(&cli.seed_file)?;
    validate_ground_truth_rules(&seed_file.ground_truth)?;
    validate_queries(&seed_file.queries)?;

    let (dataset_name, dataset_spec) = resolve_dataset(&seed_file, &cli.dataset_selector)?;
    let top_k = cli.top_k.unwrap_or(seed_file.queries.top_k);
    validate_top_k(top_k, &seed_file)?;

    let config = Config::load(None)?;
    if config.indexing.quantization != QuantizationType::Scalar {
        return Err(RecallEvalError::Config(format!(
            "recall_eval must measure the production SQ8 path; resolved quantization was {:?}",
            config.indexing.quantization
        )));
    }

    let store = ZeppelinStore::from_config(&minio_storage_config()?)?;
    let dataset = generate_dataset(dataset_spec)?;
    if top_k > dataset.entries.len() {
        return Err(RecallEvalError::Config(format!(
            "top_k {top_k} exceeds dataset vector count {}",
            dataset.entries.len()
        )));
    }
    let prepared = prepare_namespace(&store, &config, &dataset).await?;

    let requested_nprobe = cli
        .nprobe
        .unwrap_or(NprobeArg::Count(config.indexing.default_nprobe));
    let resolved_nprobe = match requested_nprobe {
        NprobeArg::Count(value) => value,
        NprobeArg::All => prepared.segment.cluster_count,
    };
    if resolved_nprobe == 0 {
        return Err(RecallEvalError::Config(
            "nprobe must be greater than zero".into(),
        ));
    }

    let eval_context = EvalContext {
        store: &store,
        wal_reader: &prepared.wal_reader,
        namespace: &prepared.namespace,
        dataset: &dataset,
        queries_spec: &seed_file.queries,
        top_k,
        nprobe: resolved_nprobe,
        oversample_factor: config.indexing.oversample_factor,
    };
    let mode_reports = evaluate_modes(&eval_context, cli.query_mode.modes()).await?;

    let segment = prepared.segment.clone();
    let deleted = cleanup_namespace(&store, &prepared.namespace).await?;
    if deleted == 0 {
        return Err(RecallEvalError::Integrity(format!(
            "cleanup deleted zero objects for namespace {}",
            prepared.namespace
        )));
    }

    let report = Report {
        holdout_generation: seed_file.meta.generation,
        seed_file: cli.seed_file.display().to_string(),
        dataset: dataset_name,
        query_mode: cli.query_mode.to_string(),
        config: ReportConfig {
            quantization: quantization_name(config.indexing.quantization).to_string(),
            nprobe_requested: requested_nprobe.to_string(),
            nprobe_resolved: resolved_nprobe,
            default_centroids: config.indexing.default_num_centroids,
            actual_clusters: segment.cluster_count,
            oversample_factor: config.indexing.oversample_factor,
            top_k,
            consistency: "eventual".to_string(),
        },
        segment_verification: SegmentVerification {
            compacted_segment: true,
            wal_fragments_after_compaction: segment.wal_fragments_after_compaction,
            segment_quantization: quantization_name(segment.quantization).to_string(),
            sq_calibration_present: segment.sq_calibration_present,
            sq_cluster_zero_present: segment.sq_cluster_zero_present,
        },
        modes: mode_reports,
    };

    if cli.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_human_report(&report);
    }

    Ok(())
}

fn parse_cli<I>(args: I) -> Result<Option<Cli>>
where
    I: IntoIterator<Item = String>,
{
    let mut seed_file = PathBuf::from(DEFAULT_SEED_FILE);
    let mut query_mode = QueryModeSelection::All;
    let mut nprobe = None;
    let mut top_k = None;
    let mut dataset_selector = DEFAULT_DATASET.to_string();
    let mut json = false;

    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--help" | "-h" => return Ok(None),
            "--json" => json = true,
            "--seed-file" => {
                seed_file = PathBuf::from(next_value(&mut iter, "--seed-file")?);
            }
            "--query-mode" => {
                let value = next_value(&mut iter, "--query-mode")?;
                query_mode = if value == "all" {
                    QueryModeSelection::All
                } else if let Some(mode) = QueryMode::parse(&value) {
                    QueryModeSelection::One(mode)
                } else {
                    return Err(RecallEvalError::Usage(format!(
                        "invalid --query-mode {value}; expected centroid, boundary, uniform, or all"
                    )));
                };
            }
            "--nprobe" => {
                let value = next_value(&mut iter, "--nprobe")?;
                nprobe = Some(parse_nprobe(&value)?);
            }
            "--top-k" => {
                let value = next_value(&mut iter, "--top-k")?;
                top_k = Some(parse_positive_usize("--top-k", &value)?);
            }
            "--dataset" => {
                dataset_selector = next_value(&mut iter, "--dataset")?;
            }
            other => {
                return Err(RecallEvalError::Usage(format!(
                    "unknown argument {other}\n{}",
                    usage()
                )));
            }
        }
    }

    Ok(Some(Cli {
        seed_file,
        query_mode,
        nprobe,
        top_k,
        dataset_selector,
        json,
    }))
}

fn next_value<I>(iter: &mut I, flag: &str) -> Result<String>
where
    I: Iterator<Item = String>,
{
    iter.next()
        .ok_or_else(|| RecallEvalError::Usage(format!("{flag} requires a value")))
}

fn parse_nprobe(value: &str) -> Result<NprobeArg> {
    if value == "all" {
        return Ok(NprobeArg::All);
    }
    Ok(NprobeArg::Count(parse_positive_usize("--nprobe", value)?))
}

fn parse_positive_usize(flag: &str, value: &str) -> Result<usize> {
    let parsed = value.parse::<usize>().map_err(|e| {
        RecallEvalError::Usage(format!(
            "{flag} requires a positive integer, got {value}: {e}"
        ))
    })?;
    if parsed == 0 {
        return Err(RecallEvalError::Usage(format!(
            "{flag} requires a positive integer, got 0"
        )));
    }
    Ok(parsed)
}

fn usage() -> &'static str {
    "usage: recall_eval [--query-mode centroid|boundary|uniform|all] [--nprobe <n>|all] \
     [--top-k <k>] [--dataset <name-or-size>] [--seed-file <path>] [--json]"
}

fn read_seed_file(path: &Path) -> Result<SeedFile> {
    let content = std::fs::read_to_string(path)?;
    Ok(toml::from_str(&content)?)
}

fn validate_ground_truth_rules(ground_truth: &GroundTruthSpec) -> Result<()> {
    if ground_truth.k == 0 {
        return Err(RecallEvalError::Config(
            "ground_truth.k must be greater than zero".into(),
        ));
    }
    if !ground_truth.tie_break.contains("ascending") {
        return Err(RecallEvalError::Config(
            "ground_truth.tie_break must require ascending IDs".into(),
        ));
    }
    Ok(())
}

fn validate_queries(queries: &QueriesSpec) -> Result<()> {
    if queries.count_per_mode == 0 {
        return Err(RecallEvalError::Config(
            "queries.count_per_mode must be greater than zero".into(),
        ));
    }
    Ok(())
}

fn validate_top_k(top_k: usize, seed_file: &SeedFile) -> Result<()> {
    if top_k == 0 {
        return Err(RecallEvalError::Config(
            "top_k must be greater than zero".into(),
        ));
    }
    if seed_file.queries.top_k != seed_file.ground_truth.k {
        return Err(RecallEvalError::Config(format!(
            "queries.top_k {} does not match ground_truth.k {} in the seed file",
            seed_file.queries.top_k, seed_file.ground_truth.k
        )));
    }
    Ok(())
}

fn resolve_dataset(seed_file: &SeedFile, selector: &str) -> Result<(String, DatasetSpec)> {
    let normalized_selector = normalize_selector(selector);
    let requested_size = parse_size_selector(&normalized_selector);
    let mut matches = Vec::new();

    for (name, spec) in &seed_file.datasets {
        let normalized_name = normalize_selector(name);
        let short_name = normalized_name
            .split('_')
            .next()
            .map(str::to_string)
            .unwrap_or_else(|| normalized_name.clone());
        let size_matches = requested_size == Some(spec.n_vectors);
        if normalized_selector == normalized_name
            || normalized_selector == short_name
            || size_matches
        {
            matches.push((name.clone(), spec.clone()));
        }
    }

    match matches.len() {
        1 => {
            let mut iter = matches.into_iter();
            iter.next().ok_or_else(|| {
                RecallEvalError::Config("dataset resolution lost its single match".into())
            })
        }
        0 => Err(RecallEvalError::Config(format!(
            "unknown dataset selector {selector}; expected a sealed dataset name, short name, or vector count"
        ))),
        _ => Err(RecallEvalError::Config(format!(
            "ambiguous dataset selector {selector}; matched more than one sealed dataset"
        ))),
    }
}

fn normalize_selector(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace('-', "_")
}

fn parse_size_selector(value: &str) -> Option<usize> {
    if let Some(prefix) = value.strip_suffix('k') {
        return prefix.parse::<usize>().ok().map(|n| n * 1_000);
    }
    value.parse::<usize>().ok()
}

fn minio_storage_config() -> Result<StorageConfig> {
    let backend = env::var("TEST_BACKEND").unwrap_or_else(|_| "minio".to_string());
    if backend != "minio" {
        return Err(RecallEvalError::Config(format!(
            "recall_eval requires TEST_BACKEND=minio, got {backend}"
        )));
    }

    Ok(StorageConfig {
        backend: StorageBackend::S3,
        bucket: env::var("TEST_S3_BUCKET").unwrap_or_else(|_| "stormcrow-test".to_string()),
        s3_region: Some("us-east-1".to_string()),
        s3_endpoint: Some(
            env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".to_string()),
        ),
        s3_access_key_id: Some(
            env::var("MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string()),
        ),
        s3_secret_access_key: Some(
            env::var("MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string()),
        ),
        s3_allow_http: true,
    })
}

fn generate_dataset(spec: DatasetSpec) -> Result<GeneratedDataset> {
    validate_dataset_spec(&spec)?;

    let metric = parse_distance_metric(&spec.distance_metric)?;
    let mut rng = StdRng::seed_from_u64(spec.seed);
    let centroids = generate_centroids(&mut rng, spec.n_clusters, spec.dims)?;
    let sigmas = generate_sigmas(&mut rng, spec.n_clusters, spec.noise_sigma_range)?;
    let counts = zipf_counts(
        spec.n_vectors,
        spec.n_clusters,
        parse_zipf_exponent(&spec.cluster_size_distribution)?,
    );

    let mut entries = Vec::with_capacity(spec.n_vectors);
    for (cluster_idx, count) in counts.iter().copied().enumerate() {
        for local_idx in 0..count {
            let mut values = Vec::with_capacity(spec.dims);
            for &component in &centroids[cluster_idx] {
                values.push(component + gaussian(&mut rng) * sigmas[cluster_idx]);
            }
            entries.push(VectorEntry {
                id: format!("{}_{}_{}", spec.id_prefix, cluster_idx, local_idx),
                values,
                attributes: None,
            });
        }
    }

    if entries.len() != spec.n_vectors {
        return Err(RecallEvalError::Integrity(format!(
            "dataset generator produced {} vectors, expected {}",
            entries.len(),
            spec.n_vectors
        )));
    }

    let exact = ExactDataset::new(&entries, metric)?;
    Ok(GeneratedDataset {
        entries,
        exact,
        centroids,
        sigmas,
        distance_metric: metric,
    })
}

fn validate_dataset_spec(spec: &DatasetSpec) -> Result<()> {
    if spec.n_vectors == 0 {
        return Err(RecallEvalError::Config(
            "dataset n_vectors must be greater than zero".into(),
        ));
    }
    if spec.dims == 0 {
        return Err(RecallEvalError::Config(
            "dataset dims must be greater than zero".into(),
        ));
    }
    if spec.n_clusters == 0 {
        return Err(RecallEvalError::Config(
            "dataset n_clusters must be greater than zero".into(),
        ));
    }
    if spec.n_clusters < 2 {
        return Err(RecallEvalError::Config(
            "dataset n_clusters must be at least two for boundary queries".into(),
        ));
    }
    if spec.n_vectors < spec.n_clusters {
        return Err(RecallEvalError::Config(format!(
            "dataset n_vectors {} must be >= n_clusters {}",
            spec.n_vectors, spec.n_clusters
        )));
    }
    let [min_sigma, max_sigma] = spec.noise_sigma_range;
    if !min_sigma.is_finite() || !max_sigma.is_finite() || min_sigma < 0.0 || min_sigma > max_sigma
    {
        return Err(RecallEvalError::Config(format!(
            "invalid noise_sigma_range [{min_sigma}, {max_sigma}]"
        )));
    }
    Ok(())
}

fn parse_distance_metric(value: &str) -> Result<DistanceMetric> {
    match value {
        "cosine" => Ok(DistanceMetric::Cosine),
        "euclidean" => Ok(DistanceMetric::Euclidean),
        "dot_product" => Ok(DistanceMetric::DotProduct),
        other => Err(RecallEvalError::Config(format!(
            "unsupported dataset distance_metric {other}"
        ))),
    }
}

fn parse_zipf_exponent(distribution: &str) -> Result<f64> {
    let Some(start) = distribution.find("s=") else {
        return Err(RecallEvalError::Config(format!(
            "unsupported cluster_size_distribution {distribution}"
        )));
    };
    let rest = &distribution[start + 2..];
    let end = rest
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .unwrap_or(rest.len());
    let exponent = rest[..end].parse::<f64>().map_err(|e| {
        RecallEvalError::Config(format!(
            "failed to parse zipf exponent from cluster_size_distribution {distribution}: {e}"
        ))
    })?;
    if !exponent.is_finite() || exponent <= 0.0 {
        return Err(RecallEvalError::Config(format!(
            "zipf exponent must be finite and positive, got {exponent}"
        )));
    }
    Ok(exponent)
}

fn generate_centroids(rng: &mut StdRng, n_clusters: usize, dims: usize) -> Result<Vec<Vec<f32>>> {
    let mut centroids = Vec::with_capacity(n_clusters);
    for _ in 0..n_clusters {
        let values: Vec<f32> = (0..dims).map(|_| rng.gen_range(-1.0..1.0)).collect();
        centroids.push(normalize_strict(&values)?);
    }
    Ok(centroids)
}

fn generate_sigmas(rng: &mut StdRng, n_clusters: usize, range: [f32; 2]) -> Result<Vec<f32>> {
    let [min_sigma, max_sigma] = range;
    if min_sigma == max_sigma {
        return Ok(vec![min_sigma; n_clusters]);
    }
    Ok((0..n_clusters)
        .map(|_| rng.gen_range(min_sigma..max_sigma))
        .collect())
}

fn zipf_counts(total: usize, buckets: usize, exponent: f64) -> Vec<usize> {
    let weights: Vec<f64> = (1..=buckets)
        .map(|rank| 1.0 / (rank as f64).powf(exponent))
        .collect();
    let weight_sum: f64 = weights.iter().sum();
    let mut counts = Vec::with_capacity(buckets);
    let mut fractions = Vec::with_capacity(buckets);
    let mut assigned = 0usize;

    for (idx, weight) in weights.iter().copied().enumerate() {
        let exact = total as f64 * weight / weight_sum;
        let floor = exact.floor() as usize;
        counts.push(floor);
        fractions.push((idx, exact - floor as f64));
        assigned += floor;
    }

    fractions.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    for (idx, _) in fractions.into_iter().take(total - assigned) {
        counts[idx] += 1;
    }

    counts
}

fn gaussian(rng: &mut StdRng) -> f32 {
    let u1 = rng.gen::<f32>().clamp(f32::MIN_POSITIVE, 1.0);
    let u2 = rng.gen::<f32>();
    (-2.0 * u1.ln()).sqrt() * (std::f32::consts::TAU * u2).cos()
}

fn normalize_strict(values: &[f32]) -> Result<Vec<f32>> {
    let norm = values.iter().map(|v| v * v).sum::<f32>().sqrt();
    if !norm.is_finite() || norm <= f32::EPSILON {
        return Err(RecallEvalError::Integrity(
            "cannot normalize zero or non-finite vector".into(),
        ));
    }
    Ok(values.iter().map(|v| v / norm).collect())
}

impl ExactDataset {
    fn new(entries: &[VectorEntry], metric: DistanceMetric) -> Result<Self> {
        let mut ids = Vec::with_capacity(entries.len());
        let mut vectors = Vec::with_capacity(entries.len());
        let mut normalized_vectors = if metric == DistanceMetric::Cosine {
            Some(Vec::with_capacity(entries.len()))
        } else {
            None
        };

        for entry in entries {
            ids.push(entry.id.clone());
            vectors.push(entry.values.clone());
            if let Some(normalized) = normalized_vectors.as_mut() {
                normalized.push(normalize_strict(&entry.values)?);
            }
        }

        Ok(Self {
            ids,
            vectors,
            normalized_vectors,
        })
    }

    fn top_k(&self, query: &[f32], top_k: usize, metric: DistanceMetric) -> Result<Vec<String>> {
        let normalized_query = if metric == DistanceMetric::Cosine {
            Some(normalize_strict(query)?)
        } else {
            None
        };

        let mut best = Vec::with_capacity(top_k);
        for idx in 0..self.ids.len() {
            let score = match (
                metric,
                normalized_query.as_ref(),
                self.normalized_vectors.as_ref(),
            ) {
                (DistanceMetric::Cosine, Some(query_vec), Some(vectors)) => {
                    1.0 - dot(query_vec, &vectors[idx]).clamp(-1.0, 1.0)
                }
                _ => compute_distance(query, &self.vectors[idx], metric),
            };
            if !score.is_finite() {
                return Err(RecallEvalError::Integrity(format!(
                    "non-finite exact score for vector {}",
                    self.ids[idx]
                )));
            }
            push_top_k(&mut best, top_k, score, &self.ids[idx]);
        }

        best.sort_by(|a, b| compare_scored_ids(a.score, &a.id, b.score, &b.id));
        Ok(best.into_iter().map(|neighbor| neighbor.id).collect())
    }

    fn top_k_batch(
        &self,
        queries: &[Vec<f32>],
        top_k: usize,
        metric: DistanceMetric,
    ) -> Result<Vec<Vec<String>>> {
        let worker_count = thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(1)
            .min(queries.len())
            .max(1);
        let chunk_size = queries.len().div_ceil(worker_count);
        let mut results = vec![None; queries.len()];

        thread::scope(|scope| {
            let mut handles = Vec::new();
            for (chunk_idx, chunk) in queries.chunks(chunk_size).enumerate() {
                let start = chunk_idx * chunk_size;
                handles.push((
                    start,
                    scope.spawn(move || {
                        let mut chunk_results = Vec::with_capacity(chunk.len());
                        for query in chunk {
                            chunk_results.push(self.top_k(query, top_k, metric)?);
                        }
                        Ok::<_, RecallEvalError>(chunk_results)
                    }),
                ));
            }

            for (start, handle) in handles {
                let chunk_results = handle.join().map_err(|_| {
                    RecallEvalError::Integrity(
                        "exact ground-truth worker panicked during brute-force KNN".into(),
                    )
                })??;
                for (offset, ids) in chunk_results.into_iter().enumerate() {
                    results[start + offset] = Some(ids);
                }
            }

            Ok::<_, RecallEvalError>(())
        })?;

        results
            .into_iter()
            .map(|entry| {
                entry.ok_or_else(|| {
                    RecallEvalError::Integrity(
                        "exact ground-truth worker did not return a query result".into(),
                    )
                })
            })
            .collect()
    }
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    a.iter().zip(b).map(|(left, right)| left * right).sum()
}

fn push_top_k(best: &mut Vec<Neighbor>, top_k: usize, score: f32, id: &str) {
    if best.len() < top_k {
        best.push(Neighbor {
            id: id.to_string(),
            score,
        });
        return;
    }

    let mut worst_idx = 0usize;
    for idx in 1..best.len() {
        if compare_scored_ids(
            best[idx].score,
            &best[idx].id,
            best[worst_idx].score,
            &best[worst_idx].id,
        ) == Ordering::Greater
        {
            worst_idx = idx;
        }
    }

    if compare_scored_ids(score, id, best[worst_idx].score, &best[worst_idx].id) == Ordering::Less {
        best[worst_idx] = Neighbor {
            id: id.to_string(),
            score,
        };
    }
}

fn compare_scored_ids(
    left_score: f32,
    left_id: &str,
    right_score: f32,
    right_id: &str,
) -> Ordering {
    left_score
        .partial_cmp(&right_score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| left_id.cmp(right_id))
}

async fn prepare_namespace(
    store: &ZeppelinStore,
    config: &Config,
    dataset: &GeneratedDataset,
) -> Result<PreparedNamespace> {
    let namespace = format!("recall-eval-{}", Uuid::new_v4());
    let manager = NamespaceManager::new(store.clone());
    manager
        .create(
            &namespace,
            dataset
                .entries
                .first()
                .map(|entry| entry.values.len())
                .ok_or_else(|| RecallEvalError::Integrity("empty generated dataset".into()))?,
            dataset.distance_metric,
        )
        .await?;

    let writer = WalWriter::new(store.clone());
    for chunk in dataset.entries.chunks(INGEST_BATCH_SIZE) {
        writer
            .append(&namespace, chunk.to_vec(), Vec::new())
            .await?;
    }

    let compactor = Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        config.compaction.clone(),
        config.indexing.clone(),
    );
    let result = compactor.compact(&namespace).await?;
    if result.segment_id.is_none() {
        return Err(RecallEvalError::Integrity(
            "compaction did not produce a segment".into(),
        ));
    }

    let segment = verify_compacted_sq8_segment(store, &namespace, dataset.entries.len()).await?;
    Ok(PreparedNamespace {
        namespace,
        wal_reader: WalReader::new(store.clone()),
        segment,
    })
}

async fn verify_compacted_sq8_segment(
    store: &ZeppelinStore,
    namespace: &str,
    expected_vectors: usize,
) -> Result<SegmentSummary> {
    let manifest = Manifest::read(store, namespace)
        .await?
        .ok_or_else(|| RecallEvalError::Integrity("manifest missing after compaction".into()))?;
    let active_segment_id = manifest
        .active_segment
        .as_ref()
        .ok_or_else(|| RecallEvalError::Integrity("no active segment after compaction".into()))?;
    let segment = manifest
        .segments
        .iter()
        .find(|candidate| candidate.id == *active_segment_id)
        .ok_or_else(|| {
            RecallEvalError::Integrity(format!(
                "active segment {active_segment_id} missing from manifest segments"
            ))
        })?;

    if !manifest.uncompacted_fragments().is_empty() {
        return Err(RecallEvalError::Integrity(format!(
            "compaction left {} WAL fragments; recall would include a WAL path",
            manifest.uncompacted_fragments().len()
        )));
    }
    if segment.vector_count != expected_vectors {
        return Err(RecallEvalError::Integrity(format!(
            "compacted segment has {} vectors, expected {expected_vectors}",
            segment.vector_count
        )));
    }
    if segment.hierarchical {
        return Err(RecallEvalError::Integrity(
            "active segment is hierarchical, expected IVF-Flat SQ8".into(),
        ));
    }
    if segment.quantization != QuantizationType::Scalar {
        return Err(RecallEvalError::Integrity(format!(
            "active segment quantization is {:?}, expected Scalar/SQ8",
            segment.quantization
        )));
    }
    if segment.cluster_count == 0 {
        return Err(RecallEvalError::Integrity(
            "active segment has zero clusters".into(),
        ));
    }

    let calibration_key = sq_calibration_key(namespace, active_segment_id);
    let calibration = store.get(&calibration_key).await?;
    let parsed = SqCalibration::from_bytes(&calibration)?;
    if parsed.dim == 0 {
        return Err(RecallEvalError::Integrity(
            "SQ calibration has zero dimensions".into(),
        ));
    }
    let cluster_zero_key = sq_cluster_key(namespace, segment.cluster_owner(0), 0);
    let sq_cluster_zero_present = store.exists(&cluster_zero_key).await?;
    if !sq_cluster_zero_present {
        return Err(RecallEvalError::Integrity(format!(
            "missing SQ8 cluster artifact {cluster_zero_key}"
        )));
    }

    Ok(SegmentSummary {
        quantization: segment.quantization,
        cluster_count: segment.cluster_count,
        wal_fragments_after_compaction: manifest.uncompacted_fragments().len(),
        sq_calibration_present: true,
        sq_cluster_zero_present,
    })
}

async fn evaluate_modes(
    context: &EvalContext<'_>,
    modes: Vec<QueryMode>,
) -> Result<Vec<ModeReport>> {
    let mut reports = Vec::with_capacity(modes.len());
    for mode in modes {
        let start = Instant::now();
        let queries = generate_queries(mode, context.queries_spec, context.dataset)?;
        let exact_sets = context.dataset.exact.top_k_batch(
            &queries,
            context.top_k,
            context.dataset.distance_metric,
        )?;
        let mut recall_sum = 0.0f64;

        for (query, exact) in queries.iter().zip(&exact_sets) {
            let response = execute_query(QueryParams {
                store: context.store,
                wal_reader: context.wal_reader,
                namespace: context.namespace,
                query,
                top_k: context.top_k,
                nprobe: context.nprobe,
                filter: None,
                consistency: ConsistencyLevel::Eventual,
                distance_metric: context.dataset.distance_metric,
                oversample_factor: context.oversample_factor,
                cache: None,
                manifest_cache: None,
            })
            .await?;

            if response.scanned_fragments != 0 {
                return Err(RecallEvalError::Integrity(format!(
                    "query scanned {} WAL fragments; recall_eval requires compacted segment-only measurement",
                    response.scanned_fragments
                )));
            }
            if response.scanned_segments != 1 {
                return Err(RecallEvalError::Integrity(format!(
                    "query scanned {} segments; expected exactly one active compacted segment",
                    response.scanned_segments
                )));
            }

            let approx_ids: HashSet<&str> = response
                .results
                .iter()
                .map(|result| result.id.as_str())
                .collect();
            let hits = exact
                .iter()
                .filter(|id| approx_ids.contains(id.as_str()))
                .count();
            recall_sum += hits as f64 / context.top_k as f64;
        }

        reports.push(ModeReport {
            mode,
            recall_at_k: recall_sum / queries.len() as f64,
            elapsed_ms: start.elapsed().as_millis(),
        });
    }
    Ok(reports)
}

fn generate_queries(
    mode: QueryMode,
    queries_spec: &QueriesSpec,
    dataset: &GeneratedDataset,
) -> Result<Vec<Vec<f32>>> {
    let mut rng = StdRng::seed_from_u64(queries_spec.seed_for(mode));
    let mut queries = Vec::with_capacity(queries_spec.count_per_mode);
    for _ in 0..queries_spec.count_per_mode {
        let query = match mode {
            QueryMode::Centroid => generate_centroid_query(&mut rng, dataset),
            QueryMode::Boundary => generate_boundary_query(&mut rng, dataset)?,
            QueryMode::Uniform => generate_uniform_query(&mut rng, dataset)?,
        };
        queries.push(query);
    }
    Ok(queries)
}

fn generate_centroid_query(rng: &mut StdRng, dataset: &GeneratedDataset) -> Vec<f32> {
    let cluster_idx = rng.gen_range(0..dataset.centroids.len());
    let sigma = dataset.sigmas[cluster_idx] * 0.5;
    dataset.centroids[cluster_idx]
        .iter()
        .map(|component| component + gaussian(rng) * sigma)
        .collect()
}

fn generate_boundary_query(rng: &mut StdRng, dataset: &GeneratedDataset) -> Result<Vec<f32>> {
    let a = rng.gen_range(0..dataset.centroids.len());
    let mut b = rng.gen_range(0..dataset.centroids.len() - 1);
    if b >= a {
        b += 1;
    }
    let values: Vec<f32> = dataset.centroids[a]
        .iter()
        .zip(&dataset.centroids[b])
        .map(|(left, right)| 0.5 * (left + right) + gaussian(rng) * 0.05)
        .collect();
    normalize_strict(&values)
}

fn generate_uniform_query(rng: &mut StdRng, dataset: &GeneratedDataset) -> Result<Vec<f32>> {
    let dims = dataset.centroids[0].len();
    let values: Vec<f32> = (0..dims).map(|_| rng.gen_range(-1.0..1.0)).collect();
    normalize_strict(&values)
}

async fn cleanup_namespace(store: &ZeppelinStore, namespace: &str) -> Result<usize> {
    let prefix = format!("{namespace}/");
    Ok(store.delete_prefix(&prefix).await?)
}

fn quantization_name(value: QuantizationType) -> &'static str {
    match value {
        QuantizationType::None => "None",
        QuantizationType::Scalar => "Scalar",
        QuantizationType::Product => "Product",
    }
}

fn print_human_report(report: &Report) {
    println!("recall_eval");
    println!(
        "dataset={} generation={} seed_file={}",
        report.dataset, report.holdout_generation, report.seed_file
    );
    println!(
        "config: quantization={} nprobe={} resolved_nprobe={} centroids={} actual_clusters={} oversample={} top_k={} consistency={}",
        report.config.quantization,
        report.config.nprobe_requested,
        report.config.nprobe_resolved,
        report.config.default_centroids,
        report.config.actual_clusters,
        report.config.oversample_factor,
        report.config.top_k,
        report.config.consistency
    );
    println!(
        "segment: compacted={} quantization={} wal_fragments={} sq_calibration={} sq_cluster_0={}",
        report.segment_verification.compacted_segment,
        report.segment_verification.segment_quantization,
        report.segment_verification.wal_fragments_after_compaction,
        report.segment_verification.sq_calibration_present,
        report.segment_verification.sq_cluster_zero_present
    );
    println!("recall@{}:", report.config.top_k);
    for mode in &report.modes {
        println!(
            "  {:<8} {:.6} ({} ms)",
            mode.mode, mode.recall_at_k, mode.elapsed_ms
        );
    }
}
