//! Offline MMLI tensor-exchange driver.
//!
//! This binary deliberately starts at the Python/Rust exchange boundary. It
//! reads one JSON job, validates its ragged tensor sidecars, decodes raw
//! little-endian f16/f32 values, and emits a machine-readable input summary.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use zeppelin::config::IndexingConfig;
use zeppelin::error::ZeppelinError;
use zeppelin::index::ivf_flat::kmeans::train_kmeans;
use zeppelin::index::late_interaction::{
    max_sim, FdeAlgorithmVersion, FdeParams, FdeTransform, FinalProjection, InnerProjection,
    MultiVectorMatrixRef,
};

const IO_BUFFER_BYTES: usize = 1024 * 1024;
const MAX_MATRIX_ROWS: usize = 1_000_000;
const TRUTH_K: usize = 10;
const ROUTING_TRUTH_K: usize = 100;
const CANDIDATE_KS: [usize; 3] = [50, 100, 300];
const TEXT_CANDIDATE_K_MAX: usize = 700;
const VISUAL_DOCUMENT_POOLING_FACTOR: usize = 2;
const ROUTING_NPROBES: [usize; 2] = [8, 16];
const INT8_TOP_10_RECOVERY_THRESHOLD: f64 = 0.999;
const INT8_MAX_SIM_ERROR_THRESHOLD: f64 = 1.0e-3;
const CENTERING_SAMPLE_ROWS: usize = 5_000;
const GEOMETRY_SAMPLE_ROWS: usize = 256;
const SIMHASH_SAMPLE_ROWS: usize = 5_000;
const SIMHASH_SAMPLE_DOCUMENTS: usize = 256;
const FDE_SEED: u64 = 0x4d4d_4c49_0000_0002;

type Result<T> = std::result::Result<T, LabError>;

#[derive(Debug, Error)]
enum LabError {
    #[error("{0}")]
    Usage(String),

    #[error("I/O failed for {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("JSON failed for {path}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    #[error("invalid MMLI lab input: {0}")]
    Invalid(String),

    #[error(transparent)]
    Zeppelin(#[from] ZeppelinError),
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Lane {
    Text,
    Visual,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Algorithm {
    PaperV1,
    ReferenceV1,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Centering {
    Identity,
    SubtractGlobalMean,
    SubtractGlobalMeanRenormalize,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum DType {
    F16,
    F32,
}

impl DType {
    const fn byte_width(self) -> usize {
        match self {
            Self::F16 => 2,
            Self::F32 => 4,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TensorInput {
    raw: PathBuf,
    sidecar: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Job {
    lane: Lane,
    documents: TensorInput,
    queries: TensorInput,
    #[serde(default)]
    official_scores: Option<PathBuf>,
    #[serde(default)]
    int8_probe: bool,
    #[serde(default)]
    precision_ranking_audit: bool,
    #[serde(default)]
    precision_ranking_candidates: Option<Vec<PrecisionRankingCandidate>>,
    #[serde(default)]
    chosen_algorithm: Option<Algorithm>,
    #[serde(default)]
    chosen_centering: Option<Centering>,
    #[serde(default)]
    full_precision_documents: Option<TensorInput>,
    #[serde(default)]
    full_precision_queries: Option<TensorInput>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TensorSidecar {
    rows: Vec<usize>,
    dim: usize,
    dtype: DType,
    ids: Vec<String>,
}

#[derive(Debug)]
struct TensorSet {
    sidecar: TensorSidecar,
    values: Vec<f32>,
    scalar_offsets: Vec<usize>,
    total_rows: usize,
}

#[derive(Debug, Serialize)]
struct TensorSummary {
    matrix_count: usize,
    total_rows: usize,
    min_rows: usize,
    p50_rows_nearest_rank: usize,
    p95_rows_nearest_rank: usize,
    max_rows: usize,
    mean_rows: f64,
    dim: usize,
    dtype: DType,
    scalar_count: usize,
}

#[derive(Debug, Serialize)]
struct ResultDocument {
    schema_version: u32,
    lane: Lane,
    seed: u64,
    corpus_stats: TensorSummary,
    query_stats: TensorSummary,
    parity_max_relative_error: f64,
    parity: ParityReport,
    geometry: Vec<GeometryReport>,
    cells: Vec<CellResult>,
    winner: Winner,
    gate_passed: bool,
    routing: Option<RoutingReport>,
    diagnostic_probes: Option<Vec<CellResult>>,
    diagnostics: Option<Vec<DiagnosticCell>>,
    visual_diagnostics: Option<Vec<DiagnosticCell>>,
    pooling_probes: Option<Vec<PoolingProbe>>,
    precision_retention: Option<PrecisionRetentionReport>,
    int8_probe: Option<Int8ProbeReport>,
    exact_frontier_gaps: Option<Vec<ExactFrontierGap>>,
}

#[derive(Debug, Serialize)]
struct PrecisionRankingAuditResultDocument {
    schema_version: u32,
    lane: Lane,
    seed: u64,
    corpus_stats: TensorSummary,
    query_stats: TensorSummary,
    precision_ranking_audit: PrecisionRankingAuditReport,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OfficialScore {
    query_id: String,
    document_id: String,
    score: f32,
}

#[derive(Debug, Serialize)]
struct ParityReport {
    pair_count: usize,
    max_absolute_error: f64,
    max_relative_error: f64,
    passed: bool,
}

#[derive(Clone, Debug, Serialize)]
struct CellResult {
    config: &'static str,
    repetitions: u32,
    simhash_bits: u32,
    d_proj: u32,
    algorithm: Algorithm,
    centering: Centering,
    output_dimension: usize,
    recall_at_50: f64,
    recall_at_100: f64,
    recall_at_300: f64,
}

impl CellResult {
    fn recall_at(&self, candidate_k: usize) -> f64 {
        match candidate_k {
            50 => self.recall_at_50,
            100 => self.recall_at_100,
            300 => self.recall_at_300,
            _ => unreachable!("candidate K is fixed by the Phase 2 plan"),
        }
    }
}

#[derive(Debug, Serialize)]
struct Winner {
    config: &'static str,
    algorithm: Algorithm,
    centering: Centering,
    document_pooling_factor: usize,
    output_dimension: usize,
    candidate_k: usize,
    recall: f64,
}

#[derive(Debug, Serialize)]
struct GeometryReport {
    centering: Centering,
    document_mean_norm: f64,
    query_mean_norm: f64,
    mean_pairwise_document_cosine: f64,
    sampled_document_rows: usize,
    simhash_sampled_document_rows: usize,
    simhash_sampled_query_rows: usize,
    simhash_sampled_documents: usize,
    document_simhash_bucket_occupancy_rate: f64,
    document_simhash_bucket_entropy_bits: f64,
    query_simhash_bucket_occupancy_rate: f64,
    query_simhash_bucket_entropy_bits: f64,
    document_empty_bucket_fill_rate: f64,
}

#[derive(Debug, Serialize)]
struct RoutingReadout {
    metric: &'static str,
    nprobe: usize,
    recall_at_100: f64,
}

#[derive(Debug, Serialize)]
struct RoutingReport {
    nlist: usize,
    fde_dimension: usize,
    readouts: Vec<RoutingReadout>,
}

#[derive(Debug, Serialize)]
struct GoldRankDiagnostic {
    query_index: usize,
    query_id: String,
    document_index: usize,
    document_id: String,
    exact_rank: usize,
    exact_score: f32,
    transformed_exact_score: f32,
    fde_rank: usize,
    fde_inner_product: f32,
    fde_score_per_repetition: f32,
    document_rows: usize,
    query_rows: usize,
}

#[derive(Debug, Serialize)]
struct ScorePairDiagnostic {
    query_index: usize,
    query_id: String,
    document_index: usize,
    document_id: String,
    fde_rank: usize,
    exact_score: f32,
    transformed_exact_score: f32,
    fde_inner_product: f32,
    fde_score_per_repetition: f32,
    document_rows: usize,
    query_rows: usize,
}

#[derive(Debug, Serialize)]
struct DiagnosticCell {
    config: &'static str,
    repetitions: u32,
    simhash_bits: u32,
    d_proj: u32,
    algorithm: Algorithm,
    centering: Centering,
    output_dimension: usize,
    transform_checksum_sha256: String,
    recall_at_50: f64,
    recall_at_100: f64,
    recall_at_300: f64,
    gold_ranks: Vec<GoldRankDiagnostic>,
    score_pairs: Vec<ScorePairDiagnostic>,
}

impl DiagnosticCell {
    fn cell_result(&self) -> CellResult {
        CellResult {
            config: self.config,
            repetitions: self.repetitions,
            simhash_bits: self.simhash_bits,
            d_proj: self.d_proj,
            algorithm: self.algorithm,
            centering: self.centering,
            output_dimension: self.output_dimension,
            recall_at_50: self.recall_at_50,
            recall_at_100: self.recall_at_100,
            recall_at_300: self.recall_at_300,
        }
    }
}

#[derive(Debug, Serialize)]
struct PoolingProbe {
    factor: usize,
    original_mean_rows: f64,
    pooled_mean_rows: f64,
    result: CellResult,
}

#[derive(Debug, Serialize)]
struct PrecisionRetentionReport {
    query_count: usize,
    gold_count: usize,
    f32_top_1_same_rank_fraction: f64,
    f32_top_1_in_f16_top_10_fraction: f64,
    f32_top_10_recall_in_f16_top_10: f64,
}

#[derive(Debug, Serialize)]
struct PrecisionRankingAuditReport {
    reference: &'static str,
    query_count: usize,
    gold_count: usize,
    candidates: Vec<PrecisionRankingCandidateReport>,
}

#[derive(Debug, Serialize)]
struct PrecisionRankingCandidateReport {
    candidate: PrecisionRankingCandidate,
    top_10_set_exactly_equal_query_fraction: f64,
    f32_top_10_recovered_in_candidate_top_10: f64,
    ordered_top_10_exactly_equal_query_fraction: f64,
    per_rank_same_document_fractions: Vec<RankSameDocumentFraction>,
    f32_top_1_same_document_fraction: f64,
    f32_top_1_in_candidate_top_10_fraction: f64,
    coordinate_bytes_total: usize,
    metadata_bytes_per_row: usize,
    mean_payload_bytes_per_unit: f64,
    saving_fraction_vs_f16: f64,
    retrieval_decode_cost: RetrievalDecodeCost,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct RetrievalDecodeCost {
    basis: &'static str,
    scope: &'static str,
    coordinate_conversions_total: usize,
    scale_values_read_total: usize,
    offset_values_read_total: usize,
    dequantize_multiplications_total: usize,
    dequantize_additions_total: usize,
    renormalized_rows_total: usize,
    norm_accumulations_total: usize,
    square_roots_total: usize,
    normalization_divisions_total: usize,
    maxsim_scoring_included: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
enum PrecisionRankingCandidate {
    F16,
    Int8GlobalScale,
    Int8PerRowCalibrated,
    Int8PerRowCalibratedRenormalized,
    Int8SymmetricPerRowRenormalized,
    #[serde(rename = "int8_groupwise_32_symmetric_renormalized")]
    Int8Groupwise32SymmetricRenormalized,
    #[serde(rename = "int8_groupwise_16_symmetric_renormalized")]
    Int8Groupwise16SymmetricRenormalized,
}

const ALL_PRECISION_RANKING_CANDIDATES: [PrecisionRankingCandidate; 7] = [
    PrecisionRankingCandidate::F16,
    PrecisionRankingCandidate::Int8GlobalScale,
    PrecisionRankingCandidate::Int8PerRowCalibrated,
    PrecisionRankingCandidate::Int8PerRowCalibratedRenormalized,
    PrecisionRankingCandidate::Int8SymmetricPerRowRenormalized,
    PrecisionRankingCandidate::Int8Groupwise32SymmetricRenormalized,
    PrecisionRankingCandidate::Int8Groupwise16SymmetricRenormalized,
];

#[derive(Debug, Serialize)]
struct RankSameDocumentFraction {
    rank: usize,
    fraction: f64,
}

#[derive(Clone, Copy)]
struct CandidatePayload {
    coordinate_bytes_total: usize,
    metadata_bytes_per_row: usize,
    mean_payload_bytes_per_unit: f64,
    saving_fraction_vs_f16: f64,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum Int8Variant {
    GlobalScale,
    PerRowCalibrated,
}

#[derive(Debug, Serialize)]
struct Int8ProbeReport {
    candidate_config: &'static str,
    candidate_centering: Centering,
    candidate_document_pooling_factor: usize,
    global_scale_passed: bool,
    variants: Vec<Int8VariantReport>,
}

#[derive(Debug, Serialize)]
struct Int8VariantReport {
    variant: Int8Variant,
    f32_top_1_same_rank_fraction: f64,
    f32_top_10_recall_in_int8_top_10: f64,
    max_sim_50_pair_max_absolute_error: f64,
    first_100_document_row_l2_error_p50: f64,
    first_100_document_row_l2_error_p95: f64,
    first_100_document_row_l2_error_p99: f64,
    mean_payload_bytes_per_document: f64,
    candidate_recall_at_50: f64,
    candidate_recall_at_100: f64,
    candidate_recall_at_300: f64,
    passed: bool,
}

#[derive(Debug, Serialize)]
struct ExactFrontierGap {
    query_index: usize,
    query_id: String,
    query_rows: usize,
    rank_10_document_index: usize,
    rank_10_document_id: String,
    rank_10_score: f32,
    rank_100_document_index: usize,
    rank_100_document_id: String,
    rank_100_score: f32,
    rank_10_to_rank_100_gap: f32,
}

struct PerQueryDiagnostic {
    hits: [usize; 3],
    gold_ranks: Vec<GoldRankDiagnostic>,
    score_pairs: Vec<ScorePairDiagnostic>,
}

struct ExactTruth {
    top_documents: Vec<usize>,
    rank_10: (usize, f32),
    rank_100: (usize, f32),
}

struct PreparedValues<'a> {
    documents: Cow<'a, [f32]>,
    queries: Cow<'a, [f32]>,
}

struct SimhashGeometry {
    sampled_document_rows: usize,
    sampled_query_rows: usize,
    sampled_documents: usize,
    document_bucket_occupancy_rate: f64,
    document_bucket_entropy_bits: f64,
    query_bucket_occupancy_rate: f64,
    query_bucket_entropy_bits: f64,
    document_empty_bucket_fill_rate: f64,
}

struct Evaluation {
    geometry: Vec<GeometryReport>,
    cells: Vec<CellResult>,
    winner: Winner,
    gate_passed: bool,
    routing: Option<RoutingReport>,
    diagnostic_probes: Option<Vec<CellResult>>,
    diagnostics: Option<Vec<DiagnosticCell>>,
    visual_diagnostics: Option<Vec<DiagnosticCell>>,
    pooling_probes: Option<Vec<PoolingProbe>>,
    exact_frontier_gaps: Option<Vec<ExactFrontierGap>>,
}

#[derive(Clone, Copy)]
struct FdeConfig {
    name: &'static str,
    repetitions: u32,
    simhash_bits: u32,
    d_proj: u32,
}

const CONFIG_A: FdeConfig = FdeConfig {
    name: "A",
    repetitions: 20,
    simhash_bits: 5,
    d_proj: 16,
};

const CONFIG_B: FdeConfig = FdeConfig {
    name: "B",
    repetitions: 8,
    simhash_bits: 4,
    d_proj: 16,
};

const CONFIG_C_DIAGNOSTIC: FdeConfig = FdeConfig {
    name: "C-diagnostic",
    repetitions: 20,
    simhash_bits: 6,
    d_proj: 8,
};

const CONFIG_D_DPROJ_DIAGNOSTIC: FdeConfig = FdeConfig {
    name: "D-dproj-diagnostic",
    repetitions: 20,
    simhash_bits: 4,
    d_proj: 32,
};

const CONFIG_E_REPS_DIAGNOSTIC: FdeConfig = FdeConfig {
    name: "E",
    repetitions: 40,
    simhash_bits: 4,
    d_proj: 16,
};

const CONFIG_F_VISUAL_FINE: FdeConfig = FdeConfig {
    name: "F-visual-k6",
    repetitions: 10,
    simhash_bits: 6,
    d_proj: 16,
};

const CONFIG_G_VISUAL_COARSE: FdeConfig = FdeConfig {
    name: "G-visual-k3",
    repetitions: 80,
    simhash_bits: 3,
    d_proj: 16,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("mmli_lab: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let job_path = parse_cli()?;
    let job: Job = read_json(&job_path)?;
    validate_lane_contract(&job)?;

    let base = job_path.parent().unwrap_or_else(|| Path::new("."));
    let documents = load_tensor(&job.documents, base)?;
    let queries = load_tensor(&job.queries, base)?;
    if documents.sidecar.dim != queries.sidecar.dim {
        return Err(LabError::Invalid(format!(
            "document dimension {} differs from query dimension {}",
            documents.sidecar.dim, queries.sidecar.dim
        )));
    }
    if documents.sidecar.ids.len() < CANDIDATE_KS[2] {
        return Err(LabError::Invalid(format!(
            "candidate recall requires at least {} documents, got {}",
            CANDIDATE_KS[2],
            documents.sidecar.ids.len()
        )));
    }
    let full_precision_documents = job
        .full_precision_documents
        .as_ref()
        .map(|input| load_tensor(input, base))
        .transpose()?;
    let full_precision_queries = job
        .full_precision_queries
        .as_ref()
        .map(|input| load_tensor(input, base))
        .transpose()?;
    let identity = prepare_values(&documents, &queries, Centering::Identity)?;
    let identity_truth = exhaustive_truth(&documents, &queries, &identity)?;
    let full_precision = match (&full_precision_documents, &full_precision_queries) {
        (Some(full_documents), Some(full_queries)) => {
            validate_precision_pair(&documents, &queries, full_documents, full_queries)?;
            let f32_identity = prepare_values(full_documents, full_queries, Centering::Identity)?;
            let f32_truth = exhaustive_truth(full_documents, full_queries, &f32_identity)?;
            Some((f32_identity, f32_truth))
        }
        (None, None) => None,
        _ => {
            return Err(LabError::Invalid(
                "full-precision documents and queries must be provided together".to_string(),
            ));
        }
    };
    let precision_retention = full_precision
        .as_ref()
        .map(|(_, f32_truth)| compare_precision_truth(f32_truth, &identity_truth));

    if job.precision_ranking_audit {
        let (_, f32_truth) = full_precision.as_ref().ok_or_else(|| {
            LabError::Invalid(
                "precision_ranking_audit requires paired f32 document and query references"
                    .to_string(),
            )
        })?;
        return write_stdout_json(&PrecisionRankingAuditResultDocument {
            schema_version: 2,
            lane: job.lane,
            seed: FDE_SEED,
            corpus_stats: summarize(&documents),
            query_stats: summarize(&queries),
            precision_ranking_audit: evaluate_precision_ranking_audit(
                &documents,
                &queries,
                f32_truth,
                &identity_truth,
                job.precision_ranking_candidates
                    .as_deref()
                    .unwrap_or(&ALL_PRECISION_RANKING_CANDIDATES),
            )?,
        });
    }

    let official_scores = job
        .official_scores
        .as_ref()
        .ok_or_else(|| LabError::Invalid("non-audit jobs require official_scores".to_string()))?;
    let official_path = resolve_path(base, official_scores);
    let official_scores: Vec<OfficialScore> = read_json(&official_path)?;
    let parity = evaluate_parity(&documents, &queries, &official_scores)?;
    let evaluation = evaluate_fixed_grid(
        &job,
        &documents,
        &queries,
        &identity,
        &identity_truth,
        parity.passed,
    )?;
    let int8_probe = if job.int8_probe {
        let (full_documents, full_queries, (f32_identity, f32_truth)) = match (
            &full_precision_documents,
            &full_precision_queries,
            &full_precision,
        ) {
            (Some(full_documents), Some(full_queries), Some(full_precision)) => {
                (full_documents, full_queries, full_precision)
            }
            _ => {
                return Err(LabError::Invalid(
                    "int8_probe requires paired f32 document and query references".to_string(),
                ));
            }
        };
        Some(evaluate_int8_probe(
            job.lane,
            &documents,
            &queries,
            full_documents,
            full_queries,
            f32_identity,
            f32_truth,
            &official_scores,
            &evaluation.winner,
        )?)
    } else {
        None
    };
    let result = ResultDocument {
        schema_version: 2,
        lane: job.lane,
        seed: FDE_SEED,
        corpus_stats: summarize(&documents),
        query_stats: summarize(&queries),
        parity_max_relative_error: parity.max_relative_error,
        parity,
        geometry: evaluation.geometry,
        cells: evaluation.cells,
        winner: evaluation.winner,
        gate_passed: evaluation.gate_passed,
        routing: evaluation.routing,
        diagnostic_probes: evaluation.diagnostic_probes,
        diagnostics: evaluation.diagnostics,
        visual_diagnostics: evaluation.visual_diagnostics,
        pooling_probes: evaluation.pooling_probes,
        precision_retention,
        int8_probe,
        exact_frontier_gaps: evaluation.exact_frontier_gaps,
    };

    write_stdout_json(&result)
}

fn write_stdout_json(result: &impl Serialize) -> Result<()> {
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    serde_json::to_writer_pretty(&mut writer, result).map_err(|source| LabError::Json {
        path: PathBuf::from("<stdout>"),
        source,
    })?;
    writer.write_all(b"\n").map_err(|source| LabError::Io {
        path: PathBuf::from("<stdout>"),
        source,
    })?;
    writer.flush().map_err(|source| LabError::Io {
        path: PathBuf::from("<stdout>"),
        source,
    })
}

fn parse_cli() -> Result<PathBuf> {
    let mut args = env::args_os();
    let program = args
        .next()
        .and_then(|value| value.into_string().ok())
        .unwrap_or_else(|| "mmli_lab".to_string());
    let Some(job_path) = args.next() else {
        return Err(LabError::Usage(format!("usage: {program} <job.json>")));
    };
    if job_path == "-h" || job_path == "--help" {
        println!("usage: {program} <job.json>");
        std::process::exit(0);
    }
    if args.next().is_some() {
        return Err(LabError::Usage(format!("usage: {program} <job.json>")));
    }
    Ok(PathBuf::from(job_path))
}

fn validate_lane_contract(job: &Job) -> Result<()> {
    if !job.precision_ranking_audit && job.official_scores.is_none() {
        return Err(LabError::Invalid(
            "non-audit jobs require official_scores".to_string(),
        ));
    }
    if !job.precision_ranking_audit {
        match job.lane {
            Lane::Text => {
                if job.chosen_algorithm.is_some() || job.chosen_centering.is_some() {
                    return Err(LabError::Invalid(
                        "text jobs must not provide visual-only settings".to_string(),
                    ));
                }
            }
            Lane::Visual => {
                if job.chosen_algorithm.is_none() || job.chosen_centering.is_none() {
                    return Err(LabError::Invalid(
                        "visual jobs require chosen_algorithm and chosen_centering".to_string(),
                    ));
                }
            }
        }
    }
    if job.precision_ranking_audit && job.int8_probe {
        return Err(LabError::Invalid(
            "precision_ranking_audit cannot run with int8_probe".to_string(),
        ));
    }
    if let Some(candidates) = &job.precision_ranking_candidates {
        if !job.precision_ranking_audit {
            return Err(LabError::Invalid(
                "precision_ranking_candidates requires precision_ranking_audit".to_string(),
            ));
        }
        if candidates.is_empty() {
            return Err(LabError::Invalid(
                "precision_ranking_candidates must not be empty".to_string(),
            ));
        }
        let mut seen = HashSet::with_capacity(candidates.len());
        for candidate in candidates {
            if !seen.insert(*candidate) {
                return Err(LabError::Invalid(format!(
                    "precision_ranking_candidates repeats {candidate:?}"
                )));
            }
        }
    }
    if job.int8_probe
        && (job.full_precision_documents.is_none() || job.full_precision_queries.is_none())
    {
        return Err(LabError::Invalid(
            "int8_probe requires paired f32 document and query references".to_string(),
        ));
    }
    if job.precision_ranking_audit
        && (job.full_precision_documents.is_none() || job.full_precision_queries.is_none())
    {
        return Err(LabError::Invalid(
            "precision_ranking_audit requires paired f32 document and query references".to_string(),
        ));
    }
    Ok(())
}

fn validate_precision_pair(
    documents: &TensorSet,
    queries: &TensorSet,
    full_documents: &TensorSet,
    full_queries: &TensorSet,
) -> Result<()> {
    if documents.sidecar.dtype != DType::F16
        || queries.sidecar.dtype != DType::F16
        || full_documents.sidecar.dtype != DType::F32
        || full_queries.sidecar.dtype != DType::F32
    {
        return Err(LabError::Invalid(
            "precision retention requires f16 primary and f32 reference tensors".to_string(),
        ));
    }
    for (label, quantized, full) in [
        ("documents", documents, full_documents),
        ("queries", queries, full_queries),
    ] {
        if quantized.sidecar.dim != full.sidecar.dim
            || quantized.sidecar.rows != full.sidecar.rows
            || quantized.sidecar.ids != full.sidecar.ids
        {
            return Err(LabError::Invalid(format!(
                "f16 and f32 {label} tensor shapes or IDs differ"
            )));
        }
    }
    Ok(())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let file = File::open(path).map_err(|source| LabError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_reader(BufReader::new(file)).map_err(|source| LabError::Json {
        path: path.to_path_buf(),
        source,
    })
}

fn resolve_path(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn load_tensor(input: &TensorInput, base: &Path) -> Result<TensorSet> {
    let raw_path = resolve_path(base, &input.raw);
    let sidecar_path = resolve_path(base, &input.sidecar);
    let sidecar: TensorSidecar = read_json(&sidecar_path)?;
    let total_rows = validate_sidecar(&sidecar, &sidecar_path)?;
    let scalar_count = total_rows.checked_mul(sidecar.dim).ok_or_else(|| {
        LabError::Invalid(format!("{} tensor shape overflows", raw_path.display()))
    })?;
    let values = read_raw_tensor(&raw_path, sidecar.dtype, scalar_count)?;
    let mut scalar_offsets = Vec::with_capacity(sidecar.rows.len());
    let mut offset = 0usize;
    for &rows in &sidecar.rows {
        scalar_offsets.push(offset);
        offset = offset
            .checked_add(rows.checked_mul(sidecar.dim).ok_or_else(|| {
                LabError::Invalid(format!("{} matrix shape overflows", raw_path.display()))
            })?)
            .ok_or_else(|| {
                LabError::Invalid(format!("{} matrix offsets overflow", raw_path.display()))
            })?;
    }
    Ok(TensorSet {
        sidecar,
        values,
        scalar_offsets,
        total_rows,
    })
}

fn validate_sidecar(sidecar: &TensorSidecar, path: &Path) -> Result<usize> {
    if sidecar.dim == 0 {
        return Err(LabError::Invalid(format!(
            "{} has zero dimension",
            path.display()
        )));
    }
    if sidecar.rows.is_empty() {
        return Err(LabError::Invalid(format!(
            "{} describes no matrices",
            path.display()
        )));
    }
    if sidecar.rows.len() != sidecar.ids.len() {
        return Err(LabError::Invalid(format!(
            "{} has {} row counts but {} ids",
            path.display(),
            sidecar.rows.len(),
            sidecar.ids.len()
        )));
    }

    let mut ids = HashSet::with_capacity(sidecar.ids.len());
    let mut total_rows = 0usize;
    for (index, (&rows, id)) in sidecar.rows.iter().zip(&sidecar.ids).enumerate() {
        if rows == 0 {
            return Err(LabError::Invalid(format!(
                "{} matrix {index} has zero rows",
                path.display()
            )));
        }
        if id.is_empty() {
            return Err(LabError::Invalid(format!(
                "{} id {index} is empty",
                path.display()
            )));
        }
        if !ids.insert(id.as_str()) {
            return Err(LabError::Invalid(format!(
                "{} repeats id {id:?}",
                path.display()
            )));
        }
        total_rows = total_rows.checked_add(rows).ok_or_else(|| {
            LabError::Invalid(format!("{} total row count overflows", path.display()))
        })?;
    }
    Ok(total_rows)
}

fn read_raw_tensor(path: &Path, dtype: DType, scalar_count: usize) -> Result<Vec<f32>> {
    let expected_bytes = scalar_count
        .checked_mul(dtype.byte_width())
        .ok_or_else(|| LabError::Invalid(format!("{} byte size overflows", path.display())))?;
    let file = File::open(path).map_err(|source| LabError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let metadata = file.metadata().map_err(|source| LabError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    if !metadata.is_file() {
        return Err(LabError::Invalid(format!(
            "{} is not a regular file",
            path.display()
        )));
    }
    if metadata.len() != expected_bytes as u64 {
        return Err(LabError::Invalid(format!(
            "{} is {} bytes, expected {expected_bytes}",
            path.display(),
            metadata.len()
        )));
    }

    let width = dtype.byte_width();
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, file);
    let mut values = Vec::with_capacity(scalar_count);
    let mut buffer = vec![0_u8; IO_BUFFER_BYTES - (IO_BUFFER_BYTES % width)];
    while values.len() < scalar_count {
        let remaining = scalar_count - values.len();
        let value_count = remaining.min(buffer.len() / width);
        let byte_count = value_count * width;
        reader
            .read_exact(&mut buffer[..byte_count])
            .map_err(|source| LabError::Io {
                path: path.to_path_buf(),
                source,
            })?;
        match dtype {
            DType::F16 => {
                for chunk in buffer[..byte_count].chunks_exact(2) {
                    let bits = u16::from_le_bytes([chunk[0], chunk[1]]);
                    values.push(f16_to_f32(bits));
                }
            }
            DType::F32 => {
                for chunk in buffer[..byte_count].chunks_exact(4) {
                    values.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
                }
            }
        }
    }
    if let Some(index) = values.iter().position(|value| !value.is_finite()) {
        return Err(LabError::Invalid(format!(
            "{} contains non-finite value at scalar index {index}",
            path.display()
        )));
    }
    Ok(values)
}

fn f16_to_f32(bits: u16) -> f32 {
    let sign = u32::from(bits & 0x8000) << 16;
    let exponent = (bits >> 10) & 0x1f;
    let fraction = bits & 0x03ff;
    match exponent {
        0 if fraction == 0 => f32::from_bits(sign),
        0 => {
            let magnitude = f32::from(fraction) * 2.0_f32.powi(-24);
            if sign == 0 {
                magnitude
            } else {
                -magnitude
            }
        }
        0x1f => f32::from_bits(sign | 0x7f80_0000 | (u32::from(fraction) << 13)),
        _ => f32::from_bits(
            sign | (u32::from(exponent + (127 - 15)) << 23) | (u32::from(fraction) << 13),
        ),
    }
}

fn summarize(tensor: &TensorSet) -> TensorSummary {
    let min_rows = tensor.sidecar.rows.iter().copied().min().unwrap_or(0);
    let max_rows = tensor.sidecar.rows.iter().copied().max().unwrap_or(0);
    let mut ordered_rows = tensor.sidecar.rows.clone();
    ordered_rows.sort_unstable();
    TensorSummary {
        matrix_count: tensor.sidecar.rows.len(),
        total_rows: tensor.total_rows,
        min_rows,
        p50_rows_nearest_rank: nearest_rank(&ordered_rows, 50),
        p95_rows_nearest_rank: nearest_rank(&ordered_rows, 95),
        max_rows,
        mean_rows: tensor.total_rows as f64 / tensor.sidecar.rows.len() as f64,
        dim: tensor.sidecar.dim,
        dtype: tensor.sidecar.dtype,
        scalar_count: tensor.values.len(),
    }
}

fn nearest_rank(ordered: &[usize], percentile: usize) -> usize {
    let rank = percentile
        .checked_mul(ordered.len())
        .expect("validated tensor row count fits percentile arithmetic")
        .div_ceil(100);
    ordered[rank.saturating_sub(1)]
}

impl TensorSet {
    fn matrix<'a>(&self, values: &'a [f32], index: usize) -> Result<MultiVectorMatrixRef<'a>> {
        let start = self.scalar_offsets[index];
        let scalar_count = self.sidecar.rows[index]
            .checked_mul(self.sidecar.dim)
            .ok_or_else(|| LabError::Invalid("matrix scalar count overflows".to_string()))?;
        let end = start
            .checked_add(scalar_count)
            .ok_or_else(|| LabError::Invalid("matrix slice end overflows".to_string()))?;
        MultiVectorMatrixRef::new(
            &values[start..end],
            self.sidecar.rows[index],
            self.sidecar.dim,
            MAX_MATRIX_ROWS,
        )
        .map_err(Into::into)
    }
}

fn evaluate_parity(
    documents: &TensorSet,
    queries: &TensorSet,
    official_scores: &[OfficialScore],
) -> Result<ParityReport> {
    if official_scores.len() != 50 {
        return Err(LabError::Invalid(format!(
            "official_scores must contain exactly 50 pairs, got {}",
            official_scores.len()
        )));
    }
    let document_ids: HashMap<&str, usize> = documents
        .sidecar
        .ids
        .iter()
        .enumerate()
        .map(|(index, id)| (id.as_str(), index))
        .collect();
    let query_ids: HashMap<&str, usize> = queries
        .sidecar
        .ids
        .iter()
        .enumerate()
        .map(|(index, id)| (id.as_str(), index))
        .collect();
    let mut seen = HashSet::with_capacity(official_scores.len());
    let mut max_absolute_error = 0.0_f64;
    let mut max_relative_error = 0.0_f64;
    for pair in official_scores {
        if !pair.score.is_finite() {
            return Err(LabError::Invalid(format!(
                "official score for ({:?}, {:?}) is not finite",
                pair.query_id, pair.document_id
            )));
        }
        if !seen.insert((pair.query_id.as_str(), pair.document_id.as_str())) {
            return Err(LabError::Invalid(format!(
                "official_scores repeats ({:?}, {:?})",
                pair.query_id, pair.document_id
            )));
        }
        let query_index = *query_ids.get(pair.query_id.as_str()).ok_or_else(|| {
            LabError::Invalid(format!(
                "official score references unknown query id {:?}",
                pair.query_id
            ))
        })?;
        let document_index = *document_ids.get(pair.document_id.as_str()).ok_or_else(|| {
            LabError::Invalid(format!(
                "official score references unknown document id {:?}",
                pair.document_id
            ))
        })?;
        let query = queries.matrix(&queries.values, query_index)?;
        let document = documents.matrix(&documents.values, document_index)?;
        let actual = max_sim(&query, &document)?;
        let absolute = f64::from((actual - pair.score).abs());
        let relative = absolute / f64::from(pair.score.abs()).max(1.0e-12);
        max_absolute_error = max_absolute_error.max(absolute);
        max_relative_error = max_relative_error.max(relative);
    }
    Ok(ParityReport {
        pair_count: official_scores.len(),
        max_absolute_error,
        max_relative_error,
        passed: max_relative_error <= 1.0e-4,
    })
}

fn prepare_values<'a>(
    documents: &'a TensorSet,
    queries: &'a TensorSet,
    centering: Centering,
) -> Result<PreparedValues<'a>> {
    if centering == Centering::Identity {
        return Ok(PreparedValues {
            documents: Cow::Borrowed(&documents.values),
            queries: Cow::Borrowed(&queries.values),
        });
    }
    let sample_count = documents.total_rows.min(CENTERING_SAMPLE_ROWS);
    let mut mean = vec![0.0_f64; documents.sidecar.dim];
    for sample in 0..sample_count {
        let row = sample * documents.total_rows / sample_count;
        let start = row * documents.sidecar.dim;
        for (coordinate, value) in mean
            .iter_mut()
            .zip(&documents.values[start..start + documents.sidecar.dim])
        {
            *coordinate += f64::from(*value);
        }
    }
    for value in &mut mean {
        *value /= sample_count as f64;
    }
    Ok(PreparedValues {
        documents: Cow::Owned(center_tensor(
            &documents.values,
            documents.sidecar.dim,
            &mean,
            centering,
        )?),
        queries: Cow::Owned(center_tensor(
            &queries.values,
            queries.sidecar.dim,
            &mean,
            centering,
        )?),
    })
}

fn center_tensor(
    values: &[f32],
    dim: usize,
    mean: &[f64],
    centering: Centering,
) -> Result<Vec<f32>> {
    let mut centered = Vec::with_capacity(values.len());
    for (row_index, row) in values.chunks_exact(dim).enumerate() {
        let start = centered.len();
        centered.extend(
            row.iter()
                .zip(mean)
                .map(|(value, center)| (*value as f64 - center) as f32),
        );
        if centering == Centering::SubtractGlobalMeanRenormalize {
            let norm = centered[start..]
                .iter()
                .map(|value| f64::from(*value) * f64::from(*value))
                .sum::<f64>()
                .sqrt();
            if norm == 0.0 || !norm.is_finite() {
                return Err(LabError::Invalid(format!(
                    "centering produced an invalid norm for row {row_index}"
                )));
            }
            for value in &mut centered[start..] {
                *value = (f64::from(*value) / norm) as f32;
            }
        }
    }
    Ok(centered)
}

fn mean_pool_documents(documents: &TensorSet, values: &[f32], factor: usize) -> Result<TensorSet> {
    if factor < 2 {
        return Err(LabError::Invalid(
            "document pooling factor must be at least two".to_string(),
        ));
    }
    let dim = documents.sidecar.dim;
    let mut pooled_values = Vec::with_capacity(values.len().div_ceil(factor));
    let mut rows = Vec::with_capacity(documents.sidecar.rows.len());
    let mut scalar_offsets = Vec::with_capacity(documents.sidecar.rows.len());
    let mut total_rows = 0usize;
    for document_index in 0..documents.sidecar.rows.len() {
        let matrix = documents.matrix(values, document_index)?;
        scalar_offsets.push(pooled_values.len());
        let pooled_rows = matrix.vector_count().div_ceil(factor);
        rows.push(pooled_rows);
        total_rows = total_rows
            .checked_add(pooled_rows)
            .ok_or_else(|| LabError::Invalid("pooled row count overflows".to_string()))?;
        for start in (0..matrix.vector_count()).step_by(factor) {
            let end = (start + factor).min(matrix.vector_count());
            let divisor = (end - start) as f64;
            for coordinate in 0..dim {
                let sum = (start..end)
                    .map(|row| f64::from(matrix.row(row)[coordinate]))
                    .sum::<f64>();
                pooled_values.push((sum / divisor) as f32);
            }
        }
    }
    Ok(TensorSet {
        sidecar: TensorSidecar {
            rows,
            dim,
            dtype: DType::F32,
            ids: documents.sidecar.ids.clone(),
        },
        values: pooled_values,
        scalar_offsets,
        total_rows,
    })
}

fn exhaustive_truth(
    documents: &TensorSet,
    queries: &TensorSet,
    prepared: &PreparedValues<'_>,
) -> Result<Vec<ExactTruth>> {
    parallel_indexed_map(queries.sidecar.ids.len(), |query_index| {
        let query = queries.matrix(&prepared.queries, query_index)?;
        let mut scores = Vec::with_capacity(documents.sidecar.ids.len());
        for document_index in 0..documents.sidecar.ids.len() {
            let document = documents.matrix(&prepared.documents, document_index)?;
            let score = max_sim(&query, &document)?;
            if !score.is_finite() {
                return Err(LabError::Invalid(format!(
                    "MaxSim overflowed for query {query_index}, document {document_index}"
                )));
            }
            scores.push((document_index, score));
        }
        rank_scores(&mut scores);
        Ok(ExactTruth {
            top_documents: scores
                .iter()
                .take(TRUTH_K)
                .map(|&(index, _)| index)
                .collect(),
            rank_10: scores[TRUTH_K - 1],
            rank_100: scores[ROUTING_TRUTH_K - 1],
        })
    })
}

fn compare_precision_truth(
    reference: &[ExactTruth],
    quantized: &[ExactTruth],
) -> PrecisionRetentionReport {
    assert_eq!(reference.len(), quantized.len());
    let mut top_1_same_rank = 0usize;
    let mut top_1_in_top_10 = 0usize;
    let mut top_10_hits = 0usize;
    for (reference_query, quantized_query) in reference.iter().zip(quantized) {
        let reference_top_1 = reference_query.top_documents[0];
        top_1_same_rank += usize::from(reference_top_1 == quantized_query.top_documents[0]);
        top_1_in_top_10 += usize::from(quantized_query.top_documents.contains(&reference_top_1));
        top_10_hits += reference_query
            .top_documents
            .iter()
            .filter(|document| quantized_query.top_documents.contains(document))
            .count();
    }
    let query_count = reference.len();
    let gold_count = query_count * TRUTH_K;
    PrecisionRetentionReport {
        query_count,
        gold_count,
        f32_top_1_same_rank_fraction: top_1_same_rank as f64 / query_count as f64,
        f32_top_1_in_f16_top_10_fraction: top_1_in_top_10 as f64 / query_count as f64,
        f32_top_10_recall_in_f16_top_10: top_10_hits as f64 / gold_count as f64,
    }
}

fn evaluate_precision_ranking_audit(
    documents: &TensorSet,
    queries: &TensorSet,
    f32_truth: &[ExactTruth],
    f16_truth: &[ExactTruth],
    requested_candidates: &[PrecisionRankingCandidate],
) -> Result<PrecisionRankingAuditReport> {
    let mut candidates = Vec::with_capacity(requested_candidates.len());
    for &candidate in requested_candidates {
        candidates.push(evaluate_precision_ranking_candidate(
            documents, queries, f32_truth, f16_truth, candidate,
        )?);
    }

    Ok(PrecisionRankingAuditReport {
        reference: "f32_documents_f32_queries",
        query_count: f32_truth.len(),
        gold_count: f32_truth.len() * TRUTH_K,
        candidates,
    })
}

fn evaluate_precision_ranking_candidate(
    documents: &TensorSet,
    queries: &TensorSet,
    f32_truth: &[ExactTruth],
    f16_truth: &[ExactTruth],
    candidate: PrecisionRankingCandidate,
) -> Result<PrecisionRankingCandidateReport> {
    let payload = precision_ranking_candidate_payload(documents, candidate)?;
    let retrieval_decode_cost = precision_ranking_candidate_decode_cost(documents, candidate)?;
    if candidate == PrecisionRankingCandidate::F16 {
        return Ok(compare_precision_ranking_candidate(
            candidate,
            f32_truth,
            f16_truth,
            payload,
            retrieval_decode_cost,
        ));
    }

    let mut candidate_documents = match candidate {
        PrecisionRankingCandidate::F16 => unreachable!("f16 returned above"),
        PrecisionRankingCandidate::Int8GlobalScale => {
            quantize_documents(documents, Int8Variant::GlobalScale)?
        }
        PrecisionRankingCandidate::Int8PerRowCalibrated
        | PrecisionRankingCandidate::Int8PerRowCalibratedRenormalized => {
            quantize_documents(documents, Int8Variant::PerRowCalibrated)?
        }
        PrecisionRankingCandidate::Int8SymmetricPerRowRenormalized => {
            quantize_symmetric_renormalized(documents, documents.sidecar.dim)?
        }
        PrecisionRankingCandidate::Int8Groupwise32SymmetricRenormalized => {
            quantize_symmetric_renormalized(documents, 32)?
        }
        PrecisionRankingCandidate::Int8Groupwise16SymmetricRenormalized => {
            quantize_symmetric_renormalized(documents, 16)?
        }
    };
    if candidate == PrecisionRankingCandidate::Int8PerRowCalibratedRenormalized {
        renormalize_rows(&mut candidate_documents, documents.sidecar.dim)?;
    }
    let prepared = PreparedValues {
        documents: Cow::Borrowed(&candidate_documents),
        queries: Cow::Borrowed(&queries.values),
    };
    let candidate_truth = exhaustive_truth(documents, queries, &prepared)?;
    Ok(compare_precision_ranking_candidate(
        candidate,
        f32_truth,
        &candidate_truth,
        payload,
        retrieval_decode_cost,
    ))
}

fn precision_ranking_candidate_payload(
    documents: &TensorSet,
    candidate: PrecisionRankingCandidate,
) -> Result<CandidatePayload> {
    let (coordinate_width, metadata_bytes_per_row) = match candidate {
        PrecisionRankingCandidate::F16 => (DType::F16.byte_width(), 0),
        PrecisionRankingCandidate::Int8GlobalScale => (1, 0),
        PrecisionRankingCandidate::Int8PerRowCalibrated
        | PrecisionRankingCandidate::Int8PerRowCalibratedRenormalized => (1, 8),
        PrecisionRankingCandidate::Int8SymmetricPerRowRenormalized => (1, 2),
        PrecisionRankingCandidate::Int8Groupwise32SymmetricRenormalized => (
            1,
            documents
                .sidecar
                .dim
                .div_ceil(32)
                .checked_mul(DType::F16.byte_width())
                .ok_or_else(|| {
                    LabError::Invalid("groupwise-32 metadata size overflows".to_string())
                })?,
        ),
        PrecisionRankingCandidate::Int8Groupwise16SymmetricRenormalized => (
            1,
            documents
                .sidecar
                .dim
                .div_ceil(16)
                .checked_mul(DType::F16.byte_width())
                .ok_or_else(|| {
                    LabError::Invalid("groupwise-16 metadata size overflows".to_string())
                })?,
        ),
    };
    candidate_payload(documents, coordinate_width, metadata_bytes_per_row)
}

fn precision_ranking_candidate_decode_cost(
    documents: &TensorSet,
    candidate: PrecisionRankingCandidate,
) -> Result<RetrievalDecodeCost> {
    let scalar_count = documents.values.len();
    let row_count = documents.total_rows;
    let renormalized = matches!(
        candidate,
        PrecisionRankingCandidate::Int8PerRowCalibratedRenormalized
            | PrecisionRankingCandidate::Int8SymmetricPerRowRenormalized
            | PrecisionRankingCandidate::Int8Groupwise32SymmetricRenormalized
            | PrecisionRankingCandidate::Int8Groupwise16SymmetricRenormalized
    );
    let (scale_reads, offset_reads, dequant_multiplications, dequant_additions) = match candidate {
        PrecisionRankingCandidate::F16 => (0, 0, 0, 0),
        PrecisionRankingCandidate::Int8GlobalScale => (0, 0, scalar_count, 0),
        PrecisionRankingCandidate::Int8PerRowCalibrated
        | PrecisionRankingCandidate::Int8PerRowCalibratedRenormalized => {
            (row_count, row_count, scalar_count, scalar_count)
        }
        PrecisionRankingCandidate::Int8SymmetricPerRowRenormalized => {
            (row_count, 0, scalar_count, 0)
        }
        PrecisionRankingCandidate::Int8Groupwise32SymmetricRenormalized => (
            row_count
                .checked_mul(documents.sidecar.dim.div_ceil(32))
                .ok_or_else(|| {
                    LabError::Invalid("groupwise-32 scale-read count overflows".to_string())
                })?,
            0,
            scalar_count,
            0,
        ),
        PrecisionRankingCandidate::Int8Groupwise16SymmetricRenormalized => (
            row_count
                .checked_mul(documents.sidecar.dim.div_ceil(16))
                .ok_or_else(|| {
                    LabError::Invalid("groupwise-16 scale-read count overflows".to_string())
                })?,
            0,
            scalar_count,
            0,
        ),
    };
    Ok(RetrievalDecodeCost {
        basis: "analytic_exact_counts",
        scope: "payload_to_f32_before_maxsim",
        coordinate_conversions_total: scalar_count,
        scale_values_read_total: scale_reads,
        offset_values_read_total: offset_reads,
        dequantize_multiplications_total: dequant_multiplications,
        dequantize_additions_total: dequant_additions,
        renormalized_rows_total: if renormalized { row_count } else { 0 },
        norm_accumulations_total: if renormalized { scalar_count } else { 0 },
        square_roots_total: if renormalized { row_count } else { 0 },
        normalization_divisions_total: if renormalized { scalar_count } else { 0 },
        maxsim_scoring_included: false,
    })
}

fn compare_precision_ranking_candidate(
    candidate: PrecisionRankingCandidate,
    reference: &[ExactTruth],
    candidate_truth: &[ExactTruth],
    payload: CandidatePayload,
    retrieval_decode_cost: RetrievalDecodeCost,
) -> PrecisionRankingCandidateReport {
    assert_eq!(reference.len(), candidate_truth.len());
    let mut top_10_set_exactly_equal_queries = 0usize;
    let mut top_10_recovered = 0usize;
    let mut ordered_top_10_exactly_equal_queries = 0usize;
    let mut per_rank_same_document = [0usize; TRUTH_K];
    let mut top_1_same_document = 0usize;
    let mut top_1_in_candidate_top_10 = 0usize;

    for (reference_query, candidate_query) in reference.iter().zip(candidate_truth) {
        top_10_set_exactly_equal_queries += usize::from(
            reference_query
                .top_documents
                .iter()
                .all(|document| candidate_query.top_documents.contains(document)),
        );
        top_10_recovered += reference_query
            .top_documents
            .iter()
            .filter(|document| candidate_query.top_documents.contains(document))
            .count();
        ordered_top_10_exactly_equal_queries +=
            usize::from(reference_query.top_documents == candidate_query.top_documents);
        for (rank, (reference_document, candidate_document)) in reference_query
            .top_documents
            .iter()
            .zip(&candidate_query.top_documents)
            .enumerate()
        {
            per_rank_same_document[rank] += usize::from(reference_document == candidate_document);
        }
        let reference_top_1 = reference_query.top_documents[0];
        top_1_same_document += usize::from(reference_top_1 == candidate_query.top_documents[0]);
        top_1_in_candidate_top_10 +=
            usize::from(candidate_query.top_documents.contains(&reference_top_1));
    }

    let query_count = reference.len();
    PrecisionRankingCandidateReport {
        candidate,
        top_10_set_exactly_equal_query_fraction: top_10_set_exactly_equal_queries as f64
            / query_count as f64,
        f32_top_10_recovered_in_candidate_top_10: top_10_recovered as f64
            / (query_count * TRUTH_K) as f64,
        ordered_top_10_exactly_equal_query_fraction: ordered_top_10_exactly_equal_queries as f64
            / query_count as f64,
        per_rank_same_document_fractions: per_rank_same_document
            .into_iter()
            .enumerate()
            .map(|(rank, count)| RankSameDocumentFraction {
                rank: rank + 1,
                fraction: count as f64 / query_count as f64,
            })
            .collect(),
        f32_top_1_same_document_fraction: top_1_same_document as f64 / query_count as f64,
        f32_top_1_in_candidate_top_10_fraction: top_1_in_candidate_top_10 as f64
            / query_count as f64,
        coordinate_bytes_total: payload.coordinate_bytes_total,
        metadata_bytes_per_row: payload.metadata_bytes_per_row,
        mean_payload_bytes_per_unit: payload.mean_payload_bytes_per_unit,
        saving_fraction_vs_f16: payload.saving_fraction_vs_f16,
        retrieval_decode_cost,
    }
}

fn candidate_payload(
    documents: &TensorSet,
    coordinate_width: usize,
    metadata_bytes_per_row: usize,
) -> Result<CandidatePayload> {
    let coordinate_bytes_total = documents
        .values
        .len()
        .checked_mul(coordinate_width)
        .ok_or_else(|| LabError::Invalid("coordinate payload size overflows".to_string()))?;
    let metadata_bytes_total = documents
        .total_rows
        .checked_mul(metadata_bytes_per_row)
        .ok_or_else(|| LabError::Invalid("metadata payload size overflows".to_string()))?;
    let payload_bytes_total = coordinate_bytes_total
        .checked_add(metadata_bytes_total)
        .ok_or_else(|| LabError::Invalid("combined payload size overflows".to_string()))?;
    let f16_bytes_total = documents
        .values
        .len()
        .checked_mul(DType::F16.byte_width())
        .ok_or_else(|| LabError::Invalid("f16 payload size overflows".to_string()))?;
    Ok(CandidatePayload {
        coordinate_bytes_total,
        metadata_bytes_per_row,
        mean_payload_bytes_per_unit: payload_bytes_total as f64
            / documents.sidecar.ids.len() as f64,
        saving_fraction_vs_f16: 1.0 - payload_bytes_total as f64 / f16_bytes_total as f64,
    })
}

#[allow(clippy::too_many_arguments)]
fn evaluate_int8_probe(
    lane: Lane,
    documents: &TensorSet,
    queries: &TensorSet,
    full_documents: &TensorSet,
    full_queries: &TensorSet,
    f32_identity: &PreparedValues<'_>,
    f32_truth: &[ExactTruth],
    official_scores: &[OfficialScore],
    winner: &Winner,
) -> Result<Int8ProbeReport> {
    if winner.config != CONFIG_E_REPS_DIAGNOSTIC.name {
        return Err(LabError::Invalid(format!(
            "int8 probe requires selected config E, got {}",
            winner.config
        )));
    }
    let (document_fdes, query_fdes, candidate_centering, pooling_factor) =
        selected_int8_candidate_fdes(lane, documents, queries, winner)?;
    let fde_dimension = generate_transform(
        documents.sidecar.dim,
        CONFIG_E_REPS_DIAGNOSTIC,
        winner.algorithm,
    )?
    .output_dimension();

    let mut variants = Vec::with_capacity(2);
    for variant in [Int8Variant::GlobalScale, Int8Variant::PerRowCalibrated] {
        let dequantized_documents = quantize_documents(documents, variant)?;
        let int8_identity = PreparedValues {
            documents: Cow::Borrowed(&dequantized_documents),
            queries: Cow::Borrowed(&queries.values),
        };
        let int8_truth = exhaustive_truth(documents, queries, &int8_identity)?;
        let (top_1_same_rank, top_10_recovery) = compare_int8_truth(f32_truth, &int8_truth);
        let max_sim_error = int8_max_sim_error(
            documents,
            queries,
            full_documents,
            full_queries,
            f32_identity,
            &dequantized_documents,
            official_scores,
        )?;
        let row_errors = int8_row_l2_errors(documents, full_documents, &dequantized_documents)?;
        let recalls = candidate_recalls(&document_fdes, &query_fdes, fde_dimension, &int8_truth)?;
        let metadata_bytes_per_row = match variant {
            Int8Variant::GlobalScale => 0usize,
            Int8Variant::PerRowCalibrated => 8,
        };
        let payload_bytes = documents
            .values
            .len()
            .checked_add(
                documents
                    .total_rows
                    .checked_mul(metadata_bytes_per_row)
                    .ok_or_else(|| {
                        LabError::Invalid("int8 calibration payload size overflows".to_string())
                    })?,
            )
            .ok_or_else(|| LabError::Invalid("int8 payload size overflows".to_string()))?;
        let passed = top_10_recovery >= INT8_TOP_10_RECOVERY_THRESHOLD
            && max_sim_error <= INT8_MAX_SIM_ERROR_THRESHOLD;
        variants.push(Int8VariantReport {
            variant,
            f32_top_1_same_rank_fraction: top_1_same_rank,
            f32_top_10_recall_in_int8_top_10: top_10_recovery,
            max_sim_50_pair_max_absolute_error: max_sim_error,
            first_100_document_row_l2_error_p50: nearest_rank_f64(&row_errors, 50),
            first_100_document_row_l2_error_p95: nearest_rank_f64(&row_errors, 95),
            first_100_document_row_l2_error_p99: nearest_rank_f64(&row_errors, 99),
            mean_payload_bytes_per_document: payload_bytes as f64
                / documents.sidecar.ids.len() as f64,
            candidate_recall_at_50: recalls[0],
            candidate_recall_at_100: recalls[1],
            candidate_recall_at_300: recalls[2],
            passed,
        });
    }
    let global_scale_passed = variants.first().is_some_and(|variant| variant.passed);
    Ok(Int8ProbeReport {
        candidate_config: CONFIG_E_REPS_DIAGNOSTIC.name,
        candidate_centering,
        candidate_document_pooling_factor: pooling_factor,
        global_scale_passed,
        variants,
    })
}

fn selected_int8_candidate_fdes(
    lane: Lane,
    documents: &TensorSet,
    queries: &TensorSet,
    winner: &Winner,
) -> Result<(Vec<f32>, Vec<f32>, Centering, usize)> {
    let transform = generate_transform(
        documents.sidecar.dim,
        CONFIG_E_REPS_DIAGNOSTIC,
        winner.algorithm,
    )?;
    match lane {
        Lane::Text => {
            if winner.document_pooling_factor != 1 {
                return Err(LabError::Invalid(format!(
                    "text int8 probe expected unpooled documents, got factor {}",
                    winner.document_pooling_factor
                )));
            }
            let prepared = prepare_values(documents, queries, winner.centering)?;
            Ok((
                encode_set(documents, &prepared.documents, &transform, true)?,
                encode_set(queries, &prepared.queries, &transform, false)?,
                winner.centering,
                1,
            ))
        }
        Lane::Visual => {
            if winner.centering != Centering::Identity
                || winner.document_pooling_factor != VISUAL_DOCUMENT_POOLING_FACTOR
            {
                return Err(LabError::Invalid(format!(
                    "visual int8 probe requires identity centering and {}x pooling",
                    VISUAL_DOCUMENT_POOLING_FACTOR
                )));
            }
            let pooled =
                mean_pool_documents(documents, &documents.values, VISUAL_DOCUMENT_POOLING_FACTOR)?;
            Ok((
                encode_set(&pooled, &pooled.values, &transform, true)?,
                encode_set(queries, &queries.values, &transform, false)?,
                Centering::Identity,
                VISUAL_DOCUMENT_POOLING_FACTOR,
            ))
        }
    }
}

fn quantize_documents(documents: &TensorSet, variant: Int8Variant) -> Result<Vec<f32>> {
    if documents.sidecar.dtype != DType::F16 {
        return Err(LabError::Invalid(
            "int8 probe requires f16 primary documents".to_string(),
        ));
    }
    let mut dequantized = Vec::with_capacity(documents.values.len());
    for row in documents.values.chunks_exact(documents.sidecar.dim) {
        match variant {
            Int8Variant::GlobalScale => {
                dequantized.extend(row.iter().map(|value| {
                    let quantized = (*value * 127.0).round().clamp(-127.0, 127.0) as i8;
                    f32::from(quantized) / 127.0
                }));
            }
            Int8Variant::PerRowCalibrated => {
                let minimum = row.iter().copied().fold(f32::INFINITY, f32::min);
                let maximum = row.iter().copied().fold(f32::NEG_INFINITY, f32::max);
                if minimum == maximum {
                    dequantized.extend(std::iter::repeat_n(minimum, row.len()));
                    continue;
                }
                let offset = minimum + (maximum - minimum) * 0.5;
                let scale = (maximum - minimum) / 254.0;
                if !scale.is_finite() || scale <= 0.0 {
                    return Err(LabError::Invalid(
                        "per-row int8 calibration produced an invalid scale".to_string(),
                    ));
                }
                dequantized.extend(row.iter().map(|value| {
                    let quantized = ((*value - offset) / scale).round().clamp(-127.0, 127.0) as i8;
                    f32::from(quantized) * scale + offset
                }));
            }
        }
    }
    Ok(dequantized)
}

fn quantize_symmetric_renormalized(documents: &TensorSet, group_size: usize) -> Result<Vec<f32>> {
    if documents.sidecar.dtype != DType::F16 {
        return Err(LabError::Invalid(
            "symmetric int8 audit requires f16 primary documents".to_string(),
        ));
    }
    if group_size == 0 {
        return Err(LabError::Invalid(
            "symmetric int8 group size must be nonzero".to_string(),
        ));
    }
    let mut dequantized = Vec::with_capacity(documents.values.len());
    for (row_index, row) in documents
        .values
        .chunks_exact(documents.sidecar.dim)
        .enumerate()
    {
        for (group_index, group) in row.chunks(group_size).enumerate() {
            let maximum_absolute_value = group.iter().copied().map(f32::abs).fold(0.0, f32::max);
            if !maximum_absolute_value.is_finite() || maximum_absolute_value == 0.0 {
                return Err(LabError::Invalid(format!(
                    "symmetric int8 row {row_index} group {group_index} has zero or non-finite \
                     range"
                )));
            }
            let scale = round_f32_through_f16(maximum_absolute_value / 127.0);
            if !scale.is_finite() || scale <= 0.0 {
                return Err(LabError::Invalid(format!(
                    "symmetric int8 row {row_index} group {group_index} has invalid f16 scale"
                )));
            }
            for &value in group {
                let quantized = quantize_ties_away_from_zero(value, scale)?;
                let reconstructed = f32::from(quantized) * scale;
                if !reconstructed.is_finite() {
                    return Err(LabError::Invalid(format!(
                        "symmetric int8 row {row_index} group {group_index} reconstructed a \
                         non-finite coordinate"
                    )));
                }
                dequantized.push(reconstructed);
            }
        }
    }
    renormalize_rows(&mut dequantized, documents.sidecar.dim)?;
    Ok(dequantized)
}

fn quantize_ties_away_from_zero(value: f32, scale: f32) -> Result<i8> {
    if !value.is_finite() || !scale.is_finite() || scale <= 0.0 {
        return Err(LabError::Invalid(
            "symmetric int8 quantization received zero or non-finite input".to_string(),
        ));
    }
    Ok((value / scale).round().clamp(-127.0, 127.0) as i8)
}

fn renormalize_rows(values: &mut [f32], dim: usize) -> Result<()> {
    if dim == 0 || values.len() % dim != 0 {
        return Err(LabError::Invalid(
            "row renormalization received an invalid tensor shape".to_string(),
        ));
    }
    for (row_index, row) in values.chunks_exact_mut(dim).enumerate() {
        let squared_norm = row.iter().try_fold(0.0_f64, |sum, value| {
            if value.is_finite() {
                Ok(sum + f64::from(*value) * f64::from(*value))
            } else {
                Err(LabError::Invalid(format!(
                    "row renormalization found non-finite coordinate in row {row_index}"
                )))
            }
        })?;
        let norm = squared_norm.sqrt();
        if !norm.is_finite() || norm == 0.0 {
            return Err(LabError::Invalid(format!(
                "row renormalization produced zero or non-finite norm for row {row_index}"
            )));
        }
        for value in row {
            *value = (f64::from(*value) / norm) as f32;
            if !value.is_finite() {
                return Err(LabError::Invalid(format!(
                    "row renormalization produced non-finite coordinate in row {row_index}"
                )));
            }
        }
    }
    Ok(())
}

fn round_f32_through_f16(value: f32) -> f32 {
    f16_to_f32(f32_to_f16_bits(value))
}

fn f32_to_f16_bits(value: f32) -> u16 {
    let bits = value.to_bits();
    let sign = (bits >> 16) & 0x8000;
    let exponent = ((bits >> 23) & 0xff) as i32;
    let fraction = bits & 0x007f_ffff;
    if exponent == 0xff {
        let half_fraction = if fraction == 0 {
            0
        } else {
            (fraction >> 13).max(1)
        };
        return (sign | 0x7c00 | half_fraction) as u16;
    }

    let half_exponent = exponent - 127 + 15;
    if half_exponent >= 0x1f {
        return (sign | 0x7c00) as u16;
    }
    if half_exponent <= 0 {
        if half_exponent < -10 {
            return sign as u16;
        }
        let significand = fraction | 0x0080_0000;
        let shift = (14 - half_exponent) as u32;
        let mut half_fraction = significand >> shift;
        let remainder = significand & ((1_u32 << shift) - 1);
        let halfway = 1_u32 << (shift - 1);
        if remainder > halfway || (remainder == halfway && half_fraction & 1 == 1) {
            half_fraction += 1;
        }
        return (sign | half_fraction) as u16;
    }

    let mut half = sign | ((half_exponent as u32) << 10) | (fraction >> 13);
    let remainder = fraction & 0x1fff;
    if remainder > 0x1000 || (remainder == 0x1000 && half & 1 == 1) {
        half += 1;
    }
    half as u16
}

fn compare_int8_truth(reference: &[ExactTruth], int8: &[ExactTruth]) -> (f64, f64) {
    assert_eq!(reference.len(), int8.len());
    let mut top_1_same_rank = 0usize;
    let mut top_10_hits = 0usize;
    for (reference_query, int8_query) in reference.iter().zip(int8) {
        top_1_same_rank +=
            usize::from(reference_query.top_documents[0] == int8_query.top_documents[0]);
        top_10_hits += reference_query
            .top_documents
            .iter()
            .filter(|document| int8_query.top_documents.contains(document))
            .count();
    }
    (
        top_1_same_rank as f64 / reference.len() as f64,
        top_10_hits as f64 / (reference.len() * TRUTH_K) as f64,
    )
}

#[allow(clippy::too_many_arguments)]
fn int8_max_sim_error(
    documents: &TensorSet,
    queries: &TensorSet,
    full_documents: &TensorSet,
    full_queries: &TensorSet,
    f32_identity: &PreparedValues<'_>,
    dequantized_documents: &[f32],
    official_scores: &[OfficialScore],
) -> Result<f64> {
    let document_ids: HashMap<&str, usize> = documents
        .sidecar
        .ids
        .iter()
        .enumerate()
        .map(|(index, id)| (id.as_str(), index))
        .collect();
    let query_ids: HashMap<&str, usize> = queries
        .sidecar
        .ids
        .iter()
        .enumerate()
        .map(|(index, id)| (id.as_str(), index))
        .collect();
    let mut maximum = 0.0_f64;
    for pair in official_scores {
        let query_index = *query_ids.get(pair.query_id.as_str()).ok_or_else(|| {
            LabError::Invalid(format!(
                "int8 pair references unknown query id {:?}",
                pair.query_id
            ))
        })?;
        let document_index = *document_ids.get(pair.document_id.as_str()).ok_or_else(|| {
            LabError::Invalid(format!(
                "int8 pair references unknown document id {:?}",
                pair.document_id
            ))
        })?;
        let reference_query = full_queries.matrix(&f32_identity.queries, query_index)?;
        let reference_document = full_documents.matrix(&f32_identity.documents, document_index)?;
        let int8_query = queries.matrix(&queries.values, query_index)?;
        let int8_document = documents.matrix(dequantized_documents, document_index)?;
        let reference_score = max_sim(&reference_query, &reference_document)?;
        let int8_score = max_sim(&int8_query, &int8_document)?;
        maximum = maximum.max(f64::from((reference_score - int8_score).abs()));
    }
    Ok(maximum)
}

fn int8_row_l2_errors(
    documents: &TensorSet,
    full_documents: &TensorSet,
    dequantized_documents: &[f32],
) -> Result<Vec<f64>> {
    let document_count = documents.sidecar.ids.len().min(100);
    let sampled_rows = documents.sidecar.rows[..document_count]
        .iter()
        .try_fold(0usize, |total, rows| total.checked_add(*rows))
        .ok_or_else(|| LabError::Invalid("int8 sampled row count overflows".to_string()))?;
    let scalar_count = sampled_rows
        .checked_mul(documents.sidecar.dim)
        .ok_or_else(|| LabError::Invalid("int8 sampled scalar count overflows".to_string()))?;
    let mut errors = Vec::with_capacity(sampled_rows);
    for (int8_row, f32_row) in dequantized_documents[..scalar_count]
        .chunks_exact(documents.sidecar.dim)
        .zip(full_documents.values[..scalar_count].chunks_exact(documents.sidecar.dim))
    {
        errors.push(squared_l2(int8_row, f32_row).sqrt());
    }
    if errors.is_empty() {
        return Err(LabError::Invalid(
            "int8 row-error sample is empty".to_string(),
        ));
    }
    errors.sort_unstable_by(f64::total_cmp);
    Ok(errors)
}

fn nearest_rank_f64(ordered: &[f64], percentile: usize) -> f64 {
    let rank = percentile
        .checked_mul(ordered.len())
        .expect("validated int8 row count fits percentile arithmetic")
        .div_ceil(100);
    ordered[rank.saturating_sub(1)]
}

fn exact_frontier_gaps(
    documents: &TensorSet,
    queries: &TensorSet,
    truth: &[ExactTruth],
) -> Vec<ExactFrontierGap> {
    truth
        .iter()
        .enumerate()
        .map(|(query_index, query_truth)| {
            let (rank_10_document_index, rank_10_score) = query_truth.rank_10;
            let (rank_100_document_index, rank_100_score) = query_truth.rank_100;
            ExactFrontierGap {
                query_index,
                query_id: queries.sidecar.ids[query_index].clone(),
                query_rows: queries.sidecar.rows[query_index],
                rank_10_document_index,
                rank_10_document_id: documents.sidecar.ids[rank_10_document_index].clone(),
                rank_10_score,
                rank_100_document_index,
                rank_100_document_id: documents.sidecar.ids[rank_100_document_index].clone(),
                rank_100_score,
                rank_10_to_rank_100_gap: rank_10_score - rank_100_score,
            }
        })
        .collect()
}

fn rank_scores(scores: &mut [(usize, f32)]) {
    scores.sort_unstable_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
}

fn generate_transform(
    input_dimension: usize,
    config: FdeConfig,
    algorithm: Algorithm,
) -> Result<FdeTransform> {
    let input_dimension = u32::try_from(input_dimension)
        .map_err(|_| LabError::Invalid("input dimension exceeds u32".to_string()))?;
    let (version, inner) = match algorithm {
        Algorithm::PaperV1 => (
            FdeAlgorithmVersion::PaperV1,
            InnerProjection::Rademacher {
                d_proj: config.d_proj,
            },
        ),
        Algorithm::ReferenceV1 => (
            FdeAlgorithmVersion::ReferenceV1,
            InnerProjection::AmsSketch {
                d_proj: config.d_proj,
            },
        ),
    };
    Ok(FdeTransform::generate(
        &FdeParams {
            algorithm: version,
            repetitions: config.repetitions,
            simhash_bits: config.simhash_bits,
            input_dimension,
            inner,
            final_projection: FinalProjection::None,
        },
        FDE_SEED,
    )?)
}

fn encode_set(
    tensor: &TensorSet,
    values: &[f32],
    transform: &FdeTransform,
    document: bool,
) -> Result<Vec<f32>> {
    let output_dimension = transform.output_dimension();
    let capacity = tensor
        .sidecar
        .ids
        .len()
        .checked_mul(output_dimension)
        .ok_or_else(|| LabError::Invalid("FDE matrix shape overflows".to_string()))?;
    let mut encoded = Vec::with_capacity(capacity);
    for index in 0..tensor.sidecar.ids.len() {
        let matrix = tensor.matrix(values, index)?;
        let row = if document {
            transform.encode_document(&matrix)?
        } else {
            transform.encode_query(&matrix)?
        };
        encoded.extend(row);
    }
    Ok(encoded)
}

fn evaluate_cell(
    documents: &TensorSet,
    queries: &TensorSet,
    prepared: &PreparedValues<'_>,
    truth: &[ExactTruth],
    config: FdeConfig,
    algorithm: Algorithm,
    centering: Centering,
) -> Result<CellResult> {
    let transform = generate_transform(documents.sidecar.dim, config, algorithm)?;
    let output_dimension = transform.output_dimension();
    let document_fdes = encode_set(documents, &prepared.documents, &transform, true)?;
    let query_fdes = encode_set(queries, &prepared.queries, &transform, false)?;
    let recalls = candidate_recalls(&document_fdes, &query_fdes, output_dimension, truth)?;
    Ok(CellResult {
        config: config.name,
        repetitions: config.repetitions,
        simhash_bits: config.simhash_bits,
        d_proj: config.d_proj,
        algorithm,
        centering,
        output_dimension,
        recall_at_50: recalls[0],
        recall_at_100: recalls[1],
        recall_at_300: recalls[2],
    })
}

fn candidate_recalls(
    document_fdes: &[f32],
    query_fdes: &[f32],
    dim: usize,
    truth: &[ExactTruth],
) -> Result<[f64; 3]> {
    let document_count = document_fdes.len() / dim;
    let query_count = query_fdes.len() / dim;
    let per_query = parallel_indexed_map(query_count, |query_index| {
        let query = &query_fdes[query_index * dim..(query_index + 1) * dim];
        let mut scores = Vec::with_capacity(document_count);
        for (document_index, document) in document_fdes.chunks_exact(dim).enumerate() {
            let score = dot(query, document);
            if !score.is_finite() {
                return Err(LabError::Invalid(format!(
                    "FDE dot overflowed for query {query_index}, document {document_index}"
                )));
            }
            scores.push((document_index, score));
        }
        rank_scores(&mut scores);
        let mut hits = [0usize; 3];
        for (readout, candidate_k) in CANDIDATE_KS.into_iter().enumerate() {
            let candidates = &scores[..candidate_k];
            hits[readout] += truth[query_index]
                .top_documents
                .iter()
                .filter(|truth_index| {
                    candidates
                        .iter()
                        .any(|(candidate_index, _)| candidate_index == *truth_index)
                })
                .count();
        }
        Ok(hits)
    })?;
    let mut hits = [0usize; 3];
    for query_hits in per_query {
        for readout in 0..hits.len() {
            hits[readout] += query_hits[readout];
        }
    }
    let denominator = (truth.len() * TRUTH_K) as f64;
    Ok(hits.map(|hit| hit as f64 / denominator))
}

#[allow(clippy::too_many_arguments)]
fn diagnose_cell(
    documents: &TensorSet,
    queries: &TensorSet,
    exact: &PreparedValues<'_>,
    prepared: &PreparedValues<'_>,
    truth: &[ExactTruth],
    config: FdeConfig,
    algorithm: Algorithm,
    centering: Centering,
) -> Result<DiagnosticCell> {
    let transform = generate_transform(documents.sidecar.dim, config, algorithm)?;
    let output_dimension = transform.output_dimension();
    let transform_checksum_sha256 = transform
        .checksum()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let document_fdes = encode_set(documents, &prepared.documents, &transform, true)?;
    let query_fdes = encode_set(queries, &prepared.queries, &transform, false)?;
    let document_count = documents.sidecar.ids.len();
    let repetition_scale = config.repetitions as f32;

    let per_query = parallel_indexed_map(queries.sidecar.ids.len(), |query_index| {
        let query_fde =
            &query_fdes[query_index * output_dimension..(query_index + 1) * output_dimension];
        let exact_query = queries.matrix(&exact.queries, query_index)?;
        let transformed_query = queries.matrix(&prepared.queries, query_index)?;
        let mut scores = Vec::with_capacity(document_count);
        for (document_index, document_fde) in
            document_fdes.chunks_exact(output_dimension).enumerate()
        {
            let score = dot(query_fde, document_fde);
            if !score.is_finite() {
                return Err(LabError::Invalid(format!(
                    "diagnostic FDE dot overflowed for query {query_index}, document \
                     {document_index}"
                )));
            }
            scores.push((document_index, score));
        }
        rank_scores(&mut scores);

        let mut rank_by_document = vec![0usize; document_count];
        let mut score_by_document = vec![0.0_f32; document_count];
        for (rank, &(document_index, score)) in scores.iter().enumerate() {
            rank_by_document[document_index] = rank + 1;
            score_by_document[document_index] = score;
        }

        let mut hits = [0usize; 3];
        for (readout, candidate_k) in CANDIDATE_KS.into_iter().enumerate() {
            hits[readout] = truth[query_index]
                .top_documents
                .iter()
                .filter(|&&document_index| rank_by_document[document_index] <= candidate_k)
                .count();
        }

        let mut gold_ranks = Vec::with_capacity(TRUTH_K);
        for (exact_rank, &document_index) in truth[query_index].top_documents.iter().enumerate() {
            let exact_document = documents.matrix(&exact.documents, document_index)?;
            let transformed_document = documents.matrix(&prepared.documents, document_index)?;
            let exact_score = max_sim(&exact_query, &exact_document)?;
            let transformed_exact_score = max_sim(&transformed_query, &transformed_document)?;
            gold_ranks.push(GoldRankDiagnostic {
                query_index,
                query_id: queries.sidecar.ids[query_index].clone(),
                document_index,
                document_id: documents.sidecar.ids[document_index].clone(),
                exact_rank: exact_rank + 1,
                exact_score,
                transformed_exact_score,
                fde_rank: rank_by_document[document_index],
                fde_inner_product: score_by_document[document_index],
                fde_score_per_repetition: score_by_document[document_index] / repetition_scale,
                document_rows: documents.sidecar.rows[document_index],
                query_rows: queries.sidecar.rows[query_index],
            });
        }

        let mut sampled_documents = Vec::with_capacity(6);
        for fde_rank in [1usize, 10, 100, 300, 1_000] {
            if let Some(&(document_index, _)) = scores.get(fde_rank - 1) {
                sampled_documents.push(document_index);
            }
        }
        sampled_documents.push(truth[query_index].top_documents[0]);
        sampled_documents.sort_unstable();
        sampled_documents.dedup();

        let mut score_pairs = Vec::with_capacity(sampled_documents.len());
        for document_index in sampled_documents {
            let exact_document = documents.matrix(&exact.documents, document_index)?;
            let transformed_document = documents.matrix(&prepared.documents, document_index)?;
            let exact_score = max_sim(&exact_query, &exact_document)?;
            let transformed_exact_score = max_sim(&transformed_query, &transformed_document)?;
            score_pairs.push(ScorePairDiagnostic {
                query_index,
                query_id: queries.sidecar.ids[query_index].clone(),
                document_index,
                document_id: documents.sidecar.ids[document_index].clone(),
                fde_rank: rank_by_document[document_index],
                exact_score,
                transformed_exact_score,
                fde_inner_product: score_by_document[document_index],
                fde_score_per_repetition: score_by_document[document_index] / repetition_scale,
                document_rows: documents.sidecar.rows[document_index],
                query_rows: queries.sidecar.rows[query_index],
            });
        }

        Ok(PerQueryDiagnostic {
            hits,
            gold_ranks,
            score_pairs,
        })
    })?;

    let mut hits = [0usize; 3];
    let mut gold_ranks = Vec::with_capacity(queries.sidecar.ids.len() * TRUTH_K);
    let mut score_pairs = Vec::with_capacity(queries.sidecar.ids.len() * 6);
    for query in per_query {
        for (total, query_hits) in hits.iter_mut().zip(query.hits) {
            *total += query_hits;
        }
        gold_ranks.extend(query.gold_ranks);
        score_pairs.extend(query.score_pairs);
    }
    let denominator = (truth.len() * TRUTH_K) as f64;
    let recalls = hits.map(|hit| hit as f64 / denominator);
    Ok(DiagnosticCell {
        config: config.name,
        repetitions: config.repetitions,
        simhash_bits: config.simhash_bits,
        d_proj: config.d_proj,
        algorithm,
        centering,
        output_dimension,
        transform_checksum_sha256,
        recall_at_50: recalls[0],
        recall_at_100: recalls[1],
        recall_at_300: recalls[2],
        gold_ranks,
        score_pairs,
    })
}

fn dot(left: &[f32], right: &[f32]) -> f32 {
    left.iter().zip(right).map(|(a, b)| a * b).sum()
}

fn evaluate_fixed_grid(
    job: &Job,
    documents: &TensorSet,
    queries: &TensorSet,
    identity: &PreparedValues<'_>,
    identity_truth: &[ExactTruth],
    parity_passed: bool,
) -> Result<Evaluation> {
    let mut cells = Vec::new();

    let chosen_algorithm = match job.lane {
        Lane::Text => {
            for algorithm in [Algorithm::PaperV1, Algorithm::ReferenceV1] {
                for config in [CONFIG_A, CONFIG_B] {
                    cells.push(evaluate_cell(
                        documents,
                        queries,
                        identity,
                        identity_truth,
                        config,
                        algorithm,
                        Centering::Identity,
                    )?);
                }
            }
            best_cell(&cells).algorithm
        }
        Lane::Visual => job
            .chosen_algorithm
            .ok_or_else(|| LabError::Invalid("visual algorithm is missing".to_string()))?,
    };

    let mut visual_diagnostics = None;
    if job.lane == Lane::Visual {
        let diagnostics = [
            CONFIG_A,
            CONFIG_E_REPS_DIAGNOSTIC,
            CONFIG_F_VISUAL_FINE,
            CONFIG_G_VISUAL_COARSE,
        ]
        .into_iter()
        .map(|config| {
            diagnose_cell(
                documents,
                queries,
                identity,
                identity,
                identity_truth,
                config,
                chosen_algorithm,
                Centering::Identity,
            )
        })
        .collect::<Result<Vec<_>>>()?;
        for config in [
            CONFIG_A,
            CONFIG_B,
            CONFIG_E_REPS_DIAGNOSTIC,
            CONFIG_F_VISUAL_FINE,
            CONFIG_G_VISUAL_COARSE,
        ] {
            if config.name == CONFIG_B.name {
                cells.push(evaluate_cell(
                    documents,
                    queries,
                    identity,
                    identity_truth,
                    config,
                    chosen_algorithm,
                    Centering::Identity,
                )?);
            } else {
                let diagnostic = diagnostics
                    .iter()
                    .find(|diagnostic| diagnostic.config == config.name)
                    .expect("visual diagnostic config is present");
                cells.push(diagnostic.cell_result());
            }
        }
        visual_diagnostics = Some(diagnostics);
    }

    let centerings: Vec<Centering> = match job.lane {
        Lane::Text => vec![
            Centering::SubtractGlobalMean,
            Centering::SubtractGlobalMeanRenormalize,
        ],
        Lane::Visual => job
            .chosen_centering
            .filter(|centering| *centering != Centering::Identity)
            .into_iter()
            .collect(),
    };
    let mut geometry = vec![measure_geometry(
        documents,
        queries,
        identity,
        Centering::Identity,
    )?];
    for centering in centerings {
        let prepared = prepare_values(documents, queries, centering)?;
        cells.push(evaluate_cell(
            documents,
            queries,
            &prepared,
            identity_truth,
            CONFIG_A,
            chosen_algorithm,
            centering,
        )?);
        geometry.push(measure_geometry(documents, queries, &prepared, centering)?);
    }

    let threshold = match job.lane {
        Lane::Text => 0.95,
        Lane::Visual => 0.90,
    };
    let mut winner = choose_winner(&cells, job.lane, threshold);
    let (diagnostic_probes, diagnostics, exact_frontier_gaps) = if job.lane == Lane::Text {
        let prepared = prepare_values(documents, queries, winner.centering)?;
        (
            Some(vec![evaluate_cell(
                documents,
                queries,
                &prepared,
                identity_truth,
                CONFIG_D_DPROJ_DIAGNOSTIC,
                winner.algorithm,
                winner.centering,
            )?]),
            Some(vec![
                diagnose_cell(
                    documents,
                    queries,
                    identity,
                    &prepared,
                    identity_truth,
                    CONFIG_A,
                    winner.algorithm,
                    winner.centering,
                )?,
                diagnose_cell(
                    documents,
                    queries,
                    identity,
                    &prepared,
                    identity_truth,
                    CONFIG_C_DIAGNOSTIC,
                    winner.algorithm,
                    winner.centering,
                )?,
                diagnose_cell(
                    documents,
                    queries,
                    identity,
                    &prepared,
                    identity_truth,
                    CONFIG_E_REPS_DIAGNOSTIC,
                    winner.algorithm,
                    winner.centering,
                )?,
            ]),
            Some(exact_frontier_gaps(documents, queries, identity_truth)),
        )
    } else {
        (None, None, None)
    };
    let pooling_probes = if job.lane == Lane::Visual {
        let selected = cells
            .iter()
            .filter(|cell| {
                cell.centering == Centering::Identity
                    && cell.output_dimension
                        == CONFIG_A.repetitions as usize
                            * (1usize << CONFIG_A.simhash_bits)
                            * CONFIG_A.d_proj as usize
            })
            .max_by(|left, right| left.recall_at_300.total_cmp(&right.recall_at_300))
            .expect("visual fixed-D identity cells are non-empty");
        let config = config_by_name(selected.config);
        let original_mean_rows = documents.total_rows as f64 / documents.sidecar.rows.len() as f64;
        let mut probes = Vec::with_capacity(2);
        for factor in [VISUAL_DOCUMENT_POOLING_FACTOR, 4] {
            let pooled = mean_pool_documents(documents, &identity.documents, factor)?;
            let pooled_prepared = PreparedValues {
                documents: Cow::Borrowed(&pooled.values),
                queries: Cow::Borrowed(identity.queries.as_ref()),
            };
            let result = evaluate_cell(
                &pooled,
                queries,
                &pooled_prepared,
                identity_truth,
                config,
                chosen_algorithm,
                Centering::Identity,
            )?;
            probes.push(PoolingProbe {
                factor,
                original_mean_rows,
                pooled_mean_rows: pooled.total_rows as f64 / pooled.sidecar.rows.len() as f64,
                result,
            });
        }
        Some(probes)
    } else {
        None
    };
    let recall_gate_passed = match job.lane {
        Lane::Text => {
            let selected = diagnostics
                .as_ref()
                .and_then(|cells| {
                    cells
                        .iter()
                        .find(|cell| cell.config == CONFIG_E_REPS_DIAGNOSTIC.name)
                })
                .expect("text diagnostics include config E");
            let candidate_k = candidate_k_for_recall(selected, threshold);
            let bounded_k = candidate_k.min(TEXT_CANDIDATE_K_MAX);
            let recall = diagnostic_recall_at(selected, bounded_k);
            winner = Winner {
                config: selected.config,
                algorithm: selected.algorithm,
                centering: selected.centering,
                document_pooling_factor: 1,
                output_dimension: selected.output_dimension,
                candidate_k: bounded_k,
                recall,
            };
            candidate_k <= TEXT_CANDIDATE_K_MAX && recall >= threshold
        }
        Lane::Visual => {
            let unpooled_passed = cells.iter().any(|cell| cell.recall_at_300 >= threshold);
            if unpooled_passed {
                true
            } else {
                let approved = pooling_probes
                    .as_ref()
                    .and_then(|probes| {
                        probes
                            .iter()
                            .find(|probe| probe.factor == VISUAL_DOCUMENT_POOLING_FACTOR)
                    })
                    .expect("visual diagnostics include the approved 2x pooling probe");
                if approved.result.recall_at_300 >= threshold {
                    winner = Winner {
                        config: approved.result.config,
                        algorithm: approved.result.algorithm,
                        centering: approved.result.centering,
                        document_pooling_factor: approved.factor,
                        output_dimension: approved.result.output_dimension,
                        candidate_k: 300,
                        recall: approved.result.recall_at_300,
                    };
                    true
                } else {
                    false
                }
            }
        }
    };
    let gate_passed = parity_passed && recall_gate_passed;
    let routing = if job.lane == Lane::Text && gate_passed {
        let config = config_by_name(winner.config);
        let prepared = prepare_values(documents, queries, winner.centering)?;
        Some(evaluate_routing(
            documents,
            queries,
            &prepared,
            config,
            winner.algorithm,
        )?)
    } else {
        None
    };

    Ok(Evaluation {
        geometry,
        cells,
        winner,
        gate_passed,
        routing,
        diagnostic_probes,
        diagnostics,
        visual_diagnostics,
        pooling_probes,
        exact_frontier_gaps,
    })
}

fn best_cell(cells: &[CellResult]) -> &CellResult {
    cells
        .iter()
        .max_by(|left, right| compare_cells(left, right))
        .expect("the fixed grid is non-empty")
}

fn compare_cells(left: &CellResult, right: &CellResult) -> Ordering {
    left.recall_at_100
        .total_cmp(&right.recall_at_100)
        .then_with(|| left.recall_at_300.total_cmp(&right.recall_at_300))
        .then_with(|| left.recall_at_50.total_cmp(&right.recall_at_50))
        .then_with(|| right.output_dimension.cmp(&left.output_dimension))
        .then_with(|| algorithm_tie_rank(left.algorithm).cmp(&algorithm_tie_rank(right.algorithm)))
}

const fn algorithm_tie_rank(algorithm: Algorithm) -> u8 {
    match algorithm {
        Algorithm::PaperV1 => 1,
        Algorithm::ReferenceV1 => 0,
    }
}

fn choose_winner(cells: &[CellResult], lane: Lane, threshold: f64) -> Winner {
    let allowed_ks: &[usize] = match lane {
        Lane::Text => &CANDIDATE_KS[..2],
        Lane::Visual => &CANDIDATE_KS,
    };
    let mut passing: Vec<(&CellResult, usize)> = cells
        .iter()
        .filter_map(|cell| {
            allowed_ks
                .iter()
                .copied()
                .find(|candidate_k| cell.recall_at(*candidate_k) >= threshold)
                .map(|candidate_k| (cell, candidate_k))
        })
        .collect();
    passing.sort_by(|(left_cell, left_k), (right_cell, right_k)| {
        left_k
            .cmp(right_k)
            .then_with(|| left_cell.output_dimension.cmp(&right_cell.output_dimension))
            .then_with(|| {
                right_cell
                    .recall_at(*right_k)
                    .total_cmp(&left_cell.recall_at(*left_k))
            })
    });
    let (cell, candidate_k) = passing
        .first()
        .copied()
        .unwrap_or_else(|| (best_cell(cells), *allowed_ks.last().expect("non-empty Ks")));
    Winner {
        config: cell.config,
        algorithm: cell.algorithm,
        centering: cell.centering,
        document_pooling_factor: 1,
        output_dimension: cell.output_dimension,
        candidate_k,
        recall: cell.recall_at(candidate_k),
    }
}

fn config_by_name(name: &str) -> FdeConfig {
    match name {
        "A" => CONFIG_A,
        "B" => CONFIG_B,
        "E" => CONFIG_E_REPS_DIAGNOSTIC,
        "F-visual-k6" => CONFIG_F_VISUAL_FINE,
        "G-visual-k3" => CONFIG_G_VISUAL_COARSE,
        _ => unreachable!("only fixed Phase 2 configs are emitted"),
    }
}

fn candidate_k_for_recall(cell: &DiagnosticCell, threshold: f64) -> usize {
    let mut ranks: Vec<usize> = cell.gold_ranks.iter().map(|row| row.fde_rank).collect();
    ranks.sort_unstable();
    let index = ((threshold * ranks.len() as f64).ceil() as usize)
        .saturating_sub(1)
        .min(ranks.len() - 1);
    ranks[index]
}

fn diagnostic_recall_at(cell: &DiagnosticCell, candidate_k: usize) -> f64 {
    cell.gold_ranks
        .iter()
        .filter(|row| row.fde_rank <= candidate_k)
        .count() as f64
        / cell.gold_ranks.len() as f64
}

fn measure_geometry(
    documents: &TensorSet,
    queries: &TensorSet,
    prepared: &PreparedValues<'_>,
    centering: Centering,
) -> Result<GeometryReport> {
    let document_mean_norm = mean_norm(&prepared.documents, documents.sidecar.dim)?;
    let query_mean_norm = mean_norm(&prepared.queries, queries.sidecar.dim)?;
    let simhash =
        measure_simhash_geometry(documents, queries, &prepared.documents, &prepared.queries)?;
    let sampled_document_rows = documents.total_rows.min(GEOMETRY_SAMPLE_ROWS);
    let mut sampled = Vec::with_capacity(sampled_document_rows);
    for sample in 0..sampled_document_rows {
        let row = sample * documents.total_rows / sampled_document_rows;
        let start = row * documents.sidecar.dim;
        sampled.push(&prepared.documents[start..start + documents.sidecar.dim]);
    }
    let mut cosine_sum = 0.0_f64;
    let mut pairs = 0usize;
    for left in 0..sampled.len() {
        let left_norm = row_norm(sampled[left]);
        if left_norm == 0.0 {
            return Err(LabError::Invalid(
                "document geometry contains a zero row".to_string(),
            ));
        }
        for right in left + 1..sampled.len() {
            let right_norm = row_norm(sampled[right]);
            if right_norm == 0.0 {
                return Err(LabError::Invalid(
                    "document geometry contains a zero row".to_string(),
                ));
            }
            cosine_sum += f64::from(dot(sampled[left], sampled[right])) / (left_norm * right_norm);
            pairs += 1;
        }
    }
    Ok(GeometryReport {
        centering,
        document_mean_norm,
        query_mean_norm,
        mean_pairwise_document_cosine: if pairs == 0 {
            0.0
        } else {
            cosine_sum / pairs as f64
        },
        sampled_document_rows,
        simhash_sampled_document_rows: simhash.sampled_document_rows,
        simhash_sampled_query_rows: simhash.sampled_query_rows,
        simhash_sampled_documents: simhash.sampled_documents,
        document_simhash_bucket_occupancy_rate: simhash.document_bucket_occupancy_rate,
        document_simhash_bucket_entropy_bits: simhash.document_bucket_entropy_bits,
        query_simhash_bucket_occupancy_rate: simhash.query_bucket_occupancy_rate,
        query_simhash_bucket_entropy_bits: simhash.query_bucket_entropy_bits,
        document_empty_bucket_fill_rate: simhash.document_empty_bucket_fill_rate,
    })
}

fn measure_simhash_geometry(
    documents: &TensorSet,
    queries: &TensorSet,
    document_values: &[f32],
    query_values: &[f32],
) -> Result<SimhashGeometry> {
    let transform = generate_transform(documents.sidecar.dim, CONFIG_A, Algorithm::PaperV1)?;
    let repetitions = usize::try_from(CONFIG_A.repetitions)
        .map_err(|_| LabError::Invalid("FDE repetition count exceeds usize".to_string()))?;
    let bucket_count = 1usize
        .checked_shl(CONFIG_A.simhash_bits)
        .ok_or_else(|| LabError::Invalid("FDE bucket count overflows usize".to_string()))?;
    let projected_dimension = usize::try_from(CONFIG_A.d_proj)
        .map_err(|_| LabError::Invalid("FDE projected dimension exceeds usize".to_string()))?;
    let repetition_dimension = bucket_count
        .checked_mul(projected_dimension)
        .ok_or_else(|| LabError::Invalid("FDE repetition dimension overflows".to_string()))?;
    let expected_dimension = repetitions
        .checked_mul(repetition_dimension)
        .ok_or_else(|| LabError::Invalid("FDE output dimension overflows".to_string()))?;
    if transform.output_dimension() != expected_dimension {
        return Err(LabError::Invalid(format!(
            "config-A transform dimension {} differs from expected {expected_dimension}",
            transform.output_dimension()
        )));
    }

    let (sampled_document_rows, document_bucket_occupancy_rate, document_bucket_entropy_bits) =
        simhash_row_distribution(
            "document",
            documents,
            document_values,
            &transform,
            repetitions,
            bucket_count,
            projected_dimension,
            repetition_dimension,
        )?;
    let (sampled_query_rows, query_bucket_occupancy_rate, query_bucket_entropy_bits) =
        simhash_row_distribution(
            "query",
            queries,
            query_values,
            &transform,
            repetitions,
            bucket_count,
            projected_dimension,
            repetition_dimension,
        )?;

    let sampled_documents = documents.sidecar.ids.len().min(SIMHASH_SAMPLE_DOCUMENTS);
    let mut empty_buckets = 0usize;
    for sample in 0..sampled_documents {
        let document_index = sample * documents.sidecar.ids.len() / sampled_documents;
        let matrix = documents.matrix(document_values, document_index)?;
        let encoded = transform.encode_query(&matrix)?;
        for repetition in 0..repetitions {
            let repetition_start = repetition * repetition_dimension;
            for bucket in 0..bucket_count {
                let block_start = repetition_start + bucket * projected_dimension;
                let block = &encoded[block_start..block_start + projected_dimension];
                empty_buckets += usize::from(block.iter().all(|value| *value == 0.0));
            }
        }
    }
    let document_bucket_slots = sampled_documents
        .checked_mul(repetitions)
        .and_then(|count| count.checked_mul(bucket_count))
        .ok_or_else(|| LabError::Invalid("sampled document bucket count overflows".to_string()))?;

    Ok(SimhashGeometry {
        sampled_document_rows,
        sampled_query_rows,
        sampled_documents,
        document_bucket_occupancy_rate,
        document_bucket_entropy_bits,
        query_bucket_occupancy_rate,
        query_bucket_entropy_bits,
        document_empty_bucket_fill_rate: empty_buckets as f64 / document_bucket_slots as f64,
    })
}

#[allow(clippy::too_many_arguments)]
fn simhash_row_distribution(
    label: &str,
    tensor: &TensorSet,
    values: &[f32],
    transform: &FdeTransform,
    repetitions: usize,
    bucket_count: usize,
    projected_dimension: usize,
    repetition_dimension: usize,
) -> Result<(usize, f64, f64)> {
    let sampled_rows = tensor.total_rows.min(SIMHASH_SAMPLE_ROWS);
    let mut bucket_hits = vec![0usize; repetitions * bucket_count];
    for sample in 0..sampled_rows {
        let row_index = sample * tensor.total_rows / sampled_rows;
        let start = row_index * tensor.sidecar.dim;
        let matrix = MultiVectorMatrixRef::new(
            &values[start..start + tensor.sidecar.dim],
            1,
            tensor.sidecar.dim,
            1,
        )?;
        let encoded = transform.encode_query(&matrix)?;
        for repetition in 0..repetitions {
            let repetition_start = repetition * repetition_dimension;
            let mut occupied_bucket = None;
            for bucket in 0..bucket_count {
                let block_start = repetition_start + bucket * projected_dimension;
                let block = &encoded[block_start..block_start + projected_dimension];
                if block.iter().any(|value| *value != 0.0)
                    && occupied_bucket.replace(bucket).is_some()
                {
                    return Err(LabError::Invalid(format!(
                        "single {label} row populated multiple config-A buckets in \
                         repetition {repetition}"
                    )));
                }
            }
            let bucket = occupied_bucket.ok_or_else(|| {
                LabError::Invalid(format!(
                    "single {label} row populated no config-A bucket in repetition {repetition}"
                ))
            })?;
            bucket_hits[repetition * bucket_count + bucket] += 1;
        }
    }

    let occupied_slots = bucket_hits.iter().filter(|&&count| count > 0).count();
    let bucket_occupancy_rate = occupied_slots as f64 / bucket_hits.len() as f64;
    let mut entropy_sum = 0.0_f64;
    for repetition in 0..repetitions {
        let counts = &bucket_hits[repetition * bucket_count..(repetition + 1) * bucket_count];
        let mut entropy = 0.0_f64;
        for &count in counts {
            if count > 0 {
                let probability = count as f64 / sampled_rows as f64;
                entropy -= probability * probability.log2();
            }
        }
        entropy_sum += entropy;
    }
    Ok((
        sampled_rows,
        bucket_occupancy_rate,
        entropy_sum / repetitions as f64,
    ))
}

fn mean_norm(values: &[f32], dim: usize) -> Result<f64> {
    let mut total = 0.0_f64;
    let mut rows = 0usize;
    for row in values.chunks_exact(dim) {
        let norm = row_norm(row);
        if !norm.is_finite() {
            return Err(LabError::Invalid("geometry norm is not finite".to_string()));
        }
        total += norm;
        rows += 1;
    }
    Ok(total / rows as f64)
}

fn row_norm(row: &[f32]) -> f64 {
    row.iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt()
}

fn squared_l2(left: &[f32], right: &[f32]) -> f64 {
    left.iter()
        .zip(right)
        .map(|(a, b)| {
            let delta = f64::from(*a) - f64::from(*b);
            delta * delta
        })
        .sum()
}

fn evaluate_routing(
    documents: &TensorSet,
    queries: &TensorSet,
    prepared: &PreparedValues<'_>,
    config: FdeConfig,
    algorithm: Algorithm,
) -> Result<RoutingReport> {
    let transform = generate_transform(documents.sidecar.dim, config, algorithm)?;
    let dim = transform.output_dimension();
    let document_fdes = encode_set(documents, &prepared.documents, &transform, true)?;
    let query_fdes = encode_set(queries, &prepared.queries, &transform, false)?;
    let indexing = IndexingConfig::default();
    let nlist = indexing.effective_num_centroids(documents.sidecar.ids.len());
    if nlist < ROUTING_NPROBES[1] {
        return Err(LabError::Invalid(format!(
            "routing nlist {nlist} is smaller than required nprobe {}",
            ROUTING_NPROBES[1]
        )));
    }
    let document_refs: Vec<&[f32]> = document_fdes.chunks_exact(dim).collect();
    let centroids = train_kmeans(
        &document_refs,
        dim,
        nlist,
        indexing.kmeans_max_iterations,
        indexing.kmeans_convergence_epsilon,
    )?;
    let assignments: Vec<usize> = document_refs
        .iter()
        .map(|document| nearest_centroid(document, &centroids))
        .collect();

    let mut dot_hits = [0usize; 2];
    let mut l2_hits = [0usize; 2];
    for query in query_fdes.chunks_exact(dim) {
        let mut document_scores: Vec<(usize, f32)> = document_refs
            .iter()
            .enumerate()
            .map(|(index, document)| (index, dot(query, document)))
            .collect();
        rank_scores(&mut document_scores);
        let truth: Vec<usize> = document_scores
            .into_iter()
            .take(ROUTING_TRUTH_K)
            .map(|(index, _)| index)
            .collect();

        let mut dot_centroids: Vec<(usize, f32)> = centroids
            .iter()
            .enumerate()
            .map(|(index, centroid)| (index, dot(query, centroid)))
            .collect();
        rank_scores(&mut dot_centroids);
        let mut l2_centroids: Vec<(usize, f64)> = centroids
            .iter()
            .enumerate()
            .map(|(index, centroid)| (index, squared_l2(query, centroid)))
            .collect();
        l2_centroids.sort_unstable_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| left.0.cmp(&right.0))
        });

        for (readout, nprobe) in ROUTING_NPROBES.into_iter().enumerate() {
            let dot_clusters: HashSet<usize> = dot_centroids[..nprobe]
                .iter()
                .map(|(index, _)| *index)
                .collect();
            let l2_clusters: HashSet<usize> = l2_centroids[..nprobe]
                .iter()
                .map(|(index, _)| *index)
                .collect();
            for &document_index in &truth {
                dot_hits[readout] +=
                    usize::from(dot_clusters.contains(&assignments[document_index]));
                l2_hits[readout] += usize::from(l2_clusters.contains(&assignments[document_index]));
            }
        }
    }
    let denominator = (queries.sidecar.ids.len() * ROUTING_TRUTH_K) as f64;
    let mut readouts = Vec::with_capacity(4);
    for (readout, nprobe) in ROUTING_NPROBES.into_iter().enumerate() {
        readouts.push(RoutingReadout {
            metric: "dot",
            nprobe,
            recall_at_100: dot_hits[readout] as f64 / denominator,
        });
        readouts.push(RoutingReadout {
            metric: "negative_l2",
            nprobe,
            recall_at_100: l2_hits[readout] as f64 / denominator,
        });
    }
    Ok(RoutingReport {
        nlist,
        fde_dimension: dim,
        readouts,
    })
}

fn nearest_centroid(vector: &[f32], centroids: &[Vec<f32>]) -> usize {
    centroids
        .iter()
        .enumerate()
        .min_by(|(left_index, left), (right_index, right)| {
            squared_l2(vector, left)
                .total_cmp(&squared_l2(vector, right))
                .then_with(|| left_index.cmp(right_index))
        })
        .map(|(index, _)| index)
        .expect("production k-means returns at least one centroid")
}

fn parallel_indexed_map<T, F>(count: usize, operation: F) -> Result<Vec<T>>
where
    T: Send,
    F: Fn(usize) -> Result<T> + Sync,
{
    let worker_count = std::thread::available_parallelism()
        .map_or(1, usize::from)
        .min(count.max(1));
    let next = AtomicUsize::new(0);
    let results: Mutex<Vec<Option<Result<T>>>> = Mutex::new((0..count).map(|_| None).collect());
    std::thread::scope(|scope| {
        for _ in 0..worker_count {
            let next = &next;
            let results = &results;
            let operation = &operation;
            scope.spawn(move || loop {
                let index = next.fetch_add(1, AtomicOrdering::Relaxed);
                if index >= count {
                    break;
                }
                let value = operation(index);
                results.lock().expect("result mutex poisoned")[index] = Some(value);
            });
        }
    });
    results
        .into_inner()
        .map_err(|_| LabError::Invalid("parallel result mutex was poisoned".to_string()))?
        .into_iter()
        .enumerate()
        .map(|(index, result)| {
            result.ok_or_else(|| {
                LabError::Invalid(format!("parallel worker omitted result {index}"))
            })?
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn precision_ranking_comparison_separates_set_order_rank_and_recovery() {
        let reference = [
            truth([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            truth([10, 11, 12, 13, 14, 15, 16, 17, 18, 19]),
            truth([20, 21, 22, 23, 24, 25, 26, 27, 28, 29]),
        ];
        let candidate = [
            truth([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            truth([11, 10, 12, 13, 14, 15, 16, 17, 18, 19]),
            truth([20, 21, 22, 23, 24, 25, 26, 27, 28, 99]),
        ];

        let report = compare_precision_ranking_candidate(
            PrecisionRankingCandidate::F16,
            &reference,
            &candidate,
            CandidatePayload {
                coordinate_bytes_total: 0,
                metadata_bytes_per_row: 0,
                mean_payload_bytes_per_unit: 0.0,
                saving_fraction_vs_f16: 0.0,
            },
            RetrievalDecodeCost {
                basis: "analytic_exact_counts",
                scope: "payload_to_f32_before_maxsim",
                coordinate_conversions_total: 0,
                scale_values_read_total: 0,
                offset_values_read_total: 0,
                dequantize_multiplications_total: 0,
                dequantize_additions_total: 0,
                renormalized_rows_total: 0,
                norm_accumulations_total: 0,
                square_roots_total: 0,
                normalization_divisions_total: 0,
                maxsim_scoring_included: false,
            },
        );
        assert_eq!(report.top_10_set_exactly_equal_query_fraction, 2.0 / 3.0);
        assert_eq!(report.f32_top_10_recovered_in_candidate_top_10, 29.0 / 30.0);
        assert_eq!(
            report.ordered_top_10_exactly_equal_query_fraction,
            1.0 / 3.0
        );
        assert_eq!(
            report
                .per_rank_same_document_fractions
                .iter()
                .map(|rank| (rank.rank, rank.fraction))
                .collect::<Vec<_>>(),
            vec![
                (1, 2.0 / 3.0),
                (2, 2.0 / 3.0),
                (3, 1.0),
                (4, 1.0),
                (5, 1.0),
                (6, 1.0),
                (7, 1.0),
                (8, 1.0),
                (9, 1.0),
                (10, 2.0 / 3.0),
            ]
        );
        assert_eq!(report.f32_top_1_same_document_fraction, 2.0 / 3.0);
        assert_eq!(report.f32_top_1_in_candidate_top_10_fraction, 1.0);
    }

    #[test]
    fn symmetric_quantization_is_deterministic_renormalized_and_rounds_scales_to_f16() {
        let documents = tensor_set(
            vec![1.0, 0.5, -0.25, -0.75, 0.2, -0.4, 0.8, -1.0],
            vec![1, 1],
            4,
        );
        let first = quantize_symmetric_renormalized(&documents, 2).expect("quantize");
        let second = quantize_symmetric_renormalized(&documents, 2).expect("quantize");
        assert_eq!(first, second);
        for row in first.chunks_exact(4) {
            let norm = row
                .iter()
                .map(|value| f64::from(*value) * f64::from(*value))
                .sum::<f64>()
                .sqrt();
            assert!((norm - 1.0).abs() < 1.0e-6);
        }
        assert_eq!(round_f32_through_f16(0.1), f16_to_f32(0x2e66));
        assert_eq!(
            quantize_ties_away_from_zero(0.5, 1.0).expect("positive tie"),
            1
        );
        assert_eq!(
            quantize_ties_away_from_zero(-0.5, 1.0).expect("negative tie"),
            -1
        );
    }

    #[test]
    fn audit_quantizers_fail_loud_on_zero_and_non_finite_rows() {
        let zero_group = tensor_set(vec![0.0, 0.0, 1.0, -1.0], vec![1], 4);
        assert!(matches!(
            quantize_symmetric_renormalized(&zero_group, 2),
            Err(LabError::Invalid(_))
        ));
        let non_finite = tensor_set(vec![f32::NAN, 1.0], vec![1], 2);
        assert!(matches!(
            quantize_symmetric_renormalized(&non_finite, 2),
            Err(LabError::Invalid(_))
        ));

        let mut zero_row = [0.0_f32, 0.0];
        assert!(matches!(
            renormalize_rows(&mut zero_row, 2),
            Err(LabError::Invalid(_))
        ));
        let mut non_finite_row = [f32::INFINITY, 1.0];
        assert!(matches!(
            renormalize_rows(&mut non_finite_row, 2),
            Err(LabError::Invalid(_))
        ));
    }

    #[test]
    fn payload_accounting_includes_exact_coordinate_and_row_metadata_bytes() {
        let documents = tensor_set(vec![1.0; 8], vec![1, 1], 4);
        let f16 = candidate_payload(&documents, 2, 0).expect("f16 payload");
        assert_eq!(f16.coordinate_bytes_total, 16);
        assert_eq!(f16.metadata_bytes_per_row, 0);
        assert_eq!(f16.mean_payload_bytes_per_unit, 8.0);
        assert_eq!(f16.saving_fraction_vs_f16, 0.0);

        let int8_with_f16_scale = candidate_payload(&documents, 1, 2).expect("int8 payload");
        assert_eq!(int8_with_f16_scale.coordinate_bytes_total, 8);
        assert_eq!(int8_with_f16_scale.metadata_bytes_per_row, 2);
        assert_eq!(int8_with_f16_scale.mean_payload_bytes_per_unit, 6.0);
        assert_eq!(int8_with_f16_scale.saving_fraction_vs_f16, 0.25);
    }

    #[test]
    fn retrieval_decode_costs_are_exact_analytic_counts() {
        let documents = tensor_set(vec![1.0; 128], vec![1, 1], 64);
        let f16 =
            precision_ranking_candidate_decode_cost(&documents, PrecisionRankingCandidate::F16)
                .expect("f16 cost");
        assert_eq!(f16.coordinate_conversions_total, 128);
        assert_eq!(f16.dequantize_multiplications_total, 0);
        assert!(!f16.maxsim_scoring_included);

        let affine = precision_ranking_candidate_decode_cost(
            &documents,
            PrecisionRankingCandidate::Int8PerRowCalibratedRenormalized,
        )
        .expect("affine cost");
        assert_eq!(affine.scale_values_read_total, 2);
        assert_eq!(affine.offset_values_read_total, 2);
        assert_eq!(affine.dequantize_multiplications_total, 128);
        assert_eq!(affine.dequantize_additions_total, 128);
        assert_eq!(affine.renormalized_rows_total, 2);
        assert_eq!(affine.norm_accumulations_total, 128);
        assert_eq!(affine.square_roots_total, 2);
        assert_eq!(affine.normalization_divisions_total, 128);

        let groupwise_32 = precision_ranking_candidate_decode_cost(
            &documents,
            PrecisionRankingCandidate::Int8Groupwise32SymmetricRenormalized,
        )
        .expect("groupwise-32 cost");
        assert_eq!(groupwise_32.scale_values_read_total, 4);
        let groupwise_16 = precision_ranking_candidate_decode_cost(
            &documents,
            PrecisionRankingCandidate::Int8Groupwise16SymmetricRenormalized,
        )
        .expect("groupwise-16 cost");
        assert_eq!(groupwise_16.scale_values_read_total, 8);
    }

    #[test]
    fn groupwise_candidate_names_include_group_size_separator() {
        for (name, expected) in [
            (
                "int8_groupwise_32_symmetric_renormalized",
                PrecisionRankingCandidate::Int8Groupwise32SymmetricRenormalized,
            ),
            (
                "int8_groupwise_16_symmetric_renormalized",
                PrecisionRankingCandidate::Int8Groupwise16SymmetricRenormalized,
            ),
        ] {
            let parsed: PrecisionRankingCandidate =
                serde_json::from_str(&format!("\"{name}\"")).expect("deserialize candidate");
            assert_eq!(parsed, expected);
            assert_eq!(
                serde_json::to_string(&parsed).expect("serialize candidate"),
                format!("\"{name}\"")
            );
        }
    }

    fn truth(top_documents: [usize; TRUTH_K]) -> ExactTruth {
        ExactTruth {
            rank_10: (top_documents[TRUTH_K - 1], 0.0),
            rank_100: (top_documents[TRUTH_K - 1], 0.0),
            top_documents: top_documents.to_vec(),
        }
    }

    fn tensor_set(values: Vec<f32>, rows: Vec<usize>, dim: usize) -> TensorSet {
        let mut scalar_offsets = Vec::with_capacity(rows.len());
        let mut offset = 0usize;
        for &row_count in &rows {
            scalar_offsets.push(offset);
            offset += row_count * dim;
        }
        TensorSet {
            sidecar: TensorSidecar {
                ids: (0..rows.len()).map(|index| index.to_string()).collect(),
                rows: rows.clone(),
                dim,
                dtype: DType::F16,
            },
            values,
            scalar_offsets,
            total_rows: rows.into_iter().sum(),
        }
    }
}
