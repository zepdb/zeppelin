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
const ROUTING_NPROBES: [usize; 2] = [8, 16];
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

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
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
    official_scores: PathBuf,
    #[serde(default)]
    chosen_algorithm: Option<Algorithm>,
    #[serde(default)]
    chosen_centering: Option<Centering>,
}

#[derive(Debug, Deserialize)]
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
    diagnostics: Option<Vec<DiagnosticCell>>,
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

struct PerQueryDiagnostic {
    hits: [usize; 3],
    gold_ranks: Vec<GoldRankDiagnostic>,
    score_pairs: Vec<ScorePairDiagnostic>,
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
    diagnostics: Option<Vec<DiagnosticCell>>,
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

    let official_path = resolve_path(base, &job.official_scores);
    let official_scores: Vec<OfficialScore> = read_json(&official_path)?;
    let parity = evaluate_parity(&documents, &queries, &official_scores)?;
    let evaluation = evaluate_fixed_grid(&job, &documents, &queries, parity.passed)?;
    let result = ResultDocument {
        schema_version: 1,
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
        diagnostics: evaluation.diagnostics,
    };

    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    serde_json::to_writer_pretty(&mut writer, &result).map_err(|source| LabError::Json {
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
    match job.lane {
        Lane::Text => {
            if job.chosen_algorithm.is_some() || job.chosen_centering.is_some() {
                return Err(LabError::Invalid(
                    "text jobs must not provide chosen_algorithm or chosen_centering".to_string(),
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

fn exhaustive_truth(
    documents: &TensorSet,
    queries: &TensorSet,
    prepared: &PreparedValues<'_>,
) -> Result<Vec<Vec<usize>>> {
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
        Ok(scores
            .into_iter()
            .take(TRUTH_K)
            .map(|(index, _)| index)
            .collect())
    })
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
    truth: &[Vec<usize>],
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
    truth: &[Vec<usize>],
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
    truth: &[Vec<usize>],
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
                .iter()
                .filter(|&&document_index| rank_by_document[document_index] <= candidate_k)
                .count();
        }

        let mut gold_ranks = Vec::with_capacity(TRUTH_K);
        for (exact_rank, &document_index) in truth[query_index].iter().enumerate() {
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
        sampled_documents.push(truth[query_index][0]);
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
    parity_passed: bool,
) -> Result<Evaluation> {
    let identity = prepare_values(documents, queries, Centering::Identity)?;
    let identity_truth = exhaustive_truth(documents, queries, &identity)?;
    let mut cells = Vec::new();

    let chosen_algorithm = match job.lane {
        Lane::Text => {
            for algorithm in [Algorithm::PaperV1, Algorithm::ReferenceV1] {
                for config in [CONFIG_A, CONFIG_B] {
                    cells.push(evaluate_cell(
                        documents,
                        queries,
                        &identity,
                        &identity_truth,
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

    if job.lane == Lane::Visual {
        for config in [CONFIG_A, CONFIG_B] {
            cells.push(evaluate_cell(
                documents,
                queries,
                &identity,
                &identity_truth,
                config,
                chosen_algorithm,
                Centering::Identity,
            )?);
        }
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
        &identity,
        Centering::Identity,
    )?];
    for centering in centerings {
        let prepared = prepare_values(documents, queries, centering)?;
        cells.push(evaluate_cell(
            documents,
            queries,
            &prepared,
            &identity_truth,
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
    let recall_gate_passed = match job.lane {
        Lane::Text => cells.iter().any(|cell| cell.recall_at_100 >= threshold),
        Lane::Visual => cells.iter().any(|cell| cell.recall_at_300 >= threshold),
    };
    let gate_passed = parity_passed && recall_gate_passed;
    let winner = choose_winner(&cells, job.lane, threshold);
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
    let diagnostics = if job.lane == Lane::Text {
        let prepared = prepare_values(documents, queries, winner.centering)?;
        Some(vec![
            diagnose_cell(
                documents,
                queries,
                &identity,
                &prepared,
                &identity_truth,
                CONFIG_A,
                winner.algorithm,
                winner.centering,
            )?,
            diagnose_cell(
                documents,
                queries,
                &identity,
                &prepared,
                &identity_truth,
                CONFIG_C_DIAGNOSTIC,
                winner.algorithm,
                winner.centering,
            )?,
        ])
    } else {
        None
    };

    Ok(Evaluation {
        geometry,
        cells,
        winner,
        gate_passed,
        routing,
        diagnostics,
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
        output_dimension: cell.output_dimension,
        candidate_k,
        recall: cell.recall_at(candidate_k),
    }
}

fn config_by_name(name: &str) -> FdeConfig {
    match name {
        "A" => CONFIG_A,
        "B" => CONFIG_B,
        _ => unreachable!("only fixed Phase 2 configs are emitted"),
    }
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
