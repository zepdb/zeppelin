//! Deterministic end-to-end recall evaluation against Zeppelin's production
//! quantized paths.
//!
//! This binary is a quality gate for approximate vector search. It reads a
//! seed specification outside the repository, deterministically generates a
//! clustered dataset and three query families, writes the dataset through the
//! normal WAL path to MinIO, compacts it into one immutable quantized
//! IVF-Flat segment (two-bit by default, SQ8 with
//! `ZEPPELIN_QUANTIZATION=scalar`), and compares production query results
//! with exact
//! brute-force nearest neighbors. It reports mean recall at `k`; it is not a
//! latency benchmark, HTTP client, server process, or substitute for API tests.
//!
//! Object storage remains authoritative throughout the run. WAL uploads become
//! visible only through manifest publication, compaction publishes an immutable
//! segment, and the evaluator refuses to measure when uncompacted WAL fragments
//! remain. Query execution deliberately disables local artifact and manifest
//! caches, so every result is derived from the selected object-store state.
//!
//! ## Reading map
//!
//! 1. Start with `run` and `Cli` for orchestration and command-line choices.
//! 2. Read `SeedFile`, `resolve_dataset`, and `generate_dataset` for deterministic
//!    fixture construction.
//! 3. Read `ExactDataset` and `push_top_k` for independent ground truth.
//! 4. Read `prepare_namespace` and `verify_compacted_segment` for WAL,
//!    compaction, manifest, and coarse-payload artifact invariants.
//! 5. Read `evaluate_modes` and the three query generators for recall scoring.
//! 6. Finish with `cleanup_namespace`, `Report`, and `print_human_report` for the
//!    observable result and cleanup boundary.
//!
//! ## Evaluation lifecycle
//!
//! ```text
//! CLI + external seed specification
//!              |
//!              v
//! deterministic dataset + exact-search copy
//!              |
//!              v
//! create temporary namespace in MinIO
//!              |
//!              v
//! append immutable WAL batches -> publish manifest generations
//!              |
//!              v
//! compact and publish one immutable quantized IVF-Flat segment
//!              |
//!              v
//! verify: active segment + no WAL + coarse payload artifacts
//!              |
//!      +-------+--------+
//!      |                |
//!      v                v
//! exact brute-force   production `execute_query`
//! scoped CPU threads  sequential async queries, caches disabled
//!      |                |
//!      +-------+--------+
//!              v
//! mean set-overlap recall@k per query mode
//!              |
//!              v
//! delete namespace prefix -> print JSON or human report
//! ```
//!
//! Cleanup runs only after every selected mode evaluates successfully. A parse
//! or configuration failure creates no namespace, but an ingestion, compaction,
//! verification, or query failure can leave the generated namespace in MinIO.
//! That fail-loud behavior preserves evidence for diagnosis; operators must
//! remove a leftover `recall-eval-*` prefix explicitly.
//!
//! ## Artifact verification
//!
//! ```text
//! authoritative manifest
//!        |
//!        +--> exactly one active non-hierarchical Scalar or TwoBit segment
//!        +--> expected vector count, positive cluster count
//!        +--> zero uncompacted WAL fragments
//!        |
//!        +--> Scalar arm
//!        |       +--> centroids object
//!        |       |       +--> ZCT2: embedded SQ calibration
//!        |       |       `--> legacy: separate calibration object
//!        |       `--> cluster 0 SQ payload in one supported layout
//!        |               +--> manifest row-layout coarse block (ZBP5)
//!        |               +--> manifest grouped cluster object (ZBP4)
//!        |               +--> co-located singleton cluster object (ZCL2)
//!        |               `--> legacy separate SQ cluster object
//!        |
//!        `--> TwoBit arm
//!                +--> centroids object exists
//!                +--> manifest CoarsePayloadEncoding::TwoBit tag
//!                `--> cluster 0 coarse block in-bounds via its
//!                    manifest-published row layout (ZBP5)
//! ```
//!
//! Layout alternatives provide persisted-format compatibility; they do not
//! permit corrupt bytes to degrade into a lower-quality search path. A selected
//! artifact that is malformed, contradictory, or missing produces an integrity
//! or storage error.
//!
//! ## Invariants
//!
//! - Generation is reproducible for the same seed file, dataset selector, and
//!   pinned `rand` implementation; this binary never writes generated data back
//!   to the seed file.
//! - Exact ground truth uses the dataset metric, lower-is-better score ordering,
//!   and ascending vector ID as the deterministic tie-break.
//! - Recall is measured only through the production quantized IVF path (SQ8 or
//!   two-bit), against one active segment and zero WAL-scored fragments.
//! - Immutable artifacts and the manifest, never local memory, define the
//!   measured dataset.
//! - `--nprobe all` resolves to the actual compacted cluster count. Positive
//!   numeric values are passed to query execution and may be bounded there.
//! - Recall for each query is `|approximate IDs intersect exact IDs| / top_k`;
//!   the mode report is the arithmetic mean across its deterministic queries.
//!
//! ## Rust concepts used here
//!
//! Owned seed and dataset structures make the data lifetime explicit. The
//! production ingestion copy and independent exact-search copy intentionally
//! allocate separate vectors so mutation or quantization on one path cannot
//! contaminate ground truth. Java would normally share arrays unless copied by
//! convention; C would need explicit ownership and cleanup rules.
//!
//! `thread::scope` lets exact-search workers borrow `&ExactDataset` and query
//! slices without [`std::sync::Arc`] or `'static` allocations. The scope joins
//! every worker before those borrows expire, which Rust proves at compile time.
//! Production queries remain async and sequential so one measured request does
//! not interfere with another. [`std::result::Result`] and the `?` operator
//! propagate typed usage, configuration, integrity, storage, and decode errors;
//! RAII releases all in-memory allocations on every exit path.

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
use zeppelin::index::ivf_flat::build::centroids_key;
use zeppelin::index::quantization::sq::{
    deserialize_sq_cluster, deserialize_sq_codes_only, sq_calibration_key, sq_cluster_key,
    SqCalibration,
};
use zeppelin::index::quantization::QuantizationType;
use zeppelin::namespace::branching::ArtifactOrigin;
use zeppelin::namespace::manager::NamespaceManager;
use zeppelin::query::{execute_query, QueryParams};
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::{ConsistencyLevel, DistanceMetric, VectorEntry};
use zeppelin::wal::manifest::CoarsePayloadEncoding;
use zeppelin::wal::{Manifest, WalReader, WalWriter};

/// Default external holdout specification used when `--seed-file` is omitted.
///
/// The absolute path keeps the sealed seed material outside this repository.
/// Operators can supply a different compatible file explicitly.
const DEFAULT_SEED_FILE: &str = "/Users/aghatage/Documents/code/zeppelin-holdout/holdout_seed.toml";
/// Default named dataset selected from the seed specification.
const DEFAULT_DATASET: &str = "d1_primary";
/// Maximum generated entries moved into one immutable WAL fragment.
const INGEST_BATCH_SIZE: usize = 1_000;

/// Failures that make a recall result unusable or prevent the run from starting.
///
/// The distinction between `Config` and `Integrity` is intentional: configuration
/// rejects unsupported operator/seed choices, while integrity means the
/// generated or persisted state contradicted the evaluator's measurement
/// assumptions.
#[derive(Debug, Error)]
enum RecallEvalError {
    /// Invalid command-line syntax or a missing flag value.
    #[error("{0}")]
    Usage(String),
    /// Filesystem failure while loading the external seed specification.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// TOML decoding failure in the external seed specification.
    #[error("seed TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),
    /// JSON encoding failure while producing machine-readable output.
    #[error("json serialization error: {0}")]
    Json(#[from] serde_json::Error),
    /// Storage, namespace, WAL, compaction, index, or query failure from Zeppelin.
    #[error("zeppelin error: {0}")]
    Zeppelin(#[from] ZeppelinError),
    /// Unsupported or internally inconsistent CLI, runtime, or seed configuration.
    #[error("invalid recall-eval configuration: {0}")]
    Config(String),
    /// Generated or persisted state that would invalidate the recall measurement.
    #[error("recall integrity check failed: {0}")]
    Integrity(String),
}

/// Binary-local result alias carrying [`RecallEvalError`].
type Result<T> = std::result::Result<T, RecallEvalError>;

/// Parsed command-line choices after defaults and syntax validation.
#[derive(Debug)]
struct Cli {
    /// External TOML seed specification to read.
    seed_file: PathBuf,
    /// One query family or all supported families.
    query_mode: QueryModeSelection,
    /// Optional explicit probe count; `None` defers to production configuration.
    nprobe: Option<NprobeArg>,
    /// Optional recall cutoff; `None` uses `queries.top_k` from the seed file.
    top_k: Option<usize>,
    /// Dataset name, short name, or human-sized vector-count selector.
    dataset_selector: String,
    /// Whether stdout should contain pretty JSON instead of the human summary.
    json: bool,
}

/// User-facing `--nprobe` representation before the active cluster count exists.
#[derive(Debug, Clone, Copy)]
enum NprobeArg {
    /// Explicit positive number of IVF clusters to inspect.
    Count(usize),
    /// Resolve to every cluster in the compacted segment.
    All,
}

impl fmt::Display for NprobeArg {
    /// Formats the probe request for reports and diagnostics.
    ///
    /// # Parameters
    ///
    /// - `f`: Destination formatter owned by the caller.
    ///
    /// # Returns
    ///
    /// Formatter success after writing a decimal count or the literal `all`.
    ///
    /// # Examples
    ///
    /// `Count(16)` renders as `16`; `All` renders as `all`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Count(value) => write!(f, "{value}"),
            Self::All => write!(f, "all"),
        }
    }
}

/// User-facing query-family selection.
#[derive(Debug, Clone, Copy)]
enum QueryModeSelection {
    /// Evaluate one concrete query family.
    One(QueryMode),
    /// Evaluate centroid, boundary, and uniform families in stable order.
    All,
}

impl QueryModeSelection {
    /// Expands the CLI selection into a stable execution list.
    ///
    /// # Parameters
    ///
    /// - `self`: Copyable selection consumed by value.
    ///
    /// # Returns
    ///
    /// One selected mode or all three modes ordered centroid, boundary,
    /// uniform. The returned vector is newly allocated.
    ///
    /// # Examples
    ///
    /// `One(Boundary)` returns `[Boundary]`; `All` returns three modes.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Because this enum is `Copy`, passing `self` by value copies its small
    /// tagged representation. It is unlike moving an owned `String`: the
    /// original binding remains usable, with no allocation or reference count.
    #[must_use]
    fn modes(self) -> Vec<QueryMode> {
        match self {
            Self::One(mode) => vec![mode],
            Self::All => vec![QueryMode::Centroid, QueryMode::Boundary, QueryMode::Uniform],
        }
    }
}

impl fmt::Display for QueryModeSelection {
    /// Formats the original selection without expanding `all`.
    ///
    /// # Parameters
    ///
    /// - `f`: Destination formatter.
    ///
    /// # Returns
    ///
    /// Formatter success after writing one mode name or `all`.
    ///
    /// # Examples
    ///
    /// `One(Uniform)` renders as `uniform`; `All` renders as `all`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::One(mode) => write!(f, "{mode}"),
            Self::All => write!(f, "all"),
        }
    }
}

/// Deterministic query distribution used to challenge a different recall regime.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
enum QueryMode {
    /// Queries near one generated cluster center; the easiest in-distribution case.
    Centroid,
    /// Queries near the midpoint of two clusters; stresses multi-cell probing.
    Boundary,
    /// Random normalized directions; probes out-of-distribution behavior.
    Uniform,
}

impl QueryMode {
    /// Parses one exact lowercase CLI mode spelling.
    ///
    /// # Parameters
    ///
    /// - `value`: Borrowed command-line value.
    ///
    /// # Returns
    ///
    /// `Some` for `centroid`, `boundary`, or `uniform`; `None` for every other
    /// spelling. `all` belongs to `QueryModeSelection` and returns `None` here.
    ///
    /// # Examples
    ///
    /// `boundary` returns `Some(Boundary)` while `Boundary` returns `None`.
    #[must_use]
    fn parse(value: &str) -> Option<Self> {
        match value {
            "centroid" => Some(Self::Centroid),
            "boundary" => Some(Self::Boundary),
            "uniform" => Some(Self::Uniform),
            _ => None,
        }
    }

    /// Returns the stable lowercase wire/report spelling for this mode.
    ///
    /// # Returns
    ///
    /// A process-lifetime string literal; no allocation occurs.
    ///
    /// # Examples
    ///
    /// `QueryMode::Centroid` returns `centroid`.
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
    /// Writes the stable lowercase mode spelling to a formatter.
    ///
    /// # Parameters
    ///
    /// - `f`: Destination formatter.
    ///
    /// # Returns
    ///
    /// Formatter success from writing `as_str()`.
    ///
    /// # Examples
    ///
    /// Formatting `Boundary` produces `boundary`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Decoded subset of the external holdout TOML consumed by this binary.
///
/// Serde ignores additional seed-file sections owned by other evaluation tools.
#[derive(Debug, Deserialize)]
struct SeedFile {
    /// Version/provenance metadata included in the final report.
    meta: SeedMeta,
    /// Sealed dataset specifications keyed by stable names.
    datasets: BTreeMap<String, DatasetSpec>,
    /// Query counts, default cutoff, and per-mode deterministic seeds.
    queries: QueriesSpec,
    /// Rules that define deterministic exact-neighbor ordering.
    ground_truth: GroundTruthSpec,
}

/// Seed-file provenance included with every result.
#[derive(Debug, Deserialize)]
struct SeedMeta {
    /// Holdout generation number used to distinguish deliberate seed rotations.
    generation: u64,
}

/// Complete deterministic recipe for one synthetic clustered dataset.
#[derive(Debug, Clone, Deserialize)]
struct DatasetSpec {
    /// PRNG seed from which centroids, sigmas, and members are derived.
    seed: u64,
    /// Exact number of vectors to generate.
    n_vectors: usize,
    /// Coordinates per vector and centroid.
    dims: usize,
    /// Number of source clusters in the synthetic distribution.
    n_clusters: usize,
    /// Inclusive conceptual minimum and exclusive sampled maximum noise sigma.
    noise_sigma_range: [f32; 2],
    /// Textual distribution recipe from which the Zipf exponent is parsed.
    cluster_size_distribution: String,
    /// Prefix used to construct deterministic unique vector IDs.
    id_prefix: String,
    /// Stable metric spelling parsed by `parse_distance_metric`.
    distance_metric: String,
}

/// Shared query-generation and exact-neighbor configuration.
#[derive(Debug, Deserialize)]
struct QueriesSpec {
    /// Number of queries independently generated for every selected mode.
    count_per_mode: usize,
    /// Default recall cutoff used when the CLI does not override it.
    top_k: usize,
    /// Deterministic seed for near-centroid queries.
    centroid: QueryModeSpec,
    /// Deterministic seed for between-cluster queries.
    boundary: QueryModeSpec,
    /// Deterministic seed for random-direction queries.
    uniform: QueryModeSpec,
}

impl QueriesSpec {
    /// Selects the deterministic PRNG seed assigned to a query family.
    ///
    /// # Parameters
    ///
    /// - `mode`: Concrete family being generated.
    ///
    /// # Returns
    ///
    /// The corresponding seed copied from this specification.
    ///
    /// # Examples
    ///
    /// Boundary generation receives `queries.boundary.seed`; seeds from other
    /// modes cannot be selected accidentally because the match is exhaustive.
    #[must_use]
    fn seed_for(&self, mode: QueryMode) -> u64 {
        match mode {
            QueryMode::Centroid => self.centroid.seed,
            QueryMode::Boundary => self.boundary.seed,
            QueryMode::Uniform => self.uniform.seed,
        }
    }
}

/// One query mode's deterministic PRNG input.
#[derive(Debug, Deserialize)]
struct QueryModeSpec {
    /// Seed supplied to [`StdRng::seed_from_u64`].
    seed: u64,
}

/// Seed-file assertions about exact-neighbor cutoff and deterministic ties.
#[derive(Debug, Deserialize)]
struct GroundTruthSpec {
    /// Expected seed-file cutoff; validated against `QueriesSpec::top_k`.
    k: usize,
    /// Human-readable rule that must state ascending-ID tie order.
    tie_break: String,
}

/// Generated ingestion fixture plus independent exact-search state.
#[derive(Debug)]
struct GeneratedDataset {
    /// Owned entries retained for WAL ingestion and dimensionality discovery.
    entries: Vec<VectorEntry>,
    /// Deep-owned exact-search copy, including optional cosine-normalized rows.
    exact: ExactDataset,
    /// Unit-normalized source centers used to generate members and queries.
    centroids: Vec<Vec<f32>>,
    /// Per-source-cluster Gaussian standard deviations.
    sigmas: Vec<f32>,
    /// Metric used identically by namespace creation, exact search, and queries.
    distance_metric: DistanceMetric,
}

/// Brute-force corpus independent of Zeppelin's persisted index representation.
#[derive(Debug)]
struct ExactDataset {
    /// IDs aligned one-to-one with `vectors` and `normalized_vectors`.
    ids: Vec<String>,
    /// Original generated coordinates in deterministic row order.
    vectors: Vec<Vec<f32>>,
    /// Pre-normalized rows for cosine, or `None` for other metrics.
    normalized_vectors: Option<Vec<Vec<f32>>>,
}

/// One candidate retained by the bounded exact top-k selection.
#[derive(Debug)]
struct Neighbor {
    /// Owned vector ID used as the deterministic secondary key.
    id: String,
    /// Lower-is-better distance under the dataset metric.
    score: f32,
}

/// Stable machine-readable record of one complete evaluator run.
#[derive(Debug, Serialize)]
struct Report {
    /// External holdout generation copied from seed metadata.
    holdout_generation: u64,
    /// Seed-file path used for this run; seed contents are never emitted.
    seed_file: String,
    /// Canonical dataset name resolved from the selector.
    dataset: String,
    /// Original mode selection (`all` or one concrete mode).
    query_mode: String,
    /// Effective index/query knobs reported alongside quality.
    config: ReportConfig,
    /// Evidence that the measured source was the required compacted segment.
    segment_verification: SegmentVerification,
    /// One result per evaluated mode in execution order.
    modes: Vec<ModeReport>,
}

/// Effective settings needed to reproduce and interpret recall.
#[derive(Debug, Serialize)]
struct ReportConfig {
    /// Production quantization mode measured by the evaluator.
    quantization: String,
    /// User-facing probe request, retaining `all` when specified.
    nprobe_requested: String,
    /// Numeric probe count supplied to query execution.
    nprobe_resolved: usize,
    /// Configured centroid target before dataset-dependent index construction.
    default_centroids: usize,
    /// Cluster count actually published in the active segment.
    actual_clusters: usize,
    /// Production candidate oversampling factor used before reranking.
    oversample_factor: usize,
    /// Neighbor cutoff used by exact and approximate paths.
    top_k: usize,
    /// Query consistency spelling; segment-only evaluation uses eventual mode.
    consistency: String,
}

/// Persisted-artifact checks completed before query measurement.
#[derive(Debug, Serialize)]
struct SegmentVerification {
    /// Always true for a report that reached output.
    compacted_segment: bool,
    /// Remaining manifest WAL refs; valid reports require zero.
    wal_fragments_after_compaction: usize,
    /// Quantization recorded by the active segment descriptor.
    segment_quantization: String,
    /// Manifest coarse payload tag recorded for the measured segment.
    coarse_payload_encoding: String,
    /// Whether a valid embedded or legacy SQ calibration was decoded (SQ8 arm).
    sq_calibration_present: bool,
    /// Whether cluster zero exposed a valid SQ payload in a supported layout
    /// (SQ8 arm).
    sq_cluster_zero_present: bool,
    /// Whether cluster zero's two-bit coarse block was addressable and in
    /// bounds (two-bit arm).
    rq_cluster_zero_present: bool,
}

/// Recall and wall-clock duration for one deterministic query family.
#[derive(Debug, Serialize)]
struct ModeReport {
    /// Query distribution evaluated.
    mode: QueryMode,
    /// Arithmetic mean of per-query set-overlap recall at `k`.
    recall_at_k: f64,
    /// Milliseconds for query generation, exact truth, and production queries.
    elapsed_ms: u128,
}

/// Temporary namespace resources that survive preparation into evaluation.
struct PreparedNamespace {
    /// Unique MinIO namespace/prefix created for this run.
    namespace: String,
    /// Reader sharing the same store for production query execution.
    wal_reader: WalReader,
    /// Verified active-segment facts needed to resolve probes and report output.
    segment: SegmentSummary,
}

/// Borrowed immutable inputs shared across sequential mode evaluation.
///
/// The lifetime ensures the context cannot outlive the store, reader, namespace,
/// generated corpus, or query specification it references.
struct EvalContext<'a> {
    /// Authoritative MinIO gateway used by query execution.
    store: &'a ZeppelinStore,
    /// WAL reader required by the query interface; verified runs scan no WAL.
    wal_reader: &'a WalReader,
    /// Prepared namespace containing exactly one active segment.
    namespace: &'a str,
    /// Generated corpus, exact copy, metric, and query source geometry.
    dataset: &'a GeneratedDataset,
    /// Per-mode query counts and deterministic seeds.
    queries_spec: &'a QueriesSpec,
    /// Exact and approximate neighbor cutoff.
    top_k: usize,
    /// IVF cells requested per production query.
    nprobe: usize,
    /// Candidate expansion applied before exact reranking.
    oversample_factor: usize,
}

/// Verified facts copied out of the manifest before cleanup.
#[derive(Clone, Debug)]
struct SegmentSummary {
    /// Active segment's quantization mode.
    quantization: QuantizationType,
    /// Actual active-segment cluster count.
    cluster_count: usize,
    /// Visible uncompacted refs after compaction; valid summaries contain zero.
    wal_fragments_after_compaction: usize,
    /// Manifest coarse payload tag recorded for the active segment.
    coarse_payload_encoding: CoarsePayloadEncoding,
    /// Whether SQ calibration passed structural decoding (SQ8 arm only).
    sq_calibration_present: bool,
    /// Whether cluster zero's SQ payload passed structural decoding (SQ8 arm
    /// only).
    sq_cluster_zero_present: bool,
    /// Whether cluster zero's two-bit coarse block was addressable and in
    /// bounds (two-bit arm only).
    rq_cluster_zero_present: bool,
}

/// Runs the async evaluator and translates its result into process exit status.
///
/// # Side Effects
///
/// Initializes a Tokio runtime, writes the report to stdout on success, writes
/// one prefixed diagnostic to stderr on failure, and exits with status `1` for
/// any error. A successful/help invocation returns normally with status `0`.
///
/// # Examples
///
/// `recall_eval --help` prints usage and exits successfully. A malformed seed
/// file prints `recall_eval error: ...` and exits nonzero.
///
/// # Rust Notes for Java/C Engineers
///
/// `#[tokio::main]` is a procedural macro that generates the synchronous
/// runtime bootstrap around this `async fn`. It is analogous to creating an
/// executor in Java `main` or initializing an event loop in C, but the returned
/// future's state machine and awaited lifetimes remain compiler-checked.
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

/// Orchestrates one complete generation, publication, evaluation, cleanup, and report.
///
/// The function intentionally checks all cheap local configuration before
/// creating remote state. It then requires a production quantization mode the
/// evaluator supports (SQ8 or two-bit), builds a MinIO-backed store, prepares
/// and verifies one temporary namespace, evaluates selected modes, deletes
/// that namespace, and prints only after cleanup succeeds.
///
/// # Returns
///
/// `Ok(())` after help output or a fully evaluated, cleaned, and printed run.
///
/// # Errors
///
/// Propagates CLI, seed I/O/TOML, configuration, generation, Zeppelin, artifact
/// integrity, cleanup, and JSON-output failures. Failures before namespace
/// creation leave no remote state. Failures after creation but before successful
/// `cleanup_namespace` can leave the temporary namespace in MinIO for manual
/// inspection/removal. A JSON serialization failure occurs after cleanup.
///
/// # Side Effects
///
/// Reads process arguments, environment/configuration, and an external file;
/// creates many immutable objects and manifest generations in MinIO; performs
/// compaction and object-store reads; uses scoped CPU threads for exact search;
/// recursively deletes the temporary namespace; and writes stdout.
///
/// # Consistency
///
/// WAL appends and compaction use their normal manifest publication boundaries.
/// Evaluation begins only after a fresh manifest proves one active quantized
/// segment contains all vectors and no visible WAL refs remain. Production
/// queries use eventual mode only because segment-only state has no newer WAL
/// writes to reconcile.
///
/// # Performance
///
/// Generates and retains multiple full copies of the dataset, uploads batches
/// of at most [`INGEST_BATCH_SIZE`] entries, runs production compaction, computes
/// exact `O(queries * vectors * top_k)` ground truth in scoped threads, then
/// issues approximate queries sequentially. Use a release build for meaningful
/// elapsed numbers; `elapsed_ms` includes exact and approximate work.
///
/// # Examples
///
/// With defaults and a valid MinIO service, the run selects the default sealed
/// dataset, uses configured `default_nprobe`, evaluates all three modes, removes
/// its `recall-eval-*` prefix, and prints a human report. `--nprobe all --json`
/// instead resolves probes to the built cluster count and emits pretty JSON.
///
/// # Rust Notes for Java/C Engineers
///
/// `let Some(cli) = ... else` narrows an optional parsed CLI into the normal-run
/// state while returning early for help. Borrowed references assembled in
/// `EvalContext` remain valid because their owners stay in this stack frame
/// until evaluation completes. Moving `prepared.segment` is avoided until after
/// evaluation; cloning its small summary leaves the namespace/reader available
/// for cleanup without sharing mutable state.
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

    let config = recall_eval_config()?;
    match config.indexing.quantization {
        QuantizationType::Scalar | QuantizationType::TwoBit => {}
        other => {
            return Err(RecallEvalError::Config(format!(
                "recall_eval must measure a production quantized path (SQ8 or two-bit); resolved quantization was {other:?}"
            )));
        }
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
            coarse_payload_encoding: coarse_payload_encoding_name(segment.coarse_payload_encoding)
                .to_string(),
            sq_calibration_present: segment.sq_calibration_present,
            sq_cluster_zero_present: segment.sq_cluster_zero_present,
            rq_cluster_zero_present: segment.rq_cluster_zero_present,
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

/// Build validated production indexing defaults for this non-server evaluator.
///
/// [`Config::load`] deliberately requires an operator-selected security mode
/// because it is the server boot boundary. This binary never opens an HTTP
/// listener or authenticates requests, so it selects `open_unsafe` explicitly
/// in code and validates the remaining production compaction/indexing defaults.
/// Keeping that distinction local prevents an offline quality gate from
/// weakening the fail-closed server configuration contract. Environment
/// overrides (notably `ZEPPELIN_QUANTIZATION=scalar` to measure the SQ8 arm
/// instead of the two-bit default) apply before the security mode is pinned.
fn recall_eval_config() -> Result<Config> {
    let mut config = Config::default();
    config.apply_env_overrides()?;
    config.security.mode = zeppelin::config::SecurityMode::OpenUnsafe;
    config.validate()?;
    Ok(config)
}

/// Parses command-line tokens with deterministic defaults and no external CLI framework.
///
/// Repeated value-taking flags use the last value seen. `--help`/`-h` returns
/// immediately, while any unknown token is an error that includes usage text.
///
/// # Parameters
///
/// - `args`: Owned argument tokens excluding the executable name.
///
/// # Returns
///
/// `Some(Cli)` for an executable evaluation or `None` when help was requested.
///
/// # Errors
///
/// Returns `Usage` for unknown arguments, missing values, invalid query modes,
/// non-positive `--nprobe`, or non-positive `--top-k`. It performs no file,
/// environment, or object-store access.
///
/// # Examples
///
/// Tokens `--query-mode boundary --nprobe all --json` select one mode, defer
/// probe resolution until after compaction, and request JSON. `--top-k 0` and
/// `--wat` fail locally.
///
/// # Rust Notes for Java/C Engineers
///
/// The generic `IntoIterator<Item = String>` accepts real process arguments or
/// an owned test vector without dynamic dispatch. Consuming tokens makes value
/// ownership explicit; `while let` exhaustively handles end-of-input rather
/// than relying on a null sentinel or index arithmetic.
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

/// Takes the next owned token as a flag's required value.
///
/// # Parameters
///
/// - `iter`: Mutable iterator positioned immediately after the flag.
/// - `flag`: Borrowed spelling included in a missing-value diagnostic.
///
/// # Returns
///
/// The next owned string, even if it begins with `-`; semantic parsers decide
/// whether that token is valid for the flag.
///
/// # Errors
///
/// Returns `Usage` when the iterator is exhausted.
///
/// # Examples
///
/// After `--dataset`, token `20k` is returned. A trailing `--dataset` fails with
/// `--dataset requires a value`.
fn next_value<I>(iter: &mut I, flag: &str) -> Result<String>
where
    I: Iterator<Item = String>,
{
    iter.next()
        .ok_or_else(|| RecallEvalError::Usage(format!("{flag} requires a value")))
}

/// Parses the special `--nprobe` value syntax.
///
/// # Parameters
///
/// - `value`: Borrowed CLI token.
///
/// # Returns
///
/// `All` for the exact lowercase literal `all`, otherwise `Count` containing a
/// positive decimal integer.
///
/// # Errors
///
/// Returns `Usage` when a numeric value cannot be parsed or is zero.
///
/// # Examples
///
/// `all` defers to active cluster count, `16` returns `Count(16)`, and `0`
/// fails.
fn parse_nprobe(value: &str) -> Result<NprobeArg> {
    if value == "all" {
        return Ok(NprobeArg::All);
    }
    Ok(NprobeArg::Count(parse_positive_usize("--nprobe", value)?))
}

/// Parses a positive platform-sized decimal integer for one CLI flag.
///
/// # Parameters
///
/// - `flag`: Flag name included in any error.
/// - `value`: Borrowed decimal text.
///
/// # Returns
///
/// A `usize` greater than zero.
///
/// # Errors
///
/// Returns `Usage` for malformed, negative, overflowing, or zero values.
///
/// # Examples
///
/// `("--top-k", "10")` returns `10`; `"0"` and `"ten"` fail with
/// flag-specific context.
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

/// Returns the complete one-line command synopsis.
///
/// # Returns
///
/// A process-lifetime string literal; calling this function allocates nothing.
///
/// # Examples
///
/// Help output and unknown-argument errors use the same synopsis so supported
/// flags cannot drift between two strings.
fn usage() -> &'static str {
    "usage: recall_eval [--query-mode centroid|boundary|uniform|all] [--nprobe <n>|all] \
     [--top-k <k>] [--dataset <name-or-size>] [--seed-file <path>] [--json]"
}

/// Reads and decodes the external holdout specification.
///
/// # Parameters
///
/// - `path`: Filesystem path supplied by defaults or `--seed-file`.
///
/// # Returns
///
/// Owned seed metadata, dataset recipes, query recipes, and ground-truth rules.
/// Additional TOML fields not represented by `SeedFile` are ignored by Serde.
///
/// # Errors
///
/// Returns `Io` for file access/UTF-8 failures and `Toml` for an incompatible
/// document. No MinIO state exists at this phase.
///
/// # Side Effects
///
/// Performs one complete local filesystem read and no writes.
///
/// # Examples
///
/// A valid sealed specification is decoded entirely before configuration
/// validation. A missing path fails without creating an evaluation namespace.
fn read_seed_file(path: &Path) -> Result<SeedFile> {
    let content = std::fs::read_to_string(path)?;
    Ok(toml::from_str(&content)?)
}

/// Confirms that seed-file exact-neighbor rules match this implementation.
///
/// # Parameters
///
/// - `ground_truth`: Borrowed cutoff and textual tie-break contract.
///
/// # Returns
///
/// `Ok(())` when `k` is positive and the tie-break text contains `ascending`.
///
/// # Errors
///
/// Returns `Config` for zero `k` or a tie-break description that does not
/// require ascending IDs. Matching is case-sensitive and intentionally checks
/// only the keyword used by the sealed schema.
///
/// # Examples
///
/// `k=10` with `ties ... ascending IDs` passes; `k=0` or a descending rule
/// fails before dataset generation.
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

/// Rejects a query specification that would produce no measurements.
///
/// # Parameters
///
/// - `queries`: Borrowed shared query recipe.
///
/// # Returns
///
/// `Ok(())` when every selected mode will receive at least one query.
///
/// # Errors
///
/// Returns `Config` when `count_per_mode` is zero. Other fields are validated
/// by their consumers.
///
/// # Examples
///
/// A count of 1 passes; a count of 0 fails before a namespace is created and
/// avoids division by zero in the mode mean.
fn validate_queries(queries: &QueriesSpec) -> Result<()> {
    if queries.count_per_mode == 0 {
        return Err(RecallEvalError::Config(
            "queries.count_per_mode must be greater than zero".into(),
        ));
    }
    Ok(())
}

/// Validates the effective cutoff and the seed file's internal cutoff agreement.
///
/// # Parameters
///
/// - `top_k`: Effective CLI/default cutoff to use for this run.
/// - `seed_file`: Borrowed specification whose query and ground-truth defaults
///   must agree.
///
/// # Returns
///
/// `Ok(())` when `top_k` is positive and the two seed-file cutoff declarations
/// match. A CLI override may differ from those declarations; it changes both
/// exact and approximate cutoff consistently.
///
/// # Errors
///
/// Returns `Config` for zero effective cutoff or mismatched seed-file defaults.
/// Dataset-size bounds are checked after generation.
///
/// # Examples
///
/// Seed defaults `10/10` with CLI `top_k=20` pass this consistency check and are
/// later allowed only if the dataset has at least 20 vectors. Defaults `10/20`
/// fail even when the CLI overrides them.
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

/// Resolves a dataset by full name, prefix before `_`, or exact vector count.
///
/// Names are trimmed, lowercased, and normalize `-` to `_`. A size selector may
/// be a decimal count or a lowercase `k` suffix interpreted as thousands.
/// Resolution requires exactly one match so short aliases cannot silently pick
/// one of several sealed datasets.
///
/// # Parameters
///
/// - `seed_file`: Borrowed map of canonical dataset names and specifications.
/// - `selector`: Raw CLI selector.
///
/// # Returns
///
/// The canonical owned name and a deep clone of its [`DatasetSpec`].
///
/// # Errors
///
/// Returns `Config` when no dataset matches, more than one matches, or the
/// logically single-match vector cannot yield its entry (a defensive invariant).
///
/// # Performance
///
/// Scans every configured dataset and clones only matching specifications.
/// Complexity is linear in dataset count and name length.
///
/// # Examples
///
/// Selector `d1-primary` can match canonical `d1_primary`; `50k` can match the
/// unique 50,000-vector recipe. If two recipes contain the same vector count,
/// that size selector is rejected as ambiguous.
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

/// Canonicalizes a human dataset selector for comparison.
///
/// # Parameters
///
/// - `value`: Borrowed user or seed-file name.
///
/// # Returns
///
/// A newly allocated trimmed ASCII-lowercase string with dashes replaced by
/// underscores. Non-ASCII characters are otherwise preserved.
///
/// # Examples
///
/// `" D1-Primary "` becomes `"d1_primary"`.
fn normalize_selector(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace('-', "_")
}

/// Interprets a normalized selector as an exact vector count when possible.
///
/// # Parameters
///
/// - `value`: Normalized selector, normally from `normalize_selector`.
///
/// # Returns
///
/// A decimal `usize`, a lowercase-`k` value multiplied by 1,000, or `None` when
/// parsing fails. This helper does not accept decimals such as `1.5k` or an
/// uppercase suffix after normalization.
///
/// # Panics
///
/// In overflow-checking builds, a syntactically valid `k` prefix whose value
/// times 1,000 exceeds `usize` panics. Normal operational selectors are bounded
/// by the seed-file dataset sizes.
///
/// # Examples
///
/// `20000` and `20k` both return `Some(20_000)`; `d2` returns `None`.
fn parse_size_selector(value: &str) -> Option<usize> {
    if let Some(prefix) = value.strip_suffix('k') {
        return prefix.parse::<usize>().ok().map(|n| n * 1_000);
    }
    value.parse::<usize>().ok()
}

/// Constructs the explicit MinIO/S3 storage configuration for evaluation.
///
/// Environment variables can override endpoint, bucket, and credentials. The
/// harness defaults `TEST_BACKEND` to `minio` but rejects every explicit other
/// backend so recall cannot accidentally measure in-memory/local semantics.
///
/// # Returns
///
/// A fail-fast [`StorageConfig`] using the S3 backend, MinIO-compatible region,
/// HTTP allowance, and resolved connection values. Construction performs no
/// network request.
///
/// # Errors
///
/// Returns `Config` when `TEST_BACKEND` is present and not exactly `minio`.
/// Invalid endpoint or credentials surface later from `ZeppelinStore`.
///
/// # Examples
///
/// With no evaluation environment variables, the conventional local MinIO
/// endpoint and test credentials are selected. `TEST_BACKEND=memory` fails
/// before any namespace is created.
///
/// # Rust Notes for Java/C Engineers
///
/// Every optional configuration string is owned by the returned struct, so it
/// remains valid after temporary environment values are dropped. `unwrap_or_else`
/// lazily allocates defaults only when an environment variable is absent.
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
        fail_fast: true,
    })
}

/// Materializes one deterministic clustered corpus and its independent exact copy.
///
/// Generation seeds one [`StdRng`], draws unit-normalized source centroids,
/// draws per-cluster noise scales, allocates exact vector counts using a Zipf
/// distribution, and emits members in cluster/local-index order. Member vectors
/// are centroid plus Gaussian noise and are not normalized in-place; cosine
/// ground truth stores a separate normalized view.
///
/// # Parameters
///
/// - `spec`: Owned sealed recipe. Its strings are consumed after validation and
///   metric/distribution parsing.
///
/// # Returns
///
/// Owned ingestion entries, exact-search state, source centroids/sigmas, and the
/// parsed metric. Entry IDs are `{id_prefix}_{cluster}_{local_index}` and are
/// deterministic for the same specification and `rand` implementation.
///
/// # Errors
///
/// Returns `Config` for invalid dimensions/counts/sigmas/metric/distribution,
/// or `Integrity` if normalization fails or the final entry count contradicts
/// `n_vectors`. No remote state is touched.
///
/// # Performance
///
/// Generates `n_vectors * dims` coordinates and retains at least two full
/// coordinate copies (`entries` and `ExactDataset::vectors`), plus a third
/// normalized copy for cosine. Time and memory are therefore linear in corpus
/// size before ingestion cloning adds transient batch copies.
///
/// # Examples
///
/// A recipe with 1,000 vectors, 8 clusters, and 32 dimensions emits exactly
/// 1,000 unique IDs in deterministic cluster order. Repeating the same recipe
/// reproduces values; changing only the seed changes geometry.
///
/// # Rust Notes for Java/C Engineers
///
/// `spec` is moved into this function, while `validate_dataset_spec` borrows it
/// temporarily. The final struct then moves completed vectors into one owner.
/// Rust prevents accidental use-after-move and automatically releases partial
/// allocations if any `?` returns early.
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

/// Validates shape assumptions required by dataset and boundary-query generation.
///
/// # Parameters
///
/// - `spec`: Borrowed dataset recipe.
///
/// # Returns
///
/// `Ok(())` when vector/dimension counts are positive, at least two clusters
/// exist, every cluster can conceptually receive data, and sigma bounds are
/// finite, non-negative, and ordered.
///
/// # Errors
///
/// Returns `Config` naming the first violated condition. It does not validate
/// the metric, Zipf text, or ID prefix; later parsers/generation own those.
///
/// # Examples
///
/// Ten vectors across two clusters pass shape validation. One cluster fails
/// because boundary queries require two distinct centers; fewer vectors than
/// clusters also fail.
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

/// Parses the seed schema's exact distance-metric spelling.
///
/// # Parameters
///
/// - `value`: Borrowed metric text.
///
/// # Returns
///
/// The matching Zeppelin [`DistanceMetric`] for `cosine`, `euclidean`, or
/// `dot_product`.
///
/// # Errors
///
/// Returns `Config` for every other or differently cased spelling.
///
/// # Examples
///
/// `dot_product` selects Zeppelin's lower-is-better negated-dot distance;
/// `dot-product` is rejected.
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

/// Extracts a positive `s=` exponent from the distribution recipe text.
///
/// Parsing starts immediately after the first `s=` and consumes ASCII digits
/// and dots until another character. The surrounding distribution name is not
/// otherwise interpreted, so compatibility is defined by this textual marker.
///
/// # Parameters
///
/// - `distribution`: Borrowed seed-file description.
///
/// # Returns
///
/// A finite positive `f64` exponent.
///
/// # Errors
///
/// Returns `Config` when `s=` is absent, the numeric prefix is empty/malformed,
/// or the parsed exponent is non-finite or non-positive.
///
/// # Examples
///
/// `zipf(s=1.1) over clusters` returns `1.1`; `uniform` and `zipf(s=0)` fail.
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

/// Draws random source centers and normalizes each to unit length.
///
/// # Parameters
///
/// - `rng`: Mutable deterministic PRNG whose state advances for every component.
/// - `n_clusters`: Number of centroids to return.
/// - `dims`: Coordinates per centroid.
///
/// # Returns
///
/// `n_clusters` owned vectors of length `dims`, each normalized from independent
/// uniform `[-1, 1)` components.
///
/// # Errors
///
/// Returns `Integrity` if a drawn vector has zero/near-zero or non-finite norm.
/// Zero dimensions always trigger this error when at least one cluster is
/// requested; the caller normally rejects them earlier.
///
/// # Performance
///
/// Allocates `n_clusters * dims` floats and performs one normalization pass per
/// centroid.
///
/// # Examples
///
/// Two clusters in three dimensions produce two unit-length three-value vectors
/// and advance the PRNG by six uniform draws.
fn generate_centroids(rng: &mut StdRng, n_clusters: usize, dims: usize) -> Result<Vec<Vec<f32>>> {
    let mut centroids = Vec::with_capacity(n_clusters);
    for _ in 0..n_clusters {
        let values: Vec<f32> = (0..dims).map(|_| rng.gen_range(-1.0..1.0)).collect();
        centroids.push(normalize_strict(&values)?);
    }
    Ok(centroids)
}

/// Draws one Gaussian noise scale per source cluster.
///
/// # Parameters
///
/// - `rng`: Mutable deterministic PRNG.
/// - `n_clusters`: Number of scales to produce.
/// - `range`: Minimum and maximum sigma validated by the caller.
///
/// # Returns
///
/// A constant vector when bounds are equal, otherwise independent uniform
/// samples from `[min_sigma, max_sigma)`.
///
/// # Errors
///
/// The current implementation has no fallible branch after valid inputs and
/// returns `Ok` for signature consistency with adjacent generators.
///
/// # Panics
///
/// `rand::gen_range` can panic when unequal bounds are reversed or otherwise
/// invalid. `validate_dataset_spec` establishes the required finite order.
///
/// # Examples
///
/// Range `[0.1, 0.1]` returns the same sigma for every cluster without advancing
/// the PRNG; `[0.05, 0.15]` draws one value per cluster.
fn generate_sigmas(rng: &mut StdRng, n_clusters: usize, range: [f32; 2]) -> Result<Vec<f32>> {
    let [min_sigma, max_sigma] = range;
    if min_sigma == max_sigma {
        return Ok(vec![min_sigma; n_clusters]);
    }
    Ok((0..n_clusters)
        .map(|_| rng.gen_range(min_sigma..max_sigma))
        .collect())
}

/// Allocates an exact total across ranked clusters using Zipf weights.
///
/// The method floors each ideal weighted count, then assigns the remaining
/// vectors to the largest fractional remainders. Equal remainders favor lower
/// cluster indexes, making integer allocation deterministic.
///
/// # Parameters
///
/// - `total`: Number of vectors to distribute.
/// - `buckets`: Number of ranked source clusters.
/// - `exponent`: Positive finite skew parameter; larger values concentrate more
///   vectors in early clusters.
///
/// # Returns
///
/// Exactly `buckets` counts whose sum is `total` under validated inputs.
/// Ordering is source-cluster rank order, not sorted count order.
///
/// # Panics
///
/// `total - assigned` can underflow if invalid/non-finite weights or floating
/// arithmetic violate the floor-sum invariant. Callers provide a finite positive
/// exponent and positive bucket count parsed from validated seed data.
///
/// # Performance
///
/// Uses `O(buckets)` memory and `O(buckets log buckets)` time because fractional
/// remainders are sorted before the final allocation.
///
/// # Examples
///
/// Distributing 10 vectors across three clusters returns three deterministic
/// counts summing to 10, with the rank-one cluster receiving the largest share
/// for a positive exponent.
///
/// # Rust Notes for Java/C Engineers
///
/// Iterator pipelines build owned weight/fraction vectors, and the sort closure
/// captures no mutable external state. `partial_cmp` handles the floating-point
/// API explicitly; validated finite fractions make the `Equal` fallback only a
/// defensive total-order completion.
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

/// Draws one standard-normal sample with the Box-Muller transform.
///
/// # Parameters
///
/// - `rng`: Mutable deterministic PRNG advanced by two uniform `f32` draws.
///
/// # Returns
///
/// One approximately standard-normal `f32`. The first uniform is clamped away
/// from zero so `ln(0)` cannot create infinity.
///
/// # Examples
///
/// Reusing the same seeded `StdRng` sequence reproduces the same Gaussian
/// samples, which makes dataset and query generation deterministic.
fn gaussian(rng: &mut StdRng) -> f32 {
    let u1 = rng.gen::<f32>().clamp(f32::MIN_POSITIVE, 1.0);
    let u2 = rng.gen::<f32>();
    (-2.0 * u1.ln()).sqrt() * (std::f32::consts::TAU * u2).cos()
}

/// Returns an owned unit-length copy or rejects an unusable vector.
///
/// # Parameters
///
/// - `values`: Borrowed coordinates whose Euclidean norm is computed in `f32`.
///
/// # Returns
///
/// A newly allocated vector with every component divided by the finite norm.
/// The input is unchanged.
///
/// # Errors
///
/// Returns `Integrity` when the norm is non-finite or no greater than
/// `f32::EPSILON`, covering empty, zero, near-zero, NaN, and overflowed inputs.
///
/// # Performance
///
/// Performs two linear passes and allocates one vector of `values.len()` floats.
///
/// # Examples
///
/// `[3, 4]` becomes `[0.6, 0.8]`; `[0, 0]` fails rather than producing NaNs.
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
    /// Builds a deep-owned brute-force corpus from generated ingestion entries.
    ///
    /// # Parameters
    ///
    /// - `entries`: Borrowed generated rows retained by the ingestion path.
    /// - `metric`: Dataset metric; cosine triggers a second normalized-vector
    ///   allocation for stable repeated scoring.
    ///
    /// # Returns
    ///
    /// IDs and original vectors cloned in identical row order, plus normalized
    /// vectors for cosine or `None` for Euclidean/dot-product metrics.
    ///
    /// # Errors
    ///
    /// Returns `Integrity` if any cosine row cannot be normalized. Non-cosine
    /// rows are cloned without finiteness validation; later score checks remain
    /// authoritative.
    ///
    /// # Performance
    ///
    /// Deep-clones all IDs and coordinate vectors. Cosine allocates and computes
    /// an additional full coordinate copy, trading memory for avoiding repeated
    /// per-query corpus normalization.
    ///
    /// # Examples
    ///
    /// Two cosine entries create two aligned ID/original/normalized rows. A zero
    /// vector fails construction so exact ground truth cannot contain NaN scores.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `Option<Vec<Vec<f32>>>` represents the metric-dependent cache explicitly.
    /// `as_mut()` borrows its inner vector only in the `Some` branch, allowing
    /// rows to be appended without null checks or taking ownership away from the
    /// final struct.
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

    /// Computes deterministic exact nearest-neighbor IDs for one query.
    ///
    /// Cosine scoring normalizes the query once and uses cached normalized corpus
    /// rows. Other metrics call Zeppelin's shared distance primitive. A bounded
    /// candidate vector retains only the best `top_k` rows; final sorting applies
    /// ascending ID after ascending distance.
    ///
    /// # Parameters
    ///
    /// - `query`: Borrowed query coordinates matching corpus dimensionality.
    /// - `top_k`: Positive number of IDs requested.
    /// - `metric`: Metric consistent with this dataset's construction.
    ///
    /// # Returns
    ///
    /// Up to `top_k` owned IDs ordered by lower distance then ascending ID. When
    /// `top_k` exceeds corpus size, every ID is returned.
    ///
    /// # Errors
    ///
    /// Returns `Integrity` when a cosine query cannot be normalized or any exact
    /// score is non-finite. The error names the affected corpus ID for diagnosis.
    ///
    /// # Panics
    ///
    /// A zero `top_k` eventually indexes an empty bounded candidate vector, and
    /// mismatched dimensions can panic in the shared distance primitives (or a
    /// debug assertion for `dot`). `run` validates positive cutoff and generated
    /// data/query dimensionality before this method is called.
    ///
    /// # Performance
    ///
    /// Scores every corpus vector. Candidate maintenance scans at most `top_k`
    /// entries per row, for `O(n * top_k * dims)` time and `O(top_k)` candidate
    /// memory, plus returned ID allocations.
    ///
    /// # Examples
    ///
    /// If A and B have equal distance and A's ID sorts first, `top_k=1` returns
    /// A deterministically. A non-finite score aborts instead of silently placing
    /// the row at an arbitrary rank.
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

    /// Computes exact neighbors for a query batch using scoped OS threads.
    ///
    /// Queries are divided into contiguous chunks across available hardware
    /// threads. Every worker borrows this corpus and its query slice; results are
    /// joined and restored to original query order before return.
    ///
    /// # Parameters
    ///
    /// - `queries`: Non-empty borrowed query vectors.
    /// - `top_k`: Positive cutoff forwarded to `top_k`.
    /// - `metric`: Dataset metric forwarded unchanged.
    ///
    /// # Returns
    ///
    /// One ordered exact-ID vector per input query, in input order.
    ///
    /// # Errors
    ///
    /// Propagates any worker's scoring/integrity error. A worker panic becomes a
    /// descriptive `Integrity` error, and a missing result slot after joining is
    /// also treated as an invariant failure.
    ///
    /// # Panics
    ///
    /// An empty `queries` slice produces `chunk_size == 0`, which is invalid for
    /// `slice::chunks`. `validate_queries` and `generate_queries` guarantee a
    /// non-empty batch in the evaluator. `top_k` inherits `top_k`'s preconditions.
    ///
    /// # Performance
    ///
    /// Uses up to `min(available_parallelism, queries.len())` blocking OS
    /// threads. CPU work is parallel; each result still performs full brute-force
    /// scoring. The result-slot vector temporarily stores one `Option` per query.
    ///
    /// # Examples
    ///
    /// A 100-query batch on eight available CPUs is split into contiguous chunks
    /// of at most 13. Even if later chunks finish first, output index 0 still
    /// belongs to input query 0.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `thread::scope` is unlike a Java executor requiring effectively-final
    /// captured objects or C worker arguments with manual lifetime tracking: it
    /// permits non-`'static` borrows because all handles must join before the
    /// scope exits. `move` moves each borrowed chunk handle/start value into its
    /// closure, not the underlying corpus allocation.
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

/// Computes a scalar dot product over aligned borrowed slices.
///
/// # Parameters
///
/// - `a`: Left coordinates.
/// - `b`: Right coordinates with the same length under caller invariants.
///
/// # Returns
///
/// The `f32` sum of pairwise products.
///
/// # Panics
///
/// In debug builds, mismatched lengths trigger the explicit assertion. In
/// release builds, `zip` would stop at the shorter slice; all evaluator callers
/// construct equal-dimensional values.
///
/// # Examples
///
/// `[1, 2] dot [3, 4]` returns `11`.
fn dot(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    a.iter().zip(b).map(|(left, right)| left * right).sum()
}

/// Inserts one scored ID into a bounded lower-is-better candidate set.
///
/// Until full, the helper appends. Once full, it scans for the worst retained
/// candidate and replaces it only when the new `(score, ID)` sorts earlier.
/// The vector is intentionally unsorted during streaming; `ExactDataset::top_k`
/// sorts only the final small set.
///
/// # Parameters
///
/// - `best`: Exclusive mutable candidate vector with length at most `top_k`.
/// - `top_k`: Positive capacity.
/// - `score`: Finite lower-is-better distance.
/// - `id`: Borrowed vector ID cloned only if the candidate is retained.
///
/// # Returns
///
/// Returns `()` after preserving at most the best `top_k` candidates under
/// `compare_scored_ids`.
///
/// # Panics
///
/// Panics for `top_k == 0` because the full-set branch attempts to inspect index
/// zero of an empty vector. The evaluator validates a positive cutoff.
///
/// # Performance
///
/// `O(1)` while filling and `O(top_k)` per later candidate, with no ID
/// allocation for rejected candidates.
///
/// # Examples
///
/// With capacity two and retained distances A=0.1, B=0.3, candidate C=0.2
/// replaces B; candidate D=0.4 is ignored.
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

/// Defines the total ranking used by exact ground truth.
///
/// # Parameters
///
/// - `left_score`, `right_score`: Lower-is-better distances.
/// - `left_id`, `right_id`: Borrowed deterministic secondary keys.
///
/// # Returns
///
/// Ascending score order, then lexicographically ascending ID. If floating-point
/// comparison is unordered (NaN), scores are treated as equal and IDs decide;
/// exact scoring rejects non-finite values before relying on that fallback.
///
/// # Examples
///
/// `(0.1, "b")` sorts before `(0.2, "a")`; tied `(0.1, "a")` sorts before
/// `(0.1, "b")`.
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

/// Creates, ingests, compacts, and verifies one isolated evaluation namespace.
///
/// The UUID namespace prevents collisions between runs. Entries are appended in
/// bounded chunks through the ordinary [`WalWriter`], then the production
/// [`Compactor`] publishes an immutable segment. Verification re-reads
/// authoritative state before any recall query is allowed.
///
/// # Parameters
///
/// - `store`: Shared MinIO-backed object-store gateway.
/// - `config`: Production compaction/indexing/GC configuration.
/// - `dataset`: Generated entries and metric retained by the caller for exact
///   scoring.
///
/// # Returns
///
/// The unique namespace, a reader bound to the same store, and verified segment
/// summary. The namespace remains present for evaluation.
///
/// # Errors
///
/// Propagates namespace creation, WAL upload/publication, compaction, storage,
/// and verification failures. It also returns `Integrity` for an unexpectedly
/// empty generated corpus or compaction that reports no segment. Any failure
/// after namespace creation may leave its immutable objects in MinIO because
/// cleanup is intentionally owned by successful top-level orchestration.
///
/// # Side Effects
///
/// Creates namespace metadata and an initial manifest, appends
/// `ceil(entries / INGEST_BATCH_SIZE)` immutable WAL fragments and manifest
/// generations, then uploads and publishes compaction artifacts.
///
/// # Consistency
///
/// Each append is acknowledged only after manifest CAS. Compaction chooses
/// visible fragments, creates immutable artifacts, and publishes the active
/// segment through the manifest. `verify_compacted_segment` then requires
/// that no uncompacted refs remain.
///
/// # Performance
///
/// `chunk.to_vec()` deep-clones each ingestion batch because the original corpus
/// must remain available for exact ground truth. Network cost includes one WAL
/// PUT per batch, manifest publication work, full compaction I/O/CPU, and
/// verification reads.
///
/// # Examples
///
/// A 2,500-entry corpus creates three WAL append batches, compacts them into one
/// active quantized segment, and returns a reader/summary only after the
/// manifest shows zero remaining fragments.
///
/// # Rust Notes for Java/C Engineers
///
/// `store.clone()` clones a lightweight shared backend handle, not MinIO data.
/// In contrast, `chunk.to_vec()` invokes `VectorEntry::clone` and deep-copies IDs,
/// coordinates, and metadata so the WAL writer can take ownership while the
/// exact corpus remains independent.
async fn prepare_namespace(
    store: &ZeppelinStore,
    config: &Config,
    dataset: &GeneratedDataset,
) -> Result<PreparedNamespace> {
    let namespace = format!("recall-eval-{}", Uuid::new_v4());
    let manager = NamespaceManager::new(store.clone());
    let metadata = manager
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
    let authoritative_origin = metadata.artifact_origin()?.ok_or_else(|| {
        RecallEvalError::Integrity(
            "newly created recall namespace has no incarnation identity".into(),
        )
    })?;

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
        std::time::Duration::from_secs(config.gc.compaction_upload_window_secs),
    );
    let result = compactor.compact(&namespace).await?;
    if result.segment_id.is_none() {
        return Err(RecallEvalError::Integrity(
            "compaction did not produce a segment".into(),
        ));
    }

    let segment = verify_compacted_segment(
        store,
        &namespace,
        &authoritative_origin,
        dataset.entries.len(),
    )
    .await?;
    Ok(PreparedNamespace {
        namespace,
        wal_reader: WalReader::new(store.clone()),
        segment,
    })
}

/// Proves that the namespace is a segment-only production quantized measurement
/// source.
///
/// The verifier reads the authoritative manifest, resolves its active segment,
/// checks shape/quantization/WAL invariants, then applies the encoding arm
/// matching the segment's quantization. The SQ8 arm decodes either embedded or
/// legacy SQ calibration and validates a representative cluster-zero SQ payload
/// in every supported persisted layout. The two-bit arm requires the manifest's
/// `CoarsePayloadEncoding::TwoBit` tag and validates cluster zero's coarse block
/// through its manifest-published row layout; two-bit segments have no legacy
/// key fallback because the production two-bit reader has none.
///
/// # Parameters
///
/// - `store`: MinIO-backed gateway used for direct authoritative reads.
/// - `namespace`: Prepared namespace prefix.
/// - `authoritative_origin`: Metadata-bound identity of the logical namespace.
/// - `expected_vectors`: Generated corpus size that the active descriptor must
///   report exactly.
///
/// # Returns
///
/// A copied [`SegmentSummary`] containing quantization, cluster count, zero WAL
/// refs, and successful calibration/cluster-zero evidence for the measured arm.
///
/// # Errors
///
/// Returns `Integrity` when the manifest/active descriptor is absent or
/// contradictory, WAL refs remain, counts differ, the segment is hierarchical,
/// quantization is neither scalar nor two-bit, cluster count is zero, embedded
/// layout arithmetic overflows/truncates, calibration is empty/invalid, no valid
/// cluster-zero SQ representation exists (SQ8 arm), the two-bit coarse payload
/// tag is missing, or cluster zero's two-bit coarse block is unaddressable or
/// out of bounds (two-bit arm). Storage and SQ decode failures propagate.
///
/// A failed GET of the optional legacy co-located singleton is treated as
/// “layout not present” so manifest-grouped or separate-SQ layouts can still
/// prove the artifact. Reads of manifest-selected grouped objects and the final
/// legacy existence check remain fallible and propagate errors.
///
/// # Side Effects
///
/// Performs read-only manifest and object-store operations. It does not populate
/// a local cache, mutate a manifest, or repair an artifact.
///
/// # Consistency
///
/// The manifest is the visibility authority. The active descriptor selects the
/// segment and its physical cluster owners. Existing but unreferenced objects
/// cannot satisfy segment identity, while supported older layouts are accepted
/// only under keys derived from that descriptor.
///
/// # Performance
///
/// Performs one manifest read, one centroids GET, either embedded calibration
/// decoding or one legacy calibration GET, one optional manifest-grouped object
/// GET, one optional singleton GET, and possibly one legacy existence request;
/// the two-bit arm skips calibration and legacy layout probes. Only cluster
/// zero is structurally sampled; this is a path-verification guard,
/// not a full segment scrub.
///
/// # Examples
///
/// A scalar segment with all expected vectors, no WAL refs, embedded `ZCT2`
/// calibration, and cluster zero in a `ZBP4` group passes. A two-bit segment
/// with the manifest two-bit tag and an in-bounds cluster-zero coarse block
/// passes; the same segment without the tag fails. Any segment with one visible
/// WAL fragment fails because recall would mix segment and WAL paths.
///
/// # Rust Notes for Java/C Engineers
///
/// `checked_mul`/`checked_add` turn binary-offset overflow into explicit errors;
/// `try_into()` validates fixed-width byte slices before integer decoding. C
/// parsers often rely on manual pointer arithmetic, while Java `ByteBuffer`
/// still requires disciplined bounds/order checks. Rust slices prevent out-of-
/// bounds access once each calculated range has been validated.
async fn verify_compacted_segment(
    store: &ZeppelinStore,
    namespace: &str,
    authoritative_origin: &ArtifactOrigin,
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
    let physical_origin = manifest
        .active_segment_artifact_origin(authoritative_origin)?
        .ok_or_else(|| RecallEvalError::Integrity("no active segment origin".into()))?;
    let physical_namespace = physical_origin.namespace.as_str();

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
            "active segment is hierarchical, expected IVF-Flat".into(),
        ));
    }
    if segment.cluster_count == 0 {
        return Err(RecallEvalError::Integrity(
            "active segment has zero clusters".into(),
        ));
    }

    match segment.quantization {
        QuantizationType::Scalar => {
            let centroids = store
                .get(&centroids_key(physical_namespace, active_segment_id))
                .await?;
            let parsed = if centroids.starts_with(b"ZCT2") {
                if centroids.len() < 20 {
                    return Err(RecallEvalError::Integrity(
                        "v2 centroids blob too small for SQ calibration".into(),
                    ));
                }
                let num_centroids =
                    u32::from_le_bytes(centroids[4..8].try_into().map_err(|_| {
                        RecallEvalError::Integrity("v2 centroids count parse error".into())
                    })?) as usize;
                let dim = u32::from_le_bytes(centroids[8..12].try_into().map_err(|_| {
                    RecallEvalError::Integrity("v2 centroids dimension parse error".into())
                })?) as usize;
                let cal_len_offset = 12usize
                        .checked_add(
                            num_centroids
                                .checked_mul(dim)
                                .and_then(|v| v.checked_mul(4))
                                .ok_or_else(|| {
                                    RecallEvalError::Integrity(format!(
                                    "v2 centroids float section overflows: n={num_centroids}, dim={dim}"
                                ))
                                })?,
                        )
                        .ok_or_else(|| {
                            RecallEvalError::Integrity("v2 centroids offset overflow".into())
                        })?;
                if centroids.len() < cal_len_offset + 8 {
                    return Err(RecallEvalError::Integrity(
                        "v2 centroids blob truncated before SQ calibration length".into(),
                    ));
                }
                let cal_len = u64::from_le_bytes(
                    centroids[cal_len_offset..cal_len_offset + 8]
                        .try_into()
                        .map_err(|_| {
                            RecallEvalError::Integrity(
                                "v2 SQ calibration length parse error".into(),
                            )
                        })?,
                ) as usize;
                if cal_len == 0 {
                    return Err(RecallEvalError::Integrity(
                        "v2 SQ8 segment has no embedded SQ calibration".into(),
                    ));
                }
                let cal_start = cal_len_offset + 8;
                let cal_end = cal_start.checked_add(cal_len).ok_or_else(|| {
                    RecallEvalError::Integrity("v2 SQ calibration overflow".into())
                })?;
                if centroids.len() != cal_end {
                    return Err(RecallEvalError::Integrity(
                        "v2 centroids SQ calibration length does not match object size".into(),
                    ));
                }
                SqCalibration::from_bytes(&centroids[cal_start..cal_end])?
            } else {
                let calibration_key = sq_calibration_key(physical_namespace, active_segment_id);
                let calibration = store.get(&calibration_key).await?;
                SqCalibration::from_bytes(&calibration)?
            };
            if parsed.dim == 0 {
                return Err(RecallEvalError::Integrity(
                    "SQ calibration has zero dimensions".into(),
                ));
            }
            let manifest_cluster_zero_present =
                manifest_sq_cluster_artifact_present(store, segment, 0).await?;
            let cluster_zero_key = sq_cluster_key(physical_namespace, segment.cluster_owner(0), 0);
            let colocated_cluster_zero_key = format!(
                "{physical_namespace}/segments/{}/cluster_0.bin",
                segment.cluster_owner(0)
            );
            let colocated_cluster_zero_present = match store.get(&colocated_cluster_zero_key).await
            {
                Ok(data) => colocated_sq_cluster_present(&data)?,
                Err(_) => false,
            };
            let sq_cluster_zero_present = manifest_cluster_zero_present
                || colocated_cluster_zero_present
                || store.exists(&cluster_zero_key).await?;
            if !sq_cluster_zero_present {
                return Err(RecallEvalError::Integrity(format!(
                    "missing SQ8 cluster artifact {cluster_zero_key}, co-located {colocated_cluster_zero_key}, or manifest cluster_objects entry for cluster 0"
                )));
            }

            Ok(SegmentSummary {
                quantization: segment.quantization,
                cluster_count: segment.cluster_count,
                wal_fragments_after_compaction: manifest.uncompacted_fragments().len(),
                coarse_payload_encoding: manifest.coarse_payload_encoding(active_segment_id),
                sq_calibration_present: true,
                sq_cluster_zero_present,
                rq_cluster_zero_present: false,
            })
        }
        QuantizationType::TwoBit => {
            store
                .get(&centroids_key(physical_namespace, active_segment_id))
                .await?;
            let encoding = manifest.coarse_payload_encoding(active_segment_id);
            if encoding != CoarsePayloadEncoding::TwoBit {
                return Err(RecallEvalError::Integrity(format!(
                    "two-bit segment {active_segment_id} is missing its two-bit coarse payload tag"
                )));
            }
            let rq_cluster_zero_present =
                manifest_rq_cluster_artifact_present(store, segment, 0).await?;
            if !rq_cluster_zero_present {
                return Err(RecallEvalError::Integrity(format!(
                    "missing two-bit coarse artifact for cluster 0 in manifest cluster_objects of segment {active_segment_id}"
                )));
            }

            Ok(SegmentSummary {
                quantization: segment.quantization,
                cluster_count: segment.cluster_count,
                wal_fragments_after_compaction: manifest.uncompacted_fragments().len(),
                coarse_payload_encoding: encoding,
                sq_calibration_present: false,
                sq_cluster_zero_present: false,
                rq_cluster_zero_present,
            })
        }
        other => Err(RecallEvalError::Integrity(format!(
            "active segment quantization is {other:?}, expected Scalar/SQ8 or TwoBit"
        ))),
    }
}

/// Evaluates exact-versus-production recall for each selected query family.
///
/// Modes run sequentially. For each mode, deterministic queries are generated,
/// exact top-k sets are computed in scoped CPU threads, and production queries
/// are awaited one at a time with eventual consistency and both caches disabled.
/// The function verifies every response used exactly one segment and zero WAL
/// fragments before adding its set-overlap recall.
///
/// # Parameters
///
/// - `context`: Borrowed store, namespace, corpus, query recipe, and effective
///   query knobs.
/// - `modes`: Owned execution list, normally produced by
///   `QueryModeSelection::modes`.
///
/// # Returns
///
/// One [`ModeReport`] per input mode in the same order. `recall_at_k` is the
/// arithmetic mean of `intersection_count / top_k`; `elapsed_ms` covers query
/// generation, exact computation, production I/O/CPU, and validation.
///
/// # Errors
///
/// Propagates query generation, exact scoring, storage, manifest, decode, and
/// production query failures. Returns `Integrity` if a response scanned any WAL
/// fragment or did not scan exactly one segment. Earlier mode reports are
/// discarded by the failed top-level run, and namespace cleanup does not occur.
///
/// # Side Effects
///
/// Spawns/join scoped exact-search threads, issues production object-store query
/// reads, and measures wall-clock time. It performs no writes or cache fills
/// because both optional query caches are `None`.
///
/// # Consistency
///
/// Eventual mode intentionally omits WAL upserts, but preparation proves there
/// are no WAL refs. Each cacheless query loads authoritative manifest-selected
/// segment state. The scanned-source checks guard against a future preparation
/// or query change silently contaminating the recall path.
///
/// # Performance
///
/// Exact work is parallel per mode; production requests are sequential. Building
/// a borrowed `HashSet<&str>` over approximate IDs makes intersection checks
/// `O(top_k)` without cloning result strings. Total elapsed time is unsuitable
/// as a pure approximate-query latency metric because it includes exact work.
///
/// # Examples
///
/// For `top_k=10`, eight IDs shared by exact and approximate results contribute
/// `0.8` for that query. One mode with per-query recalls `0.8` and `1.0` reports
/// `0.9`. A response reporting one WAL fragment aborts instead of recording it.
///
/// # Rust Notes for Java/C Engineers
///
/// `HashSet<&str>` borrows ID storage owned by `response`; no strings are copied,
/// and Rust prevents the set from outliving the response. `queries.iter().zip`
/// pairs equal-order inputs/results as zero-cost iterator state rather than
/// allocating tuple objects as a typical Java stream might.
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
                rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
                cache: None,
                manifest_cache: None,
                include_attributes: true,
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

/// Generates one deterministic batch for a concrete query family.
///
/// # Parameters
///
/// - `mode`: Query geometry to use.
/// - `queries_spec`: Count and per-mode seed.
/// - `dataset`: Source centroids and sigmas defining query dimensionality.
///
/// # Returns
///
/// Exactly `count_per_mode` owned query vectors in PRNG order.
///
/// # Errors
///
/// Propagates normalization failures from boundary or uniform generation.
/// Centroid generation is infallible after valid dataset preparation.
///
/// # Performance
///
/// Allocates one `dims`-length vector per query and advances one mode-specific
/// [`StdRng`]. Complexity is linear in `count_per_mode * dims`.
///
/// # Examples
///
/// Repeating boundary generation with the same query seed and dataset geometry
/// produces byte-identical query values; selecting uniform uses a distinct seed.
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

/// Draws an in-distribution query near one uniformly selected source centroid.
///
/// # Parameters
///
/// - `rng`: Mutable mode-specific PRNG.
/// - `dataset`: Generated centroids and per-cluster sigmas.
///
/// # Returns
///
/// One owned vector equal to the selected centroid plus independent Gaussian
/// noise at half that source cluster's sigma. It is not normalized here.
///
/// # Panics
///
/// Panics if the dataset has no centroids or sigmas are not aligned. Dataset
/// validation/generation establish both invariants.
///
/// # Examples
///
/// A cluster with sigma `0.1` produces query noise with standard deviation
/// `0.05`, making this the easiest near-center recall family.
fn generate_centroid_query(rng: &mut StdRng, dataset: &GeneratedDataset) -> Vec<f32> {
    let cluster_idx = rng.gen_range(0..dataset.centroids.len());
    let sigma = dataset.sigmas[cluster_idx] * 0.5;
    dataset.centroids[cluster_idx]
        .iter()
        .map(|component| component + gaussian(rng) * sigma)
        .collect()
}

/// Draws a normalized query near the midpoint of two distinct source clusters.
///
/// The second index is sampled from one fewer slot and shifted past the first,
/// guaranteeing distinct clusters without rejection-loop timing variability.
///
/// # Parameters
///
/// - `rng`: Mutable mode-specific PRNG.
/// - `dataset`: Generated source centroids.
///
/// # Returns
///
/// An owned unit vector formed from the centroid midpoint plus Gaussian noise
/// with fixed sigma `0.05` per dimension.
///
/// # Errors
///
/// Returns `Integrity` if the resulting vector cannot be normalized.
///
/// # Panics
///
/// Fewer than two centroids or mismatched centroid dimensions can panic during
/// range selection/indexing. Dataset validation requires at least two and
/// generation gives every centroid the same dimension.
///
/// # Examples
///
/// A query between clusters A and B can have true neighbors in both IVF cells,
/// making this mode sensitive to an overly small `nprobe`.
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

/// Draws a normalized random direction independent of source cluster centers.
///
/// # Parameters
///
/// - `rng`: Mutable mode-specific PRNG.
/// - `dataset`: Supplies dimensionality through its first centroid.
///
/// # Returns
///
/// An owned unit vector generated from independent uniform `[-1, 1)` values.
///
/// # Errors
///
/// Returns `Integrity` if the random vector is zero/near-zero or non-finite.
///
/// # Panics
///
/// Panics if no centroid exists; validated datasets always contain at least two.
///
/// # Examples
///
/// Uniform queries probe directions not intentionally near generated clusters,
/// exposing recall behavior outside the training distribution.
fn generate_uniform_query(rng: &mut StdRng, dataset: &GeneratedDataset) -> Result<Vec<f32>> {
    let dims = dataset.centroids[0].len();
    let values: Vec<f32> = (0..dims).map(|_| rng.gen_range(-1.0..1.0)).collect();
    normalize_strict(&values)
}

/// Deletes every object under the evaluator's unique namespace prefix.
///
/// # Parameters
///
/// - `store`: Shared authoritative object-store gateway.
/// - `namespace`: Exact UUID-based evaluation namespace.
///
/// # Returns
///
/// Number of objects accepted as deleted by the recursive store operation.
/// `run` treats zero as an integrity failure because a prepared namespace must
/// have created artifacts.
///
/// # Errors
///
/// Propagates storage/list/delete errors. The object-store operation may have
/// made partial cleanup progress before returning an error; callers should list
/// the prefix before retrying.
///
/// # Side Effects
///
/// Recursively removes namespace metadata, manifests, WAL, segment, and history
/// objects. No tombstone namespace state is preserved because this is isolated
/// benchmark cleanup, not the HTTP namespace-deletion lifecycle.
///
/// # Examples
///
/// Namespace `recall-eval-<uuid>` becomes prefix `recall-eval-<uuid>/`; a
/// successful prepared run deletes multiple objects and returns a positive count.
async fn cleanup_namespace(store: &ZeppelinStore, namespace: &str) -> Result<usize> {
    let prefix = format!("{namespace}/");
    Ok(store.delete_prefix(&prefix).await?)
}

/// Checks whether manifest cluster-object metadata exposes a valid SQ payload.
///
/// The helper first enforces that the requested logical cluster appears in at
/// most one grouped-object descriptor. It then loads that exact immutable object
/// and recognizes a manifest-published `ZBP5` row layout (decoding the cluster's
/// codes-only coarse block through the production SQ decoder), grouped `ZBP`
/// data, or a co-located `ZCL2` singleton. Legacy separate SQ keys are
/// deliberately checked by the caller instead.
///
/// # Parameters
///
/// - `store`: Authoritative object-store gateway.
/// - `segment`: Active manifest descriptor containing cluster-object refs.
/// - `cluster_idx`: Logical cluster whose SQ payload is required.
///
/// # Returns
///
/// `true` when the unique matching object contains a structurally valid SQ
/// section for the cluster; `false` when no descriptor matches or its recognized
/// layout does not carry that section.
///
/// # Errors
///
/// Returns `Integrity` for duplicate manifest ownership or malformed recognized
/// layouts, and propagates object-store/SQ decode failures for a selected ref.
/// An unrecognized object signature returns `false` so the caller can check
/// other persisted-layout representations.
///
/// # Side Effects
///
/// Performs no I/O when metadata contains no matching object, otherwise one full
/// object GET. It does not mutate cache or manifest state.
///
/// # Examples
///
/// One descriptor naming clusters `[0, 1]` loads its object and validates cluster
/// 0. Two descriptors both naming 0 fail instead of choosing the first.
///
/// # Rust Notes for Java/C Engineers
///
/// `matching_object` stores a borrowed descriptor reference. `Option::replace`
/// both installs the current borrow and reports a previous one, expressing the
/// uniqueness check without a nullable raw pointer; Rust guarantees the borrow
/// remains tied to `segment` through the later await preparation.
async fn manifest_sq_cluster_artifact_present(
    store: &ZeppelinStore,
    segment: &zeppelin::wal::manifest::SegmentRef,
    cluster_idx: usize,
) -> Result<bool> {
    let mut matching_object = None;
    for object_ref in &segment.cluster_objects {
        if object_ref.clusters.contains(&cluster_idx)
            && matching_object.replace(object_ref).is_some()
        {
            return Err(RecallEvalError::Integrity(format!(
                "cluster {cluster_idx} appears in multiple manifest cluster_objects"
            )));
        }
    }

    let Some(object_ref) = matching_object else {
        return Ok(false);
    };
    let data = store.get(&object_ref.key).await?;
    if let Some(layout) = object_ref
        .row_layouts
        .iter()
        .find(|layout| layout.cluster_idx == cluster_idx)
    {
        let coarse_range = row_layout_coarse_range(layout, data.len())?;
        deserialize_sq_codes_only(&data[coarse_range])?;
        return Ok(true);
    }
    if data.starts_with(b"ZBP") {
        return grouped_sq_cluster_present(&data, cluster_idx);
    }
    if data.starts_with(b"ZCL2") {
        return colocated_sq_cluster_present(&data);
    }
    Ok(false)
}

/// Narrows one manifest row layout's coarse block to a validated byte range.
///
/// # Parameters
///
/// - `layout`: Manifest-published row layout carrying absolute offsets.
/// - `object_len`: Total length of the fetched object the range must fit.
///
/// # Returns
///
/// The coarse block's `start..end` byte range inside the object.
///
/// # Errors
///
/// Returns `Integrity` for a zero-row layout, an offset or length that cannot
/// narrow to `usize`, a zero-length block, or a range outside the object.
fn row_layout_coarse_range(
    layout: &zeppelin::wal::manifest::ClusterRowLayoutRef,
    object_len: usize,
) -> Result<std::ops::Range<usize>> {
    if layout.row_count == 0 {
        return Err(RecallEvalError::Integrity(format!(
            "row layout for cluster {} has zero rows",
            layout.cluster_idx
        )));
    }
    let coarse_start = usize::try_from(layout.coarse_offset).map_err(|_| {
        RecallEvalError::Integrity(format!(
            "coarse offset {} does not fit usize",
            layout.coarse_offset
        ))
    })?;
    let coarse_len = usize::try_from(layout.coarse_len).map_err(|_| {
        RecallEvalError::Integrity(format!(
            "coarse length {} does not fit usize",
            layout.coarse_len
        ))
    })?;
    if coarse_len == 0 {
        return Err(RecallEvalError::Integrity(format!(
            "coarse block for cluster {} is empty",
            layout.cluster_idx
        )));
    }
    let coarse_end = coarse_start.checked_add(coarse_len).ok_or_else(|| {
        RecallEvalError::Integrity(format!(
            "coarse range overflows: offset={coarse_start}, len={coarse_len}"
        ))
    })?;
    validate_object_range(coarse_start, coarse_end, object_len, "cluster coarse block")?;
    Ok(coarse_start..coarse_end)
}

/// Proves one cluster's two-bit coarse block is addressable and in bounds.
///
/// Two-bit segments persist coarse codes only inside manifest-selected grouped
/// objects addressed by typed row layouts (`ZBP5`); the production two-bit
/// reader has no legacy key or sidecar fallback, so neither does this verifier.
/// The manifest-published coarse range is validated against the fetched object
/// without re-decoding the codes: decode correctness is exercised by the
/// subsequent production queries, which fail loud on any malformed payload.
///
/// # Parameters
///
/// - `store`: MinIO-backed gateway used for direct authoritative reads.
/// - `segment`: Manifest-resolved active descriptor owning cluster objects.
/// - `cluster_idx`: Logical cluster whose row layout must be present.
///
/// # Returns
///
/// `true` when the unique matching object declares a row layout for the cluster
/// whose non-empty coarse range fits inside the fetched object; `false` when no
/// descriptor matches or the matching descriptor declares no row layout for the
/// cluster.
///
/// # Errors
///
/// Returns `Integrity` for duplicate manifest ownership, a zero-row layout, a
/// zero-length or overflowing/out-of-bounds coarse range, or an offset that
/// cannot narrow to `usize`. Object-store GET failures propagate.
///
/// # Side Effects
///
/// Performs no I/O when metadata contains no matching object, otherwise one full
/// object GET. It does not mutate cache or manifest state.
async fn manifest_rq_cluster_artifact_present(
    store: &ZeppelinStore,
    segment: &zeppelin::wal::manifest::SegmentRef,
    cluster_idx: usize,
) -> Result<bool> {
    let mut matching_object = None;
    for object_ref in &segment.cluster_objects {
        if object_ref.clusters.contains(&cluster_idx)
            && matching_object.replace(object_ref).is_some()
        {
            return Err(RecallEvalError::Integrity(format!(
                "cluster {cluster_idx} appears in multiple manifest cluster_objects"
            )));
        }
    }

    let Some(object_ref) = matching_object else {
        return Ok(false);
    };
    let Some(layout) = object_ref
        .row_layouts
        .iter()
        .find(|layout| layout.cluster_idx == cluster_idx)
    else {
        return Ok(false);
    };
    let data = store.get(&object_ref.key).await?;
    row_layout_coarse_range(layout, data.len())?;
    Ok(true)
}

/// Parses a v4 grouped cluster object and validates one cluster's SQ/full ranges.
///
/// Directory entries contain logical cluster ID plus absolute SQ and full-vector
/// byte ranges. The helper validates global header/directory bounds, requires at
/// most one target entry, validates the target ranges and their ordering, and
/// decodes its SQ payload. Ranges for unrelated entries are not fully audited.
///
/// # Parameters
///
/// - `data`: Complete grouped object bytes.
/// - `cluster_idx`: Logical target cluster.
///
/// # Returns
///
/// `true` after a unique target SQ section decodes successfully. Returns `false`
/// for non-v4 signatures or a valid v4 directory without the target.
///
/// # Errors
///
/// Returns `Integrity` for short/empty/overflowing directories, duplicate target
/// entries, SQ inside the directory, overlapping SQ/full sections, or out-of-
/// bounds ranges. SQ payload decode errors propagate as Zeppelin errors.
///
/// # Performance
///
/// Scans `entry_count` fixed-size directory records and decodes only the target
/// SQ section. It allocates only error messages and the SQ decoder's result.
///
/// # Examples
///
/// A `ZBP4` object whose cluster-0 entry points to a valid serialized SQ cluster
/// returns true. A `ZBP` object of another version returns false so another
/// compatibility layout can be checked.
///
/// # Rust Notes for Java/C Engineers
///
/// Checked offset arithmetic and borrowed slices replace C pointer stepping.
/// No slice is constructed until its start/end have been proven within `data`;
/// unlike a Java buffer view, the resulting borrow also cannot outlive `data`.
fn grouped_sq_cluster_present(data: &[u8], cluster_idx: usize) -> Result<bool> {
    /// Bytes in the grouped magic/version and entry-count prefix.
    const HEADER_LEN: usize = 8;
    /// Bytes in one v4 directory record: cluster plus four `u64` fields.
    const V4_DIR_ENTRY_LEN: usize = 36;

    if !is_v4_cluster_data_object(data) {
        return Ok(false);
    }
    if data.len() < HEADER_LEN {
        return Err(RecallEvalError::Integrity(
            "cluster data object too small for header".into(),
        ));
    }
    let entry_count = read_u32_usize(data, 4, "cluster data object entry count")?;
    if entry_count == 0 {
        return Err(RecallEvalError::Integrity(
            "cluster data object has zero entries".into(),
        ));
    }
    let directory_len = entry_count.checked_mul(V4_DIR_ENTRY_LEN).ok_or_else(|| {
        RecallEvalError::Integrity("v4 cluster data object directory overflows".into())
    })?;
    let payload_start = HEADER_LEN.checked_add(directory_len).ok_or_else(|| {
        RecallEvalError::Integrity("v4 cluster data object header overflows".into())
    })?;
    if data.len() < payload_start {
        return Err(RecallEvalError::Integrity(format!(
            "v4 cluster data object truncated directory: expected at least {payload_start}, got {}",
            data.len()
        )));
    }

    let mut found = false;
    for entry_idx in 0..entry_count {
        let base = HEADER_LEN + entry_idx * V4_DIR_ENTRY_LEN;
        let entry_cluster = read_u32_usize(data, base, "v4 cluster object index")?;
        if entry_cluster != cluster_idx {
            continue;
        }
        if found {
            return Err(RecallEvalError::Integrity(format!(
                "duplicate cluster {cluster_idx} in v4 cluster data object"
            )));
        }
        found = true;

        let sq_offset = read_u64_usize(data, base + 4, "v4 cluster object SQ offset")?;
        let sq_len = read_u64_usize(data, base + 12, "v4 cluster object SQ length")?;
        let full_offset = read_u64_usize(data, base + 20, "v4 cluster object full offset")?;
        let full_len = read_u64_usize(data, base + 28, "v4 cluster object full length")?;
        if sq_offset < payload_start {
            return Err(RecallEvalError::Integrity(format!(
                "v4 cluster SQ section starts inside directory: offset={sq_offset}, payload_start={payload_start}"
            )));
        }
        let sq_end = sq_offset.checked_add(sq_len).ok_or_else(|| {
            RecallEvalError::Integrity(format!(
                "v4 cluster SQ section overflows: offset={sq_offset}, len={sq_len}"
            ))
        })?;
        if full_offset < sq_end {
            return Err(RecallEvalError::Integrity(format!(
                "v4 cluster full section overlaps SQ section: full_offset={full_offset}, sq_end={sq_end}"
            )));
        }
        let full_end = full_offset.checked_add(full_len).ok_or_else(|| {
            RecallEvalError::Integrity(format!(
                "v4 cluster full section overflows: offset={full_offset}, len={full_len}"
            ))
        })?;
        validate_object_range(sq_offset, sq_end, data.len(), "v4 cluster SQ section")?;
        validate_object_range(full_offset, full_end, data.len(), "v4 cluster full section")?;
        deserialize_sq_cluster(&data[sq_offset..sq_end])?;
    }

    Ok(found)
}

/// Validates a v2 singleton object containing adjacent SQ and full-vector payloads.
///
/// `ZCL2` fixes the SQ section immediately after its 36-byte header and requires
/// the full section to begin exactly where SQ ends. This verifier also requires
/// the full section to end at object length, rejecting gaps or trailing bytes.
///
/// # Parameters
///
/// - `data`: Complete candidate singleton object bytes.
///
/// # Returns
///
/// `true` when a `ZCL2` object has valid offsets/ranges and a decodable SQ child;
/// `false` for any other signature.
///
/// # Errors
///
/// Returns `Integrity` for a short header, offset mismatch, arithmetic overflow,
/// out-of-bounds section, or trailing bytes. SQ decoding errors propagate.
///
/// # Examples
///
/// Header, SQ bytes, then full bytes with exact contiguous offsets returns true.
/// Moving the full offset one byte forward fails rather than accepting a gap.
fn colocated_sq_cluster_present(data: &[u8]) -> Result<bool> {
    /// Fixed `ZCL2` signature and four offset/length fields.
    const HEADER_LEN: usize = 36;

    if !data.starts_with(b"ZCL2") {
        return Ok(false);
    }
    if data.len() < HEADER_LEN {
        return Err(RecallEvalError::Integrity(
            "v2 cluster blob too small for header".into(),
        ));
    }

    let sq_offset = read_u64_usize(data, 4, "v2 cluster SQ offset")?;
    let sq_len = read_u64_usize(data, 12, "v2 cluster SQ length")?;
    let full_offset = read_u64_usize(data, 20, "v2 cluster full offset")?;
    let full_len = read_u64_usize(data, 28, "v2 cluster full length")?;
    if sq_offset != HEADER_LEN {
        return Err(RecallEvalError::Integrity(format!(
            "v2 cluster SQ offset mismatch: expected {HEADER_LEN}, got {sq_offset}"
        )));
    }
    let sq_end = sq_offset.checked_add(sq_len).ok_or_else(|| {
        RecallEvalError::Integrity(format!(
            "v2 cluster SQ section overflows: offset={sq_offset}, len={sq_len}"
        ))
    })?;
    if full_offset != sq_end {
        return Err(RecallEvalError::Integrity(format!(
            "v2 cluster full offset mismatch: expected {sq_end}, got {full_offset}"
        )));
    }
    let full_end = full_offset.checked_add(full_len).ok_or_else(|| {
        RecallEvalError::Integrity(format!(
            "v2 cluster full section overflows: offset={full_offset}, len={full_len}"
        ))
    })?;
    validate_object_range(sq_offset, sq_end, data.len(), "v2 cluster SQ section")?;
    validate_object_range(full_offset, full_end, data.len(), "v2 cluster full section")?;
    if full_end != data.len() {
        return Err(RecallEvalError::Integrity(format!(
            "v2 cluster blob size mismatch: expected {full_end}, got {}",
            data.len()
        )));
    }
    deserialize_sq_cluster(&data[sq_offset..sq_end])?;
    Ok(true)
}

/// Recognizes the four-byte grouped-cluster v4 signature.
///
/// # Parameters
///
/// - `data`: Candidate bytes; a complete object is not required.
///
/// # Returns
///
/// `true` only for at least four bytes beginning with `ZBP` and version byte 4.
///
/// # Examples
///
/// `b"ZBP\x04"` returns true, while `b"ZBP\x01"` and a three-byte prefix return
/// false without indexing out of bounds.
fn is_v4_cluster_data_object(data: &[u8]) -> bool {
    data.len() >= 4 && &data[0..3] == b"ZBP" && data[3] == 4
}

/// Reads one little-endian `u32` field through bounds-checked offset arithmetic.
///
/// # Parameters
///
/// - `data`: Complete or partial object bytes.
/// - `offset`: Start byte of the four-byte field.
/// - `label`: Human context copied into integrity diagnostics.
///
/// # Returns
///
/// The decoded value converted to `usize` for local indexing.
///
/// # Errors
///
/// Returns `Integrity` if `offset + 4` overflows, exceeds the buffer, or the
/// fixed-width slice conversion fails.
///
/// # Examples
///
/// Bytes `[1,0,0,0]` at offset zero return `1`; a two-byte buffer fails with the
/// supplied label.
fn read_u32_usize(data: &[u8], offset: usize, label: &str) -> Result<usize> {
    let end = offset.checked_add(4).ok_or_else(|| {
        RecallEvalError::Integrity(format!("{label} offset overflows: offset={offset}"))
    })?;
    if end > data.len() {
        return Err(RecallEvalError::Integrity(format!(
            "{label} out of bounds: offset={offset}, len={}",
            data.len()
        )));
    }
    Ok(u32::from_le_bytes(
        data[offset..end]
            .try_into()
            .map_err(|_| RecallEvalError::Integrity(format!("{label} parse error")))?,
    ) as usize)
}

/// Reads one little-endian `u64` field and verifies it fits local address space.
///
/// # Parameters
///
/// - `data`: Complete or partial object bytes.
/// - `offset`: Start byte of the eight-byte field.
/// - `label`: Human context copied into integrity diagnostics.
///
/// # Returns
///
/// The decoded platform-sized value.
///
/// # Errors
///
/// Returns `Integrity` for offset overflow, a truncated field, failed fixed-slice
/// conversion, or a value larger than this platform's `usize`.
///
/// # Examples
///
/// Eight little-endian bytes for 36 return `36`; a value too large for the
/// target platform fails before it can be used as a slice index.
fn read_u64_usize(data: &[u8], offset: usize, label: &str) -> Result<usize> {
    let end = offset.checked_add(8).ok_or_else(|| {
        RecallEvalError::Integrity(format!("{label} offset overflows: offset={offset}"))
    })?;
    if end > data.len() {
        return Err(RecallEvalError::Integrity(format!(
            "{label} out of bounds: offset={offset}, len={}",
            data.len()
        )));
    }
    let value = u64::from_le_bytes(
        data[offset..end]
            .try_into()
            .map_err(|_| RecallEvalError::Integrity(format!("{label} parse error")))?,
    );
    usize::try_from(value)
        .map_err(|_| RecallEvalError::Integrity(format!("{label} does not fit in usize: {value}")))
}

/// Confirms that a half-open byte range is ordered and contained in an object.
///
/// # Parameters
///
/// - `start`: Inclusive byte offset.
/// - `end`: Exclusive byte offset, already computed with checked arithmetic.
/// - `object_len`: Complete object byte length.
/// - `label`: Context included in any integrity error.
///
/// # Returns
///
/// `Ok(())` for `start <= end <= object_len`, including empty ranges.
///
/// # Errors
///
/// Returns `Integrity` for reversed or out-of-bounds ranges.
///
/// # Examples
///
/// Range `4..8` in an eight-byte object passes; `8..9` and `5..4` fail.
fn validate_object_range(start: usize, end: usize, object_len: usize, label: &str) -> Result<()> {
    if start > end || end > object_len {
        return Err(RecallEvalError::Integrity(format!(
            "{label} out of bounds: start={start}, end={end}, len={object_len}"
        )));
    }
    Ok(())
}

/// Maps quantization enum variants to stable report labels.
///
/// # Parameters
///
/// - `value`: Copyable production quantization variant.
///
/// # Returns
///
/// One process-lifetime label: `None`, `Scalar`, `TwoBit`, or `Product`.
///
/// # Examples
///
/// `QuantizationType::TwoBit` renders as `TwoBit`, the production default this
/// evaluator measures; `QuantizationType::Scalar` renders as `Scalar`, the SQ8
/// arm selected with `ZEPPELIN_QUANTIZATION=scalar`.
fn quantization_name(value: QuantizationType) -> &'static str {
    match value {
        QuantizationType::None => "None",
        QuantizationType::Scalar => "Scalar",
        QuantizationType::TwoBit => "TwoBit",
        QuantizationType::Product => "Product",
    }
}

/// Maps a manifest coarse payload encoding tag to a stable report label.
///
/// # Examples
///
/// `CoarsePayloadEncoding::TwoBit` renders as `two_bit`, matching its serde
/// spelling in the manifest.
fn coarse_payload_encoding_name(value: CoarsePayloadEncoding) -> &'static str {
    match value {
        CoarsePayloadEncoding::Sq8 => "sq8",
        CoarsePayloadEncoding::TwoBit => "two_bit",
    }
}

/// Prints a compact line-oriented report for interactive use.
///
/// # Parameters
///
/// - `report`: Borrowed completed report retained by the caller.
///
/// # Returns
///
/// Returns `()` after writing the header, provenance, effective config, segment
/// verification, and one fixed-six-decimal recall line per mode.
///
/// # Side Effects
///
/// Writes multiple lines to process stdout. It does not expose seed contents,
/// only the seed-file path and holdout generation.
///
/// # Panics
///
/// Rust's standard `println!` machinery can panic if stdout writing fails, for
/// example after a downstream pipe closes early.
///
/// # Examples
///
/// An all-mode report prints a `recall@10:` heading followed by centroid,
/// boundary, and uniform values with elapsed milliseconds.
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
        "segment: compacted={} quantization={} wal_fragments={} coarse_encoding={} sq_calibration={} sq_cluster_0={} rq_cluster_0={}",
        report.segment_verification.compacted_segment,
        report.segment_verification.segment_quantization,
        report.segment_verification.wal_fragments_after_compaction,
        report.segment_verification.coarse_payload_encoding,
        report.segment_verification.sq_calibration_present,
        report.segment_verification.sq_cluster_zero_present,
        report.segment_verification.rq_cluster_zero_present
    );
    println!("recall@{}:", report.config.top_k);
    for mode in &report.modes {
        println!(
            "  {:<8} {:.6} ({} ms)",
            mode.mode, mode.recall_at_k, mode.elapsed_ms
        );
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Focused artifact-layout regression tests for the recall gate.
    //!
    //! These tests use an in-memory object store because they verify deterministic
    //! manifest and binary-layout interpretation, not MinIO transport behavior.
    //! The main scenario protects the modern grouped SQ8 layout: if the verifier
    //! recognizes only legacy separate cluster keys, the production recall gate
    //! would reject a valid compacted segment before measuring it.
    //!
    //! ```text
    //! in-memory manifest -> active Scalar segment -> grouped cluster ref
    //!          |                                      |
    //!          +--> legacy centroids + calibration    +--> ZBP4 SQ payload
    //!                                  \              /
    //!                                   v            v
    //!                            verifier accepts cluster 0
    //! ```

    use bytes::Bytes;
    use object_store::memory::InMemory;
    use std::sync::Arc;
    use zeppelin::index::ivf_flat::build::centroids_key;
    use zeppelin::index::quantization::sq::{
        serialize_sq_cluster, sq_calibration_key, SqCalibration,
    };
    use zeppelin::index::quantization::QuantizationType;
    use zeppelin::storage::ZeppelinStore;
    use zeppelin::wal::manifest::{
        ClusterDataObjectRef, ClusterRowLayoutRef, Manifest, SegmentRef,
    };

    use super::*;

    #[test]
    fn offline_evaluator_config_does_not_require_server_credentials() {
        let config = recall_eval_config().expect("offline evaluator defaults must validate");

        assert_eq!(
            config.security.mode,
            zeppelin::config::SecurityMode::OpenUnsafe
        );
        assert!(config.security.api_keys.is_empty());
        config.validate().unwrap();
    }

    /// Proves grouped cluster metadata and a valid v4 SQ child satisfy verification.
    ///
    /// The fixture publishes one active scalar segment, legacy-format centroids
    /// with a separate calibration object, and one manifest-referenced `ZBP4`
    /// group containing cluster zero. Acceptance protects layout compatibility
    /// while still exercising the real manifest/store/SQ decoders.
    ///
    /// # Side Effects
    ///
    /// Writes isolated objects to an in-memory store only; the store drops at
    /// test completion and no external service is required.
    ///
    /// # Failure protected against
    ///
    /// The test fails if grouped-object directory offsets drift, the verifier
    /// falls back only to legacy SQ keys, or manifest cluster-object selection no
    /// longer recognizes a valid scalar segment.
    #[tokio::test]
    async fn verifier_accepts_grouped_sq8_cluster_artifact() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let namespace = "recall-eval-test";
        let segment_id = "seg_grouped";
        let group_key = format!("{namespace}/segments/{segment_id}/cluster_group_0.bin");

        let manager = NamespaceManager::new(store.clone());
        let metadata = manager
            .create(namespace, 2, DistanceMetric::Euclidean)
            .await
            .unwrap();
        let authoritative_origin = metadata.artifact_origin().unwrap().unwrap();
        let mut manifest = Manifest::read(&store, namespace).await.unwrap().unwrap();
        manifest.add_segment(SegmentRef {
            id: segment_id.to_string(),
            vector_count: 1,
            cluster_count: 1,
            quantization: QuantizationType::Scalar,
            hierarchical: false,
            bitmap_fields: Vec::new(),
            fts_fields: Vec::new(),
            has_global_fts: false,
            cluster_owners: Vec::new(),
            sketch: None,
            cluster_objects: vec![ClusterDataObjectRef {
                key: group_key.clone(),
                clusters: vec![0],
                live_offset: 0,
                live_len: 0,
                size_bytes: 0,
                cluster_layout_version: 0,
                row_layouts: Vec::new(),
            }],
            bootstrap: None,
            membership: None,
            artifact_origin: None,
        });
        manifest.write(&store, namespace).await.unwrap();

        let calibration = SqCalibration::calibrate(&[&[0.0_f32, 1.0][..]], 2);
        store
            .put(
                &centroids_key(namespace, segment_id),
                legacy_centroids_blob(1, 2),
            )
            .await
            .unwrap();
        store
            .put(
                &sq_calibration_key(namespace, segment_id),
                calibration.to_bytes(),
            )
            .await
            .unwrap();

        let sq = serialize_sq_cluster(&["v0".to_string()], &[vec![0, 255]], 2).unwrap();
        store
            .put(&group_key, grouped_cluster_object(0, &sq))
            .await
            .unwrap();

        let summary = verify_compacted_segment(&store, namespace, &authoritative_origin, 1)
            .await
            .expect("grouped SQ8 cluster object should satisfy recall verifier");
        assert!(summary.sq_cluster_zero_present);
        assert_eq!(summary.coarse_payload_encoding, CoarsePayloadEncoding::Sq8);
        assert!(!summary.rq_cluster_zero_present);
    }

    /// Proves a tagged two-bit segment with a row-layout coarse block verifies.
    ///
    /// The fixture publishes one active two-bit segment carrying the manifest
    /// `CoarsePayloadEncoding::TwoBit` tag, a centroids object, and one
    /// manifest-referenced `ZBP5` group whose row layout addresses cluster
    /// zero's coarse block. Acceptance protects the default production path:
    /// without the two-bit arm the verifier would reject the segment the
    /// evaluator just compacted.
    ///
    /// # Side Effects
    ///
    /// Writes isolated objects to an in-memory store only; the store drops at
    /// test completion and no external service is required.
    ///
    /// # Failure protected against
    ///
    /// The test fails if the verifier stops recognizing the manifest two-bit
    /// tag, ignores manifest-published row layouts, or rejects an in-bounds
    /// cluster-zero coarse block.
    #[tokio::test]
    async fn verifier_accepts_two_bit_row_layout_segment() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let namespace = "recall-eval-two-bit";
        let segment_id = "seg_two_bit";
        let group_key = format!("{namespace}/segments/{segment_id}/cluster_group_0.bin");

        let manager = NamespaceManager::new(store.clone());
        let metadata = manager
            .create(namespace, 2, DistanceMetric::Euclidean)
            .await
            .unwrap();
        let authoritative_origin = metadata.artifact_origin().unwrap().unwrap();
        let mut manifest = Manifest::read(&store, namespace).await.unwrap().unwrap();
        manifest.add_segment(row_layout_segment_ref(
            segment_id,
            &group_key,
            QuantizationType::TwoBit,
            24,
        ));
        manifest.set_coarse_payload_encoding(segment_id, CoarsePayloadEncoding::TwoBit);
        manifest.write(&store, namespace).await.unwrap();

        store
            .put(
                &centroids_key(namespace, segment_id),
                legacy_centroids_blob(1, 2),
            )
            .await
            .unwrap();
        store
            .put(&group_key, Bytes::from(vec![0_u8; 48]))
            .await
            .unwrap();

        let summary = verify_compacted_segment(&store, namespace, &authoritative_origin, 1)
            .await
            .expect("tagged two-bit segment with row layout should satisfy recall verifier");
        assert_eq!(summary.quantization, QuantizationType::TwoBit);
        assert_eq!(
            summary.coarse_payload_encoding,
            CoarsePayloadEncoding::TwoBit
        );
        assert!(summary.rq_cluster_zero_present);
        assert!(!summary.sq_calibration_present);
        assert!(!summary.sq_cluster_zero_present);
    }

    /// Proves an untagged two-bit segment fails loudly instead of verifying.
    ///
    /// An untagged segment decodes as SQ8 by default, so accepting it would
    /// let the evaluator measure a two-bit segment while reporting SQ8
    /// evidence. The verifier must refuse before any query runs.
    #[tokio::test]
    async fn verifier_rejects_two_bit_segment_without_encoding_tag() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let namespace = "recall-eval-two-bit-untagged";
        let segment_id = "seg_two_bit_untagged";
        let group_key = format!("{namespace}/segments/{segment_id}/cluster_group_0.bin");

        let manager = NamespaceManager::new(store.clone());
        let metadata = manager
            .create(namespace, 2, DistanceMetric::Euclidean)
            .await
            .unwrap();
        let authoritative_origin = metadata.artifact_origin().unwrap().unwrap();
        let mut manifest = Manifest::read(&store, namespace).await.unwrap().unwrap();
        manifest.add_segment(row_layout_segment_ref(
            segment_id,
            &group_key,
            QuantizationType::TwoBit,
            24,
        ));
        manifest.write(&store, namespace).await.unwrap();

        store
            .put(
                &centroids_key(namespace, segment_id),
                legacy_centroids_blob(1, 2),
            )
            .await
            .unwrap();
        store
            .put(&group_key, Bytes::from(vec![0_u8; 48]))
            .await
            .unwrap();

        let error = verify_compacted_segment(&store, namespace, &authoritative_origin, 1)
            .await
            .expect_err("untagged two-bit segment must not verify");
        assert!(
            matches!(error, RecallEvalError::Integrity(_)),
            "expected integrity error, got {error}"
        );
    }

    /// Proves a scalar segment with a `ZBP5` row-layout coarse block verifies.
    ///
    /// Current compaction publishes `ZBP5` grouped objects whose per-cluster
    /// coarse block is a codes-only SQ payload addressed by the manifest row
    /// layout, not a v4 directory entry. Acceptance protects the SQ8 arm
    /// against rejecting the artifacts production compaction actually writes.
    ///
    /// # Side Effects
    ///
    /// Writes isolated objects to an in-memory store only; the store drops at
    /// test completion and no external service is required.
    ///
    /// # Failure protected against
    ///
    /// The test fails if the SQ8 arm stops recognizing manifest-published row
    /// layouts or no longer decodes the codes-only coarse block through the
    /// production SQ decoder.
    #[tokio::test]
    async fn verifier_accepts_sq8_v5_row_layout_segment() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let namespace = "recall-eval-sq8-v5";
        let segment_id = "seg_sq8_v5";
        let group_key = format!("{namespace}/segments/{segment_id}/cluster_group_0.bin");

        let manager = NamespaceManager::new(store.clone());
        let metadata = manager
            .create(namespace, 2, DistanceMetric::Euclidean)
            .await
            .unwrap();
        let authoritative_origin = metadata.artifact_origin().unwrap().unwrap();
        let mut manifest = Manifest::read(&store, namespace).await.unwrap().unwrap();
        manifest.add_segment(row_layout_segment_ref(
            segment_id,
            &group_key,
            QuantizationType::Scalar,
            10,
        ));
        manifest.write(&store, namespace).await.unwrap();

        let calibration = SqCalibration::calibrate(&[&[0.0_f32, 1.0][..]], 2);
        store
            .put(
                &centroids_key(namespace, segment_id),
                legacy_centroids_blob(1, 2),
            )
            .await
            .unwrap();
        store
            .put(
                &sq_calibration_key(namespace, segment_id),
                calibration.to_bytes(),
            )
            .await
            .unwrap();

        let mut object = Vec::new();
        object.extend_from_slice(&1_u32.to_le_bytes());
        object.extend_from_slice(&2_u32.to_le_bytes());
        object.extend_from_slice(&[0_u8, 255]);
        object.resize(48, 0);
        store.put(&group_key, Bytes::from(object)).await.unwrap();

        let summary = verify_compacted_segment(&store, namespace, &authoritative_origin, 1)
            .await
            .expect("SQ8 segment with v5 row layout should satisfy recall verifier");
        assert!(summary.sq_calibration_present);
        assert!(summary.sq_cluster_zero_present);
    }

    /// Builds one active-segment descriptor with a `ZBP5` row layout.
    ///
    /// # Parameters
    ///
    /// - `segment_id`: Logical segment identifier recorded in the descriptor.
    /// - `group_key`: Object key owning cluster zero's coarse block.
    /// - `quantization`: Coarse encoding the descriptor advertises.
    /// - `coarse_len`: Byte length of cluster zero's coarse block at offset 0.
    ///
    /// # Returns
    ///
    /// A descriptor advertising one cluster whose coarse block starts at
    /// offset zero of a 48-byte grouped object.
    fn row_layout_segment_ref(
        segment_id: &str,
        group_key: &str,
        quantization: QuantizationType,
        coarse_len: u64,
    ) -> SegmentRef {
        SegmentRef {
            id: segment_id.to_string(),
            vector_count: 1,
            cluster_count: 1,
            quantization,
            hierarchical: false,
            bitmap_fields: Vec::new(),
            fts_fields: Vec::new(),
            has_global_fts: false,
            cluster_owners: Vec::new(),
            sketch: None,
            cluster_objects: vec![ClusterDataObjectRef {
                key: group_key.to_string(),
                clusters: vec![0],
                live_offset: 0,
                live_len: 0,
                size_bytes: 48,
                cluster_layout_version: 5,
                row_layouts: vec![ClusterRowLayoutRef {
                    cluster_idx: 0,
                    row_count: 1,
                    coarse_offset: 0,
                    coarse_len,
                    ids_offset: coarse_len,
                    ids_len: 8,
                    vectors_offset: coarse_len + 8,
                    vectors_len: 8,
                }],
            }],
            bootstrap: None,
            membership: None,
            artifact_origin: None,
        }
    }

    /// Builds the minimal legacy centroids bytes required by the verifier fixture.
    ///
    /// # Parameters
    ///
    /// - `num_centroids`: Header centroid count.
    /// - `dim`: Header dimension; the fixture appends eight zero payload bytes.
    ///
    /// # Returns
    ///
    /// Immutable bytes containing little-endian count, dimension, and the fixed
    /// test payload. Calibration remains a separate object in this legacy path.
    ///
    /// # Examples
    ///
    /// `(1, 2)` describes one two-dimensional zero centroid.
    fn legacy_centroids_blob(num_centroids: u32, dim: u32) -> Bytes {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&num_centroids.to_le_bytes());
        bytes.extend_from_slice(&dim.to_le_bytes());
        bytes.extend_from_slice(&[0_u8; 8]);
        Bytes::from(bytes)
    }

    /// Encodes one-cluster `ZBP4` grouped data with contiguous SQ/full sections.
    ///
    /// # Parameters
    ///
    /// - `cluster_idx`: Logical cluster written into the sole directory entry.
    /// - `sq`: Borrowed serialized SQ child bytes copied into the object.
    ///
    /// # Returns
    ///
    /// Immutable grouped bytes whose full-vector child is an empty legacy cluster
    /// with dimension two. All offsets are absolute and little-endian.
    ///
    /// # Examples
    ///
    /// Cluster zero and a valid two-dimensional SQ payload produce exactly the
    /// layout parsed by `grouped_sq_cluster_present`.
    fn grouped_cluster_object(cluster_idx: u32, sq: &[u8]) -> Bytes {
        /// Four-byte signature/version plus four-byte entry count.
        const HEADER_LEN: usize = 8;
        /// One cluster ID and four `u64` offset/length values.
        const DIR_ENTRY_LEN: usize = 36;

        let sq_offset = (HEADER_LEN + DIR_ENTRY_LEN) as u64;
        let sq_len = sq.len() as u64;
        let full_offset = sq_offset + sq_len;
        let full = legacy_empty_cluster_blob(2);
        let full_len = full.len() as u64;

        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"ZBP\x04");
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&cluster_idx.to_le_bytes());
        bytes.extend_from_slice(&sq_offset.to_le_bytes());
        bytes.extend_from_slice(&sq_len.to_le_bytes());
        bytes.extend_from_slice(&full_offset.to_le_bytes());
        bytes.extend_from_slice(&full_len.to_le_bytes());
        bytes.extend_from_slice(sq);
        bytes.extend_from_slice(&full);
        Bytes::from(bytes)
    }

    /// Builds a legacy full-vector cluster header containing zero rows.
    ///
    /// # Parameters
    ///
    /// - `dim`: Coordinate dimension recorded in the header.
    ///
    /// # Returns
    ///
    /// Eight owned bytes: zero row count followed by little-endian dimension.
    ///
    /// # Examples
    ///
    /// Dimension two yields a structurally valid empty full-vector child for the
    /// grouped SQ fixture.
    fn legacy_empty_cluster_blob(dim: u32) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.extend_from_slice(&dim.to_le_bytes());
        bytes
    }
}
