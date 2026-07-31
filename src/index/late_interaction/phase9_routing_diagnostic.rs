//! Ignored, in-memory diagnosis for the MMLI-2 Phase 9 routing gate.
//!
//! This lives beside the candidate module so the diagnostic can exercise the
//! crate-private persisted builder and resident search seam without widening
//! Zeppelin's production API. It performs no object-store I/O.

use std::collections::{BTreeSet, HashSet};
use std::env;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Mutex;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::embedding::artifact::{decode_matrix_payload, encode_matrix_payload};
use crate::embedding::{
    ArtifactChecksum, ContentHash, FdeGenerationId, MatrixDtype, MultiVectorEmbedding,
};
use crate::index::distance::{dot_product_distance, euclidean_distance};
use crate::index::ivf_flat::kmeans::train_kmeans;
use crate::index::quantization::sq::SqCalibration;

use super::candidate::{
    build_late_candidate_index, AttributeLocator, BuiltLateCandidateIndex, FdeCandidateIndex,
    FetchedLateCandidateCluster, LateCandidateBuildConfig, LateCandidateInputRow,
    LateRoutingMetric, ResidentLateCandidateIndex,
};
use super::{
    FdeAlgorithmVersion, FdeParams, FdeTransform, FinalProjection, InnerProjection,
    MatrixBlockLocator, MultiVectorMatrixRef,
};

const DOCUMENT_DIGEST: &str = "1960f7bc88a667beb76b6e15a750469e615aafe9a925928c23f7c546d12cfe22";
const QUERY_DIGEST: &str = "cefbff5713a3944f4007676b243985f393f87a7a7579bb5cba6ca09899b0aa0c";
const TRANSFORM_DIGEST: &str = "00ad4edb4292ddd64c6df00c84c2f8dfced3a092d9ddc307239d9e070deb2ad4";
const SOURCE_REVISION: &str = "518aa23";
const FDE_SEED: u64 = 0x4d4d_4c49_0000_0002;
const DIMENSION: usize = 128;
const FDE_DIMENSION: usize = 10_240;
const GOLD_PER_QUERY: usize = 10;
const CENTERING_SAMPLE_ROWS: usize = 5_000;
const CANDIDATE_BYTES_PER_ROW: u64 = (FDE_DIMENSION * size_of::<f32>()) as u64;
const PRODUCTION_PARITY_NPROBE: usize = 64;
const PRODUCTION_PARITY_K: usize = 1_000;
const PRODUCTION_PARITY_QUERY_COUNT: usize = 20;
const PRODUCTION_KMEANS_EPSILON: f64 = 1.0e-4_f32 as f64;
const K_VALUES: &[usize] = &[100, 300, 537, 700, 1_000, 1_500, 2_000, 3_000, 4_000, 5_183];
const NLIST_VALUES: &[usize] = &[16, 32, 64, 128, 256, 512];

type DiagnosticResult<T> = Result<T, String>;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum TensorDtype {
    F16,
    F32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TensorSidecar {
    rows: Vec<usize>,
    dim: usize,
    dtype: TensorDtype,
    ids: Vec<String>,
}

struct TensorSet {
    sidecar: TensorSidecar,
    values: Vec<f32>,
    scalar_offsets: Vec<usize>,
    total_rows: usize,
    composite_digest: String,
}

impl TensorSet {
    fn matrix(&self, index: usize) -> DiagnosticResult<MultiVectorMatrixRef<'_>> {
        let rows = *self
            .sidecar
            .rows
            .get(index)
            .ok_or_else(|| format!("matrix index {index} is out of bounds"))?;
        let start = self.scalar_offsets[index];
        let end = start
            .checked_add(rows * self.sidecar.dim)
            .ok_or_else(|| "matrix slice overflows".to_string())?;
        MultiVectorMatrixRef::new(&self.values[start..end], rows, self.sidecar.dim, rows)
            .map_err(|error| error.to_string())
    }

    fn matrix_truth_bytes(&self, index: usize) -> u64 {
        (self.sidecar.rows[index] * self.sidecar.dim * size_of::<u16>()) as u64
    }
}

#[derive(Clone, Copy)]
struct GoldDocument {
    document_index: usize,
    exact_rank: usize,
    durable_fde_rank: usize,
}

#[derive(Deserialize)]
struct DiagnosticsFile {
    cells: Vec<DiagnosticCell>,
}

#[derive(Deserialize)]
struct DiagnosticCell {
    config: String,
    gold_ranks: Vec<DurableGoldRank>,
}

#[derive(Deserialize)]
struct DurableGoldRank {
    query_index: usize,
    query_id: String,
    document_index: usize,
    document_id: String,
    exact_rank: usize,
    fde_rank: usize,
}

struct FdeVariants {
    lab_documents: Vec<Vec<f32>>,
    lab_queries: Vec<Vec<f32>>,
    f32_mean_documents: Vec<Vec<f32>>,
    f32_mean_queries: Vec<Vec<f32>>,
    production_documents: Vec<Vec<f32>>,
    production_queries: Vec<Vec<f32>>,
}

#[derive(Clone, Copy)]
enum DotArithmetic {
    LabScalar,
    Production,
}

#[derive(Serialize)]
struct DiagnosticReport {
    schema_version: u32,
    source_revision: &'static str,
    runtime_ms: u128,
    inputs: InputReport,
    fde_geometry: FdeGeometryReport,
    full_probe_parity: FullProbeParityReport,
    sq8_exhaustive: Sq8Report,
    kmeans_builds: Vec<KmeansBuildReport>,
    iteration_comparison: IterationComparison,
    cells: Vec<FrontierCell>,
    dynamic_probe_cells: Vec<DynamicProbeCell>,
    production_oracle: ProductionOracleReport,
    gold_cluster_ranks: Vec<GoldClusterRank>,
}

#[derive(Serialize)]
struct InputReport {
    document_count: usize,
    query_count: usize,
    gold_memberships: usize,
    document_rows: usize,
    query_rows: usize,
    dimension: usize,
    document_composite_sha256: String,
    query_composite_sha256: String,
    transform_sha256: String,
    document_f16_bytes: u64,
    query_f16_bytes: u64,
}

#[derive(Serialize)]
struct FdeGeometryReport {
    dimension: usize,
    bytes_per_row: u64,
    corpus_f32_bytes: u64,
    lab_document_sha256: String,
    lab_query_sha256: String,
    f32_mean_document_sha256: String,
    f32_mean_query_sha256: String,
    production_document_sha256: String,
    production_query_sha256: String,
    document_norms: NumericSummary,
    query_norms: NumericSummary,
    document_norm_cv: f64,
    query_norm_cv: f64,
}

#[derive(Serialize)]
struct FullProbeParityReport {
    candidate_k: usize,
    durable_phase2_hits: usize,
    variants: Vec<FullProbeVariant>,
    cumulative_deltas: Vec<MembershipDelta>,
}

#[derive(Serialize)]
struct FullProbeVariant {
    name: &'static str,
    hits: usize,
    recall: f64,
}

#[derive(Serialize)]
struct MembershipDelta {
    from: &'static str,
    to: &'static str,
    gained: usize,
    lost: usize,
    net: i64,
    changed_memberships: Vec<ChangedMembership>,
}

#[derive(Serialize)]
struct ChangedMembership {
    query_index: usize,
    query_id: String,
    document_index: usize,
    document_id: String,
    exact_rank: usize,
    from_rank: usize,
    to_rank: usize,
    from_score: f32,
    to_score: f32,
}

#[derive(Serialize)]
struct Sq8Report {
    calibration_sha256: String,
    code_sha256: String,
    code_bytes: u64,
    elapsed_ms: u128,
    points: Vec<Sq8Point>,
}

#[derive(Serialize)]
struct Sq8Point {
    candidate_k: usize,
    gold_hits: usize,
    gold_recall: f64,
    f32_frontier_containment: f64,
    truth_bytes: IntegerSummary,
}

#[derive(Serialize)]
struct KmeansBuildReport {
    label: String,
    nlist: usize,
    max_iterations: usize,
    elapsed_ms: u128,
    centroid_sha256: String,
    assignment_sha256: String,
    centroid_bytes: u64,
    centroid_norms: NumericSummary,
    occupancy: IntegerSummary,
    imbalance_max_over_mean: f64,
}

#[derive(Serialize)]
struct IterationComparison {
    nlist: usize,
    assignments_equal: usize,
    assignments_total: usize,
    assignment_agreement: f64,
    centroid_checksums_equal: bool,
}

#[derive(Serialize)]
struct FrontierCell {
    build_label: String,
    routing_metric: &'static str,
    nlist: usize,
    nprobe: usize,
    candidate_k: usize,
    gold_hits: usize,
    gold_recall: f64,
    routing_containment_losses: usize,
    routed_k_frontier_losses: usize,
    exhaustive_fde_top_k_routed_containment: f64,
    unique_routed_documents: IntegerSummary,
    queries_with_fewer_routed_documents_than_k: usize,
    mean_scan_fraction: f64,
    candidate_fde_bytes: IntegerSummary,
    truth_bytes: IntegerSummary,
    query_rows_vs_total_misses_pearson: Option<f64>,
    query_rows_vs_containment_misses_pearson: Option<f64>,
    query_rows_vs_k_frontier_misses_pearson: Option<f64>,
    query_fde_norm_vs_containment_misses_pearson: Option<f64>,
    gold_assigned_cluster_rank: IntegerSummary,
}

#[derive(Serialize)]
struct DynamicProbeCell {
    build_label: String,
    nlist: usize,
    candidate_k: usize,
    routed_row_multiple: f64,
    gold_hits: usize,
    gold_recall: f64,
    routing_containment_losses: usize,
    routed_k_frontier_losses: usize,
    probes: IntegerSummary,
    unique_routed_documents: IntegerSummary,
    mean_scan_fraction: f64,
    candidate_fde_bytes: IntegerSummary,
    truth_bytes: IntegerSummary,
}

#[derive(Serialize)]
struct ProductionOracleReport {
    nlist: usize,
    nprobe: usize,
    candidate_k: usize,
    query_count: usize,
    ordered_route_matches: usize,
    ordered_candidate_matches: usize,
    candidate_score_bit_matches: usize,
    production_route_sha256: String,
    oracle_route_sha256: String,
    production_candidate_sha256: String,
    oracle_candidate_sha256: String,
    bootstrap_sha256: String,
}

#[derive(Serialize)]
struct GoldClusterRank {
    query_index: usize,
    query_id: String,
    query_rows: usize,
    exact_rank: usize,
    document_index: usize,
    document_id: String,
    assigned_cluster: usize,
    assigned_cluster_rank: usize,
}

#[derive(Clone, Serialize)]
struct IntegerSummary {
    min: u64,
    p5: u64,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
    mean: f64,
}

#[derive(Clone, Serialize)]
struct NumericSummary {
    min: f64,
    p5: f64,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
    mean: f64,
}

struct GoldSelectionState {
    rank: usize,
    score: f32,
    selected: bool,
}

struct ScoredVariant {
    name: &'static str,
    hits: usize,
    gold_states: Vec<Vec<GoldSelectionState>>,
    order: Option<Vec<Vec<usize>>>,
}

#[derive(Clone, Copy)]
enum DiagnosticRoutingMetric {
    NegativeL2,
    Dot,
}

impl DiagnosticRoutingMetric {
    const fn name(self) -> &'static str {
        match self {
            Self::NegativeL2 => "negative_l2",
            Self::Dot => "centroid_dot",
        }
    }
}

struct RouteOrder {
    ordered_clusters: Vec<usize>,
    rank_by_cluster: Vec<usize>,
}

struct RoutingBuild {
    label: String,
    nlist: usize,
    max_iterations: usize,
    centroids: Vec<Vec<f32>>,
    assignments: Vec<usize>,
    occupancy: Vec<usize>,
    elapsed_ms: u128,
}

struct CellAccumulator {
    candidate_k: usize,
    gold_hits: usize,
    containment_losses: usize,
    k_losses: usize,
    exhaustive_top_k_contained: usize,
    exhaustive_top_k_total: usize,
    routed_counts: Vec<u64>,
    candidate_bytes: Vec<u64>,
    truth_bytes: Vec<u64>,
    total_misses: Vec<f64>,
    containment_misses: Vec<f64>,
    k_misses: Vec<f64>,
}

#[derive(Deserialize)]
struct BootstrapCentroids {
    centroids: Vec<Vec<f32>>,
}

#[test]
#[ignore = "loads pinned MMLI tensors and performs a release-mode routing sweep"]
fn phase9_routing_diagnostic() {
    if let Err(error) = run_phase9_routing_diagnostic() {
        panic!("Phase 9 routing diagnostic failed: {error}");
    }
}

fn run_phase9_routing_diagnostic() -> DiagnosticResult<()> {
    let started = Instant::now();
    let tensor_dir = required_path("MMLI_REAL_MATRIX_DIR")?;
    let output = required_path("MMLI_PHASE9_ROUTING_OUTPUT")?;
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));

    eprintln!("phase9-routing: loading and verifying tensors");
    let documents = load_tensor(
        &tensor_dir.join("text-documents.f16"),
        &tensor_dir.join("text-documents.json"),
        DOCUMENT_DIGEST,
    )?;
    let queries = load_tensor(
        &tensor_dir.join("text-queries.f16"),
        &tensor_dir.join("text-queries.json"),
        QUERY_DIGEST,
    )?;
    if documents.sidecar.dim != DIMENSION
        || queries.sidecar.dim != DIMENSION
        || documents.sidecar.ids.len() != 5_183
        || queries.sidecar.ids.len() != 1_109
    {
        return Err("pinned tensor shape or count changed".to_string());
    }
    let gold = load_gold(
        &manifest_dir.join("tasks/MMLI-2/results/lab-diagnostics.json"),
        &documents,
        &queries,
    )?;

    eprintln!("phase9-routing: building lab and production FDE variants once");
    let transform = FdeTransform::generate(
        &FdeParams {
            algorithm: FdeAlgorithmVersion::PaperV1,
            repetitions: 40,
            simhash_bits: 4,
            input_dimension: DIMENSION as u32,
            inner: InnerProjection::Rademacher { d_proj: 16 },
            final_projection: FinalProjection::None,
        },
        FDE_SEED,
    )
    .map_err(|error| error.to_string())?;
    let transform_digest = hex(&transform.checksum());
    if transform_digest != TRANSFORM_DIGEST || transform.output_dimension() != FDE_DIMENSION {
        return Err(format!(
            "config-E transform mismatch: {transform_digest}, dim {}",
            transform.output_dimension()
        ));
    }
    let fdes = build_fde_variants(&documents, &queries, &transform)?;

    eprintln!("phase9-routing: isolating full-probe parity deltas");
    let lab = score_variant(
        "lab_f64_mean_scalar_dot",
        &fdes.lab_documents,
        &fdes.lab_queries,
        &gold,
        DotArithmetic::LabScalar,
        false,
    )?;
    let f32_mean = score_variant(
        "f32_mean_scalar_dot",
        &fdes.f32_mean_documents,
        &fdes.f32_mean_queries,
        &gold,
        DotArithmetic::LabScalar,
        false,
    )?;
    let f16_documents = score_variant(
        "f32_mean_document_f16_scalar_dot",
        &fdes.production_documents,
        &fdes.production_queries,
        &gold,
        DotArithmetic::LabScalar,
        false,
    )?;
    let production = score_variant(
        "production_f32_mean_document_f16_simd_dot",
        &fdes.production_documents,
        &fdes.production_queries,
        &gold,
        DotArithmetic::Production,
        true,
    )?;
    let production_orders = production
        .order
        .as_ref()
        .ok_or_else(|| "production rankings were not retained".to_string())?;
    let durable_phase2_hits = gold
        .iter()
        .flatten()
        .filter(|gold| gold.durable_fde_rank <= 537)
        .count();
    let full_probe_parity = FullProbeParityReport {
        candidate_k: 537,
        durable_phase2_hits,
        variants: [&lab, &f32_mean, &f16_documents, &production]
            .into_iter()
            .map(|variant| FullProbeVariant {
                name: variant.name,
                hits: variant.hits,
                recall: variant.hits as f64 / (gold.len() * GOLD_PER_QUERY) as f64,
            })
            .collect(),
        cumulative_deltas: vec![
            membership_delta(&lab, &f32_mean, &gold, &documents, &queries),
            membership_delta(&f32_mean, &f16_documents, &gold, &documents, &queries),
            membership_delta(&f16_documents, &production, &gold, &documents, &queries),
            membership_delta(&lab, &production, &gold, &documents, &queries),
        ],
    };

    eprintln!("phase9-routing: running exhaustive SQ8 control");
    let sq8_exhaustive = evaluate_sq8(
        &fdes.production_documents,
        &fdes.production_queries,
        production_orders,
        &gold,
        &documents,
    )?;

    eprintln!("phase9-routing: building production nlist=256 persisted oracle");
    let (primary, artifact) =
        build_primary_production_artifact(&fdes.production_documents, &documents)?;
    let mut builds = vec![primary];
    for &nlist in NLIST_VALUES {
        if nlist == 256 {
            continue;
        }
        eprintln!("phase9-routing: training production k-means nlist={nlist} iters=100");
        builds.push(train_routing_build(
            format!("production_100_nlist_{nlist}"),
            &fdes.production_documents,
            nlist,
            100,
        )?);
    }
    eprintln!("phase9-routing: training 25-iteration parity control");
    builds.push(train_routing_build(
        "production_fdes_25_nlist_256".to_string(),
        &fdes.production_documents,
        256,
        25,
    )?);
    builds.sort_by(|left, right| {
        left.max_iterations
            .cmp(&right.max_iterations)
            .then_with(|| left.nlist.cmp(&right.nlist))
    });

    let production_100_index = builds
        .iter()
        .position(|build| build.nlist == 256 && build.max_iterations == 100)
        .ok_or_else(|| "missing production 100-iteration build".to_string())?;
    let production_25_index = builds
        .iter()
        .position(|build| build.nlist == 256 && build.max_iterations == 25)
        .ok_or_else(|| "missing 25-iteration build".to_string())?;
    let iteration_comparison =
        compare_iterations(&builds[production_25_index], &builds[production_100_index]);

    eprintln!("phase9-routing: evaluating nlist/nprobe/K frontier");
    let query_norms = vector_norms(&fdes.production_queries);
    let mut cells = Vec::new();
    let mut dynamic_probe_cells = Vec::new();
    let mut gold_cluster_ranks = Vec::new();
    for build in &builds {
        let metrics: &[DiagnosticRoutingMetric] =
            if build.nlist == 256 && build.max_iterations == 100 {
                &[
                    DiagnosticRoutingMetric::NegativeL2,
                    DiagnosticRoutingMetric::Dot,
                ]
            } else {
                &[DiagnosticRoutingMetric::NegativeL2]
            };
        for &metric in metrics {
            let routes = rank_all_centroids(&fdes.production_queries, &build.centroids, metric)?;
            let rank_summary = gold_route_rank_summary(&routes, &build.assignments, &gold);
            for nprobe in probe_values(build.nlist) {
                let k_values: &[usize] = if matches!(metric, DiagnosticRoutingMetric::Dot) {
                    &[537, 1_000, 2_000]
                } else {
                    K_VALUES
                };
                cells.extend(evaluate_probe_point(
                    build,
                    metric,
                    &routes,
                    nprobe,
                    k_values,
                    production_orders,
                    &gold,
                    &documents,
                    &queries,
                    &query_norms,
                    &rank_summary,
                ));
            }
            if matches!(metric, DiagnosticRoutingMetric::NegativeL2) {
                for &candidate_k in &[537, 700, 1_000, 1_500, 2_000] {
                    for &multiple in &[1.0, 2.0, 3.0] {
                        dynamic_probe_cells.push(evaluate_dynamic_probe(
                            build,
                            &routes,
                            candidate_k,
                            multiple,
                            production_orders,
                            &gold,
                            &documents,
                        ));
                    }
                }
            }
            if build.nlist == 256
                && build.max_iterations == 100
                && matches!(metric, DiagnosticRoutingMetric::NegativeL2)
            {
                gold_cluster_ranks = raw_gold_cluster_ranks(
                    &routes,
                    &build.assignments,
                    &gold,
                    &documents,
                    &queries,
                );
            }
        }
    }

    eprintln!("phase9-routing: comparing persisted route/candidates with oracle");
    let primary_build = &builds[production_100_index];
    let production_oracle = compare_production_oracle(
        &artifact,
        primary_build,
        &fdes.production_documents,
        &fdes.production_queries,
        production_orders,
    )?;

    let kmeans_builds = builds.iter().map(kmeans_report).collect();
    let document_norms = vector_norms(&fdes.production_documents);
    let fde_geometry = FdeGeometryReport {
        dimension: FDE_DIMENSION,
        bytes_per_row: CANDIDATE_BYTES_PER_ROW,
        corpus_f32_bytes: documents.sidecar.ids.len() as u64 * CANDIDATE_BYTES_PER_ROW,
        lab_document_sha256: checksum_vectors(&fdes.lab_documents, FDE_DIMENSION),
        lab_query_sha256: checksum_vectors(&fdes.lab_queries, FDE_DIMENSION),
        f32_mean_document_sha256: checksum_vectors(&fdes.f32_mean_documents, FDE_DIMENSION),
        f32_mean_query_sha256: checksum_vectors(&fdes.f32_mean_queries, FDE_DIMENSION),
        production_document_sha256: checksum_vectors(&fdes.production_documents, FDE_DIMENSION),
        production_query_sha256: checksum_vectors(&fdes.production_queries, FDE_DIMENSION),
        document_norms: numeric_summary(&document_norms),
        query_norms: numeric_summary(&query_norms),
        document_norm_cv: coefficient_of_variation(&document_norms),
        query_norm_cv: coefficient_of_variation(&query_norms),
    };
    let report = DiagnosticReport {
        schema_version: 1,
        source_revision: SOURCE_REVISION,
        runtime_ms: started.elapsed().as_millis(),
        inputs: InputReport {
            document_count: documents.sidecar.ids.len(),
            query_count: queries.sidecar.ids.len(),
            gold_memberships: gold.len() * GOLD_PER_QUERY,
            document_rows: documents.total_rows,
            query_rows: queries.total_rows,
            dimension: DIMENSION,
            document_composite_sha256: documents.composite_digest.clone(),
            query_composite_sha256: queries.composite_digest.clone(),
            transform_sha256: transform_digest,
            document_f16_bytes: (documents.values.len() * size_of::<u16>()) as u64,
            query_f16_bytes: (queries.values.len() * size_of::<u16>()) as u64,
        },
        fde_geometry,
        full_probe_parity,
        sq8_exhaustive,
        kmeans_builds,
        iteration_comparison,
        cells,
        dynamic_probe_cells,
        production_oracle,
        gold_cluster_ranks,
    };
    let bytes = serde_json::to_vec_pretty(&report)
        .map_err(|error| format!("cannot serialize diagnostic report: {error}"))?;
    fs::write(&output, bytes)
        .map_err(|error| format!("cannot write {}: {error}", output.display()))?;
    eprintln!(
        "phase9-routing: wrote {} in {:.2}s",
        output.display(),
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

fn required_path(name: &str) -> DiagnosticResult<PathBuf> {
    env::var_os(name)
        .map(PathBuf::from)
        .ok_or_else(|| format!("{name} must be set"))
}

fn load_tensor(
    raw_path: &Path,
    sidecar_path: &Path,
    expected: &str,
) -> DiagnosticResult<TensorSet> {
    let sidecar_bytes = fs::read(sidecar_path)
        .map_err(|error| format!("cannot read {}: {error}", sidecar_path.display()))?;
    let sidecar: TensorSidecar = serde_json::from_slice(&sidecar_bytes)
        .map_err(|error| format!("invalid {}: {error}", sidecar_path.display()))?;
    if sidecar.dtype != TensorDtype::F16
        || sidecar.dim == 0
        || sidecar.rows.is_empty()
        || sidecar.rows.len() != sidecar.ids.len()
    {
        return Err(format!("invalid pinned sidecar {}", sidecar_path.display()));
    }
    let mut ids = HashSet::with_capacity(sidecar.ids.len());
    let mut total_rows = 0usize;
    let mut scalar_offsets = Vec::with_capacity(sidecar.rows.len());
    let mut scalar_count = 0usize;
    for (&rows, id) in sidecar.rows.iter().zip(&sidecar.ids) {
        if rows == 0 || id.is_empty() || !ids.insert(id.as_str()) {
            return Err(format!(
                "invalid matrix row/id in {}",
                sidecar_path.display()
            ));
        }
        scalar_offsets.push(scalar_count);
        total_rows = total_rows
            .checked_add(rows)
            .ok_or_else(|| "tensor row count overflows".to_string())?;
        scalar_count = scalar_count
            .checked_add(rows * sidecar.dim)
            .ok_or_else(|| "tensor scalar count overflows".to_string())?;
    }
    let expected_bytes = scalar_count
        .checked_mul(size_of::<u16>())
        .ok_or_else(|| "tensor byte count overflows".to_string())?;
    let file = File::open(raw_path)
        .map_err(|error| format!("cannot open {}: {error}", raw_path.display()))?;
    let actual_bytes = file
        .metadata()
        .map_err(|error| format!("cannot stat {}: {error}", raw_path.display()))?
        .len();
    if actual_bytes != expected_bytes as u64 {
        return Err(format!(
            "{} is {actual_bytes} bytes, expected {expected_bytes}",
            raw_path.display()
        ));
    }
    let mut hasher = Sha256::new();
    hash_frame(&mut hasher, b"sidecar", &sidecar_bytes);
    hasher.update((b"raw".len() as u64).to_le_bytes());
    hasher.update(b"raw");
    hasher.update((expected_bytes as u64).to_le_bytes());
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut values = Vec::with_capacity(scalar_count);
    while values.len() < scalar_count {
        let count = (scalar_count - values.len()).min(buffer.len() / 2);
        let bytes = count * 2;
        reader
            .read_exact(&mut buffer[..bytes])
            .map_err(|error| format!("cannot read {}: {error}", raw_path.display()))?;
        hasher.update(&buffer[..bytes]);
        for chunk in buffer[..bytes].chunks_exact(2) {
            let value = f16_to_f32(u16::from_le_bytes([chunk[0], chunk[1]]));
            if !value.is_finite() {
                return Err(format!(
                    "{} contains a non-finite value",
                    raw_path.display()
                ));
            }
            values.push(value);
        }
    }
    let composite_digest = hex(&hasher.finalize());
    if composite_digest != expected {
        return Err(format!(
            "{} digest mismatch: expected {expected}, got {composite_digest}",
            raw_path.display()
        ));
    }
    Ok(TensorSet {
        sidecar,
        values,
        scalar_offsets,
        total_rows,
        composite_digest,
    })
}

fn hash_frame(hasher: &mut Sha256, label: &[u8], bytes: &[u8]) {
    hasher.update((label.len() as u64).to_le_bytes());
    hasher.update(label);
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn load_gold(
    path: &Path,
    documents: &TensorSet,
    queries: &TensorSet,
) -> DiagnosticResult<Vec<Vec<GoldDocument>>> {
    let file =
        File::open(path).map_err(|error| format!("cannot open {}: {error}", path.display()))?;
    let diagnostics: DiagnosticsFile = serde_json::from_reader(BufReader::new(file))
        .map_err(|error| format!("invalid {}: {error}", path.display()))?;
    let mut cells = diagnostics
        .cells
        .into_iter()
        .filter(|cell| cell.config == "E");
    let cell = cells
        .next()
        .ok_or_else(|| format!("{} has no config-E cell", path.display()))?;
    if cells.next().is_some() || cell.gold_ranks.len() != queries.sidecar.ids.len() * GOLD_PER_QUERY
    {
        return Err(format!(
            "{} has ambiguous or incomplete config-E gold",
            path.display()
        ));
    }
    let mut ordered = vec![vec![None; GOLD_PER_QUERY]; queries.sidecar.ids.len()];
    let mut seen = vec![HashSet::with_capacity(GOLD_PER_QUERY); queries.sidecar.ids.len()];
    for rank in cell.gold_ranks {
        if queries.sidecar.ids.get(rank.query_index) != Some(&rank.query_id)
            || documents.sidecar.ids.get(rank.document_index) != Some(&rank.document_id)
            || !(1..=GOLD_PER_QUERY).contains(&rank.exact_rank)
            || !seen[rank.query_index].insert(rank.document_index)
        {
            return Err(format!(
                "{} contains an invalid config-E gold row",
                path.display()
            ));
        }
        let slot = &mut ordered[rank.query_index][rank.exact_rank - 1];
        if slot.is_some() {
            return Err(format!("{} repeats a config-E exact rank", path.display()));
        }
        *slot = Some(GoldDocument {
            document_index: rank.document_index,
            exact_rank: rank.exact_rank,
            durable_fde_rank: rank.fde_rank,
        });
    }
    ordered
        .into_iter()
        .map(|query| {
            query
                .into_iter()
                .map(|gold| gold.ok_or_else(|| "config-E gold rank is missing".to_string()))
                .collect()
        })
        .collect()
}

fn build_fde_variants(
    documents: &TensorSet,
    queries: &TensorSet,
    transform: &FdeTransform,
) -> DiagnosticResult<FdeVariants> {
    let sample_count = documents.total_rows.min(CENTERING_SAMPLE_ROWS);
    let mut mean64 = vec![0.0_f64; DIMENSION];
    for sample in 0..sample_count {
        let row = sample * documents.total_rows / sample_count;
        let start = row * DIMENSION;
        for (sum, value) in mean64
            .iter_mut()
            .zip(&documents.values[start..start + DIMENSION])
        {
            *sum += f64::from(*value);
        }
    }
    for value in &mut mean64 {
        *value /= sample_count as f64;
    }
    let mean32: Vec<f32> = mean64.iter().map(|value| *value as f32).collect();

    let document_variants = parallel_indexed_map(documents.sidecar.ids.len(), |index| {
        let matrix = documents.matrix(index)?;
        let lab_values: Vec<f32> = matrix
            .values()
            .iter()
            .zip(mean64.iter().cycle())
            .map(|(value, mean)| (f64::from(*value) - mean) as f32)
            .collect();
        let f32_values: Vec<f32> = matrix
            .values()
            .iter()
            .zip(mean32.iter().cycle())
            .map(|(value, mean)| *value - *mean)
            .collect();
        let lab_matrix = MultiVectorMatrixRef::new(
            &lab_values,
            matrix.vector_count(),
            DIMENSION,
            matrix.vector_count(),
        )
        .map_err(|error| error.to_string())?;
        let f32_matrix = MultiVectorMatrixRef::new(
            &f32_values,
            matrix.vector_count(),
            DIMENSION,
            matrix.vector_count(),
        )
        .map_err(|error| error.to_string())?;
        let lab_fde = transform
            .encode_document(&lab_matrix)
            .map_err(|error| error.to_string())?;
        let f32_fde = transform
            .encode_document(&f32_matrix)
            .map_err(|error| error.to_string())?;
        let production_embedding = MultiVectorEmbedding::new(
            f32_values,
            matrix.vector_count(),
            DIMENSION,
            matrix.vector_count(),
        )
        .map_err(|error| error.to_string())?;
        let encoded = encode_matrix_payload(MatrixDtype::F16, DIMENSION, &production_embedding)
            .map_err(|error| error.to_string())?;
        let decoded = decode_matrix_payload(
            &encoded,
            MatrixDtype::F16,
            DIMENSION,
            matrix.vector_count(),
            matrix.vector_count(),
        )
        .map_err(|error| error.to_string())?;
        Ok((
            lab_fde,
            f32_fde,
            transform
                .encode_document(&decoded.matrix_ref().map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())?,
        ))
    })?;
    let mut lab_documents = Vec::with_capacity(document_variants.len());
    let mut f32_mean_documents = Vec::with_capacity(document_variants.len());
    let mut production_documents = Vec::with_capacity(document_variants.len());
    for (lab, f32_mean, production) in document_variants {
        lab_documents.push(lab);
        f32_mean_documents.push(f32_mean);
        production_documents.push(production);
    }

    let query_variants = parallel_indexed_map(queries.sidecar.ids.len(), |index| {
        let matrix = queries.matrix(index)?;
        let lab_values: Vec<f32> = matrix
            .values()
            .iter()
            .zip(mean64.iter().cycle())
            .map(|(value, mean)| (f64::from(*value) - mean) as f32)
            .collect();
        let f32_values: Vec<f32> = matrix
            .values()
            .iter()
            .zip(mean32.iter().cycle())
            .map(|(value, mean)| *value - *mean)
            .collect();
        let lab_matrix = MultiVectorMatrixRef::new(
            &lab_values,
            matrix.vector_count(),
            DIMENSION,
            matrix.vector_count(),
        )
        .map_err(|error| error.to_string())?;
        let f32_matrix = MultiVectorMatrixRef::new(
            &f32_values,
            matrix.vector_count(),
            DIMENSION,
            matrix.vector_count(),
        )
        .map_err(|error| error.to_string())?;
        Ok((
            transform
                .encode_query(&lab_matrix)
                .map_err(|error| error.to_string())?,
            transform
                .encode_query(&f32_matrix)
                .map_err(|error| error.to_string())?,
        ))
    })?;
    let mut lab_queries = Vec::with_capacity(query_variants.len());
    let mut f32_mean_queries = Vec::with_capacity(query_variants.len());
    for (lab, production) in query_variants {
        lab_queries.push(lab);
        f32_mean_queries.push(production);
    }
    Ok(FdeVariants {
        lab_documents,
        lab_queries,
        f32_mean_documents,
        production_documents,
        production_queries: f32_mean_queries.clone(),
        f32_mean_queries,
    })
}

fn score_variant(
    name: &'static str,
    documents: &[Vec<f32>],
    queries: &[Vec<f32>],
    gold: &[Vec<GoldDocument>],
    arithmetic: DotArithmetic,
    retain_order: bool,
) -> DiagnosticResult<ScoredVariant> {
    let per_query = parallel_indexed_map(queries.len(), |query_index| {
        let mut scores = Vec::with_capacity(documents.len());
        for (document_index, document) in documents.iter().enumerate() {
            let score = match arithmetic {
                DotArithmetic::LabScalar => queries[query_index]
                    .iter()
                    .zip(document)
                    .map(|(left, right)| left * right)
                    .sum(),
                DotArithmetic::Production => -dot_product_distance(&queries[query_index], document),
            };
            scores.push((document_index, score));
        }
        scores.sort_unstable_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        let mut rank_by_document = vec![0usize; documents.len()];
        let mut score_by_document = vec![0.0_f32; documents.len()];
        for (rank, &(document_index, score)) in scores.iter().enumerate() {
            rank_by_document[document_index] = rank + 1;
            score_by_document[document_index] = score;
        }
        let states = gold[query_index]
            .iter()
            .map(|gold| GoldSelectionState {
                rank: rank_by_document[gold.document_index],
                score: score_by_document[gold.document_index],
                selected: rank_by_document[gold.document_index] <= 537,
            })
            .collect::<Vec<_>>();
        let order = retain_order.then(|| scores.into_iter().map(|(index, _)| index).collect());
        Ok((states, order))
    })?;
    let hits = per_query
        .iter()
        .flat_map(|(states, _)| states)
        .filter(|state| state.selected)
        .count();
    Ok(ScoredVariant {
        name,
        hits,
        gold_states: per_query
            .iter()
            .map(|(states, _)| {
                states
                    .iter()
                    .map(|state| GoldSelectionState {
                        rank: state.rank,
                        score: state.score,
                        selected: state.selected,
                    })
                    .collect()
            })
            .collect(),
        order: retain_order.then(|| {
            per_query
                .into_iter()
                .map(|(_, order)| order.expect("retained production order"))
                .collect()
        }),
    })
}

fn membership_delta(
    from: &ScoredVariant,
    to: &ScoredVariant,
    gold: &[Vec<GoldDocument>],
    documents: &TensorSet,
    queries: &TensorSet,
) -> MembershipDelta {
    let mut gained = 0usize;
    let mut lost = 0usize;
    let mut changed_memberships = Vec::new();
    for query_index in 0..gold.len() {
        for gold_index in 0..GOLD_PER_QUERY {
            let left = &from.gold_states[query_index][gold_index];
            let right = &to.gold_states[query_index][gold_index];
            if left.selected == right.selected {
                continue;
            }
            if right.selected {
                gained += 1;
            } else {
                lost += 1;
            }
            let membership = gold[query_index][gold_index];
            changed_memberships.push(ChangedMembership {
                query_index,
                query_id: queries.sidecar.ids[query_index].clone(),
                document_index: membership.document_index,
                document_id: documents.sidecar.ids[membership.document_index].clone(),
                exact_rank: membership.exact_rank,
                from_rank: left.rank,
                to_rank: right.rank,
                from_score: left.score,
                to_score: right.score,
            });
        }
    }
    MembershipDelta {
        from: from.name,
        to: to.name,
        gained,
        lost,
        net: gained as i64 - lost as i64,
        changed_memberships,
    }
}

fn evaluate_sq8(
    documents: &[Vec<f32>],
    queries: &[Vec<f32>],
    f32_orders: &[Vec<usize>],
    gold: &[Vec<GoldDocument>],
    tensors: &TensorSet,
) -> DiagnosticResult<Sq8Report> {
    let started = Instant::now();
    let document_refs: Vec<&[f32]> = documents.iter().map(Vec::as_slice).collect();
    let calibration = SqCalibration::calibrate(&document_refs, FDE_DIMENSION);
    let codes = calibration.encode_batch(&document_refs);
    let orders = parallel_indexed_map(queries.len(), |query_index| {
        let mut scores: Vec<(usize, f32)> = codes
            .iter()
            .enumerate()
            .map(|(document_index, code)| {
                (
                    document_index,
                    calibration.asymmetric_dot_product(&queries[query_index], code),
                )
            })
            .collect();
        scores.sort_unstable_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        Ok(scores
            .into_iter()
            .map(|(index, _)| index)
            .collect::<Vec<_>>())
    })?;
    let points = K_VALUES
        .iter()
        .map(|&candidate_k| {
            let mut hits = 0usize;
            let mut f32_contained = 0usize;
            let mut truth_bytes = Vec::with_capacity(queries.len());
            for query_index in 0..queries.len() {
                let selected = &orders[query_index][..candidate_k.min(documents.len())];
                hits += gold[query_index]
                    .iter()
                    .filter(|gold| selected.contains(&gold.document_index))
                    .count();
                f32_contained += f32_orders[query_index][..candidate_k.min(documents.len())]
                    .iter()
                    .filter(|document| selected.contains(document))
                    .count();
                truth_bytes.push(
                    selected
                        .iter()
                        .map(|&index| tensors.matrix_truth_bytes(index))
                        .sum(),
                );
            }
            Sq8Point {
                candidate_k,
                gold_hits: hits,
                gold_recall: hits as f64 / (gold.len() * GOLD_PER_QUERY) as f64,
                f32_frontier_containment: f32_contained as f64
                    / (queries.len() * candidate_k.min(documents.len())) as f64,
                truth_bytes: integer_summary(&truth_bytes),
            }
        })
        .collect();
    let mut calibration_hasher = Sha256::new();
    calibration_hasher.update((calibration.dim as u64).to_le_bytes());
    for values in [&calibration.mins, &calibration.maxs] {
        for value in values {
            calibration_hasher.update(value.to_bits().to_le_bytes());
        }
    }
    Ok(Sq8Report {
        calibration_sha256: hex(&calibration_hasher.finalize()),
        code_sha256: checksum_bytes_rows(&codes),
        code_bytes: codes.iter().map(|code| code.len() as u64).sum(),
        elapsed_ms: started.elapsed().as_millis(),
        points,
    })
}

fn build_primary_production_artifact(
    document_fdes: &[Vec<f32>],
    tensors: &TensorSet,
) -> DiagnosticResult<(RoutingBuild, BuiltLateCandidateIndex)> {
    let started = Instant::now();
    let rows = document_fdes
        .iter()
        .enumerate()
        .map(|(index, fde)| diagnostic_candidate_row(index, fde.clone(), tensors))
        .collect();
    let artifact = build_late_candidate_index(
        "mmli-phase9-diagnostic",
        "routing",
        FdeGenerationId::new([9; 32]),
        LateCandidateBuildConfig {
            fde_dimension: FDE_DIMENSION,
            nlist: 256,
            probe_budget: PRODUCTION_PARITY_NPROBE,
            candidate_k: PRODUCTION_PARITY_K,
            routing_metric: LateRoutingMetric::NegativeL2,
            kmeans_max_iters: 100,
            kmeans_epsilon: PRODUCTION_KMEANS_EPSILON,
            max_cluster_bytes: 64 * 1024 * 1024,
            max_bootstrap_bytes: 64 * 1024 * 1024,
        },
        rows,
    )
    .map_err(|error| error.to_string())?;
    let resident = ResidentLateCandidateIndex::from_index_ref(
        &artifact.bootstrap_bytes,
        &artifact.index_ref,
        64 * 1024 * 1024,
    )
    .map_err(|error| error.to_string())?;
    if resident.recipe() != &artifact.index_ref.bootstrap.recipe {
        return Err("resident recipe differs from built artifact".to_string());
    }
    let centroids = decode_bootstrap_centroids(&artifact.bootstrap_bytes)?;
    let (assignments, occupancy) = assign_documents(document_fdes, &centroids)?;
    Ok((
        RoutingBuild {
            label: "production_100_nlist_256".to_string(),
            nlist: 256,
            max_iterations: 100,
            centroids,
            assignments,
            occupancy,
            elapsed_ms: started.elapsed().as_millis(),
        },
        artifact,
    ))
}

fn diagnostic_candidate_row(
    index: usize,
    fde: Vec<f32>,
    tensors: &TensorSet,
) -> LateCandidateInputRow {
    let id = production_document_id(index);
    let checksum = ArtifactChecksum::digest(id.as_bytes());
    LateCandidateInputRow {
        id: id.clone(),
        fde,
        content_hash: ContentHash::new(checksum.as_bytes().to_owned()),
        source_sequence: index as u64,
        parent_id: None,
        unit_ordinal: None,
        matrix_locator: MatrixBlockLocator {
            object_key: format!("mmli-phase9-diagnostic/late/segments/routing/{id}-matrix"),
            byte_offset: 0,
            byte_length: tensors.matrix_truth_bytes(index),
            vector_count: tensors.sidecar.rows[index] as u32,
            payload_checksum: checksum,
        },
        attr_locator: AttributeLocator {
            object_key: format!("mmli-phase9-diagnostic/late/segments/routing/{id}-attrs"),
            byte_offset: 0,
            byte_length: 1,
            payload_checksum: checksum,
        },
        filter_attributes: None,
    }
}

fn decode_bootstrap_centroids(bytes: &[u8]) -> DiagnosticResult<Vec<Vec<f32>>> {
    const HEADER: usize = 13;
    if bytes.len() < HEADER || &bytes[..4] != b"ZLB1" || bytes[4] != 1 {
        return Err("invalid production bootstrap header".to_string());
    }
    let mut length = [0_u8; 8];
    length.copy_from_slice(&bytes[5..HEADER]);
    if u64::from_le_bytes(length) as usize != bytes.len() - HEADER {
        return Err("production bootstrap payload length mismatch".to_string());
    }
    let decoded: BootstrapCentroids = rmp_serde::from_slice(&bytes[HEADER..])
        .map_err(|error| format!("cannot decode validated bootstrap centroids: {error}"))?;
    Ok(decoded.centroids)
}

fn train_routing_build(
    label: String,
    documents: &[Vec<f32>],
    nlist: usize,
    max_iterations: usize,
) -> DiagnosticResult<RoutingBuild> {
    let started = Instant::now();
    let refs: Vec<&[f32]> = documents.iter().map(Vec::as_slice).collect();
    let centroids = train_kmeans(
        &refs,
        FDE_DIMENSION,
        nlist,
        max_iterations,
        PRODUCTION_KMEANS_EPSILON,
    )
    .map_err(|error| error.to_string())?;
    let (assignments, occupancy) = assign_documents(documents, &centroids)?;
    Ok(RoutingBuild {
        label,
        nlist,
        max_iterations,
        centroids,
        assignments,
        occupancy,
        elapsed_ms: started.elapsed().as_millis(),
    })
}

fn assign_documents(
    documents: &[Vec<f32>],
    centroids: &[Vec<f32>],
) -> DiagnosticResult<(Vec<usize>, Vec<usize>)> {
    let assignments = parallel_indexed_map(documents.len(), |index| {
        nearest_centroid_production(&documents[index], centroids)
    })?;
    let mut occupancy = vec![0usize; centroids.len()];
    for &assignment in &assignments {
        occupancy[assignment] += 1;
    }
    Ok((assignments, occupancy))
}

fn nearest_centroid_production(vector: &[f32], centroids: &[Vec<f32>]) -> DiagnosticResult<usize> {
    centroids
        .iter()
        .enumerate()
        .map(|(cluster, centroid)| (cluster, euclidean_distance(vector, centroid)))
        .min_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| left.0.cmp(&right.0))
        })
        .map(|(cluster, _)| cluster)
        .ok_or_else(|| "centroid set is empty".to_string())
}

fn rank_all_centroids(
    queries: &[Vec<f32>],
    centroids: &[Vec<f32>],
    metric: DiagnosticRoutingMetric,
) -> DiagnosticResult<Vec<RouteOrder>> {
    parallel_indexed_map(queries.len(), |query_index| {
        let mut ranked: Vec<(usize, f32)> = centroids
            .iter()
            .enumerate()
            .map(|(cluster, centroid)| {
                let score = match metric {
                    DiagnosticRoutingMetric::NegativeL2 => {
                        -euclidean_distance(&queries[query_index], centroid)
                    }
                    DiagnosticRoutingMetric::Dot => {
                        -dot_product_distance(&queries[query_index], centroid)
                    }
                };
                (cluster, score)
            })
            .collect();
        ranked.sort_unstable_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        let ordered_clusters: Vec<usize> = ranked.into_iter().map(|(cluster, _)| cluster).collect();
        let mut rank_by_cluster = vec![0usize; centroids.len()];
        for (rank, &cluster) in ordered_clusters.iter().enumerate() {
            rank_by_cluster[cluster] = rank + 1;
        }
        Ok(RouteOrder {
            ordered_clusters,
            rank_by_cluster,
        })
    })
}

#[allow(clippy::too_many_arguments)]
fn evaluate_probe_point(
    build: &RoutingBuild,
    metric: DiagnosticRoutingMetric,
    routes: &[RouteOrder],
    nprobe: usize,
    k_values: &[usize],
    global_orders: &[Vec<usize>],
    gold: &[Vec<GoldDocument>],
    documents: &TensorSet,
    queries: &TensorSet,
    query_norms: &[f64],
    gold_rank_summary: &IntegerSummary,
) -> Vec<FrontierCell> {
    let mut accumulators: Vec<CellAccumulator> = k_values
        .iter()
        .map(|&candidate_k| CellAccumulator {
            candidate_k,
            gold_hits: 0,
            containment_losses: 0,
            k_losses: 0,
            exhaustive_top_k_contained: 0,
            exhaustive_top_k_total: 0,
            routed_counts: Vec::with_capacity(routes.len()),
            candidate_bytes: Vec::with_capacity(routes.len()),
            truth_bytes: Vec::with_capacity(routes.len()),
            total_misses: Vec::with_capacity(routes.len()),
            containment_misses: Vec::with_capacity(routes.len()),
            k_misses: Vec::with_capacity(routes.len()),
        })
        .collect();
    for query_index in 0..routes.len() {
        let route = &routes[query_index];
        let routed_count: usize = route.ordered_clusters[..nprobe]
            .iter()
            .map(|&cluster| build.occupancy[cluster])
            .sum();
        let mut routed_order = Vec::with_capacity(routed_count);
        let mut routed_rank = vec![usize::MAX; build.assignments.len()];
        let mut truth_prefix = Vec::with_capacity(routed_count + 1);
        truth_prefix.push(0_u64);
        for &document in &global_orders[query_index] {
            if route.rank_by_cluster[build.assignments[document]] <= nprobe {
                routed_rank[document] = routed_order.len() + 1;
                routed_order.push(document);
                truth_prefix.push(
                    truth_prefix.last().copied().unwrap_or(0)
                        + documents.matrix_truth_bytes(document),
                );
            }
        }
        for accumulator in &mut accumulators {
            let selected = accumulator.candidate_k.min(routed_order.len());
            let mut containment_misses = 0usize;
            let mut k_misses = 0usize;
            for gold in &gold[query_index] {
                if route.rank_by_cluster[build.assignments[gold.document_index]] > nprobe {
                    containment_misses += 1;
                } else if routed_rank[gold.document_index] > accumulator.candidate_k {
                    k_misses += 1;
                }
            }
            let hits = GOLD_PER_QUERY - containment_misses - k_misses;
            accumulator.gold_hits += hits;
            accumulator.containment_losses += containment_misses;
            accumulator.k_losses += k_misses;
            let exhaustive_k = accumulator
                .candidate_k
                .min(global_orders[query_index].len());
            accumulator.exhaustive_top_k_total += exhaustive_k;
            accumulator.exhaustive_top_k_contained += global_orders[query_index][..exhaustive_k]
                .iter()
                .filter(|&&document| route.rank_by_cluster[build.assignments[document]] <= nprobe)
                .count();
            accumulator.routed_counts.push(routed_count as u64);
            accumulator
                .candidate_bytes
                .push(routed_count as u64 * CANDIDATE_BYTES_PER_ROW);
            accumulator.truth_bytes.push(truth_prefix[selected]);
            accumulator
                .total_misses
                .push((containment_misses + k_misses) as f64);
            accumulator
                .containment_misses
                .push(containment_misses as f64);
            accumulator.k_misses.push(k_misses as f64);
        }
    }
    let query_rows: Vec<f64> = queries
        .sidecar
        .rows
        .iter()
        .map(|&rows| rows as f64)
        .collect();
    accumulators
        .into_iter()
        .map(|accumulator| {
            let routed_summary = integer_summary(&accumulator.routed_counts);
            FrontierCell {
                build_label: build.label.clone(),
                routing_metric: metric.name(),
                nlist: build.nlist,
                nprobe,
                candidate_k: accumulator.candidate_k,
                gold_hits: accumulator.gold_hits,
                gold_recall: accumulator.gold_hits as f64 / (routes.len() * GOLD_PER_QUERY) as f64,
                routing_containment_losses: accumulator.containment_losses,
                routed_k_frontier_losses: accumulator.k_losses,
                exhaustive_fde_top_k_routed_containment: accumulator.exhaustive_top_k_contained
                    as f64
                    / accumulator.exhaustive_top_k_total as f64,
                queries_with_fewer_routed_documents_than_k: accumulator
                    .routed_counts
                    .iter()
                    .filter(|&&count| count < accumulator.candidate_k as u64)
                    .count(),
                mean_scan_fraction: routed_summary.mean / build.assignments.len() as f64,
                unique_routed_documents: routed_summary,
                candidate_fde_bytes: integer_summary(&accumulator.candidate_bytes),
                truth_bytes: integer_summary(&accumulator.truth_bytes),
                query_rows_vs_total_misses_pearson: pearson(&query_rows, &accumulator.total_misses),
                query_rows_vs_containment_misses_pearson: pearson(
                    &query_rows,
                    &accumulator.containment_misses,
                ),
                query_rows_vs_k_frontier_misses_pearson: pearson(
                    &query_rows,
                    &accumulator.k_misses,
                ),
                query_fde_norm_vs_containment_misses_pearson: pearson(
                    query_norms,
                    &accumulator.containment_misses,
                ),
                gold_assigned_cluster_rank: gold_rank_summary.clone(),
            }
        })
        .collect()
}

fn evaluate_dynamic_probe(
    build: &RoutingBuild,
    routes: &[RouteOrder],
    candidate_k: usize,
    multiple: f64,
    global_orders: &[Vec<usize>],
    gold: &[Vec<GoldDocument>],
    documents: &TensorSet,
) -> DynamicProbeCell {
    let target = (candidate_k as f64 * multiple).ceil() as usize;
    let mut probes = Vec::with_capacity(routes.len());
    let mut routed_counts = Vec::with_capacity(routes.len());
    let mut candidate_bytes = Vec::with_capacity(routes.len());
    let mut truth_bytes = Vec::with_capacity(routes.len());
    let mut hits = 0usize;
    let mut containment_losses = 0usize;
    let mut k_losses = 0usize;
    for query_index in 0..routes.len() {
        let route = &routes[query_index];
        let mut routed_count = 0usize;
        let mut nprobe = 0usize;
        while nprobe < build.nlist && routed_count < target {
            routed_count += build.occupancy[route.ordered_clusters[nprobe]];
            nprobe += 1;
        }
        let mut routed_order = Vec::with_capacity(routed_count);
        let mut routed_rank = vec![usize::MAX; build.assignments.len()];
        for &document in &global_orders[query_index] {
            if route.rank_by_cluster[build.assignments[document]] <= nprobe {
                routed_rank[document] = routed_order.len() + 1;
                routed_order.push(document);
            }
        }
        let mut query_truth_bytes = 0_u64;
        for &document in routed_order.iter().take(candidate_k) {
            query_truth_bytes += documents.matrix_truth_bytes(document);
        }
        for gold in &gold[query_index] {
            if route.rank_by_cluster[build.assignments[gold.document_index]] > nprobe {
                containment_losses += 1;
            } else if routed_rank[gold.document_index] > candidate_k {
                k_losses += 1;
            } else {
                hits += 1;
            }
        }
        probes.push(nprobe as u64);
        routed_counts.push(routed_count as u64);
        candidate_bytes.push(routed_count as u64 * CANDIDATE_BYTES_PER_ROW);
        truth_bytes.push(query_truth_bytes);
    }
    let routed_summary = integer_summary(&routed_counts);
    DynamicProbeCell {
        build_label: build.label.clone(),
        nlist: build.nlist,
        candidate_k,
        routed_row_multiple: multiple,
        gold_hits: hits,
        gold_recall: hits as f64 / (routes.len() * GOLD_PER_QUERY) as f64,
        routing_containment_losses: containment_losses,
        routed_k_frontier_losses: k_losses,
        probes: integer_summary(&probes),
        mean_scan_fraction: routed_summary.mean / build.assignments.len() as f64,
        unique_routed_documents: routed_summary,
        candidate_fde_bytes: integer_summary(&candidate_bytes),
        truth_bytes: integer_summary(&truth_bytes),
    }
}

fn compare_production_oracle(
    artifact: &BuiltLateCandidateIndex,
    build: &RoutingBuild,
    document_fdes: &[Vec<f32>],
    queries: &[Vec<f32>],
    global_orders: &[Vec<usize>],
) -> DiagnosticResult<ProductionOracleReport> {
    let resident = ResidentLateCandidateIndex::from_index_ref(
        &artifact.bootstrap_bytes,
        &artifact.index_ref,
        64 * 1024 * 1024,
    )
    .map_err(|error| error.to_string())?;
    let routes = rank_all_centroids(
        &queries[..PRODUCTION_PARITY_QUERY_COUNT],
        &build.centroids,
        DiagnosticRoutingMetric::NegativeL2,
    )?;
    let mut ordered_route_matches = 0usize;
    let mut ordered_candidate_matches = 0usize;
    let mut candidate_score_bit_matches = 0usize;
    let mut production_route_hasher = Sha256::new();
    let mut oracle_route_hasher = Sha256::new();
    let mut production_candidate_hasher = Sha256::new();
    let mut oracle_candidate_hasher = Sha256::new();
    for query_index in 0..PRODUCTION_PARITY_QUERY_COUNT {
        let production_route = resident
            .route(&queries[query_index])
            .map_err(|error| error.to_string())?;
        let production_logical: Vec<usize> = production_route
            .iter()
            .map(|reference| reference.cluster_id as usize)
            .collect();
        let oracle_logical =
            routes[query_index].ordered_clusters[..PRODUCTION_PARITY_NPROBE].to_vec();
        if production_logical == oracle_logical {
            ordered_route_matches += 1;
        }
        hash_usize_row(&mut production_route_hasher, &production_logical);
        hash_usize_row(&mut oracle_route_hasher, &oracle_logical);
        let fetched = production_route
            .iter()
            .map(|reference| {
                artifact
                    .clusters
                    .iter()
                    .find(|cluster| cluster.reference == *reference)
                    .map(|cluster| FetchedLateCandidateCluster {
                        reference: &cluster.reference,
                        bytes: &cluster.bytes,
                    })
                    .ok_or_else(|| "routed production cluster bytes are missing".to_string())
            })
            .collect::<DiagnosticResult<Vec<_>>>()?;
        let production_candidates = resident
            .candidates_from_fetched(
                &queries[query_index],
                &BTreeSet::new(),
                None,
                None,
                &fetched,
                64 * 1024 * 1024,
            )
            .map_err(|error| error.to_string())?;
        let oracle_candidates: Vec<usize> = global_orders[query_index]
            .iter()
            .copied()
            .filter(|&document| {
                routes[query_index].rank_by_cluster[build.assignments[document]]
                    <= PRODUCTION_PARITY_NPROBE
            })
            .take(PRODUCTION_PARITY_K)
            .collect();
        let production_ids: Vec<String> = production_candidates
            .iter()
            .map(|candidate| candidate.id.clone())
            .collect();
        let oracle_ids: Vec<String> = oracle_candidates
            .iter()
            .map(|&document| production_document_id(document))
            .collect();
        if production_ids == oracle_ids {
            ordered_candidate_matches += 1;
        }
        let score_bits_match =
            production_candidates
                .iter()
                .zip(&oracle_candidates)
                .all(|(candidate, &document)| {
                    candidate.approx_fde_score.to_bits()
                        == (-dot_product_distance(&queries[query_index], &document_fdes[document]))
                            .to_bits()
                });
        if score_bits_match {
            candidate_score_bit_matches += 1;
        }
        hash_string_row(&mut production_candidate_hasher, &production_ids);
        hash_string_row(&mut oracle_candidate_hasher, &oracle_ids);
    }
    Ok(ProductionOracleReport {
        nlist: build.nlist,
        nprobe: PRODUCTION_PARITY_NPROBE,
        candidate_k: PRODUCTION_PARITY_K,
        query_count: PRODUCTION_PARITY_QUERY_COUNT,
        ordered_route_matches,
        ordered_candidate_matches,
        candidate_score_bit_matches,
        production_route_sha256: hex(&production_route_hasher.finalize()),
        oracle_route_sha256: hex(&oracle_route_hasher.finalize()),
        production_candidate_sha256: hex(&production_candidate_hasher.finalize()),
        oracle_candidate_sha256: hex(&oracle_candidate_hasher.finalize()),
        bootstrap_sha256: ArtifactChecksum::digest(&artifact.bootstrap_bytes).to_hex(),
    })
}

fn compare_iterations(left: &RoutingBuild, right: &RoutingBuild) -> IterationComparison {
    let assignments_equal = left
        .assignments
        .iter()
        .zip(&right.assignments)
        .filter(|(left, right)| left == right)
        .count();
    IterationComparison {
        nlist: left.nlist,
        assignments_equal,
        assignments_total: left.assignments.len(),
        assignment_agreement: assignments_equal as f64 / left.assignments.len() as f64,
        centroid_checksums_equal: checksum_vectors(&left.centroids, FDE_DIMENSION)
            == checksum_vectors(&right.centroids, FDE_DIMENSION),
    }
}

fn kmeans_report(build: &RoutingBuild) -> KmeansBuildReport {
    let occupancy: Vec<u64> = build.occupancy.iter().map(|&value| value as u64).collect();
    let centroid_norms = vector_norms(&build.centroids);
    let mean = build.assignments.len() as f64 / build.nlist as f64;
    KmeansBuildReport {
        label: build.label.clone(),
        nlist: build.nlist,
        max_iterations: build.max_iterations,
        elapsed_ms: build.elapsed_ms,
        centroid_sha256: checksum_vectors(&build.centroids, FDE_DIMENSION),
        assignment_sha256: checksum_assignments(&build.assignments),
        centroid_bytes: build.nlist as u64 * CANDIDATE_BYTES_PER_ROW,
        centroid_norms: numeric_summary(&centroid_norms),
        occupancy: integer_summary(&occupancy),
        imbalance_max_over_mean: build.occupancy.iter().copied().max().unwrap_or(0) as f64 / mean,
    }
}

fn raw_gold_cluster_ranks(
    routes: &[RouteOrder],
    assignments: &[usize],
    gold: &[Vec<GoldDocument>],
    documents: &TensorSet,
    queries: &TensorSet,
) -> Vec<GoldClusterRank> {
    let mut ranks = Vec::with_capacity(gold.len() * GOLD_PER_QUERY);
    for query_index in 0..gold.len() {
        for gold in &gold[query_index] {
            let cluster = assignments[gold.document_index];
            ranks.push(GoldClusterRank {
                query_index,
                query_id: queries.sidecar.ids[query_index].clone(),
                query_rows: queries.sidecar.rows[query_index],
                exact_rank: gold.exact_rank,
                document_index: gold.document_index,
                document_id: documents.sidecar.ids[gold.document_index].clone(),
                assigned_cluster: cluster,
                assigned_cluster_rank: routes[query_index].rank_by_cluster[cluster],
            });
        }
    }
    ranks
}

fn gold_route_rank_summary(
    routes: &[RouteOrder],
    assignments: &[usize],
    gold: &[Vec<GoldDocument>],
) -> IntegerSummary {
    let ranks: Vec<u64> = gold
        .iter()
        .enumerate()
        .flat_map(|(query_index, gold)| {
            gold.iter().map(move |gold| {
                routes[query_index].rank_by_cluster[assignments[gold.document_index]] as u64
            })
        })
        .collect();
    integer_summary(&ranks)
}

fn probe_values(nlist: usize) -> Vec<usize> {
    let mut probes: BTreeSet<usize> = [
        1, 2, 4, 8, 16, 24, 32, 48, 64, 96, 128, 160, 192, 256, 384, 512,
    ]
    .into_iter()
    .filter(|probe| *probe <= nlist)
    .collect();
    for probe in [nlist / 4, nlist / 2, 3 * nlist / 4, 7 * nlist / 8, nlist] {
        probes.insert(probe.max(1));
    }
    probes.into_iter().collect()
}

fn vector_norms(vectors: &[Vec<f32>]) -> Vec<f64> {
    vectors
        .iter()
        .map(|vector| {
            vector
                .iter()
                .map(|value| f64::from(*value) * f64::from(*value))
                .sum::<f64>()
                .sqrt()
        })
        .collect()
}

fn coefficient_of_variation(values: &[f64]) -> f64 {
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| (value - mean) * (value - mean))
        .sum::<f64>()
        / values.len() as f64;
    variance.sqrt() / mean
}

fn integer_summary(values: &[u64]) -> IntegerSummary {
    let mut ordered = values.to_vec();
    ordered.sort_unstable();
    IntegerSummary {
        min: ordered[0],
        p5: nearest_rank_u64(&ordered, 5),
        p50: nearest_rank_u64(&ordered, 50),
        p95: nearest_rank_u64(&ordered, 95),
        p99: nearest_rank_u64(&ordered, 99),
        max: *ordered.last().expect("non-empty integer distribution"),
        mean: ordered.iter().map(|&value| value as f64).sum::<f64>() / ordered.len() as f64,
    }
}

fn numeric_summary(values: &[f64]) -> NumericSummary {
    let mut ordered = values.to_vec();
    ordered.sort_by(f64::total_cmp);
    NumericSummary {
        min: ordered[0],
        p5: nearest_rank_f64(&ordered, 5),
        p50: nearest_rank_f64(&ordered, 50),
        p95: nearest_rank_f64(&ordered, 95),
        p99: nearest_rank_f64(&ordered, 99),
        max: *ordered.last().expect("non-empty numeric distribution"),
        mean: ordered.iter().sum::<f64>() / ordered.len() as f64,
    }
}

fn nearest_rank_u64(ordered: &[u64], percentile: usize) -> u64 {
    let rank = (percentile * ordered.len()).div_ceil(100);
    ordered[rank.saturating_sub(1)]
}

fn nearest_rank_f64(ordered: &[f64], percentile: usize) -> f64 {
    let rank = (percentile * ordered.len()).div_ceil(100);
    ordered[rank.saturating_sub(1)]
}

fn pearson(left: &[f64], right: &[f64]) -> Option<f64> {
    if left.len() != right.len() || left.is_empty() {
        return None;
    }
    let left_mean = left.iter().sum::<f64>() / left.len() as f64;
    let right_mean = right.iter().sum::<f64>() / right.len() as f64;
    let mut numerator = 0.0_f64;
    let mut left_sum = 0.0_f64;
    let mut right_sum = 0.0_f64;
    for (&left, &right) in left.iter().zip(right) {
        let left_delta = left - left_mean;
        let right_delta = right - right_mean;
        numerator += left_delta * right_delta;
        left_sum += left_delta * left_delta;
        right_sum += right_delta * right_delta;
    }
    let denominator = (left_sum * right_sum).sqrt();
    (denominator > 0.0).then_some(numerator / denominator)
}

fn checksum_vectors(vectors: &[Vec<f32>], dimension: usize) -> String {
    let mut hasher = Sha256::new();
    hasher.update((vectors.len() as u64).to_le_bytes());
    hasher.update((dimension as u64).to_le_bytes());
    for vector in vectors {
        for value in vector {
            hasher.update(value.to_bits().to_le_bytes());
        }
    }
    hex(&hasher.finalize())
}

fn checksum_assignments(assignments: &[usize]) -> String {
    let mut hasher = Sha256::new();
    hasher.update((assignments.len() as u64).to_le_bytes());
    for &assignment in assignments {
        hasher.update((assignment as u32).to_le_bytes());
    }
    hex(&hasher.finalize())
}

fn checksum_bytes_rows(rows: &[Vec<u8>]) -> String {
    let mut hasher = Sha256::new();
    hasher.update((rows.len() as u64).to_le_bytes());
    for row in rows {
        hasher.update((row.len() as u64).to_le_bytes());
        hasher.update(row);
    }
    hex(&hasher.finalize())
}

fn hash_usize_row(hasher: &mut Sha256, values: &[usize]) {
    hasher.update((values.len() as u64).to_le_bytes());
    for &value in values {
        hasher.update((value as u64).to_le_bytes());
    }
}

fn hash_string_row(hasher: &mut Sha256, values: &[String]) {
    hasher.update((values.len() as u64).to_le_bytes());
    for value in values {
        hasher.update((value.len() as u64).to_le_bytes());
        hasher.update(value.as_bytes());
    }
}

fn production_document_id(index: usize) -> String {
    format!("mmli-replay-d-{index:020}")
}

fn parallel_indexed_map<T, F>(count: usize, operation: F) -> DiagnosticResult<Vec<T>>
where
    T: Send,
    F: Fn(usize) -> DiagnosticResult<T> + Sync,
{
    let worker_count = std::thread::available_parallelism()
        .map_or(1, usize::from)
        .min(count.max(1));
    let next = AtomicUsize::new(0);
    let results: Mutex<Vec<Option<DiagnosticResult<T>>>> =
        Mutex::new((0..count).map(|_| None).collect());
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
                let result = operation(index);
                results.lock().expect("diagnostic result lock")[index] = Some(result);
            });
        }
    });
    results
        .into_inner()
        .expect("diagnostic result lock")
        .into_iter()
        .enumerate()
        .map(|(index, result)| {
            result.ok_or_else(|| format!("worker omitted diagnostic result {index}"))?
        })
        .collect()
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

fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}
