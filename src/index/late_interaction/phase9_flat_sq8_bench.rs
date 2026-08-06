//! Ignored, production-shaped flat-SQ8 candidate benchmark for MMLI-2 Phase 9.
//!
//! Operator-approved next measurement after the routing diagnosis
//! (`tasks/MMLI-2/results/phase9-routing-review.md` §4). One physical arm at
//! K=1000 over the pinned Phase 2 text tensors against local MinIO:
//!
//! - persists production matrix truth blocks and attribute blocks;
//! - persists a prototype flat-SQ8 candidate artifact (`ZFQ1`) built from the
//!   production `SqCalibration` seam;
//! - hydrates it once (cold), then runs resident exhaustive SQ8 selection per
//!   query and executes production wave-two truth reads through the Phase 8
//!   read planner via the crate-private `segment_search` truth-wave seam;
//! - asserts exact parity with the in-memory diagnostic frontier
//!   (`10,869 / 11,090` at K=1000) and observed-equals-planned GET counts.
//!
//! No production default, cap, gate, or dense path is touched. The `candidate_k`
//! and resident-byte values used here are test-scoped build inputs, not config.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::env;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::embedding::artifact::{decode_matrix_payload, encode_matrix_payload};
use crate::embedding::{
    ArtifactChecksum, ContentHash, EmbeddingProfileId, FdeGenerationId, MatrixDtype,
    MultiVectorEmbedding, MultiVectorEpochId,
};
use crate::index::quantization::sq::SqCalibration;
use crate::storage::read_plan::{ReadPlan, ReadPlanConfig};
use crate::storage::ZeppelinStore;
use crate::types::{AttributeValue, Filter, VectorId};
use crate::wal::LateInteractionSegmentRef;

use super::attribute_artifact::{build_attribute_blocks, AttributeBlockInputRow};
use super::candidate::{
    AttributeLocator, LateCandidateBootstrapRef, LateCandidateIndexRef, LateCandidateRecipe,
    LateRoutingMetric,
};
use super::matrix_artifact::{build_matrix_blocks, MatrixBlockInputRow};
use super::segment_search::{
    build_truth_requests, compare_scored_rows, execute_truth_wave_pipelined, filter_matches,
    SegmentSearchBounds, SegmentSearchRequest,
};
use super::{
    FdeAlgorithmVersion, FdeParams, FdeTransform, FinalProjection, InnerProjection,
    MatrixBlockLocator, MultiVectorMatrixRef,
};

pub(super) const DOCUMENT_DIGEST: &str =
    "1960f7bc88a667beb76b6e15a750469e615aafe9a925928c23f7c546d12cfe22";
pub(super) const QUERY_DIGEST: &str =
    "cefbff5713a3944f4007676b243985f393f87a7a7579bb5cba6ca09899b0aa0c";
pub(super) const TRANSFORM_DIGEST: &str =
    "00ad4edb4292ddd64c6df00c84c2f8dfced3a092d9ddc307239d9e070deb2ad4";
pub(super) const PRODUCTION_DOCUMENT_FDE_DIGEST: &str =
    "399264618538eabd2fdbe0e979d92ed5d552768a2ad5235b4c934acbb1b3dad1";
pub(super) const PRODUCTION_QUERY_FDE_DIGEST: &str =
    "c63a2756f5d74cf0079383d512fa1d0b9171c006d5a5dfcd9939d5870e28ab8e";
const SQ8_CODE_DIGEST: &str = "08d3e1d29684689b18330e838acc92f10872c5ecd2cb89ba57a8c7fe8f247d98";
const SQ8_CALIBRATION_DIGEST: &str =
    "ba1b608200e4568711dfa0cf16e978990fb1064ab4901bc816d4c10b907144ae";
pub(super) const FDE_SEED: u64 = 0x4d4d_4c49_0000_0002;
pub(super) const DIMENSION: usize = 128;
pub(super) const FDE_DIMENSION: usize = 10_240;
pub(super) const GOLD_PER_QUERY: usize = 10;
const CENTERING_SAMPLE_ROWS: usize = 5_000;
pub(super) const DOCUMENT_COUNT: usize = 5_183;
pub(super) const QUERY_COUNT: usize = 1_109;
pub(super) const CANDIDATE_K: usize = 1_000;
// Expected exact hits from the pinned frontier JSON (sq8_exhaustive points).
const EXPECTED_HITS_K700: usize = 10_708;
pub(super) const EXPECTED_HITS_K1000: usize = 10_869;
const EXPECTED_HITS_K1500: usize = 10_993;
const SEGMENT_ID: &str = "flat-sq8-bench";
const FLAT_MAGIC: &[u8; 4] = b"ZFQ1";
const FLAT_VERSION: u8 = 1;
const MAX_MATRIX_OBJECT_BYTES: usize = 64 * 1024 * 1024;
const MAX_ATTRIBUTE_OBJECT_BYTES: usize = 8 * 1024 * 1024;
const FILTER_COLORS: [&str; 4] = ["c0", "c1", "c2", "c3"];
const FILTERED_QUERY_COUNT: usize = 20;
const COLD_HYDRATIONS: usize = 3;

type BenchResult<T> = Result<T, String>;
pub(super) type ProductionFdes = (Vec<Vec<f32>>, Vec<Vec<f32>>, f64);

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum TensorDtype {
    F16,
    F32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TensorSidecar {
    rows: Vec<usize>,
    dim: usize,
    dtype: TensorDtype,
    pub(super) ids: Vec<String>,
}

pub(super) struct TensorSet {
    pub(super) sidecar: TensorSidecar,
    values: Vec<f32>,
    scalar_offsets: Vec<usize>,
    total_rows: usize,
}

impl TensorSet {
    fn matrix(&self, index: usize) -> BenchResult<MultiVectorMatrixRef<'_>> {
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
}

#[derive(Clone, Copy)]
pub(super) struct GoldDocument {
    pub(super) document_index: usize,
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
    #[allow(dead_code)]
    fde_rank: usize,
}

/// Persisted per-row metadata inside the `ZFQ1` prototype artifact.
#[derive(Clone, Serialize, Deserialize)]
struct FlatRowMeta {
    id: String,
    matrix_locator: MatrixBlockLocator,
    attr_locator: AttributeLocator,
    content_hash: [u8; 32],
    source_sequence: u64,
    color: String,
}

#[derive(Serialize, Deserialize)]
struct FlatHeader {
    fde_dimension: u32,
    row_count: u32,
    calibration: Vec<u8>,
    rows: Vec<FlatRowMeta>,
}

struct ResidentFlatIndex {
    calibration: SqCalibration,
    rows: Vec<FlatRowMeta>,
    codes: Vec<u8>,
}

#[derive(Serialize)]
struct IntegerSummary {
    min: u64,
    p5: u64,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
    mean: f64,
}

#[derive(Serialize)]
struct FloatSummary {
    min: f64,
    p5: f64,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
    mean: f64,
}

#[derive(Serialize)]
struct ColdHydration {
    fetch_ms: f64,
    decode_ms: f64,
    bytes: u64,
}

#[derive(Serialize)]
struct BenchReport {
    schema_version: u32,
    source_revision: String,
    candidate_k: usize,
    read_gap_budget_bytes: usize,
    read_max_request_bytes: usize,
    read_max_concurrency: usize,
    pipeline_enabled: bool,
    document_count: usize,
    query_count: usize,
    gold_memberships: usize,
    hits_k700: usize,
    hits_k1000: usize,
    hits_k1500: usize,
    recall_k1000: f64,
    per_query_hits_k1000: IntegerSummary,
    per_query_hits_k1000_values: Vec<u64>,
    per_query_top10_ids: Vec<Vec<String>>,
    queries_below_8_of_10: usize,
    queries_below_5_of_10: usize,
    flat_artifact_bytes: u64,
    flat_metadata_bytes: u64,
    flat_code_bytes: u64,
    resident_bytes: u64,
    uploaded_objects: usize,
    uploaded_bytes: u64,
    setup_put_requests: u64,
    cold_hydrations: Vec<ColdHydration>,
    warm_query_ms: IntegerSummary,
    sq8_scan_ms: IntegerSummary,
    truth_wave_ms: IntegerSummary,
    truth_fetch_ms: FloatSummary,
    truth_score_ms: FloatSummary,
    truth_decode_ms: FloatSummary,
    truth_maxsim_ms: FloatSummary,
    truth_score_workers: usize,
    truth_get_latency_ms: FloatSummary,
    truth_logical_ranges: IntegerSummary,
    truth_logical_bytes: IntegerSummary,
    truth_planned_requests: IntegerSummary,
    truth_planned_bytes: IntegerSummary,
    truth_gap_waste_bytes: IntegerSummary,
    truth_request_waves: IntegerSummary,
    truth_total_get_requests: u64,
    truth_total_request_waves: u64,
    observed_get_requests_equal_planned: bool,
    query_fde_encode_ms_mean: f64,
    filtered_queries: usize,
    filtered_all_results_match: bool,
    filtered_planned_requests: IntegerSummary,
    flat_artifact_sha256: String,
    sq8_code_sha256: String,
    sq8_calibration_sha256: String,
    elapsed_ms: u128,
}

/// Counts physical object-store operations issued beneath `ZeppelinStore`.
///
/// The planner's singleton `get_ranges` calls `get_range`, while whole-object
/// reads call `get_opts`; overriding both observes every physical GET once.
#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    get_requests: Arc<AtomicU64>,
    put_requests: Arc<AtomicU64>,
    get_latency_ms: Arc<Mutex<Vec<f64>>>,
}

type CountingStoreSetup = (
    ZeppelinStore,
    Arc<AtomicU64>,
    Arc<AtomicU64>,
    Arc<Mutex<Vec<f64>>>,
);

impl CountingStore {
    fn record_get(&self, elapsed_ms: f64) {
        self.get_requests.fetch_add(1, AtomicOrdering::Relaxed);
        self.get_latency_ms
            .lock()
            .expect("GET latency lock")
            .push(elapsed_ms);
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "CountingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.put_requests.fetch_add(1, AtomicOrdering::Relaxed);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.put_requests.fetch_add(1, AtomicOrdering::Relaxed);
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let started = Instant::now();
        let result = self.inner.get_opts(location, options).await;
        self.record_get(started.elapsed().as_secs_f64() * 1_000.0);
        result
    }

    async fn get_range(
        &self,
        location: &ObjectPath,
        range: Range<usize>,
    ) -> object_store::Result<Bytes> {
        let started = Instant::now();
        let result = self.inner.get_range(location, range).await;
        self.record_get(started.elapsed().as_secs_f64() * 1_000.0);
        result
    }

    async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires MinIO, ~360 MB uploads, and the pinned Phase 2 text tensors"]
async fn phase9_flat_sq8_benchmark() {
    if let Err(error) = run_benchmark().await {
        panic!("phase9 flat SQ8 benchmark failed: {error}");
    }
}

async fn run_benchmark() -> BenchResult<()> {
    let started = Instant::now();
    let backend = env::var("TEST_BACKEND").unwrap_or_default();
    if backend != "minio" {
        return Err("TEST_BACKEND=minio is required for the physical benchmark".to_string());
    }
    let tensor_dir = required_path("MMLI_REAL_MATRIX_DIR")?;
    let output = required_path("MMLI_FLAT_BENCH_OUTPUT")?;
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let pipeline_enabled = true;
    let query_count = optional_usize("MMLI_FLAT_BENCH_QUERY_LIMIT", QUERY_COUNT)?;
    if !(1..=QUERY_COUNT).contains(&query_count) {
        return Err(format!(
            "MMLI_FLAT_BENCH_QUERY_LIMIT must be between 1 and {QUERY_COUNT}"
        ));
    }
    let default_read_plan_config = ReadPlanConfig::default();
    let read_gap_budget_bytes = optional_usize(
        "MMLI_FLAT_BENCH_GAP_BUDGET",
        default_read_plan_config.gap_budget_bytes,
    )?;
    let read_max_request_bytes = optional_usize(
        "MMLI_FLAT_BENCH_MAX_REQUEST",
        default_read_plan_config.max_request_bytes,
    )?;
    let read_max_concurrency = optional_usize(
        "MMLI_FLAT_BENCH_CONCURRENCY",
        default_read_plan_config.max_concurrent_requests,
    )?;
    let read_plan_config = ReadPlanConfig::new(
        read_gap_budget_bytes,
        read_max_request_bytes,
        read_max_concurrency,
    )
    .map_err(|error| error.to_string())?;

    eprintln!("flat-bench: loading and verifying pinned tensors");
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
    if documents.sidecar.ids.len() != DOCUMENT_COUNT || queries.sidecar.ids.len() != QUERY_COUNT {
        return Err("pinned tensor counts changed".to_string());
    }
    let gold = load_gold(
        &manifest_dir.join("tasks/MMLI-2/results/lab-diagnostics.json"),
        &documents,
        &queries,
    )?;

    eprintln!("flat-bench: building production-semantics FDEs");
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
    if hex(&transform.checksum()) != TRANSFORM_DIGEST
        || transform.output_dimension() != FDE_DIMENSION
    {
        return Err("config-E transform mismatch".to_string());
    }
    let (document_fdes, query_fdes, query_fde_encode_ms_mean) =
        build_production_fdes(&documents, &queries, &transform)?;
    if checksum_vectors(&document_fdes, FDE_DIMENSION) != PRODUCTION_DOCUMENT_FDE_DIGEST {
        return Err("production document FDEs diverge from the pinned diagnosis".to_string());
    }
    if checksum_vectors(&query_fdes, FDE_DIMENSION) != PRODUCTION_QUERY_FDE_DIGEST {
        return Err("production query FDEs diverge from the pinned diagnosis".to_string());
    }

    eprintln!("flat-bench: building SQ8 codes through the production quantizer");
    let document_refs: Vec<&[f32]> = document_fdes.iter().map(Vec::as_slice).collect();
    let calibration = SqCalibration::calibrate(&document_refs, FDE_DIMENSION);
    let codes = calibration.encode_batch(&document_refs);
    let sq8_code_sha256 = checksum_bytes_rows(&codes);
    let sq8_calibration_sha256 = checksum_calibration(&calibration);
    if sq8_code_sha256 != SQ8_CODE_DIGEST {
        return Err("SQ8 codes diverge from the pinned diagnosis".to_string());
    }
    if sq8_calibration_sha256 != SQ8_CALIBRATION_DIGEST {
        return Err("SQ8 calibration diverges from the pinned diagnosis".to_string());
    }
    drop(document_refs);

    eprintln!("flat-bench: building production truth and attribute blocks");
    let epoch = MultiVectorEpochId::new([9; 32]);
    let generation = FdeGenerationId::new([10; 32]);
    let namespace = format!("mmli-flat-bench-{}", uuid::Uuid::new_v4());
    let matrix_rows = (0..DOCUMENT_COUNT)
        .map(|index| {
            let matrix = documents.matrix(index)?;
            Ok(MatrixBlockInputRow::encoded(
                production_document_id(index),
                index as u32,
                document_content_hash(index),
                MatrixDtype::F16,
                MultiVectorEmbedding::new(
                    matrix.values().to_vec(),
                    matrix.vector_count(),
                    DIMENSION,
                    matrix.vector_count(),
                )
                .map_err(|error| error.to_string())?,
            ))
        })
        .collect::<BenchResult<Vec<_>>>()?;
    let matrix_blocks = build_matrix_blocks(
        &namespace,
        SEGMENT_ID,
        MatrixDtype::F16,
        epoch,
        generation,
        DIMENSION,
        MAX_MATRIX_OBJECT_BYTES,
        matrix_rows,
    )
    .map_err(|error| error.to_string())?;
    let attribute_rows = (0..DOCUMENT_COUNT)
        .map(|index| AttributeBlockInputRow {
            id: production_document_id(index),
            ordinal: index as u32,
            attributes: Some(document_attributes(index)),
        })
        .collect();
    let attribute_blocks = build_attribute_blocks(
        &namespace,
        SEGMENT_ID,
        MAX_ATTRIBUTE_OBJECT_BYTES,
        attribute_rows,
    )
    .map_err(|error| error.to_string())?;
    let matrix_locators: Vec<MatrixBlockLocator> = matrix_blocks
        .iter()
        .flat_map(|block| block.locators.iter().cloned())
        .collect();
    let attribute_locators: Vec<AttributeLocator> = attribute_blocks
        .iter()
        .flat_map(|block| block.locators.iter().cloned())
        .collect();
    if matrix_locators.len() != DOCUMENT_COUNT || attribute_locators.len() != DOCUMENT_COUNT {
        return Err("block locator counts disagree with the corpus".to_string());
    }
    let total_vector_count = documents.total_rows as u64;
    drop(documents);

    eprintln!("flat-bench: encoding the ZFQ1 flat artifact");
    let flat_rows: Vec<FlatRowMeta> = (0..DOCUMENT_COUNT)
        .map(|index| FlatRowMeta {
            id: production_document_id(index),
            matrix_locator: matrix_locators[index].clone(),
            attr_locator: attribute_locators[index].clone(),
            content_hash: document_content_hash_bytes(index),
            source_sequence: index as u64,
            color: FILTER_COLORS[index % FILTER_COLORS.len()].to_string(),
        })
        .collect();
    let (flat_bytes, flat_metadata_bytes) = encode_flat_artifact(&calibration, &flat_rows, &codes)?;
    let flat_artifact_sha256 = hex(&Sha256::digest(&flat_bytes));
    let flat_code_bytes = codes.iter().map(|code| code.len() as u64).sum::<u64>();
    let flat_key =
        format!("{namespace}/late/segments/{SEGMENT_ID}/flat-sq8-{flat_artifact_sha256}.bin");
    drop(codes);

    eprintln!("flat-bench: uploading artifacts to MinIO");
    let (store, get_requests, put_requests, get_latency_ms) = counting_minio_store()?;
    let mut uploaded_objects = 0usize;
    let mut uploaded_bytes = 0u64;
    for block in &matrix_blocks {
        store
            .put_create(&block.reference.key, block.bytes.clone())
            .await
            .map_err(|error| error.to_string())?;
        uploaded_objects += 1;
        uploaded_bytes += block.bytes.len() as u64;
    }
    for block in &attribute_blocks {
        store
            .put_create(&block.reference.key, block.bytes.clone())
            .await
            .map_err(|error| error.to_string())?;
        uploaded_objects += 1;
        uploaded_bytes += block.bytes.len() as u64;
    }
    let flat_payload = Bytes::from(flat_bytes);
    let flat_artifact_bytes = flat_payload.len() as u64;
    store
        .put_create(&flat_key, flat_payload)
        .await
        .map_err(|error| error.to_string())?;
    uploaded_objects += 1;
    uploaded_bytes += flat_artifact_bytes;
    let setup_put_requests = put_requests.load(AtomicOrdering::Relaxed);

    let segment = LateInteractionSegmentRef {
        id: SEGMENT_ID.to_string(),
        profile: EmbeddingProfileId::new("flat-bench-profile"),
        semantic_epoch: epoch,
        fde_generation: generation,
        matrix_dtype: MatrixDtype::F16,
        record_count: DOCUMENT_COUNT as u64,
        total_vector_count,
        vector_dimension: DIMENSION as u32,
        fde_dimension: FDE_DIMENSION as u32,
        coverage_sequence: 1,
        candidate_index: Some(placeholder_candidate_index(generation, &namespace)),
        matrix_objects: matrix_blocks
            .into_iter()
            .map(|block| block.reference)
            .collect(),
        attribute_objects: attribute_blocks
            .into_iter()
            .map(|block| block.reference)
            .collect(),
        fts_objects: Vec::new(),
        artifact_origin: None,
        candidate_kind: crate::wal::LateCandidateKind::Ivf,
        flat_candidate: None,
    };

    eprintln!("flat-bench: measuring cold hydration");
    let mut cold_hydrations = Vec::with_capacity(COLD_HYDRATIONS);
    let mut resident = None;
    for _ in 0..COLD_HYDRATIONS {
        let fetch_started = Instant::now();
        let fetched = store
            .get(&flat_key)
            .await
            .map_err(|error| error.to_string())?;
        let fetch_ms = fetch_started.elapsed().as_secs_f64() * 1_000.0;
        let decode_started = Instant::now();
        let decoded = decode_flat_artifact(&fetched)?;
        let decode_ms = decode_started.elapsed().as_secs_f64() * 1_000.0;
        cold_hydrations.push(ColdHydration {
            fetch_ms,
            decode_ms,
            bytes: fetched.len() as u64,
        });
        resident = Some(decoded);
    }
    let resident = resident.ok_or_else(|| "cold hydration produced no index".to_string())?;
    if resident.rows.len() != DOCUMENT_COUNT
        || resident.codes.len() != DOCUMENT_COUNT * FDE_DIMENSION
    {
        return Err("hydrated flat index shape mismatch".to_string());
    }
    let resident_bytes = (resident.codes.len()
        + resident.rows.len() * std::mem::size_of::<FlatRowMeta>()) as u64
        + flat_metadata_bytes;

    eprintln!(
        "flat-bench: running {query_count} warm queries at K={CANDIDATE_K}, \
         gap_budget={read_gap_budget_bytes}, max_request={read_max_request_bytes}, \
         concurrency={read_max_concurrency}, pipeline={pipeline_enabled}"
    );
    let bounds = SegmentSearchBounds {
        max_resident_bytes: 64 * 1024 * 1024,
        max_cluster_bytes: 64 * 1024 * 1024,
        max_vectors_per_document: 512,
        max_attribute_payload_bytes: 64 * 1024,
    };
    let id_to_index: HashMap<String, usize> = (0..DOCUMENT_COUNT)
        .map(|index| (production_document_id(index), index))
        .collect();
    let excluded_ids = BTreeSet::new();

    let mut hits_k700 = 0usize;
    let mut hits_k1000 = 0usize;
    let mut hits_k1500 = 0usize;
    let mut per_query_hits = Vec::with_capacity(query_count);
    let mut per_query_top10_ids = Vec::with_capacity(query_count);
    let mut warm_ms = Vec::with_capacity(query_count);
    let mut scan_ms = Vec::with_capacity(query_count);
    let mut truth_ms = Vec::with_capacity(query_count);
    let mut fetch_ms = Vec::with_capacity(query_count);
    let mut score_ms = Vec::with_capacity(query_count);
    let mut decode_ms = Vec::with_capacity(query_count);
    let mut maxsim_ms = Vec::with_capacity(query_count);
    let mut truth_score_workers = 0usize;
    let mut truth_logical = Vec::with_capacity(query_count);
    let mut truth_logical_bytes = Vec::with_capacity(query_count);
    let mut truth_planned_requests = Vec::with_capacity(query_count);
    let mut truth_planned_bytes = Vec::with_capacity(query_count);
    let mut truth_gap_waste_bytes = Vec::with_capacity(query_count);
    let mut truth_request_waves = Vec::with_capacity(query_count);
    let observed_equals_planned = true;
    {
        let mut latencies = get_latency_ms.lock().expect("GET latency lock");
        latencies.clear();
        latencies.reserve(query_count * CANDIDATE_K);
    }

    for query_index in 0..query_count {
        let query_started = Instant::now();
        let scan_started = Instant::now();
        let order = flat_selection_order(&resident, &query_fdes[query_index]);
        let scan_elapsed = scan_started.elapsed();
        let selected = &order[..CANDIDATE_K];

        let gold_set: HashSet<usize> = gold[query_index]
            .iter()
            .map(|gold| gold.document_index)
            .collect();
        hits_k700 += order[..700]
            .iter()
            .filter(|(index, _)| gold_set.contains(index))
            .count();
        hits_k1500 += order[..1_500]
            .iter()
            .filter(|(index, _)| gold_set.contains(index))
            .count();
        let candidate_hits = selected
            .iter()
            .filter(|(index, _)| gold_set.contains(index))
            .count();

        let candidates = selected
            .iter()
            .map(|&(index, negated_dot)| flat_candidate(&resident.rows[index], -negated_dot, None))
            .collect::<BenchResult<Vec<_>>>()?;
        let truth_started = Instant::now();
        let truth_requests =
            build_truth_requests(&segment, &candidates).map_err(|error| error.to_string())?;
        let plan = ReadPlan::build(&truth_requests, &read_plan_config)
            .map_err(|error| error.to_string())?;
        let planned_request_count = plan.planned_request_count() as u64;
        let observed_before = get_requests.load(AtomicOrdering::Relaxed);
        let request = SegmentSearchRequest {
            store: &store,
            bootstrap_cache: None,
            segment: &segment,
            exact_query: queries.matrix(query_index)?,
            candidate_query_fde: &query_fdes[query_index],
            mandatory_filter: None,
            request_filter: None,
            excluded_ids: &excluded_ids,
            top_k: GOLD_PER_QUERY,
            read_plan: &read_plan_config,
            bounds,
        };
        let piped = execute_truth_wave_pipelined(&request, candidates, &plan)
            .await
            .map_err(|error| error.to_string())?;
        let (mut rows, logical_bytes, fetch_elapsed, score_elapsed, timing) = (
            piped.rows,
            piped.logical_bytes,
            piped.fetch_elapsed,
            piped.score_elapsed,
            piped.timing,
        );
        if truth_score_workers == 0 {
            truth_score_workers = timing.workers;
        } else if truth_score_workers != timing.workers {
            return Err(format!(
                "truth score worker count changed from {truth_score_workers} to {}",
                timing.workers
            ));
        }
        // The flat frontier contains unique documents, so its matrix and
        // attribute ranges do not overlap and physical excess is gap waste.
        let gap_waste_bytes = plan
            .planned_bytes()
            .checked_sub(logical_bytes)
            .ok_or_else(|| {
                format!(
                "query {query_index}: planned {} bytes but delivered {logical_bytes} logical bytes",
                plan.planned_bytes()
            )
            })?;
        let observed_delta = get_requests.load(AtomicOrdering::Relaxed) - observed_before;
        if observed_delta != planned_request_count {
            return Err(format!(
                "query {query_index}: observed {observed_delta} GETs, planned {planned_request_count}"
            ));
        }
        rows.sort_by(compare_scored_rows);
        rows.truncate(GOLD_PER_QUERY);
        let truth_elapsed = truth_started.elapsed();
        let query_elapsed = query_started.elapsed();

        let final_hits = rows
            .iter()
            .filter(|row| {
                id_to_index
                    .get(row.id.as_str())
                    .is_some_and(|index| gold_set.contains(index))
            })
            .count();
        if final_hits != candidate_hits {
            return Err(format!(
                "query {query_index}: exact rerank returned {final_hits} golds but the \
                 candidate frontier contained {candidate_hits}"
            ));
        }
        hits_k1000 += final_hits;
        per_query_hits.push(final_hits as u64);
        per_query_top10_ids.push(rows.iter().map(|row| row.id.clone()).collect());
        warm_ms.push(query_elapsed.as_secs_f64() * 1_000.0);
        scan_ms.push(scan_elapsed.as_secs_f64() * 1_000.0);
        truth_ms.push(truth_elapsed.as_secs_f64() * 1_000.0);
        fetch_ms.push(fetch_elapsed.as_secs_f64() * 1_000.0);
        score_ms.push(score_elapsed.as_secs_f64() * 1_000.0);
        decode_ms.push(timing.decode.as_secs_f64() * 1_000.0);
        maxsim_ms.push(timing.maxsim.as_secs_f64() * 1_000.0);
        truth_logical.push(truth_requests.len() as u64);
        truth_logical_bytes.push(logical_bytes);
        truth_planned_requests.push(planned_request_count);
        truth_planned_bytes.push(plan.planned_bytes());
        truth_gap_waste_bytes.push(gap_waste_bytes);
        truth_request_waves
            .push(planned_request_count.div_ceil(read_plan_config.max_concurrent_requests as u64));
        if (query_index + 1) % 200 == 0 {
            eprintln!(
                "flat-bench: {} queries done, running recall {:.6}",
                query_index + 1,
                hits_k1000 as f64 / ((query_index + 1) * GOLD_PER_QUERY) as f64
            );
        }
    }

    if query_count == QUERY_COUNT
        && (hits_k700 != EXPECTED_HITS_K700
            || hits_k1000 != EXPECTED_HITS_K1000
            || hits_k1500 != EXPECTED_HITS_K1500)
    {
        return Err(format!(
            "recall parity failed: K700 {hits_k700}/{EXPECTED_HITS_K700}, \
             K1000 {hits_k1000}/{EXPECTED_HITS_K1000}, \
             K1500 {hits_k1500}/{EXPECTED_HITS_K1500}"
        ));
    }
    let truth_get_latency_ms = get_latency_ms.lock().expect("GET latency lock").clone();
    let truth_total_get_requests = truth_planned_requests.iter().sum::<u64>();
    if truth_get_latency_ms.len() as u64 != truth_total_get_requests {
        return Err(format!(
            "recorded {} truth GET latencies for {truth_total_get_requests} requests",
            truth_get_latency_ms.len()
        ));
    }
    let truth_total_request_waves = truth_request_waves.iter().sum::<u64>();

    let filtered_query_count = FILTERED_QUERY_COUNT.min(query_count);
    eprintln!("flat-bench: running {filtered_query_count} filtered queries");
    let filter = Filter::Eq {
        field: "color".to_string(),
        value: AttributeValue::String(FILTER_COLORS[0].to_string()),
    };
    let mut filtered_planned = Vec::with_capacity(filtered_query_count);
    let mut filtered_all_results_match = true;
    for (query_index, query_fde) in query_fdes.iter().enumerate().take(filtered_query_count) {
        let order = flat_selection_order(&resident, query_fde);
        let candidates = order
            .iter()
            .filter(|(index, _)| {
                let attributes = Some(document_attributes(*index));
                filter_matches(Some(&filter), attributes.as_ref())
            })
            .take(CANDIDATE_K)
            .map(|&(index, negated_dot)| {
                flat_candidate(&resident.rows[index], -negated_dot, Some(&filter))
            })
            .collect::<BenchResult<Vec<_>>>()?;
        let truth_requests =
            build_truth_requests(&segment, &candidates).map_err(|error| error.to_string())?;
        let plan = ReadPlan::build(&truth_requests, &read_plan_config)
            .map_err(|error| error.to_string())?;
        filtered_planned.push(plan.planned_request_count() as u64);
        let observed_before = get_requests.load(AtomicOrdering::Relaxed);
        let request = SegmentSearchRequest {
            store: &store,
            bootstrap_cache: None,
            segment: &segment,
            exact_query: queries.matrix(query_index)?,
            candidate_query_fde: query_fde,
            mandatory_filter: None,
            request_filter: Some(&filter),
            excluded_ids: &excluded_ids,
            top_k: GOLD_PER_QUERY,
            read_plan: &read_plan_config,
            bounds,
        };
        let mut rows = execute_truth_wave_pipelined(&request, candidates, &plan)
            .await
            .map_err(|error| error.to_string())?
            .rows;
        let observed_delta = get_requests.load(AtomicOrdering::Relaxed) - observed_before;
        if observed_delta != plan.planned_request_count() as u64 {
            return Err(format!(
                "filtered query {query_index}: observed GETs diverge from the plan"
            ));
        }
        rows.sort_by(compare_scored_rows);
        rows.truncate(GOLD_PER_QUERY);
        for row in &rows {
            let matches = row.attributes.as_ref().is_some_and(|attributes| {
                attributes.get("color")
                    == Some(&AttributeValue::String(FILTER_COLORS[0].to_string()))
            });
            if !matches {
                filtered_all_results_match = false;
            }
        }
    }
    if !filtered_all_results_match {
        return Err("filtered arm returned a row that fails the filter".to_string());
    }

    let report = BenchReport {
        schema_version: 4,
        source_revision: "518aa23+flat-bench".to_string(),
        candidate_k: CANDIDATE_K,
        read_gap_budget_bytes,
        read_max_request_bytes,
        read_max_concurrency,
        pipeline_enabled,
        document_count: DOCUMENT_COUNT,
        query_count,
        gold_memberships: query_count * GOLD_PER_QUERY,
        hits_k700,
        hits_k1000,
        hits_k1500,
        recall_k1000: hits_k1000 as f64 / (query_count * GOLD_PER_QUERY) as f64,
        per_query_hits_k1000: integer_summary(&per_query_hits),
        per_query_hits_k1000_values: per_query_hits.clone(),
        per_query_top10_ids,
        queries_below_8_of_10: per_query_hits.iter().filter(|&&hits| hits < 8).count(),
        queries_below_5_of_10: per_query_hits.iter().filter(|&&hits| hits < 5).count(),
        flat_artifact_bytes,
        flat_metadata_bytes,
        flat_code_bytes,
        resident_bytes,
        uploaded_objects,
        uploaded_bytes,
        setup_put_requests,
        cold_hydrations,
        warm_query_ms: integer_summary_f64(&warm_ms),
        sq8_scan_ms: integer_summary_f64(&scan_ms),
        truth_wave_ms: integer_summary_f64(&truth_ms),
        truth_fetch_ms: float_summary(&fetch_ms),
        truth_score_ms: float_summary(&score_ms),
        truth_decode_ms: float_summary(&decode_ms),
        truth_maxsim_ms: float_summary(&maxsim_ms),
        truth_score_workers,
        truth_get_latency_ms: float_summary(&truth_get_latency_ms),
        truth_logical_ranges: integer_summary(&truth_logical),
        truth_logical_bytes: integer_summary(&truth_logical_bytes),
        truth_planned_requests: integer_summary(&truth_planned_requests),
        truth_planned_bytes: integer_summary(&truth_planned_bytes),
        truth_gap_waste_bytes: integer_summary(&truth_gap_waste_bytes),
        truth_request_waves: integer_summary(&truth_request_waves),
        truth_total_get_requests,
        truth_total_request_waves,
        observed_get_requests_equal_planned: observed_equals_planned,
        query_fde_encode_ms_mean,
        filtered_queries: filtered_query_count,
        filtered_all_results_match,
        filtered_planned_requests: integer_summary(&filtered_planned),
        flat_artifact_sha256,
        sq8_code_sha256,
        sq8_calibration_sha256,
        elapsed_ms: started.elapsed().as_millis(),
    };
    let serialized = serde_json::to_vec_pretty(&report)
        .map_err(|error| format!("cannot serialize report: {error}"))?;
    fs::write(&output, serialized)
        .map_err(|error| format!("cannot write {}: {error}", output.display()))?;
    eprintln!(
        "phase9_flat_sq8 lane=text candidate_k={CANDIDATE_K} hits={hits_k1000}/{} \
         recall={:.6} warm_p50_ms={:.1} warm_p95_ms={:.1} \
         planned_bytes_p50={} artifact_bytes={flat_artifact_bytes} elapsed_ms={}",
        query_count * GOLD_PER_QUERY,
        report.recall_k1000,
        report.warm_query_ms.p50,
        report.warm_query_ms.p95,
        report.truth_planned_bytes.p50,
        report.elapsed_ms,
    );

    eprintln!("flat-bench: cleaning namespace {namespace}");
    let removed = store
        .delete_prefix(&format!("{namespace}/"))
        .await
        .map_err(|error| error.to_string())?;
    if removed < uploaded_objects {
        return Err(format!(
            "cleanup removed {removed} objects, expected at least {uploaded_objects}"
        ));
    }
    Ok(())
}

/// Score every resident SQ8 row and return the full ascending selection order.
///
/// `asymmetric_dot_product` returns the negated reconstructed dot product, so
/// ascending order ranks best-first. The comparator (score bits, then row
/// index) replicates the routing diagnostic exactly; the prefix at any K is
/// the diagnostic's selected frontier at that K.
fn flat_selection_order(resident: &ResidentFlatIndex, query_fde: &[f32]) -> Vec<(usize, f32)> {
    let mut scores: Vec<(usize, f32)> = (0..resident.rows.len())
        .map(|index| {
            let code = &resident.codes[index * FDE_DIMENSION..(index + 1) * FDE_DIMENSION];
            (
                index,
                resident.calibration.asymmetric_dot_product(query_fde, code),
            )
        })
        .collect();
    scores.sort_unstable_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    scores
}

fn flat_candidate(
    row: &FlatRowMeta,
    approx_fde_score: f32,
    filter: Option<&Filter>,
) -> BenchResult<super::candidate::LateCandidate> {
    let attributes = Some(color_attributes(&row.color));
    if !filter_matches(filter, attributes.as_ref()) {
        return Err(format!("candidate {} fails its own filter", row.id));
    }
    Ok(super::candidate::LateCandidate {
        id: VectorId::from(row.id.clone()),
        approx_fde_score,
        matrix_locator: row.matrix_locator.clone(),
        attr_locator: row.attr_locator.clone(),
        metadata: super::candidate::LateCandidateMetadata {
            content_hash: ContentHash::new(row.content_hash),
            source_sequence: row.source_sequence,
            parent_id: None,
            unit_ordinal: None,
            attributes,
        },
    })
}

fn placeholder_candidate_index(
    generation: FdeGenerationId,
    namespace: &str,
) -> LateCandidateIndexRef {
    // The flat arm never routes through the IVF candidate index; the segment
    // descriptor requires one, so a minimal unused reference is recorded.
    LateCandidateIndexRef {
        bootstrap: LateCandidateBootstrapRef {
            key: format!("{namespace}/late/segments/{SEGMENT_ID}/unused-bootstrap.bin"),
            checksum: ArtifactChecksum::new([0; 32]),
            size_bytes: 1,
            recipe: LateCandidateRecipe {
                fde_generation: generation,
                fde_dimension: FDE_DIMENSION as u32,
                nlist: 1,
                probe_budget: 1,
                candidate_k: CANDIDATE_K as u32,
                routing_metric: LateRoutingMetric::NegativeL2,
            },
            row_count: DOCUMENT_COUNT as u64,
            format_version: 1,
        },
        clusters: Vec::new(),
    }
}

fn document_attributes(index: usize) -> HashMap<String, AttributeValue> {
    color_attributes(FILTER_COLORS[index % FILTER_COLORS.len()])
}

fn color_attributes(color: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([(
        "color".to_string(),
        AttributeValue::String(color.to_string()),
    )])
}

fn document_content_hash(index: usize) -> ContentHash {
    ContentHash::new(document_content_hash_bytes(index))
}

fn document_content_hash_bytes(index: usize) -> [u8; 32] {
    *ArtifactChecksum::digest(production_document_id(index).as_bytes()).as_bytes()
}

fn encode_flat_artifact(
    calibration: &SqCalibration,
    rows: &[FlatRowMeta],
    codes: &[Vec<u8>],
) -> BenchResult<(Vec<u8>, u64)> {
    let header = FlatHeader {
        fde_dimension: FDE_DIMENSION as u32,
        row_count: rows.len() as u32,
        calibration: calibration.to_bytes().to_vec(),
        rows: rows.to_vec(),
    };
    let header_bytes = rmp_serde::to_vec(&header)
        .map_err(|error| format!("cannot encode flat header: {error}"))?;
    let code_bytes: usize = codes.iter().map(Vec::len).sum();
    let mut output = Vec::with_capacity(FLAT_MAGIC.len() + 1 + 4 + header_bytes.len() + code_bytes);
    output.extend_from_slice(FLAT_MAGIC);
    output.push(FLAT_VERSION);
    output.extend_from_slice(&(header_bytes.len() as u32).to_le_bytes());
    output.extend_from_slice(&header_bytes);
    for code in codes {
        output.extend_from_slice(code);
    }
    Ok((output, header_bytes.len() as u64))
}

fn decode_flat_artifact(bytes: &[u8]) -> BenchResult<ResidentFlatIndex> {
    if bytes.len() < FLAT_MAGIC.len() + 1 + 4 || &bytes[..4] != FLAT_MAGIC {
        return Err("flat artifact magic mismatch".to_string());
    }
    if bytes[4] != FLAT_VERSION {
        return Err("flat artifact version mismatch".to_string());
    }
    let header_len = u32::from_le_bytes(
        bytes[5..9]
            .try_into()
            .map_err(|_| "flat artifact header length is unreadable".to_string())?,
    ) as usize;
    let header_end = 9usize
        .checked_add(header_len)
        .ok_or_else(|| "flat artifact header length overflows".to_string())?;
    if bytes.len() < header_end {
        return Err("flat artifact is truncated before its header ends".to_string());
    }
    let header: FlatHeader = rmp_serde::from_slice(&bytes[9..header_end])
        .map_err(|error| format!("cannot decode flat header: {error}"))?;
    let calibration = SqCalibration::from_bytes(&header.calibration)
        .map_err(|error| format!("cannot decode flat calibration: {error}"))?;
    let expected_codes = header.row_count as usize * header.fde_dimension as usize;
    let codes = bytes[header_end..].to_vec();
    if header.fde_dimension as usize != FDE_DIMENSION
        || codes.len() != expected_codes
        || header.rows.len() != header.row_count as usize
    {
        return Err("flat artifact payload shape mismatch".to_string());
    }
    Ok(ResidentFlatIndex {
        calibration,
        rows: header.rows,
        codes,
    })
}

fn counting_minio_store() -> BenchResult<CountingStoreSetup> {
    use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
    use object_store::ClientOptions;

    let bucket = env::var("TEST_S3_BUCKET").unwrap_or_else(|_| "zeppelin-test".to_string());
    let endpoint =
        env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());
    let access_key = env::var("MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_key = env::var("MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let inner = AmazonS3Builder::new()
        .with_bucket_name(&bucket)
        .with_region("us-east-1")
        .with_endpoint(&endpoint)
        .with_virtual_hosted_style_request(false)
        .with_access_key_id(&access_key)
        .with_secret_access_key(&secret_key)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .with_client_options(
            ClientOptions::new()
                .with_allow_http(true)
                .with_pool_max_idle_per_host(64),
        )
        .build()
        .map_err(|error| format!("cannot build MinIO store: {error}"))?;
    let get_requests = Arc::new(AtomicU64::new(0));
    let put_requests = Arc::new(AtomicU64::new(0));
    let get_latency_ms = Arc::new(Mutex::new(Vec::new()));
    let counting = CountingStore {
        inner: Arc::new(inner),
        get_requests: Arc::clone(&get_requests),
        put_requests: Arc::clone(&put_requests),
        get_latency_ms: Arc::clone(&get_latency_ms),
    };
    Ok((
        ZeppelinStore::new(Arc::new(counting)),
        get_requests,
        put_requests,
        get_latency_ms,
    ))
}

/// Build the production-semantics FDEs: subtract the f32 sampled document-row
/// mean, push documents through the f16 persistence boundary, then encode.
/// Replicates the routing diagnostic's `production_*` variant bit-for-bit
/// (verified by the pinned FDE checksums).
pub(super) fn build_production_fdes(
    documents: &TensorSet,
    queries: &TensorSet,
    transform: &FdeTransform,
) -> BenchResult<ProductionFdes> {
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

    let document_fdes = parallel_indexed_map(documents.sidecar.ids.len(), |index| {
        let matrix = documents.matrix(index)?;
        let centered: Vec<f32> = matrix
            .values()
            .iter()
            .zip(mean32.iter().cycle())
            .map(|(value, mean)| *value - *mean)
            .collect();
        let embedding = MultiVectorEmbedding::new(
            centered,
            matrix.vector_count(),
            DIMENSION,
            matrix.vector_count(),
        )
        .map_err(|error| error.to_string())?;
        let encoded = encode_matrix_payload(MatrixDtype::F16, DIMENSION, &embedding)
            .map_err(|error| error.to_string())?;
        let decoded = decode_matrix_payload(
            &encoded,
            MatrixDtype::F16,
            DIMENSION,
            matrix.vector_count(),
            matrix.vector_count(),
        )
        .map_err(|error| error.to_string())?;
        transform
            .encode_document(&decoded.matrix_ref().map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())
    })?;

    let encode_timings = Mutex::new(Vec::with_capacity(queries.sidecar.ids.len()));
    let query_fdes = parallel_indexed_map(queries.sidecar.ids.len(), |index| {
        let matrix = queries.matrix(index)?;
        let centered: Vec<f32> = matrix
            .values()
            .iter()
            .zip(mean32.iter().cycle())
            .map(|(value, mean)| *value - *mean)
            .collect();
        let centered_matrix = MultiVectorMatrixRef::new(
            &centered,
            matrix.vector_count(),
            DIMENSION,
            matrix.vector_count(),
        )
        .map_err(|error| error.to_string())?;
        let encode_started = Instant::now();
        let fde = transform
            .encode_query(&centered_matrix)
            .map_err(|error| error.to_string())?;
        encode_timings
            .lock()
            .expect("encode timing lock")
            .push(encode_started.elapsed().as_secs_f64() * 1_000.0);
        Ok(fde)
    })?;
    let timings = encode_timings.into_inner().expect("encode timing lock");
    let encode_ms_mean = timings.iter().sum::<f64>() / timings.len().max(1) as f64;
    Ok((document_fdes, query_fdes, encode_ms_mean))
}

pub(super) fn required_path(name: &str) -> BenchResult<PathBuf> {
    env::var_os(name)
        .map(PathBuf::from)
        .ok_or_else(|| format!("{name} must be set"))
}

fn optional_usize(name: &str, default: usize) -> BenchResult<usize> {
    match env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .map_err(|error| format!("invalid {name}={value:?}: {error}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("cannot read {name}: {error}")),
    }
}

pub(super) fn load_tensor(
    raw_path: &Path,
    sidecar_path: &Path,
    expected: &str,
) -> BenchResult<TensorSet> {
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
    })
}

fn hash_frame(hasher: &mut Sha256, label: &[u8], bytes: &[u8]) {
    hasher.update((label.len() as u64).to_le_bytes());
    hasher.update(label);
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

pub(super) fn load_gold(
    path: &Path,
    documents: &TensorSet,
    queries: &TensorSet,
) -> BenchResult<Vec<Vec<GoldDocument>>> {
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

fn production_document_id(index: usize) -> String {
    format!("mmli-replay-d-{index:020}")
}

pub(super) fn checksum_vectors(vectors: &[Vec<f32>], dimension: usize) -> String {
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

fn checksum_bytes_rows(rows: &[Vec<u8>]) -> String {
    let mut hasher = Sha256::new();
    hasher.update((rows.len() as u64).to_le_bytes());
    for row in rows {
        hasher.update((row.len() as u64).to_le_bytes());
        hasher.update(row);
    }
    hex(&hasher.finalize())
}

fn checksum_calibration(calibration: &SqCalibration) -> String {
    let mut hasher = Sha256::new();
    hasher.update((calibration.dim as u64).to_le_bytes());
    for values in [&calibration.mins, &calibration.maxs] {
        for value in values {
            hasher.update(value.to_bits().to_le_bytes());
        }
    }
    hex(&hasher.finalize())
}

fn integer_summary(values: &[u64]) -> IntegerSummary {
    let mut ordered = values.to_vec();
    ordered.sort_unstable();
    let percentile = |percent: usize| -> u64 {
        if ordered.is_empty() {
            return 0;
        }
        let rank = (percent * ordered.len()).div_ceil(100).max(1);
        ordered[rank - 1]
    };
    IntegerSummary {
        min: ordered.first().copied().unwrap_or(0),
        p5: percentile(5),
        p50: percentile(50),
        p95: percentile(95),
        p99: percentile(99),
        max: ordered.last().copied().unwrap_or(0),
        mean: if ordered.is_empty() {
            0.0
        } else {
            ordered.iter().map(|&value| value as f64).sum::<f64>() / ordered.len() as f64
        },
    }
}

fn integer_summary_f64(values: &[f64]) -> IntegerSummary {
    let as_u64: Vec<u64> = values.iter().map(|&value| value.round() as u64).collect();
    integer_summary(&as_u64)
}

fn float_summary(values: &[f64]) -> FloatSummary {
    let mut ordered = values.to_vec();
    ordered.sort_by(f64::total_cmp);
    let percentile = |percent: usize| -> f64 {
        if ordered.is_empty() {
            return 0.0;
        }
        let rank = (percent * ordered.len()).div_ceil(100).max(1);
        ordered[rank - 1]
    };
    FloatSummary {
        min: ordered.first().copied().unwrap_or(0.0),
        p5: percentile(5),
        p50: percentile(50),
        p95: percentile(95),
        p99: percentile(99),
        max: ordered.last().copied().unwrap_or(0.0),
        mean: if ordered.is_empty() {
            0.0
        } else {
            ordered.iter().sum::<f64>() / ordered.len() as f64
        },
    }
}

pub(super) fn parallel_indexed_map<T, F>(count: usize, operation: F) -> BenchResult<Vec<T>>
where
    T: Send,
    F: Fn(usize) -> BenchResult<T> + Sync,
{
    let worker_count = std::thread::available_parallelism()
        .map_or(1, usize::from)
        .min(count.max(1));
    let next = AtomicUsize::new(0);
    let results: Mutex<Vec<Option<BenchResult<T>>>> =
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
                results.lock().expect("bench result lock")[index] = Some(result);
            });
        }
    });
    results
        .into_inner()
        .expect("bench result lock")
        .into_iter()
        .enumerate()
        .map(|(index, result)| {
            result.ok_or_else(|| format!("worker omitted bench result {index}"))?
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

pub(super) fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}
