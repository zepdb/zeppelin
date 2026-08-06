//! Resident exhaustive flat-SQ8 candidate artifacts (`ZFQ1`).
//!
//! For the ≲50–100k-unit corpus regime, candidate selection is an exhaustive
//! scan of SQ8-quantized document FDEs held resident after one hydration fetch.
//! One immutable object carries the production [`SqCalibration`], per-row
//! locator metadata, and the raw codes; queries never read per-candidate
//! objects. IVF wave-one routing (`candidate.rs`) is retained only for the
//! future scale phase. Adopted 2026-07-31 from the measured Phase 9 benchmark
//! (`tasks/MMLI-2/results/phase9-flat-sq8-benchmark.md`).

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap};
use std::mem::size_of;
use std::thread;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::embedding::{ArtifactChecksum, ContentHash, FdeGenerationId};
use crate::error::{Result, ZeppelinError};
use crate::index::quantization::sq::SqCalibration;
use crate::types::{AttributeValue, Filter, VectorId};

use super::candidate::{
    late_segment_key, validate_attribute_locator, validate_filter_attributes, validate_input_row,
    validate_matrix_locator, AttributeLocator, CandidateFdeSource, LateCandidate,
    LateCandidateInputRow, LateCandidateMetadata,
};
use super::matrix_artifact::MatrixBlockLocator;
use super::segment_search::filter_matches;

const FLAT_MAGIC: &[u8; 4] = b"ZFQ1";
const FLAT_ARTIFACT_VERSION: u8 = 1;
const FLAT_HEADER_LEN: usize = 4 + 1 + size_of::<u64>();

/// Persisted flat-candidate format version carried by manifest references.
pub(crate) const LATE_FLAT_FORMAT_VERSION: u32 = 1;

/// Persisted operating point for one flat-SQ8 candidate artifact.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LateFlatCandidateRecipe {
    /// Candidate FDE generation represented by every row.
    pub fde_generation: FdeGenerationId,
    /// Coordinates in every document and query FDE.
    pub fde_dimension: u32,
    /// Candidate frontier retained after filtering and SQ8 scoring.
    pub candidate_k: u32,
}

/// Manifest-visible reference to one immutable flat-SQ8 candidate object.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LateFlatCandidateRef {
    /// Exact immutable object key.
    pub key: String,
    /// SHA-256 over the complete artifact.
    pub checksum: ArtifactChecksum,
    /// Complete artifact size.
    pub size_bytes: u64,
    /// Live rows covered by the artifact.
    pub row_count: u64,
    /// Persisted selection operating point.
    pub recipe: LateFlatCandidateRecipe,
    /// Persisted codec version.
    pub format_version: u32,
}

/// Explicit build controls supplied by late-interaction configuration.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LateFlatCandidateBuildConfig {
    /// Coordinates in every input FDE.
    pub(crate) fde_dimension: usize,
    /// Frontier retained after filtering and SQ8 scoring.
    pub(crate) candidate_k: usize,
    /// Hard maximum complete artifact bytes; this artifact is held resident.
    pub(crate) max_artifact_bytes: usize,
}

/// Complete flat-candidate build output prior to manifest publication.
#[derive(Debug)]
pub(crate) struct BuiltLateFlatCandidate {
    /// Manifest-rooted artifact descriptor.
    pub(crate) reference: LateFlatCandidateRef,
    /// Complete immutable artifact bytes.
    pub(crate) bytes: Bytes,
}

/// One decoded baseline row carried by the resident flat index.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FlatCandidateRow {
    /// Retrieval-unit identity.
    pub(crate) id: VectorId,
    /// Source content identity represented by the segment row.
    pub(crate) content_hash: ContentHash,
    /// Authoritative source mutation sequence.
    pub(crate) source_sequence: u64,
    /// Optional caller-provided parent identity.
    pub(crate) parent_id: Option<String>,
    /// Optional ordinal within the parent.
    pub(crate) unit_ordinal: Option<u32>,
    /// Direct ranged-read locator for the exact document matrix.
    pub(crate) matrix_locator: MatrixBlockLocator,
    /// Direct ranged-read locator for exact attributes.
    pub(crate) attr_locator: AttributeLocator,
    /// Exact attributes used by the wave-one filter evaluator.
    pub(crate) filter_attributes: Option<HashMap<String, AttributeValue>>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FlatRowWire {
    id: VectorId,
    content_hash: ContentHash,
    source_sequence: u64,
    parent_id: Option<String>,
    unit_ordinal: Option<u32>,
    matrix_locator: MatrixBlockLocator,
    attr_locator: AttributeLocator,
    filter_attributes: Option<BTreeMap<String, AttributeValue>>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FlatHeaderWire {
    fde_generation: FdeGenerationId,
    fde_dimension: u32,
    candidate_k: u32,
    row_count: u32,
    calibration: Vec<u8>,
    rows: Vec<FlatRowWire>,
}

/// Resident hydrated flat-SQ8 candidate index for one manifest-selected segment.
#[derive(Debug)]
pub(crate) struct ResidentFlatCandidateIndex {
    recipe: LateFlatCandidateRecipe,
    calibration: SqCalibration,
    rows: Vec<FlatCandidateRow>,
    codes: Vec<u8>,
}

/// Calibration authority for one flat-SQ8 build.
#[derive(Clone, Debug)]
pub(crate) enum FlatCalibrationSource {
    /// Recalibrate over the complete row set; every row must supply a raw FDE.
    Recalibrate,
    /// Reuse the previous artifact's calibration; `Sq8` rows carry their codes
    /// verbatim and raw rows are encoded under the frozen calibration.
    Frozen(SqCalibration),
}

/// Build one deterministic flat-SQ8 candidate artifact (recalibrating).
#[cfg(test)]
pub(crate) fn build_flat_candidate_artifact(
    namespace: &str,
    segment_id: &str,
    fde_generation: FdeGenerationId,
    config: LateFlatCandidateBuildConfig,
    rows: Vec<LateCandidateInputRow>,
) -> Result<BuiltLateFlatCandidate> {
    build_flat_candidate_artifact_with_calibration(
        namespace,
        segment_id,
        fde_generation,
        config,
        FlatCalibrationSource::Recalibrate,
        rows,
    )
}

/// Build one flat-SQ8 artifact under an explicit calibration authority.
pub(crate) fn build_flat_candidate_artifact_with_calibration(
    namespace: &str,
    segment_id: &str,
    fde_generation: FdeGenerationId,
    config: LateFlatCandidateBuildConfig,
    calibration_source: FlatCalibrationSource,
    mut rows: Vec<LateCandidateInputRow>,
) -> Result<BuiltLateFlatCandidate> {
    if config.fde_dimension == 0
        || config.candidate_k == 0
        || config.max_artifact_bytes <= FLAT_HEADER_LEN
        || rows.is_empty()
    {
        return Err(flat_error("invalid explicit flat candidate build config"));
    }
    let fde_dimension = u32::try_from(config.fde_dimension)
        .map_err(|_| flat_error("flat FDE dimension exceeds u32"))?;
    let candidate_k = u32::try_from(config.candidate_k)
        .map_err(|_| flat_error("flat candidate K exceeds u32"))?;
    rows.sort_by(|left, right| left.id.cmp(&right.id));
    let mut ids = BTreeSet::new();
    for row in &rows {
        validate_input_row(row, config.fde_dimension)?;
        if !ids.insert(row.id.clone()) {
            return Err(flat_error("flat candidate build contains a duplicate id"));
        }
    }

    let calibration = match &calibration_source {
        FlatCalibrationSource::Recalibrate => {
            let fde_refs: Vec<&[f32]> = rows
                .iter()
                .map(|row| row.fde.raw())
                .collect::<Result<Vec<_>>>()
                .map_err(|_| flat_error("flat recalibration requires a raw FDE for every row"))?;
            SqCalibration::calibrate(&fde_refs, config.fde_dimension)
        }
        FlatCalibrationSource::Frozen(calibration) => calibration.clone(),
    };
    let mut codes = Vec::with_capacity(rows.len() * config.fde_dimension);
    for row in &rows {
        match &row.fde {
            CandidateFdeSource::Raw(fde) => {
                let code = calibration.encode(fde);
                if code.len() != config.fde_dimension {
                    return Err(flat_error("flat SQ8 code width disagrees with dimension"));
                }
                codes.extend_from_slice(&code);
            }
            CandidateFdeSource::Sq8(code) => {
                if matches!(calibration_source, FlatCalibrationSource::Recalibrate) {
                    return Err(flat_error("carried SQ8 codes require a frozen calibration"));
                }
                codes.extend_from_slice(code);
            }
        }
    }

    let recipe = LateFlatCandidateRecipe {
        fde_generation,
        fde_dimension,
        candidate_k,
    };
    let header = FlatHeaderWire {
        fde_generation,
        fde_dimension,
        candidate_k,
        row_count: u32::try_from(rows.len())
            .map_err(|_| flat_error("flat candidate row count exceeds u32"))?,
        calibration: calibration.to_bytes().to_vec(),
        rows: rows.into_iter().map(flat_row_wire).collect(),
    };
    let header_bytes = rmp_serde::to_vec_named(&header)
        .map_err(|error| flat_error(format!("failed to encode flat header: {error}")))?;
    let header_len = u64::try_from(header_bytes.len())
        .map_err(|_| flat_error("flat header length exceeds u64"))?;
    let total = FLAT_HEADER_LEN
        .checked_add(header_bytes.len())
        .and_then(|bytes| bytes.checked_add(codes.len()))
        .ok_or_else(|| flat_error("flat artifact size overflows"))?;
    if total > config.max_artifact_bytes {
        return Err(flat_error(format!(
            "flat candidate artifact is {total} bytes, resident maximum is {}",
            config.max_artifact_bytes
        )));
    }
    let mut bytes = BytesMut::with_capacity(total);
    bytes.extend_from_slice(FLAT_MAGIC);
    bytes.put_u8(FLAT_ARTIFACT_VERSION);
    bytes.put_u64_le(header_len);
    bytes.extend_from_slice(&header_bytes);
    bytes.extend_from_slice(&codes);
    let bytes = bytes.freeze();

    let checksum = ArtifactChecksum::digest(&bytes);
    let key = late_segment_key(
        namespace,
        segment_id,
        &format!("flat-sq8-{}.bin", checksum.to_hex()),
    )?;
    let reference = LateFlatCandidateRef {
        key,
        checksum,
        size_bytes: u64::try_from(bytes.len())
            .map_err(|_| flat_error("flat artifact size exceeds u64"))?,
        row_count: u64::from(header.row_count),
        recipe,
        format_version: LATE_FLAT_FORMAT_VERSION,
    };
    Ok(BuiltLateFlatCandidate { reference, bytes })
}

impl ResidentFlatCandidateIndex {
    /// Validate and hydrate one manifest-selected flat artifact.
    pub(crate) fn from_bytes(
        bytes: &[u8],
        reference: &LateFlatCandidateRef,
        max_resident_bytes: usize,
    ) -> Result<Self> {
        validate_flat_reference(reference)?;
        if max_resident_bytes == 0 || bytes.len() > max_resident_bytes {
            return Err(flat_error(
                "flat candidate artifact exceeds the resident byte budget",
            ));
        }
        if u64::try_from(bytes.len()).ok() != Some(reference.size_bytes) {
            return Err(flat_error("flat candidate artifact size mismatch"));
        }
        if ArtifactChecksum::digest(bytes) != reference.checksum {
            return Err(flat_error("flat candidate artifact checksum mismatch"));
        }
        if bytes.len() < FLAT_HEADER_LEN || &bytes[..4] != FLAT_MAGIC {
            return Err(flat_error("invalid flat artifact magic or header"));
        }
        if bytes[4] != FLAT_ARTIFACT_VERSION {
            return Err(flat_error("unsupported flat artifact version"));
        }
        let mut length_bytes = [0_u8; size_of::<u64>()];
        length_bytes.copy_from_slice(&bytes[5..FLAT_HEADER_LEN]);
        let header_len = usize::try_from(u64::from_le_bytes(length_bytes))
            .map_err(|_| flat_error("flat header length exceeds usize"))?;
        let header_end = FLAT_HEADER_LEN
            .checked_add(header_len)
            .ok_or_else(|| flat_error("flat header length overflows"))?;
        if bytes.len() < header_end {
            return Err(flat_error("flat artifact is truncated before header end"));
        }
        let header: FlatHeaderWire = rmp_serde::from_slice(&bytes[FLAT_HEADER_LEN..header_end])
            .map_err(|error| flat_error(format!("failed to decode flat header: {error}")))?;
        if header.fde_generation != reference.recipe.fde_generation
            || header.fde_dimension != reference.recipe.fde_dimension
            || header.candidate_k != reference.recipe.candidate_k
            || u64::from(header.row_count) != reference.row_count
        {
            return Err(flat_error(
                "flat header disagrees with its manifest reference",
            ));
        }
        let dimension = usize::try_from(header.fde_dimension)
            .map_err(|_| flat_error("flat FDE dimension exceeds usize"))?;
        if dimension == 0 || header.candidate_k == 0 || header.row_count == 0 {
            return Err(flat_error("flat header fields must be positive"));
        }
        let calibration = SqCalibration::from_bytes(&header.calibration)?;
        if calibration.dim != dimension {
            return Err(flat_error(
                "flat calibration dimension disagrees with the recipe",
            ));
        }
        let row_count = header.rows.len();
        if row_count != usize::try_from(header.row_count).unwrap_or(usize::MAX) {
            return Err(flat_error("flat row directory disagrees with row count"));
        }
        let expected_codes = row_count
            .checked_mul(dimension)
            .ok_or_else(|| flat_error("flat code length overflows"))?;
        if bytes.len() - header_end != expected_codes {
            return Err(flat_error("flat code payload length mismatch"));
        }
        let mut rows = Vec::with_capacity(row_count);
        let mut previous: Option<&VectorId> = None;
        for wire in &header.rows {
            if wire.id.is_empty() {
                return Err(flat_error("flat row id cannot be empty"));
            }
            if previous.is_some_and(|previous| previous >= &wire.id) {
                return Err(flat_error("flat rows are not in canonical id order"));
            }
            previous = Some(&wire.id);
            validate_matrix_locator(&wire.matrix_locator)?;
            validate_attribute_locator(&wire.attr_locator)?;
            validate_filter_attributes(wire.filter_attributes.as_ref())?;
        }
        for wire in header.rows {
            rows.push(FlatCandidateRow {
                id: wire.id,
                content_hash: wire.content_hash,
                source_sequence: wire.source_sequence,
                parent_id: wire.parent_id,
                unit_ordinal: wire.unit_ordinal,
                matrix_locator: wire.matrix_locator,
                attr_locator: wire.attr_locator,
                filter_attributes: wire
                    .filter_attributes
                    .map(|attributes| attributes.into_iter().collect()),
            });
        }
        Ok(Self {
            recipe: reference.recipe.clone(),
            calibration,
            rows,
            codes: bytes[header_end..].to_vec(),
        })
    }

    /// Borrow every decoded baseline row in canonical id order.
    pub(crate) fn rows(&self) -> &[FlatCandidateRow] {
        &self.rows
    }

    /// Borrow the persisted calibration for frozen incremental reuse.
    pub(crate) fn calibration(&self) -> &SqCalibration {
        &self.calibration
    }

    /// Borrow one row's SQ8 code by its position in [`Self::rows`].
    pub(crate) fn row_code(&self, index: usize) -> Result<&[u8]> {
        let dimension = usize::try_from(self.recipe.fde_dimension)
            .map_err(|_| flat_error("flat FDE dimension exceeds usize"))?;
        let start = index
            .checked_mul(dimension)
            .ok_or_else(|| flat_error("flat code offset overflows"))?;
        let end = start
            .checked_add(dimension)
            .ok_or_else(|| flat_error("flat code end overflows"))?;
        self.codes
            .get(start..end)
            .ok_or_else(|| flat_error("flat code index is out of bounds"))
    }

    /// Exhaustively score, filter, and truncate the resident candidate frontier.
    ///
    /// Filters and overlay exclusions are applied BEFORE truncation to the
    /// recipe's `candidate_k`, so a filtered query still receives a full
    /// frontier of matching rows. Ordering is ascending negated reconstructed
    /// dot product with row-index tie-break, replicating the pinned Phase 9
    /// selection exactly. The scan itself runs chunked on scoped workers via
    /// [`select_flat_top_k`], which is bit-identical to the sequential scan.
    pub(crate) fn select_candidates(
        &self,
        query_fde: &[f32],
        excluded_ids: &BTreeSet<VectorId>,
        mandatory_filter: Option<&Filter>,
        request_filter: Option<&Filter>,
    ) -> Result<Vec<LateCandidate>> {
        let dimension = usize::try_from(self.recipe.fde_dimension)
            .map_err(|_| flat_error("flat FDE dimension exceeds usize"))?;
        if query_fde.len() != dimension {
            return Err(ZeppelinError::DimensionMismatch {
                expected: dimension,
                actual: query_fde.len(),
            });
        }
        if query_fde.iter().any(|value| !value.is_finite()) {
            return Err(flat_error("flat query FDE contains a non-finite value"));
        }
        let candidate_k = usize::try_from(self.recipe.candidate_k)
            .map_err(|_| flat_error("flat candidate K exceeds usize"))?;
        let admit = |index: usize| {
            let row = &self.rows[index];
            !excluded_ids.contains(&row.id)
                && filter_matches(mandatory_filter, row.filter_attributes.as_ref())
                && filter_matches(request_filter, row.filter_attributes.as_ref())
        };
        let selection = select_flat_top_k(
            &self.calibration,
            &self.codes,
            dimension,
            query_fde,
            candidate_k,
            &admit,
            None,
        )?;
        let mut candidates = Vec::with_capacity(selection.len());
        for (index, negated_dot) in selection {
            let row = &self.rows[index];
            candidates.push(LateCandidate {
                id: row.id.clone(),
                approx_fde_score: -negated_dot,
                matrix_locator: row.matrix_locator.clone(),
                attr_locator: row.attr_locator.clone(),
                metadata: LateCandidateMetadata {
                    content_hash: row.content_hash,
                    source_sequence: row.source_sequence,
                    parent_id: row.parent_id.clone(),
                    unit_ordinal: row.unit_ordinal,
                    attributes: row.filter_attributes.clone(),
                },
            });
        }
        Ok(candidates)
    }
}

/// Minimum rows per scan worker before another thread is worth its spawn cost.
const FLAT_SCAN_MIN_ROWS_PER_WORKER: usize = 256;

/// Wall-clock observability for one chunked flat scan.
///
/// `scoring` is the longest single-chunk scan wall (the critical worker,
/// matching phase 07's truth-wave reporting convention), `merge` is the
/// cross-chunk frontier merge wall, and `workers` is the scoped thread count.
#[derive(Debug, Default)]
pub(crate) struct FlatScanTiming {
    /// Longest single-worker chunk-scan wall across the scoped workers.
    pub(crate) scoring: Duration,
    /// Wall time of the final cross-chunk frontier merge and truncation.
    pub(crate) merge: Duration,
    /// Scoped worker threads that scanned disjoint row chunks.
    pub(crate) workers: usize,
}

/// One retained scan entry ordered exactly like the sequential selection.
///
/// Ascending `(score bits under `total_cmp`, row index)` replicates the
/// pinned Phase 9 comparator, so the bounded per-chunk frontier and the merge
/// reproduce the sequential candidate set, order, and tie-breaks exactly.
#[derive(Clone, Copy, Debug)]
struct FlatScanEntry {
    negated_dot: f32,
    index: usize,
}

impl Ord for FlatScanEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.negated_dot
            .total_cmp(&other.negated_dot)
            .then_with(|| self.index.cmp(&other.index))
    }
}

impl PartialOrd for FlatScanEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for FlatScanEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for FlatScanEntry {}

/// Bounded per-chunk scan output in ascending selection order.
struct ScannedFlatChunk {
    entries: Vec<FlatScanEntry>,
    scoring: Duration,
}

/// Select the top `candidate_k` admitted rows of the resident SQ8 scan.
///
/// The exhaustive scan is the corpus-size-scaling term of a late-interaction
/// query (~42 ms at 5,183 rows, ~391-414 ms at 50k rows measured
/// single-threaded), so disjoint contiguous row chunks are scored on scoped
/// standard-library workers — the same mechanism the phase 07 truth wave
/// uses — each keeping a bounded top-`candidate_k` heap of admitted rows.
/// The merged result is bit-identical to the sequential scan-sort-truncate
/// selection: every score comes from the identical scalar
/// `asymmetric_dot_product` kernel, and ordering plus tie-breaks follow the
/// pinned ascending `(total_cmp score, row index)` comparator everywhere.
///
/// `admit` decides per-row candidacy (exclusions and filters) and must be
/// pure. Every row's score is checked for finiteness before admission so a
/// non-finite score fails loud exactly as the sequential scan did; when
/// several rows are non-finite the lowest-indexed one wins because chunks
/// are joined in ascending row order and each worker stops at its first
/// offending row.
pub(crate) fn select_flat_top_k(
    calibration: &SqCalibration,
    codes: &[u8],
    dimension: usize,
    query_fde: &[f32],
    candidate_k: usize,
    admit: &(dyn Fn(usize) -> bool + Sync),
    timing: Option<&mut FlatScanTiming>,
) -> Result<Vec<(usize, f32)>> {
    if dimension == 0 {
        return Err(flat_error("flat FDE dimension must be positive"));
    }
    if codes.len() % dimension != 0 {
        return Err(flat_error("flat code payload is not row-aligned"));
    }
    if candidate_k == 0 {
        return Err(flat_error("flat candidate K must be positive"));
    }
    if query_fde.len() != dimension {
        return Err(ZeppelinError::DimensionMismatch {
            expected: dimension,
            actual: query_fde.len(),
        });
    }
    let row_count = codes.len() / dimension;
    let worker_count = thread::available_parallelism()
        .map_err(|error| flat_error(format!("cannot resolve flat scan workers: {error}")))?
        .get()
        .min(row_count.div_ceil(FLAT_SCAN_MIN_ROWS_PER_WORKER))
        .max(1);
    let chunk_rows = row_count.div_ceil(worker_count);
    let chunks = if worker_count == 1 {
        vec![scan_flat_chunk(
            calibration,
            codes,
            dimension,
            query_fde,
            0,
            candidate_k,
            admit,
        )?]
    } else {
        thread::scope(|scope| {
            let mut workers = Vec::with_capacity(worker_count);
            for chunk_index in 0..worker_count {
                let start_row = chunk_index * chunk_rows;
                let end_row = (start_row + chunk_rows).min(row_count);
                let chunk_codes = &codes[start_row * dimension..end_row * dimension];
                workers.push(scope.spawn(move || {
                    scan_flat_chunk(
                        calibration,
                        chunk_codes,
                        dimension,
                        query_fde,
                        start_row,
                        candidate_k,
                        admit,
                    )
                }));
            }
            workers
                .into_iter()
                .map(|worker| {
                    worker
                        .join()
                        .map_err(|_| flat_error("flat scan worker panicked"))?
                })
                .collect::<Result<Vec<_>>>()
        })?
    };
    let merge_started = Instant::now();
    let scoring = chunks
        .iter()
        .map(|chunk| chunk.scoring)
        .max()
        .unwrap_or_default();
    let mut merged: Vec<FlatScanEntry> =
        chunks.into_iter().flat_map(|chunk| chunk.entries).collect();
    merged.sort_unstable();
    merged.truncate(candidate_k);
    let selection = merged
        .into_iter()
        .map(|entry| (entry.index, entry.negated_dot))
        .collect();
    if let Some(timing) = timing {
        timing.scoring = scoring;
        timing.merge = merge_started.elapsed();
        timing.workers = worker_count;
    }
    Ok(selection)
}

/// Scan one contiguous row chunk with the sequential scalar kernel.
///
/// Keeps a bounded max-heap of the `candidate_k` best admitted entries; the
/// heap top is the worst retained entry under the pinned comparator, so a
/// tie with the top never displaces it, matching sequential tie retention.
fn scan_flat_chunk(
    calibration: &SqCalibration,
    chunk_codes: &[u8],
    dimension: usize,
    query_fde: &[f32],
    start_row: usize,
    candidate_k: usize,
    admit: &(dyn Fn(usize) -> bool + Sync),
) -> Result<ScannedFlatChunk> {
    let started = Instant::now();
    let rows = chunk_codes.len() / dimension;
    let mut heap: BinaryHeap<FlatScanEntry> = BinaryHeap::with_capacity(candidate_k + 1);
    for offset in 0..rows {
        let code = &chunk_codes[offset * dimension..(offset + 1) * dimension];
        let negated_dot = calibration.asymmetric_dot_product(query_fde, code);
        if !negated_dot.is_finite() {
            return Err(flat_error("flat SQ8 score is not finite"));
        }
        let index = start_row + offset;
        if !admit(index) {
            continue;
        }
        let entry = FlatScanEntry { negated_dot, index };
        if heap.len() < candidate_k {
            heap.push(entry);
        } else if let Some(mut worst) = heap.peek_mut() {
            if entry < *worst {
                *worst = entry;
            }
        }
    }
    Ok(ScannedFlatChunk {
        entries: heap.into_sorted_vec(),
        scoring: started.elapsed(),
    })
}

fn flat_row_wire(row: LateCandidateInputRow) -> FlatRowWire {
    FlatRowWire {
        id: row.id,
        content_hash: row.content_hash,
        source_sequence: row.source_sequence,
        parent_id: row.parent_id,
        unit_ordinal: row.unit_ordinal,
        matrix_locator: row.matrix_locator,
        attr_locator: row.attr_locator,
        filter_attributes: row
            .filter_attributes
            .map(|attributes| attributes.into_iter().collect()),
    }
}

/// Validate one manifest-visible flat candidate reference shape.
pub(crate) fn validate_flat_reference(reference: &LateFlatCandidateRef) -> Result<()> {
    if reference.key.is_empty()
        || reference.size_bytes == 0
        || reference.row_count == 0
        || reference.format_version != LATE_FLAT_FORMAT_VERSION
        || reference.recipe.fde_dimension == 0
        || reference.recipe.candidate_k == 0
    {
        return Err(flat_error("invalid flat candidate reference"));
    }
    Ok(())
}

fn flat_error(reason: impl Into<String>) -> ZeppelinError {
    ZeppelinError::Serialization(format!(
        "invalid late flat candidate artifact: {}",
        reason.into()
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use crate::embedding::{ArtifactChecksum, ContentHash, FdeGenerationId};
    use crate::index::late_interaction::candidate::{AttributeLocator, LateCandidateInputRow};
    use crate::index::late_interaction::matrix_artifact::MatrixBlockLocator;
    use crate::types::{AttributeValue, Filter};

    use super::{
        build_flat_candidate_artifact, LateFlatCandidateBuildConfig, ResidentFlatCandidateIndex,
    };

    fn config(candidate_k: usize) -> LateFlatCandidateBuildConfig {
        LateFlatCandidateBuildConfig {
            fde_dimension: 2,
            candidate_k,
            max_artifact_bytes: 1024 * 1024,
        }
    }

    fn row(id: &str, fde: [f32; 2], color: &str) -> LateCandidateInputRow {
        LateCandidateInputRow {
            id: id.to_string(),
            fde: super::CandidateFdeSource::Raw(fde.to_vec()),
            content_hash: ContentHash::new(
                ArtifactChecksum::digest(id.as_bytes())
                    .as_bytes()
                    .to_owned(),
            ),
            source_sequence: u64::from(id.as_bytes()[0]),
            parent_id: Some("parent".to_string()),
            unit_ordinal: Some(0),
            matrix_locator: MatrixBlockLocator {
                object_key: format!("target/late/segments/segment/{id}-matrix.bin"),
                byte_offset: 11,
                byte_length: 16,
                vector_count: 2,
                payload_checksum: ArtifactChecksum::digest(id.as_bytes()),
            },
            attr_locator: AttributeLocator {
                object_key: format!("target/late/segments/segment/{id}-attrs.bin"),
                byte_offset: 7,
                byte_length: 8,
                payload_checksum: ArtifactChecksum::digest(color.as_bytes()),
            },
            filter_attributes: Some(HashMap::from([(
                "color".to_string(),
                AttributeValue::String(color.to_string()),
            )])),
        }
    }

    #[test]
    fn flat_artifact_is_deterministic_and_round_trips() {
        let rows = vec![
            row("d", [10.0, 10.0], "blue"),
            row("b", [0.1, 0.2], "red"),
            row("a", [0.0, 0.0], "blue"),
            row("c", [9.9, 10.1], "red"),
        ];
        let generation = FdeGenerationId::new([9; 32]);
        let first =
            build_flat_candidate_artifact("target", "segment", generation, config(3), rows.clone())
                .expect("first flat build");
        let second =
            build_flat_candidate_artifact("target", "segment", generation, config(3), rows)
                .expect("second flat build");
        assert_eq!(first.bytes, second.bytes);
        assert_eq!(first.reference, second.reference);
        assert_eq!(first.reference.row_count, 4);
        assert_eq!(first.reference.recipe.candidate_k, 3);
        assert!(first
            .reference
            .key
            .starts_with("target/late/segments/segment/flat-sq8-"));

        let resident =
            ResidentFlatCandidateIndex::from_bytes(&first.bytes, &first.reference, 1024 * 1024)
                .expect("resident flat index");
        assert_eq!(
            resident
                .rows()
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "c", "d"]
        );
        assert_eq!(resident.rows()[0].parent_id.as_deref(), Some("parent"));

        let frontier = resident
            .select_candidates(&[1.0, 1.0], &BTreeSet::new(), None, None)
            .expect("unfiltered frontier");
        assert_eq!(
            frontier
                .iter()
                .map(|candidate| candidate.id.as_str())
                .collect::<Vec<_>>(),
            vec!["c", "d", "b"]
        );
        assert!(frontier[0].approx_fde_score >= frontier[1].approx_fde_score);
    }

    #[test]
    fn filters_and_exclusions_run_before_candidate_truncation() {
        let built = build_flat_candidate_artifact(
            "target",
            "segment",
            FdeGenerationId::new([5; 32]),
            config(1),
            vec![
                row("blocked-high", [10.0, 0.0], "red"),
                row("allowed-low", [1.0, 0.0], "blue"),
            ],
        )
        .expect("flat build");
        let resident =
            ResidentFlatCandidateIndex::from_bytes(&built.bytes, &built.reference, 1024 * 1024)
                .expect("resident flat index");
        let blue = Filter::Eq {
            field: "color".to_string(),
            value: AttributeValue::String("blue".to_string()),
        };
        let not_red = Filter::NotEq {
            field: "color".to_string(),
            value: AttributeValue::String("red".to_string()),
        };

        let filtered = resident
            .select_candidates(&[1.0, 0.0], &BTreeSet::new(), Some(&not_red), Some(&blue))
            .expect("filtered frontier");
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id, "allowed-low");
        assert_eq!(filtered[0].approx_fde_score, 1.0);

        let excluded = BTreeSet::from(["blocked-high".to_string()]);
        let excluded_frontier = resident
            .select_candidates(&[1.0, 0.0], &excluded, None, None)
            .expect("excluded frontier");
        assert_eq!(excluded_frontier.len(), 1);
        assert_eq!(excluded_frontier[0].id, "allowed-low");
    }

    #[test]
    fn corrupt_flat_artifact_fails_checksum_without_skipping() {
        let built = build_flat_candidate_artifact(
            "target",
            "segment",
            FdeGenerationId::new([3; 32]),
            config(1),
            vec![row("one", [1.0, 0.0], "blue")],
        )
        .expect("flat build");
        let mut corrupt = built.bytes.to_vec();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;

        let error = ResidentFlatCandidateIndex::from_bytes(&corrupt, &built.reference, 1024 * 1024)
            .expect_err("corrupt flat artifact must fail");
        assert!(error.to_string().contains("checksum mismatch"));
    }

    #[test]
    fn resident_budget_and_reference_mismatch_fail_loud() {
        let built = build_flat_candidate_artifact(
            "target",
            "segment",
            FdeGenerationId::new([2; 32]),
            config(2),
            vec![row("a", [1.0, 0.0], "blue"), row("b", [0.0, 1.0], "red")],
        )
        .expect("flat build");

        let error = ResidentFlatCandidateIndex::from_bytes(&built.bytes, &built.reference, 16)
            .expect_err("resident budget must fail");
        assert!(error.to_string().contains("resident byte budget"));

        let mut mismatched = built.reference.clone();
        mismatched.recipe.candidate_k = 7;
        let error = ResidentFlatCandidateIndex::from_bytes(&built.bytes, &mismatched, 1024 * 1024)
            .expect_err("recipe mismatch must fail");
        assert!(error.to_string().contains("disagrees"));

        let error = build_flat_candidate_artifact(
            "target",
            "segment",
            FdeGenerationId::new([2; 32]),
            LateFlatCandidateBuildConfig {
                fde_dimension: 2,
                candidate_k: 2,
                max_artifact_bytes: 64,
            },
            vec![row("a", [1.0, 0.0], "blue")],
        )
        .expect_err("artifact over the resident maximum must fail activation");
        assert!(error.to_string().contains("resident maximum"));
    }

    /// Deterministic sequential reference replicating the pinned pre-phase-09
    /// selection: score every row, sort ascending by `(total_cmp, index)`,
    /// walk in order admitting rows, truncate to `candidate_k`.
    fn sequential_reference(
        calibration: &crate::index::quantization::sq::SqCalibration,
        codes: &[u8],
        dimension: usize,
        query: &[f32],
        candidate_k: usize,
        admit: &dyn Fn(usize) -> bool,
    ) -> Vec<(usize, f32)> {
        let row_count = codes.len() / dimension;
        let mut scores: Vec<(usize, f32)> = (0..row_count)
            .map(|index| {
                let code = &codes[index * dimension..(index + 1) * dimension];
                (index, calibration.asymmetric_dot_product(query, code))
            })
            .collect();
        scores.sort_unstable_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        scores
            .into_iter()
            .filter(|&(index, _)| admit(index))
            .take(candidate_k)
            .collect()
    }

    #[test]
    fn parallel_scan_matches_sequential_selection_exactly() {
        // 4,096 rows drawn from 64 distinct patterns: every score group has
        // 64 exact ties, so the whole selection is decided by tie-breaking.
        // The row count engages 16 workers past the 256-row chunk floor.
        let dimension = 32usize;
        let row_count = 4_096usize;
        let pattern_count = 64usize;
        let mut state = 0x1234_5678_9abc_def0_u64;
        let mut next = move || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((state >> 33) as f32) / (u32::MAX as f32) - 0.5
        };
        let patterns: Vec<Vec<f32>> = (0..pattern_count)
            .map(|_| (0..dimension).map(|_| next()).collect())
            .collect();
        let fdes: Vec<&[f32]> = (0..row_count)
            .map(|row| patterns[row % pattern_count].as_slice())
            .collect();
        let calibration =
            crate::index::quantization::sq::SqCalibration::calibrate(&fdes, dimension);
        let codes: Vec<u8> = fdes
            .iter()
            .flat_map(|fde| calibration.encode(fde))
            .collect();
        let query: Vec<f32> = (0..dimension)
            .map(|coordinate| (coordinate as f32 * 0.37).sin())
            .collect();
        let candidate_k = 100usize;

        let admit_all = |_: usize| true;
        let admit_some = |index: usize| index % 7 != 0;
        for admit in [&admit_all as &(dyn Fn(usize) -> bool + Sync), &admit_some] {
            let mut timing = super::FlatScanTiming::default();
            let parallel = super::select_flat_top_k(
                &calibration,
                &codes,
                dimension,
                &query,
                candidate_k,
                admit,
                Some(&mut timing),
            )
            .expect("parallel selection must succeed");
            let sequential =
                sequential_reference(&calibration, &codes, dimension, &query, candidate_k, admit);
            assert_eq!(parallel.len(), sequential.len());
            for (left, right) in parallel.iter().zip(&sequential) {
                assert_eq!(left.0, right.0, "candidate row index diverged");
                assert_eq!(
                    left.1.to_bits(),
                    right.1.to_bits(),
                    "candidate score bits diverged at row {}",
                    left.0
                );
            }
            assert!(timing.workers > 1, "fixture must engage several workers");
            // The 64-way duplicated patterns force exact ties inside the
            // selection; every equal-score run must be in ascending row order.
            let mut tie_seen = false;
            for pair in parallel.windows(2) {
                if pair[0].1.to_bits() == pair[1].1.to_bits() {
                    tie_seen = true;
                    assert!(pair[0].0 < pair[1].0, "tie must break by row index");
                }
            }
            assert!(tie_seen, "constructed duplicates must tie inside the top K");
        }

        // K larger than the admitted row count returns every admitted row.
        let wide = super::select_flat_top_k(
            &calibration,
            &codes,
            dimension,
            &query,
            row_count * 2,
            &admit_some,
            None,
        )
        .expect("wide selection must succeed");
        let wide_reference = sequential_reference(
            &calibration,
            &codes,
            dimension,
            &query,
            row_count * 2,
            &admit_some,
        );
        assert_eq!(wide.len(), wide_reference.len());
        for (left, right) in wide.iter().zip(&wide_reference) {
            assert_eq!(left.0, right.0);
            assert_eq!(left.1.to_bits(), right.1.to_bits());
        }
    }

    #[test]
    fn flat_scan_rejects_invalid_shapes() {
        let calibration = crate::index::quantization::sq::SqCalibration::calibrate(
            &[[0.0f32, 1.0].as_slice(), [1.0f32, 0.0].as_slice()],
            2,
        );
        let admit = |_: usize| true;
        let error =
            super::select_flat_top_k(&calibration, &[0u8; 3], 2, &[0.5, 0.5], 1, &admit, None)
                .expect_err("misaligned code payload must fail loud");
        assert!(error.to_string().contains("row-aligned"));
        let error =
            super::select_flat_top_k(&calibration, &[0u8; 4], 2, &[0.5, 0.5], 0, &admit, None)
                .expect_err("zero candidate K must fail loud");
        assert!(error.to_string().contains("must be positive"));
        let error = super::select_flat_top_k(&calibration, &[0u8; 4], 2, &[0.5], 1, &admit, None)
            .expect_err("query dimension mismatch must fail loud");
        assert!(error.to_string().contains("imension"));
    }
}
