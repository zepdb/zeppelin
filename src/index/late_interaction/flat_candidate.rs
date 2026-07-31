//! Resident exhaustive flat-SQ8 candidate artifacts (`ZFQ1`).
//!
//! For the ≲50–100k-unit corpus regime, candidate selection is an exhaustive
//! scan of SQ8-quantized document FDEs held resident after one hydration fetch.
//! One immutable object carries the production [`SqCalibration`], per-row
//! locator metadata, and the raw codes; queries never read per-candidate
//! objects. IVF wave-one routing (`candidate.rs`) is retained only for the
//! future scale phase. Adopted 2026-07-31 from the measured Phase 9 benchmark
//! (`tasks/MMLI-2/results/phase9-flat-sq8-benchmark.md`).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::mem::size_of;

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::embedding::{ArtifactChecksum, ContentHash, FdeGenerationId};
use crate::error::{Result, ZeppelinError};
use crate::index::quantization::sq::SqCalibration;
use crate::types::{AttributeValue, Filter, VectorId};

use super::candidate::{
    late_segment_key, validate_attribute_locator, validate_filter_attributes, validate_input_row,
    validate_matrix_locator, AttributeLocator, LateCandidate, LateCandidateInputRow,
    LateCandidateMetadata,
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

/// Build one deterministic flat-SQ8 candidate artifact.
pub(crate) fn build_flat_candidate_artifact(
    namespace: &str,
    segment_id: &str,
    fde_generation: FdeGenerationId,
    config: LateFlatCandidateBuildConfig,
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

    let fde_refs: Vec<&[f32]> = rows.iter().map(|row| row.fde.as_slice()).collect();
    let calibration = SqCalibration::calibrate(&fde_refs, config.fde_dimension);
    let mut codes = Vec::with_capacity(rows.len() * config.fde_dimension);
    for fde in &fde_refs {
        let code = calibration.encode(fde);
        if code.len() != config.fde_dimension {
            return Err(flat_error("flat SQ8 code width disagrees with dimension"));
        }
        codes.extend_from_slice(&code);
    }
    drop(fde_refs);

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

    /// Borrow the persisted operating point.
    pub(crate) fn recipe(&self) -> &LateFlatCandidateRecipe {
        &self.recipe
    }

    /// Borrow every decoded baseline row in canonical id order.
    pub(crate) fn rows(&self) -> &[FlatCandidateRow] {
        &self.rows
    }

    /// Exhaustively score, filter, and truncate the resident candidate frontier.
    ///
    /// Filters and overlay exclusions are applied BEFORE truncation to the
    /// recipe's `candidate_k`, so a filtered query still receives a full
    /// frontier of matching rows. Ordering is ascending negated reconstructed
    /// dot product with row-index tie-break, replicating the pinned Phase 9
    /// selection exactly.
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
        let mut scores: Vec<(usize, f32)> = Vec::with_capacity(self.rows.len());
        for index in 0..self.rows.len() {
            let code = &self.codes[index * dimension..(index + 1) * dimension];
            let negated_dot = self.calibration.asymmetric_dot_product(query_fde, code);
            if !negated_dot.is_finite() {
                return Err(flat_error("flat SQ8 score is not finite"));
            }
            scores.push((index, negated_dot));
        }
        scores.sort_unstable_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| left.0.cmp(&right.0))
        });

        let candidate_k = usize::try_from(self.recipe.candidate_k)
            .map_err(|_| flat_error("flat candidate K exceeds usize"))?;
        let mut candidates = Vec::with_capacity(candidate_k.min(self.rows.len()));
        for (index, negated_dot) in scores {
            if candidates.len() == candidate_k {
                break;
            }
            let row = &self.rows[index];
            if excluded_ids.contains(&row.id) {
                continue;
            }
            if !filter_matches(mandatory_filter, row.filter_attributes.as_ref())
                || !filter_matches(request_filter, row.filter_attributes.as_ref())
            {
                continue;
            }
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
            fde: fde.to_vec(),
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
        assert_eq!(resident.recipe(), &first.reference.recipe);
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
}
