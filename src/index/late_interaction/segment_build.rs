//! Deterministic full-rebuild composition for immutable late segments.
//!
//! The lower-level codecs own their individual object formats. This module
//! aligns their row order and locators, assembles one manifest-visible segment
//! descriptor, and returns every immutable object byte needed before the root
//! manifest can publish that descriptor.

use std::collections::{BTreeSet, HashMap};

use bytes::Bytes;

use crate::embedding::{
    ArtifactChecksum, ContentHash, EmbeddingProfileId, FdeGenerationId, MatrixDtype,
    MultiVectorEmbedding, MultiVectorEpochId,
};
use crate::error::{Result, ZeppelinError};
use crate::namespace::branching::ArtifactOriginIndex;
use crate::storage::{NamespaceObjectFamily, NamespaceObjectKey};
use crate::types::{AttributeValue, VectorId};
use crate::wal::{LateCandidateKind, LateInteractionSegmentRef, LateSegmentObjectRef};

use super::attribute_artifact::{build_attribute_blocks, AttributeBlockInputRow};
use super::candidate::CandidateFdeSource;
use super::candidate::{
    build_late_candidate_index, LateCandidateBuildConfig, LateCandidateInputRow,
};
use super::flat_candidate::{
    build_flat_candidate_artifact_with_calibration, FlatCalibrationSource,
    LateFlatCandidateBuildConfig,
};
use super::matrix_artifact::{build_matrix_blocks, MatrixBlockInputRow, MatrixBlockRef};
use super::MatrixBlockLocator;

/// Candidate-kind build selection for one full segment rebuild.
#[derive(Clone, Copy, Debug)]
pub(crate) enum LateCandidateBuild {
    /// Routed IVF clusters, retained for the future scale phase. No
    /// production caller constructs this variant in the flat-SQ8 regime;
    /// tests keep the build arm exercised.
    #[allow(dead_code)]
    Ivf(LateCandidateBuildConfig),
    /// Resident exhaustive flat-SQ8 artifact (production for this regime).
    FlatSq8(LateFlatCandidateBuildConfig),
}

impl LateCandidateBuild {
    const fn fde_dimension(&self) -> usize {
        match self {
            Self::Ivf(config) => config.fde_dimension,
            Self::FlatSq8(config) => config.fde_dimension,
        }
    }
}

/// Matrix representation of one row entering a segment build.
///
/// `Fresh` rows are written into new matrix blocks (payload bytes carried
/// verbatim from their source artifact — never re-encoded). `Carried` rows
/// stay in a fully-untouched block of the previous segment referenced by
/// `LateSegmentBuildConfig::carried_matrix_blocks`; their bytes are never
/// read or rewritten.
#[derive(Clone, Debug)]
pub(crate) enum LateRowMatrixSource {
    /// Row written into a new matrix block by this build.
    Fresh {
        /// Exact multi-vector document matrix.
        exact_matrix: MultiVectorEmbedding,
        /// Stored matrix payload bytes at the epoch dtype, carried verbatim.
        exact_payload: Bytes,
    },
    /// Row remaining in a carried immutable block of the previous segment.
    Carried {
        /// Direct ranged-read locator into the carried block.
        locator: MatrixBlockLocator,
    },
}

impl LateRowMatrixSource {
    pub(crate) fn vector_count(&self) -> Result<usize> {
        match self {
            Self::Fresh { exact_matrix, .. } => Ok(exact_matrix.vector_count()),
            Self::Carried { locator } => usize::try_from(locator.vector_count)
                .map_err(|_| segment_error("carried matrix vector count exceeds usize")),
        }
    }
}

/// One normalized live retrieval unit entering a full segment rebuild.
#[derive(Clone, Debug)]
pub(crate) struct LateSegmentBuildRow {
    /// Retrieval-unit identity.
    pub(crate) id: VectorId,
    /// Source content identity represented by this version.
    pub(crate) content_hash: ContentHash,
    /// Authoritative source mutation sequence.
    pub(crate) source_sequence: u64,
    /// Optional caller-provided parent identity.
    pub(crate) parent_id: Option<String>,
    /// Optional ordinal within the parent.
    pub(crate) unit_ordinal: Option<u32>,
    /// Exact attributes used by filtering and result construction.
    pub(crate) attributes: Option<HashMap<String, AttributeValue>>,
    /// Exact matrix source (fresh bytes or a carried-block locator).
    pub(crate) matrix: LateRowMatrixSource,
    /// Document FDE representation for candidate selection.
    pub(crate) fde: CandidateFdeSource,
}

/// One already-built full-text artifact supplied by the FTS builder.
#[derive(Clone, Debug)]
pub(crate) struct PrebuiltLateFtsArtifact {
    /// Manifest descriptor produced by the FTS codec.
    pub(crate) reference: LateSegmentObjectRef,
    /// Complete immutable object bytes.
    pub(crate) bytes: Bytes,
}

/// Explicit common identities and object bounds for one rebuild.
#[derive(Clone, Debug)]
pub(crate) struct LateSegmentBuildConfig {
    /// Namespace that physically owns every newly built object.
    pub(crate) namespace: String,
    /// Stable immutable segment identity and key component.
    pub(crate) segment_id: String,
    /// Semantic profile represented by every row.
    pub(crate) profile: EmbeddingProfileId,
    /// Exact-scoring semantic epoch represented by every matrix.
    pub(crate) semantic_epoch: MultiVectorEpochId,
    /// Candidate FDE generation represented by every row.
    pub(crate) fde_generation: FdeGenerationId,
    /// Epoch-uniform exact-matrix representation.
    pub(crate) matrix_dtype: MatrixDtype,
    /// Coordinates in every exact matrix row.
    pub(crate) vector_dimension: usize,
    /// Coordinates in every raw document FDE.
    pub(crate) fde_dimension: usize,
    /// Highest contiguous source mutation included by this rebuild.
    pub(crate) coverage_sequence: u64,
    /// Hard maximum matrix block bytes.
    pub(crate) max_matrix_object_bytes: usize,
    /// Hard maximum exact-attribute block bytes.
    pub(crate) max_attribute_object_bytes: usize,
    /// Candidate build and query operating point.
    pub(crate) candidate: LateCandidateBuild,
    /// Physical owner index copied into the section descriptor.
    pub(crate) artifact_origin: Option<ArtifactOriginIndex>,
    /// Already-built full-text artifacts for the same row set.
    pub(crate) fts_artifacts: Vec<PrebuiltLateFtsArtifact>,
    /// Fully-untouched matrix blocks carried from the previous segment.
    /// Empty for full rebuilds. Carried rows reference exactly these keys.
    pub(crate) carried_matrix_blocks: Vec<MatrixBlockRef>,
    /// Calibration authority for the flat-SQ8 candidate artifact.
    pub(crate) flat_calibration: FlatCalibrationSource,
}

/// One immutable key and complete bytes ready for create-only upload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BuiltLateSegmentArtifact {
    /// Exact immutable object key.
    pub(crate) key: String,
    /// SHA-256 over the complete object.
    pub(crate) checksum: ArtifactChecksum,
    /// Complete immutable object bytes.
    pub(crate) bytes: Bytes,
}

/// Complete result of one deterministic full segment rebuild.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BuiltLateInteractionSegment {
    /// Manifest-visible segment descriptor.
    pub(crate) reference: LateInteractionSegmentRef,
    /// Every immutable artifact in canonical key order.
    pub(crate) artifacts: Vec<BuiltLateSegmentArtifact>,
}

/// Compose exact matrices, exact attributes, FDE candidates, and supplied FTS objects.
pub(crate) fn build_late_interaction_segment(
    mut config: LateSegmentBuildConfig,
    mut rows: Vec<LateSegmentBuildRow>,
) -> Result<BuiltLateInteractionSegment> {
    validate_config(&config)?;
    validate_rows(&rows, &config)?;
    rows.sort_by(|left, right| left.id.cmp(&right.id));
    config
        .fts_artifacts
        .sort_by(|left, right| left.reference.key.cmp(&right.reference.key));
    validate_fts_artifacts(&config)?;

    validate_carried_blocks(&config, &rows)?;
    let carried_key_list: Vec<String> = config
        .carried_matrix_blocks
        .iter()
        .map(|block| block.key.clone())
        .collect();
    let fresh_inputs: Vec<MatrixBlockInputRow> = rows
        .iter()
        .filter_map(|row| match &row.matrix {
            LateRowMatrixSource::Fresh {
                exact_matrix,
                exact_payload,
            } => Some(MatrixBlockInputRow {
                id: row.id.clone(),
                ordinal: block_ordinal(row.unit_ordinal),
                content_hash: row.content_hash,
                embedding: exact_matrix.clone(),
                payload: exact_payload.clone(),
            }),
            LateRowMatrixSource::Carried { .. } => None,
        })
        .collect();
    let matrix_blocks = if fresh_inputs.is_empty() {
        Vec::new()
    } else {
        build_matrix_blocks(
            &config.namespace,
            &config.segment_id,
            config.matrix_dtype,
            config.semantic_epoch,
            config.fde_generation,
            config.vector_dimension,
            config.max_matrix_object_bytes,
            fresh_inputs,
        )?
    };
    let mut fresh_locators = matrix_blocks
        .iter()
        .flat_map(|block| block.locators.iter().cloned());
    let matrix_locators = rows
        .iter()
        .map(|row| match &row.matrix {
            LateRowMatrixSource::Fresh { .. } => fresh_locators
                .next()
                .ok_or_else(|| segment_error("fresh matrix locators ran out of rows")),
            LateRowMatrixSource::Carried { locator } => Ok(locator.clone()),
        })
        .collect::<Result<Vec<_>>>()?;
    if fresh_locators.next().is_some() {
        return Err(segment_error(
            "matrix block locators disagree with normalized row count",
        ));
    }
    drop(fresh_locators);

    let attribute_blocks = build_attribute_blocks(
        &config.namespace,
        &config.segment_id,
        config.max_attribute_object_bytes,
        rows.iter()
            .map(|row| AttributeBlockInputRow {
                id: row.id.clone(),
                ordinal: block_ordinal(row.unit_ordinal),
                attributes: row.attributes.clone(),
            })
            .collect(),
    )?;
    let attribute_locators = attribute_blocks
        .iter()
        .flat_map(|block| block.locators.iter().cloned())
        .collect::<Vec<_>>();
    if attribute_locators.len() != rows.len() {
        return Err(segment_error(
            "attribute block locators disagree with normalized row count",
        ));
    }

    let candidate_rows = rows
        .iter()
        .zip(matrix_locators)
        .zip(attribute_locators)
        .map(
            |((row, matrix_locator), attr_locator)| LateCandidateInputRow {
                id: row.id.clone(),
                fde: row.fde.clone(),
                content_hash: row.content_hash,
                source_sequence: row.source_sequence,
                parent_id: row.parent_id.clone(),
                unit_ordinal: row.unit_ordinal,
                matrix_locator,
                attr_locator,
                filter_attributes: row.attributes.clone(),
            },
        )
        .collect();
    let (candidate_kind, candidate_index, flat_candidate, mut candidate_artifacts) =
        match config.candidate {
            LateCandidateBuild::Ivf(candidate_config) => {
                let candidate = build_late_candidate_index(
                    &config.namespace,
                    &config.segment_id,
                    config.fde_generation,
                    candidate_config,
                    candidate_rows,
                )?;
                let mut artifacts = vec![BuiltLateSegmentArtifact {
                    key: candidate.index_ref.bootstrap.key.clone(),
                    checksum: candidate.index_ref.bootstrap.checksum,
                    bytes: candidate.bootstrap_bytes,
                }];
                for cluster in candidate.clusters {
                    artifacts.push(BuiltLateSegmentArtifact {
                        key: cluster.reference.key,
                        checksum: cluster.reference.checksum,
                        bytes: cluster.bytes,
                    });
                }
                (
                    LateCandidateKind::Ivf,
                    Some(candidate.index_ref),
                    None,
                    artifacts,
                )
            }
            LateCandidateBuild::FlatSq8(flat_config) => {
                let flat = build_flat_candidate_artifact_with_calibration(
                    &config.namespace,
                    &config.segment_id,
                    config.fde_generation,
                    flat_config,
                    config.flat_calibration.clone(),
                    candidate_rows,
                )?;
                let artifacts = vec![BuiltLateSegmentArtifact {
                    key: flat.reference.key.clone(),
                    checksum: flat.reference.checksum,
                    bytes: flat.bytes,
                }];
                (
                    LateCandidateKind::FlatSq8,
                    None,
                    Some(flat.reference),
                    artifacts,
                )
            }
        };

    let record_count = u64::try_from(rows.len())
        .map_err(|_| segment_error("late segment row count exceeds u64"))?;
    let total_vector_count = rows.iter().try_fold(0_u64, |total, row| {
        total
            .checked_add(
                u64::try_from(row.matrix.vector_count()?)
                    .map_err(|_| segment_error("matrix vector count exceeds u64"))?,
            )
            .ok_or_else(|| segment_error("late segment total vector count overflows"))
    })?;
    let mut matrix_objects = config.carried_matrix_blocks.clone();
    matrix_objects.extend(matrix_blocks.iter().map(|block| block.reference.clone()));
    matrix_objects.sort_by(|left, right| left.key.cmp(&right.key));
    let attribute_objects = attribute_blocks
        .iter()
        .map(|block| block.reference.clone())
        .collect::<Vec<_>>();
    let fts_objects = config
        .fts_artifacts
        .iter()
        .map(|artifact| artifact.reference.clone())
        .collect::<Vec<_>>();
    let reference = LateInteractionSegmentRef {
        id: config.segment_id,
        profile: config.profile,
        semantic_epoch: config.semantic_epoch,
        fde_generation: config.fde_generation,
        matrix_dtype: config.matrix_dtype,
        record_count,
        total_vector_count,
        vector_dimension: u32::try_from(config.vector_dimension)
            .map_err(|_| segment_error("late segment vector dimension exceeds u32"))?,
        fde_dimension: u32::try_from(config.fde_dimension)
            .map_err(|_| segment_error("late segment FDE dimension exceeds u32"))?,
        coverage_sequence: config.coverage_sequence,
        candidate_index,
        matrix_objects,
        attribute_objects,
        fts_objects,
        artifact_origin: config.artifact_origin,
        candidate_kind,
        flat_candidate,
    };
    validate_segment_descriptor(&reference)?;

    let mut artifacts = Vec::new();
    for block in matrix_blocks {
        artifacts.push(BuiltLateSegmentArtifact {
            key: block.reference.key,
            checksum: block.reference.checksum,
            bytes: block.bytes,
        });
    }
    for block in attribute_blocks {
        artifacts.push(BuiltLateSegmentArtifact {
            key: block.reference.key,
            checksum: block.reference.checksum,
            bytes: block.bytes,
        });
    }
    artifacts.append(&mut candidate_artifacts);
    for artifact in config.fts_artifacts {
        artifacts.push(BuiltLateSegmentArtifact {
            key: artifact.reference.key,
            checksum: artifact.reference.checksum,
            bytes: artifact.bytes,
        });
    }
    artifacts.sort_by(|left, right| left.key.cmp(&right.key));
    validate_complete_artifacts(&reference, &artifacts, &carried_key_list)?;

    Ok(BuiltLateInteractionSegment {
        reference,
        artifacts,
    })
}

fn validate_config(config: &LateSegmentBuildConfig) -> Result<()> {
    if config.namespace.is_empty()
        || config.segment_id.is_empty()
        || config.segment_id.contains('/')
        || config.profile.as_str().is_empty()
        || config.coverage_sequence == 0
        || config.vector_dimension == 0
        || config.fde_dimension == 0
        || config.max_matrix_object_bytes == 0
        || config.max_attribute_object_bytes == 0
    {
        return Err(segment_error("invalid explicit late segment build config"));
    }
    let vector_dimension = u32::try_from(config.vector_dimension)
        .map_err(|_| segment_error("late segment vector dimension exceeds u32"))?;
    config
        .matrix_dtype
        .validate_for_dimension(vector_dimension)?;
    if config.candidate.fde_dimension() != config.fde_dimension {
        return Err(segment_error(
            "candidate and segment FDE dimensions disagree",
        ));
    }
    Ok(())
}

fn validate_rows(rows: &[LateSegmentBuildRow], config: &LateSegmentBuildConfig) -> Result<()> {
    if rows.is_empty() {
        return Err(segment_error(
            "late segment full rebuild requires at least one row",
        ));
    }
    let mut ids = BTreeSet::new();
    for row in rows {
        if row.id.is_empty() || !ids.insert(row.id.clone()) {
            return Err(segment_error(
                "late segment rows contain an empty or duplicate id",
            ));
        }
        if row.source_sequence > config.coverage_sequence {
            return Err(segment_error(
                "late segment row sequence exceeds declared coverage",
            ));
        }
        match &row.matrix {
            LateRowMatrixSource::Fresh { exact_matrix, .. } => {
                if exact_matrix.vector_dimension() != config.vector_dimension {
                    return Err(ZeppelinError::DimensionMismatch {
                        expected: config.vector_dimension,
                        actual: exact_matrix.vector_dimension(),
                    });
                }
            }
            LateRowMatrixSource::Carried { .. } => {
                if matches!(config.candidate, LateCandidateBuild::Ivf(_)) {
                    return Err(segment_error(
                        "carried matrix rows require the flat candidate kind",
                    ));
                }
            }
        }
        match &row.fde {
            CandidateFdeSource::Raw(fde) => {
                if fde.len() != config.fde_dimension {
                    return Err(ZeppelinError::DimensionMismatch {
                        expected: config.fde_dimension,
                        actual: fde.len(),
                    });
                }
                if fde.iter().any(|value| !value.is_finite()) {
                    return Err(segment_error(
                        "late segment FDE contains a non-finite value",
                    ));
                }
            }
            CandidateFdeSource::Sq8(code) => {
                if code.len() != config.fde_dimension {
                    return Err(ZeppelinError::DimensionMismatch {
                        expected: config.fde_dimension,
                        actual: code.len(),
                    });
                }
            }
        }
    }
    Ok(())
}

/// Validate carried blocks against the config identities and row locators.
///
/// Every carried block must share the segment's dtype/epoch/generation, every
/// carried row's locator must point into a declared carried block, and every
/// declared carried block must be referenced by at least one carried row so a
/// dead block can never stay rooted.
fn validate_carried_blocks(
    config: &LateSegmentBuildConfig,
    rows: &[LateSegmentBuildRow],
) -> Result<()> {
    let mut carried_keys = BTreeSet::new();
    for block in &config.carried_matrix_blocks {
        if block.dtype != config.matrix_dtype
            || block.semantic_epoch != config.semantic_epoch
            || block.fde_generation != config.fde_generation
            || u32::try_from(config.vector_dimension).ok() != Some(block.vector_dimension)
        {
            return Err(segment_error(
                "carried matrix block identity disagrees with the segment build",
            ));
        }
        if !carried_keys.insert(block.key.as_str()) {
            return Err(segment_error("carried matrix blocks repeat a key"));
        }
    }
    let mut referenced = BTreeSet::new();
    for row in rows {
        if let LateRowMatrixSource::Carried { locator } = &row.matrix {
            if !carried_keys.contains(locator.object_key.as_str()) {
                return Err(segment_error(
                    "carried row locator points outside the declared carried blocks",
                ));
            }
            referenced.insert(locator.object_key.as_str());
        }
    }
    if referenced.len() != carried_keys.len() {
        return Err(segment_error(
            "declared carried matrix block has no remaining live row",
        ));
    }
    Ok(())
}

fn validate_fts_artifacts(config: &LateSegmentBuildConfig) -> Result<()> {
    let expected_prefix = format!(
        "{}{}/",
        NamespaceObjectFamily::LateSegment.namespace_prefix(&config.namespace),
        config.segment_id
    );
    let mut keys = BTreeSet::new();
    for artifact in &config.fts_artifacts {
        let reference = &artifact.reference;
        let expected_key = format!("{expected_prefix}fts_{}.bin", reference.checksum.to_hex());
        let owned = NamespaceObjectKey::classify(&config.namespace, reference.key.clone())?;
        if owned.family() != NamespaceObjectFamily::LateSegment
            || reference.key != expected_key
            || reference.size_bytes == 0
            || reference.format_version == 0
            || u64::try_from(artifact.bytes.len()).ok() != Some(reference.size_bytes)
            || ArtifactChecksum::digest(&artifact.bytes) != reference.checksum
            || !keys.insert(reference.key.clone())
        {
            return Err(segment_error("invalid prebuilt late FTS artifact"));
        }
    }
    Ok(())
}

fn validate_complete_artifacts(
    reference: &LateInteractionSegmentRef,
    artifacts: &[BuiltLateSegmentArtifact],
    carried_keys: &[String],
) -> Result<()> {
    let mut rooted = BTreeSet::new();
    if let Some(candidate_index) = reference.candidate_index.as_ref() {
        rooted.insert(candidate_index.bootstrap.key.as_str());
        rooted.extend(
            candidate_index
                .clusters
                .iter()
                .map(|cluster| cluster.key.as_str()),
        );
    }
    if let Some(flat) = reference.flat_candidate.as_ref() {
        rooted.insert(flat.key.as_str());
    }
    rooted.extend(
        reference
            .matrix_objects
            .iter()
            .map(|object| object.key.as_str()),
    );
    rooted.extend(
        reference
            .attribute_objects
            .iter()
            .map(|object| object.key.as_str()),
    );
    rooted.extend(
        reference
            .fts_objects
            .iter()
            .map(|object| object.key.as_str()),
    );
    for carried in carried_keys {
        if !rooted.remove(carried.as_str()) {
            return Err(segment_error(
                "carried matrix block is not rooted by the segment descriptor",
            ));
        }
    }
    let returned = artifacts
        .iter()
        .map(|artifact| artifact.key.as_str())
        .collect::<BTreeSet<_>>();
    if rooted.len() != artifacts.len() || rooted != returned {
        return Err(segment_error(
            "late segment descriptor does not root every returned artifact exactly once",
        ));
    }
    for artifact in artifacts {
        if ArtifactChecksum::digest(&artifact.bytes) != artifact.checksum {
            return Err(segment_error(
                "late segment returned artifact checksum mismatch",
            ));
        }
    }
    Ok(())
}

fn validate_segment_descriptor(reference: &LateInteractionSegmentRef) -> Result<()> {
    if reference.id.is_empty()
        || reference.id.contains('/')
        || reference.profile.as_str().is_empty()
        || reference.record_count == 0
        || reference.total_vector_count == 0
        || reference.vector_dimension == 0
        || reference.fde_dimension == 0
    {
        return Err(segment_error(
            "late segment descriptor identities or counts disagree",
        ));
    }
    match reference.candidate_kind {
        LateCandidateKind::Ivf => {
            let candidate_index = reference.ivf_candidate_index()?;
            if reference.flat_candidate.is_some()
                || candidate_index.bootstrap.recipe.fde_generation != reference.fde_generation
                || candidate_index.bootstrap.recipe.fde_dimension != reference.fde_dimension
                || candidate_index.bootstrap.row_count != reference.record_count
            {
                return Err(segment_error(
                    "late segment descriptor identities or counts disagree",
                ));
            }
        }
        LateCandidateKind::FlatSq8 => {
            let flat = reference.flat_candidate_ref()?;
            if reference.candidate_index.is_some()
                || flat.recipe.fde_generation != reference.fde_generation
                || flat.recipe.fde_dimension != reference.fde_dimension
                || flat.row_count != reference.record_count
            {
                return Err(segment_error(
                    "late segment descriptor identities or counts disagree",
                ));
            }
        }
    }
    reference
        .matrix_dtype
        .validate_for_dimension(reference.vector_dimension)?;
    let matrix_rows = reference
        .matrix_objects
        .iter()
        .try_fold(0_u64, |total, object| {
            if object.dtype != reference.matrix_dtype
                || object.semantic_epoch != reference.semantic_epoch
                || object.fde_generation != reference.fde_generation
                || object.vector_dimension != reference.vector_dimension
            {
                return Err(segment_error(
                    "late segment matrix object identity disagrees with descriptor",
                ));
            }
            total
                .checked_add(u64::from(object.row_count))
                .ok_or_else(|| segment_error("late segment matrix row count overflows"))
        })?;
    let matrix_vectors = reference
        .matrix_objects
        .iter()
        .try_fold(0_u64, |total, object| {
            total
                .checked_add(object.total_vectors)
                .ok_or_else(|| segment_error("late segment matrix vector count overflows"))
        })?;
    let attribute_rows = reference
        .attribute_objects
        .iter()
        .try_fold(0_u64, |total, object| {
            total
                .checked_add(u64::from(object.row_count))
                .ok_or_else(|| segment_error("late segment attribute row count overflows"))
        })?;
    let candidate_rows = match reference.candidate_index.as_ref() {
        Some(candidate_index) => {
            candidate_index
                .clusters
                .iter()
                .try_fold(0_u64, |total, object| {
                    total
                        .checked_add(u64::from(object.row_count))
                        .ok_or_else(|| segment_error("late segment candidate row count overflows"))
                })?
        }
        None => reference.flat_candidate_ref()?.row_count,
    };
    if matrix_rows != reference.record_count
        || attribute_rows != reference.record_count
        || candidate_rows != reference.record_count
        || matrix_vectors != reference.total_vector_count
    {
        return Err(segment_error(
            "late segment child artifact counts disagree with descriptor",
        ));
    }
    Ok(())
}

fn block_ordinal(unit_ordinal: Option<u32>) -> u32 {
    unit_ordinal.unwrap_or(0)
}

fn segment_error(reason: impl Into<String>) -> ZeppelinError {
    ZeppelinError::Serialization(format!("invalid late segment build: {}", reason.into()))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap};

    use crate::embedding::{
        ArtifactChecksum, ContentHash, EmbeddingProfileId, FdeGenerationId, MatrixDtype,
        MultiVectorEmbedding, MultiVectorEpochId,
    };
    use crate::index::late_interaction::candidate::{
        decode_all_candidate_rows, FetchedLateCandidateCluster, LateCandidateBuildConfig,
        LateRoutingMetric,
    };
    use bytes::Bytes;

    use crate::index::late_interaction::attribute_artifact::decode_attribute_row;
    use crate::index::late_interaction::matrix_artifact::decode_matrix_row;
    use crate::types::AttributeValue;
    use crate::wal::LateSegmentObjectRef;

    use crate::index::late_interaction::flat_candidate::{
        LateFlatCandidateBuildConfig, ResidentFlatCandidateIndex,
    };
    use crate::wal::LateCandidateKind;

    use super::{
        build_late_interaction_segment, LateCandidateBuild, LateSegmentBuildConfig,
        LateSegmentBuildRow, PrebuiltLateFtsArtifact,
    };

    fn row(id: &str, sequence: u64, base: f32) -> LateSegmentBuildRow {
        LateSegmentBuildRow {
            id: id.to_string(),
            content_hash: ContentHash::new([u8::try_from(sequence).expect("small sequence"); 32]),
            source_sequence: sequence,
            parent_id: Some(format!("parent-{id}")),
            unit_ordinal: Some(u32::try_from(sequence).expect("small sequence")),
            attributes: Some(HashMap::from([(
                "name".to_string(),
                AttributeValue::String(id.to_string()),
            )])),
            matrix: super::LateRowMatrixSource::Fresh {
                exact_payload: crate::embedding::artifact::encode_matrix_payload(
                    MatrixDtype::F16,
                    3,
                    &MultiVectorEmbedding::new(vec![base, 0.0, 0.0, base, base, base], 2, 3, 4)
                        .expect("matrix"),
                )
                .expect("test payload"),
                exact_matrix: MultiVectorEmbedding::new(
                    vec![base, 0.0, 0.0, base, base, base],
                    2,
                    3,
                    4,
                )
                .expect("matrix"),
            },
            fde: super::CandidateFdeSource::Raw(vec![base, 0.0]),
        }
    }

    fn config(fts: Vec<PrebuiltLateFtsArtifact>) -> LateSegmentBuildConfig {
        LateSegmentBuildConfig {
            carried_matrix_blocks: Vec::new(),
            flat_calibration: super::FlatCalibrationSource::Recalibrate,
            namespace: "target".to_string(),
            segment_id: "segment".to_string(),
            profile: EmbeddingProfileId::new("profile"),
            semantic_epoch: MultiVectorEpochId::new([3; 32]),
            fde_generation: FdeGenerationId::new([4; 32]),
            matrix_dtype: MatrixDtype::F16,
            vector_dimension: 3,
            fde_dimension: 2,
            coverage_sequence: 10,
            max_matrix_object_bytes: 2 * 1024,
            max_attribute_object_bytes: 512,
            candidate: LateCandidateBuild::Ivf(LateCandidateBuildConfig {
                fde_dimension: 2,
                nlist: 2,
                probe_budget: 2,
                candidate_k: 3,
                routing_metric: LateRoutingMetric::NegativeL2,
                kmeans_max_iters: 20,
                kmeans_epsilon: 1e-6,
                max_cluster_bytes: 2 * 1024,
                max_bootstrap_bytes: 64 * 1024,
            }),
            artifact_origin: None,
            fts_artifacts: fts,
        }
    }

    fn fts_artifact() -> PrebuiltLateFtsArtifact {
        let bytes = Bytes::from_static(b"prebuilt-fts");
        let checksum = ArtifactChecksum::digest(&bytes);
        PrebuiltLateFtsArtifact {
            reference: LateSegmentObjectRef {
                key: format!("target/late/segments/segment/fts_{}.bin", checksum.to_hex()),
                checksum,
                size_bytes: u64::try_from(bytes.len()).expect("fts bytes"),
                format_version: 1,
            },
            bytes,
        }
    }

    #[test]
    fn full_rebuild_aligns_locators_and_roots_every_artifact() {
        let original_rows = vec![
            row("charlie", 3, 3.0),
            row("alpha", 1, 1.0),
            row("bravo", 2, 2.0),
        ];
        let built =
            build_late_interaction_segment(config(vec![fts_artifact()]), original_rows.clone())
                .expect("late segment build");
        assert_eq!(built.reference.record_count, 3);
        assert_eq!(built.reference.total_vector_count, 6);

        let artifact_by_key = built
            .artifacts
            .iter()
            .map(|artifact| (artifact.key.as_str(), artifact.bytes.as_ref()))
            .collect::<BTreeMap<_, _>>();
        let candidate_index = built
            .reference
            .ivf_candidate_index()
            .expect("IVF build must root a candidate index");
        let cluster_payloads = candidate_index
            .clusters
            .iter()
            .map(|reference| FetchedLateCandidateCluster {
                reference,
                bytes: artifact_by_key[reference.key.as_str()],
            })
            .collect::<Vec<_>>();
        let candidates = decode_all_candidate_rows(
            candidate_index,
            artifact_by_key[candidate_index.bootstrap.key.as_str()],
            &cluster_payloads,
            64 * 1024,
            2 * 1024,
        )
        .expect("candidate baseline decode");
        let original_by_id = original_rows
            .iter()
            .map(|row| (row.id.as_str(), row))
            .collect::<BTreeMap<_, _>>();
        for candidate in candidates {
            let original = original_by_id[candidate.id.as_str()];
            let matrix_start =
                usize::try_from(candidate.matrix_locator.byte_offset).expect("matrix offset");
            let matrix_end = matrix_start
                + usize::try_from(candidate.matrix_locator.byte_length).expect("matrix length");
            let matrix_object = artifact_by_key[candidate.matrix_locator.object_key.as_str()];
            let matrix = decode_matrix_row(
                &matrix_object[matrix_start..matrix_end],
                &candidate.matrix_locator,
                MatrixDtype::F16,
                3,
                4,
            )
            .expect("candidate matrix locator");
            match &original.matrix {
                super::LateRowMatrixSource::Fresh { exact_matrix, .. } => {
                    assert_eq!(matrix, *exact_matrix)
                }
                super::LateRowMatrixSource::Carried { .. } => panic!("row must be fresh"),
            }

            let attr_start =
                usize::try_from(candidate.attr_locator.byte_offset).expect("attribute offset");
            let attr_end = attr_start
                + usize::try_from(candidate.attr_locator.byte_length).expect("attribute length");
            let attr_object = artifact_by_key[candidate.attr_locator.object_key.as_str()];
            let attributes = decode_attribute_row(
                &attr_object[attr_start..attr_end],
                &candidate.attr_locator,
                1024,
            )
            .expect("candidate attribute locator");
            assert_eq!(attributes, original.attributes);
            assert_eq!(candidate.content_hash, original.content_hash);
            assert_eq!(candidate.source_sequence, original.source_sequence);
        }

        let rooted = std::iter::once(candidate_index.bootstrap.key.as_str())
            .chain(
                candidate_index
                    .clusters
                    .iter()
                    .map(|reference| reference.key.as_str()),
            )
            .chain(
                built
                    .reference
                    .matrix_objects
                    .iter()
                    .map(|reference| reference.key.as_str()),
            )
            .chain(
                built
                    .reference
                    .attribute_objects
                    .iter()
                    .map(|reference| reference.key.as_str()),
            )
            .chain(
                built
                    .reference
                    .fts_objects
                    .iter()
                    .map(|reference| reference.key.as_str()),
            )
            .collect::<BTreeSet<_>>();
        assert_eq!(
            rooted,
            built
                .artifacts
                .iter()
                .map(|artifact| artifact.key.as_str())
                .collect()
        );
    }

    fn flat_config() -> LateSegmentBuildConfig {
        let mut config = config(Vec::new());
        config.candidate = LateCandidateBuild::FlatSq8(LateFlatCandidateBuildConfig {
            fde_dimension: 2,
            candidate_k: 3,
            max_artifact_bytes: 64 * 1024,
        });
        config
    }

    #[test]
    fn flat_rebuild_roots_one_candidate_artifact_and_aligns_locators() {
        let original_rows = vec![
            row("charlie", 3, 3.0),
            row("alpha", 1, 1.0),
            row("bravo", 2, 2.0),
        ];
        let reversed = original_rows.iter().cloned().rev().collect::<Vec<_>>();
        let built = build_late_interaction_segment(flat_config(), original_rows.clone())
            .expect("flat segment build");
        let again =
            build_late_interaction_segment(flat_config(), reversed).expect("reversed flat build");
        assert_eq!(built, again);
        assert_eq!(built.reference.candidate_kind, LateCandidateKind::FlatSq8);
        assert!(built.reference.candidate_index.is_none());

        let flat = built
            .reference
            .flat_candidate_ref()
            .expect("flat build must root its candidate artifact");
        assert_eq!(flat.row_count, 3);
        assert_eq!(flat.recipe.candidate_k, 3);
        let artifact_by_key = built
            .artifacts
            .iter()
            .map(|artifact| (artifact.key.as_str(), artifact.bytes.as_ref()))
            .collect::<BTreeMap<_, _>>();
        assert!(artifact_by_key.contains_key(flat.key.as_str()));
        let resident = ResidentFlatCandidateIndex::from_bytes(
            artifact_by_key[flat.key.as_str()],
            flat,
            64 * 1024,
        )
        .expect("flat artifact must hydrate");
        let original_by_id = original_rows
            .iter()
            .map(|row| (row.id.as_str(), row))
            .collect::<BTreeMap<_, _>>();
        for decoded in resident.rows() {
            let original = original_by_id[decoded.id.as_str()];
            let matrix_start =
                usize::try_from(decoded.matrix_locator.byte_offset).expect("matrix offset");
            let matrix_end = matrix_start
                + usize::try_from(decoded.matrix_locator.byte_length).expect("matrix length");
            let matrix_object = artifact_by_key[decoded.matrix_locator.object_key.as_str()];
            let matrix = decode_matrix_row(
                &matrix_object[matrix_start..matrix_end],
                &decoded.matrix_locator,
                MatrixDtype::F16,
                3,
                4,
            )
            .expect("flat matrix locator");
            match &original.matrix {
                super::LateRowMatrixSource::Fresh { exact_matrix, .. } => {
                    assert_eq!(matrix, *exact_matrix)
                }
                super::LateRowMatrixSource::Carried { .. } => panic!("row must be fresh"),
            }

            let attr_start =
                usize::try_from(decoded.attr_locator.byte_offset).expect("attribute offset");
            let attr_end = attr_start
                + usize::try_from(decoded.attr_locator.byte_length).expect("attribute length");
            let attr_object = artifact_by_key[decoded.attr_locator.object_key.as_str()];
            let attributes = decode_attribute_row(
                &attr_object[attr_start..attr_end],
                &decoded.attr_locator,
                1024,
            )
            .expect("flat attribute locator");
            assert_eq!(attributes, original.attributes);
            assert_eq!(decoded.content_hash, original.content_hash);
            assert_eq!(decoded.source_sequence, original.source_sequence);
        }
    }

    #[test]
    fn full_rebuild_is_deterministic_after_id_normalization() {
        let rows = vec![row("b", 2, 2.0), row("a", 1, 1.0)];
        let reversed = rows.iter().cloned().rev().collect();
        let first =
            build_late_interaction_segment(config(Vec::new()), rows).expect("first segment build");
        let second = build_late_interaction_segment(config(Vec::new()), reversed)
            .expect("second segment build");

        assert_eq!(first, second);
    }

    #[test]
    fn full_rebuild_rejects_uncovered_rows_and_unregistered_fts_keys() {
        let mut uncovered = config(Vec::new());
        uncovered.coverage_sequence = 0;
        assert!(build_late_interaction_segment(
            uncovered,
            vec![row("a", 0, 1.0), row("b", 0, 2.0)]
        )
        .is_err());

        let mut unregistered = fts_artifact();
        unregistered.reference.key = "target/late/segments/segment/notes.bin".to_string();
        assert!(build_late_interaction_segment(
            config(vec![unregistered]),
            vec![row("a", 1, 1.0), row("b", 2, 2.0)]
        )
        .is_err());
    }
}
