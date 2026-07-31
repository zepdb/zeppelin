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
use crate::wal::{LateInteractionSegmentRef, LateSegmentObjectRef};

use super::attribute_artifact::{build_attribute_blocks, AttributeBlockInputRow};
use super::candidate::{
    build_late_candidate_index, LateCandidateBuildConfig, LateCandidateInputRow,
};
use super::matrix_artifact::{build_matrix_blocks, MatrixBlockInputRow};

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
    /// Exact multi-vector document matrix.
    pub(crate) exact_matrix: MultiVectorEmbedding,
    /// Raw uncompressed document FDE.
    pub(crate) raw_fde: Vec<f32>,
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
    pub(crate) candidate: LateCandidateBuildConfig,
    /// Physical owner index copied into the section descriptor.
    pub(crate) artifact_origin: Option<ArtifactOriginIndex>,
    /// Already-built full-text artifacts for the same row set.
    pub(crate) fts_artifacts: Vec<PrebuiltLateFtsArtifact>,
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

    let matrix_blocks = build_matrix_blocks(
        &config.namespace,
        &config.segment_id,
        config.matrix_dtype,
        config.semantic_epoch,
        config.fde_generation,
        config.vector_dimension,
        config.max_matrix_object_bytes,
        rows.iter()
            .map(|row| MatrixBlockInputRow {
                id: row.id.clone(),
                ordinal: block_ordinal(row.unit_ordinal),
                content_hash: row.content_hash,
                embedding: row.exact_matrix.clone(),
            })
            .collect(),
    )?;
    let matrix_locators = matrix_blocks
        .iter()
        .flat_map(|block| block.locators.iter().cloned())
        .collect::<Vec<_>>();
    if matrix_locators.len() != rows.len() {
        return Err(segment_error(
            "matrix block locators disagree with normalized row count",
        ));
    }

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
                fde: row.raw_fde.clone(),
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
    let candidate = build_late_candidate_index(
        &config.namespace,
        &config.segment_id,
        config.fde_generation,
        config.candidate,
        candidate_rows,
    )?;

    let record_count = u64::try_from(rows.len())
        .map_err(|_| segment_error("late segment row count exceeds u64"))?;
    let total_vector_count = rows.iter().try_fold(0_u64, |total, row| {
        total
            .checked_add(
                u64::try_from(row.exact_matrix.vector_count())
                    .map_err(|_| segment_error("matrix vector count exceeds u64"))?,
            )
            .ok_or_else(|| segment_error("late segment total vector count overflows"))
    })?;
    let matrix_objects = matrix_blocks
        .iter()
        .map(|block| block.reference.clone())
        .collect::<Vec<_>>();
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
        candidate_index: candidate.index_ref.clone(),
        matrix_objects,
        attribute_objects,
        fts_objects,
        artifact_origin: config.artifact_origin,
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
    artifacts.push(BuiltLateSegmentArtifact {
        key: candidate.index_ref.bootstrap.key.clone(),
        checksum: candidate.index_ref.bootstrap.checksum,
        bytes: candidate.bootstrap_bytes,
    });
    for cluster in candidate.clusters {
        artifacts.push(BuiltLateSegmentArtifact {
            key: cluster.reference.key,
            checksum: cluster.reference.checksum,
            bytes: cluster.bytes,
        });
    }
    for artifact in config.fts_artifacts {
        artifacts.push(BuiltLateSegmentArtifact {
            key: artifact.reference.key,
            checksum: artifact.reference.checksum,
            bytes: artifact.bytes,
        });
    }
    artifacts.sort_by(|left, right| left.key.cmp(&right.key));
    validate_complete_artifacts(&reference, &artifacts)?;

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
    if config.candidate.fde_dimension != config.fde_dimension {
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
        if row.exact_matrix.vector_dimension() != config.vector_dimension {
            return Err(ZeppelinError::DimensionMismatch {
                expected: config.vector_dimension,
                actual: row.exact_matrix.vector_dimension(),
            });
        }
        if row.raw_fde.len() != config.fde_dimension {
            return Err(ZeppelinError::DimensionMismatch {
                expected: config.fde_dimension,
                actual: row.raw_fde.len(),
            });
        }
        if row.raw_fde.iter().any(|value| !value.is_finite()) {
            return Err(segment_error(
                "late segment FDE contains a non-finite value",
            ));
        }
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
) -> Result<()> {
    let mut rooted = BTreeSet::new();
    rooted.insert(reference.candidate_index.bootstrap.key.as_str());
    rooted.extend(
        reference
            .candidate_index
            .clusters
            .iter()
            .map(|cluster| cluster.key.as_str()),
    );
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
        || reference.candidate_index.bootstrap.recipe.fde_generation != reference.fde_generation
        || reference.candidate_index.bootstrap.recipe.fde_dimension != reference.fde_dimension
        || reference.candidate_index.bootstrap.row_count != reference.record_count
    {
        return Err(segment_error(
            "late segment descriptor identities or counts disagree",
        ));
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
    let candidate_rows =
        reference
            .candidate_index
            .clusters
            .iter()
            .try_fold(0_u64, |total, object| {
                total
                    .checked_add(u64::from(object.row_count))
                    .ok_or_else(|| segment_error("late segment candidate row count overflows"))
            })?;
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

    use super::{
        build_late_interaction_segment, LateSegmentBuildConfig, LateSegmentBuildRow,
        PrebuiltLateFtsArtifact,
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
            exact_matrix: MultiVectorEmbedding::new(
                vec![base, 0.0, 0.0, base, base, base],
                2,
                3,
                4,
            )
            .expect("matrix"),
            raw_fde: vec![base, 0.0],
        }
    }

    fn config(fts: Vec<PrebuiltLateFtsArtifact>) -> LateSegmentBuildConfig {
        LateSegmentBuildConfig {
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
            candidate: LateCandidateBuildConfig {
                fde_dimension: 2,
                nlist: 2,
                probe_budget: 2,
                candidate_k: 3,
                routing_metric: LateRoutingMetric::NegativeL2,
                kmeans_max_iters: 20,
                kmeans_epsilon: 1e-6,
                max_cluster_bytes: 2 * 1024,
                max_bootstrap_bytes: 64 * 1024,
            },
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
        let cluster_payloads = built
            .reference
            .candidate_index
            .clusters
            .iter()
            .map(|reference| FetchedLateCandidateCluster {
                reference,
                bytes: artifact_by_key[reference.key.as_str()],
            })
            .collect::<Vec<_>>();
        let candidates = decode_all_candidate_rows(
            &built.reference.candidate_index,
            artifact_by_key[built.reference.candidate_index.bootstrap.key.as_str()],
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
            assert_eq!(matrix, original.exact_matrix);

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

        let rooted = std::iter::once(built.reference.candidate_index.bootstrap.key.as_str())
            .chain(
                built
                    .reference
                    .candidate_index
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
