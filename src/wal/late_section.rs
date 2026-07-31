//! Immutable content-addressed manifest late-state sections.
//!
//! The root manifest remains the only visibility point. A section object may
//! exist before publication, but readers discover it only through
//! [`ManifestSectionRef`]. Version 4 added immutable late-interaction segment
//! state; version 5 adds the flat-SQ8 candidate kind while retaining
//! version-1 through version-4 decoders.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::embedding::{
    ArtifactChecksum, CenteringArtifact, ContentHash, EmbeddingProfileId, EmbeddingProfileRef,
    FdeGenerationId, MatrixDtype, MultiVectorEpochId, PhysicalInputFragmentIdentity,
    RecordVersionCoverage, SemanticOverlayRef,
};
use crate::error::{Result, ZeppelinError};
use crate::index::late_interaction::{
    AttributeBlockRef, FdeTransform, LateCandidateIndexRef, LateFlatCandidateRef, MatrixBlockRef,
};
use crate::namespace::branching::{ArtifactOrigin, ArtifactOriginIndex};
use crate::storage::{CreateOnlyOutcome, NamespaceObjectFamily, NamespaceObjectKey, ZeppelinStore};

const LATE_STATE_MAGIC: &[u8; 4] = b"ZLS1";
const LATE_STATE_VERSION_V1: u8 = 1;
const LATE_STATE_VERSION_V2: u8 = 2;
const LATE_STATE_VERSION_V3: u8 = 3;
const LATE_STATE_VERSION_V4: u8 = 4;
const LATE_STATE_VERSION_V5: u8 = 5;

/// Persisted section format version carried by root-manifest references.
pub const LATE_STATE_FORMAT_VERSION: u32 = 5;

/// Whether a root reference names a section version this binary can decode.
#[must_use]
pub const fn is_supported_late_state_format_version(version: u32) -> bool {
    version == 1
        || version == 2
        || version == 3
        || version == 4
        || version == LATE_STATE_FORMAT_VERSION
}

/// Root-manifest reference to one immutable late-state section object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestSectionRef {
    /// Exact owned object key.
    pub key: String,
    /// SHA-256 over the complete canonical section bytes.
    pub checksum: [u8; 32],
    /// Complete serialized object size.
    pub size_bytes: u64,
    /// Section-internal format version.
    pub format_version: u32,
    /// Physical owner, or local ownership when absent.
    #[serde(default)]
    pub artifact_origin: Option<ArtifactOriginIndex>,
}

/// One checksum-addressed source object retained by the late-state section.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceInventoryRef {
    /// Exact Zeppelin-owned source object key.
    pub key: String,
    /// SHA-256 over the complete source bytes.
    pub checksum: ArtifactChecksum,
    /// Exact source object size.
    pub size_bytes: u64,
    /// Validated declared source media type.
    pub media_type: String,
    /// Section-local physical-origin index, or the section's owner when absent.
    #[serde(default)]
    pub artifact_origin: Option<ArtifactOriginIndex>,
}

/// Durable, non-sensitive evidence that exact source versions failed enrichment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuarantineEvidenceRef {
    /// Content-addressed quarantine evidence object.
    pub key: String,
    /// SHA-256 over the complete evidence bytes.
    pub checksum: ArtifactChecksum,
    /// Complete evidence object size.
    pub size_bytes: u64,
    /// Deterministic enrichment work identity.
    pub work_id: [u8; 32],
    /// Exact physical input fragment whose rows failed.
    pub source_fragment: PhysicalInputFragmentIdentity,
    /// Semantic epoch attempted by the failed work.
    pub semantic_epoch: MultiVectorEpochId,
    /// FDE generation attempted by the failed work.
    pub fde_generation: FdeGenerationId,
    /// Exact failed source versions, without source content.
    pub failed_versions: RecordVersionCoverage,
    /// Section-local owner of the evidence object, or section owner when absent.
    #[serde(default)]
    pub artifact_origin: Option<ArtifactOriginIndex>,
}

/// One immutable attribute or full-text sidecar owned by a late segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LateSegmentObjectRef {
    /// Exact immutable object key.
    pub key: String,
    /// SHA-256 over the complete object.
    pub checksum: ArtifactChecksum,
    /// Complete object size.
    pub size_bytes: u64,
    /// Persisted codec version.
    pub format_version: u32,
}

/// Wave-one candidate selection kind persisted by one late segment.
///
/// `FlatSq8` is the production kind for the ≲50–100k-unit corpus regime
/// (operator decision 2026-07-31); `Ivf` is retained for the future scale
/// phase and for decoding version-4 sections, whose bytes default to it.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LateCandidateKind {
    /// Routed candidate clusters read per query (wave-one GETs).
    #[default]
    Ivf,
    /// Resident exhaustive SQ8 selection over one hydrated `ZFQ1` artifact.
    FlatSq8,
}

/// Manifest-visible descriptor for one immutable late-interaction segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LateInteractionSegmentRef {
    /// Stable immutable segment identity.
    pub id: String,
    /// Semantic profile represented by every row.
    pub profile: EmbeddingProfileId,
    /// Exact-scoring semantic epoch represented by every matrix.
    pub semantic_epoch: MultiVectorEpochId,
    /// Fixed-dimensional encoding generation used by the candidate index.
    pub fde_generation: FdeGenerationId,
    /// Epoch-uniform exact-matrix scalar representation.
    pub matrix_dtype: MatrixDtype,
    /// Number of live retrieval-unit rows in the segment.
    pub record_count: u64,
    /// Total exact vectors across all retrieval-unit rows.
    pub total_vector_count: u64,
    /// Coordinates per exact vector.
    pub vector_dimension: u32,
    /// Coordinates per fixed-dimensional candidate vector.
    pub fde_dimension: u32,
    /// Highest source mutation sequence proven into this full rebuild.
    pub coverage_sequence: u64,
    /// Immutable IVF candidate bootstrap and cluster objects (`Ivf` kind only).
    pub candidate_index: Option<LateCandidateIndexRef>,
    /// Bounded record-major exact-matrix objects.
    pub matrix_objects: Vec<MatrixBlockRef>,
    /// Exact attribute sidecars used by wave-two result construction.
    pub attribute_objects: Vec<AttributeBlockRef>,
    /// Immutable full-text sidecars for the same retrieval-unit rows.
    pub fts_objects: Vec<LateSegmentObjectRef>,
    /// Section-local physical owner shared by every segment artifact.
    #[serde(default)]
    pub artifact_origin: Option<ArtifactOriginIndex>,
    /// Wave-one candidate selection kind; version-4 bytes default to IVF.
    #[serde(default)]
    pub candidate_kind: LateCandidateKind,
    /// Resident flat-SQ8 candidate artifact (`FlatSq8` kind only).
    #[serde(default)]
    pub flat_candidate: Option<LateFlatCandidateRef>,
}

impl LateInteractionSegmentRef {
    /// Borrow the IVF candidate descriptor, failing loud on a kind mismatch.
    pub fn ivf_candidate_index(&self) -> Result<&LateCandidateIndexRef> {
        if self.candidate_kind != LateCandidateKind::Ivf {
            return Err(ZeppelinError::Serialization(format!(
                "late segment {} is not an IVF candidate segment",
                self.id
            )));
        }
        self.candidate_index.as_ref().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "late IVF segment {} has no candidate index",
                self.id
            ))
        })
    }

    /// Borrow the flat-SQ8 candidate descriptor, failing loud on a kind mismatch.
    pub fn flat_candidate_ref(&self) -> Result<&LateFlatCandidateRef> {
        if self.candidate_kind != LateCandidateKind::FlatSq8 {
            return Err(ZeppelinError::Serialization(format!(
                "late segment {} is not a flat-SQ8 candidate segment",
                self.id
            )));
        }
        self.flat_candidate.as_ref().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "late flat segment {} has no flat candidate artifact",
                self.id
            ))
        })
    }
}

impl SourceInventoryRef {
    /// Build the checksum-addressed local source key.
    #[must_use]
    pub fn s3_key(namespace: &str, content_hash: ContentHash) -> String {
        format!(
            "{}{}",
            NamespaceObjectFamily::Source.namespace_prefix(namespace),
            content_hash.to_hex()
        )
    }
}

/// One section-resident immutable artifact with its resolved physical owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedLateArtifact {
    pub(crate) key: String,
    pub(crate) family: NamespaceObjectFamily,
    pub(crate) origin: ArtifactOrigin,
}

#[derive(Clone, Copy)]
struct NestedArtifactRef<'a> {
    kind: &'static str,
    key: &'a str,
    family: NamespaceObjectFamily,
    artifact_origin: Option<ArtifactOriginIndex>,
    content_checksum: Option<ArtifactChecksum>,
}

fn insert_unique_segment_key<'a>(
    keys: &mut BTreeSet<&'a str>,
    key: &'a str,
    kind: &str,
) -> Result<()> {
    if key.is_empty() {
        return Err(ZeppelinError::Serialization(format!(
            "late segment {kind} key must be non-empty"
        )));
    }
    if !keys.insert(key) {
        return Err(ZeppelinError::Serialization(format!(
            "late-state segments contain duplicate artifact key {key}"
        )));
    }
    Ok(())
}

/// Version-5 late-interaction manifest state.
///
/// The origin table is section-local so its indices remain stable inside the
/// immutable content-addressed bytes even when the root manifest later
/// canonicalizes or extends its own origin table.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct LateStateSection {
    /// Durable source objects reachable through this section.
    #[serde(default)]
    pub source_inventory: Vec<SourceInventoryRef>,
    /// Canonical physical owners used only by nested section references.
    #[serde(default)]
    pub artifact_origins: Vec<ArtifactOrigin>,
    /// Single active semantic profile for the MVP.
    #[serde(default)]
    pub active_profile: Option<EmbeddingProfileRef>,
    /// Immutable embedding/FDE overlays published for exact source versions.
    #[serde(default)]
    pub semantic_overlays: Vec<SemanticOverlayRef>,
    /// Deterministic failures that block the contiguous semantic watermark.
    #[serde(default)]
    pub quarantine_evidence: Vec<QuarantineEvidenceRef>,
    /// Immutable late-interaction segments reachable through this section.
    #[serde(default)]
    pub late_interaction_segments: Vec<LateInteractionSegmentRef>,
    /// Segment selected as the single-profile query baseline.
    #[serde(default)]
    pub active_late_segment: Option<String>,
}

impl LateStateSection {
    /// Construct an empty version-5 section.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            source_inventory: Vec::new(),
            artifact_origins: Vec::new(),
            active_profile: None,
            semantic_overlays: Vec::new(),
            quarantine_evidence: Vec::new(),
            late_interaction_segments: Vec::new(),
            active_late_segment: None,
        }
    }

    /// Serialize canonical version-5 bytes.
    pub fn to_bytes(&self) -> Result<Bytes> {
        self.validate_structural()?;
        let payload = rmp_serde::to_vec(self).map_err(|error| {
            ZeppelinError::Serialization(format!(
                "late-state section MessagePack serialize: {error}"
            ))
        })?;
        let mut bytes = Vec::with_capacity(LATE_STATE_MAGIC.len() + 1 + payload.len());
        bytes.extend_from_slice(LATE_STATE_MAGIC);
        bytes.push(LATE_STATE_VERSION_V5);
        bytes.extend_from_slice(&payload);
        Ok(Bytes::from(bytes))
    }

    /// Decode and validate version 1 through version 5 section bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < LATE_STATE_MAGIC.len() + 1 {
            return Err(ZeppelinError::Serialization(
                "late-state section is shorter than its magic and version header".to_string(),
            ));
        }
        if &data[..LATE_STATE_MAGIC.len()] != LATE_STATE_MAGIC {
            return Err(ZeppelinError::Serialization(
                "late-state section has invalid ZLS1 magic".to_string(),
            ));
        }
        let version = data[LATE_STATE_MAGIC.len()];
        if version != LATE_STATE_VERSION_V1
            && version != LATE_STATE_VERSION_V2
            && version != LATE_STATE_VERSION_V3
            && version != LATE_STATE_VERSION_V4
            && version != LATE_STATE_VERSION_V5
        {
            return Err(ZeppelinError::Serialization(format!(
                "unsupported late-state section version {version}"
            )));
        }
        let section: Self =
            rmp_serde::from_slice(&data[LATE_STATE_MAGIC.len() + 1..]).map_err(|error| {
                ZeppelinError::Serialization(format!(
                    "late-state section MessagePack deserialize: {error}"
                ))
            })?;
        section.validate_structural()?;
        Ok(section)
    }

    /// SHA-256 over complete canonical section bytes.
    #[must_use]
    pub fn checksum(bytes: &[u8]) -> [u8; 32] {
        Sha256::digest(bytes).into()
    }

    /// Derive the content-addressed key for canonical section bytes.
    #[must_use]
    pub fn s3_key(namespace: &str, checksum: &[u8; 32]) -> String {
        let checksum_hex = checksum
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        format!(
            "{}{checksum_hex}",
            NamespaceObjectFamily::LateSection.namespace_prefix(namespace)
        )
    }

    /// Build a checksum-addressed key for one registered late artifact family.
    #[must_use]
    pub(crate) fn artifact_s3_key(
        namespace: &str,
        family: NamespaceObjectFamily,
        checksum: ArtifactChecksum,
    ) -> String {
        format!(
            "{}{}",
            family.namespace_prefix(namespace),
            checksum.to_hex()
        )
    }

    fn for_each_nested_artifact(
        &self,
        mut visitor: impl FnMut(NestedArtifactRef<'_>) -> Result<()>,
    ) -> Result<()> {
        for source in &self.source_inventory {
            visitor(NestedArtifactRef {
                kind: "source inventory",
                key: &source.key,
                family: NamespaceObjectFamily::Source,
                artifact_origin: source.artifact_origin,
                content_checksum: None,
            })?;
        }
        if let Some(profile) = self.active_profile.as_ref() {
            if let Some(mean) = profile.epoch.exact_scoring_transform.mean() {
                visitor(NestedArtifactRef {
                    kind: "exact-scoring centering mean",
                    key: &mean.key,
                    family: NamespaceObjectFamily::Centering,
                    artifact_origin: mean.artifact_origin,
                    content_checksum: Some(mean.checksum),
                })?;
            }
            visitor(NestedArtifactRef {
                kind: "FDE transform",
                key: &profile.fde.transform_artifact.key,
                family: NamespaceObjectFamily::FdeTransform,
                artifact_origin: profile.fde.transform_artifact.artifact_origin,
                content_checksum: Some(profile.fde.transform_artifact.checksum),
            })?;
            if let Some(mean) = profile.fde.candidate_vector_transform.mean() {
                visitor(NestedArtifactRef {
                    kind: "candidate centering mean",
                    key: &mean.key,
                    family: NamespaceObjectFamily::Centering,
                    artifact_origin: mean.artifact_origin,
                    content_checksum: Some(mean.checksum),
                })?;
            }
        }
        for overlay in &self.semantic_overlays {
            visitor(NestedArtifactRef {
                kind: "overlay input fragment",
                key: &overlay.source_fragment.key,
                family: NamespaceObjectFamily::InputWal,
                artifact_origin: overlay.source_fragment.artifact_origin,
                content_checksum: None,
            })?;
            visitor(NestedArtifactRef {
                kind: "matrix fragment",
                key: &overlay.embeddings.key,
                family: NamespaceObjectFamily::MatrixFragment,
                artifact_origin: overlay.embeddings.artifact_origin,
                content_checksum: Some(overlay.embeddings.checksum),
            })?;
            visitor(NestedArtifactRef {
                kind: "FDE fragment",
                key: &overlay.fde_vectors.key,
                family: NamespaceObjectFamily::FdeFragment,
                artifact_origin: overlay.fde_vectors.artifact_origin,
                content_checksum: Some(overlay.fde_vectors.checksum),
            })?;
        }
        for segment in &self.late_interaction_segments {
            if let Some(candidate_index) = segment.candidate_index.as_ref() {
                visitor(NestedArtifactRef {
                    kind: "late candidate bootstrap",
                    key: &candidate_index.bootstrap.key,
                    family: NamespaceObjectFamily::LateSegment,
                    artifact_origin: segment.artifact_origin,
                    content_checksum: Some(candidate_index.bootstrap.checksum),
                })?;
                for cluster in &candidate_index.clusters {
                    visitor(NestedArtifactRef {
                        kind: "late candidate cluster",
                        key: &cluster.key,
                        family: NamespaceObjectFamily::LateSegment,
                        artifact_origin: segment.artifact_origin,
                        content_checksum: Some(cluster.checksum),
                    })?;
                }
            }
            if let Some(flat) = segment.flat_candidate.as_ref() {
                visitor(NestedArtifactRef {
                    kind: "late flat candidate artifact",
                    key: &flat.key,
                    family: NamespaceObjectFamily::LateSegment,
                    artifact_origin: segment.artifact_origin,
                    content_checksum: Some(flat.checksum),
                })?;
            }
            for matrix in &segment.matrix_objects {
                visitor(NestedArtifactRef {
                    kind: "late matrix block",
                    key: &matrix.key,
                    family: NamespaceObjectFamily::LateSegment,
                    artifact_origin: segment.artifact_origin,
                    content_checksum: Some(matrix.checksum),
                })?;
            }
            for attributes in &segment.attribute_objects {
                visitor(NestedArtifactRef {
                    kind: "late attribute object",
                    key: &attributes.key,
                    family: NamespaceObjectFamily::LateSegment,
                    artifact_origin: segment.artifact_origin,
                    content_checksum: Some(attributes.checksum),
                })?;
            }
            for fts in &segment.fts_objects {
                visitor(NestedArtifactRef {
                    kind: "late FTS object",
                    key: &fts.key,
                    family: NamespaceObjectFamily::LateSegment,
                    artifact_origin: segment.artifact_origin,
                    content_checksum: Some(fts.checksum),
                })?;
            }
        }
        for evidence in &self.quarantine_evidence {
            visitor(NestedArtifactRef {
                kind: "quarantine input fragment",
                key: &evidence.source_fragment.key,
                family: NamespaceObjectFamily::InputWal,
                artifact_origin: evidence.source_fragment.artifact_origin,
                content_checksum: None,
            })?;
            visitor(NestedArtifactRef {
                kind: "quarantine evidence",
                key: &evidence.key,
                family: NamespaceObjectFamily::Quarantine,
                artifact_origin: evidence.artifact_origin,
                content_checksum: Some(evidence.checksum),
            })?;
        }
        Ok(())
    }

    fn for_each_nested_artifact_mut(
        &mut self,
        mut visitor: impl FnMut(
            &'static str,
            &str,
            NamespaceObjectFamily,
            &mut Option<ArtifactOriginIndex>,
        ) -> Result<()>,
    ) -> Result<()> {
        for source in &mut self.source_inventory {
            visitor(
                "source inventory",
                &source.key,
                NamespaceObjectFamily::Source,
                &mut source.artifact_origin,
            )?;
        }
        if let Some(profile) = self.active_profile.as_mut() {
            if let Some(mean) = profile.epoch.exact_scoring_transform.mean_mut() {
                visitor(
                    "exact-scoring centering mean",
                    &mean.key,
                    NamespaceObjectFamily::Centering,
                    &mut mean.artifact_origin,
                )?;
            }
            visitor(
                "FDE transform",
                &profile.fde.transform_artifact.key,
                NamespaceObjectFamily::FdeTransform,
                &mut profile.fde.transform_artifact.artifact_origin,
            )?;
            if let Some(mean) = profile.fde.candidate_vector_transform.mean_mut() {
                visitor(
                    "candidate centering mean",
                    &mean.key,
                    NamespaceObjectFamily::Centering,
                    &mut mean.artifact_origin,
                )?;
            }
        }
        for overlay in &mut self.semantic_overlays {
            visitor(
                "overlay input fragment",
                &overlay.source_fragment.key,
                NamespaceObjectFamily::InputWal,
                &mut overlay.source_fragment.artifact_origin,
            )?;
            visitor(
                "matrix fragment",
                &overlay.embeddings.key,
                NamespaceObjectFamily::MatrixFragment,
                &mut overlay.embeddings.artifact_origin,
            )?;
            visitor(
                "FDE fragment",
                &overlay.fde_vectors.key,
                NamespaceObjectFamily::FdeFragment,
                &mut overlay.fde_vectors.artifact_origin,
            )?;
        }
        for segment in &mut self.late_interaction_segments {
            if let Some(candidate_index) = segment.candidate_index.as_ref() {
                visitor(
                    "late candidate bootstrap",
                    &candidate_index.bootstrap.key,
                    NamespaceObjectFamily::LateSegment,
                    &mut segment.artifact_origin,
                )?;
                for cluster in &candidate_index.clusters {
                    visitor(
                        "late candidate cluster",
                        &cluster.key,
                        NamespaceObjectFamily::LateSegment,
                        &mut segment.artifact_origin,
                    )?;
                }
            }
            if let Some(flat) = segment.flat_candidate.as_ref() {
                visitor(
                    "late flat candidate artifact",
                    &flat.key,
                    NamespaceObjectFamily::LateSegment,
                    &mut segment.artifact_origin,
                )?;
            }
            for matrix in &segment.matrix_objects {
                visitor(
                    "late matrix block",
                    &matrix.key,
                    NamespaceObjectFamily::LateSegment,
                    &mut segment.artifact_origin,
                )?;
            }
            for attributes in &segment.attribute_objects {
                visitor(
                    "late attribute object",
                    &attributes.key,
                    NamespaceObjectFamily::LateSegment,
                    &mut segment.artifact_origin,
                )?;
            }
            for fts in &segment.fts_objects {
                visitor(
                    "late FTS object",
                    &fts.key,
                    NamespaceObjectFamily::LateSegment,
                    &mut segment.artifact_origin,
                )?;
            }
        }
        for evidence in &mut self.quarantine_evidence {
            visitor(
                "quarantine input fragment",
                &evidence.source_fragment.key,
                NamespaceObjectFamily::InputWal,
                &mut evidence.source_fragment.artifact_origin,
            )?;
            visitor(
                "quarantine evidence",
                &evidence.key,
                NamespaceObjectFamily::Quarantine,
                &mut evidence.artifact_origin,
            )?;
        }
        Ok(())
    }

    fn nested_origin<'a>(
        &'a self,
        artifact: NestedArtifactRef<'_>,
        section_origin: &'a ArtifactOrigin,
    ) -> Result<&'a ArtifactOrigin> {
        let Some(index) = artifact.artifact_origin else {
            return Ok(section_origin);
        };
        let index = usize::try_from(index.get()).map_err(|_| {
            ZeppelinError::Serialization(format!(
                "{} origin index {} does not fit this platform",
                artifact.kind,
                index.get()
            ))
        })?;
        self.artifact_origins.get(index).ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "{} origin index {index} is out of bounds for table length {}",
                artifact.kind,
                self.artifact_origins.len()
            ))
        })
    }

    /// Resolve all section-resident artifact owners in deterministic field order.
    pub(crate) fn resolved_artifacts(
        &self,
        section_origin: &ArtifactOrigin,
    ) -> Result<Vec<ResolvedLateArtifact>> {
        let mut resolved = Vec::new();
        self.for_each_nested_artifact(|artifact| {
            resolved.push(ResolvedLateArtifact {
                key: artifact.key.to_string(),
                family: artifact.family,
                origin: self.nested_origin(artifact, section_origin)?.clone(),
            });
            Ok(())
        })?;
        Ok(resolved)
    }

    /// Resolve one nested source's owner, inheriting the section owner for `None`.
    pub fn source_origin<'a>(
        &'a self,
        source: &SourceInventoryRef,
        section_origin: &'a ArtifactOrigin,
    ) -> Result<&'a ArtifactOrigin> {
        self.nested_origin(
            NestedArtifactRef {
                kind: "source inventory",
                key: &source.key,
                family: NamespaceObjectFamily::Source,
                artifact_origin: source.artifact_origin,
                content_checksum: None,
            },
            section_origin,
        )
    }

    /// Validate every nested key against its declared family and physical owner.
    pub fn validate_for_origin(&self, section_origin: &ArtifactOrigin) -> Result<()> {
        self.validate_structural()?;
        self.for_each_nested_artifact(|artifact| {
            let origin = self.nested_origin(artifact, section_origin)?;
            let owned =
                NamespaceObjectKey::classify(origin.namespace.as_str(), artifact.key.to_string())?;
            if owned.family() != artifact.family {
                return Err(ZeppelinError::Validation(format!(
                    "{} key is outside the registered {:?} family: {}",
                    artifact.kind, artifact.family, artifact.key
                )));
            }
            if let Some(checksum) = artifact.content_checksum {
                if artifact.family != NamespaceObjectFamily::LateSegment {
                    let expected =
                        Self::artifact_s3_key(origin.namespace.as_str(), artifact.family, checksum);
                    if artifact.key != expected {
                        return Err(ZeppelinError::Validation(format!(
                            "{} key must equal its content-addressed key {expected}",
                            artifact.kind
                        )));
                    }
                }
            }
            Ok(())
        })
    }

    /// Validate local transform and centering artifacts before profile publication.
    pub(crate) async fn validate_local_profile_artifacts(
        store: &ZeppelinStore,
        namespace: &str,
        profile: &EmbeddingProfileRef,
    ) -> Result<()> {
        let transform = &profile.fde.transform_artifact;
        if transform.artifact_origin.is_some() {
            return Err(ZeppelinError::Validation(
                "new profile activation cannot supply a section-local transform origin index"
                    .to_string(),
            ));
        }
        let transform_bytes = Self::read_local_content_addressed_artifact(
            store,
            namespace,
            NamespaceObjectFamily::FdeTransform,
            &transform.key,
            transform.checksum,
            transform.size_bytes,
        )
        .await?;
        let decoded = FdeTransform::from_bytes(&transform_bytes)?;
        if decoded.params() != profile.fde.params {
            return Err(ZeppelinError::Validation(
                "FDE transform parameters do not match the profile recipe".to_string(),
            ));
        }

        let mut means = BTreeMap::new();
        for mean in [
            profile.epoch.exact_scoring_transform.mean(),
            profile.fde.candidate_vector_transform.mean(),
        ]
        .into_iter()
        .flatten()
        {
            if mean.artifact_origin.is_some() {
                return Err(ZeppelinError::Validation(
                    "new profile activation cannot supply a section-local centering origin index"
                        .to_string(),
                ));
            }
            match means.entry(mean.key.as_str()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert((mean.checksum, mean.size_bytes));
                }
                std::collections::btree_map::Entry::Occupied(entry)
                    if *entry.get() != (mean.checksum, mean.size_bytes) =>
                {
                    return Err(ZeppelinError::Validation(
                        "one centering key has inconsistent checksum or size metadata".to_string(),
                    ));
                }
                std::collections::btree_map::Entry::Occupied(_) => {}
            }
        }
        for (key, (checksum, size_bytes)) in means {
            let bytes = Self::read_local_content_addressed_artifact(
                store,
                namespace,
                NamespaceObjectFamily::Centering,
                key,
                checksum,
                size_bytes,
            )
            .await?;
            CenteringArtifact::from_bytes(
                &bytes,
                checksum,
                usize::try_from(profile.epoch.vector_dimension).map_err(|_| {
                    ZeppelinError::Validation(
                        "centering dimension does not fit this platform".to_string(),
                    )
                })?,
            )?;
        }
        Ok(())
    }

    async fn read_local_content_addressed_artifact(
        store: &ZeppelinStore,
        namespace: &str,
        family: NamespaceObjectFamily,
        key: &str,
        checksum: ArtifactChecksum,
        size_bytes: u64,
    ) -> Result<Bytes> {
        let expected = Self::artifact_s3_key(namespace, family, checksum);
        if key != expected {
            return Err(ZeppelinError::Validation(format!(
                "{family:?} artifact key must equal its content-addressed key {expected}"
            )));
        }
        let owned = NamespaceObjectKey::classify(namespace, key.to_string())?;
        if owned.family() != family {
            return Err(ZeppelinError::Validation(format!(
                "artifact key is outside the registered {family:?} family"
            )));
        }
        let bytes = store.get(key).await?;
        let actual_size = u64::try_from(bytes.len()).map_err(|_| {
            ZeppelinError::Serialization(format!(
                "{family:?} artifact size does not fit persisted u64"
            ))
        })?;
        if actual_size != size_bytes {
            return Err(ZeppelinError::Serialization(format!(
                "{family:?} artifact size mismatch: expected {size_bytes}, got {actual_size}"
            )));
        }
        if ArtifactChecksum::digest(&bytes) != checksum {
            return Err(ZeppelinError::Serialization(format!(
                "{family:?} artifact checksum mismatch"
            )));
        }
        Ok(bytes)
    }

    /// Read one manifest-selected source and verify its persisted integrity.
    pub async fn read_source_checked(
        &self,
        store: &ZeppelinStore,
        source: &SourceInventoryRef,
        section_origin: &ArtifactOrigin,
    ) -> Result<Bytes> {
        let origin = self.source_origin(source, section_origin)?;
        let owned = NamespaceObjectKey::classify(origin.namespace.as_str(), source.key.clone())?;
        if owned.family() != NamespaceObjectFamily::Source {
            return Err(ZeppelinError::Validation(format!(
                "source inventory key is outside the registered source family: {}",
                source.key
            )));
        }
        let bytes = store.get(&source.key).await?;
        let actual_size = u64::try_from(bytes.len()).map_err(|_| {
            ZeppelinError::Serialization(format!(
                "source object {} size does not fit persisted u64",
                source.key
            ))
        })?;
        if actual_size != source.size_bytes {
            return Err(ZeppelinError::Serialization(format!(
                "source object {} size mismatch: expected {}, got {}",
                source.key, source.size_bytes, actual_size
            )));
        }
        let actual_checksum = ArtifactChecksum::digest(&bytes);
        if actual_checksum != source.checksum {
            return Err(ZeppelinError::Serialization(format!(
                "source object {} checksum mismatch",
                source.key
            )));
        }
        Ok(bytes)
    }

    /// Preserve all resolved nested owners when republishing under a new owner.
    pub fn rebase_nested_artifact_origins(
        &mut self,
        previous_section_origin: &ArtifactOrigin,
        next_section_origin: &ArtifactOrigin,
    ) -> Result<()> {
        self.validate_structural()?;
        let resolved = self
            .resolved_artifacts(previous_section_origin)?
            .into_iter()
            .map(|artifact| artifact.origin)
            .collect::<Vec<_>>();
        let table = resolved
            .iter()
            .filter(|origin| *origin != next_section_origin)
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let indices = table
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, origin)| {
                u32::try_from(index)
                    .map(ArtifactOriginIndex::new)
                    .map(|index| (origin, index))
                    .map_err(|_| {
                        ZeppelinError::Serialization(
                            "late-state origin table exceeds u32 address space".to_string(),
                        )
                    })
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let mut cursor = 0_usize;
        self.for_each_nested_artifact_mut(|kind, _, _, artifact_origin| {
            let origin = resolved.get(cursor).ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "rebased late-state origin walk omitted {kind}"
                ))
            })?;
            cursor += 1;
            *artifact_origin = if origin == next_section_origin {
                None
            } else {
                Some(*indices.get(origin).ok_or_else(|| {
                    ZeppelinError::Serialization(
                        "rebased late-state origin table omitted nested artifact owner".to_string(),
                    )
                })?)
            };
            Ok(())
        })?;
        if cursor != resolved.len() {
            return Err(ZeppelinError::Serialization(
                "rebased late-state origin walk changed length".to_string(),
            ));
        }
        self.artifact_origins = table;
        self.validate_structural()
    }

    /// Return the section-local index for a foreign owner, adding it if needed.
    pub(crate) fn intern_artifact_origin(
        &mut self,
        origin: &ArtifactOrigin,
        section_origin: &ArtifactOrigin,
    ) -> Result<Option<ArtifactOriginIndex>> {
        if origin == section_origin {
            return Ok(None);
        }
        if let Some(index) = self
            .artifact_origins
            .iter()
            .position(|candidate| candidate == origin)
        {
            return u32::try_from(index)
                .map(ArtifactOriginIndex::new)
                .map(Some)
                .map_err(|_| {
                    ZeppelinError::Serialization(
                        "late-state origin table exceeds u32 address space".to_string(),
                    )
                });
        }
        let index = u32::try_from(self.artifact_origins.len()).map_err(|_| {
            ZeppelinError::Serialization(
                "late-state origin table exceeds u32 address space".to_string(),
            )
        })?;
        self.artifact_origins.push(origin.clone());
        Ok(Some(ArtifactOriginIndex::new(index)))
    }

    /// Canonicalize the section-local origin table and remap nested indices.
    pub fn canonicalize_artifact_origins(&mut self) -> Result<()> {
        self.validate_origin_indices(false)?;
        let mut resolved = Vec::new();
        self.for_each_nested_artifact(|artifact| {
            resolved.push(
                artifact
                    .artifact_origin
                    .map(|index| {
                        usize::try_from(index.get())
                            .ok()
                            .and_then(|index| self.artifact_origins.get(index))
                            .cloned()
                            .ok_or_else(|| {
                                ZeppelinError::Serialization(format!(
                                    "{} origin index {} is out of bounds",
                                    artifact.kind,
                                    index.get()
                                ))
                            })
                    })
                    .transpose()?,
            );
            Ok(())
        })?;
        let table = resolved
            .iter()
            .flatten()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let indices = table
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, origin)| {
                u32::try_from(index)
                    .map(ArtifactOriginIndex::new)
                    .map(|index| (origin, index))
                    .map_err(|_| {
                        ZeppelinError::Serialization(
                            "late-state origin table exceeds u32 address space".to_string(),
                        )
                    })
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let mut cursor = 0_usize;
        self.for_each_nested_artifact_mut(|kind, _, _, artifact_origin| {
            let origin = resolved.get(cursor).ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "canonical late-state origin walk omitted {kind}"
                ))
            })?;
            cursor += 1;
            *artifact_origin = origin
                .as_ref()
                .map(|origin| {
                    indices.get(origin).copied().ok_or_else(|| {
                        ZeppelinError::Serialization(
                            "canonical late-state origin table omitted nested artifact owner"
                                .to_string(),
                        )
                    })
                })
                .transpose()?;
            Ok(())
        })?;
        if cursor != resolved.len() {
            return Err(ZeppelinError::Serialization(
                "canonical late-state origin walk changed length".to_string(),
            ));
        }
        self.artifact_origins = table;
        self.validate_structural()
    }

    fn validate_structural(&self) -> Result<()> {
        self.validate_origin_indices(true)?;
        let mut keys = BTreeSet::new();
        for source in &self.source_inventory {
            if !keys.insert(source.key.as_str()) {
                return Err(ZeppelinError::Serialization(format!(
                    "late-state source inventory contains duplicate key {}",
                    source.key
                )));
            }
            if source.media_type.is_empty() {
                return Err(ZeppelinError::Serialization(format!(
                    "late-state source {} has an empty media type",
                    source.key
                )));
            }
        }
        if let Some(profile) = self.active_profile.as_ref() {
            profile.validate()?;
        } else if !self.semantic_overlays.is_empty()
            || !self.quarantine_evidence.is_empty()
            || !self.late_interaction_segments.is_empty()
        {
            return Err(ZeppelinError::Serialization(
                "late-state semantic state requires an active profile".to_string(),
            ));
        }
        self.validate_late_segments()?;
        let mut settled_rows = BTreeSet::new();
        for overlay in &self.semantic_overlays {
            let profile = self.active_profile.as_ref().ok_or_else(|| {
                ZeppelinError::Serialization(
                    "late-state semantic overlay lost its active profile".to_string(),
                )
            })?;
            if overlay.semantic_epoch != profile.epoch.id
                || overlay.embeddings.semantic_epoch != profile.epoch.id
            {
                return Err(ZeppelinError::Serialization(
                    "semantic overlay epoch does not match the active profile".to_string(),
                ));
            }
            if overlay.fde_generation != profile.fde.generation
                || overlay.fde_vectors.generation != profile.fde.generation
            {
                return Err(ZeppelinError::Serialization(
                    "semantic overlay FDE generation does not match the active profile".to_string(),
                ));
            }
            if overlay.embeddings.source_fragment_checksum != overlay.source_fragment.checksum
                || overlay.fde_vectors.embedding_fragment_checksum != overlay.embeddings.checksum
            {
                return Err(ZeppelinError::Serialization(
                    "semantic overlay derivation checksums are inconsistent".to_string(),
                ));
            }
            if overlay.embeddings.row_count != overlay.fde_vectors.row_count
                || overlay.embeddings.row_count as usize != overlay.covered_versions.records.len()
                || overlay.embeddings.vector_dimension != profile.epoch.vector_dimension
                || overlay.embeddings.dtype != profile.epoch.matrix_dtype
            {
                return Err(ZeppelinError::Serialization(
                    "semantic overlay persisted shapes do not match the active profile".to_string(),
                ));
            }
            if overlay.embeddings.format_version == 0
                || overlay.fde_vectors.format_version == 0
                || overlay.embeddings.size_bytes == 0
                || overlay.fde_vectors.size_bytes == 0
            {
                return Err(ZeppelinError::Serialization(
                    "semantic overlay artifact versions and sizes must be positive".to_string(),
                ));
            }
            let expected_input_suffix = format!("{}.wal", overlay.source_fragment.id);
            if !overlay
                .source_fragment
                .key
                .ends_with(&expected_input_suffix)
                || overlay.source_fragment.size_bytes == 0
            {
                return Err(ZeppelinError::Serialization(
                    "semantic overlay input-fragment identity is malformed".to_string(),
                ));
            }
            if overlay.covered_versions.records.is_empty() {
                return Err(ZeppelinError::Serialization(
                    "semantic overlay coverage must not be empty".to_string(),
                ));
            }
            let mut previous = None;
            for record in &overlay.covered_versions.records {
                if previous.is_some_and(|previous| previous >= record.row_ordinal) {
                    return Err(ZeppelinError::Serialization(
                        "semantic overlay coverage rows must be strictly increasing".to_string(),
                    ));
                }
                previous = Some(record.row_ordinal);
                if !settled_rows.insert((
                    overlay.source_fragment.key.clone(),
                    overlay.semantic_epoch,
                    overlay.fde_generation,
                    record.row_ordinal,
                )) {
                    return Err(ZeppelinError::Serialization(
                        "late-state derived coverage rows overlap".to_string(),
                    ));
                }
            }
        }
        let mut quarantine_work_ids = BTreeSet::new();
        for evidence in &self.quarantine_evidence {
            let profile = self.active_profile.as_ref().ok_or_else(|| {
                ZeppelinError::Serialization(
                    "late-state quarantine evidence lost its active profile".to_string(),
                )
            })?;
            if evidence.semantic_epoch != profile.epoch.id
                || evidence.fde_generation != profile.fde.generation
                || evidence.size_bytes == 0
                || evidence.failed_versions.records.is_empty()
            {
                return Err(ZeppelinError::Serialization(
                    "late-state quarantine evidence metadata is invalid".to_string(),
                ));
            }
            if !quarantine_work_ids.insert(evidence.work_id) {
                return Err(ZeppelinError::Serialization(
                    "late-state section contains duplicate quarantine evidence".to_string(),
                ));
            }
            let mut previous = None;
            for record in &evidence.failed_versions.records {
                if previous.is_some_and(|previous| previous >= record.row_ordinal) {
                    return Err(ZeppelinError::Serialization(
                        "quarantine coverage rows must be strictly increasing".to_string(),
                    ));
                }
                previous = Some(record.row_ordinal);
                if !settled_rows.insert((
                    evidence.source_fragment.key.clone(),
                    evidence.semantic_epoch,
                    evidence.fde_generation,
                    record.row_ordinal,
                )) {
                    return Err(ZeppelinError::Serialization(
                        "late-state derived coverage rows overlap".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_late_segments(&self) -> Result<()> {
        let mut segment_ids = BTreeSet::new();
        let mut artifact_keys = BTreeSet::new();
        let profile = self.active_profile.as_ref();

        for segment in &self.late_interaction_segments {
            if segment.id.is_empty() || segment.id.contains('/') {
                return Err(ZeppelinError::Serialization(
                    "late segment id must be one non-empty path component".to_string(),
                ));
            }
            if !segment_ids.insert(segment.id.as_str()) {
                return Err(ZeppelinError::Serialization(format!(
                    "late-state section contains duplicate segment id {}",
                    segment.id
                )));
            }
            let profile = profile.ok_or_else(|| {
                ZeppelinError::Serialization(
                    "late-state segment lost its active profile".to_string(),
                )
            })?;
            if segment.profile != profile.profile
                || segment.semantic_epoch != profile.epoch.id
                || segment.fde_generation != profile.fde.generation
                || segment.matrix_dtype != profile.epoch.matrix_dtype
                || segment.vector_dimension != profile.epoch.vector_dimension
            {
                return Err(ZeppelinError::Serialization(
                    "late segment does not match the active semantic profile".to_string(),
                ));
            }
            if segment.record_count == 0
                || segment.total_vector_count == 0
                || segment.vector_dimension == 0
                || segment.fde_dimension == 0
                || segment.coverage_sequence == 0
            {
                return Err(ZeppelinError::Serialization(
                    "late segment counts, dimensions, and coverage sequence must be positive"
                        .to_string(),
                ));
            }
            segment
                .matrix_dtype
                .validate_for_dimension(segment.vector_dimension)?;

            match segment.candidate_kind {
                LateCandidateKind::Ivf => {
                    if segment.flat_candidate.is_some() {
                        return Err(ZeppelinError::Serialization(
                            "late IVF segment cannot carry a flat candidate artifact".to_string(),
                        ));
                    }
                    let candidate = segment.candidate_index.as_ref().ok_or_else(|| {
                        ZeppelinError::Serialization(
                            "late IVF segment requires a candidate index".to_string(),
                        )
                    })?;
                    let bootstrap = &candidate.bootstrap;
                    if bootstrap.size_bytes == 0
                        || bootstrap.format_version == 0
                        || bootstrap.row_count == 0
                        || bootstrap.recipe.fde_dimension == 0
                        || bootstrap.recipe.nlist == 0
                        || bootstrap.recipe.probe_budget == 0
                        || bootstrap.recipe.candidate_k == 0
                        || bootstrap.recipe.probe_budget > bootstrap.recipe.nlist
                    {
                        return Err(ZeppelinError::Serialization(
                            "late candidate bootstrap metadata must be positive and internally \
                             bounded"
                                .to_string(),
                        ));
                    }
                    if bootstrap.row_count != segment.record_count
                        || bootstrap.recipe.fde_generation != segment.fde_generation
                        || bootstrap.recipe.fde_dimension != segment.fde_dimension
                    {
                        return Err(ZeppelinError::Serialization(
                            "late candidate bootstrap does not match its segment".to_string(),
                        ));
                    }
                    insert_unique_segment_key(
                        &mut artifact_keys,
                        bootstrap.key.as_str(),
                        "candidate bootstrap",
                    )?;

                    let mut cluster_rows = 0_u64;
                    let mut cluster_shards = BTreeSet::new();
                    let mut cluster_ids = BTreeSet::new();
                    for cluster in &candidate.clusters {
                        if cluster.size_bytes == 0
                            || cluster.format_version == 0
                            || cluster.row_count == 0
                            || cluster.cluster_id >= bootstrap.recipe.nlist
                        {
                            return Err(ZeppelinError::Serialization(
                                "late candidate cluster metadata is invalid".to_string(),
                            ));
                        }
                        if !cluster_shards.insert((cluster.cluster_id, cluster.shard_id)) {
                            return Err(ZeppelinError::Serialization(
                                "late candidate index contains a duplicate cluster shard"
                                    .to_string(),
                            ));
                        }
                        cluster_ids.insert(cluster.cluster_id);
                        cluster_rows = cluster_rows
                            .checked_add(u64::from(cluster.row_count))
                            .ok_or_else(|| {
                                ZeppelinError::Serialization(
                                    "late candidate cluster row count overflows".to_string(),
                                )
                            })?;
                        insert_unique_segment_key(
                            &mut artifact_keys,
                            cluster.key.as_str(),
                            "candidate cluster",
                        )?;
                    }
                    if cluster_rows != segment.record_count
                        || cluster_ids.len() != bootstrap.recipe.nlist as usize
                    {
                        return Err(ZeppelinError::Serialization(
                            "late candidate clusters do not cover the segment exactly".to_string(),
                        ));
                    }
                }
                LateCandidateKind::FlatSq8 => {
                    if segment.candidate_index.is_some() {
                        return Err(ZeppelinError::Serialization(
                            "late flat segment cannot carry an IVF candidate index".to_string(),
                        ));
                    }
                    let flat = segment.flat_candidate.as_ref().ok_or_else(|| {
                        ZeppelinError::Serialization(
                            "late flat segment requires a flat candidate artifact".to_string(),
                        )
                    })?;
                    if flat.size_bytes == 0
                        || flat.format_version == 0
                        || flat.row_count == 0
                        || flat.recipe.fde_dimension == 0
                        || flat.recipe.candidate_k == 0
                    {
                        return Err(ZeppelinError::Serialization(
                            "late flat candidate metadata must be positive".to_string(),
                        ));
                    }
                    if flat.row_count != segment.record_count
                        || flat.recipe.fde_generation != segment.fde_generation
                        || flat.recipe.fde_dimension != segment.fde_dimension
                    {
                        return Err(ZeppelinError::Serialization(
                            "late flat candidate does not match its segment".to_string(),
                        ));
                    }
                    insert_unique_segment_key(
                        &mut artifact_keys,
                        flat.key.as_str(),
                        "flat candidate artifact",
                    )?;
                }
            }

            let mut matrix_rows = 0_u64;
            let mut matrix_vectors = 0_u64;
            for matrix in &segment.matrix_objects {
                if matrix.size_bytes == 0
                    || matrix.format_version == 0
                    || matrix.row_count == 0
                    || matrix.total_vectors == 0
                    || matrix.vector_dimension == 0
                {
                    return Err(ZeppelinError::Serialization(
                        "late matrix block metadata must be positive".to_string(),
                    ));
                }
                if matrix.dtype != segment.matrix_dtype
                    || matrix.semantic_epoch != segment.semantic_epoch
                    || matrix.fde_generation != segment.fde_generation
                    || matrix.vector_dimension != segment.vector_dimension
                {
                    return Err(ZeppelinError::Serialization(
                        "late matrix block does not match its segment".to_string(),
                    ));
                }
                matrix_rows = matrix_rows
                    .checked_add(u64::from(matrix.row_count))
                    .ok_or_else(|| {
                        ZeppelinError::Serialization(
                            "late matrix block row count overflows".to_string(),
                        )
                    })?;
                matrix_vectors = matrix_vectors
                    .checked_add(matrix.total_vectors)
                    .ok_or_else(|| {
                        ZeppelinError::Serialization(
                            "late matrix block vector count overflows".to_string(),
                        )
                    })?;
                insert_unique_segment_key(&mut artifact_keys, matrix.key.as_str(), "matrix block")?;
            }
            if matrix_rows != segment.record_count || matrix_vectors != segment.total_vector_count {
                return Err(ZeppelinError::Serialization(
                    "late matrix blocks do not cover the segment exactly".to_string(),
                ));
            }

            let mut attribute_rows = 0_u64;
            for object in &segment.attribute_objects {
                if object.size_bytes == 0 || object.format_version == 0 || object.row_count == 0 {
                    return Err(ZeppelinError::Serialization(
                        "late attribute block metadata must be positive".to_string(),
                    ));
                }
                attribute_rows = attribute_rows
                    .checked_add(u64::from(object.row_count))
                    .ok_or_else(|| {
                        ZeppelinError::Serialization(
                            "late attribute block row count overflows".to_string(),
                        )
                    })?;
                insert_unique_segment_key(
                    &mut artifact_keys,
                    object.key.as_str(),
                    "attribute object",
                )?;
            }
            if attribute_rows != segment.record_count {
                return Err(ZeppelinError::Serialization(
                    "late attribute blocks do not cover the segment exactly".to_string(),
                ));
            }

            for object in &segment.fts_objects {
                if object.size_bytes == 0 || object.format_version == 0 {
                    return Err(ZeppelinError::Serialization(
                        "late segment FTS object size and format version must be positive"
                            .to_string(),
                    ));
                }
                insert_unique_segment_key(&mut artifact_keys, object.key.as_str(), "FTS object")?;
            }
        }

        if !self.late_interaction_segments.is_empty() && self.active_late_segment.is_none() {
            return Err(ZeppelinError::Serialization(
                "late-state segments require an active late segment".to_string(),
            ));
        }
        if let Some(active_id) = self.active_late_segment.as_deref() {
            if !segment_ids.contains(active_id) {
                return Err(ZeppelinError::Serialization(format!(
                    "active late segment {active_id} is not present in the section"
                )));
            }
        }
        Ok(())
    }

    fn validate_origin_indices(&self, require_canonical: bool) -> Result<()> {
        let mut previous = None;
        let mut unique = BTreeSet::new();
        for origin in &self.artifact_origins {
            if origin.incarnation.is_nil() {
                return Err(ZeppelinError::Serialization(
                    "late-state source origin has a nil namespace incarnation".to_string(),
                ));
            }
            if require_canonical && !unique.insert(origin) {
                return Err(ZeppelinError::Serialization(
                    "late-state source origin table contains a duplicate".to_string(),
                ));
            }
            if require_canonical && previous.is_some_and(|previous| previous > origin) {
                return Err(ZeppelinError::Serialization(
                    "late-state source origin table is not canonical".to_string(),
                ));
            }
            previous = Some(origin);
        }
        self.for_each_nested_artifact(|artifact| {
            if let Some(index) = artifact.artifact_origin {
                let index = usize::try_from(index.get()).map_err(|_| {
                    ZeppelinError::Serialization(format!(
                        "{} origin index does not fit this platform",
                        artifact.kind
                    ))
                })?;
                if index >= self.artifact_origins.len() {
                    return Err(ZeppelinError::Serialization(format!(
                        "{} origin index {index} is out of bounds for table length {}",
                        artifact.kind,
                        self.artifact_origins.len()
                    )));
                }
            }
            Ok(())
        })
    }

    /// Serialize and create the local immutable object, byte-verifying retries.
    pub(crate) async fn put_create(
        &self,
        store: &ZeppelinStore,
        namespace: &str,
    ) -> Result<ManifestSectionRef> {
        let bytes = self.to_bytes()?;
        let checksum = Self::checksum(&bytes);
        let key = Self::s3_key(namespace, &checksum);
        match store.put_create_outcome(&key, bytes.clone()).await? {
            CreateOnlyOutcome::Created { .. } => {}
            CreateOnlyOutcome::AlreadyExists => {
                let existing = store.get(&key).await?;
                if existing != bytes {
                    return Err(ZeppelinError::Serialization(format!(
                        "late-state content-address collision at {key}: existing bytes differ"
                    )));
                }
            }
        }
        let size_bytes = u64::try_from(bytes.len()).map_err(|_| {
            ZeppelinError::Serialization(
                "late-state section size does not fit persisted u64".to_string(),
            )
        })?;
        Ok(ManifestSectionRef {
            key,
            checksum,
            size_bytes,
            format_version: LATE_STATE_FORMAT_VERSION,
            artifact_origin: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use ulid::Ulid;

    use crate::embedding::{
        ArtifactChecksum, CenteringArtifact, ContentHash, EmbeddingProfileId, EmbeddingProfileRef,
        EncoderExecutionRef, ExactScorerVersion, FdeFragmentRef, FdeGenerationId, FdeRecipe,
        FdeTransformArtifactRef, InputModality, MatrixDtype, MeanVectorRef,
        MultiVectorEmbeddingFragmentRef, MultiVectorEpoch, MultiVectorEpochId, NormalizationRecipe,
        PhysicalInputFragmentIdentity, RecordVersionCoverage, RecordVersionRef, SemanticOverlayRef,
        VectorTransformRecipe,
    };
    use crate::index::late_interaction::{
        AttributeBlockRef, FdeAlgorithmVersion, FdeParams, FdeTransform, FinalProjection,
        InnerProjection, LateCandidateBootstrapRef, LateCandidateClusterRef, LateCandidateIndexRef,
        LateCandidateRecipe, LateFlatCandidateRecipe, LateFlatCandidateRef, LateRoutingMetric,
        MatrixBlockRef,
    };
    use crate::namespace::branching::{ArtifactOrigin, ArtifactOriginIndex};
    use crate::storage::{NamespaceObjectFamily, ZeppelinStore};

    use super::{
        LateCandidateKind, LateInteractionSegmentRef, LateSegmentObjectRef, LateStateSection,
        SourceInventoryRef, LATE_STATE_FORMAT_VERSION, LATE_STATE_MAGIC,
    };

    #[test]
    fn legacy_empty_sections_decode_with_current_defaults() {
        const EMPTY_V1_FIXTURE: &[u8] = b"ZLS1\x01\x90";
        const EMPTY_V2_FIXTURE: &[u8] = b"ZLS1\x02\x92\x90\x90";
        const EMPTY_V3_FIXTURE: &[u8] = b"ZLS1\x03\x95\x90\x90\xc0\x90\x90";
        const EMPTY_V4_FIXTURE: &[u8] = b"ZLS1\x04\x97\x90\x90\xc0\x90\x90\x90\xc0";
        for fixture in [
            EMPTY_V1_FIXTURE,
            EMPTY_V2_FIXTURE,
            EMPTY_V3_FIXTURE,
            EMPTY_V4_FIXTURE,
        ] {
            assert_eq!(
                LateStateSection::from_bytes(fixture).expect("fixture must decode"),
                LateStateSection::new()
            );
        }
    }

    #[test]
    fn v4_source_inventory_round_trips_with_section_local_origins() {
        let source = serde_json::from_value::<ArtifactOrigin>(serde_json::json!({
            "namespace": "source",
            "incarnation": "00000000-0000-0000-0000-000000000001"
        }))
        .expect("origin fixture");
        let mut section = LateStateSection {
            source_inventory: vec![SourceInventoryRef {
                key: SourceInventoryRef::s3_key("source", ContentHash::new([3; 32])),
                checksum: ArtifactChecksum::new([4; 32]),
                size_bytes: 12,
                media_type: "image/png".to_string(),
                artifact_origin: Some(ArtifactOriginIndex::new(0)),
            }],
            artifact_origins: vec![source.clone()],
            ..LateStateSection::new()
        };
        section
            .canonicalize_artifact_origins()
            .expect("section origins must canonicalize");

        let bytes = section.to_bytes().expect("v4 section must serialize");
        assert_eq!(&bytes[..4], LATE_STATE_MAGIC);
        assert_eq!(bytes[4], LATE_STATE_FORMAT_VERSION as u8);
        let decoded = LateStateSection::from_bytes(&bytes).expect("v4 fixture must decode");
        assert_eq!(decoded, section);
        assert_eq!(
            decoded
                .source_origin(&decoded.source_inventory[0], &source)
                .expect("nested origin must resolve"),
            &source
        );
    }

    fn profile_fixture() -> EmbeddingProfileRef {
        let mut epoch = MultiVectorEpoch {
            id: MultiVectorEpochId::new([0; 32]),
            encoder: EncoderExecutionRef {
                implementation: "roundtrip-worker".to_string(),
                version: "v1".to_string(),
                bundle_prefix: Some("models/roundtrip-worker/v1".to_string()),
                artifact_digests: BTreeMap::from([(
                    "model".to_string(),
                    ArtifactChecksum::new([1; 32]),
                )]),
                supported_modalities: vec![InputModality::Text],
            },
            preprocessing_digest: ArtifactChecksum::new([2; 32]),
            vector_dimension: 2,
            max_query_vectors: 4,
            max_document_vectors: 8,
            output_normalization: NormalizationRecipe::L2,
            exact_scoring_transform: VectorTransformRecipe::Identity,
            matrix_dtype: MatrixDtype::F16,
            exact_scorer: ExactScorerVersion::MaxSimV1,
        };
        epoch.id = epoch.canonical_id().unwrap();
        let params = FdeParams {
            algorithm: FdeAlgorithmVersion::PaperV1,
            repetitions: 1,
            simhash_bits: 1,
            input_dimension: 2,
            inner: InnerProjection::Rademacher { d_proj: 1 },
            final_projection: FinalProjection::None,
        };
        let transform = FdeTransform::generate(&params, 11).unwrap().to_bytes();
        let transform_checksum = ArtifactChecksum::digest(&transform);
        let mean = CenteringArtifact::new(vec![0.25, -0.5])
            .unwrap()
            .to_bytes()
            .unwrap();
        let mut fde = FdeRecipe {
            generation: FdeGenerationId::new([0; 32]),
            semantic_epoch: epoch.id,
            params,
            transform_artifact: FdeTransformArtifactRef {
                key: format!("catalog/late/transforms/{}", transform_checksum.to_hex()),
                checksum: transform_checksum,
                size_bytes: transform.len() as u64,
                format_version: 1,
                artifact_origin: None,
            },
            candidate_vector_transform: VectorTransformRecipe::SubtractMean {
                mean: MeanVectorRef {
                    key: format!("catalog/late/centering/{}", mean.checksum().to_hex()),
                    checksum: mean.checksum(),
                    size_bytes: mean.bytes().len() as u64,
                    vector_dimension: 2,
                    format_version: 1,
                    artifact_origin: None,
                },
                renormalize: false,
            },
            candidate_document_pooling: crate::embedding::CandidateDocumentPooling::Identity,
        };
        fde.generation = fde.canonical_generation().unwrap();
        EmbeddingProfileRef {
            profile: EmbeddingProfileId::new("roundtrip-v1"),
            epoch,
            fde,
            int8_qualification: None,
        }
    }

    fn segment_fixture(
        profile: &EmbeddingProfileRef,
        owner_namespace: &str,
    ) -> LateInteractionSegmentRef {
        let segment_prefix = format!("{owner_namespace}/late/segments/segment-v1");
        LateInteractionSegmentRef {
            id: "segment-v1".to_string(),
            profile: profile.profile.clone(),
            semantic_epoch: profile.epoch.id,
            fde_generation: profile.fde.generation,
            matrix_dtype: profile.epoch.matrix_dtype,
            record_count: 1,
            total_vector_count: 2,
            vector_dimension: profile.epoch.vector_dimension,
            fde_dimension: 2,
            coverage_sequence: 7,
            candidate_index: Some(LateCandidateIndexRef {
                bootstrap: LateCandidateBootstrapRef {
                    key: format!("{segment_prefix}/candidate-bootstrap.bin"),
                    checksum: ArtifactChecksum::new([10; 32]),
                    size_bytes: 128,
                    recipe: LateCandidateRecipe {
                        fde_generation: profile.fde.generation,
                        fde_dimension: 2,
                        nlist: 1,
                        probe_budget: 1,
                        candidate_k: 1,
                        routing_metric: LateRoutingMetric::NegativeL2,
                    },
                    row_count: 1,
                    format_version: 1,
                },
                clusters: vec![LateCandidateClusterRef {
                    key: format!("{segment_prefix}/candidate-cluster-0.bin"),
                    checksum: ArtifactChecksum::new([11; 32]),
                    size_bytes: 256,
                    cluster_id: 0,
                    shard_id: 0,
                    row_count: 1,
                    format_version: 1,
                }],
            }),
            matrix_objects: vec![MatrixBlockRef {
                key: format!("{segment_prefix}/matrix-0.bin"),
                checksum: ArtifactChecksum::new([12; 32]),
                size_bytes: 64,
                dtype: profile.epoch.matrix_dtype,
                semantic_epoch: profile.epoch.id,
                fde_generation: profile.fde.generation,
                vector_dimension: profile.epoch.vector_dimension,
                row_count: 1,
                total_vectors: 2,
                format_version: 1,
            }],
            attribute_objects: vec![AttributeBlockRef {
                key: format!("{segment_prefix}/attributes-0.bin"),
                checksum: ArtifactChecksum::new([13; 32]),
                size_bytes: 32,
                row_count: 1,
                format_version: 1,
            }],
            fts_objects: vec![LateSegmentObjectRef {
                key: format!("{segment_prefix}/fts-0.bin"),
                checksum: ArtifactChecksum::new([14; 32]),
                size_bytes: 48,
                format_version: 1,
            }],
            artifact_origin: None,
            candidate_kind: LateCandidateKind::Ivf,
            flat_candidate: None,
        }
    }

    fn flat_segment_fixture(
        profile: &EmbeddingProfileRef,
        owner_namespace: &str,
    ) -> LateInteractionSegmentRef {
        let mut segment = segment_fixture(profile, owner_namespace);
        let segment_prefix = format!("{owner_namespace}/late/segments/segment-v1");
        segment.candidate_index = None;
        segment.candidate_kind = LateCandidateKind::FlatSq8;
        segment.flat_candidate = Some(LateFlatCandidateRef {
            key: format!("{segment_prefix}/flat-sq8-0.bin"),
            checksum: ArtifactChecksum::new([15; 32]),
            size_bytes: 96,
            row_count: 1,
            recipe: LateFlatCandidateRecipe {
                fde_generation: profile.fde.generation,
                fde_dimension: 2,
                candidate_k: 1,
            },
            format_version: 1,
        });
        segment
    }

    #[test]
    fn v4_segment_round_trips_and_walks_every_owned_object() {
        let profile = profile_fixture();
        let foreign = serde_json::from_value::<ArtifactOrigin>(serde_json::json!({
            "namespace": "source",
            "incarnation": "00000000-0000-0000-0000-000000000002"
        }))
        .expect("foreign origin fixture");
        let mut segment = segment_fixture(&profile, "source");
        segment.artifact_origin = Some(ArtifactOriginIndex::new(0));
        let mut section = LateStateSection {
            active_profile: Some(profile),
            artifact_origins: vec![foreign.clone()],
            late_interaction_segments: vec![segment.clone()],
            active_late_segment: Some(segment.id.clone()),
            ..LateStateSection::new()
        };
        section
            .canonicalize_artifact_origins()
            .expect("segment origin must canonicalize");

        let bytes = section
            .to_bytes()
            .expect("v4 segment section must serialize");
        assert_eq!(bytes[4], LATE_STATE_FORMAT_VERSION as u8);
        let decoded = LateStateSection::from_bytes(&bytes).expect("v4 section must decode");
        assert_eq!(decoded, section);

        let local = serde_json::from_value::<ArtifactOrigin>(serde_json::json!({
            "namespace": "catalog",
            "incarnation": "00000000-0000-0000-0000-000000000001"
        }))
        .expect("origin fixture");
        decoded
            .validate_for_origin(&local)
            .expect("all segment objects must belong to the registered family");
        let resolved = decoded
            .resolved_artifacts(&local)
            .expect("segment ownership must resolve");
        let segment_objects = resolved
            .iter()
            .filter(|artifact| artifact.family == NamespaceObjectFamily::LateSegment)
            .collect::<Vec<_>>();
        assert_eq!(segment_objects.len(), 5);
        assert!(segment_objects
            .iter()
            .all(|artifact| artifact.origin == foreign));
    }

    #[test]
    fn v4_segment_validation_rejects_invalid_selection_and_duplicate_keys() {
        let profile = profile_fixture();
        let segment = segment_fixture(&profile, "catalog");
        let unselected = LateStateSection {
            active_profile: Some(profile.clone()),
            late_interaction_segments: vec![segment.clone()],
            ..LateStateSection::new()
        };
        assert!(unselected.to_bytes().is_err());

        let missing = LateStateSection {
            active_profile: Some(profile.clone()),
            late_interaction_segments: vec![segment.clone()],
            active_late_segment: Some("missing".to_string()),
            ..LateStateSection::new()
        };
        assert!(missing.to_bytes().is_err());

        let mut duplicate = segment;
        duplicate.attribute_objects[0].key = duplicate.matrix_objects[0].key.clone();
        let duplicate = LateStateSection {
            active_profile: Some(profile),
            late_interaction_segments: vec![duplicate],
            active_late_segment: Some("segment-v1".to_string()),
            ..LateStateSection::new()
        };
        assert!(duplicate.to_bytes().is_err());
    }

    #[test]
    fn v5_flat_segment_round_trips_and_enforces_kind_exclusivity() {
        let profile = profile_fixture();
        let segment = flat_segment_fixture(&profile, "catalog");
        let section = LateStateSection {
            active_profile: Some(profile.clone()),
            late_interaction_segments: vec![segment.clone()],
            active_late_segment: Some(segment.id.clone()),
            ..LateStateSection::new()
        };

        let bytes = section.to_bytes().expect("v5 flat section must serialize");
        assert_eq!(bytes[4], LATE_STATE_FORMAT_VERSION as u8);
        let decoded = LateStateSection::from_bytes(&bytes).expect("v5 flat section must decode");
        assert_eq!(decoded, section);
        let decoded_segment = &decoded.late_interaction_segments[0];
        assert_eq!(decoded_segment.candidate_kind, LateCandidateKind::FlatSq8);
        let flat = decoded_segment
            .flat_candidate_ref()
            .expect("flat segment must expose its flat candidate");
        assert_eq!(flat.recipe.candidate_k, 1);
        assert!(decoded_segment.ivf_candidate_index().is_err());

        let local = serde_json::from_value::<ArtifactOrigin>(serde_json::json!({
            "namespace": "catalog",
            "incarnation": "00000000-0000-0000-0000-000000000001"
        }))
        .expect("origin fixture");
        decoded
            .validate_for_origin(&local)
            .expect("flat segment objects must belong to the registered family");
        let resolved = decoded
            .resolved_artifacts(&local)
            .expect("flat segment ownership must resolve");
        assert!(resolved.iter().any(|artifact| artifact.family
            == NamespaceObjectFamily::LateSegment
            && artifact.key.contains("flat-sq8-")));

        let mut both = segment.clone();
        both.candidate_index = segment_fixture(&profile, "catalog").candidate_index;
        let invalid = LateStateSection {
            active_profile: Some(profile.clone()),
            late_interaction_segments: vec![both],
            active_late_segment: Some("segment-v1".to_string()),
            ..LateStateSection::new()
        };
        assert!(invalid.to_bytes().is_err());

        let mut missing = segment;
        missing.flat_candidate = None;
        let invalid = LateStateSection {
            active_profile: Some(profile),
            late_interaction_segments: vec![missing],
            active_late_segment: Some("segment-v1".to_string()),
            ..LateStateSection::new()
        };
        assert!(invalid.to_bytes().is_err());
    }

    #[test]
    fn v4_profile_and_overlay_round_trip() {
        let profile = profile_fixture();
        let input_id = Ulid::from(1_u128);
        let embedding_checksum = ArtifactChecksum::new([5; 32]);
        let section = LateStateSection {
            active_profile: Some(profile.clone()),
            semantic_overlays: vec![SemanticOverlayRef {
                source_fragment: PhysicalInputFragmentIdentity {
                    key: format!("catalog/input-wal/{input_id}.wal"),
                    id: input_id,
                    checksum: 7,
                    size_bytes: 16,
                    artifact_origin: None,
                },
                semantic_epoch: profile.epoch.id,
                fde_generation: profile.fde.generation,
                embeddings: MultiVectorEmbeddingFragmentRef {
                    key: format!(
                        "catalog/late/matrix-fragments/{}",
                        embedding_checksum.to_hex()
                    ),
                    checksum: embedding_checksum,
                    source_fragment_checksum: 7,
                    semantic_epoch: profile.epoch.id,
                    row_count: 1,
                    total_vectors: 2,
                    vector_dimension: 2,
                    dtype: MatrixDtype::F16,
                    format_version: 1,
                    size_bytes: 24,
                    artifact_origin: None,
                },
                fde_vectors: FdeFragmentRef {
                    key: format!(
                        "catalog/late/fde-fragments/{}",
                        ArtifactChecksum::new([6; 32]).to_hex()
                    ),
                    checksum: ArtifactChecksum::new([6; 32]),
                    embedding_fragment_checksum: embedding_checksum,
                    generation: profile.fde.generation,
                    row_count: 1,
                    fde_dimension: 2,
                    format_version: 1,
                    size_bytes: 24,
                    artifact_origin: None,
                },
                covered_versions: RecordVersionCoverage {
                    records: vec![RecordVersionRef {
                        row_ordinal: 0,
                        record_id: "row".to_string(),
                        content_hash: ContentHash::new([7; 32]),
                        sequence: 1,
                    }],
                },
                published_at_generation: 3,
            }],
            ..LateStateSection::new()
        };

        let bytes = section.to_bytes().expect("v4 section must serialize");
        assert_eq!(bytes[4], LATE_STATE_FORMAT_VERSION as u8);
        assert_eq!(
            LateStateSection::from_bytes(&bytes).expect("v4 section must decode"),
            section
        );
    }

    #[tokio::test]
    async fn profile_artifact_validation_decodes_transform_and_centering_formats() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let profile = profile_fixture();
        let transform = FdeTransform::generate(&profile.fde.params, 11)
            .unwrap()
            .to_bytes();
        store
            .put_create(&profile.fde.transform_artifact.key, transform)
            .await
            .unwrap();
        let mean = CenteringArtifact::new(vec![0.25, -0.5])
            .unwrap()
            .to_bytes()
            .unwrap();
        store
            .put_create(
                &profile.fde.candidate_vector_transform.mean().unwrap().key,
                mean.bytes().clone(),
            )
            .await
            .unwrap();

        LateStateSection::validate_local_profile_artifacts(&store, "catalog", &profile)
            .await
            .unwrap();
    }
}
