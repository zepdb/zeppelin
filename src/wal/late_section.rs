//! Immutable content-addressed manifest late-state sections.
//!
//! The root manifest remains the only visibility point. A section object may
//! exist before publication, but readers discover it only through
//! [`ManifestSectionRef`]. Version 3 adds active profile and semantic-overlay
//! state while retaining version-1 and version-2 decoders.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::embedding::{
    ArtifactChecksum, CenteringArtifact, ContentHash, EmbeddingProfileRef, FdeGenerationId,
    MultiVectorEpochId, PhysicalInputFragmentIdentity, RecordVersionCoverage, SemanticOverlayRef,
};
use crate::error::{Result, ZeppelinError};
use crate::index::late_interaction::FdeTransform;
use crate::namespace::branching::{ArtifactOrigin, ArtifactOriginIndex};
use crate::storage::{CreateOnlyOutcome, NamespaceObjectFamily, NamespaceObjectKey, ZeppelinStore};

const LATE_STATE_MAGIC: &[u8; 4] = b"ZLS1";
const LATE_STATE_VERSION_V1: u8 = 1;
const LATE_STATE_VERSION_V2: u8 = 2;
const LATE_STATE_VERSION_V3: u8 = 3;

/// Persisted section format version carried by root-manifest references.
pub const LATE_STATE_FORMAT_VERSION: u32 = 3;

/// Whether a root reference names a section version this binary can decode.
#[must_use]
pub const fn is_supported_late_state_format_version(version: u32) -> bool {
    version == 1 || version == 2 || version == LATE_STATE_FORMAT_VERSION
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

/// Version-3 late-interaction manifest state.
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
}

impl LateStateSection {
    /// Construct an empty version-3 section.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            source_inventory: Vec::new(),
            artifact_origins: Vec::new(),
            active_profile: None,
            semantic_overlays: Vec::new(),
            quarantine_evidence: Vec::new(),
        }
    }

    /// Serialize canonical version-3 bytes.
    pub fn to_bytes(&self) -> Result<Bytes> {
        self.validate_structural()?;
        let payload = rmp_serde::to_vec(self).map_err(|error| {
            ZeppelinError::Serialization(format!(
                "late-state section MessagePack serialize: {error}"
            ))
        })?;
        let mut bytes = Vec::with_capacity(LATE_STATE_MAGIC.len() + 1 + payload.len());
        bytes.extend_from_slice(LATE_STATE_MAGIC);
        bytes.push(LATE_STATE_VERSION_V3);
        bytes.extend_from_slice(&payload);
        Ok(Bytes::from(bytes))
    }

    /// Decode and validate version 1, version 2, or version 3 section bytes.
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
                let expected =
                    Self::artifact_s3_key(origin.namespace.as_str(), artifact.family, checksum);
                if artifact.key != expected {
                    return Err(ZeppelinError::Validation(format!(
                        "{} key must equal its content-addressed key {expected}",
                        artifact.kind
                    )));
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
        } else if !self.semantic_overlays.is_empty() || !self.quarantine_evidence.is_empty() {
            return Err(ZeppelinError::Serialization(
                "late-state semantic state requires an active profile".to_string(),
            ));
        }
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
        FdeAlgorithmVersion, FdeParams, FdeTransform, FinalProjection, InnerProjection,
    };
    use crate::namespace::branching::{ArtifactOrigin, ArtifactOriginIndex};
    use crate::storage::ZeppelinStore;

    use super::{
        LateStateSection, SourceInventoryRef, LATE_STATE_FORMAT_VERSION, LATE_STATE_MAGIC,
    };

    #[test]
    fn empty_v1_section_decodes_with_v2_defaults() {
        const EMPTY_V1_FIXTURE: &[u8] = b"ZLS1\x01\x90";
        const EMPTY_V2_FIXTURE: &[u8] = b"ZLS1\x02\x92\x90\x90";
        assert_eq!(
            LateStateSection::from_bytes(EMPTY_V1_FIXTURE).expect("fixture must decode"),
            LateStateSection::new()
        );
        assert_eq!(
            LateStateSection::from_bytes(EMPTY_V2_FIXTURE).expect("fixture must decode"),
            LateStateSection::new()
        );
    }

    #[test]
    fn v3_source_inventory_round_trips_with_section_local_origins() {
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

        let bytes = section.to_bytes().expect("v3 section must serialize");
        assert_eq!(&bytes[..4], LATE_STATE_MAGIC);
        assert_eq!(bytes[4], LATE_STATE_FORMAT_VERSION as u8);
        let decoded = LateStateSection::from_bytes(&bytes).expect("v3 fixture must decode");
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

    #[test]
    fn v3_profile_and_overlay_round_trip() {
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

        let bytes = section.to_bytes().expect("v3 section must serialize");
        assert_eq!(bytes[4], LATE_STATE_FORMAT_VERSION as u8);
        assert_eq!(
            LateStateSection::from_bytes(&bytes).expect("v3 section must decode"),
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
