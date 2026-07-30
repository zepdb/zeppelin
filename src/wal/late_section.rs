//! Immutable content-addressed manifest late-state sections.
//!
//! The root manifest remains the only visibility point. A section object may
//! exist before publication, but readers discover it only through
//! [`ManifestSectionRef`]. Version 2 adds the durable source inventory while
//! retaining the empty version-1 decoder.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::embedding::{ArtifactChecksum, ContentHash};
use crate::error::{Result, ZeppelinError};
use crate::namespace::branching::{ArtifactOrigin, ArtifactOriginIndex};
use crate::storage::{CreateOnlyOutcome, NamespaceObjectFamily, NamespaceObjectKey, ZeppelinStore};

const LATE_STATE_MAGIC: &[u8; 4] = b"ZLS1";
const LATE_STATE_VERSION_V1: u8 = 1;
const LATE_STATE_VERSION_V2: u8 = 2;

/// Persisted section format version carried by root-manifest references.
pub const LATE_STATE_FORMAT_VERSION: u32 = 2;

/// Whether a root reference names a section version this binary can decode.
#[must_use]
pub const fn is_supported_late_state_format_version(version: u32) -> bool {
    version == 1 || version == LATE_STATE_FORMAT_VERSION
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

/// Version-2 late-interaction manifest state.
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
}

impl LateStateSection {
    /// Construct an empty version-2 section.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            source_inventory: Vec::new(),
            artifact_origins: Vec::new(),
        }
    }

    /// Serialize canonical version-2 bytes.
    pub fn to_bytes(&self) -> Result<Bytes> {
        self.validate_structural()?;
        let payload = rmp_serde::to_vec(self).map_err(|error| {
            ZeppelinError::Serialization(format!(
                "late-state section MessagePack serialize: {error}"
            ))
        })?;
        let mut bytes = Vec::with_capacity(LATE_STATE_MAGIC.len() + 1 + payload.len());
        bytes.extend_from_slice(LATE_STATE_MAGIC);
        bytes.push(LATE_STATE_VERSION_V2);
        bytes.extend_from_slice(&payload);
        Ok(Bytes::from(bytes))
    }

    /// Decode and validate version 1 or version 2 section bytes.
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
        if version != LATE_STATE_VERSION_V1 && version != LATE_STATE_VERSION_V2 {
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
            "{}state/{checksum_hex}",
            NamespaceObjectFamily::LateSection.namespace_prefix(namespace)
        )
    }

    /// Resolve one nested source's owner, inheriting the section owner for `None`.
    pub fn source_origin<'a>(
        &'a self,
        source: &SourceInventoryRef,
        section_origin: &'a ArtifactOrigin,
    ) -> Result<&'a ArtifactOrigin> {
        let Some(index) = source.artifact_origin else {
            return Ok(section_origin);
        };
        let index = usize::try_from(index.get()).map_err(|_| {
            ZeppelinError::Serialization(format!(
                "source inventory origin index {} does not fit this platform",
                index.get()
            ))
        })?;
        self.artifact_origins.get(index).ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "source inventory origin index {index} is out of bounds for table length {}",
                self.artifact_origins.len()
            ))
        })
    }

    /// Validate every source key against its resolved physical owner.
    pub fn validate_for_origin(&self, section_origin: &ArtifactOrigin) -> Result<()> {
        self.validate_structural()?;
        for source in &self.source_inventory {
            let origin = self.source_origin(source, section_origin)?;
            let owned =
                NamespaceObjectKey::classify(origin.namespace.as_str(), source.key.clone())?;
            if owned.family() != NamespaceObjectFamily::Source {
                return Err(ZeppelinError::Validation(format!(
                    "source inventory key is outside the registered source family: {}",
                    source.key
                )));
            }
        }
        Ok(())
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

    /// Preserve resolved source owners when republishing under a new owner.
    pub fn rebase_source_origins(
        &mut self,
        previous_section_origin: &ArtifactOrigin,
        next_section_origin: &ArtifactOrigin,
    ) -> Result<()> {
        self.validate_structural()?;
        let resolved = self
            .source_inventory
            .iter()
            .map(|source| self.source_origin(source, previous_section_origin).cloned())
            .collect::<Result<Vec<_>>>()?;
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
        for (source, origin) in self.source_inventory.iter_mut().zip(resolved) {
            source.artifact_origin = if origin == *next_section_origin {
                None
            } else {
                Some(*indices.get(&origin).ok_or_else(|| {
                    ZeppelinError::Serialization(
                        "rebased late-state origin table omitted source owner".to_string(),
                    )
                })?)
            };
        }
        self.artifact_origins = table;
        self.validate_structural()
    }

    /// Canonicalize the section-local origin table and remap nested indices.
    pub fn canonicalize_artifact_origins(&mut self) -> Result<()> {
        self.validate_origin_indices(false)?;
        let resolved = self
            .source_inventory
            .iter()
            .map(|source| {
                source
                    .artifact_origin
                    .map(|index| {
                        usize::try_from(index.get())
                            .ok()
                            .and_then(|index| self.artifact_origins.get(index))
                            .cloned()
                            .ok_or_else(|| {
                                ZeppelinError::Serialization(format!(
                                    "source inventory origin index {} is out of bounds",
                                    index.get()
                                ))
                            })
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
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
        for (source, origin) in self.source_inventory.iter_mut().zip(resolved) {
            source.artifact_origin = origin
                .as_ref()
                .map(|origin| {
                    indices.get(origin).copied().ok_or_else(|| {
                        ZeppelinError::Serialization(
                            "canonical late-state origin table omitted source owner".to_string(),
                        )
                    })
                })
                .transpose()?;
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
        for source in &self.source_inventory {
            if let Some(index) = source.artifact_origin {
                let index = usize::try_from(index.get()).map_err(|_| {
                    ZeppelinError::Serialization(
                        "late-state source origin index does not fit this platform".to_string(),
                    )
                })?;
                if index >= self.artifact_origins.len() {
                    return Err(ZeppelinError::Serialization(format!(
                        "late-state source origin index {index} is out of bounds for table length {}",
                        self.artifact_origins.len()
                    )));
                }
            }
        }
        Ok(())
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
    use crate::embedding::{ArtifactChecksum, ContentHash};
    use crate::namespace::branching::{ArtifactOrigin, ArtifactOriginIndex};

    use super::{
        LateStateSection, SourceInventoryRef, LATE_STATE_FORMAT_VERSION, LATE_STATE_MAGIC,
    };

    #[test]
    fn empty_v1_section_decodes_with_v2_defaults() {
        const EMPTY_V1_FIXTURE: &[u8] = b"ZLS1\x01\x90";
        assert_eq!(
            LateStateSection::from_bytes(EMPTY_V1_FIXTURE).expect("fixture must decode"),
            LateStateSection::new()
        );
    }

    #[test]
    fn v2_source_inventory_round_trips_with_section_local_origins() {
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
        };
        section
            .canonicalize_artifact_origins()
            .expect("section origins must canonicalize");

        let bytes = section.to_bytes().expect("v2 section must serialize");
        assert_eq!(&bytes[..4], LATE_STATE_MAGIC);
        assert_eq!(bytes[4], LATE_STATE_FORMAT_VERSION as u8);
        let decoded = LateStateSection::from_bytes(&bytes).expect("v2 fixture must decode");
        assert_eq!(decoded, section);
        assert_eq!(
            decoded
                .source_origin(&decoded.source_inventory[0], &source)
                .expect("nested origin must resolve"),
            &source
        );
    }
}
