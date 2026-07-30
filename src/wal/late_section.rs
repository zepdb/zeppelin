//! Immutable content-addressed manifest late-state sections.
//!
//! The root manifest remains the only visibility point. A section object may
//! exist before publication, but readers discover it only through
//! [`ManifestSectionRef`]. Version 1 intentionally contains no domain fields.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{Result, ZeppelinError};
use crate::namespace::branching::ArtifactOriginIndex;
use crate::storage::{CreateOnlyOutcome, NamespaceObjectFamily, ZeppelinStore};

const LATE_STATE_MAGIC: &[u8; 4] = b"ZLS1";
const LATE_STATE_VERSION_BYTE: u8 = 1;

/// Persisted section format version carried by root-manifest references.
pub const LATE_STATE_FORMAT_VERSION: u32 = 1;

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

/// Version-1 late-interaction manifest state.
///
/// Version 1 is deliberately empty. Later MMLI phases append domain fields
/// with serde defaults and advance the section-internal format version.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct LateStateSection {}

impl LateStateSection {
    /// Construct the empty version-1 section.
    #[must_use]
    pub const fn new() -> Self {
        Self {}
    }

    /// Serialize canonical version-1 bytes.
    pub fn to_bytes(&self) -> Result<Bytes> {
        let payload = rmp_serde::to_vec(self).map_err(|error| {
            ZeppelinError::Serialization(format!(
                "late-state section MessagePack serialize: {error}"
            ))
        })?;
        let mut bytes = Vec::with_capacity(LATE_STATE_MAGIC.len() + 1 + payload.len());
        bytes.extend_from_slice(LATE_STATE_MAGIC);
        bytes.push(LATE_STATE_VERSION_BYTE);
        bytes.extend_from_slice(&payload);
        Ok(Bytes::from(bytes))
    }

    /// Decode and validate magic plus section-internal version.
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
        if version != LATE_STATE_VERSION_BYTE {
            return Err(ZeppelinError::Serialization(format!(
                "unsupported late-state section version {version}"
            )));
        }
        rmp_serde::from_slice(&data[LATE_STATE_MAGIC.len() + 1..]).map_err(|error| {
            ZeppelinError::Serialization(format!(
                "late-state section MessagePack deserialize: {error}"
            ))
        })
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
    use super::{LateStateSection, LATE_STATE_FORMAT_VERSION, LATE_STATE_MAGIC};

    #[test]
    fn empty_v1_section_round_trips_with_frozen_header() {
        let section = LateStateSection::new();
        let bytes = section.to_bytes().expect("empty section must serialize");
        const EMPTY_V1_FIXTURE: &[u8] = b"ZLS1\x01\x90";
        assert_eq!(bytes.as_ref(), EMPTY_V1_FIXTURE);
        assert_eq!(&bytes[..4], LATE_STATE_MAGIC);
        assert_eq!(bytes[4], LATE_STATE_FORMAT_VERSION as u8);
        assert_eq!(
            LateStateSection::from_bytes(&bytes).expect("fixture must decode"),
            section
        );
        assert_eq!(
            hex(&LateStateSection::checksum(&bytes)),
            "1054eb895c7325e5888e21caed636a14f59e84b1a1f966d2269cbaac7a2e3fb3"
        );
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}
