//! Durable typed inputs for late-interaction namespaces.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{Result, ZeppelinError};
use crate::types::{AttributeValue, VectorId};

/// SHA-256 identity of one canonical typed encoder input.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct ContentHash([u8; 32]);

impl ContentHash {
    /// Wrap a previously verified SHA-256 digest.
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Return the underlying digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Render the digest as lowercase hexadecimal for content-addressed keys.
    #[must_use]
    pub fn to_hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

/// SHA-256 integrity digest for one immutable source artifact.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct ArtifactChecksum([u8; 32]);

impl ArtifactChecksum {
    /// Wrap a previously verified SHA-256 digest.
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Return the underlying digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Compute SHA-256 over immutable artifact bytes.
    #[must_use]
    pub fn digest(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }
}

/// Input modality accepted by a late-interaction namespace.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum InputModality {
    /// Inline UTF-8 text.
    Text,
    /// Encoded image bytes.
    Image,
    /// One encoded image paired with inline UTF-8 text.
    ImageText,
}

impl InputModality {
    /// Stable API spelling used in typed admission errors.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Image => "image",
            Self::ImageText => "image_text",
        }
    }
}

/// Immutable schema declaration for one late-interaction namespace.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LateInteractionNamespaceConfig {
    /// Modalities admitted by retrieval-unit writes.
    pub accepted_modalities: Vec<InputModality>,
}

/// Text content retained directly in an input WAL fragment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TextContentRef {
    /// Inline UTF-8 source text.
    Inline(String),
}

/// Owned immutable image source selected through the source inventory.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImageObjectRef {
    /// Exact Zeppelin-owned source object key.
    pub key: String,
    /// SHA-256 over the encoded image bytes.
    pub checksum: ArtifactChecksum,
    /// Declared allowlisted image media type.
    pub media_type: String,
    /// Exact encoded byte length.
    pub encoded_size_bytes: u64,
    /// Declared image width in pixels.
    pub width: u32,
    /// Declared image height in pixels.
    pub height: u32,
}

/// Typed encoder input retained independently from derived embeddings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EncoderInputRef {
    /// Inline text input.
    Text {
        /// Retained text content.
        content: TextContentRef,
    },
    /// Image-only input.
    Image {
        /// Retained image object.
        image: ImageObjectRef,
    },
    /// Image paired with inline text.
    ImageText {
        /// Retained image object.
        image: ImageObjectRef,
        /// Retained text content.
        text: TextContentRef,
    },
}

impl EncoderInputRef {
    /// Return this input's admission modality.
    #[must_use]
    pub const fn modality(&self) -> InputModality {
        match self {
            Self::Text { .. } => InputModality::Text,
            Self::Image { .. } => InputModality::Image,
            Self::ImageText { .. } => InputModality::ImageText,
        }
    }

    /// Compute the source bytes referenced by this typed input.
    pub fn referenced_content_bytes(&self) -> Result<u64> {
        match self {
            Self::Text { content } => text_bytes(content),
            Self::Image { image } => Ok(image.encoded_size_bytes),
            Self::ImageText { image, text } => image
                .encoded_size_bytes
                .checked_add(text_bytes(text)?)
                .ok_or_else(|| {
                    ZeppelinError::Validation(
                        "image-text referenced byte count exceeds u64".to_string(),
                    )
                }),
        }
    }

    /// Compute SHA-256 over the canonical typed input.
    pub fn content_hash(&self) -> Result<ContentHash> {
        #[derive(Serialize)]
        #[serde(tag = "type", rename_all = "snake_case")]
        enum CanonicalInput<'a> {
            Text {
                text: &'a str,
            },
            Image {
                checksum: &'a ArtifactChecksum,
                media_type: &'a str,
                encoded_size_bytes: u64,
                width: u32,
                height: u32,
            },
            ImageText {
                checksum: &'a ArtifactChecksum,
                media_type: &'a str,
                encoded_size_bytes: u64,
                width: u32,
                height: u32,
                text: &'a str,
            },
        }

        let canonical = match self {
            Self::Text {
                content: TextContentRef::Inline(text),
            } => CanonicalInput::Text { text },
            Self::Image { image } => CanonicalInput::Image {
                checksum: &image.checksum,
                media_type: &image.media_type,
                encoded_size_bytes: image.encoded_size_bytes,
                width: image.width,
                height: image.height,
            },
            Self::ImageText {
                image,
                text: TextContentRef::Inline(text),
            } => CanonicalInput::ImageText {
                checksum: &image.checksum,
                media_type: &image.media_type,
                encoded_size_bytes: image.encoded_size_bytes,
                width: image.width,
                height: image.height,
                text,
            },
        };
        let bytes = serde_json::to_vec(&canonical).map_err(|error| {
            ZeppelinError::Serialization(format!(
                "canonical encoder input serialization failed: {error}"
            ))
        })?;
        Ok(ContentHash::new(Sha256::digest(bytes).into()))
    }
}

fn text_bytes(text: &TextContentRef) -> Result<u64> {
    match text {
        TextContentRef::Inline(text) => u64::try_from(text.len()).map_err(|_| {
            ZeppelinError::Validation("inline text byte count exceeds u64".to_string())
        }),
    }
}

/// One durable retrieval unit accepted by a late-interaction namespace.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetrievalUnitRecord {
    /// Caller-chosen record identity.
    pub id: VectorId,
    /// Typed source input.
    pub input: EncoderInputRef,
    /// SHA-256 over the canonical typed source input.
    pub content_hash: ContentHash,
    /// Optional caller-provided parent identity.
    pub parent_id: Option<String>,
    /// Optional ordinal within the parent.
    pub unit_ordinal: Option<u32>,
    /// Optional attributes used by filters, projection, and lexical search.
    pub attributes: Option<HashMap<String, AttributeValue>>,
}

/// Per-modality counts persisted on an input-fragment reference.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModalityCounts {
    /// Text-only upserts.
    pub text: usize,
    /// Image-only upserts.
    pub image: usize,
    /// Image-and-text upserts.
    pub image_text: usize,
}

impl ModalityCounts {
    /// Count modalities in an ordered retrieval-unit batch.
    #[must_use]
    pub fn from_records(records: &[RetrievalUnitRecord]) -> Self {
        let mut counts = Self::default();
        for record in records {
            match record.input.modality() {
                InputModality::Text => counts.text += 1,
                InputModality::Image => counts.image += 1,
                InputModality::ImageText => counts.image_text += 1,
            }
        }
        counts
    }
}

/// Stable registered semantic profile identity.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct EmbeddingProfileId(String);

impl EmbeddingProfileId {
    /// Wrap an already validated profile identifier.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrow the identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Stable identity of one multi-vector semantic epoch.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct MultiVectorEpochId([u8; 32]);

impl MultiVectorEpochId {
    /// Wrap an already verified epoch digest.
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

/// Stable identity of one fixed-dimensional encoding generation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct FdeGenerationId([u8; 32]);

impl FdeGenerationId {
    /// Wrap an already verified generation digest.
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

/// Aggregate state of contiguous semantic coverage.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SemanticState {
    /// Raw input is awaiting applicable derived artifacts.
    Pending,
    /// Every mutation through the contiguous sequence is covered.
    Ready,
    /// At least one poison or integrity failure blocks coverage.
    Failed,
}

/// Manifest-owned contiguous semantic coverage for one active recipe.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SemanticCoverageState {
    /// Selected embedding profile.
    pub profile: EmbeddingProfileId,
    /// Selected multi-vector epoch.
    pub epoch: MultiVectorEpochId,
    /// Selected fixed-dimensional encoding generation.
    pub fde_generation: FdeGenerationId,
    /// Highest sequence with no unresolved lower hole.
    pub contiguous_sequence: u64,
    /// Visible records still awaiting applicable output.
    pub pending_record_count: u64,
    /// Referenced input bytes still awaiting applicable output.
    pub pending_bytes: u64,
    /// Visible records whose deterministic failure blocks coverage.
    pub failed_record_count: u64,
    /// Aggregate coverage state.
    pub state: SemanticState,
}

#[cfg(test)]
mod tests {
    use super::{ArtifactChecksum, EncoderInputRef, ImageObjectRef, TextContentRef};

    fn image() -> ImageObjectRef {
        ImageObjectRef {
            key: "catalog/sources/fixture".to_string(),
            checksum: ArtifactChecksum::new([7; 32]),
            media_type: "image/png".to_string(),
            encoded_size_bytes: 123,
            width: 10,
            height: 20,
        }
    }

    #[test]
    fn canonical_input_hash_is_stable_and_domain_separated() {
        let first = EncoderInputRef::Text {
            content: TextContentRef::Inline("hello".to_string()),
        };
        let second = first.clone();
        let image_text = EncoderInputRef::ImageText {
            image: image(),
            text: TextContentRef::Inline("hello".to_string()),
        };

        assert_eq!(
            first.content_hash().unwrap(),
            second.content_hash().unwrap()
        );
        assert_ne!(
            first.content_hash().unwrap(),
            image_text.content_hash().unwrap()
        );
    }
}
