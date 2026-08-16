//! Durable typed inputs for late-interaction namespaces.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::error::{Result, ZeppelinError};
use crate::index::late_interaction::{FdeParams, FDE_TRANSFORM_FORMAT_VERSION};
use crate::namespace::branching::ArtifactOriginIndex;
use crate::types::{AttributeValue, VectorId};

use super::artifact::CENTERING_ARTIFACT_FORMAT_VERSION;

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

    /// Render the digest as lowercase hexadecimal for content-addressed keys.
    #[must_use]
    pub fn to_hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

/// Input modality accepted by a late-interaction namespace.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

    /// Return the underlying digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Render the digest as lowercase hexadecimal.
    #[must_use]
    pub fn to_hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
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

    /// Return the underlying digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Render the digest as lowercase hexadecimal.
    #[must_use]
    pub fn to_hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

/// Immutable encoder implementation and artifact identity.
///
/// Artifact names are canonical map keys rather than an ordered list so
/// registration order cannot change the semantic epoch ID.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncoderExecutionRef {
    /// Stable adapter implementation name.
    pub implementation: String,
    /// Pinned adapter/runtime version.
    pub version: String,
    /// Canonical S3 prefix for this immutable production model bundle.
    ///
    /// The deterministic development encoder has no bundle. Every other
    /// implementation must bind one non-empty, relative prefix into the epoch
    /// identity so changing mutable process configuration cannot change model
    /// bytes beneath an existing epoch.
    #[serde(default)]
    pub bundle_prefix: Option<String>,
    /// SHA-256 of every model, tokenizer, processor, and adapter artifact.
    pub artifact_digests: BTreeMap<String, ArtifactChecksum>,
    /// Input modalities this exact immutable encoder bundle accepts.
    pub supported_modalities: Vec<InputModality>,
}

/// Row normalization performed by the registered encoder.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NormalizationRecipe {
    /// Preserve finite encoder output without normalization.
    Identity,
    /// L2-normalize every retained row.
    L2,
}

/// Persisted scalar representation for exact multi-vector matrices.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MatrixDtype {
    /// IEEE-754 binary16 values. Artifact byte order is little-endian.
    #[default]
    F16,
    /// Symmetric groupwise INT8 with folded row normalization.
    Int8SymV1 {
        /// Number of coordinates sharing one stored f16 scale.
        group_size: u16,
    },
}

impl MatrixDtype {
    /// Validate the canonical `int8_sym_v1` shape contract.
    pub fn validate_for_dimension(self, vector_dimension: u32) -> Result<()> {
        match self {
            Self::F16 => Ok(()),
            Self::Int8SymV1 { group_size } => {
                if !matches!(group_size, 16 | 32 | 128) {
                    return Err(ZeppelinError::Validation(format!(
                        "int8_sym_v1 group size must be one of 16, 32, or 128, got {group_size}"
                    )));
                }
                if vector_dimension != 128 {
                    return Err(ZeppelinError::Validation(format!(
                        "int8_sym_v1 requires vector dimension 128, got {vector_dimension}"
                    )));
                }
                if vector_dimension % u32::from(group_size) != 0 {
                    return Err(ZeppelinError::Validation(format!(
                        "int8_sym_v1 group size {group_size} does not divide vector dimension {vector_dimension}"
                    )));
                }
                Ok(())
            }
        }
    }
}

/// Current profile-carried INT8 qualification evidence schema.
pub const INT8_QUALIFICATION_STAMP_VERSION: u32 = 1;

/// Exact INT8 qualification evidence approved for this release.
///
/// One tuple per operator-signed lane. The sole approved tuple binds the
/// TEXT-lane replay epoch at `int8_sym_v1 { group_size: 32 }`. On 2026-07-31,
/// exhaustive production writer/decoder replay over 5,183 text documents and
/// 1,109 queries reached 99.909829% same-top-1 agreement with f16, clearing the
/// ≥99.5% production bar; the visual lane reached 99.249531% and remains held.
/// The tuple carries the SHA-256 of that durable measurement.
const APPROVED_INT8_QUALIFICATIONS: &[(MultiVectorEpochId, MatrixDtype, ArtifactChecksum)] = &[(
    // Text replay epoch 04643f3cac3e8a07eab78b5b8496c79701613e0dcb3f3ad9470c97b8cbf08749
    MultiVectorEpochId::new([
        4, 100, 63, 60, 172, 62, 138, 7, 234, 183, 139, 91, 132, 150, 199, 151, 1, 97, 62, 13, 203,
        63, 58, 217, 71, 12, 151, 184, 203, 240, 135, 73,
    ]),
    MatrixDtype::Int8SymV1 { group_size: 32 },
    // sha256(int8-production-qualification.json) =
    // e91ef65c9c26a772a7a98e05985ceb7f310a094541d853559fc3aaee0a88794b
    ArtifactChecksum::new([
        233, 30, 246, 92, 156, 38, 167, 114, 167, 169, 142, 5, 152, 92, 235, 127, 49, 10, 9, 69,
        65, 216, 83, 85, 159, 195, 170, 238, 10, 136, 121, 75,
    ]),
)];

/// Operator-minted evidence binding one qualified INT8 profile epoch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Int8QualificationStamp {
    /// Qualified semantic epoch.
    pub semantic_epoch: MultiVectorEpochId,
    /// Exact qualified matrix dtype and group size.
    pub dtype: MatrixDtype,
    /// Digest of the durable production-writer/decoder evidence.
    pub evidence_digest: ArtifactChecksum,
    /// Version of the qualification evidence schema.
    pub evidence_version: u32,
}

impl Int8QualificationStamp {
    fn validate_for(&self, epoch: &MultiVectorEpoch) -> Result<()> {
        if self.evidence_version != INT8_QUALIFICATION_STAMP_VERSION {
            return Err(ZeppelinError::Validation(format!(
                "unsupported int8 qualification evidence version {}",
                self.evidence_version
            )));
        }
        if self
            .evidence_digest
            .as_bytes()
            .iter()
            .all(|byte| *byte == 0)
        {
            return Err(ZeppelinError::Validation(
                "int8 qualification evidence digest must be non-zero".to_string(),
            ));
        }
        if self.semantic_epoch != epoch.id || self.dtype != epoch.matrix_dtype {
            return Err(ZeppelinError::Validation(
                "int8 qualification stamp does not match the profile epoch and dtype".to_string(),
            ));
        }
        if !matches!(self.dtype, MatrixDtype::Int8SymV1 { .. }) {
            return Err(ZeppelinError::Validation(
                "int8 qualification stamp must bind int8_sym_v1".to_string(),
            ));
        }
        if !APPROVED_INT8_QUALIFICATIONS
            .iter()
            .any(|&(semantic_epoch, dtype, evidence_digest)| {
                semantic_epoch == self.semantic_epoch
                    && dtype == self.dtype
                    && evidence_digest == self.evidence_digest
            })
        {
            return Err(ZeppelinError::Validation(
                "int8 qualification evidence is not operator-approved for this release".to_string(),
            ));
        }
        Ok(())
    }
}

/// Versioned exact set-similarity semantics.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExactScorerVersion {
    /// Sum, over query rows, of the maximum document-row dot product.
    MaxSimV1,
}

/// Immutable mean vector used by candidate-only centering.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MeanVectorRef {
    /// Exact namespace-owned content-addressed key.
    pub key: String,
    /// SHA-256 over the complete immutable mean bytes.
    pub checksum: ArtifactChecksum,
    /// Exact artifact size.
    pub size_bytes: u64,
    /// Number of f32 coordinates in the mean.
    pub vector_dimension: u32,
    /// Artifact format version.
    pub format_version: u32,
    /// Section-local physical-origin index, or the section owner when absent.
    #[serde(default)]
    pub artifact_origin: Option<ArtifactOriginIndex>,
}

/// Typed transform applied to each multi-vector row.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VectorTransformRecipe {
    /// Preserve the encoder-produced row.
    Identity,
    /// Subtract one frozen global mean, optionally normalizing afterward.
    SubtractMean {
        /// Immutable global mean.
        mean: MeanVectorRef,
        /// Whether to L2-normalize the centered row.
        renormalize: bool,
    },
}

impl VectorTransformRecipe {
    /// Return the referenced mean artifact, if this transform uses one.
    #[must_use]
    pub const fn mean(&self) -> Option<&MeanVectorRef> {
        match self {
            Self::Identity => None,
            Self::SubtractMean { mean, .. } => Some(mean),
        }
    }

    pub(crate) fn mean_mut(&mut self) -> Option<&mut MeanVectorRef> {
        match self {
            Self::Identity => None,
            Self::SubtractMean { mean, .. } => Some(mean),
        }
    }
}

/// Candidate-only document-row pooling selected by the measured FDE recipe.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CandidateDocumentPooling {
    /// Preserve every candidate document row (the approved 1x recipe).
    Identity,
    /// Average each contiguous group without renormalizing the pooled row.
    ContiguousMean {
        /// Number of contiguous rows in each group.
        factor: u8,
    },
}

impl CandidateDocumentPooling {
    /// Validate this recipe against the candidate pooling cells approved by the lab.
    pub fn validate(self) -> Result<()> {
        match self {
            Self::Identity | Self::ContiguousMean { factor: 2 } => Ok(()),
            Self::ContiguousMean { factor } => Err(ZeppelinError::Validation(format!(
                "candidate document pooling supports only 1x identity or approved 2x contiguous mean, got factor {factor}"
            ))),
        }
    }
}

/// One complete, immutable semantic embedding epoch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MultiVectorEpoch {
    /// SHA-256 over the canonical recipe fields below.
    pub id: MultiVectorEpochId,
    /// Pinned encoder adapter and complete artifact digest set.
    pub encoder: EncoderExecutionRef,
    /// Digest of tokenizer/processor/row-selection preprocessing.
    pub preprocessing_digest: ArtifactChecksum,
    /// Number of coordinates in every retained row.
    pub vector_dimension: u32,
    /// Maximum query rows admitted from this encoder.
    pub max_query_vectors: u32,
    /// Maximum document rows admitted from this encoder.
    pub max_document_vectors: u32,
    /// Encoder output normalization.
    pub output_normalization: NormalizationRecipe,
    /// Exact-scoring row transform.
    pub exact_scoring_transform: VectorTransformRecipe,
    /// Exact persisted matrix scalar representation selected by the profile.
    #[serde(default)]
    pub matrix_dtype: MatrixDtype,
    /// Versioned exact scorer.
    pub exact_scorer: ExactScorerVersion,
}

impl MultiVectorEpoch {
    /// Derive the canonical semantic epoch ID, excluding the stored ID itself.
    pub fn canonical_id(&self) -> Result<MultiVectorEpochId> {
        #[derive(Serialize)]
        struct CanonicalEpoch<'a> {
            encoder: CanonicalEncoder<'a>,
            preprocessing_digest: ArtifactChecksum,
            vector_dimension: u32,
            max_query_vectors: u32,
            max_document_vectors: u32,
            output_normalization: NormalizationRecipe,
            exact_scoring_transform: VectorTransformRecipe,
            matrix_dtype: MatrixDtype,
            exact_scorer: ExactScorerVersion,
        }

        #[derive(Serialize)]
        struct CanonicalEncoder<'a> {
            implementation: &'a str,
            version: &'a str,
            bundle_prefix: Option<&'a str>,
            artifact_digests: &'a BTreeMap<String, ArtifactChecksum>,
            supported_modalities: Vec<InputModality>,
        }

        let mut supported_modalities = self.encoder.supported_modalities.clone();
        supported_modalities.sort_unstable();
        supported_modalities.dedup();
        let mut exact_scoring_transform = self.exact_scoring_transform.clone();
        if let Some(mean) = exact_scoring_transform.mean_mut() {
            // Epoch identity is location-independent: the mean participates
            // by content checksum and shape, never by where it is stored, so
            // branch materialization can rehome the artifact without minting
            // a new epoch.
            mean.artifact_origin = None;
            mean.key = String::new();
        }
        let canonical = CanonicalEpoch {
            encoder: CanonicalEncoder {
                implementation: &self.encoder.implementation,
                version: &self.encoder.version,
                bundle_prefix: self.encoder.bundle_prefix.as_deref(),
                artifact_digests: &self.encoder.artifact_digests,
                supported_modalities,
            },
            preprocessing_digest: self.preprocessing_digest,
            vector_dimension: self.vector_dimension,
            max_query_vectors: self.max_query_vectors,
            max_document_vectors: self.max_document_vectors,
            output_normalization: self.output_normalization,
            exact_scoring_transform,
            matrix_dtype: self.matrix_dtype,
            exact_scorer: self.exact_scorer,
        };
        canonical_sha256(b"zeppelin-multi-vector-epoch-v1", &canonical).map(MultiVectorEpochId::new)
    }

    /// Validate the stored canonical ID and load-bearing shape invariants.
    pub fn validate(&self) -> Result<()> {
        if self.encoder.implementation.is_empty() || self.encoder.version.is_empty() {
            return Err(ZeppelinError::Validation(
                "embedding epoch encoder implementation and version must be non-empty".to_string(),
            ));
        }
        match (
            self.encoder.implementation.as_str(),
            self.encoder.bundle_prefix.as_deref(),
        ) {
            ("deterministic_dev", None) => {}
            ("deterministic_dev", Some(_)) => {
                return Err(ZeppelinError::Validation(
                    "deterministic development encoder must not bind an S3 bundle prefix"
                        .to_string(),
                ));
            }
            (_, Some(prefix)) => validate_bundle_prefix(prefix)?,
            (_, None) => {
                return Err(ZeppelinError::Validation(
                    "pinned encoder epoch must bind an S3 bundle prefix".to_string(),
                ));
            }
        }
        if self.encoder.artifact_digests.is_empty() {
            return Err(ZeppelinError::Validation(
                "embedding epoch must bind at least one encoder artifact digest".to_string(),
            ));
        }
        if self.encoder.supported_modalities.is_empty() {
            return Err(ZeppelinError::Validation(
                "embedding epoch must support at least one input modality".to_string(),
            ));
        }
        if self.vector_dimension == 0
            || self.max_query_vectors == 0
            || self.max_document_vectors == 0
        {
            return Err(ZeppelinError::Validation(
                "embedding epoch dimensions and vector-count limits must be positive".to_string(),
            ));
        }
        if let Some(mean) = self.exact_scoring_transform.mean() {
            validate_mean_shape(mean, self.vector_dimension)?;
        }
        self.matrix_dtype
            .validate_for_dimension(self.vector_dimension)?;
        if matches!(self.matrix_dtype, MatrixDtype::Int8SymV1 { .. }) {
            if self.output_normalization != NormalizationRecipe::L2 {
                return Err(ZeppelinError::Validation(
                    "int8_sym_v1 requires encoder L2-normalized rows".to_string(),
                ));
            }
            // FDE derivation and flat-segment rebuilds read INT8 epochs back
            // through the stored matrix bytes, which must therefore be the
            // encoder output at the persisted boundary — no exact-side
            // centering (tasks/MMLI-2/results/int8-flat-rebuild-design.md).
            if !matches!(
                self.exact_scoring_transform,
                VectorTransformRecipe::Identity
            ) {
                return Err(ZeppelinError::Validation(
                    "int8_sym_v1 requires the identity exact-scoring transform".to_string(),
                ));
            }
        }
        let expected = self.canonical_id()?;
        if self.id != expected {
            return Err(ZeppelinError::Validation(format!(
                "multi-vector epoch ID mismatch: expected {}, got {}",
                expected.to_hex(),
                self.id.to_hex()
            )));
        }
        Ok(())
    }
}

pub(crate) fn validate_bundle_prefix(prefix: &str) -> Result<()> {
    if prefix.is_empty()
        || prefix.starts_with('/')
        || prefix.ends_with('/')
        || prefix.contains('\\')
        || prefix
            .split('/')
            .any(|component| component.is_empty() || component == "." || component == "..")
    {
        return Err(ZeppelinError::Validation(
            "encoder bundle prefix must be a canonical non-empty relative S3 prefix".to_string(),
        ));
    }
    Ok(())
}

/// Immutable materialized FDE-transform descriptor.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FdeTransformArtifactRef {
    /// Exact namespace-owned content-addressed key.
    pub key: String,
    /// SHA-256 over the existing `ZFT1` bytes.
    pub checksum: ArtifactChecksum,
    /// Exact artifact size.
    pub size_bytes: u64,
    /// Existing `ZFT1` artifact format version.
    pub format_version: u32,
    /// Section-local physical-origin index, or the section owner when absent.
    #[serde(default)]
    pub artifact_origin: Option<ArtifactOriginIndex>,
}

/// One immutable fixed-dimensional generation recipe.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FdeRecipe {
    /// SHA-256 over the canonical recipe fields below.
    pub generation: FdeGenerationId,
    /// Semantic epoch whose matrices this generation consumes.
    pub semantic_epoch: MultiVectorEpochId,
    /// Production FDE construction parameters.
    pub params: FdeParams,
    /// Existing materialized `ZFT1` transform artifact.
    pub transform_artifact: FdeTransformArtifactRef,
    /// Candidate-only row transform selected by the lab.
    pub candidate_vector_transform: VectorTransformRecipe,
    /// Candidate-only document-row pooling selected by the lab.
    pub candidate_document_pooling: CandidateDocumentPooling,
}

impl FdeRecipe {
    /// Derive the canonical generation ID, excluding the stored generation.
    pub fn canonical_generation(&self) -> Result<FdeGenerationId> {
        #[derive(Serialize)]
        struct CanonicalRecipe {
            semantic_epoch: MultiVectorEpochId,
            params: FdeParams,
            transform_artifact: FdeTransformArtifactRef,
            candidate_vector_transform: VectorTransformRecipe,
            candidate_document_pooling: CandidateDocumentPooling,
        }

        // Generation identity is location-independent: artifacts participate
        // by content checksum and shape, never by their stored key, so branch
        // materialization can rehome them without minting a new generation.
        let mut transform_artifact = self.transform_artifact.clone();
        transform_artifact.artifact_origin = None;
        transform_artifact.key = String::new();
        let mut candidate_vector_transform = self.candidate_vector_transform.clone();
        if let Some(mean) = candidate_vector_transform.mean_mut() {
            mean.artifact_origin = None;
            mean.key = String::new();
        }
        canonical_sha256(
            b"zeppelin-fde-generation-v1",
            &CanonicalRecipe {
                semantic_epoch: self.semantic_epoch,
                params: self.params,
                transform_artifact,
                candidate_vector_transform,
                candidate_document_pooling: self.candidate_document_pooling,
            },
        )
        .map(FdeGenerationId::new)
    }

    /// Validate canonical identity and compatibility with one semantic epoch.
    pub fn validate(&self, epoch: &MultiVectorEpoch) -> Result<()> {
        if self.semantic_epoch != epoch.id {
            return Err(ZeppelinError::Validation(
                "FDE recipe semantic epoch does not match its profile epoch".to_string(),
            ));
        }
        if self.params.input_dimension != epoch.vector_dimension {
            return Err(ZeppelinError::DimensionMismatch {
                expected: epoch.vector_dimension as usize,
                actual: self.params.input_dimension as usize,
            });
        }
        if self.transform_artifact.format_version != u32::from(FDE_TRANSFORM_FORMAT_VERSION) {
            return Err(ZeppelinError::Validation(format!(
                "unsupported FDE transform artifact format version {}; this binary reads version {}",
                self.transform_artifact.format_version, FDE_TRANSFORM_FORMAT_VERSION
            )));
        }
        if self.transform_artifact.size_bytes == 0 {
            return Err(ZeppelinError::Validation(
                "FDE transform artifact size must be positive".to_string(),
            ));
        }
        if let Some(mean) = self.candidate_vector_transform.mean() {
            validate_mean_shape(mean, epoch.vector_dimension)?;
        }
        self.candidate_document_pooling.validate()?;
        let expected = self.canonical_generation()?;
        if self.generation != expected {
            return Err(ZeppelinError::Validation(format!(
                "FDE generation ID mismatch: expected {}, got {}",
                expected.to_hex(),
                self.generation.to_hex()
            )));
        }
        Ok(())
    }
}

/// Manifest-section selection of one semantic epoch and FDE generation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EmbeddingProfileRef {
    /// Stable operator-facing registration identity.
    pub profile: EmbeddingProfileId,
    /// Complete semantic epoch recipe.
    pub epoch: MultiVectorEpoch,
    /// Complete FDE generation recipe.
    pub fde: FdeRecipe,
    /// Production-writer/decoder qualification required only for INT8.
    #[serde(default)]
    pub int8_qualification: Option<Int8QualificationStamp>,
}

impl EmbeddingProfileRef {
    /// Validate recipe identities and matrix qualification.
    pub fn validate(&self) -> Result<()> {
        if self.profile.as_str().is_empty() {
            return Err(ZeppelinError::Validation(
                "embedding profile ID must be non-empty".to_string(),
            ));
        }
        self.epoch.validate()?;
        self.fde.validate(&self.epoch)?;
        match (self.epoch.matrix_dtype, self.int8_qualification.as_ref()) {
            (MatrixDtype::F16, None) => {}
            (MatrixDtype::F16, Some(_)) => {
                return Err(ZeppelinError::Validation(
                    "f16 profiles must not carry an int8 qualification stamp".to_string(),
                ))
            }
            (MatrixDtype::Int8SymV1 { .. }, None) => {
                return Err(ZeppelinError::Validation(
                    "int8_sym_v1 profile activation requires a qualification stamp".to_string(),
                ))
            }
            (MatrixDtype::Int8SymV1 { .. }, Some(stamp)) => {
                stamp.validate_for(&self.epoch)?;
            }
        }
        Ok(())
    }

    /// Validate recipe identities and namespace modality compatibility.
    pub fn validate_for_modalities(&self, accepted: &[InputModality]) -> Result<()> {
        self.validate()?;
        let supported = self
            .epoch
            .encoder
            .supported_modalities
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if let Some(modality) = accepted
            .iter()
            .copied()
            .find(|modality| !supported.contains(modality))
        {
            return Err(ZeppelinError::UnsupportedInputModality {
                modality: modality.as_str(),
            });
        }
        Ok(())
    }
}

/// Exact physical identity of one typed input-WAL fragment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PhysicalInputFragmentIdentity {
    /// Exact immutable input-WAL key.
    pub key: String,
    /// Input-fragment ULID.
    pub id: Ulid,
    /// Existing canonical input-fragment checksum.
    pub checksum: u64,
    /// Exact artifact size.
    pub size_bytes: u64,
    /// Section-local physical-origin index, or the section owner when absent.
    #[serde(default)]
    pub artifact_origin: Option<ArtifactOriginIndex>,
}

/// Immutable full-matrix fragment selected by one overlay.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MultiVectorEmbeddingFragmentRef {
    /// Exact namespace-owned matrix-fragment key.
    pub key: String,
    /// SHA-256 over the complete immutable artifact.
    pub checksum: ArtifactChecksum,
    /// Checksum of the input fragment from which this artifact was derived.
    pub source_fragment_checksum: u64,
    /// Semantic epoch used to encode every row.
    pub semantic_epoch: MultiVectorEpochId,
    /// Number of retrieval-unit rows.
    pub row_count: u32,
    /// Total multi-vector rows across retrieval units.
    pub total_vectors: u64,
    /// Coordinates per multi-vector row.
    pub vector_dimension: u32,
    /// Persisted matrix scalar representation copied from the artifact header.
    pub dtype: MatrixDtype,
    /// Artifact format version reserved for the coordinator codec.
    pub format_version: u32,
    /// Exact artifact size.
    pub size_bytes: u64,
    /// Section-local physical-origin index, or the section owner when absent.
    #[serde(default)]
    pub artifact_origin: Option<ArtifactOriginIndex>,
}

/// Immutable FDE fragment selected by one overlay.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FdeFragmentRef {
    /// Exact namespace-owned FDE-fragment key.
    pub key: String,
    /// SHA-256 over the complete immutable artifact.
    pub checksum: ArtifactChecksum,
    /// Matrix-fragment checksum from which these vectors were derived.
    pub embedding_fragment_checksum: ArtifactChecksum,
    /// FDE generation used for every row.
    pub generation: FdeGenerationId,
    /// Number of retrieval-unit rows.
    pub row_count: u32,
    /// Coordinates per FDE vector.
    pub fde_dimension: u32,
    /// Artifact format version reserved for the coordinator codec.
    pub format_version: u32,
    /// Exact artifact size.
    pub size_bytes: u64,
    /// Section-local physical-origin index, or the section owner when absent.
    #[serde(default)]
    pub artifact_origin: Option<ArtifactOriginIndex>,
}

/// Exact source row/version identity covered by derived artifacts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordVersionRef {
    /// Physical row ordinal inside the immutable input fragment.
    pub row_ordinal: u32,
    /// Caller record identity.
    pub record_id: VectorId,
    /// Canonical typed-content hash.
    pub content_hash: ContentHash,
    /// Namespace-local mutation sequence.
    pub sequence: u64,
}

/// Exact immutable source versions covered by one overlay.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordVersionCoverage {
    /// Covered row/version identities in deterministic source-row order.
    pub records: Vec<RecordVersionRef>,
}

/// One published matrix-plus-FDE derivation unit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SemanticOverlayRef {
    /// Physical input fragment from which this overlay was derived.
    pub source_fragment: PhysicalInputFragmentIdentity,
    /// Semantic epoch used for full matrices.
    pub semantic_epoch: MultiVectorEpochId,
    /// FDE generation used for candidate vectors.
    pub fde_generation: FdeGenerationId,
    /// Full exact-scoring matrices.
    pub embeddings: MultiVectorEmbeddingFragmentRef,
    /// Fixed-dimensional candidate vectors.
    pub fde_vectors: FdeFragmentRef,
    /// Exact source versions satisfied by this overlay.
    pub covered_versions: RecordVersionCoverage,
    /// Root-manifest generation at which publication succeeded.
    pub published_at_generation: u64,
}

fn validate_mean_shape(mean: &MeanVectorRef, expected_dimension: u32) -> Result<()> {
    if mean.vector_dimension != expected_dimension {
        return Err(ZeppelinError::DimensionMismatch {
            expected: expected_dimension as usize,
            actual: mean.vector_dimension as usize,
        });
    }
    let payload_size = u64::from(expected_dimension) * 4;
    if mean.format_version != u32::from(CENTERING_ARTIFACT_FORMAT_VERSION) {
        return Err(ZeppelinError::Validation(format!(
            "unsupported centering artifact format version {}; this binary reads version {}",
            mean.format_version, CENTERING_ARTIFACT_FORMAT_VERSION
        )));
    }
    if mean.size_bytes <= payload_size {
        return Err(ZeppelinError::Validation(
            "centering mean must be one versioned little-endian f32 vector".to_string(),
        ));
    }
    Ok(())
}

fn canonical_sha256<T: Serialize>(domain: &[u8], value: &T) -> Result<[u8; 32]> {
    let payload = rmp_serde::to_vec(value).map_err(|error| {
        ZeppelinError::Serialization(format!("canonical recipe serialization failed: {error}"))
    })?;
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update([0]);
    hasher.update(payload);
    Ok(hasher.finalize().into())
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
    use std::collections::BTreeMap;

    use crate::index::late_interaction::{
        FdeAlgorithmVersion, FdeParams, FdeTransform, FinalProjection, InnerProjection,
    };
    use crate::namespace::branching::ArtifactOriginIndex;

    use super::{
        ArtifactChecksum, CandidateDocumentPooling, EmbeddingProfileId, EmbeddingProfileRef,
        EncoderExecutionRef, EncoderInputRef, ExactScorerVersion, FdeRecipe,
        FdeTransformArtifactRef, ImageObjectRef, InputModality, Int8QualificationStamp,
        MatrixDtype, MeanVectorRef, MultiVectorEpoch, MultiVectorEpochId, NormalizationRecipe,
        TextContentRef, VectorTransformRecipe, INT8_QUALIFICATION_STAMP_VERSION,
    };

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

    fn config_e_epoch(artifact_digests: BTreeMap<String, ArtifactChecksum>) -> MultiVectorEpoch {
        let mut epoch = MultiVectorEpoch {
            id: MultiVectorEpochId::new([0; 32]),
            encoder: EncoderExecutionRef {
                implementation: "colpali-python-worker".to_string(),
                version: "v1".to_string(),
                bundle_prefix: Some("models/colpali/v1".to_string()),
                artifact_digests,
                supported_modalities: vec![
                    InputModality::ImageText,
                    InputModality::Text,
                    InputModality::Image,
                ],
            },
            preprocessing_digest: ArtifactChecksum::new([3; 32]),
            vector_dimension: 128,
            max_query_vectors: 64,
            max_document_vectors: 1_024,
            output_normalization: NormalizationRecipe::L2,
            exact_scoring_transform: VectorTransformRecipe::Identity,
            matrix_dtype: MatrixDtype::F16,
            exact_scorer: ExactScorerVersion::MaxSimV1,
        };
        epoch.id = epoch
            .canonical_id()
            .expect("fixture epoch must canonicalize");
        epoch
    }

    fn config_e_recipe(epoch: &MultiVectorEpoch) -> FdeRecipe {
        let params = FdeParams {
            algorithm: FdeAlgorithmVersion::PaperV1,
            repetitions: 40,
            simhash_bits: 4,
            input_dimension: 128,
            inner: InnerProjection::Rademacher { d_proj: 16 },
            final_projection: FinalProjection::None,
        };
        let transform = FdeTransform::generate(&params, 7)
            .expect("config E transform must generate")
            .to_bytes();
        let transform_checksum = ArtifactChecksum::digest(&transform);
        let mut recipe = FdeRecipe {
            generation: super::FdeGenerationId::new([0; 32]),
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
                    key: format!(
                        "catalog/late/centering/{}",
                        ArtifactChecksum::new([9; 32]).to_hex()
                    ),
                    checksum: ArtifactChecksum::new([9; 32]),
                    size_bytes: 10 + 128 * 4,
                    vector_dimension: 128,
                    format_version: 1,
                    artifact_origin: None,
                },
                renormalize: false,
            },
            candidate_document_pooling: CandidateDocumentPooling::Identity,
        };
        recipe.generation = recipe
            .canonical_generation()
            .expect("fixture recipe must canonicalize");
        recipe
    }

    #[test]
    fn canonical_epoch_and_generation_ids_are_order_and_origin_stable() {
        let forward = BTreeMap::from([
            ("model".to_string(), ArtifactChecksum::new([1; 32])),
            ("processor".to_string(), ArtifactChecksum::new([2; 32])),
        ]);
        let reverse = [
            ("processor".to_string(), ArtifactChecksum::new([2; 32])),
            ("model".to_string(), ArtifactChecksum::new([1; 32])),
        ]
        .into_iter()
        .collect();
        let first = config_e_epoch(forward);
        let mut reordered = config_e_epoch(reverse);
        reordered.encoder.supported_modalities.reverse();
        assert_eq!(
            first.canonical_id().unwrap(),
            reordered.canonical_id().unwrap()
        );

        let recipe = config_e_recipe(&first);
        assert_eq!(
            FdeTransform::generate(&recipe.params, 7)
                .unwrap()
                .output_dimension(),
            10_240
        );
        let mut rebased = recipe.clone();
        rebased.transform_artifact.artifact_origin = Some(ArtifactOriginIndex::new(2));
        rebased
            .candidate_vector_transform
            .mean_mut()
            .unwrap()
            .artifact_origin = Some(ArtifactOriginIndex::new(1));
        assert_eq!(
            recipe.canonical_generation().unwrap(),
            rebased.canonical_generation().unwrap()
        );
        recipe.validate(&first).unwrap();
    }

    #[test]
    fn candidate_document_pooling_is_canonical_and_rejects_unapproved_factors() {
        let epoch = config_e_epoch(BTreeMap::from([(
            "model".to_string(),
            ArtifactChecksum::new([1; 32]),
        )]));
        let identity = config_e_recipe(&epoch);
        let mut pooled = identity.clone();
        pooled.candidate_document_pooling = CandidateDocumentPooling::ContiguousMean { factor: 2 };
        pooled.generation = pooled.canonical_generation().unwrap();

        assert_ne!(identity.generation, pooled.generation);
        pooled.validate(&epoch).unwrap();

        let mut diagnostic_only = pooled;
        diagnostic_only.candidate_document_pooling =
            CandidateDocumentPooling::ContiguousMean { factor: 4 };
        diagnostic_only.generation = diagnostic_only.canonical_generation().unwrap();
        let error = diagnostic_only.validate(&epoch).unwrap_err();
        assert!(error.to_string().contains("approved 2x contiguous mean"));
    }

    #[test]
    fn bundle_prefix_is_canonical_epoch_identity() {
        let artifacts = BTreeMap::from([("model".to_string(), ArtifactChecksum::new([1; 32]))]);
        let first = config_e_epoch(artifacts.clone());
        let mut second = config_e_epoch(artifacts);
        second.encoder.bundle_prefix = Some("models/colpali/v2".to_string());
        second.id = second.canonical_id().expect("second canonical epoch");
        assert_ne!(first.id, second.id);

        second.encoder.bundle_prefix = Some("../mutable".to_string());
        second.id = second.canonical_id().expect("unsafe prefix still hashes");
        assert!(second
            .validate()
            .unwrap_err()
            .to_string()
            .contains("canonical non-empty relative"));

        let mut missing = first;
        missing.encoder.bundle_prefix = None;
        missing.id = missing.canonical_id().expect("missing prefix hashes");
        assert!(missing
            .validate()
            .unwrap_err()
            .to_string()
            .contains("must bind an S3 bundle prefix"));
    }

    #[test]
    fn int8_profile_requires_an_operator_approved_qualification_stamp() {
        let mut epoch = config_e_epoch(BTreeMap::from([(
            "model".to_string(),
            ArtifactChecksum::new([1; 32]),
        )]));
        let f16_profile = EmbeddingProfileRef {
            profile: EmbeddingProfileId::new("f16-unqualified"),
            epoch: epoch.clone(),
            fde: config_e_recipe(&epoch),
            int8_qualification: None,
        };
        f16_profile
            .validate_for_modalities(&[InputModality::Text])
            .expect("f16 profile does not require int8 qualification");

        epoch.matrix_dtype = MatrixDtype::Int8SymV1 { group_size: 32 };
        epoch.id = epoch.canonical_id().expect("int8 epoch canonicalizes");
        let recipe = config_e_recipe(&epoch);
        let mut profile = EmbeddingProfileRef {
            profile: EmbeddingProfileId::new("int8-qualified"),
            epoch: epoch.clone(),
            fde: recipe,
            int8_qualification: None,
        };

        let error = profile
            .validate_for_modalities(&[InputModality::Text])
            .expect_err("unstamped int8 profile");
        assert!(error.to_string().contains("qualification stamp"));

        profile.int8_qualification = Some(Int8QualificationStamp {
            semantic_epoch: epoch.id,
            dtype: epoch.matrix_dtype,
            evidence_digest: ArtifactChecksum::digest(b"production-ranking-evidence"),
            evidence_version: INT8_QUALIFICATION_STAMP_VERSION,
        });
        let error = profile
            .validate_for_modalities(&[InputModality::Text])
            .expect_err("arbitrary structurally matching stamp");
        assert!(error.to_string().contains("not operator-approved"));

        profile.int8_qualification.as_mut().unwrap().dtype =
            MatrixDtype::Int8SymV1 { group_size: 16 };
        let error = profile
            .validate_for_modalities(&[InputModality::Text])
            .expect_err("mismatched int8 stamp");
        assert!(error.to_string().contains("does not match"));
    }

    #[test]
    fn approved_int8_qualifications_pin_only_the_text_lane_g32_tuple() {
        assert_eq!(super::APPROVED_INT8_QUALIFICATIONS.len(), 1);
        let (epoch, dtype, evidence) = &super::APPROVED_INT8_QUALIFICATIONS[0];
        assert_eq!(
            epoch.to_hex(),
            "04643f3cac3e8a07eab78b5b8496c79701613e0dcb3f3ad9470c97b8cbf08749"
        );
        assert_eq!(*dtype, MatrixDtype::Int8SymV1 { group_size: 32 });
        assert_eq!(
            evidence.to_hex(),
            "e91ef65c9c26a772a7a98e05985ceb7f310a094541d853559fc3aaee0a88794b"
        );
    }

    #[test]
    fn int8_epoch_requires_the_identity_exact_scoring_transform() {
        let mut epoch = config_e_epoch(BTreeMap::from([(
            "model".to_string(),
            ArtifactChecksum::new([1; 32]),
        )]));
        epoch.matrix_dtype = MatrixDtype::Int8SymV1 { group_size: 32 };
        epoch.exact_scoring_transform = VectorTransformRecipe::SubtractMean {
            mean: MeanVectorRef {
                key: "ns/late/means/deadbeef".to_string(),
                checksum: ArtifactChecksum::new([7; 32]),
                size_bytes: 520,
                vector_dimension: 128,
                format_version: 1,
                artifact_origin: None,
            },
            renormalize: false,
        };
        epoch.id = epoch.canonical_id().expect("centered int8 epoch hashes");
        let error = epoch
            .validate()
            .expect_err("int8 epoch with exact-side centering");
        assert!(error
            .to_string()
            .contains("identity exact-scoring transform"));
    }
}
