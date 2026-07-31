//! Encoder-neutral inputs, validated embeddings, and the async adapter seam.

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::{Result, ZeppelinError};
use crate::index::late_interaction::MultiVectorMatrixRef;

use super::{
    ArtifactChecksum, ContentHash, EncoderInputRef, InputModality, MultiVectorEpochId,
    TextContentRef,
};

/// One fully resolved, owned document input passed to an encoder adapter.
///
/// Image bytes are kept separate from the durable [`EncoderInputRef`]. The
/// constructor verifies that they match the immutable source reference before
/// an untrusted worker or deterministic adapter can observe them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncoderDocumentInput {
    input: EncoderInputRef,
    content_hash: ContentHash,
    image_bytes: Option<Bytes>,
}

impl EncoderDocumentInput {
    /// Validate one durable input and its optionally resolved image bytes.
    pub fn new(
        input: EncoderInputRef,
        expected_content_hash: ContentHash,
        image_bytes: Option<Bytes>,
    ) -> Result<Self> {
        match &input {
            EncoderInputRef::Text {
                content: TextContentRef::Inline(text),
            } => {
                validate_text(text, "document")?;
                if image_bytes.is_some() {
                    return Err(ZeppelinError::Validation(
                        "text encoder input must not include image bytes".to_string(),
                    ));
                }
            }
            EncoderInputRef::Image { image } => {
                validate_image_ref(image.media_type.as_str(), image.width, image.height)?;
                validate_image_bytes(image, image_bytes.as_ref())?;
            }
            EncoderInputRef::ImageText {
                image,
                text: TextContentRef::Inline(text),
            } => {
                validate_text(text, "image-text document")?;
                validate_image_ref(image.media_type.as_str(), image.width, image.height)?;
                validate_image_bytes(image, image_bytes.as_ref())?;
            }
        }

        let actual_content_hash = input.content_hash()?;
        if actual_content_hash != expected_content_hash {
            return Err(ZeppelinError::Validation(format!(
                "encoder document content hash mismatch: expected {}, got {}",
                expected_content_hash.to_hex(),
                actual_content_hash.to_hex()
            )));
        }

        Ok(Self {
            input,
            content_hash: expected_content_hash,
            image_bytes,
        })
    }

    /// Return the durable typed input descriptor.
    #[must_use]
    pub const fn input_ref(&self) -> &EncoderInputRef {
        &self.input
    }

    /// Return the verified canonical content identity.
    #[must_use]
    pub const fn content_hash(&self) -> ContentHash {
        self.content_hash
    }

    /// Return resolved image bytes for image-bearing inputs.
    #[must_use]
    pub const fn image_bytes(&self) -> Option<&Bytes> {
        self.image_bytes.as_ref()
    }

    /// Return the input modality.
    #[must_use]
    pub const fn modality(&self) -> InputModality {
        self.input.modality()
    }
}

/// One borrowed text query passed to a profile's query encoder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EncoderQueryInput<'a> {
    text: &'a str,
}

impl<'a> EncoderQueryInput<'a> {
    /// Validate and wrap the MVP text-query input.
    pub fn new(text: &'a str) -> Result<Self> {
        validate_text(text, "query")?;
        Ok(Self { text })
    }

    /// Return the exact query text.
    #[must_use]
    pub const fn text(self) -> &'a str {
        self.text
    }
}

/// One owned, validated row-major multi-vector matrix.
#[derive(Clone, Debug, PartialEq)]
pub struct MultiVectorEmbedding {
    values: Vec<f32>,
    vector_count: usize,
    vector_dimension: usize,
}

impl MultiVectorEmbedding {
    /// Validate and take ownership of one row-major matrix.
    pub fn new(
        values: Vec<f32>,
        vector_count: usize,
        vector_dimension: usize,
        max_vectors: usize,
    ) -> Result<Self> {
        MultiVectorMatrixRef::new(&values, vector_count, vector_dimension, max_vectors)?;
        Ok(Self {
            values,
            vector_count,
            vector_dimension,
        })
    }

    /// Borrow the complete row-major scalar payload.
    #[must_use]
    pub fn values(&self) -> &[f32] {
        &self.values
    }

    /// Return the number of retained vectors.
    #[must_use]
    pub const fn vector_count(&self) -> usize {
        self.vector_count
    }

    /// Return the number of coordinates per retained vector.
    #[must_use]
    pub const fn vector_dimension(&self) -> usize {
        self.vector_dimension
    }

    /// Reborrow this owned matrix through the production validation seam.
    pub fn matrix_ref(&self) -> Result<MultiVectorMatrixRef<'_>> {
        MultiVectorMatrixRef::new(
            &self.values,
            self.vector_count,
            self.vector_dimension,
            self.vector_count,
        )
    }
}

/// One ordered, epoch-bound batch of document embeddings.
#[derive(Clone, Debug, PartialEq)]
pub struct MultiVectorEmbeddingBatch {
    epoch: MultiVectorEpochId,
    embeddings: Vec<MultiVectorEmbedding>,
    vector_dimension: usize,
}

impl MultiVectorEmbeddingBatch {
    /// Validate an adapter result against the requested count and dimension.
    pub fn new(
        epoch: MultiVectorEpochId,
        expected_count: usize,
        expected_dimension: usize,
        embeddings: Vec<MultiVectorEmbedding>,
    ) -> Result<Self> {
        if expected_count == 0 {
            return Err(ZeppelinError::Validation(
                "encoder document batch must not be empty".to_string(),
            ));
        }
        if expected_dimension == 0 {
            return Err(ZeppelinError::Validation(
                "encoder output dimension must be positive".to_string(),
            ));
        }
        if embeddings.len() != expected_count {
            return Err(ZeppelinError::Validation(format!(
                "encoder document batch count mismatch: expected {expected_count}, got {}",
                embeddings.len()
            )));
        }
        if let Some((index, embedding)) = embeddings
            .iter()
            .enumerate()
            .find(|(_, embedding)| embedding.vector_dimension() != expected_dimension)
        {
            return Err(ZeppelinError::Validation(format!(
                "encoder document {index} dimension mismatch: expected {expected_dimension}, got {}",
                embedding.vector_dimension()
            )));
        }
        Ok(Self {
            epoch,
            embeddings,
            vector_dimension: expected_dimension,
        })
    }

    /// Return the semantic epoch that produced every matrix.
    #[must_use]
    pub const fn epoch(&self) -> MultiVectorEpochId {
        self.epoch
    }

    /// Return the common matrix dimension.
    #[must_use]
    pub const fn vector_dimension(&self) -> usize {
        self.vector_dimension
    }

    /// Borrow the input-order-preserving matrices.
    #[must_use]
    pub fn embeddings(&self) -> &[MultiVectorEmbedding] {
        &self.embeddings
    }

    /// Consume the batch and return its input-order-preserving matrices.
    #[must_use]
    pub fn into_embeddings(self) -> Vec<MultiVectorEmbedding> {
        self.embeddings
    }
}

/// Object-safe async adapter implemented by every registered encoder.
#[async_trait]
pub trait MultiVectorEncoder: Send + Sync {
    /// Return the immutable semantic epoch produced by this adapter.
    fn epoch(&self) -> MultiVectorEpochId;

    /// Return the fixed coordinate dimension produced by this adapter.
    fn output_dimension(&self) -> usize;

    /// Encode an ordered, non-empty batch of document inputs.
    async fn encode_documents(
        &self,
        inputs: &[EncoderDocumentInput],
    ) -> Result<MultiVectorEmbeddingBatch>;

    /// Encode one validated text query.
    async fn encode_query(&self, input: EncoderQueryInput<'_>) -> Result<MultiVectorEmbedding>;
}

fn validate_text(text: &str, role: &str) -> Result<()> {
    if text.trim().is_empty() {
        return Err(ZeppelinError::Validation(format!(
            "{role} text must not be empty"
        )));
    }
    Ok(())
}

fn validate_image_ref(media_type: &str, width: u32, height: u32) -> Result<()> {
    if media_type.is_empty() {
        return Err(ZeppelinError::Validation(
            "encoder image media type must not be empty".to_string(),
        ));
    }
    if width == 0 || height == 0 {
        return Err(ZeppelinError::Validation(
            "encoder image dimensions must be positive".to_string(),
        ));
    }
    Ok(())
}

fn validate_image_bytes(image: &super::ImageObjectRef, image_bytes: Option<&Bytes>) -> Result<()> {
    let bytes = image_bytes.ok_or_else(|| {
        ZeppelinError::Validation("image encoder input requires resolved image bytes".to_string())
    })?;
    let actual_size = u64::try_from(bytes.len()).map_err(|_| {
        ZeppelinError::Validation("resolved image byte count exceeds u64".to_string())
    })?;
    if actual_size != image.encoded_size_bytes {
        return Err(ZeppelinError::Validation(format!(
            "resolved image size mismatch: expected {}, got {actual_size}",
            image.encoded_size_bytes
        )));
    }
    let actual_checksum = ArtifactChecksum::digest(bytes);
    if actual_checksum != image.checksum {
        return Err(ZeppelinError::Validation(format!(
            "resolved image checksum mismatch: expected {}, got {}",
            image.checksum.to_hex(),
            actual_checksum.to_hex()
        )));
    }
    Ok(())
}
