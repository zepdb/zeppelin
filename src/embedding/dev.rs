//! Deterministic development-only multi-vector encoder.

use std::collections::BTreeSet;

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::error::{Result, ZeppelinError};

use super::{
    EncoderDocumentInput, EncoderQueryInput, InputModality, MultiVectorEmbedding,
    MultiVectorEmbeddingBatch, MultiVectorEncoder, MultiVectorEpoch, MultiVectorEpochId,
};

/// Registered implementation name for the deterministic development adapter.
pub const DETERMINISTIC_DEV_IMPLEMENTATION: &str = "deterministic_dev";

/// Version whose bytes participate in the development adapter's epoch recipe.
pub const DETERMINISTIC_DEV_VERSION: &str = "deterministic-dev-v1";

const MIN_ROWS: usize = 4;
const MAX_ROWS: usize = 16;

/// Pure-Rust deterministic encoder admitted only by an explicit runtime gate.
///
/// This adapter is test infrastructure shipped in product code. It does not
/// approximate a production model and cannot be constructed when the caller's
/// `allow_dev` configuration value is false.
#[derive(Clone, Debug)]
pub struct DeterministicDev {
    epoch: MultiVectorEpochId,
    output_dimension: usize,
    max_query_vectors: usize,
    max_document_vectors: usize,
    supported_modalities: BTreeSet<InputModality>,
}

impl DeterministicDev {
    /// Construct the adapter for one validated registered epoch.
    pub fn new(allow_dev: bool, epoch: &MultiVectorEpoch) -> Result<Self> {
        if !allow_dev {
            return Err(ZeppelinError::Validation(
                "deterministic development encoder is disabled by configuration".to_string(),
            ));
        }
        epoch.validate()?;
        if epoch.encoder.implementation != DETERMINISTIC_DEV_IMPLEMENTATION
            || epoch.encoder.version != DETERMINISTIC_DEV_VERSION
        {
            return Err(ZeppelinError::Validation(
                "deterministic development encoder epoch identity mismatch".to_string(),
            ));
        }

        let output_dimension = usize::try_from(epoch.vector_dimension).map_err(|_| {
            ZeppelinError::Validation(
                "deterministic development encoder dimension exceeds usize".to_string(),
            )
        })?;
        let max_query_vectors = usize::try_from(epoch.max_query_vectors).map_err(|_| {
            ZeppelinError::Validation(
                "deterministic development query-vector limit exceeds usize".to_string(),
            )
        })?;
        let max_document_vectors = usize::try_from(epoch.max_document_vectors).map_err(|_| {
            ZeppelinError::Validation(
                "deterministic development document-vector limit exceeds usize".to_string(),
            )
        })?;

        Ok(Self {
            epoch: epoch.id,
            output_dimension,
            max_query_vectors,
            max_document_vectors,
            supported_modalities: epoch.encoder.supported_modalities.iter().copied().collect(),
        })
    }

    fn encode_seed(
        &self,
        domain: &[u8],
        source: &[u8],
        max_vectors: usize,
    ) -> Result<MultiVectorEmbedding> {
        let mut seed_hasher = Sha256::new();
        seed_hasher.update(domain);
        seed_hasher.update(self.epoch.as_bytes());
        seed_hasher.update(source);
        let seed: [u8; 32] = seed_hasher.finalize().into();
        let available_rows = max_vectors.min(MAX_ROWS);
        if available_rows == 0 {
            return Err(ZeppelinError::Validation(
                "deterministic development vector limit must be positive".to_string(),
            ));
        }
        let minimum_rows = MIN_ROWS.min(available_rows);
        let row_span = available_rows - minimum_rows + 1;
        let vector_count = minimum_rows + usize::from(seed[0]) % row_span;
        let scalar_count = vector_count
            .checked_mul(self.output_dimension)
            .ok_or_else(|| {
                ZeppelinError::Validation(
                    "deterministic development output shape overflows usize".to_string(),
                )
            })?;
        let mut values = Vec::new();
        values.try_reserve_exact(scalar_count).map_err(|error| {
            ZeppelinError::Validation(format!(
                "deterministic development output allocation failed: {error}"
            ))
        })?;

        for row in 0..vector_count {
            let row_start = values.len();
            for column in 0..self.output_dimension {
                let mut scalar_hasher = Sha256::new();
                scalar_hasher.update(b"zeppelin-deterministic-dev-scalar-v1");
                scalar_hasher.update(seed);
                scalar_hasher.update((row as u64).to_le_bytes());
                scalar_hasher.update((column as u64).to_le_bytes());
                let digest = scalar_hasher.finalize();
                let integer = u32::from_le_bytes(digest[..4].try_into().map_err(|_| {
                    ZeppelinError::Validation(
                        "deterministic development scalar digest is truncated".to_string(),
                    )
                })?);
                let unit = (integer as f64 + 0.5) / (u32::MAX as f64 + 1.0);
                values.push((unit.mul_add(2.0, -1.0)) as f32);
            }
            normalize_row(&mut values[row_start..])?;
        }

        MultiVectorEmbedding::new(values, vector_count, self.output_dimension, max_vectors)
    }
}

#[async_trait]
impl MultiVectorEncoder for DeterministicDev {
    fn epoch(&self) -> MultiVectorEpochId {
        self.epoch
    }

    fn output_dimension(&self) -> usize {
        self.output_dimension
    }

    async fn encode_documents(
        &self,
        inputs: &[EncoderDocumentInput],
    ) -> Result<MultiVectorEmbeddingBatch> {
        if inputs.is_empty() {
            return Err(ZeppelinError::Validation(
                "encoder document batch must not be empty".to_string(),
            ));
        }
        let mut embeddings = Vec::new();
        embeddings
            .try_reserve_exact(inputs.len())
            .map_err(|error| {
                ZeppelinError::Validation(format!(
                    "deterministic development batch allocation failed: {error}"
                ))
            })?;
        for input in inputs {
            if !self.supported_modalities.contains(&input.modality()) {
                return Err(ZeppelinError::UnsupportedInputModality {
                    modality: input.modality().as_str(),
                });
            }
            let domain = match input.modality() {
                InputModality::Text => b"zeppelin-deterministic-dev-document-text-v1".as_slice(),
                InputModality::Image => b"zeppelin-deterministic-dev-document-image-v1".as_slice(),
                InputModality::ImageText => {
                    b"zeppelin-deterministic-dev-document-image-text-v1".as_slice()
                }
            };
            embeddings.push(self.encode_seed(
                domain,
                input.content_hash().as_bytes(),
                self.max_document_vectors,
            )?);
        }
        MultiVectorEmbeddingBatch::new(self.epoch, inputs.len(), self.output_dimension, embeddings)
    }

    async fn encode_query(&self, input: EncoderQueryInput<'_>) -> Result<MultiVectorEmbedding> {
        self.encode_seed(
            b"zeppelin-deterministic-dev-query-text-v1",
            input.text().as_bytes(),
            self.max_query_vectors,
        )
    }
}

fn normalize_row(values: &mut [f32]) -> Result<()> {
    let squared_norm = values
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>();
    if !squared_norm.is_finite() || squared_norm == 0.0 {
        return Err(ZeppelinError::Validation(
            "deterministic development encoder produced an invalid row norm".to_string(),
        ));
    }
    let reciprocal = squared_norm.sqrt().recip() as f32;
    for value in values {
        *value *= reciprocal;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{DeterministicDev, DETERMINISTIC_DEV_IMPLEMENTATION, DETERMINISTIC_DEV_VERSION};
    use crate::embedding::{
        ArtifactChecksum, EncoderDocumentInput, EncoderExecutionRef, EncoderInputRef,
        EncoderQueryInput, ExactScorerVersion, InputModality, MatrixDtype, MultiVectorEncoder,
        MultiVectorEpoch, MultiVectorEpochId, NormalizationRecipe, TextContentRef,
        VectorTransformRecipe,
    };

    fn epoch() -> MultiVectorEpoch {
        let mut artifact_digests = BTreeMap::new();
        artifact_digests.insert(
            "deterministic-dev".to_string(),
            ArtifactChecksum::digest(DETERMINISTIC_DEV_VERSION.as_bytes()),
        );
        let mut epoch = MultiVectorEpoch {
            id: MultiVectorEpochId::new([0; 32]),
            encoder: EncoderExecutionRef {
                implementation: DETERMINISTIC_DEV_IMPLEMENTATION.to_string(),
                version: DETERMINISTIC_DEV_VERSION.to_string(),
                bundle_prefix: None,
                artifact_digests,
                supported_modalities: vec![
                    InputModality::Text,
                    InputModality::Image,
                    InputModality::ImageText,
                ],
            },
            preprocessing_digest: ArtifactChecksum::digest(b"deterministic-dev-preprocessing-v1"),
            vector_dimension: 8,
            max_query_vectors: 16,
            max_document_vectors: 16,
            output_normalization: NormalizationRecipe::L2,
            exact_scoring_transform: VectorTransformRecipe::Identity,
            matrix_dtype: MatrixDtype::F16,
            exact_scorer: ExactScorerVersion::MaxSimV1,
        };
        epoch.id = epoch.canonical_id().expect("canonical dev epoch");
        epoch
    }

    fn text_document(text: &str) -> EncoderDocumentInput {
        let input = EncoderInputRef::Text {
            content: TextContentRef::Inline(text.to_string()),
        };
        let content_hash = input.content_hash().expect("content hash");
        EncoderDocumentInput::new(input, content_hash, None).expect("valid text document")
    }

    #[test]
    fn constructor_enforces_development_gate() {
        let error = DeterministicDev::new(false, &epoch()).expect_err("gate must reject");
        assert!(error.to_string().contains("disabled by configuration"));
    }

    #[tokio::test]
    async fn output_is_repeatable_and_object_safe() {
        let adapter = DeterministicDev::new(true, &epoch()).expect("dev adapter");
        let object_safe: &dyn MultiVectorEncoder = &adapter;
        let documents = vec![text_document("repeatable document")];
        let first = object_safe
            .encode_documents(&documents)
            .await
            .expect("first encode");
        let second = object_safe
            .encode_documents(&documents)
            .await
            .expect("second encode");
        assert_eq!(first, second);
        assert_eq!(first.epoch(), object_safe.epoch());
    }

    #[tokio::test]
    async fn role_and_content_domains_are_separate() {
        let adapter = DeterministicDev::new(true, &epoch()).expect("dev adapter");
        let document = adapter
            .encode_documents(&[text_document("same bytes")])
            .await
            .expect("document encode")
            .embeddings()[0]
            .clone();
        let query = adapter
            .encode_query(EncoderQueryInput::new("same bytes").expect("query input"))
            .await
            .expect("query encode");
        let other = adapter
            .encode_query(EncoderQueryInput::new("different bytes").expect("query input"))
            .await
            .expect("other query encode");
        assert_ne!(document, query);
        assert_ne!(query, other);
    }

    #[tokio::test]
    async fn output_shape_is_bounded_and_finite() {
        let adapter = DeterministicDev::new(true, &epoch()).expect("dev adapter");
        let output = adapter
            .encode_query(EncoderQueryInput::new("shape check").expect("query input"))
            .await
            .expect("query encode");
        assert!((4..=16).contains(&output.vector_count()));
        assert_eq!(output.vector_dimension(), adapter.output_dimension());
        assert!(output.values().iter().all(|value| value.is_finite()));
        output.matrix_ref().expect("production matrix validation");
    }
}
