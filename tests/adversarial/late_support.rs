//! Test-only deterministic MMLI adapter used by the adversarial profile.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use zeppelin::config::MmliConfig;
use zeppelin::embedding::{
    ArtifactChecksum, CandidateDocumentPooling, EmbeddingProfileId, EmbeddingProfileRef,
    EncoderDocumentInput, EncoderExecutionRef, EncoderInputRef, EncoderQueryInput,
    EnrichmentCoordinator, EnrichmentCoordinatorOptions, ExactScorerVersion, FdeGenerationId,
    FdeRecipe, FdeTransformArtifactRef, InputModality, MatrixDtype, MultiVectorEmbedding,
    MultiVectorEmbeddingBatch, MultiVectorEncoder, MultiVectorEncoderProvider,
    MultiVectorEncoderRegistry, MultiVectorEpoch, MultiVectorEpochId, NormalizationRecipe,
    TextContentRef, VectorTransformRecipe, DETERMINISTIC_DEV_IMPLEMENTATION,
};
use zeppelin::error::{Result, ZeppelinError};
use zeppelin::index::late_interaction::{
    FdeAlgorithmVersion, FdeParams, FdeTransform, FinalProjection, InnerProjection,
};
use zeppelin::namespace::NamespaceManager;
use zeppelin::storage::{CreateOnlyOutcome, ZeppelinStore};
use zeppelin::wal::{LeaseManager, Manifest};

/// Fixed coordinate count for every adversarial late-interaction matrix.
pub const LATE_VECTOR_DIMENSION: usize = 16;
/// Fixed upper bound for document and query token rows.
pub const LATE_MAX_VECTORS: usize = 32;

const ENCODED_MATRIX_PREFIX: &str = "zeppelin-adversarial-matrix-v1:";
const ENCODER_VERSION: &str = "adversarial-matrix-v1";
const PROFILE_ID: &str = "adversarial-matrix-v1";
const TRANSFORM_SEED: u64 = 17;
const TEXT_MODALITIES: &[InputModality] = &[InputModality::Text];

/// Encode one replayable matrix inside the typed inline-text HTTP surface.
pub fn encode_matrix_text(matrix: &[Vec<f32>]) -> Result<String> {
    validate_matrix(matrix)?;
    let encoded = serde_json::to_string(matrix).map_err(|error| {
        ZeppelinError::Serialization(format!(
            "adversarial late matrix JSON encoding failed: {error}"
        ))
    })?;
    Ok(format!("{ENCODED_MATRIX_PREFIX}{encoded}"))
}

/// Build the epoch-keyed deterministic encoder provider used by the server and
/// the enrichment coordinator for one adversarial run.
pub fn late_encoder_provider(config: &MmliConfig) -> Result<Arc<dyn MultiVectorEncoderProvider>> {
    let epoch = late_epoch()?;
    let registry = Arc::new(MultiVectorEncoderRegistry::new(config));
    registry.register(Arc::new(ReplayMatrixEncoder { epoch: epoch.id }))?;
    Ok(registry)
}

/// Upload and activate the immutable profile for an already-created late
/// namespace. Repeated activation verifies any content-addressed collision.
pub async fn activate_late_embedding_profile(
    store: &ZeppelinStore,
    namespace_manager: &NamespaceManager,
    namespace: &str,
) -> Result<EmbeddingProfileRef> {
    let metadata = namespace_manager.get(namespace).await?;
    let late_config = metadata.late_interaction.as_ref().ok_or_else(|| {
        ZeppelinError::Validation(format!(
            "adversarial late profile requires late-interaction namespace {namespace}"
        ))
    })?;
    if late_config.accepted_modalities.as_slice() != TEXT_MODALITIES {
        return Err(ZeppelinError::Validation(format!(
            "adversarial late namespace {namespace} must accept exactly text inputs"
        )));
    }
    let incarnation: uuid::Uuid = metadata
        .incarnation_id
        .as_ref()
        .ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "adversarial late namespace {namespace} has no incarnation"
            ))
        })?
        .to_string()
        .parse()
        .map_err(|error| {
            ZeppelinError::Serialization(format!(
                "adversarial late namespace {namespace} has invalid incarnation: {error}"
            ))
        })?;

    let epoch = late_epoch()?;
    let params = late_fde_params();
    let transform = FdeTransform::generate(&params, TRANSFORM_SEED)?;
    let transform_bytes = transform.to_bytes();
    let transform_checksum = ArtifactChecksum::digest(&transform_bytes);
    let transform_key = format!(
        "{namespace}/late/transforms/{}",
        transform_checksum.to_hex()
    );
    match store
        .put_create_outcome(&transform_key, transform_bytes.clone())
        .await?
    {
        CreateOnlyOutcome::Created { .. } => {}
        CreateOnlyOutcome::AlreadyExists => {
            let existing = store.get(&transform_key).await?;
            if existing != transform_bytes {
                return Err(ZeppelinError::Serialization(format!(
                    "adversarial late transform collision at {transform_key}"
                )));
            }
        }
    }

    let mut fde = FdeRecipe {
        generation: FdeGenerationId::new([0; 32]),
        semantic_epoch: epoch.id,
        params,
        transform_artifact: FdeTransformArtifactRef {
            key: transform_key,
            checksum: transform_checksum,
            size_bytes: transform_bytes.len() as u64,
            format_version: 1,
            artifact_origin: None,
        },
        candidate_vector_transform: VectorTransformRecipe::Identity,
        candidate_document_pooling: CandidateDocumentPooling::Identity,
    };
    fde.generation = fde.canonical_generation()?;
    let profile = EmbeddingProfileRef {
        profile: EmbeddingProfileId::new(PROFILE_ID),
        epoch,
        fde,
        int8_qualification: None,
    };
    profile.validate_for_modalities(&late_config.accepted_modalities)?;

    let (mut manifest, version) =
        Manifest::read_versioned_required_for_incarnation(store, namespace, incarnation).await?;
    manifest
        .activate_embedding_profile(store, namespace, &version, &profile)
        .await?;
    Ok(profile)
}

/// Encode and publish every currently pending retrieval-unit fragment.
///
/// Returns the total number of fragments admitted during this call. A zero
/// count is a valid idempotent result when the namespace is already enriched.
pub async fn enrich_pending_retrieval_units(
    store: &ZeppelinStore,
    lease_manager: Arc<LeaseManager>,
    encoder_provider: Arc<dyn MultiVectorEncoderProvider>,
    namespace_manager: &NamespaceManager,
    namespace: &str,
) -> Result<usize> {
    let metadata = namespace_manager.get(namespace).await?;
    let accepted_modalities = metadata
        .late_interaction
        .as_ref()
        .ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "adversarial enrichment requires late-interaction namespace {namespace}"
            ))
        })?
        .accepted_modalities
        .clone();
    let incarnation: uuid::Uuid = metadata
        .incarnation_id
        .as_ref()
        .ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "adversarial late namespace {namespace} has no incarnation"
            ))
        })?
        .to_string()
        .parse()
        .map_err(|error| {
            ZeppelinError::Serialization(format!(
                "adversarial late namespace {namespace} has invalid incarnation: {error}"
            ))
        })?;
    let coordinator = EnrichmentCoordinator::start(
        store.clone(),
        lease_manager,
        encoder_provider,
        EnrichmentCoordinatorOptions {
            queue_capacity: 64,
            max_retry_attempts: 4,
            checkpoint: None,
        },
    );
    let mut admitted_total = 0_usize;
    loop {
        let report = coordinator
            .discover_and_admit(
                namespace,
                incarnation,
                Some(&accepted_modalities),
                usize::MAX,
                u64::MAX,
            )
            .await?;
        admitted_total = admitted_total
            .checked_add(report.admitted_fragments)
            .ok_or_else(|| {
                ZeppelinError::Validation(
                    "adversarial enrichment admitted-fragment count overflowed".to_string(),
                )
            })?;
        if report.admitted_fragments == 0 {
            break;
        }
        coordinator.wait_for_idle().await?;
    }
    coordinator.shutdown().await?;
    Ok(admitted_total)
}

fn late_epoch() -> Result<MultiVectorEpoch> {
    let mut epoch = MultiVectorEpoch {
        id: MultiVectorEpochId::new([0; 32]),
        encoder: EncoderExecutionRef {
            implementation: DETERMINISTIC_DEV_IMPLEMENTATION.to_string(),
            version: ENCODER_VERSION.to_string(),
            bundle_prefix: None,
            artifact_digests: BTreeMap::from([(
                "adversarial-matrix-codec".to_string(),
                ArtifactChecksum::digest(ENCODER_VERSION.as_bytes()),
            )]),
            supported_modalities: TEXT_MODALITIES.to_vec(),
        },
        preprocessing_digest: ArtifactChecksum::digest(ENCODER_VERSION.as_bytes()),
        vector_dimension: LATE_VECTOR_DIMENSION as u32,
        max_query_vectors: LATE_MAX_VECTORS as u32,
        max_document_vectors: LATE_MAX_VECTORS as u32,
        output_normalization: NormalizationRecipe::Identity,
        exact_scoring_transform: VectorTransformRecipe::Identity,
        matrix_dtype: MatrixDtype::F16,
        exact_scorer: ExactScorerVersion::MaxSimV1,
    };
    epoch.id = epoch.canonical_id()?;
    epoch.validate()?;
    Ok(epoch)
}

const fn late_fde_params() -> FdeParams {
    FdeParams {
        algorithm: FdeAlgorithmVersion::PaperV1,
        repetitions: 2,
        simhash_bits: 1,
        input_dimension: LATE_VECTOR_DIMENSION as u32,
        inner: InnerProjection::Rademacher { d_proj: 4 },
        final_projection: FinalProjection::None,
    }
}

struct ReplayMatrixEncoder {
    epoch: MultiVectorEpochId,
}

#[async_trait]
impl MultiVectorEncoder for ReplayMatrixEncoder {
    fn epoch(&self) -> MultiVectorEpochId {
        self.epoch
    }

    fn output_dimension(&self) -> usize {
        LATE_VECTOR_DIMENSION
    }

    async fn encode_documents(
        &self,
        inputs: &[EncoderDocumentInput],
    ) -> Result<MultiVectorEmbeddingBatch> {
        if inputs.is_empty() {
            return Err(ZeppelinError::Validation(
                "adversarial matrix encoder document batch must not be empty".to_string(),
            ));
        }
        let embeddings = inputs
            .iter()
            .map(|input| match input.input_ref() {
                EncoderInputRef::Text {
                    content: TextContentRef::Inline(text),
                } => decode_matrix_text(text),
                EncoderInputRef::Image { .. } | EncoderInputRef::ImageText { .. } => {
                    Err(ZeppelinError::Validation(
                        "adversarial matrix encoder accepts only inline text".to_string(),
                    ))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        MultiVectorEmbeddingBatch::new(self.epoch, inputs.len(), LATE_VECTOR_DIMENSION, embeddings)
    }

    async fn encode_query(&self, input: EncoderQueryInput<'_>) -> Result<MultiVectorEmbedding> {
        decode_matrix_text(input.text())
    }
}

fn decode_matrix_text(text: &str) -> Result<MultiVectorEmbedding> {
    let encoded = text.strip_prefix(ENCODED_MATRIX_PREFIX).ok_or_else(|| {
        ZeppelinError::Validation(
            "adversarial matrix encoder input is missing its codec prefix".to_string(),
        )
    })?;
    let matrix: Vec<Vec<f32>> = serde_json::from_str(encoded).map_err(|error| {
        ZeppelinError::Serialization(format!(
            "adversarial late matrix JSON decoding failed: {error}"
        ))
    })?;
    validate_matrix(&matrix)?;
    let vector_count = matrix.len();
    let values = matrix.into_iter().flatten().collect();
    MultiVectorEmbedding::new(
        values,
        vector_count,
        LATE_VECTOR_DIMENSION,
        LATE_MAX_VECTORS,
    )
}

fn validate_matrix(matrix: &[Vec<f32>]) -> Result<()> {
    if matrix.is_empty() || matrix.len() > LATE_MAX_VECTORS {
        return Err(ZeppelinError::Validation(format!(
            "adversarial late matrix row count must be in 1..={LATE_MAX_VECTORS}, got {}",
            matrix.len()
        )));
    }
    for (row_index, row) in matrix.iter().enumerate() {
        if row.len() != LATE_VECTOR_DIMENSION {
            return Err(ZeppelinError::Validation(format!(
                "adversarial late matrix row {row_index} dimension must be {LATE_VECTOR_DIMENSION}, got {}",
                row.len()
            )));
        }
        if let Some(column_index) = row.iter().position(|value| !value.is_finite()) {
            return Err(ZeppelinError::Validation(format!(
                "adversarial late matrix row {row_index} column {column_index} is not finite"
            )));
        }
    }
    Ok(())
}
