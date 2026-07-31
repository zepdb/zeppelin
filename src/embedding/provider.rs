//! Lazy, configuration-backed encoder session ownership.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;

use super::priority::QueryPriorityEncoder;
use super::{
    DeterministicDev, EmbeddingProfileRef, MultiVectorEncoder, MultiVectorEncoderProvider,
    MultiVectorEpoch, MultiVectorEpochId, PinnedWorker, PinnedWorkerConfig,
    DETERMINISTIC_DEV_IMPLEMENTATION,
};
use crate::config::{MmliConfig, MmliWorkerConfig};
use crate::error::{Result, ZeppelinError};
use crate::storage::ZeppelinStore;

/// Lazy owner of the exact encoder sessions selected by active profiles.
///
/// Sessions are cached by canonical epoch and never replaced under the same
/// identity. Production workers materialize the S3 bundle prefix embedded in
/// that epoch; mutable process configuration supplies only local execution
/// paths and resource bounds.
pub struct ConfiguredEncoderProvider {
    store: ZeppelinStore,
    allow_dev_encoder: bool,
    worker: Option<MmliWorkerConfig>,
    state: Mutex<ProviderState>,
}

#[derive(Default)]
struct ProviderState {
    closed: bool,
    sessions: HashMap<MultiVectorEpochId, CachedEncoder>,
}

struct CachedEncoder {
    epoch: MultiVectorEpoch,
    session: OwnedEncoder,
    encoder: Arc<dyn MultiVectorEncoder>,
}

enum OwnedEncoder {
    Dev(Arc<DeterministicDev>),
    Worker(Arc<PinnedWorker>),
}

impl OwnedEncoder {
    fn as_encoder(&self) -> Arc<dyn MultiVectorEncoder> {
        match self {
            Self::Dev(encoder) => {
                let encoder: Arc<dyn MultiVectorEncoder> = encoder.clone();
                encoder
            }
            Self::Worker(encoder) => {
                let encoder: Arc<dyn MultiVectorEncoder> = encoder.clone();
                encoder
            }
        }
    }

    async fn shutdown(&self) -> Result<()> {
        match self {
            Self::Dev(_) => Ok(()),
            Self::Worker(worker) => worker.shutdown().await,
        }
    }
}

impl ConfiguredEncoderProvider {
    /// Construct a lazy provider from already validated process configuration.
    #[must_use]
    pub fn new(store: ZeppelinStore, config: &MmliConfig) -> Self {
        Self {
            store,
            allow_dev_encoder: config.allow_dev_encoder,
            worker: config.worker.clone(),
            state: Mutex::new(ProviderState::default()),
        }
    }

    async fn spawn(&self, epoch: &MultiVectorEpoch) -> Result<OwnedEncoder> {
        if epoch.encoder.implementation == DETERMINISTIC_DEV_IMPLEMENTATION {
            return DeterministicDev::new(self.allow_dev_encoder, epoch)
                .map(Arc::new)
                .map(OwnedEncoder::Dev);
        }

        let runtime = self.worker.as_ref().ok_or_else(|| {
            ZeppelinError::Config(format!(
                "pinned encoder {} selected but [mmli.worker] is absent",
                epoch.id.to_hex()
            ))
        })?;
        let bundle_prefix = epoch.encoder.bundle_prefix.as_deref().ok_or_else(|| {
            ZeppelinError::Validation(
                "pinned encoder epoch is missing its S3 bundle prefix".to_string(),
            )
        })?;
        let worker = PinnedWorker::spawn_from_s3(
            &self.store,
            pinned_worker_config(runtime),
            bundle_prefix,
            &runtime.bundle_cache_dir,
            epoch.clone(),
        )
        .await?;
        Ok(OwnedEncoder::Worker(Arc::new(worker)))
    }
}

#[async_trait]
impl MultiVectorEncoderProvider for ConfiguredEncoderProvider {
    async fn encoder_for(
        &self,
        profile: &EmbeddingProfileRef,
    ) -> Result<Arc<dyn MultiVectorEncoder>> {
        profile.validate()?;
        let mut state = self.state.lock().await;
        if state.closed {
            return Err(ZeppelinError::Validation(
                "configured encoder provider is shut down".to_string(),
            ));
        }
        if let Some(cached) = state.sessions.get(&profile.epoch.id) {
            if cached.epoch != profile.epoch {
                return Err(ZeppelinError::Validation(
                    "cached encoder epoch identity collision".to_string(),
                ));
            }
            return Ok(Arc::clone(&cached.encoder));
        }

        // Hold the construction lock so concurrent first use cannot spawn two
        // subprocesses for the same exact epoch.
        let session = self.spawn(&profile.epoch).await?;
        let raw_encoder = session.as_encoder();
        if raw_encoder.epoch() != profile.epoch.id
            || raw_encoder.output_dimension() != profile.epoch.vector_dimension as usize
        {
            session.shutdown().await?;
            return Err(ZeppelinError::Validation(
                "configured encoder identity or dimension mismatch".to_string(),
            ));
        }
        let encoder = QueryPriorityEncoder::wrap(raw_encoder);
        state.sessions.insert(
            profile.epoch.id,
            CachedEncoder {
                epoch: profile.epoch.clone(),
                session,
                encoder: Arc::clone(&encoder),
            },
        );
        Ok(encoder)
    }

    async fn shutdown(&self) -> Result<()> {
        let sessions = {
            let mut state = self.state.lock().await;
            state.closed = true;
            std::mem::take(&mut state.sessions)
        };
        let mut failures = Vec::new();
        for (epoch, cached) in sessions {
            if let Err(error) = cached.session.shutdown().await {
                failures.push(format!("{}: {error}", epoch.to_hex()));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(ZeppelinError::Validation(format!(
                "failed to shut down configured encoder sessions: {}",
                failures.join("; ")
            )))
        }
    }
}

fn pinned_worker_config(config: &MmliWorkerConfig) -> PinnedWorkerConfig {
    PinnedWorkerConfig {
        venv_dir: config.venv_dir.clone(),
        python_binary: config.python_binary.clone(),
        worker_script: config.worker_script.clone(),
        scratch_dir: config.scratch_dir.clone(),
        // Replaced by `spawn_from_s3` only after S3 materialization succeeds.
        model_bundle_dir: config.bundle_cache_dir.clone(),
        max_batch_units: config.max_batch_units,
        max_batch_input_bytes: config.max_batch_input_bytes,
        max_batch_pixels: config.max_batch_pixels,
        max_batch_rows: config.max_batch_rows,
        max_tensor_bytes: config.max_tensor_bytes,
        max_protocol_line_bytes: config.max_protocol_line_bytes,
        max_stderr_bytes: config.max_stderr_bytes,
        handshake_timeout: Duration::from_secs(config.handshake_timeout_secs),
        request_timeout: Duration::from_secs(config.request_timeout_secs),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use super::ConfiguredEncoderProvider;
    use crate::config::MmliConfig;
    use crate::embedding::{
        ArtifactChecksum, EmbeddingProfileId, EmbeddingProfileRef, EncoderExecutionRef,
        ExactScorerVersion, FdeGenerationId, FdeRecipe, FdeTransformArtifactRef, InputModality,
        MatrixDtype, MultiVectorEncoderProvider, MultiVectorEpoch, MultiVectorEpochId,
        NormalizationRecipe, VectorTransformRecipe, DETERMINISTIC_DEV_IMPLEMENTATION,
        DETERMINISTIC_DEV_VERSION,
    };
    use crate::index::late_interaction::{
        FdeAlgorithmVersion, FdeParams, FinalProjection, InnerProjection,
    };
    use crate::storage::ZeppelinStore;

    fn profile(implementation: &str, bundle_prefix: Option<&str>) -> EmbeddingProfileRef {
        let mut epoch = MultiVectorEpoch {
            id: MultiVectorEpochId::new([0; 32]),
            encoder: EncoderExecutionRef {
                implementation: implementation.to_string(),
                version: if implementation == DETERMINISTIC_DEV_IMPLEMENTATION {
                    DETERMINISTIC_DEV_VERSION.to_string()
                } else {
                    "v1".to_string()
                },
                bundle_prefix: bundle_prefix.map(str::to_string),
                artifact_digests: BTreeMap::from([(
                    "model".to_string(),
                    ArtifactChecksum::digest(b"model"),
                )]),
                supported_modalities: vec![InputModality::Text],
            },
            preprocessing_digest: ArtifactChecksum::digest(b"preprocessing"),
            vector_dimension: 8,
            max_query_vectors: 16,
            max_document_vectors: 16,
            output_normalization: NormalizationRecipe::L2,
            exact_scoring_transform: VectorTransformRecipe::Identity,
            matrix_dtype: MatrixDtype::F16,
            exact_scorer: ExactScorerVersion::MaxSimV1,
        };
        epoch.id = epoch.canonical_id().expect("canonical epoch");
        let mut fde = FdeRecipe {
            generation: FdeGenerationId::new([0; 32]),
            semantic_epoch: epoch.id,
            params: FdeParams {
                algorithm: FdeAlgorithmVersion::PaperV1,
                repetitions: 1,
                simhash_bits: 1,
                input_dimension: 8,
                inner: InnerProjection::Rademacher { d_proj: 2 },
                final_projection: FinalProjection::None,
            },
            transform_artifact: FdeTransformArtifactRef {
                key: "catalog/late/transforms/test".to_string(),
                checksum: ArtifactChecksum::digest(b"transform"),
                size_bytes: 1,
                format_version: 1,
                artifact_origin: None,
            },
            candidate_vector_transform: VectorTransformRecipe::Identity,
            candidate_document_pooling: crate::embedding::CandidateDocumentPooling::Identity,
        };
        fde.generation = fde.canonical_generation().expect("canonical generation");
        EmbeddingProfileRef {
            profile: EmbeddingProfileId::new("provider-test"),
            epoch,
            fde,
            int8_qualification: None,
        }
    }

    fn store() -> ZeppelinStore {
        ZeppelinStore::new(Arc::new(InMemory::new()))
    }

    #[tokio::test]
    async fn deterministic_session_is_lazy_cached_and_shutdown_owned() {
        let config = MmliConfig {
            allow_dev_encoder: true,
            ..MmliConfig::default()
        };
        let provider = ConfiguredEncoderProvider::new(store(), &config);
        let profile = profile(DETERMINISTIC_DEV_IMPLEMENTATION, None);

        let first = provider.encoder_for(&profile).await.expect("first resolve");
        let second = provider
            .encoder_for(&profile)
            .await
            .expect("cached resolve");
        assert!(Arc::ptr_eq(&first, &second));

        provider.shutdown().await.expect("provider shutdown");
        let error = provider
            .encoder_for(&profile)
            .await
            .err()
            .expect("closed provider must reject");
        assert!(error.to_string().contains("shut down"));
    }

    #[tokio::test]
    async fn missing_dev_gate_or_pinned_runtime_fails_loudly() {
        let provider = ConfiguredEncoderProvider::new(store(), &MmliConfig::default());
        let dev_error = provider
            .encoder_for(&profile(DETERMINISTIC_DEV_IMPLEMENTATION, None))
            .await
            .err()
            .expect("disabled dev encoder must reject");
        assert!(dev_error.to_string().contains("disabled by configuration"));

        let pinned_error = provider
            .encoder_for(&profile("pinned-test-worker", Some("models/pinned/v1")))
            .await
            .err()
            .expect("missing worker config must reject");
        assert!(pinned_error.to_string().contains("[mmli.worker] is absent"));
    }
}
