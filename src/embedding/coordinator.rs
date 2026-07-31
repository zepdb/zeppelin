//! Bounded, lease-free document enrichment with short fenced publication.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;

use crate::embedding::transform::{apply_vector_transform, load_vector_transform_mean};
use crate::embedding::{
    ArtifactChecksum, CandidateDocumentPooling, EmbeddingProfileRef, EncoderDocumentInput,
    EncoderInputRef, FdeArtifact, FdeArtifactRow, FdeFragmentRef, ImmutableArtifactBytes,
    MatrixArtifact, MatrixArtifactRow, MultiVectorEmbedding, MultiVectorEmbeddingBatch,
    MultiVectorEmbeddingFragmentRef, MultiVectorEncoder, MultiVectorEpochId,
    PhysicalInputFragmentIdentity, RecordVersionCoverage, RecordVersionRef, RetrievalUnitRecord,
    SemanticOverlayRef, FDE_ARTIFACT_FORMAT_VERSION, MATRIX_ARTIFACT_FORMAT_VERSION,
};
use crate::error::{Result, ZeppelinError};
use crate::index::late_interaction::FdeTransform;
use crate::namespace::branching::ArtifactOrigin;
use crate::storage::{CreateOnlyOutcome, NamespaceObjectFamily, ZeppelinStore};
use crate::wal::{
    EncoderInputWalFragment, InputFragmentRef, LateStateSection, LeaseManager, Manifest,
    QuarantineEvidenceRef,
};

use super::priority::QueryPriorityEncoder;

/// Deterministic identity of one complete enrichment unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EnrichmentWorkId([u8; 32]);

impl EnrichmentWorkId {
    /// Return the stable lowercase hexadecimal identity.
    #[must_use]
    pub fn to_hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct EnrichmentSourceId([u8; 32]);

/// Test-only crash boundary for restart-idempotency coverage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnrichmentCheckpoint {
    /// Stop after the immutable matrix object is durable.
    AfterMatrixPut,
    /// Stop after both immutable derived artifacts are durable.
    AfterFdePut,
    /// Stop after the new section object exists but before root CAS.
    AfterSectionPut,
}

/// Fixed coordinator bounds selected at construction.
#[derive(Clone, Debug)]
pub struct EnrichmentCoordinatorOptions {
    /// Maximum admitted work items waiting behind the single executor.
    pub queue_capacity: usize,
    /// Maximum authoritative publication attempts without re-encoding.
    pub max_retry_attempts: usize,
    /// Optional one-shot crash boundary used by integration tests.
    pub checkpoint: Option<EnrichmentCheckpoint>,
}

impl Default for EnrichmentCoordinatorOptions {
    fn default() -> Self {
        Self {
            queue_capacity: 8,
            max_retry_attempts: 3,
            checkpoint: None,
        }
    }
}

/// Result of one bounded discovery/admission pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EnrichmentAdmissionReport {
    /// Input fragments inspected within the caller's bounds.
    pub discovered_fragments: usize,
    /// Referenced content bytes inspected within the caller's bounds.
    pub inspected_bytes: u64,
    /// Newly admitted work items.
    pub admitted_fragments: usize,
    /// Referenced content bytes represented by newly admitted work.
    pub admitted_bytes: u64,
    /// Whether admission stopped because the queue was full.
    pub queue_full: bool,
}

/// Resolves one exact epoch to its owned encoder session.
#[async_trait]
pub trait MultiVectorEncoderProvider: Send + Sync {
    /// Resolve the encoder selected by this exact validated profile.
    async fn encoder_for(
        &self,
        profile: &EmbeddingProfileRef,
    ) -> Result<Arc<dyn MultiVectorEncoder>>;

    /// Stop owned sessions after the coordinator executor has drained.
    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

/// Explicit epoch-keyed registry used by tests and fixed local deployments.
#[derive(Default)]
pub struct MultiVectorEncoderRegistry {
    encoders: RwLock<HashMap<MultiVectorEpochId, Arc<dyn MultiVectorEncoder>>>,
}

impl MultiVectorEncoderRegistry {
    /// Construct an empty fail-loud registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one encoder under the exact epoch it reports.
    pub fn register(&self, encoder: Arc<dyn MultiVectorEncoder>) -> Result<()> {
        let epoch = encoder.epoch();
        let encoder = QueryPriorityEncoder::wrap(encoder);
        let mut encoders = self
            .encoders
            .write()
            .map_err(|_| ZeppelinError::Validation("encoder registry lock poisoned".to_string()))?;
        if encoders.insert(epoch, encoder).is_some() {
            return Err(ZeppelinError::Validation(format!(
                "encoder epoch {} is already registered",
                epoch.to_hex()
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl MultiVectorEncoderProvider for MultiVectorEncoderRegistry {
    async fn encoder_for(
        &self,
        profile: &EmbeddingProfileRef,
    ) -> Result<Arc<dyn MultiVectorEncoder>> {
        let encoder = self
            .encoders
            .read()
            .map_err(|_| ZeppelinError::Validation("encoder registry lock poisoned".to_string()))?
            .get(&profile.epoch.id)
            .cloned()
            .ok_or_else(|| {
                ZeppelinError::Validation(format!(
                    "no encoder registered for epoch {}",
                    profile.epoch.id.to_hex()
                ))
            })?;
        if encoder.epoch() != profile.epoch.id
            || encoder.output_dimension() != profile.epoch.vector_dimension as usize
        {
            return Err(ZeppelinError::Validation(
                "registered encoder identity or dimension mismatch".to_string(),
            ));
        }
        Ok(encoder)
    }
}

#[derive(Clone)]
struct EnrichmentWork {
    id: EnrichmentWorkId,
    source_id: EnrichmentSourceId,
    namespace: String,
    incarnation: uuid::Uuid,
    source_ref: InputFragmentRef,
    source_origin: ArtifactOrigin,
    source_checksum: u64,
    source_versions: RecordVersionCoverage,
    profile: EmbeddingProfileRef,
}

#[derive(Default)]
struct CoordinatorState {
    inflight: Mutex<HashSet<EnrichmentWorkId>>,
    terminal: Mutex<HashSet<EnrichmentSourceId>>,
    failures: Mutex<Vec<String>>,
    idle: Notify,
    failed: Notify,
}

impl CoordinatorState {
    fn insert_inflight(&self, work_id: EnrichmentWorkId) -> Result<bool> {
        self.inflight
            .lock()
            .map_err(|_| {
                ZeppelinError::Validation("enrichment inflight mutex poisoned".to_string())
            })
            .map(|mut inflight| inflight.insert(work_id))
    }

    fn remove_inflight(&self, work_id: &EnrichmentWorkId) -> Result<()> {
        self.inflight
            .lock()
            .map_err(|_| {
                ZeppelinError::Validation("enrichment inflight mutex poisoned".to_string())
            })?
            .remove(work_id);
        Ok(())
    }

    fn is_terminal(&self, source_id: &EnrichmentSourceId) -> Result<bool> {
        self.terminal
            .lock()
            .map_err(|_| {
                ZeppelinError::Validation("enrichment terminal mutex poisoned".to_string())
            })
            .map(|terminal| terminal.contains(source_id))
    }

    fn mark_terminal(&self, source_id: EnrichmentSourceId) -> Result<()> {
        self.terminal
            .lock()
            .map_err(|_| {
                ZeppelinError::Validation("enrichment terminal mutex poisoned".to_string())
            })?
            .insert(source_id);
        Ok(())
    }

    fn is_idle(&self) -> Result<bool> {
        self.inflight
            .lock()
            .map_err(|_| {
                ZeppelinError::Validation("enrichment inflight mutex poisoned".to_string())
            })
            .map(|inflight| inflight.is_empty())
    }

    fn record_failure(&self, failure: String) -> Result<()> {
        self.failures
            .lock()
            .map_err(|_| {
                ZeppelinError::Validation("enrichment failure mutex poisoned".to_string())
            })?
            .push(failure);
        self.failed.notify_one();
        Ok(())
    }

    fn first_failure(&self) -> Result<Option<String>> {
        self.failures
            .lock()
            .map_err(|_| ZeppelinError::Validation("enrichment failure mutex poisoned".to_string()))
            .map(|failures| failures.first().cloned())
    }

    fn take_failures(&self) -> Result<Vec<String>> {
        self.failures
            .lock()
            .map_err(|_| ZeppelinError::Validation("enrichment failure mutex poisoned".to_string()))
            .map(|mut failures| std::mem::take(&mut *failures))
    }
}

/// Owns one bounded, concurrency-one enrichment execution plane.
pub struct EnrichmentCoordinator {
    store: ZeppelinStore,
    sender: Option<mpsc::Sender<EnrichmentWork>>,
    state: Arc<CoordinatorState>,
    encoder_provider: Arc<dyn MultiVectorEncoderProvider>,
    executor: JoinHandle<()>,
}

impl EnrichmentCoordinator {
    /// Start the supervised executor. Construction performs no object-store I/O.
    #[must_use]
    pub fn start(
        store: ZeppelinStore,
        lease_manager: Arc<LeaseManager>,
        encoder_provider: Arc<dyn MultiVectorEncoderProvider>,
        options: EnrichmentCoordinatorOptions,
    ) -> Self {
        assert!(
            options.queue_capacity > 0,
            "enrichment queue capacity must be positive"
        );
        assert!(
            options.max_retry_attempts > 0,
            "enrichment retry bound must be positive"
        );
        let (sender, mut receiver) = mpsc::channel::<EnrichmentWork>(options.queue_capacity);
        let state = Arc::new(CoordinatorState::default());
        let executor_state = Arc::clone(&state);
        let executor_store = store.clone();
        let executor_provider = Arc::clone(&encoder_provider);
        let executor = tokio::spawn(async move {
            while let Some(work) = receiver.recv().await {
                let work_id = work.id;
                let source_id = work.source_id;
                if let Err(error) = execute_work(
                    &executor_store,
                    lease_manager.as_ref(),
                    executor_provider.as_ref(),
                    options.checkpoint,
                    options.max_retry_attempts,
                    &work,
                )
                .await
                {
                    tracing::error!(
                        work_id = %work_id.to_hex(),
                        retryable = is_transient_failure(&error),
                        error = %error,
                        "semantic enrichment work failed"
                    );
                    if suppress_rediscovery(&error) {
                        if let Err(lock_error) = executor_state.mark_terminal(source_id) {
                            tracing::error!(
                                work_id = %work_id.to_hex(),
                                error = %lock_error,
                                "semantic enrichment terminal state is unavailable"
                            );
                            return;
                        }
                    }
                    if let Err(lock_error) = executor_state.record_failure(error.to_string()) {
                        tracing::error!(
                            work_id = %work_id.to_hex(),
                            error = %lock_error,
                            "semantic enrichment executor failure state is unavailable"
                        );
                        return;
                    }
                }
                if let Err(error) = executor_state.remove_inflight(&work_id) {
                    tracing::error!(
                        work_id = %work_id.to_hex(),
                        error = %error,
                        "semantic enrichment executor inflight state is unavailable"
                    );
                    return;
                }
                executor_state.idle.notify_one();
            }
            executor_state.idle.notify_one();
        });
        Self {
            store,
            sender: Some(sender),
            state,
            encoder_provider,
            executor,
        }
    }

    /// Discover and `try_send` bounded work without awaiting encoder execution.
    pub async fn discover_and_admit(
        &self,
        namespace: &str,
        incarnation: uuid::Uuid,
        max_fragments: usize,
        max_bytes: u64,
    ) -> Result<EnrichmentAdmissionReport> {
        let sender = self.sender.as_ref().ok_or_else(|| {
            ZeppelinError::Validation("enrichment coordinator is shut down".to_string())
        })?;
        let (manifest, _) =
            Manifest::read_versioned_required_for_incarnation(&self.store, namespace, incarnation)
                .await?;
        let Some(section) = manifest.load_late_state(&self.store).await? else {
            return Ok(EnrichmentAdmissionReport::default());
        };
        let Some(profile) = section.active_profile.as_ref() else {
            return Ok(EnrichmentAdmissionReport::default());
        };
        let mut report = EnrichmentAdmissionReport::default();
        let mut inspected_bytes = 0_u64;
        for source_ref in &manifest.input_fragments {
            if source_ref.upsert_count == 0 {
                continue;
            }
            let source_origin = manifest.input_fragment_origin(source_ref)?;
            let source_key =
                EncoderInputWalFragment::s3_key(source_origin.namespace.as_str(), &source_ref.id);
            if fragment_is_fast_complete(&section, &source_key, source_ref, profile) {
                continue;
            }
            if report.discovered_fragments >= max_fragments {
                break;
            }
            let next_inspected_bytes = inspected_bytes
                .checked_add(source_ref.referenced_content_bytes)
                .ok_or_else(|| {
                    ZeppelinError::Validation(
                        "enrichment discovery byte count overflows u64".to_string(),
                    )
                })?;
            if next_inspected_bytes > max_bytes {
                break;
            }
            inspected_bytes = next_inspected_bytes;
            report.discovered_fragments += 1;
            report.inspected_bytes = inspected_bytes;
            let source =
                read_input_fragment_checked(&self.store, source_ref, &source_origin).await?;
            let source_id = enrichment_source_id(
                incarnation,
                &source_origin,
                source_ref,
                source.checksum,
                profile,
            );
            if self.state.is_terminal(&source_id)? {
                continue;
            }
            let source_versions = RecordVersionCoverage {
                records: source
                    .upserts
                    .iter()
                    .enumerate()
                    .map(|(ordinal, record)| source_record_version(source_ref, ordinal, record))
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .filter(|version| {
                        !version_is_settled(
                            &section,
                            &source_key,
                            source_ref.id,
                            source.checksum,
                            profile,
                            version,
                        )
                    })
                    .collect(),
            };
            if source_versions.records.is_empty() {
                continue;
            }
            let work_id = enrichment_work_id(
                incarnation,
                &source_origin,
                source_ref,
                source.checksum,
                &source_versions,
                profile,
            );
            if !self.state.insert_inflight(work_id)? {
                continue;
            }
            let work = EnrichmentWork {
                id: work_id,
                source_id,
                namespace: namespace.to_string(),
                incarnation,
                source_ref: source_ref.clone(),
                source_origin,
                source_checksum: source.checksum,
                source_versions,
                profile: profile.clone(),
            };
            match sender.try_send(work) {
                Ok(()) => {
                    report.admitted_fragments += 1;
                    report.admitted_bytes = report
                        .admitted_bytes
                        .checked_add(source_ref.referenced_content_bytes)
                        .ok_or_else(|| {
                            ZeppelinError::Validation(
                                "enrichment admitted-byte count overflows u64".to_string(),
                            )
                        })?;
                }
                Err(mpsc::error::TrySendError::Full(work)) => {
                    self.state.remove_inflight(&work.id)?;
                    report.queue_full = true;
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(work)) => {
                    self.state.remove_inflight(&work.id)?;
                    return Err(ZeppelinError::Validation(
                        "enrichment executor stopped".to_string(),
                    ));
                }
            }
        }
        Ok(report)
    }

    /// Wait until all admitted work completes and surface executor failures.
    pub async fn wait_for_idle(&self) -> Result<()> {
        loop {
            if self.state.is_idle()? {
                break;
            }
            self.state.idle.notified().await;
        }
        executor_failure_result(self.state.take_failures()?)
    }

    /// Wait for the first executor failure so the owning maintenance task can fail loud.
    pub async fn wait_for_executor_failure(&self) -> ZeppelinError {
        loop {
            match self.state.first_failure() {
                Ok(Some(failure)) => {
                    return ZeppelinError::Validation(format!(
                        "semantic enrichment executor failed: {failure}"
                    ));
                }
                Ok(None) => self.state.failed.notified().await,
                Err(error) => return error,
            }
        }
    }

    /// Close admission and join the executor after queued work drains.
    pub async fn shutdown(mut self) -> Result<()> {
        self.sender.take();
        let executor_result = self.executor.await.map_err(|error| {
            ZeppelinError::Validation(format!("enrichment executor join failed: {error}"))
        });
        let provider_result = self.encoder_provider.shutdown().await;
        executor_result?;
        provider_result?;
        executor_failure_result(self.state.take_failures()?)
    }
}

fn executor_failure_result(failures: Vec<String>) -> Result<()> {
    if failures.is_empty() {
        Ok(())
    } else {
        Err(ZeppelinError::Validation(format!(
            "enrichment executor failed: {}",
            failures.join("; ")
        )))
    }
}

struct PoisonedRow {
    version: RecordVersionRef,
    failure_class: &'static str,
}

struct EncodedRows {
    healthy_versions: RecordVersionCoverage,
    embeddings: Vec<MultiVectorEmbedding>,
    poisoned: Vec<PoisonedRow>,
}

struct LoadedWorkInputs {
    source: EncoderInputWalFragment,
    inputs: Vec<EncoderDocumentInput>,
    exact_mean: Option<Vec<f32>>,
    candidate_mean: Option<Vec<f32>>,
    fde_transform: FdeTransform,
}

struct PreparedArtifact {
    key: String,
    bytes: bytes::Bytes,
    checksum: ArtifactChecksum,
}

impl PreparedArtifact {
    fn from_immutable(key: String, artifact: ImmutableArtifactBytes) -> Self {
        Self {
            key,
            checksum: artifact.checksum(),
            bytes: artifact.bytes().clone(),
        }
    }

    fn from_bytes(key: String, bytes: bytes::Bytes) -> Self {
        let checksum = ArtifactChecksum::digest(&bytes);
        Self {
            key,
            bytes,
            checksum,
        }
    }
}

struct PreparedQuarantine {
    artifact: PreparedArtifact,
    evidence: QuarantineEvidenceRef,
    failure_class: &'static str,
}

struct PreparedEnrichment {
    matrix: Option<PreparedArtifact>,
    fde: Option<PreparedArtifact>,
    overlay: Option<SemanticOverlayRef>,
    quarantines: Vec<PreparedQuarantine>,
    quarantined_record_count: u64,
}

async fn execute_work(
    store: &ZeppelinStore,
    lease_manager: &LeaseManager,
    encoder_provider: &dyn MultiVectorEncoderProvider,
    checkpoint: Option<EnrichmentCheckpoint>,
    max_retry_attempts: usize,
    work: &EnrichmentWork,
) -> Result<()> {
    let encoder = encoder_provider.encoder_for(&work.profile).await?;
    if encoder.epoch() != work.profile.epoch.id
        || encoder.output_dimension() != work.profile.epoch.vector_dimension as usize
    {
        return Err(ZeppelinError::Validation(
            "enrichment encoder identity or dimension mismatch".to_string(),
        ));
    }
    let loaded = load_work_inputs_bounded(store, work, max_retry_attempts).await?;
    let encoded = encode_rows(
        encoder.as_ref(),
        &work.profile,
        &loaded.inputs,
        &work.source_versions,
    )
    .await?;
    let prepared = prepare_encoded_work(
        work,
        &loaded.source,
        encoded,
        loaded.exact_mean.as_deref(),
        loaded.candidate_mean.as_deref(),
        &loaded.fde_transform,
    )
    .map_err(terminal_post_encode_error)?;
    persist_and_publish_prepared(
        store,
        lease_manager,
        work,
        &prepared,
        checkpoint,
        max_retry_attempts,
    )
    .await
    .map_err(terminal_post_encode_error)
}

async fn load_work_inputs_bounded(
    store: &ZeppelinStore,
    work: &EnrichmentWork,
    max_retry_attempts: usize,
) -> Result<LoadedWorkInputs> {
    for attempt in 1..=max_retry_attempts {
        match load_work_inputs_once(store, work).await {
            Ok(loaded) => return Ok(loaded),
            Err(error) if is_transient_failure(&error) && attempt < max_retry_attempts => {
                tracing::warn!(
                    work_id = %work.id.to_hex(),
                    attempt,
                    max_attempts = max_retry_attempts,
                    error = %error,
                    "retrying semantic enrichment input load"
                );
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("positive enrichment retry bound must return from the loop")
}

async fn load_work_inputs_once(
    store: &ZeppelinStore,
    work: &EnrichmentWork,
) -> Result<LoadedWorkInputs> {
    let (manifest, _) =
        Manifest::read_versioned_required_for_incarnation(store, &work.namespace, work.incarnation)
            .await?;
    let section = manifest.load_late_state(store).await?.ok_or_else(|| {
        ZeppelinError::Validation("enrichment requires an active late-state section".to_string())
    })?;
    if section.active_profile.as_ref() != Some(&work.profile) {
        return Err(ZeppelinError::Validation(
            "enrichment profile changed before execution".to_string(),
        ));
    }
    let source = read_input_fragment_checked(store, &work.source_ref, &work.source_origin).await?;
    if source.checksum != work.source_checksum {
        return Err(ZeppelinError::Serialization(format!(
            "input fragment {} checksum changed after admission",
            work.source_ref.id
        )));
    }
    let section_reference = manifest.late_state.as_ref().ok_or_else(|| {
        ZeppelinError::Validation("enrichment late-state reference disappeared".to_string())
    })?;
    let section_origin = manifest.late_section_origin(section_reference)?;
    let inputs = resolve_inputs(
        store,
        &section,
        &section_origin,
        &source,
        &work.source_ref,
        &work.source_versions,
    )
    .await?;
    let exact_mean = load_vector_transform_mean(
        store,
        &work.profile.epoch.exact_scoring_transform,
        work.profile.epoch.vector_dimension as usize,
    )
    .await?;
    let candidate_mean = load_vector_transform_mean(
        store,
        &work.profile.fde.candidate_vector_transform,
        work.profile.epoch.vector_dimension as usize,
    )
    .await?;
    let transform_bytes = store.get(&work.profile.fde.transform_artifact.key).await?;
    verify_ref_bytes(
        &transform_bytes,
        work.profile.fde.transform_artifact.size_bytes,
        work.profile.fde.transform_artifact.checksum,
        "FDE transform",
    )?;
    let fde_transform = FdeTransform::from_bytes(&transform_bytes)?;
    if fde_transform.params() != work.profile.fde.params {
        return Err(ZeppelinError::Validation(
            "enrichment FDE transform recipe mismatch".to_string(),
        ));
    }
    Ok(LoadedWorkInputs {
        source,
        inputs,
        exact_mean,
        candidate_mean,
        fde_transform,
    })
}

#[allow(clippy::too_many_arguments)]
fn prepare_encoded_work(
    work: &EnrichmentWork,
    source: &EncoderInputWalFragment,
    encoded: EncodedRows,
    exact_mean: Option<&[f32]>,
    candidate_mean: Option<&[f32]>,
    fde_transform: &FdeTransform,
) -> Result<PreparedEnrichment> {
    let quarantines = prepare_quarantines(work, &encoded.poisoned)?;
    let quarantined_record_count = quarantines.iter().try_fold(0_u64, |total, prepared| {
        total
            .checked_add(prepared.evidence.failed_versions.records.len() as u64)
            .ok_or_else(|| {
                ZeppelinError::Validation(
                    "enrichment quarantined row count overflows u64".to_string(),
                )
            })
    })?;
    if encoded.healthy_versions.records.is_empty() {
        return Ok(PreparedEnrichment {
            matrix: None,
            fde: None,
            overlay: None,
            quarantines,
            quarantined_record_count,
        });
    }
    let healthy_records = encoded
        .healthy_versions
        .records
        .iter()
        .map(|version| source_record_for_version(source, &work.source_ref, version))
        .collect::<Result<Vec<_>>>()?;

    let raw_embeddings = encoded.embeddings;
    let exact_embeddings = raw_embeddings
        .iter()
        .map(|embedding| {
            apply_vector_transform(
                embedding,
                &work.profile.epoch.exact_scoring_transform,
                exact_mean,
                work.profile.epoch.max_document_vectors as usize,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let matrix_rows = healthy_records
        .iter()
        .zip(exact_embeddings)
        .map(|(record, embedding)| MatrixArtifactRow::new(record.content_hash, embedding))
        .collect();
    let matrix = MatrixArtifact::new(
        work.profile.epoch.matrix_dtype,
        work.profile.epoch.id,
        source.checksum,
        work.profile.epoch.vector_dimension as usize,
        matrix_rows,
    )?;
    let matrix_artifact = matrix.to_bytes()?;
    let matrix_key = LateStateSection::artifact_s3_key(
        &work.namespace,
        NamespaceObjectFamily::MatrixFragment,
        matrix_artifact.checksum(),
    );

    let candidate_rows = healthy_records
        .iter()
        .zip(&raw_embeddings)
        .map(|(record, embedding)| {
            apply_vector_transform(
                embedding,
                &work.profile.fde.candidate_vector_transform,
                candidate_mean,
                work.profile.epoch.max_document_vectors as usize,
            )
            .map(|embedding| MatrixArtifactRow::new(record.content_hash, embedding))
        })
        .collect::<Result<Vec<_>>>()?;
    let candidate_f16 = MatrixArtifact::new(
        crate::embedding::MatrixDtype::F16,
        work.profile.epoch.id,
        source.checksum,
        work.profile.epoch.vector_dimension as usize,
        candidate_rows,
    )?;
    let candidate_f16_bytes = candidate_f16.to_bytes()?;
    let candidate_f16 = MatrixArtifact::from_bytes(
        candidate_f16_bytes.bytes(),
        candidate_f16_bytes.checksum(),
        crate::embedding::MatrixDtype::F16,
        work.profile.epoch.id,
        source.checksum,
        work.profile.epoch.vector_dimension as usize,
        healthy_records.len(),
        work.profile.epoch.max_document_vectors as usize,
    )?;
    let mut fde_rows = Vec::with_capacity(raw_embeddings.len());
    for (record, candidate) in healthy_records.iter().zip(candidate_f16.rows()) {
        let pooled = apply_candidate_document_pooling(
            candidate.embedding(),
            work.profile.fde.candidate_document_pooling,
        )?;
        let values = fde_transform.encode_document(&pooled.matrix_ref()?)?;
        fde_rows.push(FdeArtifactRow::new(
            record.content_hash,
            values,
            fde_transform.output_dimension(),
        )?);
    }
    let fde = FdeArtifact::new(
        work.profile.fde.generation,
        matrix_artifact.checksum(),
        fde_transform.output_dimension(),
        fde_rows,
    )?;
    let fde_artifact = fde.to_bytes()?;
    let fde_key = LateStateSection::artifact_s3_key(
        &work.namespace,
        NamespaceObjectFamily::FdeFragment,
        fde_artifact.checksum(),
    );

    let row_count = u32::try_from(healthy_records.len()).map_err(|_| {
        ZeppelinError::Validation("enrichment source row count exceeds u32".to_string())
    })?;
    let total_vectors = matrix.rows().iter().try_fold(0_u64, |total, row| {
        total
            .checked_add(row.embedding().vector_count() as u64)
            .ok_or_else(|| {
                ZeppelinError::Validation(
                    "enrichment matrix total vector count overflows u64".to_string(),
                )
            })
    })?;
    let overlay = SemanticOverlayRef {
        source_fragment: PhysicalInputFragmentIdentity {
            key: EncoderInputWalFragment::s3_key(work.source_origin.namespace.as_str(), &source.id),
            id: source.id,
            checksum: source.checksum,
            size_bytes: work.source_ref.size_bytes,
            artifact_origin: None,
        },
        semantic_epoch: work.profile.epoch.id,
        fde_generation: work.profile.fde.generation,
        embeddings: MultiVectorEmbeddingFragmentRef {
            key: matrix_key,
            checksum: matrix_artifact.checksum(),
            source_fragment_checksum: source.checksum,
            semantic_epoch: work.profile.epoch.id,
            row_count,
            total_vectors,
            vector_dimension: work.profile.epoch.vector_dimension,
            dtype: work.profile.epoch.matrix_dtype,
            format_version: u32::from(MATRIX_ARTIFACT_FORMAT_VERSION),
            size_bytes: matrix_artifact.bytes().len() as u64,
            artifact_origin: None,
        },
        fde_vectors: FdeFragmentRef {
            key: fde_key,
            checksum: fde_artifact.checksum(),
            embedding_fragment_checksum: matrix_artifact.checksum(),
            generation: work.profile.fde.generation,
            row_count,
            fde_dimension: u32::try_from(fde_transform.output_dimension()).map_err(|_| {
                ZeppelinError::Validation("enrichment FDE dimension exceeds u32".to_string())
            })?,
            format_version: u32::from(FDE_ARTIFACT_FORMAT_VERSION),
            size_bytes: fde_artifact.bytes().len() as u64,
            artifact_origin: None,
        },
        covered_versions: encoded.healthy_versions,
        published_at_generation: 0,
    };

    Ok(PreparedEnrichment {
        matrix: Some(PreparedArtifact::from_immutable(
            overlay.embeddings.key.clone(),
            matrix_artifact,
        )),
        fde: Some(PreparedArtifact::from_immutable(
            overlay.fde_vectors.key.clone(),
            fde_artifact,
        )),
        overlay: Some(overlay),
        quarantines,
        quarantined_record_count,
    })
}

fn prepare_quarantines(
    work: &EnrichmentWork,
    poisoned: &[PoisonedRow],
) -> Result<Vec<PreparedQuarantine>> {
    #[derive(Serialize)]
    struct Evidence<'a> {
        work_id: [u8; 32],
        source_key: &'a str,
        source_id: ulid::Ulid,
        source_checksum: u64,
        semantic_epoch: MultiVectorEpochId,
        fde_generation: crate::embedding::FdeGenerationId,
        failed_versions: &'a RecordVersionCoverage,
        failure_class: &'static str,
    }

    let mut by_class = BTreeMap::<&'static str, Vec<RecordVersionRef>>::new();
    for row in poisoned {
        by_class
            .entry(row.failure_class)
            .or_default()
            .push(row.version.clone());
    }

    let source_key =
        EncoderInputWalFragment::s3_key(work.source_origin.namespace.as_str(), &work.source_ref.id);
    by_class
        .into_iter()
        .map(|(failure_class, records)| {
            let failed_versions = RecordVersionCoverage { records };
            let evidence_work_id = enrichment_work_id(
                work.incarnation,
                &work.source_origin,
                &work.source_ref,
                work.source_checksum,
                &failed_versions,
                &work.profile,
            );
            let payload = rmp_serde::to_vec(&Evidence {
                work_id: evidence_work_id.0,
                source_key: &source_key,
                source_id: work.source_ref.id,
                source_checksum: work.source_checksum,
                semantic_epoch: work.profile.epoch.id,
                fde_generation: work.profile.fde.generation,
                failed_versions: &failed_versions,
                failure_class,
            })
            .map_err(|error| {
                ZeppelinError::Serialization(format!(
                    "semantic quarantine evidence serialization failed: {error}"
                ))
            })?;
            let mut evidence_bytes = Vec::with_capacity(5 + payload.len());
            evidence_bytes.extend_from_slice(b"ZEQ1");
            evidence_bytes.push(1);
            evidence_bytes.extend_from_slice(&payload);
            let evidence_bytes = bytes::Bytes::from(evidence_bytes);
            let checksum = ArtifactChecksum::digest(&evidence_bytes);
            let key = LateStateSection::artifact_s3_key(
                &work.namespace,
                NamespaceObjectFamily::Quarantine,
                checksum,
            );
            let evidence = QuarantineEvidenceRef {
                key: key.clone(),
                checksum,
                size_bytes: evidence_bytes.len() as u64,
                work_id: evidence_work_id.0,
                source_fragment: PhysicalInputFragmentIdentity {
                    key: source_key.clone(),
                    id: work.source_ref.id,
                    checksum: work.source_checksum,
                    size_bytes: work.source_ref.size_bytes,
                    artifact_origin: None,
                },
                semantic_epoch: work.profile.epoch.id,
                fde_generation: work.profile.fde.generation,
                failed_versions,
                artifact_origin: None,
            };
            Ok(PreparedQuarantine {
                artifact: PreparedArtifact::from_bytes(key, evidence_bytes),
                evidence,
                failure_class,
            })
        })
        .collect()
}

async fn resolve_inputs(
    store: &ZeppelinStore,
    section: &LateStateSection,
    section_origin: &ArtifactOrigin,
    source: &EncoderInputWalFragment,
    source_ref: &InputFragmentRef,
    versions: &RecordVersionCoverage,
) -> Result<Vec<EncoderDocumentInput>> {
    let mut inputs = Vec::with_capacity(versions.records.len());
    for version in &versions.records {
        let record = source_record_for_version(source, source_ref, version)?;
        let image_ref = match &record.input {
            EncoderInputRef::Text { .. } => None,
            EncoderInputRef::Image { image } | EncoderInputRef::ImageText { image, .. } => {
                Some(image)
            }
        };
        let image_bytes = if let Some(image) = image_ref {
            let inventory = section
                .source_inventory
                .iter()
                .find(|candidate| {
                    candidate.key == image.key
                        && candidate.checksum == image.checksum
                        && candidate.size_bytes == image.encoded_size_bytes
                        && candidate.media_type == image.media_type
                })
                .ok_or_else(|| {
                    ZeppelinError::Validation(format!(
                        "encoder image source {} is absent from late-state inventory",
                        image.key
                    ))
                })?;
            Some(
                section
                    .read_source_checked(store, inventory, section_origin)
                    .await?,
            )
        } else {
            None
        };
        inputs.push(EncoderDocumentInput::new(
            record.input.clone(),
            record.content_hash,
            image_bytes,
        )?);
    }
    Ok(inputs)
}

async fn encode_rows(
    encoder: &dyn MultiVectorEncoder,
    profile: &EmbeddingProfileRef,
    inputs: &[EncoderDocumentInput],
    versions: &RecordVersionCoverage,
) -> Result<EncodedRows> {
    match encoder.encode_documents(inputs).await {
        Ok(batch) => Ok(EncodedRows {
            healthy_versions: versions.clone(),
            embeddings: validated_embeddings(batch, profile, inputs.len())?,
            poisoned: Vec::new(),
        }),
        Err(batch_error) => {
            let Some(batch_class) = poison_failure_class(&batch_error) else {
                return Err(batch_error);
            };
            if inputs.len() == 1 {
                return Ok(EncodedRows {
                    healthy_versions: RecordVersionCoverage::default(),
                    embeddings: Vec::new(),
                    poisoned: vec![PoisonedRow {
                        version: versions.records[0].clone(),
                        failure_class: batch_class,
                    }],
                });
            }

            let mut healthy_versions = Vec::with_capacity(inputs.len());
            let mut embeddings = Vec::with_capacity(inputs.len());
            let mut poisoned = Vec::new();
            for (input, version) in inputs.iter().zip(&versions.records) {
                match encoder.encode_documents(std::slice::from_ref(input)).await {
                    Ok(batch) => {
                        let mut row = validated_embeddings(batch, profile, 1)?;
                        healthy_versions.push(version.clone());
                        embeddings.push(row.pop().ok_or_else(|| {
                            ZeppelinError::Validation(
                                "single-row encoder response was empty".to_string(),
                            )
                        })?);
                    }
                    Err(error) => {
                        let Some(failure_class) = poison_failure_class(&error) else {
                            return Err(error);
                        };
                        poisoned.push(PoisonedRow {
                            version: version.clone(),
                            failure_class,
                        });
                    }
                }
            }
            if poisoned.is_empty() {
                return Err(batch_error);
            }
            Ok(EncodedRows {
                healthy_versions: RecordVersionCoverage {
                    records: healthy_versions,
                },
                embeddings,
                poisoned,
            })
        }
    }
}

fn validated_embeddings(
    batch: MultiVectorEmbeddingBatch,
    profile: &EmbeddingProfileRef,
    expected_count: usize,
) -> Result<Vec<MultiVectorEmbedding>> {
    if batch.epoch() != profile.epoch.id
        || batch.vector_dimension() != profile.epoch.vector_dimension as usize
        || batch.embeddings().len() != expected_count
    {
        return Err(ZeppelinError::Validation(
            "enrichment encoder returned the wrong epoch, shape, or row count".to_string(),
        ));
    }
    Ok(batch.into_embeddings())
}

fn source_record_version(
    source_ref: &InputFragmentRef,
    ordinal: usize,
    record: &RetrievalUnitRecord,
) -> Result<RecordVersionRef> {
    Ok(RecordVersionRef {
        row_ordinal: u32::try_from(ordinal).map_err(|_| {
            ZeppelinError::Validation("enrichment source row ordinal exceeds u32".to_string())
        })?,
        record_id: record.id.clone(),
        content_hash: record.content_hash,
        sequence: source_ref.sequence_number,
    })
}

fn source_record_for_version<'a>(
    source: &'a EncoderInputWalFragment,
    source_ref: &InputFragmentRef,
    version: &RecordVersionRef,
) -> Result<&'a RetrievalUnitRecord> {
    let ordinal = usize::try_from(version.row_ordinal).map_err(|_| {
        ZeppelinError::Validation("enrichment source row ordinal exceeds usize".to_string())
    })?;
    let record = source.upserts.get(ordinal).ok_or_else(|| {
        ZeppelinError::Validation("enrichment source row ordinal is out of bounds".to_string())
    })?;
    if source_record_version(source_ref, ordinal, record)? != *version {
        return Err(ZeppelinError::Validation(
            "enrichment source version identity mismatch".to_string(),
        ));
    }
    Ok(record)
}

fn fragment_is_fast_complete(
    section: &LateStateSection,
    source_key: &str,
    source_ref: &InputFragmentRef,
    profile: &EmbeddingProfileRef,
) -> bool {
    let mut checksum = None;
    let mut rows = HashSet::new();
    let mut observe = |identity: &PhysicalInputFragmentIdentity,
                       epoch: MultiVectorEpochId,
                       generation: crate::embedding::FdeGenerationId,
                       coverage: &RecordVersionCoverage|
     -> bool {
        if identity.key != source_key
            || identity.id != source_ref.id
            || epoch != profile.epoch.id
            || generation != profile.fde.generation
        {
            return true;
        }
        if identity.size_bytes != source_ref.size_bytes
            || checksum.is_some_and(|expected| expected != identity.checksum)
        {
            return false;
        }
        checksum = Some(identity.checksum);
        for version in &coverage.records {
            if version.sequence != source_ref.sequence_number
                || version.row_ordinal as usize >= source_ref.upsert_count
                || !rows.insert(version.row_ordinal)
            {
                return false;
            }
        }
        true
    };
    for overlay in &section.semantic_overlays {
        if !observe(
            &overlay.source_fragment,
            overlay.semantic_epoch,
            overlay.fde_generation,
            &overlay.covered_versions,
        ) {
            return false;
        }
    }
    for evidence in &section.quarantine_evidence {
        if !observe(
            &evidence.source_fragment,
            evidence.semantic_epoch,
            evidence.fde_generation,
            &evidence.failed_versions,
        ) {
            return false;
        }
    }
    checksum.is_some() && rows.len() == source_ref.upsert_count
}

fn version_is_settled(
    section: &LateStateSection,
    source_key: &str,
    source_id: ulid::Ulid,
    source_checksum: u64,
    profile: &EmbeddingProfileRef,
    version: &RecordVersionRef,
) -> bool {
    section.semantic_overlays.iter().any(|overlay| {
        overlay.source_fragment.key == source_key
            && overlay.source_fragment.id == source_id
            && overlay.source_fragment.checksum == source_checksum
            && overlay.semantic_epoch == profile.epoch.id
            && overlay.fde_generation == profile.fde.generation
            && overlay.covered_versions.records.contains(version)
    }) || section.quarantine_evidence.iter().any(|evidence| {
        evidence.source_fragment.key == source_key
            && evidence.source_fragment.id == source_id
            && evidence.source_fragment.checksum == source_checksum
            && evidence.semantic_epoch == profile.epoch.id
            && evidence.fde_generation == profile.fde.generation
            && evidence.failed_versions.records.contains(version)
    })
}

async fn persist_and_publish_prepared(
    store: &ZeppelinStore,
    lease_manager: &LeaseManager,
    work: &EnrichmentWork,
    prepared: &PreparedEnrichment,
    checkpoint: Option<EnrichmentCheckpoint>,
    max_retry_attempts: usize,
) -> Result<()> {
    for attempt in 1..=max_retry_attempts {
        match persist_and_publish_once(store, lease_manager, work, prepared, checkpoint).await {
            Ok(()) => {
                if prepared.quarantined_record_count != 0 {
                    crate::metrics::SEMANTIC_ENRICHMENT_QUARANTINED_RECORDS_TOTAL
                        .with_label_values(&[work.namespace.as_str()])
                        .inc_by(prepared.quarantined_record_count);
                    for quarantine in &prepared.quarantines {
                        tracing::warn!(
                            work_id = %work.id.to_hex(),
                            failure_class = quarantine.failure_class,
                            quarantined_records =
                                quarantine.evidence.failed_versions.records.len(),
                            "semantic enrichment rows quarantined"
                        );
                    }
                }
                return Ok(());
            }
            Err(error) if is_transient_failure(&error) && attempt < max_retry_attempts => {
                tracing::warn!(
                    work_id = %work.id.to_hex(),
                    attempt,
                    max_attempts = max_retry_attempts,
                    error = %error,
                    "retrying prepared semantic enrichment publication"
                );
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("positive enrichment retry bound must return from the loop")
}

async fn persist_and_publish_once(
    store: &ZeppelinStore,
    lease_manager: &LeaseManager,
    work: &EnrichmentWork,
    prepared: &PreparedEnrichment,
    checkpoint: Option<EnrichmentCheckpoint>,
) -> Result<()> {
    if let Some(matrix) = &prepared.matrix {
        put_create_verified(store, matrix).await?;
        checkpoint_at(checkpoint, EnrichmentCheckpoint::AfterMatrixPut)?;
    }
    if let Some(fde) = &prepared.fde {
        put_create_verified(store, fde).await?;
        checkpoint_at(checkpoint, EnrichmentCheckpoint::AfterFdePut)?;
    }
    for quarantine in &prepared.quarantines {
        put_create_verified(store, &quarantine.artifact).await?;
    }
    if let Some(overlay) = &prepared.overlay {
        publish_overlay_once(store, lease_manager, work, overlay.clone(), checkpoint).await?;
    }
    for quarantine in &prepared.quarantines {
        publish_quarantine_once(store, lease_manager, work, quarantine.evidence.clone()).await?;
    }
    Ok(())
}

async fn publish_overlay_once(
    store: &ZeppelinStore,
    lease_manager: &LeaseManager,
    work: &EnrichmentWork,
    overlay: SemanticOverlayRef,
    checkpoint: Option<EnrichmentCheckpoint>,
) -> Result<()> {
    let lease = lease_manager.acquire(&work.namespace).await?;
    let publication = Manifest::publish_semantic_overlay(
        store,
        lease_manager,
        &lease,
        &work.namespace,
        work.incarnation,
        &work.profile,
        &work.source_origin,
        overlay,
        1,
        || checkpoint_at(checkpoint, EnrichmentCheckpoint::AfterSectionPut),
    )
    .await;
    let release_lease = publication
        .as_ref()
        .map(|(_, renewed)| renewed)
        .unwrap_or(&lease);
    if let Err(error) = lease_manager.release(&work.namespace, release_lease).await {
        tracing::warn!(
            namespace = %work.namespace,
            error = %error,
            "semantic overlay lease release failed (best effort)"
        );
    }
    publication.map(|_| ())
}

async fn publish_quarantine_once(
    store: &ZeppelinStore,
    lease_manager: &LeaseManager,
    work: &EnrichmentWork,
    evidence: QuarantineEvidenceRef,
) -> Result<()> {
    let lease = lease_manager.acquire(&work.namespace).await?;
    let publication = Manifest::publish_semantic_quarantine(
        store,
        lease_manager,
        &lease,
        &work.namespace,
        work.incarnation,
        &work.profile,
        &work.source_origin,
        evidence,
        1,
    )
    .await;
    let release_lease = publication
        .as_ref()
        .map(|(_, renewed)| renewed)
        .unwrap_or(&lease);
    if let Err(error) = lease_manager.release(&work.namespace, release_lease).await {
        tracing::warn!(
            namespace = %work.namespace,
            error = %error,
            "semantic quarantine lease release failed (best effort)"
        );
    }
    publication.map(|_| ())
}

fn terminal_post_encode_error(error: ZeppelinError) -> ZeppelinError {
    ZeppelinError::Validation(format!(
        "semantic enrichment failed after encoder completion: {error}"
    ))
}

fn apply_candidate_document_pooling(
    embedding: &MultiVectorEmbedding,
    recipe: CandidateDocumentPooling,
) -> Result<MultiVectorEmbedding> {
    recipe.validate()?;
    let CandidateDocumentPooling::ContiguousMean { factor } = recipe else {
        return Ok(embedding.clone());
    };
    let factor = usize::from(factor);
    let dimension = embedding.vector_dimension();
    let vector_count = embedding.vector_count().div_ceil(factor);
    let scalar_count = vector_count.checked_mul(dimension).ok_or_else(|| {
        ZeppelinError::Validation("pooled candidate matrix scalar count overflows".to_string())
    })?;
    let mut values = Vec::with_capacity(scalar_count);
    let matrix = embedding.matrix_ref()?;
    for start in (0..embedding.vector_count()).step_by(factor) {
        let end = (start + factor).min(embedding.vector_count());
        let divisor = (end - start) as f64;
        for coordinate in 0..dimension {
            let sum = (start..end)
                .map(|row| f64::from(matrix.row(row)[coordinate]))
                .sum::<f64>();
            values.push((sum / divisor) as f32);
        }
    }
    MultiVectorEmbedding::new(values, vector_count, dimension, vector_count)
}

async fn put_create_verified(store: &ZeppelinStore, artifact: &PreparedArtifact) -> Result<()> {
    match store
        .put_create_outcome(&artifact.key, artifact.bytes.clone())
        .await?
    {
        CreateOnlyOutcome::Created { .. } => Ok(()),
        CreateOnlyOutcome::AlreadyExists => {
            let existing = store.get(&artifact.key).await?;
            if ArtifactChecksum::digest(&existing) != artifact.checksum
                || existing != artifact.bytes
            {
                return Err(ZeppelinError::Serialization(format!(
                    "content-address collision at {}",
                    artifact.key
                )));
            }
            Ok(())
        }
    }
}

fn verify_ref_bytes(
    bytes: &[u8],
    expected_size: u64,
    expected_checksum: ArtifactChecksum,
    kind: &str,
) -> Result<()> {
    if u64::try_from(bytes.len()).ok() != Some(expected_size)
        || ArtifactChecksum::digest(bytes) != expected_checksum
    {
        return Err(ZeppelinError::Serialization(format!(
            "{kind} size or checksum mismatch"
        )));
    }
    Ok(())
}

async fn read_input_fragment_checked(
    store: &ZeppelinStore,
    reference: &InputFragmentRef,
    origin: &ArtifactOrigin,
) -> Result<EncoderInputWalFragment> {
    let key = EncoderInputWalFragment::s3_key(origin.namespace.as_str(), &reference.id);
    let bytes = store.get(&key).await?;
    if u64::try_from(bytes.len()).ok() != Some(reference.size_bytes) {
        return Err(ZeppelinError::Serialization(format!(
            "input fragment {} size mismatch",
            reference.id
        )));
    }
    let source = EncoderInputWalFragment::from_bytes(&bytes)?;
    if source.id != reference.id
        || source.upserts.len() != reference.upsert_count
        || source.deletes.len() != reference.delete_count
    {
        return Err(ZeppelinError::Serialization(format!(
            "input fragment {} metadata mismatch",
            reference.id
        )));
    }
    Ok(source)
}

fn enrichment_work_id(
    incarnation: uuid::Uuid,
    origin: &ArtifactOrigin,
    reference: &InputFragmentRef,
    source_checksum: u64,
    versions: &RecordVersionCoverage,
    profile: &EmbeddingProfileRef,
) -> EnrichmentWorkId {
    let mut hasher = Sha256::new();
    hasher.update(b"zeppelin-enrichment-work-v2");
    hasher.update(incarnation.as_bytes());
    hasher.update(origin.namespace.as_str().as_bytes());
    hasher.update(origin.incarnation.as_uuid().as_bytes());
    hasher.update(reference.id.to_bytes());
    hasher.update(source_checksum.to_le_bytes());
    for version in &versions.records {
        hasher.update(version.row_ordinal.to_le_bytes());
        hasher.update(version.record_id.as_bytes());
        hasher.update(version.content_hash.as_bytes());
        hasher.update(version.sequence.to_le_bytes());
    }
    hasher.update(profile.epoch.id.as_bytes());
    hasher.update(profile.fde.generation.as_bytes());
    EnrichmentWorkId(hasher.finalize().into())
}

fn enrichment_source_id(
    incarnation: uuid::Uuid,
    origin: &ArtifactOrigin,
    reference: &InputFragmentRef,
    source_checksum: u64,
    profile: &EmbeddingProfileRef,
) -> EnrichmentSourceId {
    let mut hasher = Sha256::new();
    hasher.update(b"zeppelin-enrichment-source-v1");
    hasher.update(incarnation.as_bytes());
    hasher.update(origin.namespace.as_str().as_bytes());
    hasher.update(origin.incarnation.as_uuid().as_bytes());
    hasher.update(reference.id.to_bytes());
    hasher.update(reference.sequence_number.to_le_bytes());
    hasher.update(reference.size_bytes.to_le_bytes());
    hasher.update(source_checksum.to_le_bytes());
    hasher.update(profile.epoch.id.as_bytes());
    hasher.update(profile.fde.generation.as_bytes());
    EnrichmentSourceId(hasher.finalize().into())
}

fn is_transient_failure(error: &ZeppelinError) -> bool {
    matches!(
        error,
        ZeppelinError::Storage(_)
            | ZeppelinError::ManifestConflict { .. }
            | ZeppelinError::LeaseHeld { .. }
            | ZeppelinError::LeaseExpired { .. }
            | ZeppelinError::FencingTokenStale { .. }
    )
}

fn suppress_rediscovery(error: &ZeppelinError) -> bool {
    !is_transient_failure(error)
}

fn poison_failure_class(error: &ZeppelinError) -> Option<&'static str> {
    match error {
        ZeppelinError::PayloadTooLarge { .. } => Some("payload_too_large"),
        ZeppelinError::RetrievalUnitTooLarge { .. } => Some("retrieval_unit_too_large"),
        ZeppelinError::RetrievalUnitEmpty => Some("retrieval_unit_empty"),
        ZeppelinError::UnsupportedInputModality { .. } => Some("unsupported_input_modality"),
        ZeppelinError::UnsupportedImageMediaType { .. } => Some("unsupported_image_media_type"),
        ZeppelinError::ImageDimensionsExceeded { .. } => Some("image_dimensions_exceeded"),
        ZeppelinError::InvalidImageInput => Some("invalid_image_input"),
        _ => None,
    }
}

fn checkpoint_at(
    configured: Option<EnrichmentCheckpoint>,
    reached: EnrichmentCheckpoint,
) -> Result<()> {
    if configured == Some(reached) {
        return Err(ZeppelinError::Validation(format!(
            "injected enrichment checkpoint: {reached:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{apply_candidate_document_pooling, CandidateDocumentPooling};
    use crate::embedding::MultiVectorEmbedding;

    #[test]
    fn candidate_pooling_averages_contiguous_rows_and_uses_actual_tail_count() {
        let embedding = MultiVectorEmbedding::new(
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 8.0, 7.0, 12.0, 11.0, 20.0],
            5,
            2,
            5,
        )
        .unwrap();

        let pooled = apply_candidate_document_pooling(
            &embedding,
            CandidateDocumentPooling::ContiguousMean { factor: 2 },
        )
        .unwrap();

        assert_eq!(pooled.vector_count(), 3);
        assert_eq!(pooled.vector_dimension(), 2);
        assert_eq!(pooled.values(), &[2.0, 3.0, 6.0, 10.0, 11.0, 20.0]);
        assert_ne!(
            pooled.values()[4] * pooled.values()[4] + pooled.values()[5] * pooled.values()[5],
            1.0
        );
    }

    #[test]
    fn identity_candidate_pooling_preserves_the_matrix() {
        let embedding = MultiVectorEmbedding::new(vec![1.0, 2.0, 3.0, 4.0], 2, 2, 2).unwrap();

        let pooled =
            apply_candidate_document_pooling(&embedding, CandidateDocumentPooling::Identity)
                .unwrap();

        assert_eq!(pooled, embedding);
    }
}
