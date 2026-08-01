mod common;

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Barrier;
use uuid::Uuid;
use zeppelin::config::MmliConfig;
use zeppelin::embedding::{
    ArtifactChecksum, DeterministicDev, EmbeddingProfileId, EmbeddingProfileRef,
    EncoderDocumentInput, EncoderExecutionRef, EncoderInputRef, EncoderQueryInput,
    EnrichmentCheckpoint, EnrichmentCoordinator, EnrichmentCoordinatorOptions, ExactScorerVersion,
    FdeArtifact, FdeRecipe, FdeTransformArtifactRef, InputModality, MatrixArtifact, MatrixDtype,
    MultiVectorEmbedding, MultiVectorEmbeddingBatch, MultiVectorEncoder,
    MultiVectorEncoderProvider, MultiVectorEncoderRegistry, MultiVectorEpoch, MultiVectorEpochId,
    NormalizationRecipe, RetrievalUnitRecord, SemanticState, TextContentRef, VectorTransformRecipe,
    DETERMINISTIC_DEV_IMPLEMENTATION, DETERMINISTIC_DEV_VERSION,
};
use zeppelin::error::{Result, ZeppelinError};
use zeppelin::index::late_interaction::{
    FdeAlgorithmVersion, FdeParams, FdeTransform, FinalProjection, InnerProjection,
};
use zeppelin::namespace::NamespaceManager;
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::{DistanceMetric, IndexType};
use zeppelin::wal::{EncoderInputWalFragment, LeaseManager, Manifest, WalWriter};

use common::counting::counting_store;
use common::fault_injection::{
    pause_first_cas_matching, pause_first_create_matching, toggle_cas_precondition_failure_matching,
};
use common::harness::TestHarness;

const TEXT_MODALITIES: &[InputModality] = &[InputModality::Text];

fn text_record(id: &str, text: &str) -> RetrievalUnitRecord {
    let input = EncoderInputRef::Text {
        content: TextContentRef::Inline(text.to_string()),
    };
    RetrievalUnitRecord {
        id: id.to_string(),
        content_hash: input.content_hash().expect("text fixture must hash"),
        input,
        parent_id: None,
        unit_ordinal: None,
        attributes: None,
    }
}

async fn setup_profile(
    store: &ZeppelinStore,
    namespace: &str,
) -> (EmbeddingProfileRef, Uuid, FdeTransform) {
    let metadata = NamespaceManager::new(store.clone())
        .create_typed_with_fts_and_index_config(
            namespace,
            0,
            DistanceMetric::DotProduct,
            IndexType::LateInteractionFde,
            Some(zeppelin::embedding::LateInteractionNamespaceConfig {
                accepted_modalities: vec![zeppelin::embedding::InputModality::Text],
            }),
            HashMap::new(),
            None,
        )
        .await
        .expect("late namespace creation must succeed");
    let incarnation = metadata
        .incarnation_id
        .expect("new namespace must carry an incarnation")
        .to_string()
        .parse()
        .expect("incarnation must be a UUID");

    let mut epoch = MultiVectorEpoch {
        id: MultiVectorEpochId::new([0; 32]),
        encoder: EncoderExecutionRef {
            implementation: DETERMINISTIC_DEV_IMPLEMENTATION.to_string(),
            version: DETERMINISTIC_DEV_VERSION.to_string(),
            bundle_prefix: None,
            artifact_digests: BTreeMap::from([(
                "deterministic-dev".to_string(),
                ArtifactChecksum::digest(DETERMINISTIC_DEV_VERSION.as_bytes()),
            )]),
            supported_modalities: vec![zeppelin::embedding::InputModality::Text],
        },
        preprocessing_digest: ArtifactChecksum::digest(b"enrichment-test-preprocessing-v1"),
        vector_dimension: 8,
        max_query_vectors: 16,
        max_document_vectors: 16,
        output_normalization: NormalizationRecipe::L2,
        exact_scoring_transform: VectorTransformRecipe::Identity,
        matrix_dtype: MatrixDtype::F16,
        exact_scorer: ExactScorerVersion::MaxSimV1,
    };
    epoch.id = epoch.canonical_id().expect("epoch must canonicalize");
    let params = FdeParams {
        algorithm: FdeAlgorithmVersion::PaperV1,
        repetitions: 2,
        simhash_bits: 1,
        input_dimension: 8,
        inner: InnerProjection::Rademacher { d_proj: 4 },
        final_projection: FinalProjection::None,
    };
    let transform = FdeTransform::generate(&params, 17).expect("transform must generate");
    let transform_bytes = transform.to_bytes();
    let transform_checksum = ArtifactChecksum::digest(&transform_bytes);
    let transform_key = format!(
        "{namespace}/late/transforms/{}",
        transform_checksum.to_hex()
    );
    store
        .put_create(&transform_key, transform_bytes.clone())
        .await
        .expect("transform PUT must succeed");
    let mut recipe = FdeRecipe {
        generation: zeppelin::embedding::FdeGenerationId::new([0; 32]),
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
        candidate_document_pooling: zeppelin::embedding::CandidateDocumentPooling::Identity,
    };
    recipe.generation = recipe
        .canonical_generation()
        .expect("FDE generation must canonicalize");
    let profile = EmbeddingProfileRef {
        profile: EmbeddingProfileId::new("enrichment-test"),
        epoch,
        fde: recipe,
        int8_qualification: None,
    };
    profile.validate().expect("profile must validate");
    let (mut manifest, version) =
        Manifest::read_versioned_required_for_incarnation(store, namespace, incarnation)
            .await
            .expect("manifest must load");
    manifest
        .activate_embedding_profile(store, namespace, &version, &profile)
        .await
        .expect("profile activation must succeed");
    assert_eq!(
        manifest
            .semantic_coverage
            .as_ref()
            .expect("activation must initialize coverage")
            .state,
        SemanticState::Ready
    );
    (profile, incarnation, transform)
}

fn provider(profile: &EmbeddingProfileRef) -> Arc<dyn MultiVectorEncoderProvider> {
    let registry = Arc::new(MultiVectorEncoderRegistry::new(&MmliConfig::default()));
    registry
        .register(Arc::new(
            DeterministicDev::new(true, &profile.epoch).expect("dev encoder must construct"),
        ))
        .expect("dev encoder must register");
    registry
}

fn coordinator(
    store: &ZeppelinStore,
    profile: &EmbeddingProfileRef,
    checkpoint: Option<EnrichmentCheckpoint>,
) -> EnrichmentCoordinator {
    EnrichmentCoordinator::start(
        store.clone(),
        Arc::new(LeaseManager::new(
            store.clone(),
            format!("enrichment-test-{}", Uuid::new_v4()),
            Duration::from_secs(30),
        )),
        provider(profile),
        EnrichmentCoordinatorOptions {
            queue_capacity: 2,
            max_retry_attempts: 4,
            checkpoint,
        },
    )
}

#[tokio::test]
async fn enrichment_publishes_decodable_artifacts_and_exact_fde() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("enrichment-happy");
    let (profile, incarnation, transform) = setup_profile(&harness.store, &namespace).await;
    let (input_fragment, acknowledged) = WalWriter::new(harness.store.clone())
        .append_retrieval_units(
            &namespace,
            vec![text_record("a", "alpha"), text_record("b", "beta")],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("typed append must succeed");
    let acknowledged_coverage = acknowledged
        .semantic_coverage
        .as_ref()
        .expect("write acknowledgement must carry semantic coverage");
    assert_eq!(acknowledged_coverage.state, SemanticState::Pending);
    assert_eq!(acknowledged_coverage.pending_record_count, 2);
    let acknowledged_sequence = acknowledged
        .input_fragments
        .iter()
        .find(|reference| reference.id == input_fragment.id)
        .expect("acknowledged input reference must exist")
        .sequence_number;

    let coordinator = coordinator(&harness.store, &profile, None);
    let bounded = coordinator
        .discover_and_admit(&namespace, incarnation, Some(TEXT_MODALITIES), 2, 0)
        .await
        .expect("zero-byte discovery must succeed");
    assert_eq!(bounded.admitted_fragments, 0);
    assert_eq!(bounded.admitted_bytes, 0);
    let report = coordinator
        .discover_and_admit(&namespace, incarnation, Some(TEXT_MODALITIES), 2, 1_000_000)
        .await
        .expect("discovery must succeed");
    assert_eq!(report.admitted_fragments, 1);
    coordinator
        .wait_for_idle()
        .await
        .expect("enrichment must complete");

    let (manifest, _) =
        Manifest::read_versioned_required_for_incarnation(&harness.store, &namespace, incarnation)
            .await
            .expect("published manifest must load");
    let section = manifest
        .load_late_state(&harness.store)
        .await
        .expect("section must load")
        .expect("section must exist");
    assert_eq!(section.semantic_overlays.len(), 1);
    let overlay = &section.semantic_overlays[0];
    let matrix_bytes = harness
        .store
        .get(&overlay.embeddings.key)
        .await
        .expect("matrix must exist");
    let matrix = MatrixArtifact::from_bytes(
        &matrix_bytes,
        overlay.embeddings.checksum,
        profile.epoch.matrix_dtype,
        profile.epoch.id,
        overlay.source_fragment.checksum,
        profile.epoch.vector_dimension as usize,
        2,
        profile.epoch.max_document_vectors as usize,
    )
    .expect("matrix must decode");
    let fde_bytes = harness
        .store
        .get(&overlay.fde_vectors.key)
        .await
        .expect("FDE must exist");
    let fde = FdeArtifact::from_bytes(
        &fde_bytes,
        overlay.fde_vectors.checksum,
        profile.fde.generation,
        overlay.embeddings.checksum,
        transform.output_dimension(),
        2,
    )
    .expect("FDE must decode");
    for (matrix_row, fde_row) in matrix.rows().iter().zip(fde.rows()) {
        assert_eq!(
            transform
                .encode_document(&matrix_row.embedding().matrix_ref().expect("matrix ref"))
                .expect("direct FDE"),
            fde_row.values()
        );
    }
    let coverage = manifest
        .semantic_coverage
        .as_ref()
        .expect("published manifest must carry coverage");
    assert_eq!(coverage.state, SemanticState::Ready);
    assert_eq!(coverage.pending_record_count, 0);
    assert_eq!(coverage.contiguous_sequence, acknowledged_sequence);
    assert!(manifest.fencing_token() > 0);
    coordinator.shutdown().await.expect("shutdown must join");
    harness.cleanup_artifact_origin_namespace(&namespace).await;
}

#[tokio::test]
async fn enrichment_restarts_at_each_checkpoint_and_rejects_collisions() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("enrichment-restart");
    let (profile, incarnation, _) = setup_profile(&harness.store, &namespace).await;
    WalWriter::new(harness.store.clone())
        .append_retrieval_units(
            &namespace,
            vec![text_record("a", "restart")],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("typed append must succeed");

    let first = coordinator(
        &harness.store,
        &profile,
        Some(EnrichmentCheckpoint::AfterMatrixPut),
    );
    first
        .discover_and_admit(&namespace, incarnation, Some(TEXT_MODALITIES), 1, 1_000_000)
        .await
        .expect("first discovery must succeed");
    assert!(first.wait_for_idle().await.is_err());
    first.shutdown().await.expect("first shutdown must join");

    let matrix_keys = harness
        .store
        .list_prefix(&format!("{namespace}/late/matrix-fragments/"))
        .await
        .expect("matrix listing must succeed");
    assert_eq!(matrix_keys.len(), 1);
    let original = harness
        .store
        .get(&matrix_keys[0])
        .await
        .expect("matrix must read");
    harness
        .store
        .put(&matrix_keys[0], bytes::Bytes::from_static(b"wrong"))
        .await
        .expect("test corruption must write");
    let collision = coordinator(&harness.store, &profile, None);
    collision
        .discover_and_admit(&namespace, incarnation, Some(TEXT_MODALITIES), 1, 1_000_000)
        .await
        .expect("collision discovery must succeed");
    assert!(collision.wait_for_idle().await.is_err());
    let (collision_manifest, _) =
        Manifest::read_versioned_required_for_incarnation(&harness.store, &namespace, incarnation)
            .await
            .expect("manifest after integrity failure must load");
    let collision_section = collision_manifest
        .load_late_state(&harness.store)
        .await
        .expect("section after integrity failure must load")
        .expect("section after integrity failure must exist");
    assert!(
        collision_section.quarantine_evidence.is_empty(),
        "artifact integrity errors must not be mislabeled as poison input"
    );
    collision
        .shutdown()
        .await
        .expect("collision shutdown must join");
    harness
        .store
        .put(&matrix_keys[0], original.clone())
        .await
        .expect("fixture matrix restoration must succeed");

    let mut expected_fde = None;
    for checkpoint in [
        EnrichmentCheckpoint::AfterFdePut,
        EnrichmentCheckpoint::AfterSectionPut,
    ] {
        let crashed = coordinator(&harness.store, &profile, Some(checkpoint));
        crashed
            .discover_and_admit(&namespace, incarnation, Some(TEXT_MODALITIES), 1, 1_000_000)
            .await
            .expect("restart discovery must succeed");
        assert!(crashed.wait_for_idle().await.is_err());
        crashed
            .shutdown()
            .await
            .expect("crashed shutdown must join");
        let current_matrix_keys = harness
            .store
            .list_prefix(&format!("{namespace}/late/matrix-fragments/"))
            .await
            .expect("checkpoint matrix listing must succeed");
        assert_eq!(current_matrix_keys, matrix_keys);
        assert_eq!(
            harness
                .store
                .get(&matrix_keys[0])
                .await
                .expect("checkpoint matrix must read"),
            original,
            "checkpoint restart must reuse identical matrix bytes"
        );
        let current_fde_keys = harness
            .store
            .list_prefix(&format!("{namespace}/late/fde-fragments/"))
            .await
            .expect("checkpoint FDE listing must succeed");
        assert_eq!(current_fde_keys.len(), 1);
        let current_fde = harness
            .store
            .get(&current_fde_keys[0])
            .await
            .expect("checkpoint FDE must read");
        if let Some((expected_key, expected_bytes)) = expected_fde.as_ref() {
            assert_eq!(&current_fde_keys[0], expected_key);
            assert_eq!(
                &current_fde, expected_bytes,
                "checkpoint restart must reuse identical FDE bytes"
            );
        } else {
            expected_fde = Some((current_fde_keys[0].clone(), current_fde));
        }
    }

    let final_run = coordinator(&harness.store, &profile, None);
    final_run
        .discover_and_admit(&namespace, incarnation, Some(TEXT_MODALITIES), 1, 1_000_000)
        .await
        .expect("final discovery must succeed");
    final_run
        .wait_for_idle()
        .await
        .expect("final publication must succeed");
    let (manifest, _) =
        Manifest::read_versioned_required_for_incarnation(&harness.store, &namespace, incarnation)
            .await
            .expect("manifest must load");
    let section = manifest
        .load_late_state(&harness.store)
        .await
        .expect("section must load")
        .expect("section must exist");
    assert_eq!(section.semantic_overlays.len(), 1);
    assert_eq!(
        harness
            .store
            .list_prefix(&format!("{namespace}/late/matrix-fragments/"))
            .await
            .expect("final matrix listing must succeed"),
        matrix_keys
    );
    assert_eq!(
        harness
            .store
            .get(&matrix_keys[0])
            .await
            .expect("final matrix must read"),
        original
    );
    let (expected_fde_key, expected_fde_bytes) =
        expected_fde.expect("FDE checkpoint must establish expected bytes");
    assert_eq!(
        harness
            .store
            .list_prefix(&format!("{namespace}/late/fde-fragments/"))
            .await
            .expect("final FDE listing must succeed"),
        vec![expected_fde_key.clone()]
    );
    assert_eq!(
        harness
            .store
            .get(&expected_fde_key)
            .await
            .expect("final FDE must read"),
        expected_fde_bytes
    );
    final_run
        .shutdown()
        .await
        .expect("final shutdown must join");
    harness.cleanup_artifact_origin_namespace(&namespace).await;
}

struct FirstCallGateEncoder {
    inner: DeterministicDev,
    first: AtomicBool,
    calls: Arc<AtomicUsize>,
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
}

#[async_trait]
impl MultiVectorEncoder for FirstCallGateEncoder {
    fn epoch(&self) -> MultiVectorEpochId {
        self.inner.epoch()
    }

    fn output_dimension(&self) -> usize {
        self.inner.output_dimension()
    }

    async fn encode_documents(
        &self,
        inputs: &[EncoderDocumentInput],
    ) -> Result<MultiVectorEmbeddingBatch> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.first.swap(false, Ordering::SeqCst) {
            self.entered.wait().await;
            self.release.wait().await;
        }
        self.inner.encode_documents(inputs).await
    }

    async fn encode_query(&self, input: EncoderQueryInput<'_>) -> Result<MultiVectorEmbedding> {
        self.inner.encode_query(input).await
    }
}

struct CountingEncoder {
    inner: DeterministicDev,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl MultiVectorEncoder for CountingEncoder {
    fn epoch(&self) -> MultiVectorEpochId {
        self.inner.epoch()
    }

    fn output_dimension(&self) -> usize {
        self.inner.output_dimension()
    }

    async fn encode_documents(
        &self,
        inputs: &[EncoderDocumentInput],
    ) -> Result<MultiVectorEmbeddingBatch> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.encode_documents(inputs).await
    }

    async fn encode_query(&self, input: EncoderQueryInput<'_>) -> Result<MultiVectorEmbedding> {
        self.inner.encode_query(input).await
    }
}

struct RecoverablePoisonEncoder {
    inner: DeterministicDev,
}

#[async_trait]
impl MultiVectorEncoder for RecoverablePoisonEncoder {
    fn epoch(&self) -> MultiVectorEpochId {
        self.inner.epoch()
    }

    fn output_dimension(&self) -> usize {
        self.inner.output_dimension()
    }

    async fn encode_documents(
        &self,
        inputs: &[EncoderDocumentInput],
    ) -> Result<MultiVectorEmbeddingBatch> {
        if inputs.iter().any(|input| {
            matches!(
                input.input_ref(),
                EncoderInputRef::Text {
                    content: TextContentRef::Inline(text)
                } if text == "poison"
            )
        }) {
            return Err(ZeppelinError::InvalidImageInput);
        }
        self.inner.encode_documents(inputs).await
    }

    async fn encode_query(&self, input: EncoderQueryInput<'_>) -> Result<MultiVectorEmbedding> {
        self.inner.encode_query(input).await
    }
}

#[tokio::test]
async fn deterministic_poison_is_durable_and_does_not_stop_later_work() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("enrichment-poison");
    let (profile, incarnation, _) = setup_profile(&harness.store, &namespace).await;
    let writer = WalWriter::new(harness.store.clone());
    writer
        .append_retrieval_units(
            &namespace,
            vec![
                text_record("poison", "poison"),
                text_record("later", "healthy"),
            ],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("mixed poison fixture append must succeed");

    let registry = Arc::new(MultiVectorEncoderRegistry::new(&MmliConfig::default()));
    registry
        .register(Arc::new(RecoverablePoisonEncoder {
            inner: DeterministicDev::new(true, &profile.epoch).expect("dev encoder must construct"),
        }))
        .expect("poison encoder must register");
    let coordinator = EnrichmentCoordinator::start(
        harness.store.clone(),
        Arc::new(LeaseManager::new(
            harness.store.clone(),
            "enrichment-poison".to_string(),
            Duration::from_secs(30),
        )),
        registry,
        EnrichmentCoordinatorOptions {
            queue_capacity: 2,
            max_retry_attempts: 4,
            checkpoint: None,
        },
    );
    let metric = zeppelin::metrics::SEMANTIC_ENRICHMENT_QUARANTINED_RECORDS_TOTAL
        .with_label_values(&[namespace.as_str()]);
    let metric_before = metric.get();

    let report = coordinator
        .discover_and_admit(&namespace, incarnation, Some(TEXT_MODALITIES), 2, 1_000_000)
        .await
        .expect("poison and later work must admit");
    assert_eq!(report.admitted_fragments, 1);
    coordinator
        .wait_for_idle()
        .await
        .expect("durably quarantined poison must not fail the executor");

    let (manifest, _) =
        Manifest::read_versioned_required_for_incarnation(&harness.store, &namespace, incarnation)
            .await
            .expect("poison manifest must load");
    let coverage = manifest
        .semantic_coverage
        .as_ref()
        .expect("poison coverage must exist");
    assert_eq!(coverage.state, SemanticState::Failed);
    assert_eq!(coverage.failed_record_count, 1);
    assert_eq!(coverage.pending_record_count, 0);
    let section = manifest
        .load_late_state(&harness.store)
        .await
        .expect("poison section must load")
        .expect("poison section must exist");
    assert_eq!(section.quarantine_evidence.len(), 1);
    assert_eq!(section.semantic_overlays.len(), 1);
    let evidence = &section.quarantine_evidence[0];
    assert_eq!(evidence.failed_versions.records.len(), 1);
    assert_eq!(evidence.failed_versions.records[0].record_id, "poison");
    assert_eq!(evidence.failed_versions.records[0].row_ordinal, 0);
    let overlay = &section.semantic_overlays[0];
    assert_eq!(overlay.covered_versions.records.len(), 1);
    assert_eq!(overlay.covered_versions.records[0].record_id, "later");
    assert_eq!(overlay.covered_versions.records[0].row_ordinal, 1);
    assert_eq!(overlay.embeddings.row_count, 1);
    assert_eq!(overlay.fde_vectors.row_count, 1);
    let evidence_bytes = harness
        .store
        .get(&evidence.key)
        .await
        .expect("quarantine evidence must be durable");
    assert!(evidence_bytes.starts_with(b"ZEQ1\x01"));
    assert_eq!(metric.get() - metric_before, 1);

    let rediscovery = coordinator
        .discover_and_admit(&namespace, incarnation, Some(TEXT_MODALITIES), 2, 1_000_000)
        .await
        .expect("durable poison rediscovery must succeed");
    assert_eq!(rediscovery.admitted_fragments, 0);
    coordinator
        .shutdown()
        .await
        .expect("successfully quarantined work must not poison shutdown");
    harness.cleanup_artifact_origin_namespace(&namespace).await;
}

#[tokio::test]
async fn stale_output_keeps_a_hole_and_encoding_holds_no_lease() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("enrichment-stale");
    let (profile, incarnation, _) = setup_profile(&harness.store, &namespace).await;
    let writer = WalWriter::new(harness.store.clone());
    writer
        .append_retrieval_units(
            &namespace,
            vec![text_record("same", "old")],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("old append must succeed");
    let (counted_store, counter) = counting_store(&harness.store);
    let (section_paused_store, section_pause) =
        pause_first_create_matching(&counted_store, "/late/state/");
    let (publication_store, manifest_pause) =
        pause_first_cas_matching(&section_paused_store, "/manifest.json");

    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let encode_calls = Arc::new(AtomicUsize::new(0));
    let registry = Arc::new(MultiVectorEncoderRegistry::new(&MmliConfig::default()));
    registry
        .register(Arc::new(FirstCallGateEncoder {
            inner: DeterministicDev::new(true, &profile.epoch).expect("dev encoder must construct"),
            first: AtomicBool::new(true),
            calls: Arc::clone(&encode_calls),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        }))
        .expect("gated encoder must register");
    let provider: Arc<dyn MultiVectorEncoderProvider> = registry;
    let coordinator = EnrichmentCoordinator::start(
        publication_store.clone(),
        Arc::new(LeaseManager::new(
            publication_store,
            "enrichment-stale".to_string(),
            Duration::from_secs(30),
        )),
        provider,
        EnrichmentCoordinatorOptions {
            queue_capacity: 2,
            max_retry_attempts: 4,
            checkpoint: None,
        },
    );
    coordinator
        .discover_and_admit(&namespace, incarnation, Some(TEXT_MODALITIES), 1, 1_000_000)
        .await
        .expect("old work must admit");
    entered.wait().await;
    assert!(
        matches!(
            harness.store.get(&format!("{namespace}/lease.json")).await,
            Err(ZeppelinError::NotFound { .. })
        ),
        "encoder execution must finish before lease acquisition"
    );
    writer
        .append_retrieval_units(
            &namespace,
            vec![text_record("same", "new")],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("new append must succeed while old encoding is in flight");
    release.wait().await;
    section_pause.wait_until_paused().await;
    harness
        .store
        .get(&format!("{namespace}/lease.json"))
        .await
        .expect("lease must be active when the immutable section create begins");
    section_pause.release();
    manifest_pause.wait_until_paused().await;
    harness
        .store
        .get(&format!("{namespace}/lease.json"))
        .await
        .expect("lease must remain active when the root manifest CAS begins");
    manifest_pause.release();
    coordinator
        .wait_for_idle()
        .await
        .expect("old overlay may publish as superseded evidence");

    let (manifest, _) =
        Manifest::read_versioned_required_for_incarnation(&harness.store, &namespace, incarnation)
            .await
            .expect("manifest must load");
    let coverage = manifest
        .semantic_coverage
        .as_ref()
        .expect("coverage must exist");
    assert_eq!(coverage.state, SemanticState::Pending);
    assert_eq!(coverage.pending_record_count, 1);

    coordinator
        .discover_and_admit(&namespace, incarnation, Some(TEXT_MODALITIES), 2, 1_000_000)
        .await
        .expect("new work must admit past the covered prefix");
    coordinator
        .wait_for_idle()
        .await
        .expect("new version must enrich");
    let (manifest, _) =
        Manifest::read_versioned_required_for_incarnation(&harness.store, &namespace, incarnation)
            .await
            .expect("final manifest must load");
    assert_eq!(
        manifest
            .semantic_coverage
            .as_ref()
            .expect("coverage must exist")
            .state,
        SemanticState::Ready
    );
    let section = manifest
        .load_late_state(&harness.store)
        .await
        .expect("section must load")
        .expect("section must exist");
    assert_eq!(section.semantic_overlays.len(), 2);
    assert_eq!(
        encode_calls.load(Ordering::SeqCst),
        2,
        "each admitted immutable fragment must be encoded exactly once"
    );
    assert!(manifest.fencing_token() > 0);
    let lease_puts = counter.puts_matching("/lease.json");
    let section_creates = counter.create_puts_matching("/late/state/");
    let manifest_cas_puts = counter.update_puts_matching("/manifest.json");
    eprintln!(
        "lease_hold_measurement lease_puts={lease_puts} \
         section_creates={section_creates} manifest_cas_puts={manifest_cas_puts}"
    );
    assert!(
        lease_puts >= 3,
        "publication must acquire, renew, and release the short lease"
    );
    assert!(
        section_creates >= 1,
        "publication must create the immutable late section while leased"
    );
    assert!(
        manifest_cas_puts >= 1,
        "publication must conditionally CAS the root while leased"
    );
    coordinator.shutdown().await.expect("shutdown must join");
    harness.cleanup_artifact_origin_namespace(&namespace).await;
}

#[tokio::test]
async fn bounded_discovery_skips_settled_history_without_input_wal_gets() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("enrichment-bounded");
    let (profile, incarnation, _) = setup_profile(&harness.store, &namespace).await;
    let writer = WalWriter::new(harness.store.clone());
    let historical_coordinator = coordinator(&harness.store, &profile, None);
    let mut settled_keys = Vec::new();

    for ordinal in 0..4 {
        let (fragment, _) = writer
            .append_retrieval_units(
                &namespace,
                vec![text_record(
                    &format!("settled-{ordinal}"),
                    &format!("settled text {ordinal}"),
                )],
                Vec::new(),
                Vec::new(),
            )
            .await
            .expect("historical append must succeed");
        settled_keys.push(EncoderInputWalFragment::s3_key(&namespace, &fragment.id));
        let report = historical_coordinator
            .discover_and_admit(&namespace, incarnation, Some(TEXT_MODALITIES), 1, 1_000_000)
            .await
            .expect("historical discovery must succeed");
        assert_eq!(report.admitted_fragments, 1);
        historical_coordinator
            .wait_for_idle()
            .await
            .expect("historical enrichment must succeed");
    }
    historical_coordinator
        .shutdown()
        .await
        .expect("historical coordinator must stop");

    let (tail, _) = writer
        .append_retrieval_units(
            &namespace,
            vec![text_record("tail", "unsettled tail")],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("tail append must succeed");
    let tail_key = EncoderInputWalFragment::s3_key(&namespace, &tail.id);
    let (counted_store, counter) = counting_store(&harness.store);
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let registry = Arc::new(MultiVectorEncoderRegistry::new(&MmliConfig::default()));
    registry
        .register(Arc::new(FirstCallGateEncoder {
            inner: DeterministicDev::new(true, &profile.epoch).expect("dev encoder must construct"),
            first: AtomicBool::new(true),
            calls: Arc::new(AtomicUsize::new(0)),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        }))
        .expect("gated encoder must register");
    let coordinator = EnrichmentCoordinator::start(
        counted_store.clone(),
        Arc::new(LeaseManager::new(
            counted_store,
            "enrichment-bounded".to_string(),
            Duration::from_secs(30),
        )),
        registry,
        EnrichmentCoordinatorOptions {
            queue_capacity: 2,
            max_retry_attempts: 4,
            checkpoint: None,
        },
    );

    let zero_fragment_bound = coordinator
        .discover_and_admit(&namespace, incarnation, Some(TEXT_MODALITIES), 0, u64::MAX)
        .await
        .expect("zero-fragment discovery must succeed");
    assert_eq!(zero_fragment_bound.admitted_fragments, 0);
    assert_eq!(counter.gets_matching("/input-wal/"), 0);

    counter.reset();
    let zero_byte_bound = coordinator
        .discover_and_admit(&namespace, incarnation, Some(TEXT_MODALITIES), 1, 0)
        .await
        .expect("zero-byte discovery must succeed");
    assert_eq!(zero_byte_bound.admitted_fragments, 0);
    assert_eq!(counter.gets_matching("/input-wal/"), 0);

    counter.reset();
    let admitted = coordinator
        .discover_and_admit(&namespace, incarnation, Some(TEXT_MODALITIES), 1, 1_000_000)
        .await
        .expect("tail discovery must succeed");
    assert_eq!(admitted.admitted_fragments, 1);
    assert_eq!(
        admitted.inspected_bytes,
        tail.referenced_content_bytes()
            .expect("tail referenced bytes must fit")
    );
    entered.wait().await;
    let already_inflight = coordinator
        .discover_and_admit(&namespace, incarnation, Some(TEXT_MODALITIES), 1, 1_000_000)
        .await
        .expect("inflight rediscovery must stay bounded");
    assert_eq!(already_inflight.admitted_fragments, 0);
    assert_eq!(
        already_inflight.inspected_bytes,
        tail.referenced_content_bytes()
            .expect("tail referenced bytes must fit"),
        "inspected bytes must be charged even when the work is already inflight"
    );
    assert_eq!(
        counter.gets_matching(&format!("{namespace}/meta.json")),
        0,
        "repeated admission must not GET immutable namespace modalities"
    );
    for settled_key in settled_keys {
        assert_eq!(
            counter.gets_matching(&settled_key),
            0,
            "settled history must be classified from section metadata"
        );
    }
    assert!(
        counter.gets_matching(&tail_key) >= 1,
        "the later unprocessed tail must remain reachable without a cursor"
    );
    release.wait().await;
    coordinator
        .wait_for_idle()
        .await
        .expect("tail enrichment must succeed");

    coordinator.shutdown().await.expect("shutdown must join");
    harness.cleanup_artifact_origin_namespace(&namespace).await;
}

#[tokio::test]
async fn exhausted_publication_cas_does_not_reencode_terminal_source_recipe() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("enrichment-cas-exhausted");
    let (profile, incarnation, _) = setup_profile(&harness.store, &namespace).await;
    let writer = WalWriter::new(harness.store.clone());
    writer
        .append_retrieval_units(
            &namespace,
            vec![text_record("first", "first physical source")],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("first append must succeed");

    let (fault_store, cas_failures) =
        toggle_cas_precondition_failure_matching(&harness.store, "/manifest.json");
    cas_failures.enable();
    let encode_calls = Arc::new(AtomicUsize::new(0));
    let registry = Arc::new(MultiVectorEncoderRegistry::new(&MmliConfig::default()));
    registry
        .register(Arc::new(CountingEncoder {
            inner: DeterministicDev::new(true, &profile.epoch).expect("dev encoder must construct"),
            calls: Arc::clone(&encode_calls),
        }))
        .expect("counting encoder must register");
    let coordinator = EnrichmentCoordinator::start(
        fault_store.clone(),
        Arc::new(LeaseManager::new(
            fault_store,
            "enrichment-cas-exhausted".to_string(),
            Duration::from_secs(30),
        )),
        registry,
        EnrichmentCoordinatorOptions {
            queue_capacity: 2,
            max_retry_attempts: 2,
            checkpoint: None,
        },
    );

    let first = coordinator
        .discover_and_admit(&namespace, incarnation, Some(TEXT_MODALITIES), 1, 1_000_000)
        .await
        .expect("first work must admit");
    assert_eq!(first.admitted_fragments, 1);
    let supervised_failure = tokio::time::timeout(
        Duration::from_secs(1),
        coordinator.wait_for_executor_failure(),
    )
    .await
    .expect("the owning maintenance supervisor must observe failure promptly");
    assert!(
        supervised_failure
            .to_string()
            .contains("failed after encoder completion"),
        "supervision must receive the terminal post-encode failure: {supervised_failure}"
    );
    let failure = coordinator
        .wait_for_idle()
        .await
        .expect_err("exhausted publication CAS must surface");
    assert!(
        failure
            .to_string()
            .contains("failed after encoder completion"),
        "failure must identify the terminal post-encode stage: {failure}"
    );
    assert_eq!(cas_failures.failures_injected(), 2);
    assert_eq!(
        encode_calls.load(Ordering::SeqCst),
        1,
        "publication retries must reuse the one prepared encoding"
    );

    cas_failures.disable();
    let suppressed = coordinator
        .discover_and_admit(&namespace, incarnation, Some(TEXT_MODALITIES), 1, 1_000_000)
        .await
        .expect("terminal-source rediscovery must succeed");
    assert_eq!(suppressed.admitted_fragments, 0);
    assert_eq!(
        encode_calls.load(Ordering::SeqCst),
        1,
        "the failed physical source recipe must stay suppressed"
    );

    writer
        .append_retrieval_units(
            &namespace,
            vec![text_record("second", "new physical source")],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("new physical source append must succeed");
    let next = coordinator
        .discover_and_admit(&namespace, incarnation, Some(TEXT_MODALITIES), 2, 1_000_000)
        .await
        .expect("new physical source discovery must succeed");
    assert_eq!(
        next.admitted_fragments, 1,
        "terminal suppression must not block a new physical source"
    );
    coordinator
        .wait_for_idle()
        .await
        .expect("new physical source must publish");
    assert_eq!(
        encode_calls.load(Ordering::SeqCst),
        2,
        "the new physical source must get its own encoding"
    );

    coordinator.shutdown().await.expect("shutdown must join");
    harness.cleanup_artifact_origin_namespace(&namespace).await;
}
