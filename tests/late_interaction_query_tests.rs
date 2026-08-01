mod common;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use uuid::Uuid;
use zeppelin::config::{Config, MmliConfig};
use zeppelin::embedding::{
    ArtifactChecksum, DeterministicDev, EmbeddingProfileId, EmbeddingProfileRef,
    EncoderExecutionRef, EncoderInputRef, EncoderQueryInput, EnrichmentCoordinator,
    EnrichmentCoordinatorOptions, ExactScorerVersion, FdeRecipe, FdeTransformArtifactRef,
    InputModality, MatrixArtifact, MatrixDtype, MultiVectorEncoder, MultiVectorEncoderProvider,
    MultiVectorEncoderRegistry, MultiVectorEpoch, MultiVectorEpochId, NormalizationRecipe,
    RetrievalUnitRecord, TextContentRef, VectorTransformRecipe, DETERMINISTIC_DEV_IMPLEMENTATION,
    DETERMINISTIC_DEV_VERSION,
};
use zeppelin::error::ZeppelinError;
use zeppelin::fts::FtsFieldConfig;
use zeppelin::index::late_interaction::{
    max_sim, search, FdeAlgorithmVersion, FdeParams, FdeTransform, FinalProjection,
    InnerProjection, LateInteractionCoverage, LateInteractionError, LateInteractionSearchRequest,
    ManifestRefresh,
};
use zeppelin::namespace::NamespaceManager;
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::{AttributeValue, ConsistencyLevel, DistanceMetric, Filter, IndexType};
use zeppelin::wal::{LeaseManager, Manifest, WalWriter};

use common::fault_injection::pause_first_get_matching;
use common::harness::TestHarness;
use common::server::{client_with_bearer, start_test_server_on_store_with_config};

const TEXT_MODALITIES: &[InputModality] = &[InputModality::Text];

fn require_minio() {
    assert_eq!(
        std::env::var("TEST_BACKEND").as_deref(),
        Ok("minio"),
        "Phase 7 query tests require TEST_BACKEND=minio"
    );
}

fn text_record(
    id: impl Into<String>,
    text: impl Into<String>,
    attributes: HashMap<String, AttributeValue>,
) -> RetrievalUnitRecord {
    let input = EncoderInputRef::Text {
        content: TextContentRef::Inline(text.into()),
    };
    RetrievalUnitRecord {
        id: id.into(),
        content_hash: input.content_hash().expect("text fixture must hash"),
        input,
        parent_id: None,
        unit_ordinal: None,
        attributes: Some(attributes),
    }
}

fn fts_fields() -> HashMap<String, FtsFieldConfig> {
    HashMap::from([(
        "content".to_string(),
        FtsFieldConfig {
            stemming: false,
            remove_stopwords: false,
            ..Default::default()
        },
    )])
}

async fn setup_profile(store: &ZeppelinStore, namespace: &str) -> (EmbeddingProfileRef, Uuid) {
    let metadata = NamespaceManager::new(store.clone())
        .create_typed_with_fts_and_index_config(
            namespace,
            0,
            DistanceMetric::DotProduct,
            IndexType::LateInteractionFde,
            Some(zeppelin::embedding::LateInteractionNamespaceConfig {
                accepted_modalities: vec![zeppelin::embedding::InputModality::Text],
            }),
            fts_fields(),
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
        preprocessing_digest: ArtifactChecksum::digest(b"phase-7-query-test-preprocessing-v1"),
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
        profile: EmbeddingProfileId::new("phase-7-query-test"),
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
    (profile, incarnation)
}

fn provider(
    profile: &EmbeddingProfileRef,
) -> (Arc<dyn MultiVectorEncoderProvider>, Arc<DeterministicDev>) {
    let dev =
        Arc::new(DeterministicDev::new(true, &profile.epoch).expect("dev encoder must construct"));
    let registry = Arc::new(MultiVectorEncoderRegistry::new(&MmliConfig::default()));
    registry
        .register(dev.clone())
        .expect("dev encoder must register");
    (registry, dev)
}

fn coordinator(
    store: &ZeppelinStore,
    encoder_provider: Arc<dyn MultiVectorEncoderProvider>,
) -> EnrichmentCoordinator {
    EnrichmentCoordinator::start(
        store.clone(),
        Arc::new(LeaseManager::new(
            store.clone(),
            format!("phase-7-query-test-{}", Uuid::new_v4()),
            Duration::from_secs(30),
        )),
        encoder_provider,
        EnrichmentCoordinatorOptions {
            queue_capacity: 8,
            max_retry_attempts: 4,
            checkpoint: None,
        },
    )
}

async fn read_manifest(store: &ZeppelinStore, namespace: &str, incarnation: Uuid) -> Manifest {
    Manifest::read_versioned_required_for_incarnation(store, namespace, incarnation)
        .await
        .expect("manifest must load")
        .0
}

async fn enrich_all(
    store: &ZeppelinStore,
    namespace: &str,
    incarnation: Uuid,
    encoder_provider: Arc<dyn MultiVectorEncoderProvider>,
) {
    let coordinator = coordinator(store, encoder_provider);
    let report = coordinator
        .discover_and_admit(
            namespace,
            incarnation,
            Some(TEXT_MODALITIES),
            usize::MAX,
            u64::MAX,
        )
        .await
        .expect("enrichment discovery must succeed");
    assert!(
        report.admitted_fragments > 0,
        "fixture must contain pending enrichment work"
    );
    coordinator
        .wait_for_idle()
        .await
        .expect("enrichment must complete");
    coordinator.shutdown().await.expect("shutdown must join");
}

#[tokio::test]
async fn exhaustive_search_matches_stored_matrix_bruteforce_with_filter() {
    require_minio();
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("late-query-parity");
    let (profile, incarnation) = setup_profile(&harness.store, &namespace).await;
    let (encoder_provider, dev) = provider(&profile);
    let records = (0..200)
        .map(|ordinal| {
            text_record(
                format!("doc-{ordinal:03}"),
                format!("deterministic document text number {ordinal}"),
                HashMap::from([
                    (
                        "cohort".to_string(),
                        AttributeValue::String(if ordinal % 2 == 0 {
                            "keep".to_string()
                        } else {
                            "drop".to_string()
                        }),
                    ),
                    (
                        "content".to_string(),
                        AttributeValue::String(format!("common token document {ordinal}")),
                    ),
                ]),
            )
        })
        .collect::<Vec<_>>();
    WalWriter::new(harness.store.clone())
        .append_retrieval_units(&namespace, records.clone(), Vec::new(), Vec::new())
        .await
        .expect("typed append must succeed");
    enrich_all(
        &harness.store,
        &namespace,
        incarnation,
        Arc::clone(&encoder_provider),
    )
    .await;

    let manifest = read_manifest(&harness.store, &namespace, incarnation).await;
    let filter = Filter::Eq {
        field: "cohort".to_string(),
        value: AttributeValue::String("keep".to_string()),
    };
    let query_text = "deterministic parity query";
    let started = Instant::now();
    let output = search(LateInteractionSearchRequest {
        store: &harness.store,
        bootstrap_cache: None,
        encoder_provider: encoder_provider.as_ref(),
        namespace: &namespace,
        accepted_modalities: vec![InputModality::Text],
        manifest: manifest.clone(),
        text: query_text,
        top_k: 40,
        effective_filter: Some(&filter),
        consistency: ConsistencyLevel::Strong,
        semantic_wait: Duration::from_millis(1),
        max_overlay_bytes: 64 * 1024 * 1024,
        segment_config: Default::default(),
        manifest_refresh: ManifestRefresh::Fixed,
    })
    .await
    .expect("covered exhaustive search must succeed");
    let elapsed = started.elapsed();
    eprintln!(
        "late_interaction_dev_query_latency_ms={:.3} records=200 top_k=40",
        elapsed.as_secs_f64() * 1_000.0
    );
    assert_eq!(output.semantic_coverage, LateInteractionCoverage::Complete);

    let query = dev
        .encode_query(EncoderQueryInput::new(query_text).expect("query must validate"))
        .await
        .expect("query encoding must succeed");
    let query_matrix = query.matrix_ref().expect("query matrix must validate");
    let section = manifest
        .load_late_state(&harness.store)
        .await
        .expect("section must load")
        .expect("section must exist");
    let records_by_id = records
        .into_iter()
        .map(|record| (record.id.clone(), record))
        .collect::<BTreeMap<_, _>>();
    let mut expected = Vec::new();
    for overlay in &section.semantic_overlays {
        let bytes = harness
            .store
            .get(&overlay.embeddings.key)
            .await
            .expect("matrix artifact must load");
        let matrix = MatrixArtifact::from_bytes(
            &bytes,
            overlay.embeddings.checksum,
            profile.epoch.matrix_dtype,
            profile.epoch.id,
            overlay.source_fragment.checksum,
            profile.epoch.vector_dimension as usize,
            overlay.covered_versions.records.len(),
            profile.epoch.max_document_vectors as usize,
        )
        .expect("stored matrix must decode");
        for (row, version) in matrix.rows().iter().zip(&overlay.covered_versions.records) {
            let record = records_by_id
                .get(&version.record_id)
                .expect("covered record must exist");
            if record
                .attributes
                .as_ref()
                .and_then(|attributes| attributes.get("cohort"))
                != Some(&AttributeValue::String("keep".to_string()))
            {
                continue;
            }
            expected.push((
                version.record_id.clone(),
                max_sim(
                    &query_matrix,
                    &row.embedding()
                        .matrix_ref()
                        .expect("document matrix must validate"),
                )
                .expect("MaxSim must succeed"),
            ));
        }
    }
    expected.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    expected.truncate(40);
    assert_eq!(output.results.len(), expected.len());
    for (actual, (expected_id, expected_score)) in output.results.iter().zip(expected) {
        assert_eq!(&actual.id, &expected_id);
        assert_eq!(actual.score, expected_score);
        assert_eq!(actual.provenance.manifest_generation, manifest.version());
    }

    harness.cleanup_artifact_origin_namespace(&namespace).await;
}

#[tokio::test]
async fn strong_waits_for_coverage_while_eventual_reports_partial() {
    require_minio();
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("late-query-consistency");
    let (profile, incarnation) = setup_profile(&harness.store, &namespace).await;
    let (encoder_provider, _) = provider(&profile);
    let writer = WalWriter::new(harness.store.clone());
    writer
        .append_retrieval_units(
            &namespace,
            vec![text_record(
                "covered",
                "already covered semantic record",
                HashMap::from([(
                    "content".to_string(),
                    AttributeValue::String("covered lexical text".to_string()),
                )]),
            )],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("covered append must succeed");
    enrich_all(
        &harness.store,
        &namespace,
        incarnation,
        Arc::clone(&encoder_provider),
    )
    .await;

    let covered_manifest = read_manifest(&harness.store, &namespace, incarnation).await;
    let covered = search(LateInteractionSearchRequest {
        store: &harness.store,
        bootstrap_cache: None,
        encoder_provider: encoder_provider.as_ref(),
        namespace: &namespace,
        accepted_modalities: vec![InputModality::Text],
        manifest: covered_manifest,
        text: "coverage query",
        top_k: 10,
        effective_filter: None,
        consistency: ConsistencyLevel::Strong,
        semantic_wait: Duration::ZERO,
        max_overlay_bytes: 64 * 1024 * 1024,
        segment_config: Default::default(),
        manifest_refresh: ManifestRefresh::Fixed,
    })
    .await
    .expect("covered strong search must succeed");
    assert_eq!(covered.semantic_coverage, LateInteractionCoverage::Complete);
    assert_eq!(
        covered
            .results
            .iter()
            .map(|result| result.id.as_str())
            .collect::<Vec<_>>(),
        vec!["covered"]
    );

    writer
        .append_retrieval_units(
            &namespace,
            vec![text_record(
                "pending",
                "new pending semantic record",
                HashMap::from([(
                    "content".to_string(),
                    AttributeValue::String("pending lexical text".to_string()),
                )]),
            )],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("pending append must succeed");
    let pending_manifest = read_manifest(&harness.store, &namespace, incarnation).await;
    let expected_coverage = pending_manifest
        .semantic_coverage
        .as_ref()
        .expect("pending root must carry coverage")
        .clone();
    let error = search(LateInteractionSearchRequest {
        store: &harness.store,
        bootstrap_cache: None,
        encoder_provider: encoder_provider.as_ref(),
        namespace: &namespace,
        accepted_modalities: vec![InputModality::Text],
        manifest: pending_manifest.clone(),
        text: "coverage query",
        top_k: 10,
        effective_filter: None,
        consistency: ConsistencyLevel::Strong,
        semantic_wait: Duration::ZERO,
        max_overlay_bytes: 64 * 1024 * 1024,
        segment_config: Default::default(),
        manifest_refresh: ManifestRefresh::Live,
    })
    .await
    .expect_err("zero-budget strong search must report lag");
    match error {
        ZeppelinError::LateInteraction(LateInteractionError::SemanticIndexLag {
            requested_generation,
            covered_sequence,
            pending_records,
            failed_records,
        }) => {
            assert_eq!(requested_generation, pending_manifest.version());
            assert_eq!(covered_sequence, expected_coverage.contiguous_sequence);
            assert_eq!(pending_records, 1);
            assert_eq!(failed_records, 0);
            println!(
                "late_interaction_lag_example requested_generation={requested_generation} \
                 covered_sequence={covered_sequence} pending_records={pending_records} \
                 failed_records={failed_records}"
            );
        }
        other => panic!("expected SemanticIndexLag, got {other}"),
    }

    let eventual = search(LateInteractionSearchRequest {
        store: &harness.store,
        bootstrap_cache: None,
        encoder_provider: encoder_provider.as_ref(),
        namespace: &namespace,
        accepted_modalities: vec![InputModality::Text],
        manifest: pending_manifest.clone(),
        text: "coverage query",
        top_k: 10,
        effective_filter: None,
        consistency: ConsistencyLevel::Eventual,
        semantic_wait: Duration::ZERO,
        max_overlay_bytes: 64 * 1024 * 1024,
        segment_config: Default::default(),
        manifest_refresh: ManifestRefresh::Fixed,
    })
    .await
    .expect("eventual search must return covered rows");
    assert_eq!(eventual.semantic_coverage, LateInteractionCoverage::Partial);
    assert_eq!(eventual.pending_records, 1);
    assert_eq!(eventual.failed_records, 0);
    assert_eq!(
        eventual
            .results
            .iter()
            .map(|result| result.id.as_str())
            .collect::<Vec<_>>(),
        vec!["covered"]
    );

    let (paused_store, manifest_pause) = pause_first_get_matching(&harness.store, "/manifest.json");
    let task_store = paused_store.clone();
    let task_namespace = namespace.clone();
    let task_provider = Arc::clone(&encoder_provider);
    let waiting_search = tokio::spawn(async move {
        search(LateInteractionSearchRequest {
            store: &task_store,
            bootstrap_cache: None,
            encoder_provider: task_provider.as_ref(),
            namespace: &task_namespace,
            accepted_modalities: vec![InputModality::Text],
            manifest: pending_manifest,
            text: "coverage query",
            top_k: 10,
            effective_filter: None,
            consistency: ConsistencyLevel::Strong,
            semantic_wait: Duration::from_secs(5),
            max_overlay_bytes: 64 * 1024 * 1024,
            segment_config: Default::default(),
            manifest_refresh: ManifestRefresh::Live,
        })
        .await
    });
    manifest_pause.wait_until_paused().await;

    let coordinator = coordinator(&harness.store, Arc::clone(&encoder_provider));
    let report = coordinator
        .discover_and_admit(&namespace, incarnation, Some(TEXT_MODALITIES), 10, u64::MAX)
        .await
        .expect("pending discovery must succeed");
    assert_eq!(report.admitted_fragments, 1);
    coordinator
        .wait_for_idle()
        .await
        .expect("pending enrichment must complete");
    manifest_pause.release();
    let waited = waiting_search
        .await
        .expect("waiting search task must join")
        .expect("waiting strong search must restart on covered root");
    assert_eq!(waited.semantic_coverage, LateInteractionCoverage::Complete);
    assert_eq!(waited.pending_records, 0);
    assert_eq!(
        waited
            .results
            .iter()
            .map(|result| result.id.as_str())
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from(["covered", "pending"])
    );
    coordinator.shutdown().await.expect("shutdown must join");

    harness.cleanup_artifact_origin_namespace(&namespace).await;
}

#[tokio::test]
async fn tombstones_and_newer_pending_versions_suppress_stale_overlays() {
    require_minio();
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("late-query-suppression");
    let (profile, incarnation) = setup_profile(&harness.store, &namespace).await;
    let (encoder_provider, _) = provider(&profile);
    let writer = WalWriter::new(harness.store.clone());
    writer
        .append_retrieval_units(
            &namespace,
            vec![
                text_record(
                    "to-delete",
                    "record that will be tombstoned",
                    HashMap::from([(
                        "content".to_string(),
                        AttributeValue::String("delete target".to_string()),
                    )]),
                ),
                text_record(
                    "mutable",
                    "old mutable version",
                    HashMap::from([(
                        "content".to_string(),
                        AttributeValue::String("old mutable".to_string()),
                    )]),
                ),
            ],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("initial append must succeed");
    enrich_all(
        &harness.store,
        &namespace,
        incarnation,
        Arc::clone(&encoder_provider),
    )
    .await;

    let initial = search(LateInteractionSearchRequest {
        store: &harness.store,
        bootstrap_cache: None,
        encoder_provider: encoder_provider.as_ref(),
        namespace: &namespace,
        accepted_modalities: vec![InputModality::Text],
        manifest: read_manifest(&harness.store, &namespace, incarnation).await,
        text: "suppression query",
        top_k: 10,
        effective_filter: None,
        consistency: ConsistencyLevel::Strong,
        semantic_wait: Duration::ZERO,
        max_overlay_bytes: 64 * 1024 * 1024,
        segment_config: Default::default(),
        manifest_refresh: ManifestRefresh::Fixed,
    })
    .await
    .expect("initial covered search must succeed");
    assert_eq!(
        initial
            .results
            .iter()
            .map(|result| result.id.as_str())
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from(["mutable", "to-delete"])
    );

    writer
        .append_retrieval_units(
            &namespace,
            Vec::new(),
            vec!["to-delete".to_string()],
            Vec::new(),
        )
        .await
        .expect("tombstone append must succeed");
    let after_delete = search(LateInteractionSearchRequest {
        store: &harness.store,
        bootstrap_cache: None,
        encoder_provider: encoder_provider.as_ref(),
        namespace: &namespace,
        accepted_modalities: vec![InputModality::Text],
        manifest: read_manifest(&harness.store, &namespace, incarnation).await,
        text: "suppression query",
        top_k: 10,
        effective_filter: None,
        consistency: ConsistencyLevel::Strong,
        semantic_wait: Duration::ZERO,
        max_overlay_bytes: 64 * 1024 * 1024,
        segment_config: Default::default(),
        manifest_refresh: ManifestRefresh::Fixed,
    })
    .await
    .expect("tombstone-only root must remain fully covered");
    assert_eq!(
        after_delete.semantic_coverage,
        LateInteractionCoverage::Complete
    );
    assert_eq!(
        after_delete
            .results
            .iter()
            .map(|result| result.id.as_str())
            .collect::<Vec<_>>(),
        vec!["mutable"]
    );

    writer
        .append_retrieval_units(
            &namespace,
            vec![text_record(
                "mutable",
                "new mutable version not yet enriched",
                HashMap::from([(
                    "content".to_string(),
                    AttributeValue::String("new mutable".to_string()),
                )]),
            )],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("new mutable version append must succeed");
    let pending_manifest = read_manifest(&harness.store, &namespace, incarnation).await;
    let strong_error = search(LateInteractionSearchRequest {
        store: &harness.store,
        bootstrap_cache: None,
        encoder_provider: encoder_provider.as_ref(),
        namespace: &namespace,
        accepted_modalities: vec![InputModality::Text],
        manifest: pending_manifest.clone(),
        text: "suppression query",
        top_k: 10,
        effective_filter: None,
        consistency: ConsistencyLevel::Strong,
        semantic_wait: Duration::ZERO,
        max_overlay_bytes: 64 * 1024 * 1024,
        segment_config: Default::default(),
        manifest_refresh: ManifestRefresh::Fixed,
    })
    .await
    .expect_err("newer pending version must lag rather than return stale output");
    assert!(matches!(
        strong_error,
        ZeppelinError::LateInteraction(LateInteractionError::SemanticIndexLag {
            pending_records: 1,
            failed_records: 0,
            ..
        })
    ));
    let eventual = search(LateInteractionSearchRequest {
        store: &harness.store,
        bootstrap_cache: None,
        encoder_provider: encoder_provider.as_ref(),
        namespace: &namespace,
        accepted_modalities: vec![InputModality::Text],
        manifest: pending_manifest,
        text: "suppression query",
        top_k: 10,
        effective_filter: None,
        consistency: ConsistencyLevel::Eventual,
        semantic_wait: Duration::ZERO,
        max_overlay_bytes: 64 * 1024 * 1024,
        segment_config: Default::default(),
        manifest_refresh: ManifestRefresh::Fixed,
    })
    .await
    .expect("eventual search must omit pending final versions");
    assert_eq!(eventual.semantic_coverage, LateInteractionCoverage::Partial);
    assert!(
        eventual.results.is_empty(),
        "neither the tombstoned row nor the stale mutable overlay may return"
    );

    harness.cleanup_artifact_origin_namespace(&namespace).await;
}

#[tokio::test]
async fn http_rrf_uses_late_and_bm25_ranks_and_rejects_weighted_fusion() {
    require_minio();
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("late-query-fusion");
    let (profile, incarnation) = setup_profile(&harness.store, &namespace).await;
    let (encoder_provider, _) = provider(&profile);
    WalWriter::new(harness.store.clone())
        .append_retrieval_units(
            &namespace,
            vec![
                text_record(
                    "doc-alpha",
                    "fusion semantic query closest alpha",
                    HashMap::from([(
                        "content".to_string(),
                        AttributeValue::String("fusion fusion fusion".to_string()),
                    )]),
                ),
                text_record(
                    "doc-beta",
                    "fusion semantic query beta",
                    HashMap::from([(
                        "content".to_string(),
                        AttributeValue::String("fusion".to_string()),
                    )]),
                ),
                text_record(
                    "doc-gamma",
                    "unrelated gamma semantic text",
                    HashMap::from([(
                        "content".to_string(),
                        AttributeValue::String("fusion fusion".to_string()),
                    )]),
                ),
                text_record(
                    "doc-delta",
                    "another unrelated semantic record",
                    HashMap::from([(
                        "content".to_string(),
                        AttributeValue::String("ordinary lexical text".to_string()),
                    )]),
                ),
            ],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("fusion fixture append must succeed");
    enrich_all(&harness.store, &namespace, incarnation, encoder_provider).await;

    let mut config = Config::default();
    config.mmli.allow_dev_encoder = true;
    let (base_url, _cache, _cache_dir, admin_bearer) = start_test_server_on_store_with_config(
        &harness,
        harness.store.clone(),
        Some(harness.prefix.clone()),
        config,
    )
    .await;
    let client = client_with_bearer(&admin_bearer);
    let endpoint = format!("{base_url}/v1/namespaces/{namespace}/query");
    let late_source = json!({
        "type": "late_interaction",
        "text": "fusion semantic query",
        "top_k": 4,
        "semantic_wait_ms": 1_000
    });
    let bm25_source = json!({
        "type": "bm25",
        "rank_by": ["content", "BM25", "fusion"]
    });
    let request_for = |sources: Value, fusion: Option<Value>, explain: bool| {
        let mut request = json!({
            "sources": sources,
            "candidate_k": 4,
            "top_k": 4,
            "consistency": "strong",
            "projection": {"include_attributes": false},
            "explain": explain
        });
        if let Some(fusion) = fusion {
            request["fusion"] = fusion;
        }
        request
    };
    let post_ok = |body: Value| {
        let client = client.clone();
        let endpoint = endpoint.clone();
        async move {
            let response = client
                .post(endpoint)
                .json(&body)
                .send()
                .await
                .expect("query request must complete");
            let status = response.status();
            let bytes = response.bytes().await.expect("response body must load");
            assert_eq!(
                status.as_u16(),
                200,
                "query failed: {}",
                String::from_utf8_lossy(&bytes)
            );
            serde_json::from_slice::<Value>(&bytes).expect("query response must decode")
        }
    };

    let late = post_ok(request_for(json!([late_source.clone()]), None, true)).await;
    assert_eq!(late["semantic_coverage"], "complete");
    let late_explain = &late["explain"]["plan"]["sources"][0];
    assert_eq!(late_explain["type"], "late_interaction");
    assert_eq!(late_explain["score_direction"], "higher_is_better");
    assert_eq!(late_explain["consistency_actual"], "strong");
    assert!(
        late_explain["profile"].is_string()
            && late_explain["epoch"].is_array()
            && late_explain["fde_generation"].is_array()
            && late_explain["manifest_generation"].is_number(),
        "late explain metadata must identify the exact semantic snapshot: {late_explain}"
    );
    let late_results = late["results"]
        .as_array()
        .expect("late results must be an array");
    assert_eq!(late_results.len(), 4);

    let bm25 = post_ok(request_for(json!([bm25_source.clone()]), None, false)).await;
    let bm25_results = bm25["results"]
        .as_array()
        .expect("BM25 results must be an array");
    assert!(!bm25_results.is_empty());

    let mut expected_scores = BTreeMap::<String, f32>::new();
    for results in [late_results, bm25_results] {
        for (rank, result) in results.iter().enumerate() {
            let id = result["id"]
                .as_str()
                .expect("source result must carry an id")
                .to_string();
            *expected_scores.entry(id).or_default() += 1.0_f32 / (60.0 + rank as f32 + 1.0);
        }
    }
    let mut expected = expected_scores.into_iter().collect::<Vec<_>>();
    expected.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });

    let hybrid = post_ok(request_for(
        json!([late_source.clone(), bm25_source.clone()]),
        Some(json!({"type": "rrf", "k": 60})),
        true,
    ))
    .await;
    assert_eq!(hybrid["semantic_coverage"], "complete");
    assert_eq!(
        hybrid["explain"]["plan"]["fusion"],
        json!({"type": "rrf", "k": 60})
    );
    let actual = hybrid["results"]
        .as_array()
        .expect("hybrid results must be an array");
    assert_eq!(actual.len(), expected.len());
    for (result, (expected_id, expected_score)) in actual.iter().zip(expected) {
        assert_eq!(result["id"], expected_id);
        let actual_score = result["score"]
            .as_f64()
            .expect("hybrid result score must be numeric");
        assert!(
            (actual_score - f64::from(expected_score)).abs() < 1.0e-7,
            "RRF score mismatch for {expected_id}: expected {expected_score}, got {actual_score}"
        );
    }

    let weighted = client
        .post(&endpoint)
        .json(&request_for(
            json!([late_source, bm25_source]),
            Some(json!({"type": "weighted", "weights": [0.5, 0.5]})),
            false,
        ))
        .send()
        .await
        .expect("weighted request must complete");
    assert_eq!(weighted.status().as_u16(), 400);
    let weighted_body: Value = weighted
        .json()
        .await
        .expect("weighted rejection must be typed JSON");
    assert_eq!(
        weighted_body["code"],
        "LATE_INTERACTION_WEIGHTED_FUSION_UNSUPPORTED"
    );
    assert_eq!(weighted_body["status"], 400);
    assert_eq!(weighted_body["retryable"], false);

    harness.cleanup_artifact_origin_namespace(&namespace).await;
    harness.cleanup().await;
}
