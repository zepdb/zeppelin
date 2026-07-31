mod common;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use uuid::Uuid;
use zeppelin::compaction::Compactor;
use zeppelin::config::{Config, MmliConfig, MmliSegmentConfig};
use zeppelin::embedding::{
    ArtifactChecksum, DeterministicDev, EmbeddingProfileId, EmbeddingProfileRef,
    EncoderExecutionRef, EncoderInputRef, ExactScorerVersion, FdeRecipe, FdeTransformArtifactRef,
    ImageObjectRef, InputModality, MatrixDtype, MultiVectorEncoderProvider,
    MultiVectorEncoderRegistry, MultiVectorEpoch, MultiVectorEpochId, NormalizationRecipe,
    RetrievalUnitRecord, TextContentRef, VectorTransformRecipe, DETERMINISTIC_DEV_IMPLEMENTATION,
    DETERMINISTIC_DEV_VERSION,
};
use zeppelin::index::late_interaction::{
    search, FdeAlgorithmVersion, FdeParams, FdeTransform, FinalProjection, InnerProjection,
    LateInteractionCoverage, LateInteractionSearchOutput, LateInteractionSearchRequest,
    ManifestRefresh,
};
use zeppelin::namespace::NamespaceManager;
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::{AttributeValue, ConsistencyLevel, DistanceMetric, Filter, IndexType};
use zeppelin::wal::{LeaseManager, Manifest, SourceInventoryRef, WalReader, WalWriter};

use common::counting::counting_store;
use common::harness::TestHarness;

fn require_minio() {
    assert_eq!(
        std::env::var("TEST_BACKEND").as_deref(),
        Ok("minio"),
        "Phase 9 late-segment lifecycle requires TEST_BACKEND=minio"
    );
}

fn text_record(id: &str, text: &str, color: &str) -> RetrievalUnitRecord {
    let input = EncoderInputRef::Text {
        content: TextContentRef::Inline(text.to_string()),
    };
    RetrievalUnitRecord {
        id: id.to_string(),
        content_hash: input.content_hash().expect("text fixture must hash"),
        input,
        parent_id: Some("text-parent".to_string()),
        unit_ordinal: None,
        attributes: Some(HashMap::from([(
            "color".to_string(),
            AttributeValue::String(color.to_string()),
        )])),
    }
}

fn image_record(
    namespace: &str,
    id: &str,
    color: &str,
) -> (RetrievalUnitRecord, SourceInventoryRef, Bytes) {
    let bytes = Bytes::from_static(&[0xff, 0xd8, 0xff, 0xd9]);
    let checksum = ArtifactChecksum::digest(&bytes);
    let mut input = EncoderInputRef::Image {
        image: ImageObjectRef {
            key: String::new(),
            checksum,
            media_type: "image/jpeg".to_string(),
            encoded_size_bytes: bytes.len() as u64,
            width: 2,
            height: 2,
        },
    };
    let content_hash = input.content_hash().expect("image fixture must hash");
    let key = SourceInventoryRef::s3_key(namespace, content_hash);
    if let EncoderInputRef::Image { image } = &mut input {
        image.key.clone_from(&key);
    }
    let source = SourceInventoryRef {
        key,
        checksum,
        size_bytes: bytes.len() as u64,
        media_type: "image/jpeg".to_string(),
        artifact_origin: None,
    };
    (
        RetrievalUnitRecord {
            id: id.to_string(),
            content_hash,
            input,
            parent_id: Some("image-parent".to_string()),
            unit_ordinal: Some(0),
            attributes: Some(HashMap::from([(
                "color".to_string(),
                AttributeValue::String(color.to_string()),
            )])),
        },
        source,
        bytes,
    )
}

async fn setup_profile(store: &ZeppelinStore, namespace: &str) -> (EmbeddingProfileRef, Uuid) {
    let metadata = NamespaceManager::new(store.clone())
        .create_typed_with_fts_and_index_config(
            namespace,
            0,
            DistanceMetric::DotProduct,
            IndexType::LateInteractionFde,
            Some(zeppelin::embedding::LateInteractionNamespaceConfig {
                accepted_modalities: vec![InputModality::Text, InputModality::Image],
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
            supported_modalities: vec![InputModality::Text, InputModality::Image],
        },
        preprocessing_digest: ArtifactChecksum::digest(b"phase-9-segment-test-v1"),
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
        .expect("transform upload must succeed");
    let mut fde = FdeRecipe {
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
    fde.generation = fde
        .canonical_generation()
        .expect("FDE generation must canonicalize");
    let profile = EmbeddingProfileRef {
        profile: EmbeddingProfileId::new("phase-9-segment-test"),
        epoch,
        fde,
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

fn provider(profile: &EmbeddingProfileRef) -> Arc<dyn MultiVectorEncoderProvider> {
    let registry = Arc::new(MultiVectorEncoderRegistry::new());
    registry
        .register(Arc::new(
            DeterministicDev::new(true, &profile.epoch).expect("dev encoder must construct"),
        ))
        .expect("dev encoder must register");
    registry
}

async fn enrich_all(
    store: &ZeppelinStore,
    namespace: &str,
    incarnation: Uuid,
    profile: &EmbeddingProfileRef,
) {
    let coordinator = zeppelin::embedding::EnrichmentCoordinator::start(
        store.clone(),
        Arc::new(LeaseManager::new(
            store.clone(),
            format!("phase-9-segment-test-{}", Uuid::new_v4()),
            Duration::from_secs(30),
        )),
        provider(profile),
        zeppelin::embedding::EnrichmentCoordinatorOptions {
            queue_capacity: 8,
            max_retry_attempts: 4,
            checkpoint: None,
        },
    );
    let report = coordinator
        .discover_and_admit(namespace, incarnation, usize::MAX, u64::MAX)
        .await
        .expect("enrichment discovery must succeed");
    assert!(report.admitted_fragments > 0);
    coordinator
        .wait_for_idle()
        .await
        .expect("enrichment must complete");
    coordinator.shutdown().await.expect("shutdown must join");
}

fn mmli_config() -> MmliConfig {
    MmliConfig {
        allow_dev_encoder: true,
        segment: MmliSegmentConfig {
            nlist: 2,
            probe_budget: 2,
            candidate_k: 16,
            kmeans_max_iterations: 10,
            max_matrix_object_bytes: 1024 * 1024,
            max_cluster_object_bytes: 1024 * 1024,
            max_resident_bootstrap_bytes: 1024 * 1024,
            read_gap_budget_bytes: 16 * 1024,
            read_max_request_bytes: 1024 * 1024,
            read_max_concurrency: 2,
            ..MmliSegmentConfig::default()
        },
        ..MmliConfig::default()
    }
}

fn compactor(store: &ZeppelinStore, mmli: &MmliConfig) -> Compactor {
    let config = Config::default();
    Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        config.compaction,
        config.indexing,
        common::default_gc_upload_window(),
    )
    .with_mmli_config(mmli.clone())
}

async fn read_manifest(store: &ZeppelinStore, namespace: &str, incarnation: Uuid) -> Manifest {
    Manifest::read_versioned_required_for_incarnation(store, namespace, incarnation)
        .await
        .expect("manifest must load")
        .0
}

async fn run_query(
    store: &ZeppelinStore,
    namespace: &str,
    manifest: Manifest,
    provider: &dyn MultiVectorEncoderProvider,
    mmli: &MmliConfig,
    filter: Option<&Filter>,
) -> LateInteractionSearchOutput {
    search(LateInteractionSearchRequest {
        store,
        bootstrap_cache: None,
        encoder_provider: provider,
        namespace,
        manifest,
        text: "phase nine deterministic lifecycle query",
        top_k: 16,
        effective_filter: filter,
        consistency: ConsistencyLevel::Strong,
        semantic_wait: Duration::from_millis(1),
        max_overlay_bytes: 16 * 1024 * 1024,
        segment_config: mmli.segment.clone(),
        manifest_refresh: ManifestRefresh::Fixed,
    })
    .await
    .expect("late search must succeed")
}

fn assert_same_results(left: &LateInteractionSearchOutput, right: &LateInteractionSearchOutput) {
    assert_eq!(left.results.len(), right.results.len());
    for (left, right) in left.results.iter().zip(&right.results) {
        assert_eq!(left.id, right.id);
        assert_eq!(left.score, right.score);
        assert_eq!(left.parent_id, right.parent_id);
        assert_eq!(left.unit_ordinal, right.unit_ordinal);
        assert_eq!(left.attributes, right.attributes);
    }
}

#[tokio::test]
async fn late_segment_full_rebuild_lifecycle_matches_oracle_and_traces_two_waves() {
    require_minio();
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("late-segment-lifecycle");
    let (profile, incarnation) = setup_profile(&harness.store, &namespace).await;
    let provider = provider(&profile);
    let mmli = mmli_config();
    let (image, source, source_bytes) = image_record(&namespace, "image-c", "blue");
    let source_key = source.key.clone();
    let (first_fragment, _) = WalWriter::new(harness.store.clone())
        .append_retrieval_units(
            &namespace,
            vec![
                text_record("doc-a", "first alpha version", "red"),
                text_record("doc-b", "first beta version", "blue"),
                image,
            ],
            Vec::new(),
            vec![(source, source_bytes)],
        )
        .await
        .expect("first typed append must succeed");
    enrich_all(&harness.store, &namespace, incarnation, &profile).await;

    let before_first = read_manifest(&harness.store, &namespace, incarnation).await;
    let before_first_section_ref = before_first
        .late_state
        .as_ref()
        .expect("enrichment must publish a section")
        .key
        .clone();
    let exhaustive_oracle = run_query(
        &harness.store,
        &namespace,
        before_first.clone(),
        provider.as_ref(),
        &mmli,
        None,
    )
    .await;
    assert_eq!(
        exhaustive_oracle.semantic_coverage,
        LateInteractionCoverage::Complete
    );
    assert!(exhaustive_oracle.read_trace.is_none());

    let first_compaction = compactor(&harness.store, &mmli)
        .compact(&namespace)
        .await
        .expect("first late compaction must succeed");
    let first_segment_id = first_compaction
        .segment_id
        .expect("first compaction must publish a segment");
    let after_first = read_manifest(&harness.store, &namespace, incarnation).await;
    assert!(after_first.input_fragments.is_empty());
    assert!(after_first
        .pending_deletes
        .iter()
        .any(|key| key.ends_with(&format!("{}.wal", first_fragment.id))));
    assert!(after_first
        .pending_deletes
        .contains(&before_first_section_ref));
    let first_section = after_first
        .load_late_state(&harness.store)
        .await
        .expect("first segment section must load")
        .expect("first segment section must exist");
    assert!(first_section.semantic_overlays.is_empty());
    assert_eq!(
        first_section.active_late_segment.as_deref(),
        Some(first_segment_id.as_str())
    );
    assert_eq!(first_section.late_interaction_segments.len(), 1);
    assert!(first_section
        .source_inventory
        .iter()
        .any(|source| source.key == source_key));

    let (counted_store, counter) = counting_store(&harness.store);
    counter.reset();
    let first_segment_query = run_query(
        &counted_store,
        &namespace,
        after_first.clone(),
        provider.as_ref(),
        &mmli,
        None,
    )
    .await;
    assert_same_results(&exhaustive_oracle, &first_segment_query);
    let first_trace = first_segment_query
        .read_trace
        .expect("segment query must expose its two-wave trace");
    assert!(first_trace.candidate_wave.logical_ranges > 0);
    assert!(first_trace.candidate_wave.planned_requests > 0);
    assert!(first_trace.truth_wave.logical_ranges > 0);
    assert!(first_trace.truth_wave.planned_requests > 0);
    assert!(
        first_trace.candidate_wave.planned_requests <= first_trace.candidate_wave.logical_ranges
    );
    assert!(first_trace.truth_wave.planned_requests <= first_trace.truth_wave.logical_ranges);
    let observed_candidate_gets = counter.gets_matching("candidate-cluster-");
    let observed_truth_gets = counter.gets_matching("/matrix_") + counter.gets_matching("/attrs_");
    assert_eq!(
        observed_candidate_gets,
        first_trace.candidate_wave.planned_requests as u64
    );
    assert_eq!(
        observed_truth_gets,
        first_trace.truth_wave.planned_requests as u64
    );
    println!(
        "phase9_trace candidate={}=>{} truth={}=>{} bytes={}",
        first_trace.candidate_wave.logical_ranges,
        first_trace.candidate_wave.planned_requests,
        first_trace.truth_wave.logical_ranges,
        first_trace.truth_wave.planned_requests,
        first_trace.candidate_wave.planned_bytes + first_trace.truth_wave.planned_bytes,
    );

    let blue = Filter::Eq {
        field: "color".to_string(),
        value: AttributeValue::String("blue".to_string()),
    };
    counter.reset();
    let filtered = run_query(
        &counted_store,
        &namespace,
        after_first.clone(),
        provider.as_ref(),
        &mmli,
        Some(&blue),
    )
    .await;
    assert!(filtered.results.iter().all(|result| {
        result
            .attributes
            .as_ref()
            .and_then(|attributes| attributes.get("color"))
            == Some(&AttributeValue::String("blue".to_string()))
    }));
    let filtered_trace = filtered
        .read_trace
        .expect("filtered segment query must retain the two-wave trace");
    assert_eq!(
        filtered_trace.candidate_wave.planned_requests,
        first_trace.candidate_wave.planned_requests
    );
    assert!(filtered_trace.truth_wave.planned_requests > 0);
    assert!(filtered_trace.truth_wave.planned_requests <= first_trace.truth_wave.planned_requests);
    assert_eq!(
        counter.gets_matching("candidate-cluster-"),
        filtered_trace.candidate_wave.planned_requests as u64
    );
    assert_eq!(
        counter.gets_matching("/matrix_") + counter.gets_matching("/attrs_"),
        filtered_trace.truth_wave.planned_requests as u64
    );

    let (second_fragment, _) = WalWriter::new(harness.store.clone())
        .append_retrieval_units(
            &namespace,
            vec![
                text_record("doc-a", "second alpha version", "blue"),
                text_record("doc-d", "new delta version", "red"),
            ],
            vec!["doc-b".to_string()],
            Vec::new(),
        )
        .await
        .expect("second typed append must succeed");
    enrich_all(&harness.store, &namespace, incarnation, &profile).await;
    let before_second = read_manifest(&harness.store, &namespace, incarnation).await;
    let before_second_section_ref = before_second
        .late_state
        .as_ref()
        .expect("second enrichment must publish a section")
        .key
        .clone();
    let merged_oracle = run_query(
        &harness.store,
        &namespace,
        before_second,
        provider.as_ref(),
        &mmli,
        None,
    )
    .await;
    assert!(!merged_oracle
        .results
        .iter()
        .any(|result| result.id == "doc-b"));

    let second_compaction = compactor(&harness.store, &mmli)
        .compact(&namespace)
        .await
        .expect("second late compaction must succeed");
    assert_eq!(
        second_compaction.old_segment_removed.as_deref(),
        Some(first_segment_id.as_str())
    );
    let second_segment_id = second_compaction
        .segment_id
        .expect("second compaction must publish a replacement segment");
    assert_ne!(second_segment_id, first_segment_id);
    let after_second = read_manifest(&harness.store, &namespace, incarnation).await;
    assert!(after_second.input_fragments.is_empty());
    assert!(after_second
        .pending_deletes
        .iter()
        .any(|key| key.ends_with(&format!("{}.wal", second_fragment.id))));
    assert!(after_second
        .pending_deletes
        .contains(&before_second_section_ref));
    let second_section = after_second
        .load_late_state(&harness.store)
        .await
        .expect("replacement segment section must load")
        .expect("replacement segment section must exist");
    assert!(second_section.semantic_overlays.is_empty());
    assert_eq!(
        second_section.active_late_segment.as_deref(),
        Some(second_segment_id.as_str())
    );
    assert_eq!(second_section.late_interaction_segments.len(), 1);
    assert!(second_section
        .source_inventory
        .iter()
        .any(|source| source.key == source_key));
    let rebuilt_query = run_query(
        &harness.store,
        &namespace,
        after_second,
        provider.as_ref(),
        &mmli,
        None,
    )
    .await;
    assert_same_results(&merged_oracle, &rebuilt_query);
    assert!(rebuilt_query.read_trace.is_some());

    harness.cleanup_artifact_origin_namespace(&namespace).await;
}
