mod common;
#[path = "common/mmli_tensor.rs"]
mod mmli_tensor;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use uuid::Uuid;
use zeppelin::compaction::Compactor;
use zeppelin::config::{Config, MmliConfig, MmliSegmentConfig};
use zeppelin::embedding::{
    ArtifactChecksum, CandidateDocumentPooling, CenteringArtifact, DeterministicDev,
    EmbeddingProfileId, EmbeddingProfileRef, EncoderDocumentInput, EncoderExecutionRef,
    EncoderInputRef, ExactScorerVersion, FdeRecipe, FdeTransformArtifactRef, ImageObjectRef,
    InputModality, MatrixDtype, MeanVectorRef, MultiVectorEncoder, MultiVectorEncoderProvider,
    MultiVectorEncoderRegistry, MultiVectorEpoch, MultiVectorEpochId, NormalizationRecipe,
    RetrievalUnitRecord, TextContentRef, VectorTransformRecipe, CENTERING_ARTIFACT_FORMAT_VERSION,
    DETERMINISTIC_DEV_IMPLEMENTATION, DETERMINISTIC_DEV_VERSION,
};
use zeppelin::index::late_interaction::{
    search, FdeAlgorithmVersion, FdeParams, FdeTransform, FinalProjection, InnerProjection,
    LateInteractionCoverage, LateInteractionRankedResult, LateInteractionSearchOutput,
    LateInteractionSearchRequest, ManifestRefresh,
};
use zeppelin::namespace::NamespaceManager;
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::{AttributeValue, ConsistencyLevel, DistanceMetric, Filter, IndexType};
use zeppelin::wal::{LeaseManager, Manifest, SourceInventoryRef, WalReader, WalWriter};

use common::counting::counting_store;
use common::harness::TestHarness;
use mmli_tensor::{
    load_config_e_gold_ranks, production_document_id, FileBackedF16Tensor,
    FileBackedMultiVectorEncoder,
};

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

fn recall_fixture() -> (Vec<Vec<RetrievalUnitRecord>>, Vec<String>) {
    const GROUPS: usize = 500;
    const DOCUMENTS_PER_GROUP: usize = 10;
    const FRAGMENTS: usize = 5;

    let mut fragments = vec![Vec::new(); FRAGMENTS];
    for group in 0..GROUPS {
        for variant in 0..DOCUMENTS_PER_GROUP {
            let id = format!("recall-f{}-g{group:03}-v{variant:02}", group % FRAGMENTS);
            let text = format!("controlled recall group {group:03} variant {variant:02}");
            fragments[group % FRAGMENTS].push(text_record(&id, &text, "recall"));
        }
    }

    let queries = (0..50)
        .map(|index| {
            let group = index * 10;
            format!("phase9-recall-query-{group:03}")
        })
        .collect();
    (fragments, queries)
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
    setup_profile_with_recipe(
        store,
        namespace,
        8,
        16,
        FdeParams {
            algorithm: FdeAlgorithmVersion::PaperV1,
            repetitions: 2,
            simhash_bits: 1,
            input_dimension: 8,
            inner: InnerProjection::Rademacher { d_proj: 4 },
            final_projection: FinalProjection::None,
        },
        17,
        VectorTransformRecipe::Identity,
        b"phase-9-segment-test-v1",
    )
    .await
}

fn test_epoch(
    vector_dimension: u32,
    max_vectors: u32,
    preprocessing_digest: &[u8],
) -> MultiVectorEpoch {
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
        preprocessing_digest: ArtifactChecksum::digest(preprocessing_digest),
        vector_dimension,
        max_query_vectors: max_vectors,
        max_document_vectors: max_vectors,
        output_normalization: NormalizationRecipe::L2,
        exact_scoring_transform: VectorTransformRecipe::Identity,
        matrix_dtype: MatrixDtype::F16,
        exact_scorer: ExactScorerVersion::MaxSimV1,
    };
    epoch.id = epoch.canonical_id().expect("epoch must canonicalize");
    epoch
}

async fn setup_profile_with_recipe(
    store: &ZeppelinStore,
    namespace: &str,
    vector_dimension: u32,
    max_vectors: u32,
    params: FdeParams,
    transform_seed: u64,
    candidate_vector_transform: VectorTransformRecipe,
    preprocessing_digest: &[u8],
) -> (EmbeddingProfileRef, Uuid) {
    let epoch = test_epoch(vector_dimension, max_vectors, preprocessing_digest);
    setup_profile_for_epoch(
        store,
        namespace,
        epoch,
        params,
        transform_seed,
        candidate_vector_transform,
        CandidateDocumentPooling::Identity,
        "phase-9-segment-test",
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn setup_profile_for_epoch(
    store: &ZeppelinStore,
    namespace: &str,
    epoch: MultiVectorEpoch,
    params: FdeParams,
    transform_seed: u64,
    candidate_vector_transform: VectorTransformRecipe,
    candidate_document_pooling: CandidateDocumentPooling,
    profile_id: &str,
) -> (EmbeddingProfileRef, Uuid) {
    let metadata = NamespaceManager::new(store.clone())
        .create_typed_with_fts_and_index_config(
            namespace,
            0,
            DistanceMetric::DotProduct,
            IndexType::LateInteractionFde,
            Some(zeppelin::embedding::LateInteractionNamespaceConfig {
                accepted_modalities: epoch.encoder.supported_modalities.clone(),
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

    assert_eq!(params.input_dimension, epoch.vector_dimension);
    let transform =
        FdeTransform::generate(&params, transform_seed).expect("transform must generate");
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
        candidate_vector_transform,
        candidate_document_pooling,
    };
    fde.generation = fde
        .canonical_generation()
        .expect("FDE generation must canonicalize");
    let profile = EmbeddingProfileRef {
        profile: EmbeddingProfileId::new(profile_id),
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

async fn dev_centering_transform(
    store: &ZeppelinStore,
    namespace: &str,
    epoch: &MultiVectorEpoch,
    fragments: &[Vec<RetrievalUnitRecord>],
) -> VectorTransformRecipe {
    const CENTERING_SAMPLE_ROWS: usize = 5_000;

    let inputs = fragments
        .iter()
        .flatten()
        .map(|record| {
            EncoderDocumentInput::new(record.input.clone(), record.content_hash, None)
                .expect("recall document input must validate")
        })
        .collect::<Vec<_>>();
    let encoder = DeterministicDev::new(true, epoch).expect("dev encoder must construct");
    let batch = encoder
        .encode_documents(&inputs)
        .await
        .expect("recall documents must encode for centering");
    let dimension = usize::try_from(epoch.vector_dimension).expect("dimension must fit usize");
    let rows = batch
        .embeddings()
        .iter()
        .flat_map(|embedding| embedding.values().chunks_exact(dimension))
        .collect::<Vec<_>>();
    let sample_count = rows.len().min(CENTERING_SAMPLE_ROWS);
    assert!(sample_count > 0);
    let mut mean = vec![0.0_f64; dimension];
    for sample in 0..sample_count {
        let row = rows[sample * rows.len() / sample_count];
        for (sum, value) in mean.iter_mut().zip(row) {
            *sum += f64::from(*value);
        }
    }
    let mean = mean
        .into_iter()
        .map(|sum| (sum / sample_count as f64) as f32)
        .collect::<Vec<_>>();
    store_centering_transform(store, namespace, epoch, mean).await
}

async fn store_centering_transform(
    store: &ZeppelinStore,
    namespace: &str,
    epoch: &MultiVectorEpoch,
    mean: Vec<f32>,
) -> VectorTransformRecipe {
    let encoded = CenteringArtifact::new(mean)
        .expect("centering mean must validate")
        .to_bytes()
        .expect("centering mean must encode");
    let key = format!("{namespace}/late/centering/{}", encoded.checksum().to_hex());
    store
        .put_create(&key, encoded.bytes().clone())
        .await
        .expect("centering mean upload must succeed");
    VectorTransformRecipe::SubtractMean {
        mean: MeanVectorRef {
            key,
            checksum: encoded.checksum(),
            size_bytes: encoded.bytes().len() as u64,
            vector_dimension: epoch.vector_dimension,
            format_version: u32::from(CENTERING_ARTIFACT_FORMAT_VERSION),
            artifact_origin: None,
        },
        renormalize: false,
    }
}

async fn enrich_all(
    store: &ZeppelinStore,
    namespace: &str,
    incarnation: Uuid,
    profile: &EmbeddingProfileRef,
) {
    enrich_all_with_provider(store, namespace, incarnation, provider(profile), 8).await;
}

async fn enrich_all_with_provider(
    store: &ZeppelinStore,
    namespace: &str,
    incarnation: Uuid,
    encoder_provider: Arc<dyn MultiVectorEncoderProvider>,
    queue_capacity: usize,
) {
    let coordinator = zeppelin::embedding::EnrichmentCoordinator::start(
        store.clone(),
        Arc::new(LeaseManager::new(
            store.clone(),
            format!("phase-9-segment-test-{}", Uuid::new_v4()),
            Duration::from_secs(30),
        )),
        encoder_provider,
        zeppelin::embedding::EnrichmentCoordinatorOptions {
            queue_capacity,
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
    run_query_with_text(
        store,
        namespace,
        manifest,
        provider,
        mmli,
        filter,
        "phase nine deterministic lifecycle query",
        16,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_query_with_text(
    store: &ZeppelinStore,
    namespace: &str,
    manifest: Manifest,
    provider: &dyn MultiVectorEncoderProvider,
    mmli: &MmliConfig,
    filter: Option<&Filter>,
    text: &str,
    top_k: usize,
) -> LateInteractionSearchOutput {
    search(LateInteractionSearchRequest {
        store,
        bootstrap_cache: None,
        encoder_provider: provider,
        namespace,
        manifest,
        text,
        top_k,
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

async fn measure_segment_recall(
    store: &ZeppelinStore,
    namespace: &str,
    manifest: &Manifest,
    provider: &dyn MultiVectorEncoderProvider,
    mmli: &MmliConfig,
    queries: &[String],
    gold: &[Vec<LateInteractionRankedResult>],
) -> (usize, usize, u64) {
    let mut hits = 0_usize;
    let mut gold_count = 0_usize;
    let mut planned_bytes = 0_u64;
    for (query, expected) in queries.iter().zip(gold) {
        let output = run_query_with_text(
            store,
            namespace,
            manifest.clone(),
            provider,
            mmli,
            None,
            query,
            10,
        )
        .await;
        assert_eq!(output.results.len(), 10);
        hits += expected
            .iter()
            .filter(|gold| output.results.iter().any(|actual| actual.id == gold.id))
            .count();
        gold_count += expected.len();
        let trace = output
            .read_trace
            .expect("segment recall query must use both read waves");
        planned_bytes += trace.candidate_wave.planned_bytes + trace.truth_wave.planned_bytes;
    }
    (hits, gold_count, planned_bytes / queries.len() as u64)
}

fn ranked_results_are_identical(
    left: &[LateInteractionRankedResult],
    right: &[LateInteractionRankedResult],
) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.id == right.id
                && left.score.to_bits() == right.score.to_bits()
                && left.parent_id == right.parent_id
                && left.unit_ordinal == right.unit_ordinal
                && left.attributes == right.attributes
        })
}

fn ranked_result_membership_is_identical(
    left: &[LateInteractionRankedResult],
    right: &[LateInteractionRankedResult],
) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .map(|result| result.id.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            == right
                .iter()
                .map(|result| result.id.as_str())
                .collect::<std::collections::BTreeSet<_>>()
}

fn assert_same_results(left: &LateInteractionSearchOutput, right: &LateInteractionSearchOutput) {
    assert!(ranked_results_are_identical(&left.results, &right.results));
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

#[tokio::test]
async fn late_segment_dev_recall_tripwire_and_full_probe_converge() {
    require_minio();
    const LAB_TRANSFORM_SEED: u64 = 5_570_192_190_543_495_170;
    const PREPROCESSING_DIGEST: &[u8] = b"phase-9-dev-recall-v2";
    const DOCUMENT_COUNT: usize = 5_000;
    const PINNED_TRIPWIRE_HITS: usize = 163;

    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("late-segment-recall");
    let (fragments, queries) = recall_fixture();
    let params = FdeParams {
        algorithm: FdeAlgorithmVersion::PaperV1,
        repetitions: 40,
        simhash_bits: 4,
        input_dimension: 128,
        inner: InnerProjection::Rademacher { d_proj: 16 },
        final_projection: FinalProjection::None,
    };
    let epoch = test_epoch(128, 16, PREPROCESSING_DIGEST);
    let candidate_vector_transform =
        dev_centering_transform(&harness.store, &namespace, &epoch, &fragments).await;
    let (profile, incarnation) = setup_profile_with_recipe(
        &harness.store,
        &namespace,
        128,
        16,
        params,
        LAB_TRANSFORM_SEED,
        candidate_vector_transform,
        PREPROCESSING_DIGEST,
    )
    .await;
    assert_eq!(profile.epoch.id, epoch.id);
    let provider = provider(&profile);
    for fragment in fragments {
        WalWriter::new(harness.store.clone())
            .append_retrieval_units(&namespace, fragment, Vec::new(), Vec::new())
            .await
            .expect("recall input fragment must append");
    }
    enrich_all(&harness.store, &namespace, incarnation, &profile).await;

    let routed_mmli = MmliConfig {
        allow_dev_encoder: true,
        ..MmliConfig::default()
    };
    assert_eq!(routed_mmli.segment.nlist, 256);
    assert_eq!(routed_mmli.segment.probe_budget, 16);
    assert_eq!(routed_mmli.segment.candidate_k, 537);
    let overlay_manifest = read_manifest(&harness.store, &namespace, incarnation).await;
    let mut gold = Vec::with_capacity(queries.len());
    for query in &queries {
        let output = run_query_with_text(
            &harness.store,
            &namespace,
            overlay_manifest.clone(),
            provider.as_ref(),
            &routed_mmli,
            None,
            query,
            10,
        )
        .await;
        assert!(output.read_trace.is_none());
        assert_eq!(output.results.len(), 10);
        gold.push(output.results);
    }

    let compact_started = Instant::now();
    compactor(&harness.store, &routed_mmli)
        .compact(&namespace)
        .await
        .expect("dev regression corpus compaction must succeed");
    let routed_compaction_ms = compact_started.elapsed().as_millis();
    let routed_manifest = read_manifest(&harness.store, &namespace, incarnation).await;
    let (routed_hits, gold_count, routed_mean_planned_bytes) = measure_segment_recall(
        &harness.store,
        &namespace,
        &routed_manifest,
        provider.as_ref(),
        &routed_mmli,
        &queries,
        &gold,
    )
    .await;
    let routed_recall = routed_hits as f64 / gold_count as f64;
    println!(
        "phase9_dev_recall_tripwire documents={DOCUMENT_COUNT} queries={} \
         hits={routed_hits}/{gold_count} recall={routed_recall:.6} nlist={} nprobe={} \
         candidate_k={} mean_planned_bytes={routed_mean_planned_bytes} \
         compaction_ms={routed_compaction_ms}",
        queries.len(),
        routed_mmli.segment.nlist,
        routed_mmli.segment.probe_budget,
        routed_mmli.segment.candidate_k,
    );

    WalWriter::new(harness.store.clone())
        .append_retrieval_units(
            &namespace,
            Vec::new(),
            vec!["phase9-dev-convergence-rebuild-trigger".to_string()],
            Vec::new(),
        )
        .await
        .expect("dev convergence rebuild trigger must append");
    let mut convergence_mmli = routed_mmli.clone();
    convergence_mmli.segment.probe_budget = convergence_mmli.segment.nlist;
    convergence_mmli.segment.candidate_k = DOCUMENT_COUNT;
    let compact_started = Instant::now();
    compactor(&harness.store, &convergence_mmli)
        .compact(&namespace)
        .await
        .expect("dev convergence corpus compaction must succeed");
    let convergence_compaction_ms = compact_started.elapsed().as_millis();
    let convergence_manifest = read_manifest(&harness.store, &namespace, incarnation).await;
    let mut mismatched_queries = Vec::new();
    let mut convergence_planned_bytes = 0_u64;
    for (query, expected) in queries.iter().zip(&gold) {
        let output = run_query_with_text(
            &harness.store,
            &namespace,
            convergence_manifest.clone(),
            provider.as_ref(),
            &convergence_mmli,
            None,
            query,
            10,
        )
        .await;
        if !ranked_result_membership_is_identical(expected, &output.results) {
            mismatched_queries.push(query.clone());
        }
        let trace = output
            .read_trace
            .expect("dev convergence query must use both read waves");
        convergence_planned_bytes +=
            trace.candidate_wave.planned_bytes + trace.truth_wave.planned_bytes;
    }
    let mean_convergence_planned_bytes = convergence_planned_bytes / queries.len() as u64;
    println!(
        "phase9_dev_full_probe_convergence documents={DOCUMENT_COUNT} queries={} \
         identical_queries={}/{} nlist={} nprobe={} candidate_k={} \
         mean_planned_bytes={mean_convergence_planned_bytes} \
         compaction_ms={convergence_compaction_ms}",
        queries.len(),
        queries.len() - mismatched_queries.len(),
        queries.len(),
        convergence_mmli.segment.nlist,
        convergence_mmli.segment.probe_budget,
        convergence_mmli.segment.candidate_k,
    );

    harness.cleanup_artifact_origin_namespace(&namespace).await;
    assert_eq!(
        routed_hits, PINNED_TRIPWIRE_HITS,
        "DeterministicDev production-point recall changed"
    );
    assert!(
        mismatched_queries.is_empty(),
        "full-probe persisted results differed from the exhaustive oracle for queries: \
         {mismatched_queries:?}"
    );
}

const REAL_REPLAY_DIMENSION: u32 = 128;
const REAL_REPLAY_TRANSFORM_SEED: u64 = 5_570_192_190_543_495_170;
const REAL_REPLAY_TRANSFORM_SHA256: &str =
    "00ad4edb4292ddd64c6df00c84c2f8dfced3a092d9ddc307239d9e070deb2ad4";
const REAL_REPLAY_FRAGMENT_MATRIX_BYTES: usize = 48 * 1024 * 1024;

#[derive(Clone, Copy)]
struct RealReplayLane {
    name: &'static str,
    document_count: usize,
    query_count: usize,
    documents_sha256: &'static str,
    queries_sha256: &'static str,
    diagnostics_file: &'static str,
    candidate_k: usize,
    recall_gate: f64,
    candidate_pooling: CandidateDocumentPooling,
    center_candidates: bool,
}

fn real_replay_lane(value: &str) -> RealReplayLane {
    match value {
        "text" => RealReplayLane {
            name: "text",
            document_count: 5_183,
            query_count: 1_109,
            documents_sha256: "1960f7bc88a667beb76b6e15a750469e615aafe9a925928c23f7c546d12cfe22",
            queries_sha256: "cefbff5713a3944f4007676b243985f393f87a7a7579bb5cba6ca09899b0aa0c",
            diagnostics_file: "lab-diagnostics.json",
            candidate_k: 537,
            recall_gate: 0.95,
            candidate_pooling: CandidateDocumentPooling::Identity,
            center_candidates: true,
        },
        "visual" => RealReplayLane {
            name: "visual",
            document_count: 2_000,
            query_count: 533,
            documents_sha256: "88cddc35e4ab3176c94b77b3b7eb2953a2cd8441b54d8ace351f7b99db400f8e",
            queries_sha256: "11633637893ae8bd589ac7b1b72f9c4d9d609bd9576cdbe804ffabefb2e3577a",
            diagnostics_file: "lab-visual-diagnostics.json",
            candidate_k: 300,
            recall_gate: 0.90,
            candidate_pooling: CandidateDocumentPooling::ContiguousMean { factor: 2 },
            center_candidates: false,
        },
        other => panic!("MMLI_REAL_MATRIX_LANE must be text or visual, got {other:?}"),
    }
}

fn checksum_from_hex(value: &str) -> ArtifactChecksum {
    assert_eq!(value.len(), 64);
    let mut bytes = [0_u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .expect("checksum must be lowercase hexadecimal");
    }
    ArtifactChecksum::new(bytes)
}

fn real_replay_epoch(
    lane: RealReplayLane,
    documents: &FileBackedF16Tensor,
    queries: &FileBackedF16Tensor,
) -> MultiVectorEpoch {
    let mut epoch = MultiVectorEpoch {
        id: MultiVectorEpochId::new([0; 32]),
        encoder: EncoderExecutionRef {
            implementation: "phase9_matrix_replay".to_string(),
            version: "pinned_phase2_f16_v1".to_string(),
            bundle_prefix: Some(format!("tests/mmli/phase9-replay/{}", lane.name)),
            artifact_digests: BTreeMap::from([
                (
                    "documents".to_string(),
                    checksum_from_hex(lane.documents_sha256),
                ),
                (
                    "queries".to_string(),
                    checksum_from_hex(lane.queries_sha256),
                ),
            ]),
            supported_modalities: vec![InputModality::Text],
        },
        preprocessing_digest: ArtifactChecksum::digest(
            format!("phase9-real-matrix-replay-{}-v1", lane.name).as_bytes(),
        ),
        vector_dimension: REAL_REPLAY_DIMENSION,
        max_query_vectors: u32::try_from(queries.max_rows())
            .expect("query row maximum must fit u32"),
        max_document_vectors: u32::try_from(documents.max_rows())
            .expect("document row maximum must fit u32"),
        output_normalization: NormalizationRecipe::L2,
        exact_scoring_transform: VectorTransformRecipe::Identity,
        matrix_dtype: MatrixDtype::F16,
        exact_scorer: ExactScorerVersion::MaxSimV1,
    };
    epoch.id = epoch
        .canonical_id()
        .expect("replay epoch must canonicalize");
    epoch
}

fn real_replay_documents(
    lane: RealReplayLane,
    documents: &FileBackedF16Tensor,
) -> (
    Vec<Vec<RetrievalUnitRecord>>,
    Vec<zeppelin::embedding::ContentHash>,
) {
    let mut fragments = Vec::new();
    let mut fragment = Vec::new();
    let mut fragment_matrix_bytes = 0_usize;
    let mut content_hashes = Vec::with_capacity(documents.count());
    for index in 0..documents.count() {
        let matrix_bytes = documents
            .matrix_bytes(index)
            .expect("verified replay matrix must have a byte count");
        if !fragment.is_empty()
            && fragment_matrix_bytes
                .checked_add(matrix_bytes)
                .is_none_or(|bytes| bytes > REAL_REPLAY_FRAGMENT_MATRIX_BYTES)
        {
            fragments.push(std::mem::take(&mut fragment));
            fragment_matrix_bytes = 0;
        }
        let record = text_record(
            &production_document_id(index),
            &format!(
                "phase nine {} matrix replay document {index:020}",
                lane.name
            ),
            lane.name,
        );
        content_hashes.push(record.content_hash);
        fragment.push(record);
        fragment_matrix_bytes = fragment_matrix_bytes
            .checked_add(matrix_bytes)
            .expect("replay fragment matrix bytes must not overflow");
    }
    if !fragment.is_empty() {
        fragments.push(fragment);
    }
    (fragments, content_hashes)
}

fn real_replay_query_texts(lane: RealReplayLane) -> Vec<String> {
    (0..lane.query_count)
        .map(|index| format!("phase nine {} matrix replay query {index:020}", lane.name))
        .collect()
}

fn real_replay_provider(
    profile: &EmbeddingProfileRef,
    documents: FileBackedF16Tensor,
    queries: FileBackedF16Tensor,
    document_hashes: Vec<zeppelin::embedding::ContentHash>,
    query_texts: Vec<String>,
) -> Arc<dyn MultiVectorEncoderProvider> {
    let encoder = FileBackedMultiVectorEncoder::new(
        &profile.epoch,
        documents,
        queries,
        document_hashes,
        query_texts,
    )
    .expect("verified replay tensors must bind to the replay epoch");
    let registry = Arc::new(MultiVectorEncoderRegistry::new());
    registry
        .register(Arc::new(encoder))
        .expect("matrix replay encoder must register");
    registry
}

struct RealReplayMeasurement {
    hits: usize,
    gold_count: usize,
    mean_planned_bytes: u64,
    mean_planned_requests: u64,
    p50_millis: u128,
    p95_millis: u128,
    frontiers: Vec<BTreeSet<String>>,
    final_top_tens: Vec<BTreeSet<String>>,
}

async fn measure_real_replay(
    store: &ZeppelinStore,
    namespace: &str,
    manifest: &Manifest,
    provider: &dyn MultiVectorEncoderProvider,
    mmli: &MmliConfig,
    queries: &[String],
    gold: &[Vec<String>],
) -> RealReplayMeasurement {
    let mut hits = 0_usize;
    let mut planned_bytes = 0_u64;
    let mut planned_requests = 0_u64;
    let mut latencies = Vec::with_capacity(queries.len());
    let mut frontiers = Vec::with_capacity(queries.len());
    let mut final_top_tens = Vec::with_capacity(queries.len());
    for (query, expected) in queries.iter().zip(gold) {
        let started = Instant::now();
        let output = run_query_with_text(
            store,
            namespace,
            manifest.clone(),
            provider,
            mmli,
            None,
            query,
            mmli.segment.candidate_k,
        )
        .await;
        latencies.push(started.elapsed().as_millis());
        let trace = output
            .read_trace
            .expect("real matrix replay must use both planned waves");
        planned_bytes = planned_bytes
            .checked_add(trace.candidate_wave.planned_bytes)
            .and_then(|bytes| bytes.checked_add(trace.truth_wave.planned_bytes))
            .expect("planned replay bytes must not overflow");
        planned_requests = planned_requests
            .checked_add(trace.candidate_wave.planned_requests as u64)
            .and_then(|requests| requests.checked_add(trace.truth_wave.planned_requests as u64))
            .expect("planned replay requests must not overflow");
        let frontier = output
            .results
            .iter()
            .map(|result| result.id.clone())
            .collect::<BTreeSet<_>>();
        let final_top_ten = output
            .results
            .iter()
            .take(10)
            .map(|result| result.id.clone())
            .collect::<BTreeSet<_>>();
        hits += expected
            .iter()
            .filter(|id| final_top_ten.contains(id.as_str()))
            .count();
        frontiers.push(frontier);
        final_top_tens.push(final_top_ten);
    }
    latencies.sort_unstable();
    let percentile = |percent: usize| {
        let rank = queries.len().saturating_mul(percent).div_ceil(100);
        latencies[rank.saturating_sub(1).min(latencies.len() - 1)]
    };
    RealReplayMeasurement {
        hits,
        gold_count: gold.iter().map(Vec::len).sum(),
        mean_planned_bytes: planned_bytes / queries.len() as u64,
        mean_planned_requests: planned_requests / queries.len() as u64,
        p50_millis: percentile(50),
        p95_millis: percentile(95),
        frontiers,
        final_top_tens,
    }
}

async fn trigger_real_replay_rebuild(store: &ZeppelinStore, namespace: &str, label: &str) {
    WalWriter::new(store.clone())
        .append_retrieval_units(
            namespace,
            Vec::new(),
            vec![format!("phase9-real-replay-rebuild-{label}")],
            Vec::new(),
        )
        .await
        .expect("real replay rebuild trigger must append");
}

fn print_real_replay_measurement(
    lane: RealReplayLane,
    arm: &str,
    mmli: &MmliConfig,
    measurement: &RealReplayMeasurement,
    compaction_millis: u128,
) {
    let recall = measurement.hits as f64 / measurement.gold_count as f64;
    println!(
        "phase9_real_matrix_recall lane={} arm={arm} hits={}/{} recall={recall:.6} \
         gate={:.6} passed={} nlist={} nprobe={} candidate_k={} \
         mean_planned_bytes={} mean_planned_requests={} p50_ms={} p95_ms={} \
         compaction_ms={compaction_millis}",
        lane.name,
        measurement.hits,
        measurement.gold_count,
        lane.recall_gate,
        recall >= lane.recall_gate,
        mmli.segment.nlist,
        mmli.segment.probe_budget,
        mmli.segment.candidate_k,
        measurement.mean_planned_bytes,
        measurement.mean_planned_requests,
        measurement.p50_millis,
        measurement.p95_millis,
    );
}

#[tokio::test]
#[ignore = "requires explicitly regenerated pinned Phase 2 f16 tensors"]
async fn late_segment_recall_matches_real_lab_matrices() {
    require_minio();
    let tensor_directory = PathBuf::from(
        std::env::var_os("MMLI_REAL_MATRIX_DIR")
            .expect("MMLI_REAL_MATRIX_DIR must name the pinned tensor directory"),
    );
    let lane = real_replay_lane(
        &std::env::var("MMLI_REAL_MATRIX_LANE")
            .expect("MMLI_REAL_MATRIX_LANE must be text or visual"),
    );
    let documents = FileBackedF16Tensor::load_verified(
        tensor_directory.join(format!("{}-documents.f16", lane.name)),
        tensor_directory.join(format!("{}-documents.json", lane.name)),
        lane.documents_sha256,
    )
    .expect("pinned document tensor must validate");
    let queries = FileBackedF16Tensor::load_verified(
        tensor_directory.join(format!("{}-queries.f16", lane.name)),
        tensor_directory.join(format!("{}-queries.json", lane.name)),
        lane.queries_sha256,
    )
    .expect("pinned query tensor must validate");
    assert_eq!(documents.count(), lane.document_count);
    assert_eq!(queries.count(), lane.query_count);
    let diagnostics = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tasks/MMLI-2/results")
        .join(lane.diagnostics_file);
    let gold = load_config_e_gold_ranks(&diagnostics, &documents, &queries)
        .expect("config-E exact gold must match the pinned tensor sidecars");
    let query_texts = real_replay_query_texts(lane);
    let (fragments, document_hashes) = real_replay_documents(lane, &documents);
    let epoch = real_replay_epoch(lane, &documents, &queries);

    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace(&format!("real-matrix-{}", lane.name));
    let candidate_transform = if lane.center_candidates {
        let mean = documents
            .sampled_mean(5_000)
            .expect("pinned text tensor mean must compute");
        store_centering_transform(&harness.store, &namespace, &epoch, mean).await
    } else {
        VectorTransformRecipe::Identity
    };
    let params = FdeParams {
        algorithm: FdeAlgorithmVersion::PaperV1,
        repetitions: 40,
        simhash_bits: 4,
        input_dimension: REAL_REPLAY_DIMENSION,
        inner: InnerProjection::Rademacher { d_proj: 16 },
        final_projection: FinalProjection::None,
    };
    let (profile, incarnation) = setup_profile_for_epoch(
        &harness.store,
        &namespace,
        epoch,
        params,
        REAL_REPLAY_TRANSFORM_SEED,
        candidate_transform,
        lane.candidate_pooling,
        &format!("phase-9-real-matrix-{}", lane.name),
    )
    .await;
    let transform_checksum_matches =
        profile.fde.transform_artifact.checksum.to_hex() == REAL_REPLAY_TRANSFORM_SHA256;
    let provider = real_replay_provider(
        &profile,
        documents.clone(),
        queries,
        document_hashes,
        query_texts.clone(),
    );
    for fragment in fragments {
        WalWriter::new(harness.store.clone())
            .append_retrieval_units(&namespace, fragment, Vec::new(), Vec::new())
            .await
            .expect("real matrix replay fragment must append");
    }
    enrich_all_with_provider(
        &harness.store,
        &namespace,
        incarnation,
        provider.clone(),
        64,
    )
    .await;

    let mut full_probe = MmliConfig::default();
    full_probe.segment.nlist = 256;
    full_probe.segment.probe_budget = full_probe.segment.nlist;
    full_probe.segment.candidate_k = lane.candidate_k;
    let started = Instant::now();
    compactor(&harness.store, &full_probe)
        .compact(&namespace)
        .await
        .expect("full-probe real matrix compaction must succeed");
    let full_probe_compaction_millis = started.elapsed().as_millis();
    let manifest = read_manifest(&harness.store, &namespace, incarnation).await;
    let full_probe_measurement = measure_real_replay(
        &harness.store,
        &namespace,
        &manifest,
        provider.as_ref(),
        &full_probe,
        &query_texts,
        &gold,
    )
    .await;
    print_real_replay_measurement(
        lane,
        "full_probe",
        &full_probe,
        &full_probe_measurement,
        full_probe_compaction_millis,
    );

    trigger_real_replay_rebuild(&harness.store, &namespace, "routed-operating-point").await;
    let mut routed = full_probe.clone();
    routed.segment.probe_budget = 16;
    let routed_operating_point_is_pinned = routed.segment.nlist == 256
        && routed.segment.probe_budget == 16
        && routed.segment.candidate_k == lane.candidate_k;
    let started = Instant::now();
    compactor(&harness.store, &routed)
        .compact(&namespace)
        .await
        .expect("routed real matrix compaction must succeed");
    let routed_compaction_millis = started.elapsed().as_millis();
    let manifest = read_manifest(&harness.store, &namespace, incarnation).await;
    let routed_measurement = measure_real_replay(
        &harness.store,
        &namespace,
        &manifest,
        provider.as_ref(),
        &routed,
        &query_texts,
        &gold,
    )
    .await;
    print_real_replay_measurement(
        lane,
        "routed",
        &routed,
        &routed_measurement,
        routed_compaction_millis,
    );

    trigger_real_replay_rebuild(&harness.store, &namespace, "routing-containment").await;
    let mut containment = routed.clone();
    containment.segment.candidate_k = lane.document_count;
    let started = Instant::now();
    compactor(&harness.store, &containment)
        .compact(&namespace)
        .await
        .expect("routing-containment compaction must succeed");
    let containment_compaction_millis = started.elapsed().as_millis();
    let manifest = read_manifest(&harness.store, &namespace, incarnation).await;
    let containment_measurement = measure_real_replay(
        &harness.store,
        &namespace,
        &manifest,
        provider.as_ref(),
        &containment,
        &query_texts,
        &gold,
    )
    .await;
    print_real_replay_measurement(
        lane,
        "routing_containment",
        &containment,
        &containment_measurement,
        containment_compaction_millis,
    );

    let mut routing_losses = 0_usize;
    let mut frontier_losses = 0_usize;
    let mut rerank_losses = 0_usize;
    let mut recovered = 0_usize;
    for (((expected, routed_frontier), routed_top_ten), containment_frontier) in gold
        .iter()
        .zip(&routed_measurement.frontiers)
        .zip(&routed_measurement.final_top_tens)
        .zip(&containment_measurement.frontiers)
    {
        for id in expected {
            if !containment_frontier.contains(id) {
                routing_losses += 1;
            } else if !routed_frontier.contains(id) {
                frontier_losses += 1;
            } else if !routed_top_ten.contains(id) {
                rerank_losses += 1;
            } else {
                recovered += 1;
            }
        }
    }
    let attributed = routing_losses + frontier_losses + rerank_losses + recovered;
    println!(
        "phase9_real_matrix_loss_attribution lane={} golds={attributed} routing={} \
         frontier={} rerank={} recovered={recovered}",
        lane.name, routing_losses, frontier_losses, rerank_losses,
    );

    harness.cleanup_artifact_origin_namespace(&namespace).await;
    let full_probe_recall =
        full_probe_measurement.hits as f64 / full_probe_measurement.gold_count as f64;
    let routed_recall = routed_measurement.hits as f64 / routed_measurement.gold_count as f64;
    assert!(
        transform_checksum_matches,
        "real replay FDE transform checksum differed from Phase 2"
    );
    assert!(
        routed_operating_point_is_pinned,
        "real replay routed arm did not use the frozen operating point"
    );
    assert_eq!(attributed, routed_measurement.gold_count);
    assert_eq!(rerank_losses, 0, "exact rerank dropped an admitted gold");
    assert!(
        full_probe_recall >= lane.recall_gate,
        "{} full-probe recall {full_probe_recall:.6} missed gate {:.6}",
        lane.name,
        lane.recall_gate
    );
    assert!(
        routed_recall >= lane.recall_gate,
        "{} routed recall {routed_recall:.6} missed gate {:.6}",
        lane.name,
        lane.recall_gate
    );
}
