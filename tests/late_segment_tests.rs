mod common;
#[path = "common/mmli_tensor.rs"]
mod mmli_tensor;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use chrono::Duration as ChronoDuration;
use uuid::Uuid;
use zeppelin::cache::DiskCache;
use zeppelin::compaction::gc::{GcNamespaceIncarnation, GcRunner};
use zeppelin::compaction::Compactor;
use zeppelin::config::GcConfig;
use zeppelin::config::{Config, MmliConfig, MmliSegmentConfig};
use zeppelin::embedding::{
    ArtifactChecksum, CandidateDocumentPooling, CenteringArtifact, DeterministicDev,
    EmbeddingProfileId, EmbeddingProfileRef, EncoderDocumentInput, EncoderExecutionRef,
    EncoderInputRef, ExactScorerVersion, FdeRecipe, FdeTransformArtifactRef, ImageObjectRef,
    InputModality, Int8QualificationStamp, MatrixDtype, MeanVectorRef, MultiVectorEncoder,
    MultiVectorEncoderProvider, MultiVectorEncoderRegistry, MultiVectorEpoch, MultiVectorEpochId,
    NormalizationRecipe, RetrievalUnitRecord, TextContentRef, VectorTransformRecipe,
    CENTERING_ARTIFACT_FORMAT_VERSION, DETERMINISTIC_DEV_IMPLEMENTATION, DETERMINISTIC_DEV_VERSION,
    INT8_QUALIFICATION_STAMP_VERSION,
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

#[cfg(feature = "branching-test-support")]
use zeppelin::config::{BranchingConfig, IndexingConfig};
#[cfg(feature = "branching-test-support")]
use zeppelin::error::ZeppelinError;
#[cfg(feature = "branching-test-support")]
use zeppelin::namespace::branching::test_support::{
    activate_fork_for_test, branch_control_snapshot, delete_namespace_for_test,
    delete_namespace_with_config_and_clock_for_test, resume_delete_with_config_and_clock_for_test,
};
#[cfg(feature = "branching-test-support")]
use zeppelin::namespace::branching::{BranchError, NamespaceDeleteOutcome};
#[cfg(feature = "branching-test-support")]
use zeppelin::namespace::NamespaceId;
#[cfg(feature = "branching-test-support")]
use zeppelin::time::{Clock, TimeSource};

use common::counting::counting_store;
use common::harness::TestHarness;
use mmli_tensor::{
    load_config_e_gold_ranks, production_document_id, FileBackedF16Tensor,
    FileBackedMultiVectorEncoder,
};

#[cfg(feature = "branching-test-support")]
#[derive(Debug)]
struct AdjustableWallClock(std::sync::Mutex<chrono::DateTime<chrono::Utc>>);

#[cfg(feature = "branching-test-support")]
impl AdjustableWallClock {
    fn new(now: chrono::DateTime<chrono::Utc>) -> Self {
        Self(std::sync::Mutex::new(now))
    }

    fn jump(&self, delta: chrono::Duration) {
        let mut now = self.0.lock().expect("test wall clock mutex poisoned");
        *now += delta;
    }
}

#[cfg(feature = "branching-test-support")]
impl TimeSource for AdjustableWallClock {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        *self.0.lock().expect("test wall clock mutex poisoned")
    }
}

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

#[allow(clippy::too_many_arguments)]
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
        None,
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
    int8_qualification: Option<Int8QualificationStamp>,
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
        int8_qualification,
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
    run_query_cached(store, None, namespace, manifest, provider, mmli, filter).await
}

#[allow(clippy::too_many_arguments)]
async fn run_query_cached(
    store: &ZeppelinStore,
    bootstrap_cache: Option<&DiskCache>,
    namespace: &str,
    manifest: Manifest,
    provider: &dyn MultiVectorEncoderProvider,
    mmli: &MmliConfig,
    filter: Option<&Filter>,
) -> LateInteractionSearchOutput {
    run_query_with_text(
        store,
        bootstrap_cache,
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
    bootstrap_cache: Option<&DiskCache>,
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
        bootstrap_cache,
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
            None,
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

    let cache_dir = tempfile::tempdir().expect("bootstrap cache dir");
    let bootstrap_cache =
        DiskCache::new_with_max_bytes(cache_dir.path().join("cache"), 64 * 1024 * 1024)
            .expect("bootstrap cache");
    let (counted_store, counter) = counting_store(&harness.store);
    counter.reset();
    let first_segment_query = run_query_cached(
        &counted_store,
        Some(&bootstrap_cache),
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
    assert_eq!(first_trace.candidate_wave.logical_ranges, 0);
    assert_eq!(first_trace.candidate_wave.planned_requests, 0);
    assert_eq!(first_trace.candidate_wave.planned_bytes, 0);
    assert!(first_trace.truth_wave.logical_ranges > 0);
    assert!(first_trace.truth_wave.planned_requests > 0);
    assert!(first_trace.truth_wave.planned_requests <= first_trace.truth_wave.logical_ranges);
    assert_eq!(
        counter.gets_matching("flat-sq8-"),
        1,
        "cold wave one must hydrate the flat artifact exactly once"
    );
    assert_eq!(counter.gets_matching("candidate-cluster-"), 0);
    let observed_truth_gets = counter.gets_matching("/matrix_") + counter.gets_matching("/attrs_");
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
    let filtered = run_query_cached(
        &counted_store,
        Some(&bootstrap_cache),
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
    assert_eq!(filtered_trace.candidate_wave.planned_requests, 0);
    assert!(filtered_trace.truth_wave.planned_requests > 0);
    assert!(filtered_trace.truth_wave.planned_requests <= first_trace.truth_wave.planned_requests);
    assert_eq!(
        counter.gets_matching("flat-sq8-"),
        0,
        "warm wave one must perform zero candidate reads"
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
    // Re-pinned 2026-07-31 on the flat-SQ8 candidate path at the production
    // defaults (exhaustive SQ8, candidate_k 1000). The prior IVF-routed pin
    // (nlist 256 / nprobe 16 / K 537) was 163.
    const PINNED_TRIPWIRE_HITS: usize = 260;

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
    assert_eq!(routed_mmli.segment.candidate_k, 1000);
    let overlay_manifest = read_manifest(&harness.store, &namespace, incarnation).await;
    let mut gold = Vec::with_capacity(queries.len());
    for query in &queries {
        let output = run_query_with_text(
            &harness.store,
            None,
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
            None,
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
/// SHA-256 of the durable text-lane INT8 production qualification evidence
/// (`tasks/MMLI-2/results/int8-production-qualification.json`), matching the
/// operator-approved tuple in `embedding::types`.
const INT8_TEXT_EVIDENCE_SHA256: &str =
    "e91ef65c9c26a772a7a98e05985ceb7f310a094541d853559fc3aaee0a88794b";

fn real_replay_dtype() -> MatrixDtype {
    match std::env::var("MMLI_REAL_MATRIX_DTYPE").as_deref() {
        Err(_) | Ok("f16") => MatrixDtype::F16,
        Ok("int8_g32") => MatrixDtype::Int8SymV1 { group_size: 32 },
        Ok(other) => panic!("MMLI_REAL_MATRIX_DTYPE must be f16 or int8_g32, got {other:?}"),
    }
}

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
    /// D9.5 parity tripwire: the exact measured hit count at the lane's
    /// operating point. Any change requires a recorded cause (encoder,
    /// transform, or quantizer revision). `None` until first passing run.
    expected_hits: Option<usize>,
    /// D9.5 tail tripwire: maximum queries below 8/10 golds (`None` until a
    /// first passing run pins the lane's tails).
    max_queries_below_8: Option<usize>,
    /// D9.5 deep-tail tripwire: maximum queries below 5/10 golds, pinned
    /// alongside `max_queries_below_8` from the same passing run.
    max_queries_below_5: Option<usize>,
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
            candidate_k: 1_000,
            recall_gate: 0.975,
            expected_hits: Some(10_869),
            max_queries_below_8: Some(12),
            max_queries_below_5: Some(0),
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
            // Pinned from the first passing acceptance run (2026-08-01):
            // 4,817/5,330 = 0.903752 at K=300, tails 62 <8/10 and 7 <5/10.
            expected_hits: Some(4_817),
            max_queries_below_8: Some(62),
            max_queries_below_5: Some(7),
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
    matrix_dtype: MatrixDtype,
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
        matrix_dtype,
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
    per_query_hits: Vec<usize>,
    mean_planned_bytes: u64,
    mean_planned_requests: u64,
    p50_millis: u128,
    p95_millis: u128,
}

impl RealReplayMeasurement {
    fn queries_below(&self, threshold: usize) -> usize {
        self.per_query_hits
            .iter()
            .filter(|&&hits| hits < threshold)
            .count()
    }
}

#[allow(clippy::too_many_arguments)]
async fn measure_real_replay(
    store: &ZeppelinStore,
    bootstrap_cache: &DiskCache,
    namespace: &str,
    manifest: &Manifest,
    provider: &dyn MultiVectorEncoderProvider,
    mmli: &MmliConfig,
    queries: &[String],
    gold: &[Vec<String>],
) -> RealReplayMeasurement {
    let mut hits = 0_usize;
    let mut per_query_hits = Vec::with_capacity(queries.len());
    let mut planned_bytes = 0_u64;
    let mut planned_requests = 0_u64;
    let mut latencies = Vec::with_capacity(queries.len());
    for (query, expected) in queries.iter().zip(gold) {
        let started = Instant::now();
        let output = run_query_with_text(
            store,
            Some(bootstrap_cache),
            namespace,
            manifest.clone(),
            provider,
            mmli,
            None,
            query,
            10,
        )
        .await;
        latencies.push(started.elapsed().as_millis());
        let trace = output
            .read_trace
            .expect("real matrix replay must trace its planned waves");
        assert_eq!(
            trace.candidate_wave.planned_requests, 0,
            "flat wave one must plan zero per-query candidate reads"
        );
        planned_bytes = planned_bytes
            .checked_add(trace.truth_wave.planned_bytes)
            .expect("planned replay bytes must not overflow");
        planned_requests = planned_requests
            .checked_add(trace.truth_wave.planned_requests as u64)
            .expect("planned replay requests must not overflow");
        let final_top_ten = output
            .results
            .iter()
            .take(10)
            .map(|result| result.id.clone())
            .collect::<BTreeSet<_>>();
        let query_hits = expected
            .iter()
            .filter(|id| final_top_ten.contains(id.as_str()))
            .count();
        hits += query_hits;
        per_query_hits.push(query_hits);
    }
    latencies.sort_unstable();
    let percentile = |percent: usize| {
        let rank = queries.len().saturating_mul(percent).div_ceil(100);
        latencies[rank.saturating_sub(1).min(latencies.len() - 1)]
    };
    RealReplayMeasurement {
        hits,
        gold_count: gold.iter().map(Vec::len).sum(),
        per_query_hits,
        mean_planned_bytes: planned_bytes / queries.len() as u64,
        mean_planned_requests: planned_requests / queries.len() as u64,
        p50_millis: percentile(50),
        p95_millis: percentile(95),
    }
}

fn print_real_replay_measurement(
    lane: RealReplayLane,
    matrix_dtype: MatrixDtype,
    mmli: &MmliConfig,
    measurement: &RealReplayMeasurement,
    compaction_millis: u128,
) {
    let dtype_label = match matrix_dtype {
        MatrixDtype::F16 => "f16".to_string(),
        MatrixDtype::Int8SymV1 { group_size } => format!("int8_g{group_size}"),
    };
    let recall = measurement.hits as f64 / measurement.gold_count as f64;
    println!(
        "phase9_real_matrix_recall lane={} kind=flat_sq8 dtype={dtype_label} hits={}/{} \
         recall={recall:.6} \
         gate={:.6} passed={} candidate_k={} min_hits={} below_8={} below_5={} \
         mean_planned_bytes={} mean_planned_requests={} p50_ms={} p95_ms={} \
         compaction_ms={compaction_millis}",
        lane.name,
        measurement.hits,
        measurement.gold_count,
        lane.recall_gate,
        recall >= lane.recall_gate,
        mmli.segment.candidate_k,
        measurement
            .per_query_hits
            .iter()
            .min()
            .copied()
            .unwrap_or(0),
        measurement.queries_below(8),
        measurement.queries_below(5),
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
    let matrix_dtype = real_replay_dtype();
    let epoch = real_replay_epoch(lane, &documents, &queries, matrix_dtype);
    let int8_qualification = match matrix_dtype {
        MatrixDtype::F16 => None,
        MatrixDtype::Int8SymV1 { .. } => Some(Int8QualificationStamp {
            semantic_epoch: epoch.id,
            dtype: matrix_dtype,
            evidence_digest: checksum_from_hex(INT8_TEXT_EVIDENCE_SHA256),
            evidence_version: INT8_QUALIFICATION_STAMP_VERSION,
        }),
    };

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
        int8_qualification,
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

    let mut mmli = MmliConfig::default();
    mmli.segment.candidate_k = lane.candidate_k;
    let operating_point_is_default =
        mmli.segment.candidate_k == MmliSegmentConfig::default().candidate_k;
    let started = Instant::now();
    compactor(&harness.store, &mmli)
        .compact(&namespace)
        .await
        .expect("flat real matrix compaction must succeed");
    let compaction_millis = started.elapsed().as_millis();
    let manifest = read_manifest(&harness.store, &namespace, incarnation).await;
    let cache_dir = tempfile::tempdir().expect("replay bootstrap cache dir");
    let bootstrap_cache =
        DiskCache::new_with_max_bytes(cache_dir.path().join("cache"), 256 * 1024 * 1024)
            .expect("replay bootstrap cache");
    let measurement = measure_real_replay(
        &harness.store,
        &bootstrap_cache,
        &namespace,
        &manifest,
        provider.as_ref(),
        &mmli,
        &query_texts,
        &gold,
    )
    .await;
    print_real_replay_measurement(lane, matrix_dtype, &mmli, &measurement, compaction_millis);

    harness.cleanup_artifact_origin_namespace(&namespace).await;
    let recall = measurement.hits as f64 / measurement.gold_count as f64;
    assert!(
        transform_checksum_matches,
        "real replay FDE transform checksum differed from Phase 2"
    );
    if lane.name == "text" {
        assert!(
            operating_point_is_default,
            "text lane must measure the production default candidate_k"
        );
    }
    assert!(
        recall >= lane.recall_gate,
        "{} flat recall {recall:.6} missed gate {:.6}",
        lane.name,
        lane.recall_gate
    );
    // The parity and tail tripwires are pinned for the f16 operating point;
    // an INT8 confirm run reports its own numbers against the gate only.
    if matrix_dtype == MatrixDtype::F16 {
        if let Some(expected_hits) = lane.expected_hits {
            assert_eq!(
                measurement.hits, expected_hits,
                "{} parity tripwire: measured hits changed without a recorded \
                 encoder, transform, or quantizer revision (D9.5)",
                lane.name
            );
        }
        if let Some(max_below_8) = lane.max_queries_below_8 {
            assert!(
                measurement.queries_below(8) <= max_below_8,
                "{} tail tripwire: {} queries below 8/10 exceeds pinned {} (D9.5)",
                lane.name,
                measurement.queries_below(8),
                max_below_8
            );
        }
        if let Some(max_below_5) = lane.max_queries_below_5 {
            assert!(
                measurement.queries_below(5) <= max_below_5,
                "{} deep-tail tripwire: {} queries below 5/10 exceeds pinned {} (D9.5)",
                lane.name,
                measurement.queries_below(5),
                max_below_5
            );
        }
    }
}

/// One-time mint helper: prints the canonical INT8 g32 epoch identity for a
/// replay lane so the operator-approved tuple in `embedding::types` can bind
/// it. Requires only the pinned tensors, not MinIO.
#[tokio::test]
#[ignore = "mint helper; requires the pinned Phase 2 tensors"]
async fn print_real_replay_int8_epoch_identity() {
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
    let epoch = real_replay_epoch(
        lane,
        &documents,
        &queries,
        MatrixDtype::Int8SymV1 { group_size: 32 },
    );
    println!(
        "int8_epoch_identity lane={} epoch_id={} evidence_sha256={}",
        lane.name,
        epoch.id.to_hex(),
        INT8_TEXT_EVIDENCE_SHA256,
    );
}

/// W10.2 equivalence gate: appends + updates + deletes compacted
/// incrementally must return byte-equivalent query results to a from-scratch
/// full rebuild of the same logical state, while carrying untouched matrix
/// blocks by reference and keeping their objects retained.
#[tokio::test]
async fn late_segment_incremental_compaction_matches_full_rebuild() {
    require_minio();
    const BASE_DOCUMENTS: usize = 50;
    let harness = TestHarness::new().await;

    let base_fragments: Vec<Vec<RetrievalUnitRecord>> = (0..3)
        .map(|fragment| {
            let start = fragment * 17;
            let end = (start + 17).min(BASE_DOCUMENTS);
            (start..end)
                .map(|index| {
                    text_record(
                        &format!("inc-{index:04}"),
                        &format!("incremental corpus document {index} token {}", index * 7),
                        ["red", "blue", "green"][index % 3],
                    )
                })
                .collect()
        })
        .collect();
    let churn_upserts = vec![
        text_record("inc-0003", "incremental corpus document 3 REVISED", "red"),
        text_record("inc-0021", "incremental corpus document 21 REVISED", "blue"),
        text_record("inc-9001", "incremental appended document 9001", "green"),
        text_record("inc-9002", "incremental appended document 9002", "red"),
        text_record("inc-9003", "incremental appended document 9003", "blue"),
    ];
    let churn_deletes = vec!["inc-0007".to_string(), "inc-0033".to_string()];

    // Small matrix objects force multiple blocks so untouched blocks exist.
    let mut incremental_mmli = mmli_config();
    incremental_mmli.segment.max_matrix_object_bytes = 2048;
    incremental_mmli.segment.candidate_k = 64;
    let mut full_mmli = incremental_mmli.clone();
    full_mmli.segment.incremental_max_changed_fraction = 0.0;

    let mut outputs: Vec<(
        String,
        Uuid,
        Arc<dyn MultiVectorEncoderProvider>,
        MmliConfig,
    )> = Vec::new();
    for (label, mmli) in [
        ("inc", incremental_mmli.clone()),
        ("full", full_mmli.clone()),
    ] {
        let namespace = harness.artifact_origin_namespace(&format!("w102-{label}"));
        let (profile, incarnation) = setup_profile_for_epoch(
            &harness.store,
            &namespace,
            test_epoch(8, 16, b"w102-equivalence-v1"),
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
            CandidateDocumentPooling::Identity,
            &format!("w102-{label}"),
            None,
        )
        .await;
        let provider = provider(&profile);
        for fragment in &base_fragments {
            WalWriter::new(harness.store.clone())
                .append_retrieval_units(&namespace, fragment.clone(), Vec::new(), Vec::new())
                .await
                .expect("base fragment must append");
        }
        enrich_all_with_provider(&harness.store, &namespace, incarnation, provider.clone(), 8)
            .await;
        compactor(&harness.store, &mmli)
            .compact(&namespace)
            .await
            .expect("initial full compaction must succeed");
        outputs.push((namespace, incarnation, provider, mmli));
    }

    // Record the first-generation segment ids before churn.
    let mut first_segment_ids = Vec::new();
    for (namespace, incarnation, _, _) in &outputs {
        let manifest = read_manifest(&harness.store, namespace, *incarnation).await;
        let section = manifest
            .load_late_state(&harness.store)
            .await
            .expect("section must load")
            .expect("section must exist");
        let segment_id = section
            .active_late_segment
            .clone()
            .expect("active segment after initial compaction");
        first_segment_ids.push(segment_id);
    }

    // Churn: updates + deletes + appends, then compact both namespaces.
    for (namespace, incarnation, provider, mmli) in &outputs {
        WalWriter::new(harness.store.clone())
            .append_retrieval_units(
                namespace,
                churn_upserts.clone(),
                churn_deletes.clone(),
                Vec::new(),
            )
            .await
            .expect("churn fragment must append");
        enrich_all_with_provider(&harness.store, namespace, *incarnation, provider.clone(), 8)
            .await;
        compactor(&harness.store, mmli)
            .compact(namespace)
            .await
            .expect("churn compaction must succeed");
    }

    // The incremental namespace must carry old-generation matrix blocks; the
    // forced-full namespace must not.
    let mut carried_keys = Vec::new();
    for (index, (namespace, incarnation, _, _)) in outputs.iter().enumerate() {
        let manifest = read_manifest(&harness.store, namespace, *incarnation).await;
        assert!(
            manifest.input_fragments.is_empty(),
            "compaction must consume every input fragment"
        );
        let section = manifest
            .load_late_state(&harness.store)
            .await
            .expect("section must load")
            .expect("section must exist");
        let active_id = section
            .active_late_segment
            .clone()
            .expect("active segment after churn compaction");
        assert_ne!(active_id, first_segment_ids[index]);
        let segment = section
            .late_interaction_segments
            .iter()
            .find(|segment| segment.id == active_id)
            .expect("active segment descriptor");
        let old_marker = format!("/{}/", first_segment_ids[index]);
        let new_marker = format!("/{active_id}/");
        let carried: Vec<String> = segment
            .matrix_objects
            .iter()
            .filter(|block| block.key.contains(&old_marker))
            .map(|block| block.key.clone())
            .collect();
        let fresh = segment
            .matrix_objects
            .iter()
            .filter(|block| block.key.contains(&new_marker))
            .count();
        if index == 0 {
            assert!(
                !carried.is_empty(),
                "incremental compaction must carry untouched matrix blocks"
            );
            assert!(fresh > 0, "incremental compaction must write fresh blocks");
            carried_keys = carried;
        } else {
            assert!(
                carried.is_empty(),
                "forced-full compaction must not carry old blocks"
            );
        }
    }
    for key in &carried_keys {
        harness
            .store
            .get(key)
            .await
            .expect("carried matrix block must remain retained");
    }

    // Byte-equivalent query results across both namespaces at a full frontier.
    for query_text in [
        "incremental corpus document 3 REVISED",
        "incremental appended document 9002",
        "incremental corpus document 40 token 280",
        "phase ten equivalence probe",
    ] {
        let mut results = Vec::new();
        for (namespace, incarnation, provider, mmli) in &outputs {
            let manifest = read_manifest(&harness.store, namespace, *incarnation).await;
            let output = run_query_with_text(
                &harness.store,
                None,
                namespace,
                manifest,
                provider.as_ref(),
                mmli,
                None,
                query_text,
                64,
            )
            .await;
            results.push(output);
        }
        assert!(
            ranked_results_are_identical(&results[0].results, &results[1].results),
            "incremental and full rebuilds must return identical results for {query_text:?}"
        );
        assert!(!results[0].results.is_empty());
    }

    // A later full rebuild over the incremental segment must hold the strict
    // row closure across carried blocks, and results must stay equivalent.
    let tail_upserts = vec![text_record(
        "inc-9004",
        "incremental appended document 9004",
        "green",
    )];
    for (index, (namespace, incarnation, provider, _)) in outputs.iter().enumerate() {
        WalWriter::new(harness.store.clone())
            .append_retrieval_units(namespace, tail_upserts.clone(), Vec::new(), Vec::new())
            .await
            .expect("tail fragment must append");
        enrich_all_with_provider(&harness.store, namespace, *incarnation, provider.clone(), 8)
            .await;
        let mmli = if index == 0 {
            // Force the incremental namespace through a full rebuild that must
            // read carried blocks back through the strict closure.
            full_mmli.clone()
        } else {
            full_mmli.clone()
        };
        compactor(&harness.store, &mmli)
            .compact(namespace)
            .await
            .expect("tail compaction must succeed");
    }
    let mut tails = Vec::new();
    for (namespace, incarnation, provider, mmli) in &outputs {
        let manifest = read_manifest(&harness.store, namespace, *incarnation).await;
        let output = run_query_with_text(
            &harness.store,
            None,
            namespace,
            manifest,
            provider.as_ref(),
            mmli,
            None,
            "incremental appended document 9004",
            64,
        )
        .await;
        tails.push(output);
    }
    assert!(
        ranked_results_are_identical(&tails[0].results, &tails[1].results),
        "post-full-rebuild results must stay identical"
    );

    for (namespace, _, _, _) in &outputs {
        harness.cleanup_artifact_origin_namespace(namespace).await;
    }
}

/// W10.3 gate (b): exact-key GC over the late segment families after an
/// incremental compaction — carried old-generation blocks survive because the
/// new segment roots them, superseded old objects and a crash orphan under
/// the late-segment prefix collect, and live objects stay untouched.
#[tokio::test]
async fn late_segment_gc_keeps_carried_blocks_and_collects_superseded() {
    require_minio();
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("w103-gc");
    let (profile, incarnation) = setup_profile_for_epoch(
        &harness.store,
        &namespace,
        test_epoch(8, 16, b"w103-gc-v1"),
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
        CandidateDocumentPooling::Identity,
        "w103-gc",
        None,
    )
    .await;
    let provider = provider(&profile);
    let mut mmli = mmli_config();
    mmli.segment.max_matrix_object_bytes = 2048;

    let base: Vec<RetrievalUnitRecord> = (0..40)
        .map(|index| {
            text_record(
                &format!("gc-{index:04}"),
                &format!("gc corpus document {index} token {}", index * 3),
                ["red", "blue"][index % 2],
            )
        })
        .collect();
    WalWriter::new(harness.store.clone())
        .append_retrieval_units(&namespace, base, Vec::new(), Vec::new())
        .await
        .expect("base append must succeed");
    enrich_all_with_provider(&harness.store, &namespace, incarnation, provider.clone(), 8).await;
    compactor(&harness.store, &mmli)
        .compact(&namespace)
        .await
        .expect("initial compaction must succeed");
    let manifest = read_manifest(&harness.store, &namespace, incarnation).await;
    let section = manifest
        .load_late_state(&harness.store)
        .await
        .expect("section must load")
        .expect("section must exist");
    let first_segment = section
        .late_interaction_segments
        .first()
        .expect("initial segment")
        .clone();
    let mut first_keys = BTreeSet::new();
    first_keys.insert(first_segment.flat_candidate.as_ref().unwrap().key.clone());
    first_keys.extend(first_segment.matrix_objects.iter().map(|b| b.key.clone()));
    first_keys.extend(
        first_segment
            .attribute_objects
            .iter()
            .map(|b| b.key.clone()),
    );

    // Churn one row so exactly one block is touched and the rest carry.
    WalWriter::new(harness.store.clone())
        .append_retrieval_units(
            &namespace,
            vec![text_record(
                "gc-0001",
                "gc corpus document 1 REVISED",
                "red",
            )],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("churn append must succeed");
    enrich_all_with_provider(&harness.store, &namespace, incarnation, provider.clone(), 8).await;
    compactor(&harness.store, &mmli)
        .compact(&namespace)
        .await
        .expect("incremental compaction must succeed");
    let manifest = read_manifest(&harness.store, &namespace, incarnation).await;
    let section = manifest
        .load_late_state(&harness.store)
        .await
        .expect("section must load")
        .expect("section must exist");
    let second_id = section
        .active_late_segment
        .clone()
        .expect("second segment id");
    assert_ne!(second_id, first_segment.id);
    let second_segment = section
        .late_interaction_segments
        .iter()
        .find(|segment| segment.id == second_id)
        .expect("second segment descriptor")
        .clone();
    let mut second_keys = BTreeSet::new();
    second_keys.insert(second_segment.flat_candidate.as_ref().unwrap().key.clone());
    second_keys.extend(second_segment.matrix_objects.iter().map(|b| b.key.clone()));
    second_keys.extend(
        second_segment
            .attribute_objects
            .iter()
            .map(|b| b.key.clone()),
    );
    let carried: Vec<String> = first_keys.intersection(&second_keys).cloned().collect();
    let superseded: Vec<String> = first_keys.difference(&second_keys).cloned().collect();
    assert!(
        !carried.is_empty(),
        "incremental compaction must carry old blocks"
    );
    assert!(
        !superseded.is_empty(),
        "incremental compaction must supersede some old objects"
    );

    // A crash orphan with a known artifact shape must collect; an unknown
    // shape must be skipped fail-closed (never deleted).
    let orphan_key = format!(
        "{namespace}/late/segments/{second_id}/flat-sq8-{}.bin",
        "ab".repeat(32)
    );
    harness
        .store
        .put(&orphan_key, Bytes::from_static(b"crash orphan"))
        .await
        .expect("orphan upload must succeed");
    let unknown_shape_key = format!("{namespace}/late/segments/{second_id}/mystery.bin");
    harness
        .store
        .put(&unknown_shape_key, Bytes::from_static(b"unknown shape"))
        .await
        .expect("unknown-shape upload must succeed");

    let latest_modified = harness
        .store
        .list_prefix_meta(&format!("{namespace}/"))
        .await
        .expect("namespace LIST must succeed")
        .into_iter()
        .map(|object| object.last_modified)
        .max()
        .expect("namespace must contain objects");
    let now = latest_modified + ChronoDuration::seconds(5);
    let gc = GcConfig {
        horizon_secs: 1,
        compaction_upload_window_secs: 1,
        skew_slop_secs: 0,
        allow_unsafe_short_horizon: true,
        manifest_history_keep_count: 1,
        pitr_retention_secs: 0,
    };
    let metadata = NamespaceManager::new(harness.store.clone())
        .get(&namespace)
        .await
        .expect("metadata must load");
    let gc_incarnation = GcNamespaceIncarnation::from_metadata(&metadata);
    let mut runner = GcRunner::new(harness.store.clone(), gc);
    for offset in 0..4 {
        runner
            .run_cycle_at(
                gc_incarnation.clone(),
                now + ChronoDuration::seconds(offset),
            )
            .await
            .expect("GC cycle must succeed");
    }

    for key in &carried {
        assert!(
            harness.store.exists(key).await.expect("existence check"),
            "carried block {key} must survive GC"
        );
    }
    for key in second_keys.iter() {
        assert!(
            harness.store.exists(key).await.expect("existence check"),
            "live second-generation object {key} must survive GC"
        );
    }
    for key in &superseded {
        assert!(
            !harness.store.exists(key).await.expect("existence check"),
            "superseded object {key} must collect"
        );
    }
    assert!(
        !harness
            .store
            .exists(&orphan_key)
            .await
            .expect("existence check"),
        "late-segment crash orphan must collect"
    );
    assert!(
        harness
            .store
            .exists(&unknown_shape_key)
            .await
            .expect("existence check"),
        "an unknown-shaped key must be skipped fail-closed, never deleted"
    );

    // The surviving segment must still answer queries after GC.
    let manifest = read_manifest(&harness.store, &namespace, incarnation).await;
    let output = run_query_with_text(
        &harness.store,
        None,
        &namespace,
        manifest,
        provider.as_ref(),
        &mmli,
        None,
        "gc corpus document 1 REVISED",
        10,
    )
    .await;
    assert!(!output.results.is_empty());

    harness.cleanup_artifact_origin_namespace(&namespace).await;
}

/// W10.3 gate (c): the flat bootstrap cache keys artifacts by content
/// digest alone, so distinct artifacts coexist in one shared cache with no
/// collision or cross-pollution, and a re-query of the same artifact stays
/// warm (zero physical reads).
#[tokio::test]
async fn late_flat_bootstrap_cache_isolates_artifacts_by_content_digest() {
    require_minio();
    let harness = TestHarness::new().await;
    let mmli = mmli_config();
    let cache_dir = tempfile::tempdir().expect("cache dir");
    let shared_cache =
        DiskCache::new_with_max_bytes(cache_dir.path().join("cache"), 64 * 1024 * 1024)
            .expect("shared cache");

    let mut states = Vec::new();
    for label in ["cache-a", "cache-b"] {
        let namespace = harness.artifact_origin_namespace(&format!("w103-{label}"));
        let (profile, incarnation) = setup_profile_for_epoch(
            &harness.store,
            &namespace,
            test_epoch(8, 16, b"w103-cache-v1"),
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
            CandidateDocumentPooling::Identity,
            &format!("w103-{label}"),
            None,
        )
        .await;
        let provider = provider(&profile);
        let records: Vec<RetrievalUnitRecord> = (0..12)
            .map(|index| {
                text_record(
                    &format!("{label}-{index:03}"),
                    &format!("{label} corpus document {index}"),
                    "red",
                )
            })
            .collect();
        WalWriter::new(harness.store.clone())
            .append_retrieval_units(&namespace, records, Vec::new(), Vec::new())
            .await
            .expect("append must succeed");
        enrich_all_with_provider(&harness.store, &namespace, incarnation, provider.clone(), 8)
            .await;
        compactor(&harness.store, &mmli)
            .compact(&namespace)
            .await
            .expect("compaction must succeed");
        states.push((namespace, incarnation, provider));
    }

    let (counted_store, counter) = counting_store(&harness.store);
    let mut outputs = Vec::new();
    for (round, (namespace, incarnation, provider)) in states.iter().enumerate() {
        let manifest = read_manifest(&harness.store, namespace, *incarnation).await;
        counter.reset();
        let output = run_query_with_text(
            &counted_store,
            Some(&shared_cache),
            namespace,
            manifest.clone(),
            provider.as_ref(),
            &mmli,
            None,
            "corpus document 5",
            12,
        )
        .await;
        assert_eq!(
            counter.gets_matching("flat-sq8-"),
            1,
            "round {round}: a distinct artifact must hydrate exactly once"
        );
        counter.reset();
        let warm = run_query_with_text(
            &counted_store,
            Some(&shared_cache),
            namespace,
            manifest,
            provider.as_ref(),
            &mmli,
            None,
            "corpus document 5",
            12,
        )
        .await;
        assert_eq!(
            counter.gets_matching("flat-sq8-"),
            0,
            "round {round}: the same artifact must stay warm in the shared cache"
        );
        assert!(ranked_results_are_identical(&output.results, &warm.results));
        outputs.push(output);
    }
    // Distinct artifacts share the cache without cross-pollution: each
    // namespace's results only contain its own ids.
    for (index, (_, _, _)) in states.iter().enumerate() {
        let prefix = if index == 0 { "cache-a-" } else { "cache-b-" };
        assert!(
            outputs[index]
                .results
                .iter()
                .all(|result| result.id.starts_with(prefix)),
            "shared cache must never leak rows across artifacts"
        );
    }

    for (namespace, _, _) in &states {
        harness.cleanup_artifact_origin_namespace(namespace).await;
    }
}

/// W10.3 gate (d): governed namespace destruction sweeps every persisted
/// object family. The family universe is pinned by the Phase 3 conformance
/// fixture: every pre-destruction key must classify into a known family (a
/// new family cannot silently escape the sweep), the late-interaction
/// families must actually be populated — across carried, superseded, and
/// uncompacted-overlay generations — and `finish_delete` must leave zero
/// keys under the namespace prefix.
#[tokio::test]
async fn late_namespace_destruction_leaves_zero_keys_across_all_families() {
    require_minio();
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("w103-destroy");
    let (profile, incarnation) = setup_profile_for_epoch(
        &harness.store,
        &namespace,
        test_epoch(8, 16, b"w103-destroy-v1"),
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
        CandidateDocumentPooling::Identity,
        "w103-destroy",
        None,
    )
    .await;
    let provider = provider(&profile);
    let mut mmli = mmli_config();
    mmli.segment.max_matrix_object_bytes = 2048;

    let mut base: Vec<RetrievalUnitRecord> = (0..24)
        .map(|index| {
            text_record(
                &format!("destroy-{index:04}"),
                &format!("destruction corpus document {index} token {}", index * 3),
                ["red", "blue"][index % 2],
            )
        })
        .collect();
    // A source-backed image unit populates the `sources/` family so the
    // destruction sweep is proven against it too.
    let (image, source, source_bytes) = image_record(&namespace, "destroy-image", "blue");
    base.push(image);
    WalWriter::new(harness.store.clone())
        .append_retrieval_units(&namespace, base, Vec::new(), vec![(source, source_bytes)])
        .await
        .expect("base append must succeed");
    enrich_all_with_provider(&harness.store, &namespace, incarnation, provider.clone(), 8).await;
    compactor(&harness.store, &mmli)
        .compact(&namespace)
        .await
        .expect("initial compaction must succeed");
    // Churn one row so the second generation both carries and supersedes
    // first-generation blocks: destruction must sweep all of them.
    WalWriter::new(harness.store.clone())
        .append_retrieval_units(
            &namespace,
            vec![text_record(
                "destroy-0001",
                "destruction corpus document 1 REVISED",
                "red",
            )],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("churn append must succeed");
    enrich_all_with_provider(&harness.store, &namespace, incarnation, provider.clone(), 8).await;
    compactor(&harness.store, &mmli)
        .compact(&namespace)
        .await
        .expect("incremental compaction must succeed");
    // A final enriched-but-uncompacted append keeps overlay fragments live
    // at destruction time.
    WalWriter::new(harness.store.clone())
        .append_retrieval_units(
            &namespace,
            vec![text_record(
                "destroy-9999",
                "destruction corpus late arrival",
                "blue",
            )],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("overlay append must succeed");
    enrich_all_with_provider(&harness.store, &namespace, incarnation, provider.clone(), 8).await;

    // Family universe pinned by the Phase 3 conformance fixture. The
    // matcher must know exactly the fixture's families: registry growth
    // fails here until the destruction sweep is re-audited.
    let fixture = std::fs::read_to_string("tests/fixtures/mmli2/phase3_family_conformance.tsv")
        .expect("family conformance fixture must be readable");
    let fixture_families: BTreeSet<String> = fixture
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            line.split('\t')
                .next()
                .expect("fixture row must name a family")
                .to_string()
        })
        .collect();
    let exact_families: &[(&str, &str)] = &[
        ("Metadata", "meta.json"),
        ("Manifest", "manifest.json"),
        ("Lease", "lease.json"),
    ];
    let prefix_families: &[(&str, &str)] = &[
        ("ManifestHistory", "manifests/"),
        ("Snapshot", "snapshots/"),
        ("Wal", "wal/"),
        ("InputWal", "input-wal/"),
        ("Source", "sources/"),
        ("Segment", "segments/"),
        ("LateSection", "late/state/"),
        ("MatrixFragment", "late/matrix-fragments/"),
        ("FdeFragment", "late/fde-fragments/"),
        ("FdeTransform", "late/transforms/"),
        ("Centering", "late/centering/"),
        ("Quarantine", "late/quarantine/"),
        ("LateSegment", "late/segments/"),
        ("Staging", "_staging/"),
        ("Gc", "_gc/"),
        (
            "BranchVisibilityRemoved",
            "_lifecycle/branch_visibility_removed/",
        ),
    ];
    let known_families: BTreeSet<String> = exact_families
        .iter()
        .chain(prefix_families)
        .map(|(family, _)| (*family).to_string())
        .collect();
    assert_eq!(
        fixture_families, known_families,
        "family matcher must cover exactly the conformance fixture"
    );

    let namespace_prefix = format!("{namespace}/");
    let pre_keys = harness
        .store
        .list_prefix(&namespace_prefix)
        .await
        .expect("pre-destruction LIST must succeed");
    assert!(
        pre_keys.len() >= 10,
        "fixture namespace must be non-trivial, saw {} keys",
        pre_keys.len()
    );
    let mut populated = BTreeSet::new();
    for key in &pre_keys {
        let relative = key
            .strip_prefix(&namespace_prefix)
            .expect("listed key must live under the namespace prefix");
        let family = exact_families
            .iter()
            .find(|(_, exact)| relative == *exact)
            .or_else(|| {
                prefix_families
                    .iter()
                    .find(|(_, prefix)| relative.starts_with(prefix))
            })
            .map(|(family, _)| *family)
            .unwrap_or_else(|| {
                panic!("key {key} belongs to no known object family: sweep coverage unproven")
            });
        populated.insert(family.to_string());
    }
    for required in [
        "Metadata",
        "Manifest",
        "Source",
        "LateSection",
        "FdeTransform",
        "MatrixFragment",
        "FdeFragment",
        "LateSegment",
    ] {
        assert!(
            populated.contains(required),
            "family {required} must be populated pre-destruction, saw {populated:?}"
        );
    }

    let manager = NamespaceManager::new(harness.store.clone());
    manager
        .start_delete(&namespace)
        .await
        .expect("start_delete must succeed");
    let outcome = manager
        .finish_delete(&namespace, Duration::MAX)
        .await
        .expect("finish_delete must succeed");
    assert!(
        outcome.complete,
        "governed destruction must run to completion"
    );
    let remaining = harness
        .store
        .list_prefix(&namespace_prefix)
        .await
        .expect("post-destruction LIST must succeed");
    assert!(
        remaining.is_empty(),
        "governed destruction must leave zero keys, found {remaining:?}"
    );

    harness.cleanup_artifact_origin_namespace(&namespace).await;
}

/// W10.3 gate (a): an activated branch of a source with an active flat late
/// segment answers queries zero-copy from source-owned artifacts, blocks
/// source deletion while its visible refs are foreign, fully materializes
/// target-owned artifacts on its first compaction, and releases the source
/// root through governed target deletion so the source becomes deletable.
#[cfg(feature = "branching-test-support")]
#[tokio::test]
async fn late_flat_branch_materializes_target_owned_and_releases_source_root() {
    require_minio();
    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("w103-branch-source");
    let target = harness.artifact_origin_namespace("w103-branch-target");
    let (profile, incarnation) = setup_profile_for_epoch(
        &harness.store,
        &source,
        test_epoch(8, 16, b"w103-branch-v1"),
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
        CandidateDocumentPooling::Identity,
        "w103-branch",
        None,
    )
    .await;
    let provider = provider(&profile);
    let mut mmli = mmli_config();
    mmli.segment.max_matrix_object_bytes = 2048;

    let base: Vec<RetrievalUnitRecord> = (0..24)
        .map(|index| {
            text_record(
                &format!("branch-{index:04}"),
                &format!("branch corpus document {index} token {}", index * 3),
                ["red", "blue"][index % 2],
            )
        })
        .collect();
    WalWriter::new(harness.store.clone())
        .append_retrieval_units(&source, base, Vec::new(), Vec::new())
        .await
        .expect("base append must succeed");
    enrich_all_with_provider(&harness.store, &source, incarnation, provider.clone(), 8).await;
    compactor(&harness.store, &mmli)
        .compact(&source)
        .await
        .expect("initial compaction must succeed");
    // Churn one row so the inherited segment contains carried blocks: the
    // branch must resolve a multi-generation flat segment cross-origin.
    WalWriter::new(harness.store.clone())
        .append_retrieval_units(
            &source,
            vec![text_record(
                "branch-0001",
                "branch corpus document 1 REVISED",
                "red",
            )],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("churn append must succeed");
    enrich_all_with_provider(&harness.store, &source, incarnation, provider.clone(), 8).await;
    compactor(&harness.store, &mmli)
        .compact(&source)
        .await
        .expect("incremental compaction must succeed");

    let branching = BranchingConfig {
        enabled: true,
        max_children_per_namespace: 8,
        max_depth: 4,
    };
    activate_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).expect("source id"),
        NamespaceId::new(target.clone()).expect("target id"),
        IndexingConfig::default(),
        branching.clone(),
    )
    .await
    .expect("fork activation must succeed");

    let target_manifest = Manifest::read(&harness.store, &target)
        .await
        .expect("target manifest read must succeed")
        .expect("target manifest must exist");
    assert!(
        target_manifest
            .late_state
            .as_ref()
            .expect("target must inherit the late section")
            .artifact_origin
            .is_some(),
        "the inherited late section must carry its source origin"
    );
    let inherited_section = target_manifest
        .load_late_state(&harness.store)
        .await
        .expect("inherited section must load")
        .expect("inherited section must exist");
    assert!(
        !target_manifest
            .visible_refs_are_local_with_late_state(Some(&inherited_section))
            .expect("locality projection must succeed"),
        "an inherited flat segment must block the locality projection"
    );
    let target_keys = harness
        .store
        .list_prefix(&format!("{target}/"))
        .await
        .expect("target LIST must succeed");
    assert!(
        target_keys.iter().all(|key| !key.contains("/late/")),
        "activation must not copy late artifacts under the target"
    );

    let source_manifest = read_manifest(&harness.store, &source, incarnation).await;
    let source_output = run_query_with_text(
        &harness.store,
        None,
        &source,
        source_manifest,
        provider.as_ref(),
        &mmli,
        None,
        "branch corpus document 7",
        10,
    )
    .await;
    let target_output = run_query_with_text(
        &harness.store,
        None,
        &target,
        target_manifest.clone(),
        provider.as_ref(),
        &mmli,
        None,
        "branch corpus document 7",
        10,
    )
    .await;
    assert!(!target_output.results.is_empty());
    assert!(
        ranked_results_are_identical(&source_output.results, &target_output.results),
        "a zero-copy branch must answer identically to its source"
    );

    let error = delete_namespace_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).expect("source id"),
        IndexingConfig::default(),
        branching.clone(),
    )
    .await
    .expect_err("a live child root must block source deletion");
    assert!(matches!(
        error,
        ZeppelinError::Branch(inner)
            if matches!(*inner, BranchError::NamespaceHasLiveBranches { .. })
    ));

    // The first target compaction must fully materialize: the foreign
    // origin is ineligible for the incremental path, and every late
    // artifact of the new active segment must be target-owned.
    compactor(&harness.store, &mmli)
        .compact(&target)
        .await
        .expect("materializing compaction must succeed");
    let materialized_manifest = Manifest::read(&harness.store, &target)
        .await
        .expect("materialized manifest read must succeed")
        .expect("materialized manifest must exist");
    let materialized_section = materialized_manifest
        .load_late_state(&harness.store)
        .await
        .expect("materialized section must load")
        .expect("materialized section must exist");
    let active_id = materialized_section
        .active_late_segment
        .clone()
        .expect("materialized active segment id");
    let segment = materialized_section
        .late_interaction_segments
        .iter()
        .find(|segment| segment.id == active_id)
        .expect("materialized active segment descriptor");
    let mut segment_keys = vec![segment
        .flat_candidate
        .as_ref()
        .expect("materialized flat artifact")
        .key
        .clone()];
    segment_keys.extend(segment.matrix_objects.iter().map(|block| block.key.clone()));
    segment_keys.extend(
        segment
            .attribute_objects
            .iter()
            .map(|block| block.key.clone()),
    );
    let target_prefix = format!("{target}/");
    assert!(
        segment_keys
            .iter()
            .all(|key| key.starts_with(&target_prefix)),
        "materialization must write target-owned late artifacts, got {segment_keys:?}"
    );
    assert!(
        materialized_manifest
            .visible_refs_are_local_with_late_state(Some(&materialized_section))
            .expect("materialized locality projection must succeed"),
        "a materialized branch must project local visible refs"
    );
    let materialized_output = run_query_with_text(
        &harness.store,
        None,
        &target,
        materialized_manifest,
        provider.as_ref(),
        &mmli,
        None,
        "branch corpus document 7",
        10,
    )
    .await;
    assert!(
        ranked_results_are_identical(&source_output.results, &materialized_output.results),
        "materialization must preserve ranked results"
    );

    // Governed target deletion releases the source root; the source then
    // deletes cleanly with its own governed destruction. Branch-target
    // deletion first parks in the reader-safety visibility grace window, so
    // resume it with a jumped test clock instead of sleeping on wall time.
    let outcome = delete_namespace_for_test(
        harness.store.clone(),
        NamespaceId::new(target.clone()).expect("target id"),
        IndexingConfig::default(),
        branching.clone(),
    )
    .await
    .expect("governed target deletion must run");
    let not_before = match outcome {
        NamespaceDeleteOutcome::BranchGraceWait { not_before } => not_before,
        other => panic!("target deletion must enter the visibility grace window, got {other:?}"),
    };
    let clock_start = chrono::Utc::now();
    let wall_clock = Arc::new(AdjustableWallClock::new(clock_start));
    wall_clock.jump(not_before.signed_duration_since(clock_start));
    let mut grace_config = Config::default();
    grace_config.branching.enabled = true;
    grace_config
        .security
        .set_cursor_hmac_key_hex("42".repeat(32));
    grace_config
        .validate()
        .expect("grace-resume config must validate");
    let outcome = resume_delete_with_config_and_clock_for_test(
        harness.store.clone(),
        NamespaceId::new(target.clone()).expect("target id"),
        &grace_config,
        Clock::from_source(wall_clock.clone()),
        Duration::from_secs(30),
    )
    .await
    .expect("target deletion must resume past the grace window");
    assert!(
        matches!(outcome, NamespaceDeleteOutcome::Deleted),
        "target deletion must complete, got {outcome:?}"
    );
    let control = branch_control_snapshot(&harness.store, &source)
        .await
        .expect("source branch control must load");
    assert!(
        control.roots.is_empty(),
        "target deletion must release the source root"
    );
    // Jump past any delete-machinery lease TTL so the source delete does not
    // trip over the parent lease acquired during root release. A bounded
    // cleanup pass may report AlreadyDeleting; converge through resume.
    wall_clock.jump(ChronoDuration::seconds(600));
    let mut outcome = delete_namespace_with_config_and_clock_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).expect("source id"),
        &grace_config,
        Clock::from_source(wall_clock.clone()),
    )
    .await
    .expect("source deletion must run after root release");
    for _ in 0..5 {
        if matches!(outcome, NamespaceDeleteOutcome::Deleted) {
            break;
        }
        assert!(
            matches!(outcome, NamespaceDeleteOutcome::AlreadyDeleting),
            "source deletion must converge, got {outcome:?}"
        );
        outcome = resume_delete_with_config_and_clock_for_test(
            harness.store.clone(),
            NamespaceId::new(source.clone()).expect("source id"),
            &grace_config,
            Clock::from_source(wall_clock.clone()),
            Duration::from_secs(30),
        )
        .await
        .expect("source deletion resume must run");
    }
    assert!(
        matches!(outcome, NamespaceDeleteOutcome::Deleted),
        "source deletion must complete, got {outcome:?}"
    );

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup_artifact_origin_namespace(&target).await;
    harness.cleanup().await;
}
